#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Set
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

try:
    from migrate_db import migrate_connection
except ImportError:  # pragma: no cover
    migrate_connection = None  # type: ignore

from backfill_events import (
    build_daily_bars,
    build_events,
    compute_atr,
    compute_realized_volatility,
    fetch_gamma_context,
    fetch_market,
    insert_bars,
    insert_events,
    normalize_range_for_source,
    parse_candles,
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _interval_to_seconds(interval: str) -> int:
    if interval.endswith("m"):
        return int(interval[:-1]) * 60
    if interval.endswith("h"):
        return int(interval[:-1]) * 3600
    return int(interval)


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("live_collector")

DB_PATH = os.getenv("PIVOT_DB", str(ROOT_DIR / "data" / "pivot_events.sqlite"))
HOST = os.getenv("LIVE_COLLECTOR_BIND", "127.0.0.1")
PORT = int(os.getenv("LIVE_COLLECTOR_PORT", "5004"))
SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("LIVE_COLLECTOR_SYMBOLS", "SPY").split(",")
    if s.strip()
]
INTERVAL = os.getenv("LIVE_COLLECTOR_INTERVAL", "1m")
RANGE_STR = os.getenv("LIVE_COLLECTOR_RANGE", "2d")
SOURCE = os.getenv("LIVE_COLLECTOR_SOURCE", "yahoo").lower()
THRESHOLD_BPS = float(os.getenv("LIVE_COLLECTOR_THRESHOLD_BPS", "10"))
COOLDOWN_MIN = int(os.getenv("LIVE_COLLECTOR_COOLDOWN_MIN", "10"))
ATR_WINDOW = int(os.getenv("LIVE_COLLECTOR_ATR_WINDOW", "14"))
POLL_SEC = max(5, int(os.getenv("LIVE_COLLECTOR_POLL_SEC", "45")))
WRITE_BARS = _env_bool("LIVE_COLLECTOR_WRITE_BARS", True)
WRITE_EVENTS = _env_bool("LIVE_COLLECTOR_WRITE_EVENTS", True)
SCORE_ENABLED = _env_bool("LIVE_COLLECTOR_SCORE_ENABLED", True)
SCORE_API_URL = os.getenv("LIVE_COLLECTOR_SCORE_URL", "http://127.0.0.1:5003/score")
SCORE_BATCH_SIZE = max(1, int(os.getenv("LIVE_COLLECTOR_SCORE_BATCH_SIZE", "16")))
SCORE_TIMEOUT_SEC = max(1.0, float(os.getenv("LIVE_COLLECTOR_SCORE_TIMEOUT_SEC", "12")))
SCORE_MAX_ATTEMPTS = max(1, int(os.getenv("LIVE_COLLECTOR_SCORE_MAX_ATTEMPTS", "2")))
SCORE_RETRY_BASE_SEC = max(0.0, float(os.getenv("LIVE_COLLECTOR_SCORE_RETRY_BASE_SEC", "0.5")))
SCORE_RETRY_MAX_SEC = max(SCORE_RETRY_BASE_SEC, float(os.getenv("LIVE_COLLECTOR_SCORE_RETRY_MAX_SEC", "4.0")))
SCORE_RATE_LIMIT_COOLDOWN_SEC = max(
    5.0, float(os.getenv("LIVE_COLLECTOR_SCORE_RATE_LIMIT_COOLDOWN_SEC", "180"))
)
SCORE_UNSCORED_LOOKBACK_DAYS = max(0, int(os.getenv("LIVE_COLLECTOR_SCORE_UNSCORED_LOOKBACK_DAYS", "3")))
SCORE_UNSCORED_MAX_PER_CYCLE = max(0, int(os.getenv("LIVE_COLLECTOR_SCORE_UNSCORED_MAX_PER_CYCLE", "12")))
GAMMA_REFRESH_SEC = max(30, int(os.getenv("LIVE_COLLECTOR_GAMMA_REFRESH_SEC", "300")))
GAMMA_RETRY_SEC = max(30, int(os.getenv("LIVE_COLLECTOR_GAMMA_RETRY_SEC", "1800")))
LIVE_COLLECTOR_SQLITE_SYNC = (os.getenv("LIVE_COLLECTOR_SQLITE_SYNC", "FULL") or "FULL").strip().upper()
if LIVE_COLLECTOR_SQLITE_SYNC not in {"OFF", "NORMAL", "FULL", "EXTRA"}:
    LIVE_COLLECTOR_SQLITE_SYNC = "FULL"
LIVE_COLLECTOR_WAL_AUTOCHECKPOINT = max(
    100,
    int(os.getenv("LIVE_COLLECTOR_WAL_AUTOCHECKPOINT", "1000")),
)
INTERVAL_SEC = _interval_to_seconds(INTERVAL)
_DEFAULT_CORS_ORIGINS = "http://127.0.0.1:3000,http://localhost:3000"

if SOURCE not in ("auto", "ibkr", "yahoo", "marketdata"):
    raise ValueError(f"LIVE_COLLECTOR_SOURCE must be auto/ibkr/yahoo/marketdata, got: {SOURCE}")

_state_lock = threading.Lock()
_stop_event = threading.Event()
_gamma_cache_lock = threading.Lock()
_gamma_cache: Dict[str, Dict[str, Any]] = {}
_state: Dict[str, Any] = {
    "status": "starting",
    "started_at_ms": int(time.time() * 1000),
    "last_cycle_start_ms": None,
    "last_cycle_end_ms": None,
    "last_success_ms": None,
    "last_error": None,
    "cycles": 0,
    "symbols": {},
    "score_status": "idle",
    "score_backoff_until_ms": 0,
    "score_backoff_reason": None,
    "score_backoff_skip_cycles": 0,
    "score_backoff_skipped_events": 0,
    "score_rate_limit_count": 0,
    "score_last_rate_limit_ms": None,
}


def _parse_allowed_origins() -> List[str]:
    origins = [
        origin.strip()
        for origin in os.getenv("ML_CORS_ORIGINS", _DEFAULT_CORS_ORIGINS).split(",")
        if origin.strip()
    ]
    return origins or ["http://127.0.0.1:3000"]


ALLOWED_ORIGINS = _parse_allowed_origins()


def _cors_origin(request_origin: str | None) -> str:
    if request_origin and request_origin in ALLOWED_ORIGINS:
        return request_origin
    return ALLOWED_ORIGINS[0]


def _connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(f"PRAGMA synchronous={LIVE_COLLECTOR_SQLITE_SYNC};")
    conn.execute(f"PRAGMA wal_autocheckpoint={LIVE_COLLECTOR_WAL_AUTOCHECKPOINT};")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.row_factory = sqlite3.Row
    if migrate_connection is not None:
        migrate_connection(conn, verbose=False)
    return conn


def _set_state(updates: Dict[str, Any]) -> None:
    with _state_lock:
        _state.update(updates)


def _get_state_snapshot() -> Dict[str, Any]:
    with _state_lock:
        return dict(_state)


def _chunked(items: List[Any], chunk_size: int) -> List[List[Any]]:
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def _score_retry_delay_sec(attempt: int) -> float:
    # Exponential backoff capped by SCORE_RETRY_MAX_SEC.
    if attempt <= 0:
        return 0.0
    return min(SCORE_RETRY_MAX_SEC, SCORE_RETRY_BASE_SEC * (2 ** (attempt - 1)))


class ScoreRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_sec: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_sec = retry_after_sec


def _parse_retry_after_sec(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _is_rate_limited_exception(error: Exception) -> bool:
    if isinstance(error, ScoreRequestError):
        return int(error.status_code or 0) == 429
    lowered = str(error).lower()
    return "429" in lowered or "too many requests" in lowered


def _score_backoff_snapshot() -> tuple[int, str | None]:
    with _state_lock:
        backoff_until_ms = int(_state.get("score_backoff_until_ms") or 0)
        reason_raw = _state.get("score_backoff_reason")
    reason = str(reason_raw) if reason_raw else None
    return backoff_until_ms, reason


def _score_backoff_remaining_ms(now_ms: int | None = None) -> tuple[int, str | None]:
    current_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    until_ms, reason = _score_backoff_snapshot()
    if until_ms > current_ms:
        return until_ms - current_ms, reason
    return 0, None


def _clear_score_backoff_if_expired(now_ms: int) -> bool:
    with _state_lock:
        until_ms = int(_state.get("score_backoff_until_ms") or 0)
        if until_ms <= 0 or now_ms < until_ms:
            return False
        _state["score_backoff_until_ms"] = 0
        _state["score_backoff_reason"] = None
    return True


def _activate_score_backoff(reason: str, retry_after_sec: float | None = None) -> int:
    now_ms = int(time.time() * 1000)
    cooldown_sec = max(float(SCORE_RATE_LIMIT_COOLDOWN_SEC), float(retry_after_sec or 0.0))
    target_until_ms = now_ms + int(cooldown_sec * 1000)
    with _state_lock:
        prev_until = int(_state.get("score_backoff_until_ms") or 0)
        if target_until_ms < prev_until:
            target_until_ms = prev_until
        _state["score_backoff_until_ms"] = target_until_ms
        _state["score_backoff_reason"] = reason
        _state["score_last_rate_limit_ms"] = now_ms
        _state["score_rate_limit_count"] = int(_state.get("score_rate_limit_count") or 0) + 1
    return target_until_ms


def _score_chunk(chunk: List[Dict[str, Any]], headers: Dict[str, str]) -> None:
    payload = json.dumps({"events": chunk}).encode("utf-8")
    req = Request(SCORE_API_URL, data=payload, headers=headers, method="POST")
    retryable_http = {408, 409, 425, 429, 500, 502, 503, 504}
    last_exc: Exception | None = None
    last_status_code: int | None = None
    last_retry_after_sec: float | None = None

    for attempt in range(1, SCORE_MAX_ATTEMPTS + 1):
        try:
            with urlopen(req, timeout=SCORE_TIMEOUT_SEC) as response:
                raw = response.read().decode("utf-8")
            data = json.loads(raw) if raw else {}
            if not isinstance(data, dict):
                raise RuntimeError("ML score response is not JSON object")
            if "results" not in data and "scores" not in data:
                raise RuntimeError("ML score response missing expected keys")
            return
        except HTTPError as exc:
            last_exc = exc
            last_status_code = int(getattr(exc, "code", 0) or 0)
            headers_obj = getattr(exc, "headers", None)
            retry_after_raw = headers_obj.get("Retry-After") if headers_obj is not None else None
            last_retry_after_sec = _parse_retry_after_sec(retry_after_raw)
            retryable = last_status_code in retryable_http
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_exc = exc
            retryable = True

        if attempt >= SCORE_MAX_ATTEMPTS or not retryable:
            break
        delay_sec = _score_retry_delay_sec(attempt)
        log.warning(
            "ML score request retry %d/%d (batch_size=%d, delay=%.2fs): %s",
            attempt + 1,
            SCORE_MAX_ATTEMPTS,
            len(chunk),
            delay_sec,
            last_exc,
        )
        if delay_sec > 0:
            time.sleep(delay_sec)

    raise ScoreRequestError(
        f"ML score request failed: {last_exc}",
        status_code=last_status_code,
        retry_after_sec=last_retry_after_sec,
    )


def _get_gamma_context(symbol: str) -> Dict[str, Any] | None:
    now_ms = int(time.time() * 1000)
    refresh_ms = GAMMA_REFRESH_SEC * 1000
    retry_ms = GAMMA_RETRY_SEC * 1000

    with _gamma_cache_lock:
        entry = dict(_gamma_cache.get(symbol, {}))

    cached_context = entry.get("context")
    status = entry.get("status")
    next_refresh_ms = int(entry.get("next_refresh_ms") or 0)

    if cached_context is not None and now_ms < next_refresh_ms:
        return cached_context
    if cached_context is None and status == "failed" and now_ms < next_refresh_ms:
        return None

    fresh_context = fetch_gamma_context(symbol)
    if fresh_context is not None:
        recovered = status in ("failed", "degraded")
        with _gamma_cache_lock:
            _gamma_cache[symbol] = {
                "context": fresh_context,
                "status": "ok",
                "next_refresh_ms": now_ms + refresh_ms,
                "last_success_ms": now_ms,
            }
        if recovered:
            log.info("Gamma context recovered for %s", symbol)
        return fresh_context

    if cached_context is not None:
        with _gamma_cache_lock:
            _gamma_cache[symbol] = {
                "context": cached_context,
                "status": "degraded",
                "next_refresh_ms": now_ms + retry_ms,
                "last_success_ms": entry.get("last_success_ms"),
            }
        if status != "degraded":
            log.warning(
                "Gamma context unavailable for %s; using cached snapshot and retrying in %ss",
                symbol,
                GAMMA_RETRY_SEC,
            )
        return cached_context

    with _gamma_cache_lock:
        _gamma_cache[symbol] = {
            "context": None,
            "status": "failed",
            "next_refresh_ms": now_ms + retry_ms,
            "last_success_ms": entry.get("last_success_ms"),
        }
    if status != "failed":
        log.warning(
            "Gamma context unavailable for %s; retrying in %ss",
            symbol,
            GAMMA_RETRY_SEC,
        )
    return None


def _fetch_existing_event_ids(conn: sqlite3.Connection, event_ids: List[str]) -> set[str]:
    existing: set[str] = set()
    if not event_ids:
        return existing
    for chunk in _chunked(event_ids, 400):
        placeholders = ",".join(["?"] * len(chunk))
        sql = f"SELECT event_id FROM touch_events WHERE event_id IN ({placeholders})"
        rows = conn.execute(sql, chunk).fetchall()
        existing.update(row[0] for row in rows)
    return existing


def _fetch_unscored_events(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    min_ts_ms: int,
    limit: int,
    exclude_event_ids: Set[str] | None = None,
) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []
    where = [
        "te.symbol = ?",
        "pl.event_id IS NULL",
    ]
    params: List[Any] = [symbol]
    if min_ts_ms > 0:
        where.append("te.ts_event >= ?")
        params.append(min_ts_ms)
    if exclude_event_ids:
        placeholders = ",".join("?" for _ in exclude_event_ids)
        where.append(f"te.event_id NOT IN ({placeholders})")
        params.extend(sorted(exclude_event_ids))
    params.append(int(limit))
    sql = f"""
        SELECT te.*
        FROM touch_events te
        LEFT JOIN prediction_log pl
          ON pl.event_id = te.event_id
         AND COALESCE(pl.is_preview, 0) = 0
        WHERE {" AND ".join(where)}
        ORDER BY te.ts_event DESC
        LIMIT ?
    """
    rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def _score_events(events: List[Dict[str, Any]]) -> int:
    if not events or not SCORE_ENABLED:
        return 0

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "PivotQuantLiveCollector/1.0",
    }
    scored = 0
    for chunk in _chunked(events, SCORE_BATCH_SIZE):
        try:
            _score_chunk(chunk, headers)
            scored += len(chunk)
            continue
        except Exception as chunk_exc:
            if len(chunk) <= 1:
                raise RuntimeError(f"ML score request failed: {chunk_exc}") from chunk_exc
            log.warning(
                "ML score batch failed; retrying each event individually (batch_size=%d): %s",
                len(chunk),
                chunk_exc,
            )

        for event in chunk:
            try:
                _score_chunk([event], headers)
                scored += 1
            except Exception as event_exc:
                event_id = event.get("event_id")
                raise RuntimeError(
                    f"ML score request failed for event_id={event_id}: {event_exc}"
                ) from event_exc
    return scored


def _collect_symbol(conn: sqlite3.Connection, symbol: str) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    range_str = normalize_range_for_source(INTERVAL, RANGE_STR, SOURCE)
    payload, resolved_source = fetch_market(symbol, INTERVAL, range_str, SOURCE)
    candles = parse_candles(payload)
    if not candles:
        return (
            {
                "symbol": symbol,
                "source": resolved_source,
                "bars_inserted": 0,
                "events_built": 0,
                "events_inserted": 0,
                "events_scored": 0,
                "candles": 0,
                "session_count": 0,
            },
            [],
        )

    bars_inserted = 0
    if WRITE_BARS:
        bars_inserted = insert_bars(conn, symbol, candles, INTERVAL_SEC)

    sessions = build_daily_bars(candles)
    if len(sessions) < 2:
        return (
            {
                "symbol": symbol,
                "source": resolved_source,
                "bars_inserted": bars_inserted,
                "events_built": 0,
                "events_inserted": 0,
                "events_scored": 0,
                "candles": len(candles),
                "session_count": len(sessions),
            },
            [],
        )

    atr_by_date = compute_atr(sessions, ATR_WINDOW)
    rv_by_date, rv_regime_by_date = compute_realized_volatility(sessions, window=30)
    gamma_context = _get_gamma_context(symbol)
    events = build_events(
        symbol=symbol,
        sessions=sessions,
        interval_sec=INTERVAL_SEC,
        threshold_bps=THRESHOLD_BPS,
        cooldown_min=COOLDOWN_MIN,
        source=resolved_source,
        atr_by_date=atr_by_date,
        conn=conn,
        rv_by_date=rv_by_date,
        rv_regime_by_date=rv_regime_by_date,
        gamma_context=gamma_context,
    )

    new_events: List[Dict[str, Any]] = []
    events_inserted = 0
    if WRITE_EVENTS and events:
        event_ids = [ev["event_id"] for ev in events if ev.get("event_id")]
        existing_ids = _fetch_existing_event_ids(conn, event_ids)
        new_events = [ev for ev in events if ev.get("event_id") not in existing_ids]
        if new_events:
            events_inserted = insert_events(conn, new_events)
            # Score only events that actually made it into touch_events.
            # Natural-key dedupe can ignore rows even when event_id is novel.
            inserted_ids = _fetch_existing_event_ids(
                conn, [ev["event_id"] for ev in new_events if ev.get("event_id")]
            )
            new_events = [ev for ev in new_events if ev.get("event_id") in inserted_ids]

    return (
        {
            "symbol": symbol,
            "source": resolved_source,
            "bars_inserted": bars_inserted,
            "events_built": len(events),
            "events_inserted": events_inserted,
            "events_scored": 0,
            "candles": len(candles),
            "session_count": len(sessions),
        },
        new_events,
    )


class _HealthHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", _cors_origin(self.headers.get("Origin")))
        self.send_header("Vary", "Origin")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", _cors_origin(self.headers.get("Origin")))
        self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        if self.path not in ("/", "/health"):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")
            return
        snap = _get_state_snapshot()
        status = "ok" if snap.get("last_success_ms") else "starting"
        if snap.get("status") == "degraded":
            status = "degraded"
        payload = {
            "status": status,
            "service": "live_event_collector",
            "db": DB_PATH,
            "poll_sec": POLL_SEC,
            "symbols": SYMBOLS,
            "interval": INTERVAL,
            "range": RANGE_STR,
            "source": SOURCE,
            "score_enabled": SCORE_ENABLED,
            "score_url": SCORE_API_URL,
            "state": snap,
        }
        self._send_json(200, payload)

    def log_message(self, _format: str, *_args: Any) -> None:
        return


def _run_health_server() -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((HOST, PORT), _HealthHandler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _handle_signal(_sig: int, _frame: Any) -> None:
    _stop_event.set()


def main() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    if not SYMBOLS:
        raise ValueError("LIVE_COLLECTOR_SYMBOLS must include at least one symbol")

    health_server = _run_health_server()
    log.info(
        "Live collector starting (symbols=%s interval=%s range=%s source=%s poll=%ss score_enabled=%s db=%s health=http://%s:%s/health)",
        ",".join(SYMBOLS),
        INTERVAL,
        RANGE_STR,
        SOURCE,
        POLL_SEC,
        SCORE_ENABLED,
        DB_PATH,
        HOST,
        PORT,
    )

    conn = _connect_db()
    try:
        while not _stop_event.is_set():
            cycle_started = int(time.time() * 1000)
            score_backoff_recovered = _clear_score_backoff_if_expired(cycle_started)
            if score_backoff_recovered:
                log.info("ML score cooldown expired; resuming live scoring.")
            with _state_lock:
                _state["last_cycle_start_ms"] = cycle_started
                _state["cycles"] = int(_state.get("cycles", 0)) + 1

            symbol_results: List[Dict[str, Any]] = []
            cycle_errors: List[str] = []
            total_bars = 0
            total_events = 0
            total_scored = 0
            total_score_skipped = 0

            for symbol in SYMBOLS:
                try:
                    result, new_events = _collect_symbol(conn, symbol)
                    conn.commit()
                    result.setdefault("events_score_skipped", 0)

                    if SCORE_ENABLED and new_events:
                        remaining_ms, backoff_reason = _score_backoff_remaining_ms()
                        if remaining_ms > 0:
                            skipped_now = len(new_events)
                            result["events_score_skipped"] += skipped_now
                            total_score_skipped += skipped_now
                            log.info(
                                "Skipping live scoring for %s (%d events) due to active cooldown %.1fs (%s)",
                                symbol,
                                skipped_now,
                                remaining_ms / 1000.0,
                                backoff_reason or "rate_limited",
                            )
                        else:
                            try:
                                scored_now = _score_events(new_events)
                                result["events_scored"] = scored_now
                                total_scored += scored_now
                            except Exception as score_exc:
                                if _is_rate_limited_exception(score_exc):
                                    retry_after_sec = (
                                        score_exc.retry_after_sec
                                        if isinstance(score_exc, ScoreRequestError)
                                        else None
                                    )
                                    until_ms = _activate_score_backoff(
                                        "ML score endpoint returned HTTP 429",
                                        retry_after_sec=retry_after_sec,
                                    )
                                    cooldown_sec = max(
                                        0.0, (until_ms - int(time.time() * 1000)) / 1000.0
                                    )
                                    skipped_now = len(new_events)
                                    result["events_score_skipped"] += skipped_now
                                    total_score_skipped += skipped_now
                                    log.warning(
                                        "Collector scoring rate-limited for %s; entering cooldown %.1fs (%s)",
                                        symbol,
                                        cooldown_sec,
                                        score_exc,
                                    )
                                else:
                                    cycle_errors.append(f"{symbol}:score:{score_exc}")
                                    log.warning("Collector scoring failed for %s: %s", symbol, score_exc)

                    # Also score a small backlog slice so retrain/backfill-created
                    # events do not stay permanently unscored in prediction_log.
                    if SCORE_ENABLED and SCORE_UNSCORED_MAX_PER_CYCLE > 0:
                        remaining_ms, backoff_reason = _score_backoff_remaining_ms()
                        if remaining_ms > 0:
                            # Do not hit score endpoint while cooling down.
                            log.info(
                                "Skipping backlog scoring for %s due to active cooldown %.1fs (%s)",
                                symbol,
                                remaining_ms / 1000.0,
                                backoff_reason or "rate_limited",
                            )
                        else:
                            lookback_ms = SCORE_UNSCORED_LOOKBACK_DAYS * 86_400_000
                            min_ts_ms = max(0, int(time.time() * 1000) - lookback_ms)
                            exclude_ids = {ev.get("event_id") for ev in new_events if ev.get("event_id")}
                            backlog_events = _fetch_unscored_events(
                                conn,
                                symbol=symbol,
                                min_ts_ms=min_ts_ms,
                                limit=SCORE_UNSCORED_MAX_PER_CYCLE,
                                exclude_event_ids=exclude_ids,
                            )
                            if backlog_events:
                                try:
                                    scored_backlog = _score_events(backlog_events)
                                    result["events_scored"] += scored_backlog
                                    total_scored += scored_backlog
                                except Exception as score_gap_exc:
                                    if _is_rate_limited_exception(score_gap_exc):
                                        retry_after_sec = (
                                            score_gap_exc.retry_after_sec
                                            if isinstance(score_gap_exc, ScoreRequestError)
                                            else None
                                        )
                                        until_ms = _activate_score_backoff(
                                            "ML score endpoint returned HTTP 429",
                                            retry_after_sec=retry_after_sec,
                                        )
                                        cooldown_sec = max(
                                            0.0, (until_ms - int(time.time() * 1000)) / 1000.0
                                        )
                                        skipped_now = len(backlog_events)
                                        result["events_score_skipped"] += skipped_now
                                        total_score_skipped += skipped_now
                                        log.warning(
                                            "Collector backlog scoring rate-limited for %s; entering cooldown %.1fs (%s)",
                                            symbol,
                                            cooldown_sec,
                                            score_gap_exc,
                                        )
                                    else:
                                        cycle_errors.append(f"{symbol}:score_gap:{score_gap_exc}")
                                        log.warning(
                                            "Collector backlog scoring failed for %s: %s",
                                            symbol,
                                            score_gap_exc,
                                        )

                    symbol_results.append(result)
                    total_bars += result["bars_inserted"]
                    total_events += result["events_inserted"]
                except Exception as exc:
                    conn.rollback()
                    cycle_errors.append(f"{symbol}: {exc}")
                    log.exception("Collector cycle failed for %s", symbol)

            now = int(time.time() * 1000)
            status = "ok" if not cycle_errors else "degraded"
            remaining_backoff_ms, _ = _score_backoff_remaining_ms(now)
            state_before = _get_state_snapshot()
            score_status = "disabled"
            if SCORE_ENABLED:
                if remaining_backoff_ms > 0:
                    score_status = "cooldown"
                elif total_scored > 0:
                    score_status = "ok"
                else:
                    score_status = "idle"
            updates: Dict[str, Any] = {
                "status": status,
                "last_cycle_end_ms": now,
                "symbols": {entry["symbol"]: entry for entry in symbol_results},
                "score_status": score_status,
            }
            if total_score_skipped > 0:
                updates["score_backoff_skip_cycles"] = int(
                    state_before.get("score_backoff_skip_cycles") or 0
                ) + 1
                updates["score_backoff_skipped_events"] = int(
                    state_before.get("score_backoff_skipped_events") or 0
                ) + int(total_score_skipped)
            if not cycle_errors:
                updates["last_success_ms"] = now
                updates["last_error"] = None
            else:
                updates["last_error"] = "; ".join(cycle_errors)
            _set_state(updates)

            if cycle_errors:
                log.warning(
                    "Collector cycle complete with errors (bars=%d events=%d scored=%d skipped=%d): %s",
                    total_bars,
                    total_events,
                    total_scored,
                    total_score_skipped,
                    "; ".join(cycle_errors),
                )
            elif total_bars > 0 or total_events > 0 or total_scored > 0 or total_score_skipped > 0:
                log.info(
                    "Collector cycle complete (bars_inserted=%d events_inserted=%d events_scored=%d events_score_skipped=%d)",
                    total_bars,
                    total_events,
                    total_scored,
                    total_score_skipped,
                )
            else:
                log.info("Collector cycle complete (no new bars/events)")

            _stop_event.wait(POLL_SEC)
    finally:
        conn.close()
        health_server.shutdown()
        health_server.server_close()
        log.info("Live collector stopped.")


if __name__ == "__main__":
    main()
