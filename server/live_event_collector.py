#!/usr/bin/env python3
import json
import logging
import os
import signal
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List
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
SCORE_BATCH_SIZE = max(1, int(os.getenv("LIVE_COLLECTOR_SCORE_BATCH_SIZE", "64")))
SCORE_TIMEOUT_SEC = max(1, int(os.getenv("LIVE_COLLECTOR_SCORE_TIMEOUT_SEC", "6")))
INTERVAL_SEC = _interval_to_seconds(INTERVAL)

if SOURCE not in ("auto", "ibkr", "yahoo"):
    raise ValueError(f"LIVE_COLLECTOR_SOURCE must be auto/ibkr/yahoo, got: {SOURCE}")

_state_lock = threading.Lock()
_stop_event = threading.Event()
_state: Dict[str, Any] = {
    "status": "starting",
    "started_at_ms": int(time.time() * 1000),
    "last_cycle_start_ms": None,
    "last_cycle_end_ms": None,
    "last_success_ms": None,
    "last_error": None,
    "cycles": 0,
    "symbols": {},
}


def _connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
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


def _score_events(events: List[Dict[str, Any]]) -> None:
    if not events or not SCORE_ENABLED:
        return

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "PivotQuantLiveCollector/1.0",
    }
    for chunk in _chunked(events, SCORE_BATCH_SIZE):
        payload = json.dumps({"events": chunk}).encode("utf-8")
        request = Request(SCORE_API_URL, data=payload, headers=headers, method="POST")
        try:
            with urlopen(request, timeout=SCORE_TIMEOUT_SEC) as response:
                raw = response.read().decode("utf-8")
            data = json.loads(raw) if raw else {}
            if not isinstance(data, dict):
                raise RuntimeError("ML score response is not JSON object")
            if "results" not in data and "scores" not in data:
                raise RuntimeError("ML score response missing expected keys")
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"ML score request failed: {exc}") from exc


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
    gamma_context = fetch_gamma_context(symbol)
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
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

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


def _run_health_server() -> HTTPServer:
    server = HTTPServer((HOST, PORT), _HealthHandler)
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
            with _state_lock:
                _state["last_cycle_start_ms"] = cycle_started
                _state["cycles"] = int(_state.get("cycles", 0)) + 1

            symbol_results: List[Dict[str, Any]] = []
            cycle_errors: List[str] = []
            total_bars = 0
            total_events = 0
            total_scored = 0

            for symbol in SYMBOLS:
                try:
                    result, new_events = _collect_symbol(conn, symbol)
                    conn.commit()

                    if SCORE_ENABLED and new_events:
                        try:
                            _score_events(new_events)
                            result["events_scored"] = len(new_events)
                            total_scored += len(new_events)
                        except Exception as score_exc:
                            cycle_errors.append(f"{symbol}:score:{score_exc}")
                            log.warning("Collector scoring failed for %s: %s", symbol, score_exc)

                    symbol_results.append(result)
                    total_bars += result["bars_inserted"]
                    total_events += result["events_inserted"]
                except Exception as exc:
                    conn.rollback()
                    cycle_errors.append(f"{symbol}: {exc}")
                    log.exception("Collector cycle failed for %s", symbol)

            now = int(time.time() * 1000)
            status = "ok" if not cycle_errors else "degraded"
            updates: Dict[str, Any] = {
                "status": status,
                "last_cycle_end_ms": now,
                "symbols": {entry["symbol"]: entry for entry in symbol_results},
            }
            if not cycle_errors:
                updates["last_success_ms"] = now
                updates["last_error"] = None
            else:
                updates["last_error"] = "; ".join(cycle_errors)
            _set_state(updates)

            if cycle_errors:
                log.warning(
                    "Collector cycle complete with errors (bars=%d events=%d scored=%d): %s",
                    total_bars,
                    total_events,
                    total_scored,
                    "; ".join(cycle_errors),
                )
            elif total_bars > 0 or total_events > 0 or total_scored > 0:
                log.info(
                    "Collector cycle complete (bars_inserted=%d events_inserted=%d events_scored=%d)",
                    total_bars,
                    total_events,
                    total_scored,
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
