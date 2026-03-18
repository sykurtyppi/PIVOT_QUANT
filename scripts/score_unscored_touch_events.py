#!/usr/bin/env python3
"""Score touch events that do not yet have a live prediction row."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite"))
DEFAULT_SCORE_URL = os.getenv("LIVE_COLLECTOR_SCORE_URL", "http://127.0.0.1:5003/score")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score unscored touch_events into prediction_log.")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite path (default: PIVOT_DB or data/pivot_events.sqlite)")
    parser.add_argument("--score-url", default=DEFAULT_SCORE_URL, help="ML /score endpoint")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbols")
    parser.add_argument("--lookback-days", type=int, default=7, help="Lookback window when start/end not provided")
    parser.add_argument("--start-date", default="", help="Inclusive UTC date YYYY-MM-DD")
    parser.add_argument("--end-date", default="", help="Inclusive UTC date YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=600, help="Max events to process")
    parser.add_argument("--batch-size", type=int, default=64, help="Events per /score batch")
    parser.add_argument("--timeout-sec", type=float, default=12.0, help="HTTP timeout per call")
    parser.add_argument("--max-attempts", type=int, default=3, help="HTTP retry attempts")
    parser.add_argument("--retry-base-sec", type=float, default=0.5, help="Retry base sleep")
    parser.add_argument("--retry-max-sec", type=float, default=5.0, help="Retry max sleep")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help="Emit progress to stderr every N processed events (0 disables)",
    )
    parser.add_argument(
        "--max-consecutive-transport-failures",
        type=int,
        default=0,
        help="Abort early after N consecutive transport-outage batches (0 disables)",
    )
    parser.add_argument(
        "--single-fallback-on-failure",
        dest="single_fallback_on_failure",
        action="store_true",
        default=True,
        help="On batch failure, retry each event as single-event requests (default: enabled)",
    )
    parser.add_argument(
        "--no-single-fallback-on-failure",
        dest="single_fallback_on_failure",
        action="store_false",
        help="Disable single-event fallback retries after batch failures",
    )
    parser.add_argument("--verify-after", action="store_true", help="Count remaining unscored events after run")
    parser.add_argument(
        "--max-remaining",
        type=int,
        default=-1,
        help="If >=0 and verify-after is set, return error when remaining unscored exceeds this threshold",
    )
    parser.add_argument(
        "--fail-on-partial",
        action="store_true",
        help="Return error when any batch/event failed to score",
    )
    parser.add_argument(
        "--rescore-existing",
        action="store_true",
        help=(
            "Ignore existing prediction_log rows and rescore all matching touch_events "
            "(INSERT OR IGNORE in ml_server prevents duplicate rows for the same event_id+model_version)."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Only list how many events are eligible")
    return parser.parse_args(argv)


def _parse_symbols(raw: str) -> list[str]:
    return [token.strip().upper() for token in (raw or "").split(",") if token.strip()]


def _validate_date(raw: str) -> str:
    if not raw:
        return ""
    try:
        time.strptime(raw, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date '{raw}' (expected YYYY-MM-DD)") from exc
    return raw


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def fetch_unscored_events(
    conn: sqlite3.Connection,
    *,
    symbols: list[str],
    start_date: str,
    end_date: str,
    lookback_days: int,
    limit: int,
    rescore_existing: bool,
) -> list[dict[str, Any]]:
    if not _has_table(conn, "touch_events"):
        return []

    if _has_table(conn, "prediction_log") and not rescore_existing:
        join_clause = (
            "LEFT JOIN prediction_log pl "
            "ON pl.event_id = te.event_id AND COALESCE(pl.is_preview, 0) = 0"
        )
        where = ["pl.event_id IS NULL"]
    else:
        join_clause = ""
        where = []

    params: list[Any] = []
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        where.append(f"te.symbol IN ({placeholders})")
        params.extend(symbols)

    if start_date:
        where.append("date(te.ts_event/1000,'unixepoch') >= ?")
        params.append(start_date)
    if end_date:
        where.append("date(te.ts_event/1000,'unixepoch') <= ?")
        params.append(end_date)
    if not start_date and not end_date and lookback_days > 0:
        min_ts_ms = int(time.time() * 1000) - lookback_days * 86_400_000
        where.append("te.ts_event >= ?")
        params.append(min_ts_ms)

    where_sql = " AND ".join(where) if where else "1=1"
    params.append(int(limit))
    sql = f"""
        SELECT te.*
        FROM touch_events te
        {join_clause}
        WHERE {where_sql}
        ORDER BY te.ts_event DESC
        LIMIT ?
    """
    rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def count_unscored_events(
    conn: sqlite3.Connection,
    *,
    symbols: list[str],
    start_date: str,
    end_date: str,
    lookback_days: int,
    rescore_existing: bool,
) -> int:
    if not _has_table(conn, "touch_events"):
        return 0

    if _has_table(conn, "prediction_log") and not rescore_existing:
        join_clause = (
            "LEFT JOIN prediction_log pl "
            "ON pl.event_id = te.event_id AND COALESCE(pl.is_preview, 0) = 0"
        )
        where = ["pl.event_id IS NULL"]
    else:
        join_clause = ""
        where = []

    params: list[Any] = []
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        where.append(f"te.symbol IN ({placeholders})")
        params.extend(symbols)

    if start_date:
        where.append("date(te.ts_event/1000,'unixepoch') >= ?")
        params.append(start_date)
    if end_date:
        where.append("date(te.ts_event/1000,'unixepoch') <= ?")
        params.append(end_date)
    if not start_date and not end_date and lookback_days > 0:
        min_ts_ms = int(time.time() * 1000) - lookback_days * 86_400_000
        where.append("te.ts_event >= ?")
        params.append(min_ts_ms)

    where_sql = " AND ".join(where) if where else "1=1"
    sql = f"""
        SELECT COUNT(*)
        FROM touch_events te
        {join_clause}
        WHERE {where_sql}
    """
    row = conn.execute(sql, params).fetchone()
    return int(row[0] if row else 0)


def count_recently_scored_events(
    conn: sqlite3.Connection,
    *,
    symbols: list[str],
    start_date: str,
    end_date: str,
    lookback_days: int,
    since_ts_ms: int,
) -> int:
    if not _has_table(conn, "touch_events") or not _has_table(conn, "prediction_log"):
        return 0

    where: list[str] = []
    params: list[Any] = [int(since_ts_ms)]

    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        where.append(f"te.symbol IN ({placeholders})")
        params.extend(symbols)

    if start_date:
        where.append("date(te.ts_event/1000,'unixepoch') >= ?")
        params.append(start_date)
    if end_date:
        where.append("date(te.ts_event/1000,'unixepoch') <= ?")
        params.append(end_date)
    if not start_date and not end_date and lookback_days > 0:
        min_ts_ms = int(time.time() * 1000) - lookback_days * 86_400_000
        where.append("te.ts_event >= ?")
        params.append(min_ts_ms)

    where_sql = " AND ".join(where) if where else "1=1"
    sql = f"""
        SELECT COUNT(DISTINCT te.event_id)
        FROM touch_events te
        JOIN prediction_log pl
          ON pl.event_id = te.event_id
         AND COALESCE(pl.is_preview, 0) = 0
         AND pl.ts_prediction >= ?
        WHERE {where_sql}
    """
    row = conn.execute(sql, params).fetchone()
    return int(row[0] if row else 0)


def _post_score_batch(
    *,
    score_url: str,
    events: list[dict[str, Any]],
    timeout_sec: float,
    max_attempts: int,
    retry_base_sec: float,
    retry_max_sec: float,
) -> tuple[int, str | None]:
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "PivotQuantGapScorer/1.0",
    }
    payload = json.dumps({"events": events}).encode("utf-8")
    request = Request(score_url, data=payload, headers=headers, method="POST")

    last_error: str | None = None
    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(request, timeout=timeout_sec) as response:
                raw = response.read().decode("utf-8")
            data = json.loads(raw) if raw else {}
            if not isinstance(data, dict):
                raise RuntimeError("Score response is not a JSON object")
            results = data.get("results")
            if not isinstance(results, list):
                raise RuntimeError("Score response missing 'results' list")
            return len(results), None
        except (
            HTTPError,
            URLError,
            TimeoutError,
            json.JSONDecodeError,
            RuntimeError,
            OSError,
        ) as exc:
            last_error = str(exc)
            if attempt >= attempts:
                break
            delay = min(retry_max_sec, retry_base_sec * (2 ** (attempt - 1)))
            time.sleep(max(0.0, delay))
    return 0, last_error


def _is_transport_outage_error(error_text: str | None) -> bool:
    if not error_text:
        return False
    lowered = str(error_text).lower()
    markers = (
        "connection refused",
        "connection reset",
        "timed out",
        "remote end closed connection",
        "temporarily unavailable",
        "broken pipe",
        "http error 429",
        "too many requests",
        "status 429",
    )
    return any(marker in lowered for marker in markers)


def _is_rate_limited_error(error_text: str | None) -> bool:
    if not error_text:
        return False
    lowered = str(error_text).lower()
    return "429" in lowered or "too many requests" in lowered


def _emit_progress(
    *,
    processed: int,
    attempted: int,
    scored_ok: int,
    failed: int,
    last_error: str | None = None,
) -> None:
    message = (
        f"[score_unscored] progress processed={processed}/{attempted} "
        f"scored_ok={scored_ok} failed={failed}"
    )
    if last_error:
        message = f"{message} last_error={last_error}"
    print(message, file=sys.stderr, flush=True)


def run(args: argparse.Namespace) -> dict[str, Any]:
    symbols = _parse_symbols(args.symbols)
    start_date = _validate_date(args.start_date)
    end_date = _validate_date(args.end_date)
    if start_date and end_date and end_date < start_date:
        raise ValueError("end-date must be >= start-date")

    conn = connect(args.db)
    try:
        eligible_total = count_unscored_events(
            conn,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            lookback_days=max(0, int(args.lookback_days)),
            rescore_existing=bool(args.rescore_existing),
        )
        events = fetch_unscored_events(
            conn,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            lookback_days=max(0, int(args.lookback_days)),
            limit=max(0, int(args.limit)),
            rescore_existing=bool(args.rescore_existing),
        )
    finally:
        conn.close()

    attempted = len(events)
    if args.dry_run:
        return {
            "status": "ok",
            "db": args.db,
            "eligible_total": eligible_total,
            "attempted": attempted,
            "scored_ok": 0,
            "failed": 0,
            "dry_run": True,
            "symbols": symbols,
            "start_date": start_date or None,
            "end_date": end_date or None,
        }

    scored_ok = 0
    failed = 0
    processed_events = 0
    aborted_early = False
    aborted_reason: str | None = None
    refreshed_count: int | None = None
    last_error: str | None = None
    single_fallback_attempted = 0
    single_fallback_scored = 0
    single_fallback_failed = 0
    single_fallback_skipped_transport = 0
    consecutive_transport_failures = 0
    batch_size = max(1, int(args.batch_size))
    run_started_ms = int(time.time() * 1000)
    progress_every = max(0, int(args.progress_every))
    next_progress = progress_every if progress_every > 0 else None
    max_consecutive_transport_failures = max(0, int(args.max_consecutive_transport_failures))

    if attempted > 0:
        print(
            (
                "[score_unscored] start "
                f"eligible_total={eligible_total} attempted={attempted} batch_size={batch_size} "
                f"timeout_sec={float(args.timeout_sec):.3f} max_attempts={max(1, int(args.max_attempts))}"
            ),
            file=sys.stderr,
            flush=True,
        )

    for offset in range(0, attempted, batch_size):
        batch = events[offset : offset + batch_size]
        processed_events += len(batch)
        ok_count, error = _post_score_batch(
            score_url=args.score_url,
            events=batch,
            timeout_sec=float(args.timeout_sec),
            max_attempts=max(1, int(args.max_attempts)),
            retry_base_sec=max(0.0, float(args.retry_base_sec)),
            retry_max_sec=max(0.0, float(args.retry_max_sec)),
        )
        scored_ok += ok_count
        batch_failed = max(0, len(batch) - ok_count)
        failed += batch_failed
        saw_transport_outage = False
        if error and batch_failed > 0:
            last_error = error
            if _is_transport_outage_error(error):
                # During transport outages, per-event fallback multiplies wait time
                # without improving success rate. Keep this batch as failed and move on.
                saw_transport_outage = True
                single_fallback_skipped_transport += len(batch)
            elif bool(args.single_fallback_on_failure) and len(batch) > 0:
                # Retry individual events so partial outages do not strand large
                # backlogs in prediction_log.
                failed -= batch_failed
                for event in batch:
                    single_fallback_attempted += 1
                    one_ok, one_error = _post_score_batch(
                        score_url=args.score_url,
                        events=[event],
                        timeout_sec=float(args.timeout_sec),
                        max_attempts=max(1, int(args.max_attempts)),
                        retry_base_sec=max(0.0, float(args.retry_base_sec)),
                        retry_max_sec=max(0.0, float(args.retry_max_sec)),
                    )
                    if one_ok >= 1:
                        scored_ok += 1
                        single_fallback_scored += 1
                    else:
                        failed += 1
                        single_fallback_failed += 1
                        if one_error:
                            last_error = one_error

        if saw_transport_outage:
            consecutive_transport_failures += 1
            if _is_rate_limited_error(error):
                # Prevent retry storms when the ML server is actively
                # rate-limiting this worker.
                cooldown = min(
                    max(0.25, float(args.retry_max_sec)),
                    max(0.25, float(args.retry_base_sec)) * 4.0,
                )
                time.sleep(cooldown)
            if max_consecutive_transport_failures > 0 and (
                consecutive_transport_failures >= max_consecutive_transport_failures
            ):
                aborted_early = True
                aborted_reason = (
                    "aborted after "
                    f"{consecutive_transport_failures} consecutive transport failures"
                )
                remaining_events = max(0, attempted - processed_events)
                if remaining_events > 0:
                    failed += remaining_events
                break
        else:
            consecutive_transport_failures = 0

        if progress_every > 0 and next_progress is not None and processed_events >= next_progress:
            _emit_progress(
                processed=processed_events,
                attempted=attempted,
                scored_ok=scored_ok,
                failed=failed,
                last_error=last_error,
            )
            while next_progress <= processed_events:
                next_progress += progress_every

    remaining_unscored: int | None = None
    if bool(args.verify_after) or int(args.max_remaining) >= 0:
        conn_verify = connect(args.db)
        try:
            if bool(args.rescore_existing):
                refreshed_count = count_recently_scored_events(
                    conn_verify,
                    symbols=symbols,
                    start_date=start_date,
                    end_date=end_date,
                    lookback_days=max(0, int(args.lookback_days)),
                    since_ts_ms=run_started_ms,
                )
                remaining_unscored = max(0, eligible_total - int(refreshed_count))
            else:
                remaining_unscored = count_unscored_events(
                    conn_verify,
                    symbols=symbols,
                    start_date=start_date,
                    end_date=end_date,
                    lookback_days=max(0, int(args.lookback_days)),
                    rescore_existing=False,
                )
        finally:
            conn_verify.close()

    if progress_every > 0 and processed_events > 0 and processed_events < attempted:
        _emit_progress(
            processed=processed_events,
            attempted=attempted,
            scored_ok=scored_ok,
            failed=failed,
            last_error=last_error,
        )

    status = "ok" if failed == 0 else "partial"
    if aborted_early:
        status = "error"
        last_error = aborted_reason if not last_error else f"{aborted_reason}; last_error={last_error}"
    if bool(args.fail_on_partial) and failed > 0:
        status = "error"
    if int(args.max_remaining) >= 0:
        remaining_value = remaining_unscored if remaining_unscored is not None else eligible_total
        if remaining_value > int(args.max_remaining):
            status = "error"
            threshold_msg = (
                f"remaining_unscored {remaining_value} exceeds max_remaining {int(args.max_remaining)}"
            )
            last_error = threshold_msg if not last_error else f"{last_error}; {threshold_msg}"

    return {
        "status": status,
        "db": args.db,
        "eligible_total": eligible_total,
        "attempted": attempted,
        "scored_ok": scored_ok,
        "failed": failed,
        "processed_events": processed_events,
        "aborted_early": aborted_early,
        "aborted_reason": aborted_reason,
        "consecutive_transport_failures": consecutive_transport_failures,
        "single_fallback_attempted": single_fallback_attempted,
        "single_fallback_scored": single_fallback_scored,
        "single_fallback_failed": single_fallback_failed,
        "single_fallback_skipped_transport": single_fallback_skipped_transport,
        "refreshed_count": refreshed_count,
        "remaining_unscored": remaining_unscored,
        "max_remaining": int(args.max_remaining),
        "dry_run": False,
        "rescore_existing": bool(args.rescore_existing),
        "symbols": symbols,
        "start_date": start_date or None,
        "end_date": end_date or None,
        "last_error": last_error,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = run(args)
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, indent=2))
        return 1
    print(json.dumps(payload, indent=2))
    return 0 if payload.get("status") in {"ok", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
