#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import math
import os
from pathlib import Path
import sqlite3
import sys
from typing import Iterable
from zoneinfo import ZoneInfo

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from label_eligibility import normalize_bar_interval  # noqa: E402
from trading_calendar import (  # noqa: E402
    REGULAR_SESSION_OPEN_ET,
    is_trading_day,
    session_close_et,
)

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
DEFAULT_HORIZONS = [5, 15, 30, 60]

LABEL_QUALITY_COLUMNS = {
    "expected_bar_count": "INTEGER",
    "observed_bar_count": "INTEGER",
    "coverage_ratio": "REAL",
    "max_gap_sec": "REAL",
    "endpoint_gap_sec": "REAL",
    "endpoint_status": "TEXT",
    "coverage_status": "TEXT",
}


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def fetch_bars(
    conn: sqlite3.Connection, symbol: str, start_ts: int, end_ts: int, interval_sec: int | None
) -> list[dict]:
    if interval_sec is None:
        cur = conn.execute(
            """
            SELECT ts, open, high, low, close
            FROM bar_data
            WHERE symbol = ? AND ts >= ? AND ts <= ?
            ORDER BY ts
            """,
            (symbol, start_ts, end_ts),
        )
    else:
        cur = conn.execute(
            """
            SELECT ts, open, high, low, close
            FROM bar_data
            WHERE symbol = ? AND ts >= ? AND ts <= ? AND bar_interval_sec = ?
            ORDER BY ts
            """,
            (symbol, start_ts, end_ts, interval_sec),
        )
    return [
        {"ts": row[0], "open": row[1], "high": row[2], "low": row[3], "close": row[4]}
        for row in cur.fetchall()
    ]


def compute_mfe_mae(
    bars: Iterable[dict], touch_price: float, touch_side: int | None
) -> tuple[float, float]:
    """Compute MFE/MAE in basis points, directionally aware.

    For touch_side == -1 (below level, expecting rejection downward):
      MFE = max downward move (positive bps value)
      MAE = max adverse upward move (negative bps value)
    For touch_side == 1 (above level, expecting rejection upward):
      MFE = max upward move (positive bps value)
      MAE = max adverse downward move (negative bps value)
    For unknown side: use symmetric absolute excursion.
    """
    max_fav = 0.0
    max_adv = 0.0
    for bar in bars:
        up_bps = (bar["high"] - touch_price) / touch_price * 1e4
        down_bps = (bar["low"] - touch_price) / touch_price * 1e4
        if touch_side == 1:
            max_fav = max(max_fav, up_bps)
            max_adv = min(max_adv, down_bps)
        elif touch_side == -1:
            max_fav = max(max_fav, -down_bps)
            max_adv = min(max_adv, -up_bps)
        else:
            # Unknown side: treat excursion magnitude symmetrically.
            max_abs_excursion = max(abs(up_bps), abs(down_bps))
            max_fav = max(max_fav, max_abs_excursion)
            max_adv = min(max_adv, -max_abs_excursion)
    return max_fav, max_adv


def forward_bars_after_touch(bars: Iterable[dict], ts_event: int) -> list[dict]:
    """Exclude the touch bar itself to avoid look-ahead leakage.

    ts_event marks the bar where the touch occurred. We only score movement
    strictly after that timestamp.
    """
    return [bar for bar in bars if int(bar["ts"]) > int(ts_event)]


def normalize_ohlc_path(bars: Iterable[dict]) -> str | None:
    """Coerce and validate every OHLC value, returning a failure status if invalid."""
    for bar in bars:
        try:
            open_price = float(bar["open"])
            high_price = float(bar["high"])
            low_price = float(bar["low"])
            close_price = float(bar["close"])
        except (KeyError, TypeError, ValueError, OverflowError):
            return "invalid_ohlc_values"

        values = (open_price, high_price, low_price, close_price)
        if not all(math.isfinite(value) for value in values):
            return "invalid_ohlc_values"
        if (
            high_price < max(open_price, close_price, low_price)
            or low_price > min(open_price, close_price, high_price)
            or high_price < low_price
        ):
            return "invalid_ohlc_structure"

        bar["open"], bar["high"], bar["low"], bar["close"] = values
    return None


def assess_bar_path_coverage(
    bars: Iterable[dict], ts_event: int, end_ts: int, interval_sec: int
) -> dict:
    """Return strict path-coverage metadata for a forward label window."""
    bars = list(bars)
    interval_ms = int(interval_sec) * 1000
    window_ms = int(end_ts) - int(ts_event)
    expected = window_ms // interval_ms if interval_ms > 0 and window_ms > 0 else 0
    raw_timestamps = [
        int(bar["ts"])
        for bar in bars
        if int(ts_event) < int(bar["ts"]) <= int(end_ts)
    ]
    timestamps = sorted(raw_timestamps)
    observed = len(timestamps)
    coverage_ratio = observed / expected if expected > 0 else 0.0

    if timestamps:
        endpoint_gap_ms = max(0, int(end_ts) - timestamps[-1])
        boundary_gaps = [timestamps[0] - int(ts_event)]
        boundary_gaps.extend(b - a for a, b in zip(timestamps, timestamps[1:]))
        boundary_gaps.append(endpoint_gap_ms)
        max_gap_ms = max(boundary_gaps)
    else:
        endpoint_gap_ms = window_ms if window_ms > 0 else 0
        max_gap_ms = endpoint_gap_ms

    endpoint_status = "exact" if timestamps and timestamps[-1] == int(end_ts) else "missing"
    eastern = ZoneInfo("America/New_York")
    event_dt = datetime.fromtimestamp(int(ts_event) / 1000.0, tz=eastern)
    end_dt = datetime.fromtimestamp(int(end_ts) / 1000.0, tz=eastern)
    event_date = event_dt.date()
    session_open = datetime.combine(event_date, REGULAR_SESSION_OPEN_ET, tzinfo=eastern)
    session_close = datetime.combine(event_date, session_close_et(event_date), tzinfo=eastern)
    bars_outside_session = any(
        not (session_open < datetime.fromtimestamp(ts / 1000.0, tz=eastern) <= session_close)
        for ts in timestamps
    )
    ohlc_status = normalize_ohlc_path(bars)

    if expected <= 0:
        coverage_status = "invalid_window"
    elif not is_trading_day(event_date) or not (session_open <= event_dt < session_close):
        coverage_status = "event_outside_regular_session"
    elif end_dt.date() != event_date or end_dt > session_close:
        coverage_status = "horizon_past_session_close"
    elif bars_outside_session:
        coverage_status = "bar_outside_regular_session"
    elif len(set(raw_timestamps)) != len(raw_timestamps):
        coverage_status = "duplicate_timestamps"
    elif not timestamps:
        coverage_status = "no_bars"
    elif timestamps[0] - int(ts_event) > interval_ms:
        coverage_status = "late_first_bar"
    elif endpoint_status != "exact":
        coverage_status = "missing_endpoint"
    elif max_gap_ms > interval_ms:
        coverage_status = "excessive_gap"
    elif observed != expected:
        coverage_status = "unexpected_bar_count"
    elif ohlc_status is not None:
        coverage_status = ohlc_status
    else:
        coverage_status = "qualified"

    return {
        "qualified": coverage_status == "qualified",
        "expected_bar_count": int(expected),
        "observed_bar_count": int(observed),
        "coverage_ratio": float(coverage_ratio),
        "max_gap_sec": float(max_gap_ms / 1000.0),
        "endpoint_gap_sec": float(endpoint_gap_ms / 1000.0),
        "endpoint_status": endpoint_status,
        "coverage_status": coverage_status,
    }


def ensure_label_quality_columns(conn: sqlite3.Connection) -> None:
    """Add quality columns for databases not yet run through migration 9."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(event_labels)")}
    for name, sql_type in LABEL_QUALITY_COLUMNS.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE event_labels ADD COLUMN {name} {sql_type}")
    conn.commit()


def label_event(
    bars: list[dict],
    touch_price: float,
    level_price: float,
    touch_side: int | None,
    reject_bps: float,
    break_bps: float,
    sustain_bars: int,
) -> tuple[int, int, float | None]:
    reject = 0
    brk = 0
    resolution = None

    sustain_target = max(1, int(sustain_bars))
    reject_idx = None
    break_idx = None

    if touch_side in (1, -1):
        reject_dir = 1 if touch_side == 1 else -1
        break_dir = -reject_dir
        break_streak = 0
        for idx, bar in enumerate(bars):
            dist = (bar["close"] - level_price) / level_price * 1e4
            if reject_idx is None and dist * reject_dir >= reject_bps:
                reject_idx = idx

            if dist * break_dir >= break_bps:
                break_streak += 1
            else:
                break_streak = 0

            if break_idx is None and break_streak >= sustain_target:
                break_idx = idx
            if reject_idx is not None and break_idx is not None:
                break
    else:
        break_streak = 0
        for idx, bar in enumerate(bars):
            dist = (bar["close"] - level_price) / level_price * 1e4
            if reject_idx is None and abs(dist) >= reject_bps:
                reject_idx = idx

            if abs(dist) >= break_bps:
                break_streak += 1
            else:
                break_streak = 0

            if break_idx is None and break_streak >= sustain_target:
                break_idx = idx
            if reject_idx is not None and break_idx is not None:
                break

    if break_idx is not None and (reject_idx is None or break_idx <= reject_idx):
        brk = 1
        resolution = break_idx
    elif reject_idx is not None:
        reject = 1
        resolution = reject_idx

    return reject, brk, resolution


def has_sufficient_bars(
    conn: sqlite3.Connection, symbol: str, end_ts: int, interval_sec: int | None
) -> bool:
    if interval_sec is None:
        cur = conn.execute(
            "SELECT MAX(ts) FROM bar_data WHERE symbol = ?",
            (symbol,),
        )
    else:
        cur = conn.execute(
            "SELECT MAX(ts) FROM bar_data WHERE symbol = ? AND bar_interval_sec = ?",
            (symbol, interval_sec),
        )
    row = cur.fetchone()
    return row is not None and row[0] is not None and row[0] >= end_ts


def label_exists(conn: sqlite3.Connection, event_id: str, horizon: int) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM event_labels WHERE event_id = ? AND horizon_min = ? LIMIT 1",
        (event_id, horizon),
    )
    return cur.fetchone() is not None


def main() -> None:
    parser = argparse.ArgumentParser(description="Build labels for touch events.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--horizons", nargs="+", type=int, default=DEFAULT_HORIZONS)
    parser.add_argument("--reject-bps", type=float, default=10)
    parser.add_argument("--break-bps", type=float, default=10)
    parser.add_argument("--sustain-bars", type=int, default=2)
    parser.add_argument("--incremental", action="store_true", default=False)
    parser.add_argument("--force", action="store_true", default=False,
                        help="Delete all existing labels and rebuild from scratch")
    args = parser.parse_args()

    conn = connect(args.db)
    ensure_label_quality_columns(conn)

    if args.force:
        conn.execute("DELETE FROM event_labels")
        conn.commit()
        print("Deleted all existing labels (--force mode)")

    cur = conn.execute(
        """
        SELECT event_id, symbol, ts_event, touch_price, level_price, touch_side, bar_interval_sec
        FROM touch_events
        ORDER BY ts_event
        """
    )
    events = cur.fetchall()

    labeled = 0
    skipped_missing_interval = 0
    skipped_incomplete_path = 0
    skipped_by_status: Counter[str] = Counter()
    for event_id, symbol, ts_event, touch_price, level_price, touch_side, bar_interval_sec in events:
        # Requalification is authoritative for the requested horizon set. Clear
        # stale rows before any event-level early return, while preserving labels
        # for horizons that were not requested by this run.
        for horizon in args.horizons:
            conn.execute(
                "DELETE FROM event_labels WHERE event_id = ? AND horizon_min = ?",
                (event_id, horizon),
            )

        # P0-A guard: refuse to label an event with no known bar grid. A
        # NULL/0/invalid bar_interval_sec would otherwise fall through to
        # interval-agnostic bar queries (mixed-interval label leakage), so the
        # supervision target would not match the event's feature interval.
        interval = normalize_bar_interval(bar_interval_sec)
        if interval is None:
            skipped_missing_interval += 1
            continue
        for horizon in args.horizons:
            horizon_ms = horizon * 60 * 1000
            end_ts = ts_event + horizon_ms
            if not has_sufficient_bars(conn, symbol, end_ts, interval):
                skipped_by_status["insufficient_latest_bar"] += 1
                continue

            bars = fetch_bars(conn, symbol, ts_event, end_ts, interval)
            bars = forward_bars_after_touch(bars, ts_event)
            quality = assess_bar_path_coverage(bars, ts_event, end_ts, interval)
            if not quality["qualified"]:
                skipped_incomplete_path += 1
                skipped_by_status[str(quality["coverage_status"])] += 1
                continue

            mfe_bps, mae_bps = compute_mfe_mae(bars, touch_price, touch_side)
            return_bps = (bars[-1]["close"] - touch_price) / touch_price * 1e4
            reject, brk, resolution_idx = label_event(
                bars,
                touch_price,
                level_price,
                touch_side,
                args.reject_bps,
                args.break_bps,
                args.sustain_bars,
            )
            if resolution_idx is not None:
                delta_ms = max(0, bars[resolution_idx]["ts"] - ts_event)
                resolution_min = delta_ms / 60000.0
            else:
                resolution_min = None

            conn.execute(
                """
                INSERT OR REPLACE INTO event_labels
                (event_id, horizon_min, return_bps, mfe_bps, mae_bps, reject, break,
                 resolution_min, expected_bar_count, observed_bar_count, coverage_ratio,
                 max_gap_sec, endpoint_gap_sec, endpoint_status, coverage_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id, horizon, return_bps, mfe_bps, mae_bps, reject, brk,
                    resolution_min, quality["expected_bar_count"],
                    quality["observed_bar_count"], quality["coverage_ratio"],
                    quality["max_gap_sec"], quality["endpoint_gap_sec"],
                    quality["endpoint_status"], quality["coverage_status"],
                ),
            )
            labeled += 1

    conn.commit()
    conn.close()
    print(f"Built {labeled} labels")
    if skipped_missing_interval:
        print(
            f"Skipped {skipped_missing_interval} events with missing/zero "
            "bar_interval_sec (no deterministic bar grid; not labeled)"
        )
    if skipped_incomplete_path:
        print(
            f"Skipped {skipped_incomplete_path} event/horizon paths without complete, "
            "qualified bar coverage"
        )
    if skipped_by_status:
        print("Coverage skips: " + ", ".join(
            f"{status}={count}" for status, count in sorted(skipped_by_status.items())
        ))


if __name__ == "__main__":
    main()
