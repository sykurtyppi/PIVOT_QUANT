#!/usr/bin/env python3
import argparse
import os
import sqlite3
from typing import Iterable

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
DEFAULT_HORIZONS = [5, 15, 60]


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
    For unknown side: use absolute max excursion.
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
            max_fav = max(max_fav, up_bps, -down_bps)
            max_adv = min(max_adv, down_bps, -up_bps)
    return max_fav, max_adv


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

    reject_idx = None
    break_idx = None

    if touch_side in (1, -1):
        reject_dir = 1 if touch_side == 1 else -1
        break_dir = -reject_dir
        for idx, bar in enumerate(bars):
            dist = (bar["close"] - level_price) / level_price * 1e4
            if reject_idx is None and dist * reject_dir >= reject_bps:
                reject_idx = idx
            if break_idx is None:
                if dist * break_dir >= break_bps:
                    streak = 1
                else:
                    streak = 0
                if streak:
                    for j in range(idx + 1, len(bars)):
                        next_dist = (bars[j]["close"] - level_price) / level_price * 1e4
                        if next_dist * break_dir >= break_bps:
                            streak += 1
                        else:
                            break
                        if streak >= sustain_bars:
                            break_idx = j
                            break
            if reject_idx is not None and break_idx is not None:
                break
    else:
        for idx, bar in enumerate(bars):
            dist = (bar["close"] - level_price) / level_price * 1e4
            if reject_idx is None and abs(dist) >= reject_bps:
                reject_idx = idx
            if break_idx is None:
                streak = 0
                for j in range(idx, len(bars)):
                    next_dist = (bars[j]["close"] - level_price) / level_price * 1e4
                    if abs(next_dist) >= break_bps:
                        streak += 1
                    else:
                        streak = 0
                    if streak >= sustain_bars:
                        break_idx = j
                        break
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
    for event_id, symbol, ts_event, touch_price, level_price, touch_side, bar_interval_sec in events:
        for horizon in args.horizons:
            horizon_ms = horizon * 60 * 1000
            end_ts = ts_event + horizon_ms
            if args.incremental and label_exists(conn, event_id, horizon):
                continue

            interval = bar_interval_sec or None
            if not has_sufficient_bars(conn, symbol, end_ts, interval):
                continue

            bars = fetch_bars(conn, symbol, ts_event, end_ts, interval)
            if not bars:
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
                (event_id, horizon_min, return_bps, mfe_bps, mae_bps, reject, break, resolution_min)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (event_id, horizon, return_bps, mfe_bps, mae_bps, reject, brk, resolution_min),
            )
            labeled += 1

    conn.commit()
    conn.close()
    print(f"Built {labeled} labels")


if __name__ == "__main__":
    main()
