from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ET_TZ = ZoneInfo("America/New_York")


def _epoch_seconds(value: int) -> float:
    numeric = float(value)
    if abs(numeric) >= 1_000_000_000_000:
        return numeric / 1000.0
    return numeric


def _assert_bar_data_exists(con: sqlite3.Connection, source_db_path: Path) -> None:
    row = con.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'bar_data'
        """
    ).fetchone()
    if row is None:
        raise RuntimeError(f"bar_data not found in source SQLite DB: {source_db_path}")


def load_trading_calendar_rows(source_db_path: Path) -> list[dict[str, Any]]:
    source_db_path = source_db_path.expanduser()
    if not source_db_path.exists():
        raise FileNotFoundError(f"Source SQLite DB not found: {source_db_path}")

    con = sqlite3.connect(str(source_db_path))
    try:
        _assert_bar_data_exists(con, source_db_path)
        rows = con.execute(
            """
            WITH min_interval AS (
                SELECT
                    symbol,
                    MIN(bar_interval_sec) AS calendar_bar_interval_sec
                FROM bar_data
                GROUP BY symbol
            )
            SELECT
                b.symbol,
                b.ts,
                b.bar_interval_sec
            FROM bar_data b
            JOIN min_interval mi
              ON mi.symbol = b.symbol
             AND mi.calendar_bar_interval_sec = b.bar_interval_sec
            ORDER BY b.symbol, b.ts
            """
        ).fetchall()
    finally:
        con.close()

    by_day: dict[tuple[str, str], dict[str, Any]] = {}
    for symbol, ts_value, interval_value in rows:
        ts_int = int(ts_value)
        interval_int = int(interval_value)
        event_date_et = (
            datetime.fromtimestamp(_epoch_seconds(ts_int), tz=timezone.utc)
            .astimezone(ET_TZ)
            .date()
            .isoformat()
        )
        key = (str(symbol), event_date_et)
        entry = by_day.setdefault(
            key,
            {
                "symbol": str(symbol),
                "event_date_et": event_date_et,
                "calendar_bar_interval_sec": interval_int,
                "bar_count": 0,
                "min_ts": ts_int,
                "max_ts": ts_int,
            },
        )
        entry["bar_count"] += 1
        entry["min_ts"] = min(int(entry["min_ts"]), ts_int)
        entry["max_ts"] = max(int(entry["max_ts"]), ts_int)

    return [
        by_day[key]
        for key in sorted(by_day.keys(), key=lambda item: (item[0], item[1]))
    ]
