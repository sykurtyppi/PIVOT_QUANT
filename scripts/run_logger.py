#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
import uuid

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
DEFAULT_THRESHOLD_BPS = float(os.getenv("TOUCH_THRESHOLD_BPS", "10"))
DEFAULT_COOLDOWN_SEC = int(os.getenv("TOUCH_COOLDOWN_SEC", "600"))


def now_ms() -> int:
    return int(time.time() * 1000)


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def recent_touch(conn: sqlite3.Connection, symbol: str, level_type: str, since_ms: int) -> bool:
    cur = conn.execute(
        """
        SELECT 1
        FROM touch_events
        WHERE symbol = ? AND level_type = ? AND ts_event >= ?
        LIMIT 1
        """,
        (symbol, level_type, since_ms),
    )
    return cur.fetchone() is not None


def log_event(conn: sqlite3.Connection, payload: dict) -> str:
    event_id = payload.get("event_id") or str(uuid.uuid4())
    fields = {
        "event_id": event_id,
        "symbol": payload["symbol"],
        "ts_event": payload["ts_event"],
        "session": payload.get("session"),
        "level_type": payload["level_type"],
        "level_price": payload["level_price"],
        "touch_price": payload["touch_price"],
        "touch_side": payload.get("touch_side"),
        "distance_bps": payload["distance_bps"],
        "is_first_touch_today": payload.get("is_first_touch_today", 0),
        "touch_count_today": payload.get("touch_count_today", 1),
        "confluence_count": payload.get("confluence_count", 0),
        "confluence_types": payload.get("confluence_types"),
        "ema9": payload.get("ema9"),
        "ema21": payload.get("ema21"),
        "ema_state": payload.get("ema_state"),
        "vwap": payload.get("vwap"),
        "vwap_dist_bps": payload.get("vwap_dist_bps"),
        "atr": payload.get("atr"),
        "rv_30": payload.get("rv_30"),
        "rv_regime": payload.get("rv_regime"),
        "iv_rv_state": payload.get("iv_rv_state"),
        "gamma_mode": payload.get("gamma_mode"),
        "gamma_flip": payload.get("gamma_flip"),
        "gamma_flip_dist_bps": payload.get("gamma_flip_dist_bps"),
        "gamma_confidence": payload.get("gamma_confidence"),
        "oi_concentration_top5": payload.get("oi_concentration_top5"),
        "zero_dte_share": payload.get("zero_dte_share"),
        "data_quality": payload.get("data_quality"),
        "bar_interval_sec": payload.get("bar_interval_sec"),
        "source": payload.get("source"),
        "created_at": payload.get("created_at", now_ms()),
    }

    columns = ", ".join(fields.keys())
    placeholders = ", ".join("?" for _ in fields)
    values = list(fields.values())

    conn.execute(f"INSERT INTO touch_events ({columns}) VALUES ({placeholders})", values)
    return event_id


def maybe_log_touch(conn: sqlite3.Connection, payload: dict, threshold_bps: float, cooldown_sec: int) -> str | None:
    if payload["distance_bps"] > threshold_bps:
        return None

    cooldown_ms = cooldown_sec * 1000
    if recent_touch(conn, payload["symbol"], payload["level_type"], payload["ts_event"] - cooldown_ms):
        return None

    return log_event(conn, payload)


def load_events(path: str) -> list[dict]:
    events = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description="Append touch events to SQLite.")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path")
    parser.add_argument("--events", required=True, help="Path to JSONL event file")
    parser.add_argument("--threshold-bps", type=float, default=DEFAULT_THRESHOLD_BPS)
    parser.add_argument("--cooldown-sec", type=int, default=DEFAULT_COOLDOWN_SEC)
    args = parser.parse_args()

    conn = connect(args.db)
    events = load_events(args.events)
    inserted = 0

    for event in events:
        event_id = maybe_log_touch(conn, event, args.threshold_bps, args.cooldown_sec)
        if event_id:
            inserted += 1

    conn.commit()
    conn.close()
    print(f"Inserted {inserted} events")


if __name__ == "__main__":
    main()
