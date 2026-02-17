#!/usr/bin/env python3
"""Persist lightweight operational status key-values in SQLite."""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from pathlib import Path

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")


def connect(db_path: str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_status (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at INTEGER NOT NULL
        );
        """
    )
    conn.commit()


def set_values(conn: sqlite3.Connection, pairs: list[str]) -> None:
    now_ms = int(time.time() * 1000)
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"Invalid --set '{pair}', expected key=value")
        key, value = pair.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid key in --set '{pair}'")
        conn.execute(
            """
            INSERT INTO ops_status(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE
              SET value = excluded.value,
                  updated_at = excluded.updated_at
            """,
            (key, value, now_ms),
        )
    conn.commit()


def get_value(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM ops_status WHERE key = ? LIMIT 1", (key,)).fetchone()
    return None if row is None else row[0]


def dump_all(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT key, value, updated_at FROM ops_status ORDER BY key ASC").fetchall()
    for key, value, updated_at in rows:
        print(f"{key}={value}\t(updated_at={updated_at})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Set/get operational status values.")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite DB")
    parser.add_argument("--set", action="append", default=[], help="Set key=value (repeatable)")
    parser.add_argument("--get", default="", help="Get value for key")
    parser.add_argument("--dump", action="store_true", default=False, help="Dump all key-values")
    args = parser.parse_args()

    conn = connect(args.db)
    try:
        ensure_schema(conn)
        if args.set:
            set_values(conn, args.set)
        if args.get:
            value = get_value(conn, args.get)
            if value is None:
                raise SystemExit(1)
            print(value)
        if args.dump:
            dump_all(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
