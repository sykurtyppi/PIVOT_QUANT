#!/usr/bin/env python3
import argparse
import os
import sqlite3

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")


def ensure_interval_column(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(bar_data)")
    cols = {row[1] for row in cur.fetchall()}
    if "bar_interval_sec" not in cols:
        conn.execute("ALTER TABLE bar_data ADD COLUMN bar_interval_sec INTEGER")
        conn.commit()


def count_rows(conn: sqlite3.Connection, where_clause: str | None = None) -> int:
    sql = "SELECT COUNT(*) FROM bar_data"
    if where_clause:
        sql += f" WHERE {where_clause}"
    cur = conn.execute(sql)
    row = cur.fetchone()
    return int(row[0]) if row else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Cleanup bar_data to keep intraday-only bars.")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path")
    parser.add_argument("--max-interval-sec", type=int, default=3600)
    parser.add_argument("--prune", action="store_true", help="Remove non-intraday bars")
    parser.add_argument("--truncate", action="store_true", help="Delete all bar_data rows")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without deleting")
    args = parser.parse_args()

    if not args.prune and not args.truncate:
        print("No action specified. Use --prune or --truncate.")
        return

    conn = sqlite3.connect(args.db)
    ensure_interval_column(conn)

    if args.truncate:
        total = count_rows(conn)
        if args.dry_run:
            print(f"[dry-run] Would delete {total} rows from bar_data")
            return
        conn.execute("DELETE FROM bar_data")
        conn.commit()
        print(f"Deleted {total} rows from bar_data")
        return

    where_clause = f"bar_interval_sec IS NULL OR bar_interval_sec > {args.max_interval_sec}"
    to_delete = count_rows(conn, where_clause)
    if args.dry_run:
        print(f"[dry-run] Would delete {to_delete} rows from bar_data")
        return

    conn.execute(f"DELETE FROM bar_data WHERE {where_clause}")
    conn.commit()
    print(f"Deleted {to_delete} rows from bar_data")


if __name__ == "__main__":
    main()
