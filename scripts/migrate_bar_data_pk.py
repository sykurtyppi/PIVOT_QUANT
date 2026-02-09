#!/usr/bin/env python3
import argparse
import os
import sqlite3
import time

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")


def get_table_info(conn, table):
    return conn.execute(f"PRAGMA table_info({table})").fetchall()


def has_column(conn, table, column):
    return any(row[1] == column for row in get_table_info(conn, table))


def pk_columns(conn, table):
    return [row[1] for row in get_table_info(conn, table) if row[5] > 0]


def main():
    parser = argparse.ArgumentParser(description="Migrate bar_data PK to include bar_interval_sec.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--default-interval-sec", type=int, default=60)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys=OFF;")

    if not has_column(conn, "bar_data", "bar_interval_sec"):
        print("bar_data missing bar_interval_sec. Adding column...")
        if not args.dry_run:
            conn.execute("ALTER TABLE bar_data ADD COLUMN bar_interval_sec INTEGER")
            conn.commit()

    pk = pk_columns(conn, "bar_data")
    if pk == ["symbol", "ts", "bar_interval_sec"]:
        print("bar_data already has composite PK. No migration needed.")
        return

    print(f"Current PK: {pk or 'none'}")
    print("Migrating bar_data to composite PK (symbol, ts, bar_interval_sec)...")

    backup = f"bar_data_backup_{int(time.time())}"
    if args.dry_run:
        print(f"[dry-run] Would create backup table {backup} and rebuild bar_data")
        return

    conn.execute(f"ALTER TABLE bar_data RENAME TO {backup}")
    conn.execute(
        """
        CREATE TABLE bar_data (
            symbol TEXT NOT NULL,
            ts INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL,
            bar_interval_sec INTEGER,
            PRIMARY KEY (symbol, ts, bar_interval_sec)
        );
        """
    )

    conn.execute(
        f"""
        INSERT INTO bar_data (symbol, ts, open, high, low, close, volume, bar_interval_sec)
        SELECT
            symbol,
            ts,
            open,
            high,
            low,
            close,
            volume,
            COALESCE(bar_interval_sec, ?)
        FROM {backup}
        """,
        (args.default_interval_sec,),
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bar_symbol_ts ON bar_data(symbol, ts);")
    conn.commit()
    conn.close()
    print(f"Migration complete. Backup table: {backup}")


if __name__ == "__main__":
    main()
