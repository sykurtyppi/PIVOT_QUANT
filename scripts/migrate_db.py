#!/usr/bin/env python3
"""Schema migrations for PivotQuant SQLite storage.

This script is intentionally idempotent and safe to run on every startup.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from pathlib import Path
from typing import Callable

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
LATEST_SCHEMA_VERSION = 4


TOUCH_EVENT_SQL = """
CREATE TABLE IF NOT EXISTS touch_events (
    event_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    ts_event INTEGER NOT NULL,
    session TEXT,
    level_type TEXT NOT NULL,
    level_price REAL NOT NULL,
    touch_price REAL NOT NULL,
    touch_side INTEGER,
    distance_bps REAL NOT NULL,
    is_first_touch_today INTEGER DEFAULT 0,
    touch_count_today INTEGER DEFAULT 1,
    confluence_count INTEGER DEFAULT 0,
    confluence_types TEXT,
    ema9 REAL,
    ema21 REAL,
    ema_state INTEGER,
    vwap REAL,
    vwap_dist_bps REAL,
    atr REAL,
    rv_30 REAL,
    rv_regime INTEGER,
    iv_rv_state INTEGER,
    gamma_mode INTEGER,
    gamma_flip REAL,
    gamma_flip_dist_bps REAL,
    gamma_confidence INTEGER,
    oi_concentration_top5 REAL,
    zero_dte_share REAL,
    data_quality REAL,
    bar_interval_sec INTEGER,
    source TEXT,
    created_at INTEGER NOT NULL,
    vpoc REAL,
    vpoc_dist_bps REAL,
    volume_at_level REAL,
    mtf_confluence INTEGER DEFAULT 0,
    mtf_confluence_types TEXT,
    weekly_pivot REAL,
    monthly_pivot REAL,
    level_age_days INTEGER DEFAULT 0,
    hist_reject_rate REAL,
    hist_break_rate REAL,
    hist_sample_size INTEGER DEFAULT 0,
    regime_type INTEGER,
    overnight_gap_atr REAL,
    or_high REAL,
    or_low REAL,
    or_size_atr REAL,
    or_breakout INTEGER,
    or_high_dist_bps REAL,
    or_low_dist_bps REAL,
    session_std REAL,
    sigma_band_position REAL,
    distance_to_upper_sigma_bps REAL,
    distance_to_lower_sigma_bps REAL
);
"""

BAR_DATA_SQL = """
CREATE TABLE IF NOT EXISTS bar_data (
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

EVENT_LABELS_SQL = """
CREATE TABLE IF NOT EXISTS event_labels (
    event_id TEXT NOT NULL,
    horizon_min INTEGER NOT NULL,
    return_bps REAL,
    mfe_bps REAL,
    mae_bps REAL,
    reject INTEGER,
    break INTEGER,
    resolution_min REAL,
    PRIMARY KEY (event_id, horizon_min),
    FOREIGN KEY (event_id) REFERENCES touch_events(event_id)
);
"""

TOUCH_COLUMNS = {
    "vpoc": "REAL",
    "vpoc_dist_bps": "REAL",
    "volume_at_level": "REAL",
    "mtf_confluence": "INTEGER DEFAULT 0",
    "mtf_confluence_types": "TEXT",
    "weekly_pivot": "REAL",
    "monthly_pivot": "REAL",
    "level_age_days": "INTEGER DEFAULT 0",
    "hist_reject_rate": "REAL",
    "hist_break_rate": "REAL",
    "hist_sample_size": "INTEGER DEFAULT 0",
    "regime_type": "INTEGER",
    "overnight_gap_atr": "REAL",
    "or_high": "REAL",
    "or_low": "REAL",
    "or_size_atr": "REAL",
    "or_breakout": "INTEGER",
    "or_high_dist_bps": "REAL",
    "or_low_dist_bps": "REAL",
    "session_std": "REAL",
    "sigma_band_position": "REAL",
    "distance_to_upper_sigma_bps": "REAL",
    "distance_to_lower_sigma_bps": "REAL",
}


def connect(db_path: str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def ensure_meta_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """
    )


def get_schema_version(conn: sqlite3.Connection) -> int:
    ensure_meta_table(conn)
    row = conn.execute("SELECT value FROM schema_meta WHERE key = 'schema_version'").fetchone()
    if not row:
        return 0
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return 0


def set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    ts = int(time.time() * 1000)
    conn.execute(
        """
        INSERT INTO schema_meta(key, value, updated_at)
        VALUES('schema_version', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (str(version), ts),
    )


def migration_1_base_tables(conn: sqlite3.Connection) -> None:
    conn.execute(TOUCH_EVENT_SQL)
    conn.execute(BAR_DATA_SQL)
    conn.execute(EVENT_LABELS_SQL)


def migration_2_columns_and_indexes(conn: sqlite3.Connection) -> None:
    # Ensure touch_events columns on old DBs.
    touch_cols = {row[1] for row in conn.execute("PRAGMA table_info(touch_events)").fetchall()}
    for col_name, col_type in TOUCH_COLUMNS.items():
        if col_name not in touch_cols:
            conn.execute(f"ALTER TABLE touch_events ADD COLUMN {col_name} {col_type}")

    # Ensure bar_data interval column on older schemas.
    bar_cols = {row[1] for row in conn.execute("PRAGMA table_info(bar_data)").fetchall()}
    if "bar_interval_sec" not in bar_cols:
        conn.execute("ALTER TABLE bar_data ADD COLUMN bar_interval_sec INTEGER")

    # Indexes and dedup constraints.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_touch_symbol_ts ON touch_events(symbol, ts_event);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_touch_level_ts ON touch_events(level_type, ts_event);")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_touch_natural_key "
        "ON touch_events(symbol, ts_event, level_type, level_price, bar_interval_sec);"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bar_symbol_ts ON bar_data(symbol, ts);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bar_symbol_interval ON bar_data(symbol, bar_interval_sec, ts);")


def migration_3_prediction_log(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS prediction_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            ts_prediction INTEGER NOT NULL,
            model_version TEXT,
            feature_version TEXT,
            best_horizon INTEGER,
            abstain INTEGER NOT NULL DEFAULT 0,
            signal_5m TEXT,
            signal_15m TEXT,
            signal_60m TEXT,
            prob_reject_5m REAL,
            prob_reject_15m REAL,
            prob_reject_60m REAL,
            prob_break_5m REAL,
            prob_break_15m REAL,
            prob_break_60m REAL,
            threshold_reject_5m REAL,
            threshold_reject_15m REAL,
            threshold_reject_60m REAL,
            threshold_break_5m REAL,
            threshold_break_15m REAL,
            threshold_break_60m REAL,
            quality_flags TEXT,
            is_preview INTEGER NOT NULL DEFAULT 0,
            UNIQUE(event_id, model_version)
        );"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_predlog_event "
        "ON prediction_log(event_id);"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_predlog_ts "
        "ON prediction_log(ts_prediction);"
    )


def migration_4_prediction_log_compat(conn: sqlite3.Connection) -> None:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "prediction_log" not in tables:
        return

    pred_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(prediction_log)").fetchall()
    }
    if "is_preview" not in pred_cols:
        conn.execute(
            "ALTER TABLE prediction_log "
            "ADD COLUMN is_preview INTEGER NOT NULL DEFAULT 0"
        )


MIGRATIONS: list[tuple[int, str, Callable[[sqlite3.Connection], None]]] = [
    (1, "base_schema_tables", migration_1_base_tables),
    (2, "columns_and_indexes", migration_2_columns_and_indexes),
    (3, "prediction_log", migration_3_prediction_log),
    (4, "prediction_log_compat", migration_4_prediction_log_compat),
]


def migrate_connection(conn: sqlite3.Connection, target_version: int = LATEST_SCHEMA_VERSION, verbose: bool = True) -> dict:
    ensure_meta_table(conn)
    current_version = get_schema_version(conn)
    applied: list[dict] = []

    for version, name, fn in MIGRATIONS:
        if version > target_version:
            break
        if version <= current_version:
            continue
        fn(conn)
        set_schema_version(conn, version)
        conn.commit()
        applied.append({"version": version, "name": name})
        if verbose:
            print(f"[migrate_db] applied v{version}: {name}")

    final_version = get_schema_version(conn)
    summary = {
        "db": conn.execute("PRAGMA database_list").fetchone()[2],
        "applied": applied,
        "from_version": current_version,
        "to_version": final_version,
        "target_version": target_version,
    }
    if verbose:
        print(
            f"[migrate_db] schema version {current_version} -> {final_version} "
            f"(target: {target_version})"
        )
    return summary


def migrate_db(db_path: str, target_version: int = LATEST_SCHEMA_VERSION, verbose: bool = True) -> dict:
    conn = connect(db_path)
    try:
        return migrate_connection(conn, target_version=target_version, verbose=verbose)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply SQLite schema migrations for PivotQuant.")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite DB")
    parser.add_argument("--target-version", type=int, default=LATEST_SCHEMA_VERSION)
    parser.add_argument("--quiet", action="store_true", default=False)
    args = parser.parse_args()

    if args.target_version < 1:
        raise SystemExit("--target-version must be >= 1")

    migrate_db(args.db, target_version=args.target_version, verbose=not args.quiet)


if __name__ == "__main__":
    main()
