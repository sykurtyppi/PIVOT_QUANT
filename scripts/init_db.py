#!/usr/bin/env python3
import os
import sqlite3
from pathlib import Path

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def init_db(db_path: str) -> None:
    path = Path(db_path)
    ensure_parent(path)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    conn.execute(
        """
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
    )

    conn.execute(
        """
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
    )

    conn.execute(
        """
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
    )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_touch_symbol_ts ON touch_events(symbol, ts_event);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_touch_level_ts ON touch_events(level_type, ts_event);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bar_symbol_ts ON bar_data(symbol, ts);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bar_symbol_interval ON bar_data(symbol, bar_interval_sec, ts);")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db(DEFAULT_DB)
    print(f"Initialized DB at {DEFAULT_DB}")
