#!/usr/bin/env python3
import os
import sys
from pathlib import Path

EXPORT_DIR = Path(os.getenv("EXPORT_DIR", "data/exports"))
DB_PATH = Path(os.getenv("DUCKDB_PATH", "data/pivot_training.duckdb"))


def main() -> None:
    try:
        import duckdb  # type: ignore
    except Exception:
        print("duckdb not installed. Install with: python3 -m pip install duckdb", file=sys.stderr)
        sys.exit(1)

    touch_csv = EXPORT_DIR / "touch_events.csv"
    labels_csv = EXPORT_DIR / "event_labels.csv"
    touch_parquet = EXPORT_DIR / "touch_events.parquet"
    labels_parquet = EXPORT_DIR / "event_labels.parquet"

    use_parquet = touch_parquet.exists() and labels_parquet.exists()
    if not use_parquet and (not touch_csv.exists() or not labels_csv.exists()):
        print(
            "Exports missing. Run: python3 scripts/export_parquet.py "
            "or python3 scripts/export_csv.py",
            file=sys.stderr,
        )
        sys.exit(1)

    if use_parquet:
        print("Using parquet exports for training view.")
    else:
        print("Using CSV exports for training view.")

    con = duckdb.connect(str(DB_PATH))
    con.execute(
        f"""
        CREATE OR REPLACE VIEW training_events_v1 AS
        WITH touch AS (
            SELECT
                event_id,
                symbol,
                try_cast(ts_event AS BIGINT) AS ts_event,
                session,
                level_type,
                try_cast(level_price AS DOUBLE) AS level_price,
                try_cast(touch_price AS DOUBLE) AS touch_price,
                try_cast(touch_side AS INTEGER) AS touch_side,
                try_cast(distance_bps AS DOUBLE) AS distance_bps,
                try_cast(is_first_touch_today AS INTEGER) AS is_first_touch_today,
                try_cast(touch_count_today AS INTEGER) AS touch_count_today,
                try_cast(confluence_count AS INTEGER) AS confluence_count,
                confluence_types,
                try_cast(ema9 AS DOUBLE) AS ema9,
                try_cast(ema21 AS DOUBLE) AS ema21,
                try_cast(ema_state AS INTEGER) AS ema_state,
                try_cast(vwap AS DOUBLE) AS vwap,
                try_cast(vwap_dist_bps AS DOUBLE) AS vwap_dist_bps,
                try_cast(atr AS DOUBLE) AS atr,
                try_cast(rv_30 AS DOUBLE) AS rv_30,
                try_cast(rv_regime AS INTEGER) AS rv_regime,
                try_cast(iv_rv_state AS INTEGER) AS iv_rv_state,
                try_cast(gamma_mode AS INTEGER) AS gamma_mode,
                try_cast(gamma_flip AS DOUBLE) AS gamma_flip,
                try_cast(gamma_flip_dist_bps AS DOUBLE) AS gamma_flip_dist_bps,
                try_cast(gamma_confidence AS INTEGER) AS gamma_confidence,
                try_cast(oi_concentration_top5 AS DOUBLE) AS oi_concentration_top5,
                try_cast(zero_dte_share AS DOUBLE) AS zero_dte_share,
                try_cast(data_quality AS DOUBLE) AS data_quality,
                try_cast(bar_interval_sec AS INTEGER) AS bar_interval_sec,
                source,
                try_cast(created_at AS BIGINT) AS created_at
            FROM {('read_parquet' if use_parquet else 'read_csv_auto')}('{touch_parquet if use_parquet else touch_csv}')
        ),
        labels AS (
            SELECT
                event_id,
                try_cast(horizon_min AS INTEGER) AS horizon_min,
                try_cast(return_bps AS DOUBLE) AS return_bps,
                try_cast(mfe_bps AS DOUBLE) AS mfe_bps,
                try_cast(mae_bps AS DOUBLE) AS mae_bps,
                try_cast(reject AS INTEGER) AS reject,
                try_cast("break" AS INTEGER) AS break,
                try_cast(resolution_min AS DOUBLE) AS resolution_min
            FROM {('read_parquet' if use_parquet else 'read_csv_auto')}('{labels_parquet if use_parquet else labels_csv}')
        ),
        joined AS (
            SELECT
                t.*,
                l.horizon_min,
                l.return_bps,
                l.mfe_bps,
                l.mae_bps,
                l.reject,
                l.break,
                l.resolution_min
            FROM touch t
            JOIN labels l
            ON t.event_id = l.event_id
        ),
        timed AS (
            SELECT
                joined.*,
                to_timestamp(joined.ts_event / 1000) AS event_ts_utc,
                timezone('America/New_York', to_timestamp(joined.ts_event / 1000)) AS event_ts_et
            FROM joined
        )
        SELECT
            timed.*,
            CAST(strftime(timed.event_ts_et, '%Y-%m-%d') AS DATE) AS event_date_et,
            EXTRACT('hour' FROM timed.event_ts_et) AS event_hour_et,
            CASE
                WHEN EXTRACT('hour' FROM timed.event_ts_et) < 10 THEN 'open'
                WHEN EXTRACT('hour' FROM timed.event_ts_et) < 14 THEN 'mid'
                WHEN EXTRACT('hour' FROM timed.event_ts_et) < 16 THEN 'power'
                ELSE 'overnight'
            END AS tod_bucket,
            CASE
                WHEN starts_with(timed.level_type, 'R') THEN 'resistance'
                WHEN starts_with(timed.level_type, 'S') THEN 'support'
                WHEN timed.level_type = 'GAMMA' THEN 'gamma'
                ELSE 'pivot'
            END AS level_family,
            CASE
                WHEN timed.ema_state IS NOT NULL THEN timed.ema_state
                WHEN timed.ema9 IS NULL OR timed.ema21 IS NULL THEN NULL
                WHEN timed.ema9 > timed.ema21 THEN 1
                WHEN timed.ema9 < timed.ema21 THEN -1
                ELSE 0
            END AS ema_state_calc,
            CASE
                WHEN timed.vwap_dist_bps IS NOT NULL THEN timed.vwap_dist_bps
                WHEN timed.vwap IS NULL THEN NULL
                ELSE (timed.touch_price - timed.vwap) / timed.vwap * 1e4
            END AS vwap_dist_bps_calc,
            CASE
                WHEN timed.gamma_flip_dist_bps IS NOT NULL THEN timed.gamma_flip_dist_bps
                WHEN timed.gamma_flip IS NULL THEN NULL
                ELSE (timed.touch_price - timed.gamma_flip) / timed.gamma_flip * 1e4
            END AS gamma_flip_dist_bps_calc
        FROM timed
        """
    )
    con.close()
    print(f"DuckDB view created at {DB_PATH}")


if __name__ == "__main__":
    main()
