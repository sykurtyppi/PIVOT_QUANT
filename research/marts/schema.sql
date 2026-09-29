CREATE SCHEMA IF NOT EXISTS pq_research;

CREATE OR REPLACE TABLE pq_research.mart_event_base AS
WITH dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY horizon_min) AS rn
    FROM training_events_v1
)
SELECT
    event_id,
    symbol,
    ts_event,
    event_ts_utc,
    event_ts_et,
    CAST(event_date_et AS DATE) AS event_date_et,
    event_hour_et,
    tod_bucket,
    level_type,
    level_family,
    touch_side,
    distance_bps,
    touch_price,
    level_price,
    bar_interval_sec,
    confluence_count,
    COALESCE(mtf_confluence_calc, 0) AS mtf_confluence,
    CASE
        WHEN POSITION('weekly' IN LOWER(COALESCE(CAST(mtf_confluence_types AS VARCHAR), ''))) > 0 THEN 1
        ELSE 0
    END AS has_weekly_confluence,
    CASE
        WHEN POSITION('monthly' IN LOWER(COALESCE(CAST(mtf_confluence_types AS VARCHAR), ''))) > 0 THEN 1
        ELSE 0
    END AS has_monthly_confluence,
    CASE
        WHEN (
            CASE
                WHEN POSITION('weekly' IN LOWER(COALESCE(CAST(mtf_confluence_types AS VARCHAR), ''))) > 0 THEN 1
                ELSE 0
            END
            +
            CASE
                WHEN POSITION('monthly' IN LOWER(COALESCE(CAST(mtf_confluence_types AS VARCHAR), ''))) > 0 THEN 1
                ELSE 0
            END
        ) >= 2 THEN 'stacked'
        WHEN (
            CASE
                WHEN POSITION('weekly' IN LOWER(COALESCE(CAST(mtf_confluence_types AS VARCHAR), ''))) > 0 THEN 1
                ELSE 0
            END
            +
            CASE
                WHEN POSITION('monthly' IN LOWER(COALESCE(CAST(mtf_confluence_types AS VARCHAR), ''))) > 0 THEN 1
                ELSE 0
            END
        ) = 1 THEN 'single'
        ELSE 'none'
    END AS confluence_bucket,
    regime_type,
    CASE
        WHEN regime_type IN (1, 2, 4) THEN 'expansion'
        WHEN regime_type = 3 THEN 'compression'
        ELSE 'neutral'
    END AS regime_bucket,
    rv_regime,
    data_quality,
    gamma_mode,
    gamma_flip_dist_bps_calc,
    ema_state_calc,
    vwap_dist_bps_calc,
    vpoc_dist_bps_calc,
    weekly_pivot_dist_bps,
    monthly_pivot_dist_bps,
    session_std,
    or_size_atr,
    or_breakout,
    or_high_dist_bps,
    or_low_dist_bps,
    sigma_band_position,
    distance_to_upper_sigma_bps,
    distance_to_lower_sigma_bps,
    is_persistent_level,
    hist_sample_size_calc,
    (
        EXTRACT('hour' FROM event_ts_et) * 60
        + EXTRACT('minute' FROM event_ts_et)
        - 570
    ) AS minutes_since_open,
    CASE
        WHEN (
            EXTRACT('hour' FROM event_ts_et) * 60
            + EXTRACT('minute' FROM event_ts_et)
            - 570
        ) <= 30 THEN 1
        ELSE 0
    END AS is_first_30min,
    CASE
        WHEN atr IS NULL OR touch_price IS NULL OR touch_price = 0 THEN 'unknown'
        WHEN ABS(distance_bps) / NULLIF((atr / touch_price) * 1e4, 0) <= 0.05 THEN 'ultra'
        WHEN ABS(distance_bps) / NULLIF((atr / touch_price) * 1e4, 0) <= 0.10 THEN 'near'
        WHEN ABS(distance_bps) / NULLIF((atr / touch_price) * 1e4, 0) <= 0.20 THEN 'mid'
        ELSE 'far'
    END AS atr_zone
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE TABLE pq_research.mart_event_labels AS
WITH build_cfg AS (
    SELECT trade_cost_bps
    FROM pq_research.mart_build_config
    LIMIT 1
)
SELECT
    event_id,
    symbol,
    CAST(event_date_et AS DATE) AS event_date_et,
    horizon_min,
    return_bps,
    mfe_bps,
    mae_bps,
    reject,
    break,
    resolution_min,
    touch_side,
    CASE
        WHEN COALESCE(touch_side, 1) >= 0 THEN return_bps
        ELSE -return_bps
    END AS directional_return_bps,
    CASE
        WHEN COALESCE(touch_side, 1) >= 0 THEN return_bps
        ELSE -return_bps
    END - (SELECT trade_cost_bps FROM build_cfg) AS reject_net_bps,
    -1 * (
        CASE
            WHEN COALESCE(touch_side, 1) >= 0 THEN return_bps
            ELSE -return_bps
        END
    ) - (SELECT trade_cost_bps FROM build_cfg) AS break_net_bps
FROM training_events_v1;

CREATE OR REPLACE TABLE pq_research.mart_slice_expectancy_daily AS
SELECT
    eb.symbol,
    eb.event_date_et,
    el.horizon_min,
    eb.regime_bucket,
    eb.level_family,
    eb.tod_bucket,
    eb.atr_zone,
    eb.confluence_bucket,
    COUNT(*) AS rows_n,
    AVG(el.reject_net_bps) AS avg_reject_net_bps,
    AVG(CASE WHEN el.reject_net_bps > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_reject,
    AVG(el.break_net_bps) AS avg_break_net_bps,
    AVG(CASE WHEN el.break_net_bps > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_break,
    AVG(el.mfe_bps) AS avg_mfe_bps,
    AVG(el.mae_bps) AS avg_mae_bps,
    STDDEV_SAMP(el.reject_net_bps) AS std_reject_net_bps,
    QUANTILE_CONT(el.reject_net_bps, 0.05) AS p05_reject_net_bps,
    QUANTILE_CONT(el.reject_net_bps, 0.50) AS p50_reject_net_bps,
    QUANTILE_CONT(el.reject_net_bps, 0.95) AS p95_reject_net_bps
FROM pq_research.mart_event_base eb
JOIN pq_research.mart_event_labels el
  ON eb.event_id = el.event_id
GROUP BY
    eb.symbol,
    eb.event_date_et,
    el.horizon_min,
    eb.regime_bucket,
    eb.level_family,
    eb.tod_bucket,
    eb.atr_zone,
    eb.confluence_bucket;

CREATE OR REPLACE TABLE pq_research.mart_slice_expectancy_rollup AS
SELECT
    symbol,
    horizon_min,
    regime_bucket,
    level_family,
    tod_bucket,
    atr_zone,
    confluence_bucket,
    SUM(rows_n) AS rows_n,
    COUNT(*) AS days_n,
    AVG(avg_reject_net_bps) AS mean_daily_reject_net_bps,
    STDDEV_SAMP(avg_reject_net_bps) AS std_daily_reject_net_bps,
    AVG(win_rate_reject) AS mean_daily_win_rate_reject,
    MIN(avg_reject_net_bps) AS worst_day_reject_net_bps,
    MAX(avg_reject_net_bps) AS best_day_reject_net_bps,
    SUM(CASE WHEN avg_reject_net_bps > 0 THEN 1 ELSE 0 END) AS positive_days
FROM pq_research.mart_slice_expectancy_daily
GROUP BY
    symbol,
    horizon_min,
    regime_bucket,
    level_family,
    tod_bucket,
    atr_zone,
    confluence_bucket;
