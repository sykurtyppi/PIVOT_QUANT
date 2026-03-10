#!/usr/bin/env python3
"""Generate daily ML quality/operations report.

Outputs:
  - Markdown report per day: logs/reports/ml_daily_<YYYY-MM-DD>.md
  - Latest alias: logs/reports/ml_daily_latest.md
  - Persistent metric snapshots in SQLite table daily_ml_metrics
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
DEFAULT_REPORT_DIR = os.getenv("ML_REPORT_DIR", "logs/reports")
ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = Path(os.getenv("RF_MODEL_DIR", str(ROOT / "data" / "models")))
DEFAULT_GAMMA_LOG = ROOT / "logs" / "gamma_bridge.log"
RF_MANIFEST_PATH = os.getenv("RF_MANIFEST_PATH", "").strip()
RF_ACTIVE_MANIFEST = os.getenv("RF_ACTIVE_MANIFEST", "manifest_active.json").strip() or "manifest_active.json"
RF_CANDIDATE_MANIFEST = (
    os.getenv("RF_CANDIDATE_MANIFEST", "manifest_runtime_latest.json").strip()
    or "manifest_runtime_latest.json"
)
LEGACY_CANDIDATE_MANIFEST = "manifest_latest.json"

try:
    from migrate_db import migrate_connection
except ImportError:  # pragma: no cover
    migrate_connection = None  # type: ignore

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

ET_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc
REGULAR_SESSION_OPEN_ET = dtime(9, 30)
REGULAR_SESSION_CLOSE_ET = dtime(16, 0)
SESSION_STALE_WARN_HOURS = float(os.getenv("ML_STALENESS_WARN_SESSION_HOURS", "13"))
SESSION_STALE_KILL_HOURS = float(os.getenv("ML_STALENESS_KILL_SESSION_HOURS", "19.5"))
REPORT_HORIZONS = [
    int(h.strip())
    for h in os.getenv("ML_REPORT_HORIZONS", "5,15,30,60").split(",")
    if h.strip().isdigit()
]
SHADOW_HORIZONS = {
    int(h.strip())
    for h in os.getenv("ML_SHADOW_HORIZONS", "30").split(",")
    if h.strip().isdigit()
}
ANALOG_DISAGREEMENT_THRESHOLD = float(os.getenv("ML_ANALOG_DISAGREEMENT_FLAG", "0.25"))
ANALOG_PROMOTION_MIN_AVAILABLE = int(os.getenv("ML_ANALOG_PROMOTION_MIN_AVAILABLE", "50"))
ANALOG_PROMOTION_MIN_QUALITY_OK = int(os.getenv("ML_ANALOG_PROMOTION_MIN_QUALITY_OK", "30"))
ANALOG_PROMOTION_MIN_EFFECTIVE_N = float(os.getenv("ML_ANALOG_PROMOTION_MIN_EFFECTIVE_N", "8"))
ANALOG_PROMOTION_MAX_MEAN_CI_WIDTH = float(os.getenv("ML_ANALOG_PROMOTION_MAX_MEAN_CI_WIDTH", "0.35"))
ANALOG_PROMOTION_MAX_BRIER_DELTA = float(os.getenv("ML_ANALOG_PROMOTION_MAX_BRIER_DELTA", "0.0"))
ANALOG_PROMOTION_MAX_ECE_DELTA = float(os.getenv("ML_ANALOG_PROMOTION_MAX_ECE_DELTA", "0.0"))
ANALOG_PROMOTION_MIN_HORIZONS = max(1, int(os.getenv("ML_ANALOG_PROMOTION_MIN_HORIZONS", "2")))
ANALOG_PROMOTION_LOOKBACK_DAYS = max(
    1, int(os.getenv("ML_ANALOG_PROMOTION_LOOKBACK_DAYS", "5"))
)
ANALOG_PROMOTION_EVAL_MODE = (
    os.getenv("ML_ANALOG_PROMOTION_EVAL_MODE", "blend") or "blend"
).strip().lower()
if ANALOG_PROMOTION_EVAL_MODE not in {"analog", "blend"}:
    ANALOG_PROMOTION_EVAL_MODE = "blend"
ANALOG_BLEND_WEIGHT_BASE = max(
    0.0, min(1.0, float(os.getenv("ML_ANALOG_BLEND_WEIGHT_BASE", "0.30")))
)
ANALOG_BLEND_WEIGHT_MAX = max(
    0.0, min(1.0, float(os.getenv("ML_ANALOG_BLEND_WEIGHT_MAX", "0.60")))
)
if ANALOG_BLEND_WEIGHT_MAX < ANALOG_BLEND_WEIGHT_BASE:
    ANALOG_BLEND_WEIGHT_MAX = ANALOG_BLEND_WEIGHT_BASE
ANALOG_BLEND_N_EFF_REF = max(
    1.0, float(os.getenv("ML_ANALOG_BLEND_N_EFF_REF", "20"))
)
ANALOG_BLEND_CI_WIDTH_REF = max(
    0.05, float(os.getenv("ML_ANALOG_MAX_CI_WIDTH", "0.6"))
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


ANALOG_PROMOTION_REQUIRE_BOTH_TARGETS = _env_bool(
    "ML_ANALOG_PROMOTION_REQUIRE_BOTH_TARGETS", True
)
ANALOG_PROMOTION_USE_SHRUNK = _env_bool(
    "ML_ANALOG_PROMOTION_USE_SHRUNK", True
)

# NYSE full-closure holidays (update annually).
# Source: https://www.nyse.com/markets/hours-calendars
NYSE_HOLIDAYS: set[date] = {
    # 2025
    date(2025, 1, 1),   # New Year's Day
    date(2025, 1, 20),  # MLK Day
    date(2025, 2, 17),  # Presidents' Day
    date(2025, 4, 18),  # Good Friday
    date(2025, 5, 26),  # Memorial Day
    date(2025, 6, 19),  # Juneteenth
    date(2025, 7, 4),   # Independence Day
    date(2025, 9, 1),   # Labor Day
    date(2025, 11, 27), # Thanksgiving
    date(2025, 12, 25), # Christmas Day
    # 2026
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Day
    date(2026, 2, 16),  # Presidents' Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (observed)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving
    date(2026, 12, 25), # Christmas Day
    # 2027 (pre-loaded for EOY runs)
    date(2027, 1, 1),   # New Year's Day
    date(2027, 1, 18),  # MLK Day
    date(2027, 2, 15),  # Presidents' Day
    date(2027, 3, 26),  # Good Friday
    date(2027, 5, 31),  # Memorial Day
    date(2027, 6, 18),  # Juneteenth (observed)
    date(2027, 7, 5),   # Independence Day (observed)
    date(2027, 9, 6),   # Labor Day
    date(2027, 11, 25), # Thanksgiving
    date(2027, 12, 24), # Christmas Day (observed)
}


@dataclass
class MetricBundle:
    horizon: int
    sample_size: int
    signal_reject_count: int
    signal_break_count: int
    signal_no_edge_count: int
    abstain_rate: float | None
    reject_precision: float | None
    reject_recall: float | None
    break_precision: float | None
    break_recall: float | None
    brier_reject: float | None
    brier_break: float | None
    ece_reject: float | None
    ece_break: float | None
    auc_reject: float | None
    auc_break: float | None
    avg_return_bps: float | None
    avg_mfe_bps: float | None
    avg_mae_bps: float | None
    confidence_misses: list[dict[str, Any]]


@dataclass
class AnalogHorizonSummary:
    horizon: int
    sample_size: int
    analog_available_count: int
    analog_quality_ok_count: int
    mean_neighbors: float | None
    mean_effective_neighbors: float | None
    mean_ci_width: float | None
    mean_disagreement: float | None
    high_disagreement_count: int
    high_disagreement_model_abs_error: float | None
    low_disagreement_model_abs_error: float | None
    model_reject_brier_matched: float | None
    analog_reject_brier: float | None
    reject_brier_delta: float | None
    model_reject_ece_matched: float | None
    analog_reject_ece: float | None
    reject_ece_delta: float | None
    model_break_brier_matched: float | None
    analog_break_brier: float | None
    break_brier_delta: float | None
    model_break_ece_matched: float | None
    analog_break_ece: float | None
    break_ece_delta: float | None
    blend_reject_brier: float | None = None
    blend_reject_ece: float | None = None
    blend_break_brier: float | None = None
    blend_break_ece: float | None = None
    reject_brier_delta_blend: float | None = None
    reject_ece_delta_blend: float | None = None
    break_brier_delta_blend: float | None = None
    break_ece_delta_blend: float | None = None
    guard_reject_keep_rate: float | None = None
    guard_break_keep_rate: float | None = None
    guard_reject_brier: float | None = None
    guard_reject_ece: float | None = None
    guard_break_brier: float | None = None
    guard_break_ece: float | None = None
    guard_reject_brier_delta: float | None = None
    guard_reject_ece_delta: float | None = None
    guard_break_brier_delta: float | None = None
    guard_break_ece_delta: float | None = None


@dataclass
class AnalogPromotionGate:
    status: str
    passed_horizons: list[int]
    evaluated_horizons: list[int]
    required_horizons: int
    reasons: list[str]
    thresholds: dict[str, Any]
    horizon_results: dict[str, dict[str, Any]] = field(default_factory=dict)


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def ensure_daily_metrics_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_ml_metrics (
            report_date TEXT NOT NULL,
            horizon_min INTEGER NOT NULL,
            sample_size INTEGER NOT NULL DEFAULT 0,
            signal_reject_count INTEGER NOT NULL DEFAULT 0,
            signal_break_count INTEGER NOT NULL DEFAULT 0,
            signal_no_edge_count INTEGER NOT NULL DEFAULT 0,
            abstain_rate REAL,
            reject_precision REAL,
            reject_recall REAL,
            break_precision REAL,
            break_recall REAL,
            brier_reject REAL,
            brier_break REAL,
            ece_reject REAL,
            ece_break REAL,
            auc_reject REAL,
            auc_break REAL,
            avg_return_bps REAL,
            avg_mfe_bps REAL,
            avg_mae_bps REAL,
            regime_low_count INTEGER NOT NULL DEFAULT 0,
            regime_normal_count INTEGER NOT NULL DEFAULT 0,
            regime_high_count INTEGER NOT NULL DEFAULT 0,
            regime_up_count INTEGER NOT NULL DEFAULT 0,
            regime_down_count INTEGER NOT NULL DEFAULT 0,
            regime_range_count INTEGER NOT NULL DEFAULT 0,
            regime_vol_exp_count INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (report_date, horizon_min)
        );
        """
    )
    conn.commit()


def parse_report_date(date_arg: str | None) -> date:
    now_et = datetime.now(ET_TZ)
    if date_arg:
        return datetime.strptime(date_arg, "%Y-%m-%d").date()
    # Default to the latest completed market day.
    if now_et.hour < 18:
        return (now_et - timedelta(days=1)).date()
    return now_et.date()


def day_bounds_ms(report_day: date) -> tuple[int, int]:
    start_dt = datetime.combine(report_day, dtime.min, tzinfo=ET_TZ)
    end_dt = start_dt + timedelta(days=1)
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def mean_or_none(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def safe_round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def brier_score(y_true: list[int], probs: list[float]) -> float | None:
    if not y_true:
        return None
    return statistics.fmean((p - y) ** 2 for y, p in zip(y_true, probs))


def expected_calibration_error(y_true: list[int], probs: list[float], bins: int = 10) -> float | None:
    if not y_true:
        return None
    n = len(y_true)
    ece = 0.0
    for i in range(bins):
        lo = i / bins
        hi = (i + 1) / bins
        idx = [
            j for j, p in enumerate(probs)
            if (p >= lo and p < hi) or (i == bins - 1 and p == hi)
        ]
        if not idx:
            continue
        acc = statistics.fmean(y_true[j] for j in idx)
        conf = statistics.fmean(probs[j] for j in idx)
        ece += (len(idx) / n) * abs(acc - conf)
    return ece


def _compute_report_blend_weight(n_eff: float | None, ci_width: float | None) -> float:
    if n_eff is None:
        return 0.0
    eff_scale = min(1.0, max(0.0, float(n_eff)) / ANALOG_BLEND_N_EFF_REF)
    if ci_width is None:
        ci_scale = 1.0
    else:
        ci_scale = max(0.0, 1.0 - (float(ci_width) / ANALOG_BLEND_CI_WIDTH_REF))
    weight = ANALOG_BLEND_WEIGHT_BASE * eff_scale * ci_scale
    return max(0.0, min(ANALOG_BLEND_WEIGHT_MAX, weight))


def _extract_analog_prob(horizon_payload: dict[str, Any], target: str) -> float | None:
    preferred_key = f"{target}_prob_shrunk" if ANALOG_PROMOTION_USE_SHRUNK else f"{target}_prob"
    raw = horizon_payload.get(preferred_key)
    if not isinstance(raw, (int, float)):
        raw = horizon_payload.get(f"{target}_prob")
    if not isinstance(raw, (int, float)):
        return None
    prob = float(raw)
    if prob < 0.0:
        return 0.0
    if prob > 1.0:
        return 1.0
    return prob


def roc_auc_binary(y_true: list[int], probs: list[float]) -> float | None:
    if not y_true:
        return None
    pos = sum(1 for y in y_true if y == 1)
    neg = sum(1 for y in y_true if y == 0)
    if pos == 0 or neg == 0:
        return None

    indexed = sorted(enumerate(probs), key=lambda item: item[1])
    ranks = [0.0] * len(probs)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + 1 + j + 1) / 2.0
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1

    rank_sum_pos = sum(ranks[i] for i, y in enumerate(y_true) if y == 1)
    auc = (rank_sum_pos - pos * (pos + 1) / 2.0) / (pos * neg)
    return auc


def compute_precision_recall(actual: list[int], predicted_positive: list[bool]) -> tuple[float | None, float | None]:
    tp = sum(1 for a, p in zip(actual, predicted_positive) if p and a == 1)
    pred_pos = sum(1 for p in predicted_positive if p)
    act_pos = sum(1 for a in actual if a == 1)
    precision = (tp / pred_pos) if pred_pos > 0 else None
    recall = (tp / act_pos) if act_pos > 0 else None
    return precision, recall


def _prediction_log_expr(pred_cols: set[str], col_name: str) -> str:
    if col_name in pred_cols:
        return f"lp.{col_name} AS {col_name}"
    return f"NULL AS {col_name}"


def fetch_labeled_records(
    conn: sqlite3.Connection,
    start_ms: int,
    end_ms: int,
    include_preview: bool,
) -> list[dict[str, Any]]:
    pred_cols = {r[1] for r in conn.execute("PRAGMA table_info(prediction_log)").fetchall()}
    has_preview = "is_preview" in pred_cols
    quality_flags_expr = _prediction_log_expr(pred_cols, "quality_flags")
    regime_policy_mode_expr = _prediction_log_expr(pred_cols, "regime_policy_mode")
    trade_regime_expr = _prediction_log_expr(pred_cols, "trade_regime")
    selected_policy_expr = _prediction_log_expr(pred_cols, "selected_policy")
    regime_policy_json_expr = _prediction_log_expr(pred_cols, "regime_policy_json")
    analog_json_expr = _prediction_log_expr(pred_cols, "analog_json")
    analog_best_reject_expr = _prediction_log_expr(pred_cols, "analog_best_reject_prob")
    analog_best_break_expr = _prediction_log_expr(pred_cols, "analog_best_break_prob")
    analog_best_n_expr = _prediction_log_expr(pred_cols, "analog_best_n")
    analog_best_ci_width_expr = _prediction_log_expr(pred_cols, "analog_best_ci_width")
    analog_best_disagreement_expr = _prediction_log_expr(pred_cols, "analog_best_disagreement")
    analog_json_expr = _prediction_log_expr(pred_cols, "analog_json")
    analog_best_reject_expr = _prediction_log_expr(pred_cols, "analog_best_reject_prob")
    analog_best_break_expr = _prediction_log_expr(pred_cols, "analog_best_break_prob")
    analog_best_n_expr = _prediction_log_expr(pred_cols, "analog_best_n")
    analog_best_ci_width_expr = _prediction_log_expr(pred_cols, "analog_best_ci_width")
    analog_best_disagreement_expr = _prediction_log_expr(pred_cols, "analog_best_disagreement")
    analog_json_expr = _prediction_log_expr(pred_cols, "analog_json")
    analog_best_reject_expr = _prediction_log_expr(pred_cols, "analog_best_reject_prob")
    analog_best_break_expr = _prediction_log_expr(pred_cols, "analog_best_break_prob")
    analog_best_n_expr = _prediction_log_expr(pred_cols, "analog_best_n")
    analog_best_ci_width_expr = _prediction_log_expr(pred_cols, "analog_best_ci_width")
    analog_best_disagreement_expr = _prediction_log_expr(pred_cols, "analog_best_disagreement")
    preview_filter = ""
    if has_preview and not include_preview:
        preview_filter = "AND COALESCE(lp.is_preview, 0) = 0"

    sql = f"""
        WITH latest_pred AS (
            SELECT *
            FROM (
                SELECT
                    pl.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY pl.event_id
                        ORDER BY pl.ts_prediction DESC
                    ) AS rn
                FROM prediction_log pl
            )
            WHERE rn = 1
        )
        SELECT
            lp.event_id,
            lp.ts_prediction,
            lp.model_version,
            lp.feature_version,
            lp.abstain,
            lp.signal_5m,
            lp.signal_15m,
            lp.signal_30m,
            lp.signal_60m,
            lp.prob_reject_5m,
            lp.prob_reject_15m,
            lp.prob_reject_30m,
            lp.prob_reject_60m,
            lp.prob_break_5m,
            lp.prob_break_15m,
            lp.prob_break_30m,
            lp.prob_break_60m,
            {quality_flags_expr},
            {regime_policy_mode_expr},
            {trade_regime_expr},
            {selected_policy_expr},
            {regime_policy_json_expr},
            {analog_json_expr},
            {analog_best_reject_expr},
            {analog_best_break_expr},
            {analog_best_n_expr},
            {analog_best_ci_width_expr},
            {analog_best_disagreement_expr},
            te.symbol,
            te.ts_event,
            te.level_type,
            te.touch_side,
            te.regime_type,
            te.rv_regime,
            el.horizon_min,
            el.reject AS actual_reject,
            el.break AS actual_break,
            el.return_bps,
            el.mfe_bps,
            el.mae_bps
        FROM latest_pred lp
        JOIN touch_events te ON te.event_id = lp.event_id
        JOIN event_labels el ON el.event_id = lp.event_id
        WHERE te.ts_event >= ? AND te.ts_event < ?
        {preview_filter}
        ORDER BY te.ts_event ASC
    """
    rows = conn.execute(sql, (start_ms, end_ms)).fetchall()
    return [dict(r) for r in rows]


def fetch_latest_predictions(
    conn: sqlite3.Connection,
    start_ms: int,
    end_ms: int,
    include_preview: bool,
) -> list[dict[str, Any]]:
    pred_cols = {r[1] for r in conn.execute("PRAGMA table_info(prediction_log)").fetchall()}
    has_preview = "is_preview" in pred_cols
    quality_flags_expr = _prediction_log_expr(pred_cols, "quality_flags")
    regime_policy_mode_expr = _prediction_log_expr(pred_cols, "regime_policy_mode")
    trade_regime_expr = _prediction_log_expr(pred_cols, "trade_regime")
    selected_policy_expr = _prediction_log_expr(pred_cols, "selected_policy")
    regime_policy_json_expr = _prediction_log_expr(pred_cols, "regime_policy_json")
    analog_json_expr = _prediction_log_expr(pred_cols, "analog_json")
    analog_best_reject_expr = _prediction_log_expr(pred_cols, "analog_best_reject_prob")
    analog_best_break_expr = _prediction_log_expr(pred_cols, "analog_best_break_prob")
    analog_best_n_expr = _prediction_log_expr(pred_cols, "analog_best_n")
    analog_best_ci_width_expr = _prediction_log_expr(pred_cols, "analog_best_ci_width")
    analog_best_disagreement_expr = _prediction_log_expr(pred_cols, "analog_best_disagreement")
    preview_filter = ""
    if has_preview and not include_preview:
        preview_filter = "AND COALESCE(lp.is_preview, 0) = 0"

    sql = f"""
        WITH latest_pred AS (
            SELECT *
            FROM (
                SELECT
                    pl.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY pl.event_id
                        ORDER BY pl.ts_prediction DESC
                    ) AS rn
                FROM prediction_log pl
            )
            WHERE rn = 1
        )
        SELECT
            lp.event_id,
            lp.ts_prediction,
            lp.model_version,
            lp.feature_version,
            lp.abstain,
            lp.signal_5m,
            lp.signal_15m,
            lp.signal_30m,
            lp.signal_60m,
            lp.prob_reject_5m,
            lp.prob_reject_15m,
            lp.prob_reject_30m,
            lp.prob_reject_60m,
            lp.prob_break_5m,
            lp.prob_break_15m,
            lp.prob_break_30m,
            lp.prob_break_60m,
            {quality_flags_expr},
            {regime_policy_mode_expr},
            {trade_regime_expr},
            {selected_policy_expr},
            {regime_policy_json_expr},
            {analog_json_expr},
            {analog_best_reject_expr},
            {analog_best_break_expr},
            {analog_best_n_expr},
            {analog_best_ci_width_expr},
            {analog_best_disagreement_expr},
            te.symbol,
            te.ts_event,
            te.level_type,
            te.touch_side,
            te.regime_type,
            te.rv_regime
        FROM latest_pred lp
        JOIN touch_events te ON te.event_id = lp.event_id
        WHERE te.ts_event >= ? AND te.ts_event < ?
        {preview_filter}
        ORDER BY te.ts_event ASC
    """
    rows = conn.execute(sql, (start_ms, end_ms)).fetchall()
    return [dict(r) for r in rows]


def compute_regime_summary(predictions: list[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "rv_low": 0,
        "rv_normal": 0,
        "rv_high": 0,
        "trend_up": 0,
        "trend_down": 0,
        "range": 0,
        "vol_expansion": 0,
        "unknown": 0,
    }
    for row in predictions:
        rv = row.get("rv_regime")
        if rv == 1:
            summary["rv_low"] += 1
        elif rv == 2:
            summary["rv_normal"] += 1
        elif rv == 3:
            summary["rv_high"] += 1
        else:
            summary["unknown"] += 1

        regime = row.get("regime_type")
        if regime == 1:
            summary["trend_up"] += 1
        elif regime == 2:
            summary["trend_down"] += 1
        elif regime == 3:
            summary["range"] += 1
        elif regime == 4:
            summary["vol_expansion"] += 1
    return summary


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return []
    return []


def compute_regime_policy_summary(predictions: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "total_predictions": len(predictions),
        "with_payload": 0,
        "mode_counts": {"off": 0, "shadow": 0, "active": 0, "unknown": 0},
        "trade_regime_counts": {"compression": 0, "expansion": 0, "neutral": 0, "unknown": 0},
        "selected_policy_counts": {"baseline": 0, "regime_active": 0, "unknown": 0},
        "atr_zone_counts": {"ultra": 0, "near": 0, "mid": 0, "far": 0, "unknown": 0},
        "atr_overlay_applied_count": 0,
        "atr_overlay_applied_by_regime": {"compression": 0, "expansion": 0, "neutral": 0, "unknown": 0},
        "divergence_count": 0,
        "divergence_by_horizon": {5: 0, 15: 0, 30: 0, 60: 0},
        "divergence_by_atr_zone": {"ultra": 0, "near": 0, "mid": 0, "far": 0, "unknown": 0},
    }
    seen_divergence_events: set[str] = set()
    allowed_modes = {"off", "shadow", "active"}
    allowed_regimes = {"compression", "expansion", "neutral"}
    allowed_policies = {"baseline", "regime_active"}
    allowed_atr_zones = {"ultra", "near", "mid", "far"}

    for row in predictions:
        event_id = str(row.get("event_id") or "")
        mode = str(row.get("regime_policy_mode") or "unknown").strip().lower()
        trade_regime = str(row.get("trade_regime") or "unknown").strip().lower()
        selected_policy = str(row.get("selected_policy") or "unknown").strip().lower()
        mode = mode if mode in allowed_modes else "unknown"
        trade_regime = trade_regime if trade_regime in allowed_regimes else "unknown"
        selected_policy = selected_policy if selected_policy in allowed_policies else "unknown"
        summary["mode_counts"][mode] += 1
        summary["trade_regime_counts"][trade_regime] += 1
        summary["selected_policy_counts"][selected_policy] += 1

        quality_flags = {
            str(flag).strip()
            for flag in _parse_json_list(row.get("quality_flags"))
            if isinstance(flag, (str, int, float))
        }

        policy_payload = _parse_json_object(row.get("regime_policy_json"))
        atr_zone = "unknown"
        if policy_payload:
            summary["with_payload"] += 1
            atr_zone = str(policy_payload.get("atr_zone") or "unknown").strip().lower()
            if atr_zone not in allowed_atr_zones:
                atr_zone = "unknown"
            atr_overlay = policy_payload.get("atr_overlay")
            if isinstance(atr_overlay, dict) and bool(atr_overlay.get("applied")):
                summary["atr_overlay_applied_count"] += 1
                overlay_regime = trade_regime if trade_regime in allowed_regimes else "unknown"
                summary["atr_overlay_applied_by_regime"][overlay_regime] += 1

            signal_diffs = policy_payload.get("signal_diffs")
            if isinstance(signal_diffs, dict):
                row_has_diff = False
                for key, payload in signal_diffs.items():
                    if not isinstance(payload, dict):
                        continue
                    baseline_sig = payload.get("baseline")
                    regime_sig = payload.get("regime")
                    if baseline_sig == regime_sig:
                        continue
                    row_has_diff = True
                    try:
                        horizon = int(str(key).replace("signal_", "").replace("m", ""))
                    except Exception:
                        continue
                    if horizon in summary["divergence_by_horizon"]:
                        summary["divergence_by_horizon"][horizon] += 1
                if row_has_diff and event_id and event_id not in seen_divergence_events:
                    seen_divergence_events.add(event_id)
                    summary["divergence_count"] += 1
                    summary["divergence_by_atr_zone"][atr_zone] += 1

        summary["atr_zone_counts"][atr_zone] += 1

        if (
            "REGIME_POLICY_DIVERGENCE" in quality_flags
            and event_id
            and event_id not in seen_divergence_events
        ):
            seen_divergence_events.add(event_id)
            summary["divergence_count"] += 1
            summary["divergence_by_atr_zone"][atr_zone] += 1

    total = int(summary["total_predictions"] or 0)
    summary["divergence_rate_pct"] = round((summary["divergence_count"] / total) * 100.0, 2) if total > 0 else 0.0
    return summary


def ts_to_et(ts_ms: int | None) -> str:
    if not ts_ms:
        return "--"
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(ET_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def gamma_permission_missing_detected(log_path: Path = DEFAULT_GAMMA_LOG, tail_lines: int = 400) -> bool:
    """Detect IBKR options market-data permission gaps from gamma bridge logs."""
    if not log_path.exists():
        return False
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return False
    tail = lines[-tail_lines:] if tail_lines > 0 else lines
    for line in tail:
        lowered = line.lower()
        if "error 10089" in lowered:
            return True
        if "requested market data requires additional subscription" in lowered:
            return True
    return False


def fetch_gamma_coverage(
    conn: sqlite3.Connection,
    start_ms: int,
    end_ms: int,
) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS events_total,
            SUM(CASE WHEN gamma_mode IS NOT NULL THEN 1 ELSE 0 END) AS gamma_mode_nonnull,
            SUM(CASE WHEN gamma_flip IS NOT NULL THEN 1 ELSE 0 END) AS gamma_flip_nonnull,
            SUM(CASE WHEN gamma_flip_dist_bps IS NOT NULL THEN 1 ELSE 0 END) AS gamma_flip_dist_nonnull
        FROM touch_events
        WHERE ts_event >= ? AND ts_event < ?
        """,
        (start_ms, end_ms),
    ).fetchone()
    if row is None:
        return {
            "events_total": 0,
            "gamma_mode_nonnull": 0,
            "gamma_flip_nonnull": 0,
            "gamma_flip_dist_nonnull": 0,
        }
    return {
        "events_total": int(row["events_total"] or 0),
        "gamma_mode_nonnull": int(row["gamma_mode_nonnull"] or 0),
        "gamma_flip_nonnull": int(row["gamma_flip_nonnull"] or 0),
        "gamma_flip_dist_nonnull": int(row["gamma_flip_dist_nonnull"] or 0),
    }


def resolve_manifest_path() -> Path:
    if RF_MANIFEST_PATH:
        return Path(RF_MANIFEST_PATH)
    active_path = MODEL_DIR / RF_ACTIVE_MANIFEST
    if active_path.exists():
        return active_path
    candidate_path = MODEL_DIR / RF_CANDIDATE_MANIFEST
    if candidate_path.exists():
        return candidate_path
    if candidate_path.name != LEGACY_CANDIDATE_MANIFEST:
        legacy_path = MODEL_DIR / LEGACY_CANDIDATE_MANIFEST
        if legacy_path.exists():
            return legacy_path
    return candidate_path


def parse_manifest() -> dict[str, Any]:
    manifest_path = resolve_manifest_path()
    if not manifest_path.exists():
        return {}
    try:
        with manifest_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}


def _trailing_avg(
    conn: sqlite3.Connection,
    report_date: str,
    horizon: int,
    col: str,
    lookback: int = 20,
) -> float | None:
    row = conn.execute(
        f"""
        SELECT AVG({col}) AS avg_val
        FROM (
            SELECT {col}
            FROM daily_ml_metrics
            WHERE horizon_min = ?
              AND report_date < ?
              AND {col} IS NOT NULL
            ORDER BY report_date DESC
            LIMIT ?
        )
        """,
        (horizon, report_date, lookback),
    ).fetchone()
    if not row or row["avg_val"] is None:
        return None
    return float(row["avg_val"])


def build_horizon_metrics(records: list[dict[str, Any]], horizon: int) -> MetricBundle:
    subset = [r for r in records if int(r["horizon_min"]) == horizon]
    signal_key = f"signal_{horizon}m"
    reject_prob_key = f"prob_reject_{horizon}m"
    break_prob_key = f"prob_break_{horizon}m"

    signal_reject = [r for r in subset if r.get(signal_key) == "reject"]
    signal_break = [r for r in subset if r.get(signal_key) == "break"]
    signal_no_edge = [r for r in subset if r.get(signal_key) == "no_edge"]

    reject_actual: list[int] = []
    reject_prob: list[float] = []
    break_actual: list[int] = []
    break_prob: list[float] = []
    returns: list[float] = []
    mfes: list[float] = []
    maes: list[float] = []

    for r in subset:
        pr = r.get(reject_prob_key)
        pb = r.get(break_prob_key)
        ar = r.get("actual_reject")
        ab = r.get("actual_break")
        if isinstance(pr, (int, float)) and ar in (0, 1):
            reject_prob.append(float(pr))
            reject_actual.append(int(ar))
        if isinstance(pb, (int, float)) and ab in (0, 1):
            break_prob.append(float(pb))
            break_actual.append(int(ab))
        if isinstance(r.get("return_bps"), (int, float)):
            returns.append(float(r["return_bps"]))
        if isinstance(r.get("mfe_bps"), (int, float)):
            mfes.append(float(r["mfe_bps"]))
        if isinstance(r.get("mae_bps"), (int, float)):
            maes.append(float(r["mae_bps"]))

    reject_pred_mask = [r.get(signal_key) == "reject" for r in subset]
    break_pred_mask = [r.get(signal_key) == "break" for r in subset]
    reject_precision, reject_recall = compute_precision_recall(
        [int(r.get("actual_reject") or 0) for r in subset],
        reject_pred_mask,
    )
    break_precision, break_recall = compute_precision_recall(
        [int(r.get("actual_break") or 0) for r in subset],
        break_pred_mask,
    )

    misses: list[dict[str, Any]] = []
    for r in subset:
        signal = r.get(signal_key)
        if signal == "reject":
            conf = r.get(reject_prob_key)
            wrong = r.get("actual_reject") == 0
        elif signal == "break":
            conf = r.get(break_prob_key)
            wrong = r.get("actual_break") == 0
        else:
            continue
        if wrong and isinstance(conf, (int, float)):
            misses.append(
                {
                    "event_id": r.get("event_id"),
                    "symbol": r.get("symbol"),
                    "ts_event": int(r.get("ts_event") or 0),
                    "signal": signal,
                    "confidence": float(conf),
                    "actual_reject": int(r.get("actual_reject") or 0),
                    "actual_break": int(r.get("actual_break") or 0),
                    "return_bps": r.get("return_bps"),
                    "mfe_bps": r.get("mfe_bps"),
                    "mae_bps": r.get("mae_bps"),
                }
            )
    misses.sort(key=lambda m: m["confidence"], reverse=True)

    abstain_rate = None
    if subset:
        abstains = sum(1 for r in subset if int(r.get("abstain") or 0) == 1)
        abstain_rate = abstains / len(subset)

    return MetricBundle(
        horizon=horizon,
        sample_size=len(subset),
        signal_reject_count=len(signal_reject),
        signal_break_count=len(signal_break),
        signal_no_edge_count=len(signal_no_edge),
        abstain_rate=abstain_rate,
        reject_precision=reject_precision,
        reject_recall=reject_recall,
        break_precision=break_precision,
        break_recall=break_recall,
        brier_reject=brier_score(reject_actual, reject_prob),
        brier_break=brier_score(break_actual, break_prob),
        ece_reject=expected_calibration_error(reject_actual, reject_prob),
        ece_break=expected_calibration_error(break_actual, break_prob),
        auc_reject=roc_auc_binary(reject_actual, reject_prob),
        auc_break=roc_auc_binary(break_actual, break_prob),
        avg_return_bps=mean_or_none(returns),
        avg_mfe_bps=mean_or_none(mfes),
        avg_mae_bps=mean_or_none(maes),
        confidence_misses=misses[:5],
    )


def compute_analog_shadow_summaries(
    records: list[dict[str, Any]],
    horizons: list[int],
    eval_mode: str = ANALOG_PROMOTION_EVAL_MODE,
) -> list[AnalogHorizonSummary]:
    def _delta(current: float | None, baseline: float | None) -> float | None:
        if current is None or baseline is None:
            return None
        return current - baseline

    summaries: list[AnalogHorizonSummary] = []
    for horizon in horizons:
        subset = [r for r in records if int(r.get("horizon_min") or 0) == int(horizon)]
        model_reject_key = f"prob_reject_{horizon}m"
        model_break_key = f"prob_break_{horizon}m"

        analog_available = 0
        analog_quality_ok = 0
        neighbor_counts: list[float] = []
        effective_neighbor_counts: list[float] = []
        ci_widths: list[float] = []
        disagreements: list[float] = []
        high_disagreement_model_abs_error: list[float] = []
        low_disagreement_model_abs_error: list[float] = []

        reject_actual: list[int] = []
        reject_actual_blend: list[int] = []
        reject_actual_guard: list[int] = []
        model_reject_prob: list[float] = []
        analog_reject_prob: list[float] = []
        blend_reject_prob: list[float] = []
        model_reject_prob_guard: list[float] = []
        break_actual: list[int] = []
        break_actual_blend: list[int] = []
        break_actual_guard: list[int] = []
        model_break_prob: list[float] = []
        analog_break_prob: list[float] = []
        blend_break_prob: list[float] = []
        model_break_prob_guard: list[float] = []
        guard_reject_eligible = 0
        guard_reject_kept = 0
        guard_break_eligible = 0
        guard_break_kept = 0

        for row in subset:
            payload = _parse_json_object(row.get("analog_json"))
            horizon_payload: dict[str, Any] = {}
            horizon_map = payload.get("horizons")
            if isinstance(horizon_map, dict):
                candidate = horizon_map.get(str(horizon))
                if isinstance(candidate, dict):
                    horizon_payload = candidate
            if not horizon_payload:
                continue

            analog_available += 1
            status = str(horizon_payload.get("status") or "").strip().lower()
            if status == "ok":
                analog_quality_ok += 1

            n_val = horizon_payload.get("n")
            if isinstance(n_val, (int, float)):
                neighbor_counts.append(float(n_val))
            n_eff_val = horizon_payload.get("n_eff")
            if isinstance(n_eff_val, (int, float)):
                effective_neighbor_counts.append(float(n_eff_val))

            reject_ci_width = horizon_payload.get("reject_ci_width")
            break_ci_width = horizon_payload.get("break_ci_width")
            ci_candidates: list[float] = []
            if isinstance(reject_ci_width, (int, float)):
                ci_candidates.append(float(reject_ci_width))
            if isinstance(break_ci_width, (int, float)):
                ci_candidates.append(float(break_ci_width))
            if ci_candidates:
                ci_widths.append(max(ci_candidates))
            ci_width_for_blend = max(ci_candidates) if ci_candidates else None

            model_abs_components: list[float] = []
            pr_model = row.get(model_reject_key)
            pr_analog = _extract_analog_prob(horizon_payload, "reject")
            ar = row.get("actual_reject")
            pr_model_f = float(pr_model) if isinstance(pr_model, (int, float)) else None
            pb_model = row.get(model_break_key)
            pb_model_f = float(pb_model) if isinstance(pb_model, (int, float)) else None
            pb_analog = _extract_analog_prob(horizon_payload, "break")
            n_eff_for_blend = (
                float(horizon_payload.get("n_eff"))
                if isinstance(horizon_payload.get("n_eff"), (int, float))
                else None
            )
            blend_weight = _compute_report_blend_weight(
                n_eff_for_blend,
                ci_width_for_blend,
            )
            blend_reject = horizon_payload.get("blend_prob_reject")
            blend_break = horizon_payload.get("blend_prob_break")
            if not isinstance(blend_reject, (int, float)):
                if (
                    status == "ok"
                    and pr_model_f is not None
                    and pr_analog is not None
                ):
                    blend_reject = (1.0 - blend_weight) * pr_model_f + blend_weight * pr_analog
                else:
                    blend_reject = None
            if not isinstance(blend_break, (int, float)):
                if (
                    status == "ok"
                    and pb_model_f is not None
                    and pb_analog is not None
                ):
                    blend_break = (1.0 - blend_weight) * pb_model_f + blend_weight * pb_analog
                else:
                    blend_break = None

            if pr_model_f is not None and pr_analog is not None and ar in (0, 1):
                model_reject_prob.append(pr_model_f)
                analog_reject_prob.append(pr_analog)
                reject_actual.append(int(ar))
                model_abs_components.append(abs(pr_model_f - int(ar)))
                if isinstance(blend_reject, (int, float)):
                    blend_reject_prob.append(float(blend_reject))
                    reject_actual_blend.append(int(ar))

            ab = row.get("actual_break")
            if pb_model_f is not None and pb_analog is not None and ab in (0, 1):
                model_break_prob.append(pb_model_f)
                analog_break_prob.append(pb_analog)
                break_actual.append(int(ab))
                model_abs_components.append(abs(pb_model_f - int(ab)))
                if isinstance(blend_break, (int, float)):
                    blend_break_prob.append(float(blend_break))
                    break_actual_blend.append(int(ab))

            disagreement_value = horizon_payload.get("disagreement")
            disagreement: float | None = None
            if isinstance(disagreement_value, (int, float)):
                disagreement = float(disagreement_value)
                disagreements.append(disagreement)
                if model_abs_components:
                    if disagreement >= ANALOG_DISAGREEMENT_THRESHOLD:
                        high_disagreement_model_abs_error.append(statistics.fmean(model_abs_components))
                    else:
                        low_disagreement_model_abs_error.append(statistics.fmean(model_abs_components))
            if disagreement is not None and pr_model_f is not None and ar in (0, 1):
                guard_reject_eligible += 1
                if disagreement < ANALOG_DISAGREEMENT_THRESHOLD:
                    guard_reject_kept += 1
                    model_reject_prob_guard.append(pr_model_f)
                    reject_actual_guard.append(int(ar))
            if disagreement is not None and pb_model_f is not None and ab in (0, 1):
                guard_break_eligible += 1
                if disagreement < ANALOG_DISAGREEMENT_THRESHOLD:
                    guard_break_kept += 1
                    model_break_prob_guard.append(pb_model_f)
                    break_actual_guard.append(int(ab))

        model_reject_brier = brier_score(reject_actual, model_reject_prob)
        analog_reject_brier = brier_score(reject_actual, analog_reject_prob)
        model_reject_ece = expected_calibration_error(reject_actual, model_reject_prob)
        analog_reject_ece = expected_calibration_error(reject_actual, analog_reject_prob)
        model_break_brier = brier_score(break_actual, model_break_prob)
        analog_break_brier = brier_score(break_actual, analog_break_prob)
        model_break_ece = expected_calibration_error(break_actual, model_break_prob)
        analog_break_ece = expected_calibration_error(break_actual, analog_break_prob)
        blend_reject_brier = brier_score(reject_actual_blend, blend_reject_prob)
        blend_reject_ece = expected_calibration_error(reject_actual_blend, blend_reject_prob)
        blend_break_brier = brier_score(break_actual_blend, blend_break_prob)
        blend_break_ece = expected_calibration_error(break_actual_blend, blend_break_prob)
        guard_reject_brier = brier_score(reject_actual_guard, model_reject_prob_guard)
        guard_reject_ece = expected_calibration_error(reject_actual_guard, model_reject_prob_guard)
        guard_break_brier = brier_score(break_actual_guard, model_break_prob_guard)
        guard_break_ece = expected_calibration_error(break_actual_guard, model_break_prob_guard)
        guard_reject_keep_rate = (
            (guard_reject_kept / guard_reject_eligible)
            if guard_reject_eligible > 0
            else None
        )
        guard_break_keep_rate = (
            (guard_break_kept / guard_break_eligible)
            if guard_break_eligible > 0
            else None
        )

        summaries.append(
            AnalogHorizonSummary(
                horizon=horizon,
                sample_size=len(subset),
                analog_available_count=analog_available,
                analog_quality_ok_count=analog_quality_ok,
                mean_neighbors=mean_or_none(neighbor_counts),
                mean_effective_neighbors=mean_or_none(effective_neighbor_counts),
                mean_ci_width=mean_or_none(ci_widths),
                mean_disagreement=mean_or_none(disagreements),
                high_disagreement_count=len(high_disagreement_model_abs_error),
                high_disagreement_model_abs_error=mean_or_none(high_disagreement_model_abs_error),
                low_disagreement_model_abs_error=mean_or_none(low_disagreement_model_abs_error),
                model_reject_brier_matched=model_reject_brier,
                analog_reject_brier=analog_reject_brier,
                reject_brier_delta=_delta(analog_reject_brier, model_reject_brier),
                model_reject_ece_matched=model_reject_ece,
                analog_reject_ece=analog_reject_ece,
                reject_ece_delta=_delta(analog_reject_ece, model_reject_ece),
                model_break_brier_matched=model_break_brier,
                analog_break_brier=analog_break_brier,
                break_brier_delta=_delta(analog_break_brier, model_break_brier),
                model_break_ece_matched=model_break_ece,
                analog_break_ece=analog_break_ece,
                break_ece_delta=_delta(analog_break_ece, model_break_ece),
                blend_reject_brier=blend_reject_brier,
                blend_reject_ece=blend_reject_ece,
                blend_break_brier=blend_break_brier,
                blend_break_ece=blend_break_ece,
                reject_brier_delta_blend=_delta(blend_reject_brier, model_reject_brier),
                reject_ece_delta_blend=_delta(blend_reject_ece, model_reject_ece),
                break_brier_delta_blend=_delta(blend_break_brier, model_break_brier),
                break_ece_delta_blend=_delta(blend_break_ece, model_break_ece),
                guard_reject_keep_rate=guard_reject_keep_rate,
                guard_break_keep_rate=guard_break_keep_rate,
                guard_reject_brier=guard_reject_brier,
                guard_reject_ece=guard_reject_ece,
                guard_break_brier=guard_break_brier,
                guard_break_ece=guard_break_ece,
                guard_reject_brier_delta=_delta(guard_reject_brier, model_reject_brier),
                guard_reject_ece_delta=_delta(guard_reject_ece, model_reject_ece),
                guard_break_brier_delta=_delta(guard_break_brier, model_break_brier),
                guard_break_ece_delta=_delta(guard_break_ece, model_break_ece),
            )
        )
    return summaries


def compute_analog_promotion_gate(
    summaries: list[AnalogHorizonSummary],
    horizons: list[int],
    eval_mode: str = ANALOG_PROMOTION_EVAL_MODE,
    lookback_days: int = ANALOG_PROMOTION_LOOKBACK_DAYS,
) -> AnalogPromotionGate:
    eval_mode_norm = (eval_mode or "blend").strip().lower()
    if eval_mode_norm not in {"analog", "blend"}:
        eval_mode_norm = "blend"

    thresholds = {
        "min_available": ANALOG_PROMOTION_MIN_AVAILABLE,
        "min_quality_ok": ANALOG_PROMOTION_MIN_QUALITY_OK,
        "min_effective_n": ANALOG_PROMOTION_MIN_EFFECTIVE_N,
        "max_mean_ci_width": ANALOG_PROMOTION_MAX_MEAN_CI_WIDTH,
        "max_brier_delta": ANALOG_PROMOTION_MAX_BRIER_DELTA,
        "max_ece_delta": ANALOG_PROMOTION_MAX_ECE_DELTA,
        "min_horizons": ANALOG_PROMOTION_MIN_HORIZONS,
        "require_both_targets": ANALOG_PROMOTION_REQUIRE_BOTH_TARGETS,
        "eval_mode": eval_mode_norm,
        "lookback_days": max(1, int(lookback_days)),
    }
    by_horizon = {int(s.horizon): s for s in summaries}
    evaluated: list[int] = []
    passed: list[int] = []
    reasons: list[str] = []
    horizon_results: dict[str, dict[str, Any]] = {}

    for horizon in horizons:
        s = by_horizon.get(int(horizon))
        if s is None:
            horizon_results[str(horizon)] = {
                "evaluated": False,
                "pass": False,
                "reasons": ["missing_summary"],
            }
            continue
        if s.analog_available_count < ANALOG_PROMOTION_MIN_AVAILABLE:
            horizon_results[str(horizon)] = {
                "evaluated": False,
                "pass": False,
                "reasons": ["min_available"],
                "available_count": int(s.analog_available_count),
            }
            continue
        evaluated.append(int(horizon))

        horizon_reasons: list[str] = []
        quality_ok_pass = s.analog_quality_ok_count >= ANALOG_PROMOTION_MIN_QUALITY_OK
        effective_n_pass = (
            s.mean_effective_neighbors is not None
            and s.mean_effective_neighbors >= ANALOG_PROMOTION_MIN_EFFECTIVE_N
        )
        ci_width_pass = (
            s.mean_ci_width is not None and s.mean_ci_width <= ANALOG_PROMOTION_MAX_MEAN_CI_WIDTH
        )
        base_quality_pass = bool(quality_ok_pass and effective_n_pass and ci_width_pass)

        if not quality_ok_pass:
            horizon_reasons.append("quality_ok")
        if not effective_n_pass:
            horizon_reasons.append("effective_n")
        if not ci_width_pass:
            horizon_reasons.append("ci_width")

        reject_brier_delta = s.reject_brier_delta
        reject_ece_delta = s.reject_ece_delta
        break_brier_delta = s.break_brier_delta
        break_ece_delta = s.break_ece_delta
        if eval_mode_norm == "blend":
            reject_brier_delta = (
                s.reject_brier_delta_blend
                if s.reject_brier_delta_blend is not None
                else s.reject_brier_delta
            )
            reject_ece_delta = (
                s.reject_ece_delta_blend
                if s.reject_ece_delta_blend is not None
                else s.reject_ece_delta
            )
            break_brier_delta = (
                s.break_brier_delta_blend
                if s.break_brier_delta_blend is not None
                else s.break_brier_delta
            )
            break_ece_delta = (
                s.break_ece_delta_blend
                if s.break_ece_delta_blend is not None
                else s.break_ece_delta
            )

        reject_checks = [
            reject_brier_delta is not None and reject_brier_delta <= ANALOG_PROMOTION_MAX_BRIER_DELTA,
            reject_ece_delta is not None and reject_ece_delta <= ANALOG_PROMOTION_MAX_ECE_DELTA,
        ]
        break_checks = [
            break_brier_delta is not None and break_brier_delta <= ANALOG_PROMOTION_MAX_BRIER_DELTA,
            break_ece_delta is not None and break_ece_delta <= ANALOG_PROMOTION_MAX_ECE_DELTA,
        ]
        reject_delta_pass = all(reject_checks)
        break_delta_pass = all(break_checks)
        reject_pass = bool(base_quality_pass and reject_delta_pass)
        break_pass = bool(base_quality_pass and break_delta_pass)

        if ANALOG_PROMOTION_REQUIRE_BOTH_TARGETS:
            if not reject_delta_pass:
                horizon_reasons.append("reject_delta")
            if not break_delta_pass:
                horizon_reasons.append("break_delta")
        else:
            if not (reject_delta_pass or break_delta_pass):
                horizon_reasons.append("delta")

        horizon_pass = len(horizon_reasons) == 0
        horizon_results[str(horizon)] = {
            "evaluated": True,
            "pass": bool(horizon_pass),
            "reasons": list(horizon_reasons),
            "quality_ok_pass": bool(quality_ok_pass),
            "effective_n_pass": bool(effective_n_pass),
            "ci_width_pass": bool(ci_width_pass),
            "base_quality_pass": bool(base_quality_pass),
            "reject_pass": bool(reject_pass),
            "break_pass": bool(break_pass),
            "reject_delta_pass": bool(reject_delta_pass),
            "break_delta_pass": bool(break_delta_pass),
            "reject_brier_delta": reject_brier_delta,
            "reject_ece_delta": reject_ece_delta,
            "break_brier_delta": break_brier_delta,
            "break_ece_delta": break_ece_delta,
            "available_count": int(s.analog_available_count),
            "quality_ok_count": int(s.analog_quality_ok_count),
            "mean_effective_n": s.mean_effective_neighbors,
            "mean_ci_width": s.mean_ci_width,
        }

        if not horizon_pass:
            reasons.append(f"{horizon}m:" + ",".join(horizon_reasons))
            continue
        passed.append(int(horizon))

    if len(evaluated) < ANALOG_PROMOTION_MIN_HORIZONS:
        reasons.append("insufficient_evaluated_horizons")
    if len(passed) < ANALOG_PROMOTION_MIN_HORIZONS:
        reasons.append("insufficient_passed_horizons")
    status = (
        "pass"
        if len(evaluated) >= ANALOG_PROMOTION_MIN_HORIZONS
        and len(passed) >= ANALOG_PROMOTION_MIN_HORIZONS
        else "fail"
    )
    return AnalogPromotionGate(
        status=status,
        passed_horizons=sorted(passed),
        evaluated_horizons=sorted(evaluated),
        required_horizons=ANALOG_PROMOTION_MIN_HORIZONS,
        reasons=reasons,
        thresholds=thresholds,
        horizon_results=horizon_results,
    )


def persist_daily_metrics(
    conn: sqlite3.Connection,
    report_date: str,
    regime_summary: dict[str, int],
    bundles: list[MetricBundle],
) -> None:
    now_ms = int(time.time() * 1000)
    for b in bundles:
        conn.execute(
            """
            INSERT INTO daily_ml_metrics (
                report_date,
                horizon_min,
                sample_size,
                signal_reject_count,
                signal_break_count,
                signal_no_edge_count,
                abstain_rate,
                reject_precision,
                reject_recall,
                break_precision,
                break_recall,
                brier_reject,
                brier_break,
                ece_reject,
                ece_break,
                auc_reject,
                auc_break,
                avg_return_bps,
                avg_mfe_bps,
                avg_mae_bps,
                regime_low_count,
                regime_normal_count,
                regime_high_count,
                regime_up_count,
                regime_down_count,
                regime_range_count,
                regime_vol_exp_count,
                created_at,
                updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(report_date, horizon_min) DO UPDATE SET
                sample_size = excluded.sample_size,
                signal_reject_count = excluded.signal_reject_count,
                signal_break_count = excluded.signal_break_count,
                signal_no_edge_count = excluded.signal_no_edge_count,
                abstain_rate = excluded.abstain_rate,
                reject_precision = excluded.reject_precision,
                reject_recall = excluded.reject_recall,
                break_precision = excluded.break_precision,
                break_recall = excluded.break_recall,
                brier_reject = excluded.brier_reject,
                brier_break = excluded.brier_break,
                ece_reject = excluded.ece_reject,
                ece_break = excluded.ece_break,
                auc_reject = excluded.auc_reject,
                auc_break = excluded.auc_break,
                avg_return_bps = excluded.avg_return_bps,
                avg_mfe_bps = excluded.avg_mfe_bps,
                avg_mae_bps = excluded.avg_mae_bps,
                regime_low_count = excluded.regime_low_count,
                regime_normal_count = excluded.regime_normal_count,
                regime_high_count = excluded.regime_high_count,
                regime_up_count = excluded.regime_up_count,
                regime_down_count = excluded.regime_down_count,
                regime_range_count = excluded.regime_range_count,
                regime_vol_exp_count = excluded.regime_vol_exp_count,
                updated_at = excluded.updated_at
            """,
            (
                report_date,
                b.horizon,
                b.sample_size,
                b.signal_reject_count,
                b.signal_break_count,
                b.signal_no_edge_count,
                safe_round(b.abstain_rate),
                safe_round(b.reject_precision),
                safe_round(b.reject_recall),
                safe_round(b.break_precision),
                safe_round(b.break_recall),
                safe_round(b.brier_reject),
                safe_round(b.brier_break),
                safe_round(b.ece_reject),
                safe_round(b.ece_break),
                safe_round(b.auc_reject),
                safe_round(b.auc_break),
                safe_round(b.avg_return_bps, 3),
                safe_round(b.avg_mfe_bps, 3),
                safe_round(b.avg_mae_bps, 3),
                regime_summary["rv_low"],
                regime_summary["rv_normal"],
                regime_summary["rv_high"],
                regime_summary["trend_up"],
                regime_summary["trend_down"],
                regime_summary["range"],
                regime_summary["vol_expansion"],
                now_ms,
                now_ms,
            ),
        )
    conn.commit()


def pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return (part / total) * 100.0


def compute_session_staleness_hours(start_ms: int | float | None, end_ms: int | float | None) -> float | None:
    if start_ms is None or end_ms is None:
        return None
    try:
        start_ts = float(start_ms)
        end_ts = float(end_ms)
    except (TypeError, ValueError):
        return None
    if end_ts <= start_ts:
        return 0.0

    start_dt = datetime.fromtimestamp(start_ts / 1000.0, tz=timezone.utc).astimezone(ET_TZ)
    end_dt = datetime.fromtimestamp(end_ts / 1000.0, tz=timezone.utc).astimezone(ET_TZ)

    total_seconds = 0.0
    day = start_dt.date()
    while day <= end_dt.date():
        if day.weekday() < 5 and day not in NYSE_HOLIDAYS:
            session_start = datetime.combine(day, REGULAR_SESSION_OPEN_ET, tzinfo=ET_TZ)
            session_end = datetime.combine(day, REGULAR_SESSION_CLOSE_ET, tzinfo=ET_TZ)
            segment_start = max(session_start, start_dt)
            segment_end = min(session_end, end_dt)
            if segment_end > segment_start:
                total_seconds += (segment_end - segment_start).total_seconds()
        day += timedelta(days=1)

    return total_seconds / 3600.0


def determine_health_status(
    bundles: list[MetricBundle],
    wall_stale_hours: float | None,
    session_stale_hours: float | None,
) -> tuple[str, list[str]]:
    risk_notes: list[str] = []
    info_notes: list[str] = []

    # Gate kill-switch on regular-session elapsed time (prevents weekend/holiday false positives).
    if session_stale_hours is not None:
        if session_stale_hours >= SESSION_STALE_KILL_HOURS:
            return (
                "kill-switch",
                [f"Model session staleness {session_stale_hours:.1f}h exceeds {SESSION_STALE_KILL_HOURS:.1f}h"],
            )
        if session_stale_hours >= SESSION_STALE_WARN_HOURS:
            risk_notes.append(
                f"Model session staleness elevated ({session_stale_hours:.1f}h >= {SESSION_STALE_WARN_HOURS:.1f}h)"
            )
        if wall_stale_hours is not None and wall_stale_hours >= 72:
            info_notes.append(
                f"Wall-clock staleness {wall_stale_hours:.1f}h (gated by session staleness {session_stale_hours:.1f}h)"
            )
    else:
        # Fallback to previous behavior if session staleness cannot be computed.
        if wall_stale_hours is not None and wall_stale_hours >= 72:
            return ("kill-switch", [f"Model staleness {wall_stale_hours:.1f}h exceeds 72h"])
        if wall_stale_hours is not None and wall_stale_hours >= 48:
            risk_notes.append(f"Model is stale ({wall_stale_hours:.1f}h)")

    active = [b for b in bundles if b.sample_size >= 30]
    if not active:
        risk_notes.append("Low matured sample count (<30 per horizon)")
        return ("degrading", risk_notes + info_notes)

    for b in active:
        if b.ece_reject is not None and b.ece_reject > 0.20:
            risk_notes.append(f"{b.horizon}m reject ECE elevated ({b.ece_reject:.3f})")
        if b.ece_break is not None and b.ece_break > 0.20:
            risk_notes.append(f"{b.horizon}m break ECE elevated ({b.ece_break:.3f})")
        if b.reject_precision is not None and b.signal_reject_count >= 20 and b.reject_precision < 0.35:
            risk_notes.append(f"{b.horizon}m reject precision low ({b.reject_precision:.3f})")
        if b.break_precision is not None and b.signal_break_count >= 20 and b.break_precision < 0.35:
            risk_notes.append(f"{b.horizon}m break precision low ({b.break_precision:.3f})")

    if any("precision low" in n for n in risk_notes) and len(risk_notes) >= 2:
        return ("kill-switch", risk_notes + info_notes)
    if risk_notes:
        return ("degrading", risk_notes + info_notes)

    healthy_notes = ["All monitored thresholds within expected ranges"]
    if info_notes:
        healthy_notes.extend(info_notes)
    return ("healthy", healthy_notes)


def format_metric(v: float | None, digits: int = 3) -> str:
    if v is None:
        return "--"
    return f"{v:.{digits}f}"


def render_report(
    report_day: date,
    start_ms: int,
    end_ms: int,
    predictions: list[dict[str, Any]],
    labeled_records: list[dict[str, Any]],
    bundles: list[MetricBundle],
    analog_summaries: list[AnalogHorizonSummary],
    analog_gate: AnalogPromotionGate,
    analog_gate_eval_mode: str,
    analog_gate_lookback_days: int,
    analog_gate_start_ms: int,
    analog_gate_end_ms: int,
    regime_summary: dict[str, int],
    regime_policy_summary: dict[str, Any],
    manifest: dict[str, Any],
    conn: sqlite3.Connection,
) -> str:
    generated_at = datetime.now(ET_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    model_version = manifest.get("version", "--")
    feature_version = manifest.get("feature_version", "--")
    trained_end_ts = manifest.get("trained_end_ts")
    trained_end = ts_to_et(trained_end_ts)
    stale_hours_wall = None
    now_ms = time.time() * 1000
    if isinstance(trained_end_ts, (int, float)):
        stale_hours_wall = (now_ms - float(trained_end_ts)) / (3600 * 1000)
    stale_hours_session = compute_session_staleness_hours(trained_end_ts, now_ms)
    gamma_permission_missing = gamma_permission_missing_detected()
    gamma_coverage = fetch_gamma_coverage(conn, start_ms, end_ms)

    health, health_notes = determine_health_status(
        bundles=bundles,
        wall_stale_hours=stale_hours_wall,
        session_stale_hours=stale_hours_session,
    )
    report_date_str = report_day.strftime("%Y-%m-%d")

    total_preds = len(predictions)
    total_labeled = len(labeled_records)
    total_events = len({p.get("event_id") for p in predictions if p.get("event_id")})

    lines: list[str] = []
    lines.append(f"# Daily ML Report - {report_date_str}")
    lines.append("")
    lines.append(f"- Generated: {generated_at}")
    lines.append(f"- Window (ET): {ts_to_et(start_ms)} -> {ts_to_et(end_ms)}")
    lines.append(f"- Model: `{model_version}` (feature `{feature_version}`)")
    lines.append(f"- Trained End: {trained_end}")
    lines.append(
        f"- Model Staleness: "
        f"{f'{stale_hours_session:.1f}h' if stale_hours_session is not None else '--'} "
        f"(regular session hours)"
    )
    lines.append(
        f"- Wall-Clock Staleness: {f'{stale_hours_wall:.1f}h' if stale_hours_wall is not None else '--'}"
    )
    lines.append(f"- Health State: **{health.upper()}**")
    if SHADOW_HORIZONS:
        shadow = ", ".join(f"{h}m" for h in sorted(SHADOW_HORIZONS))
        lines.append(f"- Shadow Horizons: {shadow} (scored/reported, excluded from best-horizon selection)")
    lines.append("")
    lines.append("## Headline Counts")
    lines.append("")
    lines.append(f"- Scored predictions (latest per event): {total_preds}")
    lines.append(f"- Unique events scored: {total_events}")
    lines.append(f"- Labeled prediction rows (matured horizons): {total_labeled}")
    lines.append("")
    lines.append("## Regime Summary")
    lines.append("")
    lines.append(f"- RV Regime: low={regime_summary['rv_low']}, normal={regime_summary['rv_normal']}, high={regime_summary['rv_high']}, unknown={regime_summary['unknown']}")
    lines.append(f"- Day Regime: trend_up={regime_summary['trend_up']}, trend_down={regime_summary['trend_down']}, range={regime_summary['range']}, vol_expansion={regime_summary['vol_expansion']}")
    lines.append("")

    lines.append("## Regime Policy")
    lines.append("")
    lines.append(
        f"- Policy modes: off={regime_policy_summary['mode_counts']['off']}, "
        f"shadow={regime_policy_summary['mode_counts']['shadow']}, "
        f"active={regime_policy_summary['mode_counts']['active']}, "
        f"unknown={regime_policy_summary['mode_counts']['unknown']}"
    )
    lines.append(
        f"- Trade regime buckets: compression={regime_policy_summary['trade_regime_counts']['compression']}, "
        f"expansion={regime_policy_summary['trade_regime_counts']['expansion']}, "
        f"neutral={regime_policy_summary['trade_regime_counts']['neutral']}, "
        f"unknown={regime_policy_summary['trade_regime_counts']['unknown']}"
    )
    atr_zone_counts = regime_policy_summary.get("atr_zone_counts", {})
    lines.append(
        f"- ATR distance zones: ultra={atr_zone_counts.get('ultra', 0)}, "
        f"near={atr_zone_counts.get('near', 0)}, mid={atr_zone_counts.get('mid', 0)}, "
        f"far={atr_zone_counts.get('far', 0)}, unknown={atr_zone_counts.get('unknown', 0)}"
    )
    lines.append(
        f"- Selected policy: baseline={regime_policy_summary['selected_policy_counts']['baseline']}, "
        f"regime_active={regime_policy_summary['selected_policy_counts']['regime_active']}, "
        f"unknown={regime_policy_summary['selected_policy_counts']['unknown']}"
    )
    atr_overlay_by_regime = regime_policy_summary.get("atr_overlay_applied_by_regime", {})
    lines.append(
        f"- ATR overlays applied: {regime_policy_summary.get('atr_overlay_applied_count', 0)} "
        f"(compression={atr_overlay_by_regime.get('compression', 0)}, "
        f"expansion={atr_overlay_by_regime.get('expansion', 0)}, "
        f"neutral={atr_overlay_by_regime.get('neutral', 0)}, "
        f"unknown={atr_overlay_by_regime.get('unknown', 0)})"
    )
    lines.append(
        f"- Regime payload attached: {regime_policy_summary['with_payload']} / {regime_policy_summary['total_predictions']}"
    )
    lines.append(
        f"- Shadow divergences: {regime_policy_summary['divergence_count']} "
        f"({regime_policy_summary['divergence_rate_pct']:.2f}%)"
    )
    div_h = regime_policy_summary.get("divergence_by_horizon", {})
    lines.append(
        f"- Divergences by horizon: "
        f"5m={div_h.get(5, 0)}, 15m={div_h.get(15, 0)}, 30m={div_h.get(30, 0)}, 60m={div_h.get(60, 0)}"
    )
    div_zone = regime_policy_summary.get("divergence_by_atr_zone", {})
    lines.append(
        f"- Divergences by ATR zone: "
        f"ultra={div_zone.get('ultra', 0)}, near={div_zone.get('near', 0)}, "
        f"mid={div_zone.get('mid', 0)}, far={div_zone.get('far', 0)}, "
        f"unknown={div_zone.get('unknown', 0)}"
    )
    lines.append("")

    lines.append("## Analog Shadow Evaluation")
    lines.append("")
    lines.append(
        f"- Disagreement threshold: `{ANALOG_DISAGREEMENT_THRESHOLD:.2f}` "
        "(from `ML_ANALOG_DISAGREEMENT_FLAG`)"
    )
    lines.append(
        f"- Promotion eval mode: `{analog_gate_eval_mode}` "
        "(gate compares model vs this probability series)"
    )
    lines.append(
        f"- Promotion eval window (ET): {ts_to_et(analog_gate_start_ms)} -> {ts_to_et(analog_gate_end_ms)} "
        f"({analog_gate_lookback_days}d lookback)"
    )
    lines.append(
        f"- Promotion gate: **{analog_gate.status.upper()}** "
        f"(required horizons={analog_gate.required_horizons}, "
        f"evaluated={len(analog_gate.evaluated_horizons)}, passed={len(analog_gate.passed_horizons)})"
    )
    if analog_gate.evaluated_horizons:
        lines.append(
            f"- Gate horizons evaluated: {', '.join(f'{h}m' for h in analog_gate.evaluated_horizons)}"
        )
    if analog_gate.passed_horizons:
        lines.append(
            f"- Gate horizons passed: {', '.join(f'{h}m' for h in analog_gate.passed_horizons)}"
        )
    gate_target_pass_lines: list[str] = []
    for horizon in analog_gate.evaluated_horizons:
        payload = analog_gate.horizon_results.get(str(horizon), {})
        if not isinstance(payload, dict):
            continue
        reject_ok = payload.get("reject_pass")
        break_ok = payload.get("break_pass")
        if isinstance(reject_ok, bool) and isinstance(break_ok, bool):
            gate_target_pass_lines.append(
                f"{horizon}m:R={'pass' if reject_ok else 'fail'},B={'pass' if break_ok else 'fail'}"
            )
    if gate_target_pass_lines:
        lines.append(f"- Gate target passes: {', '.join(gate_target_pass_lines)}")
    if analog_gate.reasons:
        lines.append(f"- Gate reasons: {', '.join(analog_gate.reasons)}")
    if not any(s.analog_available_count > 0 for s in analog_summaries):
        lines.append("- No analog payloads observed in the promotion-eval window.")
        lines.append("")
    else:
        lines.append(
            "| Horizon | Labeled N (Gate Window) | Analog Rows | Quality OK | "
            "Mean N_eff | Mean CI Width | Mean Disagreement |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        for s in analog_summaries:
            lines.append(
                f"| {s.horizon}m | {s.sample_size} | "
                f"{s.analog_available_count} ({pct(s.analog_available_count, s.sample_size):.1f}%) | "
                f"{s.analog_quality_ok_count} ({pct(s.analog_quality_ok_count, s.analog_available_count):.1f}%) | "
                f"{format_metric(s.mean_effective_neighbors, 2)} | "
                f"{format_metric(s.mean_ci_width, 3)} | "
                f"{format_metric(s.mean_disagreement, 3)} |"
            )
        lines.append("")

        def _fmt_delta(v: float | None, digits: int = 3) -> str:
            if v is None:
                return "--"
            return f"{v:+.{digits}f}"

        lines.append(
            "| Horizon | Brier R (Model) | Brier R (Analog) | Δ R (A-M) | "
            "Brier R (Blend) | Δ R (B-M) | ECE R (Model) | ECE R (Analog) | Δ R (A-M) | "
            "ECE R (Blend) | Δ R (B-M) |"
        )
        lines.append(
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
        )
        for s in analog_summaries:
            lines.append(
                f"| {s.horizon}m | "
                f"{format_metric(s.model_reject_brier_matched)} | {format_metric(s.analog_reject_brier)} | {_fmt_delta(s.reject_brier_delta)} | "
                f"{format_metric(s.blend_reject_brier)} | {_fmt_delta(s.reject_brier_delta_blend)} | "
                f"{format_metric(s.model_reject_ece_matched)} | {format_metric(s.analog_reject_ece)} | {_fmt_delta(s.reject_ece_delta)} | "
                f"{format_metric(s.blend_reject_ece)} | {_fmt_delta(s.reject_ece_delta_blend)} |"
            )
        lines.append("")
        lines.append(
            "| Horizon | Brier B (Model) | Brier B (Analog) | Δ B (A-M) | "
            "Brier B (Blend) | Δ B (B-M) | ECE B (Model) | ECE B (Analog) | Δ B (A-M) | "
            "ECE B (Blend) | Δ B (B-M) |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for s in analog_summaries:
            lines.append(
                f"| {s.horizon}m | "
                f"{format_metric(s.model_break_brier_matched)} | {format_metric(s.analog_break_brier)} | {_fmt_delta(s.break_brier_delta)} | "
                f"{format_metric(s.blend_break_brier)} | {_fmt_delta(s.break_brier_delta_blend)} | "
                f"{format_metric(s.model_break_ece_matched)} | {format_metric(s.analog_break_ece)} | {_fmt_delta(s.break_ece_delta)} | "
                f"{format_metric(s.blend_break_ece)} | {_fmt_delta(s.break_ece_delta_blend)} |"
            )
        lines.append("")
        lines.append(
            f"- Disagreement guard what-if: keep only rows with disagreement < {ANALOG_DISAGREEMENT_THRESHOLD:.2f}"
        )
        lines.append(
            "| Horizon | Keep Rate R | Brier R (Guard) | Δ R (G-M) | ECE R (Guard) | Δ R (G-M) | "
            "Keep Rate B | Brier B (Guard) | Δ B (G-M) | ECE B (Guard) | Δ B (G-M) |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for s in analog_summaries:
            lines.append(
                f"| {s.horizon}m | "
                f"{format_metric(s.guard_reject_keep_rate, 3)} | {format_metric(s.guard_reject_brier)} | {_fmt_delta(s.guard_reject_brier_delta)} | "
                f"{format_metric(s.guard_reject_ece)} | {_fmt_delta(s.guard_reject_ece_delta)} | "
                f"{format_metric(s.guard_break_keep_rate, 3)} | {format_metric(s.guard_break_brier)} | {_fmt_delta(s.guard_break_brier_delta)} | "
                f"{format_metric(s.guard_break_ece)} | {_fmt_delta(s.guard_break_ece_delta)} |"
            )
        lines.append("")
        for s in analog_summaries:
            if s.high_disagreement_count <= 0:
                continue
            if (
                s.high_disagreement_model_abs_error is None
                or s.low_disagreement_model_abs_error is None
            ):
                continue
            lines.append(
                f"- {s.horizon}m model abs-error by disagreement: "
                f"high={s.high_disagreement_model_abs_error:.3f}, "
                f"low={s.low_disagreement_model_abs_error:.3f}"
            )
        lines.append("")

    lines.append("## Gamma Coverage")
    lines.append("")
    lines.append(
        f"- Touch events in window: {gamma_coverage['events_total']}"
    )
    lines.append(
        f"- Gamma-populated events: mode={gamma_coverage['gamma_mode_nonnull']}, "
        f"flip={gamma_coverage['gamma_flip_nonnull']}, flip_dist={gamma_coverage['gamma_flip_dist_nonnull']}"
    )
    if gamma_permission_missing:
        lines.append(
            "- IBKR options market-data permission issue detected (`Error 10089`); "
            "gamma enrichment may be unavailable."
        )
    lines.append("")

    lines.append("## Horizon Metrics")
    lines.append("")
    lines.append("| Horizon | N | Reject Sig | Break Sig | No Edge | Abstain | Brier R | Brier B | ECE R | ECE B | AUC R | AUC B | Prec R | Prec B | Recall R | Recall B | Avg MFE | Avg MAE |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for b in bundles:
        lines.append(
            "| "
            f"{b.horizon}m | {b.sample_size} | {b.signal_reject_count} | {b.signal_break_count} | {b.signal_no_edge_count} | {format_metric(b.abstain_rate)} | "
            f"{format_metric(b.brier_reject)} | {format_metric(b.brier_break)} | {format_metric(b.ece_reject)} | {format_metric(b.ece_break)} | "
            f"{format_metric(b.auc_reject)} | {format_metric(b.auc_break)} | "
            f"{format_metric(b.reject_precision)} | {format_metric(b.break_precision)} | {format_metric(b.reject_recall)} | {format_metric(b.break_recall)} | "
            f"{format_metric(b.avg_mfe_bps, 2)} | {format_metric(b.avg_mae_bps, 2)} |"
        )
    lines.append("")

    lines.append("## Calibration Drift vs Trailing 20 Reports")
    lines.append("")
    lines.append("| Horizon | Brier R Δ | Brier B Δ | ECE R Δ | ECE B Δ | Avg MFE Δ | Avg MAE Δ |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for b in bundles:
        br_base = _trailing_avg(conn, report_date_str, b.horizon, "brier_reject")
        bb_base = _trailing_avg(conn, report_date_str, b.horizon, "brier_break")
        er_base = _trailing_avg(conn, report_date_str, b.horizon, "ece_reject")
        eb_base = _trailing_avg(conn, report_date_str, b.horizon, "ece_break")
        mfe_base = _trailing_avg(conn, report_date_str, b.horizon, "avg_mfe_bps")
        mae_base = _trailing_avg(conn, report_date_str, b.horizon, "avg_mae_bps")

        def delta(current: float | None, base: float | None, digits: int = 3) -> str:
            if current is None or base is None:
                return "--"
            return f"{(current - base):+.{digits}f}"

        lines.append(
            f"| {b.horizon}m | {delta(b.brier_reject, br_base)} | {delta(b.brier_break, bb_base)} | "
            f"{delta(b.ece_reject, er_base)} | {delta(b.ece_break, eb_base)} | "
            f"{delta(b.avg_mfe_bps, mfe_base, 2)} | {delta(b.avg_mae_bps, mae_base, 2)} |"
        )
    lines.append("")

    lines.append("## Biggest Misses (High-Confidence Wrong Calls)")
    lines.append("")
    for b in bundles:
        lines.append(f"### {b.horizon}m")
        if not b.confidence_misses:
            lines.append("- None")
            lines.append("")
            continue
        lines.append("| Time (ET) | Event | Symbol | Signal | Confidence | Actual Reject | Actual Break | Return bps | MFE bps | MAE bps |")
        lines.append("|---|---|---|---|---:|---:|---:|---:|---:|---:|")
        for miss in b.confidence_misses:
            lines.append(
                "| "
                f"{ts_to_et(miss['ts_event'])} | `{miss['event_id']}` | {miss['symbol']} | {miss['signal']} | "
                f"{miss['confidence']:.3f} | {miss['actual_reject']} | {miss['actual_break']} | "
                f"{format_metric(miss.get('return_bps'), 2)} | {format_metric(miss.get('mfe_bps'), 2)} | {format_metric(miss.get('mae_bps'), 2)} |"
            )
        lines.append("")

    lines.append("## Health Notes")
    lines.append("")
    for note in health_notes:
        lines.append(f"- {note}")
    if (
        gamma_permission_missing
        and gamma_coverage["events_total"] > 0
        and gamma_coverage["gamma_mode_nonnull"] == 0
    ):
        lines.append(
            "- Gamma features are currently absent due to IBKR market-data permissions "
            "(Error 10089 in `logs/gamma_bridge.log`)."
        )
    lines.append("")
    lines.append("## Action Checklist")
    lines.append("")
    lines.append("- If health is `KILL-SWITCH`, disable live execution and review the misses section first.")
    lines.append("- If health is `DEGRADING`, check calibration drift and session staleness before next session.")
    lines.append("- Verify `run_retrain_cycle.sh` completed and `/reload` succeeded in `logs/retrain.log`.")
    if gamma_permission_missing:
        lines.append(
            "- Resolve IBKR options market-data API permissions to restore gamma enrichment "
            "(bridge currently reports Error 10089)."
        )
    if (
        regime_policy_summary["mode_counts"]["shadow"] > 0
        and regime_policy_summary["divergence_count"] == 0
    ):
        lines.append(
            "- Regime policy is in shadow mode but no divergences were observed; verify regime inputs are populated."
        )
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily ML quality report.")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path")
    parser.add_argument("--report-date", default=None, help="Report date in YYYY-MM-DD (ET)")
    parser.add_argument("--out-dir", default=DEFAULT_REPORT_DIR, help="Output directory for markdown reports")
    parser.add_argument("--include-preview", action="store_true", default=False, help="Include preview predictions")
    parser.add_argument(
        "--analog-gate-lookback-days",
        type=int,
        default=ANALOG_PROMOTION_LOOKBACK_DAYS,
        help=(
            "Lookback window (days) for analog promotion-gate evaluation "
            "(default: ML_ANALOG_PROMOTION_LOOKBACK_DAYS or 5)"
        ),
    )
    parser.add_argument(
        "--analog-gate-eval-mode",
        default=ANALOG_PROMOTION_EVAL_MODE,
        choices=("analog", "blend"),
        help=(
            "Use analog-only or blended probabilities when computing promotion deltas "
            "(default: ML_ANALOG_PROMOTION_EVAL_MODE or blend)"
        ),
    )
    parser.add_argument("--print-path", action="store_true", default=True, help="Print output report path")
    args = parser.parse_args()

    conn = connect(args.db)
    try:
        if migrate_connection is not None:
            migrate_connection(conn, verbose=False)

        if not table_exists(conn, "prediction_log"):
            print("prediction_log table missing. Run scoring first.", file=sys.stderr)
            sys.exit(1)
        if not table_exists(conn, "event_labels"):
            print("event_labels table missing. Run build_labels first.", file=sys.stderr)
            sys.exit(1)
        if not table_exists(conn, "touch_events"):
            print("touch_events table missing.", file=sys.stderr)
            sys.exit(1)

        ensure_daily_metrics_schema(conn)

        report_day = parse_report_date(args.report_date)
        start_ms, end_ms = day_bounds_ms(report_day)
        gate_lookback_days = max(1, int(args.analog_gate_lookback_days))
        gate_start_ms = start_ms - ((gate_lookback_days - 1) * 86_400_000)
        gate_eval_mode = str(args.analog_gate_eval_mode or ANALOG_PROMOTION_EVAL_MODE).strip().lower()
        if gate_eval_mode not in {"analog", "blend"}:
            gate_eval_mode = ANALOG_PROMOTION_EVAL_MODE
        predictions = fetch_latest_predictions(conn, start_ms, end_ms, args.include_preview)
        labeled_records = fetch_labeled_records(conn, start_ms, end_ms, args.include_preview)
        labeled_records_gate = fetch_labeled_records(conn, gate_start_ms, end_ms, args.include_preview)

        horizons = REPORT_HORIZONS or [5, 15, 30, 60]
        bundles = [build_horizon_metrics(labeled_records, h) for h in horizons]
        analog_summaries = compute_analog_shadow_summaries(
            labeled_records_gate,
            horizons,
            eval_mode=gate_eval_mode,
        )
        analog_gate = compute_analog_promotion_gate(
            analog_summaries,
            horizons,
            eval_mode=gate_eval_mode,
            lookback_days=gate_lookback_days,
        )
        regime_summary = compute_regime_summary(predictions)
        regime_policy_summary = compute_regime_policy_summary(predictions)

        persist_daily_metrics(conn, report_day.strftime("%Y-%m-%d"), regime_summary, bundles)
        manifest = parse_manifest()
        content = render_report(
            report_day=report_day,
            start_ms=start_ms,
            end_ms=end_ms,
            predictions=predictions,
            labeled_records=labeled_records,
            bundles=bundles,
            analog_summaries=analog_summaries,
            analog_gate=analog_gate,
            analog_gate_eval_mode=gate_eval_mode,
            analog_gate_lookback_days=gate_lookback_days,
            analog_gate_start_ms=gate_start_ms,
            analog_gate_end_ms=end_ms,
            regime_summary=regime_summary,
            regime_policy_summary=regime_policy_summary,
            manifest=manifest,
            conn=conn,
        )

        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / f"ml_daily_{report_day.strftime('%Y-%m-%d')}.md"
        latest_path = out_dir / "ml_daily_latest.md"
        gate_payload = {
            "report_date": report_day.strftime("%Y-%m-%d"),
            "status": analog_gate.status,
            "required_horizons": analog_gate.required_horizons,
            "evaluated_horizons": analog_gate.evaluated_horizons,
            "passed_horizons": analog_gate.passed_horizons,
            "horizon_results": analog_gate.horizon_results,
            "reasons": analog_gate.reasons,
            "thresholds": analog_gate.thresholds,
            "eval_mode": gate_eval_mode,
            "lookback_days": gate_lookback_days,
            "window_start_ms": gate_start_ms,
            "window_end_ms": end_ms,
            "generated_at_ms": int(time.time() * 1000),
        }
        gate_path = out_dir / f"analog_promotion_gate_{report_day.strftime('%Y-%m-%d')}.json"
        gate_latest_path = out_dir / "analog_promotion_gate_latest.json"
        report_path.write_text(content, encoding="utf-8")
        latest_path.write_text(content, encoding="utf-8")
        gate_path.write_text(json.dumps(gate_payload, indent=2), encoding="utf-8")
        gate_latest_path.write_text(json.dumps(gate_payload, indent=2), encoding="utf-8")

        if args.print_path:
            print(str(report_path))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
