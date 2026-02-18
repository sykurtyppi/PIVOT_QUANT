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
from dataclasses import dataclass
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
RF_CANDIDATE_MANIFEST = os.getenv("RF_CANDIDATE_MANIFEST", "manifest_latest.json").strip() or "manifest_latest.json"

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


def fetch_labeled_records(
    conn: sqlite3.Connection,
    start_ms: int,
    end_ms: int,
    include_preview: bool,
) -> list[dict[str, Any]]:
    pred_cols = {r[1] for r in conn.execute("PRAGMA table_info(prediction_log)").fetchall()}
    has_preview = "is_preview" in pred_cols
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
            lp.signal_60m,
            lp.prob_reject_5m,
            lp.prob_reject_15m,
            lp.prob_reject_60m,
            lp.prob_break_5m,
            lp.prob_break_15m,
            lp.prob_break_60m,
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
            lp.signal_60m,
            lp.prob_reject_5m,
            lp.prob_reject_15m,
            lp.prob_reject_60m,
            lp.prob_break_5m,
            lp.prob_break_15m,
            lp.prob_break_60m,
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
    return MODEL_DIR / RF_CANDIDATE_MANIFEST


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
    regime_summary: dict[str, int],
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
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily ML quality report.")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path")
    parser.add_argument("--report-date", default=None, help="Report date in YYYY-MM-DD (ET)")
    parser.add_argument("--out-dir", default=DEFAULT_REPORT_DIR, help="Output directory for markdown reports")
    parser.add_argument("--include-preview", action="store_true", default=False, help="Include preview predictions")
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
        predictions = fetch_latest_predictions(conn, start_ms, end_ms, args.include_preview)
        labeled_records = fetch_labeled_records(conn, start_ms, end_ms, args.include_preview)

        bundles = [build_horizon_metrics(labeled_records, h) for h in (5, 15, 60)]
        regime_summary = compute_regime_summary(predictions)

        persist_daily_metrics(conn, report_day.strftime("%Y-%m-%d"), regime_summary, bundles)
        manifest = parse_manifest()
        content = render_report(
            report_day=report_day,
            start_ms=start_ms,
            end_ms=end_ms,
            predictions=predictions,
            labeled_records=labeled_records,
            bundles=bundles,
            regime_summary=regime_summary,
            manifest=manifest,
            conn=conn,
        )

        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / f"ml_daily_{report_day.strftime('%Y-%m-%d')}.md"
        latest_path = out_dir / "ml_daily_latest.md"
        report_path.write_text(content, encoding="utf-8")
        latest_path.write_text(content, encoding="utf-8")

        if args.print_path:
            print(str(report_path))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
