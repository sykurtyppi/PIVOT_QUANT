#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

ET_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc
DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
DEFAULT_OUTPUT = "logs/reports/weekly_policy_review_latest.md"
DEFAULT_MAX_PRED_LAG_HOURS = float(os.getenv("ML_WEEKLY_REVIEW_MAX_PRED_LAG_HOURS", "6"))
DEFAULT_SCORED_EVENT_BASIS = os.getenv("ML_WEEKLY_REVIEW_SCORED_EVENT_BASIS", "first").strip().lower()
if DEFAULT_SCORED_EVENT_BASIS not in {"first", "latest"}:
    DEFAULT_SCORED_EVENT_BASIS = "first"
DEFAULT_COST_BPS = float(os.getenv("ML_COST_SPREAD_BPS", "0.8")) + float(
    os.getenv("ML_COST_SLIPPAGE_BPS", "0.4")
) + float(os.getenv("ML_COST_COMMISSION_BPS", "0.1"))


POLICIES = ("baseline", "guardrail", "no5m")
POLICY_LABEL = {
    "baseline": "Baseline",
    "guardrail": "Guardrail (expansion+near no-trade)",
    "no5m": "No-5m Filter",
}


@dataclass(frozen=True)
class ScoredEvent:
    event_id: str
    event_day_et: date
    best_horizon: int
    trade_regime: str
    atr_zone: str
    selected_signal: str
    gross_bps: float | None
    baseline_trade: bool
    guardrail_trade: bool
    no5m_trade: bool
    guardrail_triggered: bool
    guardrail_applied: bool
    guardrail_mode: str
    guardrail_strategy: str
    selected_policy_runtime: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a weekly policy review markdown report "
            "(baseline vs guardrail vs no-5m, stratified pockets, cost sweep, daily trend)."
        )
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path")
    parser.add_argument("--symbol", default="SPY", help="Symbol filter (default: SPY)")
    parser.add_argument("--start-date", help="ET start date YYYY-MM-DD (optional)")
    parser.add_argument("--end-date", help="ET end date inclusive YYYY-MM-DD (optional)")
    parser.add_argument(
        "--source",
        choices=("live", "preview", "all"),
        default="live",
        help="Prediction source filter (default: live)",
    )
    parser.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS, help="Primary cost model in bps")
    parser.add_argument("--cost-min", type=float, default=0.7, help="Cost sweep min bps")
    parser.add_argument("--cost-max", type=float, default=1.5, help="Cost sweep max bps")
    parser.add_argument("--cost-step", type=float, default=0.1, help="Cost sweep step bps")
    parser.add_argument(
        "--calibration-min-support",
        type=int,
        default=50,
        help="Low-support threshold for total samples per horizon in calibration section",
    )
    parser.add_argument(
        "--coverage-sla-pct",
        type=float,
        default=99.0,
        help="Prediction coverage SLA threshold (percent)",
    )
    parser.add_argument(
        "--max-pred-lag-hours",
        type=float,
        default=DEFAULT_MAX_PRED_LAG_HOURS,
        help="Max prediction lag from event time in hours for weekly metrics (default: 6)",
    )
    parser.add_argument(
        "--scored-event-basis",
        choices=("first", "latest"),
        default=DEFAULT_SCORED_EVENT_BASIS,
        help=(
            "Prediction row basis for scored-event policy metrics "
            "(first=execution fidelity, latest=replay view)."
        ),
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Markdown output path")
    return parser.parse_args()


def parse_day(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def resolve_window(start_raw: str | None, end_raw: str | None) -> tuple[date, date]:
    if bool(start_raw) ^ bool(end_raw):
        raise SystemExit("Provide both --start-date and --end-date, or omit both.")
    if start_raw and end_raw:
        start_day = parse_day(start_raw)
        end_day = parse_day(end_raw)
    else:
        end_day = datetime.now(ET_TZ).date()
        start_day = end_day - timedelta(days=6)
    if end_day < start_day:
        raise SystemExit("end-date must be >= start-date")
    return start_day, end_day


def et_bounds_ms(start_day: date, end_day: date) -> tuple[int, int]:
    start_dt = datetime.combine(start_day, time.min, tzinfo=ET_TZ)
    end_dt = datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=ET_TZ)
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def parse_json(value: Any) -> dict[str, Any]:
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


def source_filter_sql(conn: sqlite3.Connection, source: str) -> str:
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(prediction_log)").fetchall()}
    has_preview = "is_preview" in cols
    if source == "all":
        return ""
    if not has_preview:
        if source == "preview":
            return "AND 1 = 0"
        return ""
    if source == "live":
        return "AND COALESCE(pl.is_preview, 0) = 0"
    return "AND COALESCE(pl.is_preview, 0) = 1"


def safe_mean(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def safe_round(value: float | None, digits: int = 3) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def weighted_mean(values: list[float], weights: list[float]) -> float | None:
    if not values or not weights or len(values) != len(weights):
        return None
    total_w = sum(weights)
    if total_w <= 0:
        return None
    return sum(v * w for v, w in zip(values, weights)) / total_w


def fmt(value: float | None, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:
        return set()
    return {str(row["name"]) for row in rows}


def load_calibration_rows(
    conn: sqlite3.Connection,
    *,
    start_day: date,
    end_day: date,
) -> tuple[list[dict[str, Any]], bool]:
    cols = table_columns(conn, "daily_ml_metrics")
    if not cols:
        return [], False

    select_cols = ["report_date", "horizon_min"]
    for col in (
        "sample_size",
        "brier_reject",
        "brier_break",
        "ece_reject",
        "ece_break",
        "auc_reject",
        "auc_break",
    ):
        if col in cols:
            select_cols.append(col)
        else:
            select_cols.append(f"NULL AS {col}")

    rows = conn.execute(
        f"""
        SELECT {", ".join(select_cols)}
        FROM daily_ml_metrics
        WHERE report_date >= ?
          AND report_date <= ?
        ORDER BY report_date ASC, horizon_min ASC
        """,
        (start_day.isoformat(), end_day.isoformat()),
    ).fetchall()
    return [dict(r) for r in rows], True


def load_scored_events(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    start_ms: int,
    end_ms: int,
    source: str,
    max_pred_lag_hours: float,
    scored_event_basis: str,
) -> list[ScoredEvent]:
    if scored_event_basis not in {"first", "latest"}:
        raise ValueError(f"Unsupported scored_event_basis={scored_event_basis!r}")
    src_filter = source_filter_sql(conn, source)
    max_pred_lag_ms = int(float(max_pred_lag_hours) * 3600 * 1000)
    pred_order = "ASC" if scored_event_basis == "first" else "DESC"
    pred_cols = table_columns(conn, "prediction_log")
    trade_regime_expr = "lp.trade_regime AS trade_regime" if "trade_regime" in pred_cols else "NULL AS trade_regime"
    regime_policy_expr = "lp.regime_policy_json AS regime_policy_json" if "regime_policy_json" in pred_cols else "NULL AS regime_policy_json"
    selected_policy_expr = "lp.selected_policy AS selected_policy" if "selected_policy" in pred_cols else "NULL AS selected_policy"
    rows = conn.execute(
        f"""
        WITH scoped_touch AS (
            SELECT
                te.event_id,
                te.ts_event
            FROM touch_events te
            WHERE te.symbol = ?
              AND te.ts_event >= ?
              AND te.ts_event < ?
        ),
        selected_pred AS (
            SELECT *
            FROM (
                SELECT
                    pl.*,
                    st.ts_event AS ts_event_ms,
                    ROW_NUMBER() OVER (
                        PARTITION BY pl.event_id
                        ORDER BY pl.ts_prediction {pred_order}
                    ) AS rn
                FROM scoped_touch st
                JOIN prediction_log pl ON pl.event_id = st.event_id
                WHERE 1 = 1
                  {src_filter}
                  AND (pl.ts_prediction - st.ts_event) >= 0
                  AND (pl.ts_prediction - st.ts_event) <= ?
            )
            WHERE rn = 1
        )
        SELECT
            st.event_id,
            st.ts_event,
            lp.best_horizon,
            lp.abstain,
            {trade_regime_expr},
            {regime_policy_expr},
            {selected_policy_expr},
            lp.signal_5m,
            lp.signal_15m,
            lp.signal_30m,
            lp.signal_60m,
            el.return_bps
        FROM selected_pred lp
        JOIN scoped_touch st ON st.event_id = lp.event_id
        JOIN event_labels el ON el.event_id = lp.event_id AND el.horizon_min = lp.best_horizon
        WHERE 1 = 1
          AND lp.best_horizon IS NOT NULL
        ORDER BY st.ts_event ASC
        """,
        (symbol.upper(), start_ms, end_ms, max_pred_lag_ms),
    ).fetchall()

    out: list[ScoredEvent] = []
    for row in rows:
        best_horizon = int(row["best_horizon"])
        signal_map = {
            5: str(row["signal_5m"] or "").strip().lower(),
            15: str(row["signal_15m"] or "").strip().lower(),
            30: str(row["signal_30m"] or "").strip().lower(),
            60: str(row["signal_60m"] or "").strip().lower(),
        }
        selected_signal = signal_map.get(best_horizon, "")
        ret = row["return_bps"]
        if selected_signal == "reject" and isinstance(ret, (int, float)):
            gross_bps: float | None = float(ret)
        elif selected_signal == "break" and isinstance(ret, (int, float)):
            gross_bps = -float(ret)
        else:
            gross_bps = None

        payload = parse_json(row["regime_policy_json"])
        atr_zone = str(payload.get("atr_zone") or "unknown").strip().lower() or "unknown"
        trade_regime = str(row["trade_regime"] or "unknown").strip().lower() or "unknown"
        guardrail_payload = payload.get("guardrail")
        guardrail_obj = guardrail_payload if isinstance(guardrail_payload, dict) else {}
        guardrail_triggered = bool(guardrail_obj.get("triggered"))
        guardrail_applied = bool(guardrail_obj.get("applied"))
        guardrail_mode = str(guardrail_obj.get("mode") or "").strip().lower()
        guardrail_strategy = str(guardrail_obj.get("strategy") or "").strip().lower()
        selected_policy_runtime = str(row["selected_policy"] or "").strip().lower()

        baseline_trade = bool(int(row["abstain"] or 0) == 0 and selected_signal in {"reject", "break"} and gross_bps is not None)
        guardrail_trade = bool(baseline_trade and not (trade_regime == "expansion" and atr_zone == "near"))
        no5m_trade = bool(baseline_trade and best_horizon != 5)

        event_day = datetime.fromtimestamp(int(row["ts_event"]) / 1000, tz=timezone.utc).astimezone(ET_TZ).date()
        out.append(
            ScoredEvent(
                event_id=str(row["event_id"]),
                event_day_et=event_day,
                best_horizon=best_horizon,
                trade_regime=trade_regime,
                atr_zone=atr_zone,
                selected_signal=selected_signal,
                gross_bps=gross_bps,
                baseline_trade=baseline_trade,
                guardrail_trade=guardrail_trade,
                no5m_trade=no5m_trade,
                guardrail_triggered=guardrail_triggered,
                guardrail_applied=guardrail_applied,
                guardrail_mode=guardrail_mode,
                guardrail_strategy=guardrail_strategy,
                selected_policy_runtime=selected_policy_runtime,
            )
        )
    return out


def load_scored_event_selection_summary(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    start_ms: int,
    end_ms: int,
    source: str,
    max_pred_lag_hours: float,
    scored_event_basis: str,
) -> dict[str, int]:
    if scored_event_basis not in {"first", "latest"}:
        raise ValueError(f"Unsupported scored_event_basis={scored_event_basis!r}")
    src_filter = source_filter_sql(conn, source)
    max_pred_lag_ms = int(float(max_pred_lag_hours) * 3600 * 1000)
    pred_order = "ASC" if scored_event_basis == "first" else "DESC"
    row = conn.execute(
        f"""
        WITH scoped_touch AS (
            SELECT te.event_id, te.ts_event
            FROM touch_events te
            WHERE te.symbol = ?
              AND te.ts_event >= ?
              AND te.ts_event < ?
        ),
        selected_pred AS (
            SELECT *
            FROM (
                SELECT
                    pl.event_id,
                    pl.best_horizon,
                    ROW_NUMBER() OVER (
                        PARTITION BY pl.event_id
                        ORDER BY pl.ts_prediction {pred_order}
                    ) AS rn
                FROM scoped_touch st
                JOIN prediction_log pl ON pl.event_id = st.event_id
                WHERE 1 = 1
                  {src_filter}
                  AND (pl.ts_prediction - st.ts_event) >= 0
                  AND (pl.ts_prediction - st.ts_event) <= ?
            )
            WHERE rn = 1
        )
        SELECT
            COUNT(*) AS selected_rows,
            SUM(CASE WHEN sp.best_horizon IS NOT NULL THEN 1 ELSE 0 END) AS with_horizon_rows,
            SUM(
                CASE
                    WHEN sp.best_horizon IS NOT NULL
                     AND EXISTS (
                         SELECT 1
                         FROM event_labels el
                         WHERE el.event_id = sp.event_id
                           AND el.horizon_min = sp.best_horizon
                     )
                    THEN 1 ELSE 0
                END
            ) AS labeled_rows
        FROM selected_pred sp
        """,
        (symbol.upper(), start_ms, end_ms, max_pred_lag_ms),
    ).fetchone()
    selected_rows = int((row["selected_rows"] if row else 0) or 0)
    with_horizon_rows = int((row["with_horizon_rows"] if row else 0) or 0)
    labeled_rows = int((row["labeled_rows"] if row else 0) or 0)
    return {
        "selected_rows": selected_rows,
        "with_horizon_rows": with_horizon_rows,
        "labeled_rows": labeled_rows,
        "dropped_missing_label_rows": max(0, with_horizon_rows - labeled_rows),
    }


def load_prediction_coverage(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    start_ms: int,
    end_ms: int,
    source: str,
    max_pred_lag_hours: float,
) -> dict[str, Any]:
    src_filter = source_filter_sql(conn, source)
    max_pred_lag_ms = int(float(max_pred_lag_hours) * 3600 * 1000)
    rows = conn.execute(
        f"""
        WITH te AS (
            SELECT
                te.event_id AS event_id,
                te.ts_event AS ts_event_ms
            FROM touch_events te
            WHERE te.symbol = ?
              AND te.ts_event >= ?
              AND te.ts_event < ?
        ),
        pred_ids AS (
            SELECT DISTINCT te.event_id
            FROM te
            JOIN prediction_log pl ON pl.event_id = te.event_id
            WHERE 1 = 1
              {src_filter}
              AND (pl.ts_prediction - te.ts_event_ms) >= 0
              AND (pl.ts_prediction - te.ts_event_ms) <= ?
        )
        SELECT
            te.ts_event_ms AS ts_event_ms,
            CASE WHEN te.event_id IN (SELECT event_id FROM pred_ids) THEN 1 ELSE 0 END AS has_prediction
        FROM te
        ORDER BY te.ts_event_ms ASC
        """,
        (symbol.upper(), start_ms, end_ms, max_pred_lag_ms),
    ).fetchall()

    day_counts: dict[str, dict[str, int]] = {}
    for row in rows:
        ts_event_ms = int(row["ts_event_ms"])
        day = (
            datetime.fromtimestamp(ts_event_ms / 1000, tz=timezone.utc)
            .astimezone(ET_TZ)
            .date()
            .isoformat()
        )
        bucket = day_counts.setdefault(day, {"touch_n": 0, "pred_n": 0})
        bucket["touch_n"] += 1
        bucket["pred_n"] += int(row["has_prediction"] or 0)

    day_rows: list[dict[str, Any]] = []
    touch_total = 0
    pred_total = 0
    zero_pred_days = 0
    for day in sorted(day_counts.keys()):
        counts = day_counts[day]
        touch_n = int(counts["touch_n"])
        pred_n = int(counts["pred_n"])
        coverage_pct = (100.0 * pred_n / touch_n) if touch_n > 0 else None
        if pred_n == 0:
            zero_pred_days += 1
        touch_total += touch_n
        pred_total += pred_n
        day_rows.append(
            {
                "day": day,
                "touch_n": touch_n,
                "pred_n": pred_n,
                "gap_n": max(0, touch_n - pred_n),
                "coverage_pct": coverage_pct,
            }
        )

    overall_pct = (100.0 * pred_total / touch_total) if touch_total > 0 else None
    return {
        "days": day_rows,
        "touch_total": touch_total,
        "pred_total": pred_total,
        "coverage_pct": overall_pct,
        "zero_pred_days": zero_pred_days,
    }


def load_prediction_lag_profile(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    start_ms: int,
    end_ms: int,
    source: str,
) -> dict[str, Any]:
    src_filter = source_filter_sql(conn, source)
    one_hour_ms = 3600 * 1000
    three_hour_ms = 3 * 3600 * 1000
    six_hour_ms = 6 * 3600 * 1000
    rows = conn.execute(
        f"""
        WITH te AS (
            SELECT
                te.event_id AS event_id,
                te.ts_event AS ts_event_ms
            FROM touch_events te
            WHERE te.symbol = ?
              AND te.ts_event >= ?
              AND te.ts_event < ?
        ),
        first_pred AS (
            SELECT
                te.event_id AS event_id,
                te.ts_event_ms AS ts_event_ms,
                MIN(pl.ts_prediction) AS first_pred_ms
            FROM te
            LEFT JOIN prediction_log pl
                ON pl.event_id = te.event_id
                {src_filter}
            GROUP BY te.event_id, te.ts_event_ms
        )
        SELECT
            fp.event_id AS event_id,
            fp.ts_event_ms AS ts_event_ms,
            fp.first_pred_ms AS first_pred_ms
        FROM first_pred fp
        ORDER BY fp.ts_event_ms ASC
        """,
        (
            symbol.upper(),
            start_ms,
            end_ms,
        ),
    ).fetchall()

    day_buckets: dict[str, dict[str, int]] = {}
    for row in rows:
        ts_event_ms = int(row["ts_event_ms"] or 0)
        if ts_event_ms <= 0:
            continue
        event_day_et = (
            datetime.fromtimestamp(ts_event_ms / 1000, tz=timezone.utc)
            .astimezone(ET_TZ)
            .date()
            .isoformat()
        )
        bucket = day_buckets.setdefault(
            event_day_et,
            {
                "touch_n": 0,
                "no_pred_n": 0,
                "lag_le_1h_n": 0,
                "lag_1_to_3h_n": 0,
                "lag_3_to_6h_n": 0,
                "lag_gt_6h_n": 0,
                "lag_negative_n": 0,
            },
        )
        bucket["touch_n"] += 1
        first_pred_ms = row["first_pred_ms"]
        if first_pred_ms is None:
            bucket["no_pred_n"] += 1
            continue
        lag_ms = int(first_pred_ms) - ts_event_ms
        if lag_ms < 0:
            bucket["lag_negative_n"] += 1
        elif lag_ms <= one_hour_ms:
            bucket["lag_le_1h_n"] += 1
        elif lag_ms <= three_hour_ms:
            bucket["lag_1_to_3h_n"] += 1
        elif lag_ms <= six_hour_ms:
            bucket["lag_3_to_6h_n"] += 1
        else:
            bucket["lag_gt_6h_n"] += 1

    day_rows: list[dict[str, Any]] = []
    totals = {
        "touch_total": 0,
        "no_pred_total": 0,
        "lag_le_1h_total": 0,
        "lag_1_to_3h_total": 0,
        "lag_3_to_6h_total": 0,
        "lag_gt_6h_total": 0,
        "lag_negative_total": 0,
    }
    for day in sorted(day_buckets):
        day_counts = day_buckets[day]
        touch_n = int(day_counts["touch_n"])
        no_pred_n = int(day_counts["no_pred_n"])
        lag_le_1h_n = int(day_counts["lag_le_1h_n"])
        lag_1_to_3h_n = int(day_counts["lag_1_to_3h_n"])
        lag_3_to_6h_n = int(day_counts["lag_3_to_6h_n"])
        lag_gt_6h_n = int(day_counts["lag_gt_6h_n"])
        lag_negative_n = int(day_counts["lag_negative_n"])
        day_rows.append(
            {
                "day": str(day),
                "touch_n": touch_n,
                "no_pred_n": no_pred_n,
                "lag_le_1h_n": lag_le_1h_n,
                "lag_1_to_3h_n": lag_1_to_3h_n,
                "lag_3_to_6h_n": lag_3_to_6h_n,
                "lag_gt_6h_n": lag_gt_6h_n,
                "lag_negative_n": lag_negative_n,
            }
        )
        totals["touch_total"] += touch_n
        totals["no_pred_total"] += no_pred_n
        totals["lag_le_1h_total"] += lag_le_1h_n
        totals["lag_1_to_3h_total"] += lag_1_to_3h_n
        totals["lag_3_to_6h_total"] += lag_3_to_6h_n
        totals["lag_gt_6h_total"] += lag_gt_6h_n
        totals["lag_negative_total"] += lag_negative_n

    return {
        "days": day_rows,
        **totals,
    }


def policy_trade(event: ScoredEvent, policy: str) -> bool:
    if policy == "baseline":
        return event.baseline_trade
    if policy == "guardrail":
        return event.guardrail_trade
    if policy == "no5m":
        return event.no5m_trade
    raise ValueError(f"Unsupported policy: {policy}")


def policy_net(event: ScoredEvent, policy: str, cost_bps: float) -> float:
    if event.gross_bps is None or not policy_trade(event, policy):
        return 0.0
    return float(event.gross_bps) - float(cost_bps)


def summarize_policy(events: list[ScoredEvent], cost_bps: float) -> dict[str, dict[str, float | int | None]]:
    out: dict[str, dict[str, float | int | None]] = {}
    guardrail_candidates = sum(
        1 for e in events if e.baseline_trade and e.trade_regime == "expansion" and e.atr_zone == "near"
    )
    no5m_candidates = sum(1 for e in events if e.baseline_trade and int(e.best_horizon) == 5)
    for policy in POLICIES:
        nets = [policy_net(e, policy, cost_bps) for e in events]
        traded_nets = [n for e, n in zip(events, nets) if policy_trade(e, policy)]
        wins = sum(1 for n in traded_nets if n > 0)
        out[policy] = {
            "trades": len(traded_nets),
            "avg_net_trade": safe_mean(traded_nets),
            "avg_net_event": safe_mean(nets),
            "total_net": sum(nets),
            "win_rate": ((wins * 100.0 / len(traded_nets)) if traded_nets else None),
        }
    out["meta"] = {
        "events": len(events),
        "guardrail_candidates": guardrail_candidates,
        "no5m_candidates": no5m_candidates,
        "filtered_by_guardrail": sum(1 for e in events if e.baseline_trade and not e.guardrail_trade),
        "filtered_by_no5m": sum(1 for e in events if e.baseline_trade and not e.no5m_trade),
    }
    return out


def generate_cost_values(cost_min: float, cost_max: float, cost_step: float) -> list[float]:
    values: list[float] = []
    cur = float(cost_min)
    guard = 0
    while cur <= float(cost_max) + (float(cost_step) / 10.0):
        values.append(round(cur, 4))
        cur += float(cost_step)
        guard += 1
        if guard > 1000:
            break
    return values


def build_report(
    *,
    symbol: str,
    source: str,
    scored_event_basis: str,
    start_day: date,
    end_day: date,
    events: list[ScoredEvent],
    calibration_rows: list[dict[str, Any]],
    has_daily_ml_metrics: bool,
    coverage_summary: dict[str, Any],
    scored_event_selection: dict[str, Any],
    lag_profile: dict[str, Any],
    coverage_sla_pct: float,
    max_pred_lag_hours: float,
    calibration_min_support: int,
    base_cost_bps: float,
    cost_min: float,
    cost_max: float,
    cost_step: float,
) -> str:
    summary = summarize_policy(events, base_cost_bps)
    baseline_total = float(summary["baseline"]["total_net"] or 0.0)
    guardrail_total = float(summary["guardrail"]["total_net"] or 0.0)
    no5m_total = float(summary["no5m"]["total_net"] or 0.0)
    cov_days = list(coverage_summary.get("days") or [])
    cov_touch_total = int(coverage_summary.get("touch_total") or 0)
    cov_pred_total = int(coverage_summary.get("pred_total") or 0)
    cov_pct = coverage_summary.get("coverage_pct")
    cov_zero_days = int(coverage_summary.get("zero_pred_days") or 0)
    selection_rows = int(scored_event_selection.get("selected_rows") or 0)
    with_horizon_rows = int(scored_event_selection.get("with_horizon_rows") or 0)
    labeled_rows = int(scored_event_selection.get("labeled_rows") or 0)
    dropped_missing_label_rows = int(scored_event_selection.get("dropped_missing_label_rows") or 0)
    cov_below = 0
    for row in cov_days:
        pct = row.get("coverage_pct")
        if isinstance(pct, (int, float)) and float(pct) < float(coverage_sla_pct):
            cov_below += 1
    coverage_status = "PASS" if cov_touch_total > 0 and cov_below == 0 else "FAIL"
    lag_days = list(lag_profile.get("days") or [])
    lag_touch_total = int(lag_profile.get("touch_total") or 0)
    lag_no_pred_total = int(lag_profile.get("no_pred_total") or 0)
    lag_le_1h_total = int(lag_profile.get("lag_le_1h_total") or 0)
    lag_1_to_3h_total = int(lag_profile.get("lag_1_to_3h_total") or 0)
    lag_3_to_6h_total = int(lag_profile.get("lag_3_to_6h_total") or 0)
    lag_gt_6h_total = int(lag_profile.get("lag_gt_6h_total") or 0)
    lag_negative_total = int(lag_profile.get("lag_negative_total") or 0)
    policy_gate_line = (
        "- Policy Change Gate: BLOCK POLICY CHANGES (coverage SLA FAIL)"
        if coverage_status == "FAIL"
        else "- Policy Change Gate: ALLOW POLICY CHANGES (coverage SLA PASS)"
    )
    basis_label = "first prediction per event" if scored_event_basis == "first" else "latest prediction per event"

    lines: list[str] = []
    lines.append(f"# Weekly Policy Review ({symbol.upper()})")
    lines.append("")
    lines.append(f"- Window (ET): {start_day.isoformat()} -> {end_day.isoformat()}")
    lines.append(f"- Source: {source}")
    lines.append(f"- Scored-event basis: {basis_label}")
    lines.append(f"- Events ({basis_label}): {len(events)}")
    lines.append(f"- Primary cost model: {base_cost_bps:.2f} bps")
    lines.append(policy_gate_line)
    lines.append("")

    lines.append("## Prediction Coverage SLA")
    lines.append("")
    lines.append(f"- Timely prediction lag filter: <= {float(max_pred_lag_hours):.2f} hours")

    if cov_touch_total <= 0:
        lines.append("- No touch events in selected window; coverage SLA not applicable.")
        lines.append("- Coverage status: FAIL")
        lines.append("")
    else:
        lines.append(f"- SLA target: {coverage_sla_pct:.2f}%")
        lines.append(
            f"- Overall coverage: {fmt(float(cov_pct), 2) if cov_pct is not None else 'n/a'}% "
            f"({cov_pred_total}/{cov_touch_total})"
        )
        lines.append(f"- Days below SLA: {cov_below}/{len(cov_days)}")
        lines.append(f"- Zero-prediction days: {cov_zero_days}")
        lines.append(
            f"- Scored-event basis rows selected: {selection_rows} "
            f"(with best_horizon={with_horizon_rows}, labeled={labeled_rows})"
        )
        lines.append(
            f"- Scored-event rows dropped (missing matching horizon label): "
            f"{dropped_missing_label_rows}"
        )
        lines.append(f"- Coverage status: {coverage_status}")
        lines.append("")
        lines.append("| Day (ET) | Touch Events | Predicted Events | Gap | Coverage % |")
        lines.append("|---|---:|---:|---:|---:|")
        for row in cov_days:
            pct = row.get("coverage_pct")
            lines.append(
                f"| {row['day']} | {int(row['touch_n'])} | {int(row['pred_n'])} | {int(row['gap_n'])} | "
                f"{fmt(float(pct), 2) if pct is not None else 'n/a'} |"
            )
        lines.append("")

    lines.append("## Prediction Lag Profile (First Live Prediction)")
    lines.append("")
    lines.append("- Buckets: <=1h, 1-3h, 3-6h, >6h from touch event time")
    lines.append(f"- Touch events: {lag_touch_total}")
    lines.append(f"- <=1h: {lag_le_1h_total}")
    lines.append(f"- 1-3h: {lag_1_to_3h_total}")
    lines.append(f"- 3-6h: {lag_3_to_6h_total}")
    lines.append(f"- >6h: {lag_gt_6h_total}")
    lines.append(f"- No prediction: {lag_no_pred_total}")
    if lag_negative_total > 0:
        lines.append(f"- Negative lag (data anomaly): {lag_negative_total}")
    lines.append("")
    lines.append("| Day (ET) | Touch Events | <=1h | 1-3h | 3-6h | >6h | No Prediction |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for row in lag_days:
        lines.append(
            f"| {row['day']} | {int(row['touch_n'])} | {int(row['lag_le_1h_n'])} | {int(row['lag_1_to_3h_n'])} | "
            f"{int(row['lag_3_to_6h_n'])} | {int(row['lag_gt_6h_n'])} | {int(row['no_pred_n'])} |"
        )
    lines.append("")

    lines.append("## What-if Policy Comparison (Baseline vs Guardrail vs No-5m)")
    lines.append("")
    lines.append("- Interpretation: hypothetical trade filters applied over the same scored event set.")
    lines.append("")
    lines.append("| Policy | Trades | Avg Net/Trade | Avg Net/Event | Total Net (bps) | Net Win % |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for policy in POLICIES:
        bucket = summary[policy]
        lines.append(
            f"| {POLICY_LABEL[policy]} | {int(bucket['trades'] or 0)} | {fmt(bucket['avg_net_trade'])} | "
            f"{fmt(bucket['avg_net_event'])} | {fmt(float(bucket['total_net'] or 0.0))} | {fmt(bucket['win_rate'], 2)} |"
        )
    lines.append("")
    lines.append(f"- What-if delta Guardrail vs Baseline (total bps): {guardrail_total - baseline_total:.3f}")
    lines.append(f"- What-if delta No-5m vs Baseline (total bps): {no5m_total - baseline_total:.3f}")
    lines.append(
        f"- What-if guardrail candidate trades (baseline in expansion+near): "
        f"{int(summary['meta']['guardrail_candidates'] or 0)}"
    )
    lines.append(
        f"- What-if no-5m candidate trades (baseline best_horizon=5): "
        f"{int(summary['meta']['no5m_candidates'] or 0)}"
    )
    lines.append(f"- What-if filtered trades by guardrail: {int(summary['meta']['filtered_by_guardrail'] or 0)}")
    lines.append(f"- What-if filtered trades by no-5m filter: {int(summary['meta']['filtered_by_no5m'] or 0)}")
    lines.append("")

    lines.append("## Runtime Applied Policy Summary")
    lines.append("")
    lines.append("- Interpretation: rows grouped by persisted `selected_policy` from runtime scoring.")
    lines.append("| Runtime Policy | Rows | Tradeable Signals | Avg Net/Event | Total Net (bps) |")
    lines.append("|---|---:|---:|---:|---:|")
    runtime_groups: dict[str, list[ScoredEvent]] = {}
    for event in events:
        key = event.selected_policy_runtime or "unknown"
        runtime_groups.setdefault(key, []).append(event)
    for runtime_policy in sorted(runtime_groups.keys()):
        bucket_events = runtime_groups[runtime_policy]
        nets = [policy_net(e, "baseline", base_cost_bps) for e in bucket_events]
        traded = [n for e, n in zip(bucket_events, nets) if e.baseline_trade]
        lines.append(
            f"| {runtime_policy} | {len(bucket_events)} | {len(traded)} | "
            f"{fmt(safe_mean(nets))} | {fmt(sum(nets))} |"
        )
    lines.append("")

    lines.append("## Cost Sweep")
    lines.append("")
    lines.append("| Cost (bps) | Baseline Total | Guardrail Total | No-5m Total | Guardrail Delta vs Base | No-5m Delta vs Base |")
    lines.append("|---:|---:|---:|---:|---:|---:|")
    for cost in generate_cost_values(cost_min, cost_max, cost_step):
        sm = summarize_policy(events, cost)
        b_total = float(sm["baseline"]["total_net"] or 0.0)
        g_total = float(sm["guardrail"]["total_net"] or 0.0)
        n_total = float(sm["no5m"]["total_net"] or 0.0)
        lines.append(
            f"| {cost:.2f} | {b_total:.3f} | {g_total:.3f} | {n_total:.3f} | {g_total - b_total:.3f} | {n_total - b_total:.3f} |"
        )
    lines.append("")

    lines.append("## Stratified PnL (Regime x ATR Zone x Horizon)")
    lines.append("")
    lines.append(
        "| Regime | ATR Zone | Horizon | Rows | Base Trades | Base Avg Net | Guard Avg Net | No-5m Avg Net | "
        "Guard Delta vs Base (Total) | No-5m Delta vs Base (Total) |"
    )
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    strat: dict[tuple[str, str, int], list[ScoredEvent]] = {}
    for e in events:
        key = (e.trade_regime or "unknown", e.atr_zone or "unknown", int(e.best_horizon))
        strat.setdefault(key, []).append(e)
    for key in sorted(strat.keys()):
        bucket_events = strat[key]
        sm = summarize_policy(bucket_events, base_cost_bps)
        b_total = float(sm["baseline"]["total_net"] or 0.0)
        g_total = float(sm["guardrail"]["total_net"] or 0.0)
        n_total = float(sm["no5m"]["total_net"] or 0.0)
        regime, atr_zone, horizon = key
        lines.append(
            f"| {regime} | {atr_zone} | {horizon}m | {len(bucket_events)} | "
            f"{int(sm['baseline']['trades'] or 0)} | {fmt(sm['baseline']['avg_net_trade'])} | "
            f"{fmt(sm['guardrail']['avg_net_trade'])} | {fmt(sm['no5m']['avg_net_trade'])} | "
            f"{g_total - b_total:.3f} | {n_total - b_total:.3f} |"
        )
    lines.append("")

    lines.append("## Daily Expectancy")
    lines.append("")
    lines.append("| Day (ET) | Rows | Base Trades | Base Avg Net/Event | Guard Avg Net/Event | No-5m Avg Net/Event | Base Total Net | Guard Total Net | No-5m Total Net |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    by_day: dict[date, list[ScoredEvent]] = {}
    for e in events:
        by_day.setdefault(e.event_day_et, []).append(e)
    for d in sorted(by_day.keys()):
        sm = summarize_policy(by_day[d], base_cost_bps)
        lines.append(
            f"| {d.isoformat()} | {len(by_day[d])} | {int(sm['baseline']['trades'] or 0)} | "
            f"{fmt(sm['baseline']['avg_net_event'])} | {fmt(sm['guardrail']['avg_net_event'])} | {fmt(sm['no5m']['avg_net_event'])} | "
            f"{fmt(float(sm['baseline']['total_net'] or 0.0))} | {fmt(float(sm['guardrail']['total_net'] or 0.0))} | "
            f"{fmt(float(sm['no5m']['total_net'] or 0.0))} |"
        )
    lines.append("")

    lines.append("## Trade Share by Horizon (Baseline Trades)")
    lines.append("")
    lines.append("| Horizon | Trades | Share % | Avg Net/Trade |")
    lines.append("|---:|---:|---:|---:|")
    horizon_trades: dict[int, list[float]] = {}
    for e in events:
        if not e.baseline_trade:
            continue
        net = policy_net(e, "baseline", base_cost_bps)
        horizon_trades.setdefault(int(e.best_horizon), []).append(net)
    total_h_trades = sum(len(v) for v in horizon_trades.values())
    for horizon in sorted(horizon_trades.keys()):
        vals = horizon_trades[horizon]
        share = (100.0 * len(vals) / total_h_trades) if total_h_trades else 0.0
        lines.append(f"| {horizon}m | {len(vals)} | {share:.2f} | {fmt(safe_mean(vals))} |")
    lines.append("")

    lines.append("## Calibration Health (AUC/Brier/ECE)")
    lines.append("")
    if not has_daily_ml_metrics:
        lines.append("- No `daily_ml_metrics` table found in this DB.")
        lines.append("")
    else:
        total_rows = len(calibration_rows)
        effective_rows = [r for r in calibration_rows if float(r.get("sample_size") or 0.0) > 0]
        lines.append(f"- Daily rows in window: {total_rows}")
        lines.append(f"- Rows with `sample_size > 0`: {len(effective_rows)}")
        lines.append(f"- Low-support threshold (total samples per horizon): {int(calibration_min_support)}")
        lines.append("")
        lines.append("| Horizon | Effective Days | Total Sample | Mean Sample/Day | Brier Reject | Brier Break | ECE Reject | ECE Break | AUC Reject | AUC Break | Support |")
        lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")

        by_horizon: dict[int, list[dict[str, Any]]] = {}
        for row in effective_rows:
            horizon = int(row.get("horizon_min") or 0)
            if horizon <= 0:
                continue
            by_horizon.setdefault(horizon, []).append(row)

        low_support_horizons: list[tuple[int, float]] = []
        for horizon in sorted(by_horizon.keys()):
            rows = by_horizon[horizon]
            samples = [float(r.get("sample_size") or 0.0) for r in rows]
            total_sample = sum(samples)
            mean_sample = (total_sample / len(rows)) if rows else 0.0

            def metric_weighted_mean(key: str) -> float | None:
                vals: list[float] = []
                wts: list[float] = []
                for row, w in zip(rows, samples):
                    v = row.get(key)
                    if v is None:
                        continue
                    vals.append(float(v))
                    wts.append(float(w))
                return weighted_mean(vals, wts)

            support = "OK" if total_sample >= float(calibration_min_support) else "LOW_SUPPORT"
            if support != "OK":
                low_support_horizons.append((horizon, total_sample))

            lines.append(
                f"| {horizon}m | {len(rows)} | {int(total_sample)} | {mean_sample:.1f} | "
                f"{fmt(metric_weighted_mean('brier_reject'), 4)} | {fmt(metric_weighted_mean('brier_break'), 4)} | "
                f"{fmt(metric_weighted_mean('ece_reject'), 4)} | {fmt(metric_weighted_mean('ece_break'), 4)} | "
                f"{fmt(metric_weighted_mean('auc_reject'), 4)} | {fmt(metric_weighted_mean('auc_break'), 4)} | {support} |"
            )

        if not by_horizon:
            lines.append("| n/a | 0 | 0 | n/a | n/a | n/a | n/a | n/a | n/a | n/a | LOW_SUPPORT |")
        lines.append("")

        if low_support_horizons:
            details = ", ".join(f"{h}m(total={int(n)})" for h, n in low_support_horizons)
            lines.append(f"- Low-support horizons: {details}")
        else:
            lines.append("- Low-support horizons: none")
        lines.append("")

    lines.append("## Runtime Guardrail Audit")
    lines.append("")
    runtime_guardrail_selected = sum(
        1 for e in events if (e.selected_policy_runtime or "").startswith("guardrail_")
    )
    runtime_regime_active_selected = sum(1 for e in events if e.selected_policy_runtime == "regime_active")
    runtime_baseline_selected = sum(1 for e in events if e.selected_policy_runtime == "baseline")
    triggered = sum(1 for e in events if e.guardrail_triggered)
    applied = sum(1 for e in events if e.guardrail_applied)
    leaked = sum(1 for e in events if e.guardrail_applied and e.selected_signal in {"reject", "break"})
    active_no_trade = sum(
        1
        for e in events
        if e.guardrail_mode == "active" and e.guardrail_strategy == "no_trade" and e.guardrail_triggered
    )
    lines.append(f"- Runtime selected_policy baseline rows: {runtime_baseline_selected}")
    lines.append(f"- Runtime selected_policy regime_active rows: {runtime_regime_active_selected}")
    lines.append(f"- Runtime selected_policy guardrail_* rows: {runtime_guardrail_selected}")
    lines.append(f"- Guardrail triggered rows: {triggered}")
    lines.append(f"- Guardrail applied rows: {applied}")
    lines.append(f"- Active+no_trade triggered rows: {active_no_trade}")
    lines.append(f"- Leak rows (applied and still traded): {leaked}")
    lines.append("")

    lines.append("## Decision Notes")
    lines.append("")
    lines.append("- Keep the rule set fixed during the next live collection window.")
    lines.append("- Review this report weekly and only promote one policy change at a time.")
    lines.append("- Treat replay as directional evidence; use live rows for deployment decisions.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    start_day, end_day = resolve_window(args.start_date, args.end_date)
    start_ms, end_ms = et_bounds_ms(start_day, end_day)

    conn = connect(args.db)
    try:
        events = load_scored_events(
            conn,
            symbol=args.symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            source=args.source,
            max_pred_lag_hours=float(args.max_pred_lag_hours),
            scored_event_basis=str(args.scored_event_basis).strip().lower(),
        )
        calibration_rows, has_daily_ml_metrics = load_calibration_rows(
            conn,
            start_day=start_day,
            end_day=end_day,
        )
        coverage_summary = load_prediction_coverage(
            conn,
            symbol=args.symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            source=args.source,
            max_pred_lag_hours=float(args.max_pred_lag_hours),
        )
        scored_event_selection = load_scored_event_selection_summary(
            conn,
            symbol=args.symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            source=args.source,
            max_pred_lag_hours=float(args.max_pred_lag_hours),
            scored_event_basis=str(args.scored_event_basis).strip().lower(),
        )
        lag_profile = load_prediction_lag_profile(
            conn,
            symbol=args.symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            source=args.source,
        )
    finally:
        conn.close()

    report = build_report(
        symbol=args.symbol,
        source=args.source,
        scored_event_basis=str(args.scored_event_basis).strip().lower(),
        start_day=start_day,
        end_day=end_day,
        events=events,
        calibration_rows=calibration_rows,
        has_daily_ml_metrics=has_daily_ml_metrics,
        coverage_summary=coverage_summary,
        scored_event_selection=scored_event_selection,
        lag_profile=lag_profile,
        coverage_sla_pct=float(args.coverage_sla_pct),
        max_pred_lag_hours=float(args.max_pred_lag_hours),
        calibration_min_support=int(args.calibration_min_support),
        base_cost_bps=float(args.cost_bps),
        cost_min=float(args.cost_min),
        cost_max=float(args.cost_max),
        cost_step=float(args.cost_step),
    )
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report + "\n", encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
