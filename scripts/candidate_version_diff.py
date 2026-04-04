#!/usr/bin/env python3
"""Compare two candidate/model versions on a common prediction cohort.

Use this after candidate preview backtests to answer:
- Did the newer candidate materially change scores or signals?
- Did policy/regime assignments change?
- Were changes concentrated in good or bad realized-utility rows?
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ml.regime_semantics import favored_side_for_trade_regime

DEFAULT_DB = ROOT / "data" / "pivot_events.sqlite"
DEFAULT_OUT_DIR = ROOT / "logs" / "reports" / "research"
ET = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class VersionRow:
    event_id: str
    ts_event: int
    event_day_et: str
    regime_bucket: str
    selected_policy: str
    runtime_signal: str
    chosen_side: str
    realized_utility: float
    model_side_prob: float | None
    model_side_margin: float | None
    analog_side_prob: float | None
    analog_penalty_025: float | None
    disagreement: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two model/candidate versions on a common cohort.")
    parser.add_argument("--db-a", default=str(DEFAULT_DB), help="SQLite DB for version A.")
    parser.add_argument("--db-b", default="", help="SQLite DB for version B (defaults to --db-a).")
    parser.add_argument("--version-a", required=True, help="Model version A, e.g. v218.")
    parser.add_argument("--version-b", required=True, help="Model version B, e.g. v227.")
    parser.add_argument("--symbol", default="SPY", help="Symbol to analyze.")
    parser.add_argument("--horizon", type=int, default=60, help="Single horizon in minutes.")
    parser.add_argument(
        "--source",
        choices=("live", "preview", "all"),
        default="preview",
        help="Prediction source filter.",
    )
    parser.add_argument(
        "--prediction-basis",
        choices=("first", "latest"),
        default="first",
        help="Which prediction row to use per event.",
    )
    parser.add_argument("--cost-bps", type=float, default=1.3, help="Per-trade cost in bps.")
    parser.add_argument(
        "--max-pred-lag-hours",
        type=float,
        default=6.0,
        help="Maximum prediction lag from event time for live/all rows.",
    )
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory.")
    return parser.parse_args()


def validate_db_path(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()
    required = {"prediction_log", "touch_events", "event_labels"}
    missing = sorted(required - tables)
    if missing:
        raise RuntimeError(
            f"SQLite DB at {db_path} is missing required tables: {', '.join(missing)}"
        )


def source_filter_sql(source: str) -> str:
    if source == "all":
        return ""
    if source == "live":
        return "AND COALESCE(pl.is_preview, 0) = 0"
    return "AND COALESCE(pl.is_preview, 0) = 1"


def regime_bucket_from_event(regime_type_value: Any, trade_regime_value: Any) -> str:
    trade_regime = str(trade_regime_value or "").strip().lower()
    if trade_regime in {"compression", "expansion", "neutral"}:
        return trade_regime
    try:
        regime_type = int(regime_type_value) if regime_type_value is not None else None
    except (TypeError, ValueError):
        regime_type = None
    if regime_type in (1, 2, 4):
        return "expansion"
    if regime_type == 3:
        return "compression"
    return "neutral"


def chosen_side(bucket: str) -> str:
    return favored_side_for_trade_regime(bucket)


def et_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(ET).strftime("%Y-%m-%d")


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    pos = (len(ordered) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(ordered[lo])
    frac = pos - lo
    return float(ordered[lo] * (1.0 - frac) + ordered[hi] * frac)


def summarize(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {
            "rows_n": 0,
            "mean": None,
            "p05": None,
            "p50": None,
            "p95": None,
        }
    return {
        "rows_n": len(values),
        "mean": float(statistics.fmean(values)),
        "p05": percentile(values, 0.05),
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
    }


def load_rows(
    db_path: Path,
    *,
    symbol: str,
    horizon: int,
    source: str,
    prediction_basis: str,
    model_version: str,
    cost_bps: float,
    max_pred_lag_hours: float,
) -> tuple[dict[str, VersionRow], dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        pred_order = "ASC" if prediction_basis == "first" else "DESC"
        source_sql = source_filter_sql(source)
        prob_reject_col = f"prob_reject_{horizon}m"
        prob_break_col = f"prob_break_{horizon}m"
        threshold_reject_col = f"threshold_reject_{horizon}m"
        threshold_break_col = f"threshold_break_{horizon}m"
        signal_col = f"signal_{horizon}m"

        sql = f"""
            WITH scoped_pred AS (
                SELECT *
                FROM (
                    SELECT
                        pl.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY pl.event_id
                            ORDER BY pl.ts_prediction {pred_order}
                        ) AS rn
                    FROM prediction_log pl
                    JOIN touch_events te
                      ON te.event_id = pl.event_id
                    WHERE te.symbol = ?
                      AND COALESCE(pl.model_version, '') = ?
                      {source_sql}
                )
                WHERE rn = 1
            )
            SELECT
                te.event_id,
                te.ts_event,
                te.regime_type,
                sp.ts_prediction,
                sp.trade_regime,
                sp.selected_policy,
                sp.{signal_col} AS runtime_signal,
                sp.{prob_reject_col} AS prob_reject,
                sp.{prob_break_col} AS prob_break,
                sp.{threshold_reject_col} AS threshold_reject,
                sp.{threshold_break_col} AS threshold_break,
                sp.analog_best_reject_prob,
                sp.analog_best_break_prob,
                sp.analog_best_disagreement,
                el.return_bps
            FROM scoped_pred sp
            JOIN touch_events te
              ON te.event_id = sp.event_id
            JOIN event_labels el
              ON el.event_id = te.event_id
             AND el.horizon_min = ?
            ORDER BY te.ts_event
        """
        fetched = conn.execute(sql, [symbol, model_version, horizon]).fetchall()
    finally:
        conn.close()

    apply_lag_filter = source != "preview"
    max_lag_ms = int(max_pred_lag_hours * 3600 * 1000)
    meta = {
        "prediction_rows_joined": len(fetched),
        "prediction_basis": prediction_basis,
        "source": source,
        "model_version_filter": model_version,
        "max_pred_lag_hours": max_pred_lag_hours,
        "max_pred_lag_applied": apply_lag_filter,
        "dropped_for_lag": 0,
        "dropped_for_abstain": 0,
    }

    rows: dict[str, VersionRow] = {}
    for fetched_row in fetched:
        (
            event_id,
            ts_event,
            regime_type_value,
            ts_prediction,
            trade_regime,
            selected_policy,
            runtime_signal,
            prob_reject,
            prob_break,
            threshold_reject,
            threshold_break,
            analog_reject,
            analog_break,
            disagreement,
            return_bps,
        ) = fetched_row

        ts_event = int(ts_event)
        ts_prediction = int(ts_prediction)
        lag_ms = ts_prediction - ts_event
        if apply_lag_filter and (lag_ms < 0 or lag_ms > max_lag_ms):
            meta["dropped_for_lag"] += 1
            continue

        bucket = regime_bucket_from_event(regime_type_value, trade_regime)
        side = chosen_side(bucket)
        if side == "abstain":
            meta["dropped_for_abstain"] += 1
            continue

        ret = float(return_bps)
        realized = (ret - cost_bps) if side == "reject" else (-ret - cost_bps)
        model_prob = float(prob_reject) if side == "reject" and prob_reject is not None else (
            float(prob_break) if side == "break" and prob_break is not None else None
        )
        threshold = float(threshold_reject) if side == "reject" and threshold_reject is not None else (
            float(threshold_break) if side == "break" and threshold_break is not None else None
        )
        analog_prob = float(analog_reject) if side == "reject" and analog_reject is not None else (
            float(analog_break) if side == "break" and analog_break is not None else None
        )
        disagreement_value = float(disagreement) if disagreement is not None else None

        rows[str(event_id)] = VersionRow(
            event_id=str(event_id),
            ts_event=ts_event,
            event_day_et=et_day(ts_event),
            regime_bucket=bucket,
            selected_policy=str(selected_policy or "").strip().lower() or "unknown",
            runtime_signal=str(runtime_signal or "").strip().lower() or "unknown",
            chosen_side=side,
            realized_utility=realized,
            model_side_prob=model_prob,
            model_side_margin=(model_prob - threshold) if model_prob is not None and threshold is not None else None,
            analog_side_prob=analog_prob,
            analog_penalty_025=(
                analog_prob - 0.25 * disagreement_value
                if analog_prob is not None and disagreement_value is not None
                else None
            ),
            disagreement=disagreement_value,
        )

    if rows:
        ordered = sorted(rows.values(), key=lambda row: row.ts_event)
        meta["first_event_day_et"] = ordered[0].event_day_et
        meta["last_event_day_et"] = ordered[-1].event_day_et
    else:
        meta["first_event_day_et"] = None
        meta["last_event_day_et"] = None

    meta["eligible_rows"] = len(rows)
    return rows, meta


def main() -> int:
    args = parse_args()
    db_a = Path(args.db_a).expanduser().resolve()
    db_b = Path(args.db_b).expanduser().resolve() if args.db_b else db_a
    out_dir = Path(args.out_dir).expanduser().resolve()

    validate_db_path(db_a)
    validate_db_path(db_b)

    rows_a, meta_a = load_rows(
        db_a,
        symbol=args.symbol,
        horizon=args.horizon,
        source=args.source,
        prediction_basis=args.prediction_basis,
        model_version=args.version_a,
        cost_bps=args.cost_bps,
        max_pred_lag_hours=args.max_pred_lag_hours,
    )
    rows_b, meta_b = load_rows(
        db_b,
        symbol=args.symbol,
        horizon=args.horizon,
        source=args.source,
        prediction_basis=args.prediction_basis,
        model_version=args.version_b,
        cost_bps=args.cost_bps,
        max_pred_lag_hours=args.max_pred_lag_hours,
    )

    common_ids = sorted(set(rows_a) & set(rows_b))
    only_a = sorted(set(rows_a) - set(rows_b))
    only_b = sorted(set(rows_b) - set(rows_a))

    signal_same = 0
    policy_same = 0
    bucket_same = 0
    side_same = 0
    margin_deltas: list[float] = []
    prob_deltas: list[float] = []
    disagreement_deltas: list[float] = []
    improved_margin_utils: list[float] = []
    worsened_margin_utils: list[float] = []
    top_changes: list[dict[str, Any]] = []

    for event_id in common_ids:
        row_a = rows_a[event_id]
        row_b = rows_b[event_id]

        signal_same += int(row_a.runtime_signal == row_b.runtime_signal)
        policy_same += int(row_a.selected_policy == row_b.selected_policy)
        bucket_same += int(row_a.regime_bucket == row_b.regime_bucket)
        side_same += int(row_a.chosen_side == row_b.chosen_side)

        margin_delta = None
        if row_a.model_side_margin is not None and row_b.model_side_margin is not None:
            margin_delta = row_b.model_side_margin - row_a.model_side_margin
            margin_deltas.append(margin_delta)
            if margin_delta >= 0:
                improved_margin_utils.append(row_b.realized_utility)
            else:
                worsened_margin_utils.append(row_b.realized_utility)

        if row_a.model_side_prob is not None and row_b.model_side_prob is not None:
            prob_deltas.append(row_b.model_side_prob - row_a.model_side_prob)
        if row_a.disagreement is not None and row_b.disagreement is not None:
            disagreement_deltas.append(row_b.disagreement - row_a.disagreement)

        top_changes.append(
            {
                "event_id": event_id,
                "event_day_et": row_a.event_day_et,
                "bucket_a": row_a.regime_bucket,
                "bucket_b": row_b.regime_bucket,
                "policy_a": row_a.selected_policy,
                "policy_b": row_b.selected_policy,
                "signal_a": row_a.runtime_signal,
                "signal_b": row_b.runtime_signal,
                "chosen_side_a": row_a.chosen_side,
                "chosen_side_b": row_b.chosen_side,
                "model_side_margin_a": row_a.model_side_margin,
                "model_side_margin_b": row_b.model_side_margin,
                "model_side_margin_delta_b_minus_a": margin_delta,
                "realized_utility": row_b.realized_utility,
            }
        )

    top_changes.sort(
        key=lambda row: abs(row["model_side_margin_delta_b_minus_a"])
        if row["model_side_margin_delta_b_minus_a"] is not None
        else -1.0,
        reverse=True,
    )

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": args.symbol,
        "horizon": args.horizon,
        "source": args.source,
        "prediction_basis": args.prediction_basis,
        "version_a": args.version_a,
        "version_b": args.version_b,
        "db_a": str(db_a),
        "db_b": str(db_b),
        "meta_a": meta_a,
        "meta_b": meta_b,
        "overlap": {
            "common_event_rows": len(common_ids),
            "only_a_event_rows": len(only_a),
            "only_b_event_rows": len(only_b),
            "same_runtime_signal_pct": 100.0 * signal_same / len(common_ids) if common_ids else None,
            "same_selected_policy_pct": 100.0 * policy_same / len(common_ids) if common_ids else None,
            "same_bucket_pct": 100.0 * bucket_same / len(common_ids) if common_ids else None,
            "same_chosen_side_pct": 100.0 * side_same / len(common_ids) if common_ids else None,
        },
        "delta_summaries": {
            "model_side_margin_delta_b_minus_a": summarize(margin_deltas),
            "model_side_prob_delta_b_minus_a": summarize(prob_deltas),
            "disagreement_delta_b_minus_a": summarize(disagreement_deltas),
            "utility_when_margin_improved_or_flat": summarize(improved_margin_utils),
            "utility_when_margin_worsened": summarize(worsened_margin_utils),
        },
        "top_margin_changes": top_changes[:25],
    }

    lines = [
        (
            f"symbol={args.symbol} horizon={args.horizon} source={args.source} "
            f"prediction_basis={args.prediction_basis} version_a={args.version_a} version_b={args.version_b}"
        ),
        (
            f"common={payload['overlap']['common_event_rows']} only_a={payload['overlap']['only_a_event_rows']} "
            f"only_b={payload['overlap']['only_b_event_rows']}"
        ),
        (
            f"same_signal_pct={payload['overlap']['same_runtime_signal_pct']} "
            f"same_policy_pct={payload['overlap']['same_selected_policy_pct']} "
            f"same_bucket_pct={payload['overlap']['same_bucket_pct']} "
            f"same_side_pct={payload['overlap']['same_chosen_side_pct']}"
        ),
        (
            f"margin_delta_mean={payload['delta_summaries']['model_side_margin_delta_b_minus_a']['mean']} "
            f"margin_delta_p05={payload['delta_summaries']['model_side_margin_delta_b_minus_a']['p05']} "
            f"margin_delta_p95={payload['delta_summaries']['model_side_margin_delta_b_minus_a']['p95']}"
        ),
        (
            f"utility_if_margin_improved={payload['delta_summaries']['utility_when_margin_improved_or_flat']['mean']} "
            f"utility_if_margin_worsened={payload['delta_summaries']['utility_when_margin_worsened']['mean']}"
        ),
    ]

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = (
        f"{args.symbol.lower()}_h{args.horizon}_candidate_version_diff_"
        f"{args.version_a}_vs_{args.version_b}"
    )
    if args.source != "all":
        stem += f"_{args.source}"
    json_path = out_dir / f"{stem}.json"
    txt_path = out_dir / f"{stem}_summary.txt"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"wrote {json_path}")
    print(f"wrote {txt_path}")
    print()
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
