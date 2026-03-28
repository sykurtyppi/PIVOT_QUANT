#!/usr/bin/env python3
"""Ranked backtest for regime-conditioned side selection on scored prediction rows.

This is the next layer after the plain regime-side policy benchmark:
- use first (or latest) prediction per event
- choose side from the regime bucket
- rank eligible rows by model / analog / blended aligned-side scores
- evaluate top-percentile slices with day-block bootstrap
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sqlite3
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = Path(os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite")))
DEFAULT_OUT_DIR = ROOT / "logs" / "reports" / "research"
ET = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class RankedRow:
    event_id: str
    ts_event: int
    ts_prediction: int
    event_day_et: str
    regime_bucket: str
    selected_policy: str
    signal_60m: str
    chosen_side: str
    realized_utility: float
    model_side_prob: float | None
    model_side_margin: float | None
    analog_side_prob: float | None
    analog_penalty_025: float | None
    blend_30_70_penalty_025: float | None
    disagreement: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank regime-conditioned side policies on prediction rows.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite DB path.")
    parser.add_argument("--symbol", default="SPY", help="Symbol to analyze.")
    parser.add_argument("--horizon", type=int, default=60, help="Single horizon in minutes.")
    parser.add_argument(
        "--source",
        choices=("live", "preview", "all"),
        default="live",
        help="Prediction source filter.",
    )
    parser.add_argument(
        "--prediction-basis",
        choices=("first", "latest"),
        default="first",
        help="Which prediction row to use per event.",
    )
    parser.add_argument("--model-version", default="", help="Optional model_version filter.")
    parser.add_argument("--cost-bps", type=float, default=1.3, help="Per-trade cost in bps.")
    parser.add_argument(
        "--percentiles",
        default="0.1,0.2,0.3,0.5,1.0",
        help="Comma-separated retained fractions.",
    )
    parser.add_argument("--bootstrap-days", type=int, default=20, help="Trading days per bootstrap path.")
    parser.add_argument("--bootstrap-sims", type=int, default=5000, help="Bootstrap iterations.")
    parser.add_argument("--seed", type=int, default=20260328, help="Random seed.")
    parser.add_argument(
        "--max-pred-lag-hours",
        type=float,
        default=6.0,
        help="Maximum prediction lag from event time.",
    )
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory.")
    return parser.parse_args()


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


def summarize_utils(utils: list[float]) -> dict[str, float | int | None]:
    if not utils:
        return {
            "selected_rows": 0,
            "avg_utility": None,
            "total_utility": 0.0,
            "win_rate_pct": None,
            "p05": None,
            "p50": None,
            "p95": None,
        }
    return {
        "selected_rows": len(utils),
        "avg_utility": float(statistics.fmean(utils)),
        "total_utility": float(sum(utils)),
        "win_rate_pct": 100.0 * sum(1 for u in utils if u > 0) / len(utils),
        "p05": percentile(utils, 0.05),
        "p50": percentile(utils, 0.50),
        "p95": percentile(utils, 0.95),
    }


def chosen_side(bucket: str) -> str:
    if bucket == "compression":
        return "break"
    if bucket == "expansion":
        return "reject"
    return "abstain"


def source_filter_sql(source: str) -> tuple[str, tuple[Any, ...]]:
    if source == "all":
        return "", ()
    if source == "live":
        return "AND COALESCE(pl.is_preview, 0) = 0", ()
    return "AND COALESCE(pl.is_preview, 0) = 1", ()


def load_rows(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    horizon: int,
    source: str,
    prediction_basis: str,
    model_version: str,
    cost_bps: float,
    max_pred_lag_hours: float,
) -> tuple[list[RankedRow], dict[str, Any]]:
    source_sql, _ = source_filter_sql(source)
    order = "ASC" if prediction_basis == "first" else "DESC"
    params: list[Any] = [symbol]
    model_sql = ""
    if model_version.strip():
        model_sql = "AND COALESCE(pl.model_version, '') = ?"
        params.append(model_version.strip())
    params.append(horizon)

    sql = f"""
        WITH scoped_pred AS (
            SELECT *
            FROM (
                SELECT
                    pl.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY pl.event_id
                        ORDER BY pl.ts_prediction {order}
                    ) AS rn
                FROM prediction_log pl
                JOIN touch_events te
                  ON te.event_id = pl.event_id
                WHERE te.symbol = ?
                  {source_sql}
                  {model_sql}
            )
            WHERE rn = 1
        )
        SELECT
            te.event_id,
            te.ts_event,
            te.regime_type,
            sp.ts_prediction,
            sp.model_version,
            COALESCE(sp.is_preview, 0) AS is_preview,
            sp.trade_regime,
            sp.selected_policy,
            sp.signal_60m,
            sp.prob_reject_60m,
            sp.prob_break_60m,
            sp.threshold_reject_60m,
            sp.threshold_break_60m,
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
    fetched = conn.execute(sql, params).fetchall()

    max_lag_ms = int(max_pred_lag_hours * 3600 * 1000)
    rows: list[RankedRow] = []
    meta = {
        "prediction_rows_joined": len(fetched),
        "prediction_basis": prediction_basis,
        "source": source,
        "model_version_filter": model_version or None,
        "max_pred_lag_hours": max_pred_lag_hours,
        "dropped_for_lag": 0,
        "dropped_for_abstain": 0,
    }

    for fetched_row in fetched:
        (
            event_id,
            ts_event,
            regime_type_value,
            ts_prediction,
            _model_version,
            _is_preview,
            trade_regime,
            selected_policy,
            signal_60m,
            prob_reject_60m,
            prob_break_60m,
            threshold_reject_60m,
            threshold_break_60m,
            analog_best_reject_prob,
            analog_best_break_prob,
            analog_best_disagreement,
            return_bps,
        ) = fetched_row

        ts_event = int(ts_event)
        ts_prediction = int(ts_prediction)
        lag_ms = ts_prediction - ts_event
        if lag_ms < 0 or lag_ms > max_lag_ms:
            meta["dropped_for_lag"] += 1
            continue

        bucket = regime_bucket_from_event(regime_type_value, trade_regime)
        side = chosen_side(bucket)
        if side == "abstain":
            meta["dropped_for_abstain"] += 1
            continue

        ret = float(return_bps)
        reject_utility = ret - cost_bps
        break_utility = -ret - cost_bps
        realized = reject_utility if side == "reject" else break_utility

        model_prob = float(prob_reject_60m) if side == "reject" and prob_reject_60m is not None else (
            float(prob_break_60m) if side == "break" and prob_break_60m is not None else None
        )
        threshold = float(threshold_reject_60m) if side == "reject" and threshold_reject_60m is not None else (
            float(threshold_break_60m) if side == "break" and threshold_break_60m is not None else None
        )
        analog_prob = float(analog_best_reject_prob) if side == "reject" and analog_best_reject_prob is not None else (
            float(analog_best_break_prob) if side == "break" and analog_best_break_prob is not None else None
        )
        disagreement = float(analog_best_disagreement) if analog_best_disagreement is not None else None

        rows.append(
            RankedRow(
                event_id=str(event_id),
                ts_event=ts_event,
                ts_prediction=ts_prediction,
                event_day_et=et_day(ts_event),
                regime_bucket=bucket,
                selected_policy=str(selected_policy or "").strip().lower() or "unknown",
                signal_60m=str(signal_60m or "").strip().lower() or "unknown",
                chosen_side=side,
                realized_utility=realized,
                model_side_prob=model_prob,
                model_side_margin=(model_prob - threshold) if model_prob is not None and threshold is not None else None,
                analog_side_prob=analog_prob,
                analog_penalty_025=(
                    analog_prob - 0.25 * disagreement
                    if analog_prob is not None and disagreement is not None
                    else None
                ),
                blend_30_70_penalty_025=(
                    0.3 * model_prob + 0.7 * analog_prob - 0.25 * disagreement
                    if model_prob is not None and analog_prob is not None and disagreement is not None
                    else None
                ),
                disagreement=disagreement,
            )
        )

    if rows:
        meta["first_event_day_et"] = rows[0].event_day_et
        meta["last_event_day_et"] = rows[-1].event_day_et
    else:
        meta["first_event_day_et"] = None
        meta["last_event_day_et"] = None
    return rows, meta


def bootstrap_totals(rows: list[RankedRow], bootstrap_days: int, bootstrap_sims: int, seed: int) -> dict[str, float | None]:
    if not rows:
        return {
            "bootstrap_days": bootstrap_days,
            "bootstrap_sims": bootstrap_sims,
            "mean_total_utility": None,
            "p05_total_utility": None,
            "p50_total_utility": None,
            "p95_total_utility": None,
            "positive_total_rate_pct": None,
        }
    rng = random.Random(seed)
    by_day: dict[str, float] = {}
    for row in rows:
        by_day[row.event_day_et] = by_day.get(row.event_day_et, 0.0) + row.realized_utility
    days = sorted(by_day)
    totals = []
    for _ in range(bootstrap_sims):
        chosen = [rng.choice(days) for _ in range(bootstrap_days)]
        totals.append(sum(by_day[day] for day in chosen))
    return {
        "bootstrap_days": bootstrap_days,
        "bootstrap_sims": bootstrap_sims,
        "mean_total_utility": float(statistics.fmean(totals)),
        "p05_total_utility": percentile(totals, 0.05),
        "p50_total_utility": percentile(totals, 0.50),
        "p95_total_utility": percentile(totals, 0.95),
        "positive_total_rate_pct": 100.0 * sum(1 for v in totals if v > 0) / len(totals),
    }


def evaluate_method(
    rows: list[RankedRow],
    *,
    method: str,
    percentiles: list[float],
    bootstrap_days: int,
    bootstrap_sims: int,
    seed: int,
) -> list[dict[str, Any]]:
    available = [row for row in rows if getattr(row, method) is not None]
    ranked = sorted(available, key=lambda row: getattr(row, method), reverse=True)
    results: list[dict[str, Any]] = []
    for pct in percentiles:
        take_n = max(1, math.ceil(len(ranked) * pct)) if ranked else 0
        selected = ranked[:take_n]
        utils = [row.realized_utility for row in selected]
        summary = summarize_utils(utils)
        regime_mix = {
            bucket: sum(1 for row in selected if row.regime_bucket == bucket)
            for bucket in ("compression", "expansion", "neutral")
        }
        summary.update(
            {
                "method": method,
                "retain_pct": pct,
                "available_rows": len(ranked),
                "selected_rows": len(selected),
                "avg_score": float(statistics.fmean(getattr(row, method) for row in selected))
                if selected
                else None,
                "avg_disagreement": float(statistics.fmean(row.disagreement for row in selected if row.disagreement is not None))
                if any(row.disagreement is not None for row in selected)
                else None,
                "regime_mix": regime_mix,
                "bootstrap": bootstrap_totals(selected, bootstrap_days, bootstrap_sims, seed + int(pct * 1000)),
            }
        )
        results.append(summary)
    return results


def build_summary_lines(payload: dict[str, Any], methods: list[str]) -> list[str]:
    lines = []
    lines.append(
        f"symbol={payload['symbol']} horizon={payload['horizon']} source={payload['source']} "
        f"prediction_basis={payload['prediction_basis']} model_version={payload['model_version_filter']}"
    )
    lines.append(
        f"coverage={payload['coverage']['first_event_day_et']} -> {payload['coverage']['last_event_day_et']} "
        f"eligible_rows={payload['coverage']['eligible_rows']} "
        f"joined_rows={payload['coverage']['prediction_rows_joined']}"
    )
    for method in methods:
        rows = payload["methods"][method]
        best = max(rows, key=lambda row: (row["avg_utility"] if row["avg_utility"] is not None else float("-inf")))
        lines.append(
            f"{method}: best_pct={best['retain_pct']} selected={best['selected_rows']} "
            f"avg={best['avg_utility']} total={best['total_utility']} "
            f"win_rate={best['win_rate_pct']} "
            f"boot_mean={best['bootstrap']['mean_total_utility']} "
            f"boot_p05={best['bootstrap']['p05_total_utility']}"
        )
    return lines


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    percentiles = [float(token.strip()) for token in args.percentiles.split(",") if token.strip()]

    conn = sqlite3.connect(str(db_path))
    try:
        rows, meta = load_rows(
            conn,
            symbol=args.symbol,
            horizon=args.horizon,
            source=args.source,
            prediction_basis=args.prediction_basis,
            model_version=args.model_version,
            cost_bps=args.cost_bps,
            max_pred_lag_hours=args.max_pred_lag_hours,
        )
    finally:
        conn.close()

    methods = [
        "model_side_prob",
        "model_side_margin",
        "analog_side_prob",
        "analog_penalty_025",
        "blend_30_70_penalty_025",
    ]
    results = {
        method: evaluate_method(
            rows,
            method=method,
            percentiles=percentiles,
            bootstrap_days=args.bootstrap_days,
            bootstrap_sims=args.bootstrap_sims,
            seed=args.seed,
        )
        for method in methods
    }

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "symbol": args.symbol,
        "horizon": args.horizon,
        "source": args.source,
        "prediction_basis": args.prediction_basis,
        "model_version_filter": args.model_version or None,
        "cost_bps": args.cost_bps,
        "coverage": {
            **meta,
            "eligible_rows": len(rows),
        },
        "methods": results,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{args.symbol.lower()}_h{args.horizon}_regime_side_ranked_backtest"
    if args.source != "all":
        stem += f"_{args.source}"
    if args.model_version.strip():
        stem += f"_{args.model_version.strip()}"

    json_path = out_dir / f"{stem}.json"
    txt_path = out_dir / f"{stem}_summary.txt"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    summary_lines = build_summary_lines(payload, methods)
    txt_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(f"wrote {json_path}")
    print(f"wrote {txt_path}")
    print()
    print("\n".join(summary_lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
