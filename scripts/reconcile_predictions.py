#!/usr/bin/env python3
"""Reconcile ML predictions against actual outcomes.

Joins prediction_log to event_labels (built by build_labels.py) to measure
how well the live ML server is performing compared to backtest metrics.

Usage:
    python scripts/reconcile_predictions.py
    python scripts/reconcile_predictions.py --db data/pivot_events.sqlite --horizon 15
    python scripts/reconcile_predictions.py --csv  # export detailed CSV
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    return conn


def reconcile(
    conn: sqlite3.Connection,
    horizon: int | None = None,
    include_preview: bool = False,
    dedupe_policy: str = "latest_event",
    has_preview_column: bool = True,
) -> list[dict]:
    """Join prediction_log to event_labels to get predicted vs actual.

    Returns list of dicts with prediction + outcome fields.
    By default excludes preview predictions (is_preview=1).
    """
    horizon_filter = ""
    if has_preview_column and not include_preview:
        preview_filter = "WHERE is_preview = 0"
    else:
        preview_filter = ""
    params: list = []
    if horizon is not None:
        horizon_filter = "AND el.horizon_min = ?"
        params.append(horizon)

    is_preview_select = "pl.is_preview" if has_preview_column else "0 AS is_preview"

    if dedupe_policy == "none":
        cte = f"""
        WITH pred_source AS (
            SELECT * FROM prediction_log
            {preview_filter}
        )
        """
        rn_filter = ""
    elif dedupe_policy == "latest_model":
        cte = f"""
        WITH pred_source AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id, COALESCE(model_version, '')
                       ORDER BY ts_prediction DESC
                   ) AS rn
            FROM prediction_log
            {preview_filter}
        )
        """
        rn_filter = "AND pl.rn = 1"
    else:
        # latest_event: keep only one latest score per event_id
        cte = f"""
        WITH pred_source AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_id
                       ORDER BY ts_prediction DESC
                   ) AS rn
            FROM prediction_log
            {preview_filter}
        )
        """
        rn_filter = "AND pl.rn = 1"

    sql = f"""
        {cte}
        SELECT
            pl.event_id,
            pl.ts_prediction,
            pl.model_version,
            pl.feature_version,
            pl.best_horizon,
            pl.abstain,
            pl.signal_5m,
            pl.signal_15m,
            pl.signal_60m,
            pl.prob_reject_5m,
            pl.prob_reject_15m,
            pl.prob_reject_60m,
            pl.prob_break_5m,
            pl.prob_break_15m,
            pl.prob_break_60m,
            pl.threshold_reject_5m,
            pl.threshold_reject_15m,
            pl.threshold_reject_60m,
            pl.threshold_break_5m,
            pl.threshold_break_15m,
            pl.threshold_break_60m,
            pl.quality_flags,
            {is_preview_select},
            te.touch_side,
            el.horizon_min,
            el.return_bps,
            el.mfe_bps,
            el.mae_bps,
            el.reject AS actual_reject,
            el.break AS actual_break,
            el.resolution_min
        FROM pred_source pl
        JOIN touch_events te ON pl.event_id = te.event_id
        JOIN event_labels el ON pl.event_id = el.event_id
        WHERE 1=1 {rn_filter} {horizon_filter}
        ORDER BY pl.ts_prediction ASC
    """
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def _compute_cost_metrics(
    subset: list[dict],
    horizon: int,
    spread_bps: float,
    slippage_bps: float,
    commission_bps: float,
) -> dict:
    signal_key = f"signal_{horizon}m"
    round_trip_cost_bps = spread_bps + slippage_bps + commission_bps

    net_returns = []
    for r in subset:
        signal = r.get(signal_key)
        touch_side = r.get("touch_side")
        return_bps = r.get("return_bps")
        if signal not in ("reject", "break"):
            continue
        if touch_side not in (1, -1):
            continue
        if return_bps is None:
            continue

        # Reject trades in touch_side direction; break trades opposite.
        direction = touch_side if signal == "reject" else -touch_side
        gross_bps = direction * return_bps
        net_bps = gross_bps - round_trip_cost_bps
        net_returns.append(net_bps)

    if not net_returns:
        return {
            "cost_bps_round_trip": round(round_trip_cost_bps, 3),
            "trade_count": 0,
            "net_expectancy_bps": None,
            "net_total_bps": None,
            "win_rate": None,
            "profit_factor": None,
            "sharpe_trade": None,
            "max_drawdown_bps": None,
        }

    n = len(net_returns)
    total = sum(net_returns)
    expectancy = total / n
    wins = [x for x in net_returns if x > 0]
    losses = [x for x in net_returns if x <= 0]
    win_rate = len(wins) / n
    gross_wins = sum(wins)
    gross_losses = abs(sum(losses))
    profit_factor = (gross_wins / gross_losses) if gross_losses > 0 else None

    if n > 1:
        mean = expectancy
        variance = sum((x - mean) ** 2 for x in net_returns) / (n - 1)
        std = math.sqrt(variance)
        # Per-trade Sharpe: mean return / std of returns (not * sqrt(n), which is a t-statistic)
        sharpe_trade = mean / std if std > 0 else None
    else:
        sharpe_trade = None

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for ret in net_returns:
        equity += ret
        peak = max(peak, equity)
        drawdown = peak - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    return {
        "cost_bps_round_trip": round(round_trip_cost_bps, 3),
        "trade_count": n,
        "net_expectancy_bps": round(expectancy, 3),
        "net_total_bps": round(total, 3),
        "win_rate": round(win_rate, 3),
        "profit_factor": round(profit_factor, 3) if profit_factor is not None else None,
        "sharpe_trade": round(sharpe_trade, 3) if sharpe_trade is not None else None,
        "max_drawdown_bps": round(max_drawdown, 3),
    }


def compute_metrics(
    records: list[dict],
    horizon: int,
    spread_bps: float,
    slippage_bps: float,
    commission_bps: float,
) -> dict:
    """Compute accuracy metrics for a specific horizon."""
    # Filter to records that have labels for this horizon
    subset = [r for r in records if r["horizon_min"] == horizon]
    if not subset:
        return {"horizon": horizon, "n": 0, "message": "No labeled predictions yet"}

    n = len(subset)

    # Signal field
    signal_key = f"signal_{horizon}m"

    # Count signal distribution
    signal_counts = {"reject": 0, "break": 0, "no_edge": 0, "missing": 0}
    for r in subset:
        sig = r.get(signal_key)
        if sig in signal_counts:
            signal_counts[sig] += 1
        else:
            signal_counts["missing"] += 1

    # Reject accuracy: when we predicted reject, was it actually reject?
    reject_preds = [r for r in subset if r.get(signal_key) == "reject"]
    reject_correct = sum(1 for r in reject_preds if r["actual_reject"] == 1)
    reject_precision = reject_correct / len(reject_preds) if reject_preds else None

    # Break accuracy: when we predicted break, was it actually break?
    break_preds = [r for r in subset if r.get(signal_key) == "break"]
    break_correct = sum(1 for r in break_preds if r["actual_break"] == 1)
    break_precision = break_correct / len(break_preds) if break_preds else None

    # Reject recall: of actual rejects, how many did we catch?
    actual_rejects = [r for r in subset if r["actual_reject"] == 1]
    reject_caught = sum(1 for r in actual_rejects if r.get(signal_key) == "reject")
    reject_recall = reject_caught / len(actual_rejects) if actual_rejects else None

    # Break recall: of actual breaks, how many did we catch?
    actual_breaks = [r for r in subset if r["actual_break"] == 1]
    break_caught = sum(1 for r in actual_breaks if r.get(signal_key) == "break")
    break_recall = break_caught / len(actual_breaks) if actual_breaks else None

    # MFE/MAE stats by signal type
    reject_mfes = [r["mfe_bps"] for r in reject_preds if r["mfe_bps"] is not None]
    reject_maes = [r["mae_bps"] for r in reject_preds if r["mae_bps"] is not None]
    break_mfes = [r["mfe_bps"] for r in break_preds if r["mfe_bps"] is not None]

    # Abstain rate
    abstain_count = sum(1 for r in subset if r["abstain"] == 1)

    # Quality flag distribution
    flag_counts: dict[str, int] = {}
    for r in subset:
        flags = r.get("quality_flags")
        if flags:
            try:
                for f in json.loads(flags):
                    flag_counts[f] = flag_counts.get(f, 0) + 1
            except (json.JSONDecodeError, TypeError):
                pass

    return {
        "horizon": horizon,
        "n": n,
        "signal_distribution": signal_counts,
        "abstain_rate": round(abstain_count / n, 3) if n else None,
        "reject_precision": round(reject_precision, 3) if reject_precision is not None else None,
        "reject_recall": round(reject_recall, 3) if reject_recall is not None else None,
        "reject_n": len(reject_preds),
        "break_precision": round(break_precision, 3) if break_precision is not None else None,
        "break_recall": round(break_recall, 3) if break_recall is not None else None,
        "break_n": len(break_preds),
        "actual_reject_rate": round(len(actual_rejects) / n, 3) if n else None,
        "actual_break_rate": round(len(actual_breaks) / n, 3) if n else None,
        "reject_signal_avg_mfe_bps": round(sum(reject_mfes) / len(reject_mfes), 1) if reject_mfes else None,
        "reject_signal_avg_mae_bps": round(sum(reject_maes) / len(reject_maes), 1) if reject_maes else None,
        "break_signal_avg_mfe_bps": round(sum(break_mfes) / len(break_mfes), 1) if break_mfes else None,
        "quality_flag_counts": flag_counts,
        "cost_metrics": _compute_cost_metrics(
            subset,
            horizon,
            spread_bps=spread_bps,
            slippage_bps=slippage_bps,
            commission_bps=commission_bps,
        ),
    }


def print_report(metrics: dict) -> None:
    """Pretty-print a reconciliation report for one horizon."""
    h = metrics["horizon"]
    n = metrics["n"]
    print(f"\n{'='*60}")
    print(f"  Horizon: {h}m  |  Predictions with labels: {n}")
    print(f"{'='*60}")

    if n == 0:
        print("  No labeled predictions yet. Run build_labels.py after")
        print("  enough time has passed for outcomes to materialize.")
        return

    sd = metrics["signal_distribution"]
    print(f"\n  Signal Distribution:")
    print(f"    reject:  {sd.get('reject', 0):>5d}  ({sd.get('reject', 0)/n*100:.1f}%)")
    print(f"    break:   {sd.get('break', 0):>5d}  ({sd.get('break', 0)/n*100:.1f}%)")
    print(f"    no_edge: {sd.get('no_edge', 0):>5d}  ({sd.get('no_edge', 0)/n*100:.1f}%)")
    print(f"    abstain rate: {metrics['abstain_rate']}")

    print(f"\n  Base Rates (actual):")
    print(f"    reject: {metrics['actual_reject_rate']}")
    print(f"    break:  {metrics['actual_break_rate']}")

    print(f"\n  Reject Signal (n={metrics['reject_n']}):")
    rp = metrics['reject_precision']
    rr = metrics['reject_recall']
    print(f"    precision: {rp if rp is not None else 'N/A'}")
    print(f"    recall:    {rr if rr is not None else 'N/A'}")
    if metrics['reject_signal_avg_mfe_bps'] is not None:
        print(f"    avg MFE:   {metrics['reject_signal_avg_mfe_bps']} bps")
        print(f"    avg MAE:   {metrics['reject_signal_avg_mae_bps']} bps")

    print(f"\n  Break Signal (n={metrics['break_n']}):")
    bp = metrics['break_precision']
    br = metrics['break_recall']
    print(f"    precision: {bp if bp is not None else 'N/A'}")
    print(f"    recall:    {br if br is not None else 'N/A'}")
    if metrics['break_signal_avg_mfe_bps'] is not None:
        print(f"    avg MFE:   {metrics['break_signal_avg_mfe_bps']} bps")

    cost = metrics.get("cost_metrics", {})
    if cost and cost.get("trade_count", 0) > 0:
        print(f"\n  Cost-Aware Performance:")
        print(f"    round-trip cost: {cost['cost_bps_round_trip']} bps")
        print(f"    trades:          {cost['trade_count']}")
        print(f"    net expectancy:  {cost['net_expectancy_bps']} bps/trade")
        print(f"    net total:       {cost['net_total_bps']} bps")
        print(f"    win rate:        {cost['win_rate']}")
        print(f"    profit factor:   {cost['profit_factor'] if cost['profit_factor'] is not None else 'N/A'}")
        print(f"    sharpe (trade):  {cost['sharpe_trade'] if cost['sharpe_trade'] is not None else 'N/A'}")
        print(f"    max drawdown:    {cost['max_drawdown_bps']} bps")

    if metrics['quality_flag_counts']:
        print(f"\n  Quality Flags:")
        for flag, count in sorted(metrics['quality_flag_counts'].items(), key=lambda x: -x[1]):
            print(f"    {flag}: {count}")


def export_csv(records: list[dict], output_path: str) -> None:
    """Export reconciled records to CSV for external analysis."""
    if not records:
        print("No records to export.")
        return
    fieldnames = list(records[0].keys())
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"Exported {len(records)} records to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile ML predictions against outcomes.")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path")
    parser.add_argument("--horizon", type=int, default=None,
                        help="Single horizon to analyze (default: all)")
    parser.add_argument("--csv", action="store_true", default=False,
                        help="Export detailed CSV")
    parser.add_argument("--csv-path", default="data/prediction_reconciliation.csv",
                        help="CSV output path")
    parser.add_argument("--include-preview", action="store_true", default=False,
                        help="Include preview predictions (excluded by default)")
    parser.add_argument(
        "--dedupe-policy",
        choices=["latest_event", "latest_model", "none"],
        default="latest_event",
        help="How to dedupe repeated scoring records before evaluation",
    )
    parser.add_argument("--spread-bps", type=float, default=0.8,
                        help="Round-trip spread cost in bps")
    parser.add_argument("--slippage-bps", type=float, default=0.4,
                        help="Round-trip slippage cost in bps")
    parser.add_argument("--commission-bps", type=float, default=0.1,
                        help="Round-trip commissions/fees in bps")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    conn = connect(str(db_path))

    # Check if prediction_log table exists
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "prediction_log" not in tables:
        print("prediction_log table does not exist yet.")
        print("Predictions will be logged once the ML server processes live events.")
        conn.close()
        return
    if "event_labels" not in tables:
        print("event_labels table does not exist yet.")
        print("Run build_labels.py to generate outcome labels.")
        conn.close()
        return

    pred_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(prediction_log)").fetchall()
    }
    has_preview_column = "is_preview" in pred_cols

    # Count raw predictions
    pred_count = conn.execute("SELECT COUNT(*) FROM prediction_log").fetchone()[0]
    if has_preview_column:
        preview_count = conn.execute(
            "SELECT COUNT(*) FROM prediction_log WHERE is_preview = 1"
        ).fetchone()[0]
    else:
        preview_count = 0
    live_count = pred_count - preview_count
    print(f"Total predictions logged: {pred_count}  (live: {live_count}, preview: {preview_count})")
    if not has_preview_column:
        print("  (prediction_log has no is_preview column; treating all predictions as live)")
    elif not args.include_preview:
        print("  (excluding preview predictions — use --include-preview to include)")

    records = reconcile(
        conn,
        horizon=args.horizon,
        include_preview=args.include_preview,
        dedupe_policy=args.dedupe_policy,
        has_preview_column=has_preview_column,
    )
    print(f"Predictions with outcome labels: {len(records)}")

    if args.csv and records:
        export_csv(records, args.csv_path)

    horizons = [args.horizon] if args.horizon else [5, 15, 30, 60]
    for h in horizons:
        metrics = compute_metrics(
            records,
            h,
            spread_bps=args.spread_bps,
            slippage_bps=args.slippage_bps,
            commission_bps=args.commission_bps,
        )
        print_report(metrics)

    conn.close()


if __name__ == "__main__":
    main()
