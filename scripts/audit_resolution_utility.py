#!/usr/bin/env python3
"""Audit utility alignment for horizon-close vs resolution-based exits.

This script is intentionally read-only. It helps answer:
1) Is `resolution_min` coverage high enough to trust resolution-based audits?
2) Are rows with missing `resolution_min` systematically worse (selection bias risk)?
3) How different is reject/break utility when evaluated at horizon close vs resolution time?
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from statistics import mean
from typing import Iterable


DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
DEFAULT_HORIZONS = "5,15,30,60"
DEFAULT_TARGET = "reject"


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _default_trade_cost_bps() -> float:
    return (
        _env_float("ML_COST_SPREAD_BPS", 0.8)
        + _env_float("ML_COST_SLIPPAGE_BPS", 0.4)
        + _env_float("ML_COST_COMMISSION_BPS", 0.1)
    )


def _safe_mean(values: Iterable[float]) -> float | None:
    data = [float(v) for v in values]
    if not data:
        return None
    return float(mean(data))


def _safe_pct(numer: int, denom: int) -> float | None:
    if denom <= 0:
        return None
    return float(100.0 * numer / denom)


def _fmt(value: float | int | None, digits: int = 3) -> str:
    if value is None:
        return "na"
    if isinstance(value, int):
        return str(value)
    return f"{float(value):.{digits}f}"


def parse_horizons(raw: str) -> list[int]:
    out: list[int] = []
    for token in str(raw).split(","):
        t = token.strip()
        if not t:
            continue
        out.append(int(t))
    if not out:
        raise ValueError("No valid horizons configured.")
    return sorted(set(out))


@dataclass
class UtilitySummary:
    total: int
    has_resolution: int
    matched_resolution_bar: int
    resolved_early: int
    horizon_net_all: float | None
    horizon_win_all: float | None
    horizon_net_has_resolution: float | None
    horizon_win_has_resolution: float | None
    horizon_net_no_resolution: float | None
    horizon_win_no_resolution: float | None
    resolution_net_matched: float | None
    resolution_win_matched: float | None
    timing_delta_resolution_vs_horizon: float | None
    selection_bias_gap_has_vs_missing: float | None


def summarize_utility_rows(
    rows: list[sqlite3.Row],
    *,
    target: str,
    trade_cost_bps: float,
) -> UtilitySummary:
    if target not in {"reject", "break"}:
        raise ValueError(f"Unsupported target: {target}")
    direction = 1.0 if target == "reject" else -1.0

    horizon_all: list[float] = []
    horizon_has_res: list[float] = []
    horizon_no_res: list[float] = []
    resolution_vals: list[float] = []
    horizon_for_resolution_vals: list[float] = []

    has_resolution = 0
    matched_resolution_bar = 0
    resolved_early = 0

    for row in rows:
        side = row["touch_side"]
        ret = row["return_bps"]
        touch_price = row["touch_price"]
        resolution_min = row["resolution_min"]
        resolution_close = row["resolution_close"]
        horizon_min = int(row["horizon_min"])

        if side not in (-1, 1):
            continue
        if ret is None:
            continue

        directional_horizon = float(ret) * float(side)
        horizon_net = direction * directional_horizon - float(trade_cost_bps)
        horizon_all.append(horizon_net)

        row_has_resolution = resolution_min is not None
        if row_has_resolution:
            has_resolution += 1
            if float(resolution_min) < float(horizon_min):
                resolved_early += 1
            horizon_has_res.append(horizon_net)
        else:
            horizon_no_res.append(horizon_net)

        if (
            row_has_resolution
            and resolution_close is not None
            and touch_price is not None
            and float(touch_price) > 0.0
        ):
            matched_resolution_bar += 1
            directional_resolution = (
                (float(resolution_close) - float(touch_price)) / float(touch_price) * 1e4 * float(side)
            )
            resolution_net = direction * directional_resolution - float(trade_cost_bps)
            resolution_vals.append(resolution_net)
            horizon_for_resolution_vals.append(horizon_net)

    def _win_rate(values: list[float]) -> float | None:
        if not values:
            return None
        wins = sum(1 for v in values if float(v) > 0.0)
        return _safe_pct(wins, len(values))

    horizon_net_all = _safe_mean(horizon_all)
    horizon_net_has_resolution = _safe_mean(horizon_has_res)
    horizon_net_no_resolution = _safe_mean(horizon_no_res)
    resolution_net_matched = _safe_mean(resolution_vals)

    timing_delta = None
    if resolution_vals and horizon_for_resolution_vals:
        timing_delta = _safe_mean(resolution_vals) - _safe_mean(horizon_for_resolution_vals)

    selection_bias_gap = None
    if horizon_has_res and horizon_no_res:
        selection_bias_gap = _safe_mean(horizon_has_res) - _safe_mean(horizon_no_res)

    return UtilitySummary(
        total=len(horizon_all),
        has_resolution=has_resolution,
        matched_resolution_bar=matched_resolution_bar,
        resolved_early=resolved_early,
        horizon_net_all=horizon_net_all,
        horizon_win_all=_win_rate(horizon_all),
        horizon_net_has_resolution=horizon_net_has_resolution,
        horizon_win_has_resolution=_win_rate(horizon_has_res),
        horizon_net_no_resolution=horizon_net_no_resolution,
        horizon_win_no_resolution=_win_rate(horizon_no_res),
        resolution_net_matched=resolution_net_matched,
        resolution_win_matched=_win_rate(resolution_vals),
        timing_delta_resolution_vs_horizon=timing_delta,
        selection_bias_gap_has_vs_missing=selection_bias_gap,
    )


def fetch_overall_resolution_coverage(conn: sqlite3.Connection, horizons: list[int]) -> sqlite3.Row:
    placeholders = ",".join("?" for _ in horizons)
    sql = f"""
        SELECT
          COUNT(*) AS total_labels,
          SUM(CASE WHEN resolution_min IS NOT NULL THEN 1 ELSE 0 END) AS has_resolution
        FROM event_labels
        WHERE horizon_min IN ({placeholders})
    """
    return conn.execute(sql, horizons).fetchone()


def fetch_horizon_resolution_coverage(conn: sqlite3.Connection, horizons: list[int]) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in horizons)
    sql = f"""
        SELECT
          horizon_min,
          COUNT(*) AS total_labels,
          SUM(CASE WHEN resolution_min IS NOT NULL THEN 1 ELSE 0 END) AS has_resolution,
          SUM(CASE WHEN reject = 1 THEN 1 ELSE 0 END) AS reject_labels,
          SUM(CASE WHEN break = 1 THEN 1 ELSE 0 END) AS break_labels
        FROM event_labels
        WHERE horizon_min IN ({placeholders})
        GROUP BY horizon_min
        ORDER BY horizon_min
    """
    return conn.execute(sql, horizons).fetchall()


def fetch_stratification(conn: sqlite3.Connection, horizons: list[int]) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in horizons)
    sql = f"""
        SELECT
          el.horizon_min,
          CASE WHEN el.resolution_min IS NOT NULL THEN 'has_resolution' ELSE 'no_resolution' END AS grp,
          COUNT(*) AS n,
          AVG(CASE WHEN el.reject = 1 THEN 1.0 ELSE 0.0 END) AS reject_rate,
          AVG(CASE WHEN el.break = 1 THEN 1.0 ELSE 0.0 END) AS break_rate,
          AVG(el.mfe_bps) AS avg_mfe_bps,
          AVG(el.mae_bps) AS avg_mae_bps
        FROM event_labels el
        WHERE el.horizon_min IN ({placeholders})
        GROUP BY el.horizon_min, grp
        ORDER BY el.horizon_min, grp DESC
    """
    return conn.execute(sql, horizons).fetchall()


def fetch_target_rows(conn: sqlite3.Connection, horizons: list[int], target: str) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in horizons)
    label_col = "reject" if target == "reject" else "break"
    sql = f"""
        WITH base AS (
          SELECT
            el.event_id,
            el.horizon_min,
            el.return_bps,
            el.mfe_bps,
            el.mae_bps,
            el.resolution_min,
            te.symbol,
            te.ts_event,
            te.touch_price,
            te.touch_side,
            te.bar_interval_sec,
            CAST(te.ts_event + ROUND(el.resolution_min * 60000.0) AS INTEGER) AS resolution_ts
          FROM event_labels el
          JOIN touch_events te ON te.event_id = el.event_id
          WHERE el.horizon_min IN ({placeholders})
            AND el.{label_col} = 1
            AND te.touch_side IN (1, -1)
        ),
        candidates AS (
          SELECT
            b.*,
            bd.close AS resolution_close,
            ROW_NUMBER() OVER (
              PARTITION BY b.event_id, b.horizon_min
              ORDER BY
                CASE
                  WHEN b.bar_interval_sec IS NOT NULL AND bd.bar_interval_sec = b.bar_interval_sec THEN 0
                  WHEN b.bar_interval_sec IS NULL THEN 0
                  ELSE 1
                END,
                COALESCE(bd.bar_interval_sec, 999999)
            ) AS rn
          FROM base b
          LEFT JOIN bar_data bd
            ON bd.symbol = b.symbol
           AND bd.ts = b.resolution_ts
        )
        SELECT
          event_id,
          horizon_min,
          return_bps,
          mfe_bps,
          mae_bps,
          resolution_min,
          touch_price,
          touch_side,
          resolution_close
        FROM candidates
        WHERE rn = 1
        ORDER BY horizon_min, event_id
    """
    return conn.execute(sql, horizons).fetchall()


def print_report(
    *,
    db_path: str,
    target: str,
    trade_cost_bps: float,
    overall: sqlite3.Row,
    by_horizon: list[sqlite3.Row],
    strat_rows: list[sqlite3.Row],
    target_rows: list[sqlite3.Row],
) -> None:
    total = int(overall["total_labels"] or 0)
    has_res = int(overall["has_resolution"] or 0)
    coverage = _safe_pct(has_res, total)

    print(f"DB: {db_path}")
    print(f"Target Utility Audit: {target}")
    print(f"Trade Cost (bps): {_fmt(trade_cost_bps, 3)}")
    print("")
    print("Resolution Coverage")
    print(
        f"- total_labels={total} has_resolution={has_res} coverage_pct={_fmt(coverage, 1)}"
    )
    print("")

    print("Coverage by Horizon")
    for row in by_horizon:
        h = int(row["horizon_min"])
        n = int(row["total_labels"] or 0)
        hres = int(row["has_resolution"] or 0)
        print(
            f"- h={h}m total={n} has_resolution={hres} "
            f"coverage_pct={_fmt(_safe_pct(hres, n), 1)} "
            f"reject_labels={int(row['reject_labels'] or 0)} break_labels={int(row['break_labels'] or 0)}"
        )
    print("")

    print("Resolution Missingness Stratification (All Labels)")
    for row in strat_rows:
        print(
            f"- h={int(row['horizon_min'])}m grp={row['grp']} n={int(row['n'])} "
            f"reject_rate={_fmt(row['reject_rate'])} break_rate={_fmt(row['break_rate'])} "
            f"avg_mfe={_fmt(row['avg_mfe_bps'])} avg_mae={_fmt(row['avg_mae_bps'])}"
        )
    print("")

    print(f"{target.title()} Utility Comparison by Horizon")
    grouped: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for row in target_rows:
        grouped[int(row["horizon_min"])].append(row)

    for horizon in sorted(grouped):
        summary = summarize_utility_rows(
            grouped[horizon], target=target, trade_cost_bps=trade_cost_bps
        )
        matched_cov = _safe_pct(summary.matched_resolution_bar, summary.total)
        has_res_cov = _safe_pct(summary.has_resolution, summary.total)
        early_pct = _safe_pct(summary.resolved_early, summary.has_resolution)
        print(
            f"- h={horizon}m total={summary.total} has_res={summary.has_resolution} "
            f"matched_res_bar={summary.matched_resolution_bar} "
            f"has_res_pct={_fmt(has_res_cov,1)} matched_pct={_fmt(matched_cov,1)} "
            f"resolved_early_pct={_fmt(early_pct,1)}"
        )
        print(
            f"  horizon_net_all={_fmt(summary.horizon_net_all)} "
            f"horizon_win_all_pct={_fmt(summary.horizon_win_all,1)} "
            f"resolution_net_matched={_fmt(summary.resolution_net_matched)} "
            f"resolution_win_matched_pct={_fmt(summary.resolution_win_matched,1)}"
        )
        print(
            f"  horizon_net_has_res={_fmt(summary.horizon_net_has_resolution)} "
            f"horizon_net_no_res={_fmt(summary.horizon_net_no_resolution)} "
            f"selection_bias_gap(has-no)={_fmt(summary.selection_bias_gap_has_vs_missing)} "
            f"timing_delta(res-horizon_same_subset)={_fmt(summary.timing_delta_resolution_vs_horizon)}"
        )

    print("")
    print("Interpretation Guide")
    print(
        "- `selection_bias_gap(has-no)`: if strongly positive, resolution-present rows are easier, "
        "so resolution-only utility can overstate expected edge."
    )
    print(
        "- `timing_delta(res-horizon_same_subset)`: isolates exit-timing effect using the same rows "
        "(positive means horizon-close underestimates utility for resolved trades)."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit resolution_min coverage and utility alignment."
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path.")
    parser.add_argument(
        "--target",
        choices=("reject", "break"),
        default=DEFAULT_TARGET,
        help="Target utility to audit.",
    )
    parser.add_argument(
        "--horizons",
        default=DEFAULT_HORIZONS,
        help="Comma-separated horizons to audit (default: 5,15,30,60).",
    )
    parser.add_argument(
        "--trade-cost-bps",
        type=float,
        default=_default_trade_cost_bps(),
        help="Per-trade cost in bps for utility calculations.",
    )
    args = parser.parse_args()

    horizons = parse_horizons(args.horizons)
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        overall = fetch_overall_resolution_coverage(conn, horizons)
        by_horizon = fetch_horizon_resolution_coverage(conn, horizons)
        strat_rows = fetch_stratification(conn, horizons)
        target_rows = fetch_target_rows(conn, horizons, args.target)
    finally:
        conn.close()

    print_report(
        db_path=args.db,
        target=args.target,
        trade_cost_bps=float(args.trade_cost_bps),
        overall=overall,
        by_horizon=by_horizon,
        strat_rows=strat_rows,
        target_rows=target_rows,
    )


if __name__ == "__main__":
    main()

