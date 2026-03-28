#!/usr/bin/env python3
"""Benchmark simple regime-conditioned side-selection policies.

This script is intentionally simple:
- uses event labels only (no model scores)
- evaluates fixed side policies on a chosen horizon
- reports overall, by-regime-bucket, by-month, and day-block bootstrap results

It is meant to answer the next research question after the Markov pass:
"Is a regime-conditioned side rule materially better than always-break or
always-reject on the imported Mini research DB?"
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import statistics
from collections import defaultdict
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
class EventRow:
    event_id: str
    ts_event: int
    event_day_et: str
    month_et: str
    regime_bucket: str
    return_bps: float
    reject_utility: float
    break_utility: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest simple regime-side policies.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite DB path.")
    parser.add_argument("--symbol", default="SPY", help="Symbol to analyze.")
    parser.add_argument("--horizon", type=int, default=60, help="Single horizon in minutes.")
    parser.add_argument("--cost-bps", type=float, default=1.3, help="Per-trade cost in bps.")
    parser.add_argument("--bootstrap-days", type=int, default=20, help="Trading days per bootstrap path.")
    parser.add_argument("--bootstrap-sims", type=int, default=5000, help="Bootstrap iterations.")
    parser.add_argument("--seed", type=int, default=20260328, help="Random seed.")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory.")
    return parser.parse_args()


def regime_bucket(regime_type_value: Any) -> str:
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


def et_month(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(ET).strftime("%Y-%m")


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    pos = (len(ordered) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - lo
    return float(ordered[lo] * (1.0 - frac) + ordered[hi] * frac)


def summarize_utils(utils: list[float]) -> dict[str, float | int | None]:
    if not utils:
        return {
            "trades": 0,
            "avg_utility": None,
            "total_utility": 0.0,
            "win_rate_pct": None,
            "p05": None,
            "p50": None,
            "p95": None,
        }
    return {
        "trades": len(utils),
        "avg_utility": float(statistics.fmean(utils)),
        "total_utility": float(sum(utils)),
        "win_rate_pct": 100.0 * sum(1 for u in utils if u > 0) / len(utils),
        "p05": percentile(utils, 0.05),
        "p50": percentile(utils, 0.50),
        "p95": percentile(utils, 0.95),
    }


def policy_side(policy: str, bucket: str) -> str:
    if policy == "always_reject":
        return "reject"
    if policy == "always_break":
        return "break"
    if policy == "regime_side_abstain":
        if bucket == "compression":
            return "break"
        if bucket == "expansion":
            return "reject"
        return "abstain"
    if policy == "regime_side_break_neutral":
        if bucket == "compression":
            return "break"
        if bucket == "expansion":
            return "reject"
        return "break"
    if policy == "regime_side_reject_neutral":
        if bucket == "compression":
            return "break"
        if bucket == "expansion":
            return "reject"
        return "reject"
    raise ValueError(f"unsupported policy: {policy}")


def realized_utility(row: EventRow, side: str) -> float | None:
    if side == "reject":
        return row.reject_utility
    if side == "break":
        return row.break_utility
    if side == "abstain":
        return None
    raise ValueError(f"unsupported side: {side}")


def load_rows(conn: sqlite3.Connection, symbol: str, horizon: int, cost_bps: float) -> list[EventRow]:
    rows = conn.execute(
        """
        SELECT te.event_id, te.ts_event, te.regime_type, el.return_bps
        FROM touch_events te
        JOIN event_labels el
          ON el.event_id = te.event_id
        WHERE te.symbol = ?
          AND el.horizon_min = ?
        ORDER BY te.ts_event
        """,
        (symbol, horizon),
    ).fetchall()

    out = []
    for event_id, ts_event, regime_type_value, return_bps in rows:
        ts_event = int(ts_event)
        ret = float(return_bps)
        out.append(
            EventRow(
                event_id=str(event_id),
                ts_event=ts_event,
                event_day_et=et_day(ts_event),
                month_et=et_month(ts_event),
                regime_bucket=regime_bucket(regime_type_value),
                return_bps=ret,
                reject_utility=ret - cost_bps,
                break_utility=-ret - cost_bps,
            )
        )
    return out


def summarize_policy(rows: list[EventRow], policy: str) -> dict[str, Any]:
    overall_utils: list[float] = []
    by_bucket_rows: dict[str, list[float]] = defaultdict(list)
    by_month_rows: dict[str, list[float]] = defaultdict(list)
    abstains = 0

    for row in rows:
        side = policy_side(policy, row.regime_bucket)
        util = realized_utility(row, side)
        if util is None:
            abstains += 1
            continue
        overall_utils.append(util)
        by_bucket_rows[row.regime_bucket].append(util)
        by_month_rows[row.month_et].append(util)

    by_bucket = {
        bucket: summarize_utils(utils)
        for bucket, utils in sorted(by_bucket_rows.items())
    }
    by_month = [
        {"month_et": month, **summarize_utils(utils)}
        for month, utils in sorted(by_month_rows.items())
    ]
    return {
        "policy": policy,
        "event_rows": len(rows),
        "abstain_rows": abstains,
        "abstain_rate_pct": 100.0 * abstains / len(rows) if rows else None,
        "overall": summarize_utils(overall_utils),
        "by_bucket": by_bucket,
        "by_month": by_month,
    }


def build_day_policy_map(rows: list[EventRow], policies: list[str]) -> dict[str, dict[str, float]]:
    day_map: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        for policy in policies:
            side = policy_side(policy, row.regime_bucket)
            util = realized_utility(row, side)
            if util is not None:
                day_map[row.event_day_et][policy] += util
    return day_map


def bootstrap_day_paths(
    rows: list[EventRow],
    policies: list[str],
    bootstrap_days: int,
    bootstrap_sims: int,
    seed: int,
) -> dict[str, Any]:
    rng = random.Random(seed)
    day_map = build_day_policy_map(rows, policies)
    all_days = sorted(day_map)
    distributions: dict[str, list[float]] = {policy: [] for policy in policies}

    if not all_days:
        return {
            policy: {
                "bootstrap_days": bootstrap_days,
                "bootstrap_sims": bootstrap_sims,
                "mean_total_utility": None,
                "p05_total_utility": None,
                "p50_total_utility": None,
                "p95_total_utility": None,
                "positive_total_rate_pct": None,
            }
            for policy in policies
        }

    for _ in range(bootstrap_sims):
        chosen_days = [rng.choice(all_days) for _ in range(bootstrap_days)]
        for policy in policies:
            total = sum(day_map[day].get(policy, 0.0) for day in chosen_days)
            distributions[policy].append(float(total))

    out = {}
    for policy, totals in distributions.items():
        out[policy] = {
            "bootstrap_days": bootstrap_days,
            "bootstrap_sims": bootstrap_sims,
            "mean_total_utility": float(statistics.fmean(totals)) if totals else None,
            "p05_total_utility": percentile(totals, 0.05),
            "p50_total_utility": percentile(totals, 0.50),
            "p95_total_utility": percentile(totals, 0.95),
            "positive_total_rate_pct": 100.0 * sum(1 for v in totals if v > 0) / len(totals) if totals else None,
        }
    return out


def build_summary_lines(payload: dict[str, Any], policies: list[str]) -> list[str]:
    lines = []
    lines.append(
        f"symbol={payload['symbol']} horizon={payload['horizon']} cost_bps={payload['cost_bps']}"
    )
    lines.append(
        f"coverage={payload['coverage']['first_day_et']} -> {payload['coverage']['last_day_et']} "
        f"({payload['coverage']['event_rows']} rows)"
    )
    for policy in policies:
        overall = payload["policies"][policy]["overall"]
        boot = payload["bootstrap"][policy]
        lines.append(
            f"{policy}: trades={overall['trades']} "
            f"avg={overall['avg_utility']} total={overall['total_utility']} "
            f"win_rate={overall['win_rate_pct']} "
            f"boot_mean={boot['mean_total_utility']} "
            f"boot_p05={boot['p05_total_utility']} "
            f"boot_p95={boot['p95_total_utility']}"
        )
    return lines


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    policies = [
        "always_reject",
        "always_break",
        "regime_side_abstain",
        "regime_side_break_neutral",
        "regime_side_reject_neutral",
    ]

    conn = sqlite3.connect(str(db_path))
    try:
        rows = load_rows(conn, args.symbol, args.horizon, args.cost_bps)
    finally:
        conn.close()

    first_day = rows[0].event_day_et if rows else None
    last_day = rows[-1].event_day_et if rows else None
    policy_results = {policy: summarize_policy(rows, policy) for policy in policies}
    bootstrap_results = bootstrap_day_paths(
        rows=rows,
        policies=policies,
        bootstrap_days=args.bootstrap_days,
        bootstrap_sims=args.bootstrap_sims,
        seed=args.seed,
    )

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "symbol": args.symbol,
        "horizon": args.horizon,
        "cost_bps": args.cost_bps,
        "coverage": {
            "event_rows": len(rows),
            "first_day_et": first_day,
            "last_day_et": last_day,
        },
        "policies": policy_results,
        "bootstrap": bootstrap_results,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{args.symbol.lower()}_h{args.horizon}_regime_side_policy_backtest"
    json_path = out_dir / f"{stem}.json"
    txt_path = out_dir / f"{stem}_summary.txt"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    summary_lines = build_summary_lines(payload, policies)
    txt_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(f"wrote {json_path}")
    print(f"wrote {txt_path}")
    print()
    print("\n".join(summary_lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
