#!/usr/bin/env python3
"""Mini-side regime Markov and Monte Carlo research harness.

This script treats the imported research DB as the source of truth and
produces two families of outputs:

1. Day-level regime Markov transitions based on touch-event regime labels.
2. Horizon-level net-utility simulations conditioned on regime buckets.

The Monte Carlo uses a day-block approach:
- estimate a day-level bucket transition matrix
- sample future bucket paths from that matrix
- sample historical day utility totals from the chosen bucket

That keeps the first pass fast while still respecting the clustered nature of
touch events.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sqlite3
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = Path(os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite")))
DEFAULT_OUT_DIR = ROOT / "logs" / "reports" / "research"
ET = ZoneInfo("America/New_York")

RAW_REGIME_NAMES = {
    1: "trend_up",
    2: "trend_down",
    3: "range",
    4: "vol_expansion",
}
BUCKET_ORDER = ["compression", "expansion", "neutral"]


@dataclass(frozen=True)
class LabelRow:
    event_id: str
    ts_event: int
    event_day_et: str
    raw_regime_name: str
    regime_bucket: str
    horizon_min: int
    return_bps: float
    reject_utility: float
    break_utility: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run regime Markov + Monte Carlo research.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite DB path.")
    parser.add_argument("--symbol", default="SPY", help="Symbol to analyze.")
    parser.add_argument(
        "--horizons",
        default="5,15,30,60",
        help="Comma-separated horizons in minutes.",
    )
    parser.add_argument("--cost-bps", type=float, default=1.3, help="Per-trade cost in bps.")
    parser.add_argument("--sim-days", type=int, default=20, help="Monte Carlo horizon in trading days.")
    parser.add_argument("--simulations", type=int, default=2000, help="Monte Carlo iterations.")
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


def regime_name(regime_type_value: Any) -> str:
    try:
        regime_type = int(regime_type_value) if regime_type_value is not None else None
    except (TypeError, ValueError):
        regime_type = None
    return RAW_REGIME_NAMES.get(regime_type, "unknown")


def et_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(ET).strftime("%Y-%m-%d")


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(ordered[lo])
    weight = pos - lo
    return float(ordered[lo] * (1.0 - weight) + ordered[hi] * weight)


def summarize_distribution(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {
            "mean": None,
            "stdev": None,
            "p05": None,
            "p50": None,
            "p95": None,
            "positive_rate_pct": None,
        }
    return {
        "mean": float(statistics.fmean(values)),
        "stdev": float(statistics.stdev(values)) if len(values) > 1 else 0.0,
        "p05": percentile(values, 0.05),
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "positive_rate_pct": 100.0 * sum(1 for v in values if v > 0) / len(values),
    }


def load_day_regimes(conn: sqlite3.Connection, symbol: str) -> tuple[list[str], list[str], dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT ts_event, regime_type
        FROM touch_events
        WHERE symbol = ?
        ORDER BY ts_event
        """,
        (symbol,),
    ).fetchall()

    per_day: dict[str, Counter[str]] = defaultdict(Counter)
    raw_per_day: dict[str, Counter[str]] = defaultdict(Counter)
    for ts_event, regime_type_value in rows:
        day = et_day(int(ts_event))
        raw_name = regime_name(regime_type_value)
        bucket = regime_bucket(regime_type_value)
        per_day[day][bucket] += 1
        raw_per_day[day][raw_name] += 1

    ordered_days = sorted(per_day)
    bucket_states = [per_day[day].most_common(1)[0][0] for day in ordered_days]
    raw_states = [raw_per_day[day].most_common(1)[0][0] for day in ordered_days]

    inconsistent_days = 0
    for day in ordered_days:
        if len(per_day[day]) > 1 or len(raw_per_day[day]) > 1:
            inconsistent_days += 1

    meta = {
        "day_count": len(ordered_days),
        "first_day_et": ordered_days[0] if ordered_days else None,
        "last_day_et": ordered_days[-1] if ordered_days else None,
        "days_with_mixed_bucket_votes": inconsistent_days,
    }
    return bucket_states, raw_states, meta


def build_transition_summary(states: list[str], state_order: list[str] | None = None) -> dict[str, Any]:
    if not states:
        return {
            "state_counts": {},
            "transition_counts": {},
            "transition_probs": {},
            "persistence": {},
            "current_state": None,
            "next_state_probs_from_current": {},
        }

    counts = Counter(states)
    transitions = Counter(zip(states[:-1], states[1:]))
    ordered_states = state_order or sorted(counts)

    transition_counts: dict[str, dict[str, int]] = {}
    transition_probs: dict[str, dict[str, float | None]] = {}
    persistence: dict[str, float | None] = {}

    for src in ordered_states:
        row_counts = {dst: transitions.get((src, dst), 0) for dst in ordered_states}
        row_total = sum(row_counts.values())
        transition_counts[src] = row_counts
        transition_probs[src] = {
            dst: (row_counts[dst] / row_total if row_total else None) for dst in ordered_states
        }
        persistence[src] = row_counts.get(src, 0) / row_total if row_total else None

    current_state = states[-1]
    next_state_probs = transition_probs.get(current_state, {})
    return {
        "state_counts": dict(counts),
        "transition_counts": transition_counts,
        "transition_probs": transition_probs,
        "persistence": persistence,
        "current_state": current_state,
        "next_state_probs_from_current": next_state_probs,
    }


def load_label_rows(
    conn: sqlite3.Connection,
    symbol: str,
    horizons: list[int],
    cost_bps: float,
) -> list[LabelRow]:
    placeholders = ",".join("?" for _ in horizons)
    sql = f"""
        SELECT
            te.event_id,
            te.ts_event,
            te.regime_type,
            el.horizon_min,
            el.return_bps
        FROM touch_events te
        JOIN event_labels el
          ON el.event_id = te.event_id
        WHERE te.symbol = ?
          AND el.horizon_min IN ({placeholders})
        ORDER BY te.ts_event, el.horizon_min
    """
    rows = conn.execute(sql, (symbol, *horizons)).fetchall()
    out: list[LabelRow] = []
    for event_id, ts_event, regime_type_value, horizon_min, return_bps in rows:
        ts_event_int = int(ts_event)
        ret = float(return_bps)
        out.append(
            LabelRow(
                event_id=str(event_id),
                ts_event=ts_event_int,
                event_day_et=et_day(ts_event_int),
                raw_regime_name=regime_name(regime_type_value),
                regime_bucket=regime_bucket(regime_type_value),
                horizon_min=int(horizon_min),
                return_bps=ret,
                reject_utility=ret - cost_bps,
                break_utility=-ret - cost_bps,
            )
        )
    return out


def build_daily_samples(rows: list[LabelRow]) -> dict[int, dict[str, list[dict[str, float | int | str]]]]:
    grouped: dict[int, dict[str, dict[str, list[LabelRow]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for row in rows:
        grouped[row.horizon_min][row.regime_bucket][row.event_day_et].append(row)

    daily_samples: dict[int, dict[str, list[dict[str, float | int | str]]]] = defaultdict(dict)
    for horizon, by_bucket in grouped.items():
        for bucket, by_day in by_bucket.items():
            samples = []
            for day, day_rows in sorted(by_day.items()):
                samples.append(
                    {
                        "event_day_et": day,
                        "event_count": len(day_rows),
                        "reject_total": float(sum(r.reject_utility for r in day_rows)),
                        "break_total": float(sum(r.break_utility for r in day_rows)),
                    }
                )
            daily_samples[horizon][bucket] = samples
    return daily_samples


def build_horizon_bucket_stats(rows: list[LabelRow]) -> dict[int, dict[str, Any]]:
    grouped: dict[int, dict[str, list[LabelRow]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        grouped[row.horizon_min][row.regime_bucket].append(row)
        grouped[row.horizon_min]["all"].append(row)

    out: dict[int, dict[str, Any]] = {}
    for horizon, buckets in grouped.items():
        out[horizon] = {}
        for bucket, bucket_rows in buckets.items():
            reject_utils = [r.reject_utility for r in bucket_rows]
            break_utils = [r.break_utility for r in bucket_rows]
            out[horizon][bucket] = {
                "events_n": len(bucket_rows),
                "days_n": len({r.event_day_et for r in bucket_rows}),
                "avg_return_bps": float(statistics.fmean(r.return_bps for r in bucket_rows)),
                "reject_mean_utility": float(statistics.fmean(reject_utils)),
                "break_mean_utility": float(statistics.fmean(break_utils)),
                "reject_win_rate_pct": 100.0 * sum(1 for v in reject_utils if v > 0) / len(reject_utils),
                "break_win_rate_pct": 100.0 * sum(1 for v in break_utils if v > 0) / len(break_utils),
                "best_side_by_mean": "reject"
                if statistics.fmean(reject_utils) >= statistics.fmean(break_utils)
                else "break",
            }
    return out


def weighted_choice(rng: random.Random, probs: dict[str, float | None], fallback_state: str) -> str:
    valid = [(state, prob) for state, prob in probs.items() if prob is not None and prob > 0]
    if not valid:
        return fallback_state
    draw = rng.random()
    cumulative = 0.0
    for state, prob in valid:
        cumulative += float(prob)
        if draw <= cumulative:
            return state
    return valid[-1][0]


def run_monte_carlo(
    rng: random.Random,
    daily_samples: dict[int, dict[str, list[dict[str, float | int | str]]]],
    horizon_stats: dict[int, dict[str, Any]],
    bucket_transitions: dict[str, dict[str, float | None]],
    start_bucket: str,
    sim_days: int,
    simulations: int,
) -> dict[int, Any]:
    results: dict[int, Any] = {}
    for horizon, bucket_map in daily_samples.items():
        bucket_best_side = {
            bucket: str(stats.get("best_side_by_mean") or "reject")
            for bucket, stats in horizon_stats.get(horizon, {}).items()
            if bucket != "all"
        }
        distributions = {
            "reject_total": [],
            "break_total": [],
            "adaptive_total": [],
        }
        for _ in range(simulations):
            current_bucket = start_bucket
            reject_total = 0.0
            break_total = 0.0
            adaptive_total = 0.0
            for _day in range(sim_days):
                next_bucket = weighted_choice(
                    rng,
                    bucket_transitions.get(current_bucket, {}),
                    fallback_state=current_bucket,
                )
                current_bucket = next_bucket
                candidates = bucket_map.get(current_bucket) or bucket_map.get("all") or []
                if not candidates:
                    continue
                sample = rng.choice(candidates)
                sample_reject = float(sample["reject_total"])
                sample_break = float(sample["break_total"])
                reject_total += sample_reject
                break_total += sample_break
                best_side = bucket_best_side.get(current_bucket, "reject")
                adaptive_total += sample_reject if best_side == "reject" else sample_break

            distributions["reject_total"].append(reject_total)
            distributions["break_total"].append(break_total)
            distributions["adaptive_total"].append(adaptive_total)

        results[horizon] = {
            "best_side_by_bucket": bucket_best_side,
            "sim_days": sim_days,
            "simulations": simulations,
            "reject_total_bps": summarize_distribution(distributions["reject_total"]),
            "break_total_bps": summarize_distribution(distributions["break_total"]),
            "adaptive_total_bps": summarize_distribution(distributions["adaptive_total"]),
        }
    return results


def build_summary_lines(
    symbol: str,
    bucket_day_meta: dict[str, Any],
    bucket_markov: dict[str, Any],
    horizon_stats: dict[int, dict[str, Any]],
    monte_carlo: dict[int, Any],
) -> list[str]:
    lines = []
    lines.append(f"symbol={symbol}")
    lines.append(
        "day_coverage="
        f"{bucket_day_meta.get('first_day_et')} -> {bucket_day_meta.get('last_day_et')} "
        f"({bucket_day_meta.get('day_count')} trading days)"
    )
    lines.append(f"current_bucket={bucket_markov.get('current_state')}")
    lines.append(
        "next_day_bucket_probs="
        + json.dumps(bucket_markov.get("next_state_probs_from_current", {}), sort_keys=True)
    )
    for horizon in sorted(horizon_stats):
        all_stats = horizon_stats[horizon].get("all", {})
        mc = monte_carlo.get(horizon, {})
        adaptive = mc.get("adaptive_total_bps", {})
        lines.append(
            f"h{horizon}: events={all_stats.get('events_n')} "
            f"best_overall={all_stats.get('best_side_by_mean')} "
            f"adaptive_mc_mean={adaptive.get('mean')} "
            f"adaptive_mc_p05={adaptive.get('p05')} "
            f"adaptive_mc_p95={adaptive.get('p95')}"
        )
    return lines


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    horizons = [int(token.strip()) for token in args.horizons.split(",") if token.strip()]
    rng = random.Random(args.seed)

    conn = sqlite3.connect(str(db_path))
    try:
        bucket_states, raw_states, day_meta = load_day_regimes(conn, args.symbol)
        label_rows = load_label_rows(conn, args.symbol, horizons, args.cost_bps)
    finally:
        conn.close()

    raw_order = ["trend_up", "trend_down", "range", "vol_expansion", "unknown"]
    bucket_markov = build_transition_summary(bucket_states, BUCKET_ORDER)
    raw_markov = build_transition_summary(raw_states, raw_order)
    horizon_stats = build_horizon_bucket_stats(label_rows)
    daily_samples = build_daily_samples(label_rows)
    monte_carlo = run_monte_carlo(
        rng=rng,
        daily_samples=daily_samples,
        horizon_stats=horizon_stats,
        bucket_transitions=bucket_markov.get("transition_probs", {}),
        start_bucket=str(bucket_markov.get("current_state") or "neutral"),
        sim_days=args.sim_days,
        simulations=args.simulations,
    )

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "symbol": args.symbol,
        "horizons": horizons,
        "cost_bps": args.cost_bps,
        "day_regime_meta": day_meta,
        "markov": {
            "bucket_day_chain": bucket_markov,
            "raw_day_chain": raw_markov,
        },
        "utility_by_horizon_bucket": horizon_stats,
        "monte_carlo": monte_carlo,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{args.symbol.lower()}_regime_markov_monte_carlo.json"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    summary_lines = build_summary_lines(
        symbol=args.symbol,
        bucket_day_meta=day_meta,
        bucket_markov=bucket_markov,
        horizon_stats=horizon_stats,
        monte_carlo=monte_carlo,
    )
    txt_path = out_dir / f"{args.symbol.lower()}_regime_markov_monte_carlo_summary.txt"
    txt_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(f"wrote {json_path}")
    print(f"wrote {txt_path}")
    print()
    print("\n".join(summary_lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
