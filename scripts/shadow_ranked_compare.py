#!/usr/bin/env python3
"""Compare shadow-margin emits against ranked aligned-side rows for a date window."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ml.regime_semantics import favored_side_for_trade_regime
from scripts.generate_daily_ml_report import (
    DEFAULT_DB,
    DEFAULT_REPORT_DIR,
    DEFAULT_PREDICTION_BASIS,
    DEFAULT_RANKED_SHADOW_HORIZON,
    DEFAULT_RANKED_SHADOW_RETAIN_PCT,
    DEFAULT_SHADOW_POLICY_NAME,
    DEFAULT_TRADE_COST_BPS,
    ET_TZ,
    fetch_labeled_records,
    fetch_shadow_emission_records,
    parse_report_date,
    day_bounds_ms,
)


@dataclass(frozen=True)
class RankedSelectionRow:
    event_id: str
    trade_regime: str
    chosen_side: str
    side_prob: float
    utility: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare shadow emits vs ranked aligned-side rows.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite DB path.")
    parser.add_argument("--report-date", default="", help="Single ET report date in YYYY-MM-DD.")
    parser.add_argument("--start-date", default="", help="Optional ET start date in YYYY-MM-DD (inclusive).")
    parser.add_argument("--end-date", default="", help="Optional ET end date in YYYY-MM-DD (exclusive).")
    parser.add_argument("--model-version", default="", help="Optional model_version filter.")
    parser.add_argument("--symbol", default="SPY", help="Symbol filter for ranked rows.")
    parser.add_argument("--horizon", type=int, default=DEFAULT_RANKED_SHADOW_HORIZON, help="Horizon in minutes.")
    parser.add_argument(
        "--prediction-basis",
        choices=("first", "latest"),
        default=DEFAULT_PREDICTION_BASIS,
        help="Which prediction row to use per event.",
    )
    parser.add_argument("--retain-pct", type=float, default=DEFAULT_RANKED_SHADOW_RETAIN_PCT, help="Retained top fraction.")
    parser.add_argument("--policy-name", default=DEFAULT_SHADOW_POLICY_NAME, help="Shadow policy name.")
    parser.add_argument("--cost-bps", type=float, default=DEFAULT_TRADE_COST_BPS, help="Trade cost in bps.")
    parser.add_argument("--out-dir", default=str(Path(DEFAULT_REPORT_DIR) / "research"), help="Output directory.")
    return parser.parse_args()


def normalize_trade_regime(value: Any) -> str:
    regime = str(value or "").strip().lower()
    if regime in {"compression", "expansion", "neutral"}:
        return regime
    return "unknown"


def parse_window(args: argparse.Namespace) -> tuple[int, int, str, str]:
    if args.report_date:
        report_day = parse_report_date(args.report_date)
        start_ms, end_ms = day_bounds_ms(report_day)
        start_label = report_day.strftime("%Y-%m-%d")
        end_label = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).astimezone(ET_TZ).strftime("%Y-%m-%d")
        return start_ms, end_ms, start_label, end_label

    if not args.start_date:
        raise SystemExit("Provide either --report-date or --start-date/--end-date.")
    start_day = parse_report_date(args.start_date)
    if args.end_date:
        end_day = parse_report_date(args.end_date)
        start_ms, _ = day_bounds_ms(start_day)
        end_ms, _ = day_bounds_ms(end_day)
    else:
        start_ms, end_ms = day_bounds_ms(start_day)
    if end_ms <= start_ms:
        raise SystemExit("--end-date must be later than --start-date")
    return start_ms, end_ms, start_day.strftime("%Y-%m-%d"), datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).astimezone(ET_TZ).strftime("%Y-%m-%d")


def summarize_utils(utils: list[float]) -> dict[str, float | int | None]:
    if not utils:
        return {"rows": 0, "avg_utility": None, "total_utility": 0.0, "win_rate": None}
    return {
        "rows": len(utils),
        "avg_utility": float(statistics.fmean(utils)),
        "total_utility": float(sum(utils)),
        "win_rate": float(sum(1 for u in utils if u > 0) / len(utils)),
    }


def build_ranked_selection(
    labeled_rows: list[dict[str, Any]],
    *,
    horizon: int,
    retain_pct: float,
    cost_bps: float,
    symbol: str,
) -> tuple[list[RankedSelectionRow], int]:
    eligible: list[RankedSelectionRow] = []
    for row in labeled_rows:
        if str(row.get("symbol") or "").strip().upper() != symbol.upper():
            continue
        if int(row.get("horizon_min") or -1) != int(horizon):
            continue
        if str(row.get("selected_policy") or "").strip().lower() != "regime_active":
            continue
        trade_regime = normalize_trade_regime(row.get("trade_regime"))
        chosen_side = favored_side_for_trade_regime(trade_regime)
        if chosen_side not in {"reject", "break"}:
            continue
        side_prob = row.get(f"prob_{chosen_side}_{horizon}m")
        return_bps = row.get("return_bps")
        if side_prob is None or return_bps is None:
            continue
        ret = float(return_bps)
        utility = (ret - cost_bps) if chosen_side == "reject" else (-ret - cost_bps)
        eligible.append(
            RankedSelectionRow(
                event_id=str(row.get("event_id")),
                trade_regime=trade_regime,
                chosen_side=chosen_side,
                side_prob=float(side_prob),
                utility=utility,
            )
        )

    ranked = sorted(eligible, key=lambda item: item.side_prob, reverse=True)
    if not ranked:
        return [], 0
    retain_n = max(1, math.ceil(len(ranked) * float(retain_pct)))
    return ranked[:retain_n], len(ranked)


def build_shadow_selection(
    shadow_rows: list[dict[str, Any]],
    *,
    horizon: int,
    cost_bps: float,
    symbol: str,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for row in shadow_rows:
        if row.get("policy_name") is None:
            continue
        if row.get("shadow_emit") in (0, False, None):
            continue
        if int(row.get("shadow_horizon") or -1) != int(horizon):
            continue
        event_id = str(row.get("event_id") or "")
        shadow_side = str(row.get("shadow_side") or "").strip().lower()
        return_bps = row.get("return_bps")
        if not event_id or shadow_side not in {"reject", "break"} or return_bps is None:
            continue
        trade_regime = normalize_trade_regime(row.get("trade_regime"))
        ret = float(return_bps)
        utility = (ret - cost_bps) if shadow_side == "reject" else (-ret - cost_bps)
        selected.append(
            {
                "event_id": event_id,
                "trade_regime": trade_regime,
                "side": shadow_side,
                "utility": utility,
            }
        )
    return selected


def group_summary(rows: list[dict[str, Any]], utility_key: str = "utility") -> dict[str, dict[str, float | int | None]]:
    buckets: dict[str, list[float]] = {}
    for row in rows:
        buckets.setdefault(str(row.get("trade_regime") or "unknown"), []).append(float(row[utility_key]))
    return {bucket: summarize_utils(utils) for bucket, utils in sorted(buckets.items())}


def main() -> int:
    args = parse_args()
    start_ms, end_ms, start_label, end_label = parse_window(args)
    db_path = Path(args.db).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        labeled_rows = fetch_labeled_records(
            conn,
            start_ms,
            end_ms,
            False,
            args.prediction_basis,
            args.model_version or None,
        )
        shadow_rows = fetch_shadow_emission_records(
            conn,
            start_ms,
            end_ms,
            False,
            args.prediction_basis,
            args.model_version or None,
            args.policy_name,
        )
    finally:
        conn.close()

    ranked_selected, ranked_eligible = build_ranked_selection(
        labeled_rows,
        horizon=args.horizon,
        retain_pct=args.retain_pct,
        cost_bps=float(args.cost_bps),
        symbol=args.symbol,
    )
    shadow_selected = build_shadow_selection(
        shadow_rows,
        horizon=args.horizon,
        cost_bps=float(args.cost_bps),
        symbol=args.symbol,
    )

    ranked_payload_rows = [
        {
            "event_id": row.event_id,
            "trade_regime": row.trade_regime,
            "side": row.chosen_side,
            "side_prob": row.side_prob,
            "utility": row.utility,
        }
        for row in ranked_selected
    ]
    shadow_by_event = {row["event_id"]: row for row in shadow_selected}
    ranked_by_event = {row["event_id"]: row for row in ranked_payload_rows}
    overlap_ids = sorted(set(shadow_by_event) & set(ranked_by_event))
    overlap_rows: list[dict[str, Any]] = []
    for event_id in overlap_ids:
        shadow_row = shadow_by_event[event_id]
        ranked_row = ranked_by_event[event_id]
        overlap_rows.append(
            {
                "event_id": event_id,
                "trade_regime": ranked_row["trade_regime"],
                "shadow_side": shadow_row["side"],
                "ranked_side": ranked_row["side"],
                "shadow_utility": shadow_row["utility"],
                "ranked_utility": ranked_row["utility"],
            }
        )

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "symbol": args.symbol,
        "horizon": args.horizon,
        "prediction_basis": args.prediction_basis,
        "model_version_filter": args.model_version or None,
        "start_date_filter_et": start_label,
        "end_date_filter_et_exclusive": end_label,
        "policy_name": args.policy_name,
        "retain_pct": float(args.retain_pct),
        "shadow": {
            "rows": shadow_selected,
            "overall": summarize_utils([row["utility"] for row in shadow_selected]),
            "by_regime": group_summary(shadow_selected),
        },
        "ranked": {
            "eligible_rows": ranked_eligible,
            "rows": ranked_payload_rows,
            "overall": summarize_utils([row["utility"] for row in ranked_payload_rows]),
            "by_regime": group_summary(ranked_payload_rows),
        },
        "overlap": {
            "rows": overlap_rows,
            "count": len(overlap_rows),
            "by_regime": {
                bucket: {
                    "rows": len(bucket_rows),
                    "shadow_avg_utility": float(statistics.fmean([r["shadow_utility"] for r in bucket_rows])) if bucket_rows else None,
                    "ranked_avg_utility": float(statistics.fmean([r["ranked_utility"] for r in bucket_rows])) if bucket_rows else None,
                }
                for bucket, bucket_rows in sorted(
                    {
                        regime: [r for r in overlap_rows if r["trade_regime"] == regime]
                        for regime in {r["trade_regime"] for r in overlap_rows}
                    }.items()
                )
            },
        },
    }

    lines = [
        f"symbol={args.symbol} horizon={args.horizon} model_version={args.model_version or None} prediction_basis={args.prediction_basis}",
        f"filter_window_et={start_label} -> {end_label}",
        f"shadow_policy={args.policy_name} retain_pct={float(args.retain_pct):.2f}",
        (
            f"shadow: rows={payload['shadow']['overall']['rows']} "
            f"avg={payload['shadow']['overall']['avg_utility']} "
            f"total={payload['shadow']['overall']['total_utility']} "
            f"win_rate={payload['shadow']['overall']['win_rate']}"
        ),
    ]
    for bucket, summary in payload["shadow"]["by_regime"].items():
        lines.append(
            f"  shadow|{bucket}: rows={summary['rows']} avg={summary['avg_utility']} total={summary['total_utility']} win_rate={summary['win_rate']}"
        )
    lines.append(
        f"ranked: eligible={ranked_eligible} selected={payload['ranked']['overall']['rows']} "
        f"avg={payload['ranked']['overall']['avg_utility']} "
        f"total={payload['ranked']['overall']['total_utility']} "
        f"win_rate={payload['ranked']['overall']['win_rate']}"
    )
    for bucket, summary in payload["ranked"]["by_regime"].items():
        lines.append(
            f"  ranked|{bucket}: rows={summary['rows']} avg={summary['avg_utility']} total={summary['total_utility']} win_rate={summary['win_rate']}"
        )
    lines.append(f"overlap: rows={payload['overlap']['count']}")
    for bucket, summary in payload["overlap"]["by_regime"].items():
        lines.append(
            f"  overlap|{bucket}: rows={summary['rows']} shadow_avg={summary['shadow_avg_utility']} ranked_avg={summary['ranked_avg_utility']}"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{args.symbol.lower()}_h{args.horizon}_shadow_ranked_compare"
    if args.model_version:
        stem += f"_{args.model_version}"
    stem += f"_{start_label}_to_{end_label}"
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
