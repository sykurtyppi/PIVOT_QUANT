#!/usr/bin/env python3
"""Run extended bounded model-input compatibility audit against T9 data."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.model_input_compatibility import (
    build_model_input_compatibility_from_t9,
    write_model_input_compatibility_extended_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extended read-only model-input compatibility smoke.")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--start-date", default="2023-11-01")
    parser.add_argument("--end-date", default="2024-01-31")
    parser.add_argument("--max-files", type=int, default=20)
    parser.add_argument("--max-days", type=int, default=130)
    parser.add_argument(
        "--daily-source",
        choices=["yahoo", "ivolatility", "auto"],
        default=None,
        help="Canonical daily OHLCV source. Defaults to PIVOTQUANT_DAILY_SOURCE or yahoo.",
    )
    parser.add_argument("--horizons", default="1d,5d,21d")
    parser.add_argument("--json", action="store_true", help="Print full JSON report.")
    return parser.parse_args()


def _bounded_day_count(start_date: str, end_date: str) -> int:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    return (end - start).days + 1


def _print_text(report: dict, report_path: Path | None) -> None:
    readiness = report["deterministic_feature_readiness"]
    real_input = report["real_input_feature_readiness"]
    labels = report["label_availability"]
    print("PivotQuant extended model-input compatibility smoke")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['start_date']} -> {report['end_date']}")
    print(f"Status: {report['status']}")
    print(f"Rows: {report['row_count']}")
    print(f"Usable rows after daily features: {readiness['usable_row_count_after_required_daily_features']}")
    if report_path is not None:
        print(f"Report: {report_path}")

    daily_insufficient = {
        key: value
        for key, value in report["unavailable_due_to_insufficient_lookback"].items()
        if key in readiness["required_daily_features"]
    }
    real_input_insufficient = {
        key: value
        for key, value in report["unavailable_due_to_insufficient_lookback"].items()
        if key in real_input["required_real_input_features"]
    }

    print("\n[deterministic_features]")
    print(f"  computed: {report['deterministic_computed_feature_list']}")
    print(f"  insufficient_lookback: {daily_insufficient}")
    print(f"  null_rates: {readiness['null_rate_by_feature']}")

    print("\n[iv_macro_features]")
    print(f"  required: {real_input['required_real_input_features']}")
    print(f"  computed: {report['real_input_computed_feature_list']}")
    print(f"  source_counts: {real_input['source_counts']}")
    print(f"  insufficient_history: {real_input_insufficient}")
    print(f"  null_rates: {real_input['null_rate_by_feature']}")
    print(f"  unavailable_sources: {report['unavailable_due_to_missing_source']}")

    print("\n[schema_gaps]")
    print(f"  still_missing: {report['missing_required_features']}")
    print(f"  all_null_columns: {report['all_null_columns']}")
    print(f"  not_fabricated: {report['not_fabricated_features']}")

    print("\n[labels]")
    print(f"  available_horizons: {labels['available_horizons']}")
    print(f"  missing_expected_horizons: {labels['missing_expected_horizons']}")
    print(f"  counts_by_horizon: {labels['counts_by_horizon']}")

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")


def main() -> int:
    args = parse_args()
    day_count = _bounded_day_count(args.start_date, args.end_date)
    if day_count > args.max_days:
        raise SystemExit(
            f"Requested window has {day_count} calendar days, above --max-days {args.max_days}. "
            "Increase --max-days deliberately if this bounded smoke is intentional."
        )
    horizons = [part.strip() for part in args.horizons.split(",") if part.strip()]
    audit = build_model_input_compatibility_from_t9(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        max_files=args.max_files,
        daily_source=args.daily_source,
        horizons=horizons,
    )
    report = audit.report
    report_path = write_model_input_compatibility_extended_report(report)
    if args.json:
        payload = dict(report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(report, report_path)
    return 0 if report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
