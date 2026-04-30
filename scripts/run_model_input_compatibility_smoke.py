#!/usr/bin/env python3
"""Run bounded model-input compatibility audit against T9 historical features."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.historical_label_contract import DEFAULT_HORIZONS
from services.external_data.model_input_compatibility import (
    build_model_input_compatibility_from_t9,
    write_model_input_compatibility_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only model-input compatibility smoke.")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--start-date", default="2024-01-02")
    parser.add_argument("--end-date", default="2024-01-31")
    parser.add_argument("--max-files", type=int, default=20)
    parser.add_argument(
        "--daily-source",
        choices=["yahoo", "ivolatility", "auto"],
        default=None,
        help="Canonical daily OHLCV source. Defaults to PIVOTQUANT_DAILY_SOURCE or yahoo.",
    )
    parser.add_argument("--horizons", default=",".join(DEFAULT_HORIZONS))
    parser.add_argument("--json", action="store_true", help="Print full JSON report.")
    return parser.parse_args()


def _print_text(report: dict, report_path: Path | None) -> None:
    print("PivotQuant model-input compatibility smoke")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['start_date']} -> {report['end_date']}")
    print(f"Status: {report['status']}")
    print(f"Rows: {report['row_count']}")
    print(f"Expected features: {report['expected_feature_count']}")
    print(f"Available features: {report['available_feature_count']}")
    if report_path is not None:
        print(f"Report: {report_path}")

    print("\n[features]")
    print(f"  computed_feature_list: {report['computed_feature_list']}")
    print(
        "  unavailable_due_to_insufficient_lookback: "
        f"{report['unavailable_due_to_insufficient_lookback']}"
    )
    print(f"  missing_required_features: {report['missing_required_features']}")
    print(f"  extra_features: {report['extra_features']}")
    print(f"  dtype_mismatches: {report['dtype_mismatches']}")
    print(f"  all_null_columns: {report['all_null_columns']}")
    print(f"  null_rate_warnings: {report['null_rate_warnings']}")

    print("\n[labels]")
    labels = report["label_availability"]
    print(f"  available_horizons: {labels['available_horizons']}")
    print(f"  missing_expected_horizons: {labels['missing_expected_horizons']}")
    print(f"  counts_by_horizon: {labels['counts_by_horizon']}")

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")


def main() -> int:
    args = parse_args()
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
    report_path = write_model_input_compatibility_report(report)
    if args.json:
        payload = dict(report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(report, report_path)
    return 0 if report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
