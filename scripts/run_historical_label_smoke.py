#!/usr/bin/env python3
"""Run a tiny historical label-builder smoke contract against T9 data."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.historical_label_contract import (
    DEFAULT_HORIZONS,
    build_historical_label_contract_from_t9,
    write_label_contract_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only historical label smoke test.")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--max-files", type=int, default=20)
    parser.add_argument(
        "--daily-source",
        choices=["yahoo", "ivolatility", "auto"],
        default=None,
        help="Canonical daily OHLCV source. Defaults to PIVOTQUANT_DAILY_SOURCE or yahoo.",
    )
    parser.add_argument(
        "--horizons",
        default=",".join(DEFAULT_HORIZONS),
        help="Comma-separated trading-day horizons such as 1d,5d.",
    )
    parser.add_argument("--json", action="store_true", help="Print full JSON report.")
    return parser.parse_args()


def _print_text(report: dict, report_path: Path | None) -> None:
    print("PivotQuant historical label smoke")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['start_date']} -> {report['end_date']}")
    print(f"Horizons: {', '.join(report.get('config', {}).get('horizons') or [])}")
    print(f"Read-only: {report['read_only']}")
    print(f"Status: {report['status']}")
    if report_path is not None:
        print(f"Report: {report_path}")

    print("\n[rows]")
    for name, count in report.get("rows", {}).items():
        print(f"  {name}: {count}")
    print(f"  mature_label_count: {report.get('mature_label_count')}")
    print(f"  immature_or_excluded_count: {report.get('immature_or_excluded_count')}")

    print("\n[coverage]")
    for horizon, values in report.get("coverage", {}).items():
        print(
            f"  {horizon}: mature={values.get('mature')} "
            f"excluded={values.get('excluded')} coverage={values.get('coverage_ratio')}"
        )

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")

    print("\n[checks]")
    for check in report.get("checks") or []:
        status = check.get("status", "unknown").upper()
        print(f"  [{status}] {check.get('name')}: {check.get('detail')}")


def main() -> int:
    args = parse_args()
    horizons = [part.strip() for part in args.horizons.split(",") if part.strip()]
    contract = build_historical_label_contract_from_t9(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        max_files=args.max_files,
        daily_source=args.daily_source,
        horizons=horizons,
    )
    report = contract.report
    report_path = write_label_contract_report(report)

    if args.json:
        payload = dict(report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(report, report_path)
    return 0 if report.get("status") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
