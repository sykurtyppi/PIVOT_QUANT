#!/usr/bin/env python3
"""Run a tiny feature/label-readiness smoke contract against T9 data."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.historical_feature_contract import (
    build_historical_feature_contract_from_t9,
    write_feature_contract_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only historical feature contract smoke.")
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
    parser.add_argument("--json", action="store_true", help="Print full JSON report.")
    return parser.parse_args()


def _print_text(report: dict, report_path: Path | None) -> None:
    print("PivotQuant historical feature smoke")
    print(f"T9 root: {report['t9_root']}")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['start_date']} -> {report['end_date']}")
    print(f"Daily source mode: {report.get('config', {}).get('daily_source')}")
    print(f"Read-only: {report['read_only']}")
    print(f"Status: {report['status']}")
    if report_path is not None:
        print(f"Report: {report_path}")

    print("\n[rows]")
    for name, count in report.get("rows", {}).items():
        print(f"  {name}: {count}")

    print("\n[missing_values]")
    for name, counts in report.get("missing_values", {}).items():
        if counts:
            print(f"  {name}: {counts}")

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")

    print("\n[checks]")
    for check in report.get("checks") or []:
        status = check.get("status", "unknown").upper()
        print(f"  [{status}] {check.get('name')}: {check.get('detail')}")


def main() -> int:
    args = parse_args()
    contract = build_historical_feature_contract_from_t9(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        max_files=args.max_files,
        daily_source=args.daily_source,
    )
    report = contract.report
    report_path = None
    if report.get("t9_root") and not any(check["name"] == "t9_root_exists" for check in report.get("checks", [])):
        report_path = write_feature_contract_report(report)

    if args.json:
        payload = dict(report)
        payload["report_path"] = str(report_path) if report_path else None
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(report, report_path)
    return 0 if report.get("status") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
