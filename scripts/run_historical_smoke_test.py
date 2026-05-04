#!/usr/bin/env python3
"""Run a tiny read-only historical smoke test against T9 parquet data."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.t9_parquet_adapter import (
    load_historical_smoke_slice,
    write_smoke_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only T9 historical smoke test.")
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
    print("PivotQuant historical smoke test")
    print(f"T9 root: {report['t9_root']}")
    print(f"Root exists: {report['root_exists']}")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['start_date']} -> {report['end_date']}")
    print(f"Daily source mode: {report.get('config', {}).get('daily_source')}")
    print(f"Read-only: {report['read_only']}")
    if report_path is not None:
        print(f"Report: {report_path}")
    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")

    for key, section in (report.get("sections") or {}).items():
        print()
        print(f"[{key}]")
        if key == "historical_contract":
            print(f"  status: {section.get('status')}")
            for check in section.get("checks") or []:
                if check.get("status") != "pass":
                    print(f"  {check.get('status')}: {check.get('name')} - {check.get('detail')}")
            continue
        print(f"  rows: {section['row_count']}")
        print(f"  files: {section['file_count']}")
        print(f"  date_range: {section['date_range']['min']} -> {section['date_range']['max']}")
        metadata = section.get("metadata") or {}
        if metadata.get("selected_source") is not None:
            print(f"  selected_source: {metadata.get('selected_source')}")
        if metadata.get("duplicate_date_count"):
            print(f"  duplicate_source_dates: {metadata.get('duplicate_date_count')}")
        if section.get("missing_columns"):
            print(f"  missing_columns: {', '.join(section['missing_columns'])}")
        for warning in section.get("warnings") or []:
            print(f"  warning: {warning}")
        if section.get("sample_rows"):
            print(f"  sample_row: {section['sample_rows'][0]}")


def main() -> int:
    args = parse_args()
    report = load_historical_smoke_slice(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        max_files=args.max_files,
        daily_source=args.daily_source,
    )
    report_path = None
    if report.get("root_exists"):
        report_path = write_smoke_report(report)

    if args.json:
        payload = dict(report)
        payload["report_path"] = str(report_path) if report_path else None
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(report, report_path)
    return 0 if report.get("root_exists") else 1


if __name__ == "__main__":
    raise SystemExit(main())
