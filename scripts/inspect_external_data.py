#!/usr/bin/env python3
"""Inspect external PivotQuant/T9 data sources without mutating them."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.t9_inventory import DEFAULT_MAX_FILES, build_t9_inventory


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only inventory of Samsung T9 historical data sources."
    )
    parser.add_argument("--symbol", default="SPY", help="Symbol to discover, default SPY.")
    parser.add_argument(
        "--max-files",
        type=int,
        default=DEFAULT_MAX_FILES,
        help="Maximum files to inspect per source category.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of a text summary.",
    )
    return parser.parse_args()


def _print_text(report: dict) -> None:
    print("PivotQuant external data inventory")
    print(f"T9 root: {report['t9_root']}")
    print(f"Root exists: {report['root_exists']}")
    print(f"Symbol: {report['symbol']}")
    print(f"Read-only: {report['read_only']}")
    if report.get("warnings"):
        print()
        for warning in report["warnings"]:
            print(f"[WARN] {warning}")

    for key, section in report.get("sections", {}).items():
        print()
        print(f"[{key}] {section['name']}")
        print(f"  path: {section['path']}")
        print(f"  exists: {section['exists']}")
        print(f"  file_count: {section['file_count']}")
        if section.get("row_estimate") is not None:
            print(f"  row_estimate: {section['row_estimate']}")
        if section.get("date_range"):
            date_range = section["date_range"]
            print(f"  date_range: {date_range.get('min')} -> {date_range.get('max')}")
        if section.get("sample_schema"):
            preview = ", ".join(
                f"{item.get('name')}:{item.get('type')}"
                for item in section["sample_schema"][:8]
            )
            print(f"  sample_schema: {preview}")
        if section.get("files_sample"):
            print("  sample_files:")
            for sample in section["files_sample"][:5]:
                print(f"    - {sample}")
        for note in section.get("notes") or []:
            print(f"  note: {note}")


def main() -> int:
    args = parse_args()
    report = build_t9_inventory(symbol=args.symbol, max_files=args.max_files)

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        _print_text(report)

    return 0 if report.get("root_exists") else 1


if __name__ == "__main__":
    raise SystemExit(main())
