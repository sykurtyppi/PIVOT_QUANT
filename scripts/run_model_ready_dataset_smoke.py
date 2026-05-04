#!/usr/bin/env python3
"""Export a bounded model-ready feature/label dataset smoke artifact."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.model_ready_dataset_export import (
    build_model_ready_dataset_from_t9,
    write_model_ready_dataset_export,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bounded model-ready dataset export smoke.")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--analysis-start-date", default="2024-01-02")
    parser.add_argument("--analysis-end-date", default="2024-01-31")
    parser.add_argument("--start-date", default=None, help="Deprecated alias for --analysis-start-date.")
    parser.add_argument("--end-date", default=None, help="Deprecated alias for --analysis-end-date.")
    parser.add_argument("--feature-lookback-days", type=int, default=120)
    parser.add_argument("--label-lookahead-days", type=int, default=45)
    parser.add_argument("--max-files", type=int, default=20)
    parser.add_argument("--max-days", type=int, default=220)
    parser.add_argument(
        "--daily-source",
        choices=["yahoo", "ivolatility", "auto"],
        default=None,
        help="Canonical daily OHLCV source. Defaults to PIVOTQUANT_DAILY_SOURCE or yahoo.",
    )
    parser.add_argument("--horizons", default="1d,5d,21d")
    parser.add_argument("--missing-feature-policy", choices=["drop", "flag"], default="drop")
    parser.add_argument("--missing-label-policy", choices=["drop", "flag"], default="flag")
    parser.add_argument("--json", action="store_true", help="Print full metadata JSON.")
    return parser.parse_args()


def _parse_day(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def _bounded_day_count(start_date: str, end_date: str) -> int:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    return (end - start).days + 1


def _print_text(metadata: dict, paths: dict[str, Path]) -> None:
    print("PivotQuant model-ready dataset smoke")
    print(f"Symbol: {metadata['symbol']}")
    print(f"Analysis window: {metadata['analysis_start_date']} -> {metadata['analysis_end_date']}")
    print(f"Read window: {metadata['read_start_date']} -> {metadata['read_end_date']}")
    print(f"Status: {metadata['status']}")
    print(f"Rows read: {metadata['rows']['read_input']}")
    print(f"Rows input: {metadata['rows']['input']}")
    print(f"Rows exported: {metadata['rows']['exported']}")
    print(f"Rows dropped: {metadata['rows']['dropped']}")
    print(f"Dataset: {paths['dataset']}")
    print(f"Metadata: {paths['metadata']}")

    print("\n[schema]")
    print(f"  feature_count: {metadata['feature_count']}")
    print(f"  label_count: {metadata['label_count']}")
    print(f"  export_format: {metadata.get('export_format')}")

    print("\n[drop_reasons]")
    for reason, count in metadata["drop_reasons"].items():
        print(f"  {reason}: {count}")

    print("\n[label_null_counts]")
    for column, count in metadata["label_null_counts"].items():
        print(f"  {column}: {count}")
    print(f"Fully labeled rows: {metadata['fully_labeled_row_count']}")

    if metadata.get("monthly_summary"):
        print("\n[monthly_summary]")
        for month, counts in metadata["monthly_summary"].items():
            print(
                f"  {month}: analysis={counts['analysis_rows']} "
                f"exported={counts['exported_rows']} fully_labeled={counts['fully_labeled_rows']} "
                f"missing_features={counts['missing_feature_rows']} missing_labels={counts['missing_label_rows']}"
            )

    for warning in metadata.get("warnings") or []:
        print(f"[WARN] {warning}")
    for warning in metadata.get("export_warnings") or []:
        print(f"[WARN] {warning}")


def main() -> int:
    args = parse_args()
    analysis_start = args.start_date or args.analysis_start_date
    analysis_end = args.end_date or args.analysis_end_date
    read_start = _parse_day(analysis_start) - timedelta(days=max(0, args.feature_lookback_days))
    read_end = _parse_day(analysis_end) + timedelta(days=max(0, args.label_lookahead_days))
    day_count = _bounded_day_count(read_start.isoformat(), read_end.isoformat())
    if day_count > args.max_days:
        raise SystemExit(
            f"Requested read window has {day_count} calendar days, above --max-days {args.max_days}. "
            "Increase --max-days deliberately if this bounded smoke is intentional."
        )
    horizons = [part.strip() for part in args.horizons.split(",") if part.strip()]
    export = build_model_ready_dataset_from_t9(
        symbol=args.symbol,
        analysis_start_date=analysis_start,
        analysis_end_date=analysis_end,
        feature_lookback_days=args.feature_lookback_days,
        label_lookahead_days=args.label_lookahead_days,
        max_files=args.max_files,
        daily_source=args.daily_source,
        horizons=horizons,
        missing_feature_policy=args.missing_feature_policy,
        missing_label_policy=args.missing_label_policy,
    )
    paths = write_model_ready_dataset_export(export)
    if args.json:
        print(json.dumps(export.metadata, indent=2, default=str))
    else:
        _print_text(export.metadata, paths)
    return 0 if export.metadata.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
