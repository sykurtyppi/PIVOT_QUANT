#!/usr/bin/env python3
"""Run a tiny non-optimized ML training smoke on a model-ready dataset."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.ml_training_smoke import (
    DEFAULT_TARGET_COLUMN,
    run_ml_training_smoke,
    write_ml_training_smoke_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tiny fixed-model ML training smoke.")
    parser.add_argument(
        "--dataset-path",
        default="reports/model_ready_dataset_smoke/spy_2024-01-02_2024-03-29.csv",
        help="Model-ready dataset export CSV or parquet.",
    )
    parser.add_argument(
        "--metadata-path",
        default="reports/model_ready_dataset_smoke/spy_2024-01-02_2024-03-29.metadata.json",
        help="Dataset metadata sidecar.",
    )
    parser.add_argument("--target-column", default=DEFAULT_TARGET_COLUMN, choices=[DEFAULT_TARGET_COLUMN])
    parser.add_argument("--train-fraction", type=float, default=0.7)
    parser.add_argument("--json", action="store_true", help="Print full JSON report.")
    return parser.parse_args()


def _print_text(report: dict, report_path: Path) -> None:
    print("PivotQuant ML training smoke")
    print(f"Status: {report['status']}")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['analysis_start_date']} -> {report['analysis_end_date']}")
    print(f"Target: {report['target_definition']}")
    print(f"Rows input: {report['rows']['input']}")
    print(f"Train rows: {report.get('split', {}).get('train_rows')}")
    print(f"Test rows: {report.get('split', {}).get('test_rows')}")
    print(f"Report: {report_path}")

    if report.get("class_balance"):
        print("\n[class_balance]")
        print(f"  train: {report['class_balance']['train']}")
        print(f"  test: {report['class_balance']['test']}")

    if report.get("metrics"):
        print("\n[metrics]")
        for key, value in report["metrics"].items():
            print(f"  {key}: {value}")

    if report.get("naive_baseline"):
        print("\n[naive_baseline]")
        for key, value in report["naive_baseline"]["metrics"].items():
            print(f"  {key}: {value}")

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")


def main() -> int:
    args = parse_args()
    result = run_ml_training_smoke(
        dataset_path=args.dataset_path,
        metadata_path=args.metadata_path,
        target_column=args.target_column,
        train_fraction=args.train_fraction,
    )
    report_path = write_ml_training_smoke_report(result.report)
    if args.json:
        payload = dict(result.report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(result.report, report_path)
    return 0 if result.report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
