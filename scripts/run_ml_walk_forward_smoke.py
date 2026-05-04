#!/usr/bin/env python3
"""Run fixed-model chronological walk-forward ML smoke."""

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
    run_ml_walk_forward_smoke,
    write_ml_training_smoke_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fixed-model chronological walk-forward ML smoke.")
    parser.add_argument(
        "--dataset-path",
        default="reports/model_ready_dataset_smoke/spy_2024-01-02_2024-03-29.csv",
    )
    parser.add_argument(
        "--metadata-path",
        default="reports/model_ready_dataset_smoke/spy_2024-01-02_2024-03-29.metadata.json",
    )
    parser.add_argument("--target-column", default=DEFAULT_TARGET_COLUMN, choices=[DEFAULT_TARGET_COLUMN])
    parser.add_argument("--train-window-rows", type=int, default=30)
    parser.add_argument("--test-window-rows", type=int, default=10)
    parser.add_argument("--step-rows", type=int, default=10)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _print_text(report: dict, report_path: Path) -> None:
    wf = report["walk_forward"]
    print("PivotQuant ML walk-forward smoke")
    print(f"Status: {report['status']}")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['analysis_start_date']} -> {report['analysis_end_date']}")
    print(f"Target: {report['target_definition']}")
    print(f"Rows input: {report['rows']['input']}")
    print(f"Windows: {wf['window_count']} evaluable={wf['evaluable_window_count']} non_evaluable={wf['non_evaluable_window_count']}")
    print(f"Report: {report_path}")

    print("\n[aggregate]")
    for key, value in report["aggregate"]["average_metrics"].items():
        print(f"  avg_{key}: {value}")
    for key, value in report["aggregate"].get("average_naive_baseline_metrics", {}).items():
        print(f"  naive_avg_{key}: {value}")

    diagnostics = report.get("diagnostic_summary") or {}
    if diagnostics:
        print("\n[diagnostics]")
        print(f"  model_underperformed_naive_accuracy: {diagnostics.get('model_underperformed_naive_accuracy')}")
        print(f"  predicted_mostly_one_class_windows: {diagnostics.get('predicted_mostly_one_class_window_count')}")
        print(f"  class_imbalance_dominated: {diagnostics.get('class_imbalance_dominated')}")
        print(f"  low_variance_feature_count: {diagnostics.get('low_variance_feature_count')}")
        for reason in diagnostics.get("why_model_underperformed") or []:
            print(f"  reason: {reason}")

    print("\n[windows]")
    for window in wf["windows"]:
        metrics = window.get("metrics") or {}
        predictions = window.get("diagnostics", {}).get("prediction_distribution", {})
        naive_class = window.get("naive_baseline", {}).get("class_choice")
        print(
            f"  {window['window_id']}: {window['status']} "
            f"train={window['train_rows']} test={window['test_rows']} "
            f"accuracy={metrics.get('accuracy')} precision={metrics.get('precision')} "
            f"recall={metrics.get('recall')} auc={metrics.get('auc')} "
            f"pred_pos_rate={predictions.get('positive_rate')} naive_class={naive_class}"
        )

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")


def main() -> int:
    args = parse_args()
    result = run_ml_walk_forward_smoke(
        dataset_path=args.dataset_path,
        metadata_path=args.metadata_path,
        target_column=args.target_column,
        train_window_rows=args.train_window_rows,
        test_window_rows=args.test_window_rows,
        step_rows=args.step_rows,
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
