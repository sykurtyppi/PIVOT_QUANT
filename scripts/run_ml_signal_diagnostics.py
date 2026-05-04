#!/usr/bin/env python3
"""Run bounded signal diagnostics for a model-ready dataset."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.ml_signal_diagnostics import (
    DEFAULT_BUCKET_COUNT,
    DEFAULT_ROLLING_WINDOW_ROWS,
    DEFAULT_STABILITY_WINDOW_ROWS,
    run_ml_signal_diagnostics,
    write_ml_signal_diagnostics_report,
)
from services.external_data.ml_training_smoke import DEFAULT_TARGET_COLUMN


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bounded ML signal diagnostics; no training or tuning.")
    parser.add_argument(
        "--dataset-path",
        default="reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.csv",
    )
    parser.add_argument(
        "--metadata-path",
        default="reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.metadata.json",
    )
    parser.add_argument(
        "--model-diagnostics-path",
        default="reports/ml_smoke/spy_2023-01-03_2023-12-29_ml_walk_forward_smoke_forward_return_5d_positive.json",
    )
    parser.add_argument("--target-column", default=DEFAULT_TARGET_COLUMN, choices=[DEFAULT_TARGET_COLUMN])
    parser.add_argument("--bucket-count", type=int, default=DEFAULT_BUCKET_COUNT)
    parser.add_argument("--stability-window-rows", type=int, default=DEFAULT_STABILITY_WINDOW_ROWS)
    parser.add_argument("--rolling-window-rows", type=int, default=DEFAULT_ROLLING_WINDOW_ROWS)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _print_text(report: dict, report_path: Path) -> None:
    target = report["target_distribution"]
    summary = report["signal_strength_summary"]
    collapse = report["model_collapse_diagnosis"]
    print("PivotQuant ML signal diagnostics")
    print(f"Status: {report['status']}")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['analysis_start_date']} -> {report['analysis_end_date']}")
    print(f"Target: {report['target_definition']}")
    print(f"Rows input: {report['rows']['input']}")
    print(f"Report: {report_path}")

    print("\n[target]")
    print(f"  positive_rate: {target['overall']['positive_rate']}")
    print(f"  mean_forward_return_5d: {target['distribution']['mean']}")
    print(f"  std_forward_return_5d: {target['distribution']['std']}")
    print(f"  lag1_autocorr: {target['autocorrelation']['forward_return_5d_lag1']}")

    print("\n[signal_strength]")
    print(f"  assessment: {summary['overall_signal_strength']}")
    print(f"  stable_candidate_count: {summary['stable_candidate_count']}")
    print(f"  no_signal_features: {len(summary['features_with_no_signal'])}")
    print(f"  unstable_features: {len(summary['unstable_features'])}")
    for row in summary["strongest_features_by_correlation"][:3]:
        print(
            "  corr_feature: "
            f"{row['feature']} max_abs_corr={row['max_abs_correlation']} "
            f"bucket_sep={row['bucket_positive_rate_separation']}"
        )

    print("\n[model_collapse]")
    print(f"  no_feature_separated_classes: {collapse['no_feature_separated_classes']}")
    print(f"  coefficients_weak_or_unstable: {collapse['coefficients_weak_or_unstable']}")
    print(f"  probabilities_clustered_windows: {collapse['probabilities_clustered_near_0_5']['clustered_near_0_5_window_count']}")
    for reason in collapse.get("why_model_predicted_mostly_one_class") or []:
        print(f"  reason: {reason}")

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")
    print("[WARN] no edge claim")


def main() -> int:
    args = parse_args()
    result = run_ml_signal_diagnostics(
        dataset_path=args.dataset_path,
        metadata_path=args.metadata_path,
        model_diagnostics_path=args.model_diagnostics_path,
        target_column=args.target_column,
        bucket_count=args.bucket_count,
        stability_window_rows=args.stability_window_rows,
        rolling_window_rows=args.rolling_window_rows,
    )
    report_path = write_ml_signal_diagnostics_report(result.report)
    if args.json:
        payload = dict(result.report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(result.report, report_path)
    return 0 if result.report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
