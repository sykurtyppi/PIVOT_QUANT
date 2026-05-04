#!/usr/bin/env python3
"""Run bounded regime and target diagnostics; no training or tuning."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.ml_regime_target_diagnostics import (
    DEFAULT_BUCKET_COUNT,
    DEFAULT_ROLLING_WINDOW_ROWS,
    DEFAULT_STABILITY_WINDOW_ROWS,
    run_ml_regime_target_diagnostics,
    write_ml_regime_target_diagnostics_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bounded regime and target diagnostics.")
    parser.add_argument(
        "--dataset-path",
        default="reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.csv",
    )
    parser.add_argument(
        "--metadata-path",
        default="reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.metadata.json",
    )
    parser.add_argument("--bucket-count", type=int, default=DEFAULT_BUCKET_COUNT)
    parser.add_argument("--stability-window-rows", type=int, default=DEFAULT_STABILITY_WINDOW_ROWS)
    parser.add_argument("--rolling-window-rows", type=int, default=DEFAULT_ROLLING_WINDOW_ROWS)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _print_text(report: dict, report_path: Path) -> None:
    recommendation = report["recommendation"]
    stability = report["stability_summary"]
    print("PivotQuant ML regime/target diagnostics")
    print(f"Status: {report['status']}")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['analysis_start_date']} -> {report['analysis_end_date']}")
    print(f"Rows input: {report['rows']['input']}")
    print(f"Report: {report_path}")

    print("\n[target_comparison]")
    for row in report["target_comparison_table"]:
        print(
            f"  {row['target']}: status={row['status']} rows={row['rows']} "
            f"positive_rate={row.get('positive_rate')} rolling_std={row.get('rolling_metric_std')} "
            f"overlap_warning={row.get('overlap_warning')}"
        )

    print("\n[regimes]")
    for row in report["regime_table"]:
        print(
            f"  {row['regime_feature']}:{row['regime']} rows={row['rows']} "
            f"positive_rate={row['target_positive_rate']} mean_5d={row['mean_forward_return_5d']}"
        )

    print("\n[stability]")
    print(f"  regime_sensitive_segments: {stability['regime_sensitive_segment_count']}")
    for row in stability["target_scores"]:
        print(
            f"  {row['target']}: stable_corr={row['stable_correlation_features']} "
            f"unstable_corr={row['unstable_correlation_features']} "
            f"stable_bucket={row['stable_bucket_features']} "
            f"unstable_bucket={row['unstable_bucket_features']}"
        )

    print("\n[recommendation]")
    print(f"  action: {recommendation['action']}")
    for reason in recommendation["reasons"]:
        print(f"  reason: {reason}")

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")
    print("[WARN] no edge claim")


def main() -> int:
    args = parse_args()
    result = run_ml_regime_target_diagnostics(
        dataset_path=args.dataset_path,
        metadata_path=args.metadata_path,
        bucket_count=args.bucket_count,
        stability_window_rows=args.stability_window_rows,
        rolling_window_rows=args.rolling_window_rows,
    )
    report_path = write_ml_regime_target_diagnostics_report(result.report)
    if args.json:
        payload = dict(result.report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(result.report, report_path)
    return 0 if result.report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
