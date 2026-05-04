#!/usr/bin/env python3
"""Cross-period validation of the frozen candidate signal.

Runs the existing ml_regime_validation pipeline on a configurable train/test
split (default: 2020+2021 -> 2022) using auto-discovered model-ready datasets,
and aggregates the result with the original 2023+2024 -> 2025 report when it
is available.

Rules enforced by inheritance: no model training, no threshold optimization,
no filter changes, no live trading, no T9 mutation. The frozen candidate
signal logic is reused unchanged from ml_candidate_signal.py.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.ml_cross_period_validation import (
    aggregate_cross_period_validation,
    write_cross_period_report,
)
from services.external_data.ml_regime_benchmark import discover_year_datasets
from services.external_data.ml_regime_validation import (
    run_ml_regime_validation,
    write_ml_regime_validation_report,
)


PROTOCOL_STAGE = 3  # cross-period validation (RESEARCH_PROTOCOL §3, stage 3)


def parse_args() -> argparse.Namespace:
    from services.research_protocol.cli_protocol import add_protocol_arguments

    parser = argparse.ArgumentParser(
        description=(
            "Cross-period regime validation for the frozen candidate signal."
            " Runs a configurable train/test split and aggregates with the"
            " original 2023+2024 -> 2025 report when available."
        )
    )
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument(
        "--train-years",
        nargs="+",
        default=["2020", "2021"],
        help="Train years (default: 2020 2021)",
    )
    parser.add_argument(
        "--test-year",
        default="2022",
        help="Test year (default: 2022)",
    )
    parser.add_argument(
        "--datasets-dir",
        default="reports/model_ready_dataset_smoke",
        help="Directory containing per-year model-ready CSV datasets",
    )
    parser.add_argument(
        "--baseline-report",
        default=None,
        help=(
            "Path to an existing 2023+2024 -> 2025 regime validation JSON to"
            " aggregate against; pass 'none' to skip aggregation"
        ),
    )
    parser.add_argument(
        "--no-baseline-aggregate",
        action="store_true",
        help="Skip aggregation with any prior baseline report",
    )
    parser.add_argument(
        "--train-coverage-start",
        default=None,
        help="Documented first available date in the train window (e.g. 2021-04-05 if T9 starts there)",
    )
    parser.add_argument(
        "--train-coverage-end",
        default=None,
        help="Documented last available date in the train window",
    )
    parser.add_argument(
        "--train-is-partial",
        action="store_true",
        help="Mark this run's train year as partial (e.g. 2021 has only Apr-Dec on T9)",
    )
    parser.add_argument(
        "--data-coverage-note",
        default=None,
        help="Free-text note documenting any partial-coverage caveat",
    )
    parser.add_argument(
        "--period-label",
        default=None,
        help="Override the period label (default derives from train years and test year)",
    )
    parser.add_argument("--json", action="store_true", help="Print full JSON output")
    add_protocol_arguments(parser, expected_stage=PROTOCOL_STAGE)
    return parser.parse_args()


def _print_period_summary(label: str, report: dict) -> None:
    print(f"\n=== {label} ===")
    coverage = report.get("data_coverage") or {}
    if coverage:
        print(f"  period_label:     {coverage.get('period_label')}")
        print(f"  train_coverage:   {coverage.get('train_coverage_start')} -> {coverage.get('train_coverage_end')}")
        print(f"  train_is_partial: {coverage.get('train_is_partial')}")
        if coverage.get("data_coverage_note"):
            print(f"  coverage_note:    {coverage.get('data_coverage_note')}")
    print(f"  status:           {report.get('status')}")
    print(f"  validated:        {report.get('validated')}")
    crc = report.get("candidate_readiness_checklist") or {}
    print(f"  candidate_status: {crc.get('candidate_status')}")
    print(f"  paper_ready:      {crc.get('candidate_ready_for_paper_observation')}")
    ltr = report.get("late_trend_removal_validation") or {}
    bv = (ltr.get("baseline_validation") or {})
    fv = (ltr.get("filtered_validation") or {})
    impr = ltr.get("improvement_summary") or {}
    print("  -- baseline (high_vol_trend_positive) --")
    for period in ("train", "test"):
        b = bv.get(period) or {}
        print(
            f"    {period}: n={b.get('sample_size')} "
            f"win5d={b.get('win_rate_5d')} mean5d={b.get('mean_return_5d')}"
        )
    print("  -- filtered (early_trend_only) --")
    for period in ("train", "test"):
        f = fv.get(period) or {}
        print(
            f"    {period}: n={f.get('sample_size')} "
            f"win5d={f.get('win_rate_5d')} mean5d={f.get('mean_return_5d')}"
        )
    print(
        "  rows_removed: "
        f"train={impr.get('rows_removed_train')} "
        f"test={impr.get('rows_removed_test')}"
    )
    bpr = report.get("boundary_purge_report") or {}
    print(
        "  boundary_purge: "
        f"overlap_detected={bpr.get('boundary_label_overlap_detected')} "
        f"applied={bpr.get('boundary_purge_applied')} "
        f"rows_purged={bpr.get('rows_purged')}"
    )
    csd = report.get("candidate_signal_diagnostics") or {}
    train_csd = csd.get("train") or {}
    test_csd = csd.get("test") or {}
    print(
        "  sample_size_safe: "
        f"train={train_csd.get('sample_size_safe')} "
        f"test={test_csd.get('sample_size_safe')}"
    )


def _print_cross_period(cross: dict) -> None:
    print("\n=== CROSS-PERIOD AGGREGATE ===")
    print(f"  cross_period_validated: {cross.get('cross_period_validated')}")
    print(f"  period_count:           {cross.get('period_count')}")
    agree = cross.get("agreement_summary") or {}
    print(f"  all_periods_paper_ready: {agree.get('all_periods_paper_ready')}")
    if agree.get("periods_not_ready"):
        print(f"  periods_not_ready:       {agree.get('periods_not_ready')}")
    print(f"  decision_logic: {cross.get('decision_logic')}")
    print(f"  disclaimer: {cross.get('disclaimer')}")


def main() -> int:
    from services.research_protocol.cli_protocol import enforce_protocol_from_args

    args = parse_args()
    enforce_protocol_from_args(args, expected_stage=PROTOCOL_STAGE)

    year_list = list(args.train_years) + [args.test_year]
    discovered = discover_year_datasets(
        year_list, datasets_dir=args.datasets_dir, symbol=args.symbol
    )
    missing = [d for d in discovered if d.get("status") == "missing"]
    if missing:
        print("[error] required model-ready datasets are missing:")
        for d in missing:
            print(f"  year={d['year']}: {d.get('reason')}")
        print(
            "\nGenerate the missing datasets first with:\n"
            "  PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python"
            " scripts/run_model_ready_dataset_oneyear_smoke.py \\\n"
            "    --analysis-start-date YYYY-01-02 --analysis-end-date YYYY-12-31"
        )
        return 2

    new_period = run_ml_regime_validation(
        symbol=args.symbol,
        year_datasets=discovered,
        train_years=[str(y) for y in args.train_years],
        test_year=str(args.test_year),
    )

    if (
        args.train_coverage_start
        or args.train_coverage_end
        or args.train_is_partial
        or args.data_coverage_note
        or args.period_label
    ):
        train_label = (
            f"{args.train_years[0]}_partial"
            if args.train_is_partial and len(args.train_years) == 1
            else "+".join(args.train_years)
        )
        derived_label = f"train={train_label}; test={args.test_year}"
        new_period.report["data_coverage"] = {
            "period_label": args.period_label or derived_label,
            "train_coverage_start": args.train_coverage_start,
            "train_coverage_end": args.train_coverage_end,
            "train_is_partial": bool(args.train_is_partial),
            "data_coverage_note": args.data_coverage_note,
        }

    new_report_path = write_ml_regime_validation_report(
        new_period.report,
        stem=f"{args.symbol.lower()}_{'-'.join(args.train_years)}-{args.test_year}_ml_regime_validation_cross_period",
    )

    reports_for_aggregate = [new_period.report]
    baseline_path: Path | None = None
    if not args.no_baseline_aggregate:
        if args.baseline_report and args.baseline_report.lower() != "none":
            baseline_path = Path(args.baseline_report).expanduser().resolve()
        else:
            default_baseline = (
                ROOT / "reports/ml_diagnostics/spy_2023-2024-2025_ml_regime_validation.json"
            )
            if default_baseline.exists():
                baseline_path = default_baseline
        if baseline_path and baseline_path.exists():
            with baseline_path.open("r", encoding="utf-8") as fh:
                baseline_report = json.load(fh)
            reports_for_aggregate.insert(0, baseline_report)

    cross = aggregate_cross_period_validation(reports_for_aggregate)
    cross_path = write_cross_period_report(cross)

    if args.json:
        payload = {
            "new_period_report_path": str(new_report_path),
            "baseline_report_path": str(baseline_path) if baseline_path else None,
            "cross_period_report_path": str(cross_path),
            "new_period_report": new_period.report,
            "cross_period_aggregate": cross,
        }
        print(json.dumps(payload, indent=2, default=str))
        return 0 if cross.get("cross_period_validated") else 1

    print(f"PivotQuant cross-period regime validation")
    print(f"Symbol: {args.symbol}")
    print(f"New period: train={'+'.join(args.train_years)} test={args.test_year}")
    print(f"New period report: {new_report_path}")
    if baseline_path:
        print(f"Baseline report:   {baseline_path}")
    else:
        print("Baseline report:   (none aggregated)")
    print(f"Cross-period report: {cross_path}")

    if baseline_path:
        with baseline_path.open("r", encoding="utf-8") as fh:
            baseline_report = json.load(fh)
        _print_period_summary("BASELINE: 2023+2024 -> 2025", baseline_report)
    _print_period_summary(
        f"NEW PERIOD: {'+'.join(args.train_years)} -> {args.test_year}",
        new_period.report,
    )
    _print_cross_period(cross)

    return 0 if cross.get("cross_period_validated") else 1


if __name__ == "__main__":
    raise SystemExit(main())
