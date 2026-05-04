#!/usr/bin/env python3
"""Run bounded multi-year diagnostics; no training, tuning, or edge claims."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.ml_multiyear_diagnostics import (
    DEFAULT_DAILY_SOURCE,
    DEFAULT_FEATURE_LOOKBACK_DAYS,
    DEFAULT_LABEL_LOOKAHEAD_DAYS,
    DEFAULT_MAX_FILES,
    DEFAULT_YEAR_WINDOWS,
    run_ml_multiyear_diagnostics,
    write_ml_multiyear_diagnostics_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bounded multi-year diagnostics for model-ready datasets.")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--feature-lookback-days", type=int, default=DEFAULT_FEATURE_LOOKBACK_DAYS)
    parser.add_argument("--label-lookahead-days", type=int, default=DEFAULT_LABEL_LOOKAHEAD_DAYS)
    parser.add_argument("--max-files", type=int, default=DEFAULT_MAX_FILES)
    parser.add_argument("--daily-source", default=DEFAULT_DAILY_SOURCE, choices=["yahoo", "ivolatility", "auto"])
    parser.add_argument("--horizons", default="1d,5d,21d")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _print_text(report: dict, report_path: Path) -> None:
    print("PivotQuant ML multi-year diagnostics")
    print(f"Status: {report['status']}")
    print(f"Symbol: {report['symbol']}")
    print(f"Years requested: {', '.join(report['years_requested'])}")
    print(f"Successful years: {report['successful_year_count']} / {report['year_count']}")
    print(f"Report: {report_path}")

    print("\n[years]")
    for year in report["year_reports"]:
        export = year.get("export", {})
        rows = export.get("rows", {})
        print(
            f"  {year['year']}: status={year['status']} "
            f"actual={year.get('actual_start_date')}->{year.get('actual_end_date')} "
            f"exported={rows.get('exported')} fully_labeled={export.get('fully_labeled_row_count')}"
        )
        rates = year.get("target_positive_rates") or {}
        if rates:
            print(
                f"    target_rates: 1d={rates.get('forward_return_1d_positive')} "
                f"5d={rates.get('forward_return_5d_positive')} "
                f"21d={rates.get('forward_return_21d_positive')}"
            )

    print("\n[cross_year]")
    stability = report["cross_year_stability"]
    print(f"  persisting_regimes: {sum(1 for row in stability['persisting_regimes'] if row['persistent'])}")
    print(f"  feature_sign_flips: {sum(1 for row in stability['feature_sign_flips_across_years'] if row['flipped'])}")
    print(f"  target_21d: {stability['target_21d_stability']['answer']}")

    recommendation = report["final_recommendation"]
    print("\n[recommendation]")
    print(f"  action: {recommendation['action']}")
    for reason in recommendation["reasons"]:
        print(f"  reason: {reason}")

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")
    print("[WARN] no edge claim")


def main() -> int:
    args = parse_args()
    horizons = [part.strip() for part in args.horizons.split(",") if part.strip()]
    result = run_ml_multiyear_diagnostics(
        symbol=args.symbol,
        year_windows=DEFAULT_YEAR_WINDOWS,
        feature_lookback_days=args.feature_lookback_days,
        label_lookahead_days=args.label_lookahead_days,
        max_files=args.max_files,
        daily_source=args.daily_source,
        horizons=horizons,
    )
    report_path = write_ml_multiyear_diagnostics_report(result.report)
    if args.json:
        payload = dict(result.report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(result.report, report_path)
    return 0 if result.report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
