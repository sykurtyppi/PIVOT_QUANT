#!/usr/bin/env python3
"""Run fixed realized_vol_60d regime benchmark diagnostics."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.ml_regime_benchmark import (
    DEFAULT_YEAR_DATASETS,
    SMALL_SAMPLE_THRESHOLD,
    run_ml_regime_benchmark,
    write_ml_regime_benchmark_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fixed realized_vol_60d regime benchmark diagnostics.")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--small-sample-threshold", type=int, default=SMALL_SAMPLE_THRESHOLD)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _print_text(report: dict, report_path: Path) -> None:
    stability = report["stability_summary"]
    print("PivotQuant realized_vol_60d regime benchmark")
    print(f"Status: {report['status']}")
    print(f"Symbol: {report['symbol']}")
    print(f"Successful years: {report['successful_year_count']} / {report['year_count']}")
    print(f"Report: {report_path}")

    print("\n[years]")
    for year in report["year_reports"]:
        if year.get("status") != "ok":
            print(f"  {year['year']}: status={year['status']}")
            continue
        baseline = year["benchmarks"]["all_rows"]
        high = year["benchmarks"]["realized_vol_60d_high"]
        low = year["benchmarks"]["realized_vol_60d_low"]
        high_delta = year["comparisons_to_all_rows"]["realized_vol_60d_high"]
        low_delta = year["comparisons_to_all_rows"]["realized_vol_60d_low"]
        print(
            f"  {year['year']}: all_rows n={baseline['rows']} pos={baseline['positive_rate']} "
            f"mean5d={baseline['mean_forward_return_5d']}"
        )
        print(
            f"    high_vol n={high['rows']} pos={high['positive_rate']} "
            f"delta_pos={high_delta['positive_rate_delta']} delta_mean5d={high_delta['mean_forward_return_5d_delta']}"
        )
        print(
            f"    low_vol n={low['rows']} pos={low['positive_rate']} "
            f"delta_pos={low_delta['positive_rate_delta']} delta_mean5d={low_delta['mean_forward_return_5d_delta']}"
        )

    print("\n[stability]")
    print(f"  high_vol_improves_each_year: {stability['high_vol_improves_positive_rate_each_year']}")
    print(f"  low_vol_underperforms_each_year: {stability['low_vol_underperforms_positive_rate_each_year']}")
    print(f"  directionally_stable: {stability['directionally_stable']}")
    print(f"  interesting_effect_size: {stability['interesting_effect_size']}")
    print(f"  interpretation: {stability['interpretation']}")

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")
    print("[WARN] no edge claim")


def main() -> int:
    args = parse_args()
    result = run_ml_regime_benchmark(
        symbol=args.symbol,
        year_datasets=DEFAULT_YEAR_DATASETS,
        small_sample_threshold=args.small_sample_threshold,
    )
    report_path = write_ml_regime_benchmark_report(result.report)
    if args.json:
        payload = dict(result.report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(result.report, report_path)
    return 0 if result.report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
