#!/usr/bin/env python3
"""Run deterministic rule-based baselines inside historical WF windows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.historical_label_contract import DEFAULT_HORIZONS
from services.external_data.historical_rule_baseline import (
    RuleBaselineConfig,
    build_historical_rule_baseline_from_t9,
    write_rule_baseline_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only historical rule-baseline smoke.")
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
    parser.add_argument("--horizons", default=",".join(DEFAULT_HORIZONS))
    parser.add_argument("--train-window", type=int, default=10)
    parser.add_argument("--test-window", type=int, default=5)
    parser.add_argument("--step", type=int, default=5)
    parser.add_argument("--max-abs-moneyness", type=float, default=0.01)
    parser.add_argument("--min-volume", type=int, default=1)
    parser.add_argument("--min-open-interest", type=int, default=1)
    parser.add_argument("--max-relative-spread", type=float, default=0.25)
    parser.add_argument("--json", action="store_true", help="Print full JSON report.")
    return parser.parse_args()


def _print_text(report: dict, report_path: Path | None) -> None:
    print("PivotQuant historical rule-baseline smoke")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['start_date']} -> {report['end_date']}")
    print(f"Horizons: {', '.join(report.get('config', {}).get('horizons') or [])}")
    print(f"Training performed: {report['training_performed']}")
    print(f"Threshold optimization performed: {report['threshold_optimization_performed']}")
    print(f"Status: {report['status']}")
    if report_path is not None:
        print(f"Report: {report_path}")

    print("\n[summary]")
    print(f"  windows: {report['window_count']}")
    print(f"  zero/non-evaluable test windows: {report['zero_row_window_count']}")
    print(f"  train selected rows total: {report['train_selected_rows_total']}")
    print(f"  test selected rows total: {report['test_selected_rows_total']}")
    print(f"  leakage checks: {report['leakage_checks']['status']}")

    print("\n[windows]")
    for window in report.get("windows", []):
        train = window["train"]
        test = window["test"]
        print(
            f"  {window['window_id']}: "
            f"train selected={train['selected_rows']} mean={train['forward_return']['mean']} "
            f"| test selected={test['selected_rows']} mean={test['forward_return']['mean']} "
            f"win_rate={test['forward_return']['win_rate']} non_evaluable={test['non_evaluable']}"
        )

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")


def main() -> int:
    args = parse_args()
    horizons = [part.strip() for part in args.horizons.split(",") if part.strip()]
    config = RuleBaselineConfig(
        max_abs_moneyness=args.max_abs_moneyness,
        min_volume=args.min_volume,
        min_open_interest=args.min_open_interest,
        max_relative_spread=args.max_relative_spread,
    )
    baseline = build_historical_rule_baseline_from_t9(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        max_files=args.max_files,
        daily_source=args.daily_source,
        horizons=horizons,
        train_window=args.train_window,
        test_window=args.test_window,
        step=args.step,
        config=config,
    )
    report = baseline.report
    report_path = write_rule_baseline_report(report)

    if args.json:
        payload = dict(report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(report, report_path)
    return 0 if report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
