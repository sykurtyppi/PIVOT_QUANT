#!/usr/bin/env python3
"""Run a bounded walk-forward dry-run smoke without model training."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.historical_label_contract import DEFAULT_HORIZONS
from services.external_data.historical_walk_forward import (
    build_historical_walk_forward_from_t9,
    write_walk_forward_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only historical walk-forward dry-run smoke.")
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
    parser.add_argument("--json", action="store_true", help="Print full JSON report.")
    return parser.parse_args()


def _print_text(report: dict, report_path: Path | None) -> None:
    print("PivotQuant historical walk-forward dry-run")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['start_date']} -> {report['end_date']}")
    print(f"Horizons: {', '.join(report.get('config', {}).get('horizons') or [])}")
    print(f"Train/test/step: {report['config']['train_window_trading_days']}/"
          f"{report['config']['test_window_trading_days']}/{report['config']['step_trading_days']} trading days")
    print(f"Training performed: {report['training_performed']}")
    print(f"Status: {report['status']}")
    if report_path is not None:
        print(f"Report: {report_path}")

    print("\n[summary]")
    print(f"  windows: {report['window_count']}")
    print(f"  zero_row_windows: {report['zero_row_window_count']}")
    print(f"  total_train_rows: {report['total_train_rows']}")
    print(f"  total_test_rows: {report['total_test_rows']}")
    print(f"  leakage_checks: {report['leakage_checks']['status']}")

    print("\n[windows]")
    for window in report.get("windows", []):
        print(
            f"  {window['window_id']}: train {window['train_start']}->{window['train_end']} "
            f"rows={window['train_row_count']} | test {window['test_start']}->{window['test_end']} "
            f"rows={window['test_row_count']} zero={window['zero_row_window']}"
        )

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")


def main() -> int:
    args = parse_args()
    horizons = [part.strip() for part in args.horizons.split(",") if part.strip()]
    wf = build_historical_walk_forward_from_t9(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        max_files=args.max_files,
        daily_source=args.daily_source,
        horizons=horizons,
        train_window=args.train_window,
        test_window=args.test_window,
        step=args.step,
    )
    report = wf.report
    report_path = write_walk_forward_report(report)

    if args.json:
        payload = dict(report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(report, report_path)
    return 0 if report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
