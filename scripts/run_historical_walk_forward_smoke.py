#!/usr/bin/env python3
"""Bounded walk-forward smoke with optional rule-baseline and regime-conditioning."""

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
    WalkForwardRegimeConfig,
    WalkForwardRuleConfig,
    build_historical_walk_forward_from_t9,
    write_walk_forward_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only historical walk-forward dry-run smoke with optional "
        "rule-baseline scoring and regime conditioning."
    )
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
    parser.add_argument("--train-window-days", type=int, default=10, dest="train_window")
    parser.add_argument("--test-window-days", type=int, default=5, dest="test_window")
    parser.add_argument("--step-days", type=int, default=5, dest="step")
    # Rule-baseline flags
    parser.add_argument(
        "--option-type",
        choices=["both", "call", "put"],
        default="both",
        dest="option_type",
    )
    parser.add_argument("--min-open-interest", type=int, default=0, dest="min_open_interest")
    parser.add_argument("--min-volume", type=int, default=0, dest="min_volume")
    parser.add_argument("--moneyness-bucket", default=None, dest="moneyness_bucket")
    # Regime flags
    parser.add_argument(
        "--regime-signal",
        choices=["realized_vol_20d", "none"],
        default="none",
        dest="regime_signal",
        help="Regime signal computed at each train_end. Default: none.",
    )
    parser.add_argument(
        "--regime-buckets",
        type=int,
        default=3,
        dest="regime_buckets",
        help="Number of regime buckets (1, 2, or 3). Default: 3 (low/mid/high).",
    )
    parser.add_argument(
        "--regime-lookback-days",
        type=int,
        default=20,
        dest="regime_lookback_days",
        help="Trading-day lookback for realized vol computation. Default: 20.",
    )
    # Display flags
    parser.add_argument(
        "--summary-only",
        action="store_true",
        dest="summary_only",
        help="Print cross-window summary only; suppress per-window detail.",
    )
    parser.add_argument("--json", action="store_true", help="Print full JSON report.")
    return parser.parse_args()


def _fmt_ret(value: float | None) -> str:
    return f"{value:.4f}" if value is not None else "n/a"


def _fmt_pct(value: float | None) -> str:
    return f"{value:.1%}" if value is not None else "n/a"


def _print_text(report: dict, report_path: Path | None, *, summary_only: bool = False) -> None:
    rule_applied = report.get("rule_baseline_applied", False)
    regime_applied = report.get("regime_applied", False)
    label = "walk-forward"
    if rule_applied:
        label += " + rule-baseline"
    if regime_applied:
        label += " + regime"
    print(f"PivotQuant historical {label}")
    print(f"Symbol: {report['symbol']}")
    print(f"Window: {report['start_date']} -> {report['end_date']}")
    print(f"Horizons: {', '.join(report.get('config', {}).get('horizons') or [])}")
    cfg = report.get("config", {})
    print(
        f"Train/test/step: {cfg['train_window_trading_days']}/"
        f"{cfg['test_window_trading_days']}/{cfg['step_trading_days']} trading days"
    )
    print(f"Training performed: {report['training_performed']}")
    print(f"Threshold optimization performed: {report['threshold_optimization_performed']}")
    print(f"Status: {report['status']}")
    if rule_applied:
        rb_cfg = cfg.get("rule_baseline") or {}
        print(
            f"Rule filters: option_type={rb_cfg.get('option_type')} "
            f"min_oi={rb_cfg.get('min_open_interest')} "
            f"min_vol={rb_cfg.get('min_volume')} "
            f"moneyness_bucket={rb_cfg.get('moneyness_bucket')}"
        )
    if regime_applied:
        re_cfg = cfg.get("regime") or {}
        print(
            f"Regime: signal={re_cfg.get('signal')} "
            f"buckets={re_cfg.get('n_buckets')} "
            f"lookback={re_cfg.get('lookback_days')}d"
        )
    if report_path is not None:
        print(f"Report: {report_path}")

    cws = report.get("cross_window_summary") or {}
    print("\n[cross-window summary]")
    print(f"  total_windows:          {cws.get('total_windows', report.get('window_count', 0))}")
    if rule_applied:
        print(f"  evaluable_windows:      {cws.get('evaluable_windows', 'n/a')}")
        print(f"  non_evaluable_windows:  {cws.get('non_evaluable_windows', 'n/a')}")
        print(f"  total_selected_rows:    {cws.get('total_selected_rows', 0)}")
    else:
        print(f"  zero_row_windows:       {cws.get('zero_row_window_count', 0)}")
    print(f"  zero_row_fraction:      {_fmt_pct(cws.get('zero_row_window_fraction'))}")
    print(f"  leakage_checks:         {report['leakage_checks']['status']}")

    if rule_applied:
        by_horizon = cws.get("by_horizon") or {}
        if by_horizon:
            print("\n  [by horizon]")
            for hz, stats in sorted(by_horizon.items()):
                print(
                    f"    {hz}: selected={stats.get('selected_rows', 0)} "
                    f"mean={_fmt_ret(stats.get('mean_return'))} "
                    f"win_rate={_fmt_pct(stats.get('win_rate'))}"
                )

        best = cws.get("best_window")
        worst = cws.get("worst_window")
        if best:
            print(
                f"\n  best window:  {best['window_id']} ({best['test_start']}→{best['test_end']}) "
                f"mean={_fmt_ret(best.get('mean_return'))} n={best.get('selected_rows')}"
            )
        if worst:
            print(
                f"  worst window: {worst['window_id']} ({worst['test_start']}→{worst['test_end']}) "
                f"mean={_fmt_ret(worst.get('mean_return'))} n={worst.get('selected_rows')}"
            )

        means = cws.get("window_mean_returns") or []
        if means:
            print(f"\n  window mean returns: [{', '.join(_fmt_ret(m) for m in means)}]")

    if regime_applied:
        by_regime = cws.get("by_regime") or {}
        if by_regime:
            print("\n  [by regime]")
            for bucket in sorted(by_regime):
                rs = by_regime[bucket]
                print(
                    f"    {bucket}: windows={rs['total_windows']} "
                    f"evaluable={rs['evaluable_windows']} "
                    f"selected={rs['total_selected_rows']}"
                )
                for hz, hz_stats in sorted((rs.get("by_horizon") or {}).items()):
                    print(
                        f"      {hz}: selected={hz_stats.get('selected_rows', 0)} "
                        f"mean={_fmt_ret(hz_stats.get('mean_return'))} "
                        f"win_rate={_fmt_pct(hz_stats.get('win_rate'))}"
                    )
                bw = rs.get("best_window")
                ww = rs.get("worst_window")
                if bw:
                    print(f"      best:  {bw['window_id']} mean={_fmt_ret(bw.get('mean_return'))}")
                if ww:
                    print(f"      worst: {ww['window_id']} mean={_fmt_ret(ww.get('mean_return'))}")

    if not summary_only:
        print("\n[windows]")
        for window in report.get("windows", []):
            regime_tag = ""
            if "regime" in window:
                r = window["regime"]
                regime_tag = f" [{r['bucket']} rv={_fmt_ret(r.get('train_end_realized_vol'))}]"
            base_line = (
                f"  {window['window_id']}: train {window['train_start']}->{window['train_end']}"
                f"{regime_tag} | test {window['test_start']}->{window['test_end']} "
                f"rows={window['test_row_count']} zero={window['zero_row_window']}"
            )
            print(base_line)
            rb = window.get("rule_baseline")
            if rb:
                fr = rb.get("forward_return") or {}
                if rb.get("non_evaluable"):
                    reason = rb.get("non_evaluable_reason") or ""
                    print(f"    rule_baseline: selected={rb['selected_rows']} non_evaluable ({reason})")
                else:
                    print(
                        f"    rule_baseline: eligible={rb['eligible_rows']} selected={rb['selected_rows']} "
                        f"rate={_fmt_pct(rb.get('selection_rate'))} "
                        f"mean={_fmt_ret(fr.get('mean'))} median={_fmt_ret(fr.get('median'))} "
                        f"win_rate={_fmt_pct(fr.get('win_rate'))} n={fr.get('sample_size')}"
                    )

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")


def main() -> int:
    args = parse_args()
    horizons = [part.strip() for part in args.horizons.split(",") if part.strip()]
    rule_config = WalkForwardRuleConfig(
        option_type=args.option_type,
        min_open_interest=args.min_open_interest,
        min_volume=args.min_volume,
        moneyness_bucket=args.moneyness_bucket,
    )
    regime_config: WalkForwardRegimeConfig | None = None
    if args.regime_signal != "none":
        regime_config = WalkForwardRegimeConfig(
            signal=args.regime_signal,
            n_buckets=args.regime_buckets,
            lookback_days=args.regime_lookback_days,
        )
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
        rule_config=rule_config,
        regime_config=regime_config,
    )
    report = wf.report
    report_path = write_walk_forward_report(report)

    if args.json:
        payload = dict(report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(report, report_path, summary_only=args.summary_only)
    return 0 if report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
