#!/usr/bin/env python3
"""Run strict realized_vol_60d train/test regime validation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.external_data.ml_regime_validation import (
    DEFAULT_TEST_YEAR,
    DEFAULT_TRAIN_YEARS,
    run_ml_regime_validation,
    write_ml_regime_validation_report,
)


PROTOCOL_STAGE = 2  # single-period out-of-sample (RESEARCH_PROTOCOL §3, stage 2)


def parse_args() -> argparse.Namespace:
    from services.research_protocol.cli_protocol import add_protocol_arguments

    parser = argparse.ArgumentParser(description="Strict realized_vol_60d train/test regime validation.")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument("--train-years", nargs="+", default=DEFAULT_TRAIN_YEARS)
    parser.add_argument("--test-year", default=DEFAULT_TEST_YEAR)
    parser.add_argument("--json", action="store_true")
    add_protocol_arguments(parser, expected_stage=PROTOCOL_STAGE)
    return parser.parse_args()


def _print_text(report: dict, report_path: Path) -> None:
    print("PivotQuant realized_vol_60d strict validation")
    print(f"Status: {report['status']}")
    print(f"Symbol: {report['symbol']}")
    print(f"Train years: {', '.join(report['train_years'])}")
    print(f"Test year: {report['test_year']}")
    print(f"Validated: {report['validated']}")
    print(f"Degradation warning: {report['degradation_warning']}")
    print(f"Report: {report_path}")

    if "regime_definition" in report:
        definition = report["regime_definition"]
        print("\n[regime definition]")
        print(f"  feature: {definition['feature']}")
        print(f"  bucket_logic: {definition['bucket_logic']}")
        print(f"  split_source: {definition['split_source']}")
        print(f"  split_value: {definition['split_value']}")

    for period in ["train", "test"]:
        if period not in report or "benchmarks" not in report[period]:
            continue
        print(f"\n[{period}]")
        for name in ["all_rows", "realized_vol_60d_high", "realized_vol_60d_low"]:
            stats = report[period]["benchmarks"][name]
            print(
                f"  {name}: n={stats['sample_size']} "
                f"pos5d={stats['positive_rate_5d']} mean5d={stats['mean_forward_return_5d']}"
            )
        if "comparisons_to_all_rows" in report[period]:
            for name, comparison in report[period]["comparisons_to_all_rows"].items():
                print(
                    f"  {name} vs all_rows: "
                    f"delta_pos5d={comparison['positive_rate_delta']} "
                    f"delta_mean5d={comparison['mean_forward_return_5d_delta']}"
                )

    if "train_vs_test" in report:
        print("\n[win rate stability vs train]")
        for name, stability in report["train_vs_test"]["win_rate_stability_vs_train"].items():
            print(
                f"  {name}: train_delta={stability['train_positive_rate_delta_vs_all']} "
                f"test_delta={stability['test_positive_rate_delta_vs_all']} "
                f"win_rate_preserved={stability['win_rate_direction_preserved_vs_all']} "
                f"mean_return_preserved={stability['mean_return_direction_preserved_vs_all']}"
            )
    if "degradation_metric" in report:
        print(f"\n[degradation] max_abs_change={report['degradation_metric']['max_absolute_change']}")

    if "two_dimensional_conditioning" in report:
        two_d = report["two_dimensional_conditioning"]
        print("\n[2D conditioning]")
        print(f"  trend_feature: {two_d['trend_definition']['feature']}")
        print(f"  worked_in_train: {', '.join(two_d['worked_in_train']) or 'none'}")
        print(f"  survived_in_test: {', '.join(two_d['survived_in_test']) or 'none'}")
        print(f"  stable_buckets: {', '.join(two_d['stable_buckets']) or 'none'}")
        for name, bucket in two_d["train_vs_test"].items():
            print(
                f"  {name}: stable={bucket['stable_bucket']} "
                f"train_pos_delta={bucket['train_positive_rate_delta']} "
                f"test_pos_delta={bucket['test_positive_rate_delta']} "
                f"train_mean_delta={bucket['train_mean_return_delta']} "
                f"test_mean_delta={bucket['test_mean_return_delta']}"
            )

    if "time_slice_robustness" in report:
        robustness = report["time_slice_robustness"]
        summary = robustness.get("consistency_summary", {})
        print("\n[time-slice robustness]")
        print(f"  bucket: {robustness['bucket']}")
        print(f"  robust_across_time: {robustness['robust_across_time']}")
        print(f"  slice_instability_warning: {robustness['slice_instability_warning']}")
        print(f"  consistency_score: {summary.get('consistency_score')}")
        print(f"  mean_return_variance: {summary.get('mean_return_5d_variance_across_slices')}")
        for item in robustness.get("per_slice", []):
            print(
                f"  {item['slice']}: n={item['sample_size']} "
                f"win5d={item['win_rate_5d']} mean5d={item['mean_return_5d']} "
                f"positive={item['positive_slice']}"
            )

    if "failure_explanation_diagnostics" in report:
        explanation = report["failure_explanation_diagnostics"]
        print("\n[failure explanation diagnostics]")
        print(f"  failing_slice: {explanation['failing_slice']}")
        print(f"  working_slices: {', '.join(explanation['working_slices'])}")
        print(f"  candidate_explanatory_variable: {explanation['candidate_explanatory_variable']}")
        print(
            "  materially_different_variables: "
            f"{', '.join(explanation['materially_different_variables']) or 'none'}"
        )
        for row in explanation.get("table", []):
            diff = row["differences"]
            print(
                f"  {row['variable']}: fail_mean={row['failing']['mean']} "
                f"work_mean={row['working']['mean']} "
                f"std_mean_diff={diff['standardized_mean_difference']} "
                f"material={row['material_difference']}"
            )

    if "vol_regime_change_diagnostics" in report:
        vol_change = report["vol_regime_change_diagnostics"]
        print("\n[vol regime change diagnostics]")
        print(f"  bucket: {vol_change['bucket']}")
        print(f"  vol_expansion_explains_failure: {vol_change['vol_expansion_explains_failure']}")
        for period in ["train", "test"]:
            comparison = vol_change[period]["expansion_vs_compression"]
            print(
                f"  {period} expansion_vs_compression: "
                f"win_delta={comparison['win_rate_delta']} "
                f"mean_delta={comparison['mean_return_delta']}"
            )
            for name in ["vol_expansion", "vol_compression"]:
                stats = vol_change[period]["table"][name]
                print(
                    f"    {name}: n={stats['sample_size']} "
                    f"win5d={stats['positive_rate_5d']} mean5d={stats['mean_forward_return_5d']}"
                )
        q1_comparison = vol_change["q1_vs_q2_q4"]["comparison"]
        print(
            "  q1_vs_q2_q4: "
            f"q1_expansion_share={q1_comparison['q1_expansion_share']} "
            f"q2_q4_expansion_share={q1_comparison['q2_q4_expansion_share']} "
            f"q1_minus_q2_q4_mean={q1_comparison['q1_minus_q2_q4_mean_return']}"
        )

    if "trend_maturity_diagnostics" in report:
        maturity = report["trend_maturity_diagnostics"]
        print("\n[trend maturity diagnostics]")
        print(f"  bucket: {maturity['bucket']}")
        print(f"  used_variable: {maturity['definitions']['used_variable']}")
        print(f"  threshold_value: {maturity['definitions']['threshold_value']}")
        print(f"  trend_maturity_explains_failure: {maturity['trend_maturity_explains_failure']}")
        for period in ["train", "test"]:
            comparison = maturity[period]["late_vs_early"]
            print(
                f"  {period} late_vs_early: "
                f"win_delta={comparison['win_rate_delta']} "
                f"mean_delta={comparison['mean_return_delta']}"
            )
            for name in ["late_trend", "early_trend"]:
                stats = maturity[period]["table"][name]
                print(
                    f"    {name}: n={stats['sample_size']} "
                    f"win5d={stats['positive_rate_5d']} mean5d={stats['mean_forward_return_5d']}"
                )
        q1_comparison = maturity["q1_vs_q2_q4"]["comparison"]
        print(
            "  q1_vs_q2_q4: "
            f"q1_late_share={q1_comparison['q1_late_trend_share']} "
            f"q2_q4_late_share={q1_comparison['q2_q4_late_trend_share']} "
            f"q1_minus_q2_q4_mean={q1_comparison['q1_minus_q2_q4_mean_return']}"
        )
        stability = maturity.get("time_stability", {})
        print(
            "  time_stability: "
            f"quarters_consistent={stability.get('quarters_consistent')} "
            f"total_quarters={stability.get('total_quarters')} "
            f"ratio={stability.get('consistency_ratio')} "
            f"stable={stability.get('trend_maturity_stable')}"
        )
        for row in stability.get("per_quarter", []):
            print(
                f"    {row['quarter']}: late_win={row['late_win_rate_5d']} "
                f"early_win={row['early_win_rate_5d']} "
                f"diff={row['difference_late_minus_early']} "
                f"n={row['sample_size']}"
            )

    if "late_trend_filter_impact" in report:
        impact = report["late_trend_filter_impact"]
        print("\n[late trend filter impact]")
        print(f"  bucket: {impact['bucket']}")
        print(f"  threshold_value: {impact['definitions']['threshold_value']}")
        print(f"  filter_improves_performance: {impact['filter_improves_performance']}")
        for period in ["train", "test"]:
            delta = impact[period]["delta"]
            print(
                f"  {period}: delta_win={delta['delta_win_rate']} "
                f"delta_mean={delta['delta_mean_return']} rows_removed={delta['rows_removed']}"
            )
            for name in ["baseline_no_filter", "early_trend_only"]:
                row = impact[period]["table"][name]
                print(
                    f"    {row['scenario']}: n={row['sample_size']} "
                    f"win5d={row['win_rate_5d']} mean5d={row['mean_return_5d']}"
                )

    if "overextension_penalty_comparison" in report:
        penalty = report["overextension_penalty_comparison"]
        print("\n[overextension penalty comparison]")
        print(f"  bucket: {penalty['bucket']}")
        print(f"  threshold_value: {penalty['definitions']['threshold_value']}")
        print(f"  soft_penalty_preferred: {penalty['soft_penalty_preferred']}")
        for period in ["train", "test"]:
            print(f"  {period}: preferred={penalty[period]['soft_penalty_preferred']}")
            for name in [
                "baseline_no_adjustment",
                "hard_filter_early_trend_only",
                "soft_penalty_late_trend_half_weight",
            ]:
                row = penalty[period]["table"][name]
                print(
                    f"    {row['scenario']}: n={row['sample_size']} "
                    f"effective_n={row['effective_sample_size']} "
                    f"weighted_win5d={row['weighted_win_rate_5d']} "
                    f"weighted_mean5d={row['weighted_mean_return_5d']}"
                )
            soft_delta = penalty[period]["deltas_vs_baseline"]["soft_penalty_late_trend_half_weight"]
            retention = penalty[period]["soft_retention_vs_hard_filter"]
            print(
                f"    soft_delta: win={soft_delta['delta_win_rate']} "
                f"mean={soft_delta['delta_mean_return']} "
                f"win_retention={retention['win_rate_retention']} "
                f"mean_retention={retention['mean_return_retention']}"
            )

    if "overextension_method_comparison" in report:
        comparison = report["overextension_method_comparison"]
        ranking = comparison["ranking"]
        print("\n[overextension method comparison]")
        print(f"  bucket: {comparison['bucket']}")
        print(f"  best_performing_method: {ranking['best_performing_method']}")
        print(f"  most_stable_method: {ranking['most_stable_method']}")
        print(f"  current_method_optimal: {ranking['current_method_optimal']}")
        for row in comparison.get("table", []):
            print(
                f"  {row['method']}: rank={row['rank']} status={row['status']} "
                f"threshold={row['threshold_value']} "
                f"train_delta_win={row['train_delta_win']} "
                f"test_delta_win={row['test_delta_win']} "
                f"train_delta_return={row['train_delta_return']} "
                f"test_delta_return={row['test_delta_return']}"
            )

    if "trend_maturity_independence_diagnostics" in report:
        independence = report["trend_maturity_independence_diagnostics"]
        print("\n[trend maturity independence diagnostics]")
        print(f"  bucket: {independence['bucket']}")
        print(f"  trend_maturity_independent: {independence['trend_maturity_independent']}")
        print(f"  momentum_threshold: {independence['definitions']['momentum_threshold']}")
        print(f"  trend_maturity_threshold: {independence['definitions']['trend_maturity_threshold']}")
        for period in ["train", "test"]:
            print(f"  {period}: independent={independence[period]['trend_maturity_independent']}")
            for name in [
                "low_momentum_early_trend",
                "low_momentum_late_trend",
                "high_momentum_early_trend",
                "high_momentum_late_trend",
            ]:
                row = independence[period]["table"][name]
                print(
                    f"    {row['momentum_bucket']} + {row['trend_bucket']}: "
                    f"n={row['sample_size']} win5d={row['positive_rate_5d']} "
                    f"mean5d={row['mean_forward_return_5d']}"
                )
            for comparison in independence[period]["comparisons"].values():
                print(
                    f"    {comparison['momentum_bucket']} early_vs_late: "
                    f"win_delta={comparison['early_minus_late_win_rate']} "
                    f"mean_delta={comparison['early_minus_late_mean_return']} "
                    f"early_outperforms={comparison['early_outperforms_late']}"
                )

    if "overextension_fragility_diagnostics" in report:
        fragility = report["overextension_fragility_diagnostics"]
        flags = fragility.get("flags", {})
        print("\n[overextension fragility diagnostics]")
        print(f"  status: {fragility['status']}")
        print(f"  threshold_value: {fragility['definitions']['threshold_value']}")
        print(f"  flags: sample_size_safe={flags.get('sample_size_safe')} overfiltering_risk={flags.get('overfiltering_risk')} fragility_warning={flags.get('fragility_warning')}")
        for period in ["train", "test"]:
            summary = fragility.get(period, {})
            baseline = summary.get("baseline_no_adjustment", {})
            hard = summary.get("hard_filter_early_trend_only", {})
            print(
                f"  {period} baseline: total={baseline.get('total_rows')} "
                f"mean={baseline.get('mean_5d_return')} win={baseline.get('win_rate')}"
            )
            print(
                f"  {period} hard_filter: kept={hard.get('rows_kept')} removed={hard.get('rows_removed')} "
                f"pct_removed={hard.get('percent_removed')} "
                f"mean={hard.get('mean_5d_return')} win={hard.get('win_rate')} "
                f"variance={hard.get('return_variance_across_time_slices')} "
                f"avg_per_q={hard.get('avg_selected_rows_per_quarter')} "
                f"min_per_q={hard.get('min_selected_rows_per_quarter')} "
                f"low_sample_qs={hard.get('num_low_sample_quarters')}"
            )
        for row in fragility.get("per_quarter_test", []):
            print(
                f"  {row['quarter']}: baseline={row['baseline_rows']} "
                f"hard_filter={row['hard_filter_rows']} "
                f"pct_removed={row.get('percent_removed')} "
                f"hard_mean={row.get('hard_filter_mean_5d_return')} "
                f"hard_win={row.get('hard_filter_win_rate')}"
            )

    if "boundary_purge_report" in report:
        bpr = report["boundary_purge_report"]
        print("\n[boundary purge report]")
        print(f"  status: {bpr.get('status')}")
        print(f"  boundary_label_overlap_detected: {bpr.get('boundary_label_overlap_detected')}")
        print(f"  boundary_purge_applied:          {bpr.get('boundary_purge_applied')}")
        print(f"  train_rows_before_purge: {bpr.get('train_rows_before_purge')}")
        print(f"  train_rows_after_purge:  {bpr.get('train_rows_after_purge')}")
        print(f"  rows_purged:             {bpr.get('rows_purged')}")
        print(f"  max_label_date_retained: {bpr.get('max_label_date_retained')}")
        print(f"  test_start:              {bpr.get('test_start')}")
        print(f"  embargo_horizon_bdays:   {bpr.get('embargo_horizon_bdays')}")

    if "late_trend_removal_validation" in report:
        ltr = report["late_trend_removal_validation"]
        bv = ltr.get("baseline_validation", {})
        fv = ltr.get("filtered_validation", {})
        impr = ltr.get("improvement_summary", {})
        print("\n[late trend removal validation]")
        print(f"  status: {ltr['status']}")
        print(f"  threshold_value: {ltr['definitions']['threshold_value']}")
        print(f"  baseline_validated: {bv.get('validated')}")
        print(f"  filtered_validated: {fv.get('validated')}")
        print(f"  late_trend_removal_fixes_signal: {ltr['late_trend_removal_fixes_signal']}")
        for period in ["train", "test"]:
            b = bv.get(period, {})
            f = fv.get(period, {})
            print(
                f"  baseline {period}: n={b.get('sample_size')} "
                f"win={b.get('win_rate_5d')} mean={b.get('mean_return_5d')} "
                f"delta_win={b.get('win_rate_delta_vs_all')} delta_mean={b.get('mean_return_delta_vs_all')}"
            )
            print(
                f"  filtered {period}: n={f.get('sample_size')} "
                f"win={f.get('win_rate_5d')} mean={f.get('mean_return_5d')} "
                f"delta_win={f.get('win_rate_delta_vs_all')} delta_mean={f.get('mean_return_delta_vs_all')}"
            )
        print(
            f"  improvement_summary: "
            f"rows_removed_train={impr.get('rows_removed_train')} "
            f"rows_removed_test={impr.get('rows_removed_test')} "
            f"win_change_test={impr.get('win_rate_change_test')} "
            f"mean_change_test={impr.get('mean_return_change_test')}"
        )

    if "paper_eval_diagnostics" in report:
        ped = report["paper_eval_diagnostics"]
        print("\n[paper eval diagnostics]")
        print(f"  status: {ped['status']}")
        if ped["status"] == "ok":
            fl = ped.get("flags", {})
            th = ped.get("thresholds", {})
            print(f"  vol_split_value: {th.get('vol_split_value')}")
            print(f"  maturity_threshold: {th.get('maturity_threshold')}")
            print(
                f"  flags: live_trading_enabled={fl.get('live_trading_enabled')} "
                f"execution_assumptions_included={fl.get('execution_assumptions_included')} "
                f"slippage_mode={fl.get('slippage_mode')} "
                f"commission_mode={fl.get('commission_mode')} "
                f"edge_claim={fl.get('edge_claim')}"
            )
            for period in ["train", "test"]:
                ps = ped.get(period, {}).get("summary", {})
                print(
                    f"  {period}: total_entries={ps.get('total_paper_entries')} "
                    f"win_rate={ps.get('win_rate')} "
                    f"mean={ps.get('mean_return')} "
                    f"median={ps.get('median_return')} "
                    f"best={ps.get('best_return')} "
                    f"worst={ps.get('worst_return')} "
                    f"excluded_late_trend={ps.get('excluded_late_trend_count')} "
                    f"sample_size_warning={ps.get('sample_size_warning')}"
                )
                qb = ps.get("quarterly_breakdown") or []
                for row in qb:
                    print(
                        f"    {row['quarter']}: entries={row['entries']} "
                        f"win_rate={row.get('win_rate')} "
                        f"mean={row.get('mean_return')}"
                    )
            stability = ped.get("test", {}).get("stability", {})
            if stability:
                sf = stability.get("flags", {})
                print(
                    f"  stability: stability_flag={sf.get('stability_flag')} "
                    f"negative_mature_month_warning={sf.get('negative_mature_month_warning')} "
                    f"low_sample_month_warning={sf.get('low_sample_month_warning')} "
                    f"concentration_warning={sf.get('concentration_warning')}"
                )
                for row in stability.get("monthly_breakdown") or []:
                    low = " [low_sample]" if row.get("low_sample") else ""
                    print(
                        f"    {row['month']}{low}: entries={row['entries']} "
                        f"win_rate={row.get('win_rate')} "
                        f"mean={row.get('mean_return')}"
                    )
            print(f"  disclaimer: {ped.get('disclaimer')}")

    if "sensitivity_diagnostics" in report:
        sens = report["sensitivity_diagnostics"]
        print("\n[sensitivity diagnostics]")
        print(f"  status: {sens['status']}")
        if sens["status"] == "ok":
            print(f"  reference_quantile: {sens['reference_quantile']}")
            print(f"  vol_split_value: {sens['vol_split_value']}")
            print(f"  threshold_robust: {sens['threshold_robust']}")
            for row in sens.get("sensitivity_grid") or []:
                print(
                    f"  q={row['quantile']:.2f}: "
                    f"threshold={row.get('maturity_threshold')} "
                    f"signal_rows={row.get('signal_rows')} "
                    f"win_rate={row.get('win_rate')} "
                    f"mean={row.get('mean_return')} "
                    f"stability_flag={row.get('stability_flag')}"
                )

    if "candidate_signal_diagnostics" in report:
        csd = report["candidate_signal_diagnostics"]
        print("\n[candidate signal diagnostics]")
        print(f"  status: {csd['status']}")
        print(f"  signal_name: {csd.get('signal_name')}")
        if csd["status"] == "ok":
            th = csd.get("thresholds", {})
            sp = csd.get("spec", {})
            print(f"  vol_split_value: {th.get('vol_split_value')}")
            print(f"  maturity_threshold: {th.get('maturity_threshold')}")
            print(f"  threshold_source: {th.get('threshold_source')}")
            print(
                f"  spec: live_trading_enabled={sp.get('live_trading_enabled')} "
                f"model_training_performed={sp.get('model_training_performed')} "
                f"performance_claim={sp.get('performance_claim')}"
            )
            for period in ["train", "test"]:
                ps = csd.get(period, {})
                print(
                    f"  {period}: baseline_rows={ps.get('baseline_rows')} "
                    f"signal_rows={ps.get('signal_rows')} "
                    f"late_trend_excluded={ps.get('late_trend_excluded')} "
                    f"win_rate={ps.get('win_rate_5d')} "
                    f"mean_return={ps.get('mean_return_5d')} "
                    f"sample_size_safe={ps.get('sample_size_safe')}"
                )
            print(f"  disclaimer: {csd.get('disclaimer')}")

    if "candidate_readiness_checklist" in report:
        crc = report["candidate_readiness_checklist"]
        print("\n[candidate readiness checklist]")
        print(f"  candidate_ready_for_paper_observation: {crc.get('candidate_ready_for_paper_observation')}")
        print(f"  candidate_status:                      {crc.get('candidate_status')}")
        gov = crc.get("governance_flags", {})
        print(f"  edge_claim_allowed:                    {gov.get('edge_claim_allowed')}")
        print(f"  live_integration_allowed:              {gov.get('live_integration_allowed')}")
        print(f"  prospective_paper_observation_allowed: {gov.get('prospective_paper_observation_allowed')}")
        sm = crc.get("snooping_metadata", {})
        print(f"  diagnostics_explored_count:            {sm.get('diagnostics_explored_count')}")
        print(f"  pre_registered:                        {sm.get('pre_registered')}")
        print(f"  multiple_testing_adjustment_applied:   {sm.get('multiple_testing_adjustment_applied')}")
        print(f"  prospective_validation_required:       {sm.get('prospective_validation_required')}")
        criteria = crc.get("criteria", {})
        print(f"  filtered_validated:        {criteria.get('filtered_validated')}")
        print(f"  sample_size_safe:          {criteria.get('sample_size_safe')}")
        print(f"  overfiltering_risk:        {criteria.get('overfiltering_risk')}")
        print(f"  fragility_warning:         {criteria.get('fragility_warning')}")
        print(f"  stability_flag:            {criteria.get('stability_flag')}")
        print(f"  concentration_warning:     {criteria.get('concentration_warning')}")
        print(f"  threshold_robust:          {criteria.get('threshold_robust')}")
        print(f"  boundary_clean:            {criteria.get('boundary_clean')}")
        print(f"  live_trading_enabled:      {criteria.get('live_trading_enabled')}")
        print(f"  edge_claim:                {criteria.get('edge_claim')}")
        fsd = crc.get("frozen_signal_definition", {})
        print(f"  frozen_signal: {fsd.get('signal_name')} | freeze_note: {fsd.get('freeze_note')}")
        print(f"  disclaimer: {crc.get('disclaimer')}")

    for warning in report.get("warnings") or []:
        print(f"[WARN] {warning}")
    print("[WARN] no statistical edge claim; exploratory paper candidate only")


def main() -> int:
    from services.research_protocol.cli_protocol import enforce_protocol_from_args

    args = parse_args()
    enforce_protocol_from_args(args, expected_stage=PROTOCOL_STAGE)
    result = run_ml_regime_validation(
        symbol=args.symbol,
        train_years=[str(year) for year in args.train_years],
        test_year=str(args.test_year),
    )
    report_path = write_ml_regime_validation_report(result.report)
    if args.json:
        payload = dict(result.report)
        payload["report_path"] = str(report_path)
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_text(result.report, report_path)
    return 0 if result.report.get("status") in {"pass", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
