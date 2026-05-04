import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from services.external_data.ml_regime_validation import (
    build_regime_validation_report,
    write_ml_regime_validation_report,
)


class TestMLRegimeValidation(unittest.TestCase):
    def _frame(self, *, year: str, high_positive: bool = True, rows: int = 40) -> pd.DataFrame:
        dates = pd.bdate_range(f"{year}-01-03", periods=rows)
        midpoint = rows // 2
        high_return = 0.02 if high_positive else -0.02
        low_return = -0.02 if high_positive else 0.02
        return pd.DataFrame(
            {
                "symbol": ["SPY"] * rows,
                "entry_date": dates.strftime("%Y-%m-%d"),
                "underlying_price": [100 + index for index in range(rows)],
                "price_momentum_5d": [0.01 if index % 3 else -0.01 for index in range(rows)],
                "realized_vol_20d": [0.2 if index % 2 else 0.05 for index in range(rows)],
                "realized_vol_60d": [0.1 if index < midpoint else 0.3 for index in range(rows)],
                "price_momentum_20d": [0.05 if index % 4 in {0, 1} else -0.05 for index in range(rows)],
                "forward_return_5d": [low_return if index < midpoint else high_return for index in range(rows)],
            }
        )

    def _year_frame(self, year: str, frame: pd.DataFrame | None = None) -> dict:
        return {
            "year": year,
            "status": "ok",
            "dataset_path": f"/tmp/{year}.csv",
            "metadata_path": f"/tmp/{year}.metadata.json",
            "metadata": {
                "analysis_start_date": f"{year}-01-03",
                "analysis_end_date": f"{year}-12-29",
            },
            "frame": frame if frame is not None else self._frame(year=year),
        }

    def _quarter_test_frame(self, returns: list[float]) -> pd.DataFrame:
        dates = [
            "2025-01-15",
            "2025-04-15",
            "2025-07-15",
            "2025-10-15",
        ]
        return pd.DataFrame(
            {
                "symbol": ["SPY"] * len(dates),
                "entry_date": dates,
                "underlying_price": [100 + index for index in range(len(dates))],
                "price_momentum_5d": [0.01] * len(dates),
                "realized_vol_20d": [0.4] * len(dates),
                "realized_vol_60d": [0.3] * len(dates),
                "price_momentum_20d": [0.05] * len(dates),
                "forward_return_5d": returns,
            }
        )

    def test_train_median_split_is_applied_to_test_without_recalibration(self):
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023"),
                self._year_frame("2024"),
                self._year_frame("2025"),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report

        self.assertTrue(report["validated"])
        self.assertFalse(report["degradation_warning"])
        self.assertEqual(report["regime_definition"]["split_source"], "train period only")
        self.assertEqual(report["test"]["benchmarks"]["realized_vol_60d_high"]["sample_size"], 20)
        self.assertEqual(report["test"]["benchmarks"]["realized_vol_60d_low"]["sample_size"], 20)
        self.assertGreater(report["test"]["comparisons_to_all_rows"]["realized_vol_60d_high"]["positive_rate_delta"], 0)
        self.assertLess(report["test"]["comparisons_to_all_rows"]["realized_vol_60d_low"]["positive_rate_delta"], 0)

    def test_two_dimensional_buckets_report_train_test_survival_flags(self):
        frame = self._frame(year="2023")
        frame.loc[(frame["realized_vol_60d"] == 0.3) & (frame["price_momentum_20d"] > 0), "forward_return_5d"] = 0.03
        result = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=frame),
                self._year_frame("2024", frame=frame.assign(entry_date=pd.bdate_range("2024-01-03", periods=len(frame)).strftime("%Y-%m-%d"))),
                self._year_frame("2025", frame=frame.assign(entry_date=pd.bdate_range("2025-01-03", periods=len(frame)).strftime("%Y-%m-%d"))),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        )
        two_d = result.report["two_dimensional_conditioning"]

        self.assertEqual(two_d["trend_definition"]["feature"], "price_momentum_20d")
        self.assertIn("high_vol_trend_positive", two_d["train_buckets"])
        self.assertIn("high_vol_trend_positive", two_d["test_buckets"])
        self.assertIn("high_vol_trend_positive", two_d["train_vs_test"])
        self.assertIs(two_d["train_vs_test"]["high_vol_trend_positive"]["stable_bucket"], True)

    def test_time_slice_robustness_reports_positive_quarters(self):
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023"),
                self._year_frame("2024"),
                self._year_frame("2025", frame=self._quarter_test_frame([0.01, 0.02, 0.03, 0.04])),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        robustness = report["time_slice_robustness"]

        self.assertTrue(robustness["robust_across_time"])
        self.assertFalse(robustness["slice_instability_warning"])
        self.assertEqual(robustness["consistency_summary"]["consistency_score"], "4/4")
        self.assertEqual(len(robustness["per_slice"]), 4)

    def test_time_slice_robustness_flags_concentrated_or_failed_quarter(self):
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023"),
                self._year_frame("2024"),
                self._year_frame("2025", frame=self._quarter_test_frame([0.01, -0.02, 0.03, 0.04])),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        robustness = report["time_slice_robustness"]

        self.assertFalse(robustness["robust_across_time"])
        self.assertTrue(robustness["slice_instability_warning"])
        self.assertEqual(robustness["consistency_summary"]["consistency_score"], "3/4")
        self.assertGreater(robustness["consistency_summary"]["mean_return_5d_variance_across_slices"], 0)

    def test_failure_explanation_compares_failing_and_working_slices(self):
        dates = pd.bdate_range("2025-01-02", periods=90)
        test_frame = pd.DataFrame(
            {
                "symbol": ["SPY"] * len(dates),
                "entry_date": dates.strftime("%Y-%m-%d"),
                "underlying_price": [100 + index * 0.5 for index in range(len(dates))],
                "price_momentum_5d": [-0.04 if date.quarter == 1 else 0.04 for date in dates],
                "realized_vol_60d": [0.3] * len(dates),
                "price_momentum_20d": [0.05] * len(dates),
                "forward_return_5d": [-0.01 if date.quarter == 1 else 0.02 for date in dates],
            }
        )
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023"),
                self._year_frame("2024"),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        explanation = report["failure_explanation_diagnostics"]

        self.assertEqual(explanation["failing_slice"], "2025_Q1")
        self.assertEqual(explanation["working_slices"], ["2025_Q2", "2025_Q3", "2025_Q4"])
        self.assertTrue(explanation["candidate_explanatory_variable"])
        self.assertIn("price_momentum_5d", explanation["materially_different_variables"])

    def test_vol_regime_change_diagnostics_compare_expansion_and_compression(self):
        dates = pd.bdate_range("2025-01-02", periods=80)
        test_frame = pd.DataFrame(
            {
                "symbol": ["SPY"] * len(dates),
                "entry_date": dates.strftime("%Y-%m-%d"),
                "underlying_price": [100 + index for index in range(len(dates))],
                "price_momentum_5d": [0.01] * len(dates),
                "realized_vol_20d": [0.4 if date.quarter >= 2 else 0.05 for date in dates],
                "realized_vol_60d": [0.3] * len(dates),
                "price_momentum_20d": [0.05] * len(dates),
                "forward_return_5d": [0.02 if date.quarter >= 2 else -0.01 for date in dates],
            }
        )
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023"),
                self._year_frame("2024"),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        vol_change = report["vol_regime_change_diagnostics"]

        self.assertEqual(vol_change["definitions"]["vol_regime_change"], "realized_vol_20d - realized_vol_60d")
        self.assertGreater(vol_change["test"]["table"]["vol_expansion"]["sample_size"], 0)
        self.assertGreater(vol_change["test"]["expansion_vs_compression"]["mean_return_delta"], 0)
        self.assertTrue(vol_change["vol_expansion_explains_failure"])

    def test_trend_maturity_diagnostics_compare_late_and_early_trends(self):
        train_dates = pd.bdate_range("2023-01-02", periods=80)
        train_frame = pd.DataFrame(
            {
                "symbol": ["SPY"] * len(train_dates),
                "entry_date": train_dates.strftime("%Y-%m-%d"),
                "underlying_price": [100 + index for index in range(len(train_dates))],
                "price_momentum_5d": [0.01] * len(train_dates),
                "realized_vol_20d": [0.4] * len(train_dates),
                "realized_vol_60d": [0.3] * len(train_dates),
                "price_momentum_20d": [0.05] * len(train_dates),
                "forward_return_5d": [0.02] * len(train_dates),
                "distance_from_20d_mean": [0.1] * 56 + [1.0] * 24,
            }
        )
        test_dates = pd.bdate_range("2025-01-02", periods=80)
        test_frame = pd.DataFrame(
            {
                "symbol": ["SPY"] * len(test_dates),
                "entry_date": test_dates.strftime("%Y-%m-%d"),
                "underlying_price": [100 + index for index in range(len(test_dates))],
                "price_momentum_5d": [0.01] * len(test_dates),
                "realized_vol_20d": [0.4] * len(test_dates),
                "realized_vol_60d": [0.3] * len(test_dates),
                "price_momentum_20d": [0.05] * len(test_dates),
                "forward_return_5d": [-0.02 if date.quarter == 1 else 0.02 for date in test_dates],
                "distance_from_20d_mean": [1.0 if date.quarter == 1 else 0.1 for date in test_dates],
            }
        )
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=train_frame),
                self._year_frame("2024", frame=train_frame.assign(entry_date=pd.bdate_range("2024-01-02", periods=len(train_frame)).strftime("%Y-%m-%d"))),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        maturity = report["trend_maturity_diagnostics"]

        self.assertEqual(maturity["definitions"]["used_variable"], "distance_from_20d_mean")
        self.assertGreater(maturity["test"]["table"]["late_trend"]["sample_size"], 0)
        self.assertLess(maturity["test"]["late_vs_early"]["mean_return_delta"], 0)
        self.assertTrue(maturity["trend_maturity_explains_failure"])

    def test_trend_maturity_time_stability_counts_quarters(self):
        train_dates = pd.bdate_range("2023-01-02", periods=80)
        train_frame = pd.DataFrame(
            {
                "symbol": ["SPY"] * len(train_dates),
                "entry_date": train_dates.strftime("%Y-%m-%d"),
                "underlying_price": [100 + index for index in range(len(train_dates))],
                "price_momentum_5d": [0.01] * len(train_dates),
                "realized_vol_20d": [0.4] * len(train_dates),
                "realized_vol_60d": [0.3] * len(train_dates),
                "price_momentum_20d": [0.05] * len(train_dates),
                "forward_return_5d": [0.02] * len(train_dates),
                "distance_from_20d_mean": [0.1] * 56 + [1.0] * 24,
            }
        )
        quarter_dates = ["2025-01-15", "2025-01-16", "2025-04-15", "2025-04-16", "2025-07-15", "2025-07-16", "2025-10-15", "2025-10-16"]
        test_frame = pd.DataFrame(
            {
                "symbol": ["SPY"] * len(quarter_dates),
                "entry_date": quarter_dates,
                "underlying_price": [100 + index for index in range(len(quarter_dates))],
                "price_momentum_5d": [0.01] * len(quarter_dates),
                "realized_vol_20d": [0.4] * len(quarter_dates),
                "realized_vol_60d": [0.3] * len(quarter_dates),
                "price_momentum_20d": [0.05] * len(quarter_dates),
                "distance_from_20d_mean": [1.0, 0.1, 1.0, 0.1, 1.0, 0.1, 1.0, 0.1],
                "forward_return_5d": [-0.01, 0.02, -0.01, 0.02, -0.01, 0.02, 0.03, -0.01],
            }
        )
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=train_frame),
                self._year_frame("2024", frame=train_frame.assign(entry_date=pd.bdate_range("2024-01-02", periods=len(train_frame)).strftime("%Y-%m-%d"))),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        stability = report["trend_maturity_diagnostics"]["time_stability"]

        self.assertEqual(stability["quarters_consistent"], 3)
        self.assertEqual(stability["total_quarters"], 4)
        self.assertEqual(stability["consistency_ratio"], 0.75)
        self.assertTrue(stability["trend_maturity_stable"])
        self.assertEqual(len(stability["per_quarter"]), 4)

    def test_late_trend_filter_impact_measures_baseline_vs_early_trend_only(self):
        def filter_impact_frame(year: str) -> pd.DataFrame:
            dates = pd.bdate_range(f"{year}-01-02", periods=10)
            return pd.DataFrame(
                {
                    "symbol": ["SPY"] * len(dates),
                    "entry_date": dates.strftime("%Y-%m-%d"),
                    "underlying_price": [100 + index for index in range(len(dates))],
                    "price_momentum_5d": [0.01] * len(dates),
                    "realized_vol_20d": [0.4] * len(dates),
                    "realized_vol_60d": [0.3] * len(dates),
                    "price_momentum_20d": [0.05] * len(dates),
                    "distance_from_20d_mean": [0.1] * 7 + [1.0] * 3,
                    "forward_return_5d": [0.03] * 7 + [-0.02] * 3,
                }
            )

        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=filter_impact_frame("2023")),
                self._year_frame("2024", frame=filter_impact_frame("2024")),
                self._year_frame("2025", frame=filter_impact_frame("2025")),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        impact = report["late_trend_filter_impact"]

        self.assertEqual(impact["definitions"]["late_trend"], "top 30% of train-bucket maturity values, same as trend_maturity_diagnostics")
        self.assertFalse(impact["definitions"]["threshold_optimization_performed"])
        self.assertEqual(impact["test"]["table"]["baseline_no_filter"]["sample_size"], 10)
        self.assertEqual(impact["test"]["table"]["early_trend_only"]["sample_size"], 7)
        self.assertEqual(impact["test"]["delta"]["rows_removed"], 3)
        self.assertGreater(impact["test"]["delta"]["delta_win_rate"], 0)
        self.assertGreater(impact["test"]["delta"]["delta_mean_return"], 0)
        self.assertTrue(impact["filter_improves_performance"])

    def test_overextension_penalty_comparison_reports_soft_vs_hard(self):
        def penalty_frame(year: str) -> pd.DataFrame:
            dates = pd.bdate_range(f"{year}-01-02", periods=10)
            return pd.DataFrame(
                {
                    "symbol": ["SPY"] * len(dates),
                    "entry_date": dates.strftime("%Y-%m-%d"),
                    "underlying_price": [100 + index for index in range(len(dates))],
                    "price_momentum_5d": [0.01] * len(dates),
                    "realized_vol_20d": [0.4] * len(dates),
                    "realized_vol_60d": [0.3] * len(dates),
                    "price_momentum_20d": [0.05] * len(dates),
                    "distance_from_20d_mean": [0.1] * 7 + [1.0] * 3,
                    "forward_return_5d": [0.03] * 7 + [-0.02] * 3,
                }
            )

        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=penalty_frame("2023")),
                self._year_frame("2024", frame=penalty_frame("2024")),
                self._year_frame("2025", frame=penalty_frame("2025")),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        penalty = report["overextension_penalty_comparison"]
        test_table = penalty["test"]["table"]
        soft_delta = penalty["test"]["deltas_vs_baseline"]["soft_penalty_late_trend_half_weight"]

        self.assertEqual(penalty["definitions"]["late_trend_weight"], 0.5)
        self.assertEqual(test_table["baseline_no_adjustment"]["sample_size"], 10)
        self.assertEqual(test_table["hard_filter_early_trend_only"]["sample_size"], 7)
        self.assertEqual(test_table["soft_penalty_late_trend_half_weight"]["sample_size"], 10)
        self.assertEqual(test_table["soft_penalty_late_trend_half_weight"]["effective_sample_size"], 8.5)
        self.assertGreater(soft_delta["delta_win_rate"], 0)
        self.assertGreater(soft_delta["delta_mean_return"], 0)
        self.assertIn("soft_penalty_preferred", penalty)

    def test_overextension_method_comparison_ranks_fixed_definitions(self):
        def overextension_frame(year: str) -> pd.DataFrame:
            dates = pd.bdate_range(f"{year}-01-02", periods=10)
            return pd.DataFrame(
                {
                    "symbol": ["SPY"] * len(dates),
                    "entry_date": dates.strftime("%Y-%m-%d"),
                    "underlying_price": [100 + index for index in range(len(dates))],
                    "price_momentum_5d": [0.01] * len(dates),
                    "realized_vol_20d": [0.4] * len(dates),
                    "realized_vol_60d": [0.3] * len(dates),
                    "price_momentum_20d": [0.05] * len(dates),
                    "distance_from_20d_mean": [0.1] * 7 + [1.0] * 3,
                    "rsi_14": [55.0] * 7 + [80.0] * 3,
                    "forward_return_5d": [0.03] * 7 + [-0.02] * 3,
                }
            )

        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=overextension_frame("2023")),
                self._year_frame("2024", frame=overextension_frame("2024")),
                self._year_frame("2025", frame=overextension_frame("2025")),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        comparison = report["overextension_method_comparison"]
        rows = {row["method"]: row for row in comparison["table"]}

        self.assertIn("distance_from_20d_mean", rows)
        self.assertIn("bollinger_style", rows)
        self.assertIn("rsi_14", rows)
        self.assertIn("atr_14", rows)
        self.assertIn("cumulative_return_20d", rows)
        self.assertEqual(rows["rsi_14"]["threshold_value"], 70.0)
        self.assertEqual(rows["atr_14"]["status"], "missing")
        self.assertGreater(rows["distance_from_20d_mean"]["test_delta_win"], 0)
        self.assertGreater(rows["distance_from_20d_mean"]["test_delta_return"], 0)
        self.assertTrue(comparison["ranking"]["current_method_optimal"])
        self.assertLessEqual(comparison["ranking"]["current_method_rank"], 2)

    def test_trend_maturity_independence_controls_for_momentum_bucket(self):
        def independence_frame(year: str) -> pd.DataFrame:
            dates = pd.bdate_range(f"{year}-01-02", periods=8)
            return pd.DataFrame(
                {
                    "symbol": ["SPY"] * len(dates),
                    "entry_date": dates.strftime("%Y-%m-%d"),
                    "underlying_price": [100 + index for index in range(len(dates))],
                    "price_momentum_5d": [0.01] * len(dates),
                    "realized_vol_20d": [0.4] * len(dates),
                    "realized_vol_60d": [0.3] * len(dates),
                    "price_momentum_20d": [0.02, 0.02, 0.02, 0.02, 0.08, 0.08, 0.08, 0.08],
                    "distance_from_20d_mean": [0.1, 0.1, 1.0, 1.0, 0.1, 0.1, 1.0, 1.0],
                    "forward_return_5d": [0.03, 0.02, -0.01, -0.02, 0.04, 0.03, -0.01, -0.03],
                }
            )

        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=independence_frame("2023")),
                self._year_frame("2024", frame=independence_frame("2024")),
                self._year_frame("2025", frame=independence_frame("2025")),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        independence = report["trend_maturity_independence_diagnostics"]

        self.assertEqual(independence["definitions"]["momentum_split"], "median split from train high_vol_trend_positive bucket only")
        self.assertEqual(independence["definitions"]["trend_maturity_split"], "median split from train high_vol_trend_positive bucket only")
        self.assertTrue(independence["train"]["trend_maturity_independent"])
        self.assertTrue(independence["test"]["trend_maturity_independent"])
        self.assertTrue(independence["trend_maturity_independent"])
        self.assertEqual(independence["test"]["table"]["low_momentum_early_trend"]["sample_size"], 2)
        self.assertEqual(independence["test"]["table"]["high_momentum_late_trend"]["sample_size"], 2)
        self.assertGreater(
            independence["test"]["comparisons"]["low_momentum_early_vs_late"]["early_minus_late_mean_return"],
            0,
        )
        self.assertGreater(
            independence["test"]["comparisons"]["high_momentum_early_vs_late"]["early_minus_late_mean_return"],
            0,
        )

    def test_validation_flags_degradation_when_test_relationship_flips(self):
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023"),
                self._year_frame("2024"),
                self._year_frame("2025", frame=self._frame(year="2025", high_positive=False)),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report

        self.assertFalse(report["validated"])
        self.assertTrue(report["degradation_warning"])
        self.assertEqual(report["status"], "warn")
        self.assertFalse(
            report["train_vs_test"]["win_rate_stability_vs_train"]["realized_vol_60d_high"]["win_rate_direction_preserved_vs_all"]
        )
        self.assertTrue(any("high-vol regime did not preserve" in warning for warning in report["warnings"]))

    def test_missing_realized_vol_fails_cleanly(self):
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023"),
                self._year_frame("2024"),
                self._year_frame("2025", frame=self._frame(year="2025").drop(columns=["realized_vol_60d"])),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report

        self.assertEqual(report["status"], "fail")
        self.assertFalse(report["validated"])
        self.assertTrue(report["degradation_warning"])
        self.assertIn("realized_vol_60d", report["missing_columns"])

    def test_missing_trend_conditioning_feature_fails_cleanly(self):
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023"),
                self._year_frame("2024"),
                self._year_frame("2025", frame=self._frame(year="2025").drop(columns=["price_momentum_20d"])),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report

        self.assertEqual(report["status"], "fail")
        self.assertIn("price_momentum_20d", report["missing_columns"])

    def _fragility_frame(self, year: str, distances: list[float], returns: list[float], dates: list[str] | None = None) -> pd.DataFrame:
        n = len(distances)
        if dates is None:
            dates = pd.bdate_range(f"{year}-01-02", periods=n).strftime("%Y-%m-%d").tolist()
        return pd.DataFrame(
            {
                "symbol": ["SPY"] * n,
                "entry_date": dates,
                "underlying_price": [100.0 + i for i in range(n)],
                "price_momentum_5d": [0.01] * n,
                "realized_vol_20d": [0.4] * n,
                "realized_vol_60d": [0.3] * n,
                "price_momentum_20d": [0.05] * n,
                "distance_from_20d_mean": distances,
                "forward_return_5d": returns,
            }
        )

    def test_fragility_percent_removed_calculation(self):
        distances = [0.1] * 7 + [1.0] * 3
        returns = [0.02] * 7 + [-0.01] * 3
        train_frame = self._fragility_frame("2023", distances, returns)
        test_frame = self._fragility_frame("2025", distances, returns)
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=train_frame),
                self._year_frame("2024", frame=train_frame.assign(entry_date=pd.bdate_range("2024-01-02", periods=len(train_frame)).strftime("%Y-%m-%d"))),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        fragility = report["overextension_fragility_diagnostics"]
        test_hard = fragility["test"]["hard_filter_early_trend_only"]

        self.assertEqual(test_hard["total_rows"], 10)
        self.assertEqual(test_hard["rows_kept"], 7)
        self.assertEqual(test_hard["rows_removed"], 3)
        self.assertAlmostEqual(test_hard["percent_removed"], 30.0, places=1)

    def test_fragility_low_sample_quarter_flag(self):
        train_distances = [0.1] * 7 + [1.0] * 3
        train_returns = [0.02] * 7 + [-0.01] * 3
        train_frame = self._fragility_frame("2023", train_distances, train_returns)
        test_dates = ["2025-01-15", "2025-04-15", "2025-07-15", "2025-10-15"]
        test_frame = self._fragility_frame(
            "2025",
            distances=[0.1, 0.1, 0.1, 1.0],
            returns=[0.02, 0.02, 0.02, -0.01],
            dates=test_dates,
        )
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=train_frame),
                self._year_frame("2024", frame=train_frame.assign(entry_date=pd.bdate_range("2024-01-02", periods=len(train_frame)).strftime("%Y-%m-%d"))),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        flags = report["overextension_fragility_diagnostics"]["flags"]

        self.assertFalse(flags["sample_size_safe"])

    def test_fragility_overfiltering_risk_above_50_percent(self):
        train_distances = [0.1] * 7 + [1.0] * 3
        train_returns = [0.02] * 7 + [-0.01] * 3
        train_frame = self._fragility_frame("2023", train_distances, train_returns)
        test_distances = [1.0] * 7 + [0.1] * 3
        test_returns = [-0.01] * 7 + [0.02] * 3
        test_frame = self._fragility_frame("2025", test_distances, test_returns)
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=train_frame),
                self._year_frame("2024", frame=train_frame.assign(entry_date=pd.bdate_range("2024-01-02", periods=len(train_frame)).strftime("%Y-%m-%d"))),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        flags = report["overextension_fragility_diagnostics"]["flags"]

        self.assertTrue(flags["overfiltering_risk"])
        test_hard = report["overextension_fragility_diagnostics"]["test"]["hard_filter_early_trend_only"]
        self.assertGreater(test_hard["percent_removed"], 50.0)

    def test_fragility_sample_size_safe_when_all_quarters_have_enough_rows(self):
        train_distances = [0.1] * 7 + [1.0] * 3
        train_returns = [0.02] * 7 + [-0.01] * 3
        train_frame = self._fragility_frame("2023", train_distances, train_returns)
        q_dates = (
            pd.bdate_range("2025-01-02", periods=12).strftime("%Y-%m-%d").tolist()
            + pd.bdate_range("2025-04-01", periods=12).strftime("%Y-%m-%d").tolist()
            + pd.bdate_range("2025-07-01", periods=12).strftime("%Y-%m-%d").tolist()
            + pd.bdate_range("2025-10-01", periods=12).strftime("%Y-%m-%d").tolist()
        )
        test_distances = ([0.1] * 10 + [1.0] * 2) * 4
        test_returns = ([0.02] * 10 + [-0.01] * 2) * 4
        test_frame = self._fragility_frame("2025", test_distances, test_returns, dates=q_dates)
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=train_frame),
                self._year_frame("2024", frame=train_frame.assign(entry_date=pd.bdate_range("2024-01-02", periods=len(train_frame)).strftime("%Y-%m-%d"))),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        flags = report["overextension_fragility_diagnostics"]["flags"]
        per_quarter = report["overextension_fragility_diagnostics"]["per_quarter_test"]

        self.assertTrue(flags["sample_size_safe"])
        self.assertTrue(all(q["hard_filter_rows"] >= 10 for q in per_quarter))

    def _ltr_frame(self, year: str, early_return: float, late_return: float, other_return: float) -> pd.DataFrame:
        """Frame with 5 early-trend + 5 late-trend high-vol rows and 10 low-vol other rows."""
        dates = pd.bdate_range(f"{year}-01-02", periods=20).strftime("%Y-%m-%d").tolist()
        return pd.DataFrame(
            {
                "symbol": ["SPY"] * 20,
                "entry_date": dates,
                "underlying_price": [100.0 + i for i in range(20)],
                "price_momentum_5d": [0.01] * 20,
                "realized_vol_20d": [0.4] * 10 + [0.1] * 10,
                "realized_vol_60d": [0.3] * 10 + [0.1] * 10,
                "price_momentum_20d": [0.05] * 10 + [-0.05] * 10,
                "distance_from_20d_mean": [0.1] * 5 + [1.0] * 5 + [0.0] * 10,
                "forward_return_5d": [early_return] * 5 + [late_return] * 5 + [other_return] * 10,
            }
        )

    def test_late_trend_removal_fixes_failing_baseline_signal(self):
        train_frame = self._ltr_frame("2023", early_return=0.05, late_return=-0.03, other_return=0.02)
        test_frame = self._ltr_frame("2025", early_return=0.06, late_return=-0.08, other_return=0.05)
        train_2024 = train_frame.assign(entry_date=pd.bdate_range("2024-01-02", periods=20).strftime("%Y-%m-%d"))
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=train_frame),
                self._year_frame("2024", frame=train_2024),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        ltr = report["late_trend_removal_validation"]

        self.assertFalse(ltr["baseline_validation"]["validated"])
        self.assertTrue(ltr["filtered_validation"]["validated"])
        self.assertTrue(ltr["late_trend_removal_fixes_signal"])
        self.assertTrue(ltr["filtered_validation"]["validation_checks"]["win_rate_positive_in_test"])
        self.assertTrue(ltr["filtered_validation"]["validation_checks"]["mean_return_positive_in_test"])

    def test_late_trend_removal_improvement_summary(self):
        train_frame = self._ltr_frame("2023", early_return=0.05, late_return=-0.03, other_return=0.02)
        test_frame = self._ltr_frame("2025", early_return=0.06, late_return=-0.08, other_return=0.05)
        train_2024 = train_frame.assign(entry_date=pd.bdate_range("2024-01-02", periods=20).strftime("%Y-%m-%d"))
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=train_frame),
                self._year_frame("2024", frame=train_2024),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        impr = report["late_trend_removal_validation"]["improvement_summary"]
        fv_test = report["late_trend_removal_validation"]["filtered_validation"]["test"]
        bv_test = report["late_trend_removal_validation"]["baseline_validation"]["test"]

        self.assertEqual(impr["baseline_rows_test"], 10)
        self.assertEqual(impr["filtered_rows_test"], 5)
        self.assertEqual(impr["rows_removed_test"], 5)
        self.assertGreater(impr["win_rate_change_test"], 0)
        self.assertGreater(impr["mean_return_change_test"], 0)
        self.assertGreater(fv_test["win_rate_5d"], bv_test["win_rate_5d"])
        self.assertGreater(fv_test["mean_return_5d"], bv_test["mean_return_5d"])

    def test_late_trend_removal_no_training_or_tuning_flags(self):
        train_frame = self._ltr_frame("2023", early_return=0.05, late_return=-0.03, other_return=0.02)
        test_frame = self._ltr_frame("2025", early_return=0.06, late_return=-0.08, other_return=0.05)
        train_2024 = train_frame.assign(entry_date=pd.bdate_range("2024-01-02", periods=20).strftime("%Y-%m-%d"))
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=train_frame),
                self._year_frame("2024", frame=train_2024),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        ltr_defs = report["late_trend_removal_validation"]["definitions"]

        self.assertFalse(report["training_performed"])
        self.assertFalse(report["threshold_optimization_performed"])
        self.assertFalse(report["filter_changes_performed"])
        self.assertFalse(ltr_defs["training_performed"])
        self.assertFalse(ltr_defs["threshold_optimization_performed"])
        self.assertFalse(ltr_defs["filter_changes_performed"])
        self.assertFalse(report["performance_claim"])

    def test_late_trend_removal_signal_not_fixed_when_filtered_fails(self):
        train_frame = self._ltr_frame("2023", early_return=-0.02, late_return=-0.05, other_return=0.04)
        test_frame = self._ltr_frame("2025", early_return=-0.03, late_return=-0.06, other_return=0.05)
        train_2024 = train_frame.assign(entry_date=pd.bdate_range("2024-01-02", periods=20).strftime("%Y-%m-%d"))
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=train_frame),
                self._year_frame("2024", frame=train_2024),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        ltr = report["late_trend_removal_validation"]

        self.assertFalse(ltr["filtered_validation"]["validated"])
        self.assertFalse(ltr["late_trend_removal_fixes_signal"])

    def test_fragility_no_ml_training_or_tuning_flags(self):
        distances = [0.1] * 7 + [1.0] * 3
        returns = [0.02] * 7 + [-0.01] * 3
        train_frame = self._fragility_frame("2023", distances, returns)
        test_frame = self._fragility_frame("2025", distances, returns)
        report = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023", frame=train_frame),
                self._year_frame("2024", frame=train_frame.assign(entry_date=pd.bdate_range("2024-01-02", periods=len(train_frame)).strftime("%Y-%m-%d"))),
                self._year_frame("2025", frame=test_frame),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        ).report
        fragility_defs = report["overextension_fragility_diagnostics"]["definitions"]

        self.assertFalse(report["training_performed"])
        self.assertFalse(report["threshold_optimization_performed"])
        self.assertFalse(report["filter_changes_performed"])
        self.assertFalse(fragility_defs["training_performed"])
        self.assertFalse(fragility_defs["threshold_optimization_performed"])
        self.assertFalse(fragility_defs["filter_changes_performed"])

    def test_no_edge_claim_language_and_write_report(self):
        result = build_regime_validation_report(
            symbol="SPY",
            year_frames=[
                self._year_frame("2023"),
                self._year_frame("2024"),
                self._year_frame("2025"),
            ],
            train_years=["2023", "2024"],
            test_year="2025",
        )

        self.assertFalse(result.report["performance_claim"])
        self.assertFalse(result.report["training_performed"])
        self.assertFalse(result.report["threshold_optimization_performed"])
        self.assertEqual(result.report["explicit_warning"], "no edge claim")
        self.assertTrue(any("no model training" in warning for warning in result.report["warnings"]))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with patch.object(Path, "cwd", return_value=root):
                path = write_ml_regime_validation_report(result.report, reports_dir=root / "reports")
            self.assertTrue(path.exists())
            self.assertEqual(path.suffix, ".json")


if __name__ == "__main__":
    unittest.main()
