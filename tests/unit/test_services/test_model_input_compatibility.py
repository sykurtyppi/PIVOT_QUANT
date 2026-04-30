import unittest

import pandas as pd

from services.external_data.model_input_compatibility import (
    EXPECTED_FEATURE_SCHEMA,
    build_model_input_compatibility_report,
)


class TestModelInputCompatibility(unittest.TestCase):
    def _complete_features(self) -> pd.DataFrame:
        row = {
            "symbol": "SPY",
            "date": "2024-01-02",
            "underlying_price": 471.0,
            "price_momentum_5d": 0.01,
            "price_momentum_20d": 0.02,
            "volume_ratio_10d": 1.1,
            "iv_rank": 0.5,
            "iv_percentile": 50.0,
            "iv30_rv30_ratio": 1.2,
            "vol_term_structure_slope": 0.1,
            "rsi_14": 55.0,
            "bb_position": 0.6,
            "vix_level": 15.0,
            "volume": 1_000_000,
            "realized_vol_30d": 0.18,
            "realized_vol_60d": 0.2,
        }
        return pd.DataFrame([row])

    def _labels(self, horizons=None) -> pd.DataFrame:
        horizons = horizons or ["1d", "5d", "21d"]
        return pd.DataFrame(
            [
                {
                    "observation_date": "2024-01-02",
                    "label_date": "2024-01-03",
                    "horizon": horizon,
                    "forward_return": 0.01,
                }
                for horizon in horizons
            ]
        )

    def _option_context(self, dates) -> pd.DataFrame:
        rows = []
        for index, day in enumerate(dates):
            rows.append(
                {
                    "date": day.strftime("%Y-%m-%d"),
                    "days_to_expiration": 30,
                    "moneyness": 0.0,
                    "implied_volatility": 0.20 + (index * 0.001),
                    "volume": 100,
                    "open_interest": 1_000,
                    "mid": 1.5,
                    "relative_spread": 0.05,
                }
            )
            rows.append(
                {
                    "date": day.strftime("%Y-%m-%d"),
                    "days_to_expiration": 60,
                    "moneyness": 0.0,
                    "implied_volatility": 0.25 + (index * 0.001),
                    "volume": 100,
                    "open_interest": 1_000,
                    "mid": 2.0,
                    "relative_spread": 0.04,
                }
            )
        return pd.DataFrame(rows)

    def _vix(self, dates) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "close": [14.0 + (0.1 * i) for i in range(len(dates))],
            }
        )

    def test_complete_schema_passes(self):
        report = build_model_input_compatibility_report(
            model_ready_daily_features=self._complete_features(),
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            requested_horizons=["1d", "5d", "21d"],
        ).report

        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["missing_required_features"], [])
        self.assertEqual(report["label_availability"]["target_horizon_compatibility"], "pass")

    def test_missing_required_feature_fails(self):
        features = self._complete_features().drop(columns=["iv_rank"])
        report = build_model_input_compatibility_report(
            model_ready_daily_features=features,
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            requested_horizons=["1d", "5d", "21d"],
        ).report

        self.assertEqual(report["status"], "fail")
        self.assertIn("iv_rank", report["missing_required_features"])

    def test_daily_adapter_improves_compatibility_without_fabricating_unavailable_fields(self):
        dates = pd.bdate_range("2024-01-02", periods=25)
        features = pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "open": [100.0 + i for i in range(25)],
                "high": [101.0 + i for i in range(25)],
                "low": [99.0 + i for i in range(25)],
                "close": [100.0 + i for i in range(25)],
                "volume": [1_000 + (10 * i) for i in range(25)],
                "source": ["yahoo"] * 25,
            }
        )

        report = build_model_input_compatibility_report(
            model_ready_daily_features=features,
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(["1d", "5d"]),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-02-06",
            requested_horizons=["1d", "5d"],
        ).report

        self.assertEqual(report["status"], "fail")
        self.assertIn("price_momentum_5d", report["computed_feature_list"])
        self.assertIn("price_momentum_20d", report["computed_feature_list"])
        self.assertNotIn("price_momentum_5d", report["missing_required_features"])
        self.assertNotIn("volume_ratio_10d", report["missing_required_features"])
        self.assertIn("iv_rank", report["missing_required_features"])
        self.assertIn("vix_level", report["missing_required_features"])
        self.assertIn("21d", report["label_availability"]["missing_expected_horizons"])

    def test_extended_window_reduces_insufficient_lookback_nulls(self):
        short_dates = pd.bdate_range("2024-01-02", periods=25)
        long_dates = pd.bdate_range("2023-11-01", periods=70)

        def frame(dates):
            return pd.DataFrame(
                {
                    "date": dates.strftime("%Y-%m-%d"),
                    "open": [100.0 + i for i in range(len(dates))],
                    "high": [101.0 + i for i in range(len(dates))],
                    "low": [99.0 + i for i in range(len(dates))],
                    "close": [100.0 + i for i in range(len(dates))],
                    "volume": [1_000 + (10 * i) for i in range(len(dates))],
                    "source": ["yahoo"] * len(dates),
                }
            )

        short = build_model_input_compatibility_report(
            model_ready_daily_features=frame(short_dates),
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(["1d", "5d", "21d"]),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-02-06",
            requested_horizons=["1d", "5d", "21d"],
        ).report
        extended = build_model_input_compatibility_report(
            model_ready_daily_features=frame(long_dates),
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(["1d", "5d", "21d"]),
            symbol="SPY",
            start_date="2023-11-01",
            end_date="2024-02-06",
            requested_horizons=["1d", "5d", "21d"],
        ).report

        self.assertLess(
            extended["deterministic_feature_readiness"]["null_rate_by_feature"]["realized_vol_60d"],
            short["deterministic_feature_readiness"]["null_rate_by_feature"]["realized_vol_60d"],
        )
        self.assertGreater(
            extended["deterministic_feature_readiness"]["usable_row_count_after_required_daily_features"],
            short["deterministic_feature_readiness"]["usable_row_count_after_required_daily_features"],
        )

    def test_60d_realized_vol_requires_extended_history(self):
        dates = pd.bdate_range("2023-11-01", periods=70)
        features = pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "close": [100.0 + (i % 7) for i in range(70)],
                "volume": [1_000 + i for i in range(70)],
            }
        )
        report = build_model_input_compatibility_report(
            model_ready_daily_features=features,
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(["1d", "5d", "21d"]),
            symbol="SPY",
            start_date="2023-11-01",
            end_date="2024-02-06",
            requested_horizons=["1d", "5d", "21d"],
        ).report

        self.assertGreater(report["computed_non_null_counts"]["realized_vol_60d"], 0)
        self.assertLess(report["deterministic_feature_readiness"]["null_rate_by_feature"]["realized_vol_60d"], 1.0)

    def test_21d_labels_checked_separately_from_feature_availability(self):
        report = build_model_input_compatibility_report(
            model_ready_daily_features=self._complete_features(),
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(["1d", "5d"]),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            requested_horizons=["1d", "5d", "21d"],
        ).report

        self.assertEqual(report["label_availability"]["target_horizon_compatibility"], "fail")
        self.assertIn("21d", report["label_availability"]["missing_requested_horizons"])
        self.assertEqual(report["missing_required_features"], [])

    def test_true_missing_iv_macro_fields_remain_missing_in_extended_window(self):
        dates = pd.bdate_range("2023-11-01", periods=70)
        features = pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "close": [100.0 + i for i in range(70)],
                "volume": [1_000 + i for i in range(70)],
            }
        )
        report = build_model_input_compatibility_report(
            model_ready_daily_features=features,
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(["1d", "5d", "21d"]),
            symbol="SPY",
            start_date="2023-11-01",
            end_date="2024-02-06",
            requested_horizons=["1d", "5d", "21d"],
        ).report

        for field in ["iv_rank", "iv_percentile", "iv30_rv30_ratio", "vix_level", "vol_term_structure_slope"]:
            self.assertIn(field, report["missing_required_features"])
            self.assertIn(field, report["not_fabricated_features"])

    def test_real_input_adapter_fills_iv_and_vix_fields_when_sources_exist(self):
        dates = pd.bdate_range("2023-11-01", periods=70)
        features = pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "close": [100.0 + (i % 9) for i in range(70)],
                "volume": [1_000 + i for i in range(70)],
            }
        )
        report = build_model_input_compatibility_report(
            model_ready_daily_features=features,
            option_context_features=self._option_context(dates),
            vix_daily_features=self._vix(dates),
            label_candidates=self._labels(["1d", "5d", "21d"]),
            symbol="SPY",
            start_date="2023-11-01",
            end_date="2024-02-06",
            requested_horizons=["1d", "5d", "21d"],
        ).report

        for field in ["iv_rank", "iv_percentile", "iv30_rv30_ratio", "vix_level", "vol_term_structure_slope"]:
            self.assertNotIn(field, report["missing_required_features"])
            self.assertNotIn(field, report["not_fabricated_features"])
        self.assertGreater(report["real_input_feature_readiness"]["non_null_count_by_feature"]["vix_level"], 0)
        self.assertGreater(report["real_input_feature_readiness"]["source_counts"]["option_iv_dates"], 0)

    def test_extra_feature_warns_not_fails(self):
        features = self._complete_features()
        features["extra_field"] = 1.0
        report = build_model_input_compatibility_report(
            model_ready_daily_features=features,
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            requested_horizons=["1d", "5d", "21d"],
        ).report

        self.assertEqual(report["status"], "warn")
        self.assertIn("extra_field", report["extra_features"])

    def test_dtype_mismatch_warns_when_coercion_fails(self):
        features = self._complete_features()
        features["iv_rank"] = "not-a-number"
        report = build_model_input_compatibility_report(
            model_ready_daily_features=features,
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            requested_horizons=["1d", "5d", "21d"],
        ).report

        self.assertEqual(report["status"], "warn")
        self.assertIn("iv_rank", report["dtype_mismatches"])

    def test_all_null_required_feature_fails(self):
        features = self._complete_features()
        features["iv_rank"] = pd.NA
        report = build_model_input_compatibility_report(
            model_ready_daily_features=features,
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            requested_horizons=["1d", "5d", "21d"],
        ).report

        self.assertEqual(report["status"], "fail")
        self.assertIn("iv_rank", report["all_null_columns"])

    def test_label_horizon_missing_fails(self):
        report = build_model_input_compatibility_report(
            model_ready_daily_features=self._complete_features(),
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(["1d", "5d"]),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            requested_horizons=["1d", "5d"],
        ).report

        self.assertEqual(report["status"], "fail")
        self.assertIn("21d", report["label_availability"]["missing_expected_horizons"])

    def test_report_schema_is_stable(self):
        report = build_model_input_compatibility_report(
            model_ready_daily_features=self._complete_features(),
            option_context_features=pd.DataFrame(),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            requested_horizons=["1d", "5d", "21d"],
        ).report

        for key in [
            "name",
            "status",
            "schema_source",
            "row_count",
            "expected_feature_count",
            "available_feature_count",
            "missing_required_features",
            "extra_features",
            "dtype_mismatches",
            "label_availability",
            "target_horizon_compatibility",
            "computed_feature_list",
            "deterministic_computed_feature_list",
            "real_input_computed_feature_list",
            "unavailable_due_to_insufficient_lookback",
            "no_lookahead_notes",
            "deterministic_feature_readiness",
            "real_input_feature_readiness",
            "unavailable_due_to_missing_source",
        ]:
            self.assertIn(key, report)
        self.assertEqual(report["expected_feature_count"], len(EXPECTED_FEATURE_SCHEMA))
