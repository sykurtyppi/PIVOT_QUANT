import unittest

import pandas as pd

from services.external_data.historical_rule_baseline import (
    RuleBaselineConfig,
    build_historical_rule_baseline_report,
    validate_selection_columns,
)


class TestHistoricalRuleBaseline(unittest.TestCase):
    def _options(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "date": "2024-01-02",
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 471.0,
                    "option_type": "call",
                    "bid": 1.1,
                    "ask": 1.2,
                    "mid": 1.15,
                    "volume": 42,
                    "open_interest": 100,
                    "implied_volatility": 0.22,
                    "underlying_close": 471.0,
                    "days_to_expiration": 17,
                    "moneyness": 0.0,
                    "spread": 0.1,
                    "relative_spread": 0.087,
                },
                {
                    "date": "2024-01-03",
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 480.0,
                    "option_type": "put",
                    "bid": 0.5,
                    "ask": 0.9,
                    "mid": 0.7,
                    "volume": 0,
                    "open_interest": 0,
                    "implied_volatility": 0.3,
                    "underlying_close": 472.0,
                    "days_to_expiration": 16,
                    "moneyness": 0.0169,
                    "spread": 0.4,
                    "relative_spread": 0.57,
                },
            ]
        )

    def _labels(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "observation_date": "2024-01-02",
                    "label_date": "2024-01-03",
                    "horizon": "1d",
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 471.0,
                    "option_type": "call",
                    "days_to_expiration": 17,
                    "underlying_close": 471.0,
                    "future_underlying_close": 472.0,
                    "forward_return": 0.002123,
                    "label_status": "mature",
                },
                {
                    "observation_date": "2024-01-03",
                    "label_date": "2024-01-04",
                    "horizon": "1d",
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 480.0,
                    "option_type": "put",
                    "days_to_expiration": 16,
                    "underlying_close": 472.0,
                    "future_underlying_close": 470.0,
                    "forward_return": -0.004237,
                    "label_status": "mature",
                },
            ]
        )

    def _windows(self) -> list[dict]:
        return [
            {
                "window_id": "wf_001",
                "train_start": "2024-01-02",
                "train_end": "2024-01-02",
                "test_start": "2024-01-03",
                "test_end": "2024-01-03",
            },
            {
                "window_id": "wf_002",
                "train_start": "2024-01-03",
                "train_end": "2024-01-03",
                "test_start": "2024-01-04",
                "test_end": "2024-01-04",
            },
        ]

    def test_rule_baseline_keeps_train_and_test_summaries_separate(self):
        report = build_historical_rule_baseline_report(
            option_context_features=self._options(),
            label_candidates=self._labels(),
            walk_forward_windows=self._windows(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-04",
            horizons=["1d"],
            config=RuleBaselineConfig(),
        ).report

        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["window_count"], 2)
        self.assertIn("train", report["windows"][0])
        self.assertIn("test", report["windows"][0])
        self.assertEqual(report["windows"][0]["train"]["selected_rows"], 1)
        self.assertEqual(report["windows"][0]["test"]["selected_rows"], 0)
        self.assertTrue(report["windows"][1]["test"]["non_evaluable"])

    def test_selection_columns_reject_label_fields(self):
        with self.assertRaises(ValueError):
            validate_selection_columns(["moneyness", "forward_return"])

    def test_missing_labels_are_counted(self):
        labels = self._labels()
        labels.loc[0, "forward_return"] = pd.NA
        report = build_historical_rule_baseline_report(
            option_context_features=self._options(),
            label_candidates=labels,
            walk_forward_windows=self._windows(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-04",
            horizons=["1d"],
            config=RuleBaselineConfig(),
        ).report

        self.assertEqual(report["windows"][0]["train"]["missing_label_count"], 1)
        self.assertEqual(report["windows"][0]["train"]["selected_rows"], 0)

    def test_report_schema_is_stable(self):
        report = build_historical_rule_baseline_report(
            option_context_features=self._options(),
            label_candidates=self._labels(),
            walk_forward_windows=self._windows(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-04",
            horizons=["1d"],
        ).report

        for key in [
            "name",
            "status",
            "config",
            "rows",
            "window_count",
            "train_selected_rows_total",
            "test_selected_rows_total",
            "leakage_checks",
            "windows",
        ]:
            self.assertIn(key, report)
