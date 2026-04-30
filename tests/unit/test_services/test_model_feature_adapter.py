import unittest

import pandas as pd

from services.external_data.model_feature_adapter import adapt_daily_features_for_model_schema


class TestModelFeatureAdapter(unittest.TestCase):
    def _daily(self, periods: int = 25) -> pd.DataFrame:
        dates = pd.bdate_range("2024-01-02", periods=periods)
        return pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "open": [100.0 + i for i in range(periods)],
                "high": [101.0 + i for i in range(periods)],
                "low": [99.0 + i for i in range(periods)],
                "close": [100.0 + i for i in range(periods)],
                "volume": [1_000 + (10 * i) for i in range(periods)],
                "source": ["yahoo"] * periods,
            }
        )

    def test_5d_momentum_uses_only_historical_data(self):
        result = adapt_daily_features_for_model_schema(self._daily(8))
        rows = result.rows

        self.assertTrue(pd.isna(rows.loc[4, "price_momentum_5d"]))
        self.assertAlmostEqual(rows.loc[5, "price_momentum_5d"], 105.0 / 100.0 - 1)
        self.assertIn("price_momentum_5d", result.report["computed_feature_list"])

    def test_20d_momentum_insufficient_history_is_reported(self):
        result = adapt_daily_features_for_model_schema(self._daily(15))

        self.assertEqual(result.rows["price_momentum_20d"].notna().sum(), 0)
        self.assertEqual(result.report["unavailable_due_to_insufficient_lookback"]["price_momentum_20d"], 15)

    def test_volume_ratio_requires_prior_10_days(self):
        result = adapt_daily_features_for_model_schema(self._daily(12))
        rows = result.rows

        self.assertTrue(pd.isna(rows.loc[9, "volume_ratio_10d"]))
        self.assertTrue(pd.notna(rows.loc[10, "volume_ratio_10d"]))
        prior_avg = sum(1_000 + (10 * i) for i in range(10)) / 10
        self.assertAlmostEqual(rows.loc[10, "volume_ratio_10d"], 1_100 / prior_avg)

    def test_realized_vol_insufficient_history_is_reported(self):
        result = adapt_daily_features_for_model_schema(self._daily(25))

        self.assertEqual(result.rows["realized_vol_30d"].notna().sum(), 0)
        self.assertEqual(result.rows["realized_vol_60d"].notna().sum(), 0)
        self.assertEqual(result.report["unavailable_due_to_insufficient_lookback"]["realized_vol_30d"], 25)
        self.assertEqual(result.report["unavailable_due_to_insufficient_lookback"]["realized_vol_60d"], 25)

    def test_label_columns_are_rejected(self):
        daily = self._daily(8)
        daily["forward_return_1d"] = 0.01

        with self.assertRaises(ValueError):
            adapt_daily_features_for_model_schema(daily)

    def test_iv_and_macro_fields_are_not_fabricated(self):
        result = adapt_daily_features_for_model_schema(self._daily(25))

        for field in ["iv_rank", "iv_percentile", "iv30_rv30_ratio", "vix_level"]:
            self.assertNotIn(field, result.rows.columns)
            self.assertIn(field, result.report["not_fabricated_features"])
