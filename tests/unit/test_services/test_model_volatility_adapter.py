import unittest

import pandas as pd

from services.external_data.model_volatility_adapter import (
    adapt_real_input_volatility_features_for_model_schema,
)


class TestModelVolatilityAdapter(unittest.TestCase):
    def _daily(self, periods: int = 25, *, with_rv: bool = True) -> pd.DataFrame:
        dates = pd.bdate_range("2024-01-02", periods=periods)
        frame = pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "close": [100.0 + i for i in range(periods)],
                "volume": [1_000 + i for i in range(periods)],
            }
        )
        if with_rv:
            frame["realized_vol_30d"] = 0.20
        return frame

    def _options(self, periods: int = 25, *, include_long_tenor: bool = True) -> pd.DataFrame:
        rows = []
        dates = pd.bdate_range("2024-01-02", periods=periods)
        for index, day in enumerate(dates):
            rows.append(
                {
                    "date": day.strftime("%Y-%m-%d"),
                    "days_to_expiration": 30,
                    "moneyness": 0.0,
                    "implied_volatility": 0.20 + (index * 0.001),
                }
            )
            if include_long_tenor:
                rows.append(
                    {
                        "date": day.strftime("%Y-%m-%d"),
                        "days_to_expiration": 60,
                        "moneyness": 0.0,
                        "implied_volatility": 0.25 + (index * 0.001),
                    }
                )
        return pd.DataFrame(rows)

    def _vix(self, periods: int = 25) -> pd.DataFrame:
        dates = pd.bdate_range("2024-01-02", periods=periods)
        return pd.DataFrame(
            {
                "date": dates.strftime("%Y-%m-%d"),
                "close": [14.0 + (i * 0.1) for i in range(periods)],
            }
        )

    def test_iv_percentile_and_rank_use_trailing_rows_only(self):
        daily = self._daily(25)
        options = self._options(25)
        base = adapt_real_input_volatility_features_for_model_schema(
            daily,
            option_context_features=options,
            vix_daily_features=self._vix(25),
        ).rows

        future_shocked = options.copy()
        future_shocked.loc[future_shocked["date"] > "2024-01-29", "implied_volatility"] = 9.99
        shocked = adapt_real_input_volatility_features_for_model_schema(
            daily,
            option_context_features=future_shocked,
            vix_daily_features=self._vix(25),
        ).rows

        self.assertTrue(pd.isna(base.loc[18, "iv_rank"]))
        self.assertAlmostEqual(base.loc[19, "iv_percentile"], 100.0)
        self.assertAlmostEqual(base.loc[19, "iv_rank"], 1.0)
        self.assertAlmostEqual(base.loc[19, "iv_rank"], shocked.loc[19, "iv_rank"])

    def test_iv_rv_ratio_requires_iv_and_realized_vol(self):
        result = adapt_real_input_volatility_features_for_model_schema(
            self._daily(25, with_rv=False),
            option_context_features=self._options(25),
            vix_daily_features=self._vix(25),
        )

        self.assertIn("iv30_rv30_ratio", result.rows.columns)
        self.assertEqual(int(result.rows["iv30_rv30_ratio"].notna().sum()), 0)
        self.assertIn("iv30_rv30_ratio", result.report["unavailable_due_to_insufficient_history"])

    def test_term_slope_requires_two_valid_tenors(self):
        result = adapt_real_input_volatility_features_for_model_schema(
            self._daily(25),
            option_context_features=self._options(25, include_long_tenor=False),
            vix_daily_features=self._vix(25),
        )

        self.assertIn("vol_term_structure_slope", result.rows.columns)
        self.assertEqual(int(result.rows["vol_term_structure_slope"].notna().sum()), 0)

    def test_vix_missing_stays_missing(self):
        result = adapt_real_input_volatility_features_for_model_schema(
            self._daily(25),
            option_context_features=self._options(25),
            vix_daily_features=pd.DataFrame(),
        )

        self.assertNotIn("vix_level", result.rows.columns)
        self.assertIn("vix_level", result.report["not_fabricated_features"])
        self.assertEqual(result.report["unavailable_due_to_missing_source"]["vix_level"], 25)

    def test_label_columns_are_rejected(self):
        daily = self._daily(25)
        daily["forward_return_1d"] = 0.01

        with self.assertRaises(ValueError):
            adapt_real_input_volatility_features_for_model_schema(
                daily,
                option_context_features=self._options(25),
                vix_daily_features=self._vix(25),
            )


if __name__ == "__main__":
    unittest.main()
