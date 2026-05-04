import unittest

import pandas as pd

from services.external_data.ml_effective_sample import (
    date_weighted_metrics,
    effective_sample_diagnostics,
)


def _multi_contract_frame() -> pd.DataFrame:
    """3 dates × 4 contracts = 12 rows; forward_return is the same per date."""
    dates = ["2024-01-02"] * 4 + ["2024-01-03"] * 4 + ["2024-01-04"] * 4
    returns = [0.02] * 4 + [-0.01] * 4 + [0.03] * 4
    return pd.DataFrame({"observation_date": dates, "forward_return": returns})


def _single_contract_frame() -> pd.DataFrame:
    """1 row per date — no inflation."""
    return pd.DataFrame(
        {
            "observation_date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "forward_return": [0.02, -0.01, 0.03],
        }
    )


class TestEffectiveSampleDiagnostics(unittest.TestCase):

    # ------------------------------------------------------------------ #
    # Test 1: multiple contracts per date triggers warning
    # ------------------------------------------------------------------ #
    def test_multiple_contracts_per_date_triggers_warning(self):
        frame = _multi_contract_frame()
        result = effective_sample_diagnostics(frame, date_col="observation_date")

        self.assertEqual(result["row_count"], 12)
        self.assertEqual(result["unique_entry_dates"], 3)
        self.assertEqual(result["average_rows_per_date"], 4.0)
        self.assertEqual(result["max_rows_per_date"], 4)
        self.assertTrue(result["effective_sample_warning"])

    # ------------------------------------------------------------------ #
    # Test 2: one contract per date — no warning
    # ------------------------------------------------------------------ #
    def test_single_contract_per_date_no_warning(self):
        frame = _single_contract_frame()
        result = effective_sample_diagnostics(frame, date_col="observation_date")

        self.assertEqual(result["row_count"], 3)
        self.assertEqual(result["unique_entry_dates"], 3)
        self.assertEqual(result["average_rows_per_date"], 1.0)
        self.assertEqual(result["max_rows_per_date"], 1)
        self.assertFalse(result["effective_sample_warning"])

    # ------------------------------------------------------------------ #
    # Test 3: empty frame returns safe defaults
    # ------------------------------------------------------------------ #
    def test_empty_frame_returns_safe_defaults(self):
        result = effective_sample_diagnostics(pd.DataFrame(), date_col="observation_date")
        self.assertEqual(result["row_count"], 0)
        self.assertEqual(result["unique_entry_dates"], 0)
        self.assertIsNone(result["average_rows_per_date"])
        self.assertIsNone(result["max_rows_per_date"])
        self.assertFalse(result["effective_sample_warning"])

    # ------------------------------------------------------------------ #
    # Test 4: missing date column returns safe defaults
    # ------------------------------------------------------------------ #
    def test_missing_date_column_returns_safe_defaults(self):
        frame = pd.DataFrame({"forward_return": [0.01, 0.02]})
        result = effective_sample_diagnostics(frame, date_col="observation_date")
        self.assertEqual(result["row_count"], 0)
        self.assertFalse(result["effective_sample_warning"])

    # ------------------------------------------------------------------ #
    # Test 5: all required keys present
    # ------------------------------------------------------------------ #
    def test_all_required_keys_present(self):
        result = effective_sample_diagnostics(_multi_contract_frame(), date_col="observation_date")
        for key in (
            "row_count",
            "unique_entry_dates",
            "average_rows_per_date",
            "max_rows_per_date",
            "effective_sample_warning",
        ):
            self.assertIn(key, result, msg=f"missing key: {key}")


class TestDateWeightedMetrics(unittest.TestCase):

    # ------------------------------------------------------------------ #
    # Test 6: date-weighted mean differs from row-level mean
    # ------------------------------------------------------------------ #
    def test_date_weighted_mean_differs_from_row_mean(self):
        """
        Date 1: 4 contracts with return=0.02  → date mean = 0.02
        Date 2: 4 contracts with return=-0.01 → date mean = -0.01
        Date 3: 4 contracts with return=0.03  → date mean = 0.03
        Date-weighted mean = mean(0.02, -0.01, 0.03) = 0.04/3 ≈ 0.0133

        Row-level mean = (4*0.02 + 4*-0.01 + 4*0.03) / 12 = 0.16/12 ≈ 0.0133
        In this balanced case they match; use unbalanced frame to show difference.
        """
        frame = pd.DataFrame(
            {
                "observation_date": ["2024-01-02"] * 8 + ["2024-01-03"] * 2,
                "forward_return": [0.10] * 8 + [-0.10] * 2,
            }
        )
        dw = date_weighted_metrics(
            frame, date_col="observation_date", return_col="forward_return"
        )
        # Date-weighted: mean(0.10, -0.10) = 0.0
        # Row-weighted:  (8*0.10 + 2*-0.10) / 10 = 0.60/10 = 0.06
        self.assertAlmostEqual(dw["date_weighted_mean_return"], 0.0, places=10)
        self.assertNotAlmostEqual(dw["date_weighted_mean_return"], 0.06, places=3)

    # ------------------------------------------------------------------ #
    # Test 7: date-weighted metrics match row-level when one contract per date
    # ------------------------------------------------------------------ #
    def test_date_weighted_matches_row_level_when_one_contract_per_date(self):
        frame = _single_contract_frame()
        dw = date_weighted_metrics(
            frame, date_col="observation_date", return_col="forward_return"
        )
        row_mean = float(pd.to_numeric(frame["forward_return"]).mean())
        self.assertAlmostEqual(dw["date_weighted_mean_return"], row_mean, places=10)
        self.assertEqual(dw["date_weighted_count"], 3)
        self.assertTrue(dw["date_weighted_metrics_available"])

    # ------------------------------------------------------------------ #
    # Test 8: win rate uses date-level signals not option rows
    # ------------------------------------------------------------------ #
    def test_date_weighted_win_rate_uses_date_signals(self):
        """
        Date 1: 3 positive contracts (return=0.02) → date mean = 0.02 (win)
        Date 2: 5 negative contracts (return=-0.05) → date mean = -0.05 (loss)
        Date-weighted win rate = 1/2 = 0.5
        Row-level win rate = 3/8 = 0.375
        """
        frame = pd.DataFrame(
            {
                "observation_date": ["2024-01-02"] * 3 + ["2024-01-03"] * 5,
                "forward_return": [0.02] * 3 + [-0.05] * 5,
            }
        )
        dw = date_weighted_metrics(
            frame, date_col="observation_date", return_col="forward_return"
        )
        self.assertAlmostEqual(dw["date_weighted_win_rate"], 0.5, places=10)

    # ------------------------------------------------------------------ #
    # Test 9: empty frame returns safe defaults with available=False
    # ------------------------------------------------------------------ #
    def test_empty_frame_returns_unavailable(self):
        result = date_weighted_metrics(
            pd.DataFrame(), date_col="observation_date", return_col="forward_return"
        )
        self.assertFalse(result["date_weighted_metrics_available"])
        self.assertEqual(result["date_weighted_count"], 0)
        self.assertIsNone(result["date_weighted_mean_return"])
        self.assertIsNone(result["date_weighted_win_rate"])
        self.assertIsNone(result["date_weighted_median_return"])

    # ------------------------------------------------------------------ #
    # Test 10: all required keys present
    # ------------------------------------------------------------------ #
    def test_all_required_keys_present(self):
        result = date_weighted_metrics(
            _multi_contract_frame(),
            date_col="observation_date",
            return_col="forward_return",
        )
        for key in (
            "date_weighted_win_rate",
            "date_weighted_mean_return",
            "date_weighted_median_return",
            "date_weighted_count",
            "date_weighted_metrics_available",
        ):
            self.assertIn(key, result, msg=f"missing key: {key}")

    # ------------------------------------------------------------------ #
    # Test 11: missing return column returns unavailable
    # ------------------------------------------------------------------ #
    def test_missing_return_column_returns_unavailable(self):
        frame = pd.DataFrame({"observation_date": ["2024-01-02", "2024-01-03"]})
        result = date_weighted_metrics(
            frame, date_col="observation_date", return_col="forward_return"
        )
        self.assertFalse(result["date_weighted_metrics_available"])

    # ------------------------------------------------------------------ #
    # Test 12: date_weighted_count equals unique dates not row count
    # ------------------------------------------------------------------ #
    def test_date_weighted_count_equals_unique_dates(self):
        frame = _multi_contract_frame()  # 12 rows, 3 dates
        result = date_weighted_metrics(
            frame, date_col="observation_date", return_col="forward_return"
        )
        self.assertEqual(result["date_weighted_count"], 3)
        self.assertNotEqual(result["date_weighted_count"], 12)
