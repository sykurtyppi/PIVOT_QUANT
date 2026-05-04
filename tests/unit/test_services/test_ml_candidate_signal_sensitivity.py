import unittest

import pandas as pd

from services.external_data.ml_candidate_signal_sensitivity import (
    REFERENCE_QUANTILE,
    SENSITIVITY_QUANTILES,
    SensitivityConfig,
    build_sensitivity_report,
)


class TestMLCandidateSignalSensitivity(unittest.TestCase):
    def _uniform_distance_train_frame(self) -> pd.DataFrame:
        """10 rows in high_vol_trend_positive bucket with distances 0.0–0.9.

        vol_split_value = median([0.3]*10 + [0.1]*10) = 0.2
        Bucket distances: [0.0, 0.1, 0.2, ..., 0.9] — 10 evenly spaced values.
        Thresholds increase strictly with quantile, so signal_rows is
        non-decreasing as quantile increases in the test frame.
        """
        dates = pd.bdate_range("2023-01-02", periods=20).strftime("%Y-%m-%d").tolist()
        distances = [round(i * 0.1, 1) for i in range(10)] + [0.0] * 10
        return pd.DataFrame(
            {
                "symbol": ["SPY"] * 20,
                "entry_date": dates,
                "underlying_price": [100.0 + i for i in range(20)],
                "price_momentum_5d": [0.01] * 20,
                "realized_vol_20d": [0.4] * 10 + [0.1] * 10,
                "realized_vol_60d": [0.3] * 10 + [0.1] * 10,
                "price_momentum_20d": [0.05] * 10 + [-0.05] * 10,
                "distance_from_20d_mean": distances,
                "forward_return_5d": [0.03] * 20,
            }
        )

    def _uniform_distance_test_frame(self, ret: float) -> pd.DataFrame:
        """10 high_vol_trend_positive rows with distances 0.0–0.9 and fixed return.

        As the maturity threshold rises (higher quantile), rows with higher
        distances are included → signal_rows increases monotonically.
        """
        dates = pd.bdate_range("2025-01-02", periods=10).strftime("%Y-%m-%d").tolist()
        return pd.DataFrame(
            {
                "symbol": ["SPY"] * 10,
                "entry_date": dates,
                "underlying_price": [100.0 + i for i in range(10)],
                "price_momentum_5d": [0.01] * 10,
                "realized_vol_20d": [0.4] * 10,
                "realized_vol_60d": [0.3] * 10,
                "price_momentum_20d": [0.05] * 10,
                "distance_from_20d_mean": [round(i * 0.1, 1) for i in range(10)],
                "forward_return_5d": [ret] * 10,
            }
        )

    # ------------------------------------------------------------------ #
    # Test 1: signal_rows is non-decreasing as quantile increases
    # ------------------------------------------------------------------ #
    def test_more_rows_at_higher_quantile(self):
        """A higher maturity quantile yields a higher (or equal) threshold → more signal rows."""
        train = self._uniform_distance_train_frame()
        test = self._uniform_distance_test_frame(ret=0.03)
        report = build_sensitivity_report(train_frame=train, test_frame=test)

        self.assertEqual(report["status"], "ok")
        grid = [r for r in report["sensitivity_grid"] if r["status"] == "ok"]
        self.assertGreater(len(grid), 1)

        # Signal rows must be non-decreasing across increasing quantiles
        signal_rows = [r["signal_rows"] for r in grid]
        for prev, curr in zip(signal_rows, signal_rows[1:]):
            self.assertGreaterEqual(
                curr, prev,
                msg=f"signal_rows not non-decreasing: {signal_rows}",
            )

        # Thresholds must also be non-decreasing
        thresholds = [r["maturity_threshold"] for r in grid]
        for prev, curr in zip(thresholds, thresholds[1:]):
            self.assertGreaterEqual(curr, prev)

    # ------------------------------------------------------------------ #
    # Test 2: reference quantile (0.70) is always present in the grid
    # ------------------------------------------------------------------ #
    def test_reference_quantile_present(self):
        """The reference quantile must appear in the sensitivity_grid output."""
        train = self._uniform_distance_train_frame()
        test = self._uniform_distance_test_frame(ret=0.02)
        report = build_sensitivity_report(train_frame=train, test_frame=test)

        quantiles_in_grid = [r["quantile"] for r in report["sensitivity_grid"]]
        self.assertIn(REFERENCE_QUANTILE, quantiles_in_grid)
        self.assertEqual(report["reference_quantile"], REFERENCE_QUANTILE)

        # Default config also exposes the reference quantile
        config = SensitivityConfig()
        self.assertEqual(config.reference_quantile, REFERENCE_QUANTILE)
        self.assertIn(REFERENCE_QUANTILE, config.quantiles)

    # ------------------------------------------------------------------ #
    # Test 3: all safety flags remain disabled
    # ------------------------------------------------------------------ #
    def test_no_tuning_flags(self):
        """Sensitivity analysis must not enable live trading, tuning, or edge claims."""
        train = self._uniform_distance_train_frame()
        test = self._uniform_distance_test_frame(ret=0.02)
        report = build_sensitivity_report(train_frame=train, test_frame=test)

        fl = report["flags"]
        self.assertFalse(fl["live_trading_enabled"])
        self.assertFalse(fl["threshold_optimization_performed"])
        self.assertFalse(fl["edge_claim"])

        defs = report["definitions"]
        self.assertFalse(defs["training_performed"])
        self.assertFalse(defs["threshold_optimization_performed"])
        self.assertFalse(defs["live_trading_enabled"])
        self.assertFalse(defs["governance_promotion_performed"])

        self.assertIn("no edge claim", report["disclaimer"])
        self.assertIn("no threshold selection", report["disclaimer"])

        # Dataclass defaults
        config = SensitivityConfig()
        self.assertFalse(config.live_trading_enabled)
        self.assertFalse(config.threshold_optimization_performed)
        self.assertFalse(config.edge_claim)

    # ------------------------------------------------------------------ #
    # Test 4: threshold_robust flag logic
    # ------------------------------------------------------------------ #
    def test_threshold_robust_flag(self):
        """threshold_robust = True when reference and ≥4 others have mean_return > 0."""
        train = self._uniform_distance_train_frame()

        # All returns positive → mean_return > 0 at every quantile → robust
        test_pos = self._uniform_distance_test_frame(ret=0.04)
        report_pos = build_sensitivity_report(train_frame=train, test_frame=test_pos)
        self.assertTrue(report_pos["threshold_robust"])

        # All returns negative → mean_return < 0 everywhere → not robust
        test_neg = self._uniform_distance_test_frame(ret=-0.04)
        report_neg = build_sensitivity_report(train_frame=train, test_frame=test_neg)
        self.assertFalse(report_neg["threshold_robust"])

        # threshold_robust is always a bool
        self.assertIsInstance(report_pos["threshold_robust"], bool)
        self.assertIsInstance(report_neg["threshold_robust"], bool)

    # ------------------------------------------------------------------ #
    # Bonus: full default quantile set is present in a standard run
    # ------------------------------------------------------------------ #
    def test_default_quantile_grid_is_complete(self):
        """All SENSITIVITY_QUANTILES appear in sensitivity_grid when using defaults."""
        train = self._uniform_distance_train_frame()
        test = self._uniform_distance_test_frame(ret=0.02)
        report = build_sensitivity_report(train_frame=train, test_frame=test)

        grid_quantiles = sorted(r["quantile"] for r in report["sensitivity_grid"])
        for q in SENSITIVITY_QUANTILES:
            self.assertIn(q, grid_quantiles)
