import unittest

import pandas as pd

from services.external_data.ml_candidate_signal import (
    CandidateSignalSpec,
    apply_candidate_signal,
    build_candidate_signal_report,
    derive_candidate_signal_thresholds,
)


class TestMLCandidateSignal(unittest.TestCase):
    def _frame(self, year: str, early_return: float, late_return: float, other_return: float) -> pd.DataFrame:
        """5 early-trend + 5 late-trend high-vol rows + 10 low-vol other rows.

        realized_vol_60d: [0.3]*10 + [0.1]*10 → median = 0.2 = vol_split_value
        price_momentum_20d: [0.05]*10 (high_vol) + [-0.05]*10 (low_vol)
        distance_from_20d_mean: [0.1]*5 (early) + [1.0]*5 (late) + [0.0]*10 (other)
        Train threshold: quantile(0.70) of [0.1]*5+[1.0]*5 in high_vol_trend_positive bucket
        Early rows: distance < threshold → rows 0-4 (distance=0.1)
        """
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

    def test_signal_spec_disabled_by_default(self):
        spec = CandidateSignalSpec()
        self.assertFalse(spec.live_trading_enabled)
        self.assertFalse(spec.model_training_performed)
        self.assertFalse(spec.threshold_optimization_performed)
        self.assertFalse(spec.governance_promotion_performed)
        self.assertFalse(spec.performance_claim)

    def test_formula_reproducibility(self):
        train = self._frame("2023", early_return=0.05, late_return=-0.03, other_return=0.02)
        thresholds = derive_candidate_signal_thresholds(train)
        vol_split = thresholds["vol_split_value"]
        maturity_threshold = thresholds["maturity_threshold"]

        # vol_split = median([0.3]*10 + [0.1]*10) = 0.2
        self.assertAlmostEqual(vol_split, 0.2, places=5)

        # high_vol_trend_positive bucket: rows 0-9 (vol=0.3>=0.2 AND trend=0.05>0)
        # distances in bucket: [0.1]*5 + [1.0]*5
        # quantile(0.70) of 10 values: index = 0.70*9 = 6.3 → between index 6 and 7, both 1.0
        self.assertAlmostEqual(maturity_threshold, 1.0, places=5)

        # apply_candidate_signal: early_trend = distance < 1.0
        # rows 0-4 have distance=0.1 → selected (5 rows)
        # rows 5-9 have distance=1.0 → excluded (1.0 is NOT < 1.0)
        signal = apply_candidate_signal(train, vol_split_value=vol_split, maturity_threshold=maturity_threshold)
        self.assertEqual(len(signal), 5)
        self.assertTrue(all(signal["distance_from_20d_mean"] < maturity_threshold))
        self.assertTrue(all(signal["forward_return_5d"] == 0.05))

    def test_threshold_uses_train_only(self):
        train = self._frame("2023", early_return=0.05, late_return=-0.03, other_return=0.02)

        # Two very different test frames
        test_a = self._frame("2025", early_return=0.06, late_return=-0.08, other_return=0.05)
        test_b = self._frame("2025", early_return=-0.03, late_return=0.10, other_return=-0.05)

        report_a = build_candidate_signal_report(train_frame=train, test_frame=test_a)
        report_b = build_candidate_signal_report(train_frame=train, test_frame=test_b)

        # Thresholds are derived from train only — must be identical regardless of test
        self.assertEqual(
            report_a["thresholds"]["vol_split_value"],
            report_b["thresholds"]["vol_split_value"],
        )
        self.assertEqual(
            report_a["thresholds"]["maturity_threshold"],
            report_b["thresholds"]["maturity_threshold"],
        )
        # Train stats also identical since train frame is the same
        self.assertEqual(report_a["train"]["signal_rows"], report_b["train"]["signal_rows"])
        self.assertEqual(report_a["train"]["win_rate_5d"], report_b["train"]["win_rate_5d"])

    def test_no_test_leakage(self):
        train = self._frame("2023", early_return=0.05, late_return=-0.03, other_return=0.02)
        thresholds = derive_candidate_signal_thresholds(train)
        vol_split = thresholds["vol_split_value"]
        maturity_threshold = thresholds["maturity_threshold"]

        # Two test frames with identical structure but different returns
        test_positive = self._frame("2025", early_return=0.06, late_return=-0.08, other_return=0.05)
        test_negative = self._frame("2025", early_return=-0.03, late_return=-0.06, other_return=0.05)

        signal_pos = apply_candidate_signal(test_positive, vol_split_value=vol_split, maturity_threshold=maturity_threshold)
        signal_neg = apply_candidate_signal(test_negative, vol_split_value=vol_split, maturity_threshold=maturity_threshold)

        # Same thresholds → same structural selection (same row count, same structure)
        self.assertEqual(len(signal_pos), len(signal_neg))
        self.assertTrue(all(signal_pos["distance_from_20d_mean"] < maturity_threshold))
        self.assertTrue(all(signal_neg["distance_from_20d_mean"] < maturity_threshold))

        # Returns differ → thresholds were not derived from test
        self.assertFalse(signal_pos["forward_return_5d"].equals(signal_neg["forward_return_5d"]))
