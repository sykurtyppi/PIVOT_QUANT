import unittest

import pandas as pd

from services.external_data.ml_candidate_signal import derive_candidate_signal_thresholds
from services.external_data.ml_candidate_signal_paper_eval import (
    PaperEvalConfig,
    build_paper_eval_report,
    generate_paper_entries,
)


class TestMLCandidateSignalPaperEval(unittest.TestCase):
    def _base_frame(
        self,
        year: str,
        early_return: float,
        late_return: float,
        other_return: float,
    ) -> pd.DataFrame:
        """5 early-trend + 5 late-trend high-vol rows + 10 low-vol other rows.

        Train threshold: quantile(0.70) of [0.1]*5+[1.0]*5 = 1.0
        Signal rows: distance < 1.0 AND vol >= 0.2 AND trend > 0 → rows 0-4
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

    def _multi_quarter_frame(self, year: str = "2025") -> pd.DataFrame:
        """One row per quarter; Q1 and Q3 are early-trend, Q2 and Q4 are late-trend.

        All rows are in the high_vol_trend_positive bucket.
        Given train threshold = 1.0:
          Q1 (distance=0.1 < 1.0): signal row  → entry generated
          Q2 (distance=1.0, NOT < 1.0): excluded → no entry
          Q3 (distance=0.1 < 1.0): signal row  → entry generated
          Q4 (distance=1.0, NOT < 1.0): excluded → no entry
        """
        return pd.DataFrame(
            {
                "symbol": ["SPY"] * 4,
                "entry_date": [f"{year}-01-15", f"{year}-04-15", f"{year}-07-15", f"{year}-10-15"],
                "underlying_price": [100.0, 101.0, 102.0, 103.0],
                "price_momentum_5d": [0.01] * 4,
                "realized_vol_20d": [0.4] * 4,
                "realized_vol_60d": [0.3] * 4,
                "price_momentum_20d": [0.05] * 4,
                "distance_from_20d_mean": [0.1, 1.0, 0.1, 1.0],
                "forward_return_5d": [0.05, -0.03, 0.04, -0.02],
            }
        )

    def _mixed_condition_frame(self, year: str = "2025") -> pd.DataFrame:
        """4 rows with different condition failures.

        Row 0: high_vol=True, pos_trend=True, early=True  → SIGNAL (should appear)
        Row 1: high_vol=False, pos_trend=True, early=True → not signal (low vol)
        Row 2: high_vol=True, pos_trend=False, early=True → not signal (neg trend)
        Row 3: high_vol=True, pos_trend=True, early=False → not signal (late trend)
        Train vol_split = 0.2, maturity_threshold = 1.0 (from _base_frame train)
        """
        dates = pd.bdate_range(f"{year}-01-02", periods=4).strftime("%Y-%m-%d").tolist()
        return pd.DataFrame(
            {
                "symbol": ["SPY"] * 4,
                "entry_date": dates,
                "underlying_price": [100.0, 101.0, 102.0, 103.0],
                "price_momentum_5d": [0.01] * 4,
                "realized_vol_20d": [0.4, 0.1, 0.4, 0.4],
                "realized_vol_60d": [0.3, 0.1, 0.3, 0.3],   # row 1: low vol
                "price_momentum_20d": [0.05, 0.05, -0.05, 0.05],  # row 2: neg trend
                "distance_from_20d_mean": [0.1, 0.1, 0.1, 1.0],   # row 3: late trend
                "forward_return_5d": [0.05, 0.04, 0.03, 0.02],
            }
        )

    # ------------------------------------------------------------------ #
    # Test 1: entries only generated for candidate signal rows
    # ------------------------------------------------------------------ #
    def test_entries_only_generated_for_candidate_signal_rows(self):
        train = self._base_frame("2023", early_return=0.05, late_return=-0.03, other_return=0.02)
        thresholds = derive_candidate_signal_thresholds(train)
        vol_split = thresholds["vol_split_value"]
        maturity_threshold = thresholds["maturity_threshold"]

        test = self._mixed_condition_frame()
        entries = generate_paper_entries(
            test, vol_split_value=vol_split, maturity_threshold=maturity_threshold
        )

        # Only row 0 satisfies all three conditions
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["forward_return_5d"], 0.05)
        self.assertEqual(entry["horizon"], "5d")

        # Signal metadata must contain the threshold values used
        meta = entry["signal_metadata"]
        self.assertEqual(meta["vol_split_value"], vol_split)
        self.assertEqual(meta["maturity_threshold"], maturity_threshold)

        # Confirm excluded rows are absent by checking returned dates
        returned_dates = {e["entry_date"] for e in entries}
        all_dates = set(test["entry_date"])
        excluded_dates = all_dates - returned_dates
        self.assertEqual(len(excluded_dates), 3)

    # ------------------------------------------------------------------ #
    # Test 2: thresholds derived from train only
    # ------------------------------------------------------------------ #
    def test_thresholds_derived_from_train_only(self):
        train = self._base_frame("2023", early_return=0.05, late_return=-0.03, other_return=0.02)

        # Two very different test frames
        test_a = self._base_frame("2025", early_return=0.06, late_return=-0.08, other_return=0.05)
        test_b = self._base_frame("2025", early_return=-0.10, late_return=0.15, other_return=-0.05)

        report_a = build_paper_eval_report(train_frame=train, test_frame=test_a)
        report_b = build_paper_eval_report(train_frame=train, test_frame=test_b)

        # Thresholds must be identical because they come from the same train frame
        self.assertEqual(
            report_a["thresholds"]["vol_split_value"],
            report_b["thresholds"]["vol_split_value"],
        )
        self.assertEqual(
            report_a["thresholds"]["maturity_threshold"],
            report_b["thresholds"]["maturity_threshold"],
        )
        self.assertEqual(report_a["thresholds"]["threshold_source"], "train period only")

        # Train summaries must also be identical (same train)
        self.assertEqual(
            report_a["train"]["summary"]["total_paper_entries"],
            report_b["train"]["summary"]["total_paper_entries"],
        )

        # Test summaries differ because test frames differ in returns
        entries_a = report_a["test"]["summary"]["total_paper_entries"]
        entries_b = report_b["test"]["summary"]["total_paper_entries"]
        self.assertEqual(entries_a, entries_b)  # same structure → same count
        self.assertNotEqual(
            report_a["test"]["summary"]["mean_return"],
            report_b["test"]["summary"]["mean_return"],
        )

    # ------------------------------------------------------------------ #
    # Test 3: no live trading flags
    # ------------------------------------------------------------------ #
    def test_no_live_trading_flags(self):
        train = self._base_frame("2023", early_return=0.05, late_return=-0.03, other_return=0.02)
        test = self._base_frame("2025", early_return=0.06, late_return=-0.08, other_return=0.05)
        report = build_paper_eval_report(train_frame=train, test_frame=test)

        flags = report["flags"]
        self.assertFalse(flags["live_trading_enabled"])
        self.assertFalse(flags["execution_assumptions_included"])
        self.assertEqual(flags["slippage_mode"], "none")
        self.assertEqual(flags["commission_mode"], "none")
        self.assertFalse(flags["edge_claim"])

        defs = report["definitions"]
        self.assertFalse(defs["training_performed"])
        self.assertFalse(defs["threshold_optimization_performed"])
        self.assertFalse(defs["filter_changes_performed"])
        self.assertFalse(defs["live_trading_enabled"])
        self.assertFalse(defs["governance_promotion_performed"])

        # Dataclass default guard also holds
        config = PaperEvalConfig()
        self.assertFalse(config.live_trading_enabled)
        self.assertFalse(config.edge_claim)
        self.assertEqual(config.slippage_mode, "none")
        self.assertEqual(config.commission_mode, "none")

        self.assertIn("no edge claim", report["disclaimer"])

    # ------------------------------------------------------------------ #
    # Test 4: quarterly breakdown includes zero-entry quarters
    # ------------------------------------------------------------------ #
    def test_quarterly_breakdown_includes_zero_entry_quarters(self):
        train = self._base_frame("2023", early_return=0.05, late_return=-0.03, other_return=0.02)
        test = self._multi_quarter_frame("2025")
        report = build_paper_eval_report(train_frame=train, test_frame=test)

        qb = report["test"]["summary"]["quarterly_breakdown"]
        quarters = {row["quarter"]: row for row in qb}

        # All four calendar quarters must be present
        self.assertIn("2025Q1", quarters)
        self.assertIn("2025Q2", quarters)
        self.assertIn("2025Q3", quarters)
        self.assertIn("2025Q4", quarters)

        # Q1 and Q3 have early-trend rows → entries > 0
        self.assertGreater(quarters["2025Q1"]["entries"], 0)
        self.assertGreater(quarters["2025Q3"]["entries"], 0)

        # Q2 and Q4 have only late-trend rows → entries = 0
        self.assertEqual(quarters["2025Q2"]["entries"], 0)
        self.assertEqual(quarters["2025Q4"]["entries"], 0)

        # Zero-entry quarters report None for win_rate and mean_return
        self.assertIsNone(quarters["2025Q2"]["win_rate"])
        self.assertIsNone(quarters["2025Q4"]["mean_return"])

        # Total entries = Q1 + Q3
        total = report["test"]["summary"]["total_paper_entries"]
        self.assertEqual(total, quarters["2025Q1"]["entries"] + quarters["2025Q3"]["entries"])
