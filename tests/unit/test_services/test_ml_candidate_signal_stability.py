import unittest

import pandas as pd

from services.external_data.ml_candidate_signal_paper_eval import (
    PaperEvalConfig,
    _monthly_breakdown,
    _stability_flags,
    build_paper_eval_report,
)
from services.external_data.ml_candidate_signal import derive_candidate_signal_thresholds


_MIN_ENTRIES = 5  # default min_month_entries_for_warning


class TestMLCandidateSignalStability(unittest.TestCase):
    def _train_frame(self) -> pd.DataFrame:
        """Standard 20-row train frame.

        Rows 0-4:   high_vol, pos_trend, distance=0.1 (early trend)
        Rows 5-9:   high_vol, pos_trend, distance=1.0 (late trend)
        Rows 10-19: low_vol, neg_trend (outside bucket)

        vol_split_value = 0.2, maturity_threshold = 1.0
        Signal: distance < 1.0 → early-trend rows only.
        """
        dates = pd.bdate_range("2023-01-02", periods=20).strftime("%Y-%m-%d").tolist()
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
                "forward_return_5d": [0.03] * 5 + [-0.02] * 5 + [0.01] * 10,
            }
        )

    def _signal_rows(self, month: str, n: int, ret: float, distance: float = 0.1) -> list[dict]:
        """n rows in YYYY-MM, high_vol + pos_trend + specified distance."""
        dates = pd.bdate_range(f"{month}-01", periods=n).strftime("%Y-%m-%d").tolist()
        return [
            {
                "symbol": "SPY",
                "entry_date": d,
                "underlying_price": 100.0,
                "price_momentum_5d": 0.01,
                "realized_vol_20d": 0.4,
                "realized_vol_60d": 0.3,
                "price_momentum_20d": 0.05,
                "distance_from_20d_mean": distance,
                "forward_return_5d": ret,
            }
            for d in dates
        ]

    def _make_test_frame(self, row_specs: list[tuple]) -> pd.DataFrame:
        rows = []
        for spec in row_specs:
            month, n, ret = spec[0], spec[1], spec[2]
            distance = spec[3] if len(spec) > 3 else 0.1
            rows.extend(self._signal_rows(month, n, ret, distance))
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------ #
    # PR41 Test 1: 1-entry negative month does NOT trigger mature warning
    # ------------------------------------------------------------------ #
    def test_low_entry_negative_month_does_not_trigger_mature_warning(self):
        """A single-entry negative month is low_sample and must not fire negative_mature_month_warning."""
        # Jan: 1 entry, return=-0.05 (would trigger the old warning)
        # Feb + Mar: 6 entries each (mature, positive) — spread across two months so
        # neither hits >50% of total entries (6/13 ≈ 46%), preventing concentration_warning.
        test = self._make_test_frame([
            ("2025-01", 1, -0.05),
            ("2025-02", 6,  0.05),
            ("2025-03", 6,  0.05),
        ])
        report = build_paper_eval_report(train_frame=self._train_frame(), test_frame=test)
        sf = report["test"]["stability"]["flags"]

        # 1-entry Jan should NOT fire mature warning (it is below threshold)
        self.assertFalse(sf["negative_mature_month_warning"])
        # stability_flag True: no mature negative, no concentration, mature month exists (Feb)
        self.assertTrue(sf["stability_flag"])

        # Confirm Jan is marked low_sample in the breakdown
        monthly = {row["month"]: row for row in report["test"]["stability"]["monthly_breakdown"]}
        self.assertTrue(monthly["2025-01"]["low_sample"])
        self.assertFalse(monthly["2025-02"]["low_sample"])

    # ------------------------------------------------------------------ #
    # PR41 Test 2: low_sample_month_warning DOES trigger
    # ------------------------------------------------------------------ #
    def test_low_sample_month_warning_triggers_for_subthreshold_month(self):
        """low_sample_month_warning fires whenever 0 < entries < min_month_entries_for_warning."""
        # Jan: 1 entry (below threshold=5)
        # Feb: 6 entries (mature)
        test = self._make_test_frame([
            ("2025-01", 1, 0.03),   # positive return, but low sample
            ("2025-02", 6, 0.03),
        ])
        report = build_paper_eval_report(train_frame=self._train_frame(), test_frame=test)
        sf = report["test"]["stability"]["flags"]

        self.assertTrue(sf["low_sample_month_warning"])
        # But no mature negative month → mature warning still False
        self.assertFalse(sf["negative_mature_month_warning"])

        # Exactly the subthreshold months are marked low_sample
        monthly = {row["month"]: row for row in report["test"]["stability"]["monthly_breakdown"]}
        self.assertTrue(monthly["2025-01"]["low_sample"])
        self.assertFalse(monthly["2025-02"]["low_sample"])

    # ------------------------------------------------------------------ #
    # PR41 Test 3: mature negative month triggers negative_mature_month_warning
    # ------------------------------------------------------------------ #
    def test_mature_negative_month_triggers_warning(self):
        """A month with entries >= threshold, win_rate < 0.5, and mean < 0 fires the mature warning."""
        # Jan: 6 entries (mature), all negative → win_rate=0.0, mean=-0.05
        # Feb: 6 entries, all positive
        test = self._make_test_frame([
            ("2025-01", 6, -0.05),
            ("2025-02", 6,  0.05),
        ])
        report = build_paper_eval_report(train_frame=self._train_frame(), test_frame=test)
        sf = report["test"]["stability"]["flags"]

        self.assertTrue(sf["negative_mature_month_warning"])
        self.assertFalse(sf["stability_flag"])
        self.assertFalse(sf["low_sample_month_warning"])  # both months are mature

    # ------------------------------------------------------------------ #
    # PR41 Test 4: zero-entry month included but does not trigger mature warning
    # ------------------------------------------------------------------ #
    def test_zero_entry_month_included_no_mature_warning(self):
        """Zero-entry months appear in breakdown with entries=0 and must not fire mature warning."""
        # Jan: late-trend (excluded) → entries=0
        # Feb + Apr: 6 early-trend rows each (mature, positive) — two entry months
        #   so each is 6/12 = 50%, NOT > 50%, preventing concentration_warning.
        # Mar: late-trend → entries=0
        test = self._make_test_frame([
            ("2025-01", 2,  0.04, 1.0),   # late trend → excluded
            ("2025-02", 6,  0.04, 0.1),   # signal
            ("2025-03", 2,  0.04, 1.0),   # late trend → excluded
            ("2025-04", 6,  0.04, 0.1),   # signal
        ])
        report = build_paper_eval_report(train_frame=self._train_frame(), test_frame=test)
        sf = report["test"]["stability"]["flags"]
        monthly = {row["month"]: row for row in report["test"]["stability"]["monthly_breakdown"]}

        # All four months present
        self.assertIn("2025-01", monthly)
        self.assertIn("2025-02", monthly)
        self.assertIn("2025-03", monthly)
        self.assertIn("2025-04", monthly)

        # Zero-entry months
        self.assertEqual(monthly["2025-01"]["entries"], 0)
        self.assertEqual(monthly["2025-03"]["entries"], 0)

        # Zero-entry months are NOT low_sample (they are absent)
        self.assertFalse(monthly["2025-01"]["low_sample"])
        self.assertFalse(monthly["2025-03"]["low_sample"])

        # No mature negative month → mature warning False
        self.assertFalse(sf["negative_mature_month_warning"])
        # No low-sample months either
        self.assertFalse(sf["low_sample_month_warning"])
        # stability_flag True (mature month Feb exists, no warnings)
        self.assertTrue(sf["stability_flag"])

    # ------------------------------------------------------------------ #
    # PR41 Test 5: all safety flags remain disabled
    # ------------------------------------------------------------------ #
    def test_safety_flags_unchanged_after_pr41(self):
        """Stability changes must not enable live trading, tuning, or edge claims."""
        test = self._make_test_frame([
            ("2025-01", 6, 0.03),
            ("2025-02", 6, 0.03),
        ])
        report = build_paper_eval_report(train_frame=self._train_frame(), test_frame=test)

        fl = report["flags"]
        self.assertFalse(fl["live_trading_enabled"])
        self.assertFalse(fl["execution_assumptions_included"])
        self.assertEqual(fl["slippage_mode"], "none")
        self.assertEqual(fl["commission_mode"], "none")
        self.assertFalse(fl["edge_claim"])

        defs = report["definitions"]
        self.assertFalse(defs["training_performed"])
        self.assertFalse(defs["live_trading_enabled"])
        self.assertFalse(defs["governance_promotion_performed"])

        stab_defs = report["test"]["stability"]["definitions"]
        self.assertFalse(stab_defs["training_performed"])
        self.assertFalse(stab_defs["live_trading_enabled"])

        # Stability flags have the four expected keys and are typed bool
        sf = report["test"]["stability"]["flags"]
        for key in ("stability_flag", "negative_mature_month_warning",
                    "low_sample_month_warning", "concentration_warning"):
            self.assertIn(key, sf)
            self.assertIsInstance(sf[key], bool)

        # min_month_entries_for_warning is documented in the stability block
        self.assertIn("min_month_entries_for_warning", report["test"]["stability"])

        config = PaperEvalConfig()
        self.assertFalse(config.live_trading_enabled)
        self.assertFalse(config.edge_claim)
        self.assertEqual(config.min_month_entries_for_warning, 5)

        self.assertIn("no edge claim", report["disclaimer"])

    # ------------------------------------------------------------------ #
    # Concentration flag (updated from original suite)
    # ------------------------------------------------------------------ #
    def test_concentration_flag(self):
        """concentration_warning fires when one month holds >50% of entries."""
        # Jan: 7 entries, Feb: 3 entries → Jan=70% > 50%
        test = self._make_test_frame([
            ("2025-01", 7, 0.02),
            ("2025-02", 3, 0.02),
        ])
        report = build_paper_eval_report(train_frame=self._train_frame(), test_frame=test)
        sf = report["test"]["stability"]["flags"]

        self.assertTrue(sf["concentration_warning"])
        self.assertFalse(sf["negative_mature_month_warning"])
        self.assertFalse(sf["stability_flag"])

    # ------------------------------------------------------------------ #
    # _stability_flags unit: stable when mature months are all positive
    # ------------------------------------------------------------------ #
    def test_stability_flag_true_when_no_warnings(self):
        """stability_flag = True when both warnings are False and mature months exist."""
        breakdown = [
            {"month": "2025-01", "entries": 6, "low_sample": False, "win_rate": 0.67,
             "mean_return": 0.02, "median_return": 0.02, "worst_return": -0.01,
             "best_return": 0.05, "positive_return_sum": 0.07},
            {"month": "2025-02", "entries": 6, "low_sample": False, "win_rate": 0.67,
             "mean_return": 0.02, "median_return": 0.02, "worst_return": -0.01,
             "best_return": 0.05, "positive_return_sum": 0.07},
        ]
        flags = _stability_flags(breakdown)
        # 6/12 = 50%, NOT > 50% → no entry concentration
        # win_rate=0.67 > 0.5, mean > 0 → no mature negative
        # has_mature_month: 6 >= 5 → True
        self.assertFalse(flags["negative_mature_month_warning"])
        self.assertFalse(flags["low_sample_month_warning"])
        self.assertFalse(flags["concentration_warning"])
        self.assertTrue(flags["stability_flag"])

    # ------------------------------------------------------------------ #
    # Zero-entry months preserve low_sample = False
    # ------------------------------------------------------------------ #
    def test_zero_entry_months_not_low_sample(self):
        """Months with entries=0 must have low_sample=False (absent, not under-sampled)."""
        train = self._train_frame()
        test = self._make_test_frame([
            ("2025-01", 2, 0.03, 1.0),   # late trend → excluded
            ("2025-02", 6, 0.03, 0.1),   # signal
        ])
        thresholds = derive_candidate_signal_thresholds(train)
        breakdown = _monthly_breakdown(
            test,
            vol_split_value=thresholds["vol_split_value"],
            maturity_threshold=thresholds["maturity_threshold"],
        )
        monthly = {row["month"]: row for row in breakdown}
        self.assertEqual(monthly["2025-01"]["entries"], 0)
        self.assertFalse(monthly["2025-01"]["low_sample"])
        self.assertFalse(monthly["2025-02"]["low_sample"])
