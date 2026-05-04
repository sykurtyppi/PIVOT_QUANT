import unittest

import pandas as pd

from services.external_data.ml_boundary_purge import (
    MAX_EVALUATED_HORIZON_BDAYS,
    _label_dates_series,
    _test_start_date,
    apply_boundary_purge,
)
from services.external_data.ml_candidate_signal import derive_candidate_signal_thresholds


def _train_frame(entry_dates: list[str], vols: list[float] | None = None) -> pd.DataFrame:
    n = len(entry_dates)
    vols = vols or [0.3] * n
    return pd.DataFrame(
        {
            "symbol": ["SPY"] * n,
            "entry_date": entry_dates,
            "realized_vol_60d": vols,
            "price_momentum_20d": [0.05] * n,
            "distance_from_20d_mean": [0.5] * n,
            "forward_return_5d": [0.01] * n,
        }
    )


def _test_frame(entry_dates: list[str]) -> pd.DataFrame:
    n = len(entry_dates)
    return pd.DataFrame(
        {
            "symbol": ["SPY"] * n,
            "entry_date": entry_dates,
            "realized_vol_60d": [0.3] * n,
            "price_momentum_20d": [0.05] * n,
            "distance_from_20d_mean": [0.5] * n,
            "forward_return_5d": [0.01] * n,
        }
    )


class TestBoundaryPurge(unittest.TestCase):

    # ------------------------------------------------------------------ #
    # Test 1: overlapping train rows are purged
    # ------------------------------------------------------------------ #
    def test_overlapping_train_rows_are_purged(self):
        """Train rows whose label_date (entry + 5 bdays) >= test_start are removed.

        pd.offsets.BDay counts Mon-Fri weekdays only (no market-holiday adjustment):
          2024-12-26 (Thu) + 5 bdays = 2025-01-02 (Thu) — overlap
          2024-12-27 (Fri) + 5 bdays = 2025-01-03 (Fri) — overlap
          2024-12-20 (Fri) + 5 bdays = 2024-12-27 (Fri) — safe
        """
        train = _train_frame(["2024-12-20", "2024-12-26", "2024-12-27"])
        test = _test_frame(["2025-01-02", "2025-01-03"])

        result = apply_boundary_purge(train, test)
        purged = result["purged_frame"]
        rep = result["report"]

        self.assertEqual(rep["rows_purged"], 2)
        self.assertEqual(rep["train_rows_before_purge"], 3)
        self.assertEqual(rep["train_rows_after_purge"], 1)
        self.assertTrue(rep["boundary_label_overlap_detected"])
        self.assertTrue(rep["boundary_purge_applied"])
        # Only the clean row should remain
        self.assertEqual(list(purged["entry_date"]), ["2024-12-20"])

    # ------------------------------------------------------------------ #
    # Test 2: non-overlapping train rows are kept intact
    # ------------------------------------------------------------------ #
    def test_non_overlapping_rows_remain(self):
        """Train rows whose label_date < test_start are fully retained."""
        # 2024-12-13 + 5 bdays = 2024-12-20 < 2025-01-02  (safe)
        # 2024-12-16 + 5 bdays = 2024-12-23 < 2025-01-02  (safe)
        train = _train_frame(["2024-12-13", "2024-12-16"])
        test = _test_frame(["2025-01-02"])

        result = apply_boundary_purge(train, test)
        purged = result["purged_frame"]
        rep = result["report"]

        self.assertEqual(rep["rows_purged"], 0)
        self.assertEqual(rep["train_rows_after_purge"], 2)
        self.assertFalse(rep["boundary_label_overlap_detected"])
        self.assertFalse(rep["boundary_purge_applied"])
        self.assertEqual(len(purged), 2)

    # ------------------------------------------------------------------ #
    # Test 3: test rows are not modified
    # ------------------------------------------------------------------ #
    def test_test_rows_are_unchanged(self):
        """apply_boundary_purge must not alter the test frame.

        2024-12-26 + 5 bdays = 2025-01-02 — contaminated, gets purged.
        """
        train = _train_frame(["2024-12-26"])  # contaminated
        test_dates = ["2025-01-02", "2025-01-03", "2025-01-06"]
        test = _test_frame(test_dates)

        result = apply_boundary_purge(train, test)
        # train was purged
        self.assertEqual(result["report"]["rows_purged"], 1)
        # test frame passed in should be the same object / unchanged
        self.assertEqual(list(test["entry_date"]), test_dates)

    # ------------------------------------------------------------------ #
    # Test 4: thresholds are derived from the purged train frame
    # ------------------------------------------------------------------ #
    def test_thresholds_derived_after_purge(self):
        """derive_candidate_signal_thresholds gives different maturity_threshold
        when a contaminated boundary row with extreme distance is removed.

        2024-12-26 + 5 bdays = 2025-01-02 — that row is purged.
        Remaining rows have distance=0.5; purged row had distance=2.0.
        quantile(0.70) of [0.5, 0.5, 2.0] ≈ 1.1; of [0.5, 0.5] = 0.5.
        """
        train = pd.DataFrame(
            {
                "entry_date": ["2024-12-13", "2024-12-16", "2024-12-26"],
                "realized_vol_60d": [0.3, 0.3, 0.3],
                "price_momentum_20d": [0.05, 0.05, 0.05],
                "distance_from_20d_mean": [0.5, 0.5, 2.0],
                "forward_return_5d": [0.01, 0.01, 0.01],
            }
        )
        test = _test_frame(["2025-01-02"])

        thresholds_original = derive_candidate_signal_thresholds(train)

        result = apply_boundary_purge(train, test)
        purged = result["purged_frame"]
        thresholds_purged = derive_candidate_signal_thresholds(purged)

        # Contaminated row shifts maturity_threshold; purged removes it
        self.assertNotEqual(
            thresholds_original["maturity_threshold"],
            thresholds_purged["maturity_threshold"],
        )
        self.assertAlmostEqual(thresholds_purged["maturity_threshold"], 0.5)

    # ------------------------------------------------------------------ #
    # Test 5: label_date column used when present
    # ------------------------------------------------------------------ #
    def test_uses_label_date_column_when_present(self):
        """When label_date_5d column exists it is used instead of entry+5bdays."""
        # Construct a frame where the label_date_5d column explicitly says 2025-01-02
        # (same result as entry_date + 5 bdays, but exercising the column path)
        train = pd.DataFrame(
            {
                "entry_date": ["2024-12-24"],
                "label_date_5d": ["2025-01-02"],  # explicit column
                "realized_vol_60d": [0.3],
                "price_momentum_20d": [0.05],
                "distance_from_20d_mean": [0.5],
                "forward_return_5d": [0.01],
            }
        )
        test = _test_frame(["2025-01-02"])

        result = apply_boundary_purge(train, test)
        rep = result["report"]
        self.assertTrue(rep["boundary_label_overlap_detected"])
        self.assertEqual(rep["rows_purged"], 1)

    # ------------------------------------------------------------------ #
    # Test 6: report fields are all present
    # ------------------------------------------------------------------ #
    def test_report_fields_present(self):
        """All required report keys must be present."""
        train = _train_frame(["2024-12-16"])
        test = _test_frame(["2025-01-02"])
        rep = apply_boundary_purge(train, test)["report"]

        for key in (
            "status",
            "train_rows_before_purge",
            "train_rows_after_purge",
            "rows_purged",
            "max_label_date_retained",
            "test_start",
            "embargo_horizon_bdays",
            "boundary_label_overlap_detected",
            "boundary_purge_applied",
        ):
            self.assertIn(key, rep, msg=f"missing key: {key}")

    # ------------------------------------------------------------------ #
    # Test 7: MAX_EVALUATED_HORIZON_BDAYS is 5
    # ------------------------------------------------------------------ #
    def test_max_horizon_constant(self):
        self.assertEqual(MAX_EVALUATED_HORIZON_BDAYS, 5)

    # ------------------------------------------------------------------ #
    # Test 8: empty train frame returns skipped status
    # ------------------------------------------------------------------ #
    def test_empty_train_returns_skipped(self):
        train = pd.DataFrame(columns=["entry_date", "realized_vol_60d"])
        test = _test_frame(["2025-01-02"])
        rep = apply_boundary_purge(train, test)["report"]
        self.assertEqual(rep["status"], "skipped")
        self.assertFalse(rep["boundary_label_overlap_detected"])
        self.assertFalse(rep["boundary_purge_applied"])

    # ------------------------------------------------------------------ #
    # Test 9: max_label_date_retained is the latest safe label date
    # ------------------------------------------------------------------ #
    def test_max_label_date_retained_is_accurate(self):
        """After purge, max_label_date_retained reflects the latest safe label.

        2024-12-13 + 5 bdays = 2024-12-20 (safe)
        2024-12-16 + 5 bdays = 2024-12-23 (safe)
        2024-12-26 + 5 bdays = 2025-01-02 (purged)
        max retained label_date = 2024-12-23
        """
        train = _train_frame(["2024-12-13", "2024-12-16", "2024-12-26"])
        test = _test_frame(["2025-01-02"])
        rep = apply_boundary_purge(train, test)["report"]
        self.assertEqual(rep["max_label_date_retained"], "2024-12-23")

    # ------------------------------------------------------------------ #
    # Test 10: _test_start_date helper
    # ------------------------------------------------------------------ #
    def test_start_date_helper(self):
        test = _test_frame(["2025-01-06", "2025-01-02", "2025-01-03"])
        self.assertEqual(_test_start_date(test), "2025-01-02")

    def test_start_date_empty_frame(self):
        self.assertIsNone(_test_start_date(pd.DataFrame()))


class TestBoundaryCleanInReadiness(unittest.TestCase):
    """Verify the boundary_clean criterion in the readiness checklist."""

    def _base_kwargs(self) -> dict:
        return dict(
            late_trend_removal_validation={"filtered_validation": {"validated": True}},
            candidate_signal_diagnostics={"test": {"sample_size_safe": True}},
            overextension_fragility_diagnostics={
                "flags": {"overfiltering_risk": False, "fragility_warning": False}
            },
            paper_eval_diagnostics={
                "test": {
                    "stability": {
                        "flags": {"stability_flag": True, "concentration_warning": False}
                    }
                }
            },
            sensitivity_diagnostics={"threshold_robust": True},
        )

    def test_not_ready_when_overlap_detected_and_purge_not_applied(self):
        from services.external_data.ml_candidate_signal_readiness import build_readiness_checklist
        kwargs = self._base_kwargs()
        kwargs["boundary_purge_report"] = {
            "boundary_label_overlap_detected": True,
            "boundary_purge_applied": False,
        }
        report = build_readiness_checklist(**kwargs)
        self.assertFalse(report["criteria"]["boundary_clean"])
        self.assertFalse(report["candidate_ready_for_paper_observation"])

    def test_ready_when_overlap_detected_and_purge_applied(self):
        # Post-falsification: candidate_ready_for_paper_observation is hard-blocked
        # by FALSIFICATION_RECORD; assert against criteria_pass_pre_falsification
        # so this test continues to verify the boundary-clean criterion path.
        from services.external_data.ml_candidate_signal_readiness import build_readiness_checklist
        kwargs = self._base_kwargs()
        kwargs["boundary_purge_report"] = {
            "boundary_label_overlap_detected": True,
            "boundary_purge_applied": True,
        }
        report = build_readiness_checklist(**kwargs)
        self.assertTrue(report["criteria"]["boundary_clean"])
        self.assertTrue(report["criteria_pass_pre_falsification"])
        self.assertFalse(report["candidate_ready_for_paper_observation"])  # blocked by falsification

    def test_ready_when_no_overlap_detected(self):
        from services.external_data.ml_candidate_signal_readiness import build_readiness_checklist
        kwargs = self._base_kwargs()
        kwargs["boundary_purge_report"] = {
            "boundary_label_overlap_detected": False,
            "boundary_purge_applied": False,
        }
        report = build_readiness_checklist(**kwargs)
        self.assertTrue(report["criteria"]["boundary_clean"])
        self.assertTrue(report["criteria_pass_pre_falsification"])
        self.assertFalse(report["candidate_ready_for_paper_observation"])  # blocked by falsification

    def test_boundary_clean_defaults_true_when_report_absent(self):
        """Without boundary_purge_report, boundary_clean defaults to True (no report = no detected overlap)."""
        from services.external_data.ml_candidate_signal_readiness import build_readiness_checklist
        kwargs = self._base_kwargs()
        # no boundary_purge_report kwarg
        report = build_readiness_checklist(**kwargs)
        self.assertTrue(report["criteria"]["boundary_clean"])
