"""Tests for the Falsification PR.

Verifies that:
  - cross_period_validated=false in the cross-period aggregate cannot coexist
    with prospective_paper_observation_allowed=true in the readiness checklist.
  - A falsified candidate cannot be marked ready, regardless of per-run
    criteria.
  - live_integration_allowed and edge_claim_allowed remain False under
    falsification.
  - Original per-run diagnostics (criteria, criteria_pass_pre_falsification)
    remain populated for audit history.
  - The FALSIFICATION_RECORD constant carries the user-specified fields with
    the recorded numerical values.
"""

import unittest

from services.external_data.ml_candidate_signal_readiness import (
    FALSIFICATION_RECORD,
    SNOOPING_METADATA,
    _candidate_status,
    _governance_flags,
    build_readiness_checklist,
)
from services.external_data.ml_cross_period_validation import (
    aggregate_cross_period_validation,
)


def _passing_inputs() -> dict:
    return dict(
        late_trend_removal_validation={"filtered_validation": {"validated": True}},
        candidate_signal_diagnostics={"test": {"sample_size_safe": True}},
        overextension_fragility_diagnostics={
            "flags": {"overfiltering_risk": False, "fragility_warning": False}
        },
        paper_eval_diagnostics={
            "test": {
                "stability": {
                    "flags": {
                        "stability_flag": True,
                        "concentration_warning": False,
                        "negative_mature_month_warning": False,
                        "low_sample_month_warning": False,
                    }
                }
            }
        },
        sensitivity_diagnostics={"threshold_robust": True},
    )


def _failing_period_report() -> dict:
    return {
        "name": "ml_regime_validation",
        "status": "warn",
        "symbol": "SPY",
        "train_years": ["2021"],
        "test_year": "2022",
        "validated": False,
        "candidate_readiness_checklist": {
            "candidate_status": "falsified_cross_period",
            "candidate_ready_for_paper_observation": False,
            "criteria": {"boundary_clean": True, "filtered_validated": False},
        },
        "candidate_signal_diagnostics": {
            "thresholds": {"vol_split_value": 0.12, "maturity_threshold": 1.5},
            "train": {"sample_size_safe": False},
            "test": {"sample_size_safe": False},
        },
        "late_trend_removal_validation": {
            "baseline_validation": {
                "train": {"sample_size": 66, "win_rate_5d": 0.6212, "mean_return_5d": 0.0019},
                "test": {"sample_size": 98, "win_rate_5d": 0.4796, "mean_return_5d": -0.0038},
            },
            "filtered_validation": {
                "train": {"sample_size": 33, "win_rate_5d": 0.6970, "mean_return_5d": 0.0019},
                "test": {"sample_size": 51, "win_rate_5d": 0.4118, "mean_return_5d": -0.0052},
            },
            "improvement_summary": {"rows_removed_train": 33, "rows_removed_test": 47},
        },
        "boundary_purge_report": {
            "boundary_label_overlap_detected": True,
            "boundary_purge_applied": True,
            "rows_purged": 5,
        },
        "data_coverage": {
            "period_label": "train=2021_partial; test=2022",
            "train_coverage_start": "2021-04-05",
            "train_coverage_end": "2021-12-31",
            "train_is_partial": True,
            "data_coverage_note": "T9 SPY options-features coverage begins 2021-04",
        },
    }


def _passing_baseline_report() -> dict:
    return {
        "name": "ml_regime_validation",
        "status": "ok",
        "symbol": "SPY",
        "train_years": ["2023", "2024"],
        "test_year": "2025",
        "validated": True,
        "candidate_readiness_checklist": {
            "candidate_status": "exploratory_paper_candidate",
            "candidate_ready_for_paper_observation": True,
            "criteria": {"boundary_clean": True, "filtered_validated": True},
        },
        "candidate_signal_diagnostics": {
            "thresholds": {"vol_split_value": 0.12, "maturity_threshold": 1.5},
            "train": {"sample_size_safe": False},
            "test": {"sample_size_safe": True},
        },
        "late_trend_removal_validation": {
            "baseline_validation": {
                "train": {"sample_size": 187, "win_rate_5d": 0.668, "mean_return_5d": 0.0056},
                "test": {"sample_size": 99, "win_rate_5d": 0.687, "mean_return_5d": 0.0061},
            },
            "filtered_validation": {
                "train": {"sample_size": 124, "win_rate_5d": 0.685, "mean_return_5d": 0.0054},
                "test": {"sample_size": 68, "win_rate_5d": 0.81, "mean_return_5d": 0.0101},
            },
            "improvement_summary": {"rows_removed_train": 63, "rows_removed_test": 31},
        },
        "boundary_purge_report": {
            "boundary_label_overlap_detected": True,
            "boundary_purge_applied": True,
            "rows_purged": 4,
        },
    }


class TestFalsificationRecord(unittest.TestCase):
    """The FALSIFICATION_RECORD constant carries the user-specified facts."""

    def test_record_marks_signal_falsified(self):
        self.assertTrue(FALSIFICATION_RECORD["candidate_falsified"])

    def test_record_period_label(self):
        self.assertEqual(
            FALSIFICATION_RECORD["falsification_period"],
            "train=2021_partial; test=2022",
        )

    def test_record_filtered_test_metrics(self):
        self.assertAlmostEqual(
            FALSIFICATION_RECORD["filtered_test_win_rate"], 0.4117647, places=4
        )
        self.assertAlmostEqual(
            FALSIFICATION_RECORD["filtered_test_mean_return"], -0.0051746, places=4
        )

    def test_record_reason_mentions_bear_regime(self):
        self.assertIn("bear-regime", FALSIFICATION_RECORD["reason"])
        self.assertIn("cross-period", FALSIFICATION_RECORD["reason"])

    def test_record_prohibits_tune_or_repair(self):
        self.assertTrue(FALSIFICATION_RECORD["tune_or_repair_prohibited"])
        self.assertIn(
            "Do NOT tune", FALSIFICATION_RECORD["tune_or_repair_note"]
        )

    def test_record_artifacts_point_to_real_paths(self):
        self.assertEqual(
            FALSIFICATION_RECORD["falsification_run_artifact"],
            "reports/ml_diagnostics/spy_2021-2022_ml_regime_validation_cross_period.json",
        )
        self.assertEqual(
            FALSIFICATION_RECORD["cross_period_aggregate_artifact"],
            "reports/ml_diagnostics/ml_cross_period_validation.json",
        )


class TestFalsificationBlocksReadiness(unittest.TestCase):
    """A falsified candidate cannot be ready, regardless of per-run criteria."""

    def test_falsified_candidate_cannot_be_ready_with_passing_inputs(self):
        report = build_readiness_checklist(**_passing_inputs())
        self.assertFalse(report["candidate_ready_for_paper_observation"])
        self.assertEqual(report["candidate_status"], "falsified_cross_period")

    def test_falsified_candidate_status_via_helper_for_any_input(self):
        # The status helper ignores candidate_ready while falsified.
        for candidate_ready in (True, False):
            self.assertEqual(
                _candidate_status(candidate_ready), "falsified_cross_period"
            )

    def test_governance_flags_block_paper_observation_under_falsification(self):
        for candidate_ready in (True, False):
            flags = _governance_flags(candidate_ready)
            self.assertFalse(flags["prospective_paper_observation_allowed"])

    def test_live_and_edge_flags_remain_false_under_falsification(self):
        for candidate_ready in (True, False):
            flags = _governance_flags(candidate_ready)
            self.assertFalse(flags["live_integration_allowed"])
            self.assertFalse(flags["edge_claim_allowed"])
        report = build_readiness_checklist(**_passing_inputs())
        self.assertFalse(report["governance_flags"]["live_integration_allowed"])
        self.assertFalse(report["governance_flags"]["edge_claim_allowed"])


class TestCrossPeriodFailureBlocksPaperObservation(unittest.TestCase):
    """cross_period_validated=false must coexist with paper observation = blocked."""

    def test_cross_period_failed_aggregate_is_consistent_with_blocked_readiness(self):
        agg = aggregate_cross_period_validation(
            [_passing_baseline_report(), _failing_period_report()]
        )
        self.assertFalse(agg["cross_period_validated"])
        self.assertIn(
            "train=2021_partial; test=2022",
            agg["agreement_summary"]["periods_not_ready"],
        )
        # Module-level falsification record means the readiness checklist
        # built from any inputs blocks paper observation:
        readiness = build_readiness_checklist(**_passing_inputs())
        self.assertFalse(readiness["governance_flags"]["prospective_paper_observation_allowed"])
        self.assertFalse(readiness["candidate_ready_for_paper_observation"])
        self.assertEqual(readiness["candidate_status"], "falsified_cross_period")

    def test_falsification_record_is_present_in_readiness_checklist(self):
        readiness = build_readiness_checklist(**_passing_inputs())
        self.assertIn("falsification_record", readiness)
        fr = readiness["falsification_record"]
        for key in (
            "candidate_falsified",
            "falsification_period",
            "filtered_test_win_rate",
            "filtered_test_mean_return",
            "reason",
            "tune_or_repair_prohibited",
        ):
            self.assertIn(key, fr, msg=f"falsification_record missing key: {key}")


class TestAuditHistoryPreserved(unittest.TestCase):
    """Original per-run diagnostics remain populated for audit history."""

    def test_criteria_pass_pre_falsification_reflects_raw_run(self):
        passing = build_readiness_checklist(**_passing_inputs())
        self.assertTrue(passing["criteria_pass_pre_falsification"])
        # Even though paper observation is blocked, raw criteria still pass:
        self.assertTrue(passing["criteria"]["filtered_validated"])
        self.assertTrue(passing["criteria"]["sample_size_safe"])
        self.assertTrue(passing["criteria"]["threshold_robust"])

    def test_failing_run_reports_criteria_pass_false(self):
        failing_inputs = _passing_inputs()
        failing_inputs["sensitivity_diagnostics"] = {"threshold_robust": False}
        failing = build_readiness_checklist(**failing_inputs)
        self.assertFalse(failing["criteria_pass_pre_falsification"])
        self.assertFalse(failing["criteria"]["threshold_robust"])

    def test_snooping_metadata_still_present(self):
        readiness = build_readiness_checklist(**_passing_inputs())
        self.assertIn("snooping_metadata", readiness)
        self.assertEqual(
            readiness["snooping_metadata"]["pre_registered"],
            SNOOPING_METADATA["pre_registered"],
        )

    def test_frozen_signal_definition_unchanged(self):
        readiness = build_readiness_checklist(**_passing_inputs())
        defn = readiness["frozen_signal_definition"]
        self.assertEqual(defn["signal_name"], "high_vol_trend_early_candidate")
        self.assertFalse(defn["live_trading_enabled"])
        self.assertFalse(defn["governance_promotion_performed"])


if __name__ == "__main__":
    unittest.main()
