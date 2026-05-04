"""Tests for governance metadata across PR3 (snooping) and the Falsification PR.

Verifies that:
  - candidate_status is never ready_for_live
  - candidate_status is "falsified_cross_period" while FALSIFICATION_RECORD
    records cross-period failure (current state)
  - live_integration_allowed is always False
  - edge_claim_allowed is always False
  - prospective_paper_observation_allowed is False under falsification, even
    when all per-run criteria pass
  - snooping_metadata fields are correct and frozen
  - governance logic is derived from SNOOPING_METADATA + FALSIFICATION_RECORD,
    not ad-hoc
  - original per-run criteria remain populated for audit history
"""

import unittest

from services.external_data.ml_candidate_signal_readiness import (
    DIAGNOSTICS_EXPLORED_COUNT,
    FALSIFICATION_RECORD,
    SNOOPING_METADATA,
    _candidate_status,
    _governance_flags,
    build_readiness_checklist,
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


def _failing_inputs() -> dict:
    kwargs = _passing_inputs()
    kwargs["sensitivity_diagnostics"] = {"threshold_robust": False}
    return kwargs


class TestCandidateStatusClassification(unittest.TestCase):

    # ------------------------------------------------------------------ #
    # Test 1: all criteria pass → falsified_cross_period (post falsification),
    # not exploratory_paper_candidate, not ready_for_live
    # ------------------------------------------------------------------ #
    def test_all_criteria_pass_gives_falsified_post_falsification(self):
        self.assertTrue(FALSIFICATION_RECORD["candidate_falsified"])
        report = build_readiness_checklist(**_passing_inputs())
        self.assertEqual(report["candidate_status"], "falsified_cross_period")
        self.assertNotEqual(report["candidate_status"], "ready_for_live")
        self.assertNotEqual(report["candidate_status"], "exploratory_paper_candidate")

    # ------------------------------------------------------------------ #
    # Test 2: status is never ready_for_live regardless of criteria
    # ------------------------------------------------------------------ #
    def test_status_is_never_ready_for_live(self):
        for candidate_ready in (True, False):
            status = _candidate_status(candidate_ready)
            self.assertNotEqual(status, "ready_for_live",
                msg=f"candidate_ready={candidate_ready} should never produce ready_for_live")

    # ------------------------------------------------------------------ #
    # Test 3: criteria failure still yields falsified under current record;
    # falsification overrides per-run blocked classification.
    # ------------------------------------------------------------------ #
    def test_failing_criteria_under_falsification_gives_falsified(self):
        report = build_readiness_checklist(**_failing_inputs())
        self.assertFalse(report["candidate_ready_for_paper_observation"])
        # Falsification record overrides "blocked" — the signal is permanently
        # falsified, regardless of whether a particular run's criteria pass.
        self.assertEqual(report["candidate_status"], "falsified_cross_period")

    # ------------------------------------------------------------------ #
    # Test 4: helper status under falsification ignores candidate_ready value
    # ------------------------------------------------------------------ #
    def test_candidate_status_helper_falsified_for_any_candidate_ready(self):
        for candidate_ready in (True, False):
            self.assertEqual(_candidate_status(candidate_ready), "falsified_cross_period")


class TestGovernanceFlags(unittest.TestCase):

    # ------------------------------------------------------------------ #
    # Test 5: live_integration_allowed is always False
    # ------------------------------------------------------------------ #
    def test_live_integration_always_false_when_passing(self):
        report = build_readiness_checklist(**_passing_inputs())
        self.assertFalse(report["governance_flags"]["live_integration_allowed"])

    def test_live_integration_always_false_when_failing(self):
        report = build_readiness_checklist(**_failing_inputs())
        self.assertFalse(report["governance_flags"]["live_integration_allowed"])

    def test_live_integration_always_false_via_helper(self):
        for candidate_ready in (True, False):
            flags = _governance_flags(candidate_ready)
            self.assertFalse(flags["live_integration_allowed"],
                msg=f"live_integration_allowed must be False for candidate_ready={candidate_ready}")

    # ------------------------------------------------------------------ #
    # Test 6: no multiple-testing adjustment blocks live integration
    # ------------------------------------------------------------------ #
    def test_no_mt_adjustment_blocks_live_integration(self):
        """SNOOPING_METADATA has multiple_testing_adjustment_applied=False
        which is a necessary (not sufficient) condition to block live integration.
        """
        self.assertFalse(SNOOPING_METADATA["multiple_testing_adjustment_applied"])
        flags = _governance_flags(True)  # even with candidate_ready=True
        self.assertFalse(flags["live_integration_allowed"])

    # ------------------------------------------------------------------ #
    # Test 7: edge_claim_allowed is always False
    # ------------------------------------------------------------------ #
    def test_edge_claim_always_false_when_passing(self):
        report = build_readiness_checklist(**_passing_inputs())
        self.assertFalse(report["governance_flags"]["edge_claim_allowed"])

    def test_edge_claim_always_false_when_failing(self):
        report = build_readiness_checklist(**_failing_inputs())
        self.assertFalse(report["governance_flags"]["edge_claim_allowed"])

    def test_edge_claim_always_false_via_helper(self):
        for candidate_ready in (True, False):
            flags = _governance_flags(candidate_ready)
            self.assertFalse(flags["edge_claim_allowed"])

    # ------------------------------------------------------------------ #
    # Test 8: paper observation is BLOCKED while signal is falsified, even
    # when per-run criteria pass.
    # ------------------------------------------------------------------ #
    def test_paper_observation_blocked_under_falsification_with_passing_inputs(self):
        report = build_readiness_checklist(**_passing_inputs())
        self.assertFalse(report["governance_flags"]["prospective_paper_observation_allowed"])

    def test_paper_observation_blocked_under_falsification_with_failing_inputs(self):
        report = build_readiness_checklist(**_failing_inputs())
        self.assertFalse(report["governance_flags"]["prospective_paper_observation_allowed"])

    def test_paper_observation_helper_blocked_under_falsification(self):
        for candidate_ready in (True, False):
            flags = _governance_flags(candidate_ready)
            self.assertFalse(
                flags["prospective_paper_observation_allowed"],
                msg=(
                    "prospective_paper_observation_allowed must be False"
                    f" while FALSIFICATION_RECORD.candidate_falsified is True"
                    f" (candidate_ready={candidate_ready})"
                ),
            )

    # ------------------------------------------------------------------ #
    # Test 9: governance_flags has exactly three keys
    # ------------------------------------------------------------------ #
    def test_governance_flags_keys(self):
        report = build_readiness_checklist(**_passing_inputs())
        self.assertEqual(
            set(report["governance_flags"].keys()),
            {"edge_claim_allowed", "live_integration_allowed", "prospective_paper_observation_allowed"},
        )
        for key, value in report["governance_flags"].items():
            self.assertIsInstance(value, bool, msg=f"{key} must be bool")


class TestSnoopingMetadata(unittest.TestCase):

    # ------------------------------------------------------------------ #
    # Test 10: snooping_metadata present in report with all required keys
    # ------------------------------------------------------------------ #
    def test_snooping_metadata_present(self):
        report = build_readiness_checklist(**_passing_inputs())
        self.assertIn("snooping_metadata", report)
        sm = report["snooping_metadata"]
        for key in (
            "diagnostics_explored_count",
            "candidate_discovered_after_diagnostics",
            "pre_registered",
            "multiple_testing_adjustment_applied",
            "prospective_validation_required",
            "snooping_risk_note",
        ):
            self.assertIn(key, sm, msg=f"snooping_metadata missing key: {key}")

    # ------------------------------------------------------------------ #
    # Test 11: snooping_metadata values match SNOOPING_METADATA constant
    # ------------------------------------------------------------------ #
    def test_snooping_metadata_values_are_correct(self):
        report = build_readiness_checklist(**_passing_inputs())
        sm = report["snooping_metadata"]
        self.assertEqual(sm["diagnostics_explored_count"], DIAGNOSTICS_EXPLORED_COUNT)
        self.assertTrue(sm["candidate_discovered_after_diagnostics"])
        self.assertFalse(sm["pre_registered"])
        self.assertFalse(sm["multiple_testing_adjustment_applied"])
        self.assertTrue(sm["prospective_validation_required"])

    # ------------------------------------------------------------------ #
    # Test 12: DIAGNOSTICS_EXPLORED_COUNT constant value
    # ------------------------------------------------------------------ #
    def test_diagnostics_explored_count_is_six(self):
        self.assertEqual(DIAGNOSTICS_EXPLORED_COUNT, 6)

    # ------------------------------------------------------------------ #
    # Test 13: snooping_metadata is the same whether criteria pass or fail
    # ------------------------------------------------------------------ #
    def test_snooping_metadata_invariant_across_criteria_outcomes(self):
        passing_sm = build_readiness_checklist(**_passing_inputs())["snooping_metadata"]
        failing_sm = build_readiness_checklist(**_failing_inputs())["snooping_metadata"]
        # snooping_metadata does not depend on criteria outcome
        for key in ("diagnostics_explored_count", "pre_registered",
                    "multiple_testing_adjustment_applied", "prospective_validation_required"):
            self.assertEqual(passing_sm[key], failing_sm[key],
                msg=f"snooping_metadata[{key}] should be invariant")


class TestDisclaimerAndCandidateStatusIntegration(unittest.TestCase):

    # ------------------------------------------------------------------ #
    # Test 14: disclaimer mentions falsification, no edge claim, and the
    # explicit prohibition on tuning/repairing the signal.
    # ------------------------------------------------------------------ #
    def test_disclaimer_mentions_falsification_and_no_edge_claim(self):
        report = build_readiness_checklist(**_passing_inputs())
        self.assertIn("falsified", report["disclaimer"])
        self.assertIn("2022", report["disclaimer"])
        self.assertIn("no statistical edge claim", report["disclaimer"])
        self.assertIn("prohibited", report["disclaimer"])

    # ------------------------------------------------------------------ #
    # Test 15: post-falsification, candidate_ready cannot be True even when
    # all per-run criteria pass; original criteria block remains populated
    # (audit history) and criteria_pass_pre_falsification reflects the raw
    # per-run decision.
    # ------------------------------------------------------------------ #
    def test_paper_observation_readiness_cannot_be_true_post_falsification(self):
        report = build_readiness_checklist(**_passing_inputs())
        self.assertFalse(report["candidate_ready_for_paper_observation"])
        self.assertEqual(report["candidate_status"], "falsified_cross_period")
        self.assertFalse(report["governance_flags"]["prospective_paper_observation_allowed"])
        self.assertFalse(report["governance_flags"]["live_integration_allowed"])
        self.assertFalse(report["governance_flags"]["edge_claim_allowed"])
        self.assertNotEqual(report["candidate_status"], "ready_for_live")
        # Audit history preserved: original criteria still computed.
        self.assertTrue(report["criteria_pass_pre_falsification"])
        self.assertTrue(report["criteria"]["filtered_validated"])
        self.assertTrue(report["criteria"]["sample_size_safe"])
        self.assertTrue(report["criteria"]["threshold_robust"])
