import unittest

from services.external_data.ml_candidate_signal_readiness import (
    FROZEN_SIGNAL_DEFINITION,
    ReadinessConfig,
    _extract_criteria,
    build_readiness_checklist,
)


def _passing_late_trend() -> dict:
    return {"filtered_validation": {"validated": True}}


def _passing_candidate_signal() -> dict:
    return {"test": {"sample_size_safe": True}}


def _passing_fragility() -> dict:
    return {"flags": {"overfiltering_risk": False, "fragility_warning": False}}


def _passing_paper_eval() -> dict:
    return {
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
    }


def _passing_sensitivity() -> dict:
    return {"threshold_robust": True}


def _all_passing() -> dict:
    return dict(
        late_trend_removal_validation=_passing_late_trend(),
        candidate_signal_diagnostics=_passing_candidate_signal(),
        overextension_fragility_diagnostics=_passing_fragility(),
        paper_eval_diagnostics=_passing_paper_eval(),
        sensitivity_diagnostics=_passing_sensitivity(),
    )


class TestReadinessChecklist(unittest.TestCase):

    # ------------------------------------------------------------------ #
    # Test 1: all criteria pass → criteria_pass_pre_falsification=True;
    # candidate_ready_for_paper_observation is gated by FALSIFICATION_RECORD
    # which is currently True (falsified), so ready remains False.
    # ------------------------------------------------------------------ #
    def test_ready_when_all_criteria_met(self):
        report = build_readiness_checklist(**_all_passing())
        self.assertTrue(report["criteria_pass_pre_falsification"])
        self.assertFalse(report["candidate_ready_for_paper_observation"])
        self.assertEqual(report["status"], "ok")

    # ------------------------------------------------------------------ #
    # Test 2: each failing criterion individually → ready=False
    # ------------------------------------------------------------------ #
    def test_not_ready_when_filtered_validated_false(self):
        kwargs = _all_passing()
        kwargs["late_trend_removal_validation"] = {"filtered_validation": {"validated": False}}
        report = build_readiness_checklist(**kwargs)
        self.assertFalse(report["candidate_ready_for_paper_observation"])

    def test_not_ready_when_sample_size_safe_false(self):
        kwargs = _all_passing()
        kwargs["candidate_signal_diagnostics"] = {"test": {"sample_size_safe": False}}
        report = build_readiness_checklist(**kwargs)
        self.assertFalse(report["candidate_ready_for_paper_observation"])

    def test_not_ready_when_overfiltering_risk_true(self):
        kwargs = _all_passing()
        kwargs["overextension_fragility_diagnostics"] = {
            "flags": {"overfiltering_risk": True, "fragility_warning": False}
        }
        report = build_readiness_checklist(**kwargs)
        self.assertFalse(report["candidate_ready_for_paper_observation"])

    def test_not_ready_when_fragility_warning_true(self):
        kwargs = _all_passing()
        kwargs["overextension_fragility_diagnostics"] = {
            "flags": {"overfiltering_risk": False, "fragility_warning": True}
        }
        report = build_readiness_checklist(**kwargs)
        self.assertFalse(report["candidate_ready_for_paper_observation"])

    def test_not_ready_when_stability_flag_false(self):
        kwargs = _all_passing()
        kwargs["paper_eval_diagnostics"] = {
            "test": {
                "stability": {
                    "flags": {
                        "stability_flag": False,
                        "concentration_warning": False,
                    }
                }
            }
        }
        report = build_readiness_checklist(**kwargs)
        self.assertFalse(report["candidate_ready_for_paper_observation"])

    def test_not_ready_when_concentration_warning_true(self):
        kwargs = _all_passing()
        kwargs["paper_eval_diagnostics"] = {
            "test": {
                "stability": {
                    "flags": {
                        "stability_flag": True,
                        "concentration_warning": True,
                    }
                }
            }
        }
        report = build_readiness_checklist(**kwargs)
        self.assertFalse(report["candidate_ready_for_paper_observation"])

    def test_not_ready_when_threshold_robust_false(self):
        kwargs = _all_passing()
        kwargs["sensitivity_diagnostics"] = {"threshold_robust": False}
        report = build_readiness_checklist(**kwargs)
        self.assertFalse(report["candidate_ready_for_paper_observation"])

    # ------------------------------------------------------------------ #
    # Test 3: safety flags always False
    # ------------------------------------------------------------------ #
    def test_safety_flags_always_false(self):
        report = build_readiness_checklist(**_all_passing())
        self.assertFalse(report["flags"]["live_trading_enabled"])
        self.assertFalse(report["flags"]["edge_claim"])
        criteria = report["criteria"]
        self.assertFalse(criteria["live_trading_enabled"])
        self.assertFalse(criteria["edge_claim"])

        config = ReadinessConfig()
        self.assertFalse(config.live_trading_enabled)
        self.assertFalse(config.edge_claim)

    # ------------------------------------------------------------------ #
    # Test 4: definitions block always False
    # ------------------------------------------------------------------ #
    def test_definitions_always_false(self):
        report = build_readiness_checklist(**_all_passing())
        defs = report["definitions"]
        self.assertFalse(defs["training_performed"])
        self.assertFalse(defs["threshold_optimization_performed"])
        self.assertFalse(defs["filter_changes_performed"])
        self.assertFalse(defs["live_trading_enabled"])
        self.assertFalse(defs["governance_promotion_performed"])

    # ------------------------------------------------------------------ #
    # Test 5: frozen signal definition present with required keys
    # ------------------------------------------------------------------ #
    def test_frozen_signal_definition_present(self):
        report = build_readiness_checklist(**_all_passing())
        fsd = report["frozen_signal_definition"]
        self.assertEqual(fsd["signal_name"], "high_vol_trend_early_candidate")
        self.assertIn("condition_1", fsd["conditions"])
        self.assertIn("condition_2", fsd["conditions"])
        self.assertIn("condition_3", fsd["conditions"])
        self.assertIn("threshold_derivation", fsd)
        self.assertIn("freeze_note", fsd)
        self.assertFalse(fsd["live_trading_enabled"])
        self.assertFalse(fsd["governance_promotion_performed"])

    def test_frozen_signal_definition_constant_unchanged(self):
        """FROZEN_SIGNAL_DEFINITION constant has correct threshold sources."""
        c1 = FROZEN_SIGNAL_DEFINITION["conditions"]["condition_1"]
        c2 = FROZEN_SIGNAL_DEFINITION["conditions"]["condition_2"]
        c3 = FROZEN_SIGNAL_DEFINITION["conditions"]["condition_3"]
        self.assertEqual(c1["feature"], "realized_vol_60d")
        self.assertEqual(c1["operator"], ">=")
        self.assertIn("median", c1["threshold_source"])
        self.assertEqual(c2["feature"], "price_momentum_20d")
        self.assertEqual(c2["operator"], ">")
        self.assertEqual(c2["threshold_value"], 0)
        self.assertEqual(c3["feature"], "distance_from_20d_mean")
        self.assertEqual(c3["operator"], "<")
        self.assertIn("quantile", c3["threshold_source"])
        self.assertIn("train", FROZEN_SIGNAL_DEFINITION["threshold_derivation"])

    # ------------------------------------------------------------------ #
    # Test 6: missing/None sub-reports handled conservatively
    # ------------------------------------------------------------------ #
    def test_missing_inputs_produce_not_ready(self):
        """Empty/missing sub-reports should default conservatively to not-ready."""
        report = build_readiness_checklist(
            late_trend_removal_validation={},
            candidate_signal_diagnostics={},
            overextension_fragility_diagnostics={},
            paper_eval_diagnostics={},
            sensitivity_diagnostics={},
        )
        self.assertFalse(report["candidate_ready_for_paper_observation"])

    def test_none_flags_default_conservatively(self):
        """None values in flag positions default to the conservative side."""
        criteria = _extract_criteria(
            late_trend_removal_validation={"filtered_validation": {"validated": None}},
            candidate_signal_diagnostics={"test": {"sample_size_safe": None}},
            overextension_fragility_diagnostics={"flags": {"overfiltering_risk": None, "fragility_warning": None}},
            paper_eval_diagnostics={
                "test": {"stability": {"flags": {"stability_flag": None, "concentration_warning": None}}}
            },
            sensitivity_diagnostics={"threshold_robust": None},
        )
        # Positive criteria default to False (conservative)
        self.assertFalse(criteria["filtered_validated"])
        self.assertFalse(criteria["sample_size_safe"])
        self.assertFalse(criteria["stability_flag"])
        self.assertFalse(criteria["threshold_robust"])
        # Warning criteria default to True (conservative)
        self.assertTrue(criteria["overfiltering_risk"])
        self.assertTrue(criteria["fragility_warning"])
        self.assertTrue(criteria["concentration_warning"])

    # ------------------------------------------------------------------ #
    # Test 7: criteria dict has all 9 keys and is typed bool
    # ------------------------------------------------------------------ #
    def test_criteria_has_all_ten_keys(self):
        report = build_readiness_checklist(**_all_passing())
        criteria = report["criteria"]
        expected = {
            "filtered_validated",
            "sample_size_safe",
            "overfiltering_risk",
            "fragility_warning",
            "stability_flag",
            "concentration_warning",
            "threshold_robust",
            "boundary_clean",
            "live_trading_enabled",
            "edge_claim",
        }
        self.assertEqual(set(criteria.keys()), expected)
        for key in expected:
            self.assertIsInstance(criteria[key], bool, msg=f"{key} must be bool")

    # ------------------------------------------------------------------ #
    # Test 8: disclaimer and no edge claim
    # ------------------------------------------------------------------ #
    def test_disclaimer_present(self):
        report = build_readiness_checklist(**_all_passing())
        self.assertIn("edge claim", report["disclaimer"])
        self.assertIn("sample_size_caveats", report)
        self.assertIsInstance(report["sample_size_caveats"], list)
        self.assertGreater(len(report["sample_size_caveats"]), 0)
