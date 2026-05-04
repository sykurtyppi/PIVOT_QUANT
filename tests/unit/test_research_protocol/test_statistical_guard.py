"""Tests for services.research_protocol.statistical_guard (RESEARCH_PROTOCOL §4)."""

from __future__ import annotations

import unittest

import numpy as np

from services.research_protocol.errors import StatisticalViolationError
from services.research_protocol.statistical_guard import (
    DEFAULT_PERMUTATION_ALPHA,
    N_EFF_FLOOR_BY_STAGE,
    STATISTICAL_VALIDITY_KEY,
    SUPPRESSIBLE_METRIC_KEYS,
    StatisticalVerdict,
    assert_minimum_sample,
    assert_statistical_pass,
    compute_bootstrap_ci,
    compute_effective_sample_size,
    evaluate_statistical_validity,
    n_eff_floor_for_stage,
    run_permutation_test,
    suppress_metrics_if_invalid,
    verdict_from_dict,
    verdict_to_dict,
)


class TestEffectiveSampleSize(unittest.TestCase):
    def test_basic_floor_division(self):
        self.assertEqual(compute_effective_sample_size([0.0] * 100, horizon_days=5), 20)

    def test_uneven_division_floors(self):
        self.assertEqual(compute_effective_sample_size([0.0] * 99, horizon_days=5), 19)

    def test_too_few_observations_returns_zero(self):
        self.assertEqual(compute_effective_sample_size([0.0] * 4, horizon_days=5), 0)

    def test_empty_returns_zero(self):
        self.assertEqual(compute_effective_sample_size([], horizon_days=5), 0)

    def test_horizon_must_be_positive(self):
        for bad in (0, -1, 1.5, "5", True):
            with self.subTest(bad=bad):
                with self.assertRaises(StatisticalViolationError):
                    compute_effective_sample_size([0.0] * 10, horizon_days=bad)  # type: ignore[arg-type]


class TestNEffFloors(unittest.TestCase):
    def test_per_stage_floors_match_protocol(self):
        # RESEARCH_PROTOCOL §4.2
        self.assertEqual(N_EFF_FLOOR_BY_STAGE[2], 30)
        self.assertEqual(N_EFF_FLOOR_BY_STAGE[3], 30)
        self.assertEqual(N_EFF_FLOOR_BY_STAGE[4], 30)
        self.assertEqual(N_EFF_FLOOR_BY_STAGE[5], 60)
        self.assertEqual(N_EFF_FLOOR_BY_STAGE[6], 100)

    def test_floor_for_stage_uses_table(self):
        for stage, floor in N_EFF_FLOOR_BY_STAGE.items():
            self.assertEqual(n_eff_floor_for_stage(stage), floor)

    def test_floor_for_unknown_stage_uses_default(self):
        self.assertEqual(n_eff_floor_for_stage(99), 30)


class TestAssertMinimumSample(unittest.TestCase):
    def test_passes_when_n_eff_above_threshold(self):
        assert_minimum_sample(40, threshold=30)

    def test_passes_when_n_eff_equals_threshold(self):
        assert_minimum_sample(30, threshold=30)

    def test_raises_when_below_threshold(self):
        with self.assertRaises(StatisticalViolationError) as ctx:
            assert_minimum_sample(14, threshold=30, stage=2)
        self.assertIn("n_eff=14", str(ctx.exception))
        self.assertIn("threshold=30", str(ctx.exception))
        self.assertIn("stage 2", str(ctx.exception))

    def test_rejects_negative_threshold(self):
        with self.assertRaises(StatisticalViolationError):
            assert_minimum_sample(10, threshold=-1)

    def test_rejects_negative_n_eff(self):
        with self.assertRaises(StatisticalViolationError):
            assert_minimum_sample(-1, threshold=30)


class TestBootstrapCI(unittest.TestCase):
    def _strong_returns(self) -> np.ndarray:
        rng = np.random.default_rng(42)
        # mean ~ 0.005, std ~ 0.005; clearly positive
        return rng.normal(loc=0.005, scale=0.005, size=400)

    def _zero_mean_returns(self) -> np.ndarray:
        # Symmetric construction: half +0.005, half -0.005, mean exactly 0.
        # Any block-bootstrap resample of this has a CI that straddles 0.
        return np.array([0.005, -0.005] * 200)

    def test_constant_sample_yields_constant_ci(self):
        sample = [0.01] * 100
        lo, hi = compute_bootstrap_ci(
            sample, block_size=5, iterations=200, rng_seed=0
        )
        self.assertAlmostEqual(lo, 0.01, places=8)
        self.assertAlmostEqual(hi, 0.01, places=8)

    def test_strong_positive_signal_excludes_zero(self):
        sample = self._strong_returns().tolist()
        lo, hi = compute_bootstrap_ci(
            sample, block_size=5, iterations=2000, rng_seed=42
        )
        self.assertGreater(lo, 0.0, msg=f"CI=[{lo}, {hi}] should exclude 0")
        self.assertGreater(hi, lo)

    def test_zero_mean_signal_includes_zero(self):
        sample = self._zero_mean_returns().tolist()
        lo, hi = compute_bootstrap_ci(
            sample, block_size=5, iterations=2000, rng_seed=42
        )
        self.assertLessEqual(lo, 0.0)
        self.assertGreaterEqual(hi, 0.0)

    def test_deterministic_under_same_seed(self):
        sample = self._strong_returns().tolist()
        first = compute_bootstrap_ci(
            sample, block_size=5, iterations=500, rng_seed=123
        )
        second = compute_bootstrap_ci(
            sample, block_size=5, iterations=500, rng_seed=123
        )
        self.assertEqual(first, second)

    def test_different_seeds_produce_different_results(self):
        sample = self._strong_returns().tolist()
        first = compute_bootstrap_ci(
            sample, block_size=5, iterations=500, rng_seed=1
        )
        second = compute_bootstrap_ci(
            sample, block_size=5, iterations=500, rng_seed=2
        )
        self.assertNotEqual(first, second)

    def test_block_size_must_be_at_most_n(self):
        with self.assertRaises(StatisticalViolationError):
            compute_bootstrap_ci([1.0, 2.0, 3.0], block_size=10, iterations=200)

    def test_iterations_floor(self):
        with self.assertRaises(StatisticalViolationError):
            compute_bootstrap_ci([0.0] * 100, block_size=5, iterations=10)

    def test_bad_method_rejected(self):
        with self.assertRaises(StatisticalViolationError):
            compute_bootstrap_ci(
                [0.0] * 100, method="iid", block_size=5, iterations=200
            )

    def test_bad_statistic_rejected(self):
        with self.assertRaises(StatisticalViolationError):
            compute_bootstrap_ci(
                [0.0] * 100, block_size=5, iterations=200, statistic="alpha"
            )


class TestPermutationTest(unittest.TestCase):
    def _strong_signal_vs_baseline(self):
        rng = np.random.default_rng(11)
        sig = rng.normal(loc=0.01, scale=0.005, size=200)
        base = rng.normal(loc=0.0, scale=0.005, size=200)
        return sig, base

    def _identical_distributions(self):
        rng = np.random.default_rng(13)
        sig = rng.normal(loc=0.0, scale=0.005, size=200)
        base = rng.normal(loc=0.0, scale=0.005, size=200)
        return sig, base

    def test_strong_signal_yields_low_p(self):
        sig, base = self._strong_signal_vs_baseline()
        p = run_permutation_test(
            signal_returns=sig.tolist(),
            baseline_returns=base.tolist(),
            n_iter=500,
            rng_seed=0,
        )
        self.assertLessEqual(p, 0.01, msg=f"expected p << 0.05, got {p}")

    def test_identical_distributions_yields_high_p(self):
        sig, base = self._identical_distributions()
        p = run_permutation_test(
            signal_returns=sig.tolist(),
            baseline_returns=base.tolist(),
            n_iter=500,
            rng_seed=0,
        )
        self.assertGreaterEqual(p, 0.05, msg=f"expected p >= 0.05, got {p}")

    def test_phipson_smyth_floor(self):
        # smallest possible p is 1/(n_iter+1)
        sig = [1.0] * 50
        base = [-1.0] * 50
        p = run_permutation_test(
            signal_returns=sig, baseline_returns=base,
            n_iter=200, rng_seed=0,
        )
        self.assertGreaterEqual(p, 1 / 201)
        self.assertLessEqual(p, 0.01)

    def test_deterministic_under_same_seed(self):
        sig, base = self._strong_signal_vs_baseline()
        p1 = run_permutation_test(
            signal_returns=sig.tolist(), baseline_returns=base.tolist(),
            n_iter=300, rng_seed=99,
        )
        p2 = run_permutation_test(
            signal_returns=sig.tolist(), baseline_returns=base.tolist(),
            n_iter=300, rng_seed=99,
        )
        self.assertEqual(p1, p2)

    def test_one_sided_less_inverts_decision(self):
        sig = [-0.01] * 100
        base = [0.0] * 100
        p_greater = run_permutation_test(
            signal_returns=sig, baseline_returns=base, n_iter=200, rng_seed=0,
            one_sided="greater",
        )
        p_less = run_permutation_test(
            signal_returns=sig, baseline_returns=base, n_iter=200, rng_seed=0,
            one_sided="less",
        )
        self.assertGreater(p_greater, 0.5)
        self.assertLess(p_less, 0.05)

    def test_invalid_one_sided_rejected(self):
        with self.assertRaises(StatisticalViolationError):
            run_permutation_test(
                signal_returns=[1.0] * 10,
                baseline_returns=[0.0] * 10,
                n_iter=200, one_sided="two-sided",
            )

    def test_empty_inputs_rejected(self):
        with self.assertRaises(StatisticalViolationError):
            run_permutation_test(
                signal_returns=[], baseline_returns=[1.0],
                n_iter=200,
            )

    def test_n_iter_floor(self):
        with self.assertRaises(StatisticalViolationError):
            run_permutation_test(
                signal_returns=[1.0] * 10,
                baseline_returns=[0.0] * 10,
                n_iter=10,
            )


class TestEvaluateStatisticalValidity(unittest.TestCase):
    def test_all_pass_yields_statistical_pass(self):
        v = evaluate_statistical_validity(
            stage=2, n_obs=200, horizon_days=5,
            ci_lower=0.001, ci_upper=0.008,
            permutation_p_value=0.01,
        )
        self.assertTrue(v.statistical_pass)
        self.assertFalse(v.metrics_suppressed)
        self.assertEqual(v.suppression_reasons, ())
        self.assertEqual(v.n_eff, 40)
        self.assertEqual(v.n_eff_floor, 30)

    def test_n_eff_below_floor_suppresses_metrics(self):
        v = evaluate_statistical_validity(
            stage=2, n_obs=10, horizon_days=5,
            ci_lower=0.01, ci_upper=0.02,
            permutation_p_value=0.001,
        )
        self.assertFalse(v.statistical_pass)
        self.assertTrue(v.metrics_suppressed)
        self.assertEqual(v.n_eff, 2)
        self.assertTrue(any("n_eff" in r for r in v.suppression_reasons))

    def test_ci_includes_zero_blocks(self):
        v = evaluate_statistical_validity(
            stage=2, n_obs=200, horizon_days=5,
            ci_lower=-0.002, ci_upper=0.005,
            permutation_p_value=0.001,
        )
        self.assertFalse(v.statistical_pass)
        self.assertFalse(v.metrics_suppressed)  # n_eff was fine
        self.assertTrue(any("CI" in r for r in v.suppression_reasons))

    def test_high_p_blocks(self):
        v = evaluate_statistical_validity(
            stage=2, n_obs=200, horizon_days=5,
            ci_lower=0.001, ci_upper=0.008,
            permutation_p_value=0.10,
        )
        self.assertFalse(v.statistical_pass)
        self.assertFalse(v.metrics_suppressed)
        self.assertTrue(
            any("p_value" in r or "permutation" in r
                for r in v.suppression_reasons)
        )

    def test_multiple_failures_record_multiple_reasons(self):
        v = evaluate_statistical_validity(
            stage=2, n_obs=10, horizon_days=5,           # n_eff fail
            ci_lower=-0.01, ci_upper=0.01,              # CI fail
            permutation_p_value=0.20,                    # p fail
        )
        self.assertFalse(v.statistical_pass)
        self.assertGreaterEqual(len(v.suppression_reasons), 3)

    def test_higher_floor_at_stage_5(self):
        # n_eff = 200 / 5 = 40; below stage-5 floor of 60.
        v = evaluate_statistical_validity(
            stage=5, n_obs=200, horizon_days=5,
            ci_lower=0.001, ci_upper=0.008,
            permutation_p_value=0.001,
        )
        self.assertTrue(v.metrics_suppressed)
        self.assertFalse(v.statistical_pass)

    def test_invalid_inputs_raise(self):
        with self.assertRaises(StatisticalViolationError):
            evaluate_statistical_validity(
                stage=0, n_obs=10, horizon_days=5,
                ci_lower=None, ci_upper=None,
                permutation_p_value=None,
            )
        with self.assertRaises(StatisticalViolationError):
            evaluate_statistical_validity(
                stage=2, n_obs=-1, horizon_days=5,
                ci_lower=None, ci_upper=None, permutation_p_value=None,
            )
        with self.assertRaises(StatisticalViolationError):
            evaluate_statistical_validity(
                stage=2, n_obs=10, horizon_days=0,
                ci_lower=None, ci_upper=None, permutation_p_value=None,
            )

    def test_ci_lower_must_match_ci_upper_presence(self):
        with self.assertRaises(StatisticalViolationError):
            evaluate_statistical_validity(
                stage=2, n_obs=200, horizon_days=5,
                ci_lower=0.001, ci_upper=None,
                permutation_p_value=0.01,
            )

    def test_inverted_ci_rejected(self):
        with self.assertRaises(StatisticalViolationError):
            evaluate_statistical_validity(
                stage=2, n_obs=200, horizon_days=5,
                ci_lower=0.10, ci_upper=0.01,  # lower > upper
                permutation_p_value=0.01,
            )

    def test_p_value_out_of_range_rejected(self):
        for bad in (-0.01, 1.5):
            with self.subTest(bad=bad):
                with self.assertRaises(StatisticalViolationError):
                    evaluate_statistical_validity(
                        stage=2, n_obs=200, horizon_days=5,
                        ci_lower=0.001, ci_upper=0.008,
                        permutation_p_value=bad,
                    )


class TestAssertStatisticalPass(unittest.TestCase):
    def test_passes_on_valid_verdict(self):
        v = evaluate_statistical_validity(
            stage=2, n_obs=200, horizon_days=5,
            ci_lower=0.001, ci_upper=0.008,
            permutation_p_value=0.01,
        )
        assert_statistical_pass(v)

    def test_raises_on_invalid_verdict(self):
        v = evaluate_statistical_validity(
            stage=2, n_obs=10, horizon_days=5,
            ci_lower=0.01, ci_upper=0.02,
            permutation_p_value=0.001,
        )
        with self.assertRaises(StatisticalViolationError):
            assert_statistical_pass(v)


class TestSuppressMetricsIfInvalid(unittest.TestCase):
    def _passing_verdict(self) -> StatisticalVerdict:
        return evaluate_statistical_validity(
            stage=2, n_obs=200, horizon_days=5,
            ci_lower=0.001, ci_upper=0.008,
            permutation_p_value=0.01,
        )

    def _suppressing_verdict(self) -> StatisticalVerdict:
        return evaluate_statistical_validity(
            stage=2, n_obs=10, horizon_days=5,
            ci_lower=0.01, ci_upper=0.02,
            permutation_p_value=0.001,
        )

    def _ci_failing_verdict(self) -> StatisticalVerdict:
        return evaluate_statistical_validity(
            stage=2, n_obs=200, horizon_days=5,
            ci_lower=-0.002, ci_upper=0.005,
            permutation_p_value=0.001,
        )

    def test_passing_verdict_preserves_metrics(self):
        metrics = {"win_rate": 0.62, "mean_return": 0.005, "n_obs": 200}
        out = suppress_metrics_if_invalid(metrics, verdict=self._passing_verdict())
        self.assertEqual(out["win_rate"], 0.62)
        self.assertEqual(out["mean_return"], 0.005)
        self.assertTrue(out["statistical_pass"])
        self.assertFalse(out["metrics_suppressed"])

    def test_suppression_removes_perf_metrics_but_preserves_audit(self):
        metrics = {
            "win_rate": 0.62,
            "mean_return": 0.005,
            "filtered_test_win_rate": 0.81,
            "filtered_test_mean_return": 0.01,
            # Audit / diagnostic — must be preserved
            "registration_hash": "a" * 64,
            "run_timestamp": "2026-05-04T19:00:00Z",
            "dataset_identifier": "spy_2025",
            "candidate_id": "test-candidate",
            "n_obs": 10,
            "horizon_days": 5,
        }
        verdict = self._suppressing_verdict()
        out = suppress_metrics_if_invalid(metrics, verdict=verdict)
        # Suppressible perf keys removed
        for key in ("win_rate", "mean_return",
                    "filtered_test_win_rate", "filtered_test_mean_return"):
            self.assertNotIn(key, out, msg=f"{key} should have been suppressed")
        # Audit fields preserved
        self.assertEqual(out["registration_hash"], "a" * 64)
        self.assertEqual(out["run_timestamp"], "2026-05-04T19:00:00Z")
        self.assertEqual(out["dataset_identifier"], "spy_2025")
        self.assertEqual(out["candidate_id"], "test-candidate")
        self.assertEqual(out["n_obs"], 10)
        # Verdict mirrored at top level
        self.assertEqual(out["n_eff"], verdict.n_eff)
        self.assertEqual(out["ci_lower"], verdict.ci_lower)
        self.assertEqual(out["ci_upper"], verdict.ci_upper)
        self.assertEqual(out["permutation_p_value"], verdict.permutation_p_value)
        self.assertTrue(out["metrics_suppressed"])
        self.assertFalse(out["statistical_pass"])
        # Verdict block embedded
        self.assertIn(STATISTICAL_VALIDITY_KEY, out)
        self.assertEqual(out[STATISTICAL_VALIDITY_KEY]["statistical_pass"], False)
        self.assertEqual(out[STATISTICAL_VALIDITY_KEY]["metrics_suppressed"], True)

    def test_ci_failure_does_not_remove_metrics(self):
        """CI failure marks statistical_pass=False but does NOT suppress
        the underlying performance metrics — there is enough sample size
        to display them, they're just statistically insignificant."""
        metrics = {"win_rate": 0.55, "mean_return": 0.001}
        verdict = self._ci_failing_verdict()
        out = suppress_metrics_if_invalid(metrics, verdict=verdict)
        self.assertEqual(out["win_rate"], 0.55)
        self.assertEqual(out["mean_return"], 0.001)
        self.assertFalse(out["statistical_pass"])
        self.assertFalse(out["metrics_suppressed"])

    def test_suppressible_keys_set_is_immutable(self):
        # The frozenset ensures we cannot accidentally add/remove keys at runtime.
        with self.assertRaises(AttributeError):
            SUPPRESSIBLE_METRIC_KEYS.add("foo")  # type: ignore[attr-defined]


class TestVerdictRoundtripAndTamperingDetection(unittest.TestCase):
    def test_to_dict_then_from_dict_preserves_pass_state(self):
        v = evaluate_statistical_validity(
            stage=3, n_obs=300, horizon_days=5,
            ci_lower=0.001, ci_upper=0.008,
            permutation_p_value=0.01,
        )
        roundtripped = verdict_from_dict(verdict_to_dict(v))
        self.assertEqual(roundtripped.statistical_pass, v.statistical_pass)
        self.assertEqual(roundtripped.metrics_suppressed, v.metrics_suppressed)
        self.assertEqual(roundtripped.n_eff, v.n_eff)
        self.assertEqual(roundtripped.suppression_reasons, v.suppression_reasons)

    def test_tampered_pass_field_is_recomputed(self):
        """If a tamperer flips statistical_pass to True while the underlying
        numbers still fail, verdict_from_dict recomputes from the inputs and
        produces statistical_pass=False — the lie does not survive."""
        v = evaluate_statistical_validity(
            stage=2, n_obs=10, horizon_days=5,
            ci_lower=0.01, ci_upper=0.02,
            permutation_p_value=0.001,
        )
        d = verdict_to_dict(v)
        d["statistical_pass"] = True
        d["metrics_suppressed"] = False
        recomputed = verdict_from_dict(d)
        self.assertFalse(recomputed.statistical_pass)
        self.assertTrue(recomputed.metrics_suppressed)

    def test_missing_required_fields_rejected(self):
        v = evaluate_statistical_validity(
            stage=2, n_obs=200, horizon_days=5,
            ci_lower=0.001, ci_upper=0.008,
            permutation_p_value=0.01,
        )
        d = verdict_to_dict(v)
        del d["ci_lower"]
        with self.assertRaises(StatisticalViolationError):
            verdict_from_dict(d)


class TestProtocolConstants(unittest.TestCase):
    def test_default_alpha_is_0_05(self):
        self.assertEqual(DEFAULT_PERMUTATION_ALPHA, 0.05)


if __name__ == "__main__":
    unittest.main()
