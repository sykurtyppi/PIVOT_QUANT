"""Tests for services.research_protocol.trial_budget (RESEARCH_PROTOCOL §8)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from services.research_protocol._paths import (
    ENV_PROTOCOL_ROOT,
    trial_budget_state_path,
)
from services.research_protocol.errors import (
    RegistrationMissingError,
    TrialBudgetViolationError,
)
from services.research_protocol.kill_list import record_kill
from services.research_protocol.registration import (
    HASH_FIELD,
    Registration,
    compute_registration_hash,
    load_registration,
)
from services.research_protocol.trial_budget import (
    DEFAULT_HYPOTHESIS_FAMILY,
    MAX_TRIALS_PER_FAMILY_PER_QUARTER,
    MODIFICATION_TYPES,
    TRIAL_BUDGET_VERSION,
    TrialBudgetSummary,
    TrialEntry,
    assert_trial_budget_available,
    classify_candidate_change,
    get_trial,
    list_trials,
    load_trial_state,
    quarter_for_timestamp,
    record_trial,
    summarize_trial_budget,
)


def _registration_payload(
    *,
    candidate_id: str,
    timestamp: str = "2026-05-04T18:00:00Z",
    hypothesis_family: str | None = "iv_crush",
    parent_candidate_id: str | None = None,
    claimed_modification_type: str | None = None,
    features: list | None = None,
    thresholds: list | None = None,
    horizon_days: int = 5,
    random_seed: int = 42,
    mechanism: str = "post-FOMC dealer hedging compresses near-dated IV",
    datasets_block: dict | None = None,
) -> dict:
    payload = {
        "candidate_id": candidate_id,
        "registration_timestamp": timestamp,
        "git_commit_sha": "0" * 40,
        "hypothesis": {
            "mechanism": mechanism,
            "predicted_direction": "long",
            "why_might_fail": "regime",
            "citations": ["paper:x"],
        },
        "features": features or [{"name": "iv_change_5d", "input_columns": ["iv"]}],
        "thresholds": thresholds or [{"name": "t1", "kind": "fixed", "value": -0.05}],
        "transformations": {"allowed": [], "forbidden_unless_listed": ["x"]},
        "forbidden_changes": ["any"],
        "falsification": {"stage_3": "cross_period_validated=false"},
        "datasets": datasets_block or {
            "symbol": "SPY",
            "validation_dataset_pattern": "v.parquet",
            "holdout_dataset_pattern": "h.parquet",
        },
        "horizon_days": horizon_days,
        "random_seed": random_seed,
        "stages_required": [1, 2, 3, 4, 5, 6],
    }
    if hypothesis_family is not None:
        payload["hypothesis_family"] = hypothesis_family
    if parent_candidate_id is not None:
        payload["parent_candidate_id"] = parent_candidate_id
    if claimed_modification_type is not None:
        payload["claimed_modification_type"] = claimed_modification_type
    payload[HASH_FIELD] = compute_registration_hash(payload)
    return payload


def _write_registration(tmp: Path, payload: dict) -> Path:
    regs = tmp / "registrations"
    regs.mkdir(parents=True, exist_ok=True)
    path = regs / f"{payload['candidate_id']}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


class _ProtocolRootBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp_ctx = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_ctx.name).resolve()
        self._prev_env = os.environ.get(ENV_PROTOCOL_ROOT)
        os.environ[ENV_PROTOCOL_ROOT] = str(self.tmp)

    def tearDown(self) -> None:
        if self._prev_env is None:
            os.environ.pop(ENV_PROTOCOL_ROOT, None)
        else:
            os.environ[ENV_PROTOCOL_ROOT] = self._prev_env
        self._tmp_ctx.cleanup()

    def _register(self, **kwargs) -> Registration:
        payload = _registration_payload(**kwargs)
        _write_registration(self.tmp, payload)
        return load_registration(payload["candidate_id"])


# --------------------------------------------------------------------- #
# Quarter helper
# --------------------------------------------------------------------- #


class TestQuarterForTimestamp(unittest.TestCase):
    def test_q1(self):
        self.assertEqual(quarter_for_timestamp("2026-01-15T00:00:00+00:00"), "2026-Q1")
        self.assertEqual(quarter_for_timestamp("2026-03-31T23:59:59+00:00"), "2026-Q1")

    def test_q2(self):
        self.assertEqual(quarter_for_timestamp("2026-04-01T00:00:00+00:00"), "2026-Q2")
        self.assertEqual(quarter_for_timestamp("2026-06-30T23:59:59+00:00"), "2026-Q2")

    def test_q3(self):
        self.assertEqual(quarter_for_timestamp("2026-07-01T00:00:00+00:00"), "2026-Q3")

    def test_q4(self):
        self.assertEqual(quarter_for_timestamp("2026-12-31T23:59:59+00:00"), "2026-Q4")

    def test_trailing_z_accepted(self):
        self.assertEqual(quarter_for_timestamp("2026-05-04T19:00:00Z"), "2026-Q2")

    def test_invalid_rejected(self):
        for bad in ("yesterday", "", None, "2026/01/01"):
            with self.subTest(bad=bad):
                with self.assertRaises(TrialBudgetViolationError):
                    quarter_for_timestamp(bad)  # type: ignore[arg-type]


# --------------------------------------------------------------------- #
# Recording
# --------------------------------------------------------------------- #


class TestRecordTrial(_ProtocolRootBase):
    def test_first_registration_consumes_one_trial(self):
        reg = self._register(candidate_id="cand-001")
        entry = record_trial(reg)
        self.assertIsInstance(entry, TrialEntry)
        self.assertEqual(entry.candidate_id, "cand-001")
        self.assertEqual(entry.modification_type, "new_hypothesis")
        self.assertEqual(entry.hypothesis_family, "iv_crush")
        self.assertEqual(len(list_trials()), 1)

    def test_persists_to_disk(self):
        reg = self._register(candidate_id="cand-001")
        record_trial(reg)
        on_disk = json.loads(trial_budget_state_path().read_text())
        self.assertEqual(on_disk["version"], TRIAL_BUDGET_VERSION)
        self.assertEqual(len(on_disk["trials"]), 1)
        self.assertEqual(on_disk["trials"][0]["candidate_id"], "cand-001")

    def test_repeated_same_registration_idempotent(self):
        reg = self._register(candidate_id="cand-001")
        first = record_trial(reg)
        second = record_trial(reg)
        self.assertEqual(first.recorded_at, second.recorded_at)
        self.assertEqual(len(list_trials()), 1)

    def test_recording_with_changed_registration_hash_rejected(self):
        # Record once, then mutate the registration JSON behind the scenes
        # to simulate a new hash for the same candidate_id, then attempt
        # to record again with the new Registration instance.
        reg_first = self._register(candidate_id="cand-001", random_seed=42)
        record_trial(reg_first)
        # Overwrite the on-disk file with a different random_seed (and
        # recompute the hash field so load_registration accepts it).
        new_payload = _registration_payload(
            candidate_id="cand-001", random_seed=99,
        )
        _write_registration(self.tmp, new_payload)
        reg_second = load_registration("cand-001")
        with self.assertRaises(TrialBudgetViolationError) as ctx:
            record_trial(reg_second)
        self.assertIn("registration_hash", str(ctx.exception))

    def test_default_hypothesis_family_when_unspecified(self):
        reg = self._register(candidate_id="cand-001", hypothesis_family=None)
        entry = record_trial(reg)
        self.assertEqual(entry.hypothesis_family, DEFAULT_HYPOTHESIS_FAMILY)

    def test_invalid_hypothesis_family_rejected(self):
        reg = self._register(candidate_id="cand-001", hypothesis_family="Bad-Family")
        with self.assertRaises(TrialBudgetViolationError):
            record_trial(reg)

    def test_invalid_claimed_modification_type_rejected(self):
        reg = self._register(
            candidate_id="cand-001",
            claimed_modification_type="bogus",
        )
        with self.assertRaises(TrialBudgetViolationError):
            record_trial(reg)

    def test_record_with_unknown_parent_raises(self):
        reg = self._register(
            candidate_id="cand-002",
            parent_candidate_id="never-registered",
        )
        with self.assertRaises(TrialBudgetViolationError) as ctx:
            record_trial(reg)
        self.assertIn("loading that registration failed", str(ctx.exception))


# --------------------------------------------------------------------- #
# Classification
# --------------------------------------------------------------------- #


class TestClassifyCandidateChange(_ProtocolRootBase):
    def test_threshold_change_classified(self):
        parent = self._register(
            candidate_id="cand-001",
            thresholds=[{"name": "t", "kind": "fixed", "value": -0.05}],
        )
        child = self._register(
            candidate_id="cand-002",
            thresholds=[{"name": "t", "kind": "fixed", "value": -0.03}],
        )
        self.assertEqual(
            classify_candidate_change(child, parent), "threshold_change"
        )

    def test_feature_change_classified(self):
        parent = self._register(
            candidate_id="cand-001",
            features=[{"name": "iv_change_5d", "input_columns": ["iv"]}],
        )
        child = self._register(
            candidate_id="cand-002",
            features=[{"name": "iv_change_10d", "input_columns": ["iv"]}],
        )
        self.assertEqual(
            classify_candidate_change(child, parent), "feature_change"
        )

    def test_period_change_classified(self):
        parent = self._register(
            candidate_id="cand-001",
            datasets_block={
                "symbol": "SPY",
                "validation_dataset_pattern": "spy_2025.parquet",
                "holdout_dataset_pattern": "h.parquet",
            },
        )
        child = self._register(
            candidate_id="cand-002",
            datasets_block={
                "symbol": "SPY",
                "validation_dataset_pattern": "spy_2022.parquet",
                "holdout_dataset_pattern": "h.parquet",
            },
        )
        self.assertEqual(
            classify_candidate_change(child, parent), "period_change"
        )

    def test_symbol_change_classified(self):
        parent = self._register(
            candidate_id="cand-001",
            datasets_block={
                "symbol": "SPY",
                "validation_dataset_pattern": "spy.parquet",
                "holdout_dataset_pattern": "h.parquet",
            },
        )
        child = self._register(
            candidate_id="cand-002",
            datasets_block={
                "symbol": "QQQ",
                "validation_dataset_pattern": "spy.parquet",
                "holdout_dataset_pattern": "h.parquet",
            },
        )
        self.assertEqual(
            classify_candidate_change(child, parent), "symbol_change"
        )

    def test_parameter_change_classified(self):
        parent = self._register(candidate_id="cand-001", horizon_days=5)
        child = self._register(candidate_id="cand-002", horizon_days=21)
        self.assertEqual(
            classify_candidate_change(child, parent), "parameter_change"
        )

    def test_killed_parent_makes_revival_attempt(self):
        # Child timestamp in 2099 so it is unambiguously after wall-clock
        # killed_at on any test machine, forcing the revival path.
        parent = self._register(
            candidate_id="cand-001",
            timestamp="2026-05-04T18:00:00Z",
        )
        record_trial(parent)
        record_kill(
            candidate_id=parent.candidate_id,
            registration_hash=parent.registration_hash,
            stage=3, reason="cross-period falsified",
        )
        child = self._register(
            candidate_id="cand-002",
            timestamp="2099-12-31T18:00:00Z",
            parent_candidate_id="cand-001",
            thresholds=[{"name": "t", "kind": "fixed", "value": -0.07}],
        )
        self.assertEqual(
            classify_candidate_change(child, parent), "revival_attempt"
        )

    def test_pre_registered_before_failure_is_not_revival(self):
        """A child registration whose timestamp PRECEDES the parent's
        killed_at is a legitimate pre-registered alternate.

        Both timestamps are deliberately in the past so the kill_at
        wall-clock (datetime.now() at test time) is unambiguously after
        them under lexicographic ISO8601 comparison.
        """
        parent = self._register(
            candidate_id="cand-001",
            timestamp="2025-01-10T10:00:00Z",
        )
        record_trial(parent)
        # Pre-register the alternate BEFORE the parent fails.
        child = self._register(
            candidate_id="cand-002",
            timestamp="2025-01-10T11:00:00Z",
            parent_candidate_id="cand-001",
            thresholds=[{"name": "t", "kind": "fixed", "value": -0.07}],
        )
        # NOW the parent is killed (record_kill timestamps with now()).
        record_kill(
            candidate_id=parent.candidate_id,
            registration_hash=parent.registration_hash,
            stage=3, reason="cross-period falsified",
        )
        # The child was registered before the kill; classify by diff.
        self.assertEqual(
            classify_candidate_change(child, parent), "threshold_change"
        )

    def test_materially_different_new_hypothesis_allowed(self):
        """A killed parent + claimed_modification_type='new_hypothesis' +
        different hypothesis_family is reclassified to new_hypothesis."""
        parent = self._register(
            candidate_id="cand-001",
            timestamp="2026-05-04T18:00:00Z",
            hypothesis_family="iv_crush",
        )
        record_trial(parent)
        record_kill(
            candidate_id=parent.candidate_id,
            registration_hash=parent.registration_hash,
            stage=3, reason="cross-period falsified",
        )
        child = self._register(
            candidate_id="cand-002",
            timestamp="2099-12-31T18:00:00Z",
            parent_candidate_id="cand-001",
            hypothesis_family="weekend_decay",
            claimed_modification_type="new_hypothesis",
            mechanism="weekend gap variance and dealer hedge unwinding",
        )
        self.assertEqual(
            classify_candidate_change(child, parent), "new_hypothesis"
        )

    def test_claim_without_family_change_still_revival(self):
        """A claimed new_hypothesis but same family does not bypass revival."""
        parent = self._register(
            candidate_id="cand-001",
            timestamp="2026-05-04T18:00:00Z",
            hypothesis_family="iv_crush",
        )
        record_trial(parent)
        record_kill(
            candidate_id=parent.candidate_id,
            registration_hash=parent.registration_hash,
            stage=3, reason="cross-period falsified",
        )
        child = self._register(
            candidate_id="cand-002",
            timestamp="2099-12-31T18:00:00Z",
            parent_candidate_id="cand-001",
            hypothesis_family="iv_crush",       # SAME family
            claimed_modification_type="new_hypothesis",
        )
        self.assertEqual(
            classify_candidate_change(child, parent), "revival_attempt"
        )


# --------------------------------------------------------------------- #
# Budget gate
# --------------------------------------------------------------------- #


class TestAssertTrialBudgetAvailable(_ProtocolRootBase):
    def _record_n(
        self,
        n: int,
        *,
        family: str = "iv_crush",
        candidate_prefix: str = "iv-crush",
        quarter_offset: int = 0,
    ) -> list[TrialEntry]:
        # Generate N candidates spread across the same calendar quarter.
        results: list[TrialEntry] = []
        for i in range(n):
            month = 4 + (quarter_offset * 3)  # Q2 by default
            day = 1 + i
            ts = f"2026-{month:02d}-{day:02d}T12:00:00Z"
            reg = self._register(
                candidate_id=f"{candidate_prefix}-{i:03d}",
                timestamp=ts,
                hypothesis_family=family,
            )
            results.append(record_trial(reg))
        return results

    def test_unrecorded_candidate_passes(self):
        # No record yet — nothing to gate on.
        assert_trial_budget_available("never-registered")

    def test_first_three_pass(self):
        recorded = self._record_n(3)
        for entry in recorded:
            assert_trial_budget_available(entry.candidate_id)

    def test_fourth_in_same_family_quarter_blocks(self):
        recorded = self._record_n(4)
        for i, entry in enumerate(recorded):
            with self.subTest(idx=i):
                if i < MAX_TRIALS_PER_FAMILY_PER_QUARTER:
                    assert_trial_budget_available(entry.candidate_id)
                else:
                    with self.assertRaises(TrialBudgetViolationError) as ctx:
                        assert_trial_budget_available(entry.candidate_id)
                    self.assertIn("trial budget exceeded", str(ctx.exception))

    def test_different_quarter_resets_budget(self):
        # Q1 has 3, Q2 first candidate is allowed.
        for i in range(3):
            ts = f"2026-01-{i + 1:02d}T12:00:00Z"
            reg = self._register(
                candidate_id=f"q1-{i:03d}",
                timestamp=ts,
                hypothesis_family="iv_crush",
            )
            record_trial(reg)
        reg_q2 = self._register(
            candidate_id="q2-000",
            timestamp="2026-04-01T00:00:00Z",
            hypothesis_family="iv_crush",
        )
        record_trial(reg_q2)
        assert_trial_budget_available("q2-000")

    def test_different_family_resets_budget(self):
        for i in range(3):
            ts = f"2026-04-0{i + 1}T12:00:00Z"
            reg = self._register(
                candidate_id=f"family-a-{i:03d}",
                timestamp=ts, hypothesis_family="family_a",
            )
            record_trial(reg)
        # Different family in same quarter — fresh budget.
        reg_b = self._register(
            candidate_id="family-b-000",
            timestamp="2026-04-04T12:00:00Z",
            hypothesis_family="family_b",
        )
        record_trial(reg_b)
        assert_trial_budget_available("family-b-000")

    def test_summary_q2_with_underscored_family_name(self):
        # Sanity: hypothesis_family pattern allows underscores in the
        # family name; candidate IDs must use hyphens.
        for i in range(2):
            ts = f"2026-04-0{i + 1}T12:00:00Z"
            reg = self._register(
                candidate_id=f"weekend-decay-{i:03d}",
                timestamp=ts, hypothesis_family="weekend_decay",
            )
            record_trial(reg)
        s = summarize_trial_budget(
            "weekend_decay",
            reference_timestamp="2026-04-15T12:00:00Z",
        )
        self.assertEqual(s.trial_count, 2)
        self.assertEqual(s.budget_remaining, 1)

    def test_revival_attempt_blocks(self):
        parent = self._register(
            candidate_id="cand-001",
            timestamp="2026-05-04T18:00:00Z",
            hypothesis_family="iv_crush",
        )
        record_trial(parent)
        record_kill(
            candidate_id=parent.candidate_id,
            registration_hash=parent.registration_hash,
            stage=3, reason="cross-period falsified",
        )
        child = self._register(
            candidate_id="cand-002",
            timestamp="2099-12-31T18:00:00Z",
            parent_candidate_id="cand-001",
            thresholds=[{"name": "t", "kind": "fixed", "value": -0.07}],
        )
        record_trial(child)
        with self.assertRaises(TrialBudgetViolationError) as ctx:
            assert_trial_budget_available("cand-002")
        self.assertIn("revival_attempt", str(ctx.exception))

    def test_pre_registered_alternate_not_blocked(self):
        parent = self._register(
            candidate_id="cand-001",
            timestamp="2025-01-10T10:00:00Z",
        )
        record_trial(parent)
        child = self._register(
            candidate_id="cand-002",
            timestamp="2025-01-10T11:00:00Z",
            parent_candidate_id="cand-001",
            thresholds=[{"name": "t", "kind": "fixed", "value": -0.07}],
        )
        record_trial(child)
        record_kill(
            candidate_id=parent.candidate_id,
            registration_hash=parent.registration_hash,
            stage=3, reason="cross-period falsified",
        )
        # The pre-registered alternate is not a revival.
        assert_trial_budget_available("cand-002")


# --------------------------------------------------------------------- #
# Summary
# --------------------------------------------------------------------- #


class TestSummarizeTrialBudget(_ProtocolRootBase):
    def test_empty_summary(self):
        summary = summarize_trial_budget(
            "iv_crush",
            reference_timestamp="2026-05-04T12:00:00Z",
        )
        self.assertEqual(summary.trial_count, 0)
        self.assertEqual(summary.budget_remaining, MAX_TRIALS_PER_FAMILY_PER_QUARTER)
        self.assertEqual(summary.in_budget_trials, ())
        self.assertEqual(summary.over_budget_trials, ())

    def test_summary_with_three_in_budget(self):
        for i in range(3):
            ts = f"2026-04-0{i + 1}T12:00:00Z"
            reg = self._register(
                candidate_id=f"cand-{i:03d}",
                timestamp=ts, hypothesis_family="iv_crush",
            )
            record_trial(reg)
        summary = summarize_trial_budget(
            "iv_crush", reference_timestamp="2026-04-15T12:00:00Z"
        )
        self.assertEqual(summary.quarter, "2026-Q2")
        self.assertEqual(summary.trial_count, 3)
        self.assertEqual(summary.budget_remaining, 0)
        self.assertEqual(len(summary.in_budget_trials), 3)
        self.assertEqual(summary.over_budget_trials, ())

    def test_summary_with_over_budget_entries(self):
        for i in range(5):
            ts = f"2026-04-0{i + 1}T12:00:00Z"
            reg = self._register(
                candidate_id=f"cand-{i:03d}",
                timestamp=ts, hypothesis_family="iv_crush",
            )
            record_trial(reg)
        summary = summarize_trial_budget(
            "iv_crush", reference_timestamp="2026-04-15T12:00:00Z"
        )
        self.assertEqual(summary.trial_count, 5)
        self.assertEqual(len(summary.in_budget_trials), 3)
        self.assertEqual(len(summary.over_budget_trials), 2)
        self.assertEqual(summary.budget_remaining, 0)


# --------------------------------------------------------------------- #
# Tampering + no-removal API
# --------------------------------------------------------------------- #


class TestTrialStateTampering(_ProtocolRootBase):
    def _write_state(self, payload: dict) -> None:
        path = trial_budget_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_invalid_json_rejected(self):
        path = trial_budget_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not json", encoding="utf-8")
        with self.assertRaises(TrialBudgetViolationError):
            load_trial_state()

    def test_wrong_version_rejected(self):
        self._write_state({"version": 99, "trials": []})
        with self.assertRaises(TrialBudgetViolationError):
            load_trial_state()

    def test_top_level_array_rejected(self):
        path = trial_budget_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("[]", encoding="utf-8")
        with self.assertRaises(TrialBudgetViolationError):
            load_trial_state()

    def test_trials_not_a_list_rejected(self):
        self._write_state({"version": TRIAL_BUDGET_VERSION, "trials": {}})
        with self.assertRaises(TrialBudgetViolationError):
            load_trial_state()

    def test_unknown_modification_type_rejected(self):
        self._write_state({
            "version": TRIAL_BUDGET_VERSION,
            "trials": [{
                "candidate_id": "cand",
                "registration_hash": "a" * 64,
                "created_at": "2026-04-01T12:00:00Z",
                "hypothesis_family": "iv_crush",
                "modification_type": "bogus",
            }],
        })
        with self.assertRaises(TrialBudgetViolationError):
            load_trial_state()

    def test_missing_required_key_rejected(self):
        self._write_state({
            "version": TRIAL_BUDGET_VERSION,
            "trials": [{"candidate_id": "x"}],
        })
        with self.assertRaises(TrialBudgetViolationError):
            load_trial_state()


class TestNoRemovalAPI(unittest.TestCase):
    def test_module_exposes_no_removal_functions(self):
        from services.research_protocol import trial_budget as mod
        for name in (
            "remove",
            "remove_trial",
            "delete",
            "delete_trial",
            "clear",
            "clear_state",
            "reset",
            "reset_state",
            "purge",
            "downgrade",
            "force_record",
            "rewrite",
        ):
            self.assertFalse(
                hasattr(mod, name),
                msg=f"trial_budget must not expose {name}() — trial state is append-only",
            )

    def test_package_exports_have_no_removal_names(self):
        import services.research_protocol as pkg
        for name in pkg.__all__:
            lower = name.lower()
            for forbidden in ("remove", "delete", "clear", "reset", "purge"):
                self.assertNotIn(
                    forbidden, lower,
                    msg=f"package __all__ exposes a {forbidden}-like name: {name}",
                )


if __name__ == "__main__":
    unittest.main()
