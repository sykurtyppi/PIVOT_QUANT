"""Tests for services.research_protocol.validation_ladder (RESEARCH_PROTOCOL §3)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from services.research_protocol._paths import (
    ENV_PROTOCOL_ROOT,
    validation_ladder_state_path,
)
from services.research_protocol.errors import (
    RegistrationMissingError,
    StageGateError,
    ValidationLadderTamperingError,
)
from services.research_protocol.registration import (
    HASH_FIELD,
    compute_registration_hash,
)
from services.research_protocol.statistical_guard import STATISTICAL_VALIDITY_KEY
from services.research_protocol.validation_ladder import (
    STAGE_NAMES,
    STAGES_REQUIRING_STATISTICS,
    VALIDATION_LADDER_VERSION,
    CandidateStageStatus,
    StageResult,
    assert_stage_allowed,
    get_candidate_stage_status,
    load_validation_state,
    record_stage_result,
)


def _valid_payload(candidate_id: str) -> dict:
    payload = {
        "candidate_id": candidate_id,
        "registration_timestamp": "2026-05-04T18:00:00Z",
        "git_commit_sha": "0" * 40,
        "hypothesis": {
            "mechanism": "ladder test mechanism",
            "predicted_direction": "long",
            "why_might_fail": "regime",
            "citations": ["paper:x"],
        },
        "features": [{"name": "f1", "input_columns": ["close"]}],
        "thresholds": [{"name": "t1", "kind": "fixed", "value": 0.7}],
        "transformations": {"allowed": [], "forbidden_unless_listed": ["x"]},
        "forbidden_changes": ["any"],
        "falsification": {"stage_3": "cross_period_validated=false"},
        "datasets": {
            "validation_dataset_pattern": "v.parquet",
            "holdout_dataset_pattern": "h.parquet",
        },
        "horizon_days": 5,
        "random_seed": 42,
        "stages_required": [1, 2, 3, 4, 5, 6],
    }
    payload[HASH_FIELD] = compute_registration_hash(payload)
    return payload


def _write_registration(tmp: Path, payload: dict) -> Path:
    regs = tmp / "registrations"
    regs.mkdir(parents=True, exist_ok=True)
    path = regs / f"{payload['candidate_id']}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def _good_metadata(stage: int = 1, *, stat_pass: bool = True) -> dict:
    """Metadata for record_stage_result tests.

    For stage >= 2, includes a statistical_validity block; with
    stat_pass=True the recomputed verdict is a pass, with stat_pass=False
    the CI includes zero AND the p-value exceeds alpha so the recomputed
    verdict fails (used by the fail-recording tests so that
    passed=False is consistent with the stats).
    """
    md = {
        "run_timestamp": "2026-05-04T19:00:00Z",
        "dataset_identifier": "spy_2025_validation",
    }
    if stage >= 2:
        if stat_pass:
            md["statistical_validity"] = {
                "stage": stage,
                "n_obs": 250,
                "horizon_days": 5,
                "ci_lower": 0.001,
                "ci_upper": 0.008,
                "permutation_p_value": 0.01,
                "permutation_alpha": 0.05,
            }
        else:
            md["statistical_validity"] = {
                "stage": stage,
                "n_obs": 250,
                "horizon_days": 5,
                "ci_lower": -0.002,
                "ci_upper": 0.005,
                "permutation_p_value": 0.10,
                "permutation_alpha": 0.05,
            }
    return md


class _ProtocolRootBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp_ctx = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_ctx.name).resolve()
        self._prev_env = os.environ.get(ENV_PROTOCOL_ROOT)
        os.environ[ENV_PROTOCOL_ROOT] = str(self.tmp)
        # Pre-write a valid registration for the default test candidate.
        self.candidate_id = "ladder-test-001"
        self.payload = _valid_payload(self.candidate_id)
        _write_registration(self.tmp, self.payload)
        self.registration_hash = self.payload[HASH_FIELD]

    def tearDown(self) -> None:
        if self._prev_env is None:
            os.environ.pop(ENV_PROTOCOL_ROOT, None)
        else:
            os.environ[ENV_PROTOCOL_ROOT] = self._prev_env
        self._tmp_ctx.cleanup()


class TestEmptyState(_ProtocolRootBase):
    def test_no_state_file_means_no_recorded_stages(self):
        self.assertFalse(validation_ladder_state_path().exists())
        state = load_validation_state()
        self.assertEqual(state["version"], VALIDATION_LADDER_VERSION)
        self.assertEqual(state["candidates"], {})

    def test_status_for_unrecorded_candidate(self):
        status = get_candidate_stage_status(self.candidate_id)
        self.assertIsInstance(status, CandidateStageStatus)
        self.assertEqual(status.candidate_id, self.candidate_id)
        self.assertIsNone(status.registration_hash)
        self.assertEqual(status.stages, {})
        self.assertEqual(status.highest_passed_stage, 0)
        self.assertFalse(status.has_failure)
        self.assertIsNone(status.blocked_at_stage)


class TestAssertStageAllowed(_ProtocolRootBase):
    def test_stage_0_allowed_with_valid_registration(self):
        assert_stage_allowed(self.candidate_id, 0)

    def test_stage_0_blocked_when_registration_missing(self):
        with self.assertRaises(RegistrationMissingError):
            assert_stage_allowed("never-registered", 0)

    def test_stage_1_allowed_with_valid_registration_and_no_state(self):
        # Stage 0 is implicit; stage 1 has no priors to check.
        assert_stage_allowed(self.candidate_id, 1)

    def test_stage_2_blocked_before_stage_1_recorded(self):
        with self.assertRaises(StageGateError):
            assert_stage_allowed(self.candidate_id, 2)

    def test_stage_2_allowed_after_stage_1_pass(self):
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        assert_stage_allowed(self.candidate_id, 2)

    def test_failed_stage_1_blocks_stage_2(self):
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=False,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        with self.assertRaises(StageGateError) as ctx:
            assert_stage_allowed(self.candidate_id, 2)
        self.assertIn("failed stage 1", str(ctx.exception))

    def test_failed_stage_3_blocks_stage_5(self):
        for stage in (1, 2):
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=stage,
                passed=True,
                report_path=f"reports/stage{stage}.json",
                metadata=_good_metadata(stage=stage),
            )
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=3,
            passed=False,
            report_path="reports/stage3.json",
            metadata=_good_metadata(stage=3, stat_pass=False),
        )
        with self.assertRaises(StageGateError):
            assert_stage_allowed(self.candidate_id, 5)

    def test_cannot_skip_stages(self):
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        # Stage 2 not recorded; stage 3 should be blocked.
        with self.assertRaises(StageGateError) as ctx:
            assert_stage_allowed(self.candidate_id, 3)
        self.assertIn("prior stages", str(ctx.exception).lower())

    def test_invalid_stage_number_rejected(self):
        for bad in (-1, 7, 100):
            with self.subTest(bad=bad):
                with self.assertRaises(StageGateError):
                    assert_stage_allowed(self.candidate_id, bad)

    def test_non_int_stage_rejected(self):
        for bad in ("3", 3.0, None, True):
            with self.subTest(bad=bad):
                with self.assertRaises(StageGateError):
                    assert_stage_allowed(self.candidate_id, bad)  # type: ignore[arg-type]


class TestRecordStageResult(_ProtocolRootBase):
    def test_record_stage_1_pass_creates_entry(self):
        result = record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        self.assertIsInstance(result, StageResult)
        self.assertEqual(result.stage, 1)
        self.assertEqual(result.name, STAGE_NAMES[1])
        self.assertEqual(result.status, "pass")
        self.assertEqual(result.report_path, "reports/stage1.json")
        self.assertEqual(result.registration_hash, self.registration_hash)

    def test_record_persists_to_disk(self):
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        on_disk = json.loads(validation_ladder_state_path().read_text())
        self.assertEqual(on_disk["version"], VALIDATION_LADDER_VERSION)
        self.assertIn(self.candidate_id, on_disk["candidates"])
        body = on_disk["candidates"][self.candidate_id]
        self.assertEqual(body["registration_hash"], self.registration_hash)
        self.assertIn("1", body["stages"])
        self.assertEqual(body["stages"]["1"]["status"], "pass")

    def test_record_is_atomic_no_temp_files_left(self):
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        leftovers = list(self.tmp.glob(".validation_ladder_state.*"))
        self.assertEqual(leftovers, [])

    def test_idempotent_record_with_same_status_and_path(self):
        first = record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        second = record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        self.assertEqual(first.recorded_at, second.recorded_at)
        on_disk = json.loads(validation_ladder_state_path().read_text())
        self.assertEqual(len(on_disk["candidates"][self.candidate_id]["stages"]), 1)

    def test_cannot_overwrite_failed_stage_with_passed(self):
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=False,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        with self.assertRaises(StageGateError) as ctx:
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=1,
                passed=True,
                report_path="reports/stage1.json",
                metadata=_good_metadata(),
            )
        self.assertIn("append-only", str(ctx.exception))

    def test_cannot_overwrite_passed_stage_with_failed(self):
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        with self.assertRaises(StageGateError):
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=1,
                passed=False,
                report_path="reports/stage1.json",
                metadata=_good_metadata(),
            )

    def test_cannot_change_report_path_on_re_record(self):
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        with self.assertRaises(StageGateError):
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=1,
                passed=True,
                report_path="reports/different.json",
                metadata=_good_metadata(),
            )

    def test_record_requires_report_path(self):
        for bad in ("", "   ", None, 123):
            with self.subTest(bad=bad):
                with self.assertRaises(StageGateError):
                    record_stage_result(
                        candidate_id=self.candidate_id,
                        stage=1,
                        passed=True,
                        report_path=bad,  # type: ignore[arg-type]
                        metadata=_good_metadata(),
                    )

    def test_record_requires_run_timestamp_in_metadata(self):
        bad = {"dataset_identifier": "ds"}
        with self.assertRaises(StageGateError) as ctx:
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=1,
                passed=True,
                report_path="reports/stage1.json",
                metadata=bad,
            )
        self.assertIn("run_timestamp", str(ctx.exception))

    def test_record_requires_dataset_identifier_in_metadata(self):
        bad = {"run_timestamp": "2026-05-04T19:00:00Z"}
        with self.assertRaises(StageGateError) as ctx:
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=1,
                passed=True,
                report_path="reports/stage1.json",
                metadata=bad,
            )
        self.assertIn("dataset_identifier", str(ctx.exception))

    def test_record_rejects_empty_metadata_values(self):
        for value in ("", "   "):
            bad = {"run_timestamp": value, "dataset_identifier": "ds"}
            with self.subTest(value=value):
                with self.assertRaises(StageGateError):
                    record_stage_result(
                        candidate_id=self.candidate_id,
                        stage=1,
                        passed=True,
                        report_path="reports/stage1.json",
                        metadata=bad,
                    )

    def test_record_stage_0_rejected(self):
        with self.assertRaises(StageGateError):
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=0,
                passed=True,
                report_path="reports/registration.json",
                metadata=_good_metadata(),
            )

    def test_record_stage_out_of_range_rejected(self):
        for bad in (-1, 7, 100):
            with self.subTest(bad=bad):
                with self.assertRaises(StageGateError):
                    record_stage_result(
                        candidate_id=self.candidate_id,
                        stage=bad,
                        passed=True,
                        report_path="reports/x.json",
                        metadata=_good_metadata(),
                    )

    def test_record_stage_n_blocked_when_priors_missing(self):
        with self.assertRaises(StageGateError):
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=3,                # skipping 1 and 2
                passed=True,
                report_path="reports/stage3.json",
                metadata=_good_metadata(),
            )

    def test_registration_hash_change_detected_across_records(self):
        # First record fixes the candidate's hash.
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        # Tamper the on-disk state to simulate someone re-registering with a
        # different hash for the same candidate_id.
        state_path = validation_ladder_state_path()
        state = json.loads(state_path.read_text())
        state["candidates"][self.candidate_id]["registration_hash"] = "f" * 64
        state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        with self.assertRaises(StageGateError) as ctx:
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=2,
                passed=True,
                report_path="reports/stage2.json",
                metadata=_good_metadata(),
            )
        self.assertIn("registration changed", str(ctx.exception))

    def test_explicit_registration_hash_override_must_match(self):
        with self.assertRaises(StageGateError):
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=1,
                passed=True,
                report_path="reports/stage1.json",
                metadata=_good_metadata(),
                registration_hash="b" * 64,  # wrong
            )

    def test_record_blocked_when_registration_missing(self):
        with self.assertRaises(RegistrationMissingError):
            record_stage_result(
                candidate_id="never-registered",
                stage=1,
                passed=True,
                report_path="reports/stage1.json",
                metadata=_good_metadata(),
            )


class TestGetCandidateStageStatus(_ProtocolRootBase):
    def test_after_one_pass(self):
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        status = get_candidate_stage_status(self.candidate_id)
        self.assertEqual(status.registration_hash, self.registration_hash)
        self.assertEqual(set(status.stages.keys()), {1})
        self.assertEqual(status.highest_passed_stage, 1)
        self.assertFalse(status.has_failure)
        self.assertIsNone(status.blocked_at_stage)

    def test_after_pass_then_fail(self):
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=2,
            passed=True,
            report_path="reports/stage2.json",
            metadata=_good_metadata(stage=2),
        )
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=3,
            passed=False,
            report_path="reports/stage3.json",
            metadata=_good_metadata(stage=3, stat_pass=False),
        )
        status = get_candidate_stage_status(self.candidate_id)
        self.assertEqual(status.highest_passed_stage, 2)
        self.assertTrue(status.has_failure)
        self.assertEqual(status.blocked_at_stage, 3)


class TestStateTampering(_ProtocolRootBase):
    def _write_state(self, payload: dict) -> None:
        path = validation_ladder_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_invalid_json_rejected(self):
        path = validation_ladder_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValidationLadderTamperingError):
            load_validation_state()

    def test_wrong_version_rejected(self):
        self._write_state({"version": 99, "candidates": {}})
        with self.assertRaises(ValidationLadderTamperingError):
            load_validation_state()

    def test_top_level_array_rejected(self):
        path = validation_ladder_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("[]", encoding="utf-8")
        with self.assertRaises(ValidationLadderTamperingError):
            load_validation_state()

    def test_candidates_not_a_dict_rejected(self):
        self._write_state({"version": VALIDATION_LADDER_VERSION, "candidates": []})
        with self.assertRaises(ValidationLadderTamperingError):
            load_validation_state()

    def test_unknown_stage_in_candidate_rejected(self):
        self._write_state({
            "version": VALIDATION_LADDER_VERSION,
            "candidates": {
                self.candidate_id: {
                    "registration_hash": self.registration_hash,
                    "stages": {
                        "99": {
                            "stage": 99,
                            "name": "stage_99_doom",
                            "status": "pass",
                            "report_path": "x",
                            "metadata": _good_metadata(),
                            "recorded_at": "2026-05-04T19:00:00Z",
                        },
                    },
                },
            },
        })
        with self.assertRaises(ValidationLadderTamperingError):
            load_validation_state()

    def test_invalid_status_value_rejected(self):
        self._write_state({
            "version": VALIDATION_LADDER_VERSION,
            "candidates": {
                self.candidate_id: {
                    "registration_hash": self.registration_hash,
                    "stages": {
                        "1": {
                            "stage": 1,
                            "name": "stage_1_in_sample_sanity",
                            "status": "maybe",
                            "report_path": "x",
                            "metadata": _good_metadata(),
                            "recorded_at": "2026-05-04T19:00:00Z",
                        },
                    },
                },
            },
        })
        with self.assertRaises(ValidationLadderTamperingError):
            load_validation_state()

    def test_missing_registration_hash_rejected(self):
        self._write_state({
            "version": VALIDATION_LADDER_VERSION,
            "candidates": {self.candidate_id: {"stages": {}}},
        })
        with self.assertRaises(ValidationLadderTamperingError):
            load_validation_state()


class TestStatisticalBlockEnforcement(_ProtocolRootBase):
    """Stage 2+ requires a statistical_validity block; verdict is recomputed."""

    def _record_stage_1(self) -> None:
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )

    def test_stage_2_record_without_stat_block_rejected(self):
        self._record_stage_1()
        with self.assertRaises(StageGateError) as ctx:
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=2,
                passed=True,
                report_path="reports/stage2.json",
                metadata={
                    "run_timestamp": "2026-05-04T19:00:00Z",
                    "dataset_identifier": "spy_2025_validation",
                },  # no statistical_validity
            )
        self.assertIn(STATISTICAL_VALIDITY_KEY, str(ctx.exception))

    def test_stage_1_record_without_stat_block_succeeds(self):
        # Stage 1 is implementation sanity; statistics are not required.
        self.assertNotIn(1, STAGES_REQUIRING_STATISTICS)
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=_good_metadata(),
        )

    def test_stage_2_record_with_passing_stats(self):
        self._record_stage_1()
        result = record_stage_result(
            candidate_id=self.candidate_id,
            stage=2,
            passed=True,
            report_path="reports/stage2.json",
            metadata=_good_metadata(stage=2),
        )
        self.assertEqual(result.status, "pass")
        self.assertIn(STATISTICAL_VALIDITY_KEY, result.metadata)
        self.assertTrue(result.metadata[STATISTICAL_VALIDITY_KEY]["statistical_pass"])

    def test_stage_2_passed_true_with_failing_stats_rejected(self):
        self._record_stage_1()
        with self.assertRaises(StageGateError) as ctx:
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=2,
                passed=True,
                report_path="reports/stage2.json",
                metadata=_good_metadata(stage=2, stat_pass=False),
            )
        self.assertIn("statistical verdict", str(ctx.exception))

    def test_stage_2_passed_false_with_failing_stats_succeeds(self):
        """Recording a failure when the stats fail is the legitimate path."""
        self._record_stage_1()
        result = record_stage_result(
            candidate_id=self.candidate_id,
            stage=2,
            passed=False,
            report_path="reports/stage2.json",
            metadata=_good_metadata(stage=2, stat_pass=False),
        )
        self.assertEqual(result.status, "fail")
        self.assertFalse(result.metadata[STATISTICAL_VALIDITY_KEY]["statistical_pass"])

    def test_recorded_block_overrides_user_claim(self):
        """If the user submits a tampered block claiming statistical_pass=True
        while the inputs would yield False, recording is rejected."""
        self._record_stage_1()
        bad_metadata = _good_metadata(stage=2)
        # Flip the inputs to make stats fail, but claim pass.
        bad_metadata[STATISTICAL_VALIDITY_KEY] = {
            "stage": 2,
            "n_obs": 10,            # n_eff = 2, below floor 30
            "horizon_days": 5,
            "ci_lower": 0.01,
            "ci_upper": 0.02,
            "permutation_p_value": 0.001,
            "permutation_alpha": 0.05,
            "statistical_pass": True,        # lie
            "metrics_suppressed": False,     # lie
            "suppression_reasons": [],       # lie
        }
        with self.assertRaises(StageGateError) as ctx:
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=2,
                passed=True,
                report_path="reports/stage2.json",
                metadata=bad_metadata,
            )
        # Either disagreement message or post-recompute pass-fail message.
        msg = str(ctx.exception)
        self.assertTrue(
            "disagrees" in msg or "statistical verdict" in msg,
            msg=f"unexpected error message: {msg}",
        )

    def test_stage_block_must_match_recorded_stage(self):
        self._record_stage_1()
        bad = _good_metadata(stage=2)
        bad[STATISTICAL_VALIDITY_KEY]["stage"] = 5  # mismatch
        with self.assertRaises(StageGateError) as ctx:
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=2,
                passed=True,
                report_path="reports/stage2.json",
                metadata=bad,
            )
        self.assertIn("does not match recorded stage", str(ctx.exception))

    def test_persisted_block_is_canonical(self):
        """The stored statistical_validity block reflects the recomputed
        verdict, not whatever the user supplied."""
        self._record_stage_1()
        # User supplies inputs but no claimed pass/suppressed fields.
        md = _good_metadata(stage=2)
        record_stage_result(
            candidate_id=self.candidate_id,
            stage=2,
            passed=True,
            report_path="reports/stage2.json",
            metadata=md,
        )
        status = get_candidate_stage_status(self.candidate_id)
        block = status.stages[2].metadata[STATISTICAL_VALIDITY_KEY]
        # Recomputed fields must be present, even though we didn't pass them.
        self.assertEqual(block["statistical_pass"], True)
        self.assertEqual(block["metrics_suppressed"], False)
        self.assertEqual(block["n_eff"], 50)
        self.assertEqual(block["n_eff_floor"], 30)


class TestNoDowngradeAPI(unittest.TestCase):
    """The module surface must not expose any downgrade/removal function."""

    def test_no_removal_or_downgrade_functions(self):
        from services.research_protocol import validation_ladder as mod
        for name in (
            "remove",
            "remove_stage",
            "remove_stage_result",
            "delete",
            "delete_stage",
            "delete_stage_result",
            "clear",
            "clear_state",
            "reset",
            "reset_state",
            "downgrade",
            "downgrade_stage",
            "overwrite_stage",
            "force_record",
        ):
            self.assertFalse(
                hasattr(mod, name),
                msg=f"validation_ladder must not expose {name}() —"
                " ladder state is append-only.",
            )

    def test_package_exports_have_no_downgrade_names(self):
        import services.research_protocol as pkg
        for name in pkg.__all__:
            lower = name.lower()
            for forbidden in ("remove", "delete", "downgrade", "reset", "clear"):
                self.assertNotIn(
                    forbidden, lower,
                    msg=f"package __all__ exposes a {forbidden}-like name: {name}",
                )


if __name__ == "__main__":
    unittest.main()
