"""End-to-end tests for the single guard entry-point."""

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
from services.research_protocol.audit_logger import load_audit_events
from services.research_protocol.errors import (
    CandidateKilledError,
    RegistrationHashMismatchError,
    RegistrationMissingError,
    ReplicationViolationError,
    StageGateError,
    StatisticalViolationError,
    TrialBudgetViolationError,
)
from services.research_protocol.kill_list import record_kill
from services.research_protocol.protocol_guard import assert_protocol_compliant
from services.research_protocol.registration import (
    HASH_FIELD,
    Registration,
    compute_registration_hash,
)
from services.research_protocol.replication_guard import (
    record_cross_symbol_exemption,
    record_replication_result,
)
from services.research_protocol.statistical_guard import STATISTICAL_VALIDITY_KEY
from services.research_protocol.trial_budget import MAX_TRIALS_PER_FAMILY_PER_QUARTER
from services.research_protocol.validation_ladder import record_stage_result


def _valid_payload(candidate_id: str = "guard-test-001") -> dict:
    payload = {
        "candidate_id": candidate_id,
        "registration_timestamp": "2026-05-04T18:00:00Z",
        "git_commit_sha": "0" * 40,
        "hypothesis": {
            "mechanism": "post-FOMC dealer hedging compresses near-dated IV",
            "predicted_direction": "short",
            "why_might_fail": "regime-conditional liquidity",
            "citations": ["paper:fomc-iv"],
        },
        "features": [{"name": "iv_change_5d", "input_columns": ["iv"]}],
        "thresholds": [{"name": "iv_drop", "kind": "fixed", "value": -0.05}],
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


class TestProtocolGuard(_ProtocolRootBase):
    def test_missing_registration_blocks_run(self):
        with self.assertRaises(RegistrationMissingError):
            assert_protocol_compliant("never-registered")

    def test_hash_mismatch_blocks_run(self):
        payload = _valid_payload()
        path = _write_registration(self.tmp, payload)
        # Tamper with the file but keep the (now-stale) hash field.
        on_disk = json.loads(path.read_text())
        on_disk["random_seed"] = on_disk["random_seed"] + 1
        path.write_text(json.dumps(on_disk, indent=2), encoding="utf-8")
        with self.assertRaises(RegistrationHashMismatchError):
            assert_protocol_compliant(payload["candidate_id"])

    def test_killed_candidate_blocks_run(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        record_kill(
            candidate_id=payload["candidate_id"],
            registration_hash=payload[HASH_FIELD],
            stage=3,
            reason="cross-period falsified on synthetic test",
        )
        with self.assertRaises(CandidateKilledError):
            assert_protocol_compliant(payload["candidate_id"])

    def test_valid_path_returns_registration(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        reg = assert_protocol_compliant(payload["candidate_id"])
        self.assertIsInstance(reg, Registration)
        self.assertEqual(reg.candidate_id, payload["candidate_id"])
        self.assertEqual(reg.registration_hash, payload[HASH_FIELD])

    def test_check_order_registration_first_then_kill_list(self):
        """If a candidate has no registration AND is on the kill list, the
        missing-registration error should fire first — protocol_guard checks
        existence before kill-list membership so a researcher who deletes
        their registration cannot accidentally bypass the kill check by
        forging a candidate_id."""
        record_kill(
            candidate_id="phantom-cand",
            registration_hash="b" * 64,
            stage=3,
            reason="killed before any registration ever existed",
        )
        with self.assertRaises(RegistrationMissingError):
            assert_protocol_compliant("phantom-cand")


class TestProtocolGuardLadderIntegration(_ProtocolRootBase):
    """assert_protocol_compliant + requested_stage exercises the ladder gate."""

    def _good_metadata(self) -> dict:
        return {
            "run_timestamp": "2026-05-04T19:00:00Z",
            "dataset_identifier": "spy_2025_validation",
        }

    def test_no_requested_stage_skips_ladder_check(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        # Stage 5 would fail ladder gating, but no requested_stage means
        # the ladder check is not performed.
        reg = assert_protocol_compliant(payload["candidate_id"])
        self.assertEqual(reg.candidate_id, payload["candidate_id"])

    def test_stage_0_passes_with_valid_registration(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        reg = assert_protocol_compliant(
            payload["candidate_id"], requested_stage=0
        )
        self.assertEqual(reg.candidate_id, payload["candidate_id"])

    def test_stage_2_blocked_when_stage_1_not_recorded(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        with self.assertRaises(StageGateError):
            assert_protocol_compliant(
                payload["candidate_id"], requested_stage=2
            )

    def test_stage_2_allowed_after_stage_1_passed(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        record_stage_result(
            candidate_id=payload["candidate_id"],
            stage=1,
            passed=True,
            report_path="reports/stage1.json",
            metadata=self._good_metadata(),
        )
        reg = assert_protocol_compliant(
            payload["candidate_id"], requested_stage=2
        )
        self.assertEqual(reg.candidate_id, payload["candidate_id"])

    def test_kill_list_blocks_before_ladder_check(self):
        """A killed candidate is rejected before any ladder gating, so a
        researcher cannot diagnose ladder state by probing requested_stage."""
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        record_kill(
            candidate_id=payload["candidate_id"],
            registration_hash=payload[HASH_FIELD],
            stage=3,
            reason="cross-period falsified",
        )
        with self.assertRaises(CandidateKilledError):
            assert_protocol_compliant(
                payload["candidate_id"], requested_stage=2
            )

    def test_missing_registration_blocks_before_ladder_check(self):
        with self.assertRaises(RegistrationMissingError):
            assert_protocol_compliant(
                "never-registered", requested_stage=2
            )

    def test_hash_mismatch_blocks_before_ladder_check(self):
        payload = _valid_payload()
        path = _write_registration(self.tmp, payload)
        on_disk = json.loads(path.read_text())
        on_disk["random_seed"] = on_disk["random_seed"] + 1
        path.write_text(json.dumps(on_disk, indent=2), encoding="utf-8")
        with self.assertRaises(RegistrationHashMismatchError):
            assert_protocol_compliant(
                payload["candidate_id"], requested_stage=1
            )


class TestProtocolGuardStatisticalDefensiveCheck(_ProtocolRootBase):
    """assert_protocol_compliant recomputes prior stages' stat verdicts."""

    def _stat_block(self, stage: int, *, stat_pass: bool) -> dict:
        if stat_pass:
            return {
                "stage": stage, "n_obs": 250, "horizon_days": 5,
                "ci_lower": 0.001, "ci_upper": 0.008,
                "permutation_p_value": 0.01, "permutation_alpha": 0.05,
            }
        return {
            "stage": stage, "n_obs": 250, "horizon_days": 5,
            "ci_lower": -0.002, "ci_upper": 0.005,
            "permutation_p_value": 0.10, "permutation_alpha": 0.05,
        }

    def _good_metadata(self, stage: int = 1, *, stat_pass: bool = True) -> dict:
        md = {
            "run_timestamp": "2026-05-04T19:00:00Z",
            "dataset_identifier": "spy_2025_validation",
        }
        if stage >= 2:
            md[STATISTICAL_VALIDITY_KEY] = self._stat_block(stage, stat_pass=stat_pass)
        return md

    def _set_up_candidate(self) -> dict:
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        return payload

    def _record_clean_pipeline_through_stage(self, payload: dict, *, up_to: int) -> None:
        """Record stages 1..up_to, all passing, with valid stat blocks."""
        for stage in range(1, up_to + 1):
            record_stage_result(
                candidate_id=payload["candidate_id"],
                stage=stage,
                passed=True,
                report_path=f"reports/stage{stage}.json",
                metadata=self._good_metadata(stage=stage),
            )

    def test_stage_3_allowed_when_stage_2_stats_pass(self):
        payload = self._set_up_candidate()
        self._record_clean_pipeline_through_stage(payload, up_to=2)
        reg = assert_protocol_compliant(
            payload["candidate_id"], requested_stage=3
        )
        self.assertEqual(reg.candidate_id, payload["candidate_id"])

    def test_stage_3_blocked_when_stage_2_stats_tampered(self):
        """A tamperer flips stage 2's statistical_pass to True while the
        underlying inputs remain failing. The guard recomputes and blocks."""
        payload = self._set_up_candidate()
        # Set up stages 1 (no stats) + 2 (stats pass) cleanly.
        self._record_clean_pipeline_through_stage(payload, up_to=2)
        # Now tamper the on-disk state: replace stage 2's stat block with
        # one whose inputs fail, but whose claimed flags say pass.
        path = validation_ladder_state_path()
        state = json.loads(path.read_text())
        state["candidates"][payload["candidate_id"]]["stages"]["2"]["metadata"][
            STATISTICAL_VALIDITY_KEY
        ] = {
            "stage": 2,
            "n_obs": 10,                # n_eff = 2 (below floor)
            "horizon_days": 5,
            "ci_lower": 0.01,
            "ci_upper": 0.02,
            "permutation_p_value": 0.001,
            "permutation_alpha": 0.05,
            "n_eff": 2,
            "n_eff_floor": 30,
            "statistical_pass": True,    # lie
            "metrics_suppressed": False, # lie
            "suppression_reasons": [],
        }
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        with self.assertRaises(StatisticalViolationError) as ctx:
            assert_protocol_compliant(
                payload["candidate_id"], requested_stage=3
            )
        self.assertIn("statistical_pass=False", str(ctx.exception))

    def test_stage_3_blocked_when_stage_2_stat_block_missing(self):
        """If somebody removes stage 2's stat block from the state file,
        the defensive check raises rather than letting stage 3 proceed."""
        payload = self._set_up_candidate()
        self._record_clean_pipeline_through_stage(payload, up_to=2)
        path = validation_ladder_state_path()
        state = json.loads(path.read_text())
        del state["candidates"][payload["candidate_id"]]["stages"]["2"][
            "metadata"
        ][STATISTICAL_VALIDITY_KEY]
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        with self.assertRaises(StatisticalViolationError):
            assert_protocol_compliant(
                payload["candidate_id"], requested_stage=3
            )

    def test_enforce_statistical_validity_false_skips_recompute(self):
        """The defensive check can be disabled by callers that need to
        inspect ladder state without re-validating."""
        payload = self._set_up_candidate()
        self._record_clean_pipeline_through_stage(payload, up_to=2)
        # Tamper as in the previous test
        path = validation_ladder_state_path()
        state = json.loads(path.read_text())
        del state["candidates"][payload["candidate_id"]]["stages"]["2"][
            "metadata"
        ][STATISTICAL_VALIDITY_KEY]
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        # With enforcement off, ladder check still passes (stage 2 status=pass)
        # so the call returns successfully.
        reg = assert_protocol_compliant(
            payload["candidate_id"],
            requested_stage=3,
            enforce_statistical_validity=False,
        )
        self.assertEqual(reg.candidate_id, payload["candidate_id"])

    def test_no_requested_stage_skips_statistical_check(self):
        """When requested_stage is None, the defensive recompute does not
        run regardless of enforce flag."""
        payload = self._set_up_candidate()
        self._record_clean_pipeline_through_stage(payload, up_to=2)
        # tamper so that recompute would fail if it ran
        path = validation_ladder_state_path()
        state = json.loads(path.read_text())
        del state["candidates"][payload["candidate_id"]]["stages"]["2"][
            "metadata"
        ][STATISTICAL_VALIDITY_KEY]
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        # No requested_stage → no ladder/stat check.
        reg = assert_protocol_compliant(payload["candidate_id"])
        self.assertEqual(reg.candidate_id, payload["candidate_id"])


class TestProtocolGuardReplicationCheck(_ProtocolRootBase):
    """assert_protocol_compliant(requested_stage=6) gates on replication."""

    def _passing_stat_block(self, stage: int = 5) -> dict:
        # Per-stage n_obs sized to clear the n_eff floor (5: 60, 6: 100).
        floor_n_obs = {2: 250, 3: 250, 4: 250, 5: 350, 6: 600}.get(stage, 250)
        return {
            "stage": stage,
            "n_obs": floor_n_obs,
            "horizon_days": 5,
            "ci_lower": 0.001,
            "ci_upper": 0.008,
            "permutation_p_value": 0.01,
            "permutation_alpha": 0.05,
        }

    def _good_metadata(self, stage: int = 1) -> dict:
        md = {
            "run_timestamp": "2026-05-04T19:00:00Z",
            "dataset_identifier": "spy_2025_validation",
        }
        if stage >= 2:
            md[STATISTICAL_VALIDITY_KEY] = self._passing_stat_block(stage=stage)
        return md

    def _set_up_through_stage_5(self, payload: dict) -> None:
        for stage in range(1, 6):
            record_stage_result(
                candidate_id=payload["candidate_id"],
                stage=stage, passed=True,
                report_path=f"reports/stage{stage}.json",
                metadata=self._good_metadata(stage=stage),
            )

    def _record_passing_evidence(
        self, candidate_id: str, *,
        period_id: str, symbol: str,
        train_start: str = "2023-01-03",
        train_end: str = "2024-12-31",
        test_start: str = "2025-01-02",
        test_end: str = "2025-12-31",
    ) -> None:
        record_replication_result(
            candidate_id=candidate_id,
            period_id=period_id,
            train_start=train_start, train_end=train_end,
            test_start=test_start, test_end=test_end,
            symbol=symbol,
            report_path=f"reports/{period_id}_{symbol}.json",
            statistical_validity=self._passing_stat_block(stage=3),
        )

    def test_stage_6_blocked_when_no_replication(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        self._set_up_through_stage_5(payload)
        with self.assertRaises(ReplicationViolationError):
            assert_protocol_compliant(
                payload["candidate_id"], requested_stage=6
            )

    def test_stage_6_blocked_with_one_period_one_symbol(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        self._set_up_through_stage_5(payload)
        self._record_passing_evidence(
            payload["candidate_id"], period_id="p2025", symbol="SPY",
        )
        with self.assertRaises(ReplicationViolationError):
            assert_protocol_compliant(
                payload["candidate_id"], requested_stage=6
            )

    def test_stage_6_allowed_with_two_periods_two_symbols(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        self._set_up_through_stage_5(payload)
        self._record_passing_evidence(
            payload["candidate_id"], period_id="p2025", symbol="SPY",
        )
        self._record_passing_evidence(
            payload["candidate_id"], period_id="p2025", symbol="QQQ",
        )
        self._record_passing_evidence(
            payload["candidate_id"], period_id="p2022", symbol="SPY",
            train_start="2020-01-02", train_end="2021-12-31",
            test_start="2022-01-03", test_end="2022-12-30",
        )
        reg = assert_protocol_compliant(
            payload["candidate_id"], requested_stage=6
        )
        self.assertEqual(reg.candidate_id, payload["candidate_id"])

    def test_stage_6_allowed_with_two_periods_one_symbol_plus_exemption(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        self._set_up_through_stage_5(payload)
        self._record_passing_evidence(
            payload["candidate_id"], period_id="p2025", symbol="SPY",
        )
        self._record_passing_evidence(
            payload["candidate_id"], period_id="p2022", symbol="SPY",
            train_start="2020-01-02", train_end="2021-12-31",
            test_start="2022-01-03", test_end="2022-12-30",
        )
        record_cross_symbol_exemption(
            candidate_id=payload["candidate_id"],
            reason="iVolatility coverage limited to SPY for the windows under test",
        )
        reg = assert_protocol_compliant(
            payload["candidate_id"], requested_stage=6
        )
        self.assertEqual(reg.candidate_id, payload["candidate_id"])

    def test_stage_5_does_not_check_replication(self):
        """Replication is required only at stage 6; stage 5 must be
        runnable without any replication evidence yet."""
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        for stage in range(1, 5):
            record_stage_result(
                candidate_id=payload["candidate_id"],
                stage=stage, passed=True,
                report_path=f"reports/stage{stage}.json",
                metadata=self._good_metadata(stage=stage),
            )
        # No replication evidence recorded; stage 5 still allowed.
        reg = assert_protocol_compliant(
            payload["candidate_id"], requested_stage=5
        )
        self.assertEqual(reg.candidate_id, payload["candidate_id"])

    def test_enforce_replication_false_skips_check(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        self._set_up_through_stage_5(payload)
        # No replication; with enforce_replication=False, stage 6 passes.
        reg = assert_protocol_compliant(
            payload["candidate_id"],
            requested_stage=6,
            enforce_replication=False,
        )
        self.assertEqual(reg.candidate_id, payload["candidate_id"])

    def test_kill_list_blocks_before_replication_check(self):
        """A killed candidate at stage 6 returns CandidateKilledError, not
        a ReplicationViolationError, so the kill order is preserved."""
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        self._set_up_through_stage_5(payload)
        record_kill(
            candidate_id=payload["candidate_id"],
            registration_hash=payload[HASH_FIELD],
            stage=3,
            reason="cross-period falsified",
        )
        with self.assertRaises(CandidateKilledError):
            assert_protocol_compliant(
                payload["candidate_id"], requested_stage=6
            )


class TestProtocolGuardAuditEmits(_ProtocolRootBase):
    """Every protocol-guard decision path must emit a matching audit event."""

    def _passing_stat_block(self, stage: int = 5) -> dict:
        floor_n_obs = {2: 250, 3: 250, 4: 250, 5: 350, 6: 600}.get(stage, 250)
        return {
            "stage": stage,
            "n_obs": floor_n_obs,
            "horizon_days": 5,
            "ci_lower": 0.001,
            "ci_upper": 0.008,
            "permutation_p_value": 0.01,
            "permutation_alpha": 0.05,
        }

    def _good_metadata(self, stage: int = 1) -> dict:
        md = {
            "run_timestamp": "2026-05-04T19:00:00Z",
            "dataset_identifier": "spy_2025_validation",
        }
        if stage >= 2:
            md[STATISTICAL_VALIDITY_KEY] = self._passing_stat_block(stage=stage)
        return md

    def test_missing_registration_emits_registration_rejected(self):
        with self.assertRaises(RegistrationMissingError):
            assert_protocol_compliant("never-registered")
        events = load_audit_events()
        self.assertEqual(
            [e.event_type for e in events],
            ["registration_rejected"],
        )
        self.assertEqual(events[0].decision, "block")

    def test_killed_candidate_emits_kill_list_block(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        record_kill(
            candidate_id=payload["candidate_id"],
            registration_hash=payload[HASH_FIELD],
            stage=3,
            reason="cross-period falsified",
        )
        with self.assertRaises(CandidateKilledError):
            assert_protocol_compliant(payload["candidate_id"])
        block_events = load_audit_events(event_type="kill_list_block")
        self.assertEqual(len(block_events), 1)
        self.assertEqual(block_events[0].decision, "block")
        self.assertEqual(
            block_events[0].registration_hash, payload[HASH_FIELD]
        )

    def test_ladder_block_emits_ladder_block_event(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        # No stage 1 recorded → stage 2 blocked
        with self.assertRaises(StageGateError):
            assert_protocol_compliant(
                payload["candidate_id"], requested_stage=2
            )
        block_events = load_audit_events(event_type="ladder_block")
        self.assertEqual(len(block_events), 1)
        self.assertEqual(block_events[0].protocol_stage, 2)

    def test_protocol_pass_emits_protocol_pass(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        assert_protocol_compliant(payload["candidate_id"])
        passes = load_audit_events(event_type="protocol_pass")
        self.assertEqual(len(passes), 1)
        self.assertEqual(passes[0].decision, "pass")
        self.assertEqual(passes[0].candidate_id, payload["candidate_id"])

    def test_replication_block_emits_replication_block(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        for stage in range(1, 6):
            record_stage_result(
                candidate_id=payload["candidate_id"],
                stage=stage, passed=True,
                report_path=f"reports/stage{stage}.json",
                metadata=self._good_metadata(stage=stage),
            )
        with self.assertRaises(ReplicationViolationError):
            assert_protocol_compliant(
                payload["candidate_id"], requested_stage=6
            )
        block_events = load_audit_events(event_type="replication_block")
        self.assertEqual(len(block_events), 1)
        self.assertEqual(block_events[0].protocol_stage, 6)


class TestRecordEntryPointsEmitAuditEvents(_ProtocolRootBase):
    """record_kill / record_stage_result / record_replication_result emit."""

    def _passing_stat_block(self, stage: int = 3) -> dict:
        return {
            "stage": stage,
            "n_obs": 250,
            "horizon_days": 5,
            "ci_lower": 0.001,
            "ci_upper": 0.008,
            "permutation_p_value": 0.01,
            "permutation_alpha": 0.05,
        }

    def _good_metadata(self, stage: int = 1) -> dict:
        md = {
            "run_timestamp": "2026-05-04T19:00:00Z",
            "dataset_identifier": "spy_2025_validation",
        }
        if stage >= 2:
            md[STATISTICAL_VALIDITY_KEY] = self._passing_stat_block(stage=stage)
        return md

    def test_record_kill_emits_candidate_killed(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        record_kill(
            candidate_id=payload["candidate_id"],
            registration_hash=payload[HASH_FIELD],
            stage=3,
            reason="cross-period falsified",
        )
        events = load_audit_events(event_type="candidate_killed")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].decision, "record")
        self.assertEqual(events[0].protocol_stage, 3)
        self.assertEqual(events[0].registration_hash, payload[HASH_FIELD])

    def test_record_stage_result_emits_stage_result_recorded(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        record_stage_result(
            candidate_id=payload["candidate_id"],
            stage=1, passed=True,
            report_path="reports/stage1.json",
            metadata=self._good_metadata(stage=1),
        )
        events = load_audit_events(event_type="stage_result_recorded")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].protocol_stage, 1)
        self.assertEqual(events[0].decision, "record")
        self.assertEqual(events[0].metadata["status"], "pass")

    def test_record_stage_2_pass_includes_statistical_metadata(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        record_stage_result(
            candidate_id=payload["candidate_id"],
            stage=1, passed=True,
            report_path="reports/stage1.json",
            metadata=self._good_metadata(stage=1),
        )
        record_stage_result(
            candidate_id=payload["candidate_id"],
            stage=2, passed=True,
            report_path="reports/stage2.json",
            metadata=self._good_metadata(stage=2),
        )
        events = load_audit_events(event_type="stage_result_recorded")
        stage_2 = [e for e in events if e.protocol_stage == 2]
        self.assertEqual(len(stage_2), 1)
        self.assertTrue(stage_2[0].metadata["statistical_pass"])
        self.assertEqual(stage_2[0].metadata["n_eff"], 50)

    def test_record_replication_result_emits_replication_evidence_recorded(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        record_replication_result(
            candidate_id=payload["candidate_id"],
            period_id="p2025",
            train_start="2023-01-03", train_end="2024-12-31",
            test_start="2025-01-02", test_end="2025-12-31",
            symbol="SPY",
            report_path="reports/p2025_SPY.json",
            statistical_validity=self._passing_stat_block(stage=3),
        )
        events = load_audit_events(event_type="replication_evidence_recorded")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].decision, "record")
        self.assertEqual(events[0].metadata["period_id"], "p2025")
        self.assertEqual(events[0].metadata["symbol"], "SPY")
        self.assertTrue(events[0].metadata["statistical_pass"])


class TestProtocolGuardTrialBudget(_ProtocolRootBase):
    """assert_protocol_compliant fires the trial gate after registration."""

    def _payload(self, candidate_id: str, *, hypothesis_family: str = "iv_crush",
                 timestamp: str = "2026-04-01T12:00:00Z",
                 parent_candidate_id: str | None = None,
                 claimed_modification_type: str | None = None) -> dict:
        from services.research_protocol.registration import compute_registration_hash, HASH_FIELD as _HF
        body = {
            "candidate_id": candidate_id,
            "registration_timestamp": timestamp,
            "git_commit_sha": "0" * 40,
            "hypothesis": {
                "mechanism": "guard test mechanism",
                "predicted_direction": "long",
                "why_might_fail": "regime",
                "citations": [],
            },
            "features": [{"name": "f1", "input_columns": ["close"]}],
            "thresholds": [{"name": "t1", "kind": "fixed", "value": 0.5}],
            "transformations": {"allowed": [], "forbidden_unless_listed": ["x"]},
            "forbidden_changes": ["any"],
            "falsification": {"stage_3": "x"},
            "datasets": {
                "symbol": "SPY",
                "validation_dataset_pattern": "v.parquet",
                "holdout_dataset_pattern": "h.parquet",
            },
            "horizon_days": 5,
            "random_seed": 42,
            "stages_required": [1, 2, 3, 4, 5, 6],
            "hypothesis_family": hypothesis_family,
        }
        if parent_candidate_id is not None:
            body["parent_candidate_id"] = parent_candidate_id
        if claimed_modification_type is not None:
            body["claimed_modification_type"] = claimed_modification_type
        body[_HF] = compute_registration_hash(body)
        return body

    def test_first_three_in_family_quarter_pass(self):
        for i in range(MAX_TRIALS_PER_FAMILY_PER_QUARTER):
            p = self._payload(
                f"cand-{i:03d}",
                timestamp=f"2026-04-0{i + 1}T12:00:00Z",
            )
            _write_registration(self.tmp, p)
            assert_protocol_compliant(p["candidate_id"])

    def test_fourth_in_family_quarter_blocks(self):
        for i in range(MAX_TRIALS_PER_FAMILY_PER_QUARTER + 1):
            p = self._payload(
                f"cand-{i:03d}",
                timestamp=f"2026-04-0{i + 1}T12:00:00Z",
            )
            _write_registration(self.tmp, p)
            if i < MAX_TRIALS_PER_FAMILY_PER_QUARTER:
                assert_protocol_compliant(p["candidate_id"])
            else:
                with self.assertRaises(TrialBudgetViolationError):
                    assert_protocol_compliant(p["candidate_id"])
        # The block emits an audit event:
        block_events = load_audit_events(event_type="trial_budget_block")
        self.assertEqual(len(block_events), 1)

    def test_trial_recorded_emits_audit_event(self):
        p = self._payload("cand-001")
        _write_registration(self.tmp, p)
        assert_protocol_compliant(p["candidate_id"])
        recorded = load_audit_events(event_type="trial_recorded")
        self.assertEqual(len(recorded), 1)
        self.assertEqual(
            recorded[0].metadata["modification_type"], "new_hypothesis"
        )
        self.assertEqual(recorded[0].metadata["hypothesis_family"], "iv_crush")
        self.assertEqual(recorded[0].metadata["quarter"], "2026-Q2")

    def test_revival_attempt_blocks_at_protocol_guard(self):
        # Child timestamp is in the far future so it is unambiguously
        # *after* the parent's wall-clock killed_at, exercising the
        # post-failure revival path.
        parent = self._payload(
            "parent-001",
            timestamp="2026-04-01T12:00:00Z",
        )
        _write_registration(self.tmp, parent)
        assert_protocol_compliant(parent["candidate_id"])
        record_kill(
            candidate_id=parent["candidate_id"],
            registration_hash=parent[HASH_FIELD],
            stage=3,
            reason="cross-period falsified",
        )
        child = self._payload(
            "child-001",
            timestamp="2099-12-31T12:00:00Z",
            parent_candidate_id=parent["candidate_id"],
        )
        _write_registration(self.tmp, child)
        with self.assertRaises(TrialBudgetViolationError) as ctx:
            assert_protocol_compliant(child["candidate_id"])
        self.assertIn("revival_attempt", str(ctx.exception))

    def test_enforce_trial_budget_false_skips_gate(self):
        for i in range(MAX_TRIALS_PER_FAMILY_PER_QUARTER + 1):
            p = self._payload(
                f"cand-{i:03d}",
                timestamp=f"2026-04-0{i + 1}T12:00:00Z",
            )
            _write_registration(self.tmp, p)
            assert_protocol_compliant(
                p["candidate_id"], enforce_trial_budget=False,
            )
        # No trial_budget_block emitted because we opted out:
        block_events = load_audit_events(event_type="trial_budget_block")
        self.assertEqual(block_events, [])


if __name__ == "__main__":
    unittest.main()
