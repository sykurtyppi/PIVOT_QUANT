"""Tests for services.research_protocol.replication_guard (RESEARCH_PROTOCOL §5)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from services.research_protocol._paths import (
    ENV_PROTOCOL_ROOT,
    replication_state_path,
)
from services.research_protocol.errors import (
    RegistrationMissingError,
    ReplicationViolationError,
)
from services.research_protocol.registration import (
    HASH_FIELD,
    compute_registration_hash,
)
from services.research_protocol.replication_guard import (
    MIN_DISTINCT_PERIODS,
    MIN_DISTINCT_SYMBOLS,
    REPLICATION_VERSION,
    CrossSymbolExemption,
    ReplicationEvidence,
    ReplicationStatus,
    assert_replication_ready,
    get_cross_symbol_exemption,
    load_replication_evidence,
    record_cross_symbol_exemption,
    record_replication_result,
    summarize_replication_status,
)


def _valid_registration_payload(candidate_id: str) -> dict:
    payload = {
        "candidate_id": candidate_id,
        "registration_timestamp": "2026-05-04T18:00:00Z",
        "git_commit_sha": "0" * 40,
        "hypothesis": {
            "mechanism": "replication test mechanism",
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


def _passing_stat_block(stage: int = 3) -> dict:
    return {
        "stage": stage,
        "n_obs": 250,
        "horizon_days": 5,
        "ci_lower": 0.001,
        "ci_upper": 0.008,
        "permutation_p_value": 0.01,
        "permutation_alpha": 0.05,
    }


def _failing_stat_block(stage: int = 3) -> dict:
    # CI includes zero AND p above alpha → statistical_pass=False
    return {
        "stage": stage,
        "n_obs": 250,
        "horizon_days": 5,
        "ci_lower": -0.002,
        "ci_upper": 0.005,
        "permutation_p_value": 0.10,
        "permutation_alpha": 0.05,
    }


def _suppressed_stat_block(stage: int = 3) -> dict:
    # n_eff = 2 < floor → metrics_suppressed=True, statistical_pass=False
    return {
        "stage": stage,
        "n_obs": 10,
        "horizon_days": 5,
        "ci_lower": 0.01,
        "ci_upper": 0.02,
        "permutation_p_value": 0.001,
        "permutation_alpha": 0.05,
    }


class _ProtocolRootBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp_ctx = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_ctx.name).resolve()
        self._prev_env = os.environ.get(ENV_PROTOCOL_ROOT)
        os.environ[ENV_PROTOCOL_ROOT] = str(self.tmp)
        self.candidate_id = "replication-test-001"
        self.payload = _valid_registration_payload(self.candidate_id)
        _write_registration(self.tmp, self.payload)
        self.registration_hash = self.payload[HASH_FIELD]

    def tearDown(self) -> None:
        if self._prev_env is None:
            os.environ.pop(ENV_PROTOCOL_ROOT, None)
        else:
            os.environ[ENV_PROTOCOL_ROOT] = self._prev_env
        self._tmp_ctx.cleanup()

    def _record(
        self,
        *,
        period_id: str,
        symbol: str,
        train_start: str = "2023-01-03",
        train_end: str = "2024-12-31",
        test_start: str = "2025-01-02",
        test_end: str = "2025-12-31",
        report_path: str | None = None,
        stat_block: dict | None = None,
    ) -> ReplicationEvidence:
        return record_replication_result(
            candidate_id=self.candidate_id,
            period_id=period_id,
            train_start=train_start,
            train_end=train_end,
            test_start=test_start,
            test_end=test_end,
            symbol=symbol,
            report_path=report_path or f"reports/{period_id}_{symbol}.json",
            statistical_validity=stat_block or _passing_stat_block(),
        )


class TestEmptyState(_ProtocolRootBase):
    def test_no_state_file_means_no_evidence(self):
        self.assertFalse(replication_state_path().exists())
        self.assertEqual(load_replication_evidence(self.candidate_id), [])

    def test_summary_for_unrecorded_candidate(self):
        status = summarize_replication_status(self.candidate_id)
        self.assertIsInstance(status, ReplicationStatus)
        self.assertEqual(status.total_evidence, 0)
        self.assertFalse(status.replication_ready)
        self.assertFalse(status.meets_minimum_periods)
        self.assertFalse(status.meets_minimum_symbols)
        self.assertIsNone(status.cross_symbol_exemption)
        self.assertEqual(status.distinct_passing_periods, ())
        self.assertEqual(status.distinct_passing_symbols, ())

    def test_assert_replication_ready_raises_for_empty(self):
        with self.assertRaises(ReplicationViolationError):
            assert_replication_ready(self.candidate_id)


class TestRecordReplicationResult(_ProtocolRootBase):
    def test_basic_record_creates_passing_evidence(self):
        ev = self._record(period_id="p2025", symbol="SPY")
        self.assertIsInstance(ev, ReplicationEvidence)
        self.assertEqual(ev.candidate_id, self.candidate_id)
        self.assertEqual(ev.period_id, "p2025")
        self.assertEqual(ev.symbol, "SPY")
        self.assertTrue(ev.statistical_validity["statistical_pass"])
        self.assertTrue(ev.passed)
        self.assertEqual(ev.registration_hash, self.registration_hash)

    def test_persists_to_disk(self):
        self._record(period_id="p2025", symbol="SPY")
        on_disk = json.loads(replication_state_path().read_text())
        self.assertEqual(on_disk["version"], REPLICATION_VERSION)
        body = on_disk["candidates"][self.candidate_id]
        self.assertEqual(body["registration_hash"], self.registration_hash)
        self.assertEqual(len(body["evidence"]), 1)
        self.assertEqual(body["evidence"][0]["symbol"], "SPY")

    def test_atomic_no_temp_files_left(self):
        self._record(period_id="p2025", symbol="SPY")
        leftovers = list(self.tmp.glob(".replication_evidence.*"))
        self.assertEqual(leftovers, [])

    def test_failed_evidence_is_recorded_not_rejected(self):
        """Statistical-failure inputs are stored as failed evidence and counted
        in failing_evidence — they do NOT cause record_replication_result to
        raise."""
        ev = self._record(
            period_id="p2025", symbol="SPY",
            stat_block=_failing_stat_block(),
        )
        self.assertFalse(ev.passed)
        self.assertFalse(ev.statistical_validity["statistical_pass"])
        status = summarize_replication_status(self.candidate_id)
        self.assertEqual(status.total_evidence, 1)
        self.assertEqual(status.passing_evidence, 0)
        self.assertEqual(status.failing_evidence, 1)
        self.assertFalse(status.replication_ready)

    def test_user_claimed_pass_recomputed_to_fail(self):
        """A tampered block claiming statistical_pass=True with failing
        inputs is stored with the recomputed (False) verdict."""
        bad = _failing_stat_block()
        bad["statistical_pass"] = True       # lie
        bad["metrics_suppressed"] = False    # lie
        bad["suppression_reasons"] = []
        ev = self._record(period_id="p2025", symbol="SPY", stat_block=bad)
        self.assertFalse(ev.passed)
        self.assertFalse(ev.statistical_validity["statistical_pass"])

    def test_idempotent_on_identical_repeat(self):
        first = self._record(period_id="p2025", symbol="SPY")
        second = self._record(period_id="p2025", symbol="SPY")
        self.assertEqual(first.recorded_at, second.recorded_at)
        on_disk = json.loads(replication_state_path().read_text())
        self.assertEqual(len(on_disk["candidates"][self.candidate_id]["evidence"]), 1)

    def test_duplicate_key_with_different_report_path_rejected(self):
        self._record(
            period_id="p2025", symbol="SPY",
            report_path="reports/v1.json",
        )
        with self.assertRaises(ReplicationViolationError) as ctx:
            self._record(
                period_id="p2025", symbol="SPY",
                report_path="reports/v2.json",
            )
        self.assertIn("append-only", str(ctx.exception))

    def test_duplicate_key_with_different_status_rejected(self):
        self._record(period_id="p2025", symbol="SPY")
        with self.assertRaises(ReplicationViolationError):
            self._record(
                period_id="p2025", symbol="SPY",
                stat_block=_failing_stat_block(),
            )

    def test_missing_report_path_rejected(self):
        for bad in ("", "   ", None):
            with self.subTest(bad=bad):
                with self.assertRaises(ReplicationViolationError):
                    record_replication_result(
                        candidate_id=self.candidate_id,
                        period_id="p2025",
                        train_start="2023-01-03",
                        train_end="2024-12-31",
                        test_start="2025-01-02",
                        test_end="2025-12-31",
                        symbol="SPY",
                        report_path=bad,  # type: ignore[arg-type]
                        statistical_validity=_passing_stat_block(),
                    )

    def test_invalid_period_id_rejected(self):
        with self.assertRaises(ReplicationViolationError):
            self._record(period_id="bad period id with spaces", symbol="SPY")

    def test_invalid_symbol_rejected(self):
        for bad in ("spy", "Spy", "1SPY", ""):
            with self.subTest(bad=bad):
                with self.assertRaises(ReplicationViolationError):
                    self._record(period_id="p2025", symbol=bad)

    def test_invalid_date_rejected(self):
        with self.assertRaises(ReplicationViolationError):
            self._record(
                period_id="p2025", symbol="SPY",
                train_start="2023/01/03",
            )

    def test_test_window_overlapping_train_rejected(self):
        with self.assertRaises(ReplicationViolationError) as ctx:
            self._record(
                period_id="p2025", symbol="SPY",
                train_start="2023-01-03", train_end="2024-12-31",
                test_start="2024-06-01", test_end="2025-12-31",   # overlaps train
            )
        self.assertIn("must not overlap", str(ctx.exception))

    def test_inverted_train_window_rejected(self):
        with self.assertRaises(ReplicationViolationError):
            self._record(
                period_id="p2025", symbol="SPY",
                train_start="2024-12-31", train_end="2023-01-03",
            )

    def test_record_blocked_when_registration_missing(self):
        with self.assertRaises(RegistrationMissingError):
            record_replication_result(
                candidate_id="never-registered",
                period_id="p1", train_start="2023-01-03",
                train_end="2024-12-31", test_start="2025-01-02",
                test_end="2025-12-31", symbol="SPY",
                report_path="r.json",
                statistical_validity=_passing_stat_block(),
            )

    def test_registration_hash_drift_detected(self):
        self._record(period_id="p2025", symbol="SPY")
        # Tamper the on-disk state: simulate a re-registration with a
        # different hash for the same candidate_id.
        path = replication_state_path()
        state = json.loads(path.read_text())
        state["candidates"][self.candidate_id]["registration_hash"] = "f" * 64
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        with self.assertRaises(ReplicationViolationError) as ctx:
            self._record(period_id="p2025_alt", symbol="QQQ")
        self.assertIn("registration changed", str(ctx.exception))


class TestReplicationReadiness(_ProtocolRootBase):
    """The 9 spec cases exercise the readiness aggregator."""

    # 1. one period only blocks stage 6
    def test_one_period_only_blocks(self):
        self._record(period_id="p2025", symbol="SPY")
        with self.assertRaises(ReplicationViolationError) as ctx:
            assert_replication_ready(self.candidate_id)
        self.assertIn("distinct passing periods=1", str(ctx.exception))

    # 2. two periods same symbol but no second symbol blocks stage 6
    def test_two_periods_same_symbol_blocks(self):
        self._record(
            period_id="p2025", symbol="SPY",
            train_start="2023-01-03", train_end="2024-12-31",
            test_start="2025-01-02", test_end="2025-12-31",
        )
        self._record(
            period_id="p2022", symbol="SPY",
            train_start="2020-01-02", train_end="2021-12-31",
            test_start="2022-01-03", test_end="2022-12-30",
        )
        with self.assertRaises(ReplicationViolationError) as ctx:
            assert_replication_ready(self.candidate_id)
        self.assertIn("distinct passing symbols=1", str(ctx.exception))

    # 3. two symbols same period but no second period blocks stage 6
    def test_two_symbols_same_period_blocks(self):
        self._record(period_id="p2025", symbol="SPY")
        self._record(period_id="p2025", symbol="QQQ")
        with self.assertRaises(ReplicationViolationError) as ctx:
            assert_replication_ready(self.candidate_id)
        self.assertIn("distinct passing periods=1", str(ctx.exception))

    # 4. two periods + two symbols passes
    def test_two_periods_two_symbols_passes(self):
        self._record(period_id="p2025", symbol="SPY")
        self._record(period_id="p2025", symbol="QQQ")
        self._record(
            period_id="p2022", symbol="SPY",
            train_start="2020-01-02", train_end="2021-12-31",
            test_start="2022-01-03", test_end="2022-12-30",
        )
        # No raise.
        assert_replication_ready(self.candidate_id)
        status = summarize_replication_status(self.candidate_id)
        self.assertTrue(status.replication_ready)
        self.assertEqual(set(status.distinct_passing_periods), {"p2022", "p2025"})
        self.assertEqual(set(status.distinct_passing_symbols), {"SPY", "QQQ"})

    # 5. explicit cross-symbol exemption allows one symbol but records reason
    def test_cross_symbol_exemption_with_two_periods_one_symbol(self):
        self._record(
            period_id="p2025", symbol="SPY",
            train_start="2023-01-03", train_end="2024-12-31",
            test_start="2025-01-02", test_end="2025-12-31",
        )
        self._record(
            period_id="p2022", symbol="SPY",
            train_start="2020-01-02", train_end="2021-12-31",
            test_start="2022-01-03", test_end="2022-12-30",
        )
        with self.assertRaises(ReplicationViolationError):
            assert_replication_ready(self.candidate_id)
        record_cross_symbol_exemption(
            candidate_id=self.candidate_id,
            reason="iVolatility coverage limited to SPY for the windows under test",
        )
        # Now passes because exemption granted.
        assert_replication_ready(self.candidate_id)
        status = summarize_replication_status(self.candidate_id)
        self.assertTrue(status.replication_ready)
        self.assertIsNotNone(status.cross_symbol_exemption)
        self.assertIn("iVolatility", status.cross_symbol_exemption.reason)

    # 6. failed evidence retained but not counted
    def test_failed_evidence_retained_but_not_counted(self):
        self._record(period_id="p2025", symbol="SPY")
        # Failing piece for a different period+symbol
        self._record(
            period_id="p2022", symbol="QQQ",
            train_start="2020-01-02", train_end="2021-12-31",
            test_start="2022-01-03", test_end="2022-12-30",
            stat_block=_failing_stat_block(),
        )
        status = summarize_replication_status(self.candidate_id)
        self.assertEqual(status.total_evidence, 2)
        self.assertEqual(status.passing_evidence, 1)
        self.assertEqual(status.failing_evidence, 1)
        # Only the passing one counts toward distinct sets.
        self.assertEqual(status.distinct_passing_periods, ("p2025",))
        self.assertEqual(status.distinct_passing_symbols, ("SPY",))
        with self.assertRaises(ReplicationViolationError):
            assert_replication_ready(self.candidate_id)

    # 7. duplicate evidence not double-counted
    def test_duplicate_evidence_not_double_counted(self):
        self._record(period_id="p2025", symbol="SPY")
        self._record(period_id="p2025", symbol="SPY")  # idempotent
        status = summarize_replication_status(self.candidate_id)
        self.assertEqual(status.total_evidence, 1)
        self.assertEqual(status.distinct_passing_periods, ("p2025",))
        self.assertEqual(status.distinct_passing_symbols, ("SPY",))

    # 8. missing report_path rejected — covered in TestRecordReplicationResult
    # 9. statistical failure evidence — covered in
    #    test_failed_evidence_is_recorded_not_rejected and
    #    test_failed_evidence_retained_but_not_counted

    def test_n_eff_suppressed_evidence_does_not_count(self):
        """Evidence whose statistical block triggers metrics_suppressed=True
        is failed evidence — does not count toward readiness."""
        self._record(period_id="p2025", symbol="SPY")
        self._record(
            period_id="p2022", symbol="QQQ",
            train_start="2020-01-02", train_end="2021-12-31",
            test_start="2022-01-03", test_end="2022-12-30",
            stat_block=_suppressed_stat_block(),
        )
        status = summarize_replication_status(self.candidate_id)
        self.assertEqual(status.distinct_passing_periods, ("p2025",))
        self.assertEqual(status.distinct_passing_symbols, ("SPY",))
        self.assertEqual(status.failing_evidence, 1)
        with self.assertRaises(ReplicationViolationError):
            assert_replication_ready(self.candidate_id)


class TestCrossSymbolExemption(_ProtocolRootBase):
    def test_grant_exemption_persists(self):
        ex = record_cross_symbol_exemption(
            candidate_id=self.candidate_id,
            reason="single-symbol coverage in T9 documented in run notes",
        )
        self.assertIsInstance(ex, CrossSymbolExemption)
        self.assertTrue(ex.granted)
        # Round-trips through disk.
        recalled = get_cross_symbol_exemption(self.candidate_id)
        self.assertIsNotNone(recalled)
        self.assertEqual(recalled.reason, ex.reason)

    def test_exemption_idempotent_on_same_reason(self):
        first = record_cross_symbol_exemption(
            candidate_id=self.candidate_id,
            reason="documented coverage gap reason 12345",
        )
        second = record_cross_symbol_exemption(
            candidate_id=self.candidate_id,
            reason="documented coverage gap reason 12345",
        )
        self.assertEqual(first.recorded_at, second.recorded_at)

    def test_exemption_with_different_reason_rejected(self):
        record_cross_symbol_exemption(
            candidate_id=self.candidate_id,
            reason="documented coverage gap reason 12345",
        )
        with self.assertRaises(ReplicationViolationError):
            record_cross_symbol_exemption(
                candidate_id=self.candidate_id,
                reason="completely different reason 67890",
            )

    def test_short_reason_rejected(self):
        with self.assertRaises(ReplicationViolationError):
            record_cross_symbol_exemption(
                candidate_id=self.candidate_id,
                reason="short",
            )

    def test_get_cross_symbol_exemption_none_when_not_granted(self):
        self.assertIsNone(get_cross_symbol_exemption(self.candidate_id))

    def test_exemption_blocked_when_registration_missing(self):
        with self.assertRaises(RegistrationMissingError):
            record_cross_symbol_exemption(
                candidate_id="never-registered",
                reason="documented coverage gap reason 12345",
            )


class TestStateTampering(_ProtocolRootBase):
    def _write_state(self, payload: dict) -> None:
        path = replication_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_invalid_json_rejected(self):
        path = replication_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not json", encoding="utf-8")
        with self.assertRaises(ReplicationViolationError):
            load_replication_evidence(self.candidate_id)

    def test_wrong_version_rejected(self):
        self._write_state({"version": 99, "candidates": {}})
        with self.assertRaises(ReplicationViolationError):
            load_replication_evidence(self.candidate_id)

    def test_top_level_array_rejected(self):
        path = replication_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("[]", encoding="utf-8")
        with self.assertRaises(ReplicationViolationError):
            load_replication_evidence(self.candidate_id)

    def test_evidence_not_a_list_rejected(self):
        self._write_state({
            "version": REPLICATION_VERSION,
            "candidates": {
                self.candidate_id: {
                    "registration_hash": self.registration_hash,
                    "evidence": {},
                },
            },
        })
        with self.assertRaises(ReplicationViolationError):
            load_replication_evidence(self.candidate_id)

    def test_missing_registration_hash_rejected(self):
        self._write_state({
            "version": REPLICATION_VERSION,
            "candidates": {self.candidate_id: {"evidence": []}},
        })
        with self.assertRaises(ReplicationViolationError):
            load_replication_evidence(self.candidate_id)

    def test_evidence_missing_required_key_rejected(self):
        self._write_state({
            "version": REPLICATION_VERSION,
            "candidates": {
                self.candidate_id: {
                    "registration_hash": self.registration_hash,
                    "evidence": [{
                        "period_id": "p1",
                        "train_start": "2023-01-03",
                        # missing other keys
                    }],
                },
            },
        })
        with self.assertRaises(ReplicationViolationError):
            load_replication_evidence(self.candidate_id)


class TestNoDowngradeAPI(unittest.TestCase):
    """Module surface must not expose any removal/revocation function."""

    def test_no_removal_functions_in_module(self):
        from services.research_protocol import replication_guard as mod
        for name in (
            "remove",
            "remove_evidence",
            "remove_replication_result",
            "delete",
            "delete_evidence",
            "clear",
            "clear_evidence",
            "reset",
            "reset_state",
            "revoke_exemption",
            "revoke_cross_symbol_exemption",
            "downgrade_evidence",
            "force_record",
        ):
            self.assertFalse(
                hasattr(mod, name),
                msg=f"replication_guard must not expose {name}() —"
                " replication evidence is append-only and exemptions"
                " cannot be revoked.",
            )

    def test_package_exports_have_no_revocation_names(self):
        import services.research_protocol as pkg
        for name in pkg.__all__:
            lower = name.lower()
            for forbidden in (
                "revoke", "downgrade", "reset", "clear",
            ):
                self.assertNotIn(
                    forbidden, lower,
                    msg=f"package __all__ exposes a {forbidden}-like name: {name}",
                )


if __name__ == "__main__":
    unittest.main()
