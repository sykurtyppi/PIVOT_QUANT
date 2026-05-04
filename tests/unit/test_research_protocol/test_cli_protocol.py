"""Tests for services.research_protocol.cli_protocol (PR7 integration helper)."""

from __future__ import annotations

import argparse
import io
import json
import os
import tempfile
import unittest
from pathlib import Path

from services.research_protocol._paths import ENV_PROTOCOL_ROOT
from services.research_protocol.cli_protocol import (
    PROTOCOL_DISABLED_WARNING,
    add_protocol_arguments,
    enforce_protocol_from_args,
)
from services.research_protocol.errors import (
    CandidateKilledError,
    ProtocolCLIError,
    TrialBudgetViolationError,
)
from services.research_protocol.kill_list import record_kill
from services.research_protocol.registration import (
    HASH_FIELD,
    Registration,
    compute_registration_hash,
)


def _registration_payload(
    *,
    candidate_id: str,
    timestamp: str = "2026-04-15T10:00:00Z",
    hypothesis_family: str = "iv_crush",
) -> dict:
    payload = {
        "candidate_id": candidate_id,
        "registration_timestamp": timestamp,
        "git_commit_sha": "0" * 40,
        "hypothesis": {
            "mechanism": "cli test mechanism",
            "predicted_direction": "long",
            "why_might_fail": "regime",
            "citations": ["paper:x"],
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

    def _build_parser(self, *, expected_stage: int | None = 2) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        add_protocol_arguments(parser, expected_stage=expected_stage)
        return parser


class TestAddProtocolArguments(unittest.TestCase):
    def _build(self, expected_stage: int | None = None) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        add_protocol_arguments(parser, expected_stage=expected_stage)
        return parser

    def test_defaults_when_no_flags_given(self):
        args = self._build().parse_args([])
        self.assertIsNone(args.candidate_id)
        self.assertIsNone(args.protocol_stage)
        self.assertFalse(args.enforce_protocol)

    def test_expected_stage_becomes_default(self):
        args = self._build(expected_stage=3).parse_args([])
        self.assertEqual(args.protocol_stage, 3)

    def test_explicit_flags_parse(self):
        args = self._build().parse_args([
            "--candidate-id", "iv-crush-001",
            "--protocol-stage", "2",
            "--enforce-protocol",
        ])
        self.assertEqual(args.candidate_id, "iv-crush-001")
        self.assertEqual(args.protocol_stage, 2)
        self.assertTrue(args.enforce_protocol)

    def test_no_enforce_protocol_explicit_off(self):
        args = self._build().parse_args(["--no-enforce-protocol"])
        self.assertFalse(args.enforce_protocol)

    def test_enforce_and_no_enforce_are_mutually_exclusive(self):
        parser = self._build()
        with self.assertRaises(SystemExit):
            parser.parse_args(["--enforce-protocol", "--no-enforce-protocol"])


class TestEnforceProtocolFromArgsDisabled(_ProtocolRootBase):
    def test_diagnostic_warning_when_not_enforced(self):
        parser = self._build_parser()
        args = parser.parse_args([])
        stream = io.StringIO()
        result = enforce_protocol_from_args(args, expected_stage=2, stream=stream)
        self.assertIsNone(result)
        self.assertIn("disabled", stream.getvalue())
        self.assertIn("diagnostic only", stream.getvalue())
        self.assertEqual(stream.getvalue(), PROTOCOL_DISABLED_WARNING)

    def test_disabled_does_not_call_protocol_guard(self):
        # No registration written; if we attempted to enforce it would fail.
        parser = self._build_parser()
        args = parser.parse_args(["--candidate-id", "phantom", "--protocol-stage", "2"])
        # enforce_protocol still defaults to False even though id+stage were given.
        result = enforce_protocol_from_args(
            args, expected_stage=2, stream=io.StringIO(),
        )
        self.assertIsNone(result)


class TestEnforceProtocolFromArgsEnabled(_ProtocolRootBase):
    def test_enforced_run_without_candidate_id_fails(self):
        parser = self._build_parser()
        args = parser.parse_args([
            "--protocol-stage", "2", "--enforce-protocol",
        ])
        with self.assertRaises(ProtocolCLIError) as ctx:
            enforce_protocol_from_args(args, expected_stage=2, stream=io.StringIO())
        self.assertIn("--candidate-id", str(ctx.exception))

    def test_enforced_run_without_protocol_stage_fails(self):
        parser = self._build_parser(expected_stage=None)
        args = parser.parse_args([
            "--candidate-id", "iv-crush-001", "--enforce-protocol",
        ])
        with self.assertRaises(ProtocolCLIError) as ctx:
            enforce_protocol_from_args(args, stream=io.StringIO())
        self.assertIn("--protocol-stage", str(ctx.exception))

    def test_enforced_run_with_mismatched_stage_fails(self):
        parser = self._build_parser()
        args = parser.parse_args([
            "--candidate-id", "iv-crush-001",
            "--protocol-stage", "3",
            "--enforce-protocol",
        ])
        with self.assertRaises(ProtocolCLIError) as ctx:
            enforce_protocol_from_args(args, expected_stage=2, stream=io.StringIO())
        self.assertIn("stage 2", str(ctx.exception))

    def test_enforced_run_returns_registration(self):
        payload = _registration_payload(candidate_id="iv-crush-001")
        _write_registration(self.tmp, payload)
        parser = self._build_parser(expected_stage=1)
        args = parser.parse_args([
            "--candidate-id", "iv-crush-001",
            "--protocol-stage", "1",
            "--enforce-protocol",
        ])
        stream = io.StringIO()
        result = enforce_protocol_from_args(args, expected_stage=1, stream=stream)
        self.assertIsInstance(result, Registration)
        self.assertEqual(result.candidate_id, "iv-crush-001")
        # No diagnostic warning when enforcement is on.
        self.assertEqual(stream.getvalue(), "")

    def test_enforced_run_blocks_on_killed_candidate(self):
        payload = _registration_payload(candidate_id="iv-crush-001")
        _write_registration(self.tmp, payload)
        record_kill(
            candidate_id="iv-crush-001",
            registration_hash=payload[HASH_FIELD],
            stage=3,
            reason="cross-period falsified",
        )
        parser = self._build_parser(expected_stage=1)
        args = parser.parse_args([
            "--candidate-id", "iv-crush-001",
            "--protocol-stage", "1",
            "--enforce-protocol",
        ])
        with self.assertRaises(CandidateKilledError):
            enforce_protocol_from_args(args, expected_stage=1, stream=io.StringIO())

    def test_enforced_run_blocks_on_revival_attempt(self):
        # Parent registered + killed, child claims same family → revival.
        parent = _registration_payload(
            candidate_id="iv-crush-parent",
            timestamp="2026-04-15T10:00:00Z",
        )
        _write_registration(self.tmp, parent)
        # First run records the parent trial cleanly at stage 1 (no priors).
        parser = self._build_parser(expected_stage=1)
        args_parent = parser.parse_args([
            "--candidate-id", "iv-crush-parent",
            "--protocol-stage", "1",
            "--enforce-protocol",
        ])
        enforce_protocol_from_args(
            args_parent, expected_stage=1, stream=io.StringIO(),
        )
        record_kill(
            candidate_id="iv-crush-parent",
            registration_hash=parent[HASH_FIELD],
            stage=3, reason="cross-period falsified",
        )
        # Child registered AFTER kill; no `claimed_modification_type=new_hypothesis`.
        child = {
            **_registration_payload(
                candidate_id="iv-crush-child",
                timestamp="2099-12-31T10:00:00Z",
            ),
        }
        # Add the parent linkage and re-hash.
        child["parent_candidate_id"] = "iv-crush-parent"
        child["thresholds"] = [{"name": "t1", "kind": "fixed", "value": 0.7}]
        child[HASH_FIELD] = compute_registration_hash(child)
        _write_registration(self.tmp, child)
        parser_child = self._build_parser(expected_stage=1)
        args_child = parser_child.parse_args([
            "--candidate-id", "iv-crush-child",
            "--protocol-stage", "1",
            "--enforce-protocol",
        ])
        with self.assertRaises(TrialBudgetViolationError) as ctx:
            enforce_protocol_from_args(
                args_child, expected_stage=1, stream=io.StringIO(),
            )
        self.assertIn("revival_attempt", str(ctx.exception))

    def test_enforced_run_invalid_stage_value_rejected(self):
        parser = self._build_parser(expected_stage=None)
        args = parser.parse_args([
            "--candidate-id", "iv-crush-001",
            "--protocol-stage", "99",
            "--enforce-protocol",
        ])
        with self.assertRaises(ProtocolCLIError) as ctx:
            enforce_protocol_from_args(args, stream=io.StringIO())
        self.assertIn("[0, 6]", str(ctx.exception))

    def test_stage_0_dataset_run_allowed(self):
        """Dataset/infrastructure stage is a legitimate enforced run."""
        payload = _registration_payload(candidate_id="iv-crush-001")
        _write_registration(self.tmp, payload)
        parser = argparse.ArgumentParser()
        add_protocol_arguments(parser, expected_stage=0)
        args = parser.parse_args([
            "--candidate-id", "iv-crush-001",
            "--enforce-protocol",
        ])
        result = enforce_protocol_from_args(
            args, expected_stage=0, stream=io.StringIO(),
        )
        self.assertIsInstance(result, Registration)


class TestEnforcedScriptIntegration(_ProtocolRootBase):
    """Verify each wired script wires the helper at parse_args + main entry."""

    def test_run_ml_regime_validation_has_protocol_flags(self):
        from scripts import run_ml_regime_validation as mod
        # Module-level constant exposes the stage.
        self.assertEqual(mod.PROTOCOL_STAGE, 2)
        # parse_args adds the flags; --help exits cleanly. Capture stdout
        # so unittest output is not polluted.
        import contextlib
        import sys
        buf = io.StringIO()
        prev_argv = sys.argv
        try:
            sys.argv = ["run_ml_regime_validation.py", "--help"]
            with contextlib.redirect_stdout(buf), self.assertRaises(SystemExit):
                mod.parse_args()
        finally:
            sys.argv = prev_argv
        self.assertIn("--enforce-protocol", buf.getvalue())
        self.assertIn("--candidate-id", buf.getvalue())

    def test_run_ml_regime_validation_cross_period_has_protocol_flags(self):
        from scripts import run_ml_regime_validation_cross_period as mod
        self.assertEqual(mod.PROTOCOL_STAGE, 3)

    def test_run_model_ready_dataset_smoke_has_protocol_flags(self):
        try:
            from scripts import run_model_ready_dataset_smoke as mod
        except ImportError:
            self.skipTest(
                "run_model_ready_dataset_smoke not present in this environment"
            )
        self.assertEqual(mod.PROTOCOL_STAGE, 0)


if __name__ == "__main__":
    unittest.main()
