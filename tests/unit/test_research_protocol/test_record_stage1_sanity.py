"""Tests for scripts/record_stage1_sanity.py (Stage 1 in-sample sanity helper)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from services.research_protocol._paths import ENV_PROTOCOL_ROOT
from services.research_protocol.errors import RegistrationMissingError, StageGateError
from services.research_protocol.registration import (
    HASH_FIELD,
    compute_registration_hash,
)
from services.research_protocol.validation_ladder import (
    get_candidate_stage_status,
    record_stage_result,
)

# Import the script under test via importlib so we can call main() directly
# without relying on it being on sys.path at collection time.
import importlib.util
import sys

_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "record_stage1_sanity.py"
_spec = importlib.util.spec_from_file_location("record_stage1_sanity", _SCRIPT)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]
record_stage1_main = _mod.main


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _valid_payload(candidate_id: str = "stage1-test-001") -> dict:
    payload = {
        "candidate_id": candidate_id,
        "registration_timestamp": "2026-05-04T18:00:00Z",
        "git_commit_sha": "0" * 40,
        "hypothesis": {
            "mechanism": "stage1 test mechanism",
            "predicted_direction": "long",
            "why_might_fail": "regime change",
            "citations": ["paper:x"],
        },
        "features": [{"name": "f1", "input_columns": ["close"]}],
        "thresholds": [{"name": "t1", "kind": "fixed", "value": 0.5}],
        "transformations": {"allowed": [], "forbidden_unless_listed": []},
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
    """Base that sets ENV_PROTOCOL_ROOT to a fresh temp directory."""

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

    def _run(self, argv: list[str]) -> int:
        return record_stage1_main(argv)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMissingCandidateBlocks(_ProtocolRootBase):
    def test_missing_registration_returns_nonzero(self):
        """No registration file → exit code 1, nothing written."""
        rc = self._run([
            "--candidate-id", "never-registered",
            "--dataset-identifier", "spy_2024",
            "--passed",
        ])
        self.assertEqual(rc, 1)

    def test_missing_registration_writes_no_report(self):
        rc = self._run([
            "--candidate-id", "never-registered",
            "--dataset-identifier", "spy_2024",
            "--passed",
            "--report-path", str(self.tmp / "should_not_exist.json"),
        ])
        self.assertEqual(rc, 1)
        self.assertFalse((self.tmp / "should_not_exist.json").exists())


class TestPassRecording(_ProtocolRootBase):
    def setUp(self) -> None:
        super().setUp()
        self.payload = _valid_payload()
        _write_registration(self.tmp, self.payload)
        self.candidate_id = self.payload["candidate_id"]

    def test_valid_registration_records_stage1_pass(self):
        """Registered candidate can record stage 1 as pass."""
        report_path = self.tmp / "stage1_pass.json"
        rc = self._run([
            "--candidate-id", self.candidate_id,
            "--dataset-identifier", "spy_2024_in_sample",
            "--passed",
            "--report-path", str(report_path),
        ])
        self.assertEqual(rc, 0)

    def test_report_file_is_written(self):
        report_path = self.tmp / "stage1_report.json"
        self._run([
            "--candidate-id", self.candidate_id,
            "--dataset-identifier", "spy_2024_in_sample",
            "--passed",
            "--report-path", str(report_path),
        ])
        self.assertTrue(report_path.exists())

    def test_report_contains_required_fields(self):
        report_path = self.tmp / "stage1_fields.json"
        self._run([
            "--candidate-id", self.candidate_id,
            "--dataset-identifier", "spy_2024_in_sample",
            "--passed",
            "--reason", "Feature pipeline clean",
            "--report-path", str(report_path),
        ])
        data = json.loads(report_path.read_text())
        self.assertEqual(data["candidate_id"], self.candidate_id)
        self.assertEqual(data["protocol_stage"], 1)
        self.assertEqual(data["stage_name"], "stage_1_in_sample_sanity")
        self.assertTrue(data["passed"])
        self.assertEqual(data["dataset_identifier"], "spy_2024_in_sample")
        self.assertEqual(data["reason"], "Feature pipeline clean")
        self.assertIn("run_timestamp", data)
        self.assertIn("registration_hash", data)

    def test_metadata_includes_run_timestamp_and_dataset_identifier(self):
        """Ladder state must carry run_timestamp and dataset_identifier."""
        report_path = self.tmp / "stage1_meta.json"
        self._run([
            "--candidate-id", self.candidate_id,
            "--dataset-identifier", "spy_2024_meta_test",
            "--passed",
            "--report-path", str(report_path),
        ])
        status = get_candidate_stage_status(self.candidate_id)
        result = status.stages.get(1)
        self.assertIsNotNone(result)
        self.assertIn("run_timestamp", result.metadata)
        self.assertIn("dataset_identifier", result.metadata)
        self.assertEqual(result.metadata["dataset_identifier"], "spy_2024_meta_test")
        self.assertNotEqual(result.metadata["run_timestamp"], "")

    def test_stage1_pass_recorded_in_ladder_state(self):
        report_path = self.tmp / "stage1_ladder.json"
        self._run([
            "--candidate-id", self.candidate_id,
            "--dataset-identifier", "spy_2024_ladder",
            "--passed",
            "--report-path", str(report_path),
        ])
        status = get_candidate_stage_status(self.candidate_id)
        result = status.stages.get(1)
        self.assertIsNotNone(result)
        self.assertEqual(result.status, "pass")


class TestFailRecording(_ProtocolRootBase):
    def setUp(self) -> None:
        super().setUp()
        self.payload = _valid_payload(candidate_id="stage1-fail-001")
        _write_registration(self.tmp, self.payload)
        self.candidate_id = self.payload["candidate_id"]

    def test_failed_stage1_blocks_stage2(self):
        """A stage 1 fail must block any later stage from being attempted."""
        report_path = self.tmp / "stage1_fail.json"
        rc = self._run([
            "--candidate-id", self.candidate_id,
            "--dataset-identifier", "spy_2024_fail",
            "--failed",
            "--reason", "NaN in 30% of feature rows",
            "--report-path", str(report_path),
        ])
        self.assertEqual(rc, 0)

        # Stage 1 must be recorded as fail.
        status = get_candidate_stage_status(self.candidate_id)
        self.assertEqual(status.stages[1].status, "fail")

        # Attempting to record stage 2 now must raise StageGateError.
        with self.assertRaises(StageGateError):
            record_stage_result(
                candidate_id=self.candidate_id,
                stage=2,
                passed=True,
                report_path=str(self.tmp / "stage2.json"),
                metadata={
                    "run_timestamp": "2026-05-04T20:00:00Z",
                    "dataset_identifier": "spy_2024",
                    "statistical_validity": {
                        "stage": 2,
                        "n_obs": 250,
                        "horizon_days": 5,
                        "ci_lower": 0.001,
                        "ci_upper": 0.008,
                        "permutation_p_value": 0.01,
                        "permutation_alpha": 0.05,
                    },
                },
            )

    def test_failed_stage1_report_written_with_passed_false(self):
        report_path = self.tmp / "stage1_fail_report.json"
        self._run([
            "--candidate-id", self.candidate_id,
            "--dataset-identifier", "spy_2024_fail",
            "--failed",
            "--report-path", str(report_path),
        ])
        self.assertTrue(report_path.exists())
        data = json.loads(report_path.read_text())
        self.assertFalse(data["passed"])


class TestDefaultReportPath(_ProtocolRootBase):
    def setUp(self) -> None:
        super().setUp()
        self.payload = _valid_payload(candidate_id="stage1-default-path-001")
        _write_registration(self.tmp, self.payload)
        self.candidate_id = self.payload["candidate_id"]

    def test_default_report_path_written_when_not_specified(self):
        """Omitting --report-path uses the default naming convention."""
        rc = self._run([
            "--candidate-id", self.candidate_id,
            "--dataset-identifier", "spy_2024_default",
            "--passed",
        ])
        self.assertEqual(rc, 0)
        # Default path is ROOT/reports/research_protocol/stage1/{cid}_stage1_sanity.json
        expected = (
            Path(_SCRIPT).resolve().parents[1]
            / "reports" / "research_protocol" / "stage1"
            / f"{self.candidate_id}_stage1_sanity.json"
        )
        self.assertTrue(expected.exists(), f"Expected report at {expected}")
        expected.unlink()  # cleanup from live filesystem


if __name__ == "__main__":
    unittest.main()
