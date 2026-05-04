"""Tests for scripts/register_candidate.py.

Covers the user spec's five required cases plus a handful of guardrails:
successful creation, hash determinism, validation-failure-blocks-write,
existing-file-prevents-overwrite, mocked git-SHA injection. Tests
provide explicit ``--git-commit-sha`` and ``--registration-timestamp``
overrides so determinism does not depend on wall-clock or VCS state.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import register_candidate as rc  # noqa: E402

VALID_GIT_SHA = "0123456789abcdef0123456789abcdef01234567"
FIXED_TIMESTAMP = "2026-05-04T18:00:00Z"


def _valid_toml() -> str:
    """A complete TOML body that passes assert_registration_valid."""
    return """
candidate_id = "test-candidate-001"
horizon_days = 5
random_seed = 42
stages_required = [1, 2, 3]
hypothesis_family = "iv_crush"
forbidden_changes = ["any threshold change"]

[hypothesis]
mechanism = "post-FOMC dealer hedging compresses near-dated IV"
predicted_direction = "long"
why_might_fail = "regime-conditional liquidity"
citations = ["paper:fomc-iv-2024"]

[[features]]
name = "iv_change_5d"
input_columns = ["iv"]

[[thresholds]]
name = "iv_drop_threshold"
kind = "fixed"
value = -0.05

[transformations]
allowed = ["log"]
forbidden_unless_listed = ["any"]

[falsification]
stage_3 = "cross_period_validated=false"

[datasets]
symbol = "SPY"
validation_dataset_pattern = "spy_2025_validation.parquet"
holdout_dataset_pattern = "spy_2018_2020_holdout.parquet"
"""


class _BaseCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp_ctx = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_ctx.name).resolve()
        self.input_path = self.tmp / "candidate.toml"
        self.input_path.write_text(_valid_toml(), encoding="utf-8")
        self.reports_dir = self.tmp / "registrations"

    def tearDown(self) -> None:
        self._tmp_ctx.cleanup()

    def _argv(self, *extra: str, override_input: Path | None = None) -> list[str]:
        return [
            "--input", str(override_input or self.input_path),
            "--reports-dir", str(self.reports_dir),
            "--git-commit-sha", VALID_GIT_SHA,
            "--registration-timestamp", FIXED_TIMESTAMP,
            *extra,
        ]

    def _run(self, *extra: str) -> int:
        return rc.main(self._argv(*extra))

    def _output_path(self, candidate_id: str = "test-candidate-001") -> Path:
        return self.reports_dir / f"{candidate_id}.json"


# --------------------------------------------------------------------- #
# 1. Successful registration creation
# --------------------------------------------------------------------- #


class TestSuccessfulRegistration(_BaseCase):
    def test_creates_registration_file(self):
        self.assertEqual(self._run(), rc.EXIT_OK)
        out = self._output_path()
        self.assertTrue(out.exists())

    def test_payload_contains_required_fields(self):
        self._run()
        payload = json.loads(self._output_path().read_text())
        self.assertEqual(payload["candidate_id"], "test-candidate-001")
        self.assertEqual(payload["git_commit_sha"], VALID_GIT_SHA)
        self.assertEqual(payload["registration_timestamp"], FIXED_TIMESTAMP)
        self.assertEqual(len(payload["registration_hash"]), 64)
        self.assertEqual(payload["horizon_days"], 5)
        self.assertEqual(payload["stages_required"], [1, 2, 3])
        self.assertEqual(payload["hypothesis"]["predicted_direction"], "long")

    def test_emitted_payload_round_trips_through_protocol_validator(self):
        from services.research_protocol.registration import (
            assert_registration_valid,
        )

        self._run()
        payload = json.loads(self._output_path().read_text())
        # Must not raise.
        assert_registration_valid(payload)

    def test_atomic_write_no_temp_files_left(self):
        self._run()
        leftovers = list(self.reports_dir.glob("*.tmp"))
        self.assertEqual(leftovers, [])


# --------------------------------------------------------------------- #
# 2. Hash is deterministic
# --------------------------------------------------------------------- #


class TestHashDeterminism(_BaseCase):
    def test_same_inputs_produce_same_hash(self):
        self.assertEqual(self._run(), rc.EXIT_OK)
        first_hash = json.loads(self._output_path().read_text())[
            "registration_hash"
        ]
        self._output_path().unlink()
        self.assertEqual(self._run(), rc.EXIT_OK)
        second_hash = json.loads(self._output_path().read_text())[
            "registration_hash"
        ]
        self.assertEqual(first_hash, second_hash)

    def test_different_timestamp_changes_hash(self):
        self.assertEqual(self._run(), rc.EXIT_OK)
        first = json.loads(self._output_path().read_text())["registration_hash"]
        self._output_path().unlink()
        # Override the timestamp; re-run.
        argv = [
            "--input", str(self.input_path),
            "--reports-dir", str(self.reports_dir),
            "--git-commit-sha", VALID_GIT_SHA,
            "--registration-timestamp", "2026-05-04T19:00:00Z",
        ]
        self.assertEqual(rc.main(argv), rc.EXIT_OK)
        second = json.loads(self._output_path().read_text())["registration_hash"]
        self.assertNotEqual(first, second)

    def test_different_git_sha_changes_hash(self):
        self.assertEqual(self._run(), rc.EXIT_OK)
        first = json.loads(self._output_path().read_text())["registration_hash"]
        self._output_path().unlink()
        argv = [
            "--input", str(self.input_path),
            "--reports-dir", str(self.reports_dir),
            "--git-commit-sha", "f" * 40,
            "--registration-timestamp", FIXED_TIMESTAMP,
        ]
        self.assertEqual(rc.main(argv), rc.EXIT_OK)
        second = json.loads(self._output_path().read_text())["registration_hash"]
        self.assertNotEqual(first, second)


# --------------------------------------------------------------------- #
# 3. Validation failure blocks write
# --------------------------------------------------------------------- #


class TestValidationFailureBlocksWrite(_BaseCase):
    def test_invalid_predicted_direction_rejected(self):
        bad = _valid_toml().replace(
            'predicted_direction = "long"',
            'predicted_direction = "moonshot"',
        )
        self.input_path.write_text(bad, encoding="utf-8")
        self.assertEqual(self._run(), rc.EXIT_USER_ERROR)
        self.assertFalse(self._output_path().exists())

    def test_missing_required_top_level_key_rejected(self):
        bad = _valid_toml().replace("horizon_days = 5\n", "")
        self.input_path.write_text(bad, encoding="utf-8")
        self.assertEqual(self._run(), rc.EXIT_USER_ERROR)
        self.assertFalse(self._output_path().exists())

    def test_invalid_candidate_id_pattern_rejected(self):
        bad = _valid_toml().replace(
            'candidate_id = "test-candidate-001"',
            'candidate_id = "Test_Candidate_001"',
        )
        self.input_path.write_text(bad, encoding="utf-8")
        self.assertEqual(self._run(), rc.EXIT_USER_ERROR)
        # Filename uses sanitized id so we just check no .json files written.
        self.assertEqual(list(self.reports_dir.glob("*.json")), [])

    def test_pre_filled_hash_field_rejected(self):
        # Prepend at the top so the key lands at the top level, not
        # inside a [section] (TOML sections are sticky).
        bad = 'registration_hash = "deadbeef"\n' + _valid_toml()
        self.input_path.write_text(bad, encoding="utf-8")
        self.assertEqual(self._run(), rc.EXIT_USER_ERROR)
        self.assertFalse(self._output_path().exists())

    def test_pre_filled_timestamp_rejected(self):
        bad = 'registration_timestamp = "2025-01-01T00:00:00Z"\n' + _valid_toml()
        self.input_path.write_text(bad, encoding="utf-8")
        self.assertEqual(self._run(), rc.EXIT_USER_ERROR)
        self.assertFalse(self._output_path().exists())

    def test_pre_filled_git_commit_rejected(self):
        bad = 'git_commit_sha = "abc123"\n' + _valid_toml()
        self.input_path.write_text(bad, encoding="utf-8")
        self.assertEqual(self._run(), rc.EXIT_USER_ERROR)
        self.assertFalse(self._output_path().exists())

    def test_invalid_toml_syntax_rejected(self):
        self.input_path.write_text("{not toml\n", encoding="utf-8")
        self.assertEqual(self._run(), rc.EXIT_USER_ERROR)
        self.assertFalse(self._output_path().exists())

    def test_missing_input_file_returns_user_error(self):
        argv = [
            "--input", str(self.tmp / "nonexistent.toml"),
            "--reports-dir", str(self.reports_dir),
            "--git-commit-sha", VALID_GIT_SHA,
            "--registration-timestamp", FIXED_TIMESTAMP,
        ]
        self.assertEqual(rc.main(argv), rc.EXIT_USER_ERROR)


# --------------------------------------------------------------------- #
# 4. Existing file prevents overwrite
# --------------------------------------------------------------------- #


class TestOverwriteBehavior(_BaseCase):
    def test_default_refuses_overwrite(self):
        self.assertEqual(self._run(), rc.EXIT_OK)
        # Second run with identical inputs should still refuse — the
        # protection is path-existence, not content-difference.
        self.assertEqual(self._run(), rc.EXIT_OVERWRITE_REFUSED)

    def test_allow_overwrite_flag_succeeds(self):
        self.assertEqual(self._run(), rc.EXIT_OK)
        self.assertEqual(self._run("--allow-overwrite"), rc.EXIT_OK)

    def test_overwrite_refusal_does_not_corrupt_existing_file(self):
        self.assertEqual(self._run(), rc.EXIT_OK)
        original = self._output_path().read_text()
        self.assertEqual(self._run(), rc.EXIT_OVERWRITE_REFUSED)
        self.assertEqual(self._output_path().read_text(), original)


# --------------------------------------------------------------------- #
# 5. Git SHA correctly injected (mock subprocess)
# --------------------------------------------------------------------- #


class TestGitShaInjection(unittest.TestCase):
    def test_subprocess_success_returns_sha(self):
        with mock.patch("scripts.register_candidate.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess(
                args=["git", "rev-parse", "HEAD"],
                returncode=0,
                stdout=VALID_GIT_SHA + "\n",
                stderr="",
            )
            sha = rc.detect_git_commit_sha()
            self.assertEqual(sha, VALID_GIT_SHA)
            run.assert_called_once()
            called_args = run.call_args.args[0]
            self.assertEqual(called_args, ["git", "rev-parse", "HEAD"])

    def test_subprocess_nonzero_returncode_raises(self):
        with mock.patch("scripts.register_candidate.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess(
                args=["git", "rev-parse", "HEAD"],
                returncode=128,
                stdout="",
                stderr="fatal: not a git repository\n",
            )
            with self.assertRaises(RuntimeError) as ctx:
                rc.detect_git_commit_sha()
            self.assertIn("git rev-parse HEAD failed", str(ctx.exception))

    def test_subprocess_non_hex_output_raises(self):
        with mock.patch("scripts.register_candidate.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="not-a-real-sha\n",
                stderr="",
            )
            with self.assertRaises(RuntimeError):
                rc.detect_git_commit_sha()

    def test_subprocess_short_output_raises(self):
        with mock.patch("scripts.register_candidate.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="abc\n",
                stderr="",
            )
            with self.assertRaises(RuntimeError):
                rc.detect_git_commit_sha()


class TestEndToEndWithMockedGit(unittest.TestCase):
    """Exercise main() without --git-commit-sha so the subprocess path runs."""

    def setUp(self) -> None:
        self._tmp_ctx = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_ctx.name).resolve()
        self.input_path = self.tmp / "candidate.toml"
        self.input_path.write_text(_valid_toml(), encoding="utf-8")
        self.reports_dir = self.tmp / "registrations"

    def tearDown(self) -> None:
        self._tmp_ctx.cleanup()

    def test_main_uses_subprocess_when_no_override(self):
        with mock.patch("scripts.register_candidate.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=VALID_GIT_SHA + "\n",
                stderr="",
            )
            argv = [
                "--input", str(self.input_path),
                "--reports-dir", str(self.reports_dir),
                "--registration-timestamp", FIXED_TIMESTAMP,
            ]
            self.assertEqual(rc.main(argv), rc.EXIT_OK)
            run.assert_called_once()
            payload = json.loads(
                (self.reports_dir / "test-candidate-001.json").read_text()
            )
            self.assertEqual(payload["git_commit_sha"], VALID_GIT_SHA)


# --------------------------------------------------------------------- #
# Misc guardrails
# --------------------------------------------------------------------- #


class TestDryRun(_BaseCase):
    def test_dry_run_does_not_write(self):
        self.assertEqual(self._run("--dry-run"), rc.EXIT_OK)
        self.assertFalse(self._output_path().exists())

    def test_dry_run_with_invalid_input_still_blocks(self):
        bad = _valid_toml().replace(
            'predicted_direction = "long"',
            'predicted_direction = "moonshot"',
        )
        self.input_path.write_text(bad, encoding="utf-8")
        self.assertEqual(self._run("--dry-run"), rc.EXIT_USER_ERROR)


class TestTimestampFormat(unittest.TestCase):
    def test_utc_now_iso_returns_z_suffixed_string(self):
        ts = rc.utc_now_iso()
        self.assertTrue(ts.endswith("Z"))
        self.assertEqual(len(ts), len("YYYY-MM-DDTHH:MM:SSZ"))


if __name__ == "__main__":
    unittest.main()
