"""Tests for services.research_protocol.kill_list (RESEARCH_PROTOCOL §6)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from services.research_protocol._paths import ENV_PROTOCOL_ROOT, kill_list_path
from services.research_protocol.errors import (
    CandidateKilledError,
    KillListTamperingError,
)
from services.research_protocol.kill_list import (
    KILL_LIST_VERSION,
    KillEntry,
    assert_not_killed,
    is_killed,
    list_killed,
    record_kill,
)

VALID_HASH = "a" * 64


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


class TestEmptyState(_ProtocolRootBase):
    def test_no_file_returns_empty_list(self):
        self.assertEqual(list_killed(), [])

    def test_no_file_is_killed_returns_false(self):
        self.assertFalse(is_killed("anything"))

    def test_no_file_assert_not_killed_passes(self):
        # Should not raise.
        assert_not_killed("anything")


class TestRecordKill(_ProtocolRootBase):
    def test_record_creates_kill_entry(self):
        entry = record_kill(
            candidate_id="cand-1",
            registration_hash=VALID_HASH,
            stage=3,
            reason="cross-period failed",
        )
        self.assertIsInstance(entry, KillEntry)
        self.assertEqual(entry.candidate_id, "cand-1")
        self.assertEqual(entry.killed_at_stage, 3)
        self.assertEqual(entry.kill_reason, "cross-period failed")
        self.assertTrue(is_killed("cand-1"))

    def test_record_kill_persists_to_disk(self):
        record_kill(
            candidate_id="cand-1",
            registration_hash=VALID_HASH,
            stage=2,
            reason="oos failed",
            artifacts=["reports/x.json"],
        )
        on_disk = json.loads(kill_list_path().read_text())
        self.assertEqual(on_disk["version"], KILL_LIST_VERSION)
        self.assertEqual(len(on_disk["entries"]), 1)
        self.assertEqual(on_disk["entries"][0]["candidate_id"], "cand-1")
        self.assertEqual(
            on_disk["entries"][0]["supporting_artifacts"], ["reports/x.json"]
        )

    def test_record_kill_is_idempotent_on_candidate_id(self):
        first = record_kill(
            candidate_id="cand-1",
            registration_hash=VALID_HASH,
            stage=3,
            reason="first kill",
        )
        second = record_kill(
            candidate_id="cand-1",
            registration_hash=VALID_HASH,
            stage=5,           # different stage
            reason="second attempt with different reason",
        )
        # The original entry is preserved; the second call returns it.
        self.assertEqual(second.killed_at_stage, first.killed_at_stage)
        self.assertEqual(second.kill_reason, first.kill_reason)
        on_disk = json.loads(kill_list_path().read_text())
        self.assertEqual(len(on_disk["entries"]), 1)

    def test_invalid_stage_rejected(self):
        for bad in (0, 7, -1, "3"):
            with self.subTest(bad=bad):
                with self.assertRaises(KillListTamperingError):
                    record_kill(
                        candidate_id="cand-1",
                        registration_hash=VALID_HASH,
                        stage=bad,  # type: ignore[arg-type]
                        reason="oops",
                    )

    def test_invalid_hash_rejected(self):
        with self.assertRaises(KillListTamperingError):
            record_kill(
                candidate_id="cand-1",
                registration_hash="abc",   # too short
                stage=3,
                reason="r",
            )

    def test_empty_reason_rejected(self):
        with self.assertRaises(KillListTamperingError):
            record_kill(
                candidate_id="cand-1",
                registration_hash=VALID_HASH,
                stage=3,
                reason="   ",
            )

    def test_empty_candidate_id_rejected(self):
        with self.assertRaises(KillListTamperingError):
            record_kill(
                candidate_id="",
                registration_hash=VALID_HASH,
                stage=3,
                reason="r",
            )


class TestAssertNotKilled(_ProtocolRootBase):
    def test_raises_when_candidate_killed(self):
        record_kill(
            candidate_id="dead-cand",
            registration_hash=VALID_HASH,
            stage=3,
            reason="cross-period falsified",
        )
        with self.assertRaises(CandidateKilledError) as ctx:
            assert_not_killed("dead-cand")
        msg = str(ctx.exception)
        self.assertIn("dead-cand", msg)
        self.assertIn("stage=3", msg)
        self.assertIn("cross-period falsified", msg)

    def test_passes_when_other_candidate_killed(self):
        record_kill(
            candidate_id="other",
            registration_hash=VALID_HASH,
            stage=3,
            reason="r",
        )
        # different id is not on the list
        assert_not_killed("alive-cand")


class TestTamperingDetection(_ProtocolRootBase):
    def test_invalid_json_rejected(self):
        path = kill_list_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not json", encoding="utf-8")
        with self.assertRaises(KillListTamperingError):
            list_killed()

    def test_wrong_version_rejected(self):
        path = kill_list_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"version": 99, "entries": []}), encoding="utf-8"
        )
        with self.assertRaises(KillListTamperingError):
            list_killed()

    def test_top_level_array_rejected(self):
        path = kill_list_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("[]", encoding="utf-8")
        with self.assertRaises(KillListTamperingError):
            list_killed()

    def test_entries_not_a_list_rejected(self):
        path = kill_list_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"version": KILL_LIST_VERSION, "entries": {}}),
            encoding="utf-8",
        )
        with self.assertRaises(KillListTamperingError):
            list_killed()


class TestNoRevivalAPI(unittest.TestCase):
    """Surface contract test: there is no public function to remove a kill."""

    def test_no_remove_function_exposed(self):
        from services.research_protocol import kill_list as mod
        for name in (
            "remove",
            "remove_kill",
            "delete",
            "delete_kill",
            "clear",
            "reset",
        ):
            self.assertFalse(
                hasattr(mod, name),
                msg=f"kill_list must not expose {name}() — revival is prohibited",
            )

    def test_no_remove_in_package_exports(self):
        import services.research_protocol as pkg
        for name in pkg.__all__:
            self.assertNotIn(
                "remove", name.lower(),
                msg=f"package __all__ exposes a removal-like name: {name}",
            )
            self.assertNotIn(
                "delete", name.lower(),
                msg=f"package __all__ exposes a deletion-like name: {name}",
            )


if __name__ == "__main__":
    unittest.main()
