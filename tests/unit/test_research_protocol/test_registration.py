"""Tests for services.research_protocol.registration (RESEARCH_PROTOCOL §1, §7.2)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from services.research_protocol._paths import ENV_PROTOCOL_ROOT, registrations_dir
from services.research_protocol.errors import (
    RegistrationHashMismatchError,
    RegistrationInvalidError,
    RegistrationMissingError,
)
from services.research_protocol.registration import (
    HASH_FIELD,
    Registration,
    canonical_json,
    compute_registration_hash,
    load_registration,
    registration_path,
)


def _valid_payload(candidate_id: str = "test-candidate-001") -> dict:
    payload = {
        "candidate_id": candidate_id,
        "registration_timestamp": "2026-05-04T18:00:00Z",
        "git_commit_sha": "0" * 40,
        "hypothesis": {
            "mechanism": "explanatory mechanism",
            "predicted_direction": "long",
            "why_might_fail": "regime change",
            "citations": ["paper:abc"],
        },
        "features": [{"name": "f1", "input_columns": ["close"]}],
        "thresholds": [{"name": "t1", "kind": "fixed", "value": 0.7}],
        "transformations": {"allowed": ["log"], "forbidden_unless_listed": ["x"]},
        "forbidden_changes": ["any threshold change"],
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


class TestCanonicalJsonAndHash(_ProtocolRootBase):
    def test_canonical_json_is_sorted_and_compact(self):
        payload = {"b": 1, "a": [3, 2, 1], "c": {"y": 1, "x": 2}}
        s = canonical_json(payload)
        self.assertEqual(s, '{"a":[3,2,1],"b":1,"c":{"x":2,"y":1}}')

    def test_hash_is_deterministic_under_key_reordering(self):
        p1 = _valid_payload()
        p2 = {k: p1[k] for k in reversed(list(p1))}
        self.assertEqual(
            compute_registration_hash(p1),
            compute_registration_hash(p2),
        )

    def test_hash_excludes_the_hash_field_itself(self):
        p1 = _valid_payload()
        p2 = dict(p1)
        p2[HASH_FIELD] = "deadbeef" * 8
        # Changing only the hash field must NOT affect the recomputed hash:
        self.assertEqual(
            compute_registration_hash(p1),
            compute_registration_hash(p2),
        )

    def test_hash_changes_when_any_other_field_changes(self):
        p1 = _valid_payload()
        p2 = dict(p1)
        p2["random_seed"] = 43
        self.assertNotEqual(
            compute_registration_hash(p1),
            compute_registration_hash(p2),
        )


class TestLoadRegistration(_ProtocolRootBase):
    def test_missing_file_raises(self):
        with self.assertRaises(RegistrationMissingError):
            load_registration("never-registered-id")

    def test_valid_registration_loads(self):
        payload = _valid_payload()
        _write_registration(self.tmp, payload)
        reg = load_registration(payload["candidate_id"])
        self.assertIsInstance(reg, Registration)
        self.assertEqual(reg.candidate_id, payload["candidate_id"])
        self.assertEqual(reg.registration_hash, payload[HASH_FIELD])
        self.assertEqual(reg.horizon_days, 5)
        self.assertEqual(reg.random_seed, 42)
        self.assertEqual(reg.stages_required, [1, 2, 3, 4, 5, 6])

    def test_post_signing_edit_raises_hash_mismatch(self):
        payload = _valid_payload()
        path = _write_registration(self.tmp, payload)
        # Edit the file to flip a non-hash field; do not recompute the hash.
        on_disk = json.loads(path.read_text())
        on_disk["random_seed"] = 99
        path.write_text(json.dumps(on_disk, indent=2), encoding="utf-8")
        with self.assertRaises(RegistrationHashMismatchError):
            load_registration(payload["candidate_id"])

    def test_invalid_json_raises_invalid(self):
        regs = self.tmp / "registrations"
        regs.mkdir(parents=True, exist_ok=True)
        (regs / "bad-json.json").write_text("{not json", encoding="utf-8")
        with self.assertRaises(RegistrationInvalidError):
            load_registration("bad-json")

    def test_top_level_array_rejected(self):
        regs = self.tmp / "registrations"
        regs.mkdir(parents=True, exist_ok=True)
        (regs / "array-top.json").write_text("[1,2,3]", encoding="utf-8")
        with self.assertRaises(RegistrationInvalidError):
            load_registration("array-top")

    def test_filename_id_must_match_payload_id(self):
        payload = _valid_payload(candidate_id="real-id")
        regs = self.tmp / "registrations"
        regs.mkdir(parents=True, exist_ok=True)
        # Place under a different filename:
        path = regs / "renamed-id.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        with self.assertRaises(RegistrationInvalidError):
            load_registration("renamed-id")


class TestSchemaValidation(_ProtocolRootBase):
    def _write_and_load(self, payload: dict) -> None:
        _write_registration(self.tmp, payload)
        load_registration(payload["candidate_id"])

    def test_missing_top_level_key_rejected(self):
        payload = _valid_payload()
        del payload["features"]
        # The hash field's value will not match either, but schema check
        # runs first and raises the schema error.
        with self.assertRaises(RegistrationInvalidError):
            self._write_and_load(payload)

    def test_invalid_candidate_id_rejected(self):
        payload = _valid_payload(candidate_id="Not-Kebab")
        payload[HASH_FIELD] = compute_registration_hash(payload)
        with self.assertRaises(RegistrationInvalidError):
            self._write_and_load(payload)

    def test_negative_horizon_rejected(self):
        payload = _valid_payload()
        payload["horizon_days"] = 0
        payload[HASH_FIELD] = compute_registration_hash(payload)
        with self.assertRaises(RegistrationInvalidError):
            self._write_and_load(payload)

    def test_stages_required_out_of_range_rejected(self):
        payload = _valid_payload()
        payload["stages_required"] = [0, 1, 2]
        payload[HASH_FIELD] = compute_registration_hash(payload)
        with self.assertRaises(RegistrationInvalidError):
            self._write_and_load(payload)

    def test_predicted_direction_must_be_allowed(self):
        payload = _valid_payload()
        payload["hypothesis"]["predicted_direction"] = "moonshot"
        payload[HASH_FIELD] = compute_registration_hash(payload)
        with self.assertRaises(RegistrationInvalidError):
            self._write_and_load(payload)

    def test_short_hash_rejected(self):
        payload = _valid_payload()
        payload[HASH_FIELD] = "abc"
        with self.assertRaises(RegistrationInvalidError):
            self._write_and_load(payload)

    def test_empty_features_rejected(self):
        payload = _valid_payload()
        payload["features"] = []
        payload[HASH_FIELD] = compute_registration_hash(payload)
        with self.assertRaises(RegistrationInvalidError):
            self._write_and_load(payload)


class TestRegistrationPath(_ProtocolRootBase):
    def test_path_is_under_registrations_dir(self):
        path = registration_path("foo-bar")
        self.assertEqual(path.parent, registrations_dir())
        self.assertEqual(path.name, "foo-bar.json")


if __name__ == "__main__":
    unittest.main()
