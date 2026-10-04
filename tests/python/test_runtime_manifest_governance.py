from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import io
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def load_ml_server():
    name = "ml_server_governance_test"
    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "server" / "ml_server.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load ml_server")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _Request:
    def __init__(self, payload: dict):
        self._body = json.dumps(payload).encode("utf-8")
        self.headers = {"content-length": str(len(self._body))}

    async def body(self) -> bytes:
        return self._body


class RuntimeManifestGovernanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_ml_server()

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="pq_manifest_governance_")
        self.models_dir = Path(self.tmp.name)
        self.module.MODEL_DIR = self.models_dir
        self.module.RF_ACTIVE_MANIFEST = "manifest_active.json"
        self.module.RF_CANDIDATE_MANIFEST = "manifest_candidate.json"
        self.module.RF_GOVERNANCE_STATE = "model_registry.json"
        self.module.RF_MANIFEST_PATH = ""
        self.module._startup_error = None

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_json(self, name: str, payload: dict) -> Path:
        path = self.models_dir / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_resolver_never_falls_back_to_candidate(self) -> None:
        candidate = self._write_json("manifest_candidate.json", {"version": "v999"})
        resolved = self.module.ModelRegistry().resolve_manifest_path()
        self.assertEqual(resolved, self.models_dir / "manifest_active.json")
        self.assertNotEqual(resolved, candidate)

    def test_resolver_rejects_arbitrary_manifest_override(self) -> None:
        override = self._write_json("manual.json", {"version": "v999"})
        self.module.RF_MANIFEST_PATH = str(override)
        with self.assertRaisesRegex(ValueError, "RF_MANIFEST_PATH"):
            self.module.ModelRegistry().resolve_manifest_path()

    def test_load_rejects_manifest_not_matching_governance_active_version(self) -> None:
        self._write_json("manifest_active.json", {"version": "v002", "models": {}})
        self._write_json("model_registry.json", {"active_version": "v001"})
        with self.assertRaisesRegex(ValueError, "governance active version"):
            self.module.ModelRegistry().load()

    def test_load_rejects_missing_governance_registry(self) -> None:
        self._write_json("manifest_active.json", {"version": "v001", "models": {}})
        with self.assertRaisesRegex(FileNotFoundError, "model_registry.json"):
            self.module.ModelRegistry().load()

    def test_load_accepts_hash_verified_active_manifest(self) -> None:
        manifest_path = self._write_json(
            "manifest_active.json", {"version": "v001", "models": {}}
        )
        self._write_json(
            "model_registry.json",
            {
                "active_version": "v001",
                "active_manifest_sha256": hashlib.sha256(
                    manifest_path.read_bytes()
                ).hexdigest(),
                "active_artifact_sha256": {},
            },
        )
        registry = self.module.ModelRegistry()
        self.assertTrue(registry.load())
        self.assertEqual(registry.snapshot()["manifest"]["version"], "v001")
        self.module.registry = registry
        health = asyncio.run(self.module.health())
        self.assertTrue(health["runtime_identity"]["governance_verified"])
        self.assertEqual(health["runtime_identity"]["active_version"], "v001")
        self.assertEqual(
            health["runtime_identity"]["manifest_sha256"],
            hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        )

    def test_load_waits_for_atomic_governance_publication(self) -> None:
        active_path = self._write_json(
            "manifest_active.json", {"version": "v001", "models": {}}
        )
        state_path = self._write_json(
            "model_registry.json",
            {
                "active_version": "v001",
                "active_manifest_sha256": hashlib.sha256(
                    active_path.read_bytes()
                ).hexdigest(),
                "active_artifact_sha256": {},
            },
        )
        registry = self.module.ModelRegistry()
        self.assertTrue(registry.load())

        writer_started = threading.Event()
        allow_writer_finish = threading.Event()
        load_result: list[object] = []

        def writer() -> None:
            with self.module._governance_file_lock(exclusive=True):
                active_path.write_text(
                    json.dumps({"version": "v002", "models": {}}),
                    encoding="utf-8",
                )
                writer_started.set()
                allow_writer_finish.wait(timeout=2)
                state_path.write_text(
                    json.dumps(
                        {
                            "active_version": "v002",
                            "active_manifest_sha256": hashlib.sha256(
                                active_path.read_bytes()
                            ).hexdigest(),
                            "active_artifact_sha256": {},
                        }
                    ),
                    encoding="utf-8",
                )

        def reader() -> None:
            try:
                load_result.append(registry.load(force=True))
            except Exception as exc:  # pragma: no cover - assertion reports it
                load_result.append(exc)

        writer_thread = threading.Thread(target=writer)
        writer_thread.start()
        self.assertTrue(writer_started.wait(timeout=2))
        reader_thread = threading.Thread(target=reader)
        reader_thread.start()
        time.sleep(0.05)
        self.assertTrue(reader_thread.is_alive())
        allow_writer_finish.set()
        writer_thread.join(timeout=2)
        reader_thread.join(timeout=2)

        self.assertEqual(load_result, [True])
        self.assertEqual(registry.manifest["version"], "v002")

    def test_load_uses_configured_governance_state_path(self) -> None:
        manifest_path = self._write_json(
            "manifest_active.json", {"version": "v001", "models": {}}
        )
        self.module.RF_GOVERNANCE_STATE = "custom_registry.json"
        self._write_json(
            "custom_registry.json",
            {
                "active_version": "v001",
                "active_manifest_sha256": hashlib.sha256(
                    manifest_path.read_bytes()
                ).hexdigest(),
                "active_artifact_sha256": {},
            },
        )
        self.assertTrue(self.module.ModelRegistry().load())

    def _joblib_bytes(self, payload: dict) -> bytes:
        handle = io.BytesIO()
        self.module.joblib.dump(payload, handle)
        return handle.getvalue()

    def test_load_deserializes_the_exact_bytes_that_were_hash_verified(self) -> None:
        artifact = self.models_dir / "rf_reject_5m_v001.pkl"
        approved_bytes = self._joblib_bytes(
            {"marker": "approved", "optimal_threshold": 0.7}
        )
        replacement_bytes = self._joblib_bytes(
            {"marker": "replacement", "optimal_threshold": 0.1}
        )
        artifact.write_bytes(approved_bytes)
        manifest_path = self._write_json(
            "manifest_active.json",
            {
                "version": "v001",
                "models": {"reject": {"5": artifact.name}},
            },
        )
        self._write_json(
            "model_registry.json",
            {
                "active_version": "v001",
                "active_manifest_sha256": hashlib.sha256(
                    manifest_path.read_bytes()
                ).hexdigest(),
                "active_artifact_sha256": {
                    artifact.name: hashlib.sha256(approved_bytes).hexdigest()
                },
            },
        )
        registry = self.module.ModelRegistry()
        original_validate = registry._validate_governance_approval

        def replace_after_validation(*args):
            result = original_validate(*args)
            artifact.write_bytes(replacement_bytes)
            return result

        registry._validate_governance_approval = replace_after_validation
        self.assertTrue(registry.load())
        self.assertEqual(registry.models["reject"][5]["marker"], "approved")
        self.assertEqual(artifact.read_bytes(), replacement_bytes)

    def test_load_rejects_tampered_approved_artifact(self) -> None:
        artifact = self.models_dir / "rf_reject_5m_v001.pkl"
        artifact.write_bytes(b"tampered")
        manifest_path = self._write_json(
            "manifest_active.json",
            {
                "version": "v001",
                "models": {"reject": {"5": artifact.name}},
            },
        )
        self._write_json(
            "model_registry.json",
            {
                "active_version": "v001",
                "active_manifest_sha256": hashlib.sha256(
                    manifest_path.read_bytes()
                ).hexdigest(),
                "active_artifact_sha256": {artifact.name: "0" * 64},
            },
        )
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            self.module.ModelRegistry().load()

    def test_score_fails_closed_when_runtime_governance_is_invalid(self) -> None:
        module = self.module
        module._startup_error = "active manifest does not match governance active version"
        module.registry.manifest = {"version": "v001"}
        module.registry.models = {"reject": {5: object()}, "break": {}}
        module.serving_state._state = "active"
        request = _Request({"event": {"ts_event": 1773154800000}})

        with self.assertRaises(module.HTTPException) as ctx:
            asyncio.run(module.score(request))
        self.assertEqual(ctx.exception.status_code, 503)
        self.assertIn("governance", str(ctx.exception.detail).lower())


if __name__ == "__main__":
    unittest.main()
