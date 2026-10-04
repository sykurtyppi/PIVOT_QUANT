from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[2]


def load_governance():
    name = "model_governance_hash_test"
    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(
        name, REPO_ROOT / "scripts" / "model_governance.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load model_governance")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class GovernanceApprovalHashTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_governance()

    def test_build_approval_hashes_covers_manifest_and_every_artifact(self) -> None:
        with tempfile.TemporaryDirectory(prefix="pq_governance_hash_") as tmp:
            models_dir = Path(tmp)
            artifact_a = models_dir / "reject.pkl"
            artifact_b = models_dir / "break.pkl"
            artifact_a.write_bytes(b"reject-model")
            artifact_b.write_bytes(b"break-model")
            manifest = {
                "version": "v001",
                "models": {
                    "reject": {"5": artifact_a.name},
                    "break": {"15": artifact_b.name},
                },
            }
            manifest_path = models_dir / "manifest_active.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            hashes = self.module.build_approval_hashes(
                models_dir, manifest_path, manifest
            )

            self.assertEqual(
                hashes["active_manifest_sha256"],
                hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                hashes["active_artifact_sha256"],
                {
                    artifact_a.name: hashlib.sha256(
                        artifact_a.read_bytes()
                    ).hexdigest(),
                    artifact_b.name: hashlib.sha256(
                        artifact_b.read_bytes()
                    ).hexdigest(),
                },
            )

    def test_build_approval_hashes_rejects_missing_artifact(self) -> None:
        with tempfile.TemporaryDirectory(prefix="pq_governance_hash_") as tmp:
            models_dir = Path(tmp)
            manifest = {
                "version": "v001",
                "models": {"reject": {"5": "missing.pkl"}},
            }
            manifest_path = models_dir / "manifest_active.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(FileNotFoundError, "missing.pkl"):
                self.module.build_approval_hashes(
                    models_dir, manifest_path, manifest
                )

    def _evaluate_args(self, models_dir: Path) -> argparse.Namespace:
        return self.module.build_parser().parse_args(
            [
                "--models-dir",
                str(models_dir),
                "--candidate-manifest",
                "manifest_candidate.json",
                "--active-manifest",
                "manifest_active.json",
                "--prev-active-manifest",
                "manifest_active_prev.json",
                "--state-file",
                "model_registry.json",
                "--ops-db",
                "",
                "evaluate",
                "--no-require-oos-validation",
            ]
        )

    def test_bootstrap_records_hashes_for_new_active_manifest(self) -> None:
        with tempfile.TemporaryDirectory(prefix="pq_governance_bootstrap_") as tmp:
            models_dir = Path(tmp)
            artifact = models_dir / "v001.pkl"
            artifact.write_bytes(b"model-v1")
            candidate = {
                "version": "v001",
                "models": {"reject": {"5": artifact.name}},
            }
            (models_dir / "manifest_candidate.json").write_text(
                json.dumps(candidate), encoding="utf-8"
            )
            with mock.patch.object(self.module, "validate_manifest", return_value=[]):
                self.assertEqual(
                    self.module.cmd_evaluate(self._evaluate_args(models_dir)), 0
                )

            active_path = models_dir / "manifest_active.json"
            state = json.loads((models_dir / "model_registry.json").read_text())
            self.assertEqual(
                state["active_manifest_sha256"],
                hashlib.sha256(active_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                state["active_artifact_sha256"],
                {artifact.name: hashlib.sha256(artifact.read_bytes()).hexdigest()},
            )

    def test_same_version_evaluation_backfills_missing_approval_hashes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="pq_governance_migrate_") as tmp:
            models_dir = Path(tmp)
            artifact = models_dir / "v001.pkl"
            artifact.write_bytes(b"model-v1")
            manifest = {
                "version": "v001",
                "models": {"reject": {"5": artifact.name}},
            }
            encoded = json.dumps(manifest)
            (models_dir / "manifest_candidate.json").write_text(encoded, encoding="utf-8")
            active_path = models_dir / "manifest_active.json"
            active_path.write_text(encoded, encoding="utf-8")
            (models_dir / "model_registry.json").write_text(
                json.dumps({"active_version": "v001"}), encoding="utf-8"
            )
            with mock.patch.object(self.module, "validate_manifest", return_value=[]):
                self.assertEqual(
                    self.module.cmd_evaluate(self._evaluate_args(models_dir)), 0
                )

            state = json.loads((models_dir / "model_registry.json").read_text())
            self.assertEqual(
                state["active_manifest_sha256"],
                hashlib.sha256(active_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                state["active_artifact_sha256"],
                {artifact.name: hashlib.sha256(artifact.read_bytes()).hexdigest()},
            )

    def _rollback_args(
        self, models_dir: Path, *, ops_db: str | None = None
    ) -> argparse.Namespace:
        return argparse.Namespace(
            models_dir=str(models_dir),
            metadata_dir="metadata_runtime",
            active_manifest="manifest_active.json",
            prev_active_manifest="manifest_active_prev.json",
            state_file="model_registry.json",
            ops_db=ops_db,
            to_version=None,
        )

    def test_default_rollback_swaps_active_and_previous_and_refreshes_hashes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="pq_governance_rollback_") as tmp:
            models_dir = Path(tmp)
            (models_dir / "v001.pkl").write_bytes(b"model-v1")
            (models_dir / "v002.pkl").write_bytes(b"model-v2")
            active = {"version": "v002", "models": {"reject": {"5": "v002.pkl"}}}
            previous = {"version": "v001", "models": {"reject": {"5": "v001.pkl"}}}
            active_path = models_dir / "manifest_active.json"
            previous_path = models_dir / "manifest_active_prev.json"
            state_path = models_dir / "model_registry.json"
            active_path.write_text(json.dumps(active), encoding="utf-8")
            previous_path.write_text(json.dumps(previous), encoding="utf-8")
            state_path.write_text(
                json.dumps({"active_version": "v002", "previous_active_version": "v001"}),
                encoding="utf-8",
            )

            self.assertEqual(self.module.cmd_rollback(self._rollback_args(models_dir)), 0)

            self.assertEqual(json.loads(active_path.read_text())["version"], "v001")
            self.assertEqual(json.loads(previous_path.read_text())["version"], "v002")
            state = json.loads(state_path.read_text())
            self.assertEqual(state["active_version"], "v001")
            self.assertEqual(state["previous_active_version"], "v002")
            self.assertEqual(
                state["active_manifest_sha256"],
                hashlib.sha256(active_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                state["active_artifact_sha256"],
                {"v001.pkl": hashlib.sha256(b"model-v1").hexdigest()},
            )

    def test_ops_telemetry_failure_does_not_break_committed_rollback(self) -> None:
        with tempfile.TemporaryDirectory(prefix="pq_governance_rollback_") as tmp:
            models_dir = Path(tmp)
            (models_dir / "v001.pkl").write_bytes(b"model-v1")
            (models_dir / "v002.pkl").write_bytes(b"model-v2")
            active_path = models_dir / "manifest_active.json"
            previous_path = models_dir / "manifest_active_prev.json"
            state_path = models_dir / "model_registry.json"
            active_path.write_text(
                json.dumps({"version": "v002", "models": {"reject": {"5": "v002.pkl"}}}),
                encoding="utf-8",
            )
            previous_path.write_text(
                json.dumps({"version": "v001", "models": {"reject": {"5": "v001.pkl"}}}),
                encoding="utf-8",
            )
            state_path.write_text(
                json.dumps({"active_version": "v002", "previous_active_version": "v001"}),
                encoding="utf-8",
            )

            with mock.patch.object(
                self.module, "_ops_set", side_effect=RuntimeError("ops unavailable")
            ):
                self.assertEqual(
                    self.module.cmd_rollback(
                        self._rollback_args(models_dir, ops_db="unavailable.sqlite")
                    ),
                    0,
                )

            self.assertEqual(json.loads(active_path.read_text())["version"], "v001")
            self.assertEqual(json.loads(state_path.read_text())["active_version"], "v001")

    def test_registry_write_failure_restores_active_and_previous_manifests(self) -> None:
        with tempfile.TemporaryDirectory(prefix="pq_governance_rollback_") as tmp:
            models_dir = Path(tmp)
            (models_dir / "v001.pkl").write_bytes(b"model-v1")
            (models_dir / "v002.pkl").write_bytes(b"model-v2")
            active_path = models_dir / "manifest_active.json"
            previous_path = models_dir / "manifest_active_prev.json"
            state_path = models_dir / "model_registry.json"
            active_path.write_text(
                json.dumps({"version": "v002", "models": {"reject": {"5": "v002.pkl"}}}),
                encoding="utf-8",
            )
            previous_path.write_text(
                json.dumps({"version": "v001", "models": {"reject": {"5": "v001.pkl"}}}),
                encoding="utf-8",
            )
            state_path.write_text(
                json.dumps({"active_version": "v002", "previous_active_version": "v001"}),
                encoding="utf-8",
            )
            active_before = active_path.read_bytes()
            previous_before = previous_path.read_bytes()
            state_before = state_path.read_bytes()

            with mock.patch.object(
                self.module, "atomic_write_json", side_effect=OSError("disk full")
            ):
                with self.assertRaisesRegex(OSError, "disk full"):
                    self.module.cmd_rollback(self._rollback_args(models_dir))

            self.assertEqual(active_path.read_bytes(), active_before)
            self.assertEqual(previous_path.read_bytes(), previous_before)
            self.assertEqual(state_path.read_bytes(), state_before)

    def test_rollback_missing_artifact_does_not_mutate_manifests(self) -> None:
        with tempfile.TemporaryDirectory(prefix="pq_governance_rollback_") as tmp:
            models_dir = Path(tmp)
            (models_dir / "v002.pkl").write_bytes(b"model-v2")
            active_path = models_dir / "manifest_active.json"
            previous_path = models_dir / "manifest_active_prev.json"
            state_path = models_dir / "model_registry.json"
            active_path.write_text(
                json.dumps({"version": "v002", "models": {"reject": {"5": "v002.pkl"}}}),
                encoding="utf-8",
            )
            previous_path.write_text(
                json.dumps({"version": "v001", "models": {"reject": {"5": "missing.pkl"}}}),
                encoding="utf-8",
            )
            state_path.write_text(
                json.dumps({"active_version": "v002", "previous_active_version": "v001"}),
                encoding="utf-8",
            )
            active_before = active_path.read_bytes()
            previous_before = previous_path.read_bytes()

            with self.assertRaisesRegex(FileNotFoundError, "missing.pkl"):
                self.module.cmd_rollback(self._rollback_args(models_dir))

            self.assertEqual(active_path.read_bytes(), active_before)
            self.assertEqual(previous_path.read_bytes(), previous_before)


if __name__ == "__main__":
    unittest.main()
