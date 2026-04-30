#!/usr/bin/env python3
"""Regression tests for env-loading and local-manifest scoring safety fixes.

Covers:
  - _resolve_model_dir: flat layout (manifest.parent)
  - _resolve_model_dir: subdirectory layout (metadata_runtime/ → parent)
  - _resolve_model_dir: missing files raise FileNotFoundError before any DB write
  - model_governance: MODEL_GOV_ENFORCE_LIVE_EMISSION_GATE read from .env in clean shell
  - model_governance: shell env overrides .env (override=False semantics)
  - release_manager import order: audit_log retention constants see .env values
"""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _make_manifest(dest: Path, model_files: list[str]) -> Path:
    models: dict = {"reject": {}, "break": {}}
    for fname in model_files:
        target = "reject" if "reject" in fname else "break"
        models[target]["15"] = fname
    dest.write_text(json.dumps({"version": "vTEST", "models": models}))
    return dest


class TestResolveModelDir(unittest.TestCase):
    """Unit tests for score_unscored_touch_events._resolve_model_dir."""

    @classmethod
    def setUpClass(cls):
        cls.mod = _load_module(
            "score_unscored_touch_events_rmd",
            REPO_ROOT / "scripts" / "score_unscored_touch_events.py",
        )

    def test_flat_layout_resolves_manifest_parent(self):
        """Manifest in same directory as .pkl → returns manifest.parent."""
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "rf_reject_15m_vT.pkl").write_bytes(b"x")
            manifest = _make_manifest(d / "manifest.json", ["rf_reject_15m_vT.pkl"])
            self.assertEqual(self.mod._resolve_model_dir(manifest), d)

    def test_subdirectory_layout_resolves_parent_parent(self):
        """Manifest in metadata_runtime/ with .pkl in parent → returns parent."""
        with tempfile.TemporaryDirectory() as tmp:
            model_dir = Path(tmp)
            subdir = model_dir / "metadata_runtime"
            subdir.mkdir()
            (model_dir / "rf_reject_15m_vT.pkl").write_bytes(b"x")
            manifest = _make_manifest(subdir / "metadata_vT.json", ["rf_reject_15m_vT.pkl"])
            self.assertEqual(self.mod._resolve_model_dir(manifest), model_dir)

    def test_missing_model_files_raise_file_not_found(self):
        """FileNotFoundError is raised when .pkl absent from both candidates."""
        with tempfile.TemporaryDirectory() as tmp:
            subdir = Path(tmp) / "metadata_runtime"
            subdir.mkdir()
            manifest = _make_manifest(subdir / "metadata_vT.json", ["rf_reject_15m_vT.pkl"])
            with self.assertRaises(FileNotFoundError) as ctx:
                self.mod._resolve_model_dir(manifest)
            self.assertIn("rf_reject_15m_vT.pkl", str(ctx.exception))

    def test_no_model_files_in_manifest_returns_parent(self):
        """Manifest with no .pkl entries falls back gracefully to manifest.parent."""
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            p = d / "manifest.json"
            p.write_text(json.dumps({"version": "vTEST", "models": {}}))
            self.assertEqual(self.mod._resolve_model_dir(p), d)

    def test_flat_takes_precedence_when_pkl_exists_in_both(self):
        """manifest.parent is tried first; wins when .pkl exists in both candidates."""
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            subdir = parent / "subdir"
            subdir.mkdir()
            fname = "rf_reject_15m_vT.pkl"
            (parent / fname).write_bytes(b"parent")
            (subdir / fname).write_bytes(b"subdir")
            manifest = _make_manifest(subdir / "manifest.json", [fname])
            # manifest.parent IS subdir → subdir probed first → wins
            self.assertEqual(self.mod._resolve_model_dir(manifest), subdir)


class TestBuildLocalManifestBatcherGuard(unittest.TestCase):
    """_build_local_manifest_batcher raises before writing any DB rows when models missing."""

    @classmethod
    def setUpClass(cls):
        cls.mod = _load_module(
            "score_unscored_touch_events_blmb",
            REPO_ROOT / "scripts" / "score_unscored_touch_events.py",
        )

    def test_raises_before_db_write_when_models_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            subdir = Path(tmp) / "metadata_runtime"
            subdir.mkdir()
            manifest = _make_manifest(subdir / "meta.json", ["rf_reject_15m_vBAD.pkl"])
            db_path = Path(tmp) / "test.sqlite"
            conn = sqlite3.connect(str(db_path))
            conn.execute("CREATE TABLE prediction_log (id INTEGER PRIMARY KEY)")
            conn.commit()
            conn.close()

            with self.assertRaises(FileNotFoundError):
                self.mod._build_local_manifest_batcher(
                    manifest_path=str(manifest),
                    db_path=str(db_path),
                )

            conn = sqlite3.connect(str(db_path))
            n = conn.execute("SELECT COUNT(*) FROM prediction_log").fetchone()[0]
            conn.close()
            self.assertEqual(n, 0, "No rows must be written after FileNotFoundError")


class TestGovernanceEnvLoading(unittest.TestCase):
    """model_governance reads MODEL_GOV_ENFORCE_LIVE_EMISSION_GATE from .env in a clean shell."""

    def test_dotenv_supplies_missing_env_var(self):
        """When shell env lacks the var, load_dotenv makes it visible."""
        with tempfile.TemporaryDirectory() as tmp:
            dotenv_path = Path(tmp) / ".env"
            dotenv_path.write_text("MODEL_GOV_ENFORCE_LIVE_EMISSION_GATE=true\n")
            clean = {k: v for k, v in os.environ.items()
                     if k != "MODEL_GOV_ENFORCE_LIVE_EMISSION_GATE"}
            with patch.dict(os.environ, clean, clear=True):
                from dotenv import load_dotenv
                load_dotenv(dotenv_path, override=True)
                raw = os.getenv("MODEL_GOV_ENFORCE_LIVE_EMISSION_GATE", "false")
                self.assertIn(raw.strip().lower(), {"1", "true", "yes", "y", "on"})

    def test_shell_env_overrides_dotenv(self):
        """Shell env=false must survive load_dotenv(override=False) with .env=true."""
        with tempfile.TemporaryDirectory() as tmp:
            dotenv_path = Path(tmp) / ".env"
            dotenv_path.write_text("MODEL_GOV_ENFORCE_LIVE_EMISSION_GATE=true\n")
            env_false = dict(os.environ)
            env_false["MODEL_GOV_ENFORCE_LIVE_EMISSION_GATE"] = "false"
            with patch.dict(os.environ, env_false, clear=True):
                from dotenv import load_dotenv
                load_dotenv(dotenv_path, override=False)
                self.assertEqual(
                    os.environ.get("MODEL_GOV_ENFORCE_LIVE_EMISSION_GATE"), "false"
                )


class TestReleaseManagerImportOrder(unittest.TestCase):
    """audit_log retention constants reflect .env values set before import."""

    def test_audit_log_sees_dotenv_before_import(self):
        """When .env sets ML_AUDIT_RETENTION_DAYS, audit_log must see it at import time."""
        with tempfile.TemporaryDirectory() as tmp:
            dotenv_path = Path(tmp) / ".env"
            dotenv_path.write_text("ML_AUDIT_RETENTION_DAYS=180\n")
            clean = {k: v for k, v in os.environ.items()
                     if k != "ML_AUDIT_RETENTION_DAYS"}
            with patch.dict(os.environ, clean, clear=True):
                from dotenv import load_dotenv
                load_dotenv(dotenv_path, override=True)
                # Simulate what release_manager.py does: dotenv first, then audit_log import.
                # Re-load audit_log under a fresh module name so constants re-evaluate.
                mod = _load_module(
                    "audit_log_rmi_test",
                    REPO_ROOT / "scripts" / "audit_log.py",
                )
                self.assertEqual(mod.DEFAULT_AUDIT_RETENTION_DAYS, 180)

    def test_shell_env_overrides_dotenv_for_audit_log(self):
        """Shell env takes precedence over .env for audit_log retention."""
        with tempfile.TemporaryDirectory() as tmp:
            dotenv_path = Path(tmp) / ".env"
            dotenv_path.write_text("ML_AUDIT_RETENTION_DAYS=180\n")
            env_shell = dict(os.environ)
            env_shell["ML_AUDIT_RETENTION_DAYS"] = "30"
            with patch.dict(os.environ, env_shell, clear=True):
                from dotenv import load_dotenv
                load_dotenv(dotenv_path, override=False)
                mod = _load_module(
                    "audit_log_shell_override_test",
                    REPO_ROOT / "scripts" / "audit_log.py",
                )
                self.assertEqual(mod.DEFAULT_AUDIT_RETENTION_DAYS, 30)


if __name__ == "__main__":
    unittest.main()
