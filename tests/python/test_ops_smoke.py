#!/usr/bin/env python3
"""Lightweight smoke tests for ops resilience scripts."""

from __future__ import annotations

import json
import importlib.util
import os
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import unittest
from pathlib import Path

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = str(Path(sys.executable).resolve())


def run_cmd(cmd: list[str], cwd: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        env=full_env,
        text=True,
        capture_output=True,
        check=False,
    )


def load_module(module_name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec for {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class OpsSmokeTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="pq_ops_smoke_"))

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_db(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path))
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS bar_data(ts INTEGER)")
            conn.execute("CREATE TABLE IF NOT EXISTS touch_events(ts_event INTEGER)")
            conn.execute("CREATE TABLE IF NOT EXISTS prediction_log(ts_prediction INTEGER)")
            conn.execute("CREATE TABLE IF NOT EXISTS event_labels(ts_event INTEGER)")
            conn.commit()
        finally:
            conn.close()

    def _touch_tree(self, root: Path, rel: str, content: str) -> None:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def _create_snapshot(
        self,
        snapshots_root: Path,
        stamp: str,
        *,
        complete: bool,
    ) -> Path:
        snap = snapshots_root / stamp
        snap.mkdir(parents=True, exist_ok=True)
        (snap / "pivot_events.sqlite").write_bytes(b"sqlite-placeholder")
        with tarfile.open(snap / "models.tar.gz", "w:gz"):
            pass
        if complete:
            with tarfile.open(snap / "reports.tar.gz", "w:gz"):
                pass
            manifest = {"snapshot": stamp, "status": "complete"}
        else:
            manifest = {"snapshot": stamp, "status": "inprogress"}
        (snap / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return snap

    def test_nightly_backup_creates_complete_snapshot(self) -> None:
        db = self.tmp / "data" / "pivot_events.sqlite"
        models_dir = self.tmp / "models"
        reports_dir = self.tmp / "reports"
        backup_root = self.tmp / "backups"
        log_file = self.tmp / "logs" / "backup.log"
        state_file = self.tmp / "logs" / "backup_state.json"
        env_file = self.tmp / "empty.env"
        env_file.write_text("", encoding="utf-8")
        self._make_db(db)
        self._touch_tree(models_dir, "manifest_latest.json", '{"version":"vtest"}')
        self._touch_tree(reports_dir, "ml_daily_2099-01-01.md", "# report")

        proc = run_cmd(
            [
                PYTHON,
                "scripts/nightly_backup.py",
                "--env-file",
                str(env_file),
                "--backup-root",
                str(backup_root),
                "--db-path",
                str(db),
                "--models-dir",
                str(models_dir),
                "--reports-dir",
                str(reports_dir),
                "--log-file",
                str(log_file),
                "--state-file",
                str(state_file),
            ],
            cwd=REPO_ROOT,
            env={"PIVOT_DB": str(db)},
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

        snapshots = sorted((backup_root / "snapshots").iterdir())
        self.assertEqual(len(snapshots), 1)
        snap = snapshots[0]
        self.assertTrue((snap / "pivot_events.sqlite").exists())
        self.assertTrue((snap / "models.tar.gz").exists())
        self.assertTrue((snap / "reports.tar.gz").exists())
        manifest = json.loads((snap / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest.get("status"), "complete")
        self.assertIn("files", manifest)

    def test_restore_drill_selects_latest_complete_snapshot(self) -> None:
        db = self.tmp / "data" / "pivot_events.sqlite"
        self._make_db(db)
        backup_root = self.tmp / "backups"
        snapshots = backup_root / "snapshots"
        self._create_snapshot(snapshots, "20260218_110000", complete=True)
        self._create_snapshot(snapshots, "20260218_120000", complete=False)
        log_file = self.tmp / "logs" / "restore.log"
        env_file = self.tmp / "empty.env"
        env_file.write_text("", encoding="utf-8")

        proc = run_cmd(
            [
                PYTHON,
                "scripts/backup_restore_drill.py",
                "--env-file",
                str(env_file),
                "--backup-root",
                str(backup_root),
                "--log-file",
                str(log_file),
                "--dry-run",
            ],
            cwd=REPO_ROOT,
            env={"PIVOT_DB": str(db)},
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")
        log_text = log_file.read_text(encoding="utf-8")
        self.assertIn("snapshot=20260218_110000", log_text)
        self.assertNotIn("snapshot=20260218_120000", log_text)

    def test_daily_report_sender_dedupes_same_date_and_mode(self) -> None:
        root = self.tmp / "sandbox"
        scripts_dir = root / "scripts"
        logs_dir = root / "logs"
        scripts_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        original = (REPO_ROOT / "scripts" / "run_daily_report_send.sh").read_text(encoding="utf-8")
        (scripts_dir / "run_daily_report_send.sh").write_text(original, encoding="utf-8")

        generate_script = textwrap.dedent(
            """
            #!/usr/bin/env python3
            import argparse
            from pathlib import Path
            parser = argparse.ArgumentParser()
            parser.add_argument("--db")
            parser.add_argument("--out-dir")
            parser.add_argument("--report-date")
            args = parser.parse_args()
            out_dir = Path(args.out_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            report = out_dir / f"ml_daily_{args.report_date}.md"
            report.write_text("# smoke report\\n", encoding="utf-8")
            count_file = out_dir / ".gen_count"
            current = int(count_file.read_text(encoding="utf-8") or "0") if count_file.exists() else 0
            count_file.write_text(str(current + 1), encoding="utf-8")
            print(report)
            """
        ).strip()
        (scripts_dir / "generate_daily_ml_report.py").write_text(generate_script + "\n", encoding="utf-8")

        send_script = textwrap.dedent(
            """
            #!/usr/bin/env python3
            import argparse
            from pathlib import Path
            parser = argparse.ArgumentParser()
            parser.add_argument("--report")
            parser.add_argument("--db")
            args = parser.parse_args()
            marker = Path(args.report).parent / ".send_count"
            current = int(marker.read_text(encoding="utf-8") or "0") if marker.exists() else 0
            marker.write_text(str(current + 1), encoding="utf-8")
            print("[notify] smoke sender ok")
            """
        ).strip()
        (scripts_dir / "send_daily_report.py").write_text(send_script + "\n", encoding="utf-8")

        os.chmod(scripts_dir / "run_daily_report_send.sh", 0o755)
        os.chmod(scripts_dir / "generate_daily_ml_report.py", 0o755)
        os.chmod(scripts_dir / "send_daily_report.py", 0o755)

        # Make the test independent of launchd PATH quirks: the sender script
        # prefers ROOT/.venv/bin/python3 when available.
        venv_python = root / ".venv" / "bin" / "python3"
        venv_python.parent.mkdir(parents=True, exist_ok=True)
        target_python = Path(PYTHON).resolve()
        if venv_python.exists() or venv_python.is_symlink():
            venv_python.unlink()
        os.symlink(target_python, venv_python)

        report_date = "2026-02-18"
        env = {
            "ML_REPORT_NOTIFY_CHANNELS": "none",
            "ML_REPORT_REPORT_DATE": report_date,
            "ML_REPORT_SCHEDULE_MODE": "close",
            "PIVOT_DB": str(root / "data" / "pivot_events.sqlite"),
        }

        first = run_cmd(["/bin/bash", str(scripts_dir / "run_daily_report_send.sh")], cwd=root, env=env)
        self.assertEqual(first.returncode, 0, msg=f"{first.stdout}\n{first.stderr}")
        second = run_cmd(["/bin/bash", str(scripts_dir / "run_daily_report_send.sh")], cwd=root, env=env)
        self.assertEqual(second.returncode, 0, msg=f"{second.stdout}\n{second.stderr}")

        counters_dir = logs_dir / "reports"
        gen_count = int((counters_dir / ".gen_count").read_text(encoding="utf-8"))
        send_count = int((counters_dir / ".send_count").read_text(encoding="utf-8"))
        self.assertEqual(gen_count, 1)
        self.assertEqual(send_count, 1)

        log_text = (logs_dir / "report_delivery.log").read_text(encoding="utf-8")
        self.assertIn("DONE  daily_report_send", log_text)
        self.assertIn("report already sent", log_text)

    def test_level_converter_contract_and_route_present(self) -> None:
        proxy_source = (REPO_ROOT / "server" / "yahoo_proxy.js").read_text(encoding="utf-8")
        self.assertIn("/api/levels/convert", proxy_source)

        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        node_script = textwrap.dedent(
            """
            import { buildConversionSnapshot, convertLevels } from './server/level_converter.js';
            const snapshot = buildConversionSnapshot({
              prices: { SPY: 500, SPX: 5000, US500: 5000, ES: 5005 },
              mode: 'prior_close',
              source: 'smoke-test',
              asOf: '2026-02-19T00:00:00Z',
              esBasisMode: true,
            });
            const converted = convertLevels({
              levels: [{ label: 'PP', value: 500 }, { label: 'R1', value: 505 }],
              fromInstrument: 'SPY',
              toInstrument: 'US500',
              snapshot,
              esBasisMode: true,
            });
            console.log(JSON.stringify(converted));
            """
        ).strip()

        proc = run_cmd(
            ["node", "--input-type=module", "-e", node_script],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

        payload = json.loads(proc.stdout)
        self.assertIn("levels", payload)
        self.assertIn("metadata", payload)
        self.assertEqual(payload["metadata"]["fromInstrument"], "SPY")
        self.assertEqual(payload["metadata"]["toInstrument"], "US500")
        self.assertAlmostEqual(float(payload["metadata"]["ratio"]), 10.0, places=8)
        self.assertEqual(len(payload["levels"]), 2)
        self.assertAlmostEqual(float(payload["levels"][0]["value"]), 5000.0, places=8)

    def test_dashboard_proxy_public_auth_and_endpoint_hardening_present(self) -> None:
        proxy_source = (REPO_ROOT / "server" / "yahoo_proxy.js").read_text(encoding="utf-8")
        self.assertIn("DASH_AUTH_ENABLED", proxy_source)
        self.assertIn("DASH_AUTH_PASSWORD", proxy_source)
        self.assertIn("DASH_WRITE_ENDPOINTS_LOCAL_ONLY", proxy_source)
        self.assertIn("WRITE_ENDPOINTS", proxy_source)
        self.assertIn("handleAuthRoutes", proxy_source)
        self.assertIn("url.pathname === '/auth/login'", proxy_source)
        self.assertIn("auth_method: 'password_cookie'", proxy_source)
        self.assertIn("x-forwarded-for", proxy_source)
        self.assertIn("url.pathname === '/health'", proxy_source)

    def test_session_routine_contract_present(self) -> None:
        installer = (REPO_ROOT / "scripts" / "install_session_routine_launch_agent.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("com.pivotquant.session_routine", installer)
        self.assertIn("run_session_routine_check.sh", installer)

        checker = (REPO_ROOT / "scripts" / "session_routine_check.py").read_text(encoding="utf-8")
        self.assertIn("ML_SESSION_PREOPEN_HOUR", checker)
        self.assertIn("ML_SESSION_POSTOPEN_HOUR", checker)
        self.assertIn("ML_SESSION_OPS_STATUS_URL", checker)

        proc = run_cmd([PYTHON, "-m", "py_compile", "scripts/session_routine_check.py"], cwd=REPO_ROOT)
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_retrain_cycle_sources_dotenv(self) -> None:
        retrain_script = (REPO_ROOT / "scripts" / "run_retrain_cycle.sh").read_text(encoding="utf-8")
        self.assertIn('ENV_FILE="${ROOT_DIR}/.env"', retrain_script)
        self.assertIn("load_env_file()", retrain_script)
        self.assertIn('load_env_file "${ENV_FILE}"', retrain_script)
        self.assertIn("--horizons 5 15 30 60 --incremental", retrain_script)

    def test_30m_shadow_horizon_contract_present(self) -> None:
        ml_server = (REPO_ROOT / "server" / "ml_server.py").read_text(encoding="utf-8")
        self.assertIn("ML_SHADOW_HORIZONS", ml_server)
        self.assertIn("signal_30m", ml_server)
        self.assertIn("prob_reject_30m", ml_server)
        self.assertIn("threshold_break_30m", ml_server)

        migrate_db = (REPO_ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        self.assertIn("LATEST_SCHEMA_VERSION = 5", migrate_db)
        self.assertIn("migration_5_prediction_log_shadow_30m", migrate_db)
        self.assertIn("signal_30m", migrate_db)

    def test_30m_shadow_horizon_runtime_behavior(self) -> None:
        ml_server = load_module("ml_server_shadow_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        def set_registry(*, reject_probs: dict[int, float], break_probs: dict[int, float]) -> None:
            ml_server.registry.models = {"reject": {}, "break": {}}
            for horizon, prob in reject_probs.items():
                ml_server.registry.models["reject"][horizon] = {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(prob),
                    "calibration": "sigmoid",
                }
            for horizon, prob in break_probs.items():
                ml_server.registry.models["break"][horizon] = {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(prob),
                    "calibration": "sigmoid",
                }
            ml_server.registry.thresholds = {
                "reject": {h: 0.5 for h in (5, 15, 30, 60)},
                "break": {h: 0.5 for h in (5, 15, 30, 60)},
            }
            ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": 0}

        ml_server.build_feature_row = lambda _event: {"x": 1.0}
        ml_server.collect_missing = lambda _features: []

        # 30m is strongest, but cannot become best_horizon when shadowed.
        set_registry(
            reject_probs={5: 0.55, 15: 0.56, 30: 0.99, 60: 0.57},
            break_probs={5: 0.10, 15: 0.10, 30: 0.10, 60: 0.10},
        )
        result = ml_server._score_event({"event_id": "shadow_case_strong_30m"})
        self.assertEqual(result["signals"].get("signal_30m"), "reject")
        self.assertNotEqual(result["best_horizon"], 30)
        self.assertEqual(result["best_horizon"], 60)
        self.assertFalse(result["abstain"])

        # If only shadow horizon has directional signal, abstain remains true.
        set_registry(
            reject_probs={5: 0.10, 15: 0.10, 30: 0.95, 60: 0.10},
            break_probs={5: 0.10, 15: 0.10, 30: 0.10, 60: 0.10},
        )
        result = ml_server._score_event({"event_id": "shadow_case_only_30m"})
        self.assertEqual(result["signals"].get("signal_30m"), "reject")
        self.assertEqual(result["signals"].get("signal_5m"), "no_edge")
        self.assertEqual(result["signals"].get("signal_15m"), "no_edge")
        self.assertEqual(result["signals"].get("signal_60m"), "no_edge")
        self.assertTrue(result["abstain"])

    def test_model_governance_skips_regression_gates_when_support_is_low(self) -> None:
        module = load_module("model_governance", REPO_ROOT / "scripts" / "model_governance.py")
        gates = module.GateConfig(
            required_targets=["break"],
            required_horizons=[5],
            min_trained_end_delta_ms=0,
            max_mfe_regression_bps=1.5,
            max_mae_worsening_bps=2.0,
            min_total_samples=200,
            min_positive_samples_reject=0,
            min_positive_samples_break=25,
            allow_feature_version_change=False,
        )
        active = {
            "feature_version": "v3",
            "trained_end_ts": 1000,
            "stats": {
                "5": {
                    "break": {
                        "sample_size": 120,
                        "break_count": 10,
                        "mfe_bps_break": 8.0,
                        "mae_bps_break": -25.0,
                    }
                }
            },
        }
        candidate = {
            "feature_version": "v3",
            "trained_end_ts": 2000,
            "stats": {
                "5": {
                    "break": {
                        "sample_size": 130,
                        "break_count": 11,
                        "mfe_bps_break": 6.0,
                        "mae_bps_break": -35.0,
                    }
                }
            },
        }
        failures, skips = module.evaluate_gates(active, candidate, gates)
        self.assertEqual(failures, [])
        self.assertTrue(any("break:5m skipped regression gates" in item for item in skips))

    def test_model_governance_enforces_regression_gates_when_support_is_high(self) -> None:
        module = load_module("model_governance", REPO_ROOT / "scripts" / "model_governance.py")
        gates = module.GateConfig(
            required_targets=["break"],
            required_horizons=[5],
            min_trained_end_delta_ms=0,
            max_mfe_regression_bps=1.5,
            max_mae_worsening_bps=2.0,
            min_total_samples=200,
            min_positive_samples_reject=0,
            min_positive_samples_break=25,
            allow_feature_version_change=False,
        )
        active = {
            "feature_version": "v3",
            "trained_end_ts": 1000,
            "stats": {
                "5": {
                    "break": {
                        "sample_size": 300,
                        "break_count": 40,
                        "mfe_bps_break": 8.0,
                        "mae_bps_break": -25.0,
                    }
                }
            },
        }
        candidate = {
            "feature_version": "v3",
            "trained_end_ts": 2000,
            "stats": {
                "5": {
                    "break": {
                        "sample_size": 320,
                        "break_count": 43,
                        "mfe_bps_break": 7.5,
                        "mae_bps_break": -29.0,
                    }
                }
            },
        }
        failures, skips = module.evaluate_gates(active, candidate, gates)
        self.assertTrue(any("break:5m mae_bps_break worsened" in item for item in failures))
        self.assertEqual(skips, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
