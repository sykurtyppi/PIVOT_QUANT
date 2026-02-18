#!/usr/bin/env python3
"""Lightweight smoke tests for ops resilience scripts."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import tarfile
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = shutil.which("python3") or "python3"


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


if __name__ == "__main__":
    unittest.main(verbosity=2)
