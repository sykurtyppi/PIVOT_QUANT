"""End-to-end backup -> restore-drill round trip for the ops resilience scripts.

ops_lock's locking is unit-tested in test_ops_lock; this exercises the actual
backup and restore BEHAVIOUR on top of it:

  scripts/nightly_backup.py   creates a snapshot (online sqlite backup + model/
                              report tarballs + manifest + `latest` pointer +
                              retention + ops_status)
  scripts/backup_restore_drill.py  restores the latest snapshot into a scratch
                              dir and verifies sqlite quick_check, the model
                              manifest, and the daily report

Stdlib-only; both scripts are driven via subprocess into a throwaway temp tree
(PIVOT_DB and all paths redirected, so nothing touches real data/ or logs/).
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PYTHON = sys.executable
CANDIDATE_MANIFEST = "manifest_runtime_latest.json"


def _make_sqlite(path: Path) -> None:
    con = sqlite3.connect(str(path))
    try:
        con.execute("CREATE TABLE bar_data (ts INTEGER PRIMARY KEY, close REAL)")
        con.executemany("INSERT INTO bar_data VALUES (?, ?)", [(1, 100.0), (2, 101.0)])
        con.commit()
    finally:
        con.close()


class BackupRestoreE2ETests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

        self.backup_root = self.tmp / "backups"
        self.src_db = self.tmp / "source.sqlite"
        _make_sqlite(self.src_db)
        self.models = self.tmp / "models"
        self.models.mkdir()
        (self.models / CANDIDATE_MANIFEST).write_text('{"version": "test"}', encoding="utf-8")
        (self.models / "rf_model.bin").write_text("weights", encoding="utf-8")
        self.reports = self.tmp / "reports"
        self.reports.mkdir()
        (self.reports / "ml_daily_2026-10-20.md").write_text("# daily\n", encoding="utf-8")

        self.ops_db = self.tmp / "ops.sqlite"
        self.state = self.tmp / "state.json"
        self.lock = self.tmp / "ops.lock"
        self.env_file = self.tmp / "nonexistent.env"  # load_env_file guards a missing file

        # The restore drill hard-writes its summary to REPO/logs/; ensure the dir
        # exists and remove only the files we create, leaving the tree as found.
        self._logs = REPO / "logs"
        self._logs_existed = self._logs.exists()
        self._logs.mkdir(exist_ok=True)
        self._pre = set(self._logs.glob("restore_drill_*.json"))
        self.addCleanup(self._cleanup_repo_logs)

    def _cleanup_repo_logs(self) -> None:
        for f in self._logs.glob("restore_drill_*.json"):
            if f not in self._pre:
                f.unlink(missing_ok=True)
        if not self._logs_existed:
            try:
                self._logs.rmdir()
            except OSError:
                pass

    def _env(self) -> dict:
        return {**os.environ, "PIVOT_DB": str(self.ops_db), "RF_CANDIDATE_MANIFEST": CANDIDATE_MANIFEST}

    def _run(self, script: str, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [PYTHON, str(REPO / "scripts" / script), *args],
            cwd=str(REPO), env=self._env(), capture_output=True, text=True,
        )

    def _backup_args(self, *extra: str) -> list:
        return [
            "--env-file", str(self.env_file),
            "--backup-root", str(self.backup_root),
            "--db-path", str(self.src_db),
            "--models-dir", str(self.models),
            "--reports-dir", str(self.reports),
            "--log-file", str(self.tmp / "backup.log"),
            "--state-file", str(self.state),
            "--lock-file", str(self.lock),
            "--lock-timeout-sec", "5",
            *extra,
        ]

    def _ops_status(self) -> dict:
        if not self.ops_db.exists():
            return {}
        con = sqlite3.connect(str(self.ops_db))
        try:
            return dict(con.execute("SELECT key, value FROM ops_status").fetchall())
        finally:
            con.close()

    def test_backup_then_restore_round_trip(self) -> None:
        cp = self._run("nightly_backup.py", *self._backup_args())
        self.assertEqual(cp.returncode, 0, cp.stderr)

        snaps = sorted((self.backup_root / "snapshots").glob("[0-9]*_[0-9]*"))
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        for name in ("pivot_events.sqlite", "models.tar.gz", "reports.tar.gz", "manifest.json"):
            self.assertTrue((snap / name).exists(), f"missing {name}")

        manifest = json.loads((snap / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["status"], "complete")
        self.assertTrue((self.backup_root / "latest").exists())
        # the db copy round-trips to the hash recorded in the manifest
        digest = hashlib.sha256((snap / "pivot_events.sqlite").read_bytes()).hexdigest()
        self.assertEqual(manifest["files"]["pivot_events.sqlite"]["sha256"], digest)
        # the model tarball carries the candidate manifest under models/
        with tarfile.open(snap / "models.tar.gz") as tar:
            self.assertIn(f"models/{CANDIDATE_MANIFEST}", tar.getnames())
        self.assertEqual(self._ops_status().get("backup_last_status"), "ok")

        drill = self._run(
            "backup_restore_drill.py",
            "--env-file", str(self.env_file),
            "--backup-root", str(self.backup_root),
            "--log-file", str(self.tmp / "drill.log"),
            "--lock-file", str(self.lock),
            "--lock-timeout-sec", "5",
        )
        self.assertEqual(drill.returncode, 0, drill.stderr)
        self.assertEqual(self._ops_status().get("backup_restore_last_status"), "ok")

    def test_dry_run_creates_no_snapshot(self) -> None:
        cp = self._run("nightly_backup.py", *self._backup_args("--dry-run"))
        self.assertEqual(cp.returncode, 0, cp.stderr)
        snaps_root = self.backup_root / "snapshots"
        snaps = list(snaps_root.glob("[0-9]*_[0-9]*")) if snaps_root.exists() else []
        self.assertEqual(snaps, [])
        self.assertEqual(json.loads(self.state.read_text(encoding="utf-8"))["last_status"], "ok")

    def test_restore_drill_fails_without_snapshots(self) -> None:
        drill = self._run(
            "backup_restore_drill.py",
            "--env-file", str(self.env_file),
            "--backup-root", str(self.backup_root),  # no backup has run
            "--log-file", str(self.tmp / "drill.log"),
            "--lock-file", str(self.lock),
            "--lock-timeout-sec", "5",
        )
        self.assertEqual(drill.returncode, 1)
        self.assertEqual(self._ops_status().get("backup_restore_last_status"), "failed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
