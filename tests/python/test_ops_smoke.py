#!/usr/bin/env python3
"""Lightweight smoke tests for ops resilience scripts."""

from __future__ import annotations

import json
import importlib.util
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import URLError

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
        lock_file = self.tmp / "logs" / "ops_resilience.lock"
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
                "--lock-file",
                str(lock_file),
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
        lock_file = self.tmp / "logs" / "ops_resilience.lock"
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
                "--lock-file",
                str(lock_file),
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

    def test_daily_report_impact_is_direction_aware(self) -> None:
        db = self.tmp / "impact.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                """
                CREATE TABLE touch_events(
                    event_id TEXT PRIMARY KEY,
                    ts_event INTEGER,
                    touch_side INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE event_labels(
                    event_id TEXT,
                    horizon_min INTEGER,
                    return_bps REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE prediction_log(
                    event_id TEXT,
                    ts_prediction INTEGER,
                    signal_5m TEXT,
                    is_preview INTEGER
                )
                """
            )

            send_daily_report = load_module(
                "pq_send_daily_report_test",
                REPO_ROOT / "scripts" / "send_daily_report.py",
            )
            report_day = date(2026, 2, 26)
            start_ms, _ = send_daily_report.et_day_bounds_ms(report_day)

            # Event 1: touch_side=-1 and reject with raw return -10 bps.
            # Direction-aware reject PnL should be +10 bps gross.
            conn.execute(
                "INSERT INTO touch_events(event_id, ts_event, touch_side) VALUES (?, ?, ?)",
                ("e1", start_ms + 60_000, -1),
            )
            conn.execute(
                "INSERT INTO event_labels(event_id, horizon_min, return_bps) VALUES (?, ?, ?)",
                ("e1", 5, -10.0),
            )
            conn.execute(
                "INSERT INTO prediction_log(event_id, ts_prediction, signal_5m, is_preview) VALUES (?, ?, ?, ?)",
                ("e1", start_ms + 61_000, "reject", 0),
            )

            # Event 2: touch_side=-1 and break with raw return +10 bps.
            # Direction-aware break PnL should also be +10 bps gross.
            conn.execute(
                "INSERT INTO touch_events(event_id, ts_event, touch_side) VALUES (?, ?, ?)",
                ("e2", start_ms + 120_000, -1),
            )
            conn.execute(
                "INSERT INTO event_labels(event_id, horizon_min, return_bps) VALUES (?, ?, ?)",
                ("e2", 5, +10.0),
            )
            conn.execute(
                "INSERT INTO prediction_log(event_id, ts_prediction, signal_5m, is_preview) VALUES (?, ?, ?, ?)",
                ("e2", start_ms + 121_000, "break", 0),
            )
            conn.commit()
        finally:
            conn.close()

        impact = send_daily_report.compute_impact_stats(str(db), report_day, include_preview=False)
        self.assertNotIn("error", impact)
        self.assertEqual(impact["signals"], 2)
        self.assertAlmostEqual(float(impact["avg_gross"]), 10.0, places=6)
        # Cost defaults: 0.8 + 0.4 + 0.1 = 1.3 bps.
        self.assertAlmostEqual(float(impact["avg_net"]), 8.7, places=6)
        self.assertAlmostEqual(float(impact["win_rate_net"]), 1.0, places=6)
        self.assertEqual(int(impact["by_horizon"][5]["n"]), 2)

    def test_build_labels_unknown_side_excursions_are_symmetric(self) -> None:
        build_labels = load_module(
            "pq_build_labels_symmetry_test",
            REPO_ROOT / "scripts" / "build_labels.py",
        )
        bars = [
            {"high": 101.0, "low": 99.0, "close": 100.0},
            {"high": 102.0, "low": 99.5, "close": 100.5},
        ]
        mfe_bps, mae_bps = build_labels.compute_mfe_mae(bars, 100.0, None)
        self.assertAlmostEqual(float(mfe_bps), 200.0, places=6)
        self.assertAlmostEqual(float(mae_bps), -200.0, places=6)

    def test_build_labels_break_sustain_one_triggers_on_first_bar(self) -> None:
        build_labels = load_module(
            "pq_build_labels_sustain_test",
            REPO_ROOT / "scripts" / "build_labels.py",
        )
        bars = [
            {"close": 99.8, "high": 100.1, "low": 99.7, "ts": 1},
            {"close": 99.7, "high": 99.9, "low": 99.6, "ts": 2},
        ]
        reject, brk, resolution = build_labels.label_event(
            bars=bars,
            touch_price=100.0,
            level_price=100.0,
            touch_side=1,
            reject_bps=10.0,
            break_bps=10.0,
            sustain_bars=1,
        )
        self.assertEqual(reject, 0)
        self.assertEqual(brk, 1)
        self.assertEqual(resolution, 0)

        reject2, brk2, resolution2 = build_labels.label_event(
            bars=bars,
            touch_price=100.0,
            level_price=100.0,
            touch_side=1,
            reject_bps=10.0,
            break_bps=10.0,
            sustain_bars=2,
        )
        self.assertEqual(reject2, 0)
        self.assertEqual(brk2, 1)
        self.assertEqual(resolution2, 1)

    def test_ml_score_payload_rejects_oversized_batches(self) -> None:
        prior_limit = os.environ.get("ML_SCORE_MAX_BATCH_EVENTS")
        os.environ["ML_SCORE_MAX_BATCH_EVENTS"] = "2"
        try:
            ml_server = load_module(
                "pq_ml_server_batch_guard_test",
                REPO_ROOT / "server" / "ml_server.py",
            )
        finally:
            if prior_limit is None:
                os.environ.pop("ML_SCORE_MAX_BATCH_EVENTS", None)
            else:
                os.environ["ML_SCORE_MAX_BATCH_EVENTS"] = prior_limit

        with self.assertRaises(Exception) as ctx:
            ml_server._validate_score_payload({"events": [{}, {}, {}]})
        err = ctx.exception
        self.assertEqual(getattr(err, "status_code", None), 413)
        self.assertIn("Max allowed: 2", getattr(err, "detail", ""))

    def test_reconcile_predictions_paths_resolve_from_repo_root(self) -> None:
        reconcile_predictions = load_module(
            "pq_reconcile_paths_test",
            REPO_ROOT / "scripts" / "reconcile_predictions.py",
        )
        rel = reconcile_predictions.resolve_repo_path("data/pivot_events.sqlite")
        self.assertEqual(rel, REPO_ROOT / "data" / "pivot_events.sqlite")

        abs_path = Path("/tmp/pq_reconcile_abs.sqlite")
        self.assertEqual(reconcile_predictions.resolve_repo_path(str(abs_path)), abs_path)

    def test_audit_log_prune_preserves_chain_with_anchor(self) -> None:
        audit_log = load_module("pq_audit_log_retention_test", REPO_ROOT / "scripts" / "audit_log.py")
        db = self.tmp / "audit.sqlite"
        base_ts = 1_700_000_000_000

        for idx in range(3):
            result = audit_log.append_event(
                db_path=db,
                event_type="smoke_event",
                source="ops_smoke",
                actor="tester",
                host="localhost",
                message=f"event-{idx}",
                details={"index": idx},
                commit_hash="abc123",
                ts_ms=base_ts + (idx * audit_log.MS_PER_DAY),
            )
            self.assertEqual(result["status"], "ok")

        before = audit_log.verify_chain(db_path=db)
        self.assertEqual(before["status"], "ok")
        self.assertEqual(before["checked_events"], 3)
        self.assertEqual(before.get("anchor_prev_hash"), "")

        pruned = audit_log.prune_history(
            db_path=db,
            retention_days=1,
            now_ts_ms=base_ts + (2 * audit_log.MS_PER_DAY),
        )
        self.assertEqual(pruned["status"], "ok")
        self.assertEqual(int(pruned["deleted_rows"]), 1)
        self.assertEqual(int(pruned["remaining_rows"]), 2)
        self.assertEqual(int(pruned["anchor_event_id"]), 2)
        self.assertTrue(str(pruned["anchor_prev_hash"]))

        after = audit_log.verify_chain(db_path=db)
        self.assertEqual(after["status"], "ok")
        self.assertEqual(after["checked_events"], 2)
        self.assertEqual(after.get("anchor_prev_hash"), pruned["anchor_prev_hash"])

        tail = audit_log.fetch_tail(db_path=db, limit=10)
        self.assertEqual([event["id"] for event in tail["events"]], [2, 3])

    def test_audit_log_auto_prune_respects_interval(self) -> None:
        audit_log = load_module("pq_audit_log_prune_interval_test", REPO_ROOT / "scripts" / "audit_log.py")
        db = self.tmp / "audit_interval.sqlite"

        conn = audit_log.connect_db(db)
        try:
            audit_log.ensure_schema(conn)

            first = audit_log.maybe_prune_audit_prefix(
                conn,
                now_ts_ms=10_000,
                retention_days=90,
                prune_interval_ms=5_000,
            )
            second = audit_log.maybe_prune_audit_prefix(
                conn,
                now_ts_ms=12_000,
                retention_days=90,
                prune_interval_ms=5_000,
            )
            third = audit_log.maybe_prune_audit_prefix(
                conn,
                now_ts_ms=16_000,
                retention_days=90,
                prune_interval_ms=5_000,
            )
            conn.commit()
        finally:
            conn.close()

        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertIsNotNone(third)

    def test_local_services_use_allowlist_cors(self) -> None:
        for rel in (
            "server/event_writer.py",
            "server/live_event_collector.py",
            "server/ibkr_gamma_bridge.py",
        ):
            src = (REPO_ROOT / rel).read_text(encoding="utf-8")
            self.assertIn("ML_CORS_ORIGINS", src)
            self.assertNotIn('Access-Control-Allow-Origin", "*"', src)

    def test_backfill_gamma_context_falls_back_to_snapshots(self) -> None:
        backfill = load_module(
            "pq_backfill_gamma_snapshot_fallback_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        original_fetch_json = backfill.fetch_json
        original_token = backfill.MARKETDATA_APP_TOKEN
        backfill.MARKETDATA_APP_TOKEN = ""

        def _bridge_down(*_args, **_kwargs):
            raise RuntimeError("bridge unavailable")

        backfill.fetch_json = _bridge_down
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE gamma_snapshots (
                    symbol TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    ts_collected_ms INTEGER NOT NULL,
                    gamma_flip REAL,
                    atm_iv REAL,
                    oi_concentration_top5 REAL,
                    zero_dte_share REAL,
                    total_contracts INTEGER,
                    with_greeks INTEGER,
                    with_oi INTEGER,
                    used_open_interest INTEGER
                )
                """
            )
            snapshot_date = datetime.now(backfill.NY_TZ).date().strftime("%Y-%m-%d")
            conn.execute(
                """
                INSERT INTO gamma_snapshots(
                    symbol, snapshot_date, ts_collected_ms, gamma_flip, atm_iv,
                    oi_concentration_top5, zero_dte_share, total_contracts, with_greeks,
                    with_oi, used_open_interest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "SPY",
                    snapshot_date,
                    1_777_777_777_000,
                    501.25,
                    0.22,
                    18.5,
                    9.2,
                    2000,
                    1800,
                    1700,
                    1,
                ),
            )
            conn.commit()

            ctx = backfill.fetch_gamma_context("SPY", timeout=1, conn=conn)
            self.assertIsNotNone(ctx)
            self.assertEqual(ctx["source_name"], "gamma_snapshots")
            self.assertAlmostEqual(float(ctx["gamma_flip"]), 501.25, places=6)
            self.assertEqual(ctx["generated_at_date_et"].strftime("%Y-%m-%d"), snapshot_date)
            self.assertAlmostEqual(float(ctx["atm_iv_pct"]), 22.0, places=6)
        finally:
            backfill.fetch_json = original_fetch_json
            backfill.MARKETDATA_APP_TOKEN = original_token
            conn.close()

    def test_backfill_gamma_context_prefers_live_when_snapshot_is_stale(self) -> None:
        backfill = load_module(
            "pq_backfill_gamma_live_refresh_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        original_fetch_json = backfill.fetch_json
        original_live_fetch = backfill._fetch_gamma_context_marketdata_live
        original_token = backfill.MARKETDATA_APP_TOKEN
        backfill.MARKETDATA_APP_TOKEN = "dummy_token"

        def _bridge_down(*_args, **_kwargs):
            raise RuntimeError("bridge unavailable")

        today = datetime.now(backfill.NY_TZ).date()
        stale_day = today - timedelta(days=1)

        def _fake_live_fetch(*, symbol, timeout, conn):
            return {
                "symbol": symbol.upper(),
                "gamma_flip": 512.0,
                "gamma_confidence": 92,
                "oi_concentration_top5": 22.0,
                "zero_dte_share": 11.0,
                "atm_iv_pct": 24.0,
                "generated_at_ms": 1_777_777_888_000,
                "generated_at_date_et": today,
                "source_name": "marketdata_live",
            }

        backfill.fetch_json = _bridge_down
        backfill._fetch_gamma_context_marketdata_live = _fake_live_fetch
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE gamma_snapshots (
                    symbol TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    ts_collected_ms INTEGER NOT NULL,
                    gamma_flip REAL,
                    atm_iv REAL,
                    oi_concentration_top5 REAL,
                    zero_dte_share REAL,
                    total_contracts INTEGER,
                    with_greeks INTEGER,
                    with_oi INTEGER,
                    used_open_interest INTEGER
                )
                """
            )
            conn.execute(
                """
                INSERT INTO gamma_snapshots(
                    symbol, snapshot_date, ts_collected_ms, gamma_flip, atm_iv,
                    oi_concentration_top5, zero_dte_share, total_contracts, with_greeks,
                    with_oi, used_open_interest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "SPY",
                    stale_day.strftime("%Y-%m-%d"),
                    1_777_777_777_000,
                    501.25,
                    0.22,
                    18.5,
                    9.2,
                    2000,
                    1800,
                    1700,
                    1,
                ),
            )
            conn.commit()

            ctx = backfill.fetch_gamma_context("SPY", timeout=1, conn=conn)
            self.assertIsNotNone(ctx)
            self.assertEqual(ctx["source_name"], "marketdata_live")
            self.assertAlmostEqual(float(ctx["gamma_flip"]), 512.0, places=6)
            self.assertEqual(ctx["generated_at_date_et"], today)
        finally:
            backfill.fetch_json = original_fetch_json
            backfill._fetch_gamma_context_marketdata_live = original_live_fetch
            backfill.MARKETDATA_APP_TOKEN = original_token
            conn.close()

    def test_backfill_gamma_context_uses_snapshot_without_gamma_flip(self) -> None:
        backfill = load_module(
            "pq_backfill_gamma_partial_snapshot_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        original_fetch_json = backfill.fetch_json
        original_token = backfill.MARKETDATA_APP_TOKEN
        backfill.MARKETDATA_APP_TOKEN = ""

        def _bridge_down(*_args, **_kwargs):
            raise RuntimeError("bridge unavailable")

        backfill.fetch_json = _bridge_down
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE gamma_snapshots (
                    symbol TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    ts_collected_ms INTEGER NOT NULL,
                    gamma_flip REAL,
                    atm_iv REAL,
                    oi_concentration_top5 REAL,
                    zero_dte_share REAL,
                    total_contracts INTEGER,
                    with_greeks INTEGER,
                    with_oi INTEGER,
                    used_open_interest INTEGER
                )
                """
            )
            snapshot_date = datetime.now(backfill.NY_TZ).date().strftime("%Y-%m-%d")
            conn.execute(
                """
                INSERT INTO gamma_snapshots(
                    symbol, snapshot_date, ts_collected_ms, gamma_flip, atm_iv,
                    oi_concentration_top5, zero_dte_share, total_contracts, with_greeks,
                    with_oi, used_open_interest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "SPY",
                    snapshot_date,
                    1_777_777_999_000,
                    None,
                    0.19,
                    17.0,
                    8.5,
                    2100,
                    0,
                    1800,
                    1,
                ),
            )
            conn.commit()

            ctx = backfill.fetch_gamma_context("SPY", timeout=1, conn=conn)
            self.assertIsNotNone(ctx)
            self.assertEqual(ctx["source_name"], "gamma_snapshots")
            self.assertIsNone(ctx["gamma_flip"])
            self.assertAlmostEqual(float(ctx["atm_iv_pct"]), 19.0, places=6)
            self.assertAlmostEqual(float(ctx["oi_concentration_top5"]), 17.0, places=6)
        finally:
            backfill.fetch_json = original_fetch_json
            backfill.MARKETDATA_APP_TOKEN = original_token
            conn.close()

    def test_backfill_gamma_context_carries_recent_gamma_when_today_missing(self) -> None:
        backfill = load_module(
            "pq_backfill_gamma_carry_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        original_fetch_json = backfill.fetch_json
        original_token = backfill.MARKETDATA_APP_TOKEN
        backfill.MARKETDATA_APP_TOKEN = ""

        def _bridge_down(*_args, **_kwargs):
            raise RuntimeError("bridge unavailable")

        backfill.fetch_json = _bridge_down
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE gamma_snapshots (
                    symbol TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    ts_collected_ms INTEGER NOT NULL,
                    gamma_flip REAL,
                    atm_iv REAL,
                    oi_concentration_top5 REAL,
                    zero_dte_share REAL,
                    total_contracts INTEGER,
                    with_greeks INTEGER,
                    with_oi INTEGER,
                    used_open_interest INTEGER
                )
                """
            )
            today = datetime.now(backfill.NY_TZ).date()
            yesterday = today - timedelta(days=1)
            # Today has only OI context (no greeks/IV).
            conn.execute(
                """
                INSERT INTO gamma_snapshots(
                    symbol, snapshot_date, ts_collected_ms, gamma_flip, atm_iv,
                    oi_concentration_top5, zero_dte_share, total_contracts, with_greeks,
                    with_oi, used_open_interest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "SPY",
                    today.strftime("%Y-%m-%d"),
                    1_777_778_111_000,
                    None,
                    None,
                    15.0,
                    7.0,
                    8000,
                    0,
                    7800,
                    1,
                ),
            )
            # Yesterday has valid greeks.
            conn.execute(
                """
                INSERT INTO gamma_snapshots(
                    symbol, snapshot_date, ts_collected_ms, gamma_flip, atm_iv,
                    oi_concentration_top5, zero_dte_share, total_contracts, with_greeks,
                    with_oi, used_open_interest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "SPY",
                    yesterday.strftime("%Y-%m-%d"),
                    1_777_777_111_000,
                    587.0,
                    0.21,
                    12.0,
                    5.0,
                    12000,
                    11900,
                    11900,
                    1,
                ),
            )
            conn.commit()

            ctx = backfill.fetch_gamma_context("SPY", timeout=1, conn=conn)
            self.assertIsNotNone(ctx)
            self.assertEqual(ctx["source_name"], "gamma_snapshots+carry")
            self.assertAlmostEqual(float(ctx["gamma_flip"]), 587.0, places=6)
            self.assertAlmostEqual(float(ctx["atm_iv_pct"]), 21.0, places=6)
            self.assertEqual(ctx["generated_at_date_et"], today)
            self.assertEqual(ctx["carried_from_date_et"], yesterday)
        finally:
            backfill.fetch_json = original_fetch_json
            backfill.MARKETDATA_APP_TOKEN = original_token
            conn.close()

    def test_collect_gamma_history_allows_partial_chain_without_greeks(self) -> None:
        collector = load_module(
            "pq_collect_gamma_partial_chain_test",
            REPO_ROOT / "scripts" / "collect_gamma_history.py",
        )
        snap = collector.summarize_chain(
            symbol="SPY",
            snapshot_date=date(2026, 3, 4),
            chain={
                "strike": [580, 585, 590],
                "side": ["call", "put", "call"],
                "gamma": [None, None, None],
                "iv": [0.2, 0.21, 0.22],
                "openInterest": [1000, 900, 800],
                "delta": [0.25, -0.25, 0.4],
                "expiration": ["2026-03-04", "2026-03-04", "2026-03-11"],
                "underlyingPrice": [587.0, 587.0, 587.0],
            },
            strike_range_pct=0.6,
            max_strikes=200,
        )
        self.assertIsNone(snap["gamma_flip"])
        self.assertEqual(int(snap["with_greeks"]), 0)
        self.assertGreater(int(snap["with_oi"]), 0)
        self.assertIsNotNone(snap["oi_concentration_top5"])

    def test_collect_gamma_history_can_compute_fallback_gamma(self) -> None:
        collector = load_module(
            "pq_collect_gamma_compute_fallback_test",
            REPO_ROOT / "scripts" / "collect_gamma_history.py",
        )
        original_fallback = collector.GAMMA_COMPUTE_FALLBACK
        original_solver = collector.GAMMA_COMPUTE_FALLBACK_SOLVE_IV
        try:
            collector.GAMMA_COMPUTE_FALLBACK = True
            collector.GAMMA_COMPUTE_FALLBACK_SOLVE_IV = False
            snap = collector.summarize_chain(
                symbol="SPY",
                snapshot_date=date(2026, 3, 4),
                chain={
                    "strike": [560, 585, 610],
                    "side": ["call", "put", "call"],
                    "gamma": [None, None, None],
                    "iv": [0.22, 0.21, 0.20],
                    "openInterest": [1000, 1200, 900],
                    "delta": [0.45, -0.45, 0.25],
                    "expiration": ["2026-03-18", "2026-03-18", "2026-03-18"],
                    "underlyingPrice": [587.0, 587.0, 587.0],
                },
                strike_range_pct=0.6,
                max_strikes=200,
            )
        finally:
            collector.GAMMA_COMPUTE_FALLBACK = original_fallback
            collector.GAMMA_COMPUTE_FALLBACK_SOLVE_IV = original_solver

        self.assertIsNotNone(snap["gamma_flip"])
        self.assertEqual(int(snap["with_greeks"]), 0)
        payload = json.loads(snap["payload_json"])
        self.assertGreater(int(payload.get("computed_gamma_count", 0)), 0)
        self.assertGreater(int(payload.get("computed_gamma_from_iv", 0)), 0)

    def test_collect_gamma_history_retries_429_then_succeeds(self) -> None:
        collector = load_module(
            "pq_collect_gamma_retry_429_test",
            REPO_ROOT / "scripts" / "collect_gamma_history.py",
        )

        class _FakeResp:
            def __init__(self, payload: dict) -> None:
                self._raw = json.dumps(payload).encode("utf-8")

            def read(self) -> bytes:
                return self._raw

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

        attempts = {"count": 0}
        sleep_calls: list[float] = []
        original_urlopen = collector.urlopen
        original_sleep = collector.time.sleep
        try:
            def _fake_urlopen(req, timeout=0):
                attempts["count"] += 1
                if attempts["count"] < 3:
                    raise collector.HTTPError(
                        req.full_url,
                        429,
                        "Too Many Requests",
                        {"Retry-After": "0"},
                        None,
                    )
                return _FakeResp(
                    {
                        "s": "ok",
                        "strike": [587.0],
                        "side": ["call"],
                        "gamma": [0.01],
                        "underlyingPrice": [587.0],
                    }
                )

            collector.urlopen = _fake_urlopen
            collector.time.sleep = lambda seconds: sleep_calls.append(float(seconds))
            payload = collector.fetch_marketdata_chain(
                "SPY",
                date(2026, 3, 6),
                timeout_sec=5,
                max_attempts=4,
                retry_base_sec=0.001,
                retry_max_sec=0.001,
                retry_jitter_sec=0.0,
            )
        finally:
            collector.urlopen = original_urlopen
            collector.time.sleep = original_sleep

        self.assertEqual(payload.get("s"), "ok")
        self.assertEqual(attempts["count"], 3)
        self.assertEqual(len(sleep_calls), 2)
        self.assertTrue(all(abs(seconds - 0.001) < 1e-9 for seconds in sleep_calls))

    def test_enrich_touch_events_uses_carry_and_does_not_null_overwrite(self) -> None:
        db = self.tmp / "enrich_gamma.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                """
                CREATE TABLE touch_events (
                    symbol TEXT NOT NULL,
                    ts_event INTEGER NOT NULL,
                    touch_price REAL,
                    rv_30 REAL,
                    gamma_flip REAL,
                    gamma_mode INTEGER,
                    gamma_flip_dist_bps REAL,
                    gamma_confidence INTEGER,
                    oi_concentration_top5 REAL,
                    zero_dte_share REAL,
                    iv_rv_state INTEGER,
                    data_quality REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE gamma_snapshots (
                    symbol TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    ts_collected_ms INTEGER NOT NULL,
                    gamma_flip REAL,
                    oi_concentration_top5 REAL,
                    zero_dte_share REAL,
                    atm_iv REAL,
                    total_contracts INTEGER,
                    with_greeks INTEGER,
                    with_oi INTEGER,
                    used_open_interest INTEGER
                )
                """
            )

            event_day = date(2026, 3, 4)
            event_ts = int(datetime(2026, 3, 4, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
            yesterday = (event_day - timedelta(days=1)).strftime("%Y-%m-%d")

            # SPY row should get gamma/IV from carry.
            conn.execute(
                """
                INSERT INTO touch_events(
                    symbol, ts_event, touch_price, rv_30, gamma_flip, gamma_mode,
                    gamma_flip_dist_bps, gamma_confidence, oi_concentration_top5,
                    zero_dte_share, iv_rv_state, data_quality
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("SPY", event_ts, 590.0, 20.0, None, None, None, None, None, None, None, None),
            )
            # QQQ row has no snapshot/carry, so overwrite must not clear existing fields.
            conn.execute(
                """
                INSERT INTO touch_events(
                    symbol, ts_event, touch_price, rv_30, gamma_flip, gamma_mode,
                    gamma_flip_dist_bps, gamma_confidence, oi_concentration_top5,
                    zero_dte_share, iv_rv_state, data_quality
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("QQQ", event_ts, 500.0, 25.0, 400.0, 1, 2500.0, 2, 10.0, 0.3, 1, 0.5),
            )

            # Same-day snapshot has OI context but no greeks/IV.
            conn.execute(
                """
                INSERT INTO gamma_snapshots(
                    symbol, snapshot_date, ts_collected_ms, gamma_flip, oi_concentration_top5,
                    zero_dte_share, atm_iv, total_contracts, with_greeks, with_oi, used_open_interest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("SPY", "2026-03-04", 1_777_778_111_000, None, 16.4, 0.0, None, 7806, 0, 7806, 1),
            )
            # Prior-day snapshot has valid greeks and should be used as carry.
            conn.execute(
                """
                INSERT INTO gamma_snapshots(
                    symbol, snapshot_date, ts_collected_ms, gamma_flip, oi_concentration_top5,
                    zero_dte_share, atm_iv, total_contracts, with_greeks, with_oi, used_open_interest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("SPY", yesterday, 1_777_777_111_000, 587.0, 12.7, 0.0, 0.21, 11992, 11992, 11992, 1),
            )
            conn.commit()
        finally:
            conn.close()

        proc = run_cmd(
            [
                PYTHON,
                "scripts/enrich_touch_events_from_gamma.py",
                "--db",
                str(db),
                "--start-date",
                "2026-03-04",
                "--end-date",
                "2026-03-04",
                "--overwrite",
            ],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(int(payload["matched_rows"]), 2)
        self.assertEqual(int(payload["updated_rows"]), 1)

        conn = sqlite3.connect(str(db))
        try:
            spy = conn.execute(
                """
                SELECT gamma_flip, iv_rv_state, oi_concentration_top5, zero_dte_share
                FROM touch_events
                WHERE symbol = 'SPY'
                """
            ).fetchone()
            self.assertIsNotNone(spy)
            self.assertAlmostEqual(float(spy[0]), 587.0, places=6)
            self.assertEqual(int(spy[1]), 0)
            self.assertAlmostEqual(float(spy[2]), 16.4, places=6)
            self.assertAlmostEqual(float(spy[3]), 0.0, places=6)

            qqq = conn.execute(
                """
                SELECT gamma_flip, iv_rv_state
                FROM touch_events
                WHERE symbol = 'QQQ'
                """
            ).fetchone()
            self.assertIsNotNone(qqq)
            self.assertAlmostEqual(float(qqq[0]), 400.0, places=6)
            self.assertEqual(int(qqq[1]), 1)
        finally:
            conn.close()

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
        self.assertIn("score_unscored_touch_events.py", retrain_script)
        self.assertIn("RETRAIN_SCORE_UNSCORED_VERIFY_ON_RETRAIN", retrain_script)
        self.assertIn("RETRAIN_SCORE_UNSCORED_MAX_REMAINING", retrain_script)
        self.assertIn("RETRAIN_SCORE_UNSCORED_TIMEOUT_SEC", retrain_script)
        self.assertIn("RETRAIN_SCORE_UNSCORED_MAX_ATTEMPTS", retrain_script)
        self.assertIn("RETRAIN_SCORE_UNSCORED_FAIL_ON_PARTIAL", retrain_script)
        self.assertIn("--timeout-sec", retrain_script)
        self.assertIn("--max-attempts", retrain_script)
        self.assertIn("--fail-on-partial", retrain_script)

    def test_run_all_health_probe_retry_contract_present(self) -> None:
        run_all = (REPO_ROOT / "server" / "run_all.sh").read_text(encoding="utf-8")
        self.assertIn("MONITOR_HEALTH_TIMEOUT_SEC", run_all)
        self.assertIn("MONITOR_HEALTH_RETRIES", run_all)
        self.assertIn("MONITOR_HEALTH_RETRY_SLEEP_SEC", run_all)
        self.assertIn('health failed after ${max_attempts} attempts', run_all)

        proc = run_cmd(["bash", "-n", "server/run_all.sh"], cwd=REPO_ROOT)
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_score_unscored_touch_events_runtime_behavior(self) -> None:
        scorer = load_module(
            "pq_score_unscored_touch_events_runtime_test",
            REPO_ROOT / "scripts" / "score_unscored_touch_events.py",
        )

        db = self.tmp / "score_unscored.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                """
                CREATE TABLE touch_events(
                    event_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    ts_event INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE prediction_log(
                    event_id TEXT NOT NULL,
                    ts_prediction INTEGER NOT NULL,
                    is_preview INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            now_ms = int(time.time() * 1000)
            rows = [
                ("evt_scored", "SPY", now_ms - 60_000),
                ("evt_missing_1", "SPY", now_ms - 120_000),
                ("evt_missing_2", "SPY", now_ms - 180_000),
            ]
            conn.executemany(
                "INSERT INTO touch_events(event_id, symbol, ts_event) VALUES (?, ?, ?)",
                rows,
            )
            conn.execute(
                "INSERT INTO prediction_log(event_id, ts_prediction, is_preview) VALUES (?, ?, ?)",
                ("evt_scored", now_ms, 0),
            )
            conn.commit()
        finally:
            conn.close()

        class _FakeResp:
            def __init__(self, payload: dict) -> None:
                self._raw = json.dumps(payload).encode("utf-8")

            def read(self) -> bytes:
                return self._raw

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

        posted_event_ids: list[str] = []
        original_urlopen = scorer.urlopen
        try:

            def _fake_urlopen(req, timeout=0):  # noqa: ANN001
                payload = json.loads(req.data.decode("utf-8"))
                events = payload.get("events", [])
                posted_event_ids.extend(ev.get("event_id") for ev in events if ev.get("event_id"))
                return _FakeResp({"results": [{"status": "ok"} for _ in events]})

            scorer.urlopen = _fake_urlopen
            args = scorer.parse_args(
                [
                    "--db",
                    str(db),
                    "--symbols",
                    "SPY",
                    "--lookback-days",
                    "30",
                    "--limit",
                    "10",
                    "--batch-size",
                    "2",
                ]
            )
            result = scorer.run(args)
        finally:
            scorer.urlopen = original_urlopen

        self.assertEqual(result.get("status"), "ok")
        self.assertEqual(result.get("attempted"), 2)
        self.assertEqual(result.get("scored_ok"), 2)
        self.assertEqual(result.get("failed"), 0)
        self.assertEqual(set(posted_event_ids), {"evt_missing_1", "evt_missing_2"})
        self.assertNotIn("evt_scored", posted_event_ids)

    def test_score_unscored_touch_events_single_fallback_and_verify(self) -> None:
        scorer = load_module(
            "pq_score_unscored_touch_events_fallback_test",
            REPO_ROOT / "scripts" / "score_unscored_touch_events.py",
        )

        db = self.tmp / "score_unscored_fallback.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                """
                CREATE TABLE touch_events(
                    event_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    ts_event INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE prediction_log(
                    event_id TEXT NOT NULL,
                    ts_prediction INTEGER NOT NULL,
                    is_preview INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            now_ms = int(time.time() * 1000)
            rows = [
                ("evt_missing_a", "SPY", now_ms - 60_000),
                ("evt_missing_b", "SPY", now_ms - 120_000),
                ("evt_missing_c", "SPY", now_ms - 180_000),
            ]
            conn.executemany(
                "INSERT INTO touch_events(event_id, symbol, ts_event) VALUES (?, ?, ?)",
                rows,
            )
            conn.commit()
        finally:
            conn.close()

        class _FakeResp:
            def __init__(self, payload: dict) -> None:
                self._raw = json.dumps(payload).encode("utf-8")

            def read(self) -> bytes:
                return self._raw

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

        posted_sizes: list[int] = []
        original_urlopen = scorer.urlopen
        try:

            def _fake_urlopen(req, timeout=0):  # noqa: ANN001
                payload = json.loads(req.data.decode("utf-8"))
                events = payload.get("events", [])
                posted_sizes.append(len(events))
                if len(events) > 1:
                    raise URLError("batch failed")
                return _FakeResp({"results": [{"status": "ok"}]})

            scorer.urlopen = _fake_urlopen
            args = scorer.parse_args(
                [
                    "--db",
                    str(db),
                    "--symbols",
                    "SPY",
                    "--lookback-days",
                    "30",
                    "--limit",
                    "10",
                    "--batch-size",
                    "3",
                    "--max-attempts",
                    "1",
                ]
            )
            result = scorer.run(args)
        finally:
            scorer.urlopen = original_urlopen

        # Batch request fails once; scorer should retry each event individually.
        self.assertEqual(result.get("status"), "ok")
        self.assertEqual(result.get("attempted"), 3)
        self.assertEqual(result.get("scored_ok"), 3)
        self.assertEqual(result.get("failed"), 0)
        self.assertEqual(result.get("single_fallback_attempted"), 3)
        self.assertEqual(result.get("single_fallback_scored"), 3)
        self.assertEqual(result.get("single_fallback_failed"), 0)
        self.assertIn(3, posted_sizes)
        self.assertGreaterEqual(posted_sizes.count(1), 3)

    def test_score_unscored_touch_events_verify_threshold_enforced(self) -> None:
        scorer = load_module(
            "pq_score_unscored_touch_events_verify_threshold_test",
            REPO_ROOT / "scripts" / "score_unscored_touch_events.py",
        )

        db = self.tmp / "score_unscored_verify.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                """
                CREATE TABLE touch_events(
                    event_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    ts_event INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE prediction_log(
                    event_id TEXT NOT NULL,
                    ts_prediction INTEGER NOT NULL,
                    is_preview INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            now_ms = int(time.time() * 1000)
            conn.execute(
                "INSERT INTO touch_events(event_id, symbol, ts_event) VALUES (?, ?, ?)",
                ("evt_missing_z", "SPY", now_ms - 60_000),
            )
            conn.commit()
        finally:
            conn.close()

        original_urlopen = scorer.urlopen
        try:

            def _always_fail(req, timeout=0):  # noqa: ANN001
                raise URLError("service unavailable")

            scorer.urlopen = _always_fail
            args = scorer.parse_args(
                [
                    "--db",
                    str(db),
                    "--symbols",
                    "SPY",
                    "--lookback-days",
                    "30",
                    "--limit",
                    "10",
                    "--batch-size",
                    "1",
                    "--max-attempts",
                    "1",
                    "--verify-after",
                    "--max-remaining",
                    "0",
                    "--no-single-fallback-on-failure",
                ]
            )
            result = scorer.run(args)
        finally:
            scorer.urlopen = original_urlopen

        self.assertEqual(result.get("status"), "error")
        self.assertEqual(result.get("attempted"), 1)
        self.assertEqual(result.get("failed"), 1)
        self.assertEqual(result.get("remaining_unscored"), 1)
        self.assertEqual(result.get("max_remaining"), 0)

    def test_score_unscored_touch_events_transport_circuit_breaker(self) -> None:
        scorer = load_module(
            "pq_score_unscored_touch_events_transport_breaker_test",
            REPO_ROOT / "scripts" / "score_unscored_touch_events.py",
        )

        db = self.tmp / "score_unscored_transport_breaker.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                """
                CREATE TABLE touch_events(
                    event_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    ts_event INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE prediction_log(
                    event_id TEXT NOT NULL,
                    ts_prediction INTEGER NOT NULL,
                    is_preview INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            now_ms = int(time.time() * 1000)
            rows = [
                (f"evt_transport_{idx}", "SPY", now_ms - (idx + 1) * 60_000)
                for idx in range(5)
            ]
            conn.executemany(
                "INSERT INTO touch_events(event_id, symbol, ts_event) VALUES (?, ?, ?)",
                rows,
            )
            conn.commit()
        finally:
            conn.close()

        original_urlopen = scorer.urlopen
        try:

            def _always_transport_fail(req, timeout=0):  # noqa: ANN001
                raise URLError("timed out")

            scorer.urlopen = _always_transport_fail
            args = scorer.parse_args(
                [
                    "--db",
                    str(db),
                    "--symbols",
                    "SPY",
                    "--lookback-days",
                    "30",
                    "--limit",
                    "10",
                    "--batch-size",
                    "1",
                    "--max-attempts",
                    "1",
                    "--no-single-fallback-on-failure",
                    "--max-consecutive-transport-failures",
                    "2",
                ]
            )
            result = scorer.run(args)
        finally:
            scorer.urlopen = original_urlopen

        self.assertEqual(result.get("status"), "error")
        self.assertEqual(result.get("attempted"), 5)
        self.assertEqual(result.get("processed_events"), 2)
        self.assertEqual(result.get("failed"), 5)
        self.assertTrue(result.get("aborted_early"))
        self.assertIn("consecutive transport failures", str(result.get("aborted_reason")))

    def test_30m_shadow_horizon_contract_present(self) -> None:
        ml_server = (REPO_ROOT / "server" / "ml_server.py").read_text(encoding="utf-8")
        self.assertIn("ML_SHADOW_HORIZONS", ml_server)
        self.assertIn("ML_REGIME_POLICY_MODE", ml_server)
        self.assertIn("regime_policy", ml_server)
        self.assertIn("_compute_trade_regime", ml_server)
        self.assertIn("signal_30m", ml_server)
        self.assertIn("prob_reject_30m", ml_server)
        self.assertIn("threshold_break_30m", ml_server)
        self.assertIn("ML_ANALOG_BLEND_MODE", ml_server)
        self.assertIn("analog_blend", ml_server)
        self.assertIn("promotion_gate", ml_server)

        dashboard = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        self.assertIn('id="ml-signal-30m"', dashboard)
        self.assertIn('id="ml-reject-30m"', dashboard)
        self.assertIn('id="ml-break-30m"', dashboard)
        self.assertIn("const horizons = [5, 15, 30, 60];", dashboard)

        migrate_db = (REPO_ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        match = re.search(r"LATEST_SCHEMA_VERSION\s*=\s*(\d+)", migrate_db)
        self.assertIsNotNone(match, msg="migrate_db.py must declare LATEST_SCHEMA_VERSION")
        self.assertGreaterEqual(int(match.group(1)), 7)
        self.assertIn("migration_5_prediction_log_shadow_30m", migrate_db)
        self.assertIn("migration_6_gamma_snapshots", migrate_db)
        self.assertIn("migration_7_prediction_log_regime_policy", migrate_db)
        self.assertIn("migration_8_prediction_log_analog", migrate_db)
        self.assertIn("gamma_snapshots", migrate_db)
        self.assertIn("signal_30m", migrate_db)
        self.assertIn("regime_policy_json", migrate_db)
        self.assertIn("analog_json", migrate_db)

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

    def test_regime_policy_shadow_and_active_runtime_behavior(self) -> None:
        ml_server = load_module("ml_server_regime_policy_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        ml_server.build_feature_row = lambda event: {
            "x": 1.0,
            "regime_type": event.get("regime_type"),
            "rv_regime": event.get("rv_regime"),
            "or_size_atr": event.get("or_size_atr"),
            "or_breakout": event.get("or_breakout"),
            "overnight_gap_atr": event.get("overnight_gap_atr"),
            "gamma_mode": event.get("gamma_mode"),
            "distance_atr_ratio": event.get("distance_atr_ratio"),
        }
        ml_server.collect_missing = lambda _features: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_REGIME_THRESHOLD_MAX_DELTA = 0.05
        ml_server.ML_REGIME_COMPRESSION_REJECT_DELTA = -0.02
        ml_server.ML_REGIME_COMPRESSION_BREAK_DELTA = 0.02
        ml_server.ML_REGIME_EXPANSION_REJECT_DELTA = 0.02
        ml_server.ML_REGIME_EXPANSION_BREAK_DELTA = -0.02
        ml_server.ML_REGIME_GUARD_EXPANSION_NEAR_MODE = "off"
        ml_server.ML_REGIME_GUARD_EXPANSION_NEAR_STRATEGY = "no_trade"
        ml_server.ML_REGIME_GUARD_EXPANSION_NEAR_REJECT_DELTA = 0.03
        ml_server.ML_REGIME_GUARD_EXPANSION_NEAR_BREAK_DELTA = 0.03

        def set_registry(*, reject_prob: float, break_prob: float) -> None:
            ml_server.registry.models = {
                "reject": {
                    5: {
                        "feature_columns": ["x"],
                        "pipeline": DummyModel(reject_prob),
                        "calibration": "sigmoid",
                    }
                },
                "break": {
                    5: {
                        "feature_columns": ["x"],
                        "pipeline": DummyModel(break_prob),
                        "calibration": "sigmoid",
                    }
                },
            }
            ml_server.registry.thresholds = {"reject": {5: 0.5}, "break": {5: 0.5}}
            ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": int(time.time() * 1000)}

        # Compression event: baseline says break, regime policy says reject.
        set_registry(reject_prob=0.49, break_prob=0.51)
        compression_event = {
            "event_id": "regime_shadow_compression",
            "regime_type": 3,
            "rv_regime": 1,
            "or_size_atr": 0.2,
            "or_breakout": 0,
            "overnight_gap_atr": 0.05,
            "gamma_mode": 1,
        }

        ml_server.ML_REGIME_POLICY_MODE = "shadow"
        shadow_result = ml_server._score_event(compression_event)
        self.assertEqual(shadow_result["signals"].get("signal_5m"), "break")
        self.assertEqual(
            shadow_result["regime_policy"]["regime"]["signals"].get("signal_5m"),
            "reject",
        )
        self.assertEqual(shadow_result["regime_policy"]["selected_policy"], "baseline")
        self.assertIn("REGIME_POLICY_DIVERGENCE", shadow_result["quality_flags"])

        ml_server.ML_REGIME_POLICY_MODE = "active"
        active_result = ml_server._score_event(compression_event)
        self.assertEqual(active_result["signals"].get("signal_5m"), "reject")
        self.assertEqual(active_result["regime_policy"]["selected_policy"], "regime_active")
        self.assertEqual(active_result["regime_policy"]["trade_regime"], "compression")
        self.assertAlmostEqual(
            float(active_result["thresholds"]["threshold_reject_5m"]),
            0.48,
            places=6,
        )
        self.assertAlmostEqual(
            float(active_result["thresholds"]["threshold_break_5m"]),
            0.52,
            places=6,
        )

        # Compression + ultra ATR zone applies an additional cautious overlay.
        ultra_compression_event = {
            "event_id": "regime_active_compression_ultra",
            "regime_type": 3,
            "rv_regime": 1,
            "or_size_atr": 0.2,
            "or_breakout": 0,
            "overnight_gap_atr": 0.05,
            "gamma_mode": 1,
            "distance_atr_ratio": 0.03,
        }
        ultra_result = ml_server._score_event(ultra_compression_event)
        self.assertEqual(ultra_result["regime_policy"]["atr_zone"], "ultra")
        self.assertTrue(bool(ultra_result["regime_policy"]["atr_overlay"]["applied"]))
        self.assertAlmostEqual(
            float(ultra_result["thresholds"]["threshold_reject_5m"]),
            0.50,
            places=6,
        )
        self.assertAlmostEqual(
            float(ultra_result["thresholds"]["threshold_break_5m"]),
            0.51,
            places=6,
        )
        self.assertEqual(ultra_result["signals"].get("signal_5m"), "break")

        # Expansion event: baseline says reject, regime policy says break.
        set_registry(reject_prob=0.51, break_prob=0.49)
        expansion_event = {
            "event_id": "regime_active_expansion",
            "regime_type": 4,
            "rv_regime": 3,
            "or_size_atr": 0.9,
            "or_breakout": 1,
            "overnight_gap_atr": 0.7,
            "gamma_mode": -1,
        }
        expansion_result = ml_server._score_event(expansion_event)
        self.assertEqual(expansion_result["signals"].get("signal_5m"), "break")
        self.assertEqual(expansion_result["regime_policy"]["trade_regime"], "expansion")

        # Unknown regime in active mode falls back to baseline.
        neutral_event = {
            "event_id": "regime_active_neutral",
            "regime_type": None,
            "rv_regime": None,
            "or_size_atr": None,
            "or_breakout": None,
            "overnight_gap_atr": None,
            "gamma_mode": None,
        }
        neutral_result = ml_server._score_event(neutral_event)
        self.assertEqual(neutral_result["signals"].get("signal_5m"), "reject")
        self.assertEqual(neutral_result["regime_policy"]["trade_regime"], "neutral")
        self.assertEqual(neutral_result["regime_policy"]["selected_policy"], "baseline")

        # Expansion + near guardrail stays observational in shadow mode.
        set_registry(reject_prob=0.51, break_prob=0.52)
        near_expansion_event = {
            "event_id": "regime_expansion_near_guardrail",
            "regime_type": 4,
            "rv_regime": 3,
            "or_size_atr": 0.9,
            "or_breakout": 1,
            "overnight_gap_atr": 0.7,
            "gamma_mode": -1,
            "distance_atr_ratio": 0.07,
        }
        ml_server.ML_REGIME_POLICY_MODE = "active"
        ml_server.ML_REGIME_GUARD_EXPANSION_NEAR_MODE = "shadow"
        guardrail_shadow = ml_server._score_event(near_expansion_event)
        self.assertEqual(guardrail_shadow["signals"].get("signal_5m"), "break")
        self.assertEqual(guardrail_shadow["regime_policy"]["selected_policy"], "regime_active")
        self.assertTrue(bool(guardrail_shadow["regime_policy"]["guardrail"]["triggered"]))
        self.assertFalse(bool(guardrail_shadow["regime_policy"]["guardrail"]["applied"]))
        self.assertEqual(
            guardrail_shadow["regime_policy"]["guardrail"]["signals"].get("signal_5m"),
            "no_edge",
        )
        self.assertIn("REGIME_GUARDRAIL_DIVERGENCE", guardrail_shadow["quality_flags"])

        # In active mode the same guardrail becomes enforceable and reversible.
        ml_server.ML_REGIME_GUARD_EXPANSION_NEAR_MODE = "active"
        guardrail_active = ml_server._score_event(near_expansion_event)
        self.assertEqual(guardrail_active["signals"].get("signal_5m"), "no_edge")
        self.assertEqual(guardrail_active["regime_policy"]["selected_policy"], "guardrail_no_trade")
        self.assertTrue(bool(guardrail_active["regime_policy"]["guardrail"]["applied"]))
        self.assertAlmostEqual(
            float(guardrail_active["thresholds"]["threshold_reject_5m"]),
            0.99,
            places=6,
        )
        self.assertAlmostEqual(
            float(guardrail_active["thresholds"]["threshold_break_5m"]),
            0.99,
            places=6,
        )

        # no_trade guardrail is a hard block even when model confidence is extreme.
        set_registry(reject_prob=0.9995, break_prob=0.9997)
        guardrail_extreme = ml_server._score_event(
            {
                **near_expansion_event,
                "event_id": "regime_expansion_near_guardrail_extreme",
            }
        )
        self.assertEqual(guardrail_extreme["signals"].get("signal_5m"), "no_edge")
        self.assertEqual(guardrail_extreme["regime_policy"]["selected_policy"], "guardrail_no_trade")
        self.assertTrue(bool(guardrail_extreme["regime_policy"]["guardrail"]["applied"]))

    def test_feature_drift_respects_min_count_and_ignore_columns(self) -> None:
        ml_server = load_module("ml_server_feature_drift_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        ml_server.build_feature_row = lambda _event: {
            "x": 1.0,
            "hist_sample_size": 100.0,
            "overnight_gap_atr": -0.6,
        }
        ml_server.collect_missing = lambda _features: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_FEATURE_DRIFT_IGNORE_COLUMNS = {"hist_sample_size"}
        ml_server.ML_FEATURE_DRIFT_MIN_FEATURES = 2

        feature_bounds = {
            "hist_sample_size": {"p1": 0.0, "p99": 94.0},
            "overnight_gap_atr": {"p1": -0.5, "p99": 1.0},
        }
        ml_server.registry.models = {
            "reject": {
                5: {
                    "feature_columns": ["x", "hist_sample_size", "overnight_gap_atr"],
                    "pipeline": DummyModel(0.62),
                    "calibration": "sigmoid",
                    "feature_bounds": feature_bounds,
                }
            },
            "break": {
                5: {
                    "feature_columns": ["x", "hist_sample_size", "overnight_gap_atr"],
                    "pipeline": DummyModel(0.21),
                    "calibration": "sigmoid",
                    "feature_bounds": feature_bounds,
                }
            },
        }
        ml_server.registry.thresholds = {"reject": {5: 0.5}, "break": {5: 0.5}}
        ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": int(time.time() * 1000)}

        high_threshold_result = ml_server._score_event({"event_id": "drift_gate_high_threshold"})
        self.assertEqual(high_threshold_result["scores"]["drifted_features_reject_5m"], ["overnight_gap_atr"])
        self.assertEqual(high_threshold_result["scores"]["drifted_features_break_5m"], ["overnight_gap_atr"])
        self.assertNotIn("FEATURE_DRIFT_reject_5m", high_threshold_result["quality_flags"])
        self.assertNotIn("FEATURE_DRIFT_break_5m", high_threshold_result["quality_flags"])

        ml_server.ML_FEATURE_DRIFT_MIN_FEATURES = 1
        low_threshold_result = ml_server._score_event({"event_id": "drift_gate_low_threshold"})
        self.assertIn("FEATURE_DRIFT_reject_5m", low_threshold_result["quality_flags"])
        self.assertIn("FEATURE_DRIFT_break_5m", low_threshold_result["quality_flags"])

    def test_analog_shadow_fields_and_disagreement_flag(self) -> None:
        ml_server = load_module("ml_server_analog_shadow_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        now_ms = int(time.time() * 1000)
        ml_server.build_feature_row = lambda event: {
            "x": 1.0,
            "distance_atr_ratio": event.get("distance_atr_ratio"),
            "tod_bucket": ml_server._analog_tod_bucket(event.get("ts_event")),
        }
        ml_server.collect_missing = lambda _event: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_ANALOG_ENABLED = True
        ml_server.ML_ANALOG_MIN_POOL = 10
        ml_server.ML_ANALOG_MIN_N = 10
        ml_server.ML_ANALOG_MIN_EFFECTIVE_N = 5.0
        ml_server.ML_ANALOG_MAX_MEAN_DISTANCE = 3.0
        ml_server.ML_ANALOG_MAX_CI_WIDTH = 0.8
        ml_server.ML_ANALOG_DISAGREEMENT_FLAG = 0.25

        ml_server.registry.models = {
            "reject": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
            "break": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
        }
        ml_server.registry.thresholds = {"reject": {5: 0.5}, "break": {5: 0.5}}
        ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": now_ms}

        tod = ml_server._analog_tod_bucket(now_ms - 60_000)
        rows = []
        for idx in range(40):
            rows.append(
                {
                    "event_id": f"hist_{idx}",
                    "symbol": "SPY",
                    "ts_event": now_ms - (idx + 5) * 60_000,
                    "level_family": "support",
                    "tod_bucket": tod,
                    "regime_bucket": "compression",
                    "gamma_mode": 1,
                    "distance_bps": 2.0 + (idx % 5) * 0.2,
                    "distance_atr_ratio": 0.08 + (idx % 5) * 0.01,
                    "rv_30": 12.0 + (idx % 5) * 0.1,
                    "or_size_atr": 0.25 + (idx % 5) * 0.01,
                    "overnight_gap_atr": 0.1 + (idx % 5) * 0.01,
                    "reject": 1.0 if idx < 30 else 0.0,
                    "break": 0.0 if idx < 30 else 1.0,
                }
            )
        ml_server.analog_engine.enabled = True
        ml_server.analog_engine.error = None
        ml_server.analog_engine.loaded_at_ms = now_ms
        ml_server.analog_engine.rows_by_horizon = {5: rows}

        result = ml_server._score_event(
            {
                "event_id": "analog_case_1",
                "symbol": "SPY",
                "ts_event": now_ms,
                "level_type": "S1",
                "distance_bps": 2.1,
                "distance_atr_ratio": 0.09,
                "rv_30": 12.2,
                "or_size_atr": 0.27,
                "overnight_gap_atr": 0.11,
                "regime_type": 3,
                "rv_regime": 1,
                "gamma_mode": 1,
            }
        )

        self.assertIn("analogs", result)
        self.assertIn("5", result["analogs"]["horizons"])
        self.assertIsNotNone(result["scores"]["analog_reject_5m"])
        self.assertIsNotNone(result["scores"]["analog_break_5m"])
        self.assertGreater(float(result["scores"]["analog_n_5m"]), 0.0)
        self.assertIn("ANALOG_DISAGREE_5m", result["quality_flags"])

    def test_analog_kernel_uses_vwap_side_and_ema_stack(self) -> None:
        ml_server = load_module("ml_server_analog_kernel_new_features_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        now_ms = int(time.time() * 1000)
        ml_server.build_feature_row = lambda event: {
            "x": 1.0,
            "distance_atr_ratio": event.get("distance_atr_ratio"),
            "vwap_dist_bps_calc": event.get("vwap_dist_bps"),
            "ema_state_calc": event.get("ema_state"),
            "tod_bucket": ml_server._analog_tod_bucket(event.get("ts_event")),
        }
        ml_server.collect_missing = lambda _event: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_ANALOG_ENABLED = True
        ml_server.ML_ANALOG_MIN_POOL = 10
        ml_server.ML_ANALOG_MIN_N = 10
        ml_server.ML_ANALOG_MIN_EFFECTIVE_N = 5.0
        ml_server.ML_ANALOG_MAX_MEAN_DISTANCE = 3.0
        ml_server.ML_ANALOG_MAX_CI_WIDTH = 0.8
        ml_server.ML_ANALOG_MIN_FEATURES = 1
        ml_server.ML_ANALOG_MIN_FEATURE_OVERLAP = 1
        ml_server.ML_ANALOG_MIN_FEATURE_SUPPORT = 10
        ml_server.ML_ANALOG_FEATURE_WEIGHTS = {
            "distance_bps": 0.0,
            "distance_atr_ratio": 0.0,
            "vwap_side": 1.0,
            "ema_stack": 1.0,
            "rv_30": 0.0,
            "or_size_atr": 0.0,
            "overnight_gap_atr": 0.0,
        }

        ml_server.registry.models = {
            "reject": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
            "break": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
        }
        ml_server.registry.thresholds = {"reject": {5: 0.5}, "break": {5: 0.5}}
        ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": now_ms}

        tod = ml_server._analog_tod_bucket(now_ms - 60_000)
        rows = []
        for idx in range(40):
            bullish = idx < 30
            rows.append(
                {
                    "event_id": f"kernel_feat_{idx}",
                    "symbol": "SPY",
                    "ts_event": now_ms - (idx + 5) * 60_000,
                    "level_family": "support",
                    "tod_bucket": tod,
                    "regime_bucket": "compression",
                    "gamma_mode": 1,
                    "distance_bps": 2.0,
                    "distance_atr_ratio": 0.08,
                    "vwap_side": 1.0 if bullish else -1.0,
                    "ema_stack": 1.0 if bullish else -1.0,
                    "rv_30": 12.0,
                    "or_size_atr": 0.25,
                    "overnight_gap_atr": 0.1,
                    "reject": 1.0 if bullish else 0.0,
                    "break": 0.0 if bullish else 1.0,
                }
            )
        ml_server.analog_engine.enabled = True
        ml_server.analog_engine.error = None
        ml_server.analog_engine.loaded_at_ms = now_ms
        ml_server.analog_engine.rows_by_horizon = {5: rows}

        result = ml_server._score_event(
            {
                "event_id": "kernel_features_case",
                "symbol": "SPY",
                "ts_event": now_ms,
                "level_type": "S1",
                "distance_bps": 2.1,
                "distance_atr_ratio": 0.09,
                "vwap_dist_bps": 5.0,
                "ema_state": 1,
                "rv_30": 12.2,
                "or_size_atr": 0.27,
                "overnight_gap_atr": 0.11,
                "regime_type": 3,
                "rv_regime": 1,
                "gamma_mode": 1,
            }
        )

        analog_h = (((result.get("analogs") or {}).get("horizons") or {}).get("5")) or {}
        features_used = analog_h.get("features") or []
        self.assertIn("vwap_side", features_used)
        self.assertIn("ema_stack", features_used)
        self.assertGreater(float(result["scores"]["analog_reject_5m"]), float(result["scores"]["analog_break_5m"]))

    def test_analog_blend_active_respects_promotion_gate(self) -> None:
        ml_server = load_module("ml_server_analog_blend_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        now_ms = int(time.time() * 1000)
        ml_server.build_feature_row = lambda event: {
            "x": 1.0,
            "distance_atr_ratio": event.get("distance_atr_ratio"),
            "tod_bucket": ml_server._analog_tod_bucket(event.get("ts_event")),
        }
        ml_server.collect_missing = lambda _event: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_ANALOG_ENABLED = True
        ml_server.ML_ANALOG_MIN_POOL = 10
        ml_server.ML_ANALOG_MIN_N = 10
        ml_server.ML_ANALOG_MIN_EFFECTIVE_N = 5.0
        ml_server.ML_ANALOG_MAX_MEAN_DISTANCE = 3.0
        ml_server.ML_ANALOG_MAX_CI_WIDTH = 0.8
        ml_server.ML_ANALOG_BLEND_MODE = "active"
        ml_server.ML_ANALOG_BLEND_WEIGHT_BASE = 0.6
        ml_server.ML_ANALOG_BLEND_WEIGHT_MAX = 0.6
        ml_server.ML_ANALOG_BLEND_N_EFF_REF = 10.0

        ml_server.registry.models = {
            "reject": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
            "break": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
        }
        ml_server.registry.thresholds = {"reject": {5: 0.5}, "break": {5: 0.5}}
        ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": now_ms}

        tod = ml_server._analog_tod_bucket(now_ms - 60_000)
        rows = []
        for idx in range(40):
            rows.append(
                {
                    "event_id": f"blend_hist_{idx}",
                    "symbol": "SPY",
                    "ts_event": now_ms - (idx + 5) * 60_000,
                    "level_family": "support",
                    "tod_bucket": tod,
                    "regime_bucket": "compression",
                    "gamma_mode": 1,
                    "distance_bps": 2.0 + (idx % 5) * 0.2,
                    "distance_atr_ratio": 0.08 + (idx % 5) * 0.01,
                    "rv_30": 12.0 + (idx % 5) * 0.1,
                    "or_size_atr": 0.25 + (idx % 5) * 0.01,
                    "overnight_gap_atr": 0.1 + (idx % 5) * 0.01,
                    "reject": 1.0 if idx < 30 else 0.0,
                    "break": 0.0 if idx < 30 else 1.0,
                }
            )
        ml_server.analog_engine.enabled = True
        ml_server.analog_engine.error = None
        ml_server.analog_engine.loaded_at_ms = now_ms
        ml_server.analog_engine.rows_by_horizon = {5: rows}

        base_event = {
            "event_id": "blend_case",
            "symbol": "SPY",
            "ts_event": now_ms,
            "level_type": "S1",
            "distance_bps": 2.1,
            "distance_atr_ratio": 0.09,
            "rv_30": 12.2,
            "or_size_atr": 0.27,
            "overnight_gap_atr": 0.11,
            "regime_type": 3,
            "rv_regime": 1,
            "gamma_mode": 1,
        }

        ml_server._read_analog_promotion_gate = lambda: {"status": "fail", "reasons": ["gate_fail"]}
        blocked = ml_server._score_event({**base_event, "event_id": "blend_case_blocked"})
        self.assertAlmostEqual(float(blocked["scores"]["prob_reject_5m"]), 0.2, places=6)
        self.assertIn("ANALOG_BLEND_BLOCKED_GATE", blocked["quality_flags"])

        ml_server._read_analog_promotion_gate = lambda: {"status": "pass", "reasons": []}
        active = ml_server._score_event({**base_event, "event_id": "blend_case_active"})
        self.assertGreater(float(active["scores"]["prob_reject_5m"]), 0.2)
        self.assertIn("ANALOG_BLEND_ACTIVE", active["quality_flags"])
        self.assertEqual(active["analog_blend"]["mode"], "active")
        self.assertTrue(bool(active["analog_blend"]["allow_active_blend"]))

    def test_analog_blend_active_horizon_partial_gate(self) -> None:
        ml_server = load_module("ml_server_analog_blend_horizon_partial_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        now_ms = int(time.time() * 1000)
        ml_server.build_feature_row = lambda event: {
            "x": 1.0,
            "distance_atr_ratio": event.get("distance_atr_ratio"),
            "tod_bucket": ml_server._analog_tod_bucket(event.get("ts_event")),
        }
        ml_server.collect_missing = lambda _event: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_ANALOG_ENABLED = True
        ml_server.ML_ANALOG_MIN_POOL = 10
        ml_server.ML_ANALOG_MIN_N = 10
        ml_server.ML_ANALOG_MIN_EFFECTIVE_N = 5.0
        ml_server.ML_ANALOG_MAX_MEAN_DISTANCE = 3.0
        ml_server.ML_ANALOG_MAX_CI_WIDTH = 0.8
        ml_server.ML_ANALOG_BLEND_MODE = "active"
        ml_server.ML_ANALOG_BLEND_PARTIAL_MODE = "horizon"
        ml_server.ML_ANALOG_BLEND_WEIGHT_BASE = 0.6
        ml_server.ML_ANALOG_BLEND_WEIGHT_MAX = 0.6
        ml_server.ML_ANALOG_BLEND_N_EFF_REF = 10.0

        ml_server.registry.models = {
            "reject": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
            "break": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
        }
        ml_server.registry.thresholds = {"reject": {5: 0.5}, "break": {5: 0.5}}
        ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": now_ms}

        tod = ml_server._analog_tod_bucket(now_ms - 60_000)
        rows = []
        for idx in range(40):
            rows.append(
                {
                    "event_id": f"blend_h_partial_{idx}",
                    "symbol": "SPY",
                    "ts_event": now_ms - (idx + 5) * 60_000,
                    "level_family": "support",
                    "tod_bucket": tod,
                    "regime_bucket": "compression",
                    "gamma_mode": 1,
                    "distance_bps": 2.0 + (idx % 5) * 0.2,
                    "distance_atr_ratio": 0.08 + (idx % 5) * 0.01,
                    "rv_30": 12.0 + (idx % 5) * 0.1,
                    "or_size_atr": 0.25 + (idx % 5) * 0.01,
                    "overnight_gap_atr": 0.1 + (idx % 5) * 0.01,
                    "reject": 1.0 if idx < 30 else 0.0,
                    "break": 0.0 if idx < 30 else 1.0,
                }
            )
        ml_server.analog_engine.enabled = True
        ml_server.analog_engine.error = None
        ml_server.analog_engine.loaded_at_ms = now_ms
        ml_server.analog_engine.rows_by_horizon = {5: rows}

        ml_server._read_analog_promotion_gate = lambda: {
            "status": "fail",
            "reasons": ["insufficient_passed_horizons"],
            "passed_horizons": [5],
            "horizon_results": {
                "5": {"evaluated": True, "pass": True, "reject_pass": True, "break_pass": True}
            },
        }
        result = ml_server._score_event(
            {
                "event_id": "blend_h_partial_case",
                "symbol": "SPY",
                "ts_event": now_ms,
                "level_type": "S1",
                "distance_bps": 2.1,
                "distance_atr_ratio": 0.09,
                "rv_30": 12.2,
                "or_size_atr": 0.27,
                "overnight_gap_atr": 0.11,
                "regime_type": 3,
                "rv_regime": 1,
                "gamma_mode": 1,
            }
        )

        self.assertGreater(float(result["scores"]["prob_reject_5m"]), 0.2)
        self.assertGreater(float(result["scores"]["prob_break_5m"]), 0.2)
        self.assertIn("ANALOG_BLEND_ACTIVE", result["quality_flags"])
        self.assertIn("ANALOG_BLEND_PARTIAL_GATE", result["quality_flags"])
        self.assertNotIn("ANALOG_BLEND_BLOCKED_GATE", result["quality_flags"])
        self.assertEqual(result["analog_blend"]["partial_mode"], "horizon")
        self.assertIn(5, result["analog_blend"]["applied_horizons"])

    def test_analog_blend_active_target_partial_gate(self) -> None:
        ml_server = load_module("ml_server_analog_blend_target_partial_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        now_ms = int(time.time() * 1000)
        ml_server.build_feature_row = lambda event: {
            "x": 1.0,
            "distance_atr_ratio": event.get("distance_atr_ratio"),
            "tod_bucket": ml_server._analog_tod_bucket(event.get("ts_event")),
        }
        ml_server.collect_missing = lambda _event: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_ANALOG_ENABLED = True
        ml_server.ML_ANALOG_MIN_POOL = 10
        ml_server.ML_ANALOG_MIN_N = 10
        ml_server.ML_ANALOG_MIN_EFFECTIVE_N = 5.0
        ml_server.ML_ANALOG_MAX_MEAN_DISTANCE = 3.0
        ml_server.ML_ANALOG_MAX_CI_WIDTH = 0.8
        ml_server.ML_ANALOG_BLEND_MODE = "active"
        ml_server.ML_ANALOG_BLEND_PARTIAL_MODE = "target"
        ml_server.ML_ANALOG_BLEND_WEIGHT_BASE = 0.6
        ml_server.ML_ANALOG_BLEND_WEIGHT_MAX = 0.6
        ml_server.ML_ANALOG_BLEND_N_EFF_REF = 10.0

        ml_server.registry.models = {
            "reject": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
            "break": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
        }
        ml_server.registry.thresholds = {"reject": {5: 0.5}, "break": {5: 0.5}}
        ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": now_ms}

        tod = ml_server._analog_tod_bucket(now_ms - 60_000)
        rows = []
        for idx in range(40):
            rows.append(
                {
                    "event_id": f"blend_t_partial_{idx}",
                    "symbol": "SPY",
                    "ts_event": now_ms - (idx + 5) * 60_000,
                    "level_family": "support",
                    "tod_bucket": tod,
                    "regime_bucket": "compression",
                    "gamma_mode": 1,
                    "distance_bps": 2.0 + (idx % 5) * 0.2,
                    "distance_atr_ratio": 0.08 + (idx % 5) * 0.01,
                    "rv_30": 12.0 + (idx % 5) * 0.1,
                    "or_size_atr": 0.25 + (idx % 5) * 0.01,
                    "overnight_gap_atr": 0.1 + (idx % 5) * 0.01,
                    "reject": 1.0 if idx < 30 else 0.0,
                    "break": 0.0 if idx < 30 else 1.0,
                }
            )
        ml_server.analog_engine.enabled = True
        ml_server.analog_engine.error = None
        ml_server.analog_engine.loaded_at_ms = now_ms
        ml_server.analog_engine.rows_by_horizon = {5: rows}

        ml_server._read_analog_promotion_gate = lambda: {
            "status": "fail",
            "reasons": ["5m:break_delta", "insufficient_passed_horizons"],
            "passed_horizons": [],
            "horizon_results": {
                "5": {"evaluated": True, "pass": False, "reject_pass": True, "break_pass": False}
            },
        }
        result = ml_server._score_event(
            {
                "event_id": "blend_t_partial_case",
                "symbol": "SPY",
                "ts_event": now_ms,
                "level_type": "S1",
                "distance_bps": 2.1,
                "distance_atr_ratio": 0.09,
                "rv_30": 12.2,
                "or_size_atr": 0.27,
                "overnight_gap_atr": 0.11,
                "regime_type": 3,
                "rv_regime": 1,
                "gamma_mode": 1,
            }
        )

        self.assertGreater(float(result["scores"]["prob_reject_5m"]), 0.2)
        self.assertAlmostEqual(float(result["scores"]["prob_break_5m"]), 0.2, places=6)
        self.assertIn("ANALOG_BLEND_ACTIVE", result["quality_flags"])
        self.assertIn("ANALOG_BLEND_PARTIAL_GATE", result["quality_flags"])
        self.assertIn("ANALOG_BLEND_TARGET_SPLIT", result["quality_flags"])
        horizon_payload = result["analog_blend"]["horizons"]["5"]
        self.assertTrue(bool(horizon_payload.get("applied_reject")))
        self.assertFalse(bool(horizon_payload.get("applied_break")))
        analog_horizon_payload = (((result.get("analogs") or {}).get("horizons") or {}).get("5")) or {}
        self.assertTrue(bool(analog_horizon_payload.get("blend_applied_reject")))
        self.assertFalse(bool(analog_horizon_payload.get("blend_applied_break")))

    def test_analog_blend_shift_cap_limits_probability_move(self) -> None:
        ml_server = load_module("ml_server_analog_blend_shift_cap_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        now_ms = int(time.time() * 1000)
        ml_server.build_feature_row = lambda event: {
            "x": 1.0,
            "distance_atr_ratio": event.get("distance_atr_ratio"),
            "tod_bucket": ml_server._analog_tod_bucket(event.get("ts_event")),
        }
        ml_server.collect_missing = lambda _event: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_ANALOG_ENABLED = True
        ml_server.ML_ANALOG_MIN_POOL = 10
        ml_server.ML_ANALOG_MIN_N = 10
        ml_server.ML_ANALOG_MIN_EFFECTIVE_N = 5.0
        ml_server.ML_ANALOG_MAX_MEAN_DISTANCE = 3.0
        ml_server.ML_ANALOG_MAX_CI_WIDTH = 0.8
        ml_server.ML_ANALOG_BLEND_MODE = "active"
        ml_server.ML_ANALOG_BLEND_PARTIAL_MODE = "off"
        ml_server.ML_ANALOG_BLEND_WEIGHT_BASE = 1.0
        ml_server.ML_ANALOG_BLEND_WEIGHT_MAX = 1.0
        ml_server.ML_ANALOG_BLEND_N_EFF_REF = 1.0
        ml_server.ML_ANALOG_BLEND_MAX_SHIFT_REJECT = 0.05
        ml_server.ML_ANALOG_BLEND_MAX_SHIFT_BREAK = 0.05
        ml_server.ML_ANALOG_BLEND_MAX_SHIFT_REJECT_BY_HORIZON = {}
        ml_server.ML_ANALOG_BLEND_MAX_SHIFT_BREAK_BY_HORIZON = {}

        ml_server.registry.models = {
            "reject": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
            "break": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
        }
        ml_server.registry.thresholds = {"reject": {5: 0.5}, "break": {5: 0.5}}
        ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": now_ms}

        tod = ml_server._analog_tod_bucket(now_ms - 60_000)
        rows = []
        for idx in range(40):
            rows.append(
                {
                    "event_id": f"blend_cap_{idx}",
                    "symbol": "SPY",
                    "ts_event": now_ms - (idx + 5) * 60_000,
                    "level_family": "support",
                    "tod_bucket": tod,
                    "regime_bucket": "compression",
                    "gamma_mode": 1,
                    "distance_bps": 2.0 + (idx % 5) * 0.2,
                    "distance_atr_ratio": 0.08 + (idx % 5) * 0.01,
                    "rv_30": 12.0 + (idx % 5) * 0.1,
                    "or_size_atr": 0.25 + (idx % 5) * 0.01,
                    "overnight_gap_atr": 0.1 + (idx % 5) * 0.01,
                    "reject": 1.0,
                    "break": 0.0,
                }
            )
        ml_server.analog_engine.enabled = True
        ml_server.analog_engine.error = None
        ml_server.analog_engine.loaded_at_ms = now_ms
        ml_server.analog_engine.rows_by_horizon = {5: rows}
        ml_server._read_analog_promotion_gate = lambda: {"status": "pass", "reasons": []}

        result = ml_server._score_event(
            {
                "event_id": "blend_cap_case",
                "symbol": "SPY",
                "ts_event": now_ms,
                "level_type": "S1",
                "distance_bps": 2.1,
                "distance_atr_ratio": 0.09,
                "rv_30": 12.2,
                "or_size_atr": 0.27,
                "overnight_gap_atr": 0.11,
                "regime_type": 3,
                "rv_regime": 1,
                "gamma_mode": 1,
            }
        )

        self.assertAlmostEqual(float(result["scores"]["prob_reject_5m"]), 0.25, places=6)
        self.assertAlmostEqual(float(result["scores"]["prob_break_5m"]), 0.15, places=6)
        self.assertIn("ANALOG_BLEND_SHIFT_CAPPED", result["quality_flags"])
        self.assertIn("ANALOG_BLEND_SHIFT_CAPPED_5m", result["quality_flags"])
        horizon_payload = result["analog_blend"]["horizons"]["5"]
        self.assertTrue(bool(horizon_payload.get("blended_reject_capped")))
        self.assertTrue(bool(horizon_payload.get("blended_break_capped")))
        self.assertAlmostEqual(float(horizon_payload.get("blended_reject_shift") or 0.0), 0.05, places=6)
        self.assertAlmostEqual(float(horizon_payload.get("blended_break_shift") or 0.0), -0.05, places=6)

    def test_analog_disagreement_guard_shadow_marks_divergence(self) -> None:
        ml_server = load_module(
            "ml_server_analog_disagreement_guard_shadow_runtime",
            REPO_ROOT / "server" / "ml_server.py",
        )

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        now_ms = int(time.time() * 1000)
        ml_server.build_feature_row = lambda event: {
            "x": 1.0,
            "distance_atr_ratio": event.get("distance_atr_ratio"),
            "tod_bucket": ml_server._analog_tod_bucket(event.get("ts_event")),
        }
        ml_server.collect_missing = lambda _event: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_ANALOG_ENABLED = True
        ml_server.ML_ANALOG_MIN_POOL = 10
        ml_server.ML_ANALOG_MIN_N = 10
        ml_server.ML_ANALOG_MIN_EFFECTIVE_N = 5.0
        ml_server.ML_ANALOG_MAX_MEAN_DISTANCE = 3.0
        ml_server.ML_ANALOG_MAX_CI_WIDTH = 0.8
        ml_server.ML_ANALOG_BLEND_MODE = "off"
        ml_server.ML_ANALOG_DISAGREEMENT_FLAG = 0.25
        ml_server.ML_ANALOG_DISAGREEMENT_GUARD_MODE = "shadow"
        ml_server.ML_ANALOG_DISAGREEMENT_GUARD_HORIZONS = {5}

        ml_server.registry.models = {
            "reject": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.8),
                    "calibration": "sigmoid",
                }
            },
            "break": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
        }
        ml_server.registry.thresholds = {"reject": {5: 0.5}, "break": {5: 0.5}}
        ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": now_ms}

        tod = ml_server._analog_tod_bucket(now_ms - 60_000)
        rows = []
        for idx in range(40):
            rows.append(
                {
                    "event_id": f"guard_shadow_hist_{idx}",
                    "symbol": "SPY",
                    "ts_event": now_ms - (idx + 5) * 60_000,
                    "level_family": "support",
                    "tod_bucket": tod,
                    "regime_bucket": "compression",
                    "gamma_mode": 1,
                    "distance_bps": 2.0 + (idx % 5) * 0.2,
                    "distance_atr_ratio": 0.08 + (idx % 5) * 0.01,
                    "rv_30": 12.0 + (idx % 5) * 0.1,
                    "or_size_atr": 0.25 + (idx % 5) * 0.01,
                    "overnight_gap_atr": 0.1 + (idx % 5) * 0.01,
                    "reject": 0.0,
                    "break": 1.0,
                }
            )
        ml_server.analog_engine.enabled = True
        ml_server.analog_engine.error = None
        ml_server.analog_engine.loaded_at_ms = now_ms
        ml_server.analog_engine.rows_by_horizon = {5: rows}

        result = ml_server._score_event(
            {
                "event_id": "guard_shadow_case",
                "symbol": "SPY",
                "ts_event": now_ms,
                "level_type": "S1",
                "distance_bps": 2.1,
                "distance_atr_ratio": 0.09,
                "rv_30": 12.2,
                "or_size_atr": 0.27,
                "overnight_gap_atr": 0.11,
                "regime_type": 3,
                "rv_regime": 1,
                "gamma_mode": 1,
            }
        )

        self.assertEqual(result["signals"]["signal_5m"], "reject")
        self.assertIn("ANALOG_DISAGREE_5m", result["quality_flags"])
        self.assertIn("ANALOG_DISAGREEMENT_GUARD_DIVERGENCE", result["quality_flags"])
        guard = result.get("analog_disagreement_guard") or {}
        self.assertEqual(guard.get("mode"), "shadow")
        self.assertIn(5, guard.get("triggered_horizons") or [])
        signal_diffs = guard.get("signal_diffs") or {}
        self.assertIn("signal_5m", signal_diffs)
        self.assertFalse(bool((signal_diffs.get("signal_5m") or {}).get("applied")))

    def test_analog_disagreement_guard_active_blocks_signal(self) -> None:
        ml_server = load_module(
            "ml_server_analog_disagreement_guard_active_runtime",
            REPO_ROOT / "server" / "ml_server.py",
        )

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        now_ms = int(time.time() * 1000)
        ml_server.build_feature_row = lambda event: {
            "x": 1.0,
            "distance_atr_ratio": event.get("distance_atr_ratio"),
            "tod_bucket": ml_server._analog_tod_bucket(event.get("ts_event")),
        }
        ml_server.collect_missing = lambda _event: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_ANALOG_ENABLED = True
        ml_server.ML_ANALOG_MIN_POOL = 10
        ml_server.ML_ANALOG_MIN_N = 10
        ml_server.ML_ANALOG_MIN_EFFECTIVE_N = 5.0
        ml_server.ML_ANALOG_MAX_MEAN_DISTANCE = 3.0
        ml_server.ML_ANALOG_MAX_CI_WIDTH = 0.8
        ml_server.ML_ANALOG_BLEND_MODE = "off"
        ml_server.ML_ANALOG_DISAGREEMENT_FLAG = 0.25
        ml_server.ML_ANALOG_DISAGREEMENT_GUARD_MODE = "active"
        ml_server.ML_ANALOG_DISAGREEMENT_GUARD_HORIZONS = {5}

        ml_server.registry.models = {
            "reject": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.8),
                    "calibration": "sigmoid",
                }
            },
            "break": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.2),
                    "calibration": "sigmoid",
                }
            },
        }
        ml_server.registry.thresholds = {"reject": {5: 0.5}, "break": {5: 0.5}}
        ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": now_ms}

        tod = ml_server._analog_tod_bucket(now_ms - 60_000)
        rows = []
        for idx in range(40):
            rows.append(
                {
                    "event_id": f"guard_active_hist_{idx}",
                    "symbol": "SPY",
                    "ts_event": now_ms - (idx + 5) * 60_000,
                    "level_family": "support",
                    "tod_bucket": tod,
                    "regime_bucket": "compression",
                    "gamma_mode": 1,
                    "distance_bps": 2.0 + (idx % 5) * 0.2,
                    "distance_atr_ratio": 0.08 + (idx % 5) * 0.01,
                    "rv_30": 12.0 + (idx % 5) * 0.1,
                    "or_size_atr": 0.25 + (idx % 5) * 0.01,
                    "overnight_gap_atr": 0.1 + (idx % 5) * 0.01,
                    "reject": 0.0,
                    "break": 1.0,
                }
            )
        ml_server.analog_engine.enabled = True
        ml_server.analog_engine.error = None
        ml_server.analog_engine.loaded_at_ms = now_ms
        ml_server.analog_engine.rows_by_horizon = {5: rows}

        result = ml_server._score_event(
            {
                "event_id": "guard_active_case",
                "symbol": "SPY",
                "ts_event": now_ms,
                "level_type": "S1",
                "distance_bps": 2.1,
                "distance_atr_ratio": 0.09,
                "rv_30": 12.2,
                "or_size_atr": 0.27,
                "overnight_gap_atr": 0.11,
                "regime_type": 3,
                "rv_regime": 1,
                "gamma_mode": 1,
            }
        )

        self.assertEqual(result["signals"]["signal_5m"], "no_edge")
        self.assertIn("ANALOG_DISAGREE_5m", result["quality_flags"])
        self.assertIn("ANALOG_DISAGREEMENT_GUARD_ACTIVE", result["quality_flags"])
        guard = result.get("analog_disagreement_guard") or {}
        self.assertEqual(guard.get("mode"), "active")
        self.assertIn(5, guard.get("triggered_horizons") or [])
        self.assertIn(5, guard.get("applied_horizons") or [])
        signal_diffs = guard.get("signal_diffs") or {}
        self.assertTrue(bool((signal_diffs.get("signal_5m") or {}).get("applied")))
        self.assertEqual((signal_diffs.get("signal_5m") or {}).get("after"), "no_edge")
        self.assertIn("analog_disagreement_guard", str(result["regime_policy"].get("selected_policy")))

    def test_ml_prediction_log_persists_regime_policy_fields(self) -> None:
        db = self.tmp / "predlog_regime.sqlite"
        prev_db = os.environ.get("PREDICTION_LOG_DB")
        os.environ["PREDICTION_LOG_DB"] = str(db)
        try:
            ml_server = load_module("ml_server_regime_log_runtime", REPO_ROOT / "server" / "ml_server.py")
        finally:
            if prev_db is None:
                os.environ.pop("PREDICTION_LOG_DB", None)
            else:
                os.environ["PREDICTION_LOG_DB"] = prev_db

        event = {"event_id": "evt_regime_1"}
        result = {
            "model_version": "vtest",
            "feature_version": "v3",
            "best_horizon": 5,
            "abstain": False,
            "scores": {
                "prob_reject_5m": 0.61,
                "prob_break_5m": 0.22,
            },
            "signals": {
                "signal_5m": "reject",
            },
            "thresholds": {
                "threshold_reject_5m": 0.5,
                "threshold_break_5m": 0.5,
            },
            "quality_flags": ["REGIME_POLICY_DIVERGENCE"],
            "regime_policy": {
                "mode": "shadow",
                "trade_regime": "compression",
                "selected_policy": "baseline",
                "signal_diffs": {
                    "signal_5m": {"baseline": "no_edge", "regime": "reject", "selected": "no_edge"}
                },
            },
            "analogs": {
                "enabled": True,
                "best_horizon": 5,
                "best": {
                    "status": "ok",
                    "reject_prob": 0.66,
                    "break_prob": 0.22,
                    "n": 20,
                    "reject_ci_width": 0.12,
                    "break_ci_width": 0.09,
                    "disagreement": 0.05,
                },
            },
        }
        ml_server._log_prediction(event, result)

        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        try:
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(prediction_log)").fetchall()}
            self.assertIn("regime_policy_mode", cols)
            self.assertIn("trade_regime", cols)
            self.assertIn("selected_policy", cols)
            self.assertIn("regime_policy_json", cols)
            self.assertIn("analog_best_reject_prob", cols)
            self.assertIn("analog_best_break_prob", cols)
            self.assertIn("analog_best_n", cols)
            self.assertIn("analog_best_ci_width", cols)
            self.assertIn("analog_best_disagreement", cols)
            self.assertIn("analog_json", cols)

            row = conn.execute(
                """
                SELECT regime_policy_mode, trade_regime, selected_policy, regime_policy_json,
                       analog_best_reject_prob, analog_best_break_prob, analog_best_n,
                       analog_best_ci_width, analog_best_disagreement, analog_json
                FROM prediction_log
                WHERE event_id = ?
                """,
                ("evt_regime_1",),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["regime_policy_mode"], "shadow")
            self.assertEqual(row["trade_regime"], "compression")
            self.assertEqual(row["selected_policy"], "baseline")
            policy_json = json.loads(row["regime_policy_json"])
            self.assertEqual(policy_json.get("mode"), "shadow")
            self.assertAlmostEqual(float(row["analog_best_reject_prob"]), 0.66, places=6)
            self.assertAlmostEqual(float(row["analog_best_break_prob"]), 0.22, places=6)
            self.assertAlmostEqual(float(row["analog_best_n"]), 20.0, places=6)
            self.assertAlmostEqual(float(row["analog_best_ci_width"]), 0.12, places=6)
            self.assertAlmostEqual(float(row["analog_best_disagreement"]), 0.05, places=6)
            analog_json = json.loads(row["analog_json"])
            self.assertTrue(bool(analog_json.get("enabled")))
        finally:
            conn.close()

    def test_report_regime_policy_summary_counts_divergence(self) -> None:
        report = load_module(
            "pq_daily_report_regime_policy_summary_test",
            REPO_ROOT / "scripts" / "generate_daily_ml_report.py",
        )
        predictions = [
            {
                "event_id": "e1",
                "regime_policy_mode": "shadow",
                "trade_regime": "compression",
                "selected_policy": "baseline",
                "quality_flags": '["REGIME_POLICY_DIVERGENCE"]',
                "regime_policy_json": json.dumps(
                    {
                        "atr_zone": "ultra",
                        "atr_overlay": {"applied": True},
                        "signal_diffs": {
                            "signal_5m": {"baseline": "no_edge", "regime": "reject"},
                            "signal_15m": {"baseline": "reject", "regime": "reject"},
                        }
                    }
                ),
            },
            {
                "event_id": "e2",
                "regime_policy_mode": "shadow",
                "trade_regime": "expansion",
                "selected_policy": "baseline",
                "quality_flags": "[]",
                "regime_policy_json": json.dumps(
                    {
                        "atr_zone": "far",
                        "atr_overlay": {"applied": False},
                        "signal_diffs": {"signal_60m": {"baseline": "break", "regime": "break"}},
                    }
                ),
            },
            {
                "event_id": "e3",
                "regime_policy_mode": "active",
                "trade_regime": "expansion",
                "selected_policy": "regime_active",
                "quality_flags": "[]",
                "regime_policy_json": json.dumps(
                    {
                        "atr_zone": "near",
                        "atr_overlay": {"applied": True},
                        "signal_diffs": {"signal_15m": {"baseline": "reject", "regime": "break"}},
                    }
                ),
            },
        ]
        summary = report.compute_regime_policy_summary(predictions)
        self.assertEqual(int(summary["total_predictions"]), 3)
        self.assertEqual(int(summary["with_payload"]), 3)
        self.assertEqual(int(summary["mode_counts"]["shadow"]), 2)
        self.assertEqual(int(summary["mode_counts"]["active"]), 1)
        self.assertEqual(int(summary["trade_regime_counts"]["compression"]), 1)
        self.assertEqual(int(summary["trade_regime_counts"]["expansion"]), 2)
        self.assertEqual(int(summary["selected_policy_counts"]["baseline"]), 2)
        self.assertEqual(int(summary["selected_policy_counts"]["regime_active"]), 1)
        self.assertEqual(int(summary["atr_zone_counts"]["ultra"]), 1)
        self.assertEqual(int(summary["atr_zone_counts"]["near"]), 1)
        self.assertEqual(int(summary["atr_zone_counts"]["far"]), 1)
        self.assertEqual(int(summary["atr_overlay_applied_count"]), 2)
        self.assertEqual(int(summary["atr_overlay_applied_by_regime"]["compression"]), 1)
        self.assertEqual(int(summary["atr_overlay_applied_by_regime"]["expansion"]), 1)
        self.assertEqual(int(summary["divergence_count"]), 2)
        self.assertEqual(int(summary["divergence_by_horizon"][5]), 1)
        self.assertEqual(int(summary["divergence_by_horizon"][15]), 1)
        self.assertEqual(int(summary["divergence_by_atr_zone"]["ultra"]), 1)
        self.assertEqual(int(summary["divergence_by_atr_zone"]["near"]), 1)

    def test_report_analog_shadow_summary_and_deltas(self) -> None:
        report = load_module(
            "pq_daily_report_analog_summary_test",
            REPO_ROOT / "scripts" / "generate_daily_ml_report.py",
        )
        records = []
        rows = [
            # event_id, actual_reject, model_reject, analog_reject, actual_break, model_break, analog_break, disagreement
            ("a1", 1, 0.60, 0.90, 0, 0.40, 0.10, 0.30),
            ("a2", 1, 0.55, 0.85, 0, 0.45, 0.15, 0.30),
            ("a3", 0, 0.45, 0.10, 1, 0.55, 0.90, 0.35),
            ("a4", 0, 0.40, 0.20, 1, 0.60, 0.80, 0.20),
        ]
        for event_id, ar, mr, aar, ab, mb, aab, disagreement in rows:
            records.append(
                {
                    "event_id": event_id,
                    "horizon_min": 5,
                    "actual_reject": ar,
                    "actual_break": ab,
                    "prob_reject_5m": mr,
                    "prob_break_5m": mb,
                    "analog_json": json.dumps(
                        {
                            "horizons": {
                                "5": {
                                    "status": "ok",
                                    "reject_prob": aar,
                                    "break_prob": aab,
                                    "n": 20,
                                    "n_eff": 12,
                                    "reject_ci_width": 0.10,
                                    "break_ci_width": 0.12,
                                    "disagreement": disagreement,
                                }
                            }
                        }
                    ),
                }
            )

        summaries = report.compute_analog_shadow_summaries(records, [5])
        self.assertEqual(len(summaries), 1)
        s = summaries[0]
        self.assertEqual(int(s.horizon), 5)
        self.assertEqual(int(s.sample_size), 4)
        self.assertEqual(int(s.analog_available_count), 4)
        self.assertEqual(int(s.analog_quality_ok_count), 4)
        self.assertAlmostEqual(float(s.mean_effective_neighbors or 0.0), 12.0, places=6)
        self.assertAlmostEqual(float(s.mean_ci_width or 0.0), 0.12, places=6)
        self.assertEqual(int(s.high_disagreement_count), 3)
        self.assertIsNotNone(s.reject_brier_delta)
        self.assertIsNotNone(s.break_brier_delta)
        self.assertLess(float(s.reject_brier_delta or 0.0), 0.0)
        self.assertLess(float(s.break_brier_delta or 0.0), 0.0)
        self.assertAlmostEqual(float(s.guard_reject_keep_rate or 0.0), 0.25, places=6)
        self.assertAlmostEqual(float(s.guard_break_keep_rate or 0.0), 0.25, places=6)
        self.assertIsNotNone(s.guard_reject_brier_delta)
        self.assertIsNotNone(s.guard_break_brier_delta)

    def test_report_analog_shadow_uses_raw_model_probs_from_payload_when_present(self) -> None:
        report = load_module(
            "pq_daily_report_analog_model_baseline_test",
            REPO_ROOT / "scripts" / "generate_daily_ml_report.py",
        )
        records = []
        rows = [
            # event_id, actual_reject, raw_model_reject, row_prob_reject
            ("m1", 1, 0.9, 0.5),
            ("m2", 0, 0.1, 0.5),
        ]
        for event_id, ar, raw_model_r, row_model_r in rows:
            records.append(
                {
                    "event_id": event_id,
                    "horizon_min": 5,
                    "actual_reject": ar,
                    "actual_break": 1 - ar,
                    "prob_reject_5m": row_model_r,
                    "prob_break_5m": 1.0 - row_model_r,
                    "analog_json": json.dumps(
                        {
                            "horizons": {
                                "5": {
                                    "status": "ok",
                                    "model_reject": raw_model_r,
                                    "model_break": 1.0 - raw_model_r,
                                    "reject_prob": raw_model_r,
                                    "break_prob": 1.0 - raw_model_r,
                                    "n": 20,
                                    "n_eff": 12,
                                    "reject_ci_width": 0.10,
                                    "break_ci_width": 0.12,
                                    "disagreement": 0.05,
                                }
                            }
                        }
                    ),
                }
            )

        summaries = report.compute_analog_shadow_summaries(records, [5], eval_mode="blend")
        self.assertEqual(len(summaries), 1)
        s = summaries[0]
        self.assertIsNotNone(s.model_reject_brier_matched)
        # If row-level post-blend probs (0.5/0.5) were used this would be 0.25.
        self.assertLess(float(s.model_reject_brier_matched or 1.0), 0.05)

    def test_report_analog_shadow_blend_eval_respects_target_applied_flags(self) -> None:
        report = load_module(
            "pq_daily_report_analog_blend_applied_flags_test",
            REPO_ROOT / "scripts" / "generate_daily_ml_report.py",
        )
        records = []
        rows = [
            # event_id, actual_break, model_break, synthetic_blend_break
            ("b1", 0, 0.1, 0.9),
            ("b2", 1, 0.9, 0.1),
        ]
        for event_id, actual_break, model_break, synthetic_blend_break in rows:
            records.append(
                {
                    "event_id": event_id,
                    "horizon_min": 5,
                    "actual_reject": 1 - actual_break,
                    "actual_break": actual_break,
                    "prob_reject_5m": 1.0 - model_break,
                    "prob_break_5m": model_break,
                    "analog_json": json.dumps(
                        {
                            "horizons": {
                                "5": {
                                    "status": "ok",
                                    "model_reject": 1.0 - model_break,
                                    "model_break": model_break,
                                    "reject_prob": 1.0 - synthetic_blend_break,
                                    "break_prob": synthetic_blend_break,
                                    "blend_prob_reject": 1.0 - synthetic_blend_break,
                                    "blend_prob_break": synthetic_blend_break,
                                    "blend_applied_reject": True,
                                    "blend_applied_break": False,
                                    "n": 20,
                                    "n_eff": 12,
                                    "reject_ci_width": 0.10,
                                    "break_ci_width": 0.12,
                                    "disagreement": 0.05,
                                }
                            }
                        }
                    ),
                }
            )

        summaries = report.compute_analog_shadow_summaries(records, [5], eval_mode="blend")
        self.assertEqual(len(summaries), 1)
        s = summaries[0]
        # Break blend is blocked by blend_applied_break=False, so blend should match model baseline.
        self.assertIsNotNone(s.break_brier_delta_blend)
        self.assertIsNotNone(s.break_ece_delta_blend)
        self.assertAlmostEqual(float(s.break_brier_delta_blend), 0.0, places=9)
        self.assertAlmostEqual(float(s.break_ece_delta_blend), 0.0, places=9)

    def test_report_analog_promotion_gate_evaluates_thresholds(self) -> None:
        report = load_module(
            "pq_daily_report_analog_gate_test",
            REPO_ROOT / "scripts" / "generate_daily_ml_report.py",
        )
        summaries = [
            report.AnalogHorizonSummary(
                horizon=5,
                sample_size=200,
                analog_available_count=120,
                analog_quality_ok_count=110,
                mean_neighbors=20.0,
                mean_effective_neighbors=14.0,
                mean_ci_width=0.18,
                mean_disagreement=0.12,
                high_disagreement_count=10,
                high_disagreement_model_abs_error=0.33,
                low_disagreement_model_abs_error=0.18,
                model_reject_brier_matched=0.20,
                analog_reject_brier=0.18,
                reject_brier_delta=-0.02,
                model_reject_ece_matched=0.10,
                analog_reject_ece=0.08,
                reject_ece_delta=-0.02,
                model_break_brier_matched=0.22,
                analog_break_brier=0.20,
                break_brier_delta=-0.02,
                model_break_ece_matched=0.11,
                analog_break_ece=0.09,
                break_ece_delta=-0.02,
            ),
            report.AnalogHorizonSummary(
                horizon=15,
                sample_size=180,
                analog_available_count=100,
                analog_quality_ok_count=95,
                mean_neighbors=20.0,
                mean_effective_neighbors=12.0,
                mean_ci_width=0.20,
                mean_disagreement=0.11,
                high_disagreement_count=8,
                high_disagreement_model_abs_error=0.31,
                low_disagreement_model_abs_error=0.20,
                model_reject_brier_matched=0.21,
                analog_reject_brier=0.19,
                reject_brier_delta=-0.02,
                model_reject_ece_matched=0.11,
                analog_reject_ece=0.09,
                reject_ece_delta=-0.02,
                model_break_brier_matched=0.20,
                analog_break_brier=0.19,
                break_brier_delta=-0.01,
                model_break_ece_matched=0.10,
                analog_break_ece=0.08,
                break_ece_delta=-0.02,
            ),
        ]
        gate = report.compute_analog_promotion_gate(summaries, [5, 15, 30, 60])
        self.assertEqual(gate.status, "pass")
        self.assertEqual(gate.passed_horizons, [5, 15])
        self.assertEqual(gate.evaluated_horizons, [5, 15])
        self.assertEqual(gate.required_horizons, 2)
        self.assertIn("5", gate.horizon_results)
        self.assertTrue(bool(gate.horizon_results["5"].get("pass")))
        self.assertTrue(bool(gate.horizon_results["5"].get("reject_pass")))
        self.assertTrue(bool(gate.horizon_results["5"].get("break_pass")))
        self.assertIn("30", gate.horizon_results)
        self.assertFalse(bool(gate.horizon_results["30"].get("evaluated")))

        degraded = list(summaries)
        degraded[1] = report.AnalogHorizonSummary(
            horizon=15,
            sample_size=180,
            analog_available_count=100,
            analog_quality_ok_count=95,
            mean_neighbors=20.0,
            mean_effective_neighbors=12.0,
            mean_ci_width=0.20,
            mean_disagreement=0.11,
            high_disagreement_count=8,
            high_disagreement_model_abs_error=0.31,
            low_disagreement_model_abs_error=0.20,
            model_reject_brier_matched=0.21,
            analog_reject_brier=0.24,
            reject_brier_delta=0.03,
            model_reject_ece_matched=0.11,
            analog_reject_ece=0.13,
            reject_ece_delta=0.02,
            model_break_brier_matched=0.20,
            analog_break_brier=0.23,
            break_brier_delta=0.03,
            model_break_ece_matched=0.10,
            analog_break_ece=0.12,
            break_ece_delta=0.02,
        )
        gate_fail = report.compute_analog_promotion_gate(degraded, [5, 15, 30, 60])
        self.assertEqual(gate_fail.status, "fail")
        self.assertIn("insufficient_passed_horizons", gate_fail.reasons)
        self.assertIn("15", gate_fail.horizon_results)
        self.assertFalse(bool(gate_fail.horizon_results["15"].get("pass")))
        self.assertFalse(bool(gate_fail.horizon_results["15"].get("reject_pass")))
        self.assertFalse(bool(gate_fail.horizon_results["15"].get("break_pass")))

    def test_report_analog_promotion_gate_blend_mode_can_pass_when_analog_fails(self) -> None:
        report = load_module(
            "pq_daily_report_analog_gate_blend_mode_test",
            REPO_ROOT / "scripts" / "generate_daily_ml_report.py",
        )

        summaries = [
            report.AnalogHorizonSummary(
                horizon=5,
                sample_size=220,
                analog_available_count=120,
                analog_quality_ok_count=110,
                mean_neighbors=20.0,
                mean_effective_neighbors=14.0,
                mean_ci_width=0.18,
                mean_disagreement=0.10,
                high_disagreement_count=8,
                high_disagreement_model_abs_error=0.25,
                low_disagreement_model_abs_error=0.20,
                model_reject_brier_matched=0.20,
                analog_reject_brier=0.24,
                reject_brier_delta=0.04,
                model_reject_ece_matched=0.10,
                analog_reject_ece=0.13,
                reject_ece_delta=0.03,
                model_break_brier_matched=0.18,
                analog_break_brier=0.20,
                break_brier_delta=0.02,
                model_break_ece_matched=0.08,
                analog_break_ece=0.10,
                break_ece_delta=0.02,
                blend_reject_brier=0.19,
                blend_reject_ece=0.09,
                blend_break_brier=0.17,
                blend_break_ece=0.07,
                reject_brier_delta_blend=-0.01,
                reject_ece_delta_blend=-0.01,
                break_brier_delta_blend=-0.01,
                break_ece_delta_blend=-0.01,
            ),
            report.AnalogHorizonSummary(
                horizon=15,
                sample_size=210,
                analog_available_count=115,
                analog_quality_ok_count=108,
                mean_neighbors=20.0,
                mean_effective_neighbors=13.0,
                mean_ci_width=0.20,
                mean_disagreement=0.11,
                high_disagreement_count=9,
                high_disagreement_model_abs_error=0.24,
                low_disagreement_model_abs_error=0.21,
                model_reject_brier_matched=0.21,
                analog_reject_brier=0.23,
                reject_brier_delta=0.02,
                model_reject_ece_matched=0.11,
                analog_reject_ece=0.12,
                reject_ece_delta=0.01,
                model_break_brier_matched=0.19,
                analog_break_brier=0.21,
                break_brier_delta=0.02,
                model_break_ece_matched=0.09,
                analog_break_ece=0.10,
                break_ece_delta=0.01,
                blend_reject_brier=0.20,
                blend_reject_ece=0.10,
                blend_break_brier=0.18,
                blend_break_ece=0.08,
                reject_brier_delta_blend=-0.01,
                reject_ece_delta_blend=-0.01,
                break_brier_delta_blend=-0.01,
                break_ece_delta_blend=-0.01,
            ),
        ]

        gate_blend = report.compute_analog_promotion_gate(
            summaries,
            [5, 15, 30, 60],
            eval_mode="blend",
            lookback_days=5,
        )
        self.assertEqual(gate_blend.status, "pass")
        self.assertEqual(gate_blend.passed_horizons, [5, 15])
        self.assertEqual(gate_blend.thresholds.get("eval_mode"), "blend")
        self.assertEqual(int(gate_blend.thresholds.get("lookback_days") or 0), 5)
        self.assertTrue(bool(gate_blend.horizon_results["5"].get("pass")))
        self.assertTrue(bool(gate_blend.horizon_results["5"].get("reject_pass")))
        self.assertTrue(bool(gate_blend.horizon_results["5"].get("break_pass")))

        gate_analog = report.compute_analog_promotion_gate(
            summaries,
            [5, 15, 30, 60],
            eval_mode="analog",
            lookback_days=5,
        )
        self.assertEqual(gate_analog.status, "fail")
        self.assertIn("insufficient_passed_horizons", gate_analog.reasons)
        self.assertIn("5", gate_analog.horizon_results)
        self.assertFalse(bool(gate_analog.horizon_results["5"].get("pass")))

    def test_weekend_deep_audit_generates_markdown_with_core_sections(self) -> None:
        db = self.tmp / "weekend_audit.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                """
                CREATE TABLE touch_events(
                    event_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    ts_event INTEGER NOT NULL,
                    gamma_mode INTEGER,
                    gamma_flip REAL,
                    rv_regime INTEGER,
                    regime_type INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE event_labels(
                    event_id TEXT NOT NULL,
                    horizon_min INTEGER NOT NULL,
                    reject INTEGER,
                    break INTEGER,
                    return_bps REAL,
                    mfe_bps REAL,
                    mae_bps REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE prediction_log(
                    event_id TEXT NOT NULL,
                    ts_prediction INTEGER NOT NULL,
                    is_preview INTEGER NOT NULL DEFAULT 0,
                    regime_policy_mode TEXT,
                    trade_regime TEXT,
                    selected_policy TEXT,
                    regime_policy_json TEXT,
                    signal_5m TEXT,
                    signal_15m TEXT,
                    signal_30m TEXT,
                    signal_60m TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE gamma_snapshots(
                    symbol TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    ts_collected_ms INTEGER NOT NULL,
                    with_greeks INTEGER,
                    with_iv INTEGER,
                    with_oi INTEGER,
                    gamma_flip REAL,
                    oi_concentration_top5 REAL,
                    zero_dte_share REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE daily_ml_metrics(
                    report_date TEXT NOT NULL,
                    horizon_min INTEGER NOT NULL,
                    brier_reject REAL,
                    brier_break REAL,
                    ece_reject REAL,
                    ece_break REAL,
                    avg_return_bps REAL
                )
                """
            )

            ts_event = int(datetime(2026, 3, 6, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)
            ts_pred = ts_event + 30_000
            conn.execute(
                """
                INSERT INTO touch_events(event_id, symbol, ts_event, gamma_mode, gamma_flip, rv_regime, regime_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("evt_weekend_1", "SPY", ts_event, -1, 705.0, 2, 3),
            )
            conn.execute(
                """
                INSERT INTO event_labels(event_id, horizon_min, reject, break, return_bps, mfe_bps, mae_bps)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("evt_weekend_1", 5, 1, 0, 12.0, 18.0, -6.0),
            )
            conn.execute(
                """
                INSERT INTO prediction_log(
                    event_id, ts_prediction, is_preview, regime_policy_mode, trade_regime,
                    selected_policy, regime_policy_json, signal_5m, signal_15m, signal_30m, signal_60m
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "evt_weekend_1",
                    ts_pred,
                    0,
                    "shadow",
                    "compression",
                    "baseline",
                    json.dumps(
                        {
                            "atr_zone": "near",
                            "signal_diffs": {
                                "signal_5m": {
                                    "baseline": "break",
                                    "regime": "reject",
                                }
                            },
                        }
                    ),
                    "reject",
                    "no_edge",
                    "no_edge",
                    "no_edge",
                ),
            )
            conn.execute(
                """
                INSERT INTO gamma_snapshots(
                    symbol, snapshot_date, ts_collected_ms, with_greeks, with_iv, with_oi,
                    gamma_flip, oi_concentration_top5, zero_dte_share
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("SPY", "2026-03-05", 1_777_800_000_000, 7966, 7966, 7966, 705.0, 16.05, 0.0),
            )
            conn.execute(
                """
                INSERT INTO gamma_snapshots(
                    symbol, snapshot_date, ts_collected_ms, with_greeks, with_iv, with_oi,
                    gamma_flip, oi_concentration_top5, zero_dte_share
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("SPY", "2026-03-06", 1_777_890_000_000, 0, 0, 508, None, 17.95, 0.0),
            )
            conn.execute(
                """
                INSERT INTO daily_ml_metrics(
                    report_date, horizon_min, brier_reject, brier_break, ece_reject, ece_break, avg_return_bps
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("2026-03-06", 5, 0.21, 0.09, 0.11, 0.07, 5.2),
            )
            conn.commit()
        finally:
            conn.close()

        out_md = self.tmp / "weekend_deep_audit.md"
        proc = run_cmd(
            [
                PYTHON,
                "scripts/weekend_deep_audit.py",
                "--db",
                str(db),
                "--symbol",
                "SPY",
                "--start-date",
                "2026-03-06",
                "--end-date",
                "2026-03-06",
                "--output",
                str(out_md),
            ],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")
        text = out_md.read_text(encoding="utf-8")
        self.assertIn("## Gamma Freshness & Carry", text)
        self.assertIn("carry_prev_day", text)
        self.assertIn("## Regime Policy Attribution", text)
        self.assertIn("## Calibration Stability (daily_ml_metrics)", text)
        self.assertIn("Horizon 5m divergences: 1", text)

    def test_weekly_policy_review_generates_markdown_with_core_sections(self) -> None:
        db = self.tmp / "weekly_policy_review.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                """
                CREATE TABLE touch_events(
                    event_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    ts_event INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE event_labels(
                    event_id TEXT NOT NULL,
                    horizon_min INTEGER NOT NULL,
                    return_bps REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE prediction_log(
                    event_id TEXT NOT NULL,
                    ts_prediction INTEGER NOT NULL,
                    is_preview INTEGER NOT NULL DEFAULT 0,
                    best_horizon INTEGER,
                    abstain INTEGER NOT NULL DEFAULT 0,
                    trade_regime TEXT,
                    regime_policy_json TEXT,
                    signal_5m TEXT,
                    signal_15m TEXT,
                    signal_30m TEXT,
                    signal_60m TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE daily_ml_metrics(
                    report_date TEXT NOT NULL,
                    horizon_min INTEGER NOT NULL,
                    sample_size INTEGER,
                    brier_reject REAL,
                    brier_break REAL,
                    ece_reject REAL,
                    ece_break REAL,
                    auc_reject REAL,
                    auc_break REAL
                )
                """
            )

            ts_event_1 = int(datetime(2026, 3, 6, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)
            ts_event_2 = int(datetime(2026, 3, 7, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)
            ts_pred_1 = ts_event_1 + 60_000
            ts_pred_2 = ts_event_2 + 60_000

            conn.execute(
                "INSERT INTO touch_events(event_id, symbol, ts_event) VALUES (?, ?, ?)",
                ("evt_weekly_1", "SPY", ts_event_1),
            )
            conn.execute(
                "INSERT INTO touch_events(event_id, symbol, ts_event) VALUES (?, ?, ?)",
                ("evt_weekly_2", "SPY", ts_event_2),
            )
            conn.execute(
                "INSERT INTO event_labels(event_id, horizon_min, return_bps) VALUES (?, ?, ?)",
                ("evt_weekly_1", 5, 8.0),
            )
            conn.execute(
                "INSERT INTO event_labels(event_id, horizon_min, return_bps) VALUES (?, ?, ?)",
                ("evt_weekly_2", 60, -4.0),
            )
            conn.execute(
                """
                INSERT INTO prediction_log(
                    event_id, ts_prediction, is_preview, best_horizon, abstain,
                    trade_regime, regime_policy_json, signal_5m, signal_15m, signal_30m, signal_60m
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "evt_weekly_1",
                    ts_pred_1,
                    0,
                    5,
                    0,
                    "expansion",
                    json.dumps({"atr_zone": "near", "guardrail": {"triggered": True, "applied": True, "mode": "active", "strategy": "no_trade"}}),
                    "reject",
                    "no_edge",
                    "no_edge",
                    "no_edge",
                ),
            )
            conn.execute(
                """
                INSERT INTO prediction_log(
                    event_id, ts_prediction, is_preview, best_horizon, abstain,
                    trade_regime, regime_policy_json, signal_5m, signal_15m, signal_30m, signal_60m
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "evt_weekly_2",
                    ts_pred_2,
                    0,
                    60,
                    0,
                    "compression",
                    json.dumps({"atr_zone": "ultra", "guardrail": {"triggered": False, "applied": False, "mode": "active", "strategy": "no_trade"}}),
                    "no_edge",
                    "no_edge",
                    "no_edge",
                    "break",
                ),
            )
            conn.execute(
                """
                INSERT INTO daily_ml_metrics(
                    report_date, horizon_min, sample_size, brier_reject, brier_break,
                    ece_reject, ece_break, auc_reject, auc_break
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("2026-03-06", 5, 0, 0.22, 0.11, 0.13, 0.08, 0.60, 0.56),
            )
            conn.execute(
                """
                INSERT INTO daily_ml_metrics(
                    report_date, horizon_min, sample_size, brier_reject, brier_break,
                    ece_reject, ece_break, auc_reject, auc_break
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("2026-03-07", 5, 20, 0.24, 0.12, 0.15, 0.09, 0.61, 0.57),
            )
            conn.execute(
                """
                INSERT INTO daily_ml_metrics(
                    report_date, horizon_min, sample_size, brier_reject, brier_break,
                    ece_reject, ece_break, auc_reject, auc_break
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("2026-03-07", 60, 120, 0.18, 0.14, 0.08, 0.07, 0.69, 0.66),
            )
            conn.commit()
        finally:
            conn.close()

        out_md = self.tmp / "weekly_policy_review.md"
        proc = run_cmd(
            [
                PYTHON,
                "scripts/weekly_policy_review.py",
                "--db",
                str(db),
                "--symbol",
                "SPY",
                "--source",
                "live",
                "--start-date",
                "2026-03-06",
                "--end-date",
                "2026-03-08",
                "--output",
                str(out_md),
            ],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")
        text = out_md.read_text(encoding="utf-8")
        self.assertIn("- Policy Change Gate: ALLOW POLICY CHANGES (coverage SLA PASS)", text)
        self.assertIn("## Prediction Coverage SLA", text)
        self.assertIn("- Timely prediction lag filter: <= 6.00 hours", text)
        self.assertIn("- Overall coverage: 100.00% (2/2)", text)
        self.assertIn("- Coverage status: PASS", text)
        self.assertIn("## Policy Comparison (Baseline vs Guardrail vs No-5m)", text)
        self.assertIn("## Cost Sweep", text)
        self.assertIn("## Stratified PnL (Regime x ATR Zone x Horizon)", text)
        self.assertIn("## Daily Expectancy", text)
        self.assertIn("## Trade Share by Horizon (Baseline Trades)", text)
        self.assertIn("## Calibration Health (AUC/Brier/ECE)", text)
        self.assertIn("- Daily rows in window: 3", text)
        self.assertIn("- Rows with `sample_size > 0`: 2", text)
        self.assertIn("| 5m | 1 | 20 | 20.0 |", text)
        self.assertIn("| 60m | 1 | 120 | 120.0 |", text)
        self.assertIn("LOW_SUPPORT", text)
        self.assertIn("- Low-support horizons: 5m(total=20)", text)

        out_md_fail = self.tmp / "weekly_policy_review_fail.md"
        proc_fail = run_cmd(
            [
                PYTHON,
                "scripts/weekly_policy_review.py",
                "--db",
                str(db),
                "--symbol",
                "SPY",
                "--source",
                "preview",
                "--start-date",
                "2026-03-06",
                "--end-date",
                "2026-03-08",
                "--output",
                str(out_md_fail),
            ],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc_fail.returncode, 0, msg=f"{proc_fail.stdout}\n{proc_fail.stderr}")
        text_fail = out_md_fail.read_text(encoding="utf-8")
        self.assertIn("- Policy Change Gate: BLOCK POLICY CHANGES (coverage SLA FAIL)", text_fail)
        self.assertIn("- Coverage status: FAIL", text_fail)

    def test_weekly_policy_review_excludes_late_predictions_by_default_lag_filter(self) -> None:
        db = self.tmp / "weekly_policy_review_lag.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                """
                CREATE TABLE touch_events(
                    event_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    ts_event INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE event_labels(
                    event_id TEXT NOT NULL,
                    horizon_min INTEGER NOT NULL,
                    return_bps REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE prediction_log(
                    event_id TEXT NOT NULL,
                    ts_prediction INTEGER NOT NULL,
                    is_preview INTEGER NOT NULL DEFAULT 0,
                    best_horizon INTEGER,
                    abstain INTEGER NOT NULL DEFAULT 0,
                    trade_regime TEXT,
                    regime_policy_json TEXT,
                    signal_5m TEXT,
                    signal_15m TEXT,
                    signal_30m TEXT,
                    signal_60m TEXT
                )
                """
            )

            ts_event = int(datetime(2026, 3, 6, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)
            ts_pred_late = ts_event + (8 * 3600 * 1000)  # 8h lag, exceeds default 6h filter.

            conn.execute(
                "INSERT INTO touch_events(event_id, symbol, ts_event) VALUES (?, ?, ?)",
                ("evt_weekly_lag_1", "SPY", ts_event),
            )
            conn.execute(
                "INSERT INTO event_labels(event_id, horizon_min, return_bps) VALUES (?, ?, ?)",
                ("evt_weekly_lag_1", 15, 4.0),
            )
            conn.execute(
                """
                INSERT INTO prediction_log(
                    event_id, ts_prediction, is_preview, best_horizon, abstain,
                    trade_regime, regime_policy_json, signal_5m, signal_15m, signal_30m, signal_60m
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "evt_weekly_lag_1",
                    ts_pred_late,
                    0,
                    15,
                    0,
                    "compression",
                    json.dumps({"atr_zone": "near"}),
                    "no_edge",
                    "reject",
                    "no_edge",
                    "no_edge",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        out_md = self.tmp / "weekly_policy_review_lag.md"
        proc = run_cmd(
            [
                PYTHON,
                "scripts/weekly_policy_review.py",
                "--db",
                str(db),
                "--symbol",
                "SPY",
                "--source",
                "live",
                "--start-date",
                "2026-03-06",
                "--end-date",
                "2026-03-06",
                "--output",
                str(out_md),
            ],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")
        text = out_md.read_text(encoding="utf-8")
        self.assertIn("- Timely prediction lag filter: <= 6.00 hours", text)
        self.assertIn("- Overall coverage: 0.00% (0/1)", text)
        self.assertIn("- Coverage status: FAIL", text)
        self.assertIn("- Policy Change Gate: BLOCK POLICY CHANGES (coverage SLA FAIL)", text)
        self.assertIn("- Events (latest prediction per event): 0", text)

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
