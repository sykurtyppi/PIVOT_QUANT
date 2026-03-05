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
import unittest
from datetime import date, datetime, timedelta, timezone
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

    def test_30m_shadow_horizon_contract_present(self) -> None:
        ml_server = (REPO_ROOT / "server" / "ml_server.py").read_text(encoding="utf-8")
        self.assertIn("ML_SHADOW_HORIZONS", ml_server)
        self.assertIn("signal_30m", ml_server)
        self.assertIn("prob_reject_30m", ml_server)
        self.assertIn("threshold_break_30m", ml_server)

        dashboard = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        self.assertIn('id="ml-signal-30m"', dashboard)
        self.assertIn('id="ml-reject-30m"', dashboard)
        self.assertIn('id="ml-break-30m"', dashboard)
        self.assertIn("const horizons = [5, 15, 30, 60];", dashboard)

        migrate_db = (REPO_ROOT / "scripts" / "migrate_db.py").read_text(encoding="utf-8")
        match = re.search(r"LATEST_SCHEMA_VERSION\s*=\s*(\d+)", migrate_db)
        self.assertIsNotNone(match, msg="migrate_db.py must declare LATEST_SCHEMA_VERSION")
        self.assertGreaterEqual(int(match.group(1)), 6)
        self.assertIn("migration_5_prediction_log_shadow_30m", migrate_db)
        self.assertIn("migration_6_gamma_snapshots", migrate_db)
        self.assertIn("gamma_snapshots", migrate_db)
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
