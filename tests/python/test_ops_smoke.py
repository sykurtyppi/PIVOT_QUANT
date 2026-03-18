#!/usr/bin/env python3
"""Lightweight smoke tests for ops resilience scripts."""

from __future__ import annotations

import json
import importlib.util
import asyncio
import os
import re
import shutil
import sqlite3
import socket
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError
import urllib.request

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

    def _find_free_tcp_port(self) -> int:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.bind(("127.0.0.1", 0))
                return int(sock.getsockname()[1])
        except PermissionError as exc:
            self.skipTest(f"socket bind not permitted in this environment: {exc}")

    def _read_json_url(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout_sec: float = 2.0,
    ) -> tuple[int, dict]:
        req = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:  # noqa: S310
            payload = json.loads(resp.read().decode("utf-8"))
            return int(resp.status), payload

    async def _asgi_json_request_async(
        self,
        app,
        method: str,
        path: str,
        *,
        payload: dict | list | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict]:
        body = b""
        scope_headers: list[tuple[bytes, bytes]] = [(b"host", b"testserver")]
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            scope_headers.append((b"content-type", b"application/json"))
            scope_headers.append((b"content-length", str(len(body)).encode("ascii")))
        if headers:
            for key, value in headers.items():
                scope_headers.append((key.strip().lower().encode("ascii"), str(value).encode("utf-8")))

        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": method.upper(),
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("ascii"),
            "query_string": b"",
            "headers": scope_headers,
            "client": ("127.0.0.1", 54321),
            "server": ("testserver", 80),
        }

        request_sent = False
        response_status = 500
        response_body = b""

        async def receive():
            nonlocal request_sent
            if not request_sent:
                request_sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return {"type": "http.disconnect"}

        async def send(message):
            nonlocal response_status, response_body
            if message["type"] == "http.response.start":
                response_status = int(message["status"])
            elif message["type"] == "http.response.body":
                response_body += message.get("body", b"")

        await app(scope, receive, send)
        decoded = json.loads(response_body.decode("utf-8") or "{}")
        return response_status, decoded

    def _asgi_json_request(
        self,
        app,
        method: str,
        path: str,
        *,
        payload: dict | list | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict]:
        return asyncio.run(
            self._asgi_json_request_async(
                app,
                method,
                path,
                payload=payload,
                headers=headers,
            )
        )

    def _start_dashboard_proxy(self, port: int) -> subprocess.Popen[str]:
        env = os.environ.copy()
        env.update(
            {
                "HOST": "127.0.0.1",
                "PORT": str(port),
                "DASH_AUTH_PASSWORD": "smoke_test_password_1234567890",
            }
        )
        return subprocess.Popen(
            ["node", "server/yahoo_proxy.js"],
            cwd=str(REPO_ROOT),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

    def _wait_for_dashboard_proxy_health(
        self,
        port: int,
        proc: subprocess.Popen[str],
        *,
        timeout_sec: float = 20.0,
    ) -> dict:
        deadline = time.time() + timeout_sec
        last_error: Exception | None = None
        while time.time() < deadline:
            if proc.poll() is not None:
                output = ""
                if proc.stdout is not None:
                    output = proc.stdout.read() or ""
                self.fail(
                    f"dashboard proxy exited early with code {proc.returncode}: {output[-1200:]}"
                )
            try:
                status, payload = self._read_json_url(
                    f"http://127.0.0.1:{port}/health", timeout_sec=1.5
                )
                if status == 200 and payload.get("status") == "ok":
                    return payload
            except Exception as exc:  # pragma: no cover - startup race
                last_error = exc
            time.sleep(0.1)
        self.fail(f"dashboard proxy health did not become ready in {timeout_sec}s: {last_error}")

    def _stop_process(self, proc: subprocess.Popen[str]) -> None:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        if proc.stdout is not None:
            try:
                proc.stdout.close()
            except Exception:
                pass

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

    def _load_ml_server_module(self):
        return load_module(
            f"pq_ml_server_runtime_{time.time_ns()}",
            REPO_ROOT / "server" / "ml_server.py",
        )

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

    def test_generate_daily_report_default_date_uses_latest_completed_market_day(self) -> None:
        daily_report = load_module(
            "pq_generate_daily_report_default_date_test",
            REPO_ROOT / "scripts" / "generate_daily_ml_report.py",
        )

        class _BeforeCloseDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 16, 15, 59, tzinfo=daily_report.ET_TZ)
                return base if tz else base.replace(tzinfo=None)

        class _AfterCloseDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime(2026, 3, 16, 16, 1, tzinfo=daily_report.ET_TZ)
                return base if tz else base.replace(tzinfo=None)

        with patch.object(daily_report, "datetime", _BeforeCloseDateTime):
            # Before close, Monday should resolve to previous completed session (Friday).
            self.assertEqual(daily_report.parse_report_date(None), date(2026, 3, 13))

        with patch.object(daily_report, "datetime", _AfterCloseDateTime):
            # After close, same-day report should be selected.
            self.assertEqual(daily_report.parse_report_date(None), date(2026, 3, 16))

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

    def test_daily_report_unscored_uses_distinct_event_ids(self) -> None:
        db = self.tmp / "daily_report_counts.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                """
                CREATE TABLE touch_events(
                    event_id TEXT,
                    ts_event INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE prediction_log(
                    event_id TEXT,
                    ts_prediction INTEGER,
                    is_preview INTEGER
                )
                """
            )

            send_daily_report = load_module(
                "pq_send_daily_report_distinct_counts_test",
                REPO_ROOT / "scripts" / "send_daily_report.py",
            )
            report_day = date(2026, 3, 11)
            start_ms, _ = send_daily_report.et_day_bounds_ms(report_day)

            conn.executemany(
                "INSERT INTO touch_events(event_id, ts_event) VALUES (?, ?)",
                [
                    ("evt_a", start_ms + 10_000),
                    ("evt_b", start_ms + 20_000),
                    ("evt_c", start_ms + 30_000),
                ],
            )
            conn.executemany(
                "INSERT INTO prediction_log(event_id, ts_prediction, is_preview) VALUES (?, ?, ?)",
                [
                    ("evt_a", start_ms + 40_000, 0),
                    ("evt_a", start_ms + 50_000, 0),  # duplicate live predictions for same event
                    ("evt_b", start_ms + 60_000, 1),  # preview should not count as scored live event
                ],
            )
            conn.commit()
        finally:
            conn.close()

        stats = send_daily_report.fetch_db_progress(str(db), report_day)
        self.assertEqual(stats.get("events_today"), 3)
        self.assertEqual(stats.get("predictions_today"), 3)
        self.assertEqual(stats.get("predictions_live_today"), 2)
        self.assertEqual(stats.get("eligible_events_today"), 3)
        self.assertEqual(stats.get("scored_events_live_today"), 1)
        self.assertEqual(stats.get("unscored_eligible_today"), 2)

    def test_daily_report_timeout_count_ignores_non_failure_timeout_fields(self) -> None:
        send_daily_report = load_module(
            "pq_send_daily_report_timeout_count_test",
            REPO_ROOT / "scripts" / "send_daily_report.py",
        )

        report_day = date(2026, 3, 11)
        logs_dir = self.tmp / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        collector_log = logs_dir / "live_collector.log"
        retrain_log = logs_dir / "retrain.log"
        ml_log = logs_dir / "ml_server.log"

        collector_log.write_text(
            "\n".join(
                [
                    "2026-03-11 10:00:00 [WARNING] Collector scoring failed for SPY: ML score request failed: <urlopen error timed out>",
                    "2026-03-11 10:00:30 [INFO] score config timeout_sec=20",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        retrain_log.write_text(
            "\n".join(
                [
                    "[2026-03-11 10:01:00] [score_unscored] start eligible_total=2 timeout_sec=12.000",
                    "[2026-03-11 10:01:05] WARN score request failed: <urlopen error timed out>",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        ml_log.write_text('{"ready_timeout_sec": 45, "timeout_sec": 20}\n', encoding="utf-8")

        log_tails = {
            str(collector_log): collector_log.read_text(encoding="utf-8"),
            str(retrain_log): retrain_log.read_text(encoding="utf-8"),
            str(ml_log): ml_log.read_text(encoding="utf-8"),
        }
        self.assertEqual(send_daily_report.count_score_timeouts_in_tail(log_tails), 2)

        day_stats = send_daily_report.count_score_failures_by_day(
            [collector_log, retrain_log, ml_log],
            report_day,
        )
        self.assertEqual(day_stats.get("failures"), 2)
        self.assertEqual(day_stats.get("timeouts"), 2)

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

    def test_build_labels_forward_window_excludes_touch_bar(self) -> None:
        build_labels = load_module(
            "pq_build_labels_forward_window_test",
            REPO_ROOT / "scripts" / "build_labels.py",
        )
        bars = [
            {"ts": 1_000, "high": 101.0, "low": 99.0, "close": 100.0},
            {"ts": 2_000, "high": 102.0, "low": 99.5, "close": 100.5},
        ]
        filtered = build_labels.forward_bars_after_touch(bars, 1_000)
        self.assertEqual([int(bar["ts"]) for bar in filtered], [2_000])

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

    def test_build_feature_row_keeps_zero_touch_price_distances(self) -> None:
        features = load_module(
            "pq_features_zero_touch_price_test",
            REPO_ROOT / "ml" / "features.py",
        )
        ts_event = int(datetime.now(timezone.utc).timestamp() * 1000)
        row = features.build_feature_row(
            {
                "symbol": "SPY",
                "ts_event": ts_event,
                "level_type": "R1",
                "level_price": 100.0,
                "touch_price": 0.0,
                "distance_bps": 0.0,
                "vwap": 100.0,
                "gamma_flip": 80.0,
                "vpoc": 50.0,
                "weekly_pivot": 120.0,
                "monthly_pivot": 90.0,
            }
        )
        self.assertAlmostEqual(float(row["vwap_dist_bps_calc"]), -10_000.0, places=6)
        self.assertAlmostEqual(float(row["gamma_flip_dist_bps_calc"]), -10_000.0, places=6)
        self.assertAlmostEqual(float(row["vpoc_dist_bps_calc"]), -10_000.0, places=6)
        self.assertAlmostEqual(float(row["weekly_pivot_dist_bps"]), -10_000.0, places=6)
        self.assertAlmostEqual(float(row["monthly_pivot_dist_bps"]), -10_000.0, places=6)

    def test_build_feature_row_time_features_and_tod_buckets(self) -> None:
        features = load_module(
            "pq_features_time_bucket_test",
            REPO_ROOT / "ml" / "features.py",
        )
        dt_open = datetime(2026, 3, 10, 9, 45, tzinfo=features.NY_TZ)
        ts_open = int(dt_open.astimezone(timezone.utc).timestamp() * 1000)
        open_row = features.build_feature_row(
            {
                "symbol": "SPY",
                "ts_event": ts_open,
                "level_type": "R1",
                "level_price": 100.0,
                "touch_price": 100.0,
                "distance_bps": 0.0,
            }
        )
        self.assertEqual(int(open_row["event_hour_et"]), 9)
        self.assertEqual(open_row["tod_bucket"], "open")
        self.assertEqual(int(open_row["minutes_since_open"]), 15)
        self.assertEqual(int(open_row["minutes_until_close"]), 375)
        self.assertEqual(int(open_row["is_first_30min"]), 1)
        self.assertEqual(int(open_row["is_last_30min"]), 0)
        self.assertEqual(int(open_row["is_lunch_hour"]), 0)

        dt_power = datetime(2026, 3, 10, 15, 45, tzinfo=features.NY_TZ)
        ts_power = int(dt_power.astimezone(timezone.utc).timestamp() * 1000)
        power_row = features.build_feature_row(
            {
                "symbol": "SPY",
                "ts_event": ts_power,
                "level_type": "S1",
                "level_price": 100.0,
                "touch_price": 100.0,
                "distance_bps": 0.0,
            }
        )
        self.assertEqual(power_row["tod_bucket"], "power")
        self.assertEqual(int(power_row["is_first_30min"]), 0)
        self.assertEqual(int(power_row["is_last_30min"]), 1)

    def test_build_feature_row_ema_vwap_and_atr_derivations(self) -> None:
        features = load_module(
            "pq_features_derivations_test",
            REPO_ROOT / "ml" / "features.py",
        )
        ts_event = int(datetime(2026, 3, 10, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)
        row = features.build_feature_row(
            {
                "symbol": "SPY",
                "ts_event": ts_event,
                "level_type": "R2",
                "level_price": 100.0,
                "touch_price": 101.0,
                "distance_bps": 50.0,
                "ema9": 102.0,
                "ema21": 100.0,
                "vwap": 100.0,
                "session_std": 0.5,
                "atr": 2.0,
            }
        )
        self.assertEqual(int(row["ema_state_calc"]), 1)
        self.assertAlmostEqual(float(row["ema_spread_bps"]), 200.0, places=6)
        self.assertAlmostEqual(float(row["price_vs_ema21_bps"]), 100.0, places=6)
        self.assertAlmostEqual(float(row["vwap_dist_bps_calc"]), 100.0, places=6)
        self.assertAlmostEqual(float(row["vwap_zscore"]), 2.0, places=6)
        self.assertAlmostEqual(float(row["atr_bps"]), 198.01980198019803, places=6)
        self.assertAlmostEqual(float(row["distance_atr_ratio"]), 0.2525, places=6)

    def test_build_feature_row_prefers_explicit_distance_fields(self) -> None:
        features = load_module(
            "pq_features_explicit_distances_test",
            REPO_ROOT / "ml" / "features.py",
        )
        ts_event = int(datetime(2026, 3, 10, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)
        row = features.build_feature_row(
            {
                "symbol": "SPY",
                "ts_event": ts_event,
                "level_type": "GAMMA",
                "level_price": 100.0,
                "touch_price": 101.0,
                "distance_bps": 25.0,
                "vwap": 1.0,
                "gamma_flip": 1.0,
                "vpoc": 1.0,
                "vwap_dist_bps": 0.0,
                "gamma_flip_dist_bps": -12.5,
                "vpoc_dist_bps": 34.5,
            }
        )
        self.assertAlmostEqual(float(row["vwap_dist_bps_calc"]), 0.0, places=6)
        self.assertAlmostEqual(float(row["gamma_flip_dist_bps_calc"]), -12.5, places=6)
        self.assertAlmostEqual(float(row["vpoc_dist_bps_calc"]), 34.5, places=6)
        self.assertEqual(row["level_family"], "gamma")

    def test_build_feature_row_confluence_and_missing_keys(self) -> None:
        features = load_module(
            "pq_features_confluence_missing_test",
            REPO_ROOT / "ml" / "features.py",
        )
        ts_event = int(datetime(2026, 3, 10, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)
        row = features.build_feature_row(
            {
                "symbol": "SPY",
                "ts_event": ts_event,
                "level_type": "S2",
                "level_price": 100.0,
                "touch_price": 100.0,
                "distance_bps": 0.0,
                "mtf_confluence_types": '["weekly_pp", "monthly_pp"]',
            }
        )
        self.assertEqual(int(row["has_weekly_confluence"]), 1)
        self.assertEqual(int(row["has_monthly_confluence"]), 1)
        self.assertEqual(row["level_family"], "support")

        invalid_row = features.build_feature_row(
            {
                "symbol": "SPY",
                "ts_event": ts_event,
                "level_type": "P",
                "level_price": 100.0,
                "touch_price": 100.0,
                "distance_bps": 0.0,
                "mtf_confluence_types": "{bad json",
            }
        )
        self.assertEqual(int(invalid_row["has_weekly_confluence"]), 0)
        self.assertEqual(int(invalid_row["has_monthly_confluence"]), 0)
        self.assertEqual(invalid_row["level_family"], "pivot")

        missing = features.collect_missing({"symbol": "SPY"})
        self.assertEqual(
            missing,
            ["ts_event", "level_type", "level_price", "touch_price", "distance_bps"],
        )

    def test_drop_features_returns_copy(self) -> None:
        features = load_module(
            "pq_features_drop_copy_test",
            REPO_ROOT / "ml" / "features.py",
        )
        dropped_one = features.drop_features()
        dropped_one.add("__tmp_marker__")
        dropped_two = features.drop_features()
        self.assertIn("touch_price", dropped_two)
        self.assertNotIn("__tmp_marker__", dropped_two)

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

    def test_ml_score_payload_rejects_oversized_body_and_bad_content_length(self) -> None:
        prior_limit = os.environ.get("ML_SCORE_MAX_BODY_BYTES")
        os.environ["ML_SCORE_MAX_BODY_BYTES"] = "2048"
        try:
            ml_server = load_module(
                "pq_ml_server_body_guard_test",
                REPO_ROOT / "server" / "ml_server.py",
            )
        finally:
            if prior_limit is None:
                os.environ.pop("ML_SCORE_MAX_BODY_BYTES", None)
            else:
                os.environ["ML_SCORE_MAX_BODY_BYTES"] = prior_limit

        with self.assertRaises(Exception) as ctx:
            ml_server._enforce_score_body_size(4096, 0)
        err = ctx.exception
        self.assertEqual(getattr(err, "status_code", None), 413)
        self.assertIn("Max allowed: 2048", getattr(err, "detail", ""))

        with self.assertRaises(Exception) as ctx:
            ml_server._enforce_score_body_size(None, 4096)
        err = ctx.exception
        self.assertEqual(getattr(err, "status_code", None), 413)
        self.assertIn("Max allowed: 2048", getattr(err, "detail", ""))

        with self.assertRaises(Exception) as ctx:
            ml_server._parse_content_length_header("abc")
        self.assertEqual(getattr(ctx.exception, "status_code", None), 400)

        with self.assertRaises(Exception) as ctx:
            ml_server._parse_score_json_body(b"\x80")
        self.assertEqual(getattr(ctx.exception, "status_code", None), 400)

    def test_reconcile_predictions_paths_resolve_from_repo_root(self) -> None:
        reconcile_predictions = load_module(
            "pq_reconcile_paths_test",
            REPO_ROOT / "scripts" / "reconcile_predictions.py",
        )
        rel = reconcile_predictions.resolve_repo_path("data/pivot_events.sqlite")
        self.assertEqual(rel, REPO_ROOT / "data" / "pivot_events.sqlite")

        abs_path = Path("/tmp/pq_reconcile_abs.sqlite")
        self.assertEqual(reconcile_predictions.resolve_repo_path(str(abs_path)), abs_path)

    def test_audit_gamma_quality_touch_window_scopes_ts_event_date(self) -> None:
        db = self.tmp / "gamma_audit.sqlite"
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                """
                CREATE TABLE gamma_snapshots(
                    symbol TEXT,
                    snapshot_date TEXT,
                    gamma_flip REAL,
                    with_greeks INTEGER,
                    with_iv INTEGER,
                    payload_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE touch_events(
                    symbol TEXT,
                    ts_event INTEGER,
                    gamma_flip REAL,
                    gamma_confidence INTEGER
                )
                """
            )
            conn.execute(
                """
                INSERT INTO gamma_snapshots(symbol, snapshot_date, gamma_flip, with_greeks, with_iv, payload_json)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                ("SPY", "2026-03-10", 550.0, 1, 1, '{"computed_gamma_count": 10}'),
            )

            in_window_ms = int(datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
            out_window_ms = int(datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
            conn.execute(
                "INSERT INTO touch_events(symbol, ts_event, gamma_flip, gamma_confidence) VALUES(?, ?, ?, ?)",
                ("SPY", in_window_ms, 555.0, 1),
            )
            conn.execute(
                "INSERT INTO touch_events(symbol, ts_event, gamma_flip, gamma_confidence) VALUES(?, ?, ?, ?)",
                ("SPY", out_window_ms, 560.0, 1),
            )
            conn.commit()
        finally:
            conn.close()

        proc = run_cmd(
            [
                PYTHON,
                "scripts/audit_gamma_quality.py",
                "--db",
                str(db),
                "--symbol",
                "SPY",
                "--start-date",
                "2026-03-01",
                "--end-date",
                "2026-03-31",
            ],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")
        payload = json.loads(proc.stdout)
        touch = payload.get("touch_events") or {}
        self.assertEqual(int(touch.get("touch_rows") or 0), 1)
        self.assertEqual(int(touch.get("touch_gamma_nonnull") or 0), 1)

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
            "server/ml_server.py",
        ):
            src = (REPO_ROOT / rel).read_text(encoding="utf-8")
            self.assertIn("ML_CORS_ORIGINS", src)
            self.assertNotIn('Access-Control-Allow-Origin", "*"', src)
            self.assertNotIn('or ["*"]', src)

    def test_event_writer_daily_candles_use_ny_rth_window(self) -> None:
        event_writer = load_module(
            "pq_event_writer_daily_rth_test",
            REPO_ROOT / "server" / "event_writer.py",
        )
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE bar_data(
                    symbol TEXT NOT NULL,
                    ts INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL,
                    bar_interval_sec INTEGER
                )
                """
            )

            now_et = datetime.now(event_writer.NY_TZ)
            open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            pre_open_et = open_et - timedelta(minutes=30)
            before_close_et = open_et.replace(hour=15, minute=59)
            after_close_et = open_et.replace(hour=16, minute=30)

            rows = [
                ("SPY", int(pre_open_et.astimezone(timezone.utc).timestamp() * 1000), 90.0, 95.0, 89.0, 90.0, 40.0, 60),
                ("SPY", int(open_et.astimezone(timezone.utc).timestamp() * 1000), 100.0, 101.0, 99.0, 100.0, 10.0, 60),
                ("SPY", int(before_close_et.astimezone(timezone.utc).timestamp() * 1000), 110.0, 112.0, 108.0, 110.0, 20.0, 60),
                ("SPY", int(after_close_et.astimezone(timezone.utc).timestamp() * 1000), 130.0, 200.0, 129.0, 130.0, 30.0, 60),
            ]
            conn.executemany(
                "INSERT INTO bar_data(symbol, ts, open, high, low, close, volume, bar_interval_sec) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            conn.commit()

            payload = event_writer.aggregate_daily_candles(conn, "SPY", limit=10)
            candles = payload.get("candles", [])
            self.assertEqual(len(candles), 1)
            candle = candles[0]
            self.assertEqual(float(candle["open"]), 100.0)
            self.assertEqual(float(candle["close"]), 110.0)
            self.assertEqual(float(candle["high"]), 112.0)
            self.assertEqual(float(candle["low"]), 99.0)
            self.assertEqual(int(candle["volume"]), 30)
        finally:
            conn.close()

    def test_event_writer_uses_threading_http_server(self) -> None:
        source = (REPO_ROOT / "server" / "event_writer.py").read_text(encoding="utf-8")
        self.assertIn("ThreadingHTTPServer", source)
        self.assertIn("server.daemon_threads = True", source)

    def test_live_collector_uses_threading_http_server(self) -> None:
        source = (REPO_ROOT / "server" / "live_event_collector.py").read_text(encoding="utf-8")
        self.assertIn("ThreadingHTTPServer", source)
        self.assertIn("server.daemon_threads = True", source)

    def test_live_collector_score_retries_transient_transport_errors(self) -> None:
        collector = load_module(
            "pq_live_collector_score_retry_test",
            REPO_ROOT / "server" / "live_event_collector.py",
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

        original_urlopen = collector.urlopen
        original_sleep = collector.time.sleep
        original_attempts = collector.SCORE_MAX_ATTEMPTS
        original_retry_base = collector.SCORE_RETRY_BASE_SEC
        original_retry_max = collector.SCORE_RETRY_MAX_SEC
        try:
            collector.SCORE_MAX_ATTEMPTS = 2
            collector.SCORE_RETRY_BASE_SEC = 0.001
            collector.SCORE_RETRY_MAX_SEC = 0.001
            sleep_calls: list[float] = []
            collector.time.sleep = lambda delay: sleep_calls.append(float(delay))

            calls = {"n": 0}

            def _flaky_urlopen(req, timeout=0):  # noqa: ANN001
                payload = json.loads(req.data.decode("utf-8"))
                events = payload.get("events", [])
                calls["n"] += 1
                if calls["n"] == 1:
                    raise URLError("timed out")
                return _FakeResp({"results": [{"status": "ok"} for _ in events]})

            collector.urlopen = _flaky_urlopen
            scored = collector._score_events([{"event_id": "evt_retry"}])
        finally:
            collector.urlopen = original_urlopen
            collector.time.sleep = original_sleep
            collector.SCORE_MAX_ATTEMPTS = original_attempts
            collector.SCORE_RETRY_BASE_SEC = original_retry_base
            collector.SCORE_RETRY_MAX_SEC = original_retry_max

        self.assertEqual(scored, 1)
        self.assertEqual(calls["n"], 2)
        self.assertEqual(len(sleep_calls), 1)

    def test_live_collector_score_batch_falls_back_to_single_events(self) -> None:
        collector = load_module(
            "pq_live_collector_score_single_fallback_test",
            REPO_ROOT / "server" / "live_event_collector.py",
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

        original_urlopen = collector.urlopen
        original_attempts = collector.SCORE_MAX_ATTEMPTS
        try:
            collector.SCORE_MAX_ATTEMPTS = 1
            posted_sizes: list[int] = []

            def _batch_fail_urlopen(req, timeout=0):  # noqa: ANN001
                payload = json.loads(req.data.decode("utf-8"))
                events = payload.get("events", [])
                posted_sizes.append(len(events))
                if len(events) > 1:
                    raise URLError("timed out")
                return _FakeResp({"results": [{"status": "ok"} for _ in events]})

            collector.urlopen = _batch_fail_urlopen
            scored = collector._score_events([{"event_id": "evt_a"}, {"event_id": "evt_b"}])
        finally:
            collector.urlopen = original_urlopen
            collector.SCORE_MAX_ATTEMPTS = original_attempts

        self.assertEqual(scored, 2)
        self.assertIn(2, posted_sizes)
        self.assertGreaterEqual(posted_sizes.count(1), 2)

    def test_event_writer_registers_atexit_connection_cleanup(self) -> None:
        source = (REPO_ROOT / "server" / "event_writer.py").read_text(encoding="utf-8")
        self.assertIn("atexit.register(_close_thread_local_connection)", source)

    def test_event_writer_reuses_thread_local_sqlite_connection(self) -> None:
        event_writer = load_module(
            "pq_event_writer_conn_reuse_test",
            REPO_ROOT / "server" / "event_writer.py",
        )
        db_path = self.tmp / "event_writer_reuse.sqlite"
        event_writer.DB_PATH = str(db_path)
        event_writer._SCHEMA_READY = False
        event_writer._SCHEMA_DB_PATH = None
        event_writer._THREAD_LOCAL.conn = None
        event_writer._THREAD_LOCAL.conn_db_path = None

        conn1 = event_writer.connect()
        conn2 = event_writer.connect()
        try:
            self.assertIs(conn1, conn2)
            self.assertEqual(event_writer._THREAD_LOCAL.conn_db_path, str(db_path))
            self.assertEqual(conn1.execute("SELECT 1").fetchone()[0], 1)
        finally:
            try:
                conn1.close()
            except Exception:
                pass
            event_writer._THREAD_LOCAL.conn = None
            event_writer._THREAD_LOCAL.conn_db_path = None

    def test_backfill_gamma_context_falls_back_to_snapshots(self) -> None:
        backfill = load_module(
            "pq_backfill_gamma_snapshot_fallback_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        original_fetch_json = backfill.fetch_json
        original_urlopen = backfill.urlopen
        original_token = backfill.MARKETDATA_APP_TOKEN
        backfill.MARKETDATA_APP_TOKEN = ""

        def _bridge_down(*_args, **_kwargs):
            raise RuntimeError("bridge unavailable")

        backfill.urlopen = _bridge_down
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
            backfill.urlopen = original_urlopen
            backfill.fetch_json = original_fetch_json
            backfill.MARKETDATA_APP_TOKEN = original_token
            conn.close()

    def test_backfill_build_daily_bars_filters_non_rth(self) -> None:
        backfill = load_module(
            "pq_backfill_rth_filter_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        base_date = date(2026, 3, 10)

        def _et_ts(hour: int, minute: int) -> int:
            dt = datetime(
                base_date.year,
                base_date.month,
                base_date.day,
                hour,
                minute,
                tzinfo=backfill.NY_TZ,
            )
            return int(dt.timestamp())

        bars = [
            {"time": _et_ts(8, 0), "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
            {"time": _et_ts(9, 30), "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
            {"time": _et_ts(15, 59), "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
            {"time": _et_ts(16, 0), "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
        ]

        sessions = backfill.build_daily_bars(bars)
        self.assertEqual(len(sessions), 1)
        self.assertEqual(len(sessions[0]["bars"]), 2)

    def test_backfill_events_reject_future_gamma_context_date(self) -> None:
        backfill = load_module(
            "pq_backfill_future_gamma_guard_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        base_date = date(2026, 3, 10)
        session_date = date(2026, 3, 11)
        future_gamma_date = date(2026, 3, 12)
        bar_ts = int(datetime(2026, 3, 11, 10, 0, tzinfo=backfill.NY_TZ).timestamp())

        sessions = [
            {
                "date": base_date,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "bars": [
                    {
                        "time": int(datetime(2026, 3, 10, 10, 0, tzinfo=backfill.NY_TZ).timestamp()),
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.0,
                        "volume": 1000.0,
                    }
                ],
            },
            {
                "date": session_date,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "bars": [
                    {
                        "time": bar_ts,
                        "open": 100.0,
                        "high": 100.2,
                        "low": 99.8,
                        "close": 100.0,
                        "volume": 1000.0,
                    }
                ],
            },
        ]

        events = backfill.build_events(
            symbol="SPY",
            sessions=sessions,
            interval_sec=60,
            threshold_bps=10.0,
            cooldown_min=10,
            source="yahoo",
            atr_by_date={base_date: 1.0},
            conn=None,
            rv_by_date={},
            rv_regime_by_date={},
            gamma_context={
                "gamma_flip": 500.0,
                "generated_at_date_et": datetime(
                    future_gamma_date.year,
                    future_gamma_date.month,
                    future_gamma_date.day,
                    8,
                    30,
                    tzinfo=backfill.NY_TZ,
                ),
                "source_name": "marketdata_live",
            },
        )
        self.assertTrue(events)
        self.assertTrue(all(ev.get("gamma_flip") is None for ev in events))

    def test_backfill_fetch_json_does_not_retry_auth_errors(self) -> None:
        backfill = load_module(
            "pq_backfill_fetch_json_auth_no_retry_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        attempts = {"count": 0}
        original_urlopen = backfill.urlopen
        original_sleep = backfill.time.sleep
        try:
            def _fake_urlopen(req, timeout=0):
                attempts["count"] += 1
                raise backfill.HTTPError(req.full_url, 401, "Unauthorized", {}, None)

            backfill.urlopen = _fake_urlopen
            backfill.time.sleep = lambda _seconds: None
            with self.assertRaises(backfill.HTTPError):
                backfill.fetch_json("http://127.0.0.1:3000/api/market?source=yahoo&symbol=SPY")
        finally:
            backfill.urlopen = original_urlopen
            backfill.time.sleep = original_sleep

        self.assertEqual(attempts["count"], 1)

    def test_backfill_fetch_market_bypasses_proxy_after_auth_failure(self) -> None:
        backfill = load_module(
            "pq_backfill_market_proxy_failopen_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        calls: list[str] = []
        original_fetch_json = backfill.fetch_json
        original_proxy_url = backfill.YAHOO_PROXY_URL
        original_failopen_sec = backfill.YAHOO_PROXY_AUTH_FAILOPEN_SEC
        original_skip_auth_required = backfill.YAHOO_PROXY_SKIP_AUTH_REQUIRED
        original_service_token = backfill.YAHOO_PROXY_SERVICE_TOKEN
        try:
            with backfill._yahoo_proxy_auth_failopen_lock:
                backfill._yahoo_proxy_auth_failopen_until = 0.0
                backfill._yahoo_proxy_auth_failopen_reason = ""

            backfill.YAHOO_PROXY_URL = "http://127.0.0.1:3000/api/market"
            backfill.YAHOO_PROXY_AUTH_FAILOPEN_SEC = 300
            # Keep this test deterministic regardless of local dashboard auth settings.
            backfill.YAHOO_PROXY_SKIP_AUTH_REQUIRED = False
            backfill.YAHOO_PROXY_SERVICE_TOKEN = ""

            def _fake_fetch_json(url: str, timeout: int = 12, retries: int = 2) -> dict:
                calls.append(url)
                if "/api/market" in url:
                    raise backfill.HTTPError(url, 401, "Unauthorized", {}, None)
                return {
                    "chart": {
                        "result": [
                            {
                                "timestamp": [1_777_700_000],
                                "indicators": {
                                    "quote": [
                                        {
                                            "open": [100.0],
                                            "high": [101.0],
                                            "low": [99.0],
                                            "close": [100.5],
                                            "volume": [1000.0],
                                        }
                                    ]
                                },
                            }
                        ],
                        "error": None,
                    }
                }

            backfill.fetch_json = _fake_fetch_json
            payload1, source1 = backfill.fetch_market("SPY", "1m", "1d", "yahoo")
            payload2, source2 = backfill.fetch_market("SPY", "1m", "1d", "yahoo")
        finally:
            backfill.fetch_json = original_fetch_json
            backfill.YAHOO_PROXY_URL = original_proxy_url
            backfill.YAHOO_PROXY_AUTH_FAILOPEN_SEC = original_failopen_sec
            backfill.YAHOO_PROXY_SKIP_AUTH_REQUIRED = original_skip_auth_required
            backfill.YAHOO_PROXY_SERVICE_TOKEN = original_service_token
            with backfill._yahoo_proxy_auth_failopen_lock:
                backfill._yahoo_proxy_auth_failopen_until = 0.0
                backfill._yahoo_proxy_auth_failopen_reason = ""

        self.assertEqual(source1, "Yahoo")
        self.assertEqual(source2, "Yahoo")
        self.assertEqual(len(payload1.get("candles") or []), 1)
        self.assertEqual(len(payload2.get("candles") or []), 1)
        proxy_calls = [u for u in calls if "/api/market" in u]
        direct_calls = [u for u in calls if "query1.finance.yahoo.com" in u]
        self.assertEqual(len(proxy_calls), 1)
        self.assertEqual(len(direct_calls), 2)

    def test_backfill_fetch_json_attaches_service_token_for_local_proxy(self) -> None:
        backfill = load_module(
            "pq_backfill_proxy_service_token_header_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        captured_headers: list[dict[str, str]] = []
        original_urlopen = backfill.urlopen
        original_token = backfill.YAHOO_PROXY_SERVICE_TOKEN
        try:
            backfill.YAHOO_PROXY_SERVICE_TOKEN = "svc_token_123"

            class _FakeResp:
                def read(self) -> bytes:
                    return b"{}"

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb) -> bool:
                    return False

            def _fake_urlopen(req, timeout=0):  # noqa: ANN001
                captured_headers.append({k.lower(): v for k, v in req.header_items()})
                return _FakeResp()

            backfill.urlopen = _fake_urlopen
            backfill.fetch_json("http://127.0.0.1:3000/api/market?source=yahoo&symbol=SPY")
            backfill.fetch_json("https://query1.finance.yahoo.com/v8/finance/chart/SPY?range=1d&interval=1m")
        finally:
            backfill.urlopen = original_urlopen
            backfill.YAHOO_PROXY_SERVICE_TOKEN = original_token

        self.assertGreaterEqual(len(captured_headers), 2)
        self.assertEqual(captured_headers[0].get("x-pivot-service-token"), "svc_token_123")
        self.assertNotIn("x-pivot-service-token", captured_headers[1])

    def test_backfill_fetch_market_skips_proxy_when_auth_required_without_service_token(self) -> None:
        backfill = load_module(
            "pq_backfill_proxy_auth_skip_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        calls: list[str] = []
        original_fetch_json = backfill.fetch_json
        original_proxy_url = backfill.YAHOO_PROXY_URL
        original_skip_auth_required = backfill.YAHOO_PROXY_SKIP_AUTH_REQUIRED
        original_service_token = backfill.YAHOO_PROXY_SERVICE_TOKEN
        original_env = {
            "DASH_AUTH_ENABLED": os.environ.get("DASH_AUTH_ENABLED"),
            "DASH_AUTH_LOCAL_BYPASS": os.environ.get("DASH_AUTH_LOCAL_BYPASS"),
            "DASH_AUTH_PASSWORD": os.environ.get("DASH_AUTH_PASSWORD"),
            "HOST": os.environ.get("HOST"),
        }
        try:
            backfill.YAHOO_PROXY_URL = "http://127.0.0.1:3000/api/market"
            backfill.YAHOO_PROXY_SKIP_AUTH_REQUIRED = True
            backfill.YAHOO_PROXY_SERVICE_TOKEN = ""
            with backfill._yahoo_proxy_auth_skip_lock:
                backfill._yahoo_proxy_auth_skip_logged = False

            os.environ["DASH_AUTH_ENABLED"] = "true"
            os.environ["DASH_AUTH_LOCAL_BYPASS"] = "false"
            os.environ["DASH_AUTH_PASSWORD"] = "test-password"
            os.environ["HOST"] = "127.0.0.1"

            def _fake_fetch_json(url: str, timeout: int = 12, retries: int = 2) -> dict:
                calls.append(url)
                if "/api/market" in url:
                    raise AssertionError("proxy should be skipped when auth is required and no token is configured")
                return {
                    "chart": {
                        "result": [
                            {
                                "timestamp": [1_777_700_000],
                                "indicators": {
                                    "quote": [
                                        {
                                            "open": [100.0],
                                            "high": [101.0],
                                            "low": [99.0],
                                            "close": [100.5],
                                            "volume": [1000.0],
                                        }
                                    ]
                                },
                            }
                        ],
                        "error": None,
                    }
                }

            backfill.fetch_json = _fake_fetch_json
            payload, source = backfill.fetch_market("SPY", "1m", "1d", "yahoo")
        finally:
            backfill.fetch_json = original_fetch_json
            backfill.YAHOO_PROXY_URL = original_proxy_url
            backfill.YAHOO_PROXY_SKIP_AUTH_REQUIRED = original_skip_auth_required
            backfill.YAHOO_PROXY_SERVICE_TOKEN = original_service_token
            for key, value in original_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            with backfill._yahoo_proxy_auth_skip_lock:
                backfill._yahoo_proxy_auth_skip_logged = False

        self.assertEqual(source, "Yahoo")
        self.assertEqual(len(payload.get("candles") or []), 1)
        self.assertTrue(any("query1.finance.yahoo.com" in url for url in calls))
        self.assertFalse(any("/api/market" in url for url in calls))

    def test_backfill_gamma_context_prefers_live_when_snapshot_is_stale(self) -> None:
        backfill = load_module(
            "pq_backfill_gamma_live_refresh_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        original_fetch_json = backfill.fetch_json
        original_urlopen = backfill.urlopen
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

        backfill.urlopen = _bridge_down
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
            backfill.urlopen = original_urlopen
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
        original_urlopen = backfill.urlopen
        original_token = backfill.MARKETDATA_APP_TOKEN
        backfill.MARKETDATA_APP_TOKEN = ""

        def _bridge_down(*_args, **_kwargs):
            raise RuntimeError("bridge unavailable")

        backfill.urlopen = _bridge_down
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
            backfill.urlopen = original_urlopen
            backfill.fetch_json = original_fetch_json
            backfill.MARKETDATA_APP_TOKEN = original_token
            conn.close()

    def test_backfill_gamma_context_carries_recent_gamma_when_today_missing(self) -> None:
        backfill = load_module(
            "pq_backfill_gamma_carry_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        original_fetch_json = backfill.fetch_json
        original_urlopen = backfill.urlopen
        original_token = backfill.MARKETDATA_APP_TOKEN
        backfill.MARKETDATA_APP_TOKEN = ""

        def _bridge_down(*_args, **_kwargs):
            raise RuntimeError("bridge unavailable")

        backfill.urlopen = _bridge_down
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
            backfill.urlopen = original_urlopen
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

    def test_collect_gamma_history_fallback_uses_dte_filter(self) -> None:
        source = (REPO_ROOT / "scripts" / "collect_gamma_history.py").read_text(encoding="utf-8")
        self.assertIn("GAMMA_HISTORY_LIVE_DTE_DAYS", source)
        fetch_block = source.split("def fetch_marketdata_chain(", 1)[1].split("def _to_float", 1)[0]
        self.assertIn("?dte={GAMMA_HISTORY_LIVE_DTE_DAYS}", fetch_block)
        self.assertNotIn("?expiration=all", fetch_block)

    def test_backfill_gamma_context_avoids_marketdata_when_bridge_reports_cooldown(self) -> None:
        source = (REPO_ROOT / "scripts" / "backfill_events.py").read_text(encoding="utf-8")
        block = source.split("def fetch_gamma_context(", 1)[1].split("def et_date", 1)[0]
        self.assertIn("bridge_marketdata_cooldown", block)
        self.assertIn("cooldown active", block)
        self.assertIn("daily request limit", block)
        self.assertIn("if bridge_marketdata_cooldown:", block)
        self.assertIn("_merge_context_with_carry(snapshot_context, carry_context, today_et)", block)

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

    def test_mathematical_models_require_open_for_woodie_and_demark(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        node_script = textwrap.dedent(
            """
            import { MathematicalModels } from './src/math/MathematicalModels.js';
            const model = new MathematicalModels({ precision: 6 });

            const withOpen = [
              { high: 10.0, low: 9.0, open: 9.3, close: 9.8 },
              { high: 10.8, low: 9.6, open: 10.0, close: 10.4 },
            ];
            const missingOpen = [
              { high: 10.0, low: 9.0, close: 9.8 },
              { high: 10.8, low: 9.6, close: 10.4 },
            ];

            const woodie = await model.calculateWoodiePivots(withOpen);
            if (!Number.isFinite(woodie.PP)) {
              throw new Error('Woodie pivot should be numeric for valid OHLC input.');
            }

            let demarkErr = '';
            try {
              await model.calculateDeMarkPivots(missingOpen);
            } catch (error) {
              demarkErr = String(error?.message || error);
            }
            if (!demarkErr.includes('open required')) {
              throw new Error(`Expected open-required validation error, got: ${demarkErr}`);
            }
            console.log('ok');
            """
        ).strip()

        proc = run_cmd(
            ["node", "--input-type=module", "-e", node_script],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_quantpivot_logging_level_contract_present(self) -> None:
        engine_source = (REPO_ROOT / "src" / "core" / "QuantPivotEngine.js").read_text(
            encoding="utf-8"
        )
        index_source = (REPO_ROOT / "src" / "index.js").read_text(encoding="utf-8")

        self.assertIn("this.config.logging.level <= 2", engine_source)
        self.assertIn("this.config.logging.level <= 1", engine_source)
        self.assertIn("this.config.logging.level <= 0", engine_source)
        self.assertIn("this.config.logging.level <= 2", index_source)
        self.assertNotIn("this.config.logging.level >= 2", index_source)

    def test_quantpivot_reuses_engine_component_instances(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        node_script = textwrap.dedent(
            """
            import QuantPivot from './src/index.js';

            const qp = new QuantPivot({}, 'production');
            if (qp.validator !== qp.engine.validator) {
              throw new Error('QuantPivot validator should reuse engine.validator.');
            }
            if (qp.monitor !== qp.engine.monitor) {
              throw new Error('QuantPivot monitor should reuse engine.monitor.');
            }
            if (qp.math !== qp.engine.mathModels) {
              throw new Error('QuantPivot math should reuse engine.mathModels.');
            }
            qp.dispose();
            console.log('ok');
            """
        ).strip()

        proc = run_cmd(
            ["node", "--input-type=module", "-e", node_script],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_quantpivot_dispose_unsubscribes_config_and_window_handlers(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        node_script = textwrap.dedent(
            """
            import QuantPivot, { ConfigurationManager } from './src/index.js';

            const manager = ConfigurationManager.getInstance();
            const originalSubscribe = manager.subscribe.bind(manager);
            let unsubscribeCalls = 0;
            manager.subscribe = (callback) => {
              const unsubscribe = originalSubscribe(callback);
              return () => {
                unsubscribeCalls += 1;
                unsubscribe();
              };
            };

            const listeners = [];
            globalThis.window = {
              addEventListener(type, handler) {
                listeners.push({ type, handler, removed: false });
              },
              removeEventListener(type, handler) {
                const item = listeners.find((entry) => entry.type === type && entry.handler === handler);
                if (item) item.removed = true;
              },
            };

            const qp = new QuantPivot({}, 'production');
            qp.dispose();

            if (unsubscribeCalls !== 1) {
              throw new Error(`Expected exactly one unsubscribe call, got ${unsubscribeCalls}`);
            }
            const unhandled = listeners.find((entry) => entry.type === 'unhandledrejection');
            if (!unhandled || unhandled.removed !== true) {
              throw new Error('Expected unhandledrejection listener to be removed on dispose().');
            }
            console.log('ok');
            """
        ).strip()

        proc = run_cmd(
            ["node", "--input-type=module", "-e", node_script],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_backtest_sharpe_uses_period_returns_not_cumulative(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        node_script = textwrap.dedent(
            """
            import QuantPivot from './src/index.js';

            const trades = [
              { type: 'buy', price: 10, size: 1 },
              { type: 'sell', price: 11, size: 1 },
            ];
            const perf = QuantPivot.prototype._calculateBacktestPerformance.call(
              {},
              trades,
              100,
              0
            );

            if (!(perf.totalReturn > 0)) {
              throw new Error(`Expected positive total return, got ${perf.totalReturn}`);
            }
            if (!(perf.sharpeRatio > 0)) {
              throw new Error(`Sharpe should be positive for this path, got ${perf.sharpeRatio}`);
            }
            const expected = 0.0707106781;
            if (Math.abs(perf.sharpeRatio - expected) > 1e-4) {
              throw new Error(`Unexpected sharpe=${perf.sharpeRatio}, expected ~${expected}`);
            }
            console.log('ok');
            """
        ).strip()

        proc = run_cmd(
            ["node", "--input-type=module", "-e", node_script],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_mathematical_models_level_correlations_are_input_driven(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        node_script = textwrap.dedent(
            """
            import { MathematicalModels } from './src/math/MathematicalModels.js';
            const model = new MathematicalModels({ precision: 6 });

            const positive = model.calculateLevelCorrelations({
              base: { A: 1, B: 2, C: 3, D: 4 },
              shifted: { A: 2, B: 4, C: 6, D: 8 },
            });
            const negative = model.calculateLevelCorrelations({
              base: { A: 1, B: 2, C: 3, D: 4 },
              inverted: { A: 4, B: 3, C: 2, D: 1 },
            });

            if (!(positive.pearson > 0.99 && positive.spearman > 0.99)) {
              throw new Error(`Expected strong positive correlation, got ${JSON.stringify(positive)}`);
            }
            if (!(negative.pearson < -0.99 && negative.spearman < -0.99)) {
              throw new Error(`Expected strong negative correlation, got ${JSON.stringify(negative)}`);
            }
            console.log('ok');
            """
        ).strip()

        proc = run_cmd(
            ["node", "--input-type=module", "-e", node_script],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_mathematical_models_confidence_interval_respects_confidence_input(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        node_script = textwrap.dedent(
            """
            import { MathematicalModels } from './src/math/MathematicalModels.js';
            const model = new MathematicalModels({ precision: 6 });
            const bars = [
              { open: 100.0, high: 101.0, low: 99.0, close: 100.0 },
              { open: 100.0, high: 102.0, low: 98.0, close: 101.0 },
              { open: 101.0, high: 103.0, low: 99.0, close: 99.5 },
              { open: 99.5, high: 100.5, low: 97.5, close: 100.8 },
              { open: 100.8, high: 102.8, low: 98.8, close: 100.2 },
            ];

            const ci90 = model.calculateConfidenceInterval(100, bars, 0.90);
            const ci99 = model.calculateConfidenceInterval(100, bars, 0.99);
            const width90 = ci90.upper - ci90.lower;
            const width99 = ci99.upper - ci99.lower;

            if (!(Math.abs(ci90.confidence - 0.90) < 1e-9)) {
              throw new Error(`Expected confidence=0.90, got ${ci90.confidence}`);
            }
            if (!(Math.abs(ci99.confidence - 0.99) < 1e-9)) {
              throw new Error(`Expected confidence=0.99, got ${ci99.confidence}`);
            }
            if (!(width99 > width90)) {
              throw new Error(`Expected wider interval at 99% confidence (90=${width90}, 99=${width99})`);
            }
            console.log('ok');
            """
        ).strip()

        proc = run_cmd(
            ["node", "--input-type=module", "-e", node_script],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_mathematical_models_variance_uses_sample_denominator(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        node_script = textwrap.dedent(
            """
            import { MathematicalModels } from './src/math/MathematicalModels.js';
            const model = new MathematicalModels({ precision: 6 });

            const values = [1, 2, 3, 4];
            const variance = model._calculateVariance(values);
            const expectedSampleVariance = 5 / 3;
            if (Math.abs(variance - expectedSampleVariance) > 1e-9) {
              throw new Error(`Expected sample variance ${expectedSampleVariance}, got ${variance}`);
            }
            const singleton = model._calculateVariance([42]);
            if (singleton !== 0) {
              throw new Error(`Expected singleton variance 0, got ${singleton}`);
            }
            console.log('ok');
            """
        ).strip()

        proc = run_cmd(
            ["node", "--input-type=module", "-e", node_script],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_mathematical_models_risk_ratios_handle_zero_volatility(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        node_script = textwrap.dedent(
            """
            import { MathematicalModels } from './src/math/MathematicalModels.js';
            const model = new MathematicalModels({ precision: 6 });
            const bars = [
              { open: 100.0, high: 100.0, low: 100.0, close: 100.0 },
              { open: 100.0, high: 100.0, low: 100.0, close: 100.0 },
              { open: 100.0, high: 100.0, low: 100.0, close: 100.0 },
              { open: 100.0, high: 100.0, low: 100.0, close: 100.0 },
            ];

            const sharpe = model.calculateSharpeRatio(bars);
            const calmar = model.calculateCalmarRatio(bars);
            const sortino = model.calculateSortinoRatio(bars);
            const ratios = { sharpe, calmar, sortino };
            for (const [name, value] of Object.entries(ratios)) {
              if (!Number.isFinite(value)) {
                throw new Error(`${name} should be finite, got ${value}`);
              }
              if (value !== 0) {
                throw new Error(`${name} should resolve to 0 for flat series, got ${value}`);
              }
            }
            console.log('ok');
            """
        ).strip()

        proc = run_cmd(
            ["node", "--input-type=module", "-e", node_script],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_mathematical_models_zscore_handles_zero_variance(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        node_script = textwrap.dedent(
            """
            import { MathematicalModels } from './src/math/MathematicalModels.js';
            const model = new MathematicalModels({ precision: 6 });
            const z = model._calculateZScore([7, 7, 7]);
            if (!Array.isArray(z) || z.length !== 3) {
              throw new Error(`Expected z-score array of length 3, got ${JSON.stringify(z)}`);
            }
            if (!z.every((value) => Number.isFinite(value) && value === 0)) {
              throw new Error(`Expected all zero z-scores for zero variance input, got ${JSON.stringify(z)}`);
            }
            console.log('ok');
            """
        ).strip()

        proc = run_cmd(
            ["node", "--input-type=module", "-e", node_script],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_mathematical_models_level_accuracy_is_not_constant(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        node_script = textwrap.dedent(
            """
            import { MathematicalModels } from './src/math/MathematicalModels.js';
            const model = new MathematicalModels({ precision: 6 });
            const bars = [
              { open: 100.0, high: 101.0, low: 99.0, close: 100.0 },
              { open: 100.0, high: 101.5, low: 99.5, close: 100.8 },
              { open: 100.8, high: 102.0, low: 100.2, close: 101.2 },
              { open: 101.2, high: 101.8, low: 99.8, close: 100.4 },
            ];

            const nearLevels = {
              standard: { PP: 100.5, R1: 101.0, S1: 100.0 },
              fibonacci: { PP: 100.6, R1: 101.1, S1: 99.9 },
            };
            const farLevels = {
              standard: { PP: 140.0, R1: 145.0, S1: 135.0 },
              fibonacci: { PP: 160.0, R1: 165.0, S1: 155.0 },
            };

            const near = model.calculateLevelAccuracy(bars, nearLevels);
            const far = model.calculateLevelAccuracy(bars, farLevels);

            if (!(near.overall > far.overall)) {
              throw new Error(`Expected near levels to score better. near=${near.overall}, far=${far.overall}`);
            }
            if (near.overall === 0.75 && far.overall === 0.75) {
              throw new Error('Accuracy appears hardcoded at 0.75');
            }
            console.log('ok');
            """
        ).strip()

        proc = run_cmd(
            ["node", "--input-type=module", "-e", node_script],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_dashboard_proxy_public_auth_and_endpoint_hardening_present(self) -> None:
        proxy_source = (REPO_ROOT / "server" / "yahoo_proxy.js").read_text(encoding="utf-8")
        env_example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
        self.assertIn("DASH_AUTH_ENABLED", proxy_source)
        self.assertIn("DASH_AUTH_PASSWORD", proxy_source)
        self.assertIn("DASH_AUTH_MIN_PASSWORD_LEN", proxy_source)
        self.assertIn("DASH_AUTH_ENFORCE_STRONG_PASSWORD", proxy_source)
        self.assertIn("readSetting(procEnv, fileEnv, 'DASH_AUTH_ENFORCE_STRONG_PASSWORD', 'true')", proxy_source)
        self.assertIn("DASH_AUTH_ENFORCE_STRONG_PASSWORD=true", env_example)
        self.assertIn("DASH_AUTH_RATE_LIMIT_ENABLED", proxy_source)
        self.assertIn("DASH_AUTH_RATE_LIMIT_MAX_ATTEMPTS", proxy_source)
        self.assertIn("DASH_AUTH_RATE_LIMIT_LOCKOUT_SEC", proxy_source)
        self.assertIn("DASH_AUTH_METRICS_WINDOW_SEC", proxy_source)
        self.assertIn("DASH_AUTH_METRICS_MAX_TRACKED_CLIENTS", proxy_source)
        self.assertIn("DASH_WRITE_ENDPOINTS_LOCAL_ONLY", proxy_source)
        self.assertIn("AUTH_METRICS_STATE_FILE", proxy_source)
        self.assertIn("auth_metrics.json", proxy_source)
        self.assertIn("WRITE_ENDPOINTS", proxy_source)
        self.assertIn("handleAuthRoutes", proxy_source)
        self.assertIn("registerAuthLoginFailure", proxy_source)
        self.assertIn("clearAuthLoginFailures", proxy_source)
        self.assertIn("recordAuthLoginSuccess", proxy_source)
        self.assertIn("loadPersistedAuthMetricsState", proxy_source)
        self.assertIn("persistAuthMetricsState", proxy_source)
        self.assertIn("url.pathname === '/api/security/sessions'", proxy_source)
        self.assertIn("url.pathname === '/api/runtime/architecture'", proxy_source)
        self.assertIn("RUNTIME_ARCHITECTURE", proxy_source)
        self.assertIn("buildRuntimeArchitectureSnapshot()", proxy_source)
        self.assertIn("runtime_architecture_mode", proxy_source)
        self.assertIn("runtime_dashboard_uses_src_library", proxy_source)
        self.assertIn("dashboard_script_count_total", proxy_source)
        self.assertIn("dashboard_script_count_external", proxy_source)
        self.assertIn("dashboard_script_count_inline", proxy_source)
        self.assertIn("auth_active_session_count", proxy_source)
        self.assertIn("auth_login_success_total", proxy_source)
        self.assertIn("Retry-After", proxy_source)
        self.assertIn("url.pathname === '/auth/login'", proxy_source)
        self.assertIn("auth_method: 'password_cookie'", proxy_source)
        self.assertIn("auth_policy_ok", proxy_source)
        self.assertIn("auth_policy_issues", proxy_source)
        self.assertIn("runtime_architecture_governance_state", proxy_source)
        self.assertIn("local_bypass_with_non_loopback_bind", proxy_source)
        self.assertIn(
            "DASH_AUTH_ENFORCE_STRONG_PASSWORD=false while auth is enabled; weak passwords are allowed.",
            proxy_source,
        )
        self.assertIn("auth_rate_limit_enabled", proxy_source)
        self.assertIn("x-forwarded-for", proxy_source)
        self.assertIn("url.pathname === '/health'", proxy_source)

    def test_dashboard_proxy_runtime_architecture_live_endpoint(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        port = self._find_free_tcp_port()
        proc = self._start_dashboard_proxy(port)
        try:
            health = self._wait_for_dashboard_proxy_health(port, proc)
            self.assertEqual(health.get("runtime_architecture_mode"), "dashboard_globals")
            self.assertIn("runtime_dashboard_script_count", health)
            self.assertIn("runtime_architecture_governance_state", health)
            self.assertIn("auth_policy_ok", health)
            self.assertIn("auth_policy_issues", health)

            status, payload = self._read_json_url(
                f"http://127.0.0.1:{port}/api/runtime/architecture",
                timeout_sec=2.0,
            )
            self.assertEqual(status, 200)
            self.assertEqual(payload.get("status"), "ok")
            self.assertEqual(payload.get("runtime_mode"), "dashboard_globals")
            self.assertIn("runtime_governance_state", payload)

            total = int(payload.get("dashboard_script_count_total", -1))
            external = int(payload.get("dashboard_script_count_external", -1))
            inline = int(payload.get("dashboard_script_count_inline", -1))
            self.assertGreaterEqual(total, 0)
            self.assertGreaterEqual(external, 0)
            self.assertGreaterEqual(inline, 0)
            self.assertEqual(total, external + inline)
        finally:
            self._stop_process(proc)

    def test_dashboard_proxy_runtime_architecture_rejects_forwarded_requests(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is not available in PATH")

        port = self._find_free_tcp_port()
        proc = self._start_dashboard_proxy(port)
        try:
            self._wait_for_dashboard_proxy_health(port, proc)
            with self.assertRaises(HTTPError) as ctx:
                self._read_json_url(
                    f"http://127.0.0.1:{port}/api/runtime/architecture",
                    headers={"x-forwarded-for": "203.0.113.9"},
                    timeout_sec=2.0,
                )
            self.assertEqual(ctx.exception.code, 403)
            body = ctx.exception.read().decode("utf-8")
            self.assertIn("restricted to local requests", body)
        finally:
            self._stop_process(proc)

    def test_dashboard_toast_and_ops_fields_avoid_dynamic_innerhtml(self) -> None:
        dashboard = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        self.assertIn("msgDiv.textContent = message;", dashboard)
        self.assertNotIn("if (opts.html)", dashboard)
        self.assertIn("closeBtn.textContent = '×';", dashboard)
        self.assertIn("function setLabeledText(id, label, value)", dashboard)
        self.assertIn("setOpsField(id, label, value)", dashboard)
        self.assertIn("setLabeledText(id, label, value ?? '--');", dashboard)
        self.assertIn("cacheStale: !!data.cacheStale", dashboard)
        self.assertIn("cacheStaleReason: data.cacheStaleReason ? String(data.cacheStaleReason) : null", dashboard)
        self.assertIn("Stale (cached)", dashboard)

    def test_dashboard_proxy_ops_status_uses_async_file_reads(self) -> None:
        proxy_source = (REPO_ROOT / "server" / "yahoo_proxy.js").read_text(encoding="utf-8")
        self.assertIn("const fsp = fs.promises;", proxy_source)
        self.assertIn("async function loadEnvMapAsync(filePath)", proxy_source)
        self.assertIn("async function readJsonFileSafeAsync(filePath, fallback = null)", proxy_source)
        self.assertIn("async function readTailLinesAsync(filePath, maxLines = 120)", proxy_source)
        query_block = proxy_source.split("async function queryOpsStatus()", 1)[1].split(
            "return {",
            1,
        )[0]
        self.assertIn("await Promise.all([", query_block)
        self.assertIn("loadEnvMapAsync(ENV_FILE)", query_block)
        self.assertIn("readJsonFileSafeAsync(BACKUP_STATE_FILE, {})", query_block)
        self.assertIn("readTailLinesAsync(REPORT_DELIVERY_LOG_FILE, 200)", query_block)
        self.assertNotIn("readFileSync(", query_block)

    def test_dashboard_proxy_ml_metrics_uses_async_file_reads(self) -> None:
        proxy_source = (REPO_ROOT / "server" / "yahoo_proxy.js").read_text(encoding="utf-8")
        metrics_block = proxy_source.split("if (url.pathname === '/api/ml/metrics') {", 1)[1].split(
            "if (url.pathname === '/api/ml/health')",
            1,
        )[0]
        self.assertIn("await Promise.all([", metrics_block)
        self.assertIn("readJsonFileAsync(METRICS_FILE)", metrics_block)
        self.assertIn("readJsonFileAsync(CALIB_FILE)", metrics_block)
        self.assertNotIn("readJsonFile(METRICS_FILE)", metrics_block)
        self.assertNotIn("readJsonFile(CALIB_FILE)", metrics_block)

    def test_reload_score_stress_harness_contract_present(self) -> None:
        source = (REPO_ROOT / "scripts" / "stress_ml_reload_score.py").read_text(encoding="utf-8")
        self.assertIn("DEFAULT_BASE_URL", source)
        self.assertIn("SCORE_BACKPRESSURE_CODES", source)
        self.assertIn("RELOAD_BACKPRESSURE_CODES", source)
        self.assertIn("score_backpressure", source)
        self.assertIn("reload_backpressure", source)
        self.assertIn('"/score"', source)
        self.assertIn('"/reload"', source)
        self.assertIn("--self-test", source)
        self.assertIn("--score-interval-ms", source)
        self.assertIn("--score-error-backoff-ms", source)
        self.assertIn("--ready-timeout-sec", source)
        self.assertIn("--ready-poll-ms", source)
        self.assertIn("def _wait_for_health(", source)
        self.assertIn("\"ready_wait_sec\"", source)
        self.assertIn("\"status\": \"skipped\"", source)
        self.assertIn("ThreadingHTTPServer", source)
        self.assertIn("fail_on_error", source)
        self.assertIn("def do_GET(self)", source)
        self.assertIn('if self.path == "/health"', source)

    def test_ci_runs_reload_score_stress_harness_self_test(self) -> None:
        ci = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        self.assertIn("stress_ml_reload_score.py --self-test", ci)
        self.assertIn("--reload-interval-ms 80", ci)

    def test_ml_server_analog_engine_refresh_and_score_use_lock(self) -> None:
        source = (REPO_ROOT / "server" / "ml_server.py").read_text(encoding="utf-8")
        self.assertIn("self._lock = threading.RLock()", source)
        self.assertIn("ML_ANALOG_PREFILTER_ENABLED", source)
        self.assertIn("ML_ANALOG_PREFILTER_MAX_ROWS", source)
        self.assertIn("ML_ANALOG_PREFILTER_FEATURE_LIMIT", source)
        self.assertIn("def _prefilter_candidates(", source)
        refresh_block = source.split("def refresh(self) -> None:", 1)[1].split(
            "def health(self) -> dict[str, object]:",
            1,
        )[0]
        self.assertIn("with self._lock:", refresh_block)
        self.assertIn("self.rows_by_horizon = rows_by_horizon", refresh_block)
        score_block = source.split("def score_event(", 1)[1].split("registry = ModelRegistry()", 1)[0]
        self.assertIn("with self._lock:", score_block)
        self.assertIn("rows_by_horizon = self.rows_by_horizon", score_block)

    def test_ml_server_registry_load_and_score_use_snapshot(self) -> None:
        source = (REPO_ROOT / "server" / "ml_server.py").read_text(encoding="utf-8")
        registry_block = source.split("class ModelRegistry:", 1)[1].split(
            "def _analog_level_family(",
            1,
        )[0]
        self.assertIn("self._lock = threading.RLock()", registry_block)
        self.assertIn("ML_INFERENCE_N_JOBS", source)
        self.assertIn("def _set_inference_n_jobs(", registry_block)
        self.assertIn("def snapshot(self) -> dict[str, object]:", registry_block)
        load_block = registry_block.split("def load(self", 1)[1].split(
            "def snapshot(self) -> dict[str, object]:",
            1,
        )[0]
        self.assertIn("self.manifest_signature", load_block)
        self.assertIn("if not force:", load_block)
        self.assertIn("def is_manifest_unchanged(self) -> bool:", registry_block)
        self.assertIn("with self._lock:", load_block)
        self.assertIn(
            "ModelRegistry._set_inference_n_jobs(payload.get(\"pipeline\"), ML_INFERENCE_N_JOBS)",
            load_block,
        )
        self.assertIn(
            "ModelRegistry._set_inference_n_jobs(payload.get(\"calibrator\"), ML_INFERENCE_N_JOBS)",
            load_block,
        )
        self.assertIn("self.manifest = manifest", load_block)
        self.assertIn("self.models = models", load_block)
        self.assertIn("self.thresholds = thresholds", load_block)
        score_block = source.split("def _score_event(event: dict):", 1)[1].split(
            "def _validate_score_payload(",
            1,
        )[0]
        self.assertIn("registry_snapshot = registry.snapshot()", score_block)
        self.assertIn("snapshot_models = registry_snapshot.get(\"models\")", score_block)
        self.assertIn("snapshot_thresholds = registry_snapshot.get(\"thresholds\")", score_block)

    def test_ml_server_reload_endpoint_has_busy_and_cooldown_backpressure(self) -> None:
        source = (REPO_ROOT / "server" / "ml_server.py").read_text(encoding="utf-8")
        self.assertIn("ML_RELOAD_MIN_INTERVAL_SEC", source)
        self.assertIn("_RELOAD_LOCK = threading.Lock()", source)
        self.assertIn("_reload_state", source)
        reload_block = source.split("async def reload_models(force: bool = False):", 1)[1].split(
            "def _check_feature_drift(",
            1,
        )[0]
        self.assertIn("registry.is_manifest_unchanged()", reload_block)
        self.assertIn("await asyncio.to_thread(registry.load, force=force)", reload_block)
        self.assertIn("await asyncio.to_thread(analog_engine.refresh)", reload_block)
        self.assertIn("changed = await asyncio.to_thread(registry.load, force=force)", reload_block)
        self.assertIn("if changed:", reload_block)
        self.assertIn("\"status\": status", reload_block)
        self.assertIn("\"changed\": changed", reload_block)
        self.assertIn("if not _RELOAD_LOCK.acquire(blocking=False):", reload_block)
        self.assertIn("status_code=409", reload_block)
        self.assertIn("\"status\": \"busy\"", reload_block)
        self.assertIn("status_code=429", reload_block)
        self.assertIn("\"status\": \"cooldown\"", reload_block)
        self.assertIn("last_status=\"running\"", reload_block)
        self.assertIn("status = \"ok\" if changed else \"noop\"", reload_block)

    def test_ml_server_score_endpoint_has_concurrency_backpressure(self) -> None:
        source = (REPO_ROOT / "server" / "ml_server.py").read_text(encoding="utf-8")
        self.assertIn("ML_SCORE_MAX_IN_FLIGHT", source)
        self.assertIn("ML_SCORE_ANALOG_DISABLE_IN_FLIGHT", source)
        self.assertIn("ML_INFERENCE_N_JOBS", source)
        self.assertIn("\"inference_n_jobs\": ML_INFERENCE_N_JOBS", source)
        self.assertIn("PREDICTION_LOG_QUEUE_MAX_SIZE", source)
        self.assertIn("PREDICTION_LOG_ALERT_QUEUE_DEPTH", source)
        self.assertIn("PREDICTION_LOG_ALERT_WRITE_FAIL_TOTAL", source)
        self.assertIn("PREDICTION_LOG_ALERT_DROPPED_TOTAL", source)
        self.assertIn("PREDICTION_LOG_CONNECT_TIMEOUT_SEC", source)
        self.assertIn("PREDICTION_LOG_BUSY_TIMEOUT_MS", source)
        self.assertIn("PRAGMA busy_timeout", source)
        self.assertIn("_PREDICTION_LOG_QUEUE: queue.Queue", source)
        self.assertIn("def _enqueue_prediction(event: dict, result: dict) -> None:", source)
        self.assertIn("\"prediction_log\": prediction_log_state", source)
        self.assertIn("\"prediction_log_alerts\": prediction_log_alerts", source)
        self.assertIn("_SCORE_LOAD_SHED_LOCAL = threading.local()", source)
        self.assertIn("ANALOG_LOAD_SHED", source)
        self.assertIn("_SCORE_GATE = threading.BoundedSemaphore", source)
        self.assertIn("\"score\": _score_state_snapshot()", source)
        score_block = source.split("async def score(request: Request):", 1)[1].split(
            "def _score_events_batch(",
            1,
        )[0]
        self.assertIn("if not _try_begin_score_request():", score_block)
        self.assertIn("status_code=429", score_block)
        self.assertIn("Score concurrency limit reached.", score_block)
        self.assertIn(
            "await asyncio.to_thread(_score_single_event_with_log, event, disable_analogs)",
            score_block,
        )
        self.assertIn(
            "await asyncio.to_thread(_score_events_batch, events, disable_analogs)",
            score_block,
        )

    def test_ml_server_reload_runtime_busy_backpressure(self) -> None:
        ml_server = self._load_ml_server_module()

        class _BusyLock:
            def acquire(self, blocking=False):  # noqa: ARG002 - signature parity with threading.Lock
                return False

            def release(self):  # pragma: no cover - not used on busy branch
                return None

        original_lock = ml_server._RELOAD_LOCK
        try:
            ml_server._RELOAD_LOCK = _BusyLock()
            status, payload = self._asgi_json_request(ml_server.app, "POST", "/reload")
            self.assertEqual(status, 409)
            self.assertEqual(payload.get("status"), "busy")
            self.assertIn("Reload already in progress", payload.get("message", ""))
            self.assertIn("reload", payload)
        finally:
            ml_server._RELOAD_LOCK = original_lock

    def test_ml_server_reload_runtime_cooldown_backpressure(self) -> None:
        ml_server = self._load_ml_server_module()
        original_min_interval = ml_server.ML_RELOAD_MIN_INTERVAL_SEC
        original_reload_state = dict(ml_server._reload_state)
        try:
            ml_server.ML_RELOAD_MIN_INTERVAL_SEC = 60.0
            ml_server._reload_state.update(
                {
                    "last_started_at_ms": int(time.time() * 1000),
                    "cooldown_reject_count": 0,
                }
            )
            status, payload = self._asgi_json_request(ml_server.app, "POST", "/reload")
            self.assertEqual(status, 429)
            self.assertEqual(payload.get("status"), "cooldown")
            self.assertIn("too frequently", payload.get("message", "").lower())
            self.assertIn("reload", payload)
        finally:
            ml_server.ML_RELOAD_MIN_INTERVAL_SEC = original_min_interval
            ml_server._reload_state.clear()
            ml_server._reload_state.update(original_reload_state)

    def test_ml_server_score_runtime_busy_backpressure(self) -> None:
        ml_server = self._load_ml_server_module()
        original_snapshot = ml_server.registry.snapshot
        original_try_begin = ml_server._try_begin_score_request
        try:
            ml_server.registry.snapshot = lambda: {
                "manifest": {"version": "smoke-test"},
                "models": {"reject": {5: object()}, "break": {5: object()}},
            }
            ml_server._try_begin_score_request = lambda: False
            status, payload = self._asgi_json_request(
                ml_server.app,
                "POST",
                "/score",
                payload={"event": {"symbol": "SPY", "horizon_min": 5}},
            )
            self.assertEqual(status, 429)
            self.assertEqual(payload.get("status"), "busy")
            self.assertIn("Score concurrency limit reached", payload.get("message", ""))
            self.assertIn("score", payload)
        finally:
            ml_server.registry.snapshot = original_snapshot
            ml_server._try_begin_score_request = original_try_begin

    def test_ibkr_bridge_uses_timezone_aware_utc_datetimes(self) -> None:
        source = (REPO_ROOT / "server" / "ibkr_gamma_bridge.py").read_text(encoding="utf-8")
        self.assertNotIn("datetime.utcnow(", source)
        self.assertNotIn("datetime.utcfromtimestamp(", source)
        self.assertIn("datetime.now(timezone.utc)", source)
        self.assertIn("_utc_iso_z()", source)

    def test_ibkr_bridge_connection_guard_is_lock_protected(self) -> None:
        source = (REPO_ROOT / "server" / "ibkr_gamma_bridge.py").read_text(encoding="utf-8")
        self.assertIn("ib_lock = threading.RLock()", source)
        self.assertIn("def ensure_connected()", source)
        ensure_block = source.split("def ensure_connected()", 1)[1].split("def fetch_spot", 1)[0]
        self.assertIn("with ib_lock:", ensure_block)
        self.assertIn("if ib.isConnected()", ensure_block)
        self.assertIn("timeout=IB_CONNECT_TIMEOUT_SEC", ensure_block)
        self.assertIn("IBKR reconnect cooldown active", ensure_block)

    def test_ibkr_bridge_marketdata_cache_returns_copy(self) -> None:
        source = (REPO_ROOT / "server" / "ibkr_gamma_bridge.py").read_text(encoding="utf-8")
        self.assertIn("from copy import deepcopy", source)
        self.assertIn("return deepcopy(cached_entry[0])", source)
        self.assertIn("_mda_gamma_cache[cache_key] = (deepcopy(payload),", source)
        self.assertIn("return payload", source)

    def test_ibkr_bridge_marketdata_uses_stale_cache_on_upstream_error(self) -> None:
        source = (REPO_ROOT / "server" / "ibkr_gamma_bridge.py").read_text(encoding="utf-8")
        block = source.split("def fetch_gamma_marketdata(", 1)[1].split("class GammaHandler", 1)[0]
        self.assertIn("stale_payload = None", block)
        self.assertIn("if stale_payload is not None:", block)
        self.assertIn("stale_payload[\"cacheStale\"] = True", block)
        self.assertIn("stale_payload[\"cacheStaleReason\"] = str(exc)", block)
        self.assertIn("_mda_gamma_error_backoff_until", source)
        self.assertIn("MDA_GAMMA_ERROR_BACKOFF_SEC", source)
        self.assertIn("cooldown active", block)
        self.assertIn("backoff_sec = max(1, int(retry_after))", block)
        self.assertIn("parsedate_to_datetime", source)

    def test_ibkr_bridge_marketdata_cache_key_is_expiry_mode_scoped(self) -> None:
        source = (REPO_ROOT / "server" / "ibkr_gamma_bridge.py").read_text(encoding="utf-8")
        self.assertIn("_EXPIRY_MODE_DTE = {", source)
        self.assertIn('"front":     7', source)
        self.assertIn('"weekly":    7', source)
        self.assertIn('"monthly":  30', source)
        self.assertIn('"quarterly": 90', source)
        self.assertIn("dte_days = _EXPIRY_MODE_DTE.get(mode, MDA_GAMMA_DTE_DAYS)", source)
        self.assertIn('cache_key = f"{symbol.upper()}:{mode or \'default\'}"', source)
        self.assertIn("payload = fetch_gamma_marketdata(symbol, expiry_mode=expiry)", source)

    def test_ibkr_bridge_market_close_uses_new_york_timezone(self) -> None:
        bridge = load_module(
            "pq_ibkr_market_close_timezone_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        if getattr(bridge, "NY_TZ", None) is None:
            self.skipTest("zoneinfo unavailable")

        pre_close_utc = datetime(2026, 3, 10, 19, 59, tzinfo=timezone.utc)  # 15:59 ET
        close_utc = datetime(2026, 3, 10, 20, 0, tzinfo=timezone.utc)  # 16:00 ET
        weekend_utc = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc)  # Saturday

        self.assertFalse(bridge._is_market_session_closed(pre_close_utc))
        self.assertTrue(bridge._is_market_session_closed(close_utc))
        self.assertTrue(bridge._is_market_session_closed(weekend_utc))

    def test_alert_system_cross_and_xss_hardening_contract_present(self) -> None:
        source = (REPO_ROOT / "alert_system.js").read_text(encoding="utf-8")
        self.assertIn("this.previousObservedPrice", source)
        self.assertIn("this.lastObservedPrice", source)
        self.assertIn("checkAlert(alert, currentPrice, previousPrice)", source)
        self.assertIn("Number.isFinite(previousPrice)", source)
        self.assertIn("getPreviousPrice()", source)
        self.assertNotIn("return null;", source.split("getPreviousPrice()", 1)[1].split("}", 1)[0])
        self.assertIn("escapeHtml(value)", source)
        self.assertIn("replaceAll('<', '&lt;')", source)
        self.assertIn("updateAlertList()", source)
        self.assertIn("updateAlertHistory()", source)
        self.assertIn("this.escapeHtml(alert.type)", source)
        self.assertIn("this.escapeHtml(alert.asset)", source)

    def test_fdr_display_modules_guard_missing_global_engines(self) -> None:
        pivot_source = (REPO_ROOT / "pivot_fdr_integration.js").read_text(encoding="utf-8")
        enhanced_source = (REPO_ROOT / "enhanced_pivot_display.js").read_text(encoding="utf-8")

        self.assertIn("resolveFDRCorrectionEngine()", pivot_source)
        self.assertIn("window.FDRCorrection", pivot_source)
        self.assertIn("FDRCorrection engine unavailable", pivot_source)

        self.assertIn("resolveEnhancedFDRCorrectionEngine()", enhanced_source)
        self.assertIn("window.EnhancedFDRCorrection", enhanced_source)
        self.assertIn("buildFallbackFdrResults(", enhanced_source)
        self.assertIn("EnhancedFDRCorrection engine unavailable", enhanced_source)
        self.assertIn("fdrEngine.generateTooltip", enhanced_source)

    def test_dashboard_market_loader_cancels_stale_requests(self) -> None:
        dashboard_source = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        self.assertIn("marketLoadAbortController", dashboard_source)
        self.assertIn("state.marketLoadAbortController.abort()", dashboard_source)
        self.assertIn(
            "const requestOptions = marketLoadAbortController ? { signal: marketLoadAbortController.signal } : {};",
            dashboard_source,
        )
        self.assertIn("async function fetchJson(url, timeoutMs = 10000, options = {})", dashboard_source)
        self.assertIn("AbortSignal.any", dashboard_source)
        self.assertIn("fetchIbkrMarket(symbol, interval, range, requestOptions)", dashboard_source)
        self.assertIn("fetchDashboardMarket(symbol, interval, range, requestOptions)", dashboard_source)
        self.assertIn("fetchYahooWithBackoff(symbol, range, interval, 4, requestOptions)", dashboard_source)
        self.assertIn("fetchPersistedDailyCandles(symbol, requestOptions)", dashboard_source)
        self.assertIn("fetchVixLevel(interval, range, requestOptions)", dashboard_source)
        self.assertRegex(
            dashboard_source,
            r"fetchGammaData\(symbol,\s*(?:state\.gammaExpiry,\s*)?requestOptions\)",
        )
        self.assertIn("if (isStaleRequest() || isAbortError(error))", dashboard_source)

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
        self.assertIn("RETRAIN_REQUIRED_MODULES=", retrain_script)
        self.assertIn("RUN_OPS_SMOKE_ON_RETRAIN", retrain_script)
        self.assertIn("RETRAIN_REQUIRED_MODULES+=(fastapi ib_insync uvicorn)", retrain_script)
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
        self.assertIn("capture_ops_smoke_failure_details", retrain_script)
        self.assertIn("build_ops_smoke_alert_body", retrain_script)
        self.assertIn("summary=", retrain_script)
        self.assertIn("hint=", retrain_script)

    def test_runtime_requirements_contract_present(self) -> None:
        runtime_reqs = (REPO_ROOT / "requirements-runtime.txt").read_text(encoding="utf-8")
        smoke_reqs = (REPO_ROOT / ".github" / "ci" / "requirements-smoke.txt").read_text(encoding="utf-8")
        self.assertIn("duckdb==", runtime_reqs)
        self.assertIn("fastapi==", runtime_reqs)
        self.assertIn("ib-insync==", runtime_reqs)
        self.assertIn("joblib==", runtime_reqs)
        self.assertIn("numpy==", runtime_reqs)
        self.assertIn("pandas==", runtime_reqs)
        self.assertIn("scikit-learn==", runtime_reqs)
        self.assertIn("uvicorn==", runtime_reqs)
        self.assertIn("-r ../../requirements-runtime.txt", smoke_reqs)

    def test_run_persistent_stack_sources_dotenv_safely(self) -> None:
        stack_script = (REPO_ROOT / "server" / "run_persistent_stack.sh").read_text(encoding="utf-8")
        self.assertIn('ENV_FILE="${ROOT_DIR}/.env"', stack_script)
        self.assertIn("load_env_file()", stack_script)
        self.assertIn('load_env_file "${ENV_FILE}"', stack_script)
        self.assertNotIn('source "${ROOT_DIR}/.env"', stack_script)
        self.assertIn('if [[ "${#value}" -ge 2 ]]; then', stack_script)
        self.assertIn('value="${value:1:${#value}-2}"', stack_script)
        self.assertIn('DASH_AUTH_ENFORCE_STRONG_PASSWORD', stack_script)
        self.assertIn('DASH_AUTH_LOCAL_BYPASS=true is not allowed when HOST is non-loopback', stack_script)
        self.assertIn('DASH_AUTH_PASSWORD length', stack_script)
        self.assertNotIn("${value,,}", stack_script)
        self.assertNotIn("${host_value,,}", stack_script)

        proc = run_cmd(["bash", "-n", "server/run_persistent_stack.sh"], cwd=REPO_ROOT)
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_train_rf_threshold_tuning_uses_calibration_fold(self) -> None:
        source = (REPO_ROOT / "scripts" / "train_rf.py").read_text(encoding="utf-8")
        self.assertIn("find_optimal_threshold(y_calib, calib_probs[:, 1])", source)
        self.assertIn("optimal_threshold=optimal_threshold", source)
        self.assertNotIn("metrics_for_fold(y_test, y_prob, y_pred, optimal_threshold=None)", source)

    def test_run_all_health_probe_retry_contract_present(self) -> None:
        run_all = (REPO_ROOT / "server" / "run_all.sh").read_text(encoding="utf-8")
        env_example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
        self.assertIn("MONITOR_HEALTH_TIMEOUT_SEC", run_all)
        self.assertIn("MONITOR_HEALTH_RETRIES", run_all)
        self.assertIn("MONITOR_HEALTH_RETRY_SLEEP_SEC", run_all)
        self.assertIn("MONITOR_ML_HEALTH_TIMEOUT_SEC", run_all)
        self.assertIn("MONITOR_ML_CONSECUTIVE_FAIL_LIMIT", run_all)
        self.assertIn("MONITOR_ML_FATAL", run_all)
        self.assertIn("MONITOR_EVENT_WRITER_FATAL", run_all)
        self.assertIn("MONITOR_DASHBOARD_FATAL", run_all)
        self.assertIn("MONITOR_LIVE_COLLECTOR_CONSECUTIVE_FAIL_LIMIT", run_all)
        self.assertIn("MONITOR_LIVE_COLLECTOR_FATAL", run_all)
        self.assertIn("event_writer fail limit reached; continuing", run_all)
        self.assertIn("ml_server fail limit reached; continuing", run_all)
        self.assertIn("dashboard fail limit reached; continuing", run_all)
        self.assertIn("live_collector fail limit reached; continuing", run_all)
        self.assertIn('quick_check_service "dashboard" "3000" "http://127.0.0.1:3000/health"', run_all)
        self.assertIn('verify_service "dashboard" "3000" "http://127.0.0.1:3000/health"', run_all)
        self.assertIn('health failed after ${max_attempts} attempts', run_all)
        self.assertIn("MONITOR_EVENT_WRITER_FATAL=false", env_example)
        self.assertIn("MONITOR_DASHBOARD_FATAL=false", env_example)
        self.assertIn("MONITOR_LIVE_COLLECTOR_CONSECUTIVE_FAIL_LIMIT=3", env_example)
        self.assertIn("MONITOR_LIVE_COLLECTOR_FATAL=false", env_example)

        proc = run_cmd(["bash", "-n", "server/run_all.sh"], cwd=REPO_ROOT)
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_health_alert_watchdog_latency_regression_contract_present(self) -> None:
        watchdog = (REPO_ROOT / "scripts" / "health_alert_watchdog.py").read_text(encoding="utf-8")
        env_example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
        self.assertIn("ML_ALERT_CONSECUTIVE_FAILS", watchdog)
        self.assertIn("service_consecutive_fails", watchdog)
        self.assertIn("down_streak", watchdog)
        self.assertIn('result["status"] = f"{base_status}_pending"', watchdog)
        self.assertIn("ML_ALERT_ML_SCORE_LAST_DURATION_MAX_MS", watchdog)
        self.assertIn("ML_ALERT_ML_SCORE_MIN_SUCCESS_COUNT", watchdog)
        self.assertIn("ML_ALERT_ML_SCORE_CONSECUTIVE_FAILS", watchdog)
        self.assertIn("score_last_duration_ms", watchdog)
        self.assertIn("score_latency_breached", watchdog)
        self.assertIn("ml_score_latency_streak", watchdog)
        self.assertIn("latency_regressed", watchdog)
        self.assertIn("ML_ALERT_COLLECTOR_UNSCORED_GUARD", watchdog)
        self.assertIn("ML_ALERT_COLLECTOR_UNSCORED_MAX", watchdog)
        self.assertIn("ML_ALERT_COLLECTOR_UNSCORED_LOOKBACK_MIN", watchdog)
        self.assertIn("ML_ALERT_COLLECTOR_UNSCORED_CONSECUTIVE_FAILS", watchdog)
        self.assertIn("ML_ALERT_COLLECTOR_UNSCORED_MARKET_HOURS_ONLY", watchdog)
        self.assertIn("count_unscored_live_events", watchdog)
        self.assertIn("scoring_lagging", watchdog)
        self.assertIn("collector_unscored_streak", watchdog)
        self.assertIn("ML_ALERT_CONSECUTIVE_FAILS=2", env_example)
        self.assertIn("ML_ALERT_ML_SCORE_LAST_DURATION_MAX_MS", env_example)
        self.assertIn("ML_ALERT_ML_SCORE_MIN_SUCCESS_COUNT", env_example)
        self.assertIn("ML_ALERT_ML_SCORE_CONSECUTIVE_FAILS", env_example)
        self.assertIn("ML_ALERT_COLLECTOR_UNSCORED_GUARD=true", env_example)
        self.assertIn("ML_ALERT_COLLECTOR_UNSCORED_MAX=0", env_example)
        self.assertIn("ML_ALERT_COLLECTOR_UNSCORED_LOOKBACK_MIN=120", env_example)
        self.assertIn("ML_ALERT_COLLECTOR_UNSCORED_CONSECUTIVE_FAILS=3", env_example)
        self.assertIn("ML_ALERT_COLLECTOR_UNSCORED_MARKET_HOURS_ONLY=true", env_example)

        proc = run_cmd([PYTHON, "-m", "py_compile", "scripts/health_alert_watchdog.py"], cwd=REPO_ROOT)
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

        # Break-only opportunities must still select a break horizon (not no_edge).
        set_registry(
            reject_probs={5: 0.10, 15: 0.10, 30: 0.10, 60: 0.10},
            break_probs={5: 0.20, 15: 0.20, 30: 0.20, 60: 0.80},
        )
        result = ml_server._score_event({"event_id": "break_only_case"})
        self.assertEqual(result["signals"].get("signal_60m"), "break")
        self.assertEqual(result["best_horizon"], 60)
        self.assertFalse(result["abstain"])

        # Stronger break edge should outrank a weaker reject edge.
        set_registry(
            reject_probs={5: 0.56, 15: 0.10, 30: 0.10, 60: 0.10},
            break_probs={5: 0.10, 15: 0.10, 30: 0.10, 60: 0.90},
        )
        result = ml_server._score_event({"event_id": "break_stronger_than_reject"})
        self.assertEqual(result["signals"].get("signal_5m"), "reject")
        self.assertEqual(result["signals"].get("signal_60m"), "break")
        self.assertEqual(result["best_horizon"], 60)
        self.assertFalse(result["abstain"])

    def test_ml_server_no_edge_expectancy_prefers_target_specific_other_stats(self) -> None:
        ml_server = load_module("ml_server_no_edge_expectancy_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        ml_server.build_feature_row = lambda _event: {"x": 1.0}
        ml_server.collect_missing = lambda _features: []
        ml_server.ML_SHADOW_HORIZONS = set()

        ml_server.registry.models = {
            "reject": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.1),
                    "calibration": "sigmoid",
                }
            },
            "break": {
                5: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.1),
                    "calibration": "sigmoid",
                }
            },
        }
        ml_server.registry.thresholds = {"reject": {5: 0.5}, "break": {5: 0.5}}
        ml_server.registry.manifest = {
            "version": "vtest",
            "trained_end_ts": int(time.time() * 1000),
            "stats": {
                "5": {
                    "reject": {
                        "mfe_bps_reject_other": 11.0,
                        "mae_bps_reject_other": -7.0,
                        # Legacy unscoped aliases intentionally disagree.
                        "mfe_bps_other": 999.0,
                        "mae_bps_other": -999.0,
                    },
                    "break": {},
                }
            },
        }

        result = ml_server._score_event({"event_id": "no_edge_specific_other"})
        self.assertEqual(result["signals"].get("signal_5m"), "no_edge")
        self.assertAlmostEqual(float(result["scores"]["exp_mfe_bps_5m"]), 11.0, places=6)
        self.assertAlmostEqual(float(result["scores"]["exp_mae_bps_5m"]), -7.0, places=6)

        # Backward compatibility: legacy manifests without target-specific keys still work.
        ml_server.registry.manifest["stats"]["5"]["reject"] = {
            "mfe_bps_other": 13.0,
            "mae_bps_other": -8.0,
        }
        result = ml_server._score_event({"event_id": "no_edge_legacy_other"})
        self.assertEqual(result["signals"].get("signal_5m"), "no_edge")
        self.assertAlmostEqual(float(result["scores"]["exp_mfe_bps_5m"]), 13.0, places=6)
        self.assertAlmostEqual(float(result["scores"]["exp_mae_bps_5m"]), -8.0, places=6)

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

    def test_analog_prefilter_caps_candidate_pool(self) -> None:
        ml_server = load_module("ml_server_analog_prefilter_contract", REPO_ROOT / "server" / "ml_server.py")
        ml_server.ML_ANALOG_PREFILTER_ENABLED = True
        ml_server.ML_ANALOG_PREFILTER_MAX_ROWS = 3
        ml_server.ML_ANALOG_PREFILTER_FEATURE_LIMIT = 2
        ml_server.ML_ANALOG_FEATURE_WEIGHTS = {
            "distance_bps": 1.0,
            "distance_atr_ratio": 1.0,
            "vwap_side": 0.0,
            "ema_stack": 0.0,
            "rv_30": 0.0,
            "or_size_atr": 0.0,
            "overnight_gap_atr": 0.0,
        }
        rows = [
            {"event_id": f"pref_{idx}", "distance_bps": float(idx), "distance_atr_ratio": float(idx)}
            for idx in range(10)
        ]
        query_features = {"distance_bps": 0.0, "distance_atr_ratio": 0.0}
        feature_names = ["distance_bps", "distance_atr_ratio"]
        feature_stats = {
            "distance_bps": (0.0, 1.0),
            "distance_atr_ratio": (0.0, 1.0),
        }

        filtered = ml_server.AnalogEngine._prefilter_candidates(
            rows,
            query_features=query_features,
            feature_names=feature_names,
            feature_stats=feature_stats,
        )
        self.assertEqual(len(filtered), 3)
        ids = {row["event_id"] for row in filtered}
        self.assertIn("pref_0", ids)
        self.assertIn("pref_1", ids)
        self.assertIn("pref_2", ids)

    def test_analog_blend_weight_reaches_configured_max(self) -> None:
        ml_server = load_module("ml_server_blend_weight_contract", REPO_ROOT / "server" / "ml_server.py")
        ml_server.ML_ANALOG_BLEND_WEIGHT_BASE = 0.30
        ml_server.ML_ANALOG_BLEND_WEIGHT_MAX = 0.60
        ml_server.ML_ANALOG_BLEND_N_EFF_REF = 10.0
        ml_server.ML_ANALOG_MAX_CI_WIDTH = 0.20

        # Max confidence should reach configured max weight.
        weight_max = ml_server._compute_analog_blend_weight(n_eff=10.0, ci_width=0.0)
        self.assertAlmostEqual(float(weight_max), 0.60, places=6)

        # Mid confidence should interpolate between base and max.
        # eff_scale=0.5, ci_scale=0.5 -> blend_scale=0.25 => 0.30 + (0.30*0.25)=0.375
        weight_mid = ml_server._compute_analog_blend_weight(n_eff=5.0, ci_width=0.10)
        self.assertAlmostEqual(float(weight_mid), 0.375, places=6)

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

    def test_ml_prediction_log_reuses_thread_local_connection(self) -> None:
        db = self.tmp / "predlog_conn_reuse.sqlite"
        prev_db = os.environ.get("PREDICTION_LOG_DB")
        os.environ["PREDICTION_LOG_DB"] = str(db)
        try:
            ml_server = load_module("ml_server_predlog_conn_reuse_runtime", REPO_ROOT / "server" / "ml_server.py")
        finally:
            if prev_db is None:
                os.environ.pop("PREDICTION_LOG_DB", None)
            else:
                os.environ["PREDICTION_LOG_DB"] = prev_db

        conn1 = ml_server._get_prediction_log_conn()
        conn2 = ml_server._get_prediction_log_conn()
        self.assertIs(conn1, conn2)

        event = {"event_id": "evt_conn_reuse"}
        result = {
            "model_version": "vreuse",
            "feature_version": "v3",
            "best_horizon": 5,
            "abstain": False,
            "scores": {},
            "signals": {},
            "thresholds": {},
            "quality_flags": [],
        }
        ml_server._log_prediction(event, result)
        cached = getattr(ml_server._PREDICTION_LOG_LOCAL, "conn", None)
        self.assertIs(cached, conn1)

        try:
            conn1.close()
        except Exception:
            pass
        ml_server._PREDICTION_LOG_LOCAL.conn = None

    def test_ml_prediction_log_registers_atexit_cleanup(self) -> None:
        source = (REPO_ROOT / "server" / "ml_server.py").read_text(encoding="utf-8")
        self.assertIn("atexit.register(_close_prediction_log_conn)", source)
        self.assertIn("atexit.register(_stop_prediction_log_writer)", source)

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

    def test_report_blend_weight_reaches_configured_max(self) -> None:
        report = load_module(
            "pq_daily_report_blend_weight_contract",
            REPO_ROOT / "scripts" / "generate_daily_ml_report.py",
        )
        report.ANALOG_BLEND_WEIGHT_BASE = 0.30
        report.ANALOG_BLEND_WEIGHT_MAX = 0.60
        report.ANALOG_BLEND_N_EFF_REF = 10.0
        report.ANALOG_BLEND_CI_WIDTH_REF = 0.20

        weight_max = report._compute_report_blend_weight(10.0, 0.0)
        self.assertAlmostEqual(float(weight_max), 0.60, places=6)

        weight_mid = report._compute_report_blend_weight(5.0, 0.10)
        self.assertAlmostEqual(float(weight_mid), 0.375, places=6)

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
        self.assertIn("## Prediction Lag Profile (First Live Prediction)", text)
        self.assertIn("- <=1h: 2", text)
        self.assertIn("- >6h: 0", text)
        self.assertIn("- No prediction: 0", text)
        self.assertIn("| 2026-03-06 | 1 | 1 | 0 | 0 | 0 | 0 |", text)
        self.assertIn("## What-if Policy Comparison (Baseline vs Guardrail vs No-5m)", text)
        self.assertIn("## Runtime Applied Policy Summary", text)
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
        self.assertIn("## Prediction Lag Profile (First Live Prediction)", text)
        self.assertIn("- >6h: 1", text)
        self.assertIn("- No prediction: 0", text)
        self.assertIn("| 2026-03-06 | 1 | 0 | 0 | 0 | 1 | 0 |", text)

    def test_train_artifacts_horizon_stats_use_target_specific_other_bucket(self) -> None:
        module = load_module("train_rf_artifacts_stats", REPO_ROOT / "scripts" / "train_rf_artifacts.py")
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:
            self.skipTest(f"pandas unavailable: {exc}")

        df = pd.DataFrame(
            [
                {"horizon_min": 5, "reject": 1, "break": 0, "mfe_bps": 10.0, "mae_bps": -2.0},
                {"horizon_min": 5, "reject": 0, "break": 1, "mfe_bps": 30.0, "mae_bps": -20.0},
                {"horizon_min": 5, "reject": 0, "break": 0, "mfe_bps": 40.0, "mae_bps": -30.0},
            ]
        )

        reject_stats = module.compute_horizon_stats(df, "reject", 5)
        break_stats = module.compute_horizon_stats(df, "break", 5)

        # Reject "other" must come from reject==0 rows only (30, 40), not break==0 rows.
        self.assertAlmostEqual(float(reject_stats["mfe_bps_reject_other"]), 35.0, places=6)
        self.assertAlmostEqual(float(reject_stats["mae_bps_reject_other"]), -25.0, places=6)

        # Break "other" comes from break==0 rows (10, 40).
        self.assertAlmostEqual(float(break_stats["mfe_bps_break_other"]), 25.0, places=6)
        self.assertAlmostEqual(float(break_stats["mae_bps_break_other"]), -16.0, places=6)
        self.assertNotIn("mfe_bps_other", reject_stats)
        self.assertNotIn("mae_bps_other", reject_stats)
        self.assertNotIn("mfe_bps_other", break_stats)
        self.assertNotIn("mae_bps_other", break_stats)

    def test_train_artifacts_horizon_stats_emit_by_regime(self) -> None:
        module = load_module("train_rf_artifacts_stats_regime", REPO_ROOT / "scripts" / "train_rf_artifacts.py")
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:
            self.skipTest(f"pandas unavailable: {exc}")

        df = pd.DataFrame(
            [
                {
                    "horizon_min": 5,
                    "reject": 0,
                    "break": 1,
                    "mfe_bps": 10.0,
                    "mae_bps": -4.0,
                    "regime_type": 1,
                },
                {
                    "horizon_min": 5,
                    "reject": 0,
                    "break": 0,
                    "mfe_bps": 8.0,
                    "mae_bps": -3.0,
                    "regime_type": 1,
                },
                {
                    "horizon_min": 5,
                    "reject": 0,
                    "break": 1,
                    "mfe_bps": 6.0,
                    "mae_bps": -6.0,
                    "regime_type": 3,
                },
                {
                    "horizon_min": 5,
                    "reject": 0,
                    "break": 0,
                    "mfe_bps": 4.0,
                    "mae_bps": -5.0,
                    "regime_type": 3,
                },
            ]
        )

        stats = module.compute_horizon_stats(df, "break", 5)
        by_regime = stats.get("by_regime")
        self.assertIsInstance(by_regime, dict)
        self.assertIn("compression", by_regime)
        self.assertIn("expansion", by_regime)

        compression = by_regime["compression"]
        self.assertEqual(int(compression["sample_size"]), 2)
        self.assertEqual(int(compression["break_count"]), 1)
        self.assertAlmostEqual(float(compression["mfe_bps_break"]), 6.0, places=6)
        self.assertAlmostEqual(float(compression["mfe_bps_break_other"]), 4.0, places=6)
        self.assertAlmostEqual(float(compression["sample_share"]), 0.5, places=6)

        expansion = by_regime["expansion"]
        self.assertEqual(int(expansion["sample_size"]), 2)
        self.assertEqual(int(expansion["break_count"]), 1)
        self.assertAlmostEqual(float(expansion["mfe_bps_break"]), 10.0, places=6)
        self.assertAlmostEqual(float(expansion["mfe_bps_break_other"]), 8.0, places=6)
        self.assertAlmostEqual(float(expansion["sample_share"]), 0.5, places=6)

    def test_train_artifacts_threshold_guard_disables_nonpositive_utility(self) -> None:
        module = load_module(
            "train_rf_artifacts_threshold_guard_negative",
            REPO_ROOT / "scripts" / "train_rf_artifacts.py",
        )
        threshold, meta = module.apply_threshold_risk_guards(
            objective="utility_bps",
            threshold=0.63,
            threshold_meta={"score": -12.5, "fallback": False},
            no_trade_threshold=1.0,
            min_utility_score=0.0,
            disable_on_nonpositive_utility=True,
            disable_on_fallback=True,
        )
        self.assertEqual(float(threshold), 1.0)
        self.assertTrue(bool(meta.get("guard_applied")))
        self.assertIn("non_positive_utility", str(meta.get("guard_reason")))
        self.assertTrue(bool(meta.get("fallback")))

    def test_threshold_selector_prefers_utility_above_floor_before_stability(self) -> None:
        module = load_module(
            "thresholds_prefer_utility_floor",
            REPO_ROOT / "ml" / "thresholds.py",
        )
        y_true = np.asarray([1, 1, 1, 1], dtype=int)
        y_prob = np.asarray([0.95, 0.85, 0.75, 0.65], dtype=float)
        utility = np.asarray([20.0, -21.0, 21.0, -120.0], dtype=float)

        # Without floor preference, stability-first ranking can pick a slightly
        # negative candidate even when positive alternatives exist.
        baseline = module.select_threshold(
            y_true,
            y_prob,
            objective="utility_bps",
            precision_floor=0.0,
            min_signals=1,
            default_threshold=0.5,
            utility_per_signal=utility,
            stability_band=0.11,
            top_k=5,
        )
        self.assertAlmostEqual(float(baseline.threshold), 0.85, places=9)
        self.assertLessEqual(float(baseline.score), 0.0)

        preferred = module.select_threshold(
            y_true,
            y_prob,
            objective="utility_bps",
            precision_floor=0.0,
            min_signals=1,
            default_threshold=0.5,
            utility_per_signal=utility,
            stability_band=0.11,
            top_k=5,
            preferred_min_score=0.0,
        )
        self.assertAlmostEqual(float(preferred.threshold), 0.95, places=9)
        self.assertGreater(float(preferred.score), 0.0)

    def test_threshold_selector_uses_utility_tiebreak_within_preferred_floor(self) -> None:
        module = load_module(
            "thresholds_prefer_highest_utility_among_preferred",
            REPO_ROOT / "ml" / "thresholds.py",
        )
        y_true = np.asarray([1, 1, 1, 1, 1, 1], dtype=int)
        y_prob = np.asarray([0.99, 0.98, 0.97, 0.96, 0.95, 0.94], dtype=float)
        utility = np.asarray([12.0, -1.0, 12.0, -1.0, 12.0, -40.0], dtype=float)

        selection = module.select_threshold(
            y_true,
            y_prob,
            objective="utility_bps",
            precision_floor=0.0,
            min_signals=1,
            default_threshold=0.5,
            utility_per_signal=utility,
            stability_band=0.011,
            top_k=6,
            preferred_min_score=0.0,
        )
        # Both 0.97 and 0.95 are above the preferred floor, but 0.95 has higher
        # utility score while 0.97 has a slightly higher stability score.
        self.assertAlmostEqual(float(selection.threshold), 0.95, places=9)
        self.assertAlmostEqual(float(selection.score), 34.0, places=9)

    def test_train_artifacts_threshold_guard_disables_fallback_threshold(self) -> None:
        module = load_module(
            "train_rf_artifacts_threshold_guard_fallback",
            REPO_ROOT / "scripts" / "train_rf_artifacts.py",
        )
        threshold, meta = module.apply_threshold_risk_guards(
            objective="utility_bps",
            threshold=0.5,
            threshold_meta={"score": None, "fallback": True},
            no_trade_threshold=0.99,
            min_utility_score=0.0,
            disable_on_nonpositive_utility=True,
            disable_on_fallback=True,
        )
        self.assertEqual(float(threshold), 0.99)
        self.assertTrue(bool(meta.get("guard_applied")))
        self.assertIn("fallback_threshold", str(meta.get("guard_reason")))

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

    def test_model_governance_regime_aware_waives_aggregate_mfe_regression(self) -> None:
        module = load_module("model_governance_regime_waive", REPO_ROOT / "scripts" / "model_governance.py")
        gates = module.GateConfig(
            required_targets=["break"],
            required_horizons=[5],
            min_trained_end_delta_ms=0,
            max_mfe_regression_bps=1.5,
            max_mae_worsening_bps=2.0,
            min_total_samples=0,
            min_positive_samples_reject=0,
            min_positive_samples_break=0,
            allow_feature_version_change=False,
            regime_aware=True,
            regime_buckets=["compression", "expansion"],
            regime_min_total_samples=20,
            regime_min_positive_samples_break=10,
            regime_min_compared_buckets=2,
        )
        active = {
            "feature_version": "v3",
            "trained_end_ts": 1000,
            "stats": {
                "5": {
                    "break": {
                        "sample_size": 200,
                        "break_count": 80,
                        "mfe_bps_break": 8.0,
                        "mae_bps_break": -20.0,
                        "by_regime": {
                            "compression": {
                                "sample_size": 160,
                                "break_count": 64,
                                "mfe_bps_break": 9.0,
                                "mae_bps_break": -19.0,
                            },
                            "expansion": {
                                "sample_size": 40,
                                "break_count": 16,
                                "mfe_bps_break": 6.0,
                                "mae_bps_break": -24.0,
                            },
                        },
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
                        "sample_size": 200,
                        "break_count": 80,
                        "mfe_bps_break": 6.0,
                        "mae_bps_break": -20.0,
                        "by_regime": {
                            "compression": {
                                "sample_size": 80,
                                "break_count": 32,
                                "mfe_bps_break": 9.2,
                                "mae_bps_break": -18.5,
                            },
                            "expansion": {
                                "sample_size": 120,
                                "break_count": 48,
                                "mfe_bps_break": 6.3,
                                "mae_bps_break": -23.0,
                            },
                        },
                    }
                }
            },
        }
        failures, skips = module.evaluate_gates(active, candidate, gates)
        self.assertEqual(failures, [])
        self.assertTrue(
            any("break:5m mfe_bps_break aggregate regressed waived by regime-aware check" in item for item in skips)
        )

    def test_model_governance_regime_aware_blocks_bucket_mfe_regression(self) -> None:
        module = load_module("model_governance_regime_fail", REPO_ROOT / "scripts" / "model_governance.py")
        gates = module.GateConfig(
            required_targets=["break"],
            required_horizons=[5],
            min_trained_end_delta_ms=0,
            max_mfe_regression_bps=1.5,
            max_mae_worsening_bps=2.0,
            min_total_samples=0,
            min_positive_samples_reject=0,
            min_positive_samples_break=0,
            allow_feature_version_change=False,
            regime_aware=True,
            regime_buckets=["compression", "expansion"],
            regime_min_total_samples=20,
            regime_min_positive_samples_break=10,
            regime_min_compared_buckets=2,
        )
        active = {
            "feature_version": "v3",
            "trained_end_ts": 1000,
            "stats": {
                "5": {
                    "break": {
                        "sample_size": 200,
                        "break_count": 80,
                        "mfe_bps_break": 8.0,
                        "mae_bps_break": -20.0,
                        "by_regime": {
                            "compression": {
                                "sample_size": 160,
                                "break_count": 64,
                                "mfe_bps_break": 9.0,
                                "mae_bps_break": -19.0,
                            },
                            "expansion": {
                                "sample_size": 40,
                                "break_count": 16,
                                "mfe_bps_break": 6.0,
                                "mae_bps_break": -24.0,
                            },
                        },
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
                        "sample_size": 200,
                        "break_count": 80,
                        "mfe_bps_break": 6.0,
                        "mae_bps_break": -20.0,
                        "by_regime": {
                            "compression": {
                                "sample_size": 80,
                                "break_count": 32,
                                "mfe_bps_break": 6.8,
                                "mae_bps_break": -18.5,
                            },
                            "expansion": {
                                "sample_size": 120,
                                "break_count": 48,
                                "mfe_bps_break": 6.3,
                                "mae_bps_break": -23.0,
                            },
                        },
                    }
                }
            },
        }
        failures, skips = module.evaluate_gates(active, candidate, gates)
        self.assertTrue(any("break:5m mfe_bps_break regressed in compression" in item for item in failures))
        self.assertEqual(skips, [])

    def test_model_governance_regime_aware_waives_gate_when_active_has_no_regime_data(self) -> None:
        """Regime-aware gate must be waived (not fail) when active manifest predates
        by_regime stats — prevents legacy models from permanently blocking promotion."""
        module = load_module(
            "model_governance_bootstrap_waive",
            REPO_ROOT / "scripts" / "model_governance.py",
        )
        gates = module.GateConfig(
            required_targets=["break"],
            required_horizons=[5],
            min_trained_end_delta_ms=0,
            max_mfe_regression_bps=1.5,
            max_mae_worsening_bps=2.0,
            min_total_samples=0,
            min_positive_samples_reject=0,
            min_positive_samples_break=0,
            allow_feature_version_change=False,
            regime_aware=True,
            regime_buckets=["compression", "expansion"],
            regime_min_compared_buckets=1,
        )
        # Active has no by_regime (pre-dates the feature)
        active = {
            "feature_version": "v3",
            "trained_end_ts": 1000,
            "stats": {
                "5": {
                    "break": {
                        "sample_size": 200,
                        "break_count": 80,
                        "mfe_bps_break": 8.0,
                        "mae_bps_break": -27.6,
                    }
                }
            },
        }
        # Candidate has full by_regime data
        candidate = {
            "feature_version": "v3",
            "trained_end_ts": 2000,
            "stats": {
                "5": {
                    "break": {
                        "sample_size": 200,
                        "break_count": 80,
                        "mfe_bps_break": 6.0,
                        "mae_bps_break": -30.9,
                        "by_regime": {
                            "compression": {
                                "sample_size": 120,
                                "break_count": 48,
                                "mfe_bps_break": 8.5,
                                "mae_bps_break": -24.6,
                            },
                            "expansion": {
                                "sample_size": 80,
                                "break_count": 32,
                                "mfe_bps_break": 5.2,
                                "mae_bps_break": -33.8,
                            },
                        },
                    }
                }
            },
        }
        failures, skips = module.evaluate_gates(active, candidate, gates)
        # Gate must be waived (no failures) with an explanatory skip message
        self.assertEqual(failures, [], "bootstrap waive: should have no failures")
        self.assertTrue(
            any("active_no_regime_data" in item for item in skips),
            f"expected active_no_regime_data skip, got: {skips}",
        )

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
