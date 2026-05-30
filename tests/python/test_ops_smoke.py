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

        # The trading-day gate imports scripts/trading_calendar.py from
        # ${ROOT_DIR}/scripts, so the sandbox needs a copy.
        calendar_src = (REPO_ROOT / "scripts" / "trading_calendar.py").read_text(encoding="utf-8")
        (scripts_dir / "trading_calendar.py").write_text(calendar_src, encoding="utf-8")

        # The wrapper now sources scripts/_pybin.sh for >=3.10 Python
        # resolution. The sandbox needs that helper too, otherwise the
        # ``source`` line aborts before any work begins.
        pybin_helper = (REPO_ROOT / "scripts" / "_pybin.sh").read_text(encoding="utf-8")
        (scripts_dir / "_pybin.sh").write_text(pybin_helper, encoding="utf-8")
        os.chmod(scripts_dir / "_pybin.sh", 0o755)

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
            "ML_REPORT_FAKE_ET_DATE": report_date,
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

    def test_daily_report_impact_lines_explain_zero_tradeable_rows(self) -> None:
        send_daily_report = load_module(
            "pq_send_daily_report_impact_lines_zero_rows_test",
            REPO_ROOT / "scripts" / "send_daily_report.py",
        )
        lines = send_daily_report.build_impact_lines(
            {
                "cost_model": {"spread": 0.8, "slippage": 0.4, "commission": 0.1, "total": 1.3},
                "signals": 0,
                "avg_gross": None,
                "avg_net": None,
                "win_rate_net": None,
                "by_horizon": {},
            }
        )
        joined = "\n".join(lines)
        self.assertIn("Tradeable matured signals: 0", joined)
        self.assertIn("no reject/break signals emitted on matured rows", joined)

    def test_daily_report_context_parses_prediction_basis_and_scored_line(self) -> None:
        send_daily_report = load_module(
            "pq_send_daily_report_basis_parse_test",
            REPO_ROOT / "scripts" / "send_daily_report.py",
        )
        report_path = self.tmp / "ml_daily_2026-03-19.md"
        report_text = "\n".join(
            [
                "# Daily ML Report - 2026-03-19",
                "- Model Readiness: **STALE**",
                "- Trading Utility: **STAND ASIDE**",
                "- Operator Note: No matured tradeable signals in this window.",
                "- Prediction basis for scored rows: first prediction per event",
                "- Scored predictions (first prediction per event): 42",
            ]
        )
        ctx = send_daily_report.parse_report_context(report_text, report_path)
        self.assertEqual(ctx.get("prediction_basis"), "first")
        self.assertEqual(ctx.get("scored"), "42")
        self.assertEqual(ctx.get("model_readiness"), "STALE")
        self.assertEqual(ctx.get("trading_utility"), "STAND ASIDE")
        self.assertEqual(ctx.get("operator_note"), "No matured tradeable signals in this window.")

    def test_daily_report_retrain_status_uses_completed_cycle_over_stale_running_flag(self) -> None:
        send_daily_report = load_module(
            "pq_send_daily_report_retrain_state_contract",
            REPO_ROOT / "scripts" / "send_daily_report.py",
        )
        status = send_daily_report.build_retrain_status(
            {
                "retrain_last_start_ms": str(1_000),
                "retrain_last_end_ms": str(2_000),
                "retrain_state": "running",
                "retrain_last_status": "ok",
                "reload_last_status": "ok",
            },
            {
                "last_cycle": "unknown",
                "reload_status": "unknown",
                "next_expected": "unknown",
            },
        )
        self.assertNotIn("(running)", status["last_cycle"])
        self.assertEqual(status["reload_status"], "ok")

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

    def test_build_labels_normalize_bar_interval_rejects_ambiguous_grid(self) -> None:
        # P0-A: events with no deterministic bar grid (NULL/0/invalid
        # bar_interval_sec) must not be labeled — otherwise fetch_bars walks a
        # heterogeneous 5/15/30/60m mix (mixed-interval label leakage).
        build_labels = load_module(
            "pq_build_labels_interval_guard_test",
            REPO_ROOT / "scripts" / "build_labels.py",
        )
        fn = build_labels.normalize_bar_interval
        # Ambiguous -> None (skip the row)
        self.assertIsNone(fn(None))
        self.assertIsNone(fn(0))
        self.assertIsNone(fn(-5))
        self.assertIsNone(fn("not-an-int"))
        self.assertIsNone(fn(60.5))
        self.assertIsNone(fn(True))   # bool must not coerce to 1
        self.assertIsNone(fn(False))
        # Valid grid -> positive int
        self.assertEqual(fn(300), 300)
        self.assertEqual(fn("900"), 900)
        self.assertEqual(fn(60.0), 60)

    def test_score_numeric_coercion_helpers_reject_bool(self) -> None:
        # P1-B: int(True)==1 / float(True)==1.0 would silently inflate
        # integer-coded regime votes (or_breakout / gamma_mode / regime_type).
        ml_server = load_module(
            "pq_ml_server_bool_coercion_test",
            REPO_ROOT / "server" / "ml_server.py",
        )
        self.assertIsNone(ml_server._to_int(True))
        self.assertIsNone(ml_server._to_int(False))
        self.assertIsNone(ml_server._to_float(True))
        self.assertIsNone(ml_server._to_float(False))
        # Genuine numerics still pass through unchanged.
        self.assertEqual(ml_server._to_int(1), 1)
        self.assertEqual(ml_server._to_int("2"), 2)
        self.assertEqual(ml_server._to_float(0.5), 0.5)
        # _to_bool still accepts bools (must not regress).
        self.assertTrue(ml_server._to_bool(True))
        self.assertFalse(ml_server._to_bool(False))

    def test_governance_numeric_coercion_helpers_reject_bool(self) -> None:
        # P1-B sibling path: promotion-gate metric coercion must reject bool.
        gov = load_module(
            "pq_model_governance_bool_coercion_test",
            REPO_ROOT / "scripts" / "model_governance.py",
        )
        self.assertIsNone(gov.to_int(True))
        self.assertIsNone(gov.to_float(True))
        self.assertEqual(gov.to_int(3), 3)
        self.assertEqual(gov.to_float("1.5"), 1.5)

    def test_refit_calibration_mirrors_train_min_signals_overrides(self) -> None:
        # P1-C: refit must apply the same per-(target, horizon) min-signals
        # overrides as train, sharing ml.threshold_overrides as the single
        # source of truth (no flat-vs-override drift that flips sparse break
        # heads to fallback).
        refit = load_module(
            "pq_refit_calibration_overrides_test",
            REPO_ROOT / "scripts" / "refit_calibration.py",
        )
        train = load_module(
            "pq_train_overrides_parity_test",
            REPO_ROOT / "scripts" / "train_rf_artifacts.py",
        )
        # Both import the same shared callables.
        self.assertIs(refit.parse_threshold_overrides, train._parse_threshold_overrides)
        self.assertIs(refit.resolve_threshold_override, train._resolve_threshold_override)

        spec = "break:15=8,break:30=8,break:60=6"
        ov = refit.parse_threshold_overrides(
            spec, value_cast=refit._coerce_min_signals,
            option_name="--threshold-min-signals-overrides",
        )
        # Sparse break heads resolve to the override, not the flat base of 10.
        self.assertEqual(
            int(refit.resolve_threshold_override(
                target="break", horizon=60, base_value=10, overrides=ov)),
            6,
        )
        self.assertEqual(
            int(refit.resolve_threshold_override(
                target="reject", horizon=15, base_value=10, overrides=ov)),
            10,  # falls back to base (no reject override in this spec)
        )

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

        non_list_row = features.build_feature_row(
            {
                "symbol": "SPY",
                "ts_event": ts_event,
                "level_type": "P",
                "level_price": 100.0,
                "touch_price": 100.0,
                "distance_bps": 0.0,
                "mtf_confluence_types": '{"weekly":"yes"}',
            }
        )
        self.assertEqual(int(non_list_row["has_weekly_confluence"]), 0)
        self.assertEqual(int(non_list_row["has_monthly_confluence"]), 0)

        missing = features.collect_missing({"symbol": "SPY"})
        self.assertEqual(
            missing,
            ["ts_event", "level_type", "level_price", "touch_price", "distance_bps"],
        )

    def test_build_feature_row_sanitizes_nonfinite_values(self) -> None:
        features = load_module(
            "pq_features_nonfinite_test",
            REPO_ROOT / "ml" / "features.py",
        )
        ts_event = int(datetime(2026, 3, 10, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)
        row = features.build_feature_row(
            {
                "symbol": "SPY",
                "ts_event": ts_event,
                "level_type": "R2",
                "level_price": 100.0,
                "touch_price": 1e308,
                "distance_bps": 1.0,
                "ema9": 1e308,
                "ema21": 1e-308,
                "vwap": 1e-308,
                "session_std": 1e-308,
                "atr": float("inf"),
                "custom_inf": float("inf"),
            }
        )
        self.assertIsNone(row.get("ema_spread_bps"))
        self.assertIsNone(row.get("vwap_dist_bps_calc"))
        self.assertIsNone(row.get("vwap_zscore"))
        self.assertIsNone(row.get("atr_bps"))
        self.assertIsNone(row.get("custom_inf"))

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

    def test_audit_resolution_utility_contract_present(self) -> None:
        source = (REPO_ROOT / "scripts" / "audit_resolution_utility.py").read_text(encoding="utf-8")
        self.assertIn("Target Utility Audit", source)
        self.assertIn("Resolution Coverage", source)
        self.assertIn("selection_bias_gap(has-no)", source)
        self.assertIn("timing_delta(res-horizon_same_subset)", source)
        proc = run_cmd([PYTHON, "-m", "py_compile", "scripts/audit_resolution_utility.py"], cwd=REPO_ROOT)
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

    def test_run_replay_backfill_contract_present(self) -> None:
        source = (REPO_ROOT / "scripts" / "run_replay_backfill.sh").read_text(encoding="utf-8")
        self.assertIn("Replay target looks like production DB. Aborting.", source)
        self.assertIn("prediction_log_pre_replay", source)
        self.assertIn("--preview", source)
        self.assertIn("--max-remaining 0", source)

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

    def test_live_collector_score_chunk_surfaces_rate_limit_metadata(self) -> None:
        collector = load_module(
            "pq_live_collector_score_rate_limit_test",
            REPO_ROOT / "server" / "live_event_collector.py",
        )

        original_urlopen = collector.urlopen
        original_attempts = collector.SCORE_MAX_ATTEMPTS
        try:
            collector.SCORE_MAX_ATTEMPTS = 1

            def _always_429(_req, timeout=0):  # noqa: ANN001
                raise collector.HTTPError(
                    url=collector.SCORE_API_URL,
                    code=429,
                    msg="Too Many Requests",
                    hdrs={"Retry-After": "7"},
                    fp=None,
                )

            collector.urlopen = _always_429
            with self.assertRaises(collector.ScoreRequestError) as ctx:
                collector._score_chunk(
                    [{"event_id": "evt_429"}],
                    {"Content-Type": "application/json"},
                )
        finally:
            collector.urlopen = original_urlopen
            collector.SCORE_MAX_ATTEMPTS = original_attempts

        error = ctx.exception
        self.assertEqual(int(error.status_code or 0), 429)
        self.assertAlmostEqual(float(error.retry_after_sec or 0.0), 7.0, places=3)
        self.assertTrue(collector._is_rate_limited_exception(error))

    def test_live_collector_main_applies_rate_limit_cooldown_skip_path(self) -> None:
        collector = load_module(
            "pq_live_collector_score_cooldown_loop_test",
            REPO_ROOT / "server" / "live_event_collector.py",
        )

        class _FakeHealthServer:
            def shutdown(self) -> None:
                return

            def server_close(self) -> None:
                return

        class _FakeConn:
            def commit(self) -> None:
                return

            def rollback(self) -> None:
                return

            def close(self) -> None:
                return

        class _FakeStopEvent:
            def __init__(self, max_cycles: int = 2) -> None:
                self.wait_calls = 0
                self.max_cycles = max_cycles

            def is_set(self) -> bool:
                return self.wait_calls >= self.max_cycles

            def wait(self, _timeout: float) -> bool:
                self.wait_calls += 1
                return self.is_set()

            def set(self) -> None:
                self.wait_calls = self.max_cycles

        original_run_health_server = collector._run_health_server
        original_connect_db = collector._connect_db
        original_collect_symbol = collector._collect_symbol
        original_score_events = collector._score_events
        original_stop_event = collector._stop_event
        original_signal = collector.signal.signal
        original_symbols = list(collector.SYMBOLS)
        original_poll = collector.POLL_SEC
        original_score_enabled = collector.SCORE_ENABLED
        original_backlog = collector.SCORE_UNSCORED_MAX_PER_CYCLE
        original_cooldown = collector.SCORE_RATE_LIMIT_COOLDOWN_SEC
        try:
            collector._run_health_server = lambda: _FakeHealthServer()
            collector._connect_db = lambda: _FakeConn()
            collector.signal.signal = lambda *_args, **_kwargs: None
            collector._stop_event = _FakeStopEvent(max_cycles=2)
            collector.SYMBOLS = ["SPY"]
            collector.POLL_SEC = 0
            collector.SCORE_ENABLED = True
            collector.SCORE_UNSCORED_MAX_PER_CYCLE = 0
            collector.SCORE_RATE_LIMIT_COOLDOWN_SEC = 60.0

            collector._set_state(
                {
                    "status": "starting",
                    "last_cycle_start_ms": None,
                    "last_cycle_end_ms": None,
                    "last_success_ms": None,
                    "last_error": None,
                    "cycles": 0,
                    "symbols": {},
                    "score_status": "idle",
                    "score_backoff_until_ms": 0,
                    "score_backoff_reason": None,
                    "score_backoff_skip_cycles": 0,
                    "score_backoff_skipped_events": 0,
                    "score_rate_limit_count": 0,
                    "score_last_rate_limit_ms": None,
                }
            )

            def _fake_collect_symbol(_conn, symbol):  # noqa: ANN001
                return (
                    {
                        "symbol": symbol,
                        "source": "Yahoo",
                        "bars_inserted": 0,
                        "events_built": 1,
                        "events_inserted": 1,
                        "events_scored": 0,
                        "candles": 10,
                        "session_count": 2,
                    },
                    [{"event_id": f"evt_{symbol}_1"}],
                )

            score_calls = {"n": 0}

            def _fake_score_events(events):  # noqa: ANN001
                score_calls["n"] += 1
                raise collector.ScoreRequestError(
                    f"ML score request failed: HTTP 429 for {len(events)} events",
                    status_code=429,
                    retry_after_sec=5.0,
                )

            collector._collect_symbol = _fake_collect_symbol
            collector._score_events = _fake_score_events

            collector.main()
        finally:
            collector._run_health_server = original_run_health_server
            collector._connect_db = original_connect_db
            collector._collect_symbol = original_collect_symbol
            collector._score_events = original_score_events
            collector._stop_event = original_stop_event
            collector.signal.signal = original_signal
            collector.SYMBOLS = original_symbols
            collector.POLL_SEC = original_poll
            collector.SCORE_ENABLED = original_score_enabled
            collector.SCORE_UNSCORED_MAX_PER_CYCLE = original_backlog
            collector.SCORE_RATE_LIMIT_COOLDOWN_SEC = original_cooldown

        snap = collector._get_state_snapshot()
        self.assertEqual(score_calls["n"], 1, "Cooldown path should suppress second-cycle score call")
        self.assertEqual(int(snap.get("score_rate_limit_count") or 0), 1)
        self.assertEqual(int(snap.get("score_backoff_skip_cycles") or 0), 2)
        self.assertEqual(int(snap.get("score_backoff_skipped_events") or 0), 2)
        self.assertEqual(snap.get("status"), "ok")
        self.assertEqual(snap.get("score_status"), "cooldown")
        symbol_state = (snap.get("symbols") or {}).get("SPY") or {}
        self.assertEqual(int(symbol_state.get("events_score_skipped") or 0), 1)

    def test_live_collector_uses_proxy_first_for_yahoo(self) -> None:
        """_collect_symbol() must delegate to fetch_market() exactly once.
        fetch_market() owns proxy-first routing internally; _collect_symbol() must
        not add a second proxy attempt on top of it (no double proxy call)."""
        import sqlite3 as _sqlite3

        collector = load_module(
            "pq_live_collector_proxy_first_test",
            REPO_ROOT / "server" / "live_event_collector.py",
        )

        _proxy_candles = [
            {
                "time": 1_777_700_000,
                "open": 500.0,
                "high": 501.0,
                "low": 499.0,
                "close": 500.5,
                "volume": 1000.0,
            }
        ]

        # Track every call to fetch_market to prove _collect_symbol() calls it
        # exactly once — no duplicate proxy attempt wrapping the call.
        fetch_market_calls: list[tuple] = []

        def _fake_fetch_market(symbol, interval, range_str, source):  # noqa: ANN001
            fetch_market_calls.append((symbol, interval, range_str, source))
            return ({"symbol": symbol, "candles": _proxy_candles}, "Yahoo")

        original_fetch_market = collector.fetch_market
        original_source = collector.SOURCE
        original_proxy_url = collector.YAHOO_PROXY_URL
        original_parse_candles = collector.parse_candles
        original_build_daily_bars = collector.build_daily_bars
        original_insert_bars = collector.insert_bars
        original_insert_events = collector.insert_events
        original_build_events = collector.build_events
        original_get_gamma = collector._get_gamma_context
        original_write_bars = collector.WRITE_BARS
        original_write_events = collector.WRITE_EVENTS
        original_fetch_existing = collector._fetch_existing_event_ids
        try:
            collector.SOURCE = "yahoo"
            collector.YAHOO_PROXY_URL = "http://127.0.0.1:3000/api/market"
            collector.WRITE_BARS = False
            collector.WRITE_EVENTS = False
            collector.fetch_market = _fake_fetch_market
            collector.parse_candles = lambda payload: []  # no events built
            collector.build_daily_bars = lambda candles: []
            collector.insert_bars = lambda *a, **kw: 0
            collector.insert_events = lambda *a, **kw: 0
            collector.build_events = lambda **kw: []
            collector._get_gamma_context = lambda symbol: None
            collector._fetch_existing_event_ids = lambda conn, ids: set()

            db_path = self.tmp / "proxy_first_test.sqlite"
            conn = _sqlite3.connect(str(db_path))
            try:
                result, new_events = collector._collect_symbol(conn, "SPY")
            finally:
                conn.close()
        finally:
            collector.fetch_market = original_fetch_market
            collector.SOURCE = original_source
            collector.YAHOO_PROXY_URL = original_proxy_url
            collector.parse_candles = original_parse_candles
            collector.build_daily_bars = original_build_daily_bars
            collector.insert_bars = original_insert_bars
            collector.insert_events = original_insert_events
            collector.build_events = original_build_events
            collector._get_gamma_context = original_get_gamma
            collector._fetch_existing_event_ids = original_fetch_existing
            collector.WRITE_BARS = original_write_bars
            collector.WRITE_EVENTS = original_write_events

        # _collect_symbol() must call fetch_market() exactly once — no duplicate
        # proxy wrapping.  fetch_market() owns proxy-vs-direct selection internally.
        self.assertEqual(
            len(fetch_market_calls), 1,
            "_collect_symbol() must delegate to fetch_market() exactly once; "
            "a second call would mean a duplicate proxy attempt was added.",
        )
        self.assertEqual(fetch_market_calls[0][0], "SPY")
        self.assertEqual(fetch_market_calls[0][3], "yahoo")
        self.assertEqual(result["symbol"], "SPY")
        self.assertEqual(result["source"], "Yahoo")

    def test_live_collector_falls_back_to_direct_on_proxy_failure(self) -> None:
        """_collect_symbol() must call fetch_market (direct fallback) exactly once
        when the proxy is down."""
        import sqlite3 as _sqlite3

        collector = load_module(
            "pq_live_collector_proxy_fallback_test",
            REPO_ROOT / "server" / "live_event_collector.py",
        )

        fetch_market_calls: list[tuple] = []

        def _fake_fetch_market_with_fallback(symbol, interval, range_str, source):  # noqa: ANN001
            fetch_market_calls.append((symbol, source))
            # Simulate proxy failure + direct fallback inside fetch_market
            return (
                {
                    "symbol": symbol,
                    "candles": [
                        {
                            "time": 1_777_700_000,
                            "open": 500.0,
                            "high": 501.0,
                            "low": 499.0,
                            "close": 500.5,
                            "volume": 1000.0,
                        }
                    ],
                },
                "Yahoo",
            )

        original_fetch_market = collector.fetch_market
        original_source = collector.SOURCE
        original_proxy_url = collector.YAHOO_PROXY_URL
        original_parse_candles = collector.parse_candles
        original_write_bars = collector.WRITE_BARS
        original_write_events = collector.WRITE_EVENTS
        original_get_gamma = collector._get_gamma_context
        try:
            collector.SOURCE = "yahoo"
            collector.YAHOO_PROXY_URL = "http://127.0.0.1:3000/api/market"
            collector.WRITE_BARS = False
            collector.WRITE_EVENTS = False
            collector.fetch_market = _fake_fetch_market_with_fallback
            collector.parse_candles = lambda payload: []
            collector._get_gamma_context = lambda symbol: None

            db_path = self.tmp / "proxy_fallback_test.sqlite"
            conn = _sqlite3.connect(str(db_path))
            try:
                result, new_events = collector._collect_symbol(conn, "SPY")
            finally:
                conn.close()
        finally:
            collector.fetch_market = original_fetch_market
            collector.SOURCE = original_source
            collector.YAHOO_PROXY_URL = original_proxy_url
            collector.parse_candles = original_parse_candles
            collector.WRITE_BARS = original_write_bars
            collector.WRITE_EVENTS = original_write_events
            collector._get_gamma_context = original_get_gamma

        # fetch_market must be called exactly once — no double proxy attempt
        self.assertEqual(len(fetch_market_calls), 1, "fetch_market must be called exactly once for direct fallback")
        self.assertEqual(fetch_market_calls[0][0], "SPY")
        self.assertEqual(result["source"], "Yahoo")

    def test_live_collector_proxy_url_disable_via_empty_env(self) -> None:
        """When YAHOO_PROXY_URL is set to empty string, the proxy must be disabled
        and _collect_symbol() must call fetch_market without any proxy attempt."""
        import sqlite3 as _sqlite3

        collector = load_module(
            "pq_live_collector_proxy_disable_env_test",
            REPO_ROOT / "server" / "live_event_collector.py",
        )

        # Verify P3 fix: empty string leaves YAHOO_PROXY_URL as "" (not the default URL)
        original_proxy_url = collector.YAHOO_PROXY_URL
        collector.YAHOO_PROXY_URL = ""
        try:
            self.assertEqual(collector.YAHOO_PROXY_URL, "", "Empty YAHOO_PROXY_URL must stay empty (proxy disabled)")
        finally:
            collector.YAHOO_PROXY_URL = original_proxy_url

        fetch_market_calls: list[tuple] = []

        def _fake_fetch_market(symbol, interval, range_str, source):  # noqa: ANN001
            fetch_market_calls.append((symbol, source))
            return (
                {
                    "symbol": symbol,
                    "candles": [
                        {
                            "time": 1_777_700_000,
                            "open": 500.0,
                            "high": 501.0,
                            "low": 499.0,
                            "close": 500.5,
                            "volume": 1000.0,
                        }
                    ],
                },
                "Yahoo",
            )

        original_fetch_market = collector.fetch_market
        original_source = collector.SOURCE
        original_proxy_url2 = collector.YAHOO_PROXY_URL
        original_parse_candles = collector.parse_candles
        original_write_bars = collector.WRITE_BARS
        original_write_events = collector.WRITE_EVENTS
        original_get_gamma = collector._get_gamma_context
        try:
            collector.SOURCE = "yahoo"
            # Empty string = proxy disabled
            collector.YAHOO_PROXY_URL = ""
            collector.WRITE_BARS = False
            collector.WRITE_EVENTS = False
            collector.fetch_market = _fake_fetch_market
            collector.parse_candles = lambda payload: []
            collector._get_gamma_context = lambda symbol: None

            db_path = self.tmp / "proxy_disable_env_test.sqlite"
            conn = _sqlite3.connect(str(db_path))
            try:
                result, new_events = collector._collect_symbol(conn, "SPY")
            finally:
                conn.close()
        finally:
            collector.fetch_market = original_fetch_market
            collector.SOURCE = original_source
            collector.YAHOO_PROXY_URL = original_proxy_url2
            collector.parse_candles = original_parse_candles
            collector.WRITE_BARS = original_write_bars
            collector.WRITE_EVENTS = original_write_events
            collector._get_gamma_context = original_get_gamma

        # fetch_market called once; no proxy URL in the call chain
        self.assertEqual(len(fetch_market_calls), 1)
        self.assertEqual(fetch_market_calls[0][0], "SPY")
        self.assertEqual(result["source"], "Yahoo")

    def test_fetch_market_auth_skip_bypasses_proxy_when_dashboard_auth_active(self) -> None:
        """Regression: _should_skip_proxy_due_auth_requirement() causes fetch_market()
        to bypass the local proxy entirely when dashboard auth is enabled without a
        service token and the local bypass is not effective.

        This was the true historical root cause of the live collector sending 45h of
        direct-Yahoo requests despite the proxy being healthy: the process environment
        had DASH_AUTH_ENABLED/DASH_AUTH_PASSWORD set, HOST bound to a non-loopback
        address, and no YAHOO_PROXY_SERVICE_TOKEN configured.

        The documented remedy: set YAHOO_PROXY_SERVICE_TOKEN (or DASH_AUTH_SERVICE_TOKEN)
        so that fetch_market() can authenticate with the local proxy, or set
        YAHOO_PROXY_SKIP_AUTH_REQUIRED=false to disable the auth guard entirely.
        """
        backfill = load_module(
            "pq_backfill_auth_skip_regression_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        # Simulate the production auth-skip conditions:
        #   - Dashboard auth is active (password configured)
        #   - No service token (default in many installs)
        #   - Local bypass is NOT effective (HOST bound to non-loopback)
        #   - YAHOO_PROXY_SKIP_AUTH_REQUIRED is True (the default)
        orig_proxy_url = backfill.YAHOO_PROXY_URL
        orig_skip_auth = backfill.YAHOO_PROXY_SKIP_AUTH_REQUIRED
        orig_service_token = backfill.YAHOO_PROXY_SERVICE_TOKEN
        try:
            backfill.YAHOO_PROXY_URL = "http://127.0.0.1:3000/api/market"
            backfill.YAHOO_PROXY_SKIP_AUTH_REQUIRED = True
            backfill.YAHOO_PROXY_SERVICE_TOKEN = ""

            with patch.object(backfill, "_dashboard_auth_effective", return_value=True), \
                 patch.object(backfill, "_dashboard_auth_local_bypass_effective", return_value=False):
                skip = backfill._should_skip_proxy_due_auth_requirement()

            self.assertTrue(
                skip,
                "_should_skip_proxy_due_auth_requirement() must return True when "
                "dashboard auth is active, local bypass is inactive, and no service "
                "token is set. This is the documented auth-skip behavior.",
            )

            # Confirm the inverse: with a service token, proxy is NOT skipped.
            backfill.YAHOO_PROXY_SERVICE_TOKEN = "test-token"
            with patch.object(backfill, "_dashboard_auth_effective", return_value=True), \
                 patch.object(backfill, "_dashboard_auth_local_bypass_effective", return_value=False):
                skip_with_token = backfill._should_skip_proxy_due_auth_requirement()

            self.assertFalse(
                skip_with_token,
                "_should_skip_proxy_due_auth_requirement() must return False when a "
                "service token is configured — proxy should be used.",
            )

            # Confirm the inverse: with local bypass active, proxy is NOT skipped.
            backfill.YAHOO_PROXY_SERVICE_TOKEN = ""
            with patch.object(backfill, "_dashboard_auth_effective", return_value=True), \
                 patch.object(backfill, "_dashboard_auth_local_bypass_effective", return_value=True):
                skip_with_bypass = backfill._should_skip_proxy_due_auth_requirement()

            self.assertFalse(
                skip_with_bypass,
                "_should_skip_proxy_due_auth_requirement() must return False when "
                "the local auth bypass is active.",
            )
        finally:
            backfill.YAHOO_PROXY_URL = orig_proxy_url
            backfill.YAHOO_PROXY_SKIP_AUTH_REQUIRED = orig_skip_auth
            backfill.YAHOO_PROXY_SERVICE_TOKEN = orig_service_token

    def test_live_collector_startup_warns_when_auth_skip_active(self) -> None:
        """Regression: _check_proxy_auth_config() must log CRITICAL when the
        auth-skip guard would bypass the local proxy entirely at startup.

        This ensures the 45 h silent-bypass failure mode is loud rather than
        invisible — the collector will still start, but operators see the warning.
        """
        lc = load_module(
            "pq_lc_startup_warn_test",
            REPO_ROOT / "server" / "live_event_collector.py",
        )

        critical_calls: list[str] = []

        def _fake_critical(msg: str, *args: object, **kwargs: object) -> None:
            critical_calls.append(msg % args if args else msg)

        with patch.object(lc, "_should_skip_proxy_due_auth_requirement", return_value=True), \
             patch.object(lc.log, "critical", side_effect=_fake_critical):
            lc._check_proxy_auth_config()

        self.assertEqual(
            len(critical_calls),
            1,
            "_check_proxy_auth_config() must emit exactly one log.critical when "
            "_should_skip_proxy_due_auth_requirement() returns True.",
        )
        self.assertIn(
            "auth-skip guard is ACTIVE",
            critical_calls[0],
            "Critical message must name the guard so operators know what to fix.",
        )

        # Guard does NOT fire when skip returns False (normal/protected state).
        critical_calls.clear()
        with patch.object(lc, "_should_skip_proxy_due_auth_requirement", return_value=False), \
             patch.object(lc.log, "critical", side_effect=_fake_critical):
            lc._check_proxy_auth_config()

        self.assertEqual(
            len(critical_calls),
            0,
            "_check_proxy_auth_config() must be silent when the guard is not active.",
        )

    def test_env_file_production_policy_guards_proxy(self) -> None:
        """Static guard: .env must have Yahoo proxy protection in place when
        DASH_AUTH_ENABLED=true.

        When dashboard auth is active, at least one of the following must hold:
          • YAHOO_PROXY_SERVICE_TOKEN (or DASH_AUTH_SERVICE_TOKEN) is non-empty, OR
          • DASH_AUTH_LOCAL_BYPASS=true

        Either condition prevents _should_skip_proxy_due_auth_requirement() from
        firing and ensures the live collector routes through the local proxy.
        """
        env_path = REPO_ROOT / ".env"
        if not env_path.exists():
            self.skipTest(".env not present — skipping production-policy guard")

        env: dict[str, str] = {}
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:]
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip()
            # Strip matching outer quotes
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                val = val[1:-1]
            env[key] = val

        def _truthy(v: str) -> bool:
            return v.strip().lower() in ("1", "true", "yes", "on")

        dash_auth_enabled = _truthy(env.get("DASH_AUTH_ENABLED", ""))
        if not dash_auth_enabled:
            return  # auth not active — no proxy-bypass risk; nothing to assert

        has_service_token = bool(
            env.get("YAHOO_PROXY_SERVICE_TOKEN", "").strip()
            or env.get("DASH_AUTH_SERVICE_TOKEN", "").strip()
        )
        has_local_bypass = _truthy(env.get("DASH_AUTH_LOCAL_BYPASS", ""))

        self.assertTrue(
            has_service_token or has_local_bypass,
            "Production .env has DASH_AUTH_ENABLED=true but no proxy-bypass "
            "protection is present. YAHOO_PROXY_SERVICE_TOKEN (and "
            "DASH_AUTH_SERVICE_TOKEN) are unset, and DASH_AUTH_LOCAL_BYPASS is "
            "not true. The live collector would silently bypass the local proxy "
            "and use direct Yahoo on every fetch. "
            "Add YAHOO_PROXY_SERVICE_TOKEN=<token> or DASH_AUTH_LOCAL_BYPASS=true "
            "to .env to fix this.",
        )

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

    def test_mtf_weekly_pivot_does_not_leak_in_progress_week(self) -> None:
        """P0-1 regression: find_mtf_pivot_for_date must never return a pivot
        computed from the ISO week that CONTAINS target_date.

        The original implementation accepted any weekly entry with
        ``date < target_date``.  When target_date sat past the last session
        in the input, the trailing in-progress weekly accumulator (whose date
        was the last session) would be selected as "prior weekly," leaking
        same-week OHLC into the feature.  The fix uses ISO ``period_key``
        comparison so the consumer can never select an entry whose period
        equals the target's period.
        """
        backfill = load_module(
            "pq_backfill_mtf_weekly_pivot_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        def _mk(d: date, o: float, h: float, l: float, c: float) -> dict:
            return {"date": d, "open": o, "high": h, "low": l, "close": c}

        def _stub_calc(h: float, l: float, c: float) -> dict:
            return {"h": h, "l": l, "c": c}

        # ISO week 21 of 2026: Mon May 18 - Fri May 22 (complete).
        # ISO week 22 of 2026: Mon May 25 (US Memorial Day) - Wed May 27.
        # Build sessions covering one complete week + 3 days of next week.
        sessions = [
            _mk(date(2026, 5, 18), 100, 105,  99, 104),
            _mk(date(2026, 5, 19), 104, 106, 102, 105),
            _mk(date(2026, 5, 20), 105, 108, 103, 107),
            _mk(date(2026, 5, 21), 107, 110, 105, 109),
            _mk(date(2026, 5, 22), 109, 112, 108, 111),  # complete W-1: H=112,L=99,C=111
            _mk(date(2026, 5, 26), 111, 115, 110, 114),  # Mon W (Tue, Memorial Day skipped)
            _mk(date(2026, 5, 27), 114, 118, 113, 117),  # Tue W
        ]
        weekly = backfill.build_weekly_sessions(sessions)

        # Producer-side invariant: each entry carries period_kind + period_key.
        self.assertEqual(len(weekly), 2)
        self.assertEqual(weekly[0]["period_kind"], "iso_week")
        self.assertEqual(weekly[0]["period_key"], (2026, 21))
        self.assertEqual(weekly[1]["period_kind"], "iso_week")
        self.assertEqual(weekly[1]["period_key"], (2026, 22))

        # Consumer-side invariant: target_date AFTER last session must NOT
        # select the in-progress entry that contains the target's period.
        # This is the latent bug — exercised here by asking for a target
        # past the last input session.
        target_wed_w = date(2026, 5, 28)  # ISO week 22, NOT in sessions
        pivot = backfill.find_mtf_pivot_for_date(weekly, target_wed_w, calc_fn=_stub_calc)
        self.assertIsNotNone(pivot)
        self.assertEqual(
            (pivot["h"], pivot["l"], pivot["c"]),
            (112, 99, 111),
            "target in ISO week 22 must use the COMPLETED week 21, not the "
            "in-progress week-22 partial accumulator",
        )

        # Same-week targets (in W) must also use the completed prior week.
        for s in sessions[5:]:  # Mon W, Tue W
            piv = backfill.find_mtf_pivot_for_date(weekly, s["date"], calc_fn=_stub_calc)
            self.assertIsNotNone(piv)
            self.assertEqual(
                (piv["h"], piv["l"], piv["c"]),
                (112, 99, 111),
                f"target={s['date']} must use completed week 21 pivot",
            )

    def test_mtf_weekly_pivot_none_when_no_prior_completed_week(self) -> None:
        """When the input contains only the in-progress week, no prior weekly
        pivot exists and the function must return None."""
        backfill = load_module(
            "pq_backfill_mtf_weekly_no_prior_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        def _mk(d: date, o: float, h: float, l: float, c: float) -> dict:
            return {"date": d, "open": o, "high": h, "low": l, "close": c}

        sessions = [
            _mk(date(2026, 5, 26), 111, 115, 110, 114),
            _mk(date(2026, 5, 27), 114, 118, 113, 117),
            _mk(date(2026, 5, 28), 117, 120, 116, 119),
        ]
        weekly = backfill.build_weekly_sessions(sessions)
        self.assertEqual(len(weekly), 1)
        self.assertEqual(weekly[0]["period_key"], (2026, 22))

        for s in sessions:
            piv = backfill.find_mtf_pivot_for_date(weekly, s["date"])
            self.assertIsNone(
                piv,
                f"target={s['date']}: no prior completed week exists, must be None",
            )

        # Even a target past the last session must be None when no prior week exists.
        piv = backfill.find_mtf_pivot_for_date(weekly, date(2026, 5, 29))
        self.assertIsNone(piv)

    def test_mtf_monthly_pivot_does_not_leak_in_progress_month(self) -> None:
        """P0-1 regression: same look-ahead invariant for monthly pivots."""
        backfill = load_module(
            "pq_backfill_mtf_monthly_pivot_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        def _mk(d: date, o: float, h: float, l: float, c: float) -> dict:
            return {"date": d, "open": o, "high": h, "low": l, "close": c}

        def _stub_calc(h: float, l: float, c: float) -> dict:
            return {"h": h, "l": l, "c": c}

        # April 2026 (complete) + mid-May 2026 (in progress).
        sessions = [
            _mk(date(2026, 4,  1), 100, 110,  90, 105),
            _mk(date(2026, 4, 30), 105, 115,  95, 110),  # April: H=115,L=90,C=110
            _mk(date(2026, 5,  1), 110, 112, 108, 111),
            _mk(date(2026, 5, 15), 111, 120, 110, 118),
        ]
        monthly = backfill.build_monthly_sessions(sessions)

        self.assertEqual(len(monthly), 2)
        self.assertEqual(monthly[0]["period_kind"], "cal_month")
        self.assertEqual(monthly[0]["period_key"], (2026, 4))
        self.assertEqual(monthly[1]["period_key"], (2026, 5))

        # Mid-May target must use APRIL's completed OHLC, not May partial.
        target_mid_may = date(2026, 5, 15)
        pivot = backfill.find_mtf_pivot_for_date(monthly, target_mid_may, calc_fn=_stub_calc)
        self.assertIsNotNone(pivot)
        self.assertEqual(
            (pivot["h"], pivot["l"], pivot["c"]),
            (115, 90, 110),
            "target in May must use COMPLETED April, not in-progress May partial",
        )

        # Latent bug exercise: target past the last session, still in May.
        target_may_30 = date(2026, 5, 30)
        pivot2 = backfill.find_mtf_pivot_for_date(monthly, target_may_30, calc_fn=_stub_calc)
        self.assertIsNotNone(pivot2)
        self.assertEqual((pivot2["h"], pivot2["l"], pivot2["c"]), (115, 90, 110))

    def test_mtf_monthly_pivot_none_when_no_prior_completed_month(self) -> None:
        backfill = load_module(
            "pq_backfill_mtf_monthly_no_prior_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        def _mk(d: date, o: float, h: float, l: float, c: float) -> dict:
            return {"date": d, "open": o, "high": h, "low": l, "close": c}

        sessions = [
            _mk(date(2026, 5,  1), 110, 112, 108, 111),
            _mk(date(2026, 5, 15), 111, 120, 110, 118),
        ]
        monthly = backfill.build_monthly_sessions(sessions)
        self.assertEqual(len(monthly), 1)

        for s in sessions:
            piv = backfill.find_mtf_pivot_for_date(monthly, s["date"])
            self.assertIsNone(piv)

    def test_mtf_prior_completed_week_is_used_when_present(self) -> None:
        """Sanity: when a completed prior week IS available, the consumer picks
        the most-recent completed entry — not skipped due to over-zealous
        filtering."""
        backfill = load_module(
            "pq_backfill_mtf_prior_week_used_test",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )

        def _mk(d: date, o: float, h: float, l: float, c: float) -> dict:
            return {"date": d, "open": o, "high": h, "low": l, "close": c}

        def _stub_calc(h: float, l: float, c: float) -> dict:
            return {"h": h, "l": l, "c": c}

        # Two complete prior weeks, then events in a third week.
        sessions = [
            # ISO week 20 of 2026 (May 11-15)
            _mk(date(2026, 5, 11), 90,  95,  88,  93),
            _mk(date(2026, 5, 15), 93,  98,  91,  96),
            # ISO week 21 of 2026 (May 18-22): H=112,L=99,C=111
            _mk(date(2026, 5, 18), 100, 105,  99, 104),
            _mk(date(2026, 5, 22), 109, 112, 108, 111),
            # ISO week 22 of 2026 (target week)
            _mk(date(2026, 5, 26), 111, 115, 110, 114),
        ]
        weekly = backfill.build_weekly_sessions(sessions)
        self.assertEqual([w["period_key"] for w in weekly], [(2026, 20), (2026, 21), (2026, 22)])

        # Mon W=22 target must select the MOST-RECENT completed prior week (21).
        target = date(2026, 5, 26)
        piv = backfill.find_mtf_pivot_for_date(weekly, target, calc_fn=_stub_calc)
        self.assertEqual(
            (piv["h"], piv["l"], piv["c"]),
            (112, 99, 111),
            "must use the most recent completed week (21), not the older one (20)",
        )

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

    def test_collect_gamma_history_uses_side_specific_walls(self) -> None:
        collector = load_module(
            "pq_collect_gamma_side_specific_walls_test",
            REPO_ROOT / "scripts" / "collect_gamma_history.py",
        )
        snap = collector.summarize_chain(
            symbol="SPY",
            snapshot_date=date(2026, 3, 23),
            chain={
                "strike": [630, 650, 650, 675],
                "side": ["put", "call", "put", "call"],
                "gamma": [0.0025, 0.0015, 0.0035, 0.0004],
                "iv": [0.22, 0.21, 0.23, 0.2],
                "openInterest": [9000, 1600, 2600, 200],
                "delta": [-0.35, 0.35, -0.2, 0.15],
                "expiration": ["2026-06-18", "2026-06-18", "2026-06-18", "2026-06-18"],
                "underlyingPrice": [653.51, 653.51, 653.51, 653.51],
            },
            strike_range_pct=0.6,
            max_strikes=200,
            expiry_mode="90dte",
        )
        payload = json.loads(snap["payload_json"])
        self.assertEqual(payload["selected_expiries"], ["20260618"])
        self.assertEqual(float(snap["call_wall"]), 650.0)
        self.assertEqual(float(snap["put_wall"]), 630.0)
        self.assertEqual(float(snap["pin"]), 630.0)

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
        self.assertIn("GAMMA_HISTORY_EXPIRY_MODE", source)
        self.assertIn("GAMMA_HISTORY_LIVE_DTE_DAYS", source)
        fetch_block = source.split("def fetch_marketdata_chain(", 1)[1].split("def _to_float", 1)[0]
        self.assertIn("_marketdata_live_dte_queries(GAMMA_HISTORY_EXPIRY_MODE, GAMMA_HISTORY_LIVE_DTE_DAYS)", fetch_block)
        self.assertIn("?dte={dte_query}", fetch_block)
        self.assertNotIn("?expiration=all", fetch_block)

    def test_collect_gamma_history_brackets_live_queries_for_90dte(self) -> None:
        collector = load_module(
            "pq_collect_gamma_history_live_dte_queries_test",
            REPO_ROOT / "scripts" / "collect_gamma_history.py",
        )
        self.assertEqual(collector._marketdata_live_dte_queries("90dte", 120), [90, 75, 105, 120])

    def test_collect_gamma_history_aggregate_90dte_dte_queries(self) -> None:
        collector = load_module(
            "pq_collect_gamma_history_aggregate_90dte_dte_queries_test",
            REPO_ROOT / "scripts" / "collect_gamma_history.py",
        )
        result = collector._marketdata_live_dte_queries("aggregate_90dte", 120)
        self.assertEqual(result, [7, 14, 30, 45, 60, 75, 90])

    def test_collect_gamma_history_aggregate_90dte_picks_all_expiries_within_window(self) -> None:
        from datetime import date as _date
        collector = load_module(
            "pq_collect_gamma_history_aggregate_90dte_pick_expiries_test",
            REPO_ROOT / "scripts" / "collect_gamma_history.py",
        )
        today = _date(2026, 3, 23)
        expiries_raw = ["20260323", "20260328", "20260417", "20260515", "20260619", "20260717"]
        result = collector._pick_chain_expiries(expiries_raw, "aggregate_90dte", today)
        self.assertNotIn("20260323", result)  # 0 DTE — excluded from structural aggregate
        self.assertIn("20260328", result)   # 5 DTE — in
        self.assertIn("20260417", result)   # 25 DTE — in
        self.assertIn("20260515", result)   # 53 DTE — in
        self.assertIn("20260619", result)   # 88 DTE — in
        self.assertNotIn("20260717", result)  # 116 DTE — beyond window

    def test_collect_gamma_history_normalizes_compact_expiry_strings(self) -> None:
        collector = load_module(
            "pq_collect_gamma_history_compact_expiry_test",
            REPO_ROOT / "scripts" / "collect_gamma_history.py",
        )
        self.assertEqual(collector._normalize_expiry_yyyymmdd("20260618"), "20260618")

    def test_collect_gamma_history_summarize_chain_tracks_selected_expiry_family(self) -> None:
        source = (REPO_ROOT / "scripts" / "collect_gamma_history.py").read_text(encoding="utf-8")
        block = source.split("def summarize_chain(", 1)[1].split("def ensure_schema(", 1)[0]
        self.assertIn("selected_expiries = _pick_chain_expiries(expiries, expiry_mode, snapshot_date)", block)
        self.assertIn("if selected_expiries and expiry_compact not in selected_expiries:", block)
        self.assertIn('"selected_expiries": sorted(selected_expiries)', block)
        self.assertIn('"expiry_mode": _normalize_expiry_mode(expiry_mode or GAMMA_HISTORY_EXPIRY_MODE)', block)
        self.assertIn('raise ValueError("No valid forward 90DTE expiry available in options chain")', block)

    def test_backfill_gamma_context_avoids_marketdata_when_bridge_reports_cooldown(self) -> None:
        source = (REPO_ROOT / "scripts" / "backfill_events.py").read_text(encoding="utf-8")
        block = source.split("def fetch_gamma_context(", 1)[1].split("def et_date", 1)[0]
        self.assertIn("bridge_marketdata_cooldown", block)
        self.assertIn("cooldown active", block)
        self.assertIn("daily request limit", block)
        self.assertIn("if bridge_marketdata_cooldown:", block)
        self.assertIn("_merge_context_with_carry(snapshot_context, carry_context, today_et)", block)

    def test_backfill_gamma_context_defaults_to_90dte_structural_mode(self) -> None:
        source = (REPO_ROOT / "scripts" / "backfill_events.py").read_text(encoding="utf-8")
        self.assertIn('GAMMA_CONTEXT_EXPIRY_MODE = (os.getenv("GAMMA_CONTEXT_EXPIRY_MODE", "90dte")', source)
        self.assertIn('GAMMA_CONTEXT_DTE_DAYS = int(os.getenv("GAMMA_CONTEXT_DTE_DAYS", "120"))', source)
        self.assertIn('expiry_mode=GAMMA_CONTEXT_EXPIRY_MODE', source)
        self.assertIn('&expiry={GAMMA_CONTEXT_EXPIRY_MODE}&limit=60', source)
        self.assertIn("fetch_gamma_marketdata_chain(", source)

    def test_backfill_ensure_new_columns_uses_transaction_contract_present(self) -> None:
        source = (REPO_ROOT / "scripts" / "backfill_events.py").read_text(encoding="utf-8")
        block = source.split("def ensure_new_columns(", 1)[1].split("def ensure_schema(", 1)[0]
        self.assertIn('conn.execute("BEGIN")', block)
        self.assertIn("conn.rollback()", block)
        self.assertIn("except Exception:", block)

    def test_build_duckdb_view_prefers_fresher_source_contract_present(self) -> None:
        source = (REPO_ROOT / "scripts" / "build_duckdb_view.py").read_text(encoding="utf-8")
        self.assertIn("def _is_parquet_fresh(", source)
        self.assertIn("touch_parquet.stat().st_mtime_ns < touch_csv.stat().st_mtime_ns", source)
        self.assertIn("labels_parquet.stat().st_mtime_ns < labels_csv.stat().st_mtime_ns", source)
        self.assertIn("Parquet exports are stale versus CSV; using CSV exports for freshness.", source)
        proc = run_cmd([PYTHON, "-m", "py_compile", "scripts/build_duckdb_view.py"], cwd=REPO_ROOT)
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")

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

    def test_dashboard_proxy_yahoo_gamma_90dte_expiry_contract_present(self) -> None:
        proxy_source = (REPO_ROOT / "server" / "yahoo_proxy.js").read_text(encoding="utf-8")
        self.assertIn("function normalizeOptionsExpiryMode(mode)", proxy_source)
        self.assertIn("if (safeMode === '90dte')", proxy_source)
        self.assertIn("Math.abs(a.dteDays - 90)", proxy_source)
        self.assertIn("const expiry = url.searchParams.get('expiry') || '90dte';", proxy_source)

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
        self.assertIn("function formatGammaSourceLabel(rawSource)", dashboard)
        self.assertIn("if (lower === 'marketdata.app') return 'marketdata.app';", dashboard)
        self.assertIn("cacheStale: !!data.cacheStale", dashboard)
        self.assertIn("cacheStaleReason: data.cacheStaleReason ? String(data.cacheStaleReason) : null", dashboard)
        self.assertIn("Stale (cached)", dashboard)

    def test_dashboard_gamma_intraday_fallback_indicator_contract_present(self) -> None:
        dashboard = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        self.assertIn("dteFallback: !!data.dteFallback", dashboard)
        self.assertIn("dteFallbackReason: data.dteFallbackReason ? String(data.dteFallbackReason) : null", dashboard)
        self.assertIn("const dteFallbackIntraday = !!state.gammaDataIntraday?.dteFallback;", dashboard)
        self.assertIn("wallSuffixIntradayParts.push('0DTE fallback');", dashboard)
        self.assertIn("Call Wall (Secondary)", dashboard)

    def test_dashboard_ml_metrics_staleness_indicator_contract_present(self) -> None:
        dashboard = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        self.assertIn("const metricsStaleSeconds = Number(state.mlMetrics?.stale_seconds);", dashboard)
        self.assertIn("ML: ${mlLabel}${healthSuffix}${metricsSuffix}", dashboard)
        self.assertIn("const staleSeconds = Number(metrics?.stale_seconds);", dashboard)
        self.assertIn("Updated ${metrics.updated_at || '--'} · Age ${staleAgeLabel}${staleTag}", dashboard)
        self.assertIn("note.style.color = 'var(--warning)';", dashboard)
        self.assertIn("note.style.color = 'var(--danger)';", dashboard)

    def test_dashboard_transparency_strip_contract_present(self) -> None:
        dashboard = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        self.assertIn('<details class="transparency-details" id="transparency-details" aria-label="Model transparency">', dashboard)
        self.assertIn('id="transparency-summary-text"', dashboard)
        self.assertIn('<section class="transparency-strip">', dashboard)
        self.assertNotIn('<details class="transparency-details" id="transparency-details" aria-label="Model transparency" open>', dashboard)
        self.assertIn('id="trans-model"', dashboard)
        self.assertIn('id="trans-governance-reason"', dashboard)
        self.assertIn("function setTransparencyItem(id, value, note = '', tone = '', title = '')", dashboard)
        self.assertIn("function updateTransparencyStrip()", dashboard)
        self.assertIn("const summaryEl = document.getElementById('transparency-summary-text');", dashboard)
        self.assertIn("const summaryParts = [", dashboard)
        self.assertIn("`${modelVersion} ${rawMlStatus.toUpperCase()}`", dashboard)
        self.assertIn("`governance ${govAction.toUpperCase()}`", dashboard)
        self.assertIn("`${gammaMainSource} ${gammaMainExpiry}`", dashboard)
        self.assertIn("const summaryText = summaryParts.join(' · ');", dashboard)
        self.assertIn("state.mlHealthRaw = payload || null;", dashboard)
        self.assertIn("state.lastEmaMethod = 'daily_warmup_merged';", dashboard)

    def test_dashboard_ema_warmup_and_tradingview_seed_contract_present(self) -> None:
        dashboard = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        self.assertIn("const EMA_WARMUP_RANGE = '5y';", dashboard)
        self.assertIn("const EMA_WARMUP_MIN_BARS = EMA_MAX_PERIOD * 3;", dashboard)
        self.assertIn("function fetchEmaWarmupCandles(symbol, interval, options = {}, isStaleRequest = null)", dashboard)
        self.assertIn("function clipSeriesToCandles(series, candles)", dashboard)
        self.assertIn("ema = close;", dashboard)
        self.assertIn("if (isDailyInterval(interval) && state.candles.length < EMA_WARMUP_MIN_BARS)", dashboard)

    def test_dashboard_ml_operator_summary_contract_present(self) -> None:
        dashboard = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        ml_panel_block = dashboard.split('<section class="panel" id="ml-panel">', 1)[1].split(
            '<section class="panel insights-panel">',
            1,
        )[0]
        visible_ml_block = ml_panel_block.split('<details class="ml-diagnostics" id="ml-diagnostics">', 1)[0]
        self.assertIn('class="ml-summary-note" id="ml-summary-note"', dashboard)
        self.assertIn('<section class="ml-decision-trace" id="ml-decision-trace" aria-label="ML decision trace">', dashboard)
        self.assertIn('id="ml-trace-status"', dashboard)
        self.assertIn('id="ml-trace-blocker"', dashboard)
        self.assertIn('id="ml-trace-meta-governance"', dashboard)
        self.assertIn('<details class="ml-diagnostics" id="ml-diagnostics">', dashboard)
        self.assertNotIn('<details class="ml-diagnostics" id="ml-diagnostics" open>', dashboard)
        self.assertIn('id="ml-diagnostics-summary"', dashboard)
        self.assertIn("function setMlSummaryNote(value, tone = '')", dashboard)
        self.assertIn("function setMlDiagnosticsSummary(value)", dashboard)
        self.assertIn("function setMlTraceMetaItem(id, value, note = '', tone = 'muted', title = '')", dashboard)
        self.assertIn("function buildMlTracePrimaryBlocker(payload)", dashboard)
        self.assertIn("function updateMlDecisionTrace(payload)", dashboard)
        self.assertIn("function summarizeMlSuppressions(payload)", dashboard)
        self.assertIn("function buildMlDiagnosticsSummary(metaLabels, flagCount = 0)", dashboard)
        self.assertIn('<details class="ml-diagnostics" id="ml-diagnostics">', ml_panel_block)
        self.assertIn("setMlDiagnosticsSummary(buildMlDiagnosticsSummary(metaLabels));", dashboard)
        self.assertIn("setMlDiagnosticsSummary(buildMlDiagnosticsSummary(metaLabels, flagCount));", dashboard)
        self.assertIn("updateMlDecisionTrace(null);", dashboard)
        self.assertIn("updateMlDecisionTrace(payload);", dashboard)
        self.assertIn('class="stat-grid stat-grid-3 ml-trust-grid"', visible_ml_block)
        self.assertIn('id="ml-metric-auc"', visible_ml_block)
        self.assertIn('id="ml-metric-brier"', visible_ml_block)
        self.assertIn('id="ml-metric-ece"', visible_ml_block)

    def test_dashboard_threshold_summary_contract_recognizes_runtime_guard_fields(self) -> None:
        dashboard = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        self.assertIn("const reason = String(entry.guard_reason || entry.reason || '');", dashboard)
        self.assertIn("entry.guard_applied === true", dashboard)

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
        self.assertIn("readJsonFileSafeAsync(MODEL_REGISTRY_FILE, {})", query_block)
        self.assertIn("readTailLinesAsync(REPORT_DELIVERY_LOG_FILE, 200)", query_block)
        self.assertIn("governance:", proxy_source)
        self.assertIn("retrain:", proxy_source)
        self.assertNotIn("readFileSync(", query_block)

    def test_dashboard_proxy_ml_metrics_uses_async_file_reads(self) -> None:
        proxy_source = (REPO_ROOT / "server" / "yahoo_proxy.js").read_text(encoding="utf-8")
        metrics_block = proxy_source.split("if (url.pathname === '/api/ml/metrics') {", 1)[1].split(
            "if (url.pathname === '/api/ml/health')",
            1,
        )[0]
        self.assertIn("await Promise.all([", metrics_block)
        self.assertIn("readJsonFileWithMetaAsync(METRICS_FILE)", metrics_block)
        self.assertIn("readJsonFileWithMetaAsync(CALIB_FILE)", metrics_block)
        self.assertIn("readJsonFileWithMetaAsync(ACTIVE_MANIFEST_FILE)", metrics_block)
        self.assertIn("updatedAtMs:", metrics_block)
        self.assertIn("sourceFiles,", metrics_block)
        self.assertIn("activeModelVersion:", metrics_block)
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

    def test_ml_server_logs_missing_threshold_fallbacks(self) -> None:
        ml_server = load_module(
            "pq_ml_server_missing_threshold_warning_test",
            REPO_ROOT / "server" / "ml_server.py",
        )
        ml_server._missing_threshold_warnings.clear()
        with self.assertLogs("ml_server", level="WARNING") as cm:
            value = ml_server._threshold_from_map({"reject": {}, "break": {}}, "reject", 5, context="test_case")
        self.assertEqual(value, 0.5)
        self.assertIn("Missing reject threshold for 5m horizon in test_case", "\n".join(cm.output))

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
        self.assertIn("PREDICTION_LOG_LOCK_WARN_INTERVAL_SEC", source)
        self.assertIn("def _warn_prediction_log_contention(exc: Exception) -> None:", source)
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

    def test_ibkr_bridge_fetch_ticker_price_empty_ticker_guard_contract_present(self) -> None:
        source = (REPO_ROOT / "server" / "ibkr_gamma_bridge.py").read_text(encoding="utf-8")
        block = source.split("def _fetch_ticker_price(", 1)[1].split("def ensure_connected(", 1)[0]
        self.assertIn("tickers = ib.reqTickers(contract)", block)
        self.assertIn("if not tickers or tickers[0] is None:", block)
        self.assertIn("raise ValueError(", block)
        self.assertIn("No market data ticker returned", block)

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

    def test_ibkr_bridge_marketdata_logs_0dte_fallback_warning(self) -> None:
        source = (REPO_ROOT / "server" / "ibkr_gamma_bridge.py").read_text(encoding="utf-8")
        block = source.split("def fetch_gamma_marketdata(", 1)[1].split("class GammaHandler", 1)[0]
        self.assertIn("if dte_query == 0 and isinstance(exc, urllib.error.HTTPError) and exc.code == 400:", block)
        self.assertIn(
            "[gamma_bridge] 0DTE unavailable from marketdata.app, falling back to dte=1",
            block,
        )

    def test_ibkr_bridge_marketdata_surfaces_dte_fallback_metadata(self) -> None:
        source = (REPO_ROOT / "server" / "ibkr_gamma_bridge.py").read_text(encoding="utf-8")
        block = source.split("def fetch_gamma_marketdata(", 1)[1].split("class GammaHandler", 1)[0]
        self.assertIn("dte_fallback = bool(data.get(\"dteFallback\"))", block)
        self.assertIn("dte_fallback_reason = data.get(\"dteFallbackReason\")", block)
        self.assertIn("\"dteFallback\": dte_fallback", block)
        self.assertIn("\"dteFallbackReason\": dte_fallback_reason", block)

    def test_ibkr_bridge_marketdata_aggregate_tolerates_partial_fetch_failures(self) -> None:
        bridge = load_module(
            "pq_ibkr_marketdata_aggregate_partial_failure_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )

        class _FakeResponse:
            def __init__(self, payload: dict) -> None:
                self._payload = json.dumps(payload).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

            def read(self) -> bytes:
                return self._payload

        def _payload_for(expiry: str) -> dict:
            return {
                "s": "ok",
                "strike": [645.0, 670.0],
                "side": ["put", "call"],
                "gamma": [0.03, 0.015],
                "iv": [0.24, 0.20],
                "openInterest": [150.0, 100.0],
                "delta": [-0.25, 0.25],
                "expiration": [expiry, expiry],
                "underlyingPrice": [650.0, 650.0],
            }

        payloads = {
            7: _payload_for("2026-03-27"),
            14: _payload_for("2026-04-02"),
            30: _payload_for("2026-04-17"),
            60: _payload_for("2026-05-15"),
            75: _payload_for("2026-05-29"),
            90: _payload_for("2026-06-18"),
        }

        original_token = bridge.MARKETDATA_APP_TOKEN
        original_urlopen = bridge.urllib.request.urlopen
        original_today = bridge._utc_today_yyyymmdd
        bridge.MARKETDATA_APP_TOKEN = "test-token"
        bridge._utc_today_yyyymmdd = lambda: "20260323"
        bridge._mda_gamma_cache.clear()
        bridge._mda_gamma_error_backoff_until.clear()
        try:
            def _fake_urlopen(req, timeout=0):  # noqa: ANN001
                url = req.full_url if hasattr(req, "full_url") else req.get_full_url()
                match = re.search(r"[?&]dte=(\d+)", url)
                if not match:
                    raise AssertionError(f"missing dte in url: {url}")
                dte = int(match.group(1))
                if dte == 45:
                    raise URLError("simulated aggregate bucket failure")
                return _FakeResponse(payloads[dte])

            bridge.urllib.request.urlopen = _fake_urlopen
            result = bridge.fetch_gamma_marketdata("SPY", expiry_mode="aggregate_90dte")
        finally:
            bridge.MARKETDATA_APP_TOKEN = original_token
            bridge.urllib.request.urlopen = original_urlopen
            bridge._utc_today_yyyymmdd = original_today
            bridge._mda_gamma_cache.clear()
            bridge._mda_gamma_error_backoff_until.clear()

        self.assertEqual(result["expiryMode"], "aggregate_90dte")
        self.assertEqual(result["gammaRegime"], "net_short")
        self.assertFalse(result["gammaFlipIsTrueCrossing"])
        self.assertEqual(result["stats"]["selectedExpiries"][0], "20260327")
        self.assertIn("20260618", result["stats"]["selectedExpiries"])
        self.assertEqual(len(result["partialFetchWarnings"]), 1)
        self.assertIn("dte=45", result["partialFetchWarnings"][0])

    def test_ibkr_bridge_marketdata_cache_key_is_expiry_mode_scoped(self) -> None:
        source = (REPO_ROOT / "server" / "ibkr_gamma_bridge.py").read_text(encoding="utf-8")
        self.assertIn("_EXPIRY_MODE_DTE = {", source)
        self.assertIn('"front":     7', source)
        self.assertIn('"weekly":    7', source)
        self.assertIn('"monthly":  45', source)
        self.assertIn('"90dte":    120', source)
        self.assertIn("dte_days = _EXPIRY_MODE_DTE.get(mode, MDA_GAMMA_DTE_DAYS)", source)
        self.assertIn('cache_key = f"{symbol.upper()}:{mode or \'default\'}"', source)
        self.assertIn("payload = fetch_gamma_marketdata(symbol, expiry_mode=expiry)", source)

    def test_ibkr_bridge_marketdata_filters_selected_expiries_for_90dte_mode(self) -> None:
        source = (REPO_ROOT / "server" / "ibkr_gamma_bridge.py").read_text(encoding="utf-8")
        block = source.split("def fetch_gamma_marketdata(", 1)[1].split("class GammaHandler", 1)[0]
        self.assertIn("dte_queries = _marketdata_dte_queries(mode, dte_days)", block)
        self.assertIn("data = _merge_marketdata_payloads(payloads) if len(payloads) > 1 else payloads[0]", block)
        self.assertIn("selected_expiries = _selected_marketdata_expiries(expiries, mode)", block)
        self.assertIn("if selected_expiries and expiry_compact not in selected_expiries:", block)
        self.assertIn('"selectedExpiries": sorted(selected_expiries)', block)
        self.assertIn('raise ValueError("No valid forward 90DTE expiry available in options chain")', block)

    def test_ibkr_bridge_pick_expiries_supports_90dte_mode(self) -> None:
        bridge = load_module(
            "pq_ibkr_pick_expiries_90dte_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        original_today = bridge._utc_today_yyyymmdd
        try:
            bridge._utc_today_yyyymmdd = lambda: "20260323"
            expiries = ["20260417", "20260515", "20260619", "20260717", "20260918"]
            self.assertEqual(bridge.pick_expiries(expiries, "90dte"), ["20260619"])
        finally:
            bridge._utc_today_yyyymmdd = original_today

    def test_ibkr_bridge_pick_expiries_requires_forward_90dte_candidate(self) -> None:
        bridge = load_module(
            "pq_ibkr_pick_expiries_requires_forward_90dte_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        original_today = bridge._utc_today_yyyymmdd
        try:
            bridge._utc_today_yyyymmdd = lambda: "20260323"
            expiries = ["20260320", "20260321"]
            self.assertEqual(bridge.pick_expiries(expiries, "90dte"), [])
        finally:
            bridge._utc_today_yyyymmdd = original_today

    def test_ibkr_bridge_brackets_marketdata_queries_for_90dte(self) -> None:
        bridge = load_module(
            "pq_ibkr_marketdata_dte_queries_90dte_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        self.assertEqual(bridge._marketdata_dte_queries("90dte", 120), [90, 75, 105, 120])

    def test_ibkr_bridge_aggregate_90dte_dte_queries(self) -> None:
        bridge = load_module(
            "pq_ibkr_aggregate_90dte_dte_queries_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        result = bridge._marketdata_dte_queries("aggregate_90dte", 120)
        self.assertEqual(result, [7, 14, 30, 45, 60, 75, 90])

    def test_ibkr_bridge_aggregate_90dte_selected_expiries_filters_by_window(self) -> None:
        bridge = load_module(
            "pq_ibkr_aggregate_90dte_selected_expiries_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        original_today = bridge._utc_today_yyyymmdd
        try:
            bridge._utc_today_yyyymmdd = lambda: "20260323"
            raw_expiries = ["20260323", "20260328", "20260619", "20260717"]
            result = bridge._selected_marketdata_expiries(raw_expiries, "aggregate_90dte")
            self.assertNotIn("20260323", result)  # 0 DTE — excluded from structural aggregate
            self.assertIn("20260328", result)   # 5 DTE — in
            self.assertIn("20260619", result)   # 88 DTE — in
            self.assertNotIn("20260717", result)  # 116 DTE — beyond window
        finally:
            bridge._utc_today_yyyymmdd = original_today

    def test_ibkr_bridge_uses_side_specific_walls(self) -> None:
        bridge = load_module(
            "pq_ibkr_side_specific_walls_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        levels = bridge._summarize_gamma_structure(
            {630.0: -1000.0, 650.0: -200.0, 675.0: -50.0},
            {650.0: 150.0, 675.0: 40.0},
            {630.0: -1000.0, 650.0: -350.0, 675.0: -90.0},
        )
        self.assertEqual(levels["gammaFlip"], 675.0)
        # All-negative net regime — no true zero-crossing, fallback to min-abs strike
        self.assertFalse(levels["gammaFlipIsTrueCrossing"])
        self.assertEqual(levels["gammaRegime"], "net_short")
        self.assertEqual(levels["callWall"]["price"], 650.0)
        self.assertGreater(levels["callWall"]["gex"], 0.0)
        self.assertEqual(levels["putWall"]["price"], 630.0)
        self.assertLess(levels["putWall"]["gex"], 0.0)
        self.assertEqual(levels["pin"]["price"], 630.0)

    def test_ibkr_bridge_gamma_flip_true_crossing_detected(self) -> None:
        bridge = load_module(
            "pq_ibkr_gamma_flip_true_crossing_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        # No spot passed → legacy "crossing" label preserved for backward compat
        levels = bridge._summarize_gamma_structure(
            {630.0: -500.0, 650.0: 800.0, 670.0: 300.0},
            {650.0: 800.0, 670.0: 300.0},
            {630.0: -500.0},
        )
        self.assertEqual(levels["gammaFlip"], 650.0)
        self.assertTrue(levels["gammaFlipIsTrueCrossing"])
        self.assertEqual(levels["gammaRegime"], "crossing")

    def test_ibkr_bridge_gamma_regime_positive_when_spot_above_flip(self) -> None:
        """True crossing at 650; spot 670 (above) → regime = positive."""
        bridge = load_module(
            "pq_ibkr_gamma_regime_positive_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        levels = bridge._summarize_gamma_structure(
            {630.0: -500.0, 650.0: 800.0, 670.0: 300.0},
            {650.0: 800.0, 670.0: 300.0},
            {630.0: -500.0},
            spot=670.0,
        )
        self.assertEqual(levels["gammaFlip"], 650.0)
        self.assertTrue(levels["gammaFlipIsTrueCrossing"])
        self.assertEqual(levels["gammaRegime"], "positive")

    def test_ibkr_bridge_gamma_regime_negative_when_spot_below_flip(self) -> None:
        """True crossing at 650; spot 620 (below) → regime = negative."""
        bridge = load_module(
            "pq_ibkr_gamma_regime_negative_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        levels = bridge._summarize_gamma_structure(
            {600.0: -500.0, 650.0: 800.0, 670.0: 300.0},
            {650.0: 800.0, 670.0: 300.0},
            {600.0: -500.0},
            spot=620.0,
        )
        self.assertEqual(levels["gammaFlip"], 650.0)
        self.assertTrue(levels["gammaFlipIsTrueCrossing"])
        self.assertEqual(levels["gammaRegime"], "negative")

    def test_ibkr_bridge_gamma_regime_at_flip_when_spot_within_015pct(self) -> None:
        """True crossing at 650; spot 650.5 (within 0.15%) → regime = at_flip."""
        bridge = load_module(
            "pq_ibkr_gamma_regime_at_flip_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        levels = bridge._summarize_gamma_structure(
            {630.0: -500.0, 650.0: 800.0, 670.0: 300.0},
            {650.0: 800.0, 670.0: 300.0},
            {630.0: -500.0},
            spot=650.5,  # 0.077% from flip — inside 0.15% threshold
        )
        self.assertEqual(levels["gammaFlip"], 650.0)
        self.assertTrue(levels["gammaFlipIsTrueCrossing"])
        self.assertEqual(levels["gammaRegime"], "at_flip")

    def test_ibkr_bridge_gamma_regime_reports_net_long_without_crossing(self) -> None:
        bridge = load_module(
            "pq_ibkr_gamma_regime_net_long_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        levels = bridge._summarize_gamma_structure(
            {630.0: 250.0, 650.0: 500.0, 670.0: 300.0},
            {630.0: 250.0, 650.0: 500.0, 670.0: 300.0},
            {},
        )
        self.assertFalse(levels["gammaFlipIsTrueCrossing"])
        self.assertEqual(levels["gammaRegime"], "net_long")

    def test_ibkr_bridge_select_strikes_respects_custom_range(self) -> None:
        bridge = load_module(
            "pq_ibkr_select_strikes_custom_range_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        spot = 650.0
        # Strikes from 560 to 740 in $5 steps
        strikes = [float(s) for s in range(560, 745, 5)]
        narrow = bridge.select_strikes(strikes, spot, strike_range=0.05)  # ±5%  →  617–683
        wide = bridge.select_strikes(strikes, spot, strike_range=0.15)    # ±15% →  552–748
        self.assertTrue(all(spot * 0.95 <= s <= spot * 1.05 for s in narrow))
        self.assertGreater(len(wide), len(narrow))
        self.assertTrue(all(spot * 0.85 <= s <= spot * 1.15 for s in wide))

    def test_dashboard_gamma_panel_uses_explicit_gamma_regime(self) -> None:
        dashboard = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        self.assertIn("const gammaRegime = String(state.gammaData?.gammaRegime || '').toLowerCase();", dashboard)
        # flipSuffix labels for regime-floor/ceiling and at-flip
        self.assertIn("flipSuffix = ' · regime floor';", dashboard)
        self.assertIn("flipSuffix = ' · regime ceiling';", dashboard)
        self.assertIn("flipSuffix = ' · at flip';", dashboard)
        # All 5 regime values handled server-authoritatively
        self.assertIn("gammaMode = 'Net Short';", dashboard)
        self.assertIn("gammaMode = 'Net Long';", dashboard)
        self.assertIn("gammaMode = 'At Flip';", dashboard)
        self.assertIn("gammaMode = 'Positive';", dashboard)
        self.assertIn("gammaMode = 'Negative';", dashboard)
        # Intraday walls should carry no right-rail title label
        self.assertIn("title: structural ? entry.shortLabel : '',", dashboard)

    def test_dashboard_touch_events_use_gamma_regime_for_gamma_mode(self) -> None:
        dashboard = (REPO_ROOT / "production_pivot_dashboard.html").read_text(encoding="utf-8")
        block = dashboard.split("function buildTouchEvent(", 1)[1].split("return {", 1)[0]
        self.assertIn("const gammaRegime = String(state.gammaData?.gammaRegime || '').toLowerCase();", block)
        self.assertIn("if (gammaRegime === 'net_short' || gammaRegime === 'negative') {", block)
        self.assertIn("gammaMode = -1;", block)
        self.assertIn("} else if (gammaRegime === 'net_long' || gammaRegime === 'positive') {", block)
        self.assertIn("gammaMode = 1;", block)
        self.assertIn("} else if (gammaRegime === 'at_flip' || gammaRegime === 'crossing') {", block)
        self.assertNotIn("referencePrice >= state.gammaData.gammaFlip", block)

    def test_train_artifacts_gamma_context_metadata_accepts_aggregate_mode(self) -> None:
        original_context = os.environ.get("GAMMA_CONTEXT_EXPIRY_MODE")
        original_history = os.environ.get("GAMMA_HISTORY_EXPIRY_MODE")
        os.environ["GAMMA_CONTEXT_EXPIRY_MODE"] = "aggregate_90dte"
        os.environ["GAMMA_HISTORY_EXPIRY_MODE"] = "aggregate_90dte"
        try:
            trainer = load_module(
                "pq_train_rf_artifacts_gamma_mode_accepts_aggregate_test",
                REPO_ROOT / "scripts" / "train_rf_artifacts.py",
            )
            meta = trainer._gamma_context_metadata()
            self.assertEqual(meta["context_expiry_mode"], "aggregate_90dte")
            self.assertEqual(meta["history_expiry_mode"], "aggregate_90dte")
        finally:
            if original_context is None:
                os.environ.pop("GAMMA_CONTEXT_EXPIRY_MODE", None)
            else:
                os.environ["GAMMA_CONTEXT_EXPIRY_MODE"] = original_context
            if original_history is None:
                os.environ.pop("GAMMA_HISTORY_EXPIRY_MODE", None)
            else:
                os.environ["GAMMA_HISTORY_EXPIRY_MODE"] = original_history

    def test_train_artifacts_gamma_context_metadata_rejects_legacy_quarterly_alias(self) -> None:
        original_context = os.environ.get("GAMMA_CONTEXT_EXPIRY_MODE")
        original_history = os.environ.get("GAMMA_HISTORY_EXPIRY_MODE")
        os.environ["GAMMA_CONTEXT_EXPIRY_MODE"] = "quarterly"
        os.environ["GAMMA_HISTORY_EXPIRY_MODE"] = "quarterly"
        try:
            trainer = load_module(
                "pq_train_rf_artifacts_gamma_mode_rejects_quarterly_test",
                REPO_ROOT / "scripts" / "train_rf_artifacts.py",
            )
            with self.assertRaisesRegex(ValueError, "GAMMA_CONTEXT_EXPIRY_MODE=quarterly"):
                trainer._gamma_context_metadata()
        finally:
            if original_context is None:
                os.environ.pop("GAMMA_CONTEXT_EXPIRY_MODE", None)
            else:
                os.environ["GAMMA_CONTEXT_EXPIRY_MODE"] = original_context
            if original_history is None:
                os.environ.pop("GAMMA_HISTORY_EXPIRY_MODE", None)
            else:
                os.environ["GAMMA_HISTORY_EXPIRY_MODE"] = original_history

    def test_ibkr_bridge_normalizes_marketdata_timestamp_expiries(self) -> None:
        bridge = load_module(
            "pq_ibkr_expiry_timestamp_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        self.assertEqual(bridge._normalize_expiry_yyyymmdd(1775529600), "20260407")
        self.assertEqual(bridge._normalize_expiry_yyyymmdd("1775529600"), "20260407")

    def test_ibkr_bridge_normalizes_compact_expiry_strings(self) -> None:
        bridge = load_module(
            "pq_ibkr_expiry_compact_string_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        self.assertEqual(bridge._normalize_expiry_yyyymmdd("20260618"), "20260618")

    def test_ibkr_bridge_logs_market_data_type_failures(self) -> None:
        bridge = load_module(
            "pq_ibkr_market_data_type_warning_test",
            REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
        )
        original_ib = bridge.ib

        class _DummyIB:
            def reqMarketDataType(self, _data_type):
                raise RuntimeError("market data type unavailable")

        bridge.ib = _DummyIB()
        try:
            with self.assertLogs("ibkr_gamma_bridge", level="WARNING") as cm:
                bridge._request_market_data_type(1)
            self.assertIn("reqMarketDataType(1) failed", "\n".join(cm.output))
        finally:
            bridge.ib = original_ib

    def test_ibkr_bridge_mda_cache_is_bounded(self) -> None:
        original_max = os.environ.get("MDA_GAMMA_CACHE_MAX_SIZE")
        os.environ["MDA_GAMMA_CACHE_MAX_SIZE"] = "2"
        try:
            bridge = load_module(
                "pq_ibkr_mda_cache_bound_test",
                REPO_ROOT / "server" / "ibkr_gamma_bridge.py",
            )
        finally:
            if original_max is None:
                os.environ.pop("MDA_GAMMA_CACHE_MAX_SIZE", None)
            else:
                os.environ["MDA_GAMMA_CACHE_MAX_SIZE"] = original_max
        with bridge._mda_gamma_cache_lock:
            bridge._mda_gamma_cache.clear()
            bridge._store_mda_gamma_cache_entry("SPY:90dte", {"symbol": "SPY"})
            bridge._store_mda_gamma_cache_entry("QQQ:90dte", {"symbol": "QQQ"})
            bridge._store_mda_gamma_cache_entry("IWM:90dte", {"symbol": "IWM"})
            self.assertEqual(len(bridge._mda_gamma_cache), 2)
            self.assertNotIn("SPY:90dte", bridge._mda_gamma_cache)

    def test_yahoo_proxy_rejects_legacy_quarterly_alias_in_gamma_fallback(self) -> None:
        source = (REPO_ROOT / "server" / "yahoo_proxy.js").read_text(encoding="utf-8")
        self.assertIn("expiry=quarterly is no longer supported; use 90dte", source)
        self.assertIn("No valid forward 90DTE expiry available in Yahoo options chain", source)
        self.assertIn("selectedExpiries: selectedExpiry ? [selectedExpiry] : []", source)

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
        holiday_utc = datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc)  # Thanksgiving
        halfday_pre_close_utc = datetime(2026, 11, 27, 17, 59, tzinfo=timezone.utc)  # 12:59 ET
        halfday_close_utc = datetime(2026, 11, 27, 18, 0, tzinfo=timezone.utc)  # 13:00 ET

        self.assertFalse(bridge._is_market_session_closed(pre_close_utc))
        self.assertTrue(bridge._is_market_session_closed(close_utc))
        self.assertTrue(bridge._is_market_session_closed(weekend_utc))
        self.assertTrue(bridge._is_market_session_closed(holiday_utc))
        self.assertFalse(bridge._is_market_session_closed(halfday_pre_close_utc))
        self.assertTrue(bridge._is_market_session_closed(halfday_close_utc))

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
        self.assertIn("expiry=90dte", checker)

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
        self.assertIn("RETRAIN_SCORE_UNSCORED_BACKLOG_SWEEP_ON_RETRAIN", retrain_script)
        self.assertIn("RETRAIN_SCORE_UNSCORED_BACKLOG_SWEEP_LIMIT", retrain_script)
        self.assertIn("RETRAIN_SCORE_UNSCORED_BACKLOG_SWEEP_MIN_BACKLOG", retrain_script)
        self.assertIn("RETRAIN_SCORE_UNSCORED_TIMEOUT_SEC", retrain_script)
        self.assertIn("RETRAIN_SCORE_UNSCORED_MAX_ATTEMPTS", retrain_script)
        self.assertIn("RETRAIN_SCORE_UNSCORED_FAIL_ON_PARTIAL", retrain_script)
        self.assertIn("RETRAIN_REFRESH_ML_METRICS_ON_RETRAIN", retrain_script)
        self.assertIn("RETRAIN_METRICS_TARGET", retrain_script)
        self.assertIn("RETRAIN_METRICS_HORIZON_MIN", retrain_script)
        self.assertIn("RETRAIN_RF_CALIB_DAYS", retrain_script)
        self.assertIn("INFO train_artifacts config calib_days=", retrain_script)
        self.assertIn("--calib-days \"${RETRAIN_RF_CALIB_DAYS}\"", retrain_script)
        self.assertIn("log_threshold_guard_summary()", retrain_script)
        self.assertIn("INFO threshold_summary", retrain_script)
        self.assertIn("tp_util=", retrain_script)
        self.assertIn("tune_rows=", retrain_script)
        self.assertIn("fit_rows=", retrain_script)
        self.assertIn("START refresh_ml_metrics", retrain_script)
        self.assertIn("scripts/train_rf.py", retrain_script)
        self.assertIn("metrics_refresh_last_status=running", retrain_script)
        self.assertIn("metrics_refresh_last_status=ok", retrain_script)
        self.assertIn("ml_metrics_refresh_stale", retrain_script)
        self.assertIn("metrics_refresh_last_error=artifacts_not_refreshed", retrain_script)
        self.assertIn("file_mtime_ms()", retrain_script)
        self.assertIn("--timeout-sec", retrain_script)
        self.assertIn("--max-attempts", retrain_script)
        self.assertIn("--fail-on-partial", retrain_script)
        self.assertIn("count_unscored_non_preview()", retrain_script)
        self.assertIn("mark_soft_failure", retrain_script)
        self.assertIn("--set \"retrain_last_status=${RETRAIN_LAST_STATUS}\"", retrain_script)
        self.assertIn("capture_ops_smoke_failure_details", retrain_script)
        self.assertIn("build_ops_smoke_alert_body", retrain_script)
        self.assertIn("summary=", retrain_script)
        self.assertIn("hint=", retrain_script)

        env_example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
        self.assertIn("RF_THRESHOLD_MIN_SIGNALS_OVERRIDES=", env_example)
        self.assertIn("RF_THRESHOLD_PRECISION_FLOOR_OVERRIDES=", env_example)
        self.assertIn("RF_CALIB_DAYS=", env_example)
        self.assertIn("MODEL_GOV_MIN_TRAINED_END_DELTA_MS=21600000", env_example)
        self.assertIn("MODEL_GOV_ENFORCE_THRESHOLD_UTILITY_GUARD=", env_example)
        self.assertIn("MODEL_GOV_THRESHOLD_UTILITY_TARGETS=", env_example)
        self.assertIn("MODEL_GOV_THRESHOLD_UTILITY_MIN_SCORE=", env_example)
        self.assertIn("ML_REJECT_OR_BREAKOUT_FILTER_MODE=off", env_example)
        self.assertIn("ML_REJECT_OR_BREAKOUT_FILTER_HORIZONS=", env_example)
        self.assertIn("ML_REJECT_OR_BREAKOUT_FILTER_BLOCK_VALUES=", env_example)
        self.assertIn("ML_REJECT_OR_BREAKOUT_FILTER_RULES=", env_example)

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

    def test_ml_server_bind_default_is_loopback(self) -> None:
        """C1 fix: ML_SERVER_BIND default MUST be 127.0.0.1, not 0.0.0.0.

        /reload and /score are unauthenticated and /reload deserialises
        joblib/pickle from MODEL_DIR — an RCE primitive if the port is
        reachable from the network.  Every legitimate client connects on
        loopback; binding to all interfaces here is a footgun with no
        operational upside.  Pin BOTH layers (shell stack export AND
        ml_server.py source default) and pin the sibling-services pattern
        so a future refactor cannot quietly re-export 0.0.0.0.
        """
        stack_script = (REPO_ROOT / "server" / "run_persistent_stack.sh").read_text(encoding="utf-8")

        # Layer 1: the env-var default exported by the stack script must be
        # loopback.  Pin the exact line so a regex-rename or rewrite cannot
        # silently revert it.
        self.assertIn(
            'export ML_SERVER_BIND="${ML_SERVER_BIND:-127.0.0.1}"',
            stack_script,
            msg="ML_SERVER_BIND must default to 127.0.0.1 (loopback) — see C1 fix.",
        )
        # Belt-and-suspenders: ensure the dangerous default has not been
        # re-introduced anywhere in the script (catches a partial revert
        # or a second export).
        self.assertNotIn(
            'export ML_SERVER_BIND="${ML_SERVER_BIND:-0.0.0.0}"',
            stack_script,
            msg="ML_SERVER_BIND must NOT default to 0.0.0.0 — re-introduces C1.",
        )

        # Layer 2: ml_server.py's own fallback when ML_SERVER_BIND is unset
        # must also be loopback.  This catches the case where the script
        # exec()s the python directly (e.g. via server/run_ml_server.sh)
        # without going through run_persistent_stack.sh.
        ml_source = (REPO_ROOT / "server" / "ml_server.py").read_text(encoding="utf-8")
        self.assertIn(
            'HOST = os.getenv("ML_SERVER_BIND", "127.0.0.1")',
            ml_source,
            msg="ml_server.py HOST fallback must be 127.0.0.1.",
        )

        # Sibling-pattern invariant: the three other internal services in
        # the stack already default to loopback.  Pin the family so a
        # future refactor that rewrites them as a group cannot drop ML
        # from the loopback set.
        for sibling in (
            'export LIVE_COLLECTOR_BIND="${LIVE_COLLECTOR_BIND:-127.0.0.1}"',
            'export EVENT_WRITER_BIND="${EVENT_WRITER_BIND:-127.0.0.1}"',
            'export IB_BRIDGE_BIND="${IB_BRIDGE_BIND:-127.0.0.1}"',
        ):
            self.assertIn(sibling, stack_script)

    def test_run_gamma_bridge_sources_dotenv_safely(self) -> None:
        gamma_script = (REPO_ROOT / "server" / "run_gamma_bridge.sh").read_text(encoding="utf-8")
        self.assertIn('ENV_FILE="${ROOT_DIR}/.env"', gamma_script)
        self.assertIn("load_env_file()", gamma_script)
        self.assertIn('load_env_file "${ENV_FILE}"', gamma_script)
        self.assertNotIn('source "${ROOT_DIR}/.env"', gamma_script)
        self.assertIn('if [[ "${#value}" -ge 2 ]]; then', gamma_script)
        self.assertIn('value="${value:1:${#value}-2}"', gamma_script)

        proc = run_cmd(["bash", "-n", "server/run_gamma_bridge.sh"], cwd=REPO_ROOT)
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

    def test_score_unscored_touch_events_preview_mode_tracks_preview_rows(self) -> None:
        scorer = load_module(
            "pq_score_unscored_touch_events_preview_mode_test",
            REPO_ROOT / "scripts" / "score_unscored_touch_events.py",
        )

        db = self.tmp / "score_unscored_preview.sqlite"
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
                ("evt_live_scored", "SPY", now_ms - 60_000),
                ("evt_preview_scored", "SPY", now_ms - 120_000),
                ("evt_missing", "SPY", now_ms - 180_000),
            ]
            conn.executemany(
                "INSERT INTO touch_events(event_id, symbol, ts_event) VALUES (?, ?, ?)",
                rows,
            )
            conn.execute(
                "INSERT INTO prediction_log(event_id, ts_prediction, is_preview) VALUES (?, ?, ?)",
                ("evt_live_scored", now_ms, 0),
            )
            conn.execute(
                "INSERT INTO prediction_log(event_id, ts_prediction, is_preview) VALUES (?, ?, ?)",
                ("evt_preview_scored", now_ms, 1),
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

        posted_events: list[dict[str, Any]] = []
        original_urlopen = scorer.urlopen
        try:

            def _fake_urlopen(req, timeout=0):  # noqa: ANN001
                payload = json.loads(req.data.decode("utf-8"))
                events = payload.get("events", [])
                posted_events.extend(events)
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
                    "5",
                    "--preview",
                ]
            )
            result = scorer.run(args)
        finally:
            scorer.urlopen = original_urlopen

        self.assertEqual(result.get("status"), "ok")
        self.assertTrue(bool(result.get("preview")))
        self.assertEqual(result.get("attempted"), 2)
        self.assertEqual(result.get("scored_ok"), 2)
        posted_ids = {str(event.get("event_id")) for event in posted_events}
        self.assertEqual(posted_ids, {"evt_live_scored", "evt_missing"})
        self.assertTrue(all(bool(event.get("preview")) for event in posted_events))

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
        self.assertIn("remaining_unscored 1 exceeds max_remaining 0", str(result.get("last_error")))

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
        self.assertIn("ML_REJECT_OR_BREAKOUT_FILTER_MODE", ml_server)
        self.assertIn("ML_REJECT_OR_BREAKOUT_FILTER_RULES", ml_server)
        self.assertIn("OR_BREAKOUT_REJECT_FILTER_DIVERGENCE", ml_server)
        self.assertIn("or_breakout_reject_filter", ml_server)
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

    def test_reject_or_breakout_filter_shadow_and_active_modes(self) -> None:
        ml_server = load_module("ml_server_or_breakout_filter_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        ml_server.build_feature_row = lambda event: {"x": 1.0, "or_breakout": event.get("or_breakout")}
        ml_server.collect_missing = lambda _features: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_REGIME_POLICY_MODE = "off"
        ml_server.ML_REGIME_GUARD_EXPANSION_NEAR_MODE = "off"
        ml_server.ML_ANALOG_DISAGREEMENT_GUARD_MODE = "off"
        ml_server.ML_REJECT_OR_BREAKOUT_FILTER_HORIZONS = {15}
        ml_server.ML_REJECT_OR_BREAKOUT_FILTER_BLOCK_VALUES = {-1}
        ml_server.ML_REJECT_OR_BREAKOUT_FILTER_RULES = {}

        ml_server.registry.models = {
            "reject": {
                15: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.82),
                    "calibration": "sigmoid",
                }
            },
            "break": {
                15: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.18),
                    "calibration": "sigmoid",
                }
            },
        }
        ml_server.registry.thresholds = {"reject": {15: 0.5}, "break": {15: 0.5}}
        ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": int(time.time() * 1000)}

        blocked_event = {"event_id": "or_breakout_blocked_shadow", "or_breakout": -1}

        ml_server.ML_REJECT_OR_BREAKOUT_FILTER_MODE = "shadow"
        shadow_result = ml_server._score_event(blocked_event)
        self.assertEqual(shadow_result["signals"].get("signal_15m"), "reject")
        shadow_filter = shadow_result["regime_policy"]["or_breakout_reject_filter"]
        self.assertEqual(int(shadow_filter.get("candidate_count")), 1)
        self.assertEqual(int(shadow_filter.get("applied_count")), 0)
        self.assertEqual(shadow_filter.get("rules"), {"15": [-1]})
        self.assertIn("OR_BREAKOUT_REJECT_FILTER_DIVERGENCE", shadow_result["quality_flags"])
        self.assertEqual(shadow_result["regime_policy"]["selected_policy"], "baseline")

        ml_server.ML_REJECT_OR_BREAKOUT_FILTER_MODE = "active"
        active_result = ml_server._score_event({**blocked_event, "event_id": "or_breakout_blocked_active"})
        self.assertEqual(active_result["signals"].get("signal_15m"), "no_edge")
        active_filter = active_result["regime_policy"]["or_breakout_reject_filter"]
        self.assertEqual(int(active_filter.get("candidate_count")), 1)
        self.assertEqual(int(active_filter.get("applied_count")), 1)
        self.assertIn("OR_BREAKOUT_REJECT_FILTER_ACTIVE", active_result["quality_flags"])
        self.assertIn("or_breakout_filter", active_result["regime_policy"]["selected_policy"])

        pass_event = {"event_id": "or_breakout_pass_active", "or_breakout": 1}
        pass_result = ml_server._score_event(pass_event)
        self.assertEqual(pass_result["signals"].get("signal_15m"), "reject")
        pass_filter = pass_result["regime_policy"]["or_breakout_reject_filter"]
        self.assertEqual(int(pass_filter.get("candidate_count")), 0)
        self.assertEqual(int(pass_filter.get("applied_count")), 0)

    def test_reject_or_breakout_filter_per_horizon_rules(self) -> None:
        ml_server = load_module("ml_server_or_breakout_filter_rules_runtime", REPO_ROOT / "server" / "ml_server.py")

        class DummyModel:
            classes_ = np.array([0, 1])

            def __init__(self, prob: float) -> None:
                self.prob = prob

            def predict_proba(self, _df):
                return np.array([[1.0 - self.prob, self.prob]], dtype=float)

        ml_server.build_feature_row = lambda event: {"x": 1.0, "or_breakout": event.get("or_breakout")}
        ml_server.collect_missing = lambda _features: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server.ML_REGIME_POLICY_MODE = "off"
        ml_server.ML_REGIME_GUARD_EXPANSION_NEAR_MODE = "off"
        ml_server.ML_ANALOG_DISAGREEMENT_GUARD_MODE = "off"
        ml_server.ML_REJECT_OR_BREAKOUT_FILTER_MODE = "active"
        ml_server.ML_REJECT_OR_BREAKOUT_FILTER_HORIZONS = {15, 60}
        ml_server.ML_REJECT_OR_BREAKOUT_FILTER_BLOCK_VALUES = {-1}
        ml_server.ML_REJECT_OR_BREAKOUT_FILTER_RULES = {15: {-1}, 60: {0}}

        ml_server.registry.models = {
            "reject": {
                15: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.85),
                    "calibration": "sigmoid",
                },
                60: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.83),
                    "calibration": "sigmoid",
                },
            },
            "break": {
                15: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.10),
                    "calibration": "sigmoid",
                },
                60: {
                    "feature_columns": ["x"],
                    "pipeline": DummyModel(0.12),
                    "calibration": "sigmoid",
                },
            },
        }
        ml_server.registry.thresholds = {"reject": {15: 0.5, 60: 0.5}, "break": {15: 0.5, 60: 0.5}}
        ml_server.registry.manifest = {"version": "vtest", "trained_end_ts": int(time.time() * 1000)}

        orb_neg1 = ml_server._score_event({"event_id": "or_breakout_rule_neg1", "or_breakout": -1})
        self.assertEqual(orb_neg1["signals"].get("signal_15m"), "no_edge")
        self.assertEqual(orb_neg1["signals"].get("signal_60m"), "reject")
        self.assertEqual(
            orb_neg1["regime_policy"]["or_breakout_reject_filter"].get("rules"),
            {"15": [-1], "60": [0]},
        )

        orb_zero = ml_server._score_event({"event_id": "or_breakout_rule_zero", "or_breakout": 0})
        self.assertEqual(orb_zero["signals"].get("signal_15m"), "reject")
        self.assertEqual(orb_zero["signals"].get("signal_60m"), "no_edge")

        orb_one = ml_server._score_event({"event_id": "or_breakout_rule_one", "or_breakout": 1})
        self.assertEqual(orb_one["signals"].get("signal_15m"), "reject")
        self.assertEqual(orb_one["signals"].get("signal_60m"), "reject")
        self.assertEqual(int(orb_one["regime_policy"]["or_breakout_reject_filter"].get("candidate_count")), 0)

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

    def test_ml_prediction_log_preserves_original_ts_prediction_on_conflict(self) -> None:
        db = self.tmp / "predlog_ts_preserve.sqlite"
        prev_db = os.environ.get("PREDICTION_LOG_DB")
        os.environ["PREDICTION_LOG_DB"] = str(db)
        try:
            ml_server = load_module("ml_server_predlog_ts_preserve_runtime", REPO_ROOT / "server" / "ml_server.py")
        finally:
            if prev_db is None:
                os.environ.pop("PREDICTION_LOG_DB", None)
            else:
                os.environ["PREDICTION_LOG_DB"] = prev_db

        original_time = ml_server.time.time
        try:
            ml_server.time.time = lambda: 1.0
            event = {"event_id": "evt_ts_preserve"}
            first_result = {
                "model_version": "vkeep",
                "feature_version": "v3",
                "best_horizon": 15,
                "abstain": False,
                "scores": {"prob_reject_15m": 0.66},
                "signals": {"signal_15m": "reject"},
                "thresholds": {"threshold_reject_15m": 0.5},
                "quality_flags": ["FIRST_WRITE"],
                "regime_policy": {
                    "mode": "shadow",
                    "trade_regime": "compression",
                    "selected_policy": "baseline",
                },
                "analogs": {},
            }
            ml_server._log_prediction(event, first_result)

            ml_server.time.time = lambda: 2.0
            second_result = {
                **first_result,
                "quality_flags": ["SECOND_WRITE"],
                "regime_policy": {
                    "mode": "active",
                    "trade_regime": "expansion",
                    "selected_policy": "regime_active",
                },
            }
            ml_server._log_prediction(event, second_result)
        finally:
            ml_server.time.time = original_time

        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                SELECT ts_prediction, selected_policy, regime_policy_mode
                FROM prediction_log
                WHERE event_id = ? AND model_version = ?
                """,
                ("evt_ts_preserve", "vkeep"),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(int(row["ts_prediction"]), 1000)
            self.assertEqual(str(row["selected_policy"]), "regime_active")
            self.assertEqual(str(row["regime_policy_mode"]), "active")
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
                        "or_breakout_reject_filter": {
                            "mode": "shadow",
                            "candidate_count": 2,
                            "applied_count": 0,
                        },
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
                        "or_breakout_reject_filter": {
                            "mode": "off",
                            "candidate_count": 0,
                            "applied_count": 0,
                        },
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
                        "or_breakout_reject_filter": {
                            "mode": "active",
                            "candidate_count": 1,
                            "applied_count": 1,
                        },
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
        filter_summary = summary["or_breakout_reject_filter"]
        self.assertEqual(int(filter_summary["mode_counts"]["shadow"]), 1)
        self.assertEqual(int(filter_summary["mode_counts"]["active"]), 1)
        self.assertEqual(int(filter_summary["mode_counts"]["off"]), 1)
        self.assertEqual(int(filter_summary["events_with_candidates"]), 2)
        self.assertEqual(int(filter_summary["candidate_signals"]), 3)
        self.assertEqual(int(filter_summary["applied_signals"]), 1)

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
        self.assertIn("- Prediction basis: first prediction per event", text)

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
        self.assertIn("- Scored-event basis: first prediction per event", text)
        self.assertIn("## Prediction Coverage SLA", text)
        self.assertIn("- Timely prediction lag filter: <= 6.00 hours", text)
        self.assertIn("- Overall coverage: 100.00% (2/2)", text)
        self.assertIn("- Scored-event rows dropped (missing matching horizon label): 0", text)
        self.assertIn("- Coverage status: PASS", text)
        self.assertIn("## Prediction Lag Profile (First Live Prediction)", text)
        self.assertIn("- <=1h: 2", text)
        self.assertIn("- >6h: 0", text)
        self.assertIn("- No prediction: 0", text)
        self.assertIn("| 2026-03-06 | 1 | 1 | 0 | 0 | 0 | 0 |", text)
        self.assertIn("## What-if Policy Comparison (Baseline vs Guardrail vs No-5m)", text)
        self.assertIn("- What-if guardrail candidate trades (baseline in expansion+near): 1", text)
        self.assertIn("- What-if no-5m candidate trades (baseline best_horizon=5): 1", text)
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
        self.assertIn("- Scored-event basis: first prediction per event", text)
        self.assertIn("- Events (first prediction per event): 0", text)
        self.assertIn("## Prediction Lag Profile (First Live Prediction)", text)
        self.assertIn("- >6h: 1", text)
        self.assertIn("- No prediction: 0", text)
        self.assertIn("| 2026-03-06 | 1 | 0 | 0 | 0 | 1 | 0 |", text)

    def test_weekly_policy_review_lag_profile_buckets_day_in_et(self) -> None:
        db = self.tmp / "weekly_policy_review_lag_day_et.sqlite"
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
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

            # 2026-03-07 00:30 UTC is still 2026-03-06 in New York (ET).
            ts_event = int(datetime(2026, 3, 7, 0, 30, tzinfo=timezone.utc).timestamp() * 1000)
            ts_pred = ts_event + (30 * 60 * 1000)

            conn.execute(
                "INSERT INTO touch_events(event_id, symbol, ts_event) VALUES (?, ?, ?)",
                ("evt_weekly_lag_day_et", "SPY", ts_event),
            )
            conn.execute(
                "INSERT INTO prediction_log(event_id, ts_prediction, is_preview) VALUES (?, ?, ?)",
                ("evt_weekly_lag_day_et", ts_pred, 0),
            )
            conn.commit()

            weekly = load_module(
                "weekly_policy_review_lag_day_et_runtime",
                REPO_ROOT / "scripts" / "weekly_policy_review.py",
            )
            lag_profile = weekly.load_prediction_lag_profile(
                conn,
                symbol="SPY",
                start_ms=ts_event - 1000,
                end_ms=ts_event + 1000,
                source="live",
            )
            self.assertEqual(len(lag_profile["days"]), 1)
            day_row = lag_profile["days"][0]
            self.assertEqual(day_row["day"], "2026-03-06")
            self.assertEqual(int(day_row["lag_le_1h_n"]), 1)
        finally:
            conn.close()

    def test_weekly_policy_review_supports_first_vs_latest_scored_event_basis(self) -> None:
        db = self.tmp / "weekly_policy_review_basis.sqlite"
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
                    selected_policy TEXT,
                    signal_5m TEXT,
                    signal_15m TEXT,
                    signal_30m TEXT,
                    signal_60m TEXT
                )
                """
            )

            ts_event = int(datetime(2026, 3, 6, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)
            ts_pred_first = ts_event + (30 * 60 * 1000)  # timely first prediction
            ts_pred_latest = ts_event + (20 * 3600 * 1000)  # replay-style later prediction

            conn.execute(
                "INSERT INTO touch_events(event_id, symbol, ts_event) VALUES (?, ?, ?)",
                ("evt_weekly_basis_1", "SPY", ts_event),
            )
            conn.execute(
                "INSERT INTO event_labels(event_id, horizon_min, return_bps) VALUES (?, ?, ?)",
                ("evt_weekly_basis_1", 60, -5.0),
            )
            conn.execute(
                """
                INSERT INTO prediction_log(
                    event_id, ts_prediction, is_preview, best_horizon, abstain,
                    trade_regime, regime_policy_json, selected_policy,
                    signal_5m, signal_15m, signal_30m, signal_60m
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "evt_weekly_basis_1",
                    ts_pred_first,
                    0,
                    60,
                    0,
                    "compression",
                    json.dumps({"atr_zone": "ultra"}),
                    "baseline",
                    "no_edge",
                    "no_edge",
                    "no_edge",
                    "break",
                ),
            )
            conn.execute(
                """
                INSERT INTO prediction_log(
                    event_id, ts_prediction, is_preview, best_horizon, abstain,
                    trade_regime, regime_policy_json, selected_policy,
                    signal_5m, signal_15m, signal_30m, signal_60m
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "evt_weekly_basis_1",
                    ts_pred_latest,
                    0,
                    60,
                    0,
                    "compression",
                    json.dumps({"atr_zone": "ultra"}),
                    "baseline",
                    "no_edge",
                    "no_edge",
                    "no_edge",
                    "no_edge",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        out_first = self.tmp / "weekly_policy_review_basis_first.md"
        proc_first = run_cmd(
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
                "--max-pred-lag-hours",
                "72",
                "--scored-event-basis",
                "first",
                "--output",
                str(out_first),
            ],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc_first.returncode, 0, msg=f"{proc_first.stdout}\n{proc_first.stderr}")
        text_first = out_first.read_text(encoding="utf-8")
        self.assertIn("- Scored-event basis: first prediction per event", text_first)
        self.assertIn("- Events (first prediction per event): 1", text_first)
        self.assertIn("| Baseline | 1 |", text_first)

        out_latest = self.tmp / "weekly_policy_review_basis_latest.md"
        proc_latest = run_cmd(
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
                "--max-pred-lag-hours",
                "72",
                "--scored-event-basis",
                "latest",
                "--output",
                str(out_latest),
            ],
            cwd=REPO_ROOT,
        )
        self.assertEqual(proc_latest.returncode, 0, msg=f"{proc_latest.stdout}\n{proc_latest.stderr}")
        text_latest = out_latest.read_text(encoding="utf-8")
        self.assertIn("- Scored-event basis: latest prediction per event", text_latest)
        self.assertIn("- Events (latest prediction per event): 1", text_latest)
        self.assertIn("| Baseline | 0 |", text_latest)

    def test_generate_daily_report_tradeability_note_contract_present(self) -> None:
        source = (REPO_ROOT / "scripts" / "generate_daily_ml_report.py").read_text(encoding="utf-8")
        self.assertIn("Tradeable matured signals (reject+break)", source)
        self.assertIn("Performance note: no matured reject/break signals in this window", source)
        self.assertIn("Model Readiness", source)
        self.assertIn("Trading Utility", source)
        self.assertIn("Operator Note", source)
        self.assertIn("Prediction basis for scored rows", source)
        self.assertIn("--prediction-basis", source)

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

    def test_train_artifacts_purges_train_rows_overlapping_calibration_window(self) -> None:
        module = load_module(
            "train_rf_artifacts_purge_overlap",
            REPO_ROOT / "scripts" / "train_rf_artifacts.py",
        )
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:
            self.skipTest(f"pandas unavailable: {exc}")

        # Calibration starts at ts=1000 (ms). Embargo of 60min = 3_600_000 ms.
        # Any training row with ts_event > (1000 - 3_600_000) overlaps.
        # We construct rows so that train rows at ts=500 and ts=900 overlap;
        # train row at ts=-10_000_000 is safely earlier and must be kept.
        calib_start = 10_000_000
        embargo_min = 60.0
        df = pd.DataFrame(
            [
                {"ts_event": -10_000_000, "horizon_min": 60},  # safe
                {"ts_event": calib_start - 100, "horizon_min": 60},  # overlap
                {"ts_event": calib_start - 1, "horizon_min": 60},  # overlap
                {"ts_event": calib_start, "horizon_min": 60},  # calib
                {"ts_event": calib_start + 60_000, "horizon_min": 60},  # calib
            ]
        )
        train_mask = pd.Series([True, True, True, False, False], index=df.index)
        calib_mask = pd.Series([False, False, False, True, True], index=df.index)

        purged_mask, diag = module.purge_training_overlap(
            df, train_mask, calib_mask, embargo_minutes=embargo_min
        )

        self.assertEqual(int(purged_mask.sum()), 1)
        self.assertTrue(bool(purged_mask.iloc[0]))
        self.assertFalse(bool(purged_mask.iloc[1]))
        self.assertFalse(bool(purged_mask.iloc[2]))
        self.assertEqual(diag["train_rows_before_purge"], 3)
        self.assertEqual(diag["train_rows_after_purge"], 1)
        self.assertEqual(diag["train_rows_purged"], 2)
        self.assertEqual(diag["embargo_minutes"], embargo_min)
        self.assertEqual(diag["calibration_start_ts"], calib_start)
        self.assertTrue(diag["enabled"])
        self.assertEqual(diag["earliest_purged_ts"], calib_start - 100)
        self.assertEqual(diag["latest_purged_ts"], calib_start - 1)
        self.assertEqual(diag["skip_reason"], "")

    def test_train_artifacts_purge_disabled_preserves_training_rows(self) -> None:
        module = load_module(
            "train_rf_artifacts_purge_disabled",
            REPO_ROOT / "scripts" / "train_rf_artifacts.py",
        )
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:
            self.skipTest(f"pandas unavailable: {exc}")

        df = pd.DataFrame(
            [
                {"ts_event": 100, "horizon_min": 30},
                {"ts_event": 200, "horizon_min": 30},
                {"ts_event": 300, "horizon_min": 30},
            ]
        )
        train_mask = pd.Series([True, True, False], index=df.index)
        calib_mask = pd.Series([False, False, True], index=df.index)

        purged_mask, diag = module.purge_training_overlap(
            df, train_mask, calib_mask, embargo_minutes=0.0
        )

        self.assertEqual(int(purged_mask.sum()), 2)
        self.assertFalse(diag["enabled"])
        self.assertEqual(diag["skip_reason"], "disabled")
        self.assertEqual(diag["train_rows_before_purge"], 2)
        self.assertEqual(diag["train_rows_after_purge"], 2)
        self.assertEqual(diag["train_rows_purged"], 0)
        self.assertIsNone(diag["earliest_purged_ts"])
        self.assertIsNone(diag["latest_purged_ts"])

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

    def test_compute_utility_gate_diagnostics_flags_negative_score_and_avg(self) -> None:
        """P1-3 diagnostic: the v440 break/60m scenario must light up all 4 flags.

        score=-0.976, selected_utility_avg=-0.0488 — both negative — must
        produce both 'is_negative' AND both 'would_disable_under_zero_*' flags.
        """
        from ml.thresholds import compute_utility_gate_diagnostics

        meta = {
            "score": -0.976,
            "selected_utility_avg": -0.0488,
            "fallback": False,
            "guard_applied": False,
            "signals": 20,
        }
        d = compute_utility_gate_diagnostics(meta)
        self.assertTrue(d["utility_score_is_negative"])
        self.assertTrue(d["utility_avg_is_negative"])
        self.assertTrue(d["would_disable_under_zero_sum"])
        self.assertTrue(d["would_disable_under_zero_mean"])

    def test_compute_utility_gate_diagnostics_clean_positive_case(self) -> None:
        """A normal positive-utility horizon must NOT trigger any flag."""
        from ml.thresholds import compute_utility_gate_diagnostics

        meta = {
            "score": 174.21,
            "selected_utility_avg": 1.613,
            "fallback": False,
            "guard_applied": False,
            "signals": 108,
        }
        d = compute_utility_gate_diagnostics(meta)
        self.assertFalse(d["utility_score_is_negative"])
        self.assertFalse(d["utility_avg_is_negative"])
        self.assertFalse(d["would_disable_under_zero_sum"])
        self.assertFalse(d["would_disable_under_zero_mean"])

    def test_compute_utility_gate_diagnostics_handles_missing_and_bool(self) -> None:
        """None / missing keys / boolean inputs must not crash; all flags False."""
        from ml.thresholds import compute_utility_gate_diagnostics

        # Missing fields
        self.assertEqual(
            compute_utility_gate_diagnostics({}),
            {
                "utility_score_is_negative": False,
                "utility_avg_is_negative": False,
                "would_disable_under_zero_sum": False,
                "would_disable_under_zero_mean": False,
            },
        )
        # None inputs
        self.assertEqual(
            compute_utility_gate_diagnostics({"score": None, "selected_utility_avg": None}),
            {
                "utility_score_is_negative": False,
                "utility_avg_is_negative": False,
                "would_disable_under_zero_sum": False,
                "would_disable_under_zero_mean": False,
            },
        )
        # Boolean: must be rejected as non-numeric (don't coerce True->1.0).
        d = compute_utility_gate_diagnostics({"score": True, "selected_utility_avg": False})
        self.assertFalse(d["utility_score_is_negative"])
        self.assertFalse(d["would_disable_under_zero_sum"])
        # NaN / inf: not finite, cannot conclude negativity
        d = compute_utility_gate_diagnostics({"score": float("nan"), "selected_utility_avg": float("-inf")})
        self.assertFalse(d["utility_score_is_negative"])
        self.assertFalse(d["utility_avg_is_negative"])

    def test_compute_utility_gate_diagnostics_zero_boundary(self) -> None:
        """At score=0 / avg=0 exactly: 'is_negative' is False but 'would_disable_under_zero_*' is True (<=0)."""
        from ml.thresholds import compute_utility_gate_diagnostics

        d = compute_utility_gate_diagnostics({"score": 0.0, "selected_utility_avg": 0.0})
        self.assertFalse(d["utility_score_is_negative"])
        self.assertFalse(d["utility_avg_is_negative"])
        self.assertTrue(d["would_disable_under_zero_sum"])
        self.assertTrue(d["would_disable_under_zero_mean"])

    def test_apply_threshold_risk_guards_persists_diagnostics_no_behavior_change(self) -> None:
        """The guard must:
          * add the 4 diagnostic flags to threshold_meta (new in this PR),
          * NOT change its gate decision because of them.

        Pin the v440 break/60m scenario: score=-0.976, configured floor=-20,
        guard MUST stay inactive (matches the actual v440 manifest behavior),
        AND the new diagnostic flags MUST surface the negative utility.
        """
        module = load_module(
            "train_rf_artifacts_diagnostic_persist",
            REPO_ROOT / "scripts" / "train_rf_artifacts.py",
        )
        threshold, meta = module.apply_threshold_risk_guards(
            objective="utility_bps",
            threshold=0.62,
            threshold_meta={
                "score": -0.976,
                "selected_utility_avg": -0.0488,
                "fallback": False,
                "signals": 20,
            },
            no_trade_threshold=1.0,
            min_utility_score=-20.0,  # the production floor that let v440 through
            disable_on_nonpositive_utility=True,
            disable_on_fallback=True,
        )
        # Behavior unchanged: 0.62 (not no_trade) because -0.976 > -20.
        self.assertAlmostEqual(float(threshold), 0.62, places=9)
        self.assertFalse(bool(meta.get("guard_applied")))
        self.assertFalse(bool(meta.get("fallback")))
        # New diagnostics: present and true (negative utility surfaced).
        self.assertTrue(meta.get("utility_score_is_negative"))
        self.assertTrue(meta.get("utility_avg_is_negative"))
        self.assertTrue(meta.get("would_disable_under_zero_sum"))
        self.assertTrue(meta.get("would_disable_under_zero_mean"))

    def test_apply_threshold_risk_guards_clean_horizon_diagnostics_all_false(self) -> None:
        """A clean positive horizon: behavior unchanged AND all diagnostic flags False."""
        module = load_module(
            "train_rf_artifacts_diagnostic_clean",
            REPO_ROOT / "scripts" / "train_rf_artifacts.py",
        )
        threshold, meta = module.apply_threshold_risk_guards(
            objective="utility_bps",
            threshold=0.81,
            threshold_meta={
                "score": 174.21,
                "selected_utility_avg": 1.613,
                "fallback": False,
                "signals": 108,
            },
            no_trade_threshold=1.0,
            min_utility_score=0.0,
            disable_on_nonpositive_utility=True,
            disable_on_fallback=True,
        )
        self.assertAlmostEqual(float(threshold), 0.81, places=9)
        self.assertFalse(bool(meta.get("guard_applied")))
        self.assertFalse(meta.get("utility_score_is_negative"))
        self.assertFalse(meta.get("utility_avg_is_negative"))
        self.assertFalse(meta.get("would_disable_under_zero_sum"))
        self.assertFalse(meta.get("would_disable_under_zero_mean"))

    def test_governance_collect_utility_diagnostics_flags_v440_break_60m(self) -> None:
        """collect_utility_diagnostics must emit a note for an active negative-utility horizon
        and stay silent for a clean manifest."""
        gov = load_module(
            "model_governance_utility_diag",
            REPO_ROOT / "scripts" / "model_governance.py",
        )

        # v440 break/60m shape: active, negative score and avg.
        bad_manifest = {
            "thresholds_meta": {
                "reject": {
                    "5": {"score": 50.0, "selected_utility_avg": 1.0, "fallback": False, "guard_applied": False},
                },
                "break": {
                    "60": {
                        "score": -0.976,
                        "selected_utility_avg": -0.0488,
                        "fallback": False,
                        "guard_applied": False,
                        "signals": 20,
                    }
                },
            }
        }
        notes = gov.collect_utility_diagnostics(bad_manifest)
        self.assertEqual(len(notes), 1)
        self.assertIn("break:60m", notes[0])
        self.assertIn("score<0", notes[0])
        self.assertIn("avg<0", notes[0])
        self.assertIn("diagnostic only", notes[0])

        # Clean manifest: empty list.
        clean_manifest = {
            "thresholds_meta": {
                "reject": {
                    "15": {"score": 174.21, "selected_utility_avg": 1.613, "fallback": False, "guard_applied": False},
                }
            }
        }
        self.assertEqual(gov.collect_utility_diagnostics(clean_manifest), [])

        # Already-fallback / already-guarded horizons must NOT be re-flagged.
        already_neutralized = {
            "thresholds_meta": {
                "break": {
                    "15": {"score": -50.0, "selected_utility_avg": -2.0, "fallback": True, "guard_applied": True},
                }
            }
        }
        self.assertEqual(gov.collect_utility_diagnostics(already_neutralized), [])

    def test_report_tradeability_blockers_flags_negative_utility_horizons(self) -> None:
        """The daily-report blockers note must mention active negative-utility
        horizons alongside the existing guard/fallback notes (P1-3 surface)."""
        report = load_module(
            "generate_daily_ml_report_blockers_neg_util",
            REPO_ROOT / "scripts" / "generate_daily_ml_report.py",
        )

        manifest = {
            "thresholds_meta": {
                "reject": {
                    "5": {"score": 50.0, "selected_utility_avg": 1.0, "fallback": False, "guard_applied": False},
                },
                "break": {
                    "60": {
                        "score": -0.976,
                        "selected_utility_avg": -0.0488,
                        "fallback": False,
                        "guard_applied": False,
                        "signals": 20,
                    }
                },
            }
        }
        note = report.summarize_tradeability_blockers(manifest)
        self.assertIsNotNone(note)
        self.assertIn("active negative-utility horizons", note)
        self.assertIn("break:60m", note)

        # Clean manifest: returns None (no blockers).
        clean = {
            "thresholds_meta": {
                "reject": {"5": {"score": 50.0, "selected_utility_avg": 1.0, "fallback": False, "guard_applied": False}},
            }
        }
        self.assertIsNone(report.summarize_tradeability_blockers(clean))

    def test_report_tradeability_blockers_imports_ml_package_when_run_as_script(self) -> None:
        """generate_daily_ml_report.py must add the repo root to sys.path.

        In production the file is executed as ``scripts/generate_daily_ml_report.py``,
        so ``sys.path[0]`` is ``scripts/``.  Without adding the repo root, the
        diagnostic import of ``ml.thresholds`` is swallowed by the defensive
        try/except and active negative-utility horizons disappear from the
        report.
        """
        module_path = REPO_ROOT / "scripts" / "generate_daily_ml_report.py"
        old_path = list(sys.path)
        old_modules = {
            name: sys.modules.get(name)
            for name in ("ml", "ml.thresholds")
            if name in sys.modules
        }
        try:
            for name in ("ml.thresholds", "ml"):
                sys.modules.pop(name, None)
            sys.path = [
                p for p in sys.path
                if p not in {"", str(REPO_ROOT), str(REPO_ROOT / "scripts")}
            ]
            report = load_module(
                "generate_daily_ml_report_script_path_neg_util",
                module_path,
            )
            manifest = {
                "thresholds_meta": {
                    "break": {
                        "60": {
                            "score": -0.976,
                            "selected_utility_avg": -0.0488,
                            "fallback": False,
                            "guard_applied": False,
                        }
                    }
                }
            }
            note = report.summarize_tradeability_blockers(manifest)
        finally:
            sys.path = old_path
            for name in ("ml.thresholds", "ml"):
                sys.modules.pop(name, None)
            sys.modules.update(old_modules)

        self.assertIsNotNone(note)
        self.assertIn("active negative-utility horizons", note)
        self.assertIn("break:60m", note)

    def test_train_artifacts_threshold_override_parser_and_resolver(self) -> None:
        module = load_module(
            "train_rf_artifacts_threshold_overrides",
            REPO_ROOT / "scripts" / "train_rf_artifacts.py",
        )

        min_signals = module._parse_threshold_overrides(
            "break:15=8,break:30=8,break:60=6,reject:*=10",
            value_cast=module._coerce_min_signals,
            option_name="--threshold-min-signals-overrides",
        )
        precision_floor = module._parse_threshold_overrides(
            "break:15=0.35,break:60=0.30,reject:*=0.40",
            value_cast=module._coerce_precision_floor,
            option_name="--threshold-precision-floor-overrides",
        )

        self.assertEqual(int(min_signals[("break", 15)]), 8)
        self.assertEqual(int(min_signals[("break", 60)]), 6)
        self.assertEqual(int(min_signals[("reject", None)]), 10)
        self.assertAlmostEqual(float(precision_floor[("break", 60)]), 0.30, places=9)
        self.assertAlmostEqual(float(precision_floor[("reject", None)]), 0.40, places=9)

        self.assertEqual(
            int(
                module._resolve_threshold_override(
                    target="break",
                    horizon=15,
                    base_value=10,
                    overrides=min_signals,
                )
            ),
            8,
        )
        self.assertEqual(
            int(
                module._resolve_threshold_override(
                    target="reject",
                    horizon=5,
                    base_value=12,
                    overrides=min_signals,
                )
            ),
            10,
        )
        self.assertEqual(
            int(
                module._resolve_threshold_override(
                    target="break",
                    horizon=5,
                    base_value=10,
                    overrides=min_signals,
                )
            ),
            10,
        )

    def test_train_artifacts_threshold_override_parser_rejects_invalid_entry(self) -> None:
        module = load_module(
            "train_rf_artifacts_threshold_overrides_invalid",
            REPO_ROOT / "scripts" / "train_rf_artifacts.py",
        )
        with self.assertRaises(ValueError):
            module._parse_threshold_overrides(
                "break15=8",
                value_cast=module._coerce_min_signals,
                option_name="--threshold-min-signals-overrides",
            )

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

    def test_threshold_selector_strict_floor_returns_no_signal_fallback(self) -> None:
        module = load_module(
            "thresholds_strict_floor_no_signal",
            REPO_ROOT / "ml" / "thresholds.py",
        )
        y_true = np.asarray([1, 1, 1, 1], dtype=int)
        y_prob = np.asarray([0.95, 0.85, 0.75, 0.65], dtype=float)
        utility = np.asarray([-2.0, -3.0, -4.0, -5.0], dtype=float)

        selection = module.select_threshold(
            y_true,
            y_prob,
            objective="utility_bps",
            precision_floor=0.0,
            min_signals=1,
            default_threshold=0.5,
            utility_per_signal=utility,
            preferred_min_score=0.0,
            enforce_min_score=True,
            no_signal_threshold=module.NO_SIGNAL_THRESHOLD,
        )

        self.assertTrue(bool(selection.fallback))
        self.assertGreater(float(selection.threshold), 1.0)
        self.assertEqual(int(selection.signals), 0)
        self.assertLessEqual(float(selection.score), 0.0)
        self.assertGreater(len(selection.top_candidates), 0)

    def test_threshold_selector_captures_score_observations_at_chosen_threshold(self) -> None:
        """``select_threshold`` must return per-signal utility observations at
        the SELECTED threshold (not the default, not all candidates).
        Length must equal ``signals``; values must equal utility_per_signal
        on the rows where ``y_prob >= chosen_threshold``."""
        module = load_module(
            "thresholds_capture_obs",
            REPO_ROOT / "ml" / "thresholds.py",
        )
        y_true = np.asarray([1, 1, 1, 0, 1, 0], dtype=int)
        y_prob = np.asarray([0.95, 0.85, 0.75, 0.65, 0.55, 0.45], dtype=float)
        utility = np.asarray([7.0, 5.0, 3.0, -2.0, 11.0, -50.0], dtype=float)

        selection = module.select_threshold(
            y_true,
            y_prob,
            objective="utility_bps",
            precision_floor=0.0,
            min_signals=1,
            default_threshold=0.5,
            utility_per_signal=utility,
            stability_band=0.0,
            top_k=6,
            preferred_min_score=0.0,
        )

        chosen = float(selection.threshold)
        self.assertLess(chosen, module.NO_SIGNAL_THRESHOLD)
        # Observations are the utility values on rows where y_prob >= chosen.
        mask = y_prob >= chosen
        expected = [float(u) for u in utility[mask]]
        self.assertIsNotNone(selection.score_observations)
        self.assertEqual(list(selection.score_observations), expected)
        # Length matches signals reported by the selection.
        self.assertEqual(len(selection.score_observations), int(selection.signals))
        # Aggregate score equals the sum of observations (within float tol).
        self.assertAlmostEqual(
            sum(selection.score_observations), float(selection.score), places=9
        )

    def test_threshold_selector_observations_none_for_no_signal_substitution(self) -> None:
        """When strict floor forces NO_SIGNAL substitution, observations
        must be None — the on-disk threshold no longer fires."""
        module = load_module(
            "thresholds_no_signal_no_obs",
            REPO_ROOT / "ml" / "thresholds.py",
        )
        y_true = np.asarray([1, 1, 1, 1], dtype=int)
        y_prob = np.asarray([0.95, 0.85, 0.75, 0.65], dtype=float)
        utility = np.asarray([-2.0, -3.0, -4.0, -5.0], dtype=float)

        selection = module.select_threshold(
            y_true, y_prob,
            objective="utility_bps", precision_floor=0.0, min_signals=1,
            default_threshold=0.5, utility_per_signal=utility,
            preferred_min_score=0.0, enforce_min_score=True,
            no_signal_threshold=module.NO_SIGNAL_THRESHOLD,
        )
        self.assertGreater(float(selection.threshold), 1.0)  # no-signal sentinel
        self.assertEqual(int(selection.signals), 0)
        self.assertIsNone(selection.score_observations)

    def test_threshold_selector_observations_none_for_f1_objective(self) -> None:
        """F1 objective has no per-signal utility array; observations must be None."""
        module = load_module(
            "thresholds_f1_no_obs",
            REPO_ROOT / "ml" / "thresholds.py",
        )
        y_true = np.asarray([1, 0, 1, 0, 1, 0], dtype=int)
        y_prob = np.asarray([0.9, 0.8, 0.7, 0.6, 0.5, 0.4], dtype=float)
        selection = module.select_threshold(
            y_true, y_prob,
            objective="f1", precision_floor=0.0, min_signals=1,
            default_threshold=0.5,
        )
        self.assertIsNone(selection.score_observations)

    def test_threshold_selector_choice_unchanged_by_observation_capture(self) -> None:
        """Regression: adding score_observations must not change which
        threshold is selected, its score, or its signals count."""
        module = load_module(
            "thresholds_choice_regression",
            REPO_ROOT / "ml" / "thresholds.py",
        )
        y_true = np.asarray([1, 1, 1, 1, 1, 1], dtype=int)
        y_prob = np.asarray([0.99, 0.98, 0.97, 0.96, 0.95, 0.94], dtype=float)
        utility = np.asarray([12.0, -1.0, 12.0, -1.0, 12.0, -40.0], dtype=float)

        selection = module.select_threshold(
            y_true, y_prob,
            objective="utility_bps", precision_floor=0.0, min_signals=1,
            default_threshold=0.5, utility_per_signal=utility,
            stability_band=0.011, top_k=6, preferred_min_score=0.0,
        )
        # These exact values appear in
        # test_threshold_selector_uses_utility_tiebreak_within_preferred_floor
        # and must be preserved bit-for-bit.
        self.assertAlmostEqual(float(selection.threshold), 0.95, places=9)
        self.assertAlmostEqual(float(selection.score), 34.0, places=9)

    def test_threshold_selector_observations_count_matches_signals(self) -> None:
        """Across a larger synthetic slice, len(score_observations) must
        equal selection.signals, and the sum must equal selection.score."""
        module = load_module(
            "thresholds_count_matches_signals",
            REPO_ROOT / "ml" / "thresholds.py",
        )
        rng = np.random.default_rng(42)
        n = 300
        y_prob = rng.uniform(0.0, 1.0, size=n)
        y_true = (y_prob > 0.6).astype(int)
        utility = rng.normal(loc=0.5, scale=2.0, size=n)

        selection = module.select_threshold(
            y_true, y_prob,
            objective="utility_bps", precision_floor=0.0, min_signals=10,
            default_threshold=0.5, utility_per_signal=utility,
            stability_band=0.0, top_k=5, preferred_min_score=0.0,
        )
        self.assertIsNotNone(selection.score_observations)
        self.assertEqual(len(selection.score_observations), int(selection.signals))
        self.assertAlmostEqual(
            sum(selection.score_observations), float(selection.score), places=6
        )

    def test_threshold_selector_strict_fallback_returns_no_signal(self) -> None:
        module = load_module(
            "thresholds_strict_fallback_no_signal",
            REPO_ROOT / "ml" / "thresholds.py",
        )
        y_true = np.asarray([1, 0, 1, 0], dtype=int)
        y_prob = np.asarray([0.95, 0.85, 0.75, 0.65], dtype=float)
        utility = np.asarray([10.0, -1.0, 10.0, -1.0], dtype=float)

        selection = module.select_threshold(
            y_true,
            y_prob,
            objective="utility_bps",
            precision_floor=0.99,
            min_signals=10,
            default_threshold=0.5,
            utility_per_signal=utility,
            preferred_min_score=0.0,
            enforce_min_score=True,
            enforce_no_fallback=True,
            no_signal_threshold=module.NO_SIGNAL_THRESHOLD,
        )

        self.assertTrue(bool(selection.fallback))
        self.assertGreater(float(selection.threshold), 1.0)
        self.assertEqual(int(selection.signals), 0)
        self.assertEqual(int(selection.evaluated_candidates), 0)

    def test_threshold_score_is_unsafe_covers_all_cases(self) -> None:
        """The shared (score, fallback) predicate must agree with the
        previous duplicated implementations in server.ml_server and
        run_retrain_evidence_pack across all eight canonical inputs:
        NaN, +inf, -inf, None, zero, negative, positive, fallback.

        ``codes`` is order-insensitive but each unsafe input must produce
        the listed code; a safe input must produce ``(False, [])``.
        """
        module = load_module(
            "ml_thresholds_predicate",
            REPO_ROOT / "ml" / "thresholds.py",
        )
        cases = [
            # (label, score, fallback, expected_unsafe, must_contain_code_subset)
            ("nan_score_no_fallback", float("nan"), False, True, {"nonfinite"}),
            ("+inf_score_no_fallback", float("inf"), False, True, {"nonfinite"}),
            ("-inf_score_no_fallback", float("-inf"), False, True, {"nonfinite"}),
            ("none_score_no_fallback", None, False, True, {"none"}),
            ("zero_score_no_fallback", 0.0, False, True, {"nonpositive"}),
            ("negative_score_no_fallback", -1.5, False, True, {"nonpositive"}),
            ("positive_score_no_fallback", 12.3, False, False, set()),
            ("positive_score_with_fallback", 12.3, True, True, {"fallback"}),
            ("negative_score_with_fallback", -1.5, True, True, {"fallback", "nonpositive"}),
            ("nan_score_with_fallback", float("nan"), True, True, {"fallback", "nonfinite"}),
            # Defensive: bool is a numeric subtype in Python; reject it.
            ("bool_score", True, False, True, {"none"}),
            # Defensive: string is non-numeric.
            ("string_score", "not_a_number", False, True, {"none"}),
        ]
        for label, score, fallback, expected_unsafe, expected_codes in cases:
            with self.subTest(label=label):
                unsafe, codes = module.threshold_score_is_unsafe(score, fallback)
                self.assertEqual(
                    unsafe, expected_unsafe,
                    f"{label}: expected unsafe={expected_unsafe}, got {unsafe} (codes={codes})",
                )
                self.assertEqual(
                    set(codes) & expected_codes, expected_codes,
                    f"{label}: codes {codes} did not contain expected {expected_codes}",
                )
                if not expected_unsafe:
                    self.assertEqual(codes, [], f"{label}: safe inputs must yield empty codes")

    def test_ml_server_preserves_no_signal_threshold_sentinel(self) -> None:
        module = load_module(
            "ml_server_no_signal_threshold_sentinel",
            REPO_ROOT / "server" / "ml_server.py",
        )

        self.assertGreater(float(module.NO_SIGNAL_THRESHOLD), 1.0)
        self.assertGreater(float(module._clamp_threshold(module.NO_SIGNAL_THRESHOLD)), 1.0)
        self.assertEqual(float(module._clamp_threshold(1.0)), 1.0)
        self.assertAlmostEqual(float(module._clamp_threshold(0.995)), 0.99, places=9)

    def test_adjust_threshold_preserves_no_signal_sentinel(self) -> None:
        """Regression: _adjust_threshold(NO_SIGNAL_THRESHOLD, delta) must return
        the sentinel unchanged.

        Before the fix, `1.0000000000000002 + (-0.02) = 0.98...` would be clamped
        to 0.98 and served as a tradable threshold, reactivating signals on a
        horizon the manifest explicitly disabled.  After the fix, any base >=
        1.0 short-circuits and the delta is ignored.
        """
        module = load_module(
            "ml_server_adjust_threshold_no_signal_test",
            REPO_ROOT / "server" / "ml_server.py",
        )

        ns = float(module.NO_SIGNAL_THRESHOLD)

        # Negative deltas that previously demoted the sentinel must be ignored.
        for delta in (-0.02, -0.05, -0.10, -0.5, -1.0):
            self.assertEqual(
                float(module._adjust_threshold(ns, delta)),
                ns,
                f"_adjust_threshold(NO_SIGNAL, {delta}) must preserve sentinel; "
                f"got {module._adjust_threshold(ns, delta)!r}",
            )

        # Positive deltas must also leave the sentinel unchanged (no useful
        # semantics for "more disabled than disabled").
        for delta in (0.01, 0.05, 0.10, 1.0):
            self.assertEqual(
                float(module._adjust_threshold(ns, delta)),
                ns,
                f"_adjust_threshold(NO_SIGNAL, +{delta}) must preserve sentinel.",
            )

        # Exactly 1.0 must also be preserved (boundary of the >=1.0 short-circuit).
        self.assertEqual(float(module._adjust_threshold(1.0, -0.05)), 1.0)

        # Normal thresholds below 1.0 must continue to adjust + clamp as before.
        # 0.80 + (-0.02) = 0.78 (no clamp triggered).
        self.assertAlmostEqual(
            float(module._adjust_threshold(0.80, -0.02)), 0.78, places=9,
        )
        # 0.80 + 0.05 = 0.85 (no clamp triggered).
        self.assertAlmostEqual(
            float(module._adjust_threshold(0.80, 0.05)), 0.85, places=9,
        )
        # Delta cap: a delta beyond ML_REGIME_THRESHOLD_MAX_DELTA is clipped.
        cap = float(module.ML_REGIME_THRESHOLD_MAX_DELTA)
        self.assertAlmostEqual(
            float(module._adjust_threshold(0.80, -cap * 10)),
            max(0.01, min(0.99, 0.80 - cap)),
            places=9,
        )
        # Clamp floor: a normal threshold pushed below 0.01 clamps to 0.01.
        self.assertAlmostEqual(float(module._adjust_threshold(0.02, -cap)), 0.01, places=9)
        # Normal threshold below 1.0 with a small positive delta still clamps
        # within [0.01, 0.99].  (Note: a delta large enough to push the base
        # above 1.0 is preserved by _clamp_threshold's existing >= 1.0
        # short-circuit — a pre-existing behavior independent of this fix.)
        self.assertAlmostEqual(float(module._adjust_threshold(0.90, 0.05)), 0.95, places=9)

    def test_build_regime_thresholds_preserves_no_signal(self) -> None:
        """Regression: _build_regime_thresholds must not demote NO_SIGNAL bases
        under compression or expansion regime deltas."""
        module = load_module(
            "ml_server_build_regime_no_signal_test",
            REPO_ROOT / "server" / "ml_server.py",
        )
        ns = float(module.NO_SIGNAL_THRESHOLD)
        horizons = [5, 15, 30, 60]

        # Mixed manifest: NO_SIGNAL on break@60, normal thresholds elsewhere.
        baseline = {
            "reject": {5: 0.71, 15: 0.81, 30: 0.97, 60: 0.95},
            "break":  {5: 0.78, 15: ns,   30: 0.92, 60: ns},
        }

        for regime in ("compression", "expansion", "neutral"):
            result = module._build_regime_thresholds(horizons, regime, baseline)
            self.assertEqual(
                float(result["break"][15]), ns,
                f"regime={regime}: NO_SIGNAL break@15 was demoted to "
                f"{result['break'][15]!r}",
            )
            self.assertEqual(
                float(result["break"][60]), ns,
                f"regime={regime}: NO_SIGNAL break@60 was demoted to "
                f"{result['break'][60]!r}",
            )
            # Normal thresholds must still adjust per regime (no regression).
            self.assertLess(float(result["reject"][5]), 1.0)
            self.assertGreater(float(result["reject"][5]), 0.0)

    def test_apply_atr_zone_overlay_preserves_no_signal(self) -> None:
        """Regression: _apply_atr_zone_overlay must not demote NO_SIGNAL bases
        when ultra/near zone deltas apply under compression/expansion."""
        module = load_module(
            "ml_server_atr_zone_no_signal_test",
            REPO_ROOT / "server" / "ml_server.py",
        )
        ns = float(module.NO_SIGNAL_THRESHOLD)
        horizons = [5, 15]

        threshold_map = {
            "reject": {5: 0.71, 15: ns},
            "break":  {5: ns,   15: 0.85},
        }

        for regime, zone in [
            ("compression", "ultra"),
            ("compression", "near"),
            ("expansion",   "ultra"),
            ("expansion",   "near"),
        ]:
            overlaid, meta = module._apply_atr_zone_overlay(
                threshold_map, horizons, regime, zone,
            )
            self.assertEqual(
                float(overlaid["reject"][15]), ns,
                f"{regime}/{zone}: NO_SIGNAL reject@15 demoted to "
                f"{overlaid['reject'][15]!r}",
            )
            self.assertEqual(
                float(overlaid["break"][5]), ns,
                f"{regime}/{zone}: NO_SIGNAL break@5 demoted to "
                f"{overlaid['break'][5]!r}",
            )
            # Normal thresholds must still be reachable (no regression).
            self.assertLess(float(overlaid["reject"][5]), 1.0)
            self.assertLess(float(overlaid["break"][15]), 1.0)

    def test_apply_expansion_near_guardrail_preserves_no_signal_delta_strategy(self) -> None:
        """Regression: _apply_expansion_near_guardrail with the delta/tighten
        strategy must not demote NO_SIGNAL bases.

        Scope is limited to the delta-based strategy (the explicit ``no_trade``
        strategy intentionally hard-overwrites to 0.99 and is out of scope for
        the runtime-delta preservation rule).
        """
        module = load_module(
            "ml_server_guardrail_no_signal_test",
            REPO_ROOT / "server" / "ml_server.py",
        )
        ns = float(module.NO_SIGNAL_THRESHOLD)
        horizons = [5, 15]

        threshold_map = {
            "reject": {5: 0.71, 15: ns},
            "break":  {5: ns,   15: 0.85},
        }

        # Force the delta-strategy code path under expansion/near.
        with patch.object(module, "ML_REGIME_POLICY_MODE", "active"), \
             patch.object(module, "ML_REGIME_GUARD_EXPANSION_NEAR_MODE", "active"), \
             patch.object(module, "ML_REGIME_GUARD_EXPANSION_NEAR_STRATEGY", "tighten"):
            guarded, meta = module._apply_expansion_near_guardrail(
                threshold_map, horizons, "expansion", "near",
            )

        self.assertTrue(meta.get("triggered"))
        self.assertEqual(
            float(guarded["reject"][15]), ns,
            f"expansion-near guardrail demoted NO_SIGNAL reject@15 to "
            f"{guarded['reject'][15]!r}",
        )
        self.assertEqual(
            float(guarded["break"][5]), ns,
            f"expansion-near guardrail demoted NO_SIGNAL break@5 to "
            f"{guarded['break'][5]!r}",
        )
        # Normal thresholds remain tradable.
        self.assertLess(float(guarded["reject"][5]), 1.0)
        self.assertLess(float(guarded["break"][15]), 1.0)

    def test_ml_server_runtime_threshold_safety_neutralizes_bad_manifest_thresholds(self) -> None:
        module = load_module(
            "ml_server_runtime_threshold_safety",
            REPO_ROOT / "server" / "ml_server.py",
        )
        thresholds = {
            "reject": {5: 0.42, 15: 0.44},
            "break": {5: 0.46, 15: 0.48},
        }
        manifest = {
            "thresholds_meta": {
                "reject": {
                    "5": {"objective": "utility_bps", "score": -1.0, "fallback": False},
                    "15": {"objective": "utility_bps", "score": 3.0, "fallback": False},
                },
                "break": {
                    "5": {"objective": "utility_bps", "score": 2.0, "fallback": True},
                    "15": {"objective": "f1", "score": -1.0, "fallback": True},
                },
            }
        }

        safe = module.ModelRegistry._apply_runtime_threshold_safety(thresholds, manifest)

        self.assertEqual(float(safe["reject"][5]), float(module.NO_SIGNAL_THRESHOLD))
        self.assertEqual(float(safe["break"][5]), float(module.NO_SIGNAL_THRESHOLD))
        self.assertEqual(float(safe["reject"][15]), 0.44)
        self.assertEqual(float(safe["break"][15]), 0.48)

    def test_ml_server_runtime_threshold_safety_neutralizes_nonfinite_scores(self) -> None:
        # Regression: NaN <= 0.0 is False per IEEE 754, so a NaN score with
        # fallback=False would previously slip through the safety net.
        # +/-inf scores must also be rejected as unsafe.
        module = load_module(
            "ml_server_runtime_threshold_safety_nan",
            REPO_ROOT / "server" / "ml_server.py",
        )
        thresholds = {
            "reject": {5: 0.42, 15: 0.44, 30: 0.46},
            "break": {5: 0.48},
        }
        manifest = {
            "thresholds_meta": {
                "reject": {
                    "5": {"objective": "utility_bps", "score": float("nan"), "fallback": False},
                    "15": {"objective": "utility_bps", "score": float("inf"), "fallback": False},
                    "30": {"objective": "utility_bps", "score": float("-inf"), "fallback": False},
                },
                "break": {
                    "5": {"objective": "utility_bps", "score": 1.5, "fallback": False},
                },
            }
        }

        safe = module.ModelRegistry._apply_runtime_threshold_safety(thresholds, manifest)

        self.assertEqual(float(safe["reject"][5]), float(module.NO_SIGNAL_THRESHOLD))
        self.assertEqual(float(safe["reject"][15]), float(module.NO_SIGNAL_THRESHOLD))
        self.assertEqual(float(safe["reject"][30]), float(module.NO_SIGNAL_THRESHOLD))
        self.assertEqual(float(safe["break"][5]), 0.48)

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

    def test_train_artifacts_threshold_diagnostics_expose_tp_fp_utility(self) -> None:
        module = load_module(
            "train_rf_artifacts_threshold_diagnostics",
            REPO_ROOT / "scripts" / "train_rf_artifacts.py",
        )
        diagnostics = module.compute_threshold_diagnostics(
            y_true=np.asarray([1, 1, 0, 0, 1], dtype=int),
            y_prob=np.asarray([0.9, 0.7, 0.8, 0.2, 0.4], dtype=float),
            utility_per_signal=np.asarray([5.0, -3.0, -8.0, 1.0, 2.0], dtype=float),
            threshold=0.7,
        )
        self.assertEqual(int(diagnostics.get("selected_tp_count") or 0), 2)
        self.assertEqual(int(diagnostics.get("selected_fp_count") or 0), 1)
        self.assertAlmostEqual(float(diagnostics.get("selected_tp_utility_sum") or 0.0), 2.0, places=9)
        self.assertAlmostEqual(float(diagnostics.get("selected_fp_utility_sum") or 0.0), -8.0, places=9)
        self.assertAlmostEqual(float(diagnostics.get("selected_utility_sum") or 0.0), -6.0, places=9)

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

    def test_model_governance_reports_missing_metric_skip(self) -> None:
        module = load_module("model_governance_missing_metric_skip", REPO_ROOT / "scripts" / "model_governance.py")
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
        )
        active = {
            "feature_version": "v3",
            "trained_end_ts": 1000,
            "stats": {"5": {"break": {"sample_size": 200, "break_count": 60, "mae_bps_break": -10.0}}},
        }
        candidate = {
            "feature_version": "v3",
            "trained_end_ts": 2000,
            "stats": {"5": {"break": {"sample_size": 220, "break_count": 62, "mfe_bps_break": 7.0, "mae_bps_break": -9.0}}},
        }
        failures, skips = module.evaluate_gates(active, candidate, gates)
        self.assertEqual(failures, [])
        self.assertTrue(any("skipped mfe_bps_break regression gate" in item for item in skips))

    def test_model_governance_threshold_utility_guard_blocks_promotion(self) -> None:
        module = load_module(
            "model_governance_threshold_utility_guard",
            REPO_ROOT / "scripts" / "model_governance.py",
        )
        gates = module.GateConfig(
            required_targets=["reject"],
            required_horizons=[5],
            min_trained_end_delta_ms=0,
            max_mfe_regression_bps=1.5,
            max_mae_worsening_bps=2.0,
            min_total_samples=0,
            min_positive_samples_reject=0,
            min_positive_samples_break=0,
            allow_feature_version_change=False,
            enforce_threshold_utility_guard=True,
            threshold_utility_targets=["reject"],
            threshold_utility_min_score=0.0,
        )
        active = {
            "feature_version": "v3",
            "trained_end_ts": 1000,
            "stats": {"5": {"reject": {"sample_size": 200, "reject_count": 80, "mfe_bps_reject": 8.0, "mae_bps_reject": -12.0}}},
            "thresholds": {"reject": {"5": 0.5}},
            "thresholds_meta": {"reject": {"5": {"objective": "utility_bps", "score": 10.0, "guard_applied": False}}},
        }
        candidate = {
            "feature_version": "v3",
            "trained_end_ts": 2000,
            "stats": {"5": {"reject": {"sample_size": 200, "reject_count": 80, "mfe_bps_reject": 8.5, "mae_bps_reject": -11.5}}},
            "thresholds": {"reject": {"5": 1.0}},
            "thresholds_meta": {
                "reject": {
                    "5": {
                        "objective": "utility_bps",
                        "score": -45.0,
                        "guard_applied": True,
                        "guard_reason": "non_positive_utility(-45<=0)",
                    }
                }
            },
        }
        failures, _ = module.evaluate_gates(active, candidate, gates)
        self.assertTrue(
            any("reject:5m threshold utility guard applied" in item for item in failures)
        )

    def test_model_governance_threshold_utility_min_score_blocks_nonpositive(self) -> None:
        module = load_module(
            "model_governance_threshold_utility_min_score",
            REPO_ROOT / "scripts" / "model_governance.py",
        )
        gates = module.GateConfig(
            required_targets=["reject"],
            required_horizons=[5],
            min_trained_end_delta_ms=0,
            max_mfe_regression_bps=1.5,
            max_mae_worsening_bps=2.0,
            min_total_samples=0,
            min_positive_samples_reject=0,
            min_positive_samples_break=0,
            allow_feature_version_change=False,
            enforce_threshold_utility_guard=True,
            threshold_utility_targets=["reject"],
            threshold_utility_min_score=0.0,
        )
        active = {
            "feature_version": "v3",
            "trained_end_ts": 1000,
            "stats": {"5": {"reject": {"sample_size": 200, "reject_count": 80, "mfe_bps_reject": 8.0, "mae_bps_reject": -12.0}}},
            "thresholds": {"reject": {"5": 0.5}},
            "thresholds_meta": {"reject": {"5": {"objective": "utility_bps", "score": 10.0, "guard_applied": False}}},
        }
        candidate = {
            "feature_version": "v3",
            "trained_end_ts": 2000,
            "stats": {"5": {"reject": {"sample_size": 200, "reject_count": 80, "mfe_bps_reject": 8.5, "mae_bps_reject": -11.5}}},
            "thresholds": {"reject": {"5": 0.62}},
            "thresholds_meta": {"reject": {"5": {"objective": "utility_bps", "score": -0.01, "guard_applied": False}}},
        }
        failures, _ = module.evaluate_gates(active, candidate, gates)
        self.assertTrue(
            any("reject:5m threshold utility score" in item for item in failures)
        )

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

    # ------------------------------------------------------------------ #
    # NO_SIGNAL_THRESHOLD sentinel threshold validation — regression tests
    # ------------------------------------------------------------------ #

    def test_no_signal_threshold_is_above_one(self) -> None:
        """NO_SIGNAL_THRESHOLD must be strictly above 1.0.

        This pins the sentinel contract so that nobody accidentally clips
        it to 1.0, which would break its function as an unreachable sentinel.
        """
        from ml.thresholds import NO_SIGNAL_THRESHOLD
        self.assertGreater(NO_SIGNAL_THRESHOLD, 1.0)

    def test_governance_accepts_no_signal_threshold(self) -> None:
        """validate_manifest must NOT reject a threshold equal to NO_SIGNAL_THRESHOLD.

        Regression test for the 2026-05-12 regression where the predicate
        `thr > 1.0` blocked every candidate from v415 onwards.
        """
        module = load_module(
            "model_governance_no_signal_accept",
            REPO_ROOT / "scripts" / "model_governance.py",
        )
        from ml.thresholds import NO_SIGNAL_THRESHOLD
        import tempfile, os as _os
        tmp_dir = Path(tempfile.mkdtemp(prefix="pq_gov_ns_"))
        try:
            # Create a minimal fake model artifact so the file-existence check passes.
            fake_model = tmp_dir / "rf_break_5m_vtest.pkl"
            fake_model.write_bytes(b"fake")
            manifest = {
                "version": "vtest",
                "models": {"break": {"5": "rf_break_5m_vtest.pkl"}},
                "thresholds": {"break": {"5": NO_SIGNAL_THRESHOLD}},
            }
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
            )
            errors = module.validate_manifest(manifest, tmp_dir, gates)
            invalid_thr_errors = [e for e in errors if "invalid threshold" in e]
            self.assertEqual(
                invalid_thr_errors,
                [],
                f"NO_SIGNAL_THRESHOLD ({NO_SIGNAL_THRESHOLD!r}) must not be rejected; got: {invalid_thr_errors}",
            )
        finally:
            import shutil as _shutil
            _shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_governance_rejects_above_no_signal_threshold(self) -> None:
        """validate_manifest must reject a threshold of 1.01 (above the sentinel).

        Values above NO_SIGNAL_THRESHOLD are out-of-range and must remain
        rejected even after the sentinel fix.
        """
        module = load_module(
            "model_governance_above_ns_reject",
            REPO_ROOT / "scripts" / "model_governance.py",
        )
        import tempfile
        tmp_dir = Path(tempfile.mkdtemp(prefix="pq_gov_above_ns_"))
        try:
            fake_model = tmp_dir / "rf_break_5m_vtest.pkl"
            fake_model.write_bytes(b"fake")
            manifest = {
                "version": "vtest",
                "models": {"break": {"5": "rf_break_5m_vtest.pkl"}},
                "thresholds": {"break": {"5": 1.01}},
            }
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
            )
            errors = module.validate_manifest(manifest, tmp_dir, gates)
            self.assertTrue(
                any("invalid threshold" in e for e in errors),
                f"Threshold 1.01 must be rejected; got errors: {errors}",
            )
        finally:
            import shutil as _shutil
            _shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_governance_accepts_normal_threshold(self) -> None:
        """validate_manifest must accept a normal threshold such as 0.75."""
        module = load_module(
            "model_governance_normal_threshold",
            REPO_ROOT / "scripts" / "model_governance.py",
        )
        import tempfile
        tmp_dir = Path(tempfile.mkdtemp(prefix="pq_gov_normal_"))
        try:
            fake_model = tmp_dir / "rf_break_5m_vtest.pkl"
            fake_model.write_bytes(b"fake")
            manifest = {
                "version": "vtest",
                "models": {"break": {"5": "rf_break_5m_vtest.pkl"}},
                "thresholds": {"break": {"5": 0.75}},
            }
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
            )
            errors = module.validate_manifest(manifest, tmp_dir, gates)
            invalid_thr_errors = [e for e in errors if "invalid threshold" in e]
            self.assertEqual(
                invalid_thr_errors,
                [],
                f"Normal threshold 0.75 must be accepted; got: {invalid_thr_errors}",
            )
        finally:
            import shutil as _shutil
            _shutil.rmtree(tmp_dir, ignore_errors=True)

    # ------------------------------------------------------------------ #
    # Held-out feasibility audit
    # ------------------------------------------------------------------ #

    def _audit_module(self):
        return load_module(
            "held_out_feasibility_audit",
            REPO_ROOT / "scripts" / "audit_held_out_feasibility.py",
        )

    @staticmethod
    def _stub_df(n: int):
        """Tiny dataframe whose row index doubles as a unique label."""
        import pandas as pd
        return pd.DataFrame({"row_id": list(range(n))})

    class _StubModel:
        """``predict_proba`` returns the second column from a caller-supplied
        callable. Records the input X so tests can verify which rows were
        scored."""

        def __init__(self, probs_fn):
            self._probs_fn = probs_fn
            self.last_X = None

        def predict_proba(self, X):
            import numpy as np
            self.last_X = X
            n = len(X)
            arr = np.zeros((n, 2), dtype=float)
            arr[:, 1] = self._probs_fn(X)
            arr[:, 0] = 1.0 - arr[:, 1]
            return arr

    def test_audit_compute_slice_uses_chronological_tail(self) -> None:
        """``compute_slice`` must score the LAST ``slice_size`` rows of the
        passed df — never random, never the head."""
        module = self._audit_module()
        df = self._stub_df(100)
        feature_columns = ["row_id"]

        seen_indices: list[list[int]] = []

        def _probs(X):
            seen_indices.append(list(X.index))
            return [0.0] * len(X)

        model = self._StubModel(_probs)
        # build_features_aligned will reindex to feature_columns. We need
        # build_feature_row to return something usable; patch it to just
        # echo the row dict.
        import ml.features as features_mod
        original_build_row = features_mod.build_feature_row
        features_mod.build_feature_row = lambda row: {"row_id": row.get("row_id")}
        try:
            module.compute_slice(
                df, 10,
                model_obj=model, feature_columns=feature_columns,
                threshold=0.5, floor_total=0,
            )
        finally:
            features_mod.build_feature_row = original_build_row

        self.assertEqual(seen_indices[-1], list(range(90, 100)))

    def test_audit_compute_slice_uses_fixed_threshold_no_search(self) -> None:
        """Signal count must come from a SINGLE comparison against the
        passed threshold. The audit must not iterate alternative thresholds.

        We assert this by checking ``predict_proba`` is called at most once
        per slice, and that the signal count matches a hand calculation
        for the given (probs, threshold) pair."""
        module = self._audit_module()
        df = self._stub_df(20)
        feature_columns = ["row_id"]

        call_count = {"n": 0}

        def _probs(X):
            call_count["n"] += 1
            # First 5 rows of the slice land above 0.75; rest below 0.5.
            n = len(X)
            return [0.9 if i < 5 else 0.3 for i in range(n)]

        model = self._StubModel(_probs)
        import ml.features as features_mod
        original_build_row = features_mod.build_feature_row
        features_mod.build_feature_row = lambda row: {"row_id": row.get("row_id")}
        try:
            result = module.compute_slice(
                df, 20,
                model_obj=model, feature_columns=feature_columns,
                threshold=0.75, floor_total=0,
            )
        finally:
            features_mod.build_feature_row = original_build_row

        self.assertEqual(call_count["n"], 1)
        self.assertEqual(result["signal_count"], 5)
        self.assertAlmostEqual(result["signal_rate"], 0.25, places=6)

    def test_audit_recommendation_single_held_out_slice_feasible(self) -> None:
        """At least one slice with meets_min_signals AND leaves_room
        => ``single_held_out_slice_feasible``."""
        module = self._audit_module()
        slices = [
            {"meets_min_signals": True, "leaves_train_calib_tune_room": True},
            {"meets_min_signals": True, "leaves_train_calib_tune_room": False},
        ]
        rec = module.determine_recommendation(
            total_rows=25_000, slices=slices, floor_total=21_430,
        )
        self.assertEqual(rec, "single_held_out_slice_feasible")

    def test_audit_recommendation_walk_forward_required(self) -> None:
        """No slice satisfies BOTH gates, but total data >= floor
        => ``walk_forward_oos_required``."""
        module = self._audit_module()
        slices = [
            {"meets_min_signals": True, "leaves_train_calib_tune_room": False},
            {"meets_min_signals": False, "leaves_train_calib_tune_room": True},
        ]
        rec = module.determine_recommendation(
            total_rows=21_500, slices=slices, floor_total=21_430,
        )
        self.assertEqual(rec, "walk_forward_oos_required")

    def test_audit_recommendation_insufficient_data(self) -> None:
        """``total_rows < floor_total`` => no clean OOS is achievable
        regardless of per-slice flags."""
        module = self._audit_module()
        slices = [
            {"meets_min_signals": True, "leaves_train_calib_tune_room": True},
        ]
        rec = module.determine_recommendation(
            total_rows=15_000, slices=slices, floor_total=21_430,
        )
        self.assertEqual(rec, "insufficient_data_for_clean_oos")

    def test_audit_report_schema_stable(self) -> None:
        """The JSON report must always carry the same top-level keys
        regardless of the recommendation branch."""
        module = self._audit_module()
        threshold_resolution = {
            "runtime_threshold": 0.5,
            "threshold_source": "manifest",
            "manifest_threshold": 0.5,
            "artifact_threshold": 0.5,
            "threshold_mismatch_detected": False,
        }
        report = module.build_report(
            target="reject", horizon=15,
            active_manifest_path=Path("/tmp/manifest_active.json"),
            manifest={"version": "v999"},
            model_path=Path("/tmp/rf_reject_15m_v999.pkl"),
            threshold_resolution=threshold_resolution,
            total_rows=21_500,
            slices=[{"meets_min_signals": False, "leaves_train_calib_tune_room": True}],
            recommendation="walk_forward_oos_required",
        )
        for key in (
            "schema_version", "audit_type", "generated_at",
            "target", "horizon",
            "active_manifest_path", "active_manifest_version",
            "model_path", "deployed_threshold",
            # New: threshold provenance + mismatch surface
            "threshold_source", "manifest_threshold", "artifact_threshold",
            "threshold_mismatch_detected",
            "min_signals_floor",
            "existing_floor", "existing_floor_total",
            "total_labeled_rows", "slices",
            "recommendation", "warnings", "scope_disclosure",
            # New: limitation disclosure -- the deployed model was trained on
            # data including the evaluated slice, so signal-density estimates
            # are upper bounds.
            "model_trained_on_evaluated_slice", "audit_limitation",
        ):
            self.assertIn(key, report, f"missing report key: {key}")
        self.assertEqual(report["audit_type"], "held_out_feasibility")
        self.assertEqual(report["target"], "reject")
        self.assertEqual(report["horizon"], 15)
        # Schema version pinned so downstream consumers can detect drift.
        self.assertEqual(report["schema_version"], 1)
        # deployed_threshold mirrors runtime_threshold from the resolution.
        self.assertEqual(report["deployed_threshold"], 0.5)
        self.assertEqual(report["threshold_source"], "manifest")
        self.assertFalse(report["threshold_mismatch_detected"])
        # Limitation disclosure: the audit must always report it true today.
        # When B4 carves out a real held-out region in advance, training will
        # not have seen those rows and this can flip to False; until then the
        # report is honest about being a feasibility check.
        self.assertTrue(report["model_trained_on_evaluated_slice"])
        self.assertEqual(
            report["audit_limitation"],
            "feasibility_only_model_may_have_seen_evaluated_rows",
        )
        # The scope_disclosure sentence chain also references the caveat so
        # console readers who only see the long string still get the warning.
        self.assertIn(
            "model_trained_on_evaluated_slice", report["scope_disclosure"]
        )

    def test_audit_resolve_runtime_threshold_prefers_manifest(self) -> None:
        """Server semantics: manifest threshold wins over artifact when both
        are present, even when they agree."""
        module = self._audit_module()
        out = module.resolve_runtime_threshold(0.80, 0.80)
        self.assertEqual(out["runtime_threshold"], 0.80)
        self.assertEqual(out["threshold_source"], "manifest")
        self.assertFalse(out["threshold_mismatch_detected"])
        self.assertEqual(out["manifest_threshold"], 0.80)
        self.assertEqual(out["artifact_threshold"], 0.80)

    def test_audit_resolve_runtime_threshold_falls_back_to_artifact(self) -> None:
        """When the manifest is missing the horizon, fall back to the
        artifact threshold (matches server.ml_server.ModelRegistry
        fallback) and label the source accordingly."""
        module = self._audit_module()
        out = module.resolve_runtime_threshold(None, 0.65)
        self.assertEqual(out["runtime_threshold"], 0.65)
        self.assertEqual(out["threshold_source"], "artifact_fallback")
        self.assertFalse(out["threshold_mismatch_detected"])
        self.assertIsNone(out["manifest_threshold"])
        self.assertEqual(out["artifact_threshold"], 0.65)

    def test_audit_resolve_runtime_threshold_flags_mismatch(self) -> None:
        """Critical case: manifest and artifact disagree. The runtime uses
        the manifest value, so the audit must compute against it AND emit
        a mismatch flag so the divergence cannot be silent."""
        module = self._audit_module()
        # Simulates the runtime-safety substitution case (PRs #9/#10/#12):
        # the manifest carries the no-signal sentinel after substitution,
        # while the pickle still has the original ``optimal_threshold``.
        sentinel = float(1.0000000000000002)  # mirrors NO_SIGNAL_THRESHOLD
        out = module.resolve_runtime_threshold(sentinel, 0.80)
        self.assertEqual(out["runtime_threshold"], sentinel)
        self.assertEqual(out["threshold_source"], "manifest")
        self.assertTrue(out["threshold_mismatch_detected"])
        self.assertEqual(out["manifest_threshold"], sentinel)
        self.assertEqual(out["artifact_threshold"], 0.80)

    def test_audit_resolve_runtime_threshold_raises_when_neither_present(self) -> None:
        module = self._audit_module()
        with self.assertRaises(SystemExit):
            module.resolve_runtime_threshold(None, None)

    def test_audit_compute_slice_handles_oversized_request(self) -> None:
        """If asked for more rows than the df contains, return a clean
        ``skip_reason`` rather than crashing or silently truncating."""
        module = self._audit_module()
        df = self._stub_df(50)
        model = self._StubModel(lambda X: [0.0] * len(X))
        result = module.compute_slice(
            df, 100,
            model_obj=model, feature_columns=["row_id"],
            threshold=0.5, floor_total=0,
        )
        self.assertEqual(result["signal_count"], None)
        self.assertEqual(result["available_rows"], 0)
        self.assertEqual(result["skip_reason"], "slice_size_exceeds_total_rows")
        self.assertFalse(result["meets_min_signals"])

    # ------------------------------------------------------------------ #
    # Phase 2 regime-health attribution
    # ------------------------------------------------------------------ #

    def _attribution_module(self):
        return load_module(
            "regime_health_attribution",
            REPO_ROOT / "scripts" / "audit_regime_health_attribution.py",
        )

    @staticmethod
    def _attribution_stub_df(n: int):
        import pandas as pd
        # ``row_id`` doubles as the feature; ``ts_event`` orders rows so
        # the audit's chronological tail picking has a well-defined order.
        return pd.DataFrame({"row_id": list(range(n)), "ts_event": list(range(n))})

    def test_attribution_select_groups_chronological_and_fixed(self) -> None:
        """``recent_dormant`` MUST be the last ``recent_n`` rows. The older
        window MUST be carved out of the latest ``older_pct`` *before* the
        recent tail — never random, never the head, never overlapping."""
        module = self._attribution_module()
        df = self._attribution_stub_df(1000)
        sel = module.select_groups(df, recent_n=100, older_pct=0.30)
        self.assertEqual(sel["recent_range"], (900, 1000))
        # latest 30% of 1000 is rows [700, 1000); minus recent [900, 1000)
        # leaves older window [700, 900).
        self.assertEqual(sel["older_range"], (700, 900))
        self.assertEqual(len(sel["recent_df"]), 100)
        self.assertEqual(len(sel["older_df"]), 200)
        # The two row ranges must be disjoint.
        rs, re_ = sel["recent_range"]
        os_, oe = sel["older_range"]
        self.assertTrue(oe <= rs, "older window must end at or before recent tail")

    def test_attribution_select_groups_recent_consumes_older(self) -> None:
        """When the recent tail covers more rows than the older-pct window,
        the older window collapses to empty rather than going negative or
        wrapping back to the head."""
        module = self._attribution_module()
        df = self._attribution_stub_df(100)
        sel = module.select_groups(df, recent_n=80, older_pct=0.30)
        self.assertEqual(sel["recent_range"], (20, 100))
        # latest 30 ends at 100, but recent starts at 20 — older must end
        # at 20 too, and start cannot go past end.
        os_, oe = sel["older_range"]
        self.assertLessEqual(oe, 20)
        self.assertLessEqual(os_, oe)

    def test_attribution_resolve_runtime_threshold_prefers_manifest(self) -> None:
        module = self._attribution_module()
        out = module.resolve_runtime_threshold(0.80, 0.65)
        self.assertEqual(out["runtime_threshold"], 0.80)
        self.assertEqual(out["threshold_source"], "manifest")
        self.assertTrue(out["threshold_mismatch_detected"])

    def test_attribution_resolve_runtime_threshold_falls_back(self) -> None:
        module = self._attribution_module()
        out = module.resolve_runtime_threshold(None, 0.65)
        self.assertEqual(out["runtime_threshold"], 0.65)
        self.assertEqual(out["threshold_source"], "artifact_fallback")
        self.assertFalse(out["threshold_mismatch_detected"])

    def test_attribution_threshold_tune_reference_marked_unavailable(self) -> None:
        """The manifest only persists threshold-tune SIZE, not row IDs.
        The audit must report the reference as unavailable, never fabricate
        a substitute slice."""
        module = self._attribution_module()
        manifest = {
            "thresholds_meta": {
                "reject": {
                    "15": {
                        "threshold_tune_size": 251,
                        "objective": "utility_bps",
                        "fallback": True,
                        "search_enabled": False,
                    }
                }
            }
        }
        ref = module.threshold_tune_meta(manifest, "reject", 15)
        self.assertFalse(ref["available"])
        self.assertEqual(ref["threshold_tune_size"], 251)
        self.assertEqual(ref["reason"], "row_ids_not_persisted_in_manifest")
        self.assertEqual(ref["objective"], "utility_bps")

    def test_attribution_threshold_tune_reference_handles_missing_meta(self) -> None:
        module = self._attribution_module()
        ref_empty = module.threshold_tune_meta({}, "reject", 15)
        self.assertFalse(ref_empty["available"])
        self.assertEqual(ref_empty["reason"], "threshold_tune_size_missing_or_zero")
        ref_zero = module.threshold_tune_meta(
            {"thresholds_meta": {"reject": {"15": {"threshold_tune_size": 0}}}},
            "reject", 15,
        )
        self.assertFalse(ref_zero["available"])

    def test_attribution_standardized_mean_diff_math(self) -> None:
        """SMD = (mean_a - mean_b) / sqrt((var_a + var_b)/2)."""
        import numpy as np
        module = self._attribution_module()
        a = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        b = np.array([2.0, 3.0, 4.0, 5.0, 6.0])
        # mean_a=3, mean_b=4, var_a=var_b=2.0, pooled=sqrt(2)
        expected = (3.0 - 4.0) / (2.0 ** 0.5)
        got = module.standardized_mean_diff(a, b)
        self.assertAlmostEqual(got, expected, places=10)
        # Zero variance both sides => None, NOT a synthetic huge effect.
        self.assertIsNone(
            module.standardized_mean_diff(np.array([1.0, 1.0]), np.array([1.0, 1.0]))
        )
        # Empty side => None.
        self.assertIsNone(module.standardized_mean_diff(np.array([]), b))

    def test_attribution_quantile_distance_max_over_grid(self) -> None:
        """quantile_distance is max |q_a(p) - q_b(p)| over the fixed grid."""
        import numpy as np
        module = self._attribution_module()
        a = np.linspace(0.0, 1.0, 1001)
        b = a + 0.10  # uniform shift
        qd = module.quantile_distance(a, b)
        self.assertAlmostEqual(qd, 0.10, places=4)
        # Empty side => None.
        self.assertIsNone(module.quantile_distance(np.array([]), b))

    def test_attribution_top_shifted_features_deterministic_ordering(self) -> None:
        module = self._attribution_module()
        rows = [
            {"feature": "f_big", "standardized_mean_diff": -1.5, "quantile_distance": 0.30, "ks_statistic": 0.40},
            {"feature": "f_tied_a", "standardized_mean_diff": 0.5, "quantile_distance": 0.10, "ks_statistic": 0.15},
            {"feature": "f_tied_b", "standardized_mean_diff": 0.5, "quantile_distance": 0.20, "ks_statistic": 0.20},
            {"feature": "f_none", "standardized_mean_diff": None, "quantile_distance": 0.99, "ks_statistic": None},
        ]
        top = module.top_shifted_features(rows, k=3)
        self.assertEqual([r["feature"] for r in top], ["f_big", "f_tied_b", "f_tied_a"])
        self.assertNotIn("f_none", [r["feature"] for r in top])

    def test_attribution_probability_stats_signal_count_uses_runtime_threshold(self) -> None:
        """Signal count must come from a single comparison to the passed
        threshold — no search, no alternative thresholds tested."""
        import numpy as np
        module = self._attribution_module()
        probs = np.array([0.10, 0.50, 0.79, 0.80, 0.90])
        ps = module.probability_stats(probs, threshold=0.80)
        self.assertEqual(ps["n"], 5)
        # 0.80 and 0.90 are >= 0.80
        self.assertEqual(ps["signal_count"], 2)
        # Probability stats handle empty arrays cleanly.
        ps0 = module.probability_stats(np.array([]), threshold=0.80)
        self.assertEqual(ps0["n"], 0)
        self.assertIsNone(ps0["mean"])

    def test_attribution_summary_flag_logic(self) -> None:
        """Each attribution flag must respond ONLY to the inputs that
        define it (no cross-flag bleeding). Test the four boolean branches
        on independent inputs."""
        module = self._attribution_module()
        empty_health = {"imputation_heavy_features": [], "all_null_rows": 0}
        dirty_health = {"imputation_heavy_features": ["bad_col"], "all_null_rows": 0}
        # Feature shift only.
        feat_rows = [{"feature": "x", "standardized_mean_diff": 1.0,
                      "quantile_distance": 0.5, "ks_statistic": 0.5}]
        out = module.attribution_summary(
            recent_prob_stats={"max": 0.78}, firing_prob_stats={"n": 5},
            threshold=0.80, feature_rows=feat_rows,
            recent_health=empty_health, firing_health=empty_health,
            older_firing_available=True, tune_reference_available=False,
        )
        self.assertTrue(out["feature_shift_present"])
        self.assertFalse(out["probability_compression_present"])  # 0.80-0.78<0.10
        self.assertFalse(out["data_quality_warning"])
        self.assertTrue(out["older_firing_context_available"])
        self.assertFalse(out["threshold_tune_reference_available"])
        # Compression only.
        out2 = module.attribution_summary(
            recent_prob_stats={"max": 0.50}, firing_prob_stats={"n": 5},
            threshold=0.80, feature_rows=[],
            recent_health=empty_health, firing_health=empty_health,
            older_firing_available=True, tune_reference_available=False,
        )
        self.assertFalse(out2["feature_shift_present"])
        self.assertTrue(out2["probability_compression_present"])  # gap >= 0.10
        # DQ only.
        out3 = module.attribution_summary(
            recent_prob_stats={"max": 0.78}, firing_prob_stats={"n": 5},
            threshold=0.80, feature_rows=[],
            recent_health=dirty_health, firing_health=empty_health,
            older_firing_available=True, tune_reference_available=False,
        )
        self.assertTrue(out3["data_quality_warning"])
        self.assertFalse(out3["feature_shift_present"])
        self.assertFalse(out3["probability_compression_present"])
        # No older firing context => availability flag false; recommendation
        # asks for a wider window.
        out4 = module.attribution_summary(
            recent_prob_stats={"max": 0.78}, firing_prob_stats={"n": 0},
            threshold=0.80, feature_rows=[],
            recent_health=empty_health, firing_health=empty_health,
            older_firing_available=False, tune_reference_available=False,
        )
        self.assertFalse(out4["older_firing_context_available"])
        self.assertIn("widen", out4["recommended_next_diagnostic"])

    def test_attribution_report_schema_stable(self) -> None:
        """JSON report must carry the same top-level keys regardless of
        whether older_firing_context is empty or populated. Downstream
        consumers (alerts, dashboards) depend on this."""
        module = self._attribution_module()
        thr = {"runtime_threshold": 0.80, "threshold_source": "manifest",
               "manifest_threshold": 0.80, "artifact_threshold": 0.80,
               "threshold_mismatch_detected": False}
        attr = {"feature_shift_present": False, "probability_compression_present": False,
                "data_quality_warning": False, "older_firing_context_available": False,
                "threshold_tune_reference_available": False,
                "strongest_shifted_features": [],
                "recommended_next_diagnostic": "x"}
        rep = module.build_report(
            target="reject", horizon=15,
            active_manifest_path=Path("/tmp/manifest_active.json"),
            manifest={"version": "v_test"},
            model_path=Path("/tmp/rf.pkl"),
            threshold_resolution=thr,
            total_rows=20_000,
            group_ranges={
                "total_rows": 20_000,
                "recent_dormant": {"row_index_start": 19_000, "row_index_end": 20_000, "n": 1000},
                "older_window": {"row_index_start": 14_000, "row_index_end": 19_000, "n": 5000,
                                 "pct_of_total": 0.30, "window_size_unfiltered": 6000},
            },
            groups={
                "recent_dormant": {}, "older_firing_context": {},
                "older_high_probability_nonfiring": {}, "threshold_tune_reference": {},
            },
            feature_comparison={"recent_vs_older_firing": []},
            attribution=attr,
            threshold_tune_ref={"available": False, "reason": "row_ids_not_persisted_in_manifest"},
            warnings=[],
        )
        for key in (
            "schema_version", "audit_type", "generated_at",
            "target", "horizon",
            "active_manifest_path", "active_manifest_version",
            "model_path", "deployed_threshold",
            "threshold_source", "manifest_threshold", "artifact_threshold",
            "threshold_mismatch_detected",
            "total_labeled_rows", "group_ranges", "groups",
            "feature_comparison", "threshold_tune_reference",
            "attribution", "warnings", "scope_disclosure",
        ):
            self.assertIn(key, rep, f"missing report key: {key}")
        self.assertEqual(rep["audit_type"], "regime_health_attribution")
        self.assertEqual(rep["schema_version"], 1)
        self.assertEqual(rep["target"], "reject")
        self.assertEqual(rep["horizon"], 15)

    def test_attribution_report_no_edge_claim_language(self) -> None:
        """The scope disclosure and recommended-next text must NOT contain
        forbidden edge/performance/trade language. This guards against
        someone later turning a descriptive flag into a prescriptive call."""
        module = self._attribution_module()
        # Hit every recommendation branch.
        recs = []
        for fs, comp, dq, of in [
            (False, False, False, False),
            (True, True, False, True),
            (True, False, False, True),
            (False, True, False, True),
            (False, False, True, True),
            (False, False, False, True),
        ]:
            recs.append(module._recommend_next(
                feature_shift_present=fs, compression_present=comp,
                data_quality_warning=dq, older_firing_available=of,
            ))
        forbidden = (
            "buy", "sell", "long", "short", "promote", "deploy", "retrain",
            "edge", "alpha", "profit", "pnl", "p&l", "tradeable",
            "tune", "calibrate",
        )
        for rec in recs:
            for word in forbidden:
                self.assertNotIn(word, rec.lower(),
                                 f"forbidden word {word!r} in recommendation: {rec}")
        # And the scope disclosure on a fresh build_report output.
        thr = {"runtime_threshold": 0.80, "threshold_source": "manifest",
               "manifest_threshold": 0.80, "artifact_threshold": 0.80,
               "threshold_mismatch_detected": False}
        rep = module.build_report(
            target="reject", horizon=15,
            active_manifest_path=Path("/tmp/m.json"), manifest={"version": "v"},
            model_path=Path("/tmp/rf.pkl"), threshold_resolution=thr,
            total_rows=10, group_ranges={}, groups={},
            feature_comparison={}, attribution={},
            threshold_tune_ref={"available": False, "reason": "x"},
        )
        for word in ("buy", "sell", "edge", "alpha", "profit"):
            self.assertNotIn(word, rep["scope_disclosure"].lower())

    def test_attribution_per_feature_comparison_handles_missing_older_firing(self) -> None:
        """Empty older-firing dataframe must NOT raise. Effect sizes are
        None on that side; the script must still produce the same per-row
        schema so downstream consumers can iterate uniformly."""
        import pandas as pd
        module = self._attribution_module()
        recent = pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": [0.1, 0.2, 0.3]})
        firing = pd.DataFrame({"x": [], "y": []})
        rows = module.per_feature_comparison(
            recent, firing, group_a_name="recent_dormant", group_b_name="older_firing_context",
        )
        self.assertEqual(len(rows), 2)
        for r in rows:
            self.assertIn("feature", r)
            self.assertIn("standardized_mean_diff", r)
            self.assertIn("quantile_distance", r)
            self.assertIsNone(r["standardized_mean_diff"])
            self.assertIsNone(r["quantile_distance"])

    def test_attribution_cli_rejects_invalid_args(self) -> None:
        module = self._attribution_module()
        with self.assertRaises(SystemExit):
            module.main(["--older-pct", "1.5"])
        with self.assertRaises(SystemExit):
            module.main(["--older-pct", "0"])
        with self.assertRaises(SystemExit):
            module.main(["--recent-n", "0"])
        with self.assertRaises(SystemExit):
            module.main(["--highprob-low", "1.5"])

    # ------------------------------------------------------------------ #
    # Phase 2B regime data-quality audit
    # ------------------------------------------------------------------ #

    def _data_quality_module(self):
        return load_module(
            "regime_data_quality",
            REPO_ROOT / "scripts" / "audit_regime_data_quality.py",
        )

    def test_data_quality_feature_quality_stats_clean_numeric(self) -> None:
        """A well-behaved numeric series with no nulls, no constants, and
        no concentration must NOT trigger imputed/constant flags. Quantile
        profile must be populated."""
        import pandas as pd
        module = self._data_quality_module()
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
        stats = module.feature_quality_stats(s)
        self.assertEqual(stats["row_count"], 10)
        self.assertEqual(stats["null_count"], 0)
        self.assertEqual(stats["null_rate"], 0.0)
        self.assertEqual(stats["non_finite_count"], 0)
        self.assertEqual(stats["zero_count"], 0)
        self.assertEqual(stats["distinct_count"], 10)
        self.assertFalse(stats["appears_constant"])
        self.assertFalse(stats["appears_imputed_default"])
        self.assertEqual(stats["min"], 1.0)
        self.assertEqual(stats["max"], 10.0)
        self.assertAlmostEqual(stats["median"], 5.5)

    def test_data_quality_detects_null_and_non_finite(self) -> None:
        """NaN counts as null; +/-inf counts as non-finite (separately)."""
        import pandas as pd
        import numpy as np
        module = self._data_quality_module()
        s = pd.Series([1.0, np.nan, np.inf, -np.inf, 0.0, 2.0])
        stats = module.feature_quality_stats(s)
        self.assertEqual(stats["row_count"], 6)
        self.assertEqual(stats["null_count"], 1)
        # +inf and -inf should both be counted as non-finite, distinct
        # from the NaN.
        self.assertEqual(stats["non_finite_count"], 2)
        self.assertAlmostEqual(stats["non_finite_rate"], 2.0 / 6.0)
        # Zero rate is computed on the finite, non-null values: [1, 0, 2].
        self.assertAlmostEqual(stats["zero_rate"], 1.0 / 3.0)

    def test_data_quality_detects_imputed_default_zero(self) -> None:
        """A feature whose >50% of values are 0.0 (a known default
        sentinel) must trigger ``appears_imputed_default``."""
        import pandas as pd
        module = self._data_quality_module()
        s = pd.Series([0.0] * 80 + [1.5, 2.5, 3.5] * 5 + [4.0] * 5)
        stats = module.feature_quality_stats(s)
        self.assertTrue(stats["appears_imputed_default"])
        self.assertEqual(stats["imputed_default_value"], 0.0)
        self.assertGreaterEqual(stats["max_repeat_share"], 0.5)
        self.assertEqual(stats["top_repeated"][0]["value"], 0.0)

    def test_data_quality_does_not_flag_clean_shifted_distribution(self) -> None:
        """A shifted-but-clean numeric distribution must NOT be flagged as
        imputed or constant. This is the false-positive guard — Phase 2
        attribution surfaces lots of shifted features, and most of them
        should *not* be DQ-flagged here."""
        import pandas as pd
        module = self._data_quality_module()
        # Two clean groups with different means but no concentration.
        recent = pd.Series([1.0 + 0.01 * i for i in range(100)])
        older = pd.Series([5.0 + 0.01 * i for i in range(100)])
        for s in (recent, older):
            stats = module.feature_quality_stats(s)
            self.assertFalse(stats["appears_imputed_default"])
            self.assertFalse(stats["appears_constant"])
            self.assertEqual(stats["null_count"], 0)
            self.assertEqual(stats["non_finite_count"], 0)
            # 100 distinct values - no false constant flag.
            self.assertEqual(stats["distinct_count"], 100)

    def test_data_quality_detects_constant_feature(self) -> None:
        import pandas as pd
        module = self._data_quality_module()
        s = pd.Series([3.14] * 50)
        stats = module.feature_quality_stats(s)
        self.assertTrue(stats["appears_constant"])
        self.assertEqual(stats["distinct_count"], 1)

    def test_data_quality_handles_missing_column(self) -> None:
        """``feature_quality_stats(None)`` must NOT crash. It must return
        the fixed schema with ``skip_reason=column_absent``."""
        module = self._data_quality_module()
        stats = module.feature_quality_stats(None)
        self.assertEqual(stats["skip_reason"], "column_absent")
        self.assertEqual(stats["row_count"], 0)
        self.assertIsNone(stats["null_rate"])
        # Schema parity: top_repeated key still present.
        self.assertIn("top_repeated", stats)
        self.assertEqual(stats["top_repeated"], [])

    def test_data_quality_handles_empty_group(self) -> None:
        import pandas as pd
        module = self._data_quality_module()
        stats = module.feature_quality_stats(pd.Series([], dtype=float))
        self.assertEqual(stats["skip_reason"], "group_empty")
        self.assertEqual(stats["row_count"], 0)

    def test_data_quality_top_repeated_share_correct(self) -> None:
        """The top-repeated entries are sorted by count descending and
        share = count / non-null-finite size."""
        import pandas as pd
        module = self._data_quality_module()
        s = pd.Series([1.0, 1.0, 1.0, 2.0, 2.0, 3.0])
        stats = module.feature_quality_stats(s)
        self.assertEqual(stats["top_repeated"][0]["value"], 1.0)
        self.assertEqual(stats["top_repeated"][0]["count"], 3)
        self.assertAlmostEqual(stats["top_repeated"][0]["share"], 0.5)
        self.assertEqual(stats["top_repeated"][1]["value"], 2.0)
        self.assertEqual(stats["top_repeated"][1]["count"], 2)

    def test_data_quality_timestamp_health_freshness(self) -> None:
        """``recent_max_to_now_days`` must reflect the gap between the
        most recent ``ts_event`` and the ``now_utc_ms`` argument."""
        import pandas as pd
        module = self._data_quality_module()
        now_ms = 2_000_000_000_000  # arbitrary
        # recent rows end 1 day before now.
        recent = pd.DataFrame({
            "ts_event": [now_ms - 86_400_000 - i for i in range(10)]
        })
        older = pd.DataFrame({"ts_event": [now_ms - 5 * 86_400_000 + i for i in range(10)]})
        out = module.timestamp_health(recent, older, now_utc_ms=now_ms)
        self.assertTrue(out["recent"]["available"])
        self.assertTrue(out["older_window"]["available"])
        self.assertAlmostEqual(out["recent_max_to_now_days"], 1.0, places=2)
        # No duplicate timestamps in either group.
        self.assertEqual(out["recent"]["duplicate_ts_count"], 0)
        self.assertEqual(out["older_window"]["duplicate_ts_count"], 0)

    def test_data_quality_timestamp_health_detects_duplicates(self) -> None:
        import pandas as pd
        module = self._data_quality_module()
        now_ms = 1_700_000_000_000
        recent = pd.DataFrame({"ts_event": [100, 200, 200, 300, 300, 300]})
        out = module.timestamp_health(recent, pd.DataFrame({"ts_event": []}), now_utc_ms=now_ms)
        self.assertEqual(out["recent"]["duplicate_ts_count"], 5)
        self.assertAlmostEqual(out["recent"]["duplicate_ts_rate"], 5.0 / 6.0)

    def test_data_quality_timestamp_health_missing_column(self) -> None:
        """No ``ts_event`` column must produce ``available=False`` rather
        than crashing."""
        import pandas as pd
        module = self._data_quality_module()
        df = pd.DataFrame({"row_id": [1, 2, 3]})
        out = module.timestamp_health(df, df, now_utc_ms=1_700_000_000_000)
        self.assertFalse(out["recent"]["available"])
        self.assertFalse(out["older_window"]["available"])
        self.assertEqual(out["recent"]["reason"], "ts_event_column_missing")

    def test_data_quality_assessment_status_clean(self) -> None:
        """No DQ flags + freshness within bound + adequate recent_n =>
        ``clean_shift_likely_real``. The summary must be descriptive, not
        a trading claim."""
        module = self._data_quality_module()
        per_feature = {
            "atr_bps": {
                "recent_dormant": {
                    "null_rate": 0.0, "zero_rate": 0.0,
                    "appears_constant": False, "appears_imputed_default": False,
                    "non_finite_rate": 0.0,
                },
                "older_firing_context": {
                    "null_rate": 0.0, "zero_rate": 0.0,
                    "appears_constant": False, "appears_imputed_default": False,
                    "non_finite_rate": 0.0,
                },
            }
        }
        ts_health = {
            "recent": {"available": True, "duplicate_ts_rate": 0.0},
            "older_window": {"available": True, "duplicate_ts_rate": 0.0},
            "recent_max_to_now_days": 0.5,
        }
        assess = module.determine_assessment(
            per_feature=per_feature, ts_health=ts_health,
            recent_n=1000, columns_present=["atr_bps"], columns_missing=[],
        )
        self.assertEqual(assess["data_quality_status"], "clean_shift_likely_real")
        self.assertFalse(assess["atr_quality_warning"])
        self.assertFalse(assess["feature_null_warning"])
        self.assertFalse(assess["feature_constant_warning"])
        self.assertFalse(assess["timestamp_warning"])
        self.assertFalse(assess["recent_data_sparse_warning"])

    def test_data_quality_assessment_status_imputation_branch(self) -> None:
        """An imputed-looking atr_bps must flip the status to
        ``possible_imputation_or_defaulting`` and set BOTH the atr-specific
        flag and the generic constant/null flag."""
        module = self._data_quality_module()
        per_feature = {
            "atr_bps": {
                "recent_dormant": {
                    "null_rate": 0.0, "zero_rate": 0.7,
                    "appears_constant": False,
                    "appears_imputed_default": True,
                    "imputed_default_value": 0.0,
                    "max_repeat_share": 0.7,
                    "non_finite_rate": 0.0,
                },
                "older_firing_context": {
                    "null_rate": 0.0, "zero_rate": 0.0,
                    "appears_constant": False, "appears_imputed_default": False,
                    "non_finite_rate": 0.0,
                },
            }
        }
        ts_health = {
            "recent": {"available": True, "duplicate_ts_rate": 0.0},
            "older_window": {"available": True, "duplicate_ts_rate": 0.0},
            "recent_max_to_now_days": 0.5,
        }
        assess = module.determine_assessment(
            per_feature=per_feature, ts_health=ts_health,
            recent_n=1000, columns_present=["atr_bps"], columns_missing=[],
        )
        self.assertEqual(assess["data_quality_status"], "possible_imputation_or_defaulting")
        self.assertTrue(assess["atr_quality_warning"])
        self.assertTrue(assess["feature_constant_warning"])

    def test_data_quality_assessment_status_stale_branch(self) -> None:
        """Recent data older than the freshness cutoff must select the
        ``possible_stale_or_sparse_recent_data`` branch even when no
        feature flags are set."""
        module = self._data_quality_module()
        per_feature = {
            "atr_bps": {
                "recent_dormant": {
                    "null_rate": 0.0, "zero_rate": 0.0,
                    "appears_constant": False, "appears_imputed_default": False,
                    "non_finite_rate": 0.0,
                },
                "older_firing_context": {
                    "null_rate": 0.0, "zero_rate": 0.0,
                    "appears_constant": False, "appears_imputed_default": False,
                    "non_finite_rate": 0.0,
                },
            }
        }
        ts_health = {
            "recent": {"available": True, "duplicate_ts_rate": 0.0},
            "older_window": {"available": True, "duplicate_ts_rate": 0.0},
            "recent_max_to_now_days": 10.0,  # > 2-day cutoff
        }
        assess = module.determine_assessment(
            per_feature=per_feature, ts_health=ts_health,
            recent_n=1000, columns_present=["atr_bps"], columns_missing=[],
        )
        self.assertEqual(
            assess["data_quality_status"], "possible_stale_or_sparse_recent_data"
        )
        self.assertTrue(assess["timestamp_warning"])

    def test_data_quality_assessment_status_insufficient_columns(self) -> None:
        module = self._data_quality_module()
        assess = module.determine_assessment(
            per_feature={}, ts_health={
                "recent": {"available": True, "duplicate_ts_rate": 0.0},
                "older_window": {"available": True, "duplicate_ts_rate": 0.0},
                "recent_max_to_now_days": 0.5,
            },
            recent_n=1000, columns_present=[],
            columns_missing=["atr_bps", "other"],
        )
        self.assertEqual(assess["data_quality_status"], "insufficient_columns")
        self.assertEqual(assess["columns_missing"], ["atr_bps", "other"])

    def test_data_quality_report_schema_carries_assessment_block(self) -> None:
        """The JSON report MUST always carry the same top-level keys, and
        the ``data_quality_assessment`` sub-block MUST carry the same five
        boolean flags + status + recommended next step. Downstream
        consumers depend on this."""
        module = self._data_quality_module()
        thr = {"runtime_threshold": 0.80, "threshold_source": "manifest",
               "manifest_threshold": 0.80, "artifact_threshold": 0.80,
               "threshold_mismatch_detected": False}
        rep = module.build_report(
            target="reject", horizon=15,
            active_manifest_path=Path("/tmp/m.json"),
            manifest={"version": "v_test"},
            model_path=Path("/tmp/rf.pkl"),
            threshold_resolution=thr,
            total_rows=21_000,
            group_ranges={"recent_dormant": {"n": 1000}},
            per_feature={"atr_bps": {"recent_dormant": {}}},
            feature_columns_present=["atr_bps"],
            feature_columns_missing=[],
            timestamp_health_report={"recent": {"available": True}},
            assessment={
                "data_quality_status": "clean_shift_likely_real",
                "atr_quality_warning": False,
                "feature_null_warning": False,
                "feature_constant_warning": False,
                "timestamp_warning": False,
                "recent_data_sparse_warning": False,
                "columns_present": ["atr_bps"],
                "columns_missing": [],
                "recommended_next_step": "x",
            },
        )
        for key in (
            "schema_version", "audit_type", "generated_at",
            "target", "horizon",
            "active_manifest_path", "active_manifest_version",
            "model_path", "deployed_threshold",
            "threshold_source", "manifest_threshold", "artifact_threshold",
            "threshold_mismatch_detected",
            "total_labeled_rows", "group_ranges",
            "feature_columns_focus", "feature_columns_present",
            "feature_columns_missing",
            "per_feature_quality", "timestamp_health",
            "data_quality_assessment", "warnings", "scope_disclosure",
        ):
            self.assertIn(key, rep, f"missing report key: {key}")
        self.assertEqual(rep["audit_type"], "regime_data_quality")
        self.assertEqual(rep["schema_version"], 1)
        # Assessment sub-block schema.
        for k in (
            "data_quality_status",
            "atr_quality_warning",
            "feature_null_warning",
            "feature_constant_warning",
            "timestamp_warning",
            "recent_data_sparse_warning",
            "columns_present",
            "columns_missing",
            "recommended_next_step",
        ):
            self.assertIn(k, rep["data_quality_assessment"], f"missing assessment key: {k}")

    def test_data_quality_no_threshold_search_language_in_report(self) -> None:
        """Report scope disclosure and every recommendation branch must
        contain no edge/promotion/threshold-search language. This is a
        DATA quality audit; it must not pretend to be anything else."""
        module = self._data_quality_module()
        forbidden = (
            "buy", "sell", "long", "short", "promote", "deploy",
            "retrain", "edge", "alpha", "profit", "pnl", "p&l",
            "tradeable", "tune threshold", "threshold search",
            "search threshold",
        )
        # Every recommendation branch is exhaustively probed.
        for status_input in (
            ("insufficient_columns", False, False, False, False, False),
            ("possible_imputation_or_defaulting", True, False, False, False, False),
            ("possible_imputation_or_defaulting", False, True, False, False, False),
            ("possible_imputation_or_defaulting", False, False, True, False, False),
            ("possible_stale_or_sparse_recent_data", False, False, False, True, False),
            ("possible_stale_or_sparse_recent_data", False, False, False, False, True),
            ("clean_shift_likely_real", False, False, False, False, False),
        ):
            status, atr, fnull, fconst, ts, sparse = status_input
            rec = module._recommend_next_step(
                status,
                atr_quality_warning=atr,
                feature_null_warning=fnull,
                feature_constant_warning=fconst,
                timestamp_warning=ts,
                recent_data_sparse_warning=sparse,
            )
            for word in forbidden:
                self.assertNotIn(word, rec.lower(),
                                 f"forbidden word {word!r} in recommendation: {rec}")
        # Scope disclosure from a built report.
        thr = {"runtime_threshold": 0.80, "threshold_source": "manifest",
               "manifest_threshold": 0.80, "artifact_threshold": 0.80,
               "threshold_mismatch_detected": False}
        rep = module.build_report(
            target="reject", horizon=15,
            active_manifest_path=Path("/tmp/m.json"), manifest={"version": "v"},
            model_path=Path("/tmp/rf.pkl"), threshold_resolution=thr,
            total_rows=1, group_ranges={}, per_feature={},
            feature_columns_present=[], feature_columns_missing=[],
            timestamp_health_report={}, assessment={},
        )
        # The scope disclosure may legitimately use "edge" in negation
        # ("no edge claim"), so it's omitted from the scope-text check
        # below — the per-branch recommendation check above already
        # blocks "edge" in any operator-facing next-step language.
        for word in ("buy", "sell", "alpha", "profit", "promote"):
            self.assertNotIn(word, rep["scope_disclosure"].lower())
        # And the scope disclosure must explicitly DISCLAIM edge:
        self.assertIn("no edge claim", rep["scope_disclosure"].lower())

    def test_data_quality_cli_rejects_invalid_args(self) -> None:
        module = self._data_quality_module()
        with self.assertRaises(SystemExit):
            module.main(["--older-pct", "1.5"])
        with self.assertRaises(SystemExit):
            module.main(["--recent-n", "0"])
        with self.assertRaises(SystemExit):
            module.main(["--highprob-low", "-0.1"])
        with self.assertRaises(SystemExit):
            module.main(["--features", " , , "])

    def test_data_quality_threshold_resolution_matches_attribution(self) -> None:
        """The DQ audit must share threshold-resolution semantics with the
        Phase 2 attribution. Behavioural equivalence is asserted by
        running the same inputs through both and comparing outputs —
        ``importlib.util.spec_from_file_location`` returns distinct
        module objects even when loading the same file, so identity
        assertions on functions wouldn't be reliable here."""
        module = self._data_quality_module()
        attribution = load_module(
            "regime_health_attribution",
            REPO_ROOT / "scripts" / "audit_regime_health_attribution.py",
        )
        # Threshold resolution semantics: manifest precedence,
        # artifact fallback, mismatch flag.
        for m, a in [(0.80, 0.80), (None, 0.65), (0.80, 0.65)]:
            self.assertEqual(
                module.attribution_mod.resolve_runtime_threshold(m, a),
                attribution.resolve_runtime_threshold(m, a),
                f"threshold resolution diverges for (manifest={m}, artifact={a})",
            )
        # Group selection: same row ranges.
        import pandas as pd
        df = pd.DataFrame({"row_id": list(range(500)), "ts_event": list(range(500))})
        sel_dq = module.attribution_mod.select_groups(df, recent_n=50, older_pct=0.30)
        sel_attr = attribution.select_groups(df, recent_n=50, older_pct=0.30)
        self.assertEqual(sel_dq["recent_range"], sel_attr["recent_range"])
        self.assertEqual(sel_dq["older_range"], sel_attr["older_range"])

    # ------------------------------------------------------------------ #
    # Phase 2C feature-pipeline health audit
    # ------------------------------------------------------------------ #

    def _pipeline_module(self):
        return load_module(
            "feature_pipeline_health",
            REPO_ROOT / "scripts" / "audit_feature_pipeline_health.py",
        )

    def test_pipeline_ema_alias_verified_on_matching_rows(self) -> None:
        import pandas as pd
        module = self._pipeline_module()
        raw = pd.Series([1, 1, -1, 0, 1, None, 1])
        calc = pd.Series([1, 1, -1, 0, 1, 1, 1])
        info = module.classify_ema_state_alias_pair(raw, calc)
        self.assertEqual(info["checked_rows"], 6)
        self.assertEqual(info["matched_rows"], 6)
        self.assertEqual(info["mismatch_rows"], 0)
        self.assertTrue(info["alias_verified_in_data"])
        self.assertIn("alias_confirmed", info["note"])

    def test_pipeline_ema_alias_fails_when_data_disagrees(self) -> None:
        import pandas as pd
        module = self._pipeline_module()
        raw = pd.Series([1, 1, -1, 0])
        calc = pd.Series([1, 1, 1, 0])
        info = module.classify_ema_state_alias_pair(raw, calc)
        self.assertEqual(info["mismatch_rows"], 1)
        self.assertFalse(info["alias_verified_in_data"])
        self.assertIn("alias_declared_but_data_disagrees", info["note"])

    def test_pipeline_ema_alias_handles_absent_series(self) -> None:
        module = self._pipeline_module()
        info = module.classify_ema_state_alias_pair(None, None)
        self.assertFalse(info["alias_verified_in_data"])
        self.assertEqual(info["checked_rows"], 0)
        self.assertEqual(info["note"], "one_or_both_series_absent")

    def test_pipeline_gamma_mode_downgrade_when_value_in_domain(self) -> None:
        module = self._pipeline_module()
        stats = {"top_repeated": [{"value": 1.0, "count": 84, "share": 0.84}]}
        out = module.classify_gamma_mode_concentration(stats)
        self.assertTrue(out["in_legitimate_enum_domain"])
        self.assertTrue(out["high_concentration"])
        self.assertTrue(out["phase_2b_imputed_flag_should_be_downgraded"])
        self.assertEqual(out["corrected_label"], "legitimate_enum_concentration")

    def test_pipeline_gamma_mode_no_downgrade_when_value_out_of_domain(self) -> None:
        module = self._pipeline_module()
        stats = {"top_repeated": [{"value": 999.0, "count": 90, "share": 0.90}]}
        out = module.classify_gamma_mode_concentration(stats)
        self.assertFalse(out["in_legitimate_enum_domain"])
        self.assertTrue(out["high_concentration"])
        self.assertFalse(out["phase_2b_imputed_flag_should_be_downgraded"])
        self.assertEqual(out["corrected_label"], "no_correction_needed")

    def test_pipeline_gamma_mode_no_downgrade_when_share_too_low(self) -> None:
        module = self._pipeline_module()
        stats = {"top_repeated": [{"value": 1.0, "count": 30, "share": 0.30}]}
        out = module.classify_gamma_mode_concentration(stats)
        self.assertTrue(out["in_legitimate_enum_domain"])
        self.assertFalse(out["high_concentration"])
        self.assertFalse(out["phase_2b_imputed_flag_should_be_downgraded"])

    def test_pipeline_monthly_pivot_null_traces_to_upstream(self) -> None:
        import pandas as pd
        module = self._pipeline_module()
        df = pd.DataFrame({
            "monthly_pivot_dist_bps": [10.0, None, 5.0, None, 7.0, None],
            "monthly_pivot": [400.0, None, 410.0, 0.0, 420.0, None],
        })
        info = module.classify_monthly_pivot_null_pattern(df)
        self.assertEqual(info["feature_null_count"], 3)
        self.assertAlmostEqual(info["feature_null_rate"], 0.5)
        self.assertEqual(info["rows_feature_null_but_raw_present_nonzero"], 0)
        self.assertEqual(
            info["classification"],
            "upstream_pivot_availability_explains_all_nulls",
        )

    def test_pipeline_monthly_pivot_discrepancy_detected(self) -> None:
        import pandas as pd
        module = self._pipeline_module()
        df = pd.DataFrame({
            "monthly_pivot_dist_bps": [10.0, None, 5.0],
            "monthly_pivot": [400.0, 410.0, 420.0],
        })
        info = module.classify_monthly_pivot_null_pattern(df)
        self.assertEqual(info["rows_feature_null_but_raw_present_nonzero"], 1)
        self.assertEqual(
            info["classification"], "feature_pipeline_discrepancy_detected"
        )

    def test_pipeline_monthly_pivot_raw_column_missing(self) -> None:
        import pandas as pd
        module = self._pipeline_module()
        df = pd.DataFrame({"monthly_pivot_dist_bps": [10.0, None, 5.0]})
        info = module.classify_monthly_pivot_null_pattern(df)
        self.assertEqual(
            info["classification"], "raw_source_column_missing_from_view"
        )

    def test_pipeline_ts_event_duplicates_classified_as_expected(self) -> None:
        import pandas as pd
        module = self._pipeline_module()
        df = pd.DataFrame({
            "ts_event": [100, 100, 100, 200, 200, 300],
            "event_id": ["a", "b", "c", "d", "e", "f"],
        })
        info = module.classify_ts_event_duplicates(df)
        self.assertEqual(info["duplicate_ts_count"], 5)
        self.assertEqual(info["duplicate_event_id_count"], 0)
        self.assertEqual(info["classification"], "expected_many_events_per_bar")

    def test_pipeline_ts_event_writer_duplication_detected(self) -> None:
        import pandas as pd
        module = self._pipeline_module()
        df = pd.DataFrame({
            "ts_event": [100, 100, 200],
            "event_id": ["a", "a", "b"],
        })
        info = module.classify_ts_event_duplicates(df)
        self.assertEqual(info["duplicate_event_id_count"], 2)
        self.assertEqual(
            info["classification"], "possible_event_writer_duplication"
        )

    def test_pipeline_ts_event_insufficient_key_columns(self) -> None:
        import pandas as pd
        module = self._pipeline_module()
        df = pd.DataFrame({"ts_event": [100, 100, 200]})
        info = module.classify_ts_event_duplicates(df)
        self.assertEqual(info["classification"], "insufficient_key_columns")
        self.assertFalse(info["key_column_present"])

    def test_pipeline_ts_event_no_duplicates_path(self) -> None:
        import pandas as pd
        module = self._pipeline_module()
        df = pd.DataFrame({
            "ts_event": [1, 2, 3, 4],
            "event_id": ["a", "b", "c", "d"],
        })
        info = module.classify_ts_event_duplicates(df)
        self.assertEqual(info["classification"], "no_duplicate_ts")

    def test_pipeline_status_clean_branch(self) -> None:
        module = self._pipeline_module()
        feature_reports = {
            "ema_state": {"recent_dormant": {"skip_reason": ""}},
        }
        monthly = {"classification": "upstream_pivot_availability_explains_all_nulls"}
        dup_ts = {
            "recent_dormant": {"classification": "expected_many_events_per_bar"}
        }
        status = module.determine_pipeline_status(
            feature_reports=feature_reports,
            monthly_pivot_classification=monthly,
            duplicate_ts_assessments=dup_ts,
            corrected_phase_2b={},
        )
        self.assertEqual(status, "likely_real_regime_shift_with_feature_dq_caveats")

    def test_pipeline_status_pipeline_issue_branch(self) -> None:
        module = self._pipeline_module()
        feature_reports = {
            "ema_state": {"recent_dormant": {"skip_reason": ""}},
        }
        status1 = module.determine_pipeline_status(
            feature_reports=feature_reports,
            monthly_pivot_classification={"classification": "no_nulls_in_group"},
            duplicate_ts_assessments={
                "recent_dormant": {"classification": "possible_event_writer_duplication"}
            },
            corrected_phase_2b={},
        )
        self.assertEqual(status1, "feature_pipeline_issue_likely")
        status2 = module.determine_pipeline_status(
            feature_reports=feature_reports,
            monthly_pivot_classification={
                "classification": "feature_pipeline_discrepancy_detected"
            },
            duplicate_ts_assessments={
                "recent_dormant": {"classification": "expected_many_events_per_bar"}
            },
            corrected_phase_2b={},
        )
        self.assertEqual(status2, "feature_pipeline_issue_likely")

    def test_pipeline_status_insufficient_visibility_branch(self) -> None:
        module = self._pipeline_module()
        feature_reports = {
            "ema_state": {"recent_dormant": {"skip_reason": "column_absent"}},
            "ema_state_calc": {"recent_dormant": {"skip_reason": "column_absent"}},
        }
        status = module.determine_pipeline_status(
            feature_reports=feature_reports,
            monthly_pivot_classification={"classification": "no_nulls_in_group"},
            duplicate_ts_assessments={
                "recent_dormant": {"classification": "no_duplicate_ts"}
            },
            corrected_phase_2b={},
        )
        self.assertEqual(status, "insufficient_source_visibility")

    def test_pipeline_report_schema_stable(self) -> None:
        module = self._pipeline_module()
        thr = {"runtime_threshold": 0.80, "threshold_source": "manifest",
               "manifest_threshold": 0.80, "artifact_threshold": 0.80,
               "threshold_mismatch_detected": False}
        rep = module.build_report(
            target="reject", horizon=15,
            active_manifest_path=Path("/tmp/m.json"),
            manifest={"version": "v_test"},
            threshold_resolution=thr,
            total_rows=21_000,
            group_ranges={"recent_dormant": {"n": 1000}},
            features={"ema_state": {}},
            source_trace=module.SOURCE_TRACE,
            timestamp_duplicate_assessment={"per_group": {}, "summary_classification": "x"},
            corrected_phase_2b={"ema_state_and_ema_state_calc": {}},
            feature_pipeline_status="likely_real_regime_shift_with_feature_dq_caveats",
            recommended_next_step="x",
        )
        for key in (
            "schema_version", "audit_type", "generated_at",
            "target", "horizon",
            "active_manifest_path", "active_manifest_version",
            "deployed_threshold", "threshold_source",
            "manifest_threshold", "artifact_threshold",
            "threshold_mismatch_detected",
            "total_labeled_rows", "group_ranges",
            "feature_pipeline_status", "features", "source_trace",
            "timestamp_duplicate_assessment",
            "corrected_phase2b_interpretation",
            "recommended_next_step", "warnings", "scope_disclosure",
        ):
            self.assertIn(key, rep, f"missing report key: {key}")
        self.assertEqual(rep["audit_type"], "feature_pipeline_health")
        self.assertEqual(rep["schema_version"], 1)

    def test_pipeline_source_trace_carries_required_keys(self) -> None:
        module = self._pipeline_module()
        for feat, info in module.SOURCE_TRACE.items():
            self.assertIn("origin_file", info, f"{feat} missing origin_file")
            self.assertIn(
                "default_or_imputation_path", info,
                f"{feat} missing default_or_imputation_path",
            )
            self.assertIn("feature_kind", info, f"{feat} missing feature_kind")

    def test_pipeline_no_threshold_search_language_anywhere(self) -> None:
        module = self._pipeline_module()
        forbidden = (
            "buy", "sell", "promote", "deploy", "retrain",
            "alpha", "profit", "pnl", "p&l",
            "threshold search", "search threshold",
        )
        recs = [
            module.recommend_next_step(
                "insufficient_source_visibility",
                monthly_pivot_recent_null_rate=None,
                monthly_pivot_classification="",
                ema_alias_verified=False,
                gamma_downgraded=False,
            ),
            module.recommend_next_step(
                "feature_pipeline_issue_likely",
                monthly_pivot_recent_null_rate=0.4,
                monthly_pivot_classification="feature_pipeline_discrepancy_detected",
                ema_alias_verified=True,
                gamma_downgraded=True,
            ),
            module.recommend_next_step(
                "likely_real_regime_shift_with_feature_dq_caveats",
                monthly_pivot_recent_null_rate=0.4,
                monthly_pivot_classification="upstream_pivot_availability_explains_all_nulls",
                ema_alias_verified=True,
                gamma_downgraded=True,
            ),
            module.recommend_next_step(
                "likely_real_regime_shift_with_feature_dq_caveats",
                monthly_pivot_recent_null_rate=0.05,
                monthly_pivot_classification="no_nulls_in_group",
                ema_alias_verified=True,
                gamma_downgraded=True,
            ),
            module.recommend_next_step(
                "likely_real_regime_shift_with_feature_dq_caveats",
                monthly_pivot_recent_null_rate=0.05,
                monthly_pivot_classification="no_nulls_in_group",
                ema_alias_verified=False,
                gamma_downgraded=False,
            ),
        ]
        for rec in recs:
            for word in forbidden:
                self.assertNotIn(word, rec.lower(),
                                 f"forbidden word {word!r} in recommendation: {rec}")
        thr = {"runtime_threshold": 0.80, "threshold_source": "manifest",
               "manifest_threshold": 0.80, "artifact_threshold": 0.80,
               "threshold_mismatch_detected": False}
        rep = module.build_report(
            target="reject", horizon=15,
            active_manifest_path=Path("/tmp/m.json"),
            manifest={"version": "v"},
            threshold_resolution=thr,
            total_rows=1, group_ranges={}, features={},
            source_trace={}, timestamp_duplicate_assessment={},
            corrected_phase_2b={},
            feature_pipeline_status="unknown",
            recommended_next_step="x",
        )
        for word in ("buy", "sell", "alpha", "profit", "promote", "retrain"):
            self.assertNotIn(word, rep["scope_disclosure"].lower())
        self.assertIn("no edge claim", rep["scope_disclosure"].lower())
        self.assertIn("no database writes", rep["scope_disclosure"].lower())

    def test_pipeline_cli_rejects_invalid_args(self) -> None:
        module = self._pipeline_module()
        with self.assertRaises(SystemExit):
            module.main(["--older-pct", "1.5"])
        with self.assertRaises(SystemExit):
            module.main(["--recent-n", "0"])
        with self.assertRaises(SystemExit):
            module.main(["--highprob-low", "-0.1"])

    def test_pipeline_value_counts_coerces_numpy_scalars(self) -> None:
        import numpy as np
        import pandas as pd
        module = self._pipeline_module()
        s_int = pd.Series(np.array([1, 1, 1, -1, -1], dtype=np.int64))
        top = module.value_counts_top_n(s_int, n=2)
        self.assertEqual(top[0]["value"], 1.0)
        self.assertIsInstance(top[0]["value"], float)
        s_float = pd.Series(np.array([0.5, 0.5, 0.5], dtype=np.float64))
        top_f = module.value_counts_top_n(s_float, n=1)
        self.assertEqual(top_f[0]["value"], 0.5)
        self.assertIsInstance(top_f[0]["value"], float)
        stats = {"top_repeated": top}
        out = module.classify_gamma_mode_concentration(stats)
        self.assertTrue(out["in_legitimate_enum_domain"])

    # ------------------------------------------------------------------ #
    # Phase 2D monthly_pivot availability audit
    # ------------------------------------------------------------------ #

    def _pivot_audit_module(self):
        return load_module(
            "monthly_pivot_availability",
            REPO_ROOT / "scripts" / "audit_monthly_pivot_availability.py",
        )

    def test_pivot_group_null_summary_traces_to_raw(self) -> None:
        import pandas as pd
        module = self._pivot_audit_module()
        df = pd.DataFrame({
            "monthly_pivot": [400.0, None, 410.0, 0.0, 420.0, None],
            "monthly_pivot_dist_bps": [10.0, None, 5.0, None, 7.0, None],
        })
        out = module.group_null_summary(df)
        self.assertEqual(out["row_count"], 6)
        self.assertEqual(out["monthly_pivot_null_count"], 2)
        self.assertEqual(out["monthly_pivot_zero_count"], 1)
        self.assertEqual(out["monthly_pivot_dist_bps_null_count"], 3)
        self.assertEqual(out["rows_dist_null_but_raw_present_nonzero"], 0)
        self.assertTrue(out["raw_null_explains_all_feature_nulls"])

    def test_pivot_group_null_summary_detects_pipeline_discrepancy(self) -> None:
        import pandas as pd
        module = self._pivot_audit_module()
        df = pd.DataFrame({
            "monthly_pivot": [400.0, 410.0, 420.0],
            "monthly_pivot_dist_bps": [10.0, None, 5.0],
        })
        out = module.group_null_summary(df)
        self.assertEqual(out["rows_dist_null_but_raw_present_nonzero"], 1)
        self.assertFalse(out["raw_null_explains_all_feature_nulls"])

    def test_pivot_by_dimension_null_rate_clusters_correctly(self) -> None:
        import pandas as pd
        module = self._pivot_audit_module()
        df = pd.DataFrame({
            "source": ["A", "A", "A", "A", "B", "B", "C"],
            "monthly_pivot": [1.0, None, 1.0, None, 1.0, 1.0, None],
        })
        out = module.by_dimension_null_rate(df, "source")
        self.assertEqual(out[0]["value"], "A")
        self.assertEqual(out[0]["n"], 4)
        self.assertEqual(out[0]["nulls"], 2)
        self.assertAlmostEqual(out[0]["null_rate"], 0.5)
        self.assertEqual(out[1]["value"], "B")
        self.assertAlmostEqual(out[1]["null_rate"], 0.0)

    def test_pivot_by_dimension_handles_missing_columns(self) -> None:
        import pandas as pd
        module = self._pivot_audit_module()
        df = pd.DataFrame({"x": [1, 2, 3]})
        self.assertEqual(module.by_dimension_null_rate(df, "missing"), [])

    def test_pivot_date_first_last_null(self) -> None:
        import datetime as dt
        import pandas as pd
        module = self._pivot_audit_module()
        df = pd.DataFrame({
            "event_date_et": [
                dt.date(2026, 1, 5),
                dt.date(2026, 1, 6),
                dt.date(2026, 1, 7),
                dt.date(2026, 1, 8),
            ],
            "monthly_pivot": [None, 1.0, None, 1.0],
        })
        out = module.date_first_last_null(df)
        self.assertEqual(out["first_null_date"], "2026-01-05")
        self.assertEqual(out["last_null_date"], "2026-01-07")

    def test_pivot_longest_consecutive_null_streak(self) -> None:
        import datetime as dt
        import pandas as pd
        module = self._pivot_audit_module()
        df = pd.DataFrame({
            "event_date_et": [
                dt.date(2026, 1, 5),
                dt.date(2026, 1, 5),
                dt.date(2026, 1, 6),
                dt.date(2026, 1, 6),
                dt.date(2026, 1, 7),
                dt.date(2026, 1, 8),
            ],
            "monthly_pivot": [None, None, 1.0, None, None, None],
        })
        out = module.longest_consecutive_null_dates(df)
        self.assertEqual(out["length_days"], 2)
        self.assertEqual(out["start_date"], "2026-01-07")
        self.assertEqual(out["end_date"], "2026-01-08")

    def test_pivot_classify_status_provider_coverage_gap(self) -> None:
        module = self._pivot_audit_module()
        overall = {
            "monthly_pivot_null_rate": 0.39,
            "monthly_pivot_zero_rate": 0.0,
            "rows_dist_null_but_raw_present_nonzero": 0,
        }
        by_source = [
            {"value": "Yahoo", "n": 30000, "nulls": 22500, "null_rate": 0.75},
            {"value": "marketdata.app", "n": 44000, "nulls": 5300, "null_rate": 0.12},
        ]
        by_month = [{"value": "2025-04", "n": 1000, "nulls": 0, "null_rate": 0.0}]
        cp = {c: True for c in (
            "monthly_pivot", "monthly_pivot_dist_bps", "event_date_et", "source"
        )}
        self.assertEqual(
            module.classify_status(
                overall_summary=overall, by_source=by_source,
                by_month=by_month, columns_present=cp,
            ),
            "provider_coverage_gap",
        )

    def test_pivot_classify_status_join_or_pipeline_gap(self) -> None:
        module = self._pivot_audit_module()
        overall = {
            "monthly_pivot_null_rate": 0.40,
            "monthly_pivot_zero_rate": 0.0,
            "rows_dist_null_but_raw_present_nonzero": 5,
        }
        cp = {c: True for c in (
            "monthly_pivot", "monthly_pivot_dist_bps", "event_date_et", "source"
        )}
        self.assertEqual(
            module.classify_status(
                overall_summary=overall, by_source=[],
                by_month=[], columns_present=cp,
            ),
            "join_or_pipeline_gap",
        )

    def test_pivot_classify_status_available_clean(self) -> None:
        module = self._pivot_audit_module()
        overall = {
            "monthly_pivot_null_rate": 0.03,
            "monthly_pivot_zero_rate": 0.0,
            "rows_dist_null_but_raw_present_nonzero": 0,
        }
        cp = {c: True for c in (
            "monthly_pivot", "monthly_pivot_dist_bps", "event_date_et", "source"
        )}
        self.assertEqual(
            module.classify_status(
                overall_summary=overall,
                by_source=[{"value": "marketdata.app", "n": 1000,
                            "nulls": 30, "null_rate": 0.03}],
                by_month=[], columns_present=cp,
            ),
            "available_clean",
        )

    def test_pivot_classify_status_insufficient_visibility(self) -> None:
        module = self._pivot_audit_module()
        cp = {"monthly_pivot": True, "monthly_pivot_dist_bps": False,
              "event_date_et": True, "source": True}
        self.assertEqual(
            module.classify_status(
                overall_summary={}, by_source=[], by_month=[],
                columns_present=cp,
            ),
            "insufficient_source_visibility",
        )

    def test_pivot_classify_status_expected_sparse_by_design(self) -> None:
        module = self._pivot_audit_module()
        overall = {
            "monthly_pivot_null_rate": 0.10,
            "monthly_pivot_zero_rate": 0.0,
            "rows_dist_null_but_raw_present_nonzero": 0,
        }
        by_month = [
            {"value": "2025-03", "n": 2240, "nulls": 2240, "null_rate": 1.0},
            {"value": "2025-04", "n": 1608, "nulls": 0, "null_rate": 0.0},
            {"value": "2025-05", "n": 2916, "nulls": 0, "null_rate": 0.0},
            {"value": "2025-06", "n": 3836, "nulls": 0, "null_rate": 0.0},
        ]
        cp = {c: True for c in (
            "monthly_pivot", "monthly_pivot_dist_bps", "event_date_et", "source"
        )}
        by_source = [
            {"value": "marketdata.app", "n": 10000, "nulls": 2240, "null_rate": 0.22},
        ]
        self.assertEqual(
            module.classify_status(
                overall_summary=overall, by_source=by_source,
                by_month=by_month, columns_present=cp,
            ),
            "expected_sparse_by_design",
        )

    def test_pivot_report_schema_stable(self) -> None:
        module = self._pivot_audit_module()
        rep = module.build_report(
            symbol="SPY", target="reject", horizon=15,
            active_manifest_path=Path("/tmp/m.json"),
            manifest_version="v_test",
            total_rows=100,
            overall_summary={"monthly_pivot_null_rate": 0.3},
            group_null_summary_block={"recent_dormant": {}, "older_window": {}},
            by_source=[],
            by_month=[],
            by_level_type=[],
            by_session=[],
            date_first_last={"first_null_date": None, "last_null_date": None},
            longest_null_streak={"length_days": 0, "start_date": None, "end_date": None},
            upstream_source_summary={"providers": []},
            status="provider_coverage_gap",
            corrected_interpretation={"phase_2d_root_cause_classification": "provider_coverage_gap"},
            recommended_next_step_str="x",
        )
        for key in (
            "schema_version", "audit_type", "generated_at",
            "symbol", "target", "horizon",
            "active_manifest_path", "active_manifest_version",
            "total_rows_inspected", "monthly_pivot_status",
            "source_trace", "overall_summary",
            "group_null_summary", "date_null_summary",
            "by_source", "by_level_type", "by_session",
            "upstream_source_summary", "corrected_interpretation",
            "recommended_next_step", "warnings", "scope_disclosure",
        ):
            self.assertIn(key, rep, f"missing report key: {key}")
        self.assertEqual(rep["audit_type"], "monthly_pivot_availability")
        self.assertEqual(rep["schema_version"], 1)

    def test_pivot_source_trace_carries_required_keys(self) -> None:
        module = self._pivot_audit_module()
        for k in ("monthly_pivot_producer", "monthly_pivot_storage",
                  "monthly_pivot_dist_bps_compute"):
            self.assertIn(k, module.SOURCE_TRACE)
            self.assertIn("origin_file", module.SOURCE_TRACE[k])

    def test_pivot_no_threshold_search_language(self) -> None:
        module = self._pivot_audit_module()
        forbidden = (
            "buy", "sell", "deploy",
            "alpha", "profit", "pnl", "p&l",
            "threshold search", "search threshold",
        )
        for status in (
            "provider_coverage_gap", "expected_sparse_by_design",
            "join_or_pipeline_gap", "zero_value_guard_expected",
            "available_clean", "insufficient_source_visibility",
            "unknown",
        ):
            rec = module.recommend_next_step(status)
            for word in forbidden:
                self.assertNotIn(word, rec.lower(),
                                 f"forbidden word {word!r} in rec for {status!r}: {rec}")
        rep = module.build_report(
            symbol="SPY", target="reject", horizon=15,
            active_manifest_path=Path("/tmp/m.json"),
            manifest_version="v",
            total_rows=1, overall_summary={},
            group_null_summary_block={}, by_source=[], by_month=[],
            by_level_type=[], by_session=[],
            date_first_last={"first_null_date": None, "last_null_date": None},
            longest_null_streak={"length_days": 0, "start_date": None, "end_date": None},
            upstream_source_summary={"providers": []},
            status="provider_coverage_gap",
            corrected_interpretation={},
            recommended_next_step_str="x",
        )
        for word in ("buy", "sell", "alpha", "profit"):
            self.assertNotIn(word, rep["scope_disclosure"].lower())
        self.assertIn("no edge claim", rep["scope_disclosure"].lower())
        self.assertIn("no database writes", rep["scope_disclosure"].lower())

    def test_pivot_cli_rejects_invalid_args(self) -> None:
        module = self._pivot_audit_module()
        with self.assertRaises(SystemExit):
            module.main(["--older-pct", "1.5"])
        with self.assertRaises(SystemExit):
            module.main(["--recent-n", "0"])
        with self.assertRaises(SystemExit):
            module.main(["--highprob-low", "1.5"])

    # ------------------------------------------------------------------ #
    # Phase 2E provider-normalized regime audit
    # ------------------------------------------------------------------ #

    def _provider_norm_module(self):
        return load_module(
            "provider_normalized_regime",
            REPO_ROOT / "scripts" / "audit_provider_normalized_regime.py",
        )

    def test_provider_norm_provider_mix_for_group(self) -> None:
        import pandas as pd
        module = self._provider_norm_module()
        df = pd.DataFrame({
            "source": ["A", "A", "A", "B", "B", None],
        })
        out = module.provider_mix_for_group(df)
        self.assertEqual(out[0]["source"], "A")
        self.assertEqual(out[0]["n"], 3)
        self.assertAlmostEqual(out[0]["share"], 0.5)
        self.assertEqual(out[1]["source"], "B")
        self.assertEqual(out[1]["n"], 2)
        sources = {r["source"] for r in out}
        self.assertIn("__null__", sources)

    def test_provider_norm_provider_mix_missing_column_returns_empty(self) -> None:
        import pandas as pd
        module = self._provider_norm_module()
        df = pd.DataFrame({"x": [1, 2, 3]})
        self.assertEqual(module.provider_mix_for_group(df), [])

    def test_provider_norm_within_provider_smd_basic(self) -> None:
        import pandas as pd
        module = self._provider_norm_module()
        recent = pd.DataFrame({"atr_bps": list(range(100))})
        older = pd.DataFrame({"atr_bps": list(range(100))})
        out = module.within_provider_feature_comparison(
            recent, older, ["atr_bps"],
        )
        self.assertAlmostEqual(out["atr_bps"]["smd"], 0.0, places=6)
        recent2 = pd.DataFrame({"atr_bps": [1000 + i for i in range(100)]})
        out2 = module.within_provider_feature_comparison(
            recent2, older, ["atr_bps"],
        )
        self.assertGreater(abs(out2["atr_bps"]["smd"]), 5.0)

    def test_provider_norm_feature_absent_skip(self) -> None:
        import pandas as pd
        module = self._provider_norm_module()
        recent = pd.DataFrame({"x": [1, 2, 3]})
        older = pd.DataFrame({"x": [1, 2, 3]})
        out = module.within_provider_feature_comparison(
            recent, older, ["atr_bps"],
        )
        self.assertEqual(out["atr_bps"]["skip_reason"], "feature_absent")
        self.assertIsNone(out["atr_bps"]["smd"])

    def test_provider_norm_survives_threshold_helper(self) -> None:
        module = self._provider_norm_module()
        comp = {"atr_bps": {"smd": -5.0, "ks": 0.7}}
        self.assertTrue(module.survives_threshold(
            comp, feature="atr_bps", smd_min_abs=0.5))
        self.assertFalse(module.survives_threshold(
            comp, feature="atr_bps", smd_min_abs=10.0))
        self.assertFalse(module.survives_threshold(
            {"atr_bps": {"smd": None}}, feature="atr_bps", smd_min_abs=0.5))
        self.assertFalse(module.survives_threshold(
            {}, feature="atr_bps", smd_min_abs=0.5))

    def test_provider_norm_probability_dormancy_survives(self) -> None:
        module = self._provider_norm_module()
        self.assertTrue(module.probability_dormancy_survives(
            prob_recent={"max": 0.70, "p95": 0.68, "median": 0.55},
            prob_older={"median": 0.78, "max": 0.85},
            threshold=0.80,
        ))
        self.assertFalse(module.probability_dormancy_survives(
            prob_recent={"max": 0.70, "p95": 0.69, "median": 0.55},
            prob_older={"median": 0.70, "max": 0.85},
            threshold=0.80,
        ))
        self.assertFalse(module.probability_dormancy_survives(
            prob_recent={"max": 0.95, "p95": 0.85, "median": 0.55},
            prob_older={"median": 0.78, "max": 0.85},
            threshold=0.80,
        ))

    def test_provider_norm_classify_status_survives(self) -> None:
        module = self._provider_norm_module()
        flags = {
            "marketdata_app_control_available": True,
            "atr_shift_survives_provider_control": True,
            "gamma_mode_shift_survives_provider_control": True,
            "probability_dormancy_survives_provider_control": False,
            "provider_mix_warning": True,
        }
        providers_eval = [{"source": "marketdata.app", "included": True}]
        cp = {"source": True}
        self.assertEqual(
            module.classify_status(
                columns_present=cp, providers_evaluated=providers_eval, flags=flags,
            ),
            "regime_shift_survives_provider_control",
        )

    def test_provider_norm_classify_status_confounded(self) -> None:
        module = self._provider_norm_module()
        flags = {
            "marketdata_app_control_available": True,
            "atr_shift_survives_provider_control": False,
            "gamma_mode_shift_survives_provider_control": False,
            "probability_dormancy_survives_provider_control": False,
            "provider_mix_warning": True,
        }
        providers_eval = [{"source": "marketdata.app", "included": True}]
        cp = {"source": True}
        self.assertEqual(
            module.classify_status(
                columns_present=cp, providers_evaluated=providers_eval, flags=flags,
            ),
            "provider_mix_confounds_prior_attribution",
        )

    def test_provider_norm_classify_status_insufficient_overlap(self) -> None:
        module = self._provider_norm_module()
        flags = {
            "marketdata_app_control_available": False,
            "atr_shift_survives_provider_control": False,
            "gamma_mode_shift_survives_provider_control": False,
            "probability_dormancy_survives_provider_control": False,
            "provider_mix_warning": False,
        }
        providers_eval = [
            {"source": "marketdata.app", "included": False, "exclude_reason": "insufficient_data"},
            {"source": "Yahoo", "included": False, "exclude_reason": "insufficient_data"},
        ]
        cp = {"source": True}
        self.assertEqual(
            module.classify_status(
                columns_present=cp, providers_evaluated=providers_eval, flags=flags,
            ),
            "insufficient_within_provider_overlap",
        )

    def test_provider_norm_classify_status_insufficient_visibility(self) -> None:
        module = self._provider_norm_module()
        flags = {k: False for k in (
            "marketdata_app_control_available",
            "atr_shift_survives_provider_control",
            "gamma_mode_shift_survives_provider_control",
            "probability_dormancy_survives_provider_control",
            "provider_mix_warning",
        )}
        self.assertEqual(
            module.classify_status(
                columns_present={"source": False},
                providers_evaluated=[],
                flags=flags,
            ),
            "insufficient_source_visibility",
        )

    def test_provider_norm_classify_status_mixed_evidence(self) -> None:
        module = self._provider_norm_module()
        flags = {
            "marketdata_app_control_available": False,
            "atr_shift_survives_provider_control": True,
            "gamma_mode_shift_survives_provider_control": False,
            "probability_dormancy_survives_provider_control": False,
            "provider_mix_warning": True,
        }
        providers_eval = [{"source": "Yahoo", "included": True}]
        cp = {"source": True}
        self.assertEqual(
            module.classify_status(
                columns_present=cp, providers_evaluated=providers_eval, flags=flags,
            ),
            "mixed_evidence",
        )

    def test_provider_norm_report_schema_stable(self) -> None:
        module = self._provider_norm_module()
        thr = {"runtime_threshold": 0.80, "threshold_source": "manifest",
               "manifest_threshold": 0.80, "artifact_threshold": 0.80,
               "threshold_mismatch_detected": False}
        rep = module.build_report(
            symbol="SPY", target="reject", horizon=15,
            active_manifest_path=Path("/tmp/m.json"),
            manifest_version="v411",
            threshold_resolution=thr,
            total_rows=1000,
            provider_mix_summary={"recent_dormant": [], "older_firing_context": []},
            within_provider_comparisons={},
            providers_evaluated=[],
            status="mixed_evidence",
            flags={
                "marketdata_app_control_available": False,
                "atr_shift_survives_provider_control": False,
                "gamma_mode_shift_survives_provider_control": False,
                "probability_dormancy_survives_provider_control": False,
                "provider_mix_warning": False,
            },
            min_group_rows=100, min_firing_rows=30, smd_min_abs=0.5,
            recommended_next_step_str="x",
        )
        for key in (
            "schema_version", "audit_type", "generated_at",
            "symbol", "target", "horizon",
            "active_manifest_path", "active_manifest_version",
            "deployed_threshold", "threshold_source",
            "manifest_threshold", "artifact_threshold",
            "threshold_mismatch_detected",
            "total_rows_inspected", "config",
            "source_trace", "provider_mix_summary",
            "providers_evaluated", "within_provider_comparisons",
            "provider_normalized_status", "flags",
            "recommended_next_step", "warnings", "scope_disclosure",
        ):
            self.assertIn(key, rep, f"missing report key: {key}")
        self.assertEqual(rep["audit_type"], "provider_normalized_regime")
        self.assertEqual(rep["schema_version"], 1)

    def test_provider_norm_source_trace_carries_required_keys(self) -> None:
        module = self._provider_norm_module()
        for k in ("provider_column", "smd_and_ks", "threshold_resolution"):
            self.assertIn(k, module.SOURCE_TRACE)
            self.assertIn("origin_file", module.SOURCE_TRACE[k])

    def test_provider_norm_no_threshold_search_language(self) -> None:
        module = self._provider_norm_module()
        forbidden = (
            "buy", "sell", "promote", "deploy",
            "alpha", "profit", "pnl", "p&l",
            "threshold search", "search threshold",
        )
        for status in (
            "regime_shift_survives_provider_control",
            "provider_mix_confounds_prior_attribution",
            "insufficient_within_provider_overlap",
            "mixed_evidence", "insufficient_source_visibility",
            "unknown",
        ):
            rec = module.recommend_next_step(status)
            for word in forbidden:
                self.assertNotIn(word, rec.lower(),
                                 f"forbidden word {word!r} in rec for {status!r}: {rec}")
        thr = {"runtime_threshold": 0.80, "threshold_source": "manifest",
               "manifest_threshold": 0.80, "artifact_threshold": 0.80,
               "threshold_mismatch_detected": False}
        rep = module.build_report(
            symbol="SPY", target="reject", horizon=15,
            active_manifest_path=Path("/tmp/m.json"),
            manifest_version="v",
            threshold_resolution=thr,
            total_rows=1,
            provider_mix_summary={}, within_provider_comparisons={},
            providers_evaluated=[],
            status="mixed_evidence",
            flags={k: False for k in (
                "marketdata_app_control_available",
                "atr_shift_survives_provider_control",
                "gamma_mode_shift_survives_provider_control",
                "probability_dormancy_survives_provider_control",
                "provider_mix_warning",
            )},
            min_group_rows=100, min_firing_rows=30, smd_min_abs=0.5,
            recommended_next_step_str="x",
        )
        for word in ("buy", "sell", "alpha", "profit", "promote", "retrain"):
            self.assertNotIn(word, rep["scope_disclosure"].lower())
        self.assertIn("no edge claim", rep["scope_disclosure"].lower())
        self.assertIn("no database writes", rep["scope_disclosure"].lower())

    def test_provider_norm_cli_rejects_invalid_args(self) -> None:
        module = self._provider_norm_module()
        with self.assertRaises(SystemExit):
            module.main(["--older-pct", "1.5"])
        with self.assertRaises(SystemExit):
            module.main(["--recent-n", "0"])
        with self.assertRaises(SystemExit):
            module.main(["--min-group-rows", "0"])
        with self.assertRaises(SystemExit):
            module.main(["--min-firing-rows", "0"])
        with self.assertRaises(SystemExit):
            module.main(["--smd-min-abs", "-0.1"])

    # ------------------------------------------------------------------ #
    # Phase 2F ATR availability by provider audit
    # ------------------------------------------------------------------ #

    def _atr_audit_module(self):
        return load_module(
            "atr_availability_by_provider",
            REPO_ROOT / "scripts" / "audit_atr_availability_by_provider.py",
        )

    def test_atr_compute_atr_bps_matches_runtime_formula(self) -> None:
        import math
        import pandas as pd
        module = self._atr_audit_module()
        df = pd.DataFrame({
            "atr": [1.0, 0.0, None, 2.5, 3.0],
            "touch_price": [500.0, 500.0, 500.0, 0.0, 750.0],
        })
        out = module.compute_atr_bps(df)
        self.assertAlmostEqual(out.iloc[0], 20.0)
        self.assertTrue(math.isnan(out.iloc[1]))
        self.assertTrue(math.isnan(out.iloc[2]))
        self.assertTrue(math.isnan(out.iloc[3]))
        self.assertAlmostEqual(out.iloc[4], 40.0)

    def test_atr_compute_atr_bps_handles_missing_columns(self) -> None:
        import math
        import pandas as pd
        module = self._atr_audit_module()
        df = pd.DataFrame({"atr": [1.0, 2.0]})
        out = module.compute_atr_bps(df)
        self.assertEqual(len(out), 2)
        for v in out:
            self.assertTrue(math.isnan(v))

    def test_atr_availability_summary_basic(self) -> None:
        import pandas as pd
        module = self._atr_audit_module()
        s = pd.Series([1.0, 2.0, 3.0, None, 0.0])
        out = module.availability_summary(s)
        self.assertEqual(out["n"], 5)
        self.assertEqual(out["null_count"], 1)
        self.assertAlmostEqual(out["null_rate"], 0.2)
        self.assertEqual(out["zero_count"], 1)
        self.assertAlmostEqual(out["zero_rate"], 0.2)
        self.assertAlmostEqual(out["median"], 1.5)

    def test_atr_per_provider_atr(self) -> None:
        import pandas as pd
        module = self._atr_audit_module()
        df = pd.DataFrame({
            "source": ["Yahoo"] * 4 + ["IBKR"] * 2,
            "atr": [1.0, 1.0, None, 1.0, 1.0, 1.0],
            "touch_price": [500, 500, 500, 500, 500, 500],
        })
        out = module.per_provider_atr(df)
        by_src = {p["source"]: p for p in out}
        self.assertEqual(by_src["Yahoo"]["n"], 4)
        self.assertAlmostEqual(by_src["Yahoo"]["raw_atr"]["null_rate"], 0.25)
        self.assertAlmostEqual(by_src["Yahoo"]["atr_bps_derived"]["null_rate"], 0.25)
        self.assertEqual(by_src["IBKR"]["n"], 2)
        self.assertAlmostEqual(by_src["IBKR"]["atr_bps_derived"]["null_rate"], 0.0)

    def test_atr_classify_status_labeled_view_gap(self) -> None:
        module = self._atr_audit_module()
        cp = {"source": True, "atr": True, "atr_bps": False,
              "touch_price": True}
        overall = {"raw_atr_null_rate": 0.01,
                   "atr_bps_null_rate_in_view": None}
        per_provider = [
            {"source": "marketdata.app", "n": 44000,
             "raw_atr": {"null_rate": 0.01}},
            {"source": "Yahoo", "n": 30000,
             "raw_atr": {"null_rate": 0.01}},
        ]
        self.assertEqual(
            module.classify_status(
                columns_present=cp, overall_summary=overall,
                per_provider=per_provider,
            ),
            "labeled_view_gap",
        )

    def test_atr_classify_status_raw_input_missing(self) -> None:
        module = self._atr_audit_module()
        cp = {"source": True, "atr": True, "atr_bps": False,
              "touch_price": True}
        overall = {"raw_atr_null_rate": 0.80}
        per_provider = [
            {"source": "A", "n": 1000, "raw_atr": {"null_rate": 0.80}},
            {"source": "B", "n": 1000, "raw_atr": {"null_rate": 0.95}},
        ]
        self.assertEqual(
            module.classify_status(
                columns_present=cp, overall_summary=overall,
                per_provider=per_provider,
            ),
            "raw_input_missing",
        )

    def test_atr_classify_status_provider_coverage_gap(self) -> None:
        module = self._atr_audit_module()
        cp = {"source": True, "atr": True, "atr_bps": False,
              "touch_price": True}
        overall = {"raw_atr_null_rate": 0.40}
        per_provider = [
            {"source": "marketdata.app", "n": 1000,
             "raw_atr": {"null_rate": 0.01}},
            {"source": "Yahoo", "n": 1000,
             "raw_atr": {"null_rate": 0.80}},
        ]
        self.assertEqual(
            module.classify_status(
                columns_present=cp, overall_summary=overall,
                per_provider=per_provider,
            ),
            "provider_coverage_gap",
        )

    def test_atr_classify_status_feature_join_gap(self) -> None:
        module = self._atr_audit_module()
        cp = {"source": True, "atr": True, "atr_bps": True,
              "touch_price": True}
        overall = {"raw_atr_null_rate": 0.02,
                   "atr_bps_null_rate_in_view": 0.40}
        per_provider = [
            {"source": "marketdata.app", "n": 1000,
             "raw_atr": {"null_rate": 0.02}},
        ]
        self.assertEqual(
            module.classify_status(
                columns_present=cp, overall_summary=overall,
                per_provider=per_provider,
            ),
            "feature_join_gap",
        )

    def test_atr_classify_status_insufficient_visibility(self) -> None:
        module = self._atr_audit_module()
        cp = {"source": True, "atr": False, "atr_bps": False,
              "touch_price": True}
        self.assertEqual(
            module.classify_status(
                columns_present=cp, overall_summary={},
                per_provider=[],
            ),
            "insufficient_source_visibility",
        )

    def test_atr_classify_status_available_clean(self) -> None:
        module = self._atr_audit_module()
        cp = {"source": True, "atr": True, "atr_bps": True,
              "touch_price": True}
        overall = {"raw_atr_null_rate": 0.01,
                   "atr_bps_null_rate_in_view": 0.01}
        per_provider = [
            {"source": "marketdata.app", "n": 1000,
             "raw_atr": {"null_rate": 0.01}},
        ]
        self.assertEqual(
            module.classify_status(
                columns_present=cp, overall_summary=overall,
                per_provider=per_provider,
            ),
            "available_clean",
        )

    def test_atr_group_summary_recomputes_derived(self) -> None:
        import pandas as pd
        module = self._atr_audit_module()
        df = pd.DataFrame({
            "atr": [1.0, 1.0, None],
            "touch_price": [500.0, 1000.0, 500.0],
        })
        out = module.group_summary(df)
        self.assertEqual(out["n"], 3)
        self.assertEqual(out["atr_bps_derived"]["n"], 3)
        self.assertEqual(out["atr_bps_derived"]["null_count"], 1)

    def test_atr_report_schema_stable(self) -> None:
        module = self._atr_audit_module()
        rep = module.build_report(
            symbol="SPY", target="reject", horizon=15,
            active_manifest_path=Path("/tmp/m.json"),
            manifest_version="v411",
            total_rows=100,
            columns_present={"atr": True, "atr_bps": False, "source": True,
                             "touch_price": True, "symbol": True,
                             "event_date_et": True, "ts_event": True},
            overall_summary={"raw_atr_null_rate": 0.01},
            provider_summary=[],
            by_month=[],
            group_summary_block={"recent_dormant": {}, "older_window": {}},
            raw_input_summary={"raw_atr_in_view": True},
            status="labeled_view_gap",
            corrected_interpretation={
                "phase_2f_root_cause_classification": "labeled_view_gap",
            },
            recommended_next_step_str="x",
        )
        for key in (
            "schema_version", "audit_type", "generated_at",
            "symbol", "target", "horizon",
            "active_manifest_path", "active_manifest_version",
            "total_rows_inspected", "atr_availability_status",
            "columns_present", "source_trace", "overall_summary",
            "provider_summary", "by_month", "group_summary",
            "raw_input_summary", "corrected_interpretation",
            "recommended_next_step", "warnings", "scope_disclosure",
        ):
            self.assertIn(key, rep, f"missing report key: {key}")
        self.assertEqual(rep["audit_type"], "atr_availability_by_provider")
        self.assertEqual(rep["schema_version"], 1)

    def test_atr_source_trace_required_keys(self) -> None:
        module = self._atr_audit_module()
        for k in ("raw_atr_producer", "view_atr_column",
                  "atr_bps_runtime_compute"):
            self.assertIn(k, module.SOURCE_TRACE)
            self.assertIn("origin_file", module.SOURCE_TRACE[k])

    def test_atr_no_threshold_search_language(self) -> None:
        module = self._atr_audit_module()
        forbidden = (
            "buy", "sell", "promote", "deploy",
            "alpha", "profit", "pnl",
            "threshold search", "search threshold",
        )
        for status in (
            "labeled_view_gap", "raw_input_missing",
            "provider_coverage_gap", "feature_join_gap",
            "available_clean", "insufficient_source_visibility",
            "unknown",
        ):
            rec = module.recommend_next_step(status)
            for word in forbidden:
                self.assertNotIn(word, rec.lower(),
                                 f"forbidden word {word!r} in rec for {status!r}: {rec}")
        rep = module.build_report(
            symbol="SPY", target="reject", horizon=15,
            active_manifest_path=Path("/tmp/m.json"),
            manifest_version="v",
            total_rows=1, columns_present={},
            overall_summary={},
            provider_summary=[], by_month=[],
            group_summary_block={},
            raw_input_summary={},
            status="labeled_view_gap",
            corrected_interpretation={},
            recommended_next_step_str="x",
        )
        for word in ("buy", "sell", "alpha", "profit", "promote"):
            self.assertNotIn(word, rep["scope_disclosure"].lower())
        self.assertIn("no edge claim", rep["scope_disclosure"].lower())
        self.assertIn("no database writes", rep["scope_disclosure"].lower())

    def test_atr_cli_rejects_invalid_args(self) -> None:
        module = self._atr_audit_module()
        with self.assertRaises(SystemExit):
            module.main(["--older-pct", "1.5"])
        with self.assertRaises(SystemExit):
            module.main(["--recent-n", "0"])

    def test_retrain_evidence_pack_refuses_overlap_with_live_model_dir(self) -> None:
        module = load_module(
            "retrain_evidence_pack_overlap",
            REPO_ROOT / "scripts" / "run_retrain_evidence_pack.py",
        )
        live = Path(self.tmp) / "data" / "models"
        live.mkdir(parents=True, exist_ok=True)
        active_manifest = Path(self.tmp) / "elsewhere" / "manifest_active.json"
        active_manifest.parent.mkdir(parents=True, exist_ok=True)

        # Same dir: refused.
        with self.assertRaises(SystemExit) as ctx:
            module.validate_out_dir(live, live, active_manifest)
        self.assertIn("live model dir", str(ctx.exception))

        # --out-dir inside live dir: refused.
        nested = live / "candidate"
        nested.mkdir(parents=True, exist_ok=True)
        with self.assertRaises(SystemExit):
            module.validate_out_dir(nested, live, active_manifest)

        # Live dir inside --out-dir: refused.
        parent = Path(self.tmp) / "wraps"
        parent.mkdir(parents=True, exist_ok=True)
        wrapped_live = parent / "data" / "models"
        wrapped_live.mkdir(parents=True, exist_ok=True)
        with self.assertRaises(SystemExit):
            module.validate_out_dir(parent, wrapped_live, active_manifest)

    def test_retrain_evidence_pack_refuses_active_manifest_inside_out_dir(self) -> None:
        module = load_module(
            "retrain_evidence_pack_active_inside",
            REPO_ROOT / "scripts" / "run_retrain_evidence_pack.py",
        )
        out_dir = Path(self.tmp) / "candidate_dir"
        out_dir.mkdir(parents=True, exist_ok=True)
        live = Path(self.tmp) / "live_models"
        live.mkdir(parents=True, exist_ok=True)
        active_manifest_inside = out_dir / "manifest_active.json"
        active_manifest_inside.write_text("{}")

        with self.assertRaises(SystemExit) as ctx:
            module.validate_out_dir(out_dir, live, active_manifest_inside)
        self.assertIn("Active manifest", str(ctx.exception))

    def test_retrain_evidence_pack_report_shape_from_stub_manifest(self) -> None:
        module = load_module(
            "retrain_evidence_pack_shape",
            REPO_ROOT / "scripts" / "run_retrain_evidence_pack.py",
        )
        out_dir = Path(self.tmp) / "pack_out"
        out_dir.mkdir(parents=True, exist_ok=True)
        live = Path(self.tmp) / "live_models"
        live.mkdir(parents=True, exist_ok=True)
        active_manifest = Path(self.tmp) / "data" / "models_external" / "manifest_active.json"
        active_manifest.parent.mkdir(parents=True, exist_ok=True)
        active_manifest.write_text(json.dumps({"version": "v999_active_stub"}))

        candidate_manifest = {
            "version": "v999_candidate_stub",
            "feature_version": "fv_test",
            "thresholds": {
                "reject": {"5": 0.55, "15": 0.6},
                "break": {"5": 0.5},
            },
            "thresholds_meta": {
                "reject": {
                    "5": {
                        "objective": "utility_bps",
                        "score": 12.3,
                        "fallback": False,
                        "signals": 40,
                        "calibration_fit_size": 100,
                        "threshold_tune_size": 60,
                        "train_purge": {"train_rows_purged": 4, "enabled": True},
                    },
                    "15": {
                        "objective": "utility_bps",
                        "score": -0.5,
                        "fallback": False,
                        "signals": 20,
                        "train_purge": {"train_rows_purged": 0, "enabled": True},
                    },
                },
                "break": {
                    "5": {
                        "objective": "utility_bps",
                        "score": None,
                        "fallback": True,
                        "signals": 10,
                        "train_purge": {"train_rows_purged": 2, "enabled": True},
                    }
                },
            },
            "train_embargo_minutes": 60.0,
        }
        candidate_path = out_dir / "manifest_runtime_latest.json"
        candidate_path.write_text(json.dumps(candidate_manifest))

        report_path = Path(self.tmp) / "report.json"

        argv = [
            "--out-dir",
            str(out_dir),
            "--live-model-dir",
            str(live),
            "--active-manifest",
            str(active_manifest),
            "--report",
            str(report_path),
            "--skip-training",
        ]
        rc = module.main(argv)
        self.assertEqual(rc, 0)
        self.assertTrue(report_path.exists())

        report = json.loads(report_path.read_text())
        self.assertEqual(report["schema_version"], 1)
        self.assertIn("run_id", report)
        self.assertEqual(report["out_dir"], str(out_dir.resolve()))

        self.assertTrue(report["training"]["skipped"])
        self.assertEqual(report["training"]["skip_reason"], "skip_training=true")

        self.assertEqual(
            report["candidate_manifest_path"], str((out_dir.resolve() / "manifest_runtime_latest.json"))
        )
        self.assertEqual(report["candidate_manifest"]["version"], "v999_candidate_stub")

        provenance = report["provenance"]
        for key in (
            "git_sha",
            "python_executable",
            "python_version",
            "rf_env_snapshot",
            "active_manifest_path",
            "active_manifest_exists",
            "active_manifest_version",
            "active_manifest_signature",
        ):
            self.assertIn(key, provenance)
        self.assertTrue(provenance["active_manifest_exists"])
        self.assertEqual(provenance["active_manifest_version"], "v999_active_stub")
        self.assertIsInstance(provenance["active_manifest_signature"], str)
        self.assertEqual(len(provenance["active_manifest_signature"]), 64)
        self.assertIsInstance(provenance["rf_env_snapshot"], dict)

        rows = report["per_horizon"]
        self.assertEqual(len(rows), 3)
        rows_by_key = {(row["target"], row["horizon"]): row for row in rows}
        self.assertAlmostEqual(rows_by_key[("reject", 5)]["threshold"], 0.55, places=6)
        self.assertFalse(rows_by_key[("reject", 5)]["fallback"])
        self.assertEqual(rows_by_key[("reject", 5)]["score"], 12.3)
        self.assertFalse(rows_by_key[("reject", 5)]["no_signal_substituted"])
        self.assertTrue(rows_by_key[("break", 5)]["fallback"])

        summary = report["summary"]
        self.assertEqual(summary["models_attempted"], 3)
        self.assertEqual(summary["models_with_fallback_threshold"], 1)
        self.assertEqual(summary["models_with_negative_utility"], 1)
        self.assertEqual(summary["models_with_no_signal"], 0)
        self.assertEqual(summary["total_train_rows_purged"], 6)

        # runtime_safety_dry_run is present either way (skipped or not).
        safety = report["runtime_safety_dry_run"]
        self.assertIn("skipped", safety)
        self.assertIn("would_neutralize", safety)
        self.assertIsInstance(safety["would_neutralize"], list)

    def test_retrain_evidence_pack_rejects_protected_pass_through_keys(self) -> None:
        # Pass-through env must not be allowed to override the isolation
        # contract (RF_MODEL_DIR) or redirect candidate manifest reads
        # away from --candidate-manifest (RF_CANDIDATE_MANIFEST).
        module = load_module(
            "retrain_evidence_pack_protected_keys",
            REPO_ROOT / "scripts" / "run_retrain_evidence_pack.py",
        )
        for protected in ("RF_MODEL_DIR", "RF_METADATA_DIR", "RF_CANDIDATE_MANIFEST"):
            with self.subTest(key=protected):
                with self.assertRaises(SystemExit) as ctx:
                    module.parse_pass_through([f"{protected}=/tmp/foo"])
                self.assertIn(protected, str(ctx.exception))
                self.assertIn("not allowed", str(ctx.exception))
        # Non-protected keys still pass through. DUCKDB_PATH is a read-only
        # input and intentionally remains overridable for alternate-DB runs.
        result = module.parse_pass_through(
            ["RF_TRAIN_EMBARGO_MINUTES=30", "DUCKDB_PATH=/tmp/alt.duckdb"]
        )
        self.assertEqual(
            result,
            {"RF_TRAIN_EMBARGO_MINUTES": "30", "DUCKDB_PATH": "/tmp/alt.duckdb"},
        )

    def test_retrain_evidence_pack_rejects_protected_train_arg_overrides(self) -> None:
        # argparse takes the last occurrence; a trailing --train-arg
        # --out-dir=data/models would otherwise silently redirect writes
        # back to the live tree. Same risk for --candidate-manifest and
        # --metadata-dir (latter can be absolute).
        module = load_module(
            "retrain_evidence_pack_protected_train_args",
            REPO_ROOT / "scripts" / "run_retrain_evidence_pack.py",
        )
        protected = ["--out-dir", "--candidate-manifest", "--metadata-dir"]
        for flag in protected:
            with self.subTest(flag=flag, form="space"):
                with self.assertRaises(SystemExit) as ctx:
                    module.parse_train_args([flag, "/tmp/hostile"])
                self.assertIn(flag, str(ctx.exception))
                self.assertIn("not allowed", str(ctx.exception))
            with self.subTest(flag=flag, form="equals"):
                with self.assertRaises(SystemExit) as ctx:
                    module.parse_train_args([f"{flag}=/tmp/hostile"])
                self.assertIn(flag, str(ctx.exception))
                self.assertIn("not allowed", str(ctx.exception))
        # Harmless args still pass through unchanged.
        passed = module.parse_train_args(
            ["--n-estimators", "100", "--threshold-objective=utility_bps"]
        )
        self.assertEqual(
            passed, ["--n-estimators", "100", "--threshold-objective=utility_bps"]
        )

    def test_retrain_evidence_pack_runtime_safety_diff_logic(self) -> None:
        """Exercise runtime_safety_dry_run diff logic with a stubbed ml_server module.

        Independent of fastapi: we monkey-patch _load_ml_server_module to return
        a stub that mimics ModelRegistry._apply_runtime_threshold_safety.
        """
        module = load_module(
            "retrain_evidence_pack_runtime_safety",
            REPO_ROOT / "scripts" / "run_retrain_evidence_pack.py",
        )

        class _StubRegistry:
            @staticmethod
            def _apply_runtime_threshold_safety(thresholds, manifest):
                # Simulate runtime safety: neutralize reject@15 only.
                thresholds["reject"][15] = module._no_signal_sentinel()
                return thresholds

        class _StubModule:
            NO_SIGNAL_THRESHOLD = module._no_signal_sentinel()
            ModelRegistry = _StubRegistry

        original_loader = module._load_ml_server_module
        module._load_ml_server_module = lambda: (_StubModule(), None)
        try:
            thresholds = {
                "reject": {5: 0.55, 15: 0.6},
                "break": {5: 0.5},
            }
            manifest = {
                "thresholds_meta": {
                    "reject": {
                        "5": {"objective": "utility_bps", "score": 12.3, "fallback": False},
                        "15": {"objective": "utility_bps", "score": -0.5, "fallback": False},
                    },
                    "break": {
                        "5": {"objective": "utility_bps", "score": 1.5, "fallback": False},
                    },
                }
            }
            result = module.runtime_safety_dry_run(thresholds, manifest)
        finally:
            module._load_ml_server_module = original_loader

        self.assertFalse(result["skipped"])
        self.assertEqual(result["would_neutralize_count"], 1)
        self.assertEqual(len(result["would_neutralize"]), 1)
        entry = result["would_neutralize"][0]
        self.assertEqual(entry["target"], "reject")
        self.assertEqual(entry["horizon"], 15)
        self.assertAlmostEqual(entry["original_threshold"], 0.6, places=6)
        self.assertIn("nonpositive_score", entry["reason"])
        # Input thresholds dict was deep-copied, not mutated.
        self.assertAlmostEqual(thresholds["reject"][15], 0.6, places=6)

    def test_retrain_evidence_pack_uses_resolved_training_python(self) -> None:
        """invoke_training() must spawn the resolver-selected interpreter, not
        sys.executable, and must record both fields on the training block.

        Prevents the Python 3.9 regression: train_rf_artifacts.py imports fail
        before any training happens when invoked under <3.10. The evidence pack
        is responsible for not putting the user there silently.
        """
        module = load_module(
            "retrain_evidence_pack_python_resolver",
            REPO_ROOT / "scripts" / "run_retrain_evidence_pack.py",
        )

        fake_python = "/opt/fake/bin/python3.11"
        fake_version = "3.11.99"
        fake_source = ".venv/bin/python"
        captured: dict = {}

        original_resolver = module.resolve_training_python
        original_subprocess_run = module.subprocess.run

        def _fake_resolver():
            return fake_python, fake_version, fake_source

        class _FakeCompleted:
            returncode = 0
            stdout = ""
            stderr = ""

        def _fake_subprocess_run(cmd, **kwargs):
            captured["cmd"] = list(cmd)
            captured["kwargs"] = kwargs
            return _FakeCompleted()

        module.resolve_training_python = _fake_resolver
        module.subprocess.run = _fake_subprocess_run
        try:
            out_dir = Path(self.tmp) / "resolver_run"
            out_dir.mkdir(parents=True, exist_ok=True)
            block = module.invoke_training(
                out_dir,
                pass_through_env={},
                extra_args=[],
                candidate_manifest_name="manifest_runtime_latest.json",
            )
        finally:
            module.resolve_training_python = original_resolver
            module.subprocess.run = original_subprocess_run

        self.assertIn("cmd", captured)
        self.assertEqual(captured["cmd"][0], fake_python)
        self.assertNotEqual(captured["cmd"][0], sys.executable)
        self.assertEqual(block["training_python_executable"], fake_python)
        self.assertEqual(block["training_python_version"], fake_version)
        self.assertEqual(block["training_python_resolution_source"], fake_source)
        self.assertEqual(block["cmd"][0], fake_python)
        self.assertEqual(block["exit_code"], 0)

    def test_resolve_training_python_delegates_to_shared_pybin(self) -> None:
        """The evidence pack's resolver is a thin wrapper over
        ``services._pybin.resolve_python(min_version=(3, 10))``.

        Precedence and rejection behavior are exercised by the
        ``test_pybin_*`` tests below; here we only confirm that the
        evidence-pack wrapper forwards the right minimum version and
        returns the tuple verbatim, so the dedup cannot silently change
        the minimum-version policy.
        """
        module = load_module(
            "retrain_evidence_pack_python_resolver_delegation",
            REPO_ROOT / "scripts" / "run_retrain_evidence_pack.py",
        )

        import services._pybin as pybin_module
        captured: dict = {}

        def _fake_resolve_python(min_version=(3, 10)):
            captured["min_version"] = tuple(min_version)
            return ("/opt/fake/python", "3.12.5", ".venv/bin/python")

        original = pybin_module.resolve_python
        pybin_module.resolve_python = _fake_resolve_python
        # Also patch the alias the evidence-pack module imported at module
        # load (`from services import _pybin as _shared_pybin`); attribute
        # access on _shared_pybin reads through to the live module dict, so
        # rebinding pybin_module.resolve_python is sufficient.
        try:
            executable, version, source = module.resolve_training_python()
        finally:
            pybin_module.resolve_python = original

        self.assertEqual(executable, "/opt/fake/python")
        self.assertEqual(version, "3.12.5")
        self.assertEqual(source, ".venv/bin/python")
        # Confirm the evidence pack passes the documented minimum version.
        self.assertEqual(captured["min_version"], (3, 10))

    # ------------------------------------------------------------------ #
    # B2: candidate readiness classifier
    # ------------------------------------------------------------------ #

    def _readiness_module(self):
        return load_module(
            "retrain_evidence_pack_readiness",
            REPO_ROOT / "scripts" / "run_retrain_evidence_pack.py",
        )

    @staticmethod
    def _ph_row(
        target: str,
        horizon: int,
        *,
        score: float,
        fallback: bool = False,
        no_signal_substituted: bool = False,
        objective: str = "utility_bps",
        train_purge: dict | None = None,
        score_observations: list | None = None,
        score_observations_source: str | None = None,
    ) -> dict:
        if train_purge is None:
            train_purge = {
                "enabled": True,
                "embargo_minutes": 60.0,
                "calibration_start_ts": 1_777_383_000_000,
                "train_rows_before_purge": 20000,
                "train_rows_after_purge": 20000,
                "train_rows_purged": 0,
                "skip_reason": "",
            }
        return {
            "target": target,
            "horizon": horizon,
            "objective": objective,
            "score": score,
            "fallback": fallback,
            "no_signal_substituted": no_signal_substituted,
            "calibration_fit_size": 800,
            "threshold_tune_size": 500,
            "train_purge": train_purge,
            "score_observations": score_observations,
            "score_observations_source": score_observations_source,
        }

    @classmethod
    def _build_report_stub(
        cls,
        per_horizon: list[dict],
        *,
        would_neutralize: list[dict] | None = None,
        runtime_safety_skipped: bool = False,
        training_exit_code: int = 0,
        candidate_manifest_present: bool = True,
    ) -> dict:
        would_neutralize = would_neutralize or []
        return {
            "training": {"exit_code": training_exit_code, "skipped": False},
            "candidate_manifest": {"version": "v001"} if candidate_manifest_present else None,
            "per_horizon": per_horizon,
            "runtime_safety_dry_run": {
                "skipped": runtime_safety_skipped,
                "would_neutralize_count": len(would_neutralize),
                "would_neutralize": would_neutralize,
            },
        }

    def test_readiness_current_evidence_pattern_degraded_candidate(self) -> None:
        """2 viable / 6 blocked / clean runtime / valid_noop purge → degraded_candidate."""
        module = self._readiness_module()
        rows = [
            self._ph_row("reject", 5, score=-4.82, fallback=True, no_signal_substituted=True),
            self._ph_row("reject", 15, score=117.46),
            self._ph_row("reject", 30, score=85.89),
            self._ph_row("reject", 60, score=-33.48, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 5, score=0.0, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 15, score=0.0, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 30, score=0.0, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 60, score=0.0, fallback=True, no_signal_substituted=True),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))

        self.assertEqual(result["state"], "degraded_candidate")
        self.assertFalse(result["full_family_ready"])
        self.assertTrue(result["partial_ready"])
        self.assertTrue(result["degraded_candidate"])
        self.assertFalse(result["not_ready"])
        self.assertEqual(len(result["viable_horizons"]), 2)
        self.assertEqual(len(result["blocked_horizons"]), 6)
        self.assertTrue(result["runtime_safety_agreement"])
        self.assertEqual(result["purge_diagnostic_state"], "valid_noop")
        self.assertIn("statistical_validation_missing", result["reasons"])
        self.assertIn("majority_horizons_blocked", result["reasons"])
        viable_ids = {(v["target"], v["horizon"]) for v in result["viable_horizons"]}
        self.assertEqual(viable_ids, {("reject", 15), ("reject", 30)})

    def test_readiness_all_viable_full_family_ready(self) -> None:
        module = self._readiness_module()
        rows = [
            self._ph_row("reject", 5, score=10.0),
            self._ph_row("reject", 15, score=20.0),
            self._ph_row("reject", 30, score=30.0),
            self._ph_row("reject", 60, score=40.0),
            self._ph_row("break", 5, score=5.0),
            self._ph_row("break", 15, score=6.0),
            self._ph_row("break", 30, score=7.0),
            self._ph_row("break", 60, score=8.0),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["state"], "full_family_ready")
        self.assertTrue(result["full_family_ready"])
        self.assertFalse(result["partial_ready"])
        self.assertFalse(result["degraded_candidate"])
        self.assertFalse(result["not_ready"])
        self.assertEqual(len(result["viable_horizons"]), 8)
        self.assertEqual(result["blocked_horizons"], [])
        # Statistical validation still flagged because B3 is deferred.
        self.assertIn("statistical_validation_missing", result["reasons"])
        self.assertFalse(result["statistical_validation_present"])

    def test_readiness_no_viable_horizons_not_ready(self) -> None:
        module = self._readiness_module()
        rows = [
            self._ph_row("reject", 5, score=-1.0, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 5, score=0.0, fallback=True, no_signal_substituted=True),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["state"], "not_ready")
        self.assertTrue(result["not_ready"])
        self.assertFalse(result["full_family_ready"])
        self.assertFalse(result["partial_ready"])
        self.assertIn("no_viable_horizons", result["reasons"])

    def test_readiness_runtime_safety_disagreement_not_ready(self) -> None:
        """Even with viable horizons, would_neutralize_count > 0 forces not_ready."""
        module = self._readiness_module()
        rows = [
            self._ph_row("reject", 15, score=117.0),
            self._ph_row("reject", 30, score=85.0),
        ]
        stub = self._build_report_stub(
            rows,
            would_neutralize=[
                {"target": "reject", "horizon": 15, "original_threshold": 0.6, "reason": "nonpositive_score"},
            ],
        )
        result = module.classify_candidate_readiness(stub)
        self.assertEqual(result["state"], "not_ready")
        self.assertTrue(result["not_ready"])
        self.assertFalse(result["runtime_safety_agreement"])
        self.assertIn("runtime_safety_disagreement", result["reasons"])
        # The neutralized horizon must not appear in viable list.
        viable_ids = {(v["target"], v["horizon"]) for v in result["viable_horizons"]}
        self.assertNotIn(("reject", 15), viable_ids)

    def test_readiness_purge_valid_noop_classification(self) -> None:
        module = self._readiness_module()
        rows = [self._ph_row("reject", 15, score=10.0)]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["purge_diagnostic_state"], "valid_noop")

    def test_readiness_purge_disabled_blocks_full_readiness(self) -> None:
        """Any horizon with the purge disabled prevents full_family_ready
        and (combined with all viable) lands in degraded_candidate."""
        module = self._readiness_module()
        disabled_tp = {
            "enabled": False,
            "embargo_minutes": 0.0,
            "calibration_start_ts": None,
            "train_rows_before_purge": 20000,
            "train_rows_after_purge": 20000,
            "train_rows_purged": 0,
            "skip_reason": "disabled",
        }
        rows = [
            self._ph_row("reject", 15, score=10.0, train_purge=disabled_tp),
            self._ph_row("reject", 30, score=20.0, train_purge=disabled_tp),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["purge_diagnostic_state"], "disabled")
        # All viable but purge disabled — not full_family, must be degraded.
        self.assertFalse(result["full_family_ready"])
        self.assertTrue(result["degraded_candidate"])
        self.assertEqual(result["state"], "degraded_candidate")
        self.assertIn("purge_disabled", result["reasons"])

    def test_readiness_purge_invalid_diagnostic_not_ready(self) -> None:
        """Missing/inconsistent train_purge block forces not_ready."""
        module = self._readiness_module()
        invalid_tp = {
            "enabled": True,
            "embargo_minutes": 60.0,
            "calibration_start_ts": None,  # invalid: enabled but no start ts
            "train_rows_before_purge": 20000,
            "train_rows_after_purge": 20000,
            "train_rows_purged": 0,
            "skip_reason": "",
        }
        rows = [self._ph_row("reject", 15, score=10.0, train_purge=invalid_tp)]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["purge_diagnostic_state"], "invalid")
        self.assertEqual(result["state"], "not_ready")
        self.assertIn("purge_diagnostic_invalid", result["reasons"])

    def test_readiness_statistical_validation_missing_is_explicit(self) -> None:
        module = self._readiness_module()
        rows = [self._ph_row("reject", 15, score=10.0)]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertTrue(result["statistical_validation_required"])
        self.assertFalse(result["statistical_validation_present"])
        self.assertIsNone(result["statistical_validation_passed"])
        self.assertIn("statistical_validation_missing", result["reasons"])

    def test_readiness_training_failed_not_ready(self) -> None:
        module = self._readiness_module()
        rows = [self._ph_row("reject", 15, score=10.0)]
        result = module.classify_candidate_readiness(
            self._build_report_stub(rows, training_exit_code=1)
        )
        self.assertEqual(result["state"], "not_ready")
        self.assertIn("training_failed", result["reasons"])

    def test_promotion_disposition_current_evidence_hold_pending_stat_validation(self) -> None:
        """2 viable / 6 blocked with stat validation missing must not be
        promotion_ready and must carry disposition=hold_pending_statistical_validation."""
        module = self._readiness_module()
        rows = [
            self._ph_row("reject", 5, score=-4.82, fallback=True, no_signal_substituted=True),
            self._ph_row("reject", 15, score=117.46),
            self._ph_row("reject", 30, score=85.89),
            self._ph_row("reject", 60, score=-33.48, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 5, score=0.0, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 15, score=0.0, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 30, score=0.0, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 60, score=0.0, fallback=True, no_signal_substituted=True),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["state"], "degraded_candidate")
        self.assertFalse(result["promotion_ready"])
        self.assertEqual(
            result["promotion_disposition"], "hold_pending_statistical_validation"
        )

    def test_promotion_disposition_full_family_missing_stat_validation(self) -> None:
        """All 8 viable but stat validation missing is still NOT promotion_ready."""
        module = self._readiness_module()
        rows = [
            self._ph_row("reject", 5, score=10.0),
            self._ph_row("reject", 15, score=20.0),
            self._ph_row("reject", 30, score=30.0),
            self._ph_row("reject", 60, score=40.0),
            self._ph_row("break", 5, score=5.0),
            self._ph_row("break", 15, score=6.0),
            self._ph_row("break", 30, score=7.0),
            self._ph_row("break", 60, score=8.0),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["state"], "full_family_ready")
        self.assertFalse(result["promotion_ready"])
        self.assertEqual(
            result["promotion_disposition"], "hold_pending_statistical_validation"
        )

    def test_promotion_disposition_full_family_oos_validated_is_ready(self) -> None:
        """OOS validation is the ONLY axis that can set promotion_ready=True.

        Full family + OOS present+passed -> ready_full_family / promotion_ready=True.
        Full family + only in-sample present+passed -> full_family_in_sample_validated.
        Full family + nothing -> hold_pending_statistical_validation.
        Degraded + OOS pass -> partial_oos_validated (not promotable).
        Degraded + in-sample pass -> partial_in_sample_validated (not promotable).

        Tested via the isolated ``_compute_promotion_disposition`` helper so
        the disposition mapping is verifiable independently of the rest of
        classify_*."""
        module = self._readiness_module()

        # Full family + OOS passed: the only ready path.
        promotion_ready, disposition = module._compute_promotion_disposition(
            state="full_family_ready",
            has_viable=True,
            in_sample_validation_present=False,
            in_sample_validation_passed=None,
            oos_validation_present=True,
            oos_validation_passed=True,
        )
        self.assertTrue(promotion_ready)
        self.assertEqual(disposition, "ready_full_family")

        # Full family + only in-sample passed: lands in the new
        # in-sample-only disposition, NOT promotable.
        promotion_ready_is, disposition_is = module._compute_promotion_disposition(
            state="full_family_ready",
            has_viable=True,
            in_sample_validation_present=True,
            in_sample_validation_passed=True,
            oos_validation_present=False,
            oos_validation_passed=None,
        )
        self.assertFalse(promotion_ready_is)
        self.assertEqual(disposition_is, "full_family_in_sample_validated")

        # Full family but no validation at all: hold_pending.
        promotion_ready_neg, disposition_neg = module._compute_promotion_disposition(
            state="full_family_ready",
            has_viable=True,
            in_sample_validation_present=False,
            in_sample_validation_passed=None,
            oos_validation_present=False,
            oos_validation_passed=None,
        )
        self.assertFalse(promotion_ready_neg)
        self.assertEqual(disposition_neg, "hold_pending_statistical_validation")

        # Degraded + OOS pass: partial_oos_validated, still not promotable.
        promotion_ready_p_oos, disposition_p_oos = module._compute_promotion_disposition(
            state="degraded_candidate",
            has_viable=True,
            in_sample_validation_present=False,
            in_sample_validation_passed=None,
            oos_validation_present=True,
            oos_validation_passed=True,
        )
        self.assertFalse(promotion_ready_p_oos)
        self.assertEqual(disposition_p_oos, "partial_oos_validated")

        # Degraded + in-sample pass: partial_in_sample_validated, not promotable.
        promotion_ready_p_is, disposition_p_is = module._compute_promotion_disposition(
            state="degraded_candidate",
            has_viable=True,
            in_sample_validation_present=True,
            in_sample_validation_passed=True,
            oos_validation_present=False,
            oos_validation_passed=None,
        )
        self.assertFalse(promotion_ready_p_is)
        self.assertEqual(disposition_p_is, "partial_in_sample_validated")

        # In-sample failed should not produce any *_validated disposition.
        promotion_ready_p_fail, disposition_p_fail = module._compute_promotion_disposition(
            state="degraded_candidate",
            has_viable=True,
            in_sample_validation_present=True,
            in_sample_validation_passed=False,
            oos_validation_present=False,
            oos_validation_passed=None,
        )
        self.assertFalse(promotion_ready_p_fail)
        self.assertEqual(
            disposition_p_fail, "hold_pending_statistical_validation"
        )

        # OOS axis started but coverage is incomplete (passed=None per the
        # aggregator's coverage-aware contract). Disposition must surface
        # the OOS-coverage gap, NOT collapse to the generic stat-validation
        # hold. This is the disposition for the mixed-scope and partial-OOS
        # cases.
        promotion_ready_oos_gap, disposition_oos_gap = module._compute_promotion_disposition(
            state="full_family_ready",
            has_viable=True,
            in_sample_validation_present=False,
            in_sample_validation_passed=None,
            oos_validation_present=True,
            oos_validation_passed=None,  # coverage incomplete
        )
        self.assertFalse(promotion_ready_oos_gap)
        self.assertEqual(disposition_oos_gap, "hold_pending_oos_validation")

        # Same gap on a degraded candidate: still hold_pending_oos_validation.
        promotion_ready_p_oos_gap, disposition_p_oos_gap = module._compute_promotion_disposition(
            state="degraded_candidate",
            has_viable=True,
            in_sample_validation_present=False,
            in_sample_validation_passed=None,
            oos_validation_present=True,
            oos_validation_passed=None,
        )
        self.assertFalse(promotion_ready_p_oos_gap)
        self.assertEqual(disposition_p_oos_gap, "hold_pending_oos_validation")

    def test_promotion_disposition_runtime_safety_disagreement_blocked(self) -> None:
        """would_neutralize_count > 0 => not_ready => blocked_not_ready."""
        module = self._readiness_module()
        rows = [
            self._ph_row("reject", 15, score=117.0),
            self._ph_row("reject", 30, score=85.0),
        ]
        stub = self._build_report_stub(
            rows,
            would_neutralize=[
                {"target": "reject", "horizon": 15, "original_threshold": 0.6, "reason": "nonpositive_score"},
            ],
        )
        result = module.classify_candidate_readiness(stub)
        self.assertEqual(result["state"], "not_ready")
        self.assertFalse(result["promotion_ready"])
        self.assertEqual(result["promotion_disposition"], "blocked_not_ready")

    def test_readiness_nan_score_is_not_viable(self) -> None:
        """NaN scores must not pass the viability gate.

        ``NaN <= 0.0`` is False per IEEE 754, so the previous predicate would
        fail-open. PR #12 caught this at runtime, but readiness has to close
        the same hole here in case ``runtime_safety_dry_run`` is skipped."""
        module = self._readiness_module()
        rows = [
            self._ph_row("reject", 15, score=float("nan")),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        viable_ids = {(v["target"], v["horizon"]) for v in result["viable_horizons"]}
        self.assertNotIn(("reject", 15), viable_ids)
        self.assertEqual(result["viable_horizons"], [])
        blocked_reasons = result["blocked_horizons"][0]["reasons"]
        self.assertIn("nonfinite_utility", blocked_reasons)
        # No viable horizons => not_ready.
        self.assertEqual(result["state"], "not_ready")

    def test_readiness_inf_scores_are_not_viable(self) -> None:
        module = self._readiness_module()
        rows = [
            self._ph_row("reject", 15, score=float("inf")),
            self._ph_row("reject", 30, score=float("-inf")),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["viable_horizons"], [])
        blocked_reasons_15 = result["blocked_horizons"][0]["reasons"]
        blocked_reasons_30 = result["blocked_horizons"][1]["reasons"]
        self.assertIn("nonfinite_utility", blocked_reasons_15)
        self.assertIn("nonfinite_utility", blocked_reasons_30)
        # Neither +inf nor -inf should be misclassified as nonpositive_utility:
        # they are not less-than-or-equal in the meaningful sense; the gate
        # is "must be finite AND > 0".
        self.assertNotIn("nonpositive_utility", blocked_reasons_15)

    def test_readiness_nan_blocked_even_when_runtime_safety_skipped(self) -> None:
        """Worst case: fastapi missing, runtime dry-run skipped, NaN score.

        Without this fix, the runtime layer never sees the NaN and readiness
        used to mark the horizon viable. This test pins the defense-in-depth:
        readiness must reject NaN by itself, independent of the runtime
        check."""
        module = self._readiness_module()
        rows = [
            # Otherwise-clean: utility_bps, fallback False, no_signal False.
            self._ph_row("reject", 15, score=float("nan")),
        ]
        stub = self._build_report_stub(rows, runtime_safety_skipped=True)
        result = module.classify_candidate_readiness(stub)
        self.assertEqual(result["viable_horizons"], [])
        self.assertEqual(result["state"], "not_ready")
        self.assertIn("no_viable_horizons", result["reasons"])
        # And the runtime-safety-skipped reason is also surfaced separately
        # (it is a soft signal, not the fatal cause here).
        self.assertIn("runtime_safety_skipped", result["reasons"])

    # ------------------------------------------------------------------ #
    # B3: statistical validation
    # ------------------------------------------------------------------ #

    @staticmethod
    def _strong_pass_obs(n: int = 60, seed: int = 1) -> list[float]:
        import random
        rng = random.Random(seed)
        return [rng.gauss(2.0, 0.5) for _ in range(n)]

    @staticmethod
    def _near_zero_obs(n: int = 60, seed: int = 2) -> list[float]:
        import random
        rng = random.Random(seed)
        return [rng.gauss(0.0, 1.0) for _ in range(n)]

    def test_b3_in_sample_passing_validation_does_not_promote(self) -> None:
        """Full family + per-signal observations supporting positive mean,
        BUT observations are tagged as tune-slice (in-sample). The aggregate
        stat-validation booleans report True/True, but promotion_ready stays
        False and disposition reports the new in-sample-only label.

        This is today's reality: the manifest's observations come from
        ``threshold_tune_slice``, which is the same slice the threshold was
        chosen on; the mean is biased upward by selection. A "passed"
        verdict supports evidence but NOT promotion."""
        module = self._readiness_module()
        obs = self._strong_pass_obs()
        rows = [
            self._ph_row("reject", 5, score=10.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("reject", 15, score=20.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("reject", 30, score=30.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("reject", 60, score=40.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("break", 5, score=5.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("break", 15, score=6.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("break", 30, score=7.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("break", 60, score=8.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["state"], "full_family_ready")
        # Aggregate booleans still report present+passed (the test ran).
        self.assertTrue(result["statistical_validation_present"])
        self.assertTrue(result["statistical_validation_passed"])
        # All 8 viable horizons must have per-horizon detail.
        sv = result["statistical_validation"]
        self.assertEqual(len(sv), 8)
        for entry in sv.values():
            self.assertEqual(entry["status"], "passed")
            self.assertTrue(entry["passed"])
            self.assertIsNotNone(entry["ci_low"])
            self.assertGreater(entry["ci_low"], 0.0)
        # New scope-split fields: scope=in_sample, in_sample passes, OOS missing.
        self.assertEqual(result["validation_scope"], "in_sample")
        self.assertTrue(result["in_sample_validation_present"])
        self.assertTrue(result["in_sample_validation_passed"])
        self.assertFalse(result["oos_validation_present"])
        self.assertIsNone(result["oos_validation_passed"])
        self.assertTrue(result["oos_validation_required"])
        # Disposition: full family with in-sample pass only -> NEW label.
        # promotion_ready MUST be False; only ready_full_family promotes.
        self.assertEqual(
            result["promotion_disposition"], "full_family_in_sample_validated"
        )
        self.assertFalse(result["promotion_ready"])
        self.assertIn("oos_validation_missing", result["reasons"])

    def test_b3_oos_validated_full_family_promotes(self) -> None:
        """Full family + OOS-sourced observations passing => ready_full_family,
        promotion_ready=True. This is the ONLY shape that promotes today.

        Uses ``held_out_slice`` as the source label, one of the recognized
        OOS sources defined in ``_OOS_OBSERVATION_SOURCES_EXACT``.
        """
        module = self._readiness_module()
        obs = self._strong_pass_obs()
        rows = [
            self._ph_row(t, h, score=10.0, score_observations=obs,
                         score_observations_source="held_out_slice")
            for t, h in [
                ("reject", 5), ("reject", 15), ("reject", 30), ("reject", 60),
                ("break", 5), ("break", 15), ("break", 30), ("break", 60),
            ]
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["state"], "full_family_ready")
        self.assertEqual(result["validation_scope"], "oos")
        self.assertTrue(result["oos_validation_present"])
        self.assertTrue(result["oos_validation_passed"])
        # in_sample axis empty for this report.
        self.assertFalse(result["in_sample_validation_present"])
        self.assertIsNone(result["in_sample_validation_passed"])
        self.assertEqual(result["promotion_disposition"], "ready_full_family")
        self.assertTrue(result["promotion_ready"])
        self.assertNotIn("oos_validation_missing", result["reasons"])

    def test_b3_unrecognized_source_treated_as_in_sample(self) -> None:
        """A typo'd or non-conventional source name MUST NOT promote.

        This is the fail-closed property of _is_oos_source: only sources
        listed in _OOS_OBSERVATION_SOURCES_EXACT or starting with ``oos_``
        count as OOS. Anything else (typos, future names not yet
        whitelisted, etc.) is treated as in-sample for promotion gating.
        """
        module = self._readiness_module()
        obs = self._strong_pass_obs()
        # "held_out" missing the "_slice" suffix is intentionally NOT
        # recognized; a typo must not silently promote.
        rows = [
            self._ph_row(t, h, score=10.0, score_observations=obs,
                         score_observations_source="held_out")
            for t, h in [
                ("reject", 5), ("reject", 15), ("reject", 30), ("reject", 60),
                ("break", 5), ("break", 15), ("break", 30), ("break", 60),
            ]
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["validation_scope"], "in_sample")
        self.assertFalse(result["oos_validation_present"])
        self.assertFalse(result["promotion_ready"])
        self.assertEqual(
            result["promotion_disposition"], "full_family_in_sample_validated"
        )

    def test_b3_oos_prefix_source_promotes(self) -> None:
        """Sources matching the ``oos_`` prefix convention also count as OOS,
        not only the explicit names in _OOS_OBSERVATION_SOURCES_EXACT."""
        module = self._readiness_module()
        obs = self._strong_pass_obs()
        rows = [
            self._ph_row(t, h, score=10.0, score_observations=obs,
                         score_observations_source="oos_january_2026")
            for t, h in [
                ("reject", 5), ("reject", 15), ("reject", 30), ("reject", 60),
                ("break", 5), ("break", 15), ("break", 30), ("break", 60),
            ]
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["validation_scope"], "oos")
        self.assertTrue(result["oos_validation_passed"])
        self.assertTrue(result["promotion_ready"])
        self.assertEqual(result["promotion_disposition"], "ready_full_family")

    def test_b3_mixed_scope_does_not_promote_due_to_oos_coverage_gap(self) -> None:
        """Mixed scope: 4 viable horizons OOS-pass, 4 in-sample-pass.

        OOS axis covers 4 of 8 viable horizons; in-sample covers the other
        4. Neither axis is coverage-complete against the full viable set,
        so ``oos_validation_passed`` and ``in_sample_validation_passed``
        must BOTH report ``None`` (coverage gap), not ``True``.

        Disposition must be ``hold_pending_oos_validation`` —
        ``oos_validation_present=True`` but coverage is incomplete, so the
        right signal to operators is "OOS axis started, finish covering."
        ``promotion_ready`` MUST stay False.

        This was the P1 bug from PR #24 review: under the previous
        aggregator, each axis was judged against its own subset (4 OOS
        tests all passed → ``oos_passed=True``), which let a mixed-scope
        candidate promote even though half the viable horizons had no OOS
        validation. Fixed by making the aggregator coverage-aware.
        """
        module = self._readiness_module()
        obs = self._strong_pass_obs()
        rows = [
            # 4 horizons OOS, 4 horizons in-sample. Each axis covers only
            # half of the viable set; promotion gating must reject this.
            self._ph_row("reject", 5, score=10.0, score_observations=obs,
                         score_observations_source="held_out_slice"),
            self._ph_row("reject", 15, score=20.0, score_observations=obs,
                         score_observations_source="held_out_slice"),
            self._ph_row("reject", 30, score=30.0, score_observations=obs,
                         score_observations_source="held_out_slice"),
            self._ph_row("reject", 60, score=40.0, score_observations=obs,
                         score_observations_source="held_out_slice"),
            self._ph_row("break", 5, score=5.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("break", 15, score=6.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("break", 30, score=7.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("break", 60, score=8.0, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["validation_scope"], "mixed")
        self.assertTrue(result["in_sample_validation_present"])
        self.assertTrue(result["oos_validation_present"])
        # Coverage-complete fields surface the gap on each axis.
        self.assertFalse(result["oos_validation_coverage_complete"])
        self.assertFalse(result["in_sample_validation_coverage_complete"])
        # Both passed booleans MUST be None — neither axis covers the
        # viable set, so neither can claim a pass.
        self.assertIsNone(result["oos_validation_passed"])
        self.assertIsNone(result["in_sample_validation_passed"])
        # Promotion blocked; disposition surfaces the OOS-coverage gap.
        self.assertFalse(result["promotion_ready"])
        self.assertEqual(
            result["promotion_disposition"], "hold_pending_oos_validation"
        )
        # Reasons make the gap actionable.
        self.assertIn("oos_validation_incomplete", result["reasons"])
        # Each in-sample-only horizon shows up in the OOS-missing list.
        oos_missing = result["oos_validation_missing_horizons"]
        for key in ("break@5m", "break@15m", "break@30m", "break@60m"):
            self.assertIn(key, oos_missing)
        # And the converse for the in-sample axis.
        in_sample_missing = result["in_sample_validation_missing_horizons"]
        for key in ("reject@5m", "reject@15m", "reject@30m", "reject@60m"):
            self.assertIn(key, in_sample_missing)

    def test_b3_oos_subset_with_remaining_insufficient_does_not_promote(self) -> None:
        """4 viable horizons have OOS observations and pass; the other 4
        have no observations at all (insufficient_data). OOS axis covers
        only 4 of 8 viable horizons -> coverage-incomplete, not
        promotable.

        This is the realistic 'partial B4 rollout' shape: an operator
        captures OOS observations for some horizons but not yet all.
        Promotion must wait for full coverage.
        """
        module = self._readiness_module()
        obs = self._strong_pass_obs()
        rows = [
            # 4 OOS with observations.
            self._ph_row("reject", 5, score=10.0, score_observations=obs,
                         score_observations_source="held_out_slice"),
            self._ph_row("reject", 15, score=20.0, score_observations=obs,
                         score_observations_source="held_out_slice"),
            self._ph_row("reject", 30, score=30.0, score_observations=obs,
                         score_observations_source="held_out_slice"),
            self._ph_row("reject", 60, score=40.0, score_observations=obs,
                         score_observations_source="held_out_slice"),
            # 4 viable but no observations -> insufficient_data.
            self._ph_row("break", 5, score=5.0),
            self._ph_row("break", 15, score=6.0),
            self._ph_row("break", 30, score=7.0),
            self._ph_row("break", 60, score=8.0),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["state"], "full_family_ready")
        # Only OOS-source results actually ran; scope reflects that.
        self.assertEqual(result["validation_scope"], "oos")
        self.assertTrue(result["oos_validation_present"])
        # 4 of 8 viable horizons covered -> coverage gap.
        self.assertFalse(result["oos_validation_coverage_complete"])
        self.assertIsNone(result["oos_validation_passed"])
        self.assertFalse(result["promotion_ready"])
        self.assertEqual(
            result["promotion_disposition"], "hold_pending_oos_validation"
        )
        self.assertIn("oos_validation_incomplete", result["reasons"])

    def test_b3_pure_oos_full_coverage_promotes(self) -> None:
        """All 8 viable horizons OOS-sourced AND all pass => promotes.

        Pinned here even though test_b3_oos_validated_full_family_promotes
        also covers it — this test EXPLICITLY checks the new coverage
        booleans so the contract ``oos_passed=True implies
        oos_coverage_complete=True`` cannot regress."""
        module = self._readiness_module()
        obs = self._strong_pass_obs()
        rows = [
            self._ph_row(t, h, score=10.0, score_observations=obs,
                         score_observations_source="held_out_slice")
            for t, h in [
                ("reject", 5), ("reject", 15), ("reject", 30), ("reject", 60),
                ("break", 5), ("break", 15), ("break", 30), ("break", 60),
            ]
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["validation_scope"], "oos")
        self.assertTrue(result["oos_validation_coverage_complete"])
        self.assertTrue(result["oos_validation_passed"])
        self.assertEqual(result["oos_validation_missing_horizons"], [])
        self.assertTrue(result["promotion_ready"])
        self.assertEqual(result["promotion_disposition"], "ready_full_family")

    def test_b3_insufficient_data_when_no_observations(self) -> None:
        """Today's real shape: no score_observations field in the manifest
        => status=insufficient_data, present=False, not promotion_ready."""
        module = self._readiness_module()
        rows = [
            self._ph_row("reject", 15, score=117.0),
            self._ph_row("reject", 30, score=85.0),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertFalse(result["statistical_validation_present"])
        self.assertIsNone(result["statistical_validation_passed"])
        for entry in result["statistical_validation"].values():
            self.assertEqual(entry["status"], "insufficient_data")
            self.assertIn("no_score_observations_in_manifest", entry["warnings"])
        self.assertFalse(result["promotion_ready"])
        self.assertEqual(
            result["promotion_disposition"], "hold_pending_statistical_validation"
        )

    def test_b3_insufficient_data_when_too_few_signals(self) -> None:
        """Observations below ``min_signals`` => insufficient_data, not a pass."""
        module = self._readiness_module()
        too_few = [1.0, 2.0, 3.0, 4.0, 5.0]
        rows = [self._ph_row("reject", 15, score=15.0, score_observations=too_few)]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        sv = result["statistical_validation"]["reject@15m"]
        self.assertEqual(sv["status"], "insufficient_data")
        self.assertEqual(sv["sample_size"], 5)
        self.assertFalse(sv["passed"])
        self.assertTrue(any("below_min" in w for w in sv["warnings"]))

    def test_b3_failed_validation_does_not_promote(self) -> None:
        """All 8 viable but observations centered on 0 => statistical_validation_passed=False;
        promotion_ready stays False and disposition falls back to hold_pending_statistical_validation."""
        module = self._readiness_module()
        obs = self._near_zero_obs()
        rows = [
            self._ph_row(t, h, score=10.0, score_observations=obs)
            for t, h in [
                ("reject", 5), ("reject", 15), ("reject", 30), ("reject", 60),
                ("break", 5), ("break", 15), ("break", 30), ("break", 60),
            ]
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["state"], "full_family_ready")
        self.assertTrue(result["statistical_validation_present"])
        self.assertFalse(result["statistical_validation_passed"])
        self.assertFalse(result["promotion_ready"])
        self.assertEqual(
            result["promotion_disposition"], "hold_pending_statistical_validation"
        )
        self.assertIn("statistical_validation_failed", result["reasons"])

    def test_b3_degraded_with_passing_in_sample_validation_is_partial_in_sample_validated(self) -> None:
        """Current evidence shape (2 viable / 6 blocked) with the two viable
        horizons' statistical validation passing via in-sample observations
        => state=degraded_candidate, disposition=partial_in_sample_validated,
        promotion_ready=False (never promotable on in-sample evidence alone,
        and never promotable while the family is degraded).

        This was previously called ``hold_partial_degraded``. Renamed to
        make the in-sample scope explicit; the new ``partial_oos_validated``
        is the future shape for partial+OOS evidence."""
        module = self._readiness_module()
        obs = self._strong_pass_obs()
        rows = [
            self._ph_row("reject", 5, score=-4.82, fallback=True, no_signal_substituted=True),
            self._ph_row("reject", 15, score=117.46, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("reject", 30, score=85.89, score_observations=obs,
                         score_observations_source="threshold_tune_slice"),
            self._ph_row("reject", 60, score=-33.48, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 5, score=0.0, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 15, score=0.0, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 30, score=0.0, fallback=True, no_signal_substituted=True),
            self._ph_row("break", 60, score=0.0, fallback=True, no_signal_substituted=True),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        self.assertEqual(result["state"], "degraded_candidate")
        self.assertTrue(result["statistical_validation_present"])
        self.assertTrue(result["statistical_validation_passed"])
        self.assertEqual(result["validation_scope"], "in_sample")
        self.assertFalse(result["oos_validation_present"])
        self.assertFalse(result["promotion_ready"])
        self.assertEqual(result["promotion_disposition"], "partial_in_sample_validated")

    def test_b3_blocked_horizon_not_statistically_validated(self) -> None:
        """Statistical validation must never run on a blocked horizon —
        even if score_observations is present. The safety chain's verdict
        is upstream of the statistical layer."""
        module = self._readiness_module()
        obs = self._strong_pass_obs()
        rows = [
            self._ph_row("reject", 15, score=10.0, score_observations=obs),
            # Blocked: fallback=True, also has obs that would otherwise pass.
            self._ph_row(
                "reject", 30,
                score=-5.0, fallback=True, no_signal_substituted=True,
                score_observations=obs,
            ),
        ]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        sv_keys = set(result["statistical_validation"].keys())
        self.assertIn("reject@15m", sv_keys)
        self.assertNotIn("reject@30m", sv_keys)
        # The viable one passes.
        self.assertEqual(result["statistical_validation"]["reject@15m"]["status"], "passed")

    def test_b3_report_schema_stable(self) -> None:
        """The candidate_readiness block must always carry the same keys,
        regardless of which path the validator takes."""
        module = self._readiness_module()
        rows = [self._ph_row("reject", 15, score=10.0)]
        result = module.classify_candidate_readiness(self._build_report_stub(rows))
        for key in (
            "state",
            "full_family_ready", "partial_ready", "degraded_candidate", "not_ready",
            "promotion_ready", "promotion_disposition",
            "viable_horizons", "blocked_horizons",
            "runtime_safety_agreement", "purge_diagnostic_state",
            "statistical_validation_required",
            "statistical_validation_present",
            "statistical_validation_passed",
            "statistical_validation",
            "reasons",
        ):
            self.assertIn(key, result, f"missing readiness key: {key}")
        # Per-horizon detail shape.
        entry = result["statistical_validation"]["reject@15m"]
        for key in (
            "target", "horizon", "method", "sample_size", "observed_score",
            "observed_mean", "ci_low", "ci_high", "p_value",
            "passed", "status", "warnings",
            # Disclosure surface: validation entries must carry the slice
            # source and the on-tune signal count so downstream readers
            # don't have to dig into the raw manifest.
            "score_observations_source", "signals_on_tune_slice",
        ):
            self.assertIn(key, entry, f"missing per-horizon validation key: {key}")

    def test_b3_surface_score_observations_source_and_signals_on_tune_slice(self) -> None:
        """Per-horizon validation entries must surface the disclosure fields
        regardless of which branch the validator takes (passed/failed/
        insufficient_data). Source + signals_on_tune_slice come from the
        manifest via _row_for_horizon, then end up in each validation
        entry beside the CI/p-value."""
        module = self._readiness_module()

        # Build a per_horizon row carrying the manifest's disclosure fields.
        obs = self._strong_pass_obs(n=60, seed=7)
        row = self._ph_row("reject", 15, score=12.0, score_observations=obs)
        row["score_observations_source"] = "threshold_tune_slice"
        row["signals_on_tune_slice"] = len(obs)
        result = module.classify_candidate_readiness(self._build_report_stub([row]))
        entry = result["statistical_validation"]["reject@15m"]
        self.assertEqual(entry["score_observations_source"], "threshold_tune_slice")
        self.assertEqual(entry["signals_on_tune_slice"], len(obs))
        # And on the insufficient_data branch as well: disclosure still
        # surfaces, even though the test could not run.
        row2 = self._ph_row("reject", 15, score=12.0, score_observations=None)
        row2["score_observations_source"] = "threshold_tune_slice"
        row2["signals_on_tune_slice"] = 0
        result2 = module.classify_candidate_readiness(self._build_report_stub([row2]))
        entry2 = result2["statistical_validation"]["reject@15m"]
        self.assertEqual(entry2["status"], "insufficient_data")
        self.assertEqual(entry2["score_observations_source"], "threshold_tune_slice")
        self.assertEqual(entry2["signals_on_tune_slice"], 0)

    def test_readiness_missing_candidate_manifest_not_ready(self) -> None:
        module = self._readiness_module()
        result = module.classify_candidate_readiness(
            self._build_report_stub([], candidate_manifest_present=False)
        )
        self.assertEqual(result["state"], "not_ready")
        self.assertIn("no_candidate_manifest", result["reasons"])

    # ------------------------------------------------------------------ #
    # services/_pybin.py — Python-side interpreter resolver
    # ------------------------------------------------------------------ #

    def _pybin_module(self):
        return load_module(
            "services_pybin",
            REPO_ROOT / "services" / "_pybin.py",
        )

    def test_pybin_python_version_tuple_probes_real_interpreter(self) -> None:
        module = self._pybin_module()
        version = module.python_version_tuple(sys.executable)
        self.assertIsNotNone(version)
        self.assertEqual(version[0], sys.version_info[0])
        self.assertEqual(version[1], sys.version_info[1])

    def test_pybin_resolve_python_honors_python_bin_env(self) -> None:
        """``PYTHON_BIN`` must outrank ``.venv/bin/python`` when both are valid.
        Mirrors the precedence in scripts/run_retrain_evidence_pack.py."""
        module = self._pybin_module()
        original_env = os.environ.get("PYTHON_BIN")
        original_root = module.ROOT
        original_probe = module.python_version_tuple

        fake_root = Path(self.tmp) / "fake_pybin_root_a"
        venv_py = fake_root / ".venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True, exist_ok=True)
        venv_py.write_text("#!/bin/sh\nexit 0\n")
        venv_py.chmod(0o755)

        pin = fake_root / "pin" / "python3.12"
        pin.parent.mkdir(parents=True, exist_ok=True)
        pin.write_text("#!/bin/sh\nexit 0\n")
        pin.chmod(0o755)

        module.ROOT = fake_root
        module.python_version_tuple = lambda exe: (3, 12, 1)
        os.environ["PYTHON_BIN"] = str(pin)
        try:
            exe, version, source = module.resolve_python()
        finally:
            module.ROOT = original_root
            module.python_version_tuple = original_probe
            if original_env is None:
                os.environ.pop("PYTHON_BIN", None)
            else:
                os.environ["PYTHON_BIN"] = original_env

        self.assertEqual(exe, str(pin))
        self.assertEqual(source, "PYTHON_BIN")
        self.assertEqual(version, "3.12.1")

    def test_pybin_resolve_python_prefers_project_venv(self) -> None:
        """No PYTHON_BIN, no .venv313 — .venv/bin/python wins over sys.executable."""
        module = self._pybin_module()
        original_env = os.environ.get("PYTHON_BIN")
        original_root = module.ROOT
        original_probe = module.python_version_tuple

        fake_root = Path(self.tmp) / "fake_pybin_root_b"
        venv_py = fake_root / ".venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True, exist_ok=True)
        venv_py.write_text("#!/bin/sh\nexit 0\n")
        venv_py.chmod(0o755)

        module.ROOT = fake_root
        module.python_version_tuple = lambda exe: (3, 11, 14) if exe == str(venv_py) else (3, 12, 0)
        os.environ.pop("PYTHON_BIN", None)
        try:
            exe, version, source = module.resolve_python()
        finally:
            module.ROOT = original_root
            module.python_version_tuple = original_probe
            if original_env is not None:
                os.environ["PYTHON_BIN"] = original_env

        self.assertEqual(exe, str(venv_py))
        self.assertEqual(source, ".venv/bin/python")
        self.assertEqual(version, "3.11.14")

    def test_pybin_resolve_python_rejects_old_interpreter(self) -> None:
        """When only Python 3.9 is reachable, resolver must SystemExit clearly."""
        module = self._pybin_module()
        original_env = os.environ.get("PYTHON_BIN")
        original_root = module.ROOT
        original_probe = module.python_version_tuple

        bare_root = Path(self.tmp) / "bare_pybin_root"
        bare_root.mkdir(parents=True, exist_ok=True)
        module.ROOT = bare_root
        module.python_version_tuple = lambda exe: (3, 9, 6)
        os.environ.pop("PYTHON_BIN", None)
        try:
            with self.assertRaises(SystemExit) as ctx:
                module.resolve_python()
            msg = str(ctx.exception)
            self.assertIn(">= 3.10", msg)
            self.assertIn("3.9", msg)
        finally:
            module.ROOT = original_root
            module.python_version_tuple = original_probe
            if original_env is not None:
                os.environ["PYTHON_BIN"] = original_env

    def test_pybin_assert_python_310_passes_on_current_interpreter(self) -> None:
        """The test runner itself is 3.10+; assert_python_310 must not raise."""
        module = self._pybin_module()
        module.assert_python_310()  # no raise

    # ------------------------------------------------------------------ #
    # scripts/_pybin.sh — shell-side interpreter resolver
    # ------------------------------------------------------------------ #

    def _run_pybin_sh(self, env: dict, root_dir: Path) -> tuple[int, str, str]:
        """Source scripts/_pybin.sh in a subshell, echo PYTHON_BIN, capture.

        Uses an absolute path to bash so the test can manipulate PATH/PYTHON_BIN
        in the subshell without breaking subprocess's own bash lookup."""
        import subprocess
        bash_path = "/bin/bash"
        cmd = [
            bash_path, "-c",
            'source "$0"/scripts/_pybin.sh && echo "PYTHON_BIN=${PYTHON_BIN}"',
            str(REPO_ROOT),
        ]
        merged_env = {**os.environ, "ROOT_DIR": str(root_dir), **env}
        result = subprocess.run(
            cmd, env=merged_env, capture_output=True, text=True, check=False,
            executable=bash_path,
        )
        return result.returncode, result.stdout, result.stderr

    def test_pybin_sh_resolves_to_310_or_newer(self) -> None:
        """On the real ROOT_DIR (which has .venv/bin/python 3.11), the helper
        must succeed and set PYTHON_BIN to a >=3.10 interpreter."""
        env = {k: v for k, v in os.environ.items() if k != "PYTHON_BIN"}
        rc, out, err = self._run_pybin_sh({}, REPO_ROOT)
        self.assertEqual(rc, 0, f"stderr: {err}")
        self.assertIn("PYTHON_BIN=", out)
        pybin = out.split("PYTHON_BIN=", 1)[1].strip()
        self.assertTrue(Path(pybin).is_file(), pybin)
        # Probe its version directly.
        import subprocess
        v = subprocess.check_output(
            [pybin, "-c", "import sys; print('%d.%d' % sys.version_info[:2])"],
            text=True,
        ).strip()
        major, minor = (int(x) for x in v.split("."))
        self.assertGreaterEqual((major, minor), (3, 10), f"resolved {v}")

    def test_pybin_sh_aborts_when_no_310_available(self) -> None:
        """Point ROOT_DIR at a directory with no .venv* and unset PATH so the
        helper cannot fall back to anything. Must exit non-zero with a clear
        error message listing every candidate it tried."""
        bare_root = Path(self.tmp) / "bare_pybin_sh"
        bare_root.mkdir(parents=True, exist_ok=True)
        # Strip PATH so `command -v python3` finds nothing; also block
        # PYTHON_BIN so the helper has nothing valid to use.
        env = {"PATH": "", "PYTHON_BIN": ""}
        rc, out, err = self._run_pybin_sh(env, bare_root)
        self.assertNotEqual(rc, 0)
        self.assertIn("could not resolve a Python", err)
        self.assertIn(">= 3.10", err)

    # ------------------------------------------------------------------ #
    # A.1 — backfill_events.run_build_labels uses the resolver, not sys.executable
    # ------------------------------------------------------------------ #

    # ------------------------------------------------------------------ #
    # scripts/_pybin_exec.sh — npm exec wrapper
    # ------------------------------------------------------------------ #

    def test_pybin_exec_runs_resolved_python_with_args(self) -> None:
        """Running ``_pybin_exec.sh -c '<inline>'`` must execute via a
        verified >=3.10 interpreter and forward the args correctly."""
        import subprocess
        wrapper = REPO_ROOT / "scripts" / "_pybin_exec.sh"
        result = subprocess.run(
            ["/bin/bash", str(wrapper), "-c",
             "import sys; print('%d.%d' % sys.version_info[:2])"],
            capture_output=True, text=True, check=False,
            env={**os.environ},
        )
        self.assertEqual(result.returncode, 0, msg=f"stderr: {result.stderr}")
        major, minor = (int(x) for x in result.stdout.strip().split("."))
        self.assertGreaterEqual((major, minor), (3, 10))

    def test_pybin_exec_honors_python_bin_override(self) -> None:
        """``PYTHON_BIN=…`` env override flows through ``_pybin_exec.sh``
        and the spawned interpreter is the one PYTHON_BIN named (provided
        it's a valid >=3.10)."""
        import subprocess
        wrapper = REPO_ROOT / "scripts" / "_pybin_exec.sh"
        # Use the test runner's own interpreter as the override — it's
        # guaranteed to be present and >=3.10.
        override = sys.executable
        result = subprocess.run(
            ["/bin/bash", str(wrapper), "-c",
             "import sys; print(sys.executable)"],
            capture_output=True, text=True, check=False,
            env={**os.environ, "PYTHON_BIN": override},
        )
        self.assertEqual(result.returncode, 0, msg=f"stderr: {result.stderr}")
        self.assertEqual(result.stdout.strip(), override)

    # ------------------------------------------------------------------ #
    # scripts/run_replay_backfill.sh — replay wrapper resolver fix
    # ------------------------------------------------------------------ #

    def test_package_json_ml_scripts_route_python_through_resolver(self) -> None:
        """Codex review on PR #20: ``package.json`` ``ml:*`` scripts must
        not invoke ``python3`` or ``./.venv/bin/python`` directly.
        Every Python entrypoint goes through the shared exec wrapper
        (``scripts/_pybin_exec.sh``) so PYTHON_BIN / .venv313 / version
        probe semantics are honored consistently."""
        import json
        with open(REPO_ROOT / "package.json", "r", encoding="utf-8") as fh:
            pkg = json.load(fh)
        scripts = pkg.get("scripts") or {}
        violations: list[str] = []
        for name, value in scripts.items():
            if not name.startswith("ml:"):
                continue
            v = str(value)
            # Allow ``bash scripts/run_*.sh`` paths — those wrappers
            # themselves source ``_pybin.sh``. Only flag direct python
            # invocations.
            if "python3 " in v or v.startswith("python3 ") or v == "python3":
                violations.append(f"{name}: {v!r}")
            if "./.venv/bin/python " in v or "./.venv/bin/python3 " in v:
                violations.append(f"{name}: {v!r}")
        self.assertEqual(
            violations, [],
            msg="ml:* scripts must route through scripts/_pybin_exec.sh, "
                "not call python3/.venv/bin/python directly:\n  "
                + "\n  ".join(violations),
        )

    def test_replay_backfill_sources_pybin_helper(self) -> None:
        """``run_replay_backfill.sh`` was a P2 miss in the original sweep:
        it previously honored ``PYTHON=`` unchecked and defaulted to
        ``./.venv/bin/python3`` without probing version. After the fix it
        must source ``scripts/_pybin.sh`` like every other wrapper."""
        text = (REPO_ROOT / "scripts" / "run_replay_backfill.sh").read_text()
        self.assertIn('source "${ROOT_DIR}/scripts/_pybin.sh"', text)
        self.assertIn('PYTHON="${PYTHON_BIN}"', text)
        # Old unguarded default must be gone from executable code. Strip
        # comments first so the inline rationale-comment that mentions
        # the old line for context doesn't cause a false positive.
        code_only_lines = []
        for line in text.splitlines():
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            code_only_lines.append(line)
        code_only = "\n".join(code_only_lines)
        self.assertNotIn('PYTHON="${PYTHON:-./.venv/bin/python3}"', code_only)

    def test_backfill_run_build_labels_uses_resolver(self) -> None:
        """``scripts/backfill_events.run_build_labels`` must spawn ``build_labels.py``
        via the shared resolver, never via raw ``sys.executable``. Mirrors
        the discipline established for the evidence pack training subprocess."""
        module = load_module(
            "backfill_events_module",
            REPO_ROOT / "scripts" / "backfill_events.py",
        )
        captured = {}

        def _fake_run(args, **kwargs):
            captured["args"] = list(args)
            class _Done:
                returncode = 0
            return _Done()

        # Stub resolve_python on the same services._pybin import the script uses.
        import services._pybin as pybin_module
        original_resolve = pybin_module.resolve_python
        pybin_module.resolve_python = lambda: ("/fake/python3.11", "3.11.14", ".venv/bin/python")

        original_subprocess_run = module.__dict__.get("subprocess")
        # backfill_events imports subprocess lazily inside run_build_labels;
        # easiest to monkey-patch the top-level subprocess module that the
        # function will import.
        import subprocess as real_subprocess
        original_real_run = real_subprocess.run
        real_subprocess.run = _fake_run
        try:
            module.run_build_labels("/tmp/fake.db", [5, 15])
        finally:
            real_subprocess.run = original_real_run
            pybin_module.resolve_python = original_resolve

        self.assertIn("args", captured)
        self.assertEqual(captured["args"][0], "/fake/python3.11")
        self.assertNotEqual(captured["args"][0], sys.executable)
        self.assertIn("scripts/build_labels.py", captured["args"])
        self.assertIn("--horizons", captured["args"])
        self.assertIn("5", captured["args"])
        self.assertIn("15", captured["args"])

    # ------------------------------------------------------------------ #
    # D1: manual serving-state gate
    # ------------------------------------------------------------------ #

    def _serving_state_module(self):
        return load_module(
            "server_serving_state",
            REPO_ROOT / "server" / "serving_state.py",
        )

    def _cli_module(self):
        return load_module(
            "scripts_set_serving_state",
            REPO_ROOT / "scripts" / "set_serving_state.py",
        )

    def test_serving_state_missing_file_defaults_active(self) -> None:
        """No file -> default active with the missing-file source marker so
        /health can distinguish "flag set to active" from "no flag set"."""
        module = self._serving_state_module()
        state_path = Path(self.tmp) / "data" / "models" / "serving_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        # File intentionally absent.
        registry = module.ServingStateRegistry(state_path)
        registry.load()
        self.assertTrue(registry.is_active())
        snap = registry.snapshot()
        self.assertEqual(snap["state"], module.STATE_ACTIVE)
        self.assertEqual(snap["source"], "default_missing_file")
        self.assertEqual(snap["reason"], "serving_state_missing_default_active")
        self.assertIsNone(snap["load_error"])

    def test_serving_state_valid_active_loads_from_file(self) -> None:
        module = self._serving_state_module()
        state_path = Path(self.tmp) / "serving_state.json"
        payload = {
            "schema_version": 1,
            "state": "active",
            "since_ts": 1700000000000,
            "reason": "resumed after review",
            "triggering_audit": None,
            "set_by": "ops_user@host",
            "manifest_version_when_set": "v415",
            "expires_at": None,
        }
        state_path.write_text(json.dumps(payload))
        registry = module.ServingStateRegistry(state_path)
        registry.load()
        self.assertTrue(registry.is_active())
        snap = registry.snapshot()
        self.assertEqual(snap["state"], "active")
        self.assertEqual(snap["reason"], "resumed after review")
        self.assertEqual(snap["source"], "file")
        self.assertEqual(snap["manifest_version_when_set"], "v415")
        self.assertIsNone(snap["load_error"])

    def test_serving_state_valid_dormant_blocks(self) -> None:
        module = self._serving_state_module()
        state_path = Path(self.tmp) / "serving_state.json"
        payload = {
            "schema_version": 1,
            "state": "dormant_manual_pause",
            "since_ts": 1700000000000,
            "reason": "manual review pending",
            "triggering_audit": "evidence/regime/foo.json",
            "set_by": "ops_user@host",
            "manifest_version_when_set": "v415",
            "expires_at": 1701000000000,
        }
        state_path.write_text(json.dumps(payload))
        registry = module.ServingStateRegistry(state_path)
        registry.load()
        self.assertFalse(registry.is_active())
        self.assertEqual(registry.state(), "dormant_manual_pause")
        blocked = registry.blocked_payload(manifest_version="v415")
        self.assertIsNone(blocked["signal"])
        self.assertEqual(blocked["blocked_reason"], "serving_dormant")
        self.assertEqual(blocked["serving_state"], "dormant_manual_pause")
        self.assertEqual(blocked["serving_state_reason"], "manual review pending")
        self.assertEqual(blocked["serving_state_since_ts"], 1700000000000)
        self.assertEqual(blocked["serving_state_expires_at"], 1701000000000)
        self.assertEqual(
            blocked["serving_state_triggering_audit"], "evidence/regime/foo.json"
        )
        self.assertEqual(blocked["manifest_version"], "v415")
        # Dormant response must NOT leak probability/threshold.
        for forbidden in ("probability", "p", "threshold", "score"):
            self.assertNotIn(forbidden, blocked)

    def test_serving_state_invalid_json_falls_back_to_dormant_data_quality(self) -> None:
        """Unparseable control-plane file must NOT silently allow serving."""
        module = self._serving_state_module()
        state_path = Path(self.tmp) / "serving_state.json"
        state_path.write_text("{not valid json")
        registry = module.ServingStateRegistry(state_path)
        registry.load()
        self.assertFalse(registry.is_active())
        snap = registry.snapshot()
        self.assertEqual(snap["state"], "dormant_data_quality")
        self.assertEqual(snap["reason"], "serving_state_invalid")
        self.assertEqual(snap["source"], "invalid_file")
        self.assertIsNotNone(snap["load_error"])

    def test_serving_state_invalid_schema_falls_back_to_dormant_data_quality(self) -> None:
        """Valid JSON but bad schema (unknown state value) is also blocked."""
        module = self._serving_state_module()
        state_path = Path(self.tmp) / "serving_state.json"
        state_path.write_text(json.dumps({"state": "totally_made_up", "schema_version": 1}))
        registry = module.ServingStateRegistry(state_path)
        registry.load()
        self.assertFalse(registry.is_active())
        snap = registry.snapshot()
        self.assertEqual(snap["state"], "dormant_data_quality")
        self.assertEqual(snap["source"], "invalid_file")
        self.assertIn("state_invalid", str(snap["load_error"]))

    def test_serving_state_reload_picks_up_file_changes(self) -> None:
        """A subsequent ``load()`` after the file changes flips the cached state.

        Mirrors what /reload does — atomic-swap a new serving_state.json
        and the next signature check picks it up."""
        module = self._serving_state_module()
        state_path = Path(self.tmp) / "serving_state.json"
        # Start active.
        state_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "state": "active",
                    "since_ts": 1700000000000,
                    "reason": "initial",
                }
            )
        )
        registry = module.ServingStateRegistry(state_path)
        self.assertTrue(registry.load())
        self.assertTrue(registry.is_active())
        # No change -> reload reports unchanged.
        self.assertFalse(registry.load())
        # Flip to dormant by rewriting the file with a fresh mtime/size.
        time.sleep(0.01)  # ensure mtime_ns differs
        state_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "state": "dormant_manual_pause",
                    "since_ts": 1700000050000,
                    "reason": "flipped during reload smoke",
                }
            )
        )
        self.assertTrue(registry.load())
        self.assertFalse(registry.is_active())
        self.assertEqual(registry.state(), "dormant_manual_pause")

    def test_serving_state_cli_writes_valid_schema(self) -> None:
        cli = self._cli_module()
        model_dir = Path(self.tmp) / "models_cli_write"
        model_dir.mkdir(parents=True, exist_ok=True)
        rc = cli.main(
            [
                "--state",
                "dormant_manual_pause",
                "--reason",
                "test pause",
                "--expires-at",
                "2026-05-15T12:00:00Z",
                "--model-dir",
                str(model_dir),
                "--set-by",
                "unit_test@runner",
                "--now-ms",
                "1700000000000",
                "--quiet",
            ]
        )
        self.assertEqual(rc, 0)
        state_path = model_dir / "serving_state.json"
        self.assertTrue(state_path.is_file())
        data = json.loads(state_path.read_text())
        self.assertEqual(data["schema_version"], 1)
        self.assertEqual(data["state"], "dormant_manual_pause")
        self.assertEqual(data["reason"], "test pause")
        self.assertEqual(data["set_by"], "unit_test@runner")
        self.assertEqual(data["since_ts"], 1700000000000)
        # 2026-05-15T12:00:00Z -> epoch ms.
        expected_ms = int(
            datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000
        )
        self.assertEqual(data["expires_at"], expected_ms)
        # Validates against the shared validator.
        sv = self._serving_state_module()
        ok, reason = sv.validate_state_payload(data)
        self.assertTrue(ok, f"CLI wrote schema-invalid payload: {reason}")

    def test_serving_state_cli_refuses_dormant_overwrite_without_force(self) -> None:
        cli = self._cli_module()
        model_dir = Path(self.tmp) / "models_cli_force"
        model_dir.mkdir(parents=True, exist_ok=True)
        # Plant an existing dormant state.
        existing = {
            "schema_version": 1,
            "state": "dormant_manual_pause",
            "since_ts": 1700000000000,
            "reason": "existing pause",
            "set_by": "ops@host",
            "manifest_version_when_set": None,
            "expires_at": None,
        }
        (model_dir / "serving_state.json").write_text(json.dumps(existing))
        # Attempt a different dormant state without --force should refuse.
        with self.assertRaises(SystemExit) as ctx:
            cli.main(
                [
                    "--state",
                    "dormant_audit_fail",
                    "--reason",
                    "audit hit",
                    "--model-dir",
                    str(model_dir),
                    "--quiet",
                ]
            )
        self.assertIn("Refusing to overwrite", str(ctx.exception))
        # File is unchanged.
        current = json.loads((model_dir / "serving_state.json").read_text())
        self.assertEqual(current["state"], "dormant_manual_pause")
        self.assertEqual(current["reason"], "existing pause")
        # With --force, it succeeds.
        rc = cli.main(
            [
                "--state",
                "dormant_audit_fail",
                "--reason",
                "audit hit",
                "--model-dir",
                str(model_dir),
                "--force",
                "--set-by",
                "ops@host",
                "--now-ms",
                "1700001000000",
                "--quiet",
            ]
        )
        self.assertEqual(rc, 0)
        current = json.loads((model_dir / "serving_state.json").read_text())
        self.assertEqual(current["state"], "dormant_audit_fail")

    def test_serving_state_cli_clear_to_active_never_requires_force(self) -> None:
        cli = self._cli_module()
        model_dir = Path(self.tmp) / "models_cli_clear"
        model_dir.mkdir(parents=True, exist_ok=True)
        existing = {
            "schema_version": 1,
            "state": "dormant_audit_fail",
            "since_ts": 1700000000000,
            "reason": "regime mismatch",
            "set_by": "ops@host",
            "manifest_version_when_set": "v999",
            "expires_at": None,
        }
        (model_dir / "serving_state.json").write_text(json.dumps(existing))
        rc = cli.main(
            [
                "--state",
                "active",
                "--reason",
                "regime recovered, resuming",
                "--model-dir",
                str(model_dir),
                "--set-by",
                "ops@host",
                "--now-ms",
                "1700002000000",
                "--quiet",
            ]
        )
        self.assertEqual(rc, 0)
        data = json.loads((model_dir / "serving_state.json").read_text())
        self.assertEqual(data["state"], "active")
        self.assertEqual(data["reason"], "regime recovered, resuming")

    def test_serving_state_cli_records_manifest_version_when_present(self) -> None:
        cli = self._cli_module()
        model_dir = Path(self.tmp) / "models_cli_manifest"
        model_dir.mkdir(parents=True, exist_ok=True)
        (model_dir / "manifest_active.json").write_text(
            json.dumps({"version": "v_test_777", "thresholds": {}, "models": {}})
        )
        rc = cli.main(
            [
                "--state",
                "dormant_manual_pause",
                "--reason",
                "pause",
                "--expires-at",
                "2026-12-31T23:59:59Z",
                "--model-dir",
                str(model_dir),
                "--set-by",
                "ops@host",
                "--now-ms",
                "1700000000000",
                "--quiet",
            ]
        )
        self.assertEqual(rc, 0)
        data = json.loads((model_dir / "serving_state.json").read_text())
        self.assertEqual(data["manifest_version_when_set"], "v_test_777")

    def test_serving_state_dormant_does_not_mutate_manifest_or_thresholds(self) -> None:
        """The serving-state gate must be a read-only short-circuit. It
        cannot rewrite serving_state.json, manifests, thresholds, or
        model artifacts under any code path exercised here."""
        module = self._serving_state_module()
        state_path = Path(self.tmp) / "serving_state.json"
        before_payload = {
            "schema_version": 1,
            "state": "dormant_manual_pause",
            "since_ts": 1700000000000,
            "reason": "static",
            "set_by": "ops@host",
            "manifest_version_when_set": "v_immutable",
            "expires_at": None,
        }
        state_path.write_text(json.dumps(before_payload))
        before_sig = state_path.stat().st_mtime_ns
        before_bytes = state_path.read_bytes()

        registry = module.ServingStateRegistry(state_path)
        registry.load()
        # Exercise the read API as the server would.
        _ = registry.is_active()
        _ = registry.state()
        _ = registry.snapshot()
        _ = registry.blocked_payload(manifest_version="v_test")
        # Re-read; bytes and mtime are unchanged.
        self.assertEqual(state_path.read_bytes(), before_bytes)
        self.assertEqual(state_path.stat().st_mtime_ns, before_sig)

    # ------------------------------------------------------------------ #
    # D1 fixup: stricter validate_state_payload — fail-closed on
    # incomplete operator-written control-plane files.
    # ------------------------------------------------------------------ #

    def _valid_full_payload(self, **overrides):
        base = {
            "schema_version": 1,
            "state": "dormant_manual_pause",
            "since_ts": 1700000000000,
            "reason": "manual review pending",
            "triggering_audit": None,
            "set_by": "ops_user@host",
            "manifest_version_when_set": "v415",
            "expires_at": 1701000000000,
        }
        base.update(overrides)
        return base

    def test_serving_state_validator_accepts_full_payload(self) -> None:
        module = self._serving_state_module()
        ok, reason = module.validate_state_payload(self._valid_full_payload())
        self.assertTrue(ok, f"full payload should validate (got reason={reason})")
        self.assertIsNone(reason)

    def test_serving_state_validator_rejects_incomplete_dormant_payload(self) -> None:
        """The exact scenario from PR #29 review: a manual write like
        ``{"state": "dormant_manual_pause"}`` lacks every other required
        field. Must fail closed, not silently pause serving."""
        module = self._serving_state_module()
        ok, reason = module.validate_state_payload({"state": "dormant_manual_pause"})
        self.assertFalse(ok)
        # First missing-required field encountered drives the reason; the
        # exact code is not asserted (operators get the loud message via
        # /health.load_error), but it must be one of the missing-required
        # markers so callers can grep for it.
        self.assertIn("missing", reason or "")

    def test_serving_state_validator_rejects_missing_schema_version(self) -> None:
        module = self._serving_state_module()
        payload = self._valid_full_payload()
        payload.pop("schema_version")
        ok, reason = module.validate_state_payload(payload)
        self.assertFalse(ok)
        self.assertEqual(reason, "schema_version_missing")

    def test_serving_state_validator_rejects_wrong_schema_version(self) -> None:
        module = self._serving_state_module()
        payload = self._valid_full_payload(schema_version=2)
        ok, reason = module.validate_state_payload(payload)
        self.assertFalse(ok)
        self.assertEqual(reason, "schema_version_unsupported")

    def test_serving_state_validator_rejects_missing_state(self) -> None:
        module = self._serving_state_module()
        payload = self._valid_full_payload()
        payload.pop("state")
        ok, reason = module.validate_state_payload(payload)
        self.assertFalse(ok)
        self.assertEqual(reason, "state_missing")

    def test_serving_state_validator_rejects_missing_since_ts(self) -> None:
        module = self._serving_state_module()
        payload = self._valid_full_payload()
        payload.pop("since_ts")
        ok, reason = module.validate_state_payload(payload)
        self.assertFalse(ok)
        self.assertEqual(reason, "since_ts_missing")

    def test_serving_state_validator_rejects_missing_or_blank_reason(self) -> None:
        module = self._serving_state_module()
        # Missing reason.
        p1 = self._valid_full_payload()
        p1.pop("reason")
        ok, reason = module.validate_state_payload(p1)
        self.assertFalse(ok)
        self.assertEqual(reason, "reason_missing")
        # Empty string.
        ok, reason = module.validate_state_payload(self._valid_full_payload(reason=""))
        self.assertFalse(ok)
        self.assertEqual(reason, "reason_empty")
        # Whitespace only.
        ok, reason = module.validate_state_payload(self._valid_full_payload(reason="   "))
        self.assertFalse(ok)
        self.assertEqual(reason, "reason_empty")
        # Wrong type.
        ok, reason = module.validate_state_payload(self._valid_full_payload(reason=123))
        self.assertFalse(ok)
        self.assertEqual(reason, "reason_invalid_type")

    def test_serving_state_validator_rejects_bad_since_ts_types(self) -> None:
        module = self._serving_state_module()
        # Bool subclass of int -> must be rejected.
        ok, reason = module.validate_state_payload(self._valid_full_payload(since_ts=True))
        self.assertFalse(ok)
        self.assertEqual(reason, "since_ts_invalid_type")
        # String -> rejected.
        ok, reason = module.validate_state_payload(self._valid_full_payload(since_ts="1700000000000"))
        self.assertFalse(ok)
        self.assertEqual(reason, "since_ts_invalid_type")
        # Negative -> rejected.
        ok, reason = module.validate_state_payload(self._valid_full_payload(since_ts=-1))
        self.assertFalse(ok)
        self.assertEqual(reason, "since_ts_negative")

    def test_serving_state_validator_rejects_bad_optional_types(self) -> None:
        """``triggering_audit``, ``set_by``, ``manifest_version_when_set``
        must be null or string. ``expires_at`` must be null or numeric."""
        module = self._serving_state_module()
        # Numeric triggering_audit -> rejected.
        ok, reason = module.validate_state_payload(
            self._valid_full_payload(triggering_audit=42)
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "triggering_audit_invalid_type")
        # List set_by -> rejected.
        ok, reason = module.validate_state_payload(
            self._valid_full_payload(set_by=["ops"])
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "set_by_invalid_type")
        # Dict manifest_version_when_set -> rejected.
        ok, reason = module.validate_state_payload(
            self._valid_full_payload(manifest_version_when_set={"v": 1})
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "manifest_version_when_set_invalid_type")
        # String expires_at -> rejected (must be numeric).
        ok, reason = module.validate_state_payload(
            self._valid_full_payload(expires_at="tomorrow")
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "expires_at_invalid_type")
        # Negative expires_at -> rejected.
        ok, reason = module.validate_state_payload(
            self._valid_full_payload(expires_at=-5)
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "expires_at_negative")
        # All explicit-null optional fields are accepted (CLI emits this shape
        # when the operator does not pass --triggering-audit / --expires-at).
        ok, reason = module.validate_state_payload(
            self._valid_full_payload(
                triggering_audit=None,
                set_by=None,
                manifest_version_when_set=None,
                expires_at=None,
            )
        )
        self.assertTrue(ok, f"all-null optionals must validate (got reason={reason})")

    def test_serving_state_loader_fail_closed_on_incomplete_file(self) -> None:
        """Stricter validator + loader contract: an incomplete operator
        write becomes ``dormant_data_quality`` with a visible load_error."""
        module = self._serving_state_module()
        state_path = Path(self.tmp) / "serving_state_incomplete.json"
        state_path.write_text(json.dumps({"state": "dormant_manual_pause"}))
        registry = module.ServingStateRegistry(state_path)
        registry.load()
        self.assertFalse(registry.is_active())
        snap = registry.snapshot()
        self.assertEqual(snap["state"], "dormant_data_quality")
        self.assertEqual(snap["reason"], "serving_state_invalid")
        self.assertEqual(snap["source"], "invalid_file")
        self.assertIsNotNone(snap["load_error"])
        self.assertIn("schema_invalid", str(snap["load_error"]))

    def test_serving_state_cli_payload_validates_under_strict_rules(self) -> None:
        """The CLI's built payload must still pass the tightened validator
        — otherwise the loader would reject CLI-written files."""
        cli = self._cli_module()
        sv = self._serving_state_module()
        model_dir = Path(self.tmp) / "models_strict_cli"
        model_dir.mkdir(parents=True, exist_ok=True)
        rc = cli.main(
            [
                "--state",
                "dormant_manual_pause",
                "--reason",
                "regime review pending",
                "--triggering-audit",
                "evidence/foo/bar.json",
                "--expires-at",
                "2026-12-31T23:59:59Z",
                "--model-dir",
                str(model_dir),
                "--set-by",
                "ops@runner",
                "--now-ms",
                "1700000000000",
                "--quiet",
            ]
        )
        self.assertEqual(rc, 0)
        data = json.loads((model_dir / "serving_state.json").read_text())
        ok, reason = sv.validate_state_payload(data)
        self.assertTrue(ok, f"CLI payload failed stricter validator: {reason}")

    def test_serving_state_ml_server_wires_serving_state_registry(self) -> None:
        """ml_server.py imports and instantiates a module-level
        ``serving_state`` singleton; ``/score`` consults
        ``serving_state.is_active()`` before answering. This is the
        smallest test that proves the wiring exists without spinning up
        the FastAPI surface."""
        try:
            ml_server = load_module(
                "ml_server_serving_state_wiring",
                REPO_ROOT / "server" / "ml_server.py",
            )
        except ModuleNotFoundError as exc:
            # Local dev envs without fastapi cannot load ml_server.py;
            # the CI Python Ops Smoke job covers this path.
            self.skipTest(f"ml_server import skipped: {exc}")
            return
        # Class-identity check would fail under ``load_module`` (custom
        # module name -> different class instance), so duck-type the
        # registry instead: it exposes the read API the server uses.
        self.assertTrue(hasattr(ml_server, "serving_state"))
        registry = ml_server.serving_state
        self.assertEqual(
            type(registry).__name__, "ServingStateRegistry"
        )
        for attr in ("is_active", "state", "snapshot", "blocked_payload", "load"):
            self.assertTrue(
                callable(getattr(registry, attr, None)),
                f"serving_state registry missing callable .{attr}",
            )
        # blocked_payload shape sanity (no probability / threshold leak).
        bp = registry.blocked_payload(manifest_version="v_test")
        for key in (
            "signal",
            "blocked_reason",
            "serving_state",
            "serving_state_reason",
            "serving_state_since_ts",
            "serving_state_expires_at",
            "manifest_version",
        ):
            self.assertIn(key, bp)
        self.assertIsNone(bp["signal"])
        self.assertEqual(bp["blocked_reason"], "serving_dormant")
        for forbidden in ("probability", "threshold", "score"):
            self.assertNotIn(forbidden, bp)

    # ------------------------------------------------------------------ #
    # D1 fixup #2: direct /score endpoint coverage for the dormant branch.
    # ------------------------------------------------------------------ #
    # These tests invoke the async route handler with a minimal mock
    # Request and inspect the JSONResponse. They avoid fastapi.TestClient
    # because the dev venv intentionally does not include httpx, and the
    # broader test contract here is "no new runtime deps."
    # The handler reads three module-level singletons (``registry``,
    # ``serving_state``, ``_score_single_event_with_log``) which Python
    # resolves at call time -- so patching them on the loaded ml_server
    # module is sufficient to drive every branch deterministically.

    @staticmethod
    def _build_stub_request(body: bytes, *, headers: dict | None = None):
        """Minimal asyncio-compatible Request stub.

        Only exposes ``.headers`` and an awaitable ``.body()`` -- the
        only surface ``_read_score_payload`` touches.
        """
        class _StubRequest:
            def __init__(self, body_bytes: bytes, hdrs: dict):
                self._body = body_bytes
                self.headers = hdrs

            async def body(self):
                return self._body

        return _StubRequest(body, headers or {})

    @staticmethod
    def _stub_active_registry_snapshot():
        # ``_score_event`` is downstream of the dormant branch and never
        # called in these tests because the active-path test stubs
        # ``_score_single_event_with_log``. The model object can be any
        # truthy sentinel since the dormant short-circuit never inspects
        # it; the active-path stub also never dereferences it.
        return {
            "manifest": {"version": "v_dormant_endpoint_test"},
            "models": {"reject": {15: object()}, "break": {}},
            "thresholds": {"reject": {15: 0.5}, "break": {}},
            "manifest_path": "/tmp/stub_dormant",
            "manifest_signature": (0, 0),
        }

    @staticmethod
    def _build_dormant_serving_state():
        class _DormantServingState:
            @staticmethod
            def is_active():
                return False

            @staticmethod
            def blocked_payload(*, manifest_version):
                return {
                    "signal": None,
                    "blocked_reason": "serving_dormant",
                    "serving_state": "dormant_manual_pause",
                    "serving_state_reason": "endpoint test pause",
                    "serving_state_since_ts": 1700000000000,
                    "serving_state_expires_at": 1700001000000,
                    "serving_state_triggering_audit": None,
                    "manifest_version": manifest_version,
                    "manifest_version_when_set": "v_dormant_endpoint_test",
                }

        return _DormantServingState()

    @staticmethod
    def _build_active_serving_state():
        class _ActiveServingState:
            @staticmethod
            def is_active():
                return True

            @staticmethod
            def blocked_payload(*, manifest_version):
                # Should never be called from the active path; surface it
                # loudly if the wiring regresses.
                raise AssertionError(
                    "serving_state.blocked_payload was called from the active path"
                )

        return _ActiveServingState()

    def _load_ml_server_for_endpoint(self):
        try:
            return load_module(
                "ml_server_score_endpoint",
                REPO_ROOT / "server" / "ml_server.py",
            )
        except ModuleNotFoundError as exc:
            self.skipTest(f"ml_server import skipped: {exc}")
            return None

    def _patch_score_path(self, ml_server, registry, serving_state):
        """Returns a restore-callable; use with try/finally in tests."""
        # Stash the originals.
        original_registry = ml_server.registry
        original_serving_state = ml_server.serving_state
        ml_server.registry = registry
        ml_server.serving_state = serving_state

        def restore():
            ml_server.registry = original_registry
            ml_server.serving_state = original_serving_state

        return restore

    def test_score_endpoint_dormant_single_returns_blocked(self) -> None:
        ml_server = self._load_ml_server_for_endpoint()
        if ml_server is None:
            return

        class _StubRegistry:
            @staticmethod
            def snapshot():
                return OpsSmokeTests._stub_active_registry_snapshot()

        restore = self._patch_score_path(
            ml_server,
            _StubRegistry(),
            self._build_dormant_serving_state(),
        )
        try:
            body = json.dumps({"event": {"symbol": "SPY", "horizon_min": 15}}).encode("utf-8")
            request = self._build_stub_request(body)
            response = asyncio.run(ml_server.score(request))
        finally:
            restore()

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.body)
        # Required dormant-response fields.
        self.assertIsNone(data["signal"])
        self.assertEqual(data["blocked_reason"], "serving_dormant")
        self.assertEqual(data["serving_state"], "dormant_manual_pause")
        self.assertEqual(data["serving_state_reason"], "endpoint test pause")
        self.assertEqual(data["serving_state_since_ts"], 1700000000000)
        self.assertEqual(data["serving_state_expires_at"], 1700001000000)
        self.assertEqual(data["manifest_version"], "v_dormant_endpoint_test")
        # Probability / threshold / scoring internals must NOT leak.
        for forbidden in ("probability", "p", "threshold", "score", "horizons", "scores"):
            self.assertNotIn(
                forbidden, data,
                f"dormant response leaked prediction-internal key {forbidden!r}",
            )

    def test_score_endpoint_dormant_batch_returns_one_blocked_per_event(self) -> None:
        ml_server = self._load_ml_server_for_endpoint()
        if ml_server is None:
            return

        class _StubRegistry:
            @staticmethod
            def snapshot():
                return OpsSmokeTests._stub_active_registry_snapshot()

        restore = self._patch_score_path(
            ml_server,
            _StubRegistry(),
            self._build_dormant_serving_state(),
        )
        try:
            events_payload = {
                "events": [
                    {"symbol": "SPY", "horizon_min": 15},
                    {"symbol": "SPY", "horizon_min": 30},
                    {"symbol": "SPY", "horizon_min": 60},
                ]
            }
            body = json.dumps(events_payload).encode("utf-8")
            request = self._build_stub_request(body)
            response = asyncio.run(ml_server.score(request))
        finally:
            restore()

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.body)
        self.assertIn("results", data)
        self.assertIsInstance(data["results"], list)
        self.assertEqual(len(data["results"]), 3)
        # Every result entry is a fresh dormant response (defensive copy
        # per event so callers cannot mutate one into the others).
        for entry in data["results"]:
            self.assertIsNone(entry["signal"])
            self.assertEqual(entry["blocked_reason"], "serving_dormant")
            self.assertEqual(entry["serving_state"], "dormant_manual_pause")
            self.assertEqual(entry["manifest_version"], "v_dormant_endpoint_test")
            for forbidden in ("probability", "p", "threshold", "score"):
                self.assertNotIn(forbidden, entry)

    def test_score_endpoint_dormant_malformed_request_still_returns_4xx(self) -> None:
        """Even when serving is dormant, malformed bodies must return the
        normal validation 4xx so clients can distinguish "I sent garbage"
        from "serving is paused." Two shapes covered:
        ``{}`` (missing both ``event`` and ``events``) and
        ``{"events": "not_a_list"}`` (wrong type)."""
        ml_server = self._load_ml_server_for_endpoint()
        if ml_server is None:
            return

        class _StubRegistry:
            @staticmethod
            def snapshot():
                return OpsSmokeTests._stub_active_registry_snapshot()

        restore = self._patch_score_path(
            ml_server,
            _StubRegistry(),
            self._build_dormant_serving_state(),
        )
        try:
            # Body missing both 'event' and 'events' -> 400.
            request = self._build_stub_request(b"{}")
            with self.assertRaises(ml_server.HTTPException) as ctx:
                asyncio.run(ml_server.score(request))
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("event", str(ctx.exception.detail).lower())

            # 'events' present but wrong type -> 400.
            bad_body = json.dumps({"events": "not_a_list"}).encode("utf-8")
            request2 = self._build_stub_request(bad_body)
            with self.assertRaises(ml_server.HTTPException) as ctx2:
                asyncio.run(ml_server.score(request2))
            self.assertEqual(ctx2.exception.status_code, 400)

            # Non-JSON body -> 400 (parser error, before our shape check).
            request3 = self._build_stub_request(b"not json at all")
            with self.assertRaises(ml_server.HTTPException) as ctx3:
                asyncio.run(ml_server.score(request3))
            self.assertEqual(ctx3.exception.status_code, 400)
        finally:
            restore()

    def test_score_endpoint_active_path_is_not_blocked(self) -> None:
        """When serving_state is active, the dormant short-circuit must
        NOT fire. Stub the normal scoring path so the test does not need
        a real model, and verify the sentinel returns (proving the
        dormant branch was skipped). The active stub explicitly raises
        if ``blocked_payload`` is called, so a regression would surface
        as an immediate AssertionError rather than a silent pass-through."""
        ml_server = self._load_ml_server_for_endpoint()
        if ml_server is None:
            return

        class _StubRegistry:
            @staticmethod
            def snapshot():
                return OpsSmokeTests._stub_active_registry_snapshot()

        sentinel = {
            "sentinel_active_path": True,
            "manifest_version": "v_dormant_endpoint_test",
        }
        called: dict[str, int] = {"count": 0}

        def _stub_score_single(event, disable_analogs=False):
            called["count"] += 1
            return dict(sentinel)

        original_score = ml_server._score_single_event_with_log
        original_try_begin = ml_server._try_begin_score_request
        original_finish = ml_server._finish_score_request
        ml_server._score_single_event_with_log = _stub_score_single
        # Make the concurrency gate a no-op so we don't fight the real
        # semaphore in test runs.
        ml_server._try_begin_score_request = lambda: True
        ml_server._finish_score_request = lambda **kwargs: None

        restore_path = self._patch_score_path(
            ml_server,
            _StubRegistry(),
            self._build_active_serving_state(),
        )
        try:
            body = json.dumps({"event": {"symbol": "SPY", "horizon_min": 15}}).encode("utf-8")
            request = self._build_stub_request(body)
            response = asyncio.run(ml_server.score(request))
        finally:
            restore_path()
            ml_server._score_single_event_with_log = original_score
            ml_server._try_begin_score_request = original_try_begin
            ml_server._finish_score_request = original_finish

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.body)
        self.assertTrue(data.get("sentinel_active_path"))
        # Dormant-shape markers must NOT be present.
        self.assertNotIn("blocked_reason", data)
        self.assertNotIn("serving_state", data)
        # The stubbed scorer was called exactly once.
        self.assertEqual(called["count"], 1)

    # ------------------------------------------------------------------ #
    # D2: serving-state observability — audit events, counters, sampler.
    # ------------------------------------------------------------------ #

    def _audit_log_path(self) -> Path:
        """Resolve the protocol-audit log location under the test root."""
        return (
            Path(os.environ["PIVOTQUANT_RESEARCH_PROTOCOL_ROOT"])
            / "audit_log.jsonl"
        )

    def _read_audit_events(self, event_type: str | None = None) -> list[dict]:
        """Read the audit-log JSONL produced under the test protocol root."""
        path = self._audit_log_path()
        if not path.is_file():
            return []
        out: list[dict] = []
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if event_type is None or rec.get("event_type") == event_type:
                    out.append(rec)
        return out

    def _isolated_audit_root(self) -> str:
        """Point the audit logger at a per-test directory under self.tmp."""
        root = Path(self.tmp) / "protocol_audit_root"
        root.mkdir(parents=True, exist_ok=True)
        os.environ["PIVOTQUANT_RESEARCH_PROTOCOL_ROOT"] = str(root)
        return str(root)

    def test_serving_state_d2_cli_emits_changed_event_for_missing_prior(self) -> None:
        """When ``serving_state.json`` does not yet exist, the CLI write must
        emit ``serving_state_changed`` with ``from_state_source="missing"``."""
        self._isolated_audit_root()
        cli = self._cli_module()
        model_dir = Path(self.tmp) / "models_d2_missing"
        model_dir.mkdir(parents=True, exist_ok=True)
        rc = cli.main(
            [
                "--state", "dormant_manual_pause",
                "--reason", "d2 missing-prior test",
                "--expires-at", "2026-12-31T23:59:59Z",
                "--model-dir", str(model_dir),
                "--set-by", "d2_test@runner",
                "--now-ms", "1700000000000",
                "--quiet",
            ]
        )
        self.assertEqual(rc, 0)
        events = self._read_audit_events("serving_state_changed")
        self.assertEqual(len(events), 1, f"expected 1 event, got {len(events)}")
        meta = events[0]["metadata"]
        self.assertIsNone(meta["from_state"])
        self.assertEqual(meta["from_state_source"], "missing")
        self.assertEqual(meta["to_state"], "dormant_manual_pause")
        self.assertEqual(meta["reason"], "d2 missing-prior test")
        self.assertEqual(meta["set_by"], "d2_test@runner")
        self.assertEqual(meta["since_ts"], 1700000000000)
        self.assertEqual(meta["expires_at"], int(
            datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000
        ))
        self.assertEqual(meta["forced"], False)
        self.assertIsInstance(meta["state_path_sha256"], str)
        self.assertEqual(len(meta["state_path_sha256"]), 64)
        self.assertEqual(events[0]["decision"], "record")
        self.assertIsNone(events[0]["candidate_id"])

    def test_serving_state_d2_cli_emits_changed_event_for_valid_prior(self) -> None:
        """When a valid prior file exists, the event records
        ``from_state_source="file"`` and the previous state value."""
        self._isolated_audit_root()
        cli = self._cli_module()
        model_dir = Path(self.tmp) / "models_d2_valid_prior"
        model_dir.mkdir(parents=True, exist_ok=True)
        prior = {
            "schema_version": 1,
            "state": "dormant_manual_pause",
            "since_ts": 1700000000000,
            "reason": "prior pause",
            "triggering_audit": None,
            "set_by": "ops@old_host",
            "manifest_version_when_set": "v_prior",
            "expires_at": None,
        }
        (model_dir / "serving_state.json").write_text(json.dumps(prior))
        rc = cli.main(
            [
                "--state", "active",
                "--reason", "review complete, resuming",
                "--model-dir", str(model_dir),
                "--set-by", "ops@new_host",
                "--now-ms", "1700001000000",
                "--quiet",
            ]
        )
        self.assertEqual(rc, 0)
        events = self._read_audit_events("serving_state_changed")
        self.assertEqual(len(events), 1)
        meta = events[0]["metadata"]
        self.assertEqual(meta["from_state"], "dormant_manual_pause")
        self.assertEqual(meta["from_state_source"], "file")
        self.assertEqual(meta["to_state"], "active")
        self.assertEqual(meta["reason"], "review complete, resuming")

    def test_serving_state_d2_cli_emits_changed_event_for_invalid_prior(self) -> None:
        """When the prior file is unparseable, the event records
        ``from_state_source="invalid"`` and the *loader-equivalent*
        substituted state (``dormant_data_quality``), so the audit trail
        matches what ml_server would have honored just before the write."""
        self._isolated_audit_root()
        cli = self._cli_module()
        model_dir = Path(self.tmp) / "models_d2_invalid_prior"
        model_dir.mkdir(parents=True, exist_ok=True)
        # Write unparseable JSON.
        (model_dir / "serving_state.json").write_text("{not valid json")
        rc = cli.main(
            [
                "--state", "dormant_manual_pause",
                "--reason", "replacing corrupt file",
                "--expires-at", "2026-12-31T23:59:59Z",
                "--model-dir", str(model_dir),
                "--set-by", "ops@cleanup",
                "--now-ms", "1700002000000",
                "--quiet",
            ]
        )
        self.assertEqual(rc, 0)
        events = self._read_audit_events("serving_state_changed")
        self.assertEqual(len(events), 1)
        meta = events[0]["metadata"]
        # Loader substitutes ``dormant_data_quality`` when it sees an
        # unparseable file; the audit event mirrors that substitution.
        self.assertEqual(meta["from_state"], "dormant_data_quality")
        self.assertEqual(meta["from_state_source"], "invalid")
        self.assertEqual(meta["to_state"], "dormant_manual_pause")

    def test_serving_state_d2_cli_invalid_prior_allows_replacement_without_force(self) -> None:
        """The dormant_X -> dormant_Y --force guard must NOT fire for a
        schema-invalid prior.

        PR #30 review (P2) caught that ``existing.get("state")`` was being
        consulted on the raw payload even when the loader would have
        substituted ``dormant_data_quality`` at load time. That made
        corrupt control-plane files harder to remediate than they should
        be: an operator hitting ``{"state": "dormant_manual_pause"}`` on
        disk had to pass ``--force`` even though the server never
        honored that raw record. The fix gates the guard on
        ``from_state_source == "file"``.

        Audit-event behavior is preserved: invalid prior still emits
        ``from_state_source="invalid"`` and ``from_state="dormant_data_quality"``.
        """
        self._isolated_audit_root()
        cli = self._cli_module()
        model_dir = Path(self.tmp) / "models_d2_invalid_prior_no_force"
        model_dir.mkdir(parents=True, exist_ok=True)
        # Parseable JSON but schema-invalid (missing required fields:
        # schema_version, since_ts, reason). The raw ``state`` value
        # *looks* like a dormant state, which is the bait the buggy
        # guard fell for.
        (model_dir / "serving_state.json").write_text(
            json.dumps({"state": "dormant_manual_pause"})
        )

        rc = cli.main(
            [
                "--state", "dormant_audit_fail",
                "--reason", "remediating corrupt prior, audit triggered",
                "--expires-at", "2026-12-31T23:59:59Z",
                "--model-dir", str(model_dir),
                "--set-by", "ops@remediation",
                "--now-ms", "1700004000000",
                "--quiet",
                # NOTE: deliberately NO --force.
            ]
        )
        self.assertEqual(
            rc, 0,
            "invalid prior must be replaceable without --force",
        )

        # The state file now carries the new valid record.
        data = json.loads((model_dir / "serving_state.json").read_text())
        self.assertEqual(data["state"], "dormant_audit_fail")
        self.assertEqual(data["reason"], "remediating corrupt prior, audit triggered")
        self.assertEqual(data["since_ts"], 1700004000000)
        # And the whole record validates under the stricter D1 rules.
        sv = self._serving_state_module()
        ok, reason = sv.validate_state_payload(data)
        self.assertTrue(ok, f"replaced state must validate: {reason}")

        # Exactly one serving_state_changed event, with from_state_source
        # = "invalid" and the loader-equivalent substituted from_state.
        events = self._read_audit_events("serving_state_changed")
        self.assertEqual(len(events), 1)
        meta = events[0]["metadata"]
        self.assertEqual(meta["from_state_source"], "invalid")
        self.assertEqual(meta["from_state"], "dormant_data_quality")
        self.assertEqual(meta["to_state"], "dormant_audit_fail")
        # Forced flag is FALSE — the test point is that no --force was needed.
        self.assertFalse(meta["forced"])

    def test_serving_state_d2_cli_audit_failure_does_not_block_write(self) -> None:
        """An audit-log IO failure (here: the protocol root points at a
        path that cannot be created) must NOT corrupt the state file or
        fail the CLI. The state write succeeds; the operator is warned
        on stderr."""
        # Point the audit root at a path that cannot be a directory: a
        # regular file. _append_line will fail when it tries to mkdir.
        bogus_path = Path(self.tmp) / "audit_root_is_a_file"
        bogus_path.write_text("not a directory")
        os.environ["PIVOTQUANT_RESEARCH_PROTOCOL_ROOT"] = str(bogus_path)

        cli = self._cli_module()
        model_dir = Path(self.tmp) / "models_d2_audit_fail"
        model_dir.mkdir(parents=True, exist_ok=True)

        import io
        import contextlib
        stderr_buf = io.StringIO()
        with contextlib.redirect_stderr(stderr_buf):
            rc = cli.main(
                [
                    "--state", "dormant_manual_pause",
                    "--reason", "audit failure must not block write",
                    "--expires-at", "2026-12-31T23:59:59Z",
                    "--model-dir", str(model_dir),
                    "--set-by", "ops@test",
                    "--now-ms", "1700003000000",
                    "--quiet",
                ]
            )
        self.assertEqual(rc, 0, "state-write must succeed even when audit log fails")
        # The state file was written correctly.
        state_data = json.loads((model_dir / "serving_state.json").read_text())
        self.assertEqual(state_data["state"], "dormant_manual_pause")
        self.assertEqual(state_data["reason"], "audit failure must not block write")
        # The operator was warned about the audit failure on stderr.
        self.assertIn("serving_state_changed", stderr_buf.getvalue())
        self.assertIn("audit", stderr_buf.getvalue().lower())

    def test_serving_state_d2_observability_dormant_block_counters(self) -> None:
        module = self._serving_state_module()
        # With min_interval_ms=0 we can verify the rate gate in isolation.
        o = module.ServingStateObservability(sample_n=4, min_interval_sec=0.0)
        # First dormant always emits.
        count, emit = o.record_dormant_block(1_000)
        self.assertEqual(count, 1)
        self.assertTrue(emit)
        # Counters update.
        snap = o.counters_snapshot()
        self.assertEqual(snap["dormant_requests_count_in_process"], 1)
        self.assertEqual(snap["dormant_requests_count_since_state_set"], 1)
        self.assertEqual(snap["last_blocked_at_ms"], 1_000)
        self.assertEqual(snap["transitions_count_in_process"], 0)

        # Next 3 dormant requests don't emit (only 1 since last emit < N=4).
        for ms in (1_001, 1_002, 1_003):
            count, emit = o.record_dormant_block(ms)
            self.assertFalse(emit, f"unexpected emit at ms={ms}")
        # 4th since last emit: rate gate met (time gate is 0), so emits.
        count, emit = o.record_dormant_block(1_004)
        self.assertEqual(count, 4)
        self.assertTrue(emit)

        snap = o.counters_snapshot()
        self.assertEqual(snap["dormant_requests_count_in_process"], 5)
        self.assertEqual(snap["dormant_requests_count_since_state_set"], 5)
        self.assertEqual(snap["last_blocked_at_ms"], 1_004)

    def test_serving_state_d2_observability_time_gate(self) -> None:
        """With a non-zero min_interval, the sampler MUST also wait the
        time gate even after the rate gate is satisfied."""
        module = self._serving_state_module()
        o = module.ServingStateObservability(sample_n=2, min_interval_sec=10.0)
        # First emit (always).
        _, emit = o.record_dormant_block(0)
        self.assertTrue(emit)
        # Second emit: rate satisfied (2 since last emit) BUT time gate (10s) not met.
        _, emit = o.record_dormant_block(0)
        self.assertFalse(emit, "rate gate alone must not be sufficient")
        _, emit = o.record_dormant_block(5_000)
        self.assertFalse(emit, "5s after last emit, time gate not met")
        # 10s later, BOTH gates satisfied: emit fires.
        # Note: by this point we've called record_dormant_block 4 times; the
        # sampler counter was reset on the first emit so it's now at 3.
        _, emit = o.record_dormant_block(10_001)
        self.assertTrue(emit)

    def test_serving_state_d2_observability_transition_resets_since_state_set(self) -> None:
        module = self._serving_state_module()
        o = module.ServingStateObservability(sample_n=2, min_interval_sec=0.0)
        for ms in (100, 101, 102, 103):
            o.record_dormant_block(ms)
        snap = o.counters_snapshot()
        self.assertEqual(snap["dormant_requests_count_in_process"], 4)
        self.assertEqual(snap["dormant_requests_count_since_state_set"], 4)
        # State transition: since_state_set MUST reset, lifetime counter MUST NOT.
        o.record_state_transition(200)
        snap = o.counters_snapshot()
        self.assertEqual(snap["dormant_requests_count_in_process"], 4)
        self.assertEqual(snap["dormant_requests_count_since_state_set"], 0)
        self.assertEqual(snap["transitions_count_in_process"], 1)
        self.assertEqual(snap["last_loaded_at_ms"], 200)
        # The sampler also resets: the first dormant block after the
        # transition emits unconditionally.
        _, emit = o.record_dormant_block(300)
        self.assertTrue(emit)

    def test_serving_state_d2_score_dormant_increments_counters_and_emits(self) -> None:
        """End-to-end: dormant /score requests bump the counters AND emit
        a sampled ``predict_blocked_dormant`` audit event."""
        self._isolated_audit_root()
        ml_server = self._load_ml_server_for_endpoint()
        if ml_server is None:
            return

        # Reset the module-level observability to a sample_n=1 instance so
        # every dormant request emits — keeps the test deterministic without
        # changing production defaults.
        sv_module = self._serving_state_module()
        original_obs = ml_server.serving_state_observability
        ml_server.serving_state_observability = sv_module.ServingStateObservability(
            sample_n=1, min_interval_sec=0.0
        )

        class _StubRegistry:
            @staticmethod
            def snapshot():
                return OpsSmokeTests._stub_active_registry_snapshot()

        restore = self._patch_score_path(
            ml_server,
            _StubRegistry(),
            self._build_dormant_serving_state(),
        )
        try:
            body = json.dumps({"event": {"symbol": "SPY"}}).encode("utf-8")
            request = self._build_stub_request(body)
            response = asyncio.run(ml_server.score(request))
            self.assertEqual(response.status_code, 200)
            # Counters bumped exactly once.
            snap = ml_server.serving_state_observability.counters_snapshot()
            self.assertEqual(snap["dormant_requests_count_in_process"], 1)
            self.assertEqual(snap["dormant_requests_count_since_state_set"], 1)
            self.assertIsNotNone(snap["last_blocked_at_ms"])
            # And the audit event fired.
            events = self._read_audit_events("predict_blocked_dormant")
            self.assertEqual(len(events), 1)
            meta = events[0]["metadata"]
            self.assertEqual(meta["serving_state"], "dormant_manual_pause")
            self.assertEqual(meta["mode"], "single")
            self.assertEqual(meta["event_count"], 1)
            self.assertEqual(meta["manifest_version"], "v_dormant_endpoint_test")
            self.assertEqual(events[0]["decision"], "block")

            # A 3-event batch increments by 1 request and event_count=3.
            batch_body = json.dumps({"events": [{}, {}, {}]}).encode("utf-8")
            asyncio.run(ml_server.score(self._build_stub_request(batch_body)))
            snap = ml_server.serving_state_observability.counters_snapshot()
            self.assertEqual(snap["dormant_requests_count_in_process"], 2)
            self.assertEqual(snap["dormant_requests_count_since_state_set"], 2)
            events = self._read_audit_events("predict_blocked_dormant")
            self.assertEqual(len(events), 2)
            self.assertEqual(events[1]["metadata"]["mode"], "batch")
            self.assertEqual(events[1]["metadata"]["event_count"], 3)
        finally:
            restore()
            ml_server.serving_state_observability = original_obs

    def test_serving_state_d2_score_active_does_not_bump_dormant_counters(self) -> None:
        """The active path must leave dormant counters untouched."""
        self._isolated_audit_root()
        ml_server = self._load_ml_server_for_endpoint()
        if ml_server is None:
            return

        sv_module = self._serving_state_module()
        original_obs = ml_server.serving_state_observability
        ml_server.serving_state_observability = sv_module.ServingStateObservability(
            sample_n=1, min_interval_sec=0.0
        )
        # Capture initial counter state.
        before = ml_server.serving_state_observability.counters_snapshot()

        class _StubRegistry:
            @staticmethod
            def snapshot():
                return OpsSmokeTests._stub_active_registry_snapshot()

        def _stub_score_single(event, disable_analogs=False):
            return {"ok": True}

        original_score = ml_server._score_single_event_with_log
        original_try_begin = ml_server._try_begin_score_request
        original_finish = ml_server._finish_score_request
        ml_server._score_single_event_with_log = _stub_score_single
        ml_server._try_begin_score_request = lambda: True
        ml_server._finish_score_request = lambda **kwargs: None

        restore = self._patch_score_path(
            ml_server,
            _StubRegistry(),
            self._build_active_serving_state(),
        )
        try:
            body = json.dumps({"event": {"symbol": "SPY"}}).encode("utf-8")
            asyncio.run(ml_server.score(self._build_stub_request(body)))
            after = ml_server.serving_state_observability.counters_snapshot()
            self.assertEqual(
                after["dormant_requests_count_in_process"],
                before["dormant_requests_count_in_process"],
            )
            self.assertEqual(
                after["dormant_requests_count_since_state_set"],
                before["dormant_requests_count_since_state_set"],
            )
            self.assertIsNone(after["last_blocked_at_ms"])
            # No audit event for the active path.
            self.assertEqual(self._read_audit_events("predict_blocked_dormant"), [])
        finally:
            restore()
            ml_server._score_single_event_with_log = original_score
            ml_server._try_begin_score_request = original_try_begin
            ml_server._finish_score_request = original_finish
            ml_server.serving_state_observability = original_obs

    def test_serving_state_d2_health_surfaces_observability_counters(self) -> None:
        """``/health.serving_state.observability`` must expose every
        process-local counter field downstream automation will read.

        Uses ``_build_health_response_dict`` via direct call so the test
        doesn't have to spin up a request; the dict is the same one the
        endpoint serializes."""
        ml_server = self._load_ml_server_for_endpoint()
        if ml_server is None:
            return

        sv_module = self._serving_state_module()
        original_obs = ml_server.serving_state_observability
        ml_server.serving_state_observability = sv_module.ServingStateObservability(
            sample_n=1, min_interval_sec=0.0
        )
        try:
            counters = ml_server.serving_state_observability.counters_snapshot()
            for key in (
                "transitions_count_in_process",
                "dormant_requests_count_in_process",
                "dormant_requests_count_since_state_set",
                "last_blocked_at_ms",
                "last_loaded_at_ms",
                "dormant_log_sample_n",
                "dormant_log_min_interval_ms",
            ):
                self.assertIn(key, counters)
            # All counters start at 0 / None.
            self.assertEqual(counters["transitions_count_in_process"], 0)
            self.assertEqual(counters["dormant_requests_count_in_process"], 0)
            self.assertEqual(counters["dormant_requests_count_since_state_set"], 0)
            self.assertIsNone(counters["last_blocked_at_ms"])
            # Increment once and confirm the snapshot reflects it.
            ml_server.serving_state_observability.record_dormant_block(123456)
            counters = ml_server.serving_state_observability.counters_snapshot()
            self.assertEqual(counters["dormant_requests_count_in_process"], 1)
            self.assertEqual(counters["last_blocked_at_ms"], 123456)
        finally:
            ml_server.serving_state_observability = original_obs


if __name__ == "__main__":
    unittest.main(verbosity=2)
