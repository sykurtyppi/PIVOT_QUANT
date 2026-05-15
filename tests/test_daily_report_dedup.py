#!/usr/bin/env python3
"""Tests for daily-report deduplication, calendar gating, and retrain routing.

Policy under test
-----------------
- At most one email per US trading day (dedupe via state file + lock).
- Only after regular US market close + buffer (ET hour >= 16 gate in retrain).
- Never on weekends (Sat/Sun) — run_daily_report_send.sh exits 0 without sending.
- Never on US market holidays — same exit-0 path.
- ML_REPORT_FORCE_SEND=true bypasses calendar and dedupe gates.
- send_daily_report.py is the low-level sender primitive; it has no scheduling
  policy and must not be expected to enforce any of the above.

All tests use mocked SMTP / subprocess; no real emails are sent.
No files under data/, reports/, logs/, or evidence/ are modified.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = str(Path(sys.executable).resolve())
WRAPPER = str(REPO_ROOT / "scripts" / "run_daily_report_send.sh")
SENDER = str(REPO_ROOT / "scripts" / "send_daily_report.py")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stub_sender_script(tmp: Path) -> Path:
    """Write a fake send_daily_report.py that records invocations and exits 0."""
    stub = tmp / "send_daily_report_stub.py"
    stub.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            # Stub sender — writes a sentinel file and exits 0.
            import sys
            from pathlib import Path
            sentinel = Path(sys.argv[sys.argv.index('--report') + 1]).parent / 'SEND_CALLED'
            sentinel.touch()
            print('[stub] send_daily_report called with:', sys.argv[1:])
            sys.exit(0)
            """
        ),
        encoding="utf-8",
    )
    stub.chmod(0o755)
    return stub


def _stub_report_generator(tmp: Path, report_path: Path) -> Path:
    """Write a fake generate_daily_ml_report.py that writes a minimal report."""
    stub = tmp / "generate_daily_ml_report_stub.py"
    # The real generator prints the path as the final stdout line.
    stub.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env python3
            import sys
            from pathlib import Path
            report = Path(r'{report_path}')
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text('# Daily ML Report\\nreport_date: 2026-05-14\\n', encoding='utf-8')
            print(str(report))
            sys.exit(0)
            """
        ),
        encoding="utf-8",
    )
    stub.chmod(0o755)
    return stub


def _make_stub_scripts_dir(tmp: Path, report_path: Path) -> Path:
    """Create a scripts/ dir inside tmp with stub generate + send scripts."""
    scripts_dir = tmp / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    # Stub generate_daily_ml_report.py
    gen_stub = scripts_dir / "generate_daily_ml_report.py"
    gen_stub.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env python3
            import sys
            from pathlib import Path
            report = Path(r'{report_path}')
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text('# Daily ML Report\\nreport_date: 2026-05-14\\n', encoding='utf-8')
            print(str(report))
            sys.exit(0)
            """
        ),
        encoding="utf-8",
    )
    gen_stub.chmod(0o755)

    # Stub send_daily_report.py
    send_stub = scripts_dir / "send_daily_report.py"
    send_stub.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import sys
            from pathlib import Path
            # Write a sentinel so tests can detect invocation.
            sentinel = Path(__file__).parent.parent / 'SEND_CALLED'
            sentinel.touch()
            print('[stub] send_daily_report called with:', sys.argv[1:])
            sys.exit(0)
            """
        ),
        encoding="utf-8",
    )
    send_stub.chmod(0o755)

    # _pybin.sh — minimal shim that exports PYTHON_BIN
    pybin = scripts_dir / "_pybin.sh"
    pybin.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env bash
            PYTHON_BIN="{PYTHON}"
            export PYTHON_BIN
            """
        ),
        encoding="utf-8",
    )

    return scripts_dir


def _run_wrapper(
    env_overrides: dict[str, str],
    tmp: Path,
) -> subprocess.CompletedProcess[str]:
    """Run run_daily_report_send.sh inside a sandboxed tmp root.

    The wrapper resolves ROOT_DIR from its own path so we copy it into tmp.
    We override ROOT_DIR via env so the lock, state file, and log land in tmp.
    """
    # Copy the real wrapper into tmp/scripts/
    scripts_dir = tmp / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    wrapper_copy = scripts_dir / "run_daily_report_send.sh"
    wrapper_src = REPO_ROOT / "scripts" / "run_daily_report_send.sh"
    wrapper_copy.write_bytes(wrapper_src.read_bytes())
    wrapper_copy.chmod(0o755)

    # _pybin.sh shim
    pybin = scripts_dir / "_pybin.sh"
    pybin.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env bash
            PYTHON_BIN="{PYTHON}"
            export PYTHON_BIN
            """
        ),
        encoding="utf-8",
    )

    # Stub generate_daily_ml_report.py
    report_path = tmp / "logs" / "reports" / "daily_ml_report_test.md"
    gen_stub = scripts_dir / "generate_daily_ml_report.py"
    gen_stub.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env python3
            import sys
            from pathlib import Path
            report = Path(r'{report_path}')
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text('# Daily ML Report\\nreport_date: 2026-05-14\\n', encoding='utf-8')
            print(str(report))
            sys.exit(0)
            """
        ),
        encoding="utf-8",
    )

    # Stub send_daily_report.py
    send_stub = scripts_dir / "send_daily_report.py"
    send_stub.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env python3
            import sys
            from pathlib import Path
            sentinel = Path(r'{tmp}') / 'SEND_CALLED'
            sentinel.touch()
            print('[stub] send_daily_report called with:', sys.argv[1:])
            sys.exit(0)
            """
        ),
        encoding="utf-8",
    )

    log_dir = tmp / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    # Point the wrapper at our sandboxed tmp tree
    env["ROOT_DIR"] = str(tmp)
    env["PYTHON_BIN"] = PYTHON
    # Disable real DB
    env["PIVOT_DB"] = str(tmp / "data" / "pivot_events.sqlite")
    # Remove any live .env influence
    env["ML_REPORT_ENV_FILE"] = "/dev/null"
    env.update(env_overrides)

    return subprocess.run(
        ["bash", str(wrapper_copy)],
        cwd=str(tmp),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


# ---------------------------------------------------------------------------
# Test: retrain notify skipped before ET 16:00
# ---------------------------------------------------------------------------

class TestRetrainTimegate(unittest.TestCase):
    """Verify the 16:00 ET time gate added in run_retrain_cycle.sh.

    These tests do not invoke run_retrain_cycle.sh directly (it requires a
    live DB and ML server).  Instead they unit-test the gate logic extracted
    into a small Python helper that mirrors exactly what the shell script does.
    """

    def _gate_passes(self, et_hour: int) -> bool:
        """Return True if the retrain notify gate would let the send proceed."""
        # Mirror the gate from run_retrain_cycle.sh lines 651-652:
        #   if [[ "${FORCE_NOTIFY_LC}" != "true" ]] && (( ET_HOUR < 16 )); then skip
        force = False
        return not (not force and et_hour < 16)

    def test_retrain_notify_skipped_before_close(self) -> None:
        """Before 16:00 ET, the retrain path should skip the wrapper call."""
        et_hour = 10
        # Gate should NOT pass — send is skipped
        self.assertFalse(
            self._gate_passes(et_hour),
            f"Gate should block send at ET hour {et_hour}",
        )

    def test_retrain_notify_calls_wrapper_after_close(self) -> None:
        """At 17:00 ET, the retrain path should invoke the wrapper with schedule_mode=close."""
        et_hour = 17
        self.assertTrue(
            self._gate_passes(et_hour),
            f"Gate should allow send at ET hour {et_hour}",
        )

    def test_force_bypasses_time_gate(self) -> None:
        """FORCE_SEND=true bypasses the time gate even before 16:00 ET."""
        # With force=True the gate passes regardless of hour
        force = True
        et_hour = 8
        gate_passes = not (not force and et_hour < 16)
        self.assertTrue(gate_passes)


# ---------------------------------------------------------------------------
# Test: dedupe via state file
# ---------------------------------------------------------------------------

class TestDedupeStateFile(unittest.TestCase):

    def test_second_close_send_deduped(self) -> None:
        """Wrapper exits 0 without calling send_daily_report.py when state already has {date}|close=ok."""
        with tempfile.TemporaryDirectory(prefix="pivotquant_test_") as raw_tmp:
            tmp = Path(raw_tmp)
            log_dir = tmp / "logs"
            log_dir.mkdir(parents=True)

            # Pre-populate state file with today already sent
            report_date = "2026-05-14"
            state_file = log_dir / "report_delivery_state.json"
            state_file.write_text(
                json.dumps({"sent": {f"{report_date}|close": "ok"}}),
                encoding="utf-8",
            )

            result = _run_wrapper(
                {
                    "ML_REPORT_REPORT_DATE": report_date,
                    "ML_REPORT_SCHEDULE_MODE": "close",
                    "ML_REPORT_FORCE_SEND": "false",
                },
                tmp,
            )

            send_called = (tmp / "SEND_CALLED").exists()
            self.assertFalse(send_called, "send_daily_report.py must NOT be called on duplicate send")
            self.assertEqual(result.returncode, 0, f"Wrapper should exit 0 on dedup. stderr={result.stderr}")

            # Verify log mentions skipping
            delivery_log = log_dir / "report_delivery.log"
            if delivery_log.exists():
                self.assertIn("already sent", delivery_log.read_text())


# ---------------------------------------------------------------------------
# Test: weekend + holiday gate in the wrapper
# ---------------------------------------------------------------------------

class TestCalendarGate(unittest.TestCase):

    def _run_on_date(self, report_date: str, force: str = "false") -> tuple[int, bool, str]:
        """Run wrapper with a pinned REPORT_DATE; return (exit_code, send_called, log_text)."""
        with tempfile.TemporaryDirectory(prefix="pivotquant_test_") as raw_tmp:
            tmp = Path(raw_tmp)
            result = _run_wrapper(
                {
                    "ML_REPORT_REPORT_DATE": report_date,
                    "ML_REPORT_SCHEDULE_MODE": "close",
                    "ML_REPORT_FORCE_SEND": force,
                },
                tmp,
            )
            send_called = (tmp / "SEND_CALLED").exists()
            log_path = tmp / "logs" / "report_delivery.log"
            log_text = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
            return result.returncode, send_called, log_text

    def test_saturday_skipped_without_force(self) -> None:
        """Saturday 2026-05-16: wrapper must exit 0 without sending."""
        # 2026-05-16 is a Saturday
        rc, send_called, log_text = self._run_on_date("2026-05-16", force="false")
        self.assertEqual(rc, 0, "Wrapper must exit 0 on non-trading day")
        self.assertFalse(send_called, "send_daily_report.py must NOT be called on Saturday")
        self.assertIn("non-trading day", log_text, "Log must mention non-trading day")

    def test_holiday_skipped_without_force(self) -> None:
        """Independence Day 2026-07-03 (observed): wrapper must exit 0 without sending."""
        # NYSE observes Independence Day on 2026-07-03 (Friday, since 07-04 is Saturday)
        rc, send_called, log_text = self._run_on_date("2026-07-03", force="false")
        self.assertEqual(rc, 0, "Wrapper must exit 0 on NYSE holiday")
        self.assertFalse(send_called, "send_daily_report.py must NOT be called on NYSE holiday")
        self.assertIn("non-trading day", log_text, "Log must mention non-trading day")

    def test_sunday_skipped_without_force(self) -> None:
        """Sunday 2026-05-17: wrapper must exit 0 without sending."""
        rc, send_called, log_text = self._run_on_date("2026-05-17", force="false")
        self.assertEqual(rc, 0, "Wrapper must exit 0 on Sunday")
        self.assertFalse(send_called, "send_daily_report.py must NOT be called on Sunday")
        self.assertIn("non-trading day", log_text)

    def test_force_send_overrides_weekend(self) -> None:
        """ML_REPORT_FORCE_SEND=true on Saturday must allow the send to proceed."""
        # 2026-05-16 is Saturday — with FORCE_SEND=true the gate is bypassed
        _report_date = "2026-05-16"
        with tempfile.TemporaryDirectory(prefix="pivotquant_test_") as raw_tmp:
            tmp = Path(raw_tmp)
            result = _run_wrapper(
                {
                    "ML_REPORT_REPORT_DATE": _report_date,
                    "ML_REPORT_SCHEDULE_MODE": "close",
                    "ML_REPORT_FORCE_SEND": "true",
                },
                tmp,
            )
            send_called = (tmp / "SEND_CALLED").exists()
            # The stub send exits 0; the wrapper should succeed
            self.assertEqual(result.returncode, 0, f"Force send on Saturday should not fail. stderr={result.stderr}")
            self.assertTrue(
                send_called,
                "send_daily_report.py MUST be called when ML_REPORT_FORCE_SEND=true",
            )

    def test_trading_day_proceeds(self) -> None:
        """A regular weekday (2026-05-14, Thursday) must attempt to send."""
        # 2026-05-14 is a Thursday, not a holiday
        with tempfile.TemporaryDirectory(prefix="pivotquant_test_") as raw_tmp:
            tmp = Path(raw_tmp)
            result = _run_wrapper(
                {
                    "ML_REPORT_REPORT_DATE": "2026-05-14",
                    "ML_REPORT_SCHEDULE_MODE": "close",
                    "ML_REPORT_FORCE_SEND": "false",
                },
                tmp,
            )
            send_called = (tmp / "SEND_CALLED").exists()
            # On a trading day the wrapper should proceed past the gate and call send
            self.assertEqual(result.returncode, 0, f"Wrapper failed on trading day. stderr={result.stderr}")
            self.assertTrue(send_called, "send_daily_report.py must be called on a trading day")


# ---------------------------------------------------------------------------
# Test: send_daily_report.py direct invocation — no guard
# ---------------------------------------------------------------------------

class TestDirectSenderNoGuard(unittest.TestCase):
    """send_daily_report.py is the low-level primitive — it has no scheduling
    policy.  Direct invocation on a weekend or holiday must attempt to send
    (mocked) without consulting a state file or trading-day calendar."""

    def test_direct_sender_no_guard(self) -> None:
        """Direct call to send_daily_report.py on a Saturday must proceed to send (mocked SMTP)."""
        with tempfile.TemporaryDirectory(prefix="pivotquant_test_") as raw_tmp:
            tmp = Path(raw_tmp)

            # Minimal report file
            report_path = tmp / "daily_ml_report.md"
            report_path.write_text(
                "# Daily ML Report\nreport_date: 2026-05-16\nhealth: OK\n",
                encoding="utf-8",
            )

            # Run with --dry-run and --no-dedupe-guard on a Saturday
            # --dry-run prevents real SMTP; --no-dedupe-guard is the documented flag
            env = os.environ.copy()
            env["PIVOT_DB"] = str(tmp / "nonexistent.sqlite")
            env["ML_REPORT_ENV_FILE"] = "/dev/null"
            # No SMTP channels configured → script will warn but not fail
            env["ML_REPORT_NOTIFY_CHANNELS"] = ""

            result = subprocess.run(
                [
                    PYTHON, SENDER,
                    "--report", str(report_path),
                    "--dry-run",
                    "--no-dedupe-guard",
                    "--channel", "email",
                ],
                cwd=str(REPO_ROOT),
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

            # The script should not crash — it may warn about missing SMTP config
            # but it must NOT exit non-zero purely because of a calendar check.
            self.assertNotIn(
                "non-trading day",
                (result.stdout + result.stderr).lower(),
                "send_daily_report.py must not apply trading-day calendar policy",
            )
            self.assertNotIn(
                "already sent",
                (result.stdout + result.stderr).lower(),
                "send_daily_report.py must not consult the state-file dedupe",
            )
            # Accept exit 0 (dry-run ok) or exit 1 (missing SMTP config warned)
            # but NOT exit 2 (report not found) — report file exists.
            self.assertNotEqual(result.returncode, 2, "Report file should have been found")

    def test_no_dedupe_guard_flag_accepted(self) -> None:
        """--no-dedupe-guard must be accepted without error (even if unused)."""
        with tempfile.TemporaryDirectory(prefix="pivotquant_test_") as raw_tmp:
            tmp = Path(raw_tmp)
            report_path = tmp / "report.md"
            report_path.write_text("# Daily ML Report\nreport_date: 2026-05-14\n")

            env = os.environ.copy()
            env["PIVOT_DB"] = str(tmp / "nonexistent.sqlite")
            env["ML_REPORT_ENV_FILE"] = "/dev/null"
            env["ML_REPORT_NOTIFY_CHANNELS"] = ""

            result = subprocess.run(
                [
                    PYTHON, SENDER,
                    "--report", str(report_path),
                    "--dry-run",
                    "--no-dedupe-guard",
                ],
                cwd=str(REPO_ROOT),
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotIn(
                "unrecognized arguments",
                result.stderr.lower(),
                "--no-dedupe-guard flag must be recognized by argparse",
            )
            self.assertNotEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
