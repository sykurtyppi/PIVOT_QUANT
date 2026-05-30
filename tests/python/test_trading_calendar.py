#!/usr/bin/env python3
"""Tests for the shared NYSE trading-calendar module.

Pins:
  - Known 2026 full holidays are non-trading.
  - A normal weekday is a trading day.
  - Weekends are non-trading.
  - All three former call sites (generate_daily_ml_report.py,
    collect_gamma_history.py, run_daily_report_send.sh) now agree with the
    centralized helper rather than carrying their own copy of the holiday list.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"
PYTHON = str(Path(sys.executable).resolve())

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import trading_calendar  # noqa: E402


def _load_module(module_name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec for {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


# Known 2026 NYSE full-closure holidays (all fall on weekdays).
HOLIDAYS_2026 = [
    date(2026, 1, 1),
    date(2026, 1, 19),
    date(2026, 2, 16),
    date(2026, 4, 3),
    date(2026, 5, 25),
    date(2026, 6, 19),
    date(2026, 7, 3),
    date(2026, 9, 7),
    date(2026, 11, 26),
    date(2026, 12, 25),
]


class TestTradingCalendarCore(unittest.TestCase):
    def test_known_2026_holidays_are_non_trading(self) -> None:
        for holiday in HOLIDAYS_2026:
            self.assertIn(holiday, trading_calendar.NYSE_HOLIDAYS, msg=str(holiday))
            self.assertFalse(
                trading_calendar.is_trading_day(holiday),
                msg=f"{holiday} should be a non-trading holiday",
            )

    def test_normal_weekday_is_trading(self) -> None:
        # 2026-03-17 is a Tuesday and not a holiday.
        d = date(2026, 3, 17)
        self.assertEqual(d.weekday(), 1)
        self.assertTrue(trading_calendar.is_trading_day(d))

    def test_weekends_are_non_trading(self) -> None:
        saturday = date(2026, 3, 14)
        sunday = date(2026, 3, 15)
        self.assertEqual(saturday.weekday(), 5)
        self.assertEqual(sunday.weekday(), 6)
        self.assertFalse(trading_calendar.is_trading_day(saturday))
        self.assertFalse(trading_calendar.is_trading_day(sunday))

    def test_half_day_is_still_a_trading_day(self) -> None:
        # Day after Thanksgiving 2026 — early close, but market is open.
        d = date(2026, 11, 27)
        self.assertTrue(trading_calendar.is_half_day(d))
        self.assertTrue(trading_calendar.is_trading_day(d))
        self.assertEqual(
            trading_calendar.session_close_et(d), trading_calendar.EARLY_CLOSE_ET
        )

    def test_regular_close_for_normal_day(self) -> None:
        d = date(2026, 3, 17)
        self.assertEqual(
            trading_calendar.session_close_et(d),
            trading_calendar.REGULAR_SESSION_CLOSE_ET,
        )


class TestCallSitesAgree(unittest.TestCase):
    """Every former call site must defer to the centralized helper."""

    def test_generate_daily_ml_report_uses_shared_helper(self) -> None:
        mod = _load_module(
            "pq_test_generate_daily_ml_report",
            SCRIPTS_DIR / "generate_daily_ml_report.py",
        )
        # Imported the shared function (identity), not a private copy.
        self.assertIs(mod.is_trading_day, trading_calendar.is_trading_day)
        # And the file no longer defines its own holiday list.
        self.assertFalse(hasattr(mod, "NYSE_HOLIDAYS"))

    def test_collect_gamma_history_iter_trading_days_excludes_holidays(self) -> None:
        mod = _load_module(
            "pq_test_collect_gamma_history",
            SCRIPTS_DIR / "collect_gamma_history.py",
        )
        self.assertIs(mod.is_trading_day, trading_calendar.is_trading_day)
        # Range spans Thanksgiving week 2026 (holiday on the 26th).
        start = date(2026, 11, 23)
        end = date(2026, 11, 30)
        produced = list(mod.iter_trading_days(start, end))
        expected = [
            start + timedelta(days=i)
            for i in range((end - start).days + 1)
            if trading_calendar.is_trading_day(start + timedelta(days=i))
        ]
        self.assertEqual(produced, expected)
        self.assertNotIn(date(2026, 11, 26), produced)  # Thanksgiving
        self.assertNotIn(date(2026, 11, 28), produced)  # Saturday

    def test_shell_wrapper_has_no_inline_holiday_list(self) -> None:
        text = (SCRIPTS_DIR / "run_daily_report_send.sh").read_text(encoding="utf-8")
        self.assertIn("from trading_calendar import is_trading_day", text)
        # The PR #32 inline copy is gone — pick a couple of dates that were
        # hardcoded and assert they no longer appear in the wrapper.
        self.assertNotIn("2026-11-26", text)
        self.assertNotIn("2025-12-25", text)

    def test_shell_gate_skips_on_holiday_via_shared_module(self) -> None:
        """End-to-end: the wrapper's is_trading_day path resolves through the
        shared module and skips a full holiday."""
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = Path(tmp_str)
            scripts_dir = tmp / "scripts"
            logs_dir = tmp / "logs"
            scripts_dir.mkdir(parents=True, exist_ok=True)
            logs_dir.mkdir(parents=True, exist_ok=True)

            wrapper = scripts_dir / "run_daily_report_send.sh"
            wrapper.write_bytes((SCRIPTS_DIR / "run_daily_report_send.sh").read_bytes())
            wrapper.chmod(0o755)
            (scripts_dir / "trading_calendar.py").write_bytes(
                (SCRIPTS_DIR / "trading_calendar.py").read_bytes()
            )
            (scripts_dir / "_pybin.sh").write_text(
                textwrap.dedent(
                    f"""\
                    #!/usr/bin/env bash
                    PYTHON_BIN="{PYTHON}"
                    export PYTHON_BIN
                    """
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["PYTHON_BIN"] = PYTHON
            env["ML_REPORT_ENV_FILE"] = "/dev/null"
            env["PIVOT_DB"] = str(tmp / "data" / "pivot_events.sqlite")
            # 2026-11-26 is Thanksgiving (a full NYSE holiday).
            env["ML_REPORT_FAKE_ET_DATE"] = "2026-11-26"

            proc = subprocess.run(
                ["bash", str(wrapper)],
                cwd=str(tmp),
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")
            log_text = (logs_dir / "report_delivery.log").read_text(encoding="utf-8")
            self.assertIn("non-trading day", log_text)


if __name__ == "__main__":
    unittest.main()
