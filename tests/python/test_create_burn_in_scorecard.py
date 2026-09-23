from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from contextlib import contextmanager
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "create_burn_in_scorecard.py"


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class BurnInScorecardFreshnessTest(unittest.TestCase):
    def setUp(self):
        self.module = load_module("create_burn_in_scorecard_test", SCRIPT_PATH)

    def test_worst_status_precedence(self):
        worst = self.module._worst_status
        self.assertEqual(worst(["PASS", "WARN", "FAIL"]), "FAIL")
        self.assertEqual(worst(["PASS", "WARN"]), "WARN")
        self.assertEqual(worst(["PASS", "PASS"]), "PASS")
        self.assertEqual(worst(["PASS", "SKIP"]), "PASS")  # skip never masks a real pass
        self.assertEqual(worst(["SKIP"]), "SKIP")
        self.assertEqual(worst([]), "UNKNOWN")

    def test_template_renders_failing_freshness_section(self):
        summary = self.module.ScoringFreshnessSummary(
            db_path="data/pivot_events.sqlite",
            overall_result="FAIL",
            lines=["[FAIL] Scoring backlog: 8487 matured touch_events unscored; ..."],
        )
        body = self.module._template_body(
            report_date="2026-09-23",
            day_number=1,
            total_days=10,
            release_summary=None,
            scoring_summary=summary,
        )
        self.assertIn("## Scoring Freshness", body)
        self.assertIn("Scoring freshness result: **FAIL**", body)
        self.assertIn("Scoring backlog: 8487", body)

    def test_template_marks_not_run_when_skipped(self):
        body = self.module._template_body(
            report_date="2026-09-23",
            day_number=None,
            total_days=10,
            release_summary=None,
            scoring_summary=None,
        )
        self.assertIn("## Scoring Freshness", body)
        self.assertIn("Scoring freshness result: **NOT_RUN**", body)

    @staticmethod
    def _build_db(db_path: Path, *, n_events: int, n_scored: int) -> None:
        now = int(time.time() * 1000)
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("CREATE TABLE touch_events (event_id TEXT PRIMARY KEY, ts_event INTEGER)")
            conn.execute("CREATE TABLE prediction_log (id INTEGER PRIMARY KEY, event_id TEXT)")
            conn.execute("CREATE TABLE bar_data (ts INTEGER)")
            for i in range(n_events):
                conn.execute(
                    "INSERT INTO touch_events (event_id, ts_event) VALUES (?, ?)",
                    (f"evt{i:04d}", now - (n_events - i) * 60_000),
                )
            for i in range(n_scored):
                conn.execute("INSERT INTO prediction_log (event_id) VALUES (?)", (f"evt{i:04d}",))
            conn.execute("INSERT INTO bar_data (ts) VALUES (?)", (now,))
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def _scoring_env(self, **overrides):
        keys = ("SCORING_SETTLE_MINUTES", "SCORING_BACKLOG_WARN", "SCORING_BACKLOG_FAIL", "MARKET_DATA_STALE_MINUTES")
        saved = {k: os.environ.get(k) for k in keys}
        try:
            for k in keys:
                os.environ.pop(k, None)
            for k, v in overrides.items():
                os.environ[k] = str(v)
            yield
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_collect_scoring_freshness_flags_backlog(self):
        """End-to-end: a stalled runtime DB drives the section to FAIL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            self._build_db(db_path, n_events=80, n_scored=0)  # never scored
            with self._scoring_env(SCORING_SETTLE_MINUTES=0):
                summary = self.module._collect_scoring_freshness(db_path)
        self.assertEqual(summary.overall_result, "FAIL")
        self.assertTrue(any("stalled" in line.lower() for line in summary.lines))

    def test_scorecard_renders_pass_for_healthy_db(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            self._build_db(db_path, n_events=100, n_scored=100)
            with self._scoring_env(SCORING_SETTLE_MINUTES=0, SCORING_BACKLOG_WARN=5, SCORING_BACKLOG_FAIL=50):
                summary = self.module._collect_scoring_freshness(db_path)
                body = self.module._template_body(
                    report_date="2026-09-23",
                    day_number=1,
                    total_days=10,
                    release_summary=None,
                    scoring_summary=summary,
                )
        self.assertEqual(summary.overall_result, "PASS")
        self.assertIn("Scoring freshness result: **PASS**", body)

    def test_scorecard_renders_warn_for_backlog_band(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            self._build_db(db_path, n_events=100, n_scored=90)  # 10 unscored -> WARN band
            with self._scoring_env(SCORING_SETTLE_MINUTES=0, SCORING_BACKLOG_WARN=5, SCORING_BACKLOG_FAIL=50):
                summary = self.module._collect_scoring_freshness(db_path)
                body = self.module._template_body(
                    report_date="2026-09-23",
                    day_number=1,
                    total_days=10,
                    release_summary=None,
                    scoring_summary=summary,
                )
        self.assertEqual(summary.overall_result, "WARN")
        self.assertIn("Scoring freshness result: **WARN**", body)


if __name__ == "__main__":
    unittest.main()
