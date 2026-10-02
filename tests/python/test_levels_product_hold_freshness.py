"""Freshness, completeness, and reporting contracts for levels hold rates."""
from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
LEVELS_DIR = ROOT / "scripts" / "levels_product"
sys.path.insert(0, str(LEVELS_DIR))

from hold_engine import HORIZONS, current_rates, label_coverage  # noqa: E402


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


track_record = load_module(
    "levels_product_build_track_record",
    LEVELS_DIR / "build_track_record.py",
)
morning_map = load_module(
    "levels_product_morning_level_map",
    LEVELS_DIR / "morning_level_map.py",
)
publish = load_module(
    "levels_product_publish",
    LEVELS_DIR / "publish.py",
)


class HoldRateFreshnessTest(unittest.TestCase):
    def setUp(self):
        self.con = sqlite3.connect(":memory:")
        self.con.executescript(
            """
            CREATE TABLE touch_events (
                event_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                ts_event INTEGER NOT NULL,
                data_quality REAL NOT NULL,
                confluence_count INTEGER,
                bar_interval_sec INTEGER
            );
            CREATE TABLE bar_data (
                symbol TEXT NOT NULL,
                ts INTEGER NOT NULL,
                bar_interval_sec INTEGER NOT NULL
            );
            CREATE TABLE event_labels (
                event_id TEXT NOT NULL,
                horizon_min INTEGER NOT NULL,
                reject INTEGER,
                PRIMARY KEY (event_id, horizon_min)
            );
            """
        )

    def tearDown(self):
        self.con.close()

    def add_mature_event(self, event_id: str, ts_event: int, labeled_horizons=HORIZONS):
        self.con.execute(
            "INSERT INTO touch_events VALUES (?,?,?,?,?,?)",
            (event_id, "SPY", ts_event, 1.0, 1, 60),
        )
        for horizon in HORIZONS:
            self.con.execute(
                "INSERT INTO bar_data VALUES (?,?,?)",
                ("SPY", ts_event + horizon * 60_000, 60),
            )
        for horizon in labeled_horizons:
            self.con.execute(
                "INSERT INTO event_labels VALUES (?,?,?)",
                (event_id, horizon, 1),
            )
        self.con.commit()

    def test_missing_configured_horizon_fails_entire_snapshot_closed(self):
        for i in range(35):
            self.add_mature_event(f"e{i:02d}", 1_000_000 + i * 4_000_000, (15, 60))

        coverage = label_coverage(self.con, "SPY")
        rates = current_rates(self.con, "SPY")

        self.assertTrue(coverage[15]["complete"])
        self.assertEqual(coverage[30]["missing_count"], 35)
        self.assertFalse(coverage[30]["complete"])
        self.assertTrue(coverage[60]["complete"])
        self.assertFalse(rates["_publishable"])
        self.assertEqual(rates[15], {})
        self.assertEqual(rates[30], {})
        self.assertEqual(rates[60], {})

    def test_unlabeled_latest_mature_touch_marks_snapshot_stale(self):
        for i in range(35):
            horizons = HORIZONS if i < 34 else ()
            self.add_mature_event(f"e{i:02d}", 1_000_000 + i * 4_000_000, horizons)

        coverage = label_coverage(self.con, "SPY")
        rates = current_rates(self.con, "SPY")

        for horizon in HORIZONS:
            self.assertEqual(coverage[horizon]["missing_count"], 1)
            self.assertFalse(coverage[horizon]["complete"])
        self.assertFalse(rates["_publishable"])

    def test_complete_labels_publish_rates_with_coverage_metadata(self):
        for i in range(35):
            self.add_mature_event(f"e{i:02d}", 1_000_000 + i * 4_000_000)

        rates = current_rates(self.con, "SPY")

        self.assertTrue(rates["_publishable"])
        self.assertEqual(set(rates["_coverage"]), set(HORIZONS))
        for horizon in HORIZONS:
            self.assertTrue(rates["_coverage"][horizon]["complete"])
            self.assertEqual(rates[horizon]["1"]["rate"], 1.0)
            self.assertEqual(rates[horizon]["1"]["n"], 35)

    def test_invalid_fractional_interval_is_not_eligible_for_labels(self):
        self.con.execute(
            "INSERT INTO touch_events VALUES (?,?,?,?,?,?)",
            ("bad-grid", "SPY", 1_000_000, 1.0, 1, 60.5),
        )
        for horizon in HORIZONS:
            self.con.execute(
                "INSERT INTO bar_data VALUES (?,?,?)",
                ("SPY", 1_000_000 + horizon * 60_000, 60.5),
            )
        self.con.commit()

        coverage = label_coverage(self.con, "SPY")

        for horizon in HORIZONS:
            self.assertEqual(coverage[horizon]["eligible_count"], 0)
            self.assertEqual(coverage[horizon]["status"], "no_mature_events")

    def test_morning_snapshot_withholds_rates_when_any_horizon_is_incomplete(self):
        for i in range(35):
            self.add_mature_event(f"e{i:02d}", 1_000_000 + i * 4_000_000, (15, 60))

        snapshot = morning_map.hold_rate_snapshot(
            self.con,
            "SPY",
            anchor_ts_ms=1_000_000 + 35 * 4_000_000,
        )

        self.assertFalse(snapshot["publishable"])
        self.assertEqual(snapshot["rates"], {})
        self.assertEqual(snapshot["coverage"][30]["missing_count"], 35)


class TrackRecordWindowTest(unittest.TestCase):
    def test_recent_scoreboard_is_anchored_to_reporting_session(self):
        april = pd.DataFrame(
            {
                "d": [date(2026, 4, 23), date(2026, 4, 24)],
                "bkt": ["1", "1"],
                "reject": [1, 0],
                "pred": [0.8, 0.8],
            }
        )

        scoreboard = track_record.rolling_scoreboard(
            april,
            as_of_date=date(2026, 10, 2),
            recent_days=30,
        )

        self.assertEqual(scoreboard["window_end_date"], "2026-10-02")
        self.assertEqual(scoreboard["window_start_date"], "2026-08-21")
        self.assertEqual(scoreboard["n_alerted_touches"], 0)
        self.assertIsNone(scoreboard["actual_hold_rate"])
        self.assertEqual(scoreboard["latest_scored_date"], "2026-04-24")


class LabelOwnerConfigurationTest(unittest.TestCase):
    def test_all_label_entry_points_include_thirty_minutes_and_daily_owns_maturation(self):
        package = json.loads((ROOT / "package.json").read_text())
        daily = (LEVELS_DIR / "run_levels_product_daily.sh").read_text()
        installer = (LEVELS_DIR / "install_levels_product_agents.sh").read_text()
        wrapper = (ROOT / "scripts" / "run_daily_report_send.sh").read_text()
        retrain = (ROOT / "scripts" / "run_retrain_cycle.sh").read_text()

        self.assertIn("--horizons 5 15 30 60 --incremental", package["scripts"]["ml:build-labels"])
        self.assertIn("build_labels.py --horizons 5 15 30 60 --incremental", daily)
        self.assertNotIn("build_labels failed (continuing)", daily)
        self.assertIn('SKIP_LABELS="${LEVELS_SKIP_LABELS:-0}"', installer)
        self.assertIn('LEVELS_SKIP_LABELS="${LEVELS_SKIP_LABELS:-0}"', wrapper)
        self.assertIn('RETRAIN_SKIP_LABELS="${RETRAIN_SKIP_LABELS:-1}"', retrain)
        self.assertNotIn(
            'run_step "build_labels"    "${PYTHON}" scripts/build_labels.py',
            retrain,
        )


class MorningPublicationTest(unittest.TestCase):
    def test_render_withholds_all_rates_when_artifact_coverage_is_incomplete(self):
        mp = {
            "symbol": "SPY",
            "reference_spot": 700.0,
            "prior_session_date": "2026-10-01",
            "levels": [],
            "hold_rates_publishable": False,
            "label_coverage": {"30": {"complete": False, "missing_count": 7}},
            "unconditional_hold_rate_by_horizon": {
                "15": {"hold_rate": 0.80, "n": 100},
                "60": {"hold_rate": 0.87, "n": 100},
            },
        }
        tr = {
            "hold_rates_publishable": False,
            "label_coverage": {"30": {"complete": False, "missing_count": 7}},
            "horizons": {
                "h15": {"rolling_scoreboard_alerted_levels": {"actual_hold_rate": 0.80}},
                "h60": {"rolling_scoreboard_alerted_levels": {"actual_hold_rate": 0.87}},
            },
        }

        body = publish.render(mp, tr)

        self.assertIn("HOLD RATES UNAVAILABLE", body)
        self.assertIn("30m missing 7", body)
        self.assertNotIn("80%", body)
        self.assertNotIn("87%", body)
        self.assertNotIn("% held", body)

    def test_render_withholds_rates_when_legacy_artifacts_omit_coverage_metadata(self):
        mp = {
            "symbol": "SPY",
            "reference_spot": 700.0,
            "prior_session_date": "2026-10-01",
            "levels": [],
            "unconditional_hold_rate_by_horizon": {
                "15": {"hold_rate": 0.80, "n": 100},
            },
        }
        tr = {
            "horizons": {
                "h15": {"rolling_scoreboard_alerted_levels": {"actual_hold_rate": 0.80}},
            },
        }

        body = publish.render(mp, tr)

        self.assertIn("HOLD RATES UNAVAILABLE", body)
        self.assertIn("15m unavailable", body)
        self.assertNotIn("80%", body)
        self.assertNotIn("% held", body)


if __name__ == "__main__":
    unittest.main()
