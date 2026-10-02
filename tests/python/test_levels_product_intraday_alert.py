"""Delivery and checkpointing contracts for the intraday levels alert poller."""
from __future__ import annotations

import importlib.util
import io
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "levels_product" / "intraday_alert.py"

spec = importlib.util.spec_from_file_location("levels_product_intraday_alert", SCRIPT)
intraday_alert = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(intraday_alert)


class DeliveryCheckpointTest(unittest.TestCase):
    def setUp(self):
        self.con = sqlite3.connect(":memory:")
        self.con.execute("""CREATE TABLE alert_state (
            symbol TEXT PRIMARY KEY, last_ts INTEGER NOT NULL, last_event_id TEXT NOT NULL)""")
        self.con.execute("INSERT INTO alert_state VALUES (?,?,?)", ("SPY", 100, "e0"))
        self.con.commit()
        self.events = pd.DataFrame([
            {
                "ts_event": 101,
                "event_id": "e1",
                "level_type": "PP",
                "level_price": 760.0,
                "touch_side": 1,
                "confluence_count": 1,
            },
            {
                "ts_event": 102,
                "event_id": "e2",
                "level_type": "R1",
                "level_price": 765.0,
                "touch_side": -1,
                "confluence_count": 2,
            },
        ])

    def tearDown(self):
        self.con.close()

    def cursor(self):
        return self.con.execute(
            "SELECT last_ts, last_event_id FROM alert_state WHERE symbol='SPY'"
        ).fetchone()

    def run_delivery(self, outcomes, *, dry_run=False):
        with mock.patch.object(intraday_alert.notify, "post", side_effect=outcomes) as post:
            output = io.StringIO()
            with redirect_stdout(output):
                rc = intraday_alert.deliver_alerts(
                    self.events,
                    rates={},
                    scon=self.con,
                    symbol="SPY",
                    max_alerts=12,
                    dry_run=dry_run,
                )
        return rc, post, output.getvalue()

    def test_confirmed_deliveries_checkpoint_each_event(self):
        rc, post, output = self.run_delivery([True, True])

        self.assertEqual(rc, 0)
        self.assertEqual(post.call_count, 2)
        self.assertEqual(self.cursor(), (102, "e2"))
        self.assertIn("delivered 2 alert(s)", output)

    def test_first_delivery_failure_preserves_cursor_for_retry(self):
        rc, post, output = self.run_delivery([False])

        self.assertEqual(rc, 1)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(self.cursor(), (100, "e0"))
        self.assertIn("event_id=e1", output)
        self.assertIn("pending retry", output)

    def test_partial_batch_checkpoints_only_confirmed_delivery(self):
        rc, post, output = self.run_delivery([True, False])

        self.assertEqual(rc, 1)
        self.assertEqual(post.call_count, 2)
        self.assertEqual(self.cursor(), (101, "e1"))
        self.assertIn("delivered=1", output)
        self.assertIn("event_id=e2", output)

    def test_max_alerts_checkpoints_only_the_bounded_prefix(self):
        with mock.patch.object(intraday_alert.notify, "post", return_value=True) as post:
            output = io.StringIO()
            with redirect_stdout(output):
                rc = intraday_alert.deliver_alerts(
                    self.events,
                    rates={},
                    scon=self.con,
                    symbol="SPY",
                    max_alerts=1,
                )

        self.assertEqual(rc, 0)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(self.cursor(), (101, "e1"))
        self.assertIn("1 deferred to next poll", output.getvalue())

    def test_delivery_exception_keeps_prior_checkpoint(self):
        rc, post, output = self.run_delivery([True, OSError("network down")])

        self.assertEqual(rc, 1)
        self.assertEqual(post.call_count, 2)
        self.assertEqual(self.cursor(), (101, "e1"))
        self.assertIn("pending retry", output)

    def test_dry_run_previews_without_delivery_or_cursor_mutation(self):
        rc, post, output = self.run_delivery([], dry_run=True)

        self.assertEqual(rc, 0)
        post.assert_not_called()
        self.assertEqual(self.cursor(), (100, "e0"))
        self.assertIn("previewed 2 alert(s)", output)
        self.assertNotIn("delivered 2 alert(s)", output)


class HoldRateFormattingTest(unittest.TestCase):
    def test_incomplete_snapshot_never_formats_stale_percentages(self):
        row = pd.Series({
            "confluence_count": 1,
            "touch_side": 1,
            "level_type": "PP",
            "level_price": 760.0,
        })
        rates = {
            "_publishable": False,
            "_coverage": {
                15: {"complete": True, "missing_count": 0},
                30: {"complete": False, "missing_count": 35},
                60: {"complete": True, "missing_count": 0},
            },
            15: {"1": {"rate": 0.8046}},
            30: {},
            60: {"1": {"rate": 0.8659}},
        }

        alert = intraday_alert.fmt_alert(row, rates)

        self.assertNotIn("80%", alert)
        self.assertNotIn("87%", alert)
        self.assertIn("unavailable", alert)
        self.assertIn("30m", alert)
        self.assertNotIn("full history", alert)


class ProductionConfigurationTest(unittest.TestCase):
    def test_missing_webhook_fails_before_opening_databases(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(intraday_alert.notify.WEBHOOK_ENV, None)
            with mock.patch.object(
                intraday_alert,
                "_read_con",
                side_effect=AssertionError("database should not be opened"),
            ):
                output = io.StringIO()
                with redirect_stdout(output):
                    rc = intraday_alert.main([])

        self.assertEqual(rc, 2)
        self.assertIn("--dry-run", output.getvalue())
        self.assertIn(intraday_alert.notify.WEBHOOK_ENV, output.getvalue())

    def test_first_run_still_suppresses_historical_alerts(self):
        with tempfile.TemporaryDirectory() as td:
            read_db = Path(td) / "read.sqlite"
            state_db = Path(td) / "state.sqlite"
            rcon = sqlite3.connect(read_db)
            rcon.execute("""CREATE TABLE touch_events (
                ts_event INTEGER, event_id TEXT, symbol TEXT, data_quality REAL,
                confluence_count INTEGER)""")
            rcon.execute("INSERT INTO touch_events VALUES (?,?,?,?,?)",
                         (123, "historical", "SPY", 1.0, 2))
            rcon.commit()
            rcon.close()

            def state_connection():
                con = sqlite3.connect(state_db)
                con.execute("""CREATE TABLE IF NOT EXISTS alert_state (
                    symbol TEXT PRIMARY KEY, last_ts INTEGER NOT NULL,
                    last_event_id TEXT NOT NULL)""")
                con.commit()
                return con

            with mock.patch.dict(
                    os.environ,
                    {intraday_alert.notify.WEBHOOK_ENV: "https://example.test/webhook"},
                    clear=False), mock.patch.object(
                    intraday_alert, "_read_con",
                    side_effect=lambda: sqlite3.connect(read_db)), mock.patch.object(
                    intraday_alert, "_state_con", side_effect=state_connection), mock.patch.object(
                    intraday_alert.notify, "post") as post:
                output = io.StringIO()
                with redirect_stdout(output):
                    rc = intraday_alert.main([])

            post.assert_not_called()
            self.assertEqual(rc, 0)
            with sqlite3.connect(state_db) as check:
                self.assertEqual(
                    check.execute("SELECT last_ts, last_event_id FROM alert_state").fetchone(),
                    (123, "historical"),
                )
            self.assertIn("no alerts on backfill", output.getvalue())

    def test_first_run_dry_run_does_not_create_cursor(self):
        with tempfile.TemporaryDirectory() as td:
            read_db = Path(td) / "read.sqlite"
            state_db = Path(td) / "state.sqlite"
            rcon = sqlite3.connect(read_db)
            rcon.execute("""CREATE TABLE touch_events (
                ts_event INTEGER, event_id TEXT, symbol TEXT, data_quality REAL,
                confluence_count INTEGER)""")
            rcon.execute("INSERT INTO touch_events VALUES (?,?,?,?,?)",
                         (123, "historical", "SPY", 1.0, 2))
            rcon.commit()
            rcon.close()

            def state_connection():
                con = sqlite3.connect(state_db)
                con.execute("""CREATE TABLE IF NOT EXISTS alert_state (
                    symbol TEXT PRIMARY KEY, last_ts INTEGER NOT NULL,
                    last_event_id TEXT NOT NULL)""")
                con.commit()
                return con

            with mock.patch.object(
                    intraday_alert, "_read_con",
                    side_effect=lambda: sqlite3.connect(read_db)), mock.patch.object(
                    intraday_alert, "_state_con", side_effect=state_connection), mock.patch.object(
                    intraday_alert.notify, "post") as post:
                output = io.StringIO()
                with redirect_stdout(output):
                    rc = intraday_alert.main(["--dry-run"])

            post.assert_not_called()
            self.assertEqual(rc, 0)
            with sqlite3.connect(state_db) as check:
                self.assertEqual(
                    check.execute("SELECT COUNT(*) FROM alert_state").fetchone()[0],
                    0,
                )
            self.assertIn("state unchanged", output.getvalue())


if __name__ == "__main__":
    unittest.main()
