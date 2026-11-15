"""Delivery and checkpointing contracts for the intraday levels alert poller."""
from __future__ import annotations

import importlib.util
import io
import json
from datetime import date, datetime, timezone
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
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
            with closing(sqlite3.connect(state_db)) as check:
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
            with closing(sqlite3.connect(state_db)) as check:
                self.assertEqual(
                    check.execute("SELECT COUNT(*) FROM alert_state").fetchone()[0],
                    0,
                )
            self.assertIn("state unchanged", output.getvalue())

    def test_snapshot_contract_failure_is_audited_before_cursor_advances(self):
        with tempfile.TemporaryDirectory() as td:
            read_db = Path(td) / "read.sqlite"
            state_db = Path(td) / "state.sqlite"
            rcon = sqlite3.connect(read_db)
            rcon.execute("""CREATE TABLE touch_events (
                ts_event INTEGER, event_id TEXT, symbol TEXT, data_quality REAL,
                confluence_count INTEGER, level_type TEXT, level_price REAL,
                touch_side INTEGER)""")
            rcon.execute(
                "INSERT INTO touch_events VALUES (?,?,?,?,?,?,?,?)",
                (1_797_431_400_000, "new-event", "SPY", 1.0, 2, "R1", 765.0, -1),
            )
            rcon.commit()
            rcon.close()

            scon = sqlite3.connect(state_db)
            scon.execute("""CREATE TABLE alert_state (
                symbol TEXT PRIMARY KEY, last_ts INTEGER NOT NULL,
                last_event_id TEXT NOT NULL)""")
            scon.execute("INSERT INTO alert_state VALUES (?,?,?)", ("SPY", 100, "old-event"))
            scon.commit()
            scon.close()

            with mock.patch.dict(
                    os.environ,
                    {intraday_alert.notify.WEBHOOK_ENV: "https://example.test/webhook"},
                    clear=False), mock.patch.object(
                    intraday_alert, "_read_con",
                    side_effect=lambda: sqlite3.connect(read_db)), mock.patch.object(
                    intraday_alert, "_state_con",
                    side_effect=lambda: sqlite3.connect(state_db)), mock.patch.object(
                    intraday_alert, "_snapshot_for_event",
                    side_effect=ValueError("missing canonical snapshot")), mock.patch.object(
                    intraday_alert, "current_rates") as current_rates, mock.patch.object(
                    intraday_alert.notify, "post") as post:
                output = io.StringIO()
                with redirect_stdout(output):
                    rc = intraday_alert.main([])

            self.assertEqual(rc, 3)
            current_rates.assert_not_called()
            post.assert_not_called()
            with closing(sqlite3.connect(state_db)) as check:
                self.assertEqual(
                    check.execute("SELECT last_ts, last_event_id FROM alert_state").fetchone(),
                    (1_797_431_400_000, "new-event"),
                )
                disposition = check.execute(
                    """SELECT disposition, session_date, detail
                       FROM alert_event_dispositions
                       WHERE symbol='SPY' AND event_id='new-event'"""
                ).fetchone()
            self.assertEqual(disposition[0], "snapshot_mismatch")
            self.assertEqual(disposition[1], "2026-12-16")
            self.assertIn("missing canonical snapshot", disposition[2])
            self.assertIn("recorded snapshot_mismatch", output.getvalue())

    def test_delivery_retry_uses_snapshot_binding_captured_before_rollover(self):
        with tempfile.TemporaryDirectory() as td:
            read_db = Path(td) / "read.sqlite"
            state_db = Path(td) / "state.sqlite"
            event_ts = 1_797_431_400_000
            with closing(sqlite3.connect(read_db)) as rcon:
                rcon.execute("""CREATE TABLE touch_events (
                    ts_event INTEGER, event_id TEXT, symbol TEXT, data_quality REAL,
                    confluence_count INTEGER, level_type TEXT, level_price REAL,
                    touch_side INTEGER)""")
                rcon.execute(
                    "INSERT INTO touch_events VALUES (?,?,?,?,?,?,?,?)",
                    (event_ts, "poison-event", "SPY", 1.0, 1, "M1", 766.1820, 1),
                )
                rcon.commit()
            with closing(sqlite3.connect(state_db)) as scon:
                scon.execute("""CREATE TABLE alert_state (
                    symbol TEXT PRIMARY KEY, last_ts INTEGER NOT NULL,
                    last_event_id TEXT NOT NULL)""")
                scon.execute("INSERT INTO alert_state VALUES (?,?,?)", ("SPY", 100, "old-event"))
                scon.commit()

            bound = intraday_alert.SimpleNamespace(
                ts_event=event_ts,
                event_id="poison-event",
                level_type="M1",
                level_price=766.1820,
                touch_side=1,
                confluence_count=1,
                snapshot_id="lvs_original",
                level_id="lvl_original_m1",
            )
            with mock.patch.dict(
                    os.environ,
                    {intraday_alert.notify.WEBHOOK_ENV: "https://example.test/webhook"},
                    clear=False), mock.patch.object(
                    intraday_alert, "_read_con",
                    side_effect=lambda: sqlite3.connect(read_db)), mock.patch.object(
                    intraday_alert, "_state_con",
                    side_effect=lambda: sqlite3.connect(state_db)), mock.patch.object(
                    intraday_alert, "_snapshot_for_event",
                    return_value={"snapshot_id": "lvs_original"}) as load_snapshot, mock.patch.object(
                    intraday_alert, "bind_snapshot_identity",
                    side_effect=[bound, ValueError("newer snapshot no longer matches M1")]
                    ) as bind_identity, mock.patch.object(
                    intraday_alert, "current_rates", return_value={}), mock.patch.object(
                    intraday_alert.notify, "post", side_effect=[False, True]) as post:
                first_output = io.StringIO()
                with redirect_stdout(first_output):
                    first_rc = intraday_alert.main([])
                second_output = io.StringIO()
                with redirect_stdout(second_output):
                    second_rc = intraday_alert.main([])

            self.assertEqual(first_rc, 1)
            self.assertEqual(second_rc, 0)
            self.assertEqual(post.call_count, 2)
            self.assertEqual(load_snapshot.call_count, 1)
            self.assertEqual(bind_identity.call_count, 1)
            with closing(sqlite3.connect(state_db)) as check:
                self.assertEqual(
                    check.execute("SELECT last_ts, last_event_id FROM alert_state").fetchone(),
                    (event_ts, "poison-event"),
                )
                self.assertEqual(
                    check.execute(
                        """SELECT snapshot_id, level_id, disposition
                           FROM alert_event_dispositions
                           WHERE symbol='SPY' AND event_id='poison-event'"""
                    ).fetchone(),
                    ("lvs_original", "lvl_original_m1", "delivered"),
                )
            self.assertIn("pending retry", first_output.getvalue())
            self.assertIn("delivered 1 alert(s)", second_output.getvalue())

    def test_deferred_contract_failure_does_not_block_bounded_valid_prefix(self):
        with tempfile.TemporaryDirectory() as td:
            read_db = Path(td) / "read.sqlite"
            state_db = Path(td) / "state.sqlite"
            base_ts = 1_797_431_400_000
            with closing(sqlite3.connect(read_db)) as rcon:
                rcon.execute("""CREATE TABLE touch_events (
                    ts_event INTEGER, event_id TEXT, symbol TEXT, data_quality REAL,
                    confluence_count INTEGER, level_type TEXT, level_price REAL,
                    touch_side INTEGER)""")
                rcon.executemany(
                    "INSERT INTO touch_events VALUES (?,?,?,?,?,?,?,?)",
                    [
                        (base_ts + index, f"event-{index:02d}", "SPY", 1.0, 2,
                         "PP", 760.0, 1)
                        for index in range(13)
                    ],
                )
                rcon.commit()
            with closing(sqlite3.connect(state_db)) as scon:
                scon.execute("""CREATE TABLE alert_state (
                    symbol TEXT PRIMARY KEY, last_ts INTEGER NOT NULL,
                    last_event_id TEXT NOT NULL)""")
                scon.execute("INSERT INTO alert_state VALUES (?,?,?)", ("SPY", 100, "old-event"))
                scon.commit()

            def resolve_prefix(frame, symbol, scon, persist):
                self.assertEqual(symbol, "SPY")
                self.assertIsInstance(scon, sqlite3.Connection)
                self.assertTrue(persist)
                self.assertEqual(len(frame), 12)
                if "event-12" in set(frame.event_id):
                    raise ValueError("deferred row should not be bound")
                bound = frame.copy()
                bound["snapshot_id"] = "lvs_test"
                bound["level_id"] = "lvl_test"
                return bound, None

            with mock.patch.dict(
                    os.environ,
                    {intraday_alert.notify.WEBHOOK_ENV: "https://example.test/webhook"},
                    clear=False), mock.patch.object(
                    intraday_alert, "_read_con",
                    side_effect=lambda: sqlite3.connect(read_db)), mock.patch.object(
                    intraday_alert, "_state_con",
                    side_effect=lambda: sqlite3.connect(state_db)), mock.patch.object(
                    intraday_alert, "resolve_pending_rows",
                    side_effect=resolve_prefix), mock.patch.object(
                    intraday_alert, "current_rates",
                    return_value={}), mock.patch.object(
                    intraday_alert.notify, "post", return_value=True) as post:
                output = io.StringIO()
                with redirect_stdout(output):
                    rc = intraday_alert.main(["--max-alerts", "12"])

            self.assertEqual(rc, 0)
            self.assertEqual(post.call_count, 12)
            with closing(sqlite3.connect(state_db)) as check:
                self.assertEqual(
                    check.execute("SELECT last_ts, last_event_id FROM alert_state").fetchone(),
                    (base_ts + 11, "event-11"),
                )
            self.assertIn("1 deferred", output.getvalue())


class SnapshotUnavailablePollTest(unittest.TestCase):
    """Real poll/SQLite/filesystem regressions; only rates and transport are stubbed."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.read_db = self.root / "touches.sqlite"
        self.state_db = self.root / "state.sqlite"
        import level_snapshot
        self.contract = level_snapshot
        self.snapshots = [level_snapshot.build_snapshot(
            symbol="SPY", source_session_date=source,
            source_ohlc={"high": 770, "low": 750, "close": 760},
            generated_at_utc=f"{source.isoformat()}T22:00:00Z",
            raw_levels=level_snapshot.calculate_pivots(770, 750, 760), evidence={},
        ) for source in (date(2026, 11, 11), date(2026, 11, 12), date(2026, 11, 13))]
        self.timestamps = [int(datetime.fromisoformat(
            snapshot["session_date"] + "T15:00:00+00:00").timestamp() * 1000)
            for snapshot in self.snapshots]
        with closing(sqlite3.connect(self.read_db)) as con:
            con.execute("""CREATE TABLE touch_events (
                ts_event INTEGER, event_id TEXT, symbol TEXT, data_quality REAL,
                confluence_count INTEGER, level_type TEXT, level_price REAL,
                touch_side INTEGER)""")
            con.commit()
        with closing(sqlite3.connect(self.state_db)) as con:
            intraday_alert._ensure_state_schema(con)
            con.execute("INSERT INTO alert_state VALUES (?,?,?)", ("SPY", 100, "old"))
            con.commit()
        self.post = mock.Mock(return_value=True)
        real_snapshot_loader = intraday_alert._snapshot_for_event

        def snapshot_loader(symbol, ts_event, art_dir):
            return real_snapshot_loader(symbol, ts_event, self.root)
        for patcher in (
            mock.patch.dict(os.environ, {intraday_alert.notify.WEBHOOK_ENV: "https://example.test/unused"}),
            mock.patch.object(intraday_alert, "_read_con", side_effect=lambda: sqlite3.connect(self.read_db)),
            mock.patch.object(intraday_alert, "_state_con", side_effect=lambda: sqlite3.connect(self.state_db)),
            mock.patch.object(intraday_alert, "_snapshot_for_event", side_effect=snapshot_loader),
            mock.patch.object(intraday_alert, "current_rates", return_value={}),
            mock.patch.object(intraday_alert.notify, "post", self.post),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        clock_patcher = mock.patch.object(intraday_alert, "datetime", wraps=datetime)
        clock = clock_patcher.start()
        self.addCleanup(clock_patcher.stop)
        clock.now.return_value = datetime(2026, 11, 17, 15, tzinfo=timezone.utc)

    def add_event(self, index, *, price=760):
        with closing(sqlite3.connect(self.read_db)) as con:
            con.execute("INSERT INTO touch_events VALUES (?,?,?,?,?,?,?,?)",
                        (self.timestamps[index], f"e{index}", "SPY", 1, 2, "PP", price, 1))
            con.commit()

    def publish(self, index):
        return self.contract.write_snapshot(self.snapshots[index], self.root)[0]

    def state(self):
        with closing(sqlite3.connect(self.state_db)) as con:
            return (con.execute("SELECT last_ts, last_event_id FROM alert_state").fetchone(),
                    con.execute("SELECT event_id, disposition FROM alert_event_dispositions ORDER BY ts_event").fetchall())

    def poll(self, *args):
        output = io.StringIO()
        with redirect_stdout(output):
            rc = intraday_alert.main(list(args))
        return rc, output.getvalue()

    def test_missing_then_published_delivers_once_without_terminal_disposition(self):
        self.add_event(0)
        rc, output = self.poll()
        self.assertEqual(rc, 3)
        self.assertEqual(self.state(), ((100, "old"), []))
        self.assertIn("snapshot unavailable", output)
        self.assertIn("event_id=e0", output)
        self.assertIn("session_date=2026-11-12", output)
        self.assertIn("age_seconds=432000", output)
        self.assertIn("pending retry", output)
        self.publish(0)
        self.assertEqual(self.poll()[0], 0)
        self.assertEqual(self.poll()[0], 0)
        self.assertEqual(self.post.call_count, 1)
        self.assertEqual(self.state(), ((self.timestamps[0], "e0"), [("e0", "delivered")]))

    def test_valid_prefix_missing_middle_and_later_stay_ordered_across_polls(self):
        for index in range(3):
            self.add_event(index)
        self.publish(0)
        self.publish(2)
        self.assertEqual(self.poll()[0], 3)
        self.assertEqual(self.post.call_count, 1)
        self.assertEqual(self.state(), ((self.timestamps[0], "e0"), [("e0", "delivered")]))
        # Each main call opens fresh connections: durable behavior across restart.
        for cap in (1, 2, 12):
            self.assertEqual(self.poll("--max-alerts", str(cap))[0], 3)
            self.assertEqual(self.post.call_count, 1)
            self.assertEqual(self.state()[0], (self.timestamps[0], "e0"))
        self.publish(1)
        self.assertEqual(self.poll("--max-alerts", "1")[0], 0)
        self.assertEqual(self.state()[0], (self.timestamps[1], "e1"))
        self.assertEqual(self.poll()[0], 0)
        self.assertEqual(self.post.call_count, 3)
        self.assertEqual(self.poll()[0], 0)
        self.assertEqual(self.post.call_count, 3)

    def test_unavailable_dry_run_never_binds_or_checkpoints_valid_prefix(self):
        self.add_event(0)
        self.add_event(1)
        self.publish(0)
        self.assertEqual(self.poll("--dry-run")[0], 3)
        self.assertEqual(self.state(), ((100, "old"), []))
        self.post.assert_not_called()

    def test_unavailable_does_not_override_notifier_failure_or_saved_binding(self):
        self.add_event(0)
        self.add_event(1)
        path = self.publish(0)
        self.post.side_effect = [False, True, True]
        self.assertEqual(self.poll()[0], 1)
        self.assertEqual(self.state(), ((100, "old"), [("e0", "pending")]))
        path.unlink()
        (self.root / "levels_snapshot_SPY_latest.json").unlink()
        self.assertEqual(self.poll()[0], 3)
        self.assertEqual(self.state()[0], (self.timestamps[0], "e0"))
        self.publish(1)
        self.assertEqual(self.poll()[0], 0)
        self.assertEqual(self.post.call_count, 3)

    def test_file_disappearing_between_exists_and_load_remains_retryable(self):
        self.add_event(0)
        path = self.publish(0)
        real_load = intraday_alert.load_snapshot

        def disappear(target):
            target.unlink()
            return real_load(target)

        with mock.patch.object(intraday_alert, "load_snapshot", side_effect=disappear):
            self.assertEqual(self.poll()[0], 3)
        self.assertFalse(path.exists())
        self.assertEqual(self.state(), ((100, "old"), []))
        self.publish(0)
        self.assertEqual(self.poll()[0], 0)
        self.assertEqual(self.post.call_count, 1)

    def test_actual_price_mismatch_is_quarantined_and_allows_later_progress(self):
        self.add_event(0, price=999)
        self.add_event(1)
        self.publish(0)
        self.publish(1)
        self.assertEqual(self.poll()[0], 3)
        self.assertEqual(self.state(), ((self.timestamps[0], "e0"), [("e0", "snapshot_mismatch")]))
        self.assertEqual(self.poll()[0], 0)
        self.assertEqual(self.post.call_count, 1)
        self.assertEqual(self.state()[0], (self.timestamps[1], "e1"))

    def test_actual_identity_mismatch_is_quarantined(self):
        self.add_event(0)
        # Valid snapshot for the wrong session in the requested session filename.
        path = self.root / "levels_snapshot_SPY_2026-11-12.json"
        path.write_text(json.dumps(self.snapshots[1]))
        self.assertEqual(self.poll()[0], 3)
        self.assertEqual(self.state(), ((self.timestamps[0], "e0"), [("e0", "snapshot_mismatch")]))
        self.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
