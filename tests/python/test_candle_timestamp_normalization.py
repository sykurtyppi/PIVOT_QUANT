from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from unittest.mock import patch

ET = ZoneInfo("America/New_York")

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import backfill_events  # noqa: E402


class TestCandleTimestampNormalization(unittest.TestCase):
    def test_session_hourly_parse_persist_and_rth_aggregate_across_dst(self) -> None:
        # Both sides of the US spring and autumn DST transitions, all weekdays.
        for day in ("2026-03-06", "2026-03-09", "2026-10-30", "2026-11-02"):
            with self.subTest(day=day), tempfile.TemporaryDirectory(prefix="candle-contract-") as tmp:
                starts = [int(datetime.fromisoformat(f"{day}T{hour}:30:00").replace(tzinfo=ET).timestamp())
                          for hour in ("09", "10")]
                raw = [dict(time=starts[0], open=100, high=150, low=50, close=101, volume=10),
                       dict(time=starts[1], open=110, high=112, low=109, close=111, volume=20)]
                candles = backfill_events.normalize_candles_to_interval(
                    backfill_events.parse_candles({"candles": raw}), 3600)
                with sqlite3.connect(Path(tmp) / "bars.sqlite") as conn:
                    conn.execute("CREATE TABLE bar_data (symbol TEXT, ts INTEGER, open REAL, "
                                 "high REAL, low REAL, close REAL, volume REAL, "
                                 "bar_interval_sec INTEGER, PRIMARY KEY (symbol, ts, bar_interval_sec))")
                    backfill_events.insert_bars(conn, "SPY", candles, 3600)
                    rows = conn.execute(
                        "SELECT ts, open, high, low, close, volume, bar_interval_sec "
                        "FROM bar_data ORDER BY ts").fetchall()
                self.assertEqual([row[0] for row in rows], [ts * 1000 for ts in starts])
                self.assertEqual([row[6] for row in rows], [3600, 3600])
                # Aggregate read-back data, not a second hand-built approximation.
                stored = [dict(time=r[0] // 1000, open=r[1], high=r[2], low=r[3],
                               close=r[4], volume=r[5]) for r in rows]
                sessions = backfill_events.build_daily_bars(stored)
                self.assertEqual(len(sessions), 1)
                session = sessions[0]
                self.assertEqual(len(session["bars"]), 2)
                self.assertEqual((session["open"], session["high"], session["low"], session["close"]),
                                 (100, 150, 50, 111))

    def test_floors_and_deduplicates_provisional_bar(self) -> None:
        candles = [
            {"time": 1_800, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0},
            {"time": 1_849, "open": 100.0, "high": 102.0, "low": 98.0, "close": 101.0, "volume": 20.0},
            {"time": 1_860, "open": 101.0, "high": 103.0, "low": 100.0, "close": 102.0, "volume": 30.0},
        ]

        normalized = backfill_events.normalize_candles_to_interval(
            candles, 60, snap_provisional_minute=True)

        self.assertEqual([bar["time"] for bar in normalized], [1_800, 1_860])
        self.assertEqual(normalized[0]["close"], 101.0)
        self.assertEqual(normalized[0]["volume"], 20.0)
        self.assertEqual(candles[1]["time"], 1_849)

    def test_sorts_and_preserves_distinct_buckets(self) -> None:
        candles = [
            {"time": 3_721, "close": 102.0},
            {"time": 3_600, "close": 100.0},
            {"time": 3_661, "close": 101.0},
        ]

        normalized = backfill_events.normalize_candles_to_interval(
            candles, 60, snap_provisional_minute=True)

        self.assertEqual(
            [(bar["time"], bar["close"]) for bar in normalized],
            [(3_600, 100.0), (3_660, 101.0), (3_720, 102.0)],
        )

    def test_preserves_provider_origin_and_exact_duplicate_order(self) -> None:
        # No origin inference from the first row, no universal 09:30 offset.
        for seconds in (60, 300, 900, 3600, 5400, 86400, 604800):
            for origin in (1800, 0, 17):
                with self.subTest(seconds=seconds, origin=origin):
                    raw = [{"time": origin + 2 * seconds, "close": 3},
                           {"time": origin, "close": 1},
                           {"time": origin + seconds, "close": 2},
                           {"time": origin, "close": 9}]
                    original = [dict(bar) for bar in raw]
                    normalized = backfill_events.normalize_candles_to_interval(raw, seconds)
                    self.assertEqual(normalized, [raw[3], raw[2], raw[0]])
                    self.assertEqual(raw, original)
                    self.assertIsNot(normalized[0], raw[3])

    def test_minute_opt_in_never_floors_coarse_intervals(self) -> None:
        for seconds in (300, 900, 3600, 5400, 86400, 604800):
            with self.subTest(seconds=seconds):
                raw = [{"time": 1801, "close": 1}, {"time": 1801 + seconds, "close": 2}]
                self.assertEqual(backfill_events.normalize_candles_to_interval(
                    raw, seconds, snap_provisional_minute=True), raw)

    def test_rejects_nonpositive_intervals(self) -> None:
        for seconds in (0, -60):
            with self.subTest(seconds=seconds), self.assertRaises(ValueError):
                backfill_events.normalize_candles_to_interval([], seconds)

    def test_parse_keeps_latest_provider_time_update_within_minute(self) -> None:
        raw = [dict(time=1849, open=100, high=102, low=98, close=101),
               dict(time=1800, open=100, high=101, low=99, close=100),
               dict(time=1849, open=100, high=103, low=98, close=102)]
        normalized = backfill_events.normalize_candles_to_interval(
            backfill_events.parse_candles({"candles": raw}), 60, snap_provisional_minute=True)
        self.assertEqual([(b["time"], b["close"]) for b in normalized], [(1800, 102)])

    def test_both_callers_apply_only_resolved_yahoo_one_minute_policy(self) -> None:
        server_dir = ROOT / "server"
        if str(server_dir) not in sys.path:
            sys.path.insert(0, str(server_dir))
        import live_event_collector as collector

        # auto resolving to IBKR must not inherit the requested Yahoo policy.
        for source in ("Yahoo", "IBKR", "marketdata.app", "unknown"):
            for interval, seconds in (("1m", 60), ("5m", 300), ("60m", 3600), ("90m", 5400)):
                with self.subTest(source=source, interval=interval):
                    start = int(datetime(2026, 11, 13, 9, 30, tzinfo=ET).timestamp())
                    raw = [dict(time=start, open=100, high=150, low=50, close=101, volume=10),
                           dict(time=start + 49, open=110, high=112, low=109, close=111, volume=20)]
                    expected = [start] if source == "Yahoo" and interval == "1m" else [start, start + 49]
                    with tempfile.TemporaryDirectory(prefix="candle-caller-") as tmp:
                        db = Path(tmp) / "bars.sqlite"
                        with patch.object(backfill_events, "fetch_market", return_value=({"candles": raw}, source)), \
                             patch.object(backfill_events, "fetch_gamma_context", return_value={}), \
                             patch.object(backfill_events, "build_events", return_value=[]), \
                             patch.object(backfill_events, "run_build_labels"), \
                             patch.object(sys, "argv", ["backfill_events.py", "--db", str(db), "--interval", interval,
                                                       "--source", "auto"]):
                            backfill_events.main()
                        with sqlite3.connect(db) as conn:
                            rows = conn.execute("SELECT ts, bar_interval_sec FROM bar_data ORDER BY ts").fetchall()
                        self.assertEqual(rows, [(ts * 1000, seconds) for ts in expected])
                        # Real parser/persistence; no gamma, scoring, services or network.
                        with sqlite3.connect(db) as conn, \
                             patch.object(collector, "fetch_market", return_value=({"candles": raw}, source)), \
                             patch.object(collector, "INTERVAL", interval), \
                             patch.object(collector, "INTERVAL_SEC", seconds), \
                             patch.object(collector, "SOURCE", "auto"), \
                             patch.object(collector, "WRITE_BARS", True):
                            conn.execute("DELETE FROM bar_data")
                            collector._collect_symbol(conn, "SPY")
                            rows = conn.execute("SELECT ts, bar_interval_sec FROM bar_data ORDER BY ts").fetchall()
                        self.assertEqual(rows, [(ts * 1000, seconds) for ts in expected])

    def test_live_collector_normalizes_before_persisting(self) -> None:
        server_dir = ROOT / "server"
        if str(server_dir) not in sys.path:
            sys.path.insert(0, str(server_dir))
        import live_event_collector as collector

        raw = [
            {"time": 1_800, "close": 100.0},
            {"time": 1_849, "close": 101.0},
        ]
        persisted: list[dict] = []
        originals = {
            "fetch_market": collector.fetch_market,
            "parse_candles": collector.parse_candles,
            "insert_bars": collector.insert_bars,
            "build_daily_bars": collector.build_daily_bars,
            "WRITE_BARS": collector.WRITE_BARS,
        }
        try:
            collector.fetch_market = lambda *args: ({}, "yahoo")
            collector.parse_candles = lambda payload: raw
            collector.insert_bars = lambda conn, symbol, candles, interval: persisted.extend(candles) or len(candles)
            collector.build_daily_bars = lambda candles: []
            collector.WRITE_BARS = True

            collector._collect_symbol(None, "SPY")
        finally:
            for name, value in originals.items():
                setattr(collector, name, value)

        self.assertEqual([bar["time"] for bar in persisted], [1_800])
        self.assertEqual(persisted[0]["close"], 101.0)


if __name__ == "__main__":
    unittest.main()
