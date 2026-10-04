from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import backfill_events  # noqa: E402


class TestHistoricalAccuracyPointInTime(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            """
            CREATE TABLE touch_events (
                event_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                level_type TEXT NOT NULL,
                ts_event INTEGER NOT NULL
            );
            CREATE TABLE event_labels (
                event_id TEXT NOT NULL,
                horizon_min INTEGER NOT NULL,
                reject INTEGER,
                break INTEGER,
                coverage_status TEXT
            );
            """
        )

    def tearDown(self) -> None:
        self.conn.close()

    def _insert(
        self,
        event_id: str,
        *,
        ts_event: int,
        reject: int,
        brk: int,
        coverage_status: str = "qualified",
        horizon: int = 15,
    ) -> None:
        self.conn.execute(
            "INSERT INTO touch_events(event_id, symbol, level_type, ts_event) VALUES (?, 'SPY', 'R1', ?)",
            (event_id, ts_event),
        )
        self.conn.execute(
            """INSERT INTO event_labels(
                   event_id, horizon_min, reject, break, coverage_status
               ) VALUES (?, ?, ?, ?, ?)""",
            (event_id, horizon, reject, brk, coverage_status),
        )

    def test_excludes_labels_that_had_not_matured_at_feature_time(self) -> None:
        before_ts = 2_000_000
        self._insert("mature", ts_event=before_ts - 20 * 60_000, reject=1, brk=0)
        self._insert("future_label", ts_event=before_ts - 10 * 60_000, reject=0, brk=1)
        self.conn.commit()

        reject_rate, break_rate, sample_size = backfill_events.compute_historical_accuracy(
            self.conn, "SPY", "R1", before_ts, horizon=15
        )

        self.assertEqual(sample_size, 1)
        self.assertEqual(reject_rate, 1.0)
        self.assertEqual(break_rate, 0.0)

    def test_uses_only_qualified_labels(self) -> None:
        before_ts = 2_000_000
        self._insert("qualified", ts_event=before_ts - 30 * 60_000, reject=1, brk=0)
        self._insert(
            "incomplete",
            ts_event=before_ts - 30 * 60_000,
            reject=0,
            brk=1,
            coverage_status="incomplete",
        )
        self.conn.commit()

        reject_rate, break_rate, sample_size = backfill_events.compute_historical_accuracy(
            self.conn, "SPY", "R1", before_ts, horizon=15
        )

        self.assertEqual(sample_size, 1)
        self.assertEqual(reject_rate, 1.0)
        self.assertEqual(break_rate, 0.0)

    def test_legacy_schema_without_coverage_status_fails_closed(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.executescript(
                """
                CREATE TABLE touch_events (
                    event_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    level_type TEXT NOT NULL,
                    ts_event INTEGER NOT NULL
                );
                CREATE TABLE event_labels (
                    event_id TEXT NOT NULL,
                    horizon_min INTEGER NOT NULL,
                    reject INTEGER,
                    break INTEGER
                );
                INSERT INTO touch_events VALUES ('legacy', 'SPY', 'R1', 1000);
                INSERT INTO event_labels VALUES ('legacy', 15, 1, 0);
                """
            )

            result = backfill_events.compute_historical_accuracy(
                conn, "SPY", "R1", 2_000_000, horizon=15
            )
        finally:
            conn.close()

        self.assertEqual(result, (None, None, 0))


if __name__ == "__main__":
    unittest.main()
