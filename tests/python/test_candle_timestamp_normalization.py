from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import backfill_events  # noqa: E402


class TestCandleTimestampNormalization(unittest.TestCase):
    def test_floors_and_deduplicates_provisional_bar(self) -> None:
        candles = [
            {"time": 1_800, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0},
            {"time": 1_849, "open": 100.0, "high": 102.0, "low": 98.0, "close": 101.0, "volume": 20.0},
            {"time": 1_860, "open": 101.0, "high": 103.0, "low": 100.0, "close": 102.0, "volume": 30.0},
        ]

        normalized = backfill_events.normalize_candles_to_interval(candles, 60)

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

        normalized = backfill_events.normalize_candles_to_interval(candles, 60)

        self.assertEqual(
            [(bar["time"], bar["close"]) for bar in normalized],
            [(3_600, 100.0), (3_660, 101.0), (3_720, 102.0)],
        )

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
