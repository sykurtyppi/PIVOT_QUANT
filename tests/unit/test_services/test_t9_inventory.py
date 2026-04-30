import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.external_data.t9_inventory import build_t9_inventory, resolve_t9_root


class TestT9Inventory(unittest.TestCase):
    def test_missing_root_reports_warning_without_sections(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "missing_t9"
            report = build_t9_inventory(root=missing, symbol="SPY")

        self.assertFalse(report["root_exists"])
        self.assertTrue(report["read_only"])
        self.assertIn("T9 root does not exist", report["warnings"][0])
        self.assertEqual(report["sections"], {})

    def test_resolve_root_prefers_env(self):
        with patch.dict(os.environ, {"PIVOTQUANT_T9_ROOT": "/tmp/example_t9"}):
            self.assertEqual(resolve_t9_root(), Path("/tmp/example_t9"))

    def test_inventory_discovers_fake_sources_and_sqlite_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            symbol = "SPY"

            daily_dir = (
                root
                / "market_data"
                / "normalized"
                / "underlyings"
                / "daily_ohlcv"
                / f"underlying_symbol={symbol}"
                / "year=2025"
                / "month=01"
            )
            chain_dir = (
                root
                / "market_data"
                / "normalized"
                / "options"
                / "chains_eod"
                / f"underlying_symbol={symbol}"
                / "year=2025"
                / "month=01"
            )
            feature_dir = (
                root
                / "market_data"
                / "research"
                / "options_features_eod"
                / f"underlying_symbol={symbol}"
                / "year=2025"
                / "month=01"
            )
            raw_dir = (
                root
                / "market_data"
                / "raw"
                / "ivolatility"
                / "intraday_stock_prices"
                / "2025"
            )
            for directory in (daily_dir, chain_dir, feature_dir, raw_dir):
                directory.mkdir(parents=True)

            (daily_dir / "spy_daily_ohlcv_yahoo_2025-01.parquet").write_bytes(b"fake")
            (chain_dir / "spy_options_eod_2025-01.parquet").write_bytes(b"fake")
            (feature_dir / "spy_options_features_eod_2025-01.parquet").write_bytes(b"fake")
            (raw_dir / "ivol_spy_minute_1_2025-01-02.json").write_text(
                json.dumps([{"symbol": "SPY", "ts": 123, "close": 100.0}]),
                encoding="utf-8",
            )

            sqlite_path = root / "pivotquant" / "PIVOT_QUANT" / "data" / "pivot_events.sqlite"
            sqlite_path.parent.mkdir(parents=True)
            conn = sqlite3.connect(sqlite_path)
            try:
                conn.execute("CREATE TABLE bar_data (symbol TEXT, ts INTEGER, close REAL)")
                conn.commit()
            finally:
                conn.close()

            report = build_t9_inventory(root=root, symbol=symbol, max_files=10)

        self.assertTrue(report["root_exists"])
        sections = report["sections"]
        self.assertEqual(sections["daily_ohlcv_parquet"]["file_count"], 1)
        self.assertEqual(sections["option_chain_parquet"]["file_count"], 1)
        self.assertEqual(sections["option_feature_parquet"]["file_count"], 1)
        self.assertEqual(sections["raw_intraday_json"]["file_count"], 1)
        self.assertEqual(sections["sqlite_candidates"]["file_count"], 1)
        self.assertEqual(
            sections["raw_intraday_json"]["sample_schema"][0],
            {"name": "symbol", "type": "str"},
        )
        self.assertEqual(sections["sqlite_candidates"]["sample_schema"][0]["name"], "bar_data")

    def test_max_files_caps_category_file_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            daily_dir = (
                root
                / "market_data"
                / "normalized"
                / "underlyings"
                / "daily_ohlcv"
                / "underlying_symbol=SPY"
                / "year=2025"
                / "month=01"
            )
            daily_dir.mkdir(parents=True)
            for idx in range(5):
                (daily_dir / f"spy_daily_ohlcv_yahoo_2025-01-{idx}.parquet").write_bytes(b"fake")

            report = build_t9_inventory(root=root, symbol="SPY", max_files=2)

        self.assertEqual(report["sections"]["daily_ohlcv_parquet"]["file_count"], 2)
