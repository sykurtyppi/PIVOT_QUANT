import shutil
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from services.external_data.t9_parquet_adapter import (
    load_historical_smoke_slice,
    validate_historical_smoke_contract,
    write_smoke_report,
)


def _duckdb_available() -> bool:
    return shutil.which("duckdb") is not None


@unittest.skipUnless(_duckdb_available(), "duckdb CLI is required for parquet fixture tests")
class TestT9ParquetAdapter(unittest.TestCase):
    def _write_parquet(self, path: Path, select_sql: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["duckdb", "-c", f"COPY ({select_sql}) TO '{path}' (FORMAT PARQUET)"],
            check=True,
            capture_output=True,
            text=True,
        )

    def _build_fake_t9(self, root: Path) -> None:
        daily_path = (
            root
            / "market_data"
            / "normalized"
            / "underlyings"
            / "daily_ohlcv"
            / "underlying_symbol=SPY"
            / "year=2024"
            / "month=01"
            / "spy_daily_ohlcv_yahoo_2024-01.parquet"
        )
        option_path = (
            root
            / "market_data"
            / "research"
            / "options_features_eod"
            / "underlying_symbol=SPY"
            / "year=2024"
            / "month=01"
            / "spy_options_features_eod_2024-01.parquet"
        )
        daily_ivol_path = (
            root
            / "market_data"
            / "normalized"
            / "underlyings"
            / "daily_ohlcv"
            / "underlying_symbol=SPY"
            / "year=2024"
            / "month=01"
            / "spy_underlyings_eod_2024-01.parquet"
        )
        self._write_parquet(
            daily_path,
            """
            SELECT
              DATE '2024-01-02' AS trade_date,
              'SPY' AS underlying_symbol,
              4700000::BIGINT AS open_10000,
              4720000::BIGINT AS high_10000,
              4690000::BIGINT AS low_10000,
              4710000::BIGINT AS close_10000,
              1000::BIGINT AS volume,
              'yahoo' AS vendor
            UNION ALL
            SELECT DATE '2024-01-08', 'SPY', 4800000, 4810000, 4790000, 4805000, 2000, 'yahoo'
            """,
        )
        self._write_parquet(
            daily_ivol_path,
            """
            SELECT
              DATE '2024-01-02' AS trade_date,
              'SPY' AS underlying_symbol,
              4701000::BIGINT AS open_10000,
              4721000::BIGINT AS high_10000,
              4691000::BIGINT AS low_10000,
              4711000::BIGINT AS close_10000,
              1100::BIGINT AS volume,
              'ivolatility' AS vendor
            UNION ALL
            SELECT DATE '2024-01-08', 'SPY', 4801000, 4811000, 4791000, 4806000, 2100, 'ivolatility'
            """,
        )
        self._write_parquet(
            option_path,
            """
            SELECT
              DATE '2024-01-02' AS trade_date,
              DATE '2024-01-19' AS expiry,
              'SPY' AS underlying_symbol,
              'C' AS call_put,
              471.0 AS strike,
              1.10 AS bid,
              1.20 AS ask,
              1.15 AS mid,
              42::BIGINT AS volume,
              100::BIGINT AS open_interest,
              0.22 AS iv
            UNION ALL
            SELECT DATE '2024-01-08', DATE '2024-01-19', 'SPY', 'P', 470.0, 1.30, 1.40, 1.35, 84, 200, 0.25
            """,
        )

    def test_load_historical_smoke_slice_normalizes_bounded_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._build_fake_t9(root)

            report = load_historical_smoke_slice(
                root=root,
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-05",
                max_files=5,
                daily_source="yahoo",
            )

        daily = report["sections"]["daily_ohlcv"]
        options = report["sections"]["option_features"]
        contract = report["sections"]["historical_contract"]
        self.assertEqual(daily["row_count"], 1)
        self.assertEqual(options["row_count"], 1)
        self.assertEqual(contract["status"], "pass")
        self.assertEqual(daily["metadata"]["selected_source"], "yahoo")
        self.assertEqual(daily["metadata"]["duplicate_date_count"], 1)
        self.assertEqual(daily["sample_rows"][0]["date"], "2024-01-02")
        self.assertAlmostEqual(daily["sample_rows"][0]["open"], 470.0)
        self.assertEqual(options["sample_rows"][0]["option_type"], "call")
        self.assertAlmostEqual(options["sample_rows"][0]["implied_volatility"], 0.22)

    def test_load_historical_smoke_slice_can_select_ivolatility_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._build_fake_t9(root)

            report = load_historical_smoke_slice(
                root=root,
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-05",
                max_files=5,
                daily_source="ivolatility",
            )

        daily = report["sections"]["daily_ohlcv"]
        self.assertEqual(daily["metadata"]["selected_source"], "ivolatility")
        self.assertAlmostEqual(daily["sample_rows"][0]["open"], 470.1)

    def test_historical_contract_flags_unaligned_and_future_like_columns(self):
        daily = pd.DataFrame(
            [
                {
                    "date": "2024-01-02",
                    "open": 470.0,
                    "high": 472.0,
                    "low": 469.0,
                    "close": 471.0,
                    "volume": 1000,
                    "source": "yahoo",
                    "future_return": 0.01,
                }
            ]
        )
        options = pd.DataFrame(
            [
                {
                    "date": "2024-01-03",
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 471.0,
                    "option_type": "call",
                    "bid": 1.1,
                    "ask": 1.2,
                    "mid": 1.15,
                    "volume": 42,
                    "open_interest": 100,
                    "implied_volatility": 0.22,
                }
            ]
        )

        report = validate_historical_smoke_contract(
            daily,
            options,
            start=pd.Timestamp("2024-01-02").date(),
            end=pd.Timestamp("2024-01-05").date(),
        )

        self.assertEqual(report["status"], "fail")
        failed = {check["name"] for check in report["checks"] if check["status"] == "fail"}
        self.assertIn("daily_option_date_alignment", failed)
        self.assertIn("no_future_or_label_like_feature_columns", failed)

    def test_missing_root_is_graceful(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = load_historical_smoke_slice(
                root=Path(tmp) / "missing",
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-05",
            )

        self.assertFalse(report["root_exists"])
        self.assertIn("T9 root does not exist", report["warnings"][0])

    def test_write_smoke_report_stays_under_repo_reports_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._build_fake_t9(root)
            report = load_historical_smoke_slice(
                root=root,
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-05",
            )
            original_cwd = Path.cwd()
            with tempfile.TemporaryDirectory() as repo_tmp:
                os.chdir(repo_tmp)
                try:
                    path = write_smoke_report(report)
                finally:
                    os.chdir(original_cwd)

        self.assertTrue(path.name.endswith(".json"))
        self.assertIn("reports/historical_smoke", path.as_posix())
