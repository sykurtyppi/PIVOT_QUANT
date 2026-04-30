import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from services.external_data.historical_walk_forward import (
    build_historical_walk_forward_from_t9,
    build_historical_walk_forward_report,
)


def _duckdb_available() -> bool:
    return shutil.which("duckdb") is not None


@unittest.skipUnless(_duckdb_available(), "duckdb CLI is required for parquet fixture tests")
class TestHistoricalWalkForwardFromT9(unittest.TestCase):
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
        self._write_parquet(
            daily_path,
            """
            SELECT * FROM (
              VALUES
                (DATE '2024-01-02', 'SPY', 4700000::BIGINT, 4720000::BIGINT, 4690000::BIGINT, 4710000::BIGINT, 1000::BIGINT, 'yahoo'),
                (DATE '2024-01-03', 'SPY', 4710000::BIGINT, 4730000::BIGINT, 4700000::BIGINT, 4720000::BIGINT, 1200::BIGINT, 'yahoo'),
                (DATE '2024-01-04', 'SPY', 4720000::BIGINT, 4740000::BIGINT, 4710000::BIGINT, 4730000::BIGINT, 1300::BIGINT, 'yahoo'),
                (DATE '2024-01-05', 'SPY', 4730000::BIGINT, 4750000::BIGINT, 4720000::BIGINT, 4740000::BIGINT, 1400::BIGINT, 'yahoo'),
                (DATE '2024-01-08', 'SPY', 4740000::BIGINT, 4760000::BIGINT, 4730000::BIGINT, 4750000::BIGINT, 1500::BIGINT, 'yahoo')
            ) AS t(trade_date, underlying_symbol, open_10000, high_10000, low_10000, close_10000, volume, vendor)
            """,
        )
        self._write_parquet(
            option_path,
            """
            SELECT * FROM (
              VALUES
                (DATE '2024-01-02', DATE '2024-01-19', 'SPY', 'C', 471.0, 1.10, 1.20, 1.15, 42::BIGINT, 100::BIGINT, 0.22),
                (DATE '2024-01-03', DATE '2024-01-19', 'SPY', 'P', 472.0, 1.30, 1.40, 1.35, 84::BIGINT, 200::BIGINT, 0.25),
                (DATE '2024-01-04', DATE '2024-01-19', 'SPY', 'C', 473.0, 1.50, 1.60, 1.55, 21::BIGINT, 80::BIGINT, 0.21),
                (DATE '2024-01-05', DATE '2024-01-19', 'SPY', 'P', 474.0, 1.70, 1.80, 1.75, 31::BIGINT, 90::BIGINT, 0.23)
            ) AS t(trade_date, expiry, underlying_symbol, call_put, strike, bid, ask, mid, volume, open_interest, iv)
            """,
        )

    def test_builds_windows_from_temp_parquet_without_training(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._build_fake_t9(root)
            wf = build_historical_walk_forward_from_t9(
                root=root,
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-10",
                max_files=5,
                daily_source="yahoo",
                horizons=["1d"],
                train_window=2,
                test_window=2,
                step=2,
            )

        self.assertEqual(wf.report["status"], "pass")
        self.assertFalse(wf.report["training_performed"])
        self.assertEqual(wf.report["window_count"], 2)
        self.assertEqual(wf.report["leakage_checks"]["status"], "pass")
        self.assertEqual(wf.report["windows"][0]["train_trading_days"], 2)
        self.assertEqual(wf.report["windows"][0]["test_trading_days"], 2)


class TestHistoricalWalkForwardReport(unittest.TestCase):
    def _daily(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"date": "2024-01-02"},
                {"date": "2024-01-03"},
                {"date": "2024-01-04"},
                {"date": "2024-01-05"},
                {"date": "2024-01-08"},
                {"date": "2024-01-09"},
            ]
        )

    def _labels(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"observation_date": "2024-01-02", "label_date": "2024-01-03", "horizon": "1d", "forward_return": 0.01},
                {"observation_date": "2024-01-03", "label_date": "2024-01-04", "horizon": "1d", "forward_return": -0.01},
                {"observation_date": "2024-01-08", "label_date": "2024-01-09", "horizon": "1d", "forward_return": 0.02},
            ]
        )

    def test_includes_zero_row_windows_and_counts(self):
        wf = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily(),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-09",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
        )

        self.assertEqual(wf.report["status"], "pass")
        self.assertEqual(wf.report["window_count"], 2)
        self.assertEqual(wf.report["zero_row_window_count"], 1)
        self.assertEqual(wf.report["windows"][0]["test_row_count"], 0)
        self.assertEqual(wf.report["windows"][1]["test_row_count"], 1)
        self.assertEqual(wf.report["leakage_checks"]["status"], "pass")

    def test_leakage_check_fails_when_label_date_is_not_future(self):
        labels = self._labels()
        labels.loc[0, "label_date"] = "2024-01-02"
        wf = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily(),
            label_candidates=labels,
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-09",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
        )

        self.assertEqual(wf.report["status"], "fail")
        self.assertEqual(wf.report["leakage_checks"]["status"], "fail")

    def test_rejects_non_positive_window_config(self):
        with self.assertRaises(ValueError):
            build_historical_walk_forward_report(
                model_ready_daily_features=self._daily(),
                label_candidates=self._labels(),
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-09",
                horizons=["1d"],
                train_window=0,
                test_window=2,
                step=2,
            )
