import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from services.external_data.historical_baseline_report import (
    build_historical_baseline_report,
    build_historical_baseline_report_from_t9,
)


def _duckdb_available() -> bool:
    return shutil.which("duckdb") is not None


@unittest.skipUnless(_duckdb_available(), "duckdb CLI is required for parquet fixture tests")
class TestHistoricalBaselineReportFromT9(unittest.TestCase):
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
            SELECT DATE '2024-01-02' AS trade_date, 'SPY' AS underlying_symbol,
                   4700000::BIGINT AS open_10000, 4720000::BIGINT AS high_10000,
                   4690000::BIGINT AS low_10000, 4710000::BIGINT AS close_10000,
                   1000::BIGINT AS volume, 'yahoo' AS vendor
            UNION ALL
            SELECT DATE '2024-01-03', 'SPY', 4710000, 4730000, 4700000, 4720000, 1200, 'yahoo'
            UNION ALL
            SELECT DATE '2024-01-04', 'SPY', 4720000, 4740000, 4710000, 4730000, 1300, 'yahoo'
            """,
        )
        self._write_parquet(
            option_path,
            """
            SELECT DATE '2024-01-02' AS trade_date, DATE '2024-01-19' AS expiry,
                   'SPY' AS underlying_symbol, 'C' AS call_put, 471.0 AS strike,
                   1.10 AS bid, 1.20 AS ask, 1.15 AS mid,
                   42::BIGINT AS volume, 100::BIGINT AS open_interest, 0.22 AS iv
            UNION ALL
            SELECT DATE '2024-01-03', DATE '2024-01-19', 'SPY', 'P', 472.0,
                   1.30, 1.40, 1.35, 84, 200, 0.25
            """,
        )

    def test_builds_baseline_report_from_temp_parquet_without_training(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._build_fake_t9(root)

            baseline = build_historical_baseline_report_from_t9(
                root=root,
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-05",
                max_files=5,
                daily_source="yahoo",
                horizons=["1d"],
            )

        self.assertEqual(baseline.report["status"], "pass")
        self.assertFalse(baseline.report["training_performed"])
        self.assertEqual(baseline.report["mature_label_counts_by_horizon"], {"1d": 2})
        self.assertIn("call", baseline.report["forward_return_distribution_by_option_type"])
        self.assertIn("atm", baseline.report["forward_return_distribution_by_moneyness_bucket"])


class TestHistoricalBaselineReport(unittest.TestCase):
    def test_direct_report_groups_horizon_option_type_and_moneyness(self):
        daily = pd.DataFrame(
            [
                {"date": "2024-01-02", "close": 471.0},
                {"date": "2024-01-03", "close": 472.0},
            ]
        )
        options = pd.DataFrame(
            [
                {
                    "date": "2024-01-02",
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
                    "underlying_close": 471.0,
                    "days_to_expiration": 17,
                    "moneyness": 0.0,
                    "spread": 0.1,
                    "relative_spread": 0.087,
                },
                {
                    "date": "2024-01-03",
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 472.0,
                    "option_type": "put",
                    "bid": 1.3,
                    "ask": 1.4,
                    "mid": 1.35,
                    "volume": 84,
                    "open_interest": 200,
                    "implied_volatility": 0.25,
                    "underlying_close": 472.0,
                    "days_to_expiration": 16,
                    "moneyness": 0.0,
                    "spread": 0.1,
                    "relative_spread": 0.074,
                },
            ]
        )
        labels = pd.DataFrame(
            [
                {
                    "observation_date": "2024-01-02",
                    "label_date": "2024-01-03",
                    "horizon": "1d",
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 471.0,
                    "option_type": "call",
                    "days_to_expiration": 17,
                    "underlying_close": 471.0,
                    "future_underlying_close": 472.0,
                    "forward_return": 0.002123,
                    "label_status": "mature",
                }
            ]
        )

        baseline = build_historical_baseline_report(
            model_ready_daily_features=daily,
            option_context_features=options,
            label_candidates=labels,
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-05",
            horizons=["1d"],
        )

        self.assertEqual(baseline.report["rows"]["joined_baseline_rows"], 1)
        self.assertEqual(baseline.report["mature_label_counts_by_horizon"], {"1d": 1})
        self.assertEqual(baseline.report["row_counts_by_horizon_option_type"]["1d"]["call"], 1)
        self.assertEqual(baseline.report["row_counts_by_horizon_moneyness_bucket"]["1d"]["atm"], 1)
        self.assertEqual(baseline.report["date_coverage"]["label_candidates_label"]["max"], "2024-01-03")

    def test_empty_labels_warns_without_failing_command_layer(self):
        baseline = build_historical_baseline_report(
            model_ready_daily_features=pd.DataFrame(columns=["date"]),
            option_context_features=pd.DataFrame(),
            label_candidates=pd.DataFrame(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-05",
            horizons=["1d"],
        )

        self.assertEqual(baseline.report["status"], "warn")
        self.assertIn("no mature label candidates", " ".join(baseline.report["warnings"]))
