import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from services.external_data.historical_feature_contract import (
    build_historical_feature_contract,
    build_historical_feature_contract_from_t9,
)


def _duckdb_available() -> bool:
    return shutil.which("duckdb") is not None


@unittest.skipUnless(_duckdb_available(), "duckdb CLI is required for parquet fixture tests")
class TestHistoricalFeatureContractFromT9(unittest.TestCase):
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
            SELECT DATE '2024-01-03', 'SPY', 4710000, 4730000, 4700000, 4720000, 1200, 'yahoo'
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
            SELECT DATE '2024-01-03', DATE '2024-01-19', 'SPY', 'P', 472.0, 1.30, 1.40, 1.35, 84, 200, 0.25
            """,
        )

    def test_builds_feature_and_label_ready_frames_from_temp_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._build_fake_t9(root)

            contract = build_historical_feature_contract_from_t9(
                root=root,
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-05",
                max_files=5,
                daily_source="yahoo",
            )

        self.assertEqual(contract.report["status"], "pass")
        self.assertEqual(len(contract.model_ready_daily_features), 2)
        self.assertEqual(len(contract.option_context_features), 2)
        self.assertEqual(len(contract.label_ready_rows), 2)
        self.assertIn("return_1d", contract.model_ready_daily_features.columns)
        self.assertIn("moneyness", contract.option_context_features.columns)
        self.assertEqual(
            contract.label_ready_rows["label_status"].unique().tolist(),
            ["ready_for_future_outcome_generation"],
        )


class TestHistoricalFeatureContract(unittest.TestCase):
    def _daily(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "date": "2024-01-02",
                    "open": 470.0,
                    "high": 472.0,
                    "low": 469.0,
                    "close": 471.0,
                    "volume": 1000,
                    "source": "yahoo",
                },
                {
                    "date": "2024-01-03",
                    "open": 471.0,
                    "high": 473.0,
                    "low": 470.0,
                    "close": 472.0,
                    "volume": 1200,
                    "source": "yahoo",
                },
            ]
        )

    def _options(self) -> pd.DataFrame:
        return pd.DataFrame(
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
                }
            ]
        )

    def test_direct_contract_reports_missing_values_without_failing(self):
        daily = self._daily()
        daily.loc[1, "volume"] = pd.NA

        contract = build_historical_feature_contract(
            daily,
            self._options(),
            symbol="SPY",
            start=pd.Timestamp("2024-01-02").date(),
            end=pd.Timestamp("2024-01-05").date(),
            daily_source="yahoo",
        )

        self.assertEqual(contract.report["status"], "pass")
        self.assertEqual(
            contract.report["missing_values"]["model_ready_daily_features"]["return_1d"],
            1,
        )
        self.assertEqual(
            contract.report["missing_values"]["model_ready_daily_features"]["volume"],
            1,
        )

    def test_direct_contract_fails_on_unaligned_future_like_or_unbounded_rows(self):
        daily = self._daily()
        daily["future_return"] = [0.1, 0.2]
        options = self._options()
        options.loc[0, "date"] = "2024-01-08"

        contract = build_historical_feature_contract(
            daily,
            options,
            symbol="SPY",
            start=pd.Timestamp("2024-01-02").date(),
            end=pd.Timestamp("2024-01-05").date(),
            daily_source="yahoo",
        )

        self.assertEqual(contract.report["status"], "fail")
        failed = {check["name"] for check in contract.report["checks"] if check["status"] == "fail"}
        self.assertIn("date_range_bounded", failed)
        self.assertIn("daily_option_date_alignment", failed)
        self.assertIn("no_future_or_label_like_inputs", failed)

    def test_duplicate_daily_dates_are_collapsed(self):
        daily = pd.concat([self._daily(), self._daily().head(1)], ignore_index=True)

        contract = build_historical_feature_contract(
            daily,
            self._options(),
            symbol="SPY",
            start=pd.Timestamp("2024-01-02").date(),
            end=pd.Timestamp("2024-01-05").date(),
            daily_source="yahoo",
        )

        self.assertEqual(contract.report["status"], "pass")
        self.assertEqual(len(contract.model_ready_daily_features), 2)
        self.assertIn("duplicate canonical daily dates detected", " ".join(contract.report["warnings"]))
