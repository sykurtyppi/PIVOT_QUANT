import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from services.external_data.historical_label_contract import (
    build_historical_label_contract,
    build_historical_label_contract_from_t9,
    parse_horizons,
)


def _duckdb_available() -> bool:
    return shutil.which("duckdb") is not None


@unittest.skipUnless(_duckdb_available(), "duckdb CLI is required for parquet fixture tests")
class TestHistoricalLabelContractFromT9(unittest.TestCase):
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
            UNION ALL
            SELECT DATE '2024-01-04', DATE '2024-01-19', 'SPY', 'C', 473.0,
                   1.50, 1.60, 1.55, 21, 80, 0.21
            """,
        )

    def test_builds_mature_and_immature_label_rows_from_temp_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._build_fake_t9(root)

            contract = build_historical_label_contract_from_t9(
                root=root,
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-05",
                max_files=5,
                daily_source="yahoo",
                horizons=["1d"],
            )

        self.assertEqual(contract.report["status"], "pass")
        self.assertEqual(contract.report["mature_label_count"], 2)
        self.assertEqual(contract.report["immature_or_excluded_count"], 1)
        self.assertEqual(contract.report["excluded_by_reason"], {"immature_missing_future_close": 1})
        self.assertEqual(contract.label_candidates["label_status"].unique().tolist(), ["mature"])


class TestHistoricalLabelContract(unittest.TestCase):
    def _daily_features(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"date": "2024-01-02", "open": 470.0, "high": 472.0, "low": 469.0, "close": 471.0, "volume": 1000, "source": "yahoo", "return_1d": pd.NA, "intraday_range_pct": 0.006, "close_to_open_pct": 0.002},
                {"date": "2024-01-03", "open": 471.0, "high": 473.0, "low": 470.0, "close": 472.0, "volume": 1200, "source": "yahoo", "return_1d": 0.0021, "intraday_range_pct": 0.006, "close_to_open_pct": 0.002},
                {"date": "2024-01-04", "open": 472.0, "high": 474.0, "low": 471.0, "close": 473.0, "volume": 1300, "source": "yahoo", "return_1d": 0.0021, "intraday_range_pct": 0.006, "close_to_open_pct": 0.002},
            ]
        )

    def _label_ready(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"observation_date": "2024-01-02", "underlying_symbol": "SPY", "expiration": "2024-01-19", "strike": 471.0, "option_type": "call", "days_to_expiration": 17, "label_status": "ready_for_future_outcome_generation"},
                {"observation_date": "2024-01-03", "underlying_symbol": "SPY", "expiration": "2024-01-19", "strike": 472.0, "option_type": "put", "days_to_expiration": 16, "label_status": "ready_for_future_outcome_generation"},
                {"observation_date": "2024-01-04", "underlying_symbol": "SPY", "expiration": "2024-01-19", "strike": 473.0, "option_type": "call", "days_to_expiration": 15, "label_status": "ready_for_future_outcome_generation"},
            ]
        )

    def test_direct_contract_emits_only_when_future_close_exists(self):
        contract = build_historical_label_contract(
            label_ready_rows=self._label_ready(),
            model_ready_daily_features=self._daily_features(),
            symbol="SPY",
            start=pd.Timestamp("2024-01-02").date(),
            end=pd.Timestamp("2024-01-05").date(),
            horizons=["1d", "2d"],
        )

        self.assertEqual(contract.report["status"], "pass")
        self.assertEqual(contract.report["coverage"]["1d"]["mature"], 2)
        self.assertEqual(contract.report["coverage"]["1d"]["excluded"], 1)
        self.assertEqual(contract.report["coverage"]["2d"]["mature"], 1)
        self.assertEqual(contract.report["coverage"]["2d"]["excluded"], 2)
        self.assertTrue((contract.label_candidates["label_date"] <= "2024-01-05").all())

    def test_direct_contract_fails_when_labels_are_joined_back_as_inputs(self):
        label_ready = self._label_ready()
        label_ready["future_underlying_close"] = [472.0, 473.0, pd.NA]

        contract = build_historical_label_contract(
            label_ready_rows=label_ready,
            model_ready_daily_features=self._daily_features(),
            symbol="SPY",
            start=pd.Timestamp("2024-01-02").date(),
            end=pd.Timestamp("2024-01-05").date(),
            horizons=["1d"],
        )

        self.assertEqual(contract.report["status"], "fail")
        failed = {check["name"] for check in contract.report["checks"] if check["status"] == "fail"}
        self.assertIn("labels_not_joined_back_as_features", failed)

    def test_parse_horizons_rejects_unsupported_values(self):
        self.assertEqual(parse_horizons(["1d", "5d"]), [("1d", 1), ("5d", 5)])
        with self.assertRaises(ValueError):
            parse_horizons(["1h"])
