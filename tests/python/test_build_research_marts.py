from __future__ import annotations

import importlib.util
import json
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "build_research_marts.py"


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def create_training_view(duckdb_path: Path) -> None:
    con = duckdb.connect(str(duckdb_path))
    try:
        con.execute(
            """
            CREATE TABLE training_events_v1 (
                event_id VARCHAR,
                symbol VARCHAR,
                ts_event TIMESTAMP,
                event_ts_utc TIMESTAMP,
                event_ts_et TIMESTAMP,
                event_date_et DATE,
                event_hour_et INTEGER,
                tod_bucket VARCHAR,
                level_type VARCHAR,
                level_family VARCHAR,
                touch_side INTEGER,
                distance_bps DOUBLE,
                touch_price DOUBLE,
                level_price DOUBLE,
                bar_interval_sec INTEGER,
                confluence_count INTEGER,
                mtf_confluence_calc DOUBLE,
                mtf_confluence_types VARCHAR,
                regime_type INTEGER,
                rv_regime VARCHAR,
                data_quality VARCHAR,
                gamma_mode VARCHAR,
                gamma_flip_dist_bps_calc DOUBLE,
                ema_state_calc VARCHAR,
                vwap_dist_bps_calc DOUBLE,
                vpoc_dist_bps_calc DOUBLE,
                weekly_pivot_dist_bps DOUBLE,
                monthly_pivot_dist_bps DOUBLE,
                session_std DOUBLE,
                or_size_atr DOUBLE,
                or_breakout INTEGER,
                or_high_dist_bps DOUBLE,
                or_low_dist_bps DOUBLE,
                sigma_band_position DOUBLE,
                distance_to_upper_sigma_bps DOUBLE,
                distance_to_lower_sigma_bps DOUBLE,
                is_persistent_level INTEGER,
                hist_sample_size_calc INTEGER,
                atr DOUBLE,
                horizon_min INTEGER,
                return_bps DOUBLE,
                mfe_bps DOUBLE,
                mae_bps DOUBLE,
                reject INTEGER,
                break INTEGER,
                resolution_min INTEGER
            )
            """
        )
        rows = [
            (
                "evt_a", "SPY", "2026-03-10 14:05:00", "2026-03-10 14:05:00", "2026-03-10 10:05:00", "2026-03-10", 10,
                "open", "major", "resistance", 1, 4.2, 510.0, 509.8, 60, 2, 2.0, "weekly,monthly",
                3, "normal", "good", "positive", 12.0, "above_vwap", -3.0, -1.0, 5.0, 8.0, 1.2,
                0.35, 0, -4.0, 2.0, 0.4, 14.0, -16.0, 1, 180, 4.5, 15, 6.0, 9.0, -3.0, 1, 0, 15
            ),
            (
                "evt_a", "SPY", "2026-03-10 14:05:00", "2026-03-10 14:05:00", "2026-03-10 10:05:00", "2026-03-10", 10,
                "open", "major", "resistance", 1, 4.2, 510.0, 509.8, 60, 2, 2.0, "weekly,monthly",
                3, "normal", "good", "positive", 12.0, "above_vwap", -3.0, -1.0, 5.0, 8.0, 1.2,
                0.35, 0, -4.0, 2.0, 0.4, 14.0, -16.0, 1, 180, 4.5, 60, 12.0, 18.0, -6.0, 1, 0, 60
            ),
            (
                "evt_b", "SPY", "2026-03-11 17:30:00", "2026-03-11 17:30:00", "2026-03-11 13:30:00", "2026-03-11", 13,
                "mid", "minor", "support", -1, -6.8, 507.0, 507.5, 60, 1, 1.0, "weekly",
                1, "high", "good", "negative", -18.0, "below_vwap", 6.0, 3.0, -5.0, -9.0, 1.8,
                0.8, 1, 3.0, -5.0, -0.2, 12.0, -10.0, 0, 92, 6.0, 15, -4.0, 5.0, -9.0, 0, 1, 15
            ),
            (
                "evt_b", "SPY", "2026-03-11 17:30:00", "2026-03-11 17:30:00", "2026-03-11 13:30:00", "2026-03-11", 13,
                "mid", "minor", "support", -1, -6.8, 507.0, 507.5, 60, 1, 1.0, "weekly",
                1, "high", "good", "negative", -18.0, "below_vwap", 6.0, 3.0, -5.0, -9.0, 1.8,
                0.8, 1, 3.0, -5.0, -0.2, 12.0, -10.0, 0, 92, 6.0, 60, -10.0, 7.0, -14.0, 0, 1, 60
            ),
        ]
        placeholders = ", ".join(["?"] * len(rows[0]))
        con.executemany(f"INSERT INTO training_events_v1 VALUES ({placeholders})", rows)
    finally:
        con.close()


def create_source_db(source_db_path: Path, *, dates: list[str]) -> None:
    con = sqlite3.connect(str(source_db_path))
    try:
        con.execute(
            """
            CREATE TABLE bar_data (
                symbol TEXT NOT NULL,
                ts INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL,
                bar_interval_sec INTEGER,
                PRIMARY KEY (symbol, ts, bar_interval_sec)
            )
            """
        )
        rows = []
        for idx, event_date in enumerate(dates):
            dt = datetime.fromisoformat(f"{event_date}T14:30:00+00:00")
            ts_value = int(dt.timestamp())
            rows.append(("SPY", ts_value, 500.0 + idx, 501.0 + idx, 499.0 + idx, 500.5 + idx, 1000.0, 60))
        con.executemany("INSERT INTO bar_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        con.commit()
    finally:
        con.close()


def read_scalar(con, sql: str):
    return con.execute(sql).fetchone()[0]


class BuildResearchMartsTest(unittest.TestCase):
    def test_builds_lineage_and_expected_marts(self):
        module = load_module("build_research_marts_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            marts_dir = tmp / "marts"
            lineage_path = marts_dir / "last_build.json"
            source_db_path = tmp / "source.sqlite"
            create_source_db(source_db_path, dates=["2026-03-10", "2026-03-11"])
            create_training_view(duckdb_path)

            lineage = module.build_research_marts(
                duckdb_path=duckdb_path,
                marts_dir=marts_dir,
                lineage_path=lineage_path,
                source_db_path=source_db_path,
            )

            self.assertEqual(lineage["status"], "ok")
            self.assertTrue(lineage_path.exists())
            saved = json.loads(lineage_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["mart_counts"]["mart_event_base"], 2)
            self.assertEqual(saved["mart_counts"]["mart_event_labels"], 4)
            self.assertGreaterEqual(saved["mart_counts"]["mart_slice_expectancy_daily"], 4)
            self.assertGreaterEqual(saved["mart_counts"]["mart_slice_expectancy_rollup"], 4)
            self.assertEqual(saved["symbols"], ["SPY"])
            self.assertEqual(saved["horizons"], [15, 60])
            self.assertEqual(saved["cost_model"]["version"], "rt_cost_v1")
            self.assertEqual(saved["cost_model"]["trade_cost_bps"], 1.3)
            self.assertEqual(saved["source_db_metadata"]["source_db"], str(source_db_path))
            self.assertTrue(saved["source_db_metadata"]["source_db_exists"])
            self.assertEqual(saved["mart_counts"]["mart_trading_calendar"], 2)

    def test_writes_mart_build_config(self):
        module = load_module("build_research_marts_test_build_config", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            marts_dir = tmp / "marts"
            lineage_path = marts_dir / "last_build.json"
            source_db_path = tmp / "source.sqlite"
            create_source_db(source_db_path, dates=["2026-03-10", "2026-03-11"])
            create_training_view(duckdb_path)

            module.build_research_marts(
                duckdb_path=duckdb_path,
                marts_dir=marts_dir,
                lineage_path=lineage_path,
                source_db_path=source_db_path,
            )

            con = duckdb.connect(str(duckdb_path), read_only=True)
            try:
                self.assertEqual(read_scalar(con, "SELECT COUNT(*) FROM pq_research.mart_build_config"), 1)
                self.assertEqual(
                    read_scalar(con, "SELECT cost_model_version FROM pq_research.mart_build_config"),
                    "rt_cost_v1",
                )
                self.assertAlmostEqual(
                    float(read_scalar(con, "SELECT trade_cost_bps FROM pq_research.mart_build_config")),
                    1.3,
                )
            finally:
                con.close()

    def test_rollup_mart_exposes_rows_n_contract_column(self):
        module = load_module("build_research_marts_test_rollup_contract", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            marts_dir = tmp / "marts"
            lineage_path = marts_dir / "last_build.json"
            source_db_path = tmp / "source.sqlite"
            create_source_db(source_db_path, dates=["2026-03-10", "2026-03-11"])
            create_training_view(duckdb_path)

            module.build_research_marts(
                duckdb_path=duckdb_path,
                marts_dir=marts_dir,
                lineage_path=lineage_path,
                source_db_path=source_db_path,
            )

            con = duckdb.connect(str(duckdb_path), read_only=True)
            try:
                col_rows = con.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'pq_research' AND table_name = 'mart_slice_expectancy_rollup'
                    """
                ).fetchall()
                column_names = {str(row[0]) for row in col_rows}
                self.assertIn("rows_n", column_names)

                rows_sum = float(
                    read_scalar(
                        con,
                        "SELECT COALESCE(SUM(rows_n), 0) FROM pq_research.mart_slice_expectancy_rollup",
                    )
                )
                self.assertGreater(rows_sum, 0.0)
            finally:
                con.close()

    def test_label_mart_uses_configured_trade_cost(self):
        module = load_module("build_research_marts_test_alt_cost", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            marts_dir = tmp / "marts"
            lineage_path = marts_dir / "last_build.json"
            source_db_path = tmp / "source.sqlite"
            create_source_db(source_db_path, dates=["2026-03-10", "2026-03-11"])
            cost_models_path = tmp / "cost_models.json"
            cost_models_path.write_text(
                json.dumps(
                    {
                        "test_cost_v1": {
                            "label": "Test cost",
                            "spread_bps": 1.0,
                            "slippage_bps": 0.8,
                            "commission_bps": 0.2,
                            "trade_cost_bps": 2.0,
                        }
                    }
                ),
                encoding="utf-8",
            )
            create_training_view(duckdb_path)

            module.build_research_marts(
                duckdb_path=duckdb_path,
                marts_dir=marts_dir,
                lineage_path=lineage_path,
                cost_models_path=cost_models_path,
                cost_model_version="test_cost_v1",
                source_db_path=source_db_path,
            )

            con = duckdb.connect(str(duckdb_path), read_only=True)
            try:
                row = con.execute(
                    """
                    SELECT reject_net_bps, break_net_bps
                    FROM pq_research.mart_event_labels
                    WHERE event_id = 'evt_a' AND horizon_min = 15
                    """
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertAlmostEqual(float(row[0]), 4.0)
                self.assertAlmostEqual(float(row[1]), -8.0)
            finally:
                con.close()

    def test_lineage_contains_selected_cost_model(self):
        module = load_module("build_research_marts_test_lineage_cost", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            marts_dir = tmp / "marts"
            lineage_path = marts_dir / "last_build.json"
            source_db_path = tmp / "source.sqlite"
            create_source_db(source_db_path, dates=["2026-03-10", "2026-03-11"])
            cost_models_path = tmp / "cost_models.json"
            cost_models_path.write_text(
                json.dumps(
                    {
                        "test_cost_v1": {
                            "label": "Test cost",
                            "spread_bps": 1.0,
                            "slippage_bps": 0.8,
                            "commission_bps": 0.2,
                            "trade_cost_bps": 2.0,
                        }
                    }
                ),
                encoding="utf-8",
            )
            create_training_view(duckdb_path)

            lineage = module.build_research_marts(
                duckdb_path=duckdb_path,
                marts_dir=marts_dir,
                lineage_path=lineage_path,
                cost_models_path=cost_models_path,
                cost_model_version="test_cost_v1",
                source_db_path=source_db_path,
            )

            self.assertEqual(lineage["cost_model"]["version"], "test_cost_v1")
            self.assertEqual(lineage["cost_model"]["trade_cost_bps"], 2.0)
            self.assertEqual(lineage["cost_model"]["spread_bps"], 1.0)
            saved = json.loads(lineage_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["cost_model"]["version"], "test_cost_v1")
            self.assertEqual(saved["cost_model"]["commission_bps"], 0.2)

    def test_build_fails_on_unknown_cost_model(self):
        module = load_module("build_research_marts_test_missing_cost", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            marts_dir = tmp / "marts"
            lineage_path = marts_dir / "last_build.json"
            source_db_path = tmp / "source.sqlite"
            create_source_db(source_db_path, dates=["2026-03-10", "2026-03-11"])
            create_training_view(duckdb_path)

            with self.assertRaises(RuntimeError):
                module.build_research_marts(
                    duckdb_path=duckdb_path,
                    marts_dir=marts_dir,
                    lineage_path=lineage_path,
                    cost_model_version="missing_cost_version",
                    source_db_path=source_db_path,
                )

    def test_builds_trading_calendar_mart(self):
        module = load_module("build_research_marts_test_calendar", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            marts_dir = tmp / "marts"
            lineage_path = marts_dir / "last_build.json"
            source_db_path = tmp / "source.sqlite"
            create_source_db(
                source_db_path,
                dates=["2026-03-10", "2026-03-11", "2026-03-12"],
            )
            create_training_view(duckdb_path)

            module.build_research_marts(
                duckdb_path=duckdb_path,
                marts_dir=marts_dir,
                lineage_path=lineage_path,
                source_db_path=source_db_path,
            )

            con = duckdb.connect(str(duckdb_path), read_only=True)
            try:
                self.assertEqual(read_scalar(con, "SELECT COUNT(*) FROM pq_research.mart_trading_calendar"), 3)
                self.assertEqual(
                    read_scalar(
                        con,
                        "SELECT MIN(event_date_et) FROM pq_research.mart_trading_calendar WHERE symbol = 'SPY'",
                    ),
                    date.fromisoformat("2026-03-10"),
                )
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
