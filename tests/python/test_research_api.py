from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import duckdb
ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT_PATH = ROOT / "scripts" / "build_research_marts.py"
API_PATH = ROOT / "server" / "research_api.py"
TESTS_PYTHON_DIR = Path(__file__).resolve().parent
if str(TESTS_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_PYTHON_DIR))
from helpers.asgi_contract import request_json


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
            (
                "evt_c", "SPY", "2026-03-12 15:15:00", "2026-03-12 15:15:00", "2026-03-12 11:15:00", "2026-03-12", 11,
                "open", "major", "resistance", 1, 3.0, 512.0, 511.8, 60, 2, 2.0, "weekly,monthly",
                3, "normal", "good", "positive", 10.0, "above_vwap", -2.0, -1.2, 4.0, 6.0, 1.1,
                0.25, 0, -2.0, 1.5, 0.3, 10.0, -12.0, 1, 140, 4.2, 15, 4.0, 8.0, -2.5, 1, 0, 15
            ),
            (
                "evt_c", "SPY", "2026-03-12 15:15:00", "2026-03-12 15:15:00", "2026-03-12 11:15:00", "2026-03-12", 11,
                "open", "major", "resistance", 1, 3.0, 512.0, 511.8, 60, 2, 2.0, "weekly,monthly",
                3, "normal", "good", "positive", 10.0, "above_vwap", -2.0, -1.2, 4.0, 6.0, 1.1,
                0.25, 0, -2.0, 1.5, 0.3, 10.0, -12.0, 1, 140, 4.2, 60, 7.0, 13.0, -4.0, 1, 0, 60
            ),
            (
                "evt_d", "SPY", "2026-03-13 18:10:00", "2026-03-13 18:10:00", "2026-03-13 14:10:00", "2026-03-13", 14,
                "mid", "minor", "support", -1, -2.6, 506.4, 506.9, 60, 0, 0.0, "",
                2, "high", "good", "negative", -12.0, "below_vwap", 3.0, 1.0, -2.0, -4.0, 1.5,
                0.55, 1, 1.0, -2.0, -0.4, 9.0, -8.0, 0, 88, 5.5, 15, -2.0, 4.0, -6.0, 0, 1, 15
            ),
            (
                "evt_d", "SPY", "2026-03-13 18:10:00", "2026-03-13 18:10:00", "2026-03-13 14:10:00", "2026-03-13", 14,
                "mid", "minor", "support", -1, -2.6, 506.4, 506.9, 60, 0, 0.0, "",
                2, "high", "good", "negative", -12.0, "below_vwap", 3.0, 1.0, -2.0, -4.0, 1.5,
                0.55, 1, 1.0, -2.0, -0.4, 9.0, -8.0, 0, 88, 5.5, 60, -5.0, 6.0, -10.0, 0, 1, 60
            ),
            (
                "evt_e", "SPY", "2026-03-14 16:20:00", "2026-03-14 16:20:00", "2026-03-14 12:20:00", "2026-03-14", 12,
                "mid", "major", "resistance", 1, 5.2, 513.0, 512.6, 60, 1, 1.0, "weekly",
                3, "normal", "good", "positive", 8.0, "above_vwap", -1.0, -0.5, 2.0, 4.0, 0.9,
                0.22, 0, -1.5, 1.0, 0.2, 8.0, -9.0, 1, 132, 4.8, 15, 3.5, 5.5, -2.0, 1, 0, 15
            ),
            (
                "evt_e", "SPY", "2026-03-14 16:20:00", "2026-03-14 16:20:00", "2026-03-14 12:20:00", "2026-03-14", 12,
                "mid", "major", "resistance", 1, 5.2, 513.0, 512.6, 60, 1, 1.0, "weekly",
                3, "normal", "good", "positive", 8.0, "above_vwap", -1.0, -0.5, 2.0, 4.0, 0.9,
                0.22, 0, -1.5, 1.0, 0.2, 8.0, -9.0, 1, 132, 4.8, 60, 6.5, 10.0, -3.0, 1, 0, 60
            ),
            (
                "evt_f", "SPY", "2026-03-15 19:05:00", "2026-03-15 19:05:00", "2026-03-15 15:05:00", "2026-03-15", 15,
                "power", "minor", "support", -1, -4.0, 505.8, 506.1, 60, 1, 1.0, "monthly",
                4, "normal", "good", "negative", -6.0, "below_vwap", 2.0, 0.8, -1.0, -3.0, 1.0,
                0.42, 1, 0.8, -1.2, -0.3, 7.5, -7.0, 0, 77, 5.1, 15, -1.0, 3.0, -4.0, 0, 0, 15
            ),
            (
                "evt_f", "SPY", "2026-03-15 19:05:00", "2026-03-15 19:05:00", "2026-03-15 15:05:00", "2026-03-15", 15,
                "power", "minor", "support", -1, -4.0, 505.8, 506.1, 60, 1, 1.0, "monthly",
                4, "normal", "good", "negative", -6.0, "below_vwap", 2.0, 0.8, -1.0, -3.0, 1.0,
                0.42, 1, 0.8, -1.2, -0.3, 7.5, -7.0, 0, 77, 5.1, 60, -2.0, 5.0, -7.0, 0, 0, 60
            ),
        ]
        placeholders = ", ".join(["?"] * len(rows[0]))
        con.executemany(f"INSERT INTO training_events_v1 VALUES ({placeholders})", rows)
    finally:
        con.close()


def create_sparse_training_view(duckdb_path: Path) -> None:
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
                "evt_s1", "SPY", "2026-03-10 14:05:00", "2026-03-10 14:05:00", "2026-03-10 10:05:00", "2026-03-10", 10,
                "open", "major", "resistance", 1, 4.2, 510.0, 509.8, 60, 2, 2.0, "weekly,monthly",
                3, "normal", "good", "positive", 12.0, "above_vwap", -3.0, -1.0, 5.0, 8.0, 1.2,
                0.35, 0, -4.0, 2.0, 0.4, 14.0, -16.0, 1, 180, 4.5, 15, 6.0, 9.0, -3.0, 1, 0, 15
            ),
            (
                "evt_s2", "SPY", "2026-03-12 15:15:00", "2026-03-12 15:15:00", "2026-03-12 11:15:00", "2026-03-12", 11,
                "open", "major", "resistance", 1, 3.0, 512.0, 511.8, 60, 2, 2.0, "weekly,monthly",
                3, "normal", "good", "positive", 10.0, "above_vwap", -2.0, -1.2, 4.0, 6.0, 1.1,
                0.25, 0, -2.0, 1.5, 0.3, 10.0, -12.0, 1, 140, 4.2, 15, 4.0, 8.0, -2.5, 1, 0, 15
            ),
            (
                "evt_s3", "SPY", "2026-03-15 19:05:00", "2026-03-15 19:05:00", "2026-03-15 15:05:00", "2026-03-15", 15,
                "power", "minor", "support", -1, -4.0, 505.8, 506.1, 60, 1, 1.0, "monthly",
                4, "normal", "good", "negative", -6.0, "below_vwap", 2.0, 0.8, -1.0, -3.0, 1.0,
                0.42, 1, 0.8, -1.2, -0.3, 7.5, -7.0, 0, 77, 5.1, 15, -1.0, 3.0, -4.0, 0, 0, 15
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


class ResearchApiTest(unittest.TestCase):
    def test_endpoint_contracts_cover_health_metadata_slice_walkforward_and_replay(self):
        build_module = load_module("build_research_marts_for_endpoint_contracts", BUILD_SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            marts_dir = tmp / "marts"
            lineage_path = marts_dir / "last_build.json"
            source_db_path = tmp / "source.sqlite"
            create_source_db(
                source_db_path,
                dates=["2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13", "2026-03-14", "2026-03-15"],
            )
            create_training_view(duckdb_path)
            build_module.build_research_marts(
                duckdb_path=duckdb_path,
                marts_dir=marts_dir,
                lineage_path=lineage_path,
                source_db_path=source_db_path,
            )

            prev_duckdb = os.environ.get("DUCKDB_PATH")
            prev_lineage = os.environ.get("RESEARCH_LINEAGE_PATH")
            try:
                os.environ["DUCKDB_PATH"] = str(duckdb_path)
                os.environ["RESEARCH_LINEAGE_PATH"] = str(lineage_path)
                api_module = load_module("research_api_endpoint_contract_module", API_PATH)
                health_resp = request_json(api_module.app, "GET", "/health")
                self.assertEqual(health_resp.status_code, 200)
                health_payload = health_resp.json()
                self.assertEqual(health_payload["status"], "ok")
                self.assertIn("active_duckdb", health_payload)
                self.assertIn("lineage", health_payload)
                self.assertIn("duckdb_path", health_payload["lineage"])

                metadata_resp = request_json(api_module.app, "GET", "/metadata")
                self.assertEqual(metadata_resp.status_code, 200)
                metadata_payload = metadata_resp.json()
                self.assertIn("SPY", metadata_payload["symbols"])
                self.assertEqual(metadata_payload["horizons"], [15, 60])
                self.assertEqual(metadata_payload["date_range"]["min_date"], "2026-03-10")
                self.assertEqual(metadata_payload["date_range"]["max_date"], "2026-03-15")

                slice_resp = request_json(
                    api_module.app,
                    "POST",
                    "/slice-query",
                    json_body={
                        "symbol": "SPY",
                        "date_from": "2026-03-10",
                        "date_to": "2026-03-11",
                        "horizons": [15],
                        "filters": {"regime_bucket": ["compression"]},
                        "group_by": ["horizon_min", "regime_bucket", "level_family", "tod_bucket"],
                        "include_daily_curve": True,
                        "include_distribution": True,
                    },
                )
                self.assertEqual(slice_resp.status_code, 200)
                slice_payload = slice_resp.json()
                self.assertEqual(slice_payload["summary"]["rows"], 1)
                self.assertEqual(slice_payload["summary"]["days"], 1)
                self.assertEqual(slice_payload["groups"][0]["level_family"], "resistance")
                self.assertEqual(slice_payload["daily_curve"][0]["event_date_et"], "2026-03-10")

                walkforward_resp = request_json(
                    api_module.app,
                    "POST",
                    "/walkforward",
                    json_body={
                        "symbol": "SPY",
                        "horizon": 15,
                        "date_from": "2026-03-10",
                        "date_to": "2026-03-15",
                        "filters": {},
                        "train_days": 2,
                        "test_days": 1,
                        "step_days": 1,
                        "baseline": "slice_expectancy",
                    },
                )
                self.assertEqual(walkforward_resp.status_code, 200)
                walkforward_payload = walkforward_resp.json()
                self.assertEqual(walkforward_payload["aggregate"]["windows"], 4)
                self.assertEqual(len(walkforward_payload["windows"]), 4)
                self.assertIn("mean_test_avg_reject_net_bps", walkforward_payload["aggregate"])

                replay_resp = request_json(
                    api_module.app,
                    "POST",
                    "/replay-day",
                    json_body={
                        "symbol": "SPY",
                        "event_date": "2026-03-10",
                        "primary_horizon": 15,
                        "horizons": [15, 60],
                        "limit": 3,
                    },
                )
                self.assertEqual(replay_resp.status_code, 200)
                replay_payload = replay_resp.json()
                self.assertEqual(replay_payload["summary"]["event_date"], "2026-03-10")
                self.assertEqual(replay_payload["summary"]["primary_horizon"], 15)
                self.assertEqual(replay_payload["top_events"][0]["event_id"], "evt_a")

                invalid_walkforward_resp = request_json(
                    api_module.app,
                    "POST",
                    "/walkforward",
                    json_body={
                        "symbol": "SPY",
                        "horizon": 999,
                        "date_from": "2026-03-10",
                        "date_to": "2026-03-15",
                        "filters": {},
                        "train_days": 2,
                        "test_days": 1,
                        "step_days": 1,
                        "baseline": "slice_expectancy",
                    },
                )
                self.assertEqual(invalid_walkforward_resp.status_code, 400)
                self.assertEqual(invalid_walkforward_resp.json()["detail"], "Unsupported horizon")
            finally:
                if prev_duckdb is None:
                    os.environ.pop("DUCKDB_PATH", None)
                else:
                    os.environ["DUCKDB_PATH"] = prev_duckdb
                if prev_lineage is None:
                    os.environ.pop("RESEARCH_LINEAGE_PATH", None)
                else:
                    os.environ["RESEARCH_LINEAGE_PATH"] = prev_lineage

    def test_metadata_and_slice_query_contracts(self):
        build_module = load_module("build_research_marts_for_api", BUILD_SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            marts_dir = tmp / "marts"
            lineage_path = marts_dir / "last_build.json"
            source_db_path = tmp / "source.sqlite"
            create_source_db(source_db_path, dates=["2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13", "2026-03-14", "2026-03-15"])
            create_training_view(duckdb_path)
            build_module.build_research_marts(
                duckdb_path=duckdb_path,
                marts_dir=marts_dir,
                lineage_path=lineage_path,
                source_db_path=source_db_path,
            )

            os.environ["DUCKDB_PATH"] = str(duckdb_path)
            os.environ["RESEARCH_LINEAGE_PATH"] = str(lineage_path)
            api_module = load_module("research_api_test_module", API_PATH)

            health = api_module.health()
            self.assertEqual(health["status"], "ok")

            metadata_payload = api_module.metadata()
            self.assertIn("SPY", metadata_payload["symbols"])
            self.assertEqual(metadata_payload["horizons"], [15, 60])
            self.assertEqual(metadata_payload["date_range"]["min_date"], "2026-03-10")
            self.assertEqual(metadata_payload["date_range"]["max_date"], "2026-03-15")

            payload = api_module.slice_query(
                api_module.SliceQueryRequest(
                    symbol="SPY",
                    date_from="2026-03-10",
                    date_to="2026-03-11",
                    horizons=[15],
                    filters=api_module.SliceFilters(regime_bucket=["compression"]),
                    group_by=["horizon_min", "regime_bucket", "level_family", "tod_bucket"],
                    include_daily_curve=True,
                    include_distribution=True,
                )
            )
            self.assertEqual(payload["summary"]["rows"], 1)
            self.assertEqual(payload["summary"]["days"], 1)
            self.assertEqual(len(payload["groups"]), 1)
            self.assertEqual(payload["groups"][0]["level_family"], "resistance")
            self.assertEqual(payload["daily_curve"][0]["event_date_et"], "2026-03-10")

            map_payload = api_module.expectancy_map(
                api_module.ExpectancyMapRequest(
                    symbol="SPY",
                    date_from="2026-03-10",
                    date_to="2026-03-11",
                    horizon=60,
                    axis_x="regime_bucket",
                    axis_y="tod_bucket",
                    metric="avg_reject_net_bps",
                    filters=api_module.SliceFilters(level_family=["support", "resistance"]),
                )
            )
            self.assertEqual(map_payload["metric"], "avg_reject_net_bps")
            self.assertGreaterEqual(len(map_payload["cells"]), 2)

            walkforward_payload = api_module.walkforward(
                api_module.WalkforwardRequest(
                    symbol="SPY",
                    horizon=15,
                    date_from="2026-03-10",
                    date_to="2026-03-15",
                    filters=api_module.SliceFilters(),
                    train_days=2,
                    test_days=1,
                    step_days=1,
                    baseline="slice_expectancy",
                )
            )
            self.assertEqual(walkforward_payload["aggregate"]["windows"], 4)
            self.assertEqual(len(walkforward_payload["windows"]), 4)
            self.assertEqual(walkforward_payload["windows"][0]["train_start"], "2026-03-10")
            self.assertEqual(walkforward_payload["windows"][0]["test_start"], "2026-03-12")
            self.assertIn("mean_test_avg_reject_net_bps", walkforward_payload["aggregate"])

            drilldown_payload = api_module.cohort_drilldown(
                api_module.CohortDrilldownRequest(
                    symbol="SPY",
                    date_from="2026-03-10",
                    date_to="2026-03-15",
                    horizons=[15],
                    filters=api_module.SliceFilters(),
                    limit=3,
                    baseline="reject_net_bps",
                )
            )
            self.assertEqual(drilldown_payload["stability"]["days"], 6)
            self.assertEqual(len(drilldown_payload["best_days"]), 3)
            self.assertEqual(len(drilldown_payload["worst_days"]), 3)
            self.assertEqual(drilldown_payload["best_days"][0]["event_date_et"], "2026-03-10")
            self.assertEqual(drilldown_payload["recent_days"][0]["event_date_et"], "2026-03-15")

            replay_payload = api_module.replay_day(
                api_module.ReplayDayRequest(
                    symbol="SPY",
                    event_date="2026-03-10",
                    primary_horizon=15,
                    horizons=[15, 60],
                    limit=3,
                )
            )
            self.assertEqual(replay_payload["summary"]["event_date"], "2026-03-10")
            self.assertEqual(replay_payload["summary"]["primary_horizon"], 15)
            self.assertEqual(replay_payload["summary"]["rows"], 1)
            self.assertEqual(len(replay_payload["by_horizon"]), 2)
            self.assertEqual(replay_payload["by_horizon"][0]["horizon_min"], 15)
            self.assertEqual(replay_payload["top_events"][0]["event_id"], "evt_a")

    def test_walkforward_uses_calendar_and_keeps_zero_row_windows(self):
        build_module = load_module("build_research_marts_for_sparse_walkforward", BUILD_SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            marts_dir = tmp / "marts"
            lineage_path = marts_dir / "last_build.json"
            source_db_path = tmp / "source.sqlite"
            create_source_db(
                source_db_path,
                dates=["2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13", "2026-03-14", "2026-03-15"],
            )
            create_sparse_training_view(duckdb_path)
            build_module.build_research_marts(
                duckdb_path=duckdb_path,
                marts_dir=marts_dir,
                lineage_path=lineage_path,
                source_db_path=source_db_path,
            )

            os.environ["DUCKDB_PATH"] = str(duckdb_path)
            os.environ["RESEARCH_LINEAGE_PATH"] = str(lineage_path)
            api_module = load_module("research_api_sparse_walkforward_module", API_PATH)

            walkforward_payload = api_module.walkforward(
                api_module.WalkforwardRequest(
                    symbol="SPY",
                    horizon=15,
                    date_from="2026-03-10",
                    date_to="2026-03-15",
                    filters=api_module.SliceFilters(),
                    train_days=2,
                    test_days=1,
                    step_days=1,
                    baseline="slice_expectancy",
                )
            )

            self.assertEqual(walkforward_payload["aggregate"]["calendar_days"], 6)
            self.assertEqual(walkforward_payload["aggregate"]["windows"], 4)
            self.assertEqual(walkforward_payload["aggregate"]["windows_with_test_rows"], 2)
            self.assertEqual(walkforward_payload["aggregate"]["windows_without_test_rows"], 2)
            self.assertEqual(len(walkforward_payload["windows"]), 4)
            self.assertEqual(walkforward_payload["windows"][0]["test_start"], "2026-03-12")
            self.assertEqual(walkforward_payload["windows"][1]["test_start"], "2026-03-13")
            self.assertEqual(walkforward_payload["windows"][1]["rows_test"], 0)
            self.assertIsNone(walkforward_payload["windows"][1]["test_avg_reject_net_bps"])


if __name__ == "__main__":
    unittest.main()
