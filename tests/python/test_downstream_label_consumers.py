from __future__ import annotations

import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[2]
LEVELS_DIR = ROOT / "scripts" / "levels_product"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(LEVELS_DIR) not in sys.path:
    sys.path.insert(0, str(LEVELS_DIR))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ml_server = load_module("ml_server_downstream_labels", ROOT / "server" / "ml_server.py")
train_hold_model = load_module(
    "levels_product_train_hold_model_labels",
    LEVELS_DIR / "train_hold_model.py",
)
build_track_record = load_module(
    "levels_product_build_track_record_labels",
    LEVELS_DIR / "build_track_record.py",
)
morning_level_map = load_module(
    "levels_product_morning_level_map_labels",
    LEVELS_DIR / "morning_level_map.py",
)
forecast_store = load_module(
    "levels_product_forecast_store_labels",
    LEVELS_DIR / "forecast_store.py",
)
from hold_engine import HORIZONS, current_rates, label_coverage  # noqa: E402


def create_analog_db(path: Path, *, with_coverage_status: bool = True) -> None:
    con = sqlite3.connect(path)
    coverage_column = ", coverage_status TEXT" if with_coverage_status else ""
    con.executescript(
        f"""
        CREATE TABLE touch_events (
            event_id TEXT PRIMARY KEY,
            symbol TEXT,
            ts_event INTEGER,
            level_type TEXT,
            regime_type INTEGER,
            gamma_mode INTEGER,
            distance_bps REAL,
            atr REAL,
            touch_price REAL,
            ema_state REAL,
            vwap_dist_bps REAL,
            rv_30 REAL,
            or_size_atr REAL,
            overnight_gap_atr REAL
        );
        CREATE TABLE event_labels (
            event_id TEXT,
            horizon_min INTEGER,
            reject INTEGER,
            break INTEGER
            {coverage_column}
        );
        """
    )
    for event_id, ts_event in (("qualified", 1_700_000_000_000), ("invalid", 1_700_000_060_000)):
        con.execute(
            "INSERT INTO touch_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (event_id, "SPY", ts_event, "R1", 1, 1, 3.0, 5.0, 500.0, 1, 2.0, 0.1, 1.0, 0.0),
        )
    if with_coverage_status:
        con.executemany(
            "INSERT INTO event_labels VALUES (?,?,?,?,?)",
            [
                ("qualified", 15, 0, 0, "qualified"),
                ("invalid", 15, 1, 0, "duplicate_timestamps"),
            ],
        )
    else:
        con.executemany(
            "INSERT INTO event_labels VALUES (?,?,?,?)",
            [("qualified", 15, 0, 0), ("invalid", 15, 1, 0)],
        )
    con.commit()
    con.close()


class AnalogEngineQualifiedLabelTest(unittest.TestCase):
    def test_refresh_loads_only_qualified_labels(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "analog.sqlite"
            create_analog_db(path)
            engine = ml_server.AnalogEngine(path)
            engine.enabled = True

            engine.refresh()

            self.assertIsNone(engine.error)
            self.assertEqual(
                [row["event_id"] for row in engine.rows_by_horizon[15]],
                ["qualified"],
            )
            self.assertEqual(engine.rows_by_horizon[15][0]["reject"], 0.0)
            self.assertEqual(engine.rows_by_horizon[15][0]["break"], 0.0)

    def test_refresh_fails_closed_when_coverage_status_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy.sqlite"
            create_analog_db(path, with_coverage_status=False)
            engine = ml_server.AnalogEngine(path)
            engine.enabled = True

            engine.refresh()

            self.assertEqual(engine.rows_by_horizon, {})
            self.assertIsNotNone(engine.error)
            self.assertIn("coverage_status", engine.error)


def create_levels_db(path: Path, *, with_coverage_status: bool = True) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    coverage_column = ", coverage_status TEXT" if with_coverage_status else ""
    con.executescript(
        f"""
        CREATE TABLE touch_events (
            event_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            ts_event INTEGER NOT NULL,
            data_quality REAL NOT NULL,
            confluence_count INTEGER,
            bar_interval_sec REAL
        );
        CREATE TABLE bar_data (
            symbol TEXT NOT NULL,
            ts INTEGER NOT NULL,
            bar_interval_sec INTEGER NOT NULL
        );
        CREATE TABLE event_labels (
            event_id TEXT NOT NULL,
            horizon_min INTEGER NOT NULL,
            reject INTEGER
            {coverage_column},
            PRIMARY KEY (event_id, horizon_min)
        );
        """
    )
    return con


class LevelHoldQualifiedLabelTest(unittest.TestCase):
    def test_training_load_filters_unqualified_and_keeps_qualified_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "training.sqlite"
            con = create_levels_db(path)
            con.executemany(
                "INSERT INTO touch_events VALUES (?,?,?,?,?,?)",
                [
                    ("qualified", "SPY", 1_000_000, 1.0, 1, 60),
                    ("invalid", "SPY", 2_000_000, 1.0, 1, 60),
                ],
            )
            con.executemany(
                "INSERT INTO event_labels VALUES (?,?,?,?)",
                [
                    ("qualified", 15, 0, "qualified"),
                    ("invalid", 15, 1, "duplicate_timestamps"),
                ],
            )
            con.commit()
            con.close()
            original_db = train_hold_model.DB
            train_hold_model.DB = path
            try:
                frame = train_hold_model.load("SPY", 15, 0.9)
            finally:
                train_hold_model.DB = original_db

            self.assertEqual(frame["reject"].tolist(), [0])
            self.assertEqual(frame["ts_event"].tolist(), [1_000_000])

    def test_training_load_fails_actionably_on_legacy_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy.sqlite"
            con = create_levels_db(path, with_coverage_status=False)
            con.close()
            original_db = train_hold_model.DB
            train_hold_model.DB = path
            try:
                with self.assertRaisesRegex(Exception, "coverage_status"):
                    train_hold_model.load("SPY", 15, 0.9)
            finally:
                train_hold_model.DB = original_db

    def test_coverage_treats_invalid_labels_as_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            con = create_levels_db(Path(tmp) / "coverage.sqlite")
            for event_id, status in (("qualified", "qualified"), ("invalid", "duplicate_timestamps")):
                ts_event = 1_000_000 if event_id == "qualified" else 5_000_000
                con.execute(
                    "INSERT INTO touch_events VALUES (?,?,?,?,?,?)",
                    (event_id, "SPY", ts_event, 1.0, 1, 60),
                )
                con.execute("INSERT INTO bar_data VALUES (?,?,?)", ("SPY", ts_event + 900_000, 60))
                con.execute(
                    "INSERT INTO event_labels VALUES (?,?,?,?)",
                    (event_id, 15, 0, status),
                )
            con.commit()

            coverage = label_coverage(con, "SPY", horizons=(15,))

            self.assertEqual(coverage[15]["eligible_count"], 2)
            self.assertEqual(coverage[15]["labeled_count"], 1)
            self.assertEqual(coverage[15]["missing_count"], 1)
            self.assertFalse(coverage[15]["complete"])
            con.close()

    def test_current_rates_excludes_invalid_labels_when_coverage_is_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            con = create_levels_db(Path(tmp) / "rates.sqlite")
            for i in range(35):
                ts_event = 1_000_000 + i * 4_000_000
                event_id = f"q{i:02d}"
                con.execute(
                    "INSERT INTO touch_events VALUES (?,?,?,?,?,?)",
                    (event_id, "SPY", ts_event, 1.0, 1, 60),
                )
                for horizon in HORIZONS:
                    con.execute("INSERT INTO bar_data VALUES (?,?,?)", ("SPY", ts_event + horizon * 60_000, 60))
                    con.execute(
                        "INSERT INTO event_labels VALUES (?,?,?,?)",
                        (event_id, horizon, 0, "qualified"),
                    )
            con.execute(
                "INSERT INTO touch_events VALUES (?,?,?,?,?,?)",
                ("invalid", "SPY", 200_000_000, 1.0, 1, 60.5),
            )
            for horizon in HORIZONS:
                con.execute(
                    "INSERT INTO event_labels VALUES (?,?,?,?)",
                    ("invalid", horizon, 1, "duplicate_timestamps"),
                )
            con.commit()

            rates = current_rates(con, "SPY")

            self.assertTrue(rates["_publishable"])
            for horizon in HORIZONS:
                self.assertEqual(rates[horizon]["1"]["n"], 35)
                self.assertEqual(rates[horizon]["1"]["rate"], 0.0)
            con.close()

    def test_coverage_and_rates_fail_actionably_on_legacy_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            con = create_levels_db(Path(tmp) / "legacy.sqlite", with_coverage_status=False)
            with self.assertRaisesRegex(Exception, "coverage_status"):
                label_coverage(con, "SPY", horizons=(15,))
            with self.assertRaisesRegex(Exception, "coverage_status"):
                current_rates(con, "SPY")
            con.close()


class EquivalentLevelConsumerQualifiedLabelTest(unittest.TestCase):
    def _seed_outcomes(self, con: sqlite3.Connection) -> None:
        con.executemany(
            "INSERT INTO touch_events VALUES (?,?,?,?,?,?)",
            [
                ("qualified", "SPY", 10_000_000, 1.0, 1, 60),
                ("invalid", "SPY", 11_000_000, 1.0, 1, 60),
            ],
        )
        con.executemany(
            "INSERT INTO event_labels VALUES (?,?,?,?)",
            [
                ("qualified", 15, 0, "qualified"),
                ("invalid", 15, 1, "duplicate_timestamps"),
            ],
        )
        con.commit()

    def test_track_record_uses_only_qualified_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            con = create_levels_db(Path(tmp) / "track.sqlite")
            self._seed_outcomes(con)
            old_forecasts = build_track_record.trailing_forecasts
            old_base = build_track_record.trailing_base_rate
            build_track_record.trailing_forecasts = (
                lambda ts, buckets, outcomes, horizon: np.full(len(ts), 0.5)
            )
            build_track_record.trailing_base_rate = (
                lambda ts, outcomes, horizon: np.full(len(ts), 0.5)
            )
            try:
                result = build_track_record.horizon_record(
                    con, "SPY", 15, 0.9, 30, build_track_record.date(1970, 1, 1)
                )
            finally:
                build_track_record.trailing_forecasts = old_forecasts
                build_track_record.trailing_base_rate = old_base
                con.close()

            self.assertEqual(result["n_scored"], 1)

    def test_morning_base_rates_use_only_qualified_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            con = create_levels_db(Path(tmp) / "morning.sqlite")
            self._seed_outcomes(con)

            rates = morning_level_map.recent_base_rates(con, "SPY", 20_000_000)

            self.assertEqual(rates[15], {"hold_rate": 0.0, "n": 1})
            con.close()

    def test_forecast_emit_treats_unqualified_outcomes_as_unresolved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            read_db = Path(tmp) / "read.sqlite"
            product_db = Path(tmp) / "product.sqlite"
            con = create_levels_db(read_db)
            self._seed_outcomes(con)
            con.close()
            captured: list[list[int | None]] = []
            old_read_db, old_product_db = forecast_store.READ_DB, forecast_store.PRODUCT_DB
            old_horizons = forecast_store.HORIZONS
            old_forecasts = forecast_store.trailing_forecasts
            forecast_store.READ_DB, forecast_store.PRODUCT_DB = read_db, product_db
            forecast_store.HORIZONS = [15]

            def capture_outcomes(ts, buckets, outcomes, horizon):
                captured.append(list(outcomes))
                return np.full(len(ts), np.nan)

            forecast_store.trailing_forecasts = capture_outcomes
            try:
                forecast_store.emit("SPY", 0.9, golive_ts=0)
            finally:
                forecast_store.READ_DB, forecast_store.PRODUCT_DB = old_read_db, old_product_db
                forecast_store.HORIZONS = old_horizons
                forecast_store.trailing_forecasts = old_forecasts

            self.assertEqual(captured, [[0, None]])

    def test_forecast_score_uses_only_qualified_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            read_db = Path(tmp) / "read.sqlite"
            product_db = Path(tmp) / "product.sqlite"
            con = create_levels_db(read_db)
            self._seed_outcomes(con)
            con.close()
            pcon = sqlite3.connect(product_db)
            forecast_store.ensure_table(pcon)
            pcon.executemany(
                "INSERT INTO level_hold_forecasts VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    ("qualified", "SPY", 15, 10_000_000, 1, "1", 0.5, "test", 12_000_000),
                    ("invalid", "SPY", 15, 11_000_000, 1, "1", 0.5, "test", 12_000_000),
                ],
            )
            pcon.commit()
            pcon.close()
            old_read_db, old_product_db = forecast_store.READ_DB, forecast_store.PRODUCT_DB
            old_horizons = forecast_store.HORIZONS
            forecast_store.READ_DB, forecast_store.PRODUCT_DB = read_db, product_db
            forecast_store.HORIZONS = [15]
            try:
                result = forecast_store.score("SPY", 30)
            finally:
                forecast_store.READ_DB, forecast_store.PRODUCT_DB = old_read_db, old_product_db
                forecast_store.HORIZONS = old_horizons

            self.assertEqual(result["horizons"]["h15"]["n_scored"], 1)


if __name__ == "__main__":
    unittest.main()
