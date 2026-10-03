from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import build_labels  # noqa: E402
from scripts import migrate_db  # noqa: E402
from scripts import refit_calibration  # noqa: E402
from scripts import train_rf  # noqa: E402
from scripts import train_rf_artifacts  # noqa: E402


def _bars(ts_values: list[int]) -> list[dict]:
    return [
        {"ts": ts, "open": 100.0, "high": 100.2, "low": 99.8, "close": 100.0}
        for ts in ts_values
    ]


ET = ZoneInfo("America/New_York")


def _ts(hour: int, minute: int, day: int = 25) -> int:
    return int(datetime(2026, 11, day, hour, minute, tzinfo=ET).timestamp() * 1000)


def _create_label_db(path: Path, *, allow_duplicates: bool = False) -> int:
    conn = sqlite3.connect(path)
    migrate_db.migrate_connection(conn, verbose=False)
    if allow_duplicates:
        conn.execute("DROP TABLE bar_data")
        conn.execute(
            "CREATE TABLE bar_data (symbol TEXT, ts INTEGER, open REAL, high REAL, "
            "low REAL, close REAL, volume REAL, bar_interval_sec INTEGER)"
        )
    event_ts = _ts(10, 0)
    conn.execute(
        "INSERT INTO touch_events (event_id, symbol, ts_event, level_type, level_price, "
        "touch_price, distance_bps, touch_side, bar_interval_sec, created_at) "
        "VALUES ('evt-1', 'SPY', ?, 'PP', 100, 100, 0, 1, 60, ?)",
        (event_ts, event_ts),
    )
    rows = []
    for minute in range(1, 6):
        ts = event_ts + minute * 60_000
        rows.append(("SPY", ts, 100, 101, 99, 100, 1, 60))
    conn.executemany(
        "INSERT INTO bar_data (symbol, ts, open, high, low, close, volume, bar_interval_sec) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()
    return event_ts


def _run_main(path: Path, *, incremental: bool = False) -> str:
    argv = ["build_labels.py", "--db", str(path), "--horizons", "5"]
    if incremental:
        argv.append("--incremental")
    output = StringIO()
    with patch.object(sys, "argv", argv), redirect_stdout(output):
        build_labels.main()
    return output.getvalue()


class TestCoverageQualification(unittest.TestCase):
    def assert_unqualified(self, ts_values: list[int], expected_status: str) -> None:
        base = _ts(10, 0)
        quality = build_labels.assess_bar_path_coverage(
            _bars([base + ts for ts in ts_values]),
            ts_event=base,
            end_ts=base + 300_000,
            interval_sec=60,
        )
        self.assertFalse(quality["qualified"])
        self.assertEqual(quality["coverage_status"], expected_status)

    def test_complete_path_is_qualified_with_metadata(self) -> None:
        base = _ts(10, 0)
        quality = build_labels.assess_bar_path_coverage(
            _bars([base + ts for ts in [60_000, 120_000, 180_000, 240_000, 300_000]]),
            ts_event=base,
            end_ts=base + 300_000,
            interval_sec=60,
        )
        self.assertTrue(quality["qualified"])
        self.assertEqual(quality["expected_bar_count"], 5)
        self.assertEqual(quality["observed_bar_count"], 5)
        self.assertEqual(quality["coverage_ratio"], 1.0)
        self.assertEqual(quality["max_gap_sec"], 60.0)
        self.assertEqual(quality["endpoint_gap_sec"], 0.0)
        self.assertEqual(quality["endpoint_status"], "exact")
        self.assertEqual(quality["coverage_status"], "qualified")

    def test_missing_middle_bar_is_not_qualified(self) -> None:
        self.assert_unqualified([60_000, 120_000, 240_000, 300_000], "excessive_gap")

    def test_endpoint_only_data_is_not_qualified(self) -> None:
        self.assert_unqualified([300_000], "late_first_bar")

    def test_late_first_bar_is_not_qualified(self) -> None:
        self.assert_unqualified([120_000, 180_000, 240_000, 300_000], "late_first_bar")

    def test_absent_endpoint_is_not_qualified(self) -> None:
        self.assert_unqualified([60_000, 120_000, 180_000, 240_000], "missing_endpoint")

    def test_excessive_gap_is_not_qualified_even_with_expected_count(self) -> None:
        self.assert_unqualified(
            [60_000, 90_000, 180_000, 240_000, 300_000], "excessive_gap"
        )

    def test_duplicate_timestamp_fails_closed(self) -> None:
        self.assert_unqualified(
            [60_000, 120_000, 120_000, 180_000, 240_000, 300_000],
            "duplicate_timestamps",
        )

    def test_after_hours_event_fails_closed(self) -> None:
        start = _ts(16, 1)
        quality = build_labels.assess_bar_path_coverage(
            _bars([start + i * 60_000 for i in range(1, 6)]),
            start,
            start + 5 * 60_000,
            60,
        )
        self.assertFalse(quality["qualified"])
        self.assertEqual(quality["coverage_status"], "event_outside_regular_session")

    def test_near_close_horizon_fails_closed(self) -> None:
        start = _ts(15, 56)
        bars = _bars([start + i * 60_000 for i in range(1, 6)])
        quality = build_labels.assess_bar_path_coverage(
            bars, start, start + 5 * 60_000, 60
        )
        self.assertFalse(quality["qualified"])
        self.assertEqual(quality["coverage_status"], "horizon_past_session_close")

    def test_horizon_past_early_close_fails_closed(self) -> None:
        start = _ts(12, 58, day=27)  # 2026-11-27 is a 1 PM ET half-day.
        quality = build_labels.assess_bar_path_coverage(
            _bars([start + 60_000, start + 120_000]), start, start + 5 * 60_000, 60
        )
        self.assertFalse(quality["qualified"])
        self.assertEqual(quality["coverage_status"], "horizon_past_session_close")


class TestBuildLabelsMainIntegration(unittest.TestCase):
    def test_nonfinite_ohlc_path_is_rejected_by_cli_caller(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "labels.sqlite"
            event_ts = _create_label_db(path)
            conn = sqlite3.connect(path)
            conn.execute(
                "UPDATE bar_data SET high = ? WHERE ts = ?",
                (float("inf"), event_ts + 3 * 60_000),
            )
            conn.commit()
            conn.close()

            output = _run_main(path)

            conn = sqlite3.connect(path)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM event_labels").fetchone()[0], 0)
            conn.close()
            self.assertIn("invalid_ohlc_values=1", output)

    def test_nonnumeric_ohlc_path_is_rejected_by_cli_caller(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "labels.sqlite"
            event_ts = _create_label_db(path)
            conn = sqlite3.connect(path)
            conn.execute(
                "UPDATE bar_data SET close = ? WHERE ts = ?",
                ("not-a-price", event_ts + 3 * 60_000),
            )
            conn.commit()
            conn.close()

            output = _run_main(path)

            conn = sqlite3.connect(path)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM event_labels").fetchone()[0], 0)
            conn.close()
            self.assertIn("invalid_ohlc_values=1", output)

    def test_structurally_invalid_ohlc_path_is_rejected_by_cli_caller(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "labels.sqlite"
            event_ts = _create_label_db(path)
            conn = sqlite3.connect(path)
            conn.execute(
                "UPDATE bar_data SET high = 98 WHERE ts = ?",
                (event_ts + 3 * 60_000,),
            )
            conn.commit()
            conn.close()

            output = _run_main(path)

            conn = sqlite3.connect(path)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM event_labels").fetchone()[0], 0)
            conn.close()
            self.assertIn("invalid_ohlc_structure=1", output)

    def test_duplicate_bars_are_rejected_by_cli_caller(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "labels.sqlite"
            event_ts = _create_label_db(path, allow_duplicates=True)
            conn = sqlite3.connect(path)
            conn.execute(
                "INSERT INTO bar_data VALUES ('SPY', ?, 100, 101, 99, 100, 1, 60)",
                (event_ts + 2 * 60_000,),
            )
            conn.commit()
            conn.close()
            output = _run_main(path)
            conn = sqlite3.connect(path)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM event_labels").fetchone()[0], 0)
            conn.close()
            self.assertIn("duplicate_timestamps=1", output)

    def test_normal_rerun_removes_stale_label(self) -> None:
        self._assert_rerun_removes_stale_label(False)

    def test_incremental_rerun_removes_stale_label(self) -> None:
        self._assert_rerun_removes_stale_label(True)

    def test_normal_rerun_removes_stale_labels_for_invalid_event_intervals(self) -> None:
        self._assert_invalid_interval_removes_requested_horizon(False)

    def test_incremental_rerun_removes_stale_labels_for_invalid_event_intervals(self) -> None:
        self._assert_invalid_interval_removes_requested_horizon(True)

    def _assert_invalid_interval_removes_requested_horizon(self, incremental: bool) -> None:
        for invalid_interval in (None, 0, "not-an-interval"):
            with self.subTest(incremental=incremental, invalid_interval=invalid_interval):
                with tempfile.TemporaryDirectory() as tmp:
                    path = Path(tmp) / "labels.sqlite"
                    _create_label_db(path)
                    _run_main(path)
                    conn = sqlite3.connect(path)
                    conn.execute(
                        "INSERT INTO event_labels (event_id, horizon_min, coverage_status) "
                        "VALUES ('evt-1', 15, 'qualified')"
                    )
                    conn.execute(
                        "UPDATE touch_events SET bar_interval_sec = ? WHERE event_id = 'evt-1'",
                        (invalid_interval,),
                    )
                    conn.commit()
                    self.assertEqual(
                        conn.execute(
                            "SELECT horizon_min FROM event_labels ORDER BY horizon_min"
                        ).fetchall(),
                        [(5,), (15,)],
                    )
                    conn.close()

                    _run_main(path, incremental=incremental)

                    conn = sqlite3.connect(path)
                    self.assertEqual(
                        conn.execute(
                            "SELECT horizon_min FROM event_labels ORDER BY horizon_min"
                        ).fetchall(),
                        [(15,)],
                    )
                    conn.close()

    def _assert_rerun_removes_stale_label(self, incremental: bool) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "labels.sqlite"
            event_ts = _create_label_db(path)
            _run_main(path, incremental=incremental)
            conn = sqlite3.connect(path)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM event_labels").fetchone()[0], 1)
            conn.execute("DELETE FROM bar_data WHERE ts = ?", (event_ts + 3 * 60_000,))
            conn.commit()
            conn.close()
            _run_main(path, incremental=incremental)
            conn = sqlite3.connect(path)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM event_labels").fetchone()[0], 0)
            conn.close()


class TestLabelQualitySchema(unittest.TestCase):
    def test_migration_adds_label_quality_columns(self) -> None:
        conn = sqlite3.connect(":memory:")
        migrate_db.migrate_connection(conn, verbose=False)
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(event_labels)").fetchall()
        }
        self.assertTrue(
            {
                "expected_bar_count",
                "observed_bar_count",
                "coverage_ratio",
                "max_gap_sec",
                "endpoint_gap_sec",
                "endpoint_status",
                "coverage_status",
            }.issubset(columns)
        )
        self.assertGreaterEqual(migrate_db.LATEST_SCHEMA_VERSION, 9)

    def test_v9_ledger_reconciles_recreated_legacy_table(self) -> None:
        conn = sqlite3.connect(":memory:")
        migrate_db.migrate_connection(conn, verbose=False)
        conn.execute("DROP TABLE event_labels")
        conn.execute(
            "CREATE TABLE event_labels (event_id TEXT, horizon_min INTEGER, "
            "PRIMARY KEY(event_id, horizon_min))"
        )
        self.assertEqual(migrate_db.get_schema_version(conn), 9)
        migrate_db.migrate_connection(conn, verbose=False)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(event_labels)")}
        self.assertTrue(set(build_labels.LABEL_QUALITY_COLUMNS).issubset(columns))


class TestTrainRfIntegrity(unittest.TestCase):
    def test_preparation_filters_unqualified_and_drops_all_quality_fields(self) -> None:
        pd = train_rf.require("pandas", "python3 -m pip install pandas")
        rows = []
        for event_id, status in (("good", "qualified"), ("bad", "excessive_gap")):
            rows.append({
                "event_id": event_id, "symbol": "SPY", "ts_event": _ts(10, 0),
                "level_type": "PP", "level_price": 100.0, "touch_price": 100.0,
                "distance_bps": 0.0, "reject": 0, "coverage_status": status,
                "expected_bar_count": 5, "observed_bar_count": 5,
                "coverage_ratio": 1.0, "max_gap_sec": 60.0,
                "endpoint_gap_sec": 0.0, "endpoint_status": "exact",
            })
        filtered, features, target = train_rf.prepare_training_data(
            pd.DataFrame(rows), "reject"
        )
        self.assertEqual(filtered["event_id"].tolist(), ["good"])
        self.assertEqual(target.tolist(), [0])
        quality_fields = set(build_labels.LABEL_QUALITY_COLUMNS)
        self.assertTrue(quality_fields.isdisjoint(features.columns))

    def test_preparation_fails_closed_without_coverage_status(self) -> None:
        pd = train_rf.require("pandas", "python3 -m pip install pandas")
        with self.assertRaisesRegex(ValueError, "coverage_status"):
            train_rf.prepare_training_data(pd.DataFrame([{"reject": 0}]), "reject")

    def test_all_trainers_use_explicit_admissible_feature_contract(self) -> None:
        pd = train_rf.require("pandas", "python3 -m pip install pandas")
        row = {
            "event_id": "good",
            "symbol": "SPY",
            "ts_event": _ts(10, 0),
            "level_type": "PP",
            "level_price": 100.0,
            "touch_price": 100.0,
            "distance_bps": 3.5,
            "reject": 0,
            "coverage_status": "qualified",
            "expected_bar_count": 5,
            "observed_bar_count": 5,
            "coverage_ratio": 1.0,
            "max_gap_sec": 60.0,
            "endpoint_gap_sec": 0.0,
            "endpoint_status": "exact",
            "future_secret": 999,
            "future_outcome_note": "leaked",
        }
        frame = pd.DataFrame([row])
        trainer_frames = {
            "train_rf": train_rf.prepare_training_data(frame, "reject")[1],
            "train_rf_artifacts": train_rf_artifacts.prepare_feature_dataframe(frame),
            "refit_calibration": refit_calibration.prepare_feature_dataframe(frame),
        }
        forbidden = set(build_labels.LABEL_QUALITY_COLUMNS) | {
            "future_secret",
            "future_outcome_note",
        }
        for trainer_name, features in trainer_frames.items():
            with self.subTest(trainer=trainer_name):
                self.assertTrue(forbidden.isdisjoint(features.columns))
                self.assertIn("distance_bps", features.columns)
                self.assertIn("event_hour_et", features.columns)


class TestRefitCalibrationIntegrity(unittest.TestCase):
    def test_load_dataframe_filters_unqualified_and_keeps_qualified_negatives(self) -> None:
        duckdb = __import__("duckdb")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "training.duckdb"
            con = duckdb.connect(str(path))
            con.execute(
                "CREATE TABLE training_events_v1 AS SELECT * FROM (VALUES "
                "('qualified-negative', 15, 1, 'qualified', NULL, 0, 0), "
                "('bad-positive', 15, 2, 'excessive_gap', 3.0, 1, 0), "
                "('bad-negative', 15, 3, 'missing_endpoint', NULL, 0, 0)) "
                "AS t(event_id, horizon_min, ts_event, coverage_status, "
                "resolution_min, reject, break)"
            )
            con.close()

            loaded = refit_calibration.load_dataframe(
                str(path), "training_events_v1", 15
            )

            self.assertEqual(loaded["event_id"].tolist(), ["qualified-negative"])
            self.assertTrue(loaded["resolution_min"].isna().all())
            self.assertEqual(loaded["reject"].tolist(), [0])
            self.assertEqual(loaded["break"].tolist(), [0])

    def test_load_dataframe_fails_closed_without_coverage_status(self) -> None:
        duckdb = __import__("duckdb")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy.duckdb"
            con = duckdb.connect(str(path))
            con.execute(
                "CREATE TABLE training_events_v1 AS SELECT 15 AS horizon_min, "
                "1 AS ts_event, 0 AS reject"
            )
            con.close()

            with self.assertRaisesRegex(ValueError, "coverage_status"):
                refit_calibration.load_dataframe(
                    str(path), "training_events_v1", 15
                )


if __name__ == "__main__":
    unittest.main()