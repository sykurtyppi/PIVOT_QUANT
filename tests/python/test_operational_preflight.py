from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from contextlib import contextmanager
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "operational_preflight.py"


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def write_manifest(
    path: Path,
    *,
    shadow_policy: bool,
    model_context: bool = True,
    create_artifacts: bool = True,
) -> None:
    payload = {
        "version": "test_v1",
        "feature_version": "v3",
        "trained_end_ts": 1_700_000_000_000,
        "models": {
            "reject": {"60": "rf_reject_60m_test.pkl"},
            "break": {"60": "rf_break_60m_test.pkl"},
        },
    }
    if model_context:
        payload["model_context"] = {
            "symbols": ["SPY"],
            "default_symbol": "SPY",
            "targets": ["reject", "break"],
            "cost_model_version": "rt_cost_v1",
            "trade_cost_bps": 1.3,
            "training_view": "ml_training_events",
            "source_duckdb_path": "data/pivot_training.duckdb",
        }
    if shadow_policy:
        payload["shadow_policies"] = {
            "model_side_margin_v1": {
                "policy_name": "model_side_margin_v1",
                "horizon": 60,
                "reject": {"status": "ready", "reference_threshold": 0.7, "margin_cutoff": 0.05},
                "break": {"status": "ready", "reference_threshold": 0.8, "margin_cutoff": 0.04},
            }
        }
    path.write_text(json.dumps(payload), encoding="utf-8")
    if create_artifacts:
        for horizons in payload["models"].values():
            for filename in horizons.values():
                (path.parent / filename).write_bytes(b"fixture")


class OperationalPreflightTest(unittest.TestCase):
    def test_manifest_without_shadow_policy_fails_when_shadow_logging_enabled(self):
        module = load_module("operational_preflight_missing_policy_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            model_dir = tmp / "models"
            model_dir.mkdir()
            write_manifest(model_dir / "manifest_runtime_latest.json", shadow_policy=False)

            results = module.check_runtime_manifest(
                {
                    "RF_MODEL_DIR": str(model_dir),
                    "RF_ACTIVE_MANIFEST": "manifest_runtime_latest.json",
                    "ML_MODEL_SIDE_MARGIN_SHADOW_MODE": "log",
                }
            )

            self.assertIn("FAIL", {result.status for result in results})
            self.assertTrue(any("missing shadow policy" in result.message for result in results))

    def test_manifest_without_model_context_fails(self):
        module = load_module("operational_preflight_missing_context_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            model_dir = tmp / "models"
            model_dir.mkdir()
            write_manifest(model_dir / "manifest_runtime_latest.json", shadow_policy=True, model_context=False)

            results = module.check_runtime_manifest(
                {
                    "RF_MODEL_DIR": str(model_dir),
                    "RF_ACTIVE_MANIFEST": "manifest_runtime_latest.json",
                    "ML_MODEL_SIDE_MARGIN_SHADOW_MODE": "log",
                }
            )

            self.assertIn("FAIL", {result.status for result in results})
            self.assertTrue(any("missing model_context fields" in result.message for result in results))

    def test_manifest_with_complete_shadow_policy_passes_policy_contract(self):
        module = load_module("operational_preflight_policy_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            model_dir = tmp / "models"
            model_dir.mkdir()
            write_manifest(model_dir / "manifest_runtime_latest.json", shadow_policy=True)

            results = module.check_runtime_manifest(
                {
                    "RF_MODEL_DIR": str(model_dir),
                    "RF_ACTIVE_MANIFEST": "manifest_runtime_latest.json",
                    "ML_MODEL_SIDE_MARGIN_SHADOW_MODE": "log",
                }
            )

            self.assertNotIn("FAIL", {result.status for result in results})
            self.assertTrue(any("configured for 60m" in result.message for result in results))

    def test_manifest_fails_when_model_artifact_path_is_missing(self):
        module = load_module("operational_preflight_missing_artifact_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            model_dir = tmp / "models"
            model_dir.mkdir()
            write_manifest(
                model_dir / "manifest_runtime_latest.json",
                shadow_policy=True,
                create_artifacts=False,
            )

            results = module.check_runtime_manifest(
                {
                    "RF_MODEL_DIR": str(model_dir),
                    "RF_ACTIVE_MANIFEST": "manifest_runtime_latest.json",
                    "ML_MODEL_SIDE_MARGIN_SHADOW_MODE": "log",
                }
            )

            self.assertIn("FAIL", {result.status for result in results})
            self.assertTrue(any("Runtime model artifact missing" in result.message for result in results))

    def test_shadow_policy_fails_when_policy_side_model_is_missing(self):
        module = load_module("operational_preflight_missing_side_model_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            model_dir = tmp / "models"
            model_dir.mkdir()
            manifest_path = model_dir / "manifest_runtime_latest.json"
            write_manifest(manifest_path, shadow_policy=True)
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            payload["models"].pop("break")
            manifest_path.write_text(json.dumps(payload), encoding="utf-8")

            results = module.check_runtime_manifest(
                {
                    "RF_MODEL_DIR": str(model_dir),
                    "RF_ACTIVE_MANIFEST": "manifest_runtime_latest.json",
                    "ML_MODEL_SIDE_MARGIN_SHADOW_MODE": "log",
                }
            )

            self.assertIn("FAIL", {result.status for result in results})
            self.assertTrue(any("needs break 60m model" in result.message for result in results))

    def test_database_shadow_rows_with_missing_policy_config_fail(self):
        module = load_module("operational_preflight_db_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute("CREATE TABLE prediction_log (id INTEGER PRIMARY KEY)")
                conn.execute("CREATE TABLE event_labels (event_id TEXT)")
                conn.execute(
                    """
                    CREATE TABLE shadow_emission_log (
                        id INTEGER PRIMARY KEY,
                        eligible INTEGER,
                        shadow_emit INTEGER,
                        ineligibility_reason TEXT
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO prediction_log (id) VALUES (1)"
                )
                conn.execute(
                    """
                    INSERT INTO shadow_emission_log (
                        eligible,
                        shadow_emit,
                        ineligibility_reason
                    ) VALUES (0, 0, 'missing_policy_config')
                    """
                )
                conn.commit()
            finally:
                conn.close()

            results = module.check_runtime_database(db_path)

            self.assertIn("FAIL", {result.status for result in results})
            self.assertTrue(any("missing_policy_config" in result.message for result in results))

    def test_database_predictions_with_empty_labels_fail_when_events_are_mature(self):
        module = load_module("operational_preflight_empty_labels_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            model_dir = tmp / "models"
            model_dir.mkdir()
            write_manifest(model_dir / "manifest_runtime_latest.json", shadow_policy=True)
            contract, manifest_results = module.load_manifest_contract(
                {
                    "RF_MODEL_DIR": str(model_dir),
                    "RF_ACTIVE_MANIFEST": "manifest_runtime_latest.json",
                    "ML_MODEL_SIDE_MARGIN_SHADOW_MODE": "log",
                }
            )
            self.assertIsNotNone(contract)
            self.assertNotIn("FAIL", {result.status for result in manifest_results})

            db_path = tmp / "runtime.sqlite"
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute("CREATE TABLE prediction_log (event_id TEXT, ts_prediction INTEGER)")
                conn.execute("CREATE TABLE touch_events (event_id TEXT, ts_event INTEGER)")
                conn.execute("CREATE TABLE bar_data (ts INTEGER)")
                conn.execute("CREATE TABLE event_labels (event_id TEXT, horizon_min INTEGER)")
                conn.execute("INSERT INTO touch_events VALUES ('evt1', 1000)")
                conn.execute("INSERT INTO prediction_log VALUES ('evt1', 2000)")
                conn.execute("INSERT INTO bar_data VALUES (?)", [1000 + 61 * 60 * 1000])
                conn.commit()
            finally:
                conn.close()

            results = module.check_runtime_database(db_path, contract=contract)

            self.assertIn("FAIL", {result.status for result in results})
            self.assertTrue(any("event_labels has 0 rows" in result.message for result in results))

    def test_database_predictions_with_missing_labels_table_fail_when_events_are_mature(self):
        module = load_module("operational_preflight_missing_labels_table_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            model_dir = tmp / "models"
            model_dir.mkdir()
            write_manifest(model_dir / "manifest_runtime_latest.json", shadow_policy=True)
            contract, _ = module.load_manifest_contract(
                {
                    "RF_MODEL_DIR": str(model_dir),
                    "RF_ACTIVE_MANIFEST": "manifest_runtime_latest.json",
                    "ML_MODEL_SIDE_MARGIN_SHADOW_MODE": "log",
                }
            )
            self.assertIsNotNone(contract)

            db_path = tmp / "runtime.sqlite"
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute("CREATE TABLE prediction_log (event_id TEXT, ts_prediction INTEGER)")
                conn.execute("CREATE TABLE touch_events (event_id TEXT, ts_event INTEGER)")
                conn.execute("CREATE TABLE bar_data (ts INTEGER)")
                conn.execute("INSERT INTO touch_events VALUES ('evt1', 1000)")
                conn.execute("INSERT INTO prediction_log VALUES ('evt1', 2000)")
                conn.execute("INSERT INTO bar_data VALUES (?)", [1000 + 61 * 60 * 1000])
                conn.commit()
            finally:
                conn.close()

            results = module.check_runtime_database(db_path, contract=contract)

            self.assertIn("FAIL", {result.status for result in results})
            self.assertTrue(any("event_labels table is missing" in result.message for result in results))

    def test_model_api_registry_mismatch_fails(self):
        module = load_module("operational_preflight_model_api_mismatch_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            model_dir = tmp / "models"
            model_dir.mkdir()
            write_manifest(model_dir / "manifest_runtime_latest.json", shadow_policy=True)
            contract, _ = module.load_manifest_contract(
                {
                    "RF_MODEL_DIR": str(model_dir),
                    "RF_ACTIVE_MANIFEST": "manifest_runtime_latest.json",
                    "ML_MODEL_SIDE_MARGIN_SHADOW_MODE": "log",
                }
            )
            self.assertIsNotNone(contract)

            results = module.check_model_api_payload_parity(
                contract,
                {"models": [{"id": "other", "version": "other_v"}]},
            )

            self.assertIn("FAIL", {result.status for result in results})
            self.assertTrue(any("does not contain active runtime version" in result.message for result in results))

    def test_healthy_aligned_fixtures_pass_core_contracts(self):
        module = load_module("operational_preflight_healthy_fixture_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            model_dir = tmp / "models"
            model_dir.mkdir()
            manifest_path = model_dir / "manifest_runtime_latest.json"
            write_manifest(manifest_path, shadow_policy=True)
            contract, manifest_results = module.load_manifest_contract(
                {
                    "RF_MODEL_DIR": str(model_dir),
                    "RF_ACTIVE_MANIFEST": "manifest_runtime_latest.json",
                    "ML_MODEL_SIDE_MARGIN_SHADOW_MODE": "log",
                }
            )
            self.assertIsNotNone(contract)

            db_path = tmp / "runtime.sqlite"
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute("CREATE TABLE prediction_log (event_id TEXT, ts_prediction INTEGER)")
                conn.execute("CREATE TABLE touch_events (event_id TEXT, ts_event INTEGER)")
                conn.execute("CREATE TABLE bar_data (ts INTEGER)")
                conn.execute("CREATE TABLE event_labels (event_id TEXT, horizon_min INTEGER)")
                conn.execute(
                    """
                    CREATE TABLE shadow_emission_log (
                        eligible INTEGER,
                        shadow_emit INTEGER,
                        ineligibility_reason TEXT
                    )
                    """
                )
                conn.execute("INSERT INTO touch_events VALUES ('evt1', 1000)")
                conn.execute("INSERT INTO prediction_log VALUES ('evt1', 2000)")
                conn.execute("INSERT INTO bar_data VALUES (?)", [1000 + 61 * 60 * 1000])
                conn.execute("INSERT INTO event_labels VALUES ('evt1', 60)")
                conn.execute("INSERT INTO shadow_emission_log VALUES (1, 1, '')")
                conn.commit()
            finally:
                conn.close()

            db_results = module.check_runtime_database(db_path, contract=contract)
            ml_results = module.check_ml_server_payload_parity(
                contract,
                {
                    "manifest_path": str(manifest_path),
                    "manifest": json.loads(manifest_path.read_text(encoding="utf-8")),
                    "models": {"reject": [60], "break": [60]},
                    "runtime_readiness": {
                        "model_context_present": True,
                        "shadow_policy_ready": True,
                        "target_horizon_coverage": {"reject": [60], "break": [60]},
                    },
                },
            )
            api_results = module.check_model_api_payload_parity(
                contract,
                {"models": [{"id": "metadata/test_v1", "version": "test_v1"}]},
                {
                    "baseline_compare": {
                        "metadata_mode": "explicit",
                        "supported_targets": ["reject", "break"],
                    }
                },
            )

            for result_set in (manifest_results, db_results, ml_results, api_results):
                self.assertNotIn("FAIL", {result.status for result in result_set})

    def test_model_api_parity_reads_baseline_summary_metadata(self):
        module = load_module("operational_preflight_baseline_summary_test", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            model_dir = Path(tmpdir) / "models"
            model_dir.mkdir()
            manifest_path = model_dir / "manifest_runtime_latest.json"
            write_manifest(manifest_path, shadow_policy=True)
            contract, _ = module.load_manifest_contract(
                {
                    "RF_MODEL_DIR": str(model_dir),
                    "RF_ACTIVE_MANIFEST": "manifest_runtime_latest.json",
                    "ML_MODEL_SIDE_MARGIN_SHADOW_MODE": "log",
                }
            )
            self.assertIsNotNone(contract)

            results = module.check_model_api_payload_parity(
                contract,
                {"models": [{"id": "metadata/test_v1", "version": "test_v1"}]},
                {
                    "baseline_compare": {
                        "summary": {
                            "metadata_mode": "explicit",
                            "supported_targets": ["reject", "break"],
                        }
                    }
                },
            )

            self.assertNotIn("FAIL", {result.status for result in results})

    # ── scoring freshness / backlog (silent-scorer-death detector) ──────────────
    @staticmethod
    @contextmanager
    def _scoring_env(**overrides):
        keys = (
            "SCORING_SETTLE_MINUTES",
            "SCORING_BACKLOG_WARN",
            "SCORING_BACKLOG_FAIL",
            "MARKET_DATA_STALE_MINUTES",
        )
        saved = {k: os.environ.get(k) for k in keys}
        try:
            for k in keys:
                os.environ.pop(k, None)
            for k, v in overrides.items():
                os.environ[k] = str(v)
            yield
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    @staticmethod
    def _build_scoring_db(
        db_path: Path,
        *,
        n_events: int,
        n_scored: int,
        newest_offset_min: float = 0.0,
        bar_age_min: float = 0.0,
        with_bar: bool = True,
    ) -> None:
        now = int(time.time() * 1000)
        # Newest event sits newest_offset_min in the past; events are 1 minute apart,
        # oldest first. newest_offset_min lets a test place ALL events far from wall-clock
        # to pin event-relative (not wall-clock) settle anchoring.
        newest_ts = now - int(newest_offset_min * 60_000)
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("CREATE TABLE touch_events (event_id TEXT PRIMARY KEY, ts_event INTEGER)")
            conn.execute("CREATE TABLE prediction_log (id INTEGER PRIMARY KEY, event_id TEXT)")
            conn.execute("CREATE TABLE bar_data (ts INTEGER)")
            for i in range(n_events):
                ts = newest_ts - (n_events - 1 - i) * 60_000
                conn.execute(
                    "INSERT INTO touch_events (event_id, ts_event) VALUES (?, ?)",
                    (f"evt{i:04d}", ts),
                )
            # Score the oldest n_scored events, leaving the most recent unscored.
            for i in range(n_scored):
                conn.execute(
                    "INSERT INTO prediction_log (event_id) VALUES (?)",
                    (f"evt{i:04d}",),
                )
            if with_bar:
                conn.execute("INSERT INTO bar_data (ts) VALUES (?)", (now - int(bar_age_min * 60_000),))
            conn.commit()
        finally:
            conn.close()

    def test_scoring_freshness_fails_on_unscored_backlog(self):
        module = load_module("operational_preflight_freshness_fail", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            self._build_scoring_db(db_path, n_events=100, n_scored=10)  # 90 unscored
            with self._scoring_env(SCORING_SETTLE_MINUTES=0, SCORING_BACKLOG_WARN=1, SCORING_BACKLOG_FAIL=50):
                results = module.check_scoring_freshness(db_path)
        self.assertIn("FAIL", {r.status for r in results})
        self.assertTrue(any("backlog" in r.message.lower() for r in results))

    def test_scoring_freshness_passes_when_current(self):
        module = load_module("operational_preflight_freshness_pass", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            self._build_scoring_db(db_path, n_events=100, n_scored=100)  # all scored
            with self._scoring_env(SCORING_SETTLE_MINUTES=0, SCORING_BACKLOG_WARN=5, SCORING_BACKLOG_FAIL=50):
                results = module.check_scoring_freshness(db_path)
        self.assertNotIn("FAIL", {r.status for r in results})
        self.assertTrue(any("Scoring current" in r.message for r in results))

    def test_scoring_freshness_fails_when_nothing_ever_scored(self):
        """A large matured backlog with an empty prediction_log is a stall FAIL. (The
        backlog must exceed backlog_fail — an empty log with only a tiny backlog is a
        healthy cold start, covered by test_scoring_freshness_cold_start_passes.)"""
        module = load_module("operational_preflight_freshness_none", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            self._build_scoring_db(db_path, n_events=20, n_scored=0)
            with self._scoring_env(SCORING_SETTLE_MINUTES=0, SCORING_BACKLOG_FAIL=10):
                results = module.check_scoring_freshness(db_path)
        self.assertIn("FAIL", {r.status for r in results})
        self.assertTrue(any("stalled" in r.message.lower() for r in results))

    def test_scoring_freshness_cold_start_passes(self):
        """Empty prediction_log + only events younger than the settle window is a healthy
        cold start, NOT a stall (regression guard for the false-FAIL branch)."""
        module = load_module("operational_preflight_freshness_cold", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            # 3 events all within the last 3 minutes; settle=10 -> none matured.
            self._build_scoring_db(db_path, n_events=3, n_scored=0)
            with self._scoring_env(SCORING_SETTLE_MINUTES=10, SCORING_BACKLOG_WARN=5, SCORING_BACKLOG_FAIL=50):
                results = module.check_scoring_freshness(db_path)
        self.assertNotIn("FAIL", {r.status for r in results})
        self.assertTrue(any("Scoring current" in r.message for r in results))

    def test_scoring_freshness_settle_window_is_event_relative(self):
        """settle>0 excludes only events within the window of the NEWEST event, and the
        anchor is the newest event (not wall-clock). Kills sign-flip and wall-clock-anchor
        mutations of mature_cutoff."""
        module = load_module("operational_preflight_freshness_settle", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            # 60 unscored events, newest is 1000 min in the past (far from wall-clock).
            # settle=10 -> newest 10 events excluded, 50 mature. Wall-clock anchoring would
            # count all 60 (all far older than now-10min).
            self._build_scoring_db(db_path, n_events=60, n_scored=0, newest_offset_min=1000)
            with self._scoring_env(SCORING_SETTLE_MINUTES=10, SCORING_BACKLOG_WARN=1, SCORING_BACKLOG_FAIL=1):
                results = module.check_scoring_freshness(db_path)
        fail_msgs = [r.message for r in results if r.status == "FAIL"]
        self.assertTrue(fail_msgs, "expected a FAIL for the matured backlog")
        self.assertIn("50 matured", fail_msgs[0])
        self.assertNotIn("60 matured", fail_msgs[0])

    def test_scoring_freshness_warn_band(self):
        """unscored_mature strictly between warn and fail -> WARN (kills WARN->PASS and
        swapped-threshold mutations)."""
        module = load_module("operational_preflight_freshness_warn", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            self._build_scoring_db(db_path, n_events=100, n_scored=90)  # 10 unscored
            with self._scoring_env(SCORING_SETTLE_MINUTES=0, SCORING_BACKLOG_WARN=5, SCORING_BACKLOG_FAIL=50):
                results = module.check_scoring_freshness(db_path)
        statuses = {r.status for r in results}
        self.assertNotIn("FAIL", statuses)
        self.assertIn("WARN", statuses)
        self.assertTrue(any("backlog building" in r.message.lower() for r in results))

    def test_scoring_freshness_fail_boundary_is_inclusive(self):
        """unscored_mature == backlog_fail -> FAIL (>= boundary, kills > mutation)."""
        module = load_module("operational_preflight_freshness_boundary", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            self._build_scoring_db(db_path, n_events=100, n_scored=50)  # exactly 50 unscored
            with self._scoring_env(SCORING_SETTLE_MINUTES=0, SCORING_BACKLOG_WARN=5, SCORING_BACKLOG_FAIL=50):
                results = module.check_scoring_freshness(db_path)
        self.assertIn("FAIL", {r.status for r in results})

    def test_scoring_freshness_cutoff_is_inclusive(self):
        """A single unscored event exactly at the mature cutoff (settle=0 -> cutoff ==
        newest_event) counts as matured. Kills the `<=` -> `<` off-by-one."""
        module = load_module("operational_preflight_freshness_cutoff", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            self._build_scoring_db(db_path, n_events=1, n_scored=0)
            with self._scoring_env(SCORING_SETTLE_MINUTES=0, SCORING_BACKLOG_WARN=1, SCORING_BACKLOG_FAIL=1):
                results = module.check_scoring_freshness(db_path)
        self.assertIn("FAIL", {r.status for r in results})

    def test_scoring_freshness_market_data_stale_warns(self):
        """An old bar_data row raises the market-data WARN; a fresh bar does not."""
        module = load_module("operational_preflight_freshness_market", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            stale = Path(tmpdir) / "stale.sqlite"
            self._build_scoring_db(stale, n_events=100, n_scored=100, bar_age_min=200)
            with self._scoring_env(SCORING_SETTLE_MINUTES=0, MARKET_DATA_STALE_MINUTES=120):
                stale_results = module.check_scoring_freshness(stale)
            fresh = Path(tmpdir) / "fresh.sqlite"
            self._build_scoring_db(fresh, n_events=100, n_scored=100, bar_age_min=1)
            with self._scoring_env(SCORING_SETTLE_MINUTES=0, MARKET_DATA_STALE_MINUTES=120):
                fresh_results = module.check_scoring_freshness(fresh)
        self.assertTrue(any("Market data age" in r.message and r.status == "WARN" for r in stale_results))
        self.assertFalse(any("Market data age" in r.message for r in fresh_results))

    def test_scoring_freshness_misordered_thresholds_warn(self):
        """warn > fail clamps and emits a misorder WARN, keeping the WARN tier reachable."""
        module = load_module("operational_preflight_freshness_misorder", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            self._build_scoring_db(db_path, n_events=100, n_scored=100)
            with self._scoring_env(SCORING_SETTLE_MINUTES=0, SCORING_BACKLOG_WARN=100, SCORING_BACKLOG_FAIL=50):
                results = module.check_scoring_freshness(db_path)
        self.assertTrue(any("misordered" in r.message.lower() for r in results))

    def test_scoring_freshness_skips_when_tables_missing(self):
        module = load_module("operational_preflight_freshness_skip", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute("CREATE TABLE touch_events (event_id TEXT, ts_event INTEGER)")
                conn.commit()  # no prediction_log
            finally:
                conn.close()
            with self._scoring_env():
                results = module.check_scoring_freshness(db_path)
        self.assertEqual({r.status for r in results}, {"SKIP"})

    def test_scoring_freshness_warns_when_columns_missing(self):
        module = load_module("operational_preflight_freshness_cols", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute("CREATE TABLE touch_events (event_id TEXT)")  # no ts_event
                conn.execute("CREATE TABLE prediction_log (event_id TEXT)")
                conn.commit()
            finally:
                conn.close()
            with self._scoring_env():
                results = module.check_scoring_freshness(db_path)
        self.assertIn("WARN", {r.status for r in results})
        self.assertNotIn("FAIL", {r.status for r in results})

    def test_scoring_freshness_skips_when_no_events(self):
        module = load_module("operational_preflight_freshness_empty", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "runtime.sqlite"
            self._build_scoring_db(db_path, n_events=0, n_scored=0, with_bar=False)
            with self._scoring_env():
                results = module.check_scoring_freshness(db_path)
        self.assertEqual({r.status for r in results}, {"SKIP"})


if __name__ == "__main__":
    unittest.main()
