"""Unit tests for services/external_data/ml_regime_validation.py.

Tests are structured as:
  1. Module imports correctly (lazy guard in the script resolves to the real fn).
  2. _parse_date_range correctness and error handling.
  3. _apply_signal_filter behaviour.
  4. _validate_feature_column whitelist enforcement.
  5. run_ml_regime_validation on a small synthetic SQLite database:
       - no candidate_id (year-based split, no filter)
       - with candidate_id (registration-driven split + threshold)
       - empty OOS window → error report
       - zero signal events → error report
  6. write_ml_regime_validation_report writes valid JSON.
  7. Statistical validity block in the report passes round-trip through
     verdict_from_dict (tamper-proof recompute used by record_stage_result).
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Helpers: tiny in-memory / temp SQLite DB with known statistics
# ---------------------------------------------------------------------------


def _make_synthetic_db(
    path: str | Path,
    *,
    n_train: int = 200,
    n_test: int = 100,
    train_start: str = "2025-03-04",
    train_end: str = "2025-11-30",
    test_start: str = "2025-12-01",
    test_end: str = "2026-04-30",
    q1_break_rate: float = 0.85,
    baseline_break_rate: float = 0.58,
    threshold: float = 2.57,
    rng_seed: int = 0,
) -> None:
    """Create a synthetic pivot_events-like DB with controlled statistics.

    Events with distance_bps <= threshold have reject determined by
    q1_break_rate; all other events have reject determined so the overall
    reject rate matches (1 - baseline_break_rate).
    """
    import random

    random.seed(rng_seed)
    conn = sqlite3.connect(str(path))
    conn.execute("""
        CREATE TABLE touch_events (
            event_id      TEXT PRIMARY KEY,
            symbol        TEXT NOT NULL,
            ts_event      INTEGER NOT NULL,
            distance_bps  REAL
        )
    """)
    conn.execute("""
        CREATE TABLE event_labels (
            event_id    TEXT NOT NULL,
            horizon_min INTEGER NOT NULL,
            reject      INTEGER,
            PRIMARY KEY (event_id, horizon_min)
        )
    """)

    # Build a simple list of dates within each window.
    import datetime as dt

    def date_range(start: str, end: str, n: int) -> list[str]:
        s = dt.date.fromisoformat(start)
        e = dt.date.fromisoformat(end)
        span = (e - s).days
        step = max(1, span // n)
        dates = []
        for i in range(n):
            d = s + dt.timedelta(days=i * step)
            if d > e:
                d = e
            dates.append(d.isoformat())
        return dates

    def ts_from_date(d: str) -> int:
        return int(dt.datetime.fromisoformat(d + "T12:00:00").timestamp() * 1000)

    rows = []
    for period, n, dates_fn in [
        ("train", n_train, date_range(train_start, train_end, n_train)),
        ("test", n_test, date_range(test_start, test_end, n_test)),
    ]:
        # Alternate between "Q1" (small dist) and "rest" events.
        n_q1 = n // 2
        for i in range(n):
            eid = f"{period}-{i:04d}"
            is_q1 = i < n_q1
            dist = threshold * 0.5 if is_q1 else threshold * 2.0
            br = q1_break_rate if is_q1 else baseline_break_rate
            reject = 0 if random.random() < br else 1
            rows.append((eid, "SPY", ts_from_date(dates_fn[i]), dist, reject))

    conn.executemany(
        "INSERT INTO touch_events VALUES (?, ?, ?, ?)",
        [(r[0], r[1], r[2], r[3]) for r in rows],
    )
    conn.executemany(
        "INSERT INTO event_labels VALUES (?, 5, ?)",
        [(r[0], r[4]) for r in rows],
    )
    conn.commit()
    conn.close()


def _make_registration_file(
    reg_dir: Path,
    *,
    candidate_id: str = "test-cand-001",
    threshold: float = 2.57,
    train_period: str = "2025-03-04 to 2025-11-30",
    test_period: str = "2025-12-01 to 2026-04-30",
) -> Path:
    """Write a minimal valid registration JSON with a correct hash."""
    from services.research_protocol.registration import compute_registration_hash
    import hashlib, datetime as dt

    payload = {
        "candidate_id": candidate_id,
        "registration_timestamp": "2026-05-04T12:00:00Z",
        "git_commit_sha": "a" * 40,
        "hypothesis": {
            "mechanism": "test signal",
            "predicted_direction": "long",
            "why_might_fail": "overfitting",
            "citations": ["test"],
        },
        "features": [
            {
                "name": "distance_bps",
                "description": "test feature",
                "input_columns": ["touch_price", "level_price"],
                "lookback_days": 0,
            }
        ],
        "thresholds": [
            {
                "name": "q1_threshold",
                "kind": "fixed",
                "value": threshold,
                "description": "test threshold",
            }
        ],
        "transformations": {
            "allowed": [],
            "forbidden_unless_listed": [],
        },
        "forbidden_changes": ["threshold_definition"],
        "falsification": {"stage_1": "reject_pct < 20%", "stage_2": "delta >= -15pp"},
        "datasets": {
            "symbol": "SPY",
            "primary_db": "data/pivot_events.sqlite",
            "table": "touch_events",
            "label_table": "event_labels",
            "label_horizon_min": 5,
            "train_period": train_period,
            "test_period": test_period,
        },
        "horizon_days": 5,
        "random_seed": 42,
        "stages_required": [1, 2],
    }
    payload["registration_hash"] = compute_registration_hash(payload)
    path = reg_dir / f"{candidate_id}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# 1. Module import
# ---------------------------------------------------------------------------


class TestModuleImport(unittest.TestCase):
    def test_imports_without_error(self) -> None:
        from services.external_data.ml_regime_validation import (
            ValidationResult,
            run_ml_regime_validation,
            write_ml_regime_validation_report,
        )
        self.assertIsNotNone(run_ml_regime_validation)
        self.assertIsNotNone(write_ml_regime_validation_report)

    def test_defaults_exported(self) -> None:
        from services.external_data.ml_regime_validation import (
            DEFAULT_TEST_YEAR,
            DEFAULT_TRAIN_YEARS,
        )
        self.assertIsInstance(DEFAULT_TRAIN_YEARS, list)
        self.assertIsInstance(DEFAULT_TEST_YEAR, str)

    def test_script_lazy_guard_resolves_to_callable(self) -> None:
        """The lazy import guard in run_ml_regime_validation.py must now
        resolve to the real function, not None."""
        import importlib, types
        # Force reload so the guard re-executes.
        import scripts.run_ml_regime_validation as script_mod  # noqa: F401 — import side-effect
        # Access the module-level name set by the guard.
        import scripts.run_ml_regime_validation as m
        self.assertIsNotNone(m.run_ml_regime_validation)
        self.assertTrue(callable(m.run_ml_regime_validation))


# ---------------------------------------------------------------------------
# 2. _parse_date_range
# ---------------------------------------------------------------------------


class TestParseDateRange(unittest.TestCase):
    def _fn(self, s: str):
        from services.external_data.ml_regime_validation import _parse_date_range
        return _parse_date_range(s)

    def test_valid(self) -> None:
        start, end = self._fn("2025-12-01 to 2026-04-30")
        self.assertEqual(start, "2025-12-01")
        self.assertEqual(end, "2026-04-30")

    def test_leading_trailing_spaces(self) -> None:
        start, end = self._fn("  2025-01-01   to   2025-06-30  ")
        self.assertEqual(start, "2025-01-01")
        self.assertEqual(end, "2025-06-30")

    def test_invalid_raises(self) -> None:
        with self.assertRaises(ValueError):
            self._fn("2025-01-01")


# ---------------------------------------------------------------------------
# 3. _apply_signal_filter
# ---------------------------------------------------------------------------


class TestApplySignalFilter(unittest.TestCase):
    def _fn(self, arr, threshold):
        import numpy as np
        from services.external_data.ml_regime_validation import _apply_signal_filter
        return _apply_signal_filter(np.array(arr, dtype=float), threshold)

    def test_below_threshold_passes(self) -> None:
        mask = self._fn([1.0, 2.5, 3.0], 2.57)
        self.assertEqual(list(mask), [True, True, False])

    def test_exact_boundary_passes(self) -> None:
        mask = self._fn([2.57], 2.57)
        self.assertTrue(mask[0])

    def test_all_pass(self) -> None:
        mask = self._fn([0.1, 0.2], 100.0)
        self.assertTrue(all(mask))

    def test_none_pass(self) -> None:
        mask = self._fn([5.0, 6.0], 2.57)
        self.assertFalse(any(mask))


# ---------------------------------------------------------------------------
# 4. _validate_feature_column
# ---------------------------------------------------------------------------


class TestValidateFeatureColumn(unittest.TestCase):
    def _fn(self, col: str) -> str:
        from services.external_data.ml_regime_validation import _validate_feature_column
        return _validate_feature_column(col)

    def test_allowed_column(self) -> None:
        self.assertEqual(self._fn("distance_bps"), "distance_bps")

    def test_disallowed_column_raises(self) -> None:
        with self.assertRaises(ValueError):
            self._fn("evil_column; DROP TABLE touch_events")

    def test_unknown_column_raises(self) -> None:
        with self.assertRaises(ValueError):
            self._fn("nonexistent_col")


# ---------------------------------------------------------------------------
# 5. run_ml_regime_validation — synthetic DB
# ---------------------------------------------------------------------------


class TestRunMlRegimeValidation(unittest.TestCase):
    def setUp(self) -> None:
        import os
        self._tmp = tempfile.TemporaryDirectory()
        self._tmpdir = Path(self._tmp.name)
        self._db_path = self._tmpdir / "events.sqlite"
        _make_synthetic_db(self._db_path)

        # Point the entire protocol root at the temp dir via the environment
        # variable that _paths.py honours.  This covers registrations_dir(),
        # validation_ladder_state_path(), etc. without any monkey-patching.
        self._orig_protocol_root = os.environ.get("PIVOTQUANT_RESEARCH_PROTOCOL_ROOT")
        os.environ["PIVOTQUANT_RESEARCH_PROTOCOL_ROOT"] = str(self._tmpdir)
        self._reg_dir = self._tmpdir / "registrations"
        self._reg_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        import os
        if self._orig_protocol_root is None:
            os.environ.pop("PIVOTQUANT_RESEARCH_PROTOCOL_ROOT", None)
        else:
            os.environ["PIVOTQUANT_RESEARCH_PROTOCOL_ROOT"] = self._orig_protocol_root
        self._tmp.cleanup()

    def test_no_candidate_id_runs(self) -> None:
        from services.external_data.ml_regime_validation import run_ml_regime_validation

        result = run_ml_regime_validation(
            "SPY", ["2023", "2024"], "2025",
            db_path=str(self._db_path),
        )
        self.assertIn(result.report["status"], {"pass", "warn", "fail", "error"})
        self.assertIn("statistical_validity", result.report)

    def test_with_candidate_id_uses_registration_period(self) -> None:
        from services.external_data.ml_regime_validation import run_ml_regime_validation

        _make_registration_file(self._reg_dir)
        result = run_ml_regime_validation(
            "SPY", ["2023", "2024"], "2025",
            candidate_id="test-cand-001",
            db_path=str(self._db_path),
        )
        rpt = result.report
        # Period from registration, not from test_year.
        self.assertIn("2025-12-01", rpt["test_period"])
        self.assertIn("statistical_validity", rpt)
        sv = rpt["statistical_validity"]
        self.assertIn("statistical_pass", sv)
        self.assertIn("n_obs", sv)
        self.assertIn("ci_lower", sv)
        self.assertIn("ci_upper", sv)
        self.assertIn("permutation_p_value", sv)

    def test_report_has_required_keys(self) -> None:
        from services.external_data.ml_regime_validation import run_ml_regime_validation

        result = run_ml_regime_validation(
            "SPY", ["2023", "2024"], "2025",
            db_path=str(self._db_path),
        )
        for key in (
            "status", "validated", "symbol", "test_period",
            "n_all_oos", "n_signal_oos",
            "baseline_break_rate", "signal_break_rate",
            "delta_break_rate", "statistical_validity",
        ):
            self.assertIn(key, result.report, f"missing key: {key}")

    def test_empty_oos_window_returns_error(self) -> None:
        from services.external_data.ml_regime_validation import run_ml_regime_validation

        result = run_ml_regime_validation(
            "SPY", ["2020"], "2000",  # year with no data
            db_path=str(self._db_path),
        )
        self.assertEqual(result.report["status"], "error")
        self.assertFalse(result.report["validated"])

    def test_zero_signal_events_returns_error(self) -> None:
        from services.external_data.ml_regime_validation import run_ml_regime_validation

        # Make a registration with a threshold that matches nothing.
        _make_registration_file(
            self._reg_dir,
            candidate_id="test-zero-001",
            threshold=0.00001,  # below all distance_bps values
        )
        result = run_ml_regime_validation(
            "SPY", ["2023", "2024"], "2025",
            candidate_id="test-zero-001",
            db_path=str(self._db_path),
        )
        self.assertEqual(result.report["status"], "error")
        self.assertFalse(result.report["validated"])

    def test_statistical_validity_block_roundtrips(self) -> None:
        """The stored statistical_validity block must survive verdict_from_dict
        recomputation (used by record_stage_result to defeat tampering)."""
        from services.external_data.ml_regime_validation import run_ml_regime_validation
        from services.research_protocol.statistical_guard import verdict_from_dict

        result = run_ml_regime_validation(
            "SPY", ["2023", "2024"], "2025",
            db_path=str(self._db_path),
        )
        sv = result.report.get("statistical_validity", {})
        # verdict_from_dict must not raise.
        verdict = verdict_from_dict(sv)
        # The recomputed verdict agrees with the stored values.
        self.assertEqual(verdict.statistical_pass, sv["statistical_pass"])
        self.assertEqual(verdict.metrics_suppressed, sv["metrics_suppressed"])

    def test_determinism(self) -> None:
        """Two calls with the same inputs produce identical statistical results."""
        from services.external_data.ml_regime_validation import run_ml_regime_validation

        r1 = run_ml_regime_validation("SPY", ["2023"], "2025",
                                      db_path=str(self._db_path))
        r2 = run_ml_regime_validation("SPY", ["2023"], "2025",
                                      db_path=str(self._db_path))
        sv1 = r1.report.get("statistical_validity", {})
        sv2 = r2.report.get("statistical_validity", {})
        self.assertEqual(sv1.get("ci_lower"), sv2.get("ci_lower"))
        self.assertEqual(sv1.get("permutation_p_value"),
                         sv2.get("permutation_p_value"))


# ---------------------------------------------------------------------------
# 6. write_ml_regime_validation_report
# ---------------------------------------------------------------------------


class TestWriteReport(unittest.TestCase):
    def test_writes_valid_json(self) -> None:
        from services.external_data.ml_regime_validation import (
            write_ml_regime_validation_report,
        )
        from services.external_data import ml_regime_validation as mod

        report = {
            "status": "pass",
            "validated": True,
            "symbol": "SPY",
            "candidate_id": "test-cand",
            "test_period": "2025-12-01 to 2026-04-30",
            "dataset_identifier": "SPY_2025-12-01_to_2026-04-30",
            "statistical_validity": {"stage": 2, "n_obs": 500},
        }
        with tempfile.TemporaryDirectory() as tmp:
            orig = mod._REPORT_DIR
            mod._REPORT_DIR = Path(tmp) / "reports"
            try:
                path = write_ml_regime_validation_report(report, stem="test-report")
                # Read while the temp dir still exists.
                self.assertTrue(path.exists())
                loaded = json.loads(path.read_text())
            finally:
                mod._REPORT_DIR = orig

        self.assertEqual(loaded["status"], "pass")
        self.assertEqual(loaded["candidate_id"], "test-cand")

    def test_custom_stem_used(self) -> None:
        from services.external_data.ml_regime_validation import (
            write_ml_regime_validation_report,
        )
        from services.external_data import ml_regime_validation as mod

        with tempfile.TemporaryDirectory() as tmp:
            orig = mod._REPORT_DIR
            mod._REPORT_DIR = Path(tmp)
            try:
                path = write_ml_regime_validation_report({"status": "pass"}, stem="my-stem")
            finally:
                mod._REPORT_DIR = orig

        self.assertEqual(path.name, "my-stem.json")


if __name__ == "__main__":
    unittest.main()
