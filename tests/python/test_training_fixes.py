#!/usr/bin/env python3
"""Tests for Phase 2-6 training and governance improvements."""

from __future__ import annotations

import importlib.util
import math
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


class TestLabelShiftCorrection(unittest.TestCase):
    """Tests for ml/label_shift.py."""

    @classmethod
    def setUpClass(cls):
        cls.ls = _load_module("ml.label_shift", REPO_ROOT / "ml" / "label_shift.py")

    def test_no_shift_identity(self):
        """When pi_train == pi_current, output equals input."""
        for p in (0.3, 0.5, 0.7, 0.9):
            result = self.ls.correct_prior_shift(p, 0.6, 0.6)
            self.assertAlmostEqual(result, p, places=5)

    def test_downward_correction_when_rate_drops(self):
        """When pi_current < pi_train, reject prob should decrease."""
        p_raw = 0.75
        corrected = self.ls.correct_prior_shift(p_raw, pi_train=0.6, pi_current=0.3)
        self.assertLess(corrected, p_raw)

    def test_upward_correction_when_rate_rises(self):
        """When pi_current > pi_train, reject prob should increase."""
        p_raw = 0.4
        corrected = self.ls.correct_prior_shift(p_raw, pi_train=0.3, pi_current=0.6)
        self.assertGreater(corrected, p_raw)

    def test_output_clamped(self):
        """Output must be within (0, 1) even for extreme inputs."""
        result = self.ls.correct_prior_shift(0.999999, 0.9, 0.01)
        self.assertGreater(result, 0.0)
        self.assertLess(result, 1.0)
        result2 = self.ls.correct_prior_shift(0.000001, 0.01, 0.9)
        self.assertGreater(result2, 0.0)
        self.assertLess(result2, 1.0)

    def test_formula_correctness(self):
        """Verify the Bayes odds-ratio formula numerically."""
        p, pi_t, pi_c = 0.6, 0.5, 0.25
        r = (pi_c / pi_t) / ((1 - pi_c) / (1 - pi_t))
        expected = p * r / (p * r + (1 - p))
        result = self.ls.correct_prior_shift(p, pi_t, pi_c)
        self.assertAlmostEqual(result, expected, places=8)

    def test_rolling_class_rate_insufficient_rows(self):
        """rolling_class_rate returns None when rows < min_rows."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db_path = f.name
        try:
            con = sqlite3.connect(db_path)
            con.execute("""
                CREATE TABLE ml_predictions (
                    ts_event INTEGER, horizon_min INTEGER,
                    reject INTEGER, break_col INTEGER
                )
            """)
            # Only 5 rows — below default min_rows=20
            now_ms = int(__import__("time").time() * 1000)
            for i in range(5):
                con.execute(
                    "INSERT INTO ml_predictions VALUES (?,?,?,?)",
                    (now_ms - i * 3600 * 1000, 15, 1, 0),
                )
            con.commit()
            con.close()
            result = self.ls.rolling_class_rate(
                db_path, target="reject", horizon=15, window_days=30, min_rows=20
            )
            self.assertIsNone(result)
        finally:
            os.unlink(db_path)

    def test_rolling_class_rate_correct_value(self):
        """rolling_class_rate returns correct ratio when enough rows."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db_path = f.name
        try:
            con = sqlite3.connect(db_path)
            con.execute("""
                CREATE TABLE ml_predictions (
                    ts_event INTEGER, horizon_min INTEGER,
                    reject INTEGER, break INTEGER
                )
            """)
            now_ms = int(__import__("time").time() * 1000)
            for i in range(40):
                rej = 1 if i % 4 == 0 else 0  # 25% reject rate
                con.execute(
                    "INSERT INTO ml_predictions VALUES (?,?,?,?)",
                    (now_ms - i * 3600 * 1000, 15, rej, 0),
                )
            con.commit()
            con.close()
            rate = self.ls.rolling_class_rate(
                db_path, target="reject", horizon=15, window_days=30, min_rows=20
            )
            self.assertIsNotNone(rate)
            self.assertAlmostEqual(rate, 0.25, places=3)
        finally:
            os.unlink(db_path)


class TestExponentialDecay(unittest.TestCase):
    """Tests for the time-decay weight computation in train_rf_artifacts."""

    def test_decay_weights_monotone_decreasing(self):
        """Older samples must have strictly lower weight."""
        import numpy as np
        half_life = 45.0
        age_days = np.array([0, 10, 30, 60, 90, 180], dtype=float)
        weights = np.exp(-np.log(2) / half_life * age_days)
        for i in range(len(weights) - 1):
            self.assertGreater(weights[i], weights[i + 1])

    def test_weight_at_half_life(self):
        """Weight at exactly half_life days old should be ~0.5."""
        import numpy as np
        half_life = 45.0
        w = float(np.exp(-np.log(2) / half_life * half_life))
        self.assertAlmostEqual(w, 0.5, places=5)

    def test_weight_at_zero_age(self):
        """Most recent event must have weight 1.0."""
        import numpy as np
        half_life = 45.0
        w = float(np.exp(-np.log(2) / half_life * 0.0))
        self.assertAlmostEqual(w, 1.0, places=8)


class TestSelectCalibrationDates(unittest.TestCase):
    """Tests for select_calibration_dates in train_rf_artifacts.py."""

    @classmethod
    def setUpClass(cls):
        cls.mod = _load_module(
            "train_rf_artifacts",
            REPO_ROOT / "scripts" / "train_rf_artifacts.py",
        )
        import pandas as pd
        cls.pd = pd

    def _make_df(self, n_days: int, regime_sequence: list[int] | None = None):
        """Build a minimal DataFrame with event_date_et and regime_type columns."""
        pd = self.pd
        today = date(2026, 4, 27)
        rows = []
        for i in range(n_days):
            d = today - timedelta(days=n_days - 1 - i)
            regime = regime_sequence[i] if regime_sequence else 3  # compression
            rows.append({"event_date_et": d, "regime_type": regime})
        return pd.DataFrame(rows * 10)  # 10 events per date

    def test_recent_days_mode_returns_last_n(self):
        df = self._make_df(30)
        result = self.mod.select_calibration_dates(df, calib_days=10, calib_mode="recent_days")
        self.assertEqual(len(result), 10)
        dates = sorted(df["event_date_et"].unique())
        self.assertEqual(result, set(dates[-10:]))

    def test_recent_days_empty_df(self):
        pd = self.pd
        df = pd.DataFrame({"event_date_et": [], "regime_type": []})
        result = self.mod.select_calibration_dates(df, calib_days=10, calib_mode="recent_days")
        self.assertEqual(result, set())

    def test_regime_matched_falls_back_when_too_few_rows(self):
        """When regime-matched rows < calib_min_rows, fall back to recent_days."""
        pd = self.pd
        today = date(2026, 4, 27)
        rows = []
        for i in range(30):
            d = today - timedelta(days=29 - i)
            regime = 1 if i >= 25 else 3  # last 5 days are expansion, rest compression
            rows.append({"event_date_et": d, "regime_type": regime})
        df = pd.DataFrame(rows)  # only 1 row per date → regime-matched gives <5 rows
        result = self.mod.select_calibration_dates(
            df,
            calib_days=5,
            calib_mode="regime_matched",
            calib_lookback_days=30,
            calib_min_rows=10,  # >5, so fallback triggers
        )
        # Should fall back to last 5 dates regardless of regime
        all_dates = sorted(df["event_date_et"].unique())
        self.assertEqual(result, set(all_dates[-5:]))


class TestGovernanceConsecutiveRejections(unittest.TestCase):
    """Tests for consecutive_rejections counter in model_governance.py."""

    @classmethod
    def setUpClass(cls):
        cls.mod = _load_module(
            "model_governance",
            REPO_ROOT / "scripts" / "model_governance.py",
        )

    def test_empty_state_has_counter(self):
        state = self.mod.empty_state()
        self.assertIn("consecutive_rejections", state)
        self.assertEqual(state["consecutive_rejections"], 0)

    def test_load_state_backfills_missing_counter(self):
        """load_state must add consecutive_rejections=0 for legacy state files."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            import json
            json.dump({"schema_version": 1, "active_version": "v100", "history": []}, f)
            path = Path(f.name)
        try:
            state = self.mod.load_state(path)
            self.assertIn("consecutive_rejections", state)
            self.assertEqual(state["consecutive_rejections"], 0)
        finally:
            path.unlink(missing_ok=True)

    def test_persist_increments_on_rejection(self):
        """_persist_state_and_ops must increment counter on rejected action."""
        import json
        state = self.mod.empty_state()
        result = {"action": "rejected", "reason": "test", "gate_failures": []}
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            path = Path(f.name)
        try:
            self.mod._persist_state_and_ops(path, state, None, result)
            self.assertEqual(state["consecutive_rejections"], 1)
            self.mod._persist_state_and_ops(path, state, None, result)
            self.assertEqual(state["consecutive_rejections"], 2)
        finally:
            path.unlink(missing_ok=True)

    def test_persist_resets_on_promotion(self):
        """_persist_state_and_ops must reset counter on promoted action."""
        import json
        state = self.mod.empty_state()
        state["consecutive_rejections"] = 5
        result = {"action": "promoted", "reason": "test", "gate_failures": []}
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            path = Path(f.name)
        try:
            self.mod._persist_state_and_ops(path, state, None, result)
            self.assertEqual(state["consecutive_rejections"], 0)
        finally:
            path.unlink(missing_ok=True)

    def test_escalation_warning_emitted_at_threshold(self):
        """result should contain escalation_warning when counter >= threshold."""
        state = self.mod.empty_state()
        state["consecutive_rejections"] = self.mod.DEFAULT_ESCALATION_THRESHOLD - 1
        result = {"action": "rejected", "reason": "test", "gate_failures": []}
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            path = Path(f.name)
        try:
            self.mod._persist_state_and_ops(path, state, None, result)
            self.assertIn("escalation_warning", result)
        finally:
            path.unlink(missing_ok=True)


class TestDetectRegimeDrift(unittest.TestCase):
    """Tests for detect_regime_drift in model_governance.py."""

    @classmethod
    def setUpClass(cls):
        cls.mod = _load_module(
            "model_governance",
            REPO_ROOT / "scripts" / "model_governance.py",
        )

    def _make_db(self, horizon: int, reject_count: int, total: int) -> str:
        f = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        db_path = f.name
        f.close()
        now_ms = int(__import__("time").time() * 1000)
        con = sqlite3.connect(db_path)
        con.execute("""
            CREATE TABLE ml_predictions (
                ts_event INTEGER, horizon_min INTEGER,
                reject INTEGER, break INTEGER
            )
        """)
        for i in range(total):
            rej = 1 if i < reject_count else 0
            con.execute(
                "INSERT INTO ml_predictions VALUES (?,?,?,?)",
                (now_ms - i * 3600 * 1000, horizon, rej, 0),
            )
        con.commit()
        con.close()
        return db_path

    def test_ok_when_rate_close_to_train(self):
        db_path = self._make_db(horizon=15, reject_count=30, total=60)
        try:
            manifest = {"stats": {"15": {"reject": {"reject_rate": 0.50}}}}
            result = self.mod.detect_regime_drift(
                manifest,
                db_path=db_path,
                window_days=30,
                warn_delta=0.15,
                critical_delta=0.30,
                min_rows=20,
                horizons=[15],
            )
            self.assertEqual(result["status"], "ok")
        finally:
            os.unlink(db_path)

    def test_warn_when_rate_diverges(self):
        db_path = self._make_db(horizon=15, reject_count=10, total=60)
        try:
            manifest = {"stats": {"15": {"reject": {"reject_rate": 0.60}}}}
            result = self.mod.detect_regime_drift(
                manifest,
                db_path=db_path,
                window_days=30,
                warn_delta=0.15,
                critical_delta=0.30,
                min_rows=20,
                horizons=[15],
            )
            self.assertIn(result["status"], ("warn", "critical"))
        finally:
            os.unlink(db_path)

    def test_insufficient_data_when_rows_below_min(self):
        db_path = self._make_db(horizon=15, reject_count=5, total=5)
        try:
            manifest = {"stats": {"15": {"reject": {"reject_rate": 0.60}}}}
            result = self.mod.detect_regime_drift(
                manifest,
                db_path=db_path,
                window_days=30,
                warn_delta=0.15,
                critical_delta=0.30,
                min_rows=20,
                horizons=[15],
            )
            self.assertIn(
                result["horizons"]["15"]["status"],
                ("insufficient_data", "error"),
            )
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
