"""Tests for ml/label_shift.py — inference-time prior-shift correction.

`correct_prior_shift()` runs on every ``/score`` request (server/ml_server.py),
so its no-op boundary, correction direction, input/output clamping, and
odds-ratio invertibility are pinned here. `rolling_class_rate()` supplies
``pi_current`` from the live SQLite population; its min-rows floor, window and
horizon filters, NULL handling, and fail-closed (``None``) paths are covered.

Stdlib-only (math + sqlite3), so this runs under the full tests/python
discovery without the heavy ML dependencies.
"""
from __future__ import annotations

import math
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ml.label_shift import correct_prior_shift, rolling_class_rate  # noqa: E402

EPS = 1e-6


class CorrectPriorShiftTests(unittest.TestCase):
    def test_noop_when_priors_equal(self) -> None:
        # pi_train == pi_current: no mismatch to correct, raw prob returned as-is.
        self.assertEqual(correct_prior_shift(0.7, 0.3, 0.3), 0.7)

    def test_non_finite_inputs_are_rejected(self) -> None:
        for values in (
            (math.nan, 0.3, 0.3),
            (0.4, math.inf, 0.3),
            (0.4, 0.3, -math.inf),
        ):
            with self.subTest(values=values), self.assertRaises(ValueError):
                correct_prior_shift(*values)

    def test_noop_within_relative_tolerance(self) -> None:
        # priors differ by ~1e-4 relative (inside rel_tol=1e-4) -> treated equal.
        self.assertEqual(correct_prior_shift(0.42, 0.30, 0.300003), 0.42)

    def test_exact_formula_downward(self) -> None:
        # r = (0.25/0.5) / (0.75/0.5) = 1/3 ; p' = 0.5r/(0.5r + 0.5) = 0.25
        self.assertAlmostEqual(correct_prior_shift(0.5, 0.5, 0.25), 0.25, places=9)

    def test_exact_formula_upward(self) -> None:
        # r = (0.5/0.25) / (0.5/0.75) = 3 ; p' = 0.5*3/(0.5*3 + 0.5) = 0.75
        self.assertAlmostEqual(correct_prior_shift(0.5, 0.25, 0.5), 0.75, places=9)

    def test_fewer_positives_pushes_probability_down(self) -> None:
        # pi_current < pi_train (model over-expects positives) -> correct downward.
        self.assertLess(correct_prior_shift(0.7, 0.5, 0.3), 0.7)

    def test_more_positives_pushes_probability_up(self) -> None:
        # pi_current > pi_train -> correct upward.
        self.assertGreater(correct_prior_shift(0.3, 0.3, 0.6), 0.3)

    def test_monotonic_in_raw_probability(self) -> None:
        # The correction is a monotone transform of p_raw at fixed priors.
        pi_t, pi_c = 0.5, 0.25
        xs = [0.05, 0.2, 0.4, 0.6, 0.8, 0.95]
        ys = [correct_prior_shift(x, pi_t, pi_c) for x in xs]
        self.assertEqual(ys, sorted(ys))
        for lo, hi in zip(ys, ys[1:]):
            self.assertLess(lo, hi)

    def test_output_always_within_eps_range(self) -> None:
        for p in (0.0, 1e-12, 0.5, 1.0, 1.5, -0.3):
            for pt in (0.01, 0.5, 0.99):
                for pc in (0.01, 0.5, 0.99):
                    out = correct_prior_shift(p, pt, pc)
                    self.assertGreaterEqual(out, EPS)
                    self.assertLessEqual(out, 1.0 - EPS)

    def test_raw_probability_is_clamped(self) -> None:
        # p_raw outside (0, 1) is clamped before the (here no-op) correction.
        self.assertEqual(correct_prior_shift(0.0, 0.3, 0.3), EPS)
        self.assertEqual(correct_prior_shift(1.0, 0.3, 0.3), 1.0 - EPS)
        self.assertEqual(correct_prior_shift(5.0, 0.3, 0.3), 1.0 - EPS)

    def test_degenerate_priors_do_not_raise(self) -> None:
        # pi at 0/1 clamps away from the div-by-zero / 0-over-0 singularities.
        # Both priors clamp to eps -> equal -> no-op, so 0.5 is returned.
        self.assertEqual(correct_prior_shift(0.5, 0.0, 0.0), 0.5)
        # Opposite extremes stay finite and in range.
        out = correct_prior_shift(0.5, 1.0, 0.0)
        self.assertGreaterEqual(out, EPS)
        self.assertLessEqual(out, 1.0 - EPS)

    def test_round_trip_inverts_the_odds_ratio(self) -> None:
        # Correcting a->b then b->a multiplies the odds by r then 1/r -> identity.
        p = 0.7
        once = correct_prior_shift(p, 0.3, 0.6)
        self.assertNotAlmostEqual(once, p, places=3)  # the forward step actually moved it
        back = correct_prior_shift(once, 0.6, 0.3)
        self.assertAlmostEqual(back, p, places=9)


class RollingClassRateTests(unittest.TestCase):
    def _make_db(self, rows) -> str:
        """rows: (event_id, ts_prediction_ms, horizon_min, reject, break_)."""
        path = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
        self.addCleanup(lambda: Path(path).unlink(missing_ok=True))
        con = sqlite3.connect(path)
        try:
            con.execute(
                "CREATE TABLE prediction_log (event_id TEXT, ts_prediction INTEGER)"
            )
            con.execute(
                "CREATE TABLE event_labels "
                "(event_id TEXT, horizon_min INTEGER, reject INTEGER, break INTEGER)"
            )
            con.executemany(
                "INSERT INTO prediction_log (event_id, ts_prediction) VALUES (?, ?)",
                [(eid, ts) for eid, ts, _, _, _ in rows],
            )
            con.executemany(
                "INSERT INTO event_labels (event_id, horizon_min, reject, break) "
                "VALUES (?, ?, ?, ?)",
                [(eid, hz, rej, brk) for eid, _, hz, rej, brk in rows],
            )
            con.commit()
        finally:
            con.close()
        return path

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def test_invalid_target_returns_none(self) -> None:
        db = self._make_db([("e1", self._now_ms(), 15, 1, 0)])
        self.assertIsNone(
            rolling_class_rate(db, target="foo", horizon=15, window_days=30, min_rows=1)
        )

    def test_below_min_rows_returns_none(self) -> None:
        now = self._now_ms()
        db = self._make_db([(f"e{i}", now, 15, 1, 0) for i in range(5)])
        self.assertIsNone(
            rolling_class_rate(db, target="reject", horizon=15, window_days=30, min_rows=20)
        )

    def test_computes_reject_rate(self) -> None:
        now = self._now_ms()
        rows = [(f"e{i}", now, 15, 1 if i < 3 else 0, 0) for i in range(10)]
        db = self._make_db(rows)
        rate = rolling_class_rate(db, target="reject", horizon=15, window_days=30, min_rows=5)
        self.assertAlmostEqual(rate, 0.3, places=9)

    def test_computes_break_rate(self) -> None:
        now = self._now_ms()
        rows = [(f"e{i}", now, 15, 0, 1 if i < 4 else 0) for i in range(10)]
        db = self._make_db(rows)
        rate = rolling_class_rate(db, target="break", horizon=15, window_days=30, min_rows=5)
        self.assertAlmostEqual(rate, 0.4, places=9)

    def test_window_excludes_old_rows(self) -> None:
        now = self._now_ms()
        old = now - 40 * 86400 * 1000  # 40 days old, outside a 30-day window
        rows = [(f"recent{i}", now, 15, 1, 0) for i in range(6)]
        rows += [(f"old{i}", old, 15, 0, 0) for i in range(100)]
        db = self._make_db(rows)
        rate = rolling_class_rate(db, target="reject", horizon=15, window_days=30, min_rows=5)
        self.assertAlmostEqual(rate, 1.0, places=9)  # only the 6 recent positives count

    def test_horizon_filter(self) -> None:
        now = self._now_ms()
        rows = [(f"h15_{i}", now, 15, 1, 0) for i in range(6)]
        rows += [(f"h60_{i}", now, 60, 0, 0) for i in range(6)]
        db = self._make_db(rows)
        self.assertAlmostEqual(
            rolling_class_rate(db, target="reject", horizon=15, window_days=30, min_rows=5),
            1.0, places=9,
        )
        self.assertAlmostEqual(
            rolling_class_rate(db, target="reject", horizon=60, window_days=30, min_rows=5),
            0.0, places=9,
        )

    def test_null_labels_excluded(self) -> None:
        now = self._now_ms()
        rows = [(f"e{i}", now, 15, 1, 0) for i in range(6)]
        rows += [(f"n{i}", now, 15, None, None) for i in range(100)]
        db = self._make_db(rows)
        rate = rolling_class_rate(db, target="reject", horizon=15, window_days=30, min_rows=5)
        self.assertAlmostEqual(rate, 1.0, places=9)  # the 100 NULL rows do not count

    def test_each_event_counts_once_across_model_versions(self) -> None:
        now = self._now_ms()
        path = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
        self.addCleanup(lambda: Path(path).unlink(missing_ok=True))
        con = sqlite3.connect(path)
        try:
            con.execute(
                "CREATE TABLE prediction_log "
                "(event_id TEXT, ts_prediction INTEGER, model_version TEXT)"
            )
            con.execute(
                "CREATE TABLE event_labels "
                "(event_id TEXT, horizon_min INTEGER, reject INTEGER, break INTEGER)"
            )
            con.executemany(
                "INSERT INTO prediction_log VALUES (?, ?, ?)",
                [
                    ("positive", now, "v1"),
                    ("positive", now, "v2"),
                    ("positive", now, "v3"),
                    ("negative", now, "v1"),
                ],
            )
            con.executemany(
                "INSERT INTO event_labels VALUES (?, ?, ?, ?)",
                [("positive", 15, 1, 0), ("negative", 15, 0, 1)],
            )
            con.commit()
        finally:
            con.close()

        rate = rolling_class_rate(
            path, target="reject", horizon=15, window_days=30, min_rows=2
        )
        self.assertAlmostEqual(rate, 0.5, places=9)

    def test_missing_tables_returns_none(self) -> None:
        # An empty db (no tables) -> query raises -> caught -> None (fail-closed).
        path = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
        self.addCleanup(lambda: Path(path).unlink(missing_ok=True))
        sqlite3.connect(path).close()
        self.assertIsNone(
            rolling_class_rate(path, target="reject", horizon=15, window_days=30, min_rows=1)
        )

    def test_unopenable_db_returns_none(self) -> None:
        self.assertIsNone(
            rolling_class_rate(
                "/nonexistent/dir/nope.sqlite",
                target="reject", horizon=15, window_days=30, min_rows=1,
            )
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
