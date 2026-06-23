"""Tests for the walk-forward OOS harness (ml/walk_forward_oos.py) and the
evidence-pack OOS-axis extension (run_retrain_evidence_pack.py).

The headline guarantee is the ANTI-LEAK invariant: if test-window rows reach
fit / calibration / threshold-selection, the harness must FAIL (raise), not
silently proceed. That is proven here, not just intended.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ml import walk_forward_oos as wf  # noqa: E402
from ml.thresholds import utility_bps_for_target  # noqa: E402
from scripts import run_retrain_evidence_pack as ep  # noqa: E402


# ───────────────────────── stubs ───────────────────────── #


class _StubModel:
    """predict_proba(X)[:,1] == X[:,0]; fit is a no-op. Lets tests control firing."""

    def fit(self, X, y):  # noqa: D401
        self._fit_rows = np.asarray(X).shape[0]
        return self

    def predict_proba(self, X):
        p = np.asarray(X, dtype=float)[:, 0]
        p = np.clip(p, 0.0, 1.0)
        return np.column_stack([1.0 - p, p])


class _Selection:
    def __init__(self, threshold, signals, fallback=False):
        self.threshold = threshold
        self.signals = signals
        self.fallback = fallback


def _make_select_fn(threshold=0.5, *, record=None):
    def _select(y_true, y_prob, util):
        if record is not None:
            record.append({"n": int(np.asarray(y_prob).shape[0])})
        signals = int((np.asarray(y_prob) >= threshold).sum())
        return _Selection(threshold=threshold, signals=signals, fallback=False)

    return _select


def _util_fn(r, s, t):
    return utility_bps_for_target(r, s, t, trade_cost_bps=0.0)


def _synth(n=20):
    """n rows, ts ascending. Feature col0 high (fires) for most rows; y alternates."""
    X = np.zeros((n, 1), dtype=float)
    X[:, 0] = 0.9  # everything fires at threshold 0.5 by default
    y = np.array([i % 2 for i in range(n)], dtype=int)  # both classes present
    ret = np.full(n, 5.0)   # +5 bps realized per row
    side = np.ones(n)
    ts = np.arange(n, dtype=int)
    return X, y, ret, side, ts


# ───────────────────────── ANTI-LEAK ───────────────────────── #


class TestAntiLeak(unittest.TestCase):
    def test_assert_strictly_before_raises_on_overlap(self):
        ts = np.arange(10)
        # train idx 8 (ts=8) is NOT strictly before test min (ts=6) -> leak
        with self.assertRaises(wf.LeakageError):
            wf.assert_strictly_before(ts, np.array([0, 1, 8]), np.array([5, 6, 7]))

    def test_assert_strictly_before_raises_on_equal_boundary(self):
        ts = np.arange(10)
        # equality counts as leakage (boundary row shared)
        with self.assertRaises(wf.LeakageError):
            wf.assert_strictly_before(ts, np.array([0, 5]), np.array([5, 6]))

    def test_run_one_fold_fails_when_test_data_in_training(self):
        """The core invariant: a fold whose fit/tune indices are not strictly
        before the test window must raise before any fitting."""
        X, y, ret, side, ts = _synth(20)
        leaky_fold = wf.FoldPlan(
            fold_index=0,
            fit_idx=np.array([0, 1, 2, 18]),       # 18 is AFTER the test window
            calib_fit_idx=np.array([3, 4]),
            tune_idx=np.array([5, 6]),
            test_idx=np.array([10, 11, 12]),
        )
        with self.assertRaises(wf.LeakageError):
            wf.run_one_fold(
                leaky_fold, X=X, y=y, return_bps=ret, touch_side=side, ts=ts,
                target="reject", model_factory=_StubModel,
                calibrate_fn=None, select_threshold_fn=_make_select_fn(),
                utility_fn=_util_fn, trade_cost_bps=0.0, min_signals=1,
            )

    def test_clean_fold_does_not_raise(self):
        X, y, ret, side, ts = _synth(20)
        clean_fold = wf.FoldPlan(
            fold_index=0,
            fit_idx=np.arange(0, 6),
            calib_fit_idx=np.arange(6, 8),
            tune_idx=np.arange(8, 10),
            test_idx=np.arange(10, 13),
        )
        res = wf.run_one_fold(
            clean_fold, X=X, y=y, return_bps=ret, touch_side=side, ts=ts,
            target="reject", model_factory=_StubModel,
            calibrate_fn=None, select_threshold_fn=_make_select_fn(threshold=0.5),
            utility_fn=_util_fn, trade_cost_bps=0.0, min_signals=1,
        )
        self.assertTrue(res.feasible)
        self.assertEqual(res.test_signals, 3)  # all 3 test rows fire (p=0.9>=0.5)

    def test_tied_timestamps_at_boundary_do_not_leak(self):
        """build_expanding_folds must not place train and test rows at the SAME
        ts_event. Real SPY data has many same-minute touches; a tied cluster
        straddling the index boundary used to make assert_strictly_before raise
        (caught per-horizon in production -> the fold's OOS silently vanished).
        The tie-guard drops the straddling train rows so train is STRICTLY
        before test."""
        n = 200
        ts = np.arange(n, dtype=int)
        # tied cluster straddling fold-0's boundary (test_start = n - 20 = 180)
        ts[178:183] = 180
        self.assertEqual(ts[179], ts[180])  # precondition: a real boundary tie
        folds, skip = wf.build_expanding_folds(
            ts, n_folds=1, test_window=20, min_train=120, calib_window=40
        )
        self.assertEqual(skip, "")
        self.assertEqual(len(folds), 1)
        f = folds[0]
        train = np.concatenate([f.fit_idx, f.calib_fit_idx, f.tune_idx])
        # the guard: every train ts STRICTLY before the test window...
        self.assertLess(int(ts[train].max()), int(ts[f.test_idx].min()))
        # ...so the anti-leak assertion must NOT raise (it did before the fix).
        wf.assert_strictly_before(ts, train, f.test_idx, context="fold0")
        # the tied train rows (178, 179) were dropped from train; the tied test
        # rows (180-182) stay in the test window (untouched).
        self.assertFalse({178, 179} & set(train.tolist()))
        self.assertEqual(int(ts[f.test_idx].min()), 180)


# ───────────────────────── per-fold threshold pre-test only ───────────────── #


class TestThresholdPreTestOnly(unittest.TestCase):
    def test_threshold_selection_sees_only_tune_rows(self):
        X, y, ret, side, ts = _synth(20)
        fold = wf.FoldPlan(
            fold_index=0,
            fit_idx=np.arange(0, 6),
            calib_fit_idx=np.arange(6, 8),
            tune_idx=np.arange(8, 10),     # 2 tune rows, both < test
            test_idx=np.arange(10, 13),
        )
        seen: list[dict] = []
        wf.run_one_fold(
            fold, X=X, y=y, return_bps=ret, touch_side=side, ts=ts,
            target="reject", model_factory=_StubModel, calibrate_fn=None,
            select_threshold_fn=_make_select_fn(record=seen),
            utility_fn=_util_fn, trade_cost_bps=0.0, min_signals=1,
        )
        self.assertEqual(len(seen), 1)
        # threshold selection received exactly the tune rows (pre-test), no more
        self.assertEqual(seen[0]["n"], 2)


# ───────────────────────── full population (unresolved kept) ───────────────── #


class TestFullPopulationInFolds(unittest.TestCase):
    def test_test_window_includes_reject0_rows_and_scores_them(self):
        # y is all 0 in the test window (chop/unresolved); model still fires;
        # those rows must contribute to the pooled OOS utilities.
        X, y, ret, side, ts = _synth(20)
        y[10:13] = 0  # the test window is all reject=0
        fold = wf.FoldPlan(
            fold_index=0,
            fit_idx=np.arange(0, 6),
            calib_fit_idx=np.arange(6, 8),
            tune_idx=np.arange(8, 10),
            test_idx=np.arange(10, 13),
        )
        res = wf.run_one_fold(
            fold, X=X, y=y, return_bps=ret, touch_side=side, ts=ts,
            target="reject", model_factory=_StubModel, calibrate_fn=None,
            select_threshold_fn=_make_select_fn(threshold=0.5),
            utility_fn=_util_fn, trade_cost_bps=0.0, min_signals=1,
        )
        # all 3 reject=0 rows fired and produced utilities (full population scored)
        self.assertEqual(res.test_signals, 3)
        self.assertEqual(len(res.test_utilities), 3)


# ───────────────────────── orchestration + feasibility ───────────────── #


class TestOrchestration(unittest.TestCase):
    def test_full_run_pools_observations_when_feasible(self):
        X, y, ret, side, ts = _synth(40)
        out = wf.run_walk_forward_oos(
            X=X, y=y, return_bps=ret, touch_side=side, ts=ts, target="reject",
            model_factory=_StubModel, calibrate_fn=None,
            select_threshold_fn=_make_select_fn(threshold=0.5),
            utility_fn=_util_fn,
            n_folds=2, test_window=3, min_train=3, calib_window=4,
            fit_fraction=0.5, min_signals=1, trade_cost_bps=0.0,
        )
        self.assertTrue(out["feasible"])
        self.assertEqual(out["source"], "walk_forward_fold")
        self.assertIsNotNone(out["oos_score_observations"])
        self.assertEqual(out["signals_on_oos_slice"], len(out["oos_score_observations"]))
        self.assertEqual(len(out["oos_slice_bounds"]), 2)

    def test_insufficient_rows_reports_not_feasible(self):
        X, y, ret, side, ts = _synth(8)  # too few for the requested geometry
        out = wf.run_walk_forward_oos(
            X=X, y=y, return_bps=ret, touch_side=side, ts=ts, target="reject",
            model_factory=_StubModel, calibrate_fn=None,
            select_threshold_fn=_make_select_fn(),
            utility_fn=_util_fn,
            n_folds=5, test_window=1000, min_train=4000, calib_window=2000,
            min_signals=30,
        )
        self.assertFalse(out["feasible"])
        self.assertIsNone(out["oos_score_observations"])
        self.assertIn("insufficient_rows", out["skip_reason"])

    def test_underpowered_fold_marks_horizon_infeasible(self):
        # tune slice smaller than min_signals -> fold infeasible -> no OOS emit
        X, y, ret, side, ts = _synth(40)
        out = wf.run_walk_forward_oos(
            X=X, y=y, return_bps=ret, touch_side=side, ts=ts, target="reject",
            model_factory=_StubModel, calibrate_fn=None,
            select_threshold_fn=_make_select_fn(threshold=0.5),
            utility_fn=_util_fn,
            n_folds=2, test_window=3, min_train=3, calib_window=4,
            fit_fraction=0.5, min_signals=1000, trade_cost_bps=0.0,
        )
        self.assertFalse(out["feasible"])
        self.assertIsNone(out["oos_score_observations"])


# ───────────────────────── evidence-pack OOS axis ───────────────── #


class TestEvidencePackOOSAxis(unittest.TestCase):
    def test_walk_forward_fold_recognized_as_oos(self):
        self.assertTrue(ep._is_oos_source("walk_forward_fold"))
        self.assertFalse(ep._is_oos_source("threshold_tune_slice"))

    def _ph_row(self, target, horizon, *, in_sample, oos):
        return {
            "target": target,
            "horizon": horizon,
            "score": 10.0,
            "score_observations": in_sample,
            "score_observations_source": "threshold_tune_slice" if in_sample else None,
            "signals_on_tune_slice": len(in_sample) if in_sample else 0,
            "oos_score_observations": oos,
            "oos_score_observations_source": "walk_forward_fold" if oos else None,
            "signals_on_oos_slice": len(oos) if oos else 0,
        }

    def test_oos_coverage_complete_only_when_all_viable_have_oos(self):
        # strongly-positive observations so the stat test passes
        pos = [5.0] * 200
        rows = [
            self._ph_row("reject", 15, in_sample=pos, oos=pos),
            self._ph_row("reject", 30, in_sample=pos, oos=pos),
        ]
        viable = {("reject", 15), ("reject", 30)}
        in_s = ep.run_statistical_validation(rows, viable_set=viable)
        oos = ep.run_oos_validation(rows, viable_set=viable)
        merged = dict(in_s)
        for k, v in oos.items():
            merged[f"{k}::oos"] = v
        agg = ep._aggregate_statistical_validation(merged, viable_count=len(in_s))
        self.assertTrue(agg["oos_present"])
        self.assertTrue(agg["oos_coverage_complete"])
        self.assertTrue(agg["oos_passed"])

    def test_partial_oos_coverage_blocks(self):
        pos = [5.0] * 200
        rows = [
            self._ph_row("reject", 15, in_sample=pos, oos=pos),
            self._ph_row("reject", 30, in_sample=pos, oos=None),  # no OOS for h=30
        ]
        viable = {("reject", 15), ("reject", 30)}
        in_s = ep.run_statistical_validation(rows, viable_set=viable)
        oos = ep.run_oos_validation(rows, viable_set=viable)
        merged = dict(in_s)
        for k, v in oos.items():
            merged[f"{k}::oos"] = v
        agg = ep._aggregate_statistical_validation(merged, viable_count=len(in_s))
        self.assertTrue(agg["oos_present"])
        self.assertFalse(agg["oos_coverage_complete"])
        self.assertIsNone(agg["oos_passed"])  # coverage gap -> not a pass

    def test_pre_b4_manifest_no_oos_axis(self):
        # rows without any oos fields -> OOS axis stays empty (back-compat)
        rows = [self._ph_row("reject", 15, in_sample=[5.0] * 200, oos=None)]
        viable = {("reject", 15)}
        oos = ep.run_oos_validation(rows, viable_set=viable)
        self.assertEqual(oos, {})


if __name__ == "__main__":
    unittest.main()
