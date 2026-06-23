"""Walk-forward out-of-sample (OOS) validation harness.

Produces per-signal utility observations on FUTURE (test) windows that never
entered fitting, calibration, or threshold selection — the missing piece that
lets ``run_retrain_evidence_pack.classify_candidate_readiness`` populate the OOS
validation axis with ``source="walk_forward_fold"``.

Design (reviewed 2026-05-31):
  * K expanding folds by chronological order. Fold k trains on rows [0, t_k),
    selects its threshold on its OWN pre-test tune sub-slice (still < t_k), and
    scores the test window [t_k, t_k + w). The union of test windows is the most
    recent K*w rows. Fold boundaries are fixed from chronology + config — no
    window search, no best-fold selection.
  * Anti-leak invariant (enforced, not just intended): every index used for
    fit / calibrate / threshold-selection must be strictly earlier (by ts) than
    the first test-window row. ``run_one_fold`` raises ``LeakageError`` if this
    is violated, so a caller (or a bug) that lets test rows into training fails
    closed.
  * Per-fold power gate: the tune sub-slice must yield >= ``min_signals`` fired
    signals at the selected threshold. If a fold cannot, the fold is infeasible
    and the whole horizon is reported OOS-infeasible (we never select a
    threshold on too-few signals, and we never emit a partial OOS pass).
  * Full population: the caller passes the SPY post-quality-filter frame with
    unresolved events KEPT as reject=0; per-signal utility uses
    ``utility_bps_for_target`` so chop rows contribute their realized return.
  * Threshold semantics mirror serving: each fold scores its test window at the
    threshold *that fold selected on pre-test data* — never one informed by the
    test window.

Dependency injection keeps the core testable without training a real forest:
callers pass ``model_factory`` / ``calibrate_fn`` / ``select_threshold_fn``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

import numpy as np


class LeakageError(ValueError):
    """Raised when test-window rows would enter fit/calibrate/threshold steps."""


# ───────────────────────── fold geometry (pure) ───────────────────────── #


@dataclass
class FoldPlan:
    fold_index: int
    fit_idx: np.ndarray          # pipeline fit rows (earliest)
    calib_fit_idx: np.ndarray    # calibrator fit rows
    tune_idx: np.ndarray         # threshold-selection rows (pre-test)
    test_idx: np.ndarray         # OOS test window
    # ts bounds for the manifest record
    train_start_ts: int | None = None
    train_end_ts: int | None = None
    test_start_ts: int | None = None
    test_end_ts: int | None = None


def build_expanding_folds(
    ts_sorted: Sequence[int],
    *,
    n_folds: int,
    test_window: int,
    min_train: int,
    calib_window: int,
    fit_fraction: float = 0.6,
) -> tuple[list[FoldPlan], str]:
    """Build K expanding-window folds from a chronologically sorted ts array.

    Returns ``(folds, skip_reason)``. ``folds`` is empty with a non-empty
    ``skip_reason`` when the row budget cannot support the requested geometry —
    we report insufficiency rather than shrink windows to force a fit.

    The most-recent ``n_folds * test_window`` rows become the (disjoint) test
    windows. Fold k:
        test = [N - (K-k)*w,  N - (K-1-k)*w)
        train = [0, test_start_k)
        calib slice = last ``calib_window`` rows of train
            -> calib_fit = first ``fit_fraction`` of calib slice
            -> tune       = remainder (pre-test threshold-selection slice)
        pipeline-fit = train minus the calib slice
    """
    ts = np.asarray(ts_sorted)
    n = int(ts.shape[0])
    if n_folds < 1:
        return [], "n_folds_lt_1"
    if test_window < 1:
        return [], "test_window_lt_1"
    # enforce chronological sort (defensive; caller sorts by ts_event)
    if n >= 2 and bool(np.any(np.diff(ts) < 0)):
        return [], "ts_not_sorted_ascending"

    total_test = n_folds * test_window
    earliest_test_start = n - total_test
    # fold 0 trains on [0, earliest_test_start); it must hold the pipeline-fit
    # rows PLUS the full calib slice.
    required_head = min_train + calib_window
    if earliest_test_start < required_head:
        return (
            [],
            f"insufficient_rows: need >= {required_head + total_test} "
            f"(min_train {min_train} + calib_window {calib_window} + "
            f"n_folds*test_window {total_test}), have {n}",
        )

    folds: list[FoldPlan] = []
    for k in range(n_folds):
        test_start = n - (n_folds - k) * test_window
        test_end = test_start + test_window
        train_end = test_start  # [0, test_start)
        calib_start = train_end - calib_window
        # calib slice = [calib_start, train_end); split into calib_fit + tune
        n_calib_fit = int(round(calib_window * float(fit_fraction)))
        n_calib_fit = max(1, min(calib_window - 1, n_calib_fit))
        calib_fit_end = calib_start + n_calib_fit
        fit_idx = np.arange(0, calib_start, dtype=int)
        calib_fit_idx = np.arange(calib_start, calib_fit_end, dtype=int)
        tune_idx = np.arange(calib_fit_end, train_end, dtype=int)
        test_idx = np.arange(test_start, test_end, dtype=int)
        # Anti-leak hardening for TIED timestamps. Many rows can share a
        # ts_event (e.g. same-minute SPY touches), so the index-based boundary
        # can place train and test rows at the SAME ts. assert_strictly_before
        # (correctly) treats equality as leakage and raises — which in
        # production is caught per-horizon and silently sinks the whole fold's
        # OOS. Drop any train row whose ts ties with the test-window start so
        # train is STRICTLY before test. Conservative: only removes potential
        # leakers, never adds rows, and leaves the test window untouched. The
        # tune slice (adjacent to the boundary) absorbs the loss; run_one_fold
        # already degrades gracefully if it shrinks below min_signals.
        test_start_ts = ts[test_start]
        fit_idx = fit_idx[ts[fit_idx] < test_start_ts]
        calib_fit_idx = calib_fit_idx[ts[calib_fit_idx] < test_start_ts]
        tune_idx = tune_idx[ts[tune_idx] < test_start_ts]
        train_kept = np.concatenate([fit_idx, calib_fit_idx, tune_idx])
        train_end_ts = int(ts[int(train_kept.max())]) if train_kept.size else int(ts[0])
        folds.append(
            FoldPlan(
                fold_index=k,
                fit_idx=fit_idx,
                calib_fit_idx=calib_fit_idx,
                tune_idx=tune_idx,
                test_idx=test_idx,
                train_start_ts=int(ts[0]),
                train_end_ts=train_end_ts,
                test_start_ts=int(ts[test_start]),
                test_end_ts=int(ts[test_end - 1]),
            )
        )
    return folds, ""


# ───────────────────────── anti-leak guard ────────────────────────────── #


def assert_strictly_before(
    ts: np.ndarray,
    earlier_idx: np.ndarray,
    later_idx: np.ndarray,
    *,
    context: str = "",
) -> None:
    """Fail closed if any ``earlier_idx`` row is not strictly before all ``later_idx``.

    This is the enforced anti-leak invariant: training/calibration/tuning rows
    MUST have a max timestamp strictly less than the min timestamp of the test
    window. Equality counts as leakage (a same-ts row could be the boundary).
    """
    if earlier_idx.size == 0 or later_idx.size == 0:
        return
    earlier_max = int(np.max(ts[earlier_idx]))
    later_min = int(np.min(ts[later_idx]))
    if earlier_max >= later_min:
        raise LeakageError(
            f"walk-forward leak{(' [' + context + ']') if context else ''}: "
            f"max(train/calib/tune ts)={earlier_max} >= min(test ts)={later_min}; "
            f"test-window rows must never enter fit/calibrate/threshold selection"
        )


# ───────────────────────── per-fold execution ─────────────────────────── #


@dataclass
class FoldResult:
    fold_index: int
    feasible: bool
    skip_reason: str = ""
    threshold: float | None = None
    tune_signals: int = 0
    test_signals: int = 0
    test_utilities: list[float] = field(default_factory=list)
    test_start_ts: int | None = None
    test_end_ts: int | None = None
    train_end_ts: int | None = None


def run_one_fold(
    fold: FoldPlan,
    *,
    X,
    y: np.ndarray,
    return_bps: np.ndarray,
    touch_side: np.ndarray,
    ts: np.ndarray,
    target: str,
    model_factory: Callable[[], Any],
    calibrate_fn: Callable[[Any, Any, np.ndarray], Any] | None,
    select_threshold_fn: Callable[[np.ndarray, np.ndarray, np.ndarray], Any],
    utility_fn: Callable[[np.ndarray, np.ndarray, str], np.ndarray],
    trade_cost_bps: float,
    min_signals: int,
) -> FoldResult:
    """Fit on pre-test rows, select threshold on the tune sub-slice, score test.

    Enforces the anti-leak invariant before any fitting. Returns a FoldResult;
    ``feasible=False`` when the tune slice cannot support a powered threshold.
    """
    res = FoldResult(
        fold_index=fold.fold_index,
        feasible=False,
        test_start_ts=fold.test_start_ts,
        test_end_ts=fold.test_end_ts,
        train_end_ts=fold.train_end_ts,
    )

    # ── ENFORCED anti-leak: every fit/calib/tune row strictly before test ──
    train_all = np.concatenate([fold.fit_idx, fold.calib_fit_idx, fold.tune_idx])
    assert_strictly_before(ts, train_all, fold.test_idx, context=f"fold{fold.fold_index}")

    if fold.tune_idx.size < int(min_signals):
        res.skip_reason = (
            f"tune_rows_{int(fold.tune_idx.size)}_below_min_signals_{int(min_signals)}"
        )
        return res
    if fold.fit_idx.size == 0 or fold.test_idx.size == 0:
        res.skip_reason = "empty_fit_or_test"
        return res

    X_fit = X.iloc[fold.fit_idx] if hasattr(X, "iloc") else X[fold.fit_idx]
    y_fit = y[fold.fit_idx]
    # A degenerate fit fold (single class) cannot train a 2-class model.
    if np.unique(y_fit).size < 2:
        res.skip_reason = "single_class_fit_slice"
        return res

    model = model_factory()
    model.fit(X_fit, y_fit)
    scorer = model

    if calibrate_fn is not None and fold.calib_fit_idx.size > 0:
        X_cf = X.iloc[fold.calib_fit_idx] if hasattr(X, "iloc") else X[fold.calib_fit_idx]
        y_cf = y[fold.calib_fit_idx]
        if np.unique(y_cf).size == 2:
            try:
                scorer = calibrate_fn(model, X_cf, y_cf)
            except Exception:  # noqa: BLE001 — fall back to uncalibrated scorer
                scorer = model

    # threshold selection on the tune sub-slice (pre-test)
    X_tune = X.iloc[fold.tune_idx] if hasattr(X, "iloc") else X[fold.tune_idx]
    y_tune = y[fold.tune_idx]
    probs_tune = scorer.predict_proba(X_tune)
    if getattr(probs_tune, "shape", (0, 0))[1] != 2 or np.unique(y_tune).size < 2:
        res.skip_reason = "tune_probs_or_labels_degenerate"
        return res
    p_tune = probs_tune[:, 1]
    util_tune = utility_fn(return_bps[fold.tune_idx], touch_side[fold.tune_idx], target)
    selection = select_threshold_fn(y_tune, p_tune, util_tune)
    threshold = float(getattr(selection, "threshold", None)) if getattr(selection, "threshold", None) is not None else None
    fallback = bool(getattr(selection, "fallback", False))
    tune_signals = int(getattr(selection, "signals", 0) or 0)
    res.threshold = threshold
    res.tune_signals = tune_signals
    if threshold is None or fallback or tune_signals < int(min_signals):
        res.skip_reason = (
            f"threshold_unpowered fallback={fallback} tune_signals={tune_signals} "
            f"min_signals={int(min_signals)}"
        )
        return res

    # score the FROZEN model at the FROZEN threshold on the test window
    X_test = X.iloc[fold.test_idx] if hasattr(X, "iloc") else X[fold.test_idx]
    probs_test = scorer.predict_proba(X_test)[:, 1]
    fired = probs_test >= threshold
    util_test = utility_fn(return_bps[fold.test_idx], touch_side[fold.test_idx], target)
    fired_util = util_test[fired]
    res.test_signals = int(fired.sum())
    res.test_utilities = [float(v) for v in fired_util]
    res.feasible = True
    return res


# ───────────────────────── orchestration ──────────────────────────────── #


def run_walk_forward_oos(
    *,
    X,
    y,
    return_bps,
    touch_side,
    ts,
    target: str,
    model_factory: Callable[[], Any],
    calibrate_fn: Callable[[Any, Any, np.ndarray], Any] | None,
    select_threshold_fn: Callable[[np.ndarray, np.ndarray, np.ndarray], Any],
    utility_fn: Callable[[np.ndarray, np.ndarray, str], np.ndarray],
    n_folds: int = 5,
    test_window: int = 1000,
    min_train: int = 2000,
    calib_window: int = 1000,
    fit_fraction: float = 0.6,
    min_signals: int = 30,
    trade_cost_bps: float = 0.0,
) -> dict:
    """Run the walk-forward harness for one (target, horizon).

    Returns a manifest-ready dict:
      {
        "feasible": bool,                       # True iff EVERY fold feasible
        "source": "walk_forward_fold",
        "oos_score_observations": [float,...] | None,   # pooled fired-signal utilities
        "signals_on_oos_slice": int,
        "oos_slice_bounds": [ {fold, ts ranges, threshold, signals}, ... ],
        "skip_reason": str,
        "config": {...},
      }
    ``oos_score_observations`` is None unless feasible — we never emit a partial
    or unpowered OOS observation set (that would let an infeasible horizon look
    validated).
    """
    y = np.asarray(y)
    return_bps = np.asarray(return_bps, dtype=float)
    touch_side = np.asarray(touch_side, dtype=float)
    ts = np.asarray(ts)
    config = {
        "n_folds": int(n_folds),
        "test_window": int(test_window),
        "min_train": int(min_train),
        "calib_window": int(calib_window),
        "fit_fraction": float(fit_fraction),
        "min_signals": int(min_signals),
    }

    folds, skip_reason = build_expanding_folds(
        ts,
        n_folds=n_folds,
        test_window=test_window,
        min_train=min_train,
        calib_window=calib_window,
        fit_fraction=fit_fraction,
    )
    if not folds:
        return {
            "feasible": False,
            "source": "walk_forward_fold",
            "oos_score_observations": None,
            "signals_on_oos_slice": 0,
            "oos_slice_bounds": [],
            "skip_reason": skip_reason,
            "config": config,
        }

    pooled: list[float] = []
    bounds: list[dict] = []
    all_feasible = True
    fold_skip = ""
    for fold in folds:
        fr = run_one_fold(
            fold,
            X=X,
            y=y,
            return_bps=return_bps,
            touch_side=touch_side,
            ts=ts,
            target=target,
            model_factory=model_factory,
            calibrate_fn=calibrate_fn,
            select_threshold_fn=select_threshold_fn,
            utility_fn=utility_fn,
            trade_cost_bps=trade_cost_bps,
            min_signals=min_signals,
        )
        bounds.append(
            {
                "fold": fr.fold_index,
                "train_end_ts": fr.train_end_ts,
                "test_start_ts": fr.test_start_ts,
                "test_end_ts": fr.test_end_ts,
                "threshold": fr.threshold,
                "tune_signals": fr.tune_signals,
                "test_signals": fr.test_signals,
                "feasible": fr.feasible,
                "skip_reason": fr.skip_reason,
            }
        )
        if fr.feasible:
            pooled.extend(fr.test_utilities)
        else:
            all_feasible = False
            if not fold_skip:
                fold_skip = f"fold{fr.fold_index}:{fr.skip_reason}"

    return {
        "feasible": bool(all_feasible),
        "source": "walk_forward_fold",
        "oos_score_observations": pooled if all_feasible else None,
        "signals_on_oos_slice": int(len(pooled)) if all_feasible else 0,
        "oos_slice_bounds": bounds,
        "skip_reason": "" if all_feasible else (fold_skip or "fold_infeasible"),
        "config": config,
    }
