"""SQLite-native Stage 2 OOS validation for registered candidates.

Implements the two symbols imported by scripts/run_ml_regime_validation.py:

  - run_ml_regime_validation(symbol, train_years, test_year, ...)
  - write_ml_regime_validation_report(report, ...)

Design
------
The original ``services.external_data.ml_regime_validation`` module
expected external model-ready CSVs and a ``realized_vol_60d`` feature
that does not exist in the current ``pivot_events.sqlite`` schema.  This
replacement is SQLite-native: it reads ``touch_events`` + ``event_labels``
directly, loads the candidate registration to discover the signal definition,
and applies the signal filter in Python before running the statistical tests
defined in ``services.research_protocol.statistical_guard``.

Signal interpretation
---------------------
The registration's ``thresholds[0].value`` and ``features[0].name``
define the filter.  Only ``<=`` threshold comparisons on single numeric
columns are supported here; more complex signals must extend
``_apply_signal_filter``.  The evaluated metric is *break rate*
(``1 - reject``) because the registered hypothesis direction is "long"
(a break confirms the entry).

Statistical validity
--------------------
The "return" fed to bootstrap CI and permutation test is the *delta from
baseline*: for each signal event, ``break_outcome - baseline_break_rate``.
This centres the distribution at 0 under the null (no edge), so the
bootstrap CI lower bound > 0 is the correct pass criterion.  The
permutation test uses the raw signal/baseline break arrays and tests
``H1: signal_break_rate > baseline_break_rate``.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.research_protocol.statistical_guard import (
    compute_bootstrap_ci,
    evaluate_statistical_validity,
    run_permutation_test,
    verdict_to_dict,
)

# ------------------------------------------------------------------ #
# Public constants (imported by the script for default CLI values)
# ------------------------------------------------------------------ #

DEFAULT_TRAIN_YEARS: list[str] = ["2023", "2024"]
DEFAULT_TEST_YEAR: str = "2025"

_REPORT_DIR = ROOT / "reports" / "ml_diagnostics"

# Columns that may appear in a registration's ``features`` list and that
# can be fetched from ``touch_events``.  Extend as new signals are added.
_ALLOWED_FEATURE_COLUMNS: frozenset[str] = frozenset({
    "distance_bps",
    "ema_state",
    "iv_rv_state",
    "gamma_mode",
    "rv_30",
    "vwap_dist_bps",
    "sigma_band_position",
    "or_breakout",
    "level_age_days",
    "hist_reject_rate",
    "hist_break_rate",
    "touch_count_today",
    "confluence_count",
})


# ------------------------------------------------------------------ #
# Result type
# ------------------------------------------------------------------ #


@dataclass
class ValidationResult:
    """Thin wrapper so callers can do ``result.report``."""

    report: dict[str, Any]


# ------------------------------------------------------------------ #
# Internal helpers
# ------------------------------------------------------------------ #


def _parse_date_range(period_str: str) -> tuple[str, str]:
    """Parse ``'YYYY-MM-DD to YYYY-MM-DD'`` → ``(start, end)``."""
    parts = [p.strip() for p in period_str.split(" to ")]
    if len(parts) != 2:
        raise ValueError(
            f"Cannot parse period string: {period_str!r}."
            " Expected 'YYYY-MM-DD to YYYY-MM-DD'."
        )
    return parts[0], parts[1]


def _validate_feature_column(col: str) -> str:
    """Reject unknown column names to guard against injection."""
    if col not in _ALLOWED_FEATURE_COLUMNS:
        raise ValueError(
            f"Feature column {col!r} is not in the allowed set."
            f" Allowed: {sorted(_ALLOWED_FEATURE_COLUMNS)}"
        )
    return col


def _apply_signal_filter(
    feature_values: np.ndarray,
    threshold_val: float,
    *,
    operator: str = "lte",
) -> np.ndarray:
    """Return a boolean mask: True where the signal fires.

    ``operator="lte"`` (default) → ``value <= threshold`` (long break signal).
    ``operator="gte"``           → ``value >= threshold`` (inverted / short signal).
    """
    if operator == "gte":
        return feature_values >= threshold_val
    return feature_values <= threshold_val


def _query_oos_events(
    db: Path,
    *,
    symbol: str,
    test_start: str,
    test_end: str,
    label_horizon_min: int,
    feature_col: str,
) -> tuple[np.ndarray, np.ndarray]:
    """Return ``(feature_values, reject_labels)`` arrays for the OOS window.

    Both arrays are 1-D float64, aligned by row.  Rows where either
    ``feature_col`` or ``el.reject`` is NULL are excluded.
    """
    # Column name is already validated by _validate_feature_column.
    query = f"""
        SELECT
            te.{feature_col}     AS feature_val,
            CAST(el.reject AS REAL) AS reject_val
        FROM touch_events te
        JOIN event_labels el
          ON te.event_id     = el.event_id
         AND el.horizon_min  = ?
        WHERE te.symbol = ?
          AND date(te.ts_event / 1000, 'unixepoch') >= ?
          AND date(te.ts_event / 1000, 'unixepoch') <= ?
          AND te.{feature_col} IS NOT NULL
          AND el.reject        IS NOT NULL
        ORDER BY te.ts_event
    """
    conn = sqlite3.connect(str(db))
    try:
        cur = conn.execute(query, (label_horizon_min, symbol, test_start, test_end))
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return np.array([], dtype=float), np.array([], dtype=float)

    feature_arr = np.array([r[0] for r in rows], dtype=float)
    reject_arr = np.array([r[1] for r in rows], dtype=float)
    return feature_arr, reject_arr


# ------------------------------------------------------------------ #
# Public API
# ------------------------------------------------------------------ #


def run_ml_regime_validation(
    symbol: str,
    train_years: list[str],
    test_year: str,
    *,
    candidate_id: str | None = None,
    db_path: str | None = None,
) -> ValidationResult:
    """Run Stage 2 single-period OOS validation from ``pivot_events.sqlite``.

    Args:
      symbol: ticker symbol (e.g. ``"SPY"``).
      train_years: passed for compatibility; not used for the date split
        when the registration contains an explicit ``datasets.test_period``.
      test_year: used to build ``YYYY-01-01 … YYYY-12-31`` when no
        explicit ``datasets.test_period`` is present in the registration.
      candidate_id: when provided, the registration is loaded to obtain:
        (a) the OOS date range from ``datasets.test_period``,
        (b) the signal feature column + threshold,
        (c) ``horizon_days`` and ``random_seed`` for statistical tests.
      db_path: override path to ``pivot_events.sqlite``.  Defaults to
        ``<ROOT>/data/pivot_events.sqlite``.

    Returns:
      :class:`ValidationResult` whose ``report`` dict contains
      ``statistical_validity``, ``validated``, per-group metrics, and
      enough metadata to reconstruct the verdict deterministically.
    """
    db = Path(db_path) if db_path else ROOT / "data" / "pivot_events.sqlite"

    # -------------------------------------------------------------- #
    # 1. Resolve parameters (from registration or defaults)
    # -------------------------------------------------------------- #
    reg = None
    horizon_days: int = 5
    rng_seed: int = 42
    label_horizon_min: int = 5
    feature_col: str = "distance_bps"     # sensible default
    threshold_val: float | None = None
    threshold_operator: str = "lte"

    if candidate_id is not None:
        from services.research_protocol.registration import load_registration
        reg = load_registration(candidate_id)
        horizon_days = reg.horizon_days
        rng_seed = reg.random_seed

        ds = reg.body.get("datasets", {})
        label_horizon_min = int(ds.get("label_horizon_min", 5))

        # OOS date range: prefer explicit period in registration.
        test_period_str = ds.get("test_period", "")
        if test_period_str and " to " in test_period_str:
            test_start, test_end = _parse_date_range(test_period_str)
        else:
            test_start = f"{test_year}-01-01"
            test_end = f"{test_year}-12-31"

        # Signal definition from first feature + threshold pair.
        features = reg.body.get("features", [])
        thresholds = reg.body.get("thresholds", [])
        if features and thresholds:
            feature_col = _validate_feature_column(features[0]["name"])
            threshold_val = float(thresholds[0]["value"])
            threshold_operator = str(thresholds[0].get("operator", "lte"))
    else:
        test_start = f"{test_year}-01-01"
        test_end = f"{test_year}-12-31"

    dataset_identifier = f"{symbol}_{test_start}_to_{test_end}"

    # -------------------------------------------------------------- #
    # 2. Load OOS events from SQLite
    # -------------------------------------------------------------- #
    feature_arr, reject_arr = _query_oos_events(
        db,
        symbol=symbol,
        test_start=test_start,
        test_end=test_end,
        label_horizon_min=label_horizon_min,
        feature_col=feature_col,
    )

    if feature_arr.size == 0:
        return ValidationResult(report={
            "status": "error",
            "error": (
                f"No labeled events for {symbol!r} in"
                f" {test_start} to {test_end}"
                f" (horizon_min={label_horizon_min})."
            ),
            "validated": False,
            "symbol": symbol,
            "candidate_id": candidate_id,
            "test_period": f"{test_start} to {test_end}",
            "dataset_identifier": dataset_identifier,
        })

    # -------------------------------------------------------------- #
    # 3. Compute baseline + signal group metrics
    # -------------------------------------------------------------- #
    break_arr = 1.0 - reject_arr
    n_all = int(feature_arr.size)
    baseline_break_rate = float(break_arr.mean())
    baseline_reject_rate = 1.0 - baseline_break_rate

    if threshold_val is not None:
        signal_mask = _apply_signal_filter(
            feature_arr, threshold_val, operator=threshold_operator
        )
    else:
        signal_mask = np.ones(n_all, dtype=bool)

    signal_break = break_arr[signal_mask]
    n_signal = int(signal_mask.sum())

    if n_signal == 0:
        return ValidationResult(report={
            "status": "error",
            "error": (
                f"Signal filter ({feature_col} {threshold_operator} {threshold_val})"
                f" produced 0 events in OOS period"
                f" {test_start} to {test_end}."
            ),
            "validated": False,
            "symbol": symbol,
            "candidate_id": candidate_id,
            "test_period": f"{test_start} to {test_end}",
            "dataset_identifier": dataset_identifier,
        })

    signal_break_rate = float(signal_break.mean())
    signal_reject_rate = 1.0 - signal_break_rate
    delta_break_rate = signal_break_rate - baseline_break_rate

    # -------------------------------------------------------------- #
    # 4. Statistical tests
    # -------------------------------------------------------------- #
    # "Returns" for the bootstrap CI: delta from baseline per signal event.
    # Centred at 0 under H0 (no edge), so lower > 0 is the pass criterion.
    delta_returns = (signal_break - baseline_break_rate).tolist()
    block_size = max(1, horizon_days)

    ci_lower, ci_upper = compute_bootstrap_ci(
        delta_returns,
        block_size=block_size,
        iterations=10_000,
        rng_seed=rng_seed,
        statistic="mean",
    )

    perm_p_value = run_permutation_test(
        signal_returns=signal_break.tolist(),
        baseline_returns=break_arr.tolist(),
        n_iter=1_000,
        rng_seed=rng_seed,
        one_sided="greater",
    )

    verdict = evaluate_statistical_validity(
        stage=2,
        n_obs=n_signal,
        horizon_days=horizon_days,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        permutation_p_value=perm_p_value,
    )

    # -------------------------------------------------------------- #
    # 5. Assemble report
    # -------------------------------------------------------------- #
    validated = bool(verdict.statistical_pass)
    if validated:
        status = "pass"
    elif verdict.n_eff >= verdict.n_eff_floor:
        # Enough data but CI or p failed — still informative.
        status = "warn"
    else:
        status = "fail"

    report: dict[str, Any] = {
        "status": status,
        "validated": validated,
        "symbol": symbol,
        "candidate_id": candidate_id,
        "train_years": train_years,
        "test_year": test_year,
        "test_period": f"{test_start} to {test_end}",
        "dataset_identifier": dataset_identifier,
        "feature_col": feature_col,
        "threshold_val": threshold_val,
        "threshold_operator": threshold_operator,
        "n_all_oos": n_all,
        "n_signal_oos": n_signal,
        "baseline_break_rate": round(baseline_break_rate, 6),
        "baseline_reject_rate": round(baseline_reject_rate, 6),
        "signal_break_rate": round(signal_break_rate, 6),
        "signal_reject_rate": round(signal_reject_rate, 6),
        "delta_break_rate": round(delta_break_rate, 6),
        "degradation_warning": not validated,
        "statistical_validity": verdict_to_dict(verdict),
    }

    return ValidationResult(report=report)


def write_ml_regime_validation_report(
    report: dict[str, Any],
    *,
    stem: str | None = None,
) -> Path:
    """Write ``report`` as JSON under ``reports/ml_diagnostics/``.

    Returns the path of the written file.
    """
    _REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    candidate_id = report.get("candidate_id") or "unknown"
    if stem is None:
        stem = f"{candidate_id}_stage2_oos_{ts}"
    path = _REPORT_DIR / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return path
