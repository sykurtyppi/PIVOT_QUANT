#!/usr/bin/env python3
"""Read-only Phase 2 regime-health attribution for a (target, horizon).

Phase 1 (``scripts/audit_regime_health.py``) reported *that* the deployed
model has gone dormant on the latest chronological rows. This script
asks: *what is different* about those recent dormant rows compared to
prior rows that fired (or sat in the high-probability tail just below
the threshold)?

Strictly attribution. No training, no threshold tuning, no threshold
search, no promotion, no edge claims. Probabilities are computed once
per group using the deployed model and the runtime-resolved threshold
(manifest first, artifact only as fallback — same precedence as
``server.ml_server.ModelRegistry`` and ``audit_regime_health.py``).

Groups (fixed chronologically up front):

  1. ``recent_dormant``
     The latest ``--recent-n`` rows (default 1000). Per Phase 1 this
     is the dormant region we are attempting to explain.

  2. ``older_firing_context``
     Rows in the latest ``--older-pct`` window (default 0.30) that lie
     *before* the recent-dormant tail and that fire at the runtime
     threshold.

  3. ``older_high_probability_nonfiring``
     Rows in the same older window with probability in
     ``[--highprob-low, runtime_threshold)`` (default low = 0.70).
     Captures "almost-fired" rows that operators care about — their
     feature signatures are the closest comparable to firing rows.

  4. ``threshold_tune_reference``
     The training-time threshold-tune slice. The active manifest
     captures the *size* of that slice in ``thresholds_meta`` but does
     **not** persist the row IDs / timestamps. We surface this as
     ``available=False`` with the recorded size, instead of fabricating
     a slice from current data.

Diagnostic dimensions (each reported independently — the audit does
not pick a single causal story):

  - **Probability comparison** per group: n, signal_count, quantile
    profile (min, p10, p25, median, p75, p90, p95, p99, max), mean,
    std.
  - **Feature distribution comparison** for each column in the model
    artifact's ``feature_columns``: per-group mean/median/std/null
    rate, plus a standardized mean difference and a quantile-distance
    statistic (preferred: SciPy ``ks_2samp`` ``statistic`` if SciPy
    available; fallback: max absolute distance over fixed quantile
    grid). Top-shifted features are reported by absolute standardized
    difference.
  - **Model-input health** per group: usable feature rows
    (``~isna().all(axis=1)``), all-null rows, null-rate summary,
    feature columns missing from the built rows.
  - **Attribution summary** with five non-exclusive boolean flags
    (``feature_shift_present``, ``probability_compression_present``,
    ``data_quality_warning``, ``older_firing_context_available``,
    ``threshold_tune_reference_available``) plus a list of strongest
    shifted features. **No** "the cause is X" output. The flags are
    inputs for the next diagnostic, not a trading recommendation.

Scope contract:
- Read-only.
- No model training.
- No threshold tuning or threshold search.
- No threshold changes (the runtime threshold is read, not modified).
- No model changes.
- No promotion.
- No walk-forward OOS yet.
- No performance or edge claims.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services._pybin import assert_python_310  # noqa: E402

assert_python_310()

DEFAULT_DUCKDB = ROOT / "data" / "pivot_training.duckdb"
DEFAULT_VIEW = "training_events_v1"
DEFAULT_MANIFEST = ROOT / "data" / "models" / "manifest_active.json"
DEFAULT_MODELS_DIR = ROOT / "data" / "models"
REPORT_DIR = ROOT / "evidence" / "regime_health_attribution"

# Group-selection defaults (fixed up front; not tuned per-run).
DEFAULT_RECENT_N = 1000
DEFAULT_OLDER_PCT = 0.30
DEFAULT_HIGHPROB_LOW = 0.70

# Quantile grid for the fallback (no-SciPy) distribution distance.
QUANTILE_GRID = (0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)

# Feature-shift call-out cutoffs (descriptive only — these are NOT
# significance thresholds, they pick *which* features to surface).
TOP_SHIFTED_K = 10
STD_DIFF_NOTABLE = 0.50

# Heuristic flag cutoffs (descriptive only).
COMPRESSION_GAP = 0.10        # max(recent_prob) is >=10pp below threshold
DATA_QUALITY_NULL_RATE = 0.20  # any feature with >20% nulls counts as DQ noise


# ------------------------------------------------------------------ #
# I/O helpers (mirror audit_regime_health.py — intentionally local to
# keep the audit a single self-contained script).
# ------------------------------------------------------------------ #


def _require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Missing dependency {module_name!r}: {exc}. {hint}")


def load_active_manifest(path: Path) -> dict:
    if not path.is_file():
        raise SystemExit(f"Active manifest not found: {path}")
    return json.loads(path.read_text())


def resolve_model_path(manifest: dict, target: str, horizon: int, models_dir: Path) -> Path:
    models = (manifest.get("models") or {}).get(target) or {}
    name = models.get(str(horizon)) or models.get(horizon)
    if not name:
        raise SystemExit(
            f"No model registered for {target}@{horizon}m in active manifest"
        )
    return models_dir / str(name)


def deployed_threshold(manifest: dict, target: str, horizon: int) -> float | None:
    thresholds = (manifest.get("thresholds") or {}).get(target) or {}
    raw = thresholds.get(str(horizon), thresholds.get(horizon))
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def resolve_runtime_threshold(
    manifest_threshold: float | None,
    artifact_threshold: float | None,
    *,
    mismatch_tol: float = 1e-12,
) -> dict:
    """Mirror ``server.ml_server.ModelRegistry`` threshold resolution.

    Manifest first; artifact only if manifest is missing. Both raw
    values plus a mismatch flag are surfaced so a runtime-safety
    substitution (PRs #9/#10/#12) cannot silently disagree with the
    artifact.
    """
    have_m = manifest_threshold is not None
    have_a = artifact_threshold is not None
    if have_m:
        runtime, source = float(manifest_threshold), "manifest"
    elif have_a:
        runtime, source = float(artifact_threshold), "artifact_fallback"
    else:
        raise SystemExit(
            "Neither manifest nor artifact carries a threshold for the "
            "requested (target, horizon)."
        )
    mismatch = bool(
        have_m
        and have_a
        and abs(float(manifest_threshold) - float(artifact_threshold)) > float(mismatch_tol)
    )
    return {
        "runtime_threshold": runtime,
        "threshold_source": source,
        "manifest_threshold": float(manifest_threshold) if have_m else None,
        "artifact_threshold": float(artifact_threshold) if have_a else None,
        "threshold_mismatch_detected": mismatch,
    }


def threshold_tune_meta(manifest: dict, target: str, horizon: int) -> dict:
    """Return the recorded threshold-tune metadata for (target, horizon).

    The active manifest only retains *aggregate* fields about the
    threshold-tune slice (size, fallback flag, objective). It does not
    persist row IDs / timestamps, so we cannot reconstruct the actual
    slice. The attribution script reports this honestly rather than
    fabricating a substitute.
    """
    meta = (manifest.get("thresholds_meta") or {}).get(target) or {}
    raw = meta.get(str(horizon), meta.get(horizon)) or {}
    if not isinstance(raw, dict):
        return {"available": False, "reason": "non_dict_metadata"}
    size = raw.get("threshold_tune_size")
    if size in (None, 0):
        return {
            "available": False,
            "reason": "threshold_tune_size_missing_or_zero",
            "raw_meta": raw,
        }
    return {
        "available": False,  # row-level data is NOT persisted
        "reason": "row_ids_not_persisted_in_manifest",
        "threshold_tune_size": int(size),
        "objective": raw.get("objective"),
        "fallback": bool(raw.get("fallback", False)),
        "search_enabled": bool(raw.get("search_enabled", False)),
    }


def load_labeled_events(duckdb_path: Path, view: str, horizon: int, target: str):
    duckdb = _require("duckdb", "python3 -m pip install duckdb")
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        df = con.execute(
            f"SELECT * FROM {view} WHERE horizon_min = ? ORDER BY ts_event",
            [horizon],
        ).df()
    finally:
        con.close()
    if target not in df.columns:
        raise SystemExit(f"Training view missing target column {target!r}")
    return df[df[target].notna()].copy()


def build_features_aligned(df, feature_columns: list[str]):
    pd = _require("pandas", "python3 -m pip install pandas")
    from ml.features import build_feature_row
    rows = [build_feature_row(row) for row in df.to_dict("records")]
    feature_df = pd.DataFrame(rows, index=df.index)
    return feature_df.reindex(columns=feature_columns)


def score_probabilities(model_obj, X):
    """Positive-class probability vector, or None if predict_proba shape is wrong.

    Single call. No threshold search. No iteration over alternatives.
    """
    probs = model_obj.predict_proba(X)
    if getattr(probs, "shape", (0, 0))[1] != 2:
        return None
    return probs[:, 1]


# ------------------------------------------------------------------ #
# Group selection — chronological, fixed, never depends on outcomes.
# ------------------------------------------------------------------ #


def select_groups(
    df,
    *,
    recent_n: int,
    older_pct: float,
) -> dict:
    """Pick the recent-dormant tail and the older window that excludes it.

    Returns a dict with two dataframes plus their integer ranges. Group
    membership for ``older_firing_context`` and
    ``older_high_probability_nonfiring`` is decided *after* scoring; we
    only carve out the row ranges here.

    The older window is the *latest ``older_pct``* of the full df,
    minus the recent-dormant tail. This guarantees:
      - the two ranges are disjoint;
      - the older window is still "recent context", not deep history;
      - if the recent tail consumes the entire older window, the older
        window is empty (skip_reason recorded later).
    """
    total = int(len(df))
    if total == 0:
        return {
            "total_rows": 0,
            "recent_df": df.iloc[0:0],
            "recent_range": (0, 0),
            "older_df": df.iloc[0:0],
            "older_range": (0, 0),
            "older_window_size": 0,
            "recent_overlapped_older": False,
        }
    recent_size = min(int(recent_n), total)
    older_window_size = max(1, int(round(total * float(older_pct))))

    recent_start = total - recent_size
    recent_end = total
    # Older window endpoints, before subtracting recent.
    older_start_full = max(0, total - older_window_size)
    older_end_full = total
    # Carve out: older slice must end where recent starts.
    older_end = min(older_end_full, recent_start)
    older_start = min(older_start_full, older_end)
    recent_df = df.iloc[recent_start:recent_end]
    older_df = df.iloc[older_start:older_end]
    return {
        "total_rows": total,
        "recent_df": recent_df,
        "recent_range": (recent_start, recent_end),
        "older_df": older_df,
        "older_range": (older_start, older_end),
        "older_window_size": int(older_window_size),
        "recent_overlapped_older": bool(older_start_full >= recent_start),
    }


# ------------------------------------------------------------------ #
# Probability comparison
# ------------------------------------------------------------------ #


def probability_stats(probs, threshold: float) -> dict:
    np = _require("numpy", "python3 -m pip install numpy")
    if probs is None or len(probs) == 0:
        return {
            "n": 0,
            "signal_count": 0,
            "min": None, "p10": None, "p25": None, "median": None,
            "p75": None, "p90": None, "p95": None, "p99": None, "max": None,
            "mean": None, "std": None,
        }
    arr = np.asarray(probs, dtype=float)
    return {
        "n": int(arr.size),
        "signal_count": int((arr >= float(threshold)).sum()),
        "min": float(arr.min()),
        "p10": float(np.percentile(arr, 10)),
        "p25": float(np.percentile(arr, 25)),
        "median": float(np.percentile(arr, 50)),
        "p75": float(np.percentile(arr, 75)),
        "p90": float(np.percentile(arr, 90)),
        "p95": float(np.percentile(arr, 95)),
        "p99": float(np.percentile(arr, 99)),
        "max": float(arr.max()),
        "mean": float(arr.mean()),
        "std": float(arr.std(ddof=0)),
    }


# ------------------------------------------------------------------ #
# Feature distribution comparison
# ------------------------------------------------------------------ #


def _safe_to_float_array(series):
    """Coerce a pandas Series to a float numpy array, dropping NaN.

    Non-numeric columns (e.g. ``regime_type`` strings, booleans encoded
    as objects) are mapped via ``pd.factorize`` so we can still produce
    a "shifted" signal — the absolute scale is meaningless for those
    but the *amount of change* between groups is still informative as
    "this category mix shifted." We surface dtype in the per-feature
    record so consumers can ignore comparisons they consider unsafe.
    """
    np = _require("numpy", "python3 -m pip install numpy")
    pd = _require("pandas", "python3 -m pip install pandas")
    if series is None or len(series) == 0:
        return np.asarray([], dtype=float), "empty"
    if pd.api.types.is_bool_dtype(series):
        return series.astype(float).to_numpy(), "bool->float"
    if pd.api.types.is_numeric_dtype(series):
        arr = series.to_numpy(dtype=float)
        return arr[~np.isnan(arr)], "numeric"
    codes, _ = pd.factorize(series, sort=True)
    arr = codes.astype(float)
    arr[arr < 0] = float("nan")  # NaN codes are -1
    return arr[~np.isnan(arr)], "categorical->codes"


def standardized_mean_diff(a, b) -> float | None:
    """Cohen-d-style pooled-SD effect size for two numeric arrays.

    Returns None when both pooled variances are zero (constant or
    single-row groups) — no synthetic large effect.
    """
    np = _require("numpy", "python3 -m pip install numpy")
    if a is None or b is None:
        return None
    if a.size == 0 or b.size == 0:
        return None
    var_a = float(np.var(a, ddof=0))
    var_b = float(np.var(b, ddof=0))
    pooled = math.sqrt((var_a + var_b) / 2.0)
    if pooled == 0.0:
        return None
    return (float(np.mean(a)) - float(np.mean(b))) / pooled


def quantile_distance(a, b, grid: tuple[float, ...] = QUANTILE_GRID) -> float | None:
    """Max |q_a(p) - q_b(p)| over a fixed quantile grid.

    Used as a SciPy-free stand-in for the KS statistic. Returns None
    when either group has no usable values.
    """
    np = _require("numpy", "python3 -m pip install numpy")
    if a is None or b is None or a.size == 0 or b.size == 0:
        return None
    diffs = []
    for q in grid:
        diffs.append(abs(float(np.quantile(a, q)) - float(np.quantile(b, q))))
    return max(diffs) if diffs else None


def ks_statistic(a, b) -> float | None:
    """SciPy ``ks_2samp`` statistic if available; else None.

    The audit prefers SciPy when present (more accurate distribution
    distance) and falls back to ``quantile_distance``. We report both
    raw values so downstream readers know which one is filled in.
    """
    try:
        from scipy.stats import ks_2samp  # type: ignore
    except Exception:
        return None
    if a is None or b is None or a.size == 0 or b.size == 0:
        return None
    try:
        res = ks_2samp(a, b)
        # SciPy returns either a tuple or an object with .statistic.
        stat = getattr(res, "statistic", None)
        if stat is None and isinstance(res, tuple) and len(res) >= 1:
            stat = res[0]
        return float(stat) if stat is not None else None
    except Exception:
        return None


def per_feature_comparison(
    feat_a, feat_b, *, group_a_name: str, group_b_name: str
) -> list[dict]:
    """Build a per-feature row describing how column distributions differ.

    ``feat_a`` and ``feat_b`` are pandas DataFrames aligned to the same
    columns. Each row in the returned list has the same fixed schema so
    downstream consumers can diff column-for-column.
    """
    np = _require("numpy", "python3 -m pip install numpy")
    rows: list[dict] = []
    cols = list(feat_a.columns)
    for col in cols:
        a_series = feat_a[col] if col in feat_a.columns else None
        b_series = feat_b[col] if col in feat_b.columns else None
        a_arr, a_dtype = _safe_to_float_array(a_series)
        b_arr, b_dtype = _safe_to_float_array(b_series)

        def _stats(series, arr):
            if series is None or len(series) == 0:
                return {"mean": None, "median": None, "std": None, "null_rate": None}
            null_rate = float(series.isna().mean())
            if arr.size == 0:
                return {"mean": None, "median": None, "std": None, "null_rate": null_rate}
            return {
                "mean": float(np.mean(arr)),
                "median": float(np.median(arr)),
                "std": float(np.std(arr, ddof=0)),
                "null_rate": null_rate,
            }

        sa = _stats(a_series, a_arr)
        sb = _stats(b_series, b_arr)
        std_diff = standardized_mean_diff(a_arr, b_arr)
        qd = quantile_distance(a_arr, b_arr)
        ks = ks_statistic(a_arr, b_arr)
        all_null = bool(
            (a_series is not None and a_series.isna().all())
            or (b_series is not None and b_series.isna().all())
        )
        constant = bool(
            (a_arr.size > 0 and float(np.std(a_arr, ddof=0)) == 0.0)
            and (b_arr.size > 0 and float(np.std(b_arr, ddof=0)) == 0.0)
        )
        rows.append({
            "feature": col,
            "dtype_a": a_dtype,
            "dtype_b": b_dtype,
            f"{group_a_name}_stats": sa,
            f"{group_b_name}_stats": sb,
            "standardized_mean_diff": std_diff,
            "quantile_distance": qd,
            "ks_statistic": ks,
            "any_all_null_group": all_null,
            "both_groups_constant": constant,
        })
    return rows


def top_shifted_features(rows: list[dict], k: int = TOP_SHIFTED_K) -> list[dict]:
    """Return the top-``k`` per-feature rows by ``|standardized_mean_diff|``.

    Skips features whose effect size is undefined (None). Ties broken
    by ``quantile_distance`` (descending) then feature name (ascending)
    for determinism.
    """
    candidates = [r for r in rows if r.get("standardized_mean_diff") is not None]
    candidates.sort(
        key=lambda r: (
            -abs(float(r["standardized_mean_diff"])),
            -(float(r["quantile_distance"]) if r.get("quantile_distance") is not None else 0.0),
            str(r["feature"]),
        )
    )
    return candidates[: int(k)]


# ------------------------------------------------------------------ #
# Model-input health per group
# ------------------------------------------------------------------ #


def model_input_health(feat_df, feature_columns: list[str]) -> dict:
    pd = _require("pandas", "python3 -m pip install pandas")
    if feat_df is None or len(feat_df) == 0:
        return {
            "available_rows": 0,
            "usable_feature_rows": 0,
            "all_null_rows": 0,
            "null_rate_overall": None,
            "missing_feature_columns": list(feature_columns),
            "imputation_heavy_features": [],
        }
    available = int(len(feat_df))
    all_null_mask = feat_df.isna().all(axis=1)
    usable = int((~all_null_mask).sum())
    all_null_rows = int(all_null_mask.sum())
    null_rate_overall = float(feat_df.isna().to_numpy().mean()) if available > 0 else None
    missing = [c for c in feature_columns if c not in feat_df.columns]
    null_rate_by_col = feat_df.isna().mean().to_dict()
    imputation_heavy = sorted(
        (c for c, r in null_rate_by_col.items() if float(r) > float(DATA_QUALITY_NULL_RATE)),
        key=lambda c: -float(null_rate_by_col[c]),
    )
    return {
        "available_rows": available,
        "usable_feature_rows": usable,
        "all_null_rows": all_null_rows,
        "null_rate_overall": null_rate_overall,
        "missing_feature_columns": missing,
        "imputation_heavy_features": imputation_heavy,
    }


# ------------------------------------------------------------------ #
# Attribution summary
# ------------------------------------------------------------------ #


def attribution_summary(
    *,
    recent_prob_stats: dict,
    firing_prob_stats: dict,
    threshold: float,
    feature_rows: list[dict],
    recent_health: dict,
    firing_health: dict,
    older_firing_available: bool,
    tune_reference_available: bool,
) -> dict:
    """Five non-exclusive flags + the strongest-shifted feature list.

    No causal claim. The flags are *inputs* for the next diagnostic.
    """
    # Feature shift: any of the surfaced top-shifted features hits the
    # "notable" cutoff. Cutoff is descriptive — operator-tunable, not
    # a significance threshold.
    top_shift = top_shifted_features(feature_rows, k=TOP_SHIFTED_K)
    feature_shift_present = any(
        r.get("standardized_mean_diff") is not None
        and abs(float(r["standardized_mean_diff"])) >= float(STD_DIFF_NOTABLE)
        for r in top_shift
    )

    # Probability compression: recent group's max prob is far below the
    # threshold (mirrors Phase 1 ``probabilities_clustered_low`` cutoff).
    recent_max = recent_prob_stats.get("max")
    if recent_max is None:
        compression_present = False
    else:
        compression_present = bool((float(threshold) - float(recent_max)) >= float(COMPRESSION_GAP))

    # Data-quality warning: either group shows imputation-heavy features
    # or a non-trivial all-null row count.
    data_quality_warning = bool(
        recent_health.get("imputation_heavy_features")
        or firing_health.get("imputation_heavy_features")
        or int(recent_health.get("all_null_rows") or 0) > 0
        or int(firing_health.get("all_null_rows") or 0) > 0
    )

    return {
        "feature_shift_present": bool(feature_shift_present),
        "probability_compression_present": bool(compression_present),
        "data_quality_warning": bool(data_quality_warning),
        "older_firing_context_available": bool(older_firing_available),
        "threshold_tune_reference_available": bool(tune_reference_available),
        "strongest_shifted_features": [
            {
                "feature": r["feature"],
                "standardized_mean_diff": r["standardized_mean_diff"],
                "quantile_distance": r["quantile_distance"],
                "ks_statistic": r["ks_statistic"],
            }
            for r in top_shift
        ],
        "recommended_next_diagnostic": _recommend_next(
            feature_shift_present=feature_shift_present,
            compression_present=compression_present,
            data_quality_warning=data_quality_warning,
            older_firing_available=older_firing_available,
        ),
    }


def _recommend_next(
    *,
    feature_shift_present: bool,
    compression_present: bool,
    data_quality_warning: bool,
    older_firing_available: bool,
) -> str:
    """Choose the *cheapest* next read-only diagnostic, not a trading action."""
    if not older_firing_available:
        return "widen_older_window_or_increase_history_then_rerun_attribution"
    if data_quality_warning:
        return "investigate_imputation_or_null_pattern_before_other_attribution"
    if feature_shift_present and compression_present:
        return "drill_into_top_shifted_features_per_regime_bucket"
    if feature_shift_present:
        return "drill_into_top_shifted_features_per_regime_bucket"
    if compression_present:
        return "check_calibration_or_score_distribution_vs_training_slice"
    return "no_strong_attribution_signal_consider_held_out_oos_when_feasible"


# ------------------------------------------------------------------ #
# Report assembly
# ------------------------------------------------------------------ #


def build_report(
    *,
    target: str,
    horizon: int,
    active_manifest_path: Path,
    manifest: dict,
    model_path: Path,
    threshold_resolution: dict,
    total_rows: int,
    group_ranges: dict,
    groups: dict,
    feature_comparison: dict,
    attribution: dict,
    threshold_tune_ref: dict,
    warnings: list[str] | None = None,
) -> dict:
    return {
        "schema_version": 1,
        "audit_type": "regime_health_attribution",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "target": target,
        "horizon": horizon,
        "active_manifest_path": str(active_manifest_path),
        "active_manifest_version": manifest.get("version"),
        "model_path": str(model_path),
        "deployed_threshold": float(threshold_resolution["runtime_threshold"]),
        "threshold_source": threshold_resolution["threshold_source"],
        "manifest_threshold": threshold_resolution["manifest_threshold"],
        "artifact_threshold": threshold_resolution["artifact_threshold"],
        "threshold_mismatch_detected": bool(
            threshold_resolution["threshold_mismatch_detected"]
        ),
        "total_labeled_rows": int(total_rows),
        "group_ranges": group_ranges,
        "groups": groups,
        "feature_comparison": feature_comparison,
        "threshold_tune_reference": threshold_tune_ref,
        "attribution": attribution,
        "warnings": list(warnings or []),
        "scope_disclosure": (
            "read_only_attribution; no training; no threshold tuning; "
            "no promotion; descriptive only. Threshold resolved with "
            "server semantics (manifest first, artifact fallback). "
            "Groups are fixed chronologically; firing/non-firing partition "
            "uses the runtime threshold without any threshold search."
        ),
    }


# ------------------------------------------------------------------ #
# Orchestration
# ------------------------------------------------------------------ #


def run_audit(
    *,
    target: str,
    horizon: int,
    manifest_path: Path,
    models_dir: Path,
    duckdb_path: Path,
    view: str,
    recent_n: int,
    older_pct: float,
    highprob_low: float,
) -> dict:
    if not duckdb_path.is_file():
        raise SystemExit(f"DuckDB not found: {duckdb_path}")

    manifest = load_active_manifest(manifest_path)
    model_path = resolve_model_path(manifest, target, horizon, models_dir)
    if not model_path.is_file():
        raise SystemExit(f"Model artifact not found: {model_path}")

    joblib = _require("joblib", "python3 -m pip install joblib")
    artifact = joblib.load(model_path)
    pipeline = artifact.get("pipeline")
    calibrator = artifact.get("calibrator")
    model_obj = calibrator if calibrator is not None else pipeline
    if model_obj is None or not hasattr(model_obj, "predict_proba"):
        raise SystemExit("Model artifact missing pipeline/calibrator with predict_proba")

    manifest_thr = deployed_threshold(manifest, target, horizon)
    artifact_thr_raw = artifact.get("optimal_threshold")
    artifact_thr: float | None = None
    if artifact_thr_raw is not None:
        try:
            artifact_thr = float(artifact_thr_raw)
        except (TypeError, ValueError):
            artifact_thr = None
    threshold_resolution = resolve_runtime_threshold(manifest_thr, artifact_thr)
    threshold = float(threshold_resolution["runtime_threshold"])

    feature_columns = list(artifact.get("feature_columns") or [])
    if not feature_columns:
        raise SystemExit("Model artifact missing feature_columns")

    sub = load_labeled_events(duckdb_path, view, horizon, target)
    total_rows = int(len(sub))

    selection = select_groups(sub, recent_n=recent_n, older_pct=older_pct)
    group_ranges = {
        "total_rows": int(selection["total_rows"]),
        "recent_dormant": {
            "row_index_start": int(selection["recent_range"][0]),
            "row_index_end": int(selection["recent_range"][1]),
            "n": int(selection["recent_range"][1] - selection["recent_range"][0]),
        },
        "older_window": {
            "row_index_start": int(selection["older_range"][0]),
            "row_index_end": int(selection["older_range"][1]),
            "n": int(selection["older_range"][1] - selection["older_range"][0]),
            "pct_of_total": float(older_pct),
            "window_size_unfiltered": int(selection["older_window_size"]),
        },
    }

    pd = _require("pandas", "python3 -m pip install pandas")
    np = _require("numpy", "python3 -m pip install numpy")

    # Score recent group.
    recent_df = selection["recent_df"]
    recent_features = build_features_aligned(recent_df, feature_columns)
    if len(recent_features) > 0:
        recent_probs = score_probabilities(model_obj, recent_features)
    else:
        recent_probs = None
    recent_prob_stats = probability_stats(recent_probs, threshold)
    recent_health = model_input_health(recent_features, feature_columns)

    # Score older window.
    older_df = selection["older_df"]
    older_features = build_features_aligned(older_df, feature_columns)
    if len(older_features) > 0:
        older_probs = score_probabilities(model_obj, older_features)
    else:
        older_probs = None

    # Partition older window into firing vs high-prob nonfiring.
    if older_probs is None or len(older_df) == 0:
        firing_mask = np.zeros(len(older_df), dtype=bool)
        highprob_mask = np.zeros(len(older_df), dtype=bool)
    else:
        firing_mask = older_probs >= float(threshold)
        highprob_mask = (older_probs >= float(highprob_low)) & (older_probs < float(threshold))

    firing_df = older_df.iloc[np.where(firing_mask)[0]] if len(older_df) > 0 else older_df.iloc[0:0]
    firing_features = (
        older_features.iloc[np.where(firing_mask)[0]] if len(older_features) > 0 else older_features.iloc[0:0]
    )
    firing_probs = older_probs[firing_mask] if older_probs is not None else None
    firing_prob_stats = probability_stats(firing_probs, threshold)
    firing_health = model_input_health(firing_features, feature_columns)
    older_firing_available = bool(int(firing_prob_stats.get("n") or 0) > 0)

    highprob_features = (
        older_features.iloc[np.where(highprob_mask)[0]] if len(older_features) > 0 else older_features.iloc[0:0]
    )
    highprob_probs = older_probs[highprob_mask] if older_probs is not None else None
    highprob_prob_stats = probability_stats(highprob_probs, threshold)
    highprob_health = model_input_health(highprob_features, feature_columns)

    # Threshold-tune reference: report honestly that row-level data
    # isn't persisted in the active manifest.
    tune_ref = threshold_tune_meta(manifest, target, horizon)

    # Per-feature comparison: recent vs older-firing (the primary axis).
    feature_rows_recent_vs_firing = per_feature_comparison(
        recent_features, firing_features,
        group_a_name="recent_dormant", group_b_name="older_firing_context",
    )
    feature_rows_recent_vs_highprob = per_feature_comparison(
        recent_features, highprob_features,
        group_a_name="recent_dormant", group_b_name="older_high_probability_nonfiring",
    )

    attribution = attribution_summary(
        recent_prob_stats=recent_prob_stats,
        firing_prob_stats=firing_prob_stats,
        threshold=threshold,
        feature_rows=feature_rows_recent_vs_firing,
        recent_health=recent_health,
        firing_health=firing_health,
        older_firing_available=older_firing_available,
        tune_reference_available=bool(tune_ref.get("available")),
    )

    groups = {
        "recent_dormant": {
            "probability_stats": recent_prob_stats,
            "model_input_health": recent_health,
        },
        "older_firing_context": {
            "probability_stats": firing_prob_stats,
            "model_input_health": firing_health,
            "available": older_firing_available,
        },
        "older_high_probability_nonfiring": {
            "probability_stats": highprob_prob_stats,
            "model_input_health": highprob_health,
            "highprob_low_used": float(highprob_low),
        },
        "threshold_tune_reference": tune_ref,
    }

    feature_comparison = {
        "recent_vs_older_firing": feature_rows_recent_vs_firing,
        "recent_vs_older_high_probability_nonfiring": feature_rows_recent_vs_highprob,
        "top_shifted_recent_vs_older_firing": top_shifted_features(
            feature_rows_recent_vs_firing, k=TOP_SHIFTED_K
        ),
        "top_shifted_recent_vs_older_high_probability_nonfiring": top_shifted_features(
            feature_rows_recent_vs_highprob, k=TOP_SHIFTED_K
        ),
        "feature_columns_count": int(len(feature_columns)),
        "quantile_grid": list(QUANTILE_GRID),
        "std_diff_notable_cutoff": float(STD_DIFF_NOTABLE),
        "ks_backend": "scipy_ks_2samp_if_available_else_quantile_distance",
    }

    warnings: list[str] = []
    if threshold_resolution["threshold_mismatch_detected"]:
        warnings.append(
            f"threshold_mismatch:manifest={threshold_resolution['manifest_threshold']} "
            f"artifact={threshold_resolution['artifact_threshold']}"
        )
    if threshold_resolution["threshold_source"] == "artifact_fallback":
        warnings.append(
            "manifest_missing_threshold_for_target_horizon; used artifact fallback"
        )
    if not older_firing_available:
        warnings.append(
            f"older_firing_context_empty:older_pct={older_pct} "
            f"older_window_n={group_ranges['older_window']['n']}"
        )
    if int(group_ranges["older_window"]["n"]) == 0:
        warnings.append("older_window_collapsed_into_recent_dormant_tail")
    if not tune_ref.get("available"):
        warnings.append(
            f"threshold_tune_reference_unavailable:{tune_ref.get('reason', 'unspecified')}"
        )

    report = build_report(
        target=target,
        horizon=horizon,
        active_manifest_path=manifest_path,
        manifest=manifest,
        model_path=model_path,
        threshold_resolution=threshold_resolution,
        total_rows=total_rows,
        group_ranges=group_ranges,
        groups=groups,
        feature_comparison=feature_comparison,
        attribution=attribution,
        threshold_tune_ref=tune_ref,
        warnings=warnings,
    )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only Phase 2 regime-health attribution. Compares the "
            "recent dormant tail to older firing / near-firing rows "
            "across probability, feature distributions, and model-input "
            "health. No training, no threshold tuning, no promotion."
        ),
    )
    parser.add_argument("--target", default="reject")
    parser.add_argument("--horizon", type=int, default=15)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--models-dir", default=str(DEFAULT_MODELS_DIR))
    parser.add_argument("--duckdb", default=str(DEFAULT_DUCKDB))
    parser.add_argument("--view", default=DEFAULT_VIEW)
    parser.add_argument("--recent-n", type=int, default=DEFAULT_RECENT_N)
    parser.add_argument("--older-pct", type=float, default=DEFAULT_OLDER_PCT)
    parser.add_argument("--highprob-low", type=float, default=DEFAULT_HIGHPROB_LOW)
    parser.add_argument("--report", default="")
    args = parser.parse_args(argv)

    if not (0.0 < float(args.older_pct) <= 1.0):
        raise SystemExit("--older-pct must be in (0, 1].")
    if not (0.0 <= float(args.highprob_low) <= 1.0):
        raise SystemExit("--highprob-low must be in [0, 1].")
    if int(args.recent_n) < 1:
        raise SystemExit("--recent-n must be a positive integer.")

    report = run_audit(
        target=args.target,
        horizon=int(args.horizon),
        manifest_path=Path(args.manifest).resolve(),
        models_dir=Path(args.models_dir).resolve(),
        duckdb_path=Path(args.duckdb).resolve(),
        view=args.view,
        recent_n=int(args.recent_n),
        older_pct=float(args.older_pct),
        highprob_low=float(args.highprob_low),
    )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if args.report:
        report_path = Path(args.report).resolve()
    else:
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        report_path = REPORT_DIR / (
            f"regime_health_attribution_{args.target}_{args.horizon}m_{ts}.json"
        )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str))

    # ----- Console summary -------------------------------------------- #
    th = report["deployed_threshold"]
    src = report["threshold_source"]
    print(f"Attribution report written to {report_path}")
    print()
    print(
        f"=== {args.target}@{args.horizon}m   runtime threshold="
        f"{th:.6f} ({src}) ==="
    )
    print(f"Total labeled rows: {report['total_labeled_rows']}")
    gr = report["group_ranges"]
    print(
        f"recent_dormant n={gr['recent_dormant']['n']}  "
        f"older_window n={gr['older_window']['n']} "
        f"(latest {gr['older_window']['pct_of_total']:.0%} minus recent tail)"
    )
    print()

    g = report["groups"]
    print("[1] Probability comparison")
    rows = [
        ("recent_dormant", g["recent_dormant"]["probability_stats"]),
        ("older_firing_context", g["older_firing_context"]["probability_stats"]),
        ("older_high_prob_nonfiring", g["older_high_probability_nonfiring"]["probability_stats"]),
    ]
    hdr = (
        f"  {'group':<28} {'n':>6} {'sigs':>6} "
        f"{'min':>7} {'p25':>7} {'median':>7} {'p75':>7} {'p95':>7} {'max':>7} {'mean':>7}"
    )
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for name, ps in rows:
        n = ps.get("n") or 0
        if n == 0:
            print(f"  {name:<28} {n:>6} {'-':>6}  (empty)")
            continue
        print(
            f"  {name:<28} "
            f"{n:>6} {ps.get('signal_count') or 0:>6} "
            f"{ps.get('min'):>7.4f} {ps.get('p25'):>7.4f} {ps.get('median'):>7.4f} "
            f"{ps.get('p75'):>7.4f} {ps.get('p95'):>7.4f} {ps.get('max'):>7.4f} "
            f"{ps.get('mean'):>7.4f}"
        )

    print()
    print("[2] Top shifted features (recent_dormant vs older_firing_context)")
    top = report["feature_comparison"]["top_shifted_recent_vs_older_firing"]
    if not top:
        print("  (no usable comparable features)")
    else:
        hdr2 = f"  {'feature':<40} {'std_diff':>10} {'qd':>8} {'ks':>8}"
        print(hdr2)
        print("  " + "-" * (len(hdr2) - 2))
        for r in top:
            sd = r.get("standardized_mean_diff")
            qd = r.get("quantile_distance")
            ks = r.get("ks_statistic")
            print(
                f"  {str(r['feature'])[:40]:<40} "
                f"{(sd if sd is not None else float('nan')):>10.4f} "
                f"{(qd if qd is not None else float('nan')):>8.4f} "
                f"{(ks if ks is not None else float('nan')):>8.4f}"
            )

    print()
    print("[3] Attribution flags (independent — not a single cause)")
    attr = report["attribution"]
    for k in (
        "feature_shift_present",
        "probability_compression_present",
        "data_quality_warning",
        "older_firing_context_available",
        "threshold_tune_reference_available",
    ):
        print(f"  {k}: {attr[k]}")
    print(f"  recommended_next_diagnostic: {attr['recommended_next_diagnostic']}")

    print()
    if report["warnings"]:
        for w in report["warnings"]:
            print(f"[warning] {w}")
    print(f"[scope] {report['scope_disclosure']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
