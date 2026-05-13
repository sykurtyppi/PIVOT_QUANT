#!/usr/bin/env python3
"""Read-only Phase 2B regime data-quality diagnostic for a (target, horizon).

Phase 2 attribution (``scripts/audit_regime_health_attribution.py``,
PR #21) showed strong distributional shifts on `reject@15m` between the
recent dormant tail and older firing rows — notably ``atr_bps`` with
standardized mean diff ≈ -5.03 and KS ≈ 0.99. The attribution layer
also raised ``data_quality_warning=true`` and recommended
``investigate_imputation_or_null_pattern_before_other_attribution``.

This audit answers a single question, *strictly read-only*: do the
shifted features carry signs of data/feature-quality problems — nulls,
non-finite values, imputed defaults, repeated/constant values,
timestamp gaps or stale recent data — or do they look like clean
distribution shifts that reflect real market conditions?

The script does **not** decide whether the model edge is real or dead.
It only reports whether Phase 2 attribution should be trusted as-is or
needs a data-quality investigation first.

Hard scope contract:
- Read-only. No model training. No threshold search or tuning. No
  threshold changes. No model changes. No promotion. No walk-forward
  OOS. No performance or edge claims.
- Threshold resolved with the same precedence as
  ``server.ml_server.ModelRegistry`` and the regime-health scripts:
  manifest first, artifact only as fallback.
- Groups are fixed chronologically before any data quality is read
  (group selection is delegated to the Phase 2 attribution helper so
  the two reports describe the *same* row ranges).
- If a focus feature is missing from the model artifact, the audit
  reports that honestly under ``feature_columns_missing`` instead of
  silently dropping it or failing with an opaque error.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services._pybin import assert_python_310  # noqa: E402

assert_python_310()

# Reuse PR #21 helpers verbatim so threshold semantics, group selection
# and feature building never drift between the two reports.
import importlib.util

_ATTRIBUTION_PATH = ROOT / "scripts" / "audit_regime_health_attribution.py"
_attribution_spec = importlib.util.spec_from_file_location(
    "regime_health_attribution_module", _ATTRIBUTION_PATH
)
if _attribution_spec is None or _attribution_spec.loader is None:
    raise SystemExit(f"Could not load attribution module from {_ATTRIBUTION_PATH}")
attribution_mod = importlib.util.module_from_spec(_attribution_spec)
sys.modules["regime_health_attribution_module"] = attribution_mod
_attribution_spec.loader.exec_module(attribution_mod)

DEFAULT_DUCKDB = ROOT / "data" / "pivot_training.duckdb"
DEFAULT_VIEW = "training_events_v1"
DEFAULT_MANIFEST = ROOT / "data" / "models" / "manifest_active.json"
DEFAULT_MODELS_DIR = ROOT / "data" / "models"
REPORT_DIR = ROOT / "evidence" / "regime_data_quality"

# Defaults mirror PR #21 attribution.
DEFAULT_RECENT_N = 1000
DEFAULT_OLDER_PCT = 0.30
DEFAULT_HIGHPROB_LOW = 0.70

# Focus features called out by PR #21's live run. Defaults; can be
# overridden with ``--features``. We intentionally keep the default
# list small and explicit so a future PR #21 result with different top
# shifts can re-target without code changes.
DEFAULT_FOCUS_FEATURES: tuple[str, ...] = (
    "atr_bps",
    "ema_state",
    "ema_state_calc",
    "price_vs_ema21_bps",
    "monthly_pivot_dist_bps",
    "ema_spread_bps",
    "hist_break_rate",
    "gamma_mode",
    "distance_bps",
    "hist_reject_rate",
)

# Cutoffs (descriptive — NOT significance thresholds; surfaced in the
# report so an operator can re-evaluate later without re-reading code).
NULL_RATE_NOTABLE = 0.10           # >10% nulls in a group is noteworthy
NON_FINITE_RATE_NOTABLE = 0.01     # any non-finite at >1% is a smell
ZERO_RATE_HEAVY = 0.50             # >50% zeros in a numeric feature is suspect
REPEAT_CONCENTRATION = 0.50        # any single value covers >50% of group
CONSTANT_DISTINCT_MAX = 1          # n_distinct == 1 within the group
TOP_REPEAT_K = 3                   # surface the top-N repeated values
COMMON_DEFAULT_VALUES = (
    0.0, -1.0, 1.0, -999.0, 999.0,
)
RECENT_FRESHNESS_DAYS_NOTABLE = 2   # last ts_event older than ~2 days = stale
SPARSE_RECENT_NEEDED_ROWS = 100      # very low recent count -> sparse warning
QUANTILES_TO_REPORT = (0.0, 0.01, 0.05, 0.50, 0.95, 0.99, 1.0)


# ------------------------------------------------------------------ #
# Per-feature data-quality metrics
# ------------------------------------------------------------------ #


def _require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Missing dependency {module_name!r}: {exc}. {hint}")


def feature_quality_stats(series, *, dtype_hint: str | None = None) -> dict:
    """Per-group, per-feature data-quality stats.

    Schema is *fixed* — every key is always present so downstream
    consumers can diff group-for-group and feature-for-feature without
    branching on which side is "richer."
    """
    np = _require("numpy", "python3 -m pip install numpy")
    pd = _require("pandas", "python3 -m pip install pandas")
    base = {
        "row_count": 0,
        "null_count": 0,
        "null_rate": None,
        "non_finite_count": 0,
        "non_finite_rate": None,
        "zero_count": 0,
        "zero_rate": None,
        "distinct_count": 0,
        "top_repeated": [],
        "max_repeat_share": None,
        "appears_constant": False,
        "appears_imputed_default": False,
        "imputed_default_value": None,
        "min": None,
        "p01": None,
        "p05": None,
        "median": None,
        "mean": None,
        "p95": None,
        "p99": None,
        "max": None,
        "dtype_hint": dtype_hint or "unknown",
        "skip_reason": "",
    }
    if series is None:
        base["skip_reason"] = "column_absent"
        return base
    row_count = int(len(series))
    base["row_count"] = row_count
    if row_count == 0:
        base["skip_reason"] = "group_empty"
        return base

    null_mask = series.isna()
    null_count = int(null_mask.sum())
    base["null_count"] = null_count
    base["null_rate"] = float(null_count / row_count)

    # Numeric path: coerce. Treat object/string by factorize.
    if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_bool_dtype(series):
        as_float = series.astype(float)
        arr = as_float.to_numpy()
        finite_mask = np.isfinite(arr)
        non_null_finite = arr[finite_mask & ~null_mask.to_numpy()]
        non_finite_count = int(row_count - int(finite_mask.sum()) - null_count
                               if row_count - int(finite_mask.sum()) >= null_count
                               else max(0, row_count - int(finite_mask.sum()) - null_count))
        # More robust accounting: non-finite = NaN/Inf/-Inf; nulls in
        # numeric pandas Series are NaN, so subtract them to count only
        # Inf/-Inf separately.
        non_finite_total = int((~finite_mask).sum())
        non_finite_excl_null = max(0, non_finite_total - null_count)
        base["non_finite_count"] = non_finite_excl_null
        base["non_finite_rate"] = float(non_finite_excl_null / row_count)
        if non_null_finite.size == 0:
            base["skip_reason"] = "all_null_or_non_finite"
            return base
        zero_count = int(np.sum(non_null_finite == 0.0))
        base["zero_count"] = zero_count
        base["zero_rate"] = float(zero_count / non_null_finite.size)
        base["min"] = float(np.min(non_null_finite))
        base["max"] = float(np.max(non_null_finite))
        base["mean"] = float(np.mean(non_null_finite))
        base["median"] = float(np.percentile(non_null_finite, 50))
        base["p01"] = float(np.percentile(non_null_finite, 1))
        base["p05"] = float(np.percentile(non_null_finite, 5))
        base["p95"] = float(np.percentile(non_null_finite, 95))
        base["p99"] = float(np.percentile(non_null_finite, 99))
        # Distinct + repeat concentration on the finite values.
        # value_counts drops NaN by default; we already filtered to finite.
        vc = pd.Series(non_null_finite).value_counts(dropna=False)
        base["distinct_count"] = int(len(vc))
        top = []
        n_total = int(non_null_finite.size)
        for val, cnt in vc.head(int(TOP_REPEAT_K)).items():
            top.append({
                "value": float(val),
                "count": int(cnt),
                "share": float(cnt / n_total) if n_total > 0 else None,
            })
        base["top_repeated"] = top
        if top:
            base["max_repeat_share"] = float(top[0]["share"])
        base["appears_constant"] = bool(base["distinct_count"] <= int(CONSTANT_DISTINCT_MAX))
        # Imputed-default detection: most-common value is a known
        # default sentinel AND covers >REPEAT_CONCENTRATION of the
        # group. We never claim imputation; we surface the share and
        # let the operator decide.
        if top:
            top_val = float(top[0]["value"])
            if (
                base["max_repeat_share"] is not None
                and float(base["max_repeat_share"]) >= float(REPEAT_CONCENTRATION)
                and any(abs(top_val - float(d)) < 1e-12 for d in COMMON_DEFAULT_VALUES)
            ):
                base["appears_imputed_default"] = True
                base["imputed_default_value"] = top_val
        base["dtype_hint"] = dtype_hint or "numeric"
        return base

    # Non-numeric: report counts/distincts only; no quantiles.
    base["dtype_hint"] = dtype_hint or str(getattr(series, "dtype", "object"))
    non_null = series.dropna()
    vc = non_null.value_counts(dropna=False)
    base["distinct_count"] = int(len(vc))
    n_total = int(len(non_null))
    top = []
    for val, cnt in vc.head(int(TOP_REPEAT_K)).items():
        top.append({
            "value": (val if isinstance(val, (str, int, bool)) else str(val)),
            "count": int(cnt),
            "share": float(cnt / n_total) if n_total > 0 else None,
        })
    base["top_repeated"] = top
    if top:
        base["max_repeat_share"] = float(top[0]["share"])
    base["appears_constant"] = bool(base["distinct_count"] <= int(CONSTANT_DISTINCT_MAX))
    return base


# ------------------------------------------------------------------ #
# Timestamp / session health
# ------------------------------------------------------------------ #


def timestamp_health(df_recent, df_older, *, now_utc_ms: int | None = None) -> dict:
    """Min/max/gap/dup checks on `ts_event` for both groups + freshness.

    Returns a dict with a fixed schema even when ``ts_event`` is absent
    (``available=False``). Freshness is computed against ``now_utc_ms``
    if provided, else against ``datetime.datetime.now(timezone.utc)``.
    """
    np = _require("numpy", "python3 -m pip install numpy")
    pd = _require("pandas", "python3 -m pip install pandas")
    if now_utc_ms is None:
        now_utc_ms = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)

    def _summary(df, label: str) -> dict:
        if df is None or len(df) == 0 or "ts_event" not in df.columns:
            return {
                "label": label,
                "available": False,
                "reason": "ts_event_column_missing" if (df is not None and len(df) > 0) else "group_empty",
                "row_count": 0,
                "min_ts_ms": None, "max_ts_ms": None,
                "min_ts_iso": None, "max_ts_iso": None,
                "duplicate_ts_count": 0, "duplicate_ts_rate": None,
                "median_gap_seconds": None, "p95_gap_seconds": None,
                "max_gap_seconds": None,
            }
        ts = df["ts_event"].to_numpy(dtype="int64", copy=False)
        n = int(ts.size)
        if n == 0:
            return {
                "label": label, "available": False, "reason": "group_empty",
                "row_count": 0,
                "min_ts_ms": None, "max_ts_ms": None,
                "min_ts_iso": None, "max_ts_iso": None,
                "duplicate_ts_count": 0, "duplicate_ts_rate": None,
                "median_gap_seconds": None, "p95_gap_seconds": None,
                "max_gap_seconds": None,
            }
        min_ms, max_ms = int(ts.min()), int(ts.max())
        # Duplicates: count of rows whose ts_event repeats another row.
        ts_series = pd.Series(ts)
        dup_count = int(ts_series.duplicated(keep=False).sum())
        # Gaps between consecutive timestamps (assumes already
        # chronological — the upstream loader orders by ts_event).
        gaps_ms = np.diff(np.sort(ts))
        if gaps_ms.size > 0:
            median_gap = float(np.median(gaps_ms) / 1000.0)
            p95_gap = float(np.percentile(gaps_ms, 95) / 1000.0)
            max_gap = float(gaps_ms.max() / 1000.0)
        else:
            median_gap = p95_gap = max_gap = None
        return {
            "label": label,
            "available": True,
            "reason": "",
            "row_count": n,
            "min_ts_ms": min_ms,
            "max_ts_ms": max_ms,
            "min_ts_iso": dt.datetime.fromtimestamp(min_ms / 1000.0, dt.timezone.utc).isoformat(timespec="seconds"),
            "max_ts_iso": dt.datetime.fromtimestamp(max_ms / 1000.0, dt.timezone.utc).isoformat(timespec="seconds"),
            "duplicate_ts_count": dup_count,
            "duplicate_ts_rate": float(dup_count / n),
            "median_gap_seconds": median_gap,
            "p95_gap_seconds": p95_gap,
            "max_gap_seconds": max_gap,
        }

    recent_sum = _summary(df_recent, "recent_dormant")
    older_sum = _summary(df_older, "older_window")
    freshness_seconds: float | None = None
    freshness_days: float | None = None
    if recent_sum["available"] and recent_sum["max_ts_ms"] is not None:
        freshness_seconds = float((int(now_utc_ms) - int(recent_sum["max_ts_ms"])) / 1000.0)
        freshness_days = float(freshness_seconds / 86400.0)
    return {
        "now_utc_ms": int(now_utc_ms),
        "now_utc_iso": dt.datetime.fromtimestamp(int(now_utc_ms) / 1000.0, dt.timezone.utc).isoformat(timespec="seconds"),
        "recent": recent_sum,
        "older_window": older_sum,
        "recent_max_to_now_seconds": freshness_seconds,
        "recent_max_to_now_days": freshness_days,
    }


# ------------------------------------------------------------------ #
# Assessment
# ------------------------------------------------------------------ #


def determine_assessment(
    *,
    per_feature: dict,
    ts_health: dict,
    recent_n: int,
    columns_present: list[str],
    columns_missing: list[str],
) -> dict:
    """Translate per-feature + timestamp metrics into 5 flags + a status.

    Status is the cheapest *next-step direction*, not a causal claim:

      - ``insufficient_columns`` — too few of the focus features are
        present to assess. Operator should rerun with a refreshed
        feature build before trusting Phase 2 attribution.
      - ``possible_imputation_or_defaulting`` — at least one focus
        feature shows imputation-looking concentration in either group.
        Phase 2 shift might be an imputation artifact.
      - ``possible_stale_or_sparse_recent_data`` — recent group is
        sparse, recent ``ts_event`` is far from now, or timestamps
        clump suspiciously.
      - ``clean_shift_likely_real`` — no DQ red flags across the focus
        features or the timestamps; the Phase 2 distributional shift
        should be treated as describing real market regime conditions,
        NOT a tradeable claim.
      - ``unknown`` — escape hatch if none of the conditions apply.

    Boolean flags are independent and reported even when the status
    selects another branch — operators get the full picture.
    """
    # Focus-feature flags.
    atr_stats_recent = (per_feature.get("atr_bps") or {}).get("recent_dormant") or {}
    atr_stats_older = (per_feature.get("atr_bps") or {}).get("older_firing_context") or {}
    atr_quality_warning = bool(
        atr_stats_recent.get("appears_imputed_default")
        or atr_stats_older.get("appears_imputed_default")
        or atr_stats_recent.get("appears_constant")
        or atr_stats_older.get("appears_constant")
        or (atr_stats_recent.get("null_rate") is not None
            and float(atr_stats_recent.get("null_rate") or 0.0) >= float(NULL_RATE_NOTABLE))
        or (atr_stats_recent.get("zero_rate") is not None
            and float(atr_stats_recent.get("zero_rate") or 0.0) >= float(ZERO_RATE_HEAVY))
    )

    feature_null_warning = False
    feature_constant_warning = False
    for feat, by_group in per_feature.items():
        for grp_stats in (by_group or {}).values():
            if not isinstance(grp_stats, dict):
                continue
            nr = grp_stats.get("null_rate")
            if nr is not None and float(nr) >= float(NULL_RATE_NOTABLE):
                feature_null_warning = True
            nfr = grp_stats.get("non_finite_rate")
            if nfr is not None and float(nfr) >= float(NON_FINITE_RATE_NOTABLE):
                feature_null_warning = True
            if bool(grp_stats.get("appears_constant")):
                feature_constant_warning = True
            if bool(grp_stats.get("appears_imputed_default")):
                feature_constant_warning = True

    # Timestamp flags.
    rec_ts = (ts_health or {}).get("recent") or {}
    older_ts = (ts_health or {}).get("older_window") or {}
    timestamp_warning = False
    if rec_ts.get("available"):
        dup_rate = rec_ts.get("duplicate_ts_rate")
        if dup_rate is not None and float(dup_rate) > 0.0:
            timestamp_warning = True
    if older_ts.get("available"):
        dup_rate_o = older_ts.get("duplicate_ts_rate")
        if dup_rate_o is not None and float(dup_rate_o) > 0.0:
            timestamp_warning = True
    freshness_days = ts_health.get("recent_max_to_now_days") if ts_health else None
    if freshness_days is not None and float(freshness_days) >= float(RECENT_FRESHNESS_DAYS_NOTABLE):
        timestamp_warning = True

    recent_data_sparse_warning = bool(int(recent_n) < int(SPARSE_RECENT_NEEDED_ROWS))

    # Status precedence (most-blocking first).
    if len(columns_present) == 0:
        status = "insufficient_columns"
    elif feature_null_warning or feature_constant_warning or atr_quality_warning:
        status = "possible_imputation_or_defaulting"
    elif timestamp_warning or recent_data_sparse_warning:
        status = "possible_stale_or_sparse_recent_data"
    else:
        status = "clean_shift_likely_real"

    return {
        "data_quality_status": status,
        "atr_quality_warning": bool(atr_quality_warning),
        "feature_null_warning": bool(feature_null_warning),
        "feature_constant_warning": bool(feature_constant_warning),
        "timestamp_warning": bool(timestamp_warning),
        "recent_data_sparse_warning": bool(recent_data_sparse_warning),
        "columns_present": list(columns_present),
        "columns_missing": list(columns_missing),
        "recommended_next_step": _recommend_next_step(
            status,
            atr_quality_warning=atr_quality_warning,
            feature_null_warning=feature_null_warning,
            feature_constant_warning=feature_constant_warning,
            timestamp_warning=timestamp_warning,
            recent_data_sparse_warning=recent_data_sparse_warning,
        ),
    }


def _recommend_next_step(
    status: str,
    *,
    atr_quality_warning: bool,
    feature_null_warning: bool,
    feature_constant_warning: bool,
    timestamp_warning: bool,
    recent_data_sparse_warning: bool,
) -> str:
    """Choose the cheapest read-only next probe. NEVER a trading action."""
    if status == "insufficient_columns":
        return "rebuild_features_or_widen_feature_list_then_rerun_data_quality_audit"
    if atr_quality_warning:
        return "inspect_atr_bps_source_pipeline_and_imputation_path_before_trusting_attribution"
    if feature_constant_warning or feature_null_warning:
        return "inspect_feature_pipeline_for_imputation_or_null_path_before_trusting_attribution"
    if timestamp_warning:
        return "inspect_event_writer_timestamps_and_session_coverage_before_trusting_attribution"
    if recent_data_sparse_warning:
        return "wait_for_more_recent_rows_or_increase_recent_n_before_trusting_attribution"
    return "phase_2_attribution_can_be_treated_as_describing_real_distribution_shift"


# ------------------------------------------------------------------ #
# Orchestration
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
    per_feature: dict,
    feature_columns_present: list[str],
    feature_columns_missing: list[str],
    timestamp_health_report: dict,
    assessment: dict,
    warnings: list[str] | None = None,
) -> dict:
    return {
        "schema_version": 1,
        "audit_type": "regime_data_quality",
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
        "feature_columns_focus": list(per_feature.keys()),
        "feature_columns_present": list(feature_columns_present),
        "feature_columns_missing": list(feature_columns_missing),
        "per_feature_quality": per_feature,
        "timestamp_health": timestamp_health_report,
        "data_quality_assessment": assessment,
        "warnings": list(warnings or []),
        "scope_disclosure": (
            "read_only_data_quality; no training; no threshold tuning; "
            "no promotion; no edge claim; describes feature/data shape only. "
            "Threshold resolved with server semantics (manifest first, "
            "artifact fallback). Groups are fixed chronologically by "
            "delegation to the Phase 2 attribution helper; the data-quality "
            "audit performs no threshold search."
        ),
    }


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
    focus_features: tuple[str, ...] | list[str],
    now_utc_ms: int | None = None,
) -> dict:
    if not duckdb_path.is_file():
        raise SystemExit(f"DuckDB not found: {duckdb_path}")

    manifest = attribution_mod.load_active_manifest(manifest_path)
    model_path = attribution_mod.resolve_model_path(manifest, target, horizon, models_dir)
    if not model_path.is_file():
        raise SystemExit(f"Model artifact not found: {model_path}")

    joblib = _require("joblib", "python3 -m pip install joblib")
    artifact = joblib.load(model_path)

    manifest_thr = attribution_mod.deployed_threshold(manifest, target, horizon)
    artifact_thr_raw = artifact.get("optimal_threshold")
    artifact_thr: float | None = None
    if artifact_thr_raw is not None:
        try:
            artifact_thr = float(artifact_thr_raw)
        except (TypeError, ValueError):
            artifact_thr = None
    threshold_resolution = attribution_mod.resolve_runtime_threshold(
        manifest_thr, artifact_thr,
    )
    threshold = float(threshold_resolution["runtime_threshold"])

    feature_columns = list(artifact.get("feature_columns") or [])
    if not feature_columns:
        raise SystemExit("Model artifact missing feature_columns")

    sub = attribution_mod.load_labeled_events(duckdb_path, view, horizon, target)
    total_rows = int(len(sub))

    # Group selection delegated to PR #21 so the two audits report on
    # the same rows. Phase 2's older_window already excludes recent.
    selection = attribution_mod.select_groups(sub, recent_n=recent_n, older_pct=older_pct)
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
        },
    }

    pd = _require("pandas", "python3 -m pip install pandas")
    np = _require("numpy", "python3 -m pip install numpy")

    recent_df = selection["recent_df"]
    older_df = selection["older_df"]

    # Build features once per group (single predict_proba — only used
    # to partition older_window into firing vs near-firing).
    recent_features = attribution_mod.build_features_aligned(recent_df, feature_columns)
    older_features = attribution_mod.build_features_aligned(older_df, feature_columns)
    pipeline = artifact.get("pipeline")
    calibrator = artifact.get("calibrator")
    model_obj = calibrator if calibrator is not None else pipeline
    if model_obj is None or not hasattr(model_obj, "predict_proba"):
        raise SystemExit("Model artifact missing pipeline/calibrator with predict_proba")
    if len(older_features) > 0:
        older_probs = attribution_mod.score_probabilities(model_obj, older_features)
    else:
        older_probs = None
    if older_probs is None or len(older_df) == 0:
        firing_mask = np.zeros(len(older_df), dtype=bool)
        highprob_mask = np.zeros(len(older_df), dtype=bool)
    else:
        firing_mask = older_probs >= float(threshold)
        highprob_mask = (older_probs >= float(highprob_low)) & (older_probs < float(threshold))

    firing_features = (
        older_features.iloc[np.where(firing_mask)[0]] if len(older_features) > 0 else older_features.iloc[0:0]
    )
    highprob_features = (
        older_features.iloc[np.where(highprob_mask)[0]] if len(older_features) > 0 else older_features.iloc[0:0]
    )

    # Resolve which focus features are actually in the model artifact's
    # feature columns. We deliberately do NOT silently drop unknown
    # features; they go into ``feature_columns_missing`` and produce a
    # ``column_absent`` skip in per_feature_quality so the JSON shape
    # stays uniform.
    focus_list = list(dict.fromkeys(focus_features))  # de-dupe, preserve order
    present = [c for c in focus_list if c in feature_columns]
    missing = [c for c in focus_list if c not in feature_columns]

    per_feature: dict[str, dict] = {}
    for feat in focus_list:
        per_feature[feat] = {
            "recent_dormant": feature_quality_stats(
                recent_features[feat] if feat in recent_features.columns else None,
            ),
            "older_firing_context": feature_quality_stats(
                firing_features[feat] if feat in firing_features.columns else None,
            ),
            "older_high_prob_nonfiring": feature_quality_stats(
                highprob_features[feat] if feat in highprob_features.columns else None,
            ),
        }

    ts_health_report = timestamp_health(recent_df, older_df, now_utc_ms=now_utc_ms)

    assessment = determine_assessment(
        per_feature=per_feature,
        ts_health=ts_health_report,
        recent_n=int(group_ranges["recent_dormant"]["n"]),
        columns_present=present,
        columns_missing=missing,
    )

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
    if missing:
        warnings.append(f"feature_columns_missing_from_artifact:{','.join(missing)}")
    if assessment["recent_data_sparse_warning"]:
        warnings.append(
            f"recent_data_sparse:recent_n={group_ranges['recent_dormant']['n']} "
            f"min_required={SPARSE_RECENT_NEEDED_ROWS}"
        )
    if assessment["timestamp_warning"]:
        warnings.append("timestamp_health_anomaly_in_recent_or_older_window")

    return build_report(
        target=target,
        horizon=horizon,
        active_manifest_path=manifest_path,
        manifest=manifest,
        model_path=model_path,
        threshold_resolution=threshold_resolution,
        total_rows=total_rows,
        group_ranges=group_ranges,
        per_feature=per_feature,
        feature_columns_present=present,
        feature_columns_missing=missing,
        timestamp_health_report=ts_health_report,
        assessment=assessment,
        warnings=warnings,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only Phase 2B regime data-quality audit. Determines "
            "whether the shifts surfaced by Phase 2 attribution reflect "
            "real distribution change or data/feature-quality issues. "
            "No training, no threshold tuning, no promotion."
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
    parser.add_argument(
        "--features",
        default=",".join(DEFAULT_FOCUS_FEATURES),
        help="Comma-separated focus feature list (defaults to PR #21 top shifts).",
    )
    parser.add_argument("--report", default="")
    args = parser.parse_args(argv)

    if not (0.0 < float(args.older_pct) <= 1.0):
        raise SystemExit("--older-pct must be in (0, 1].")
    if not (0.0 <= float(args.highprob_low) <= 1.0):
        raise SystemExit("--highprob-low must be in [0, 1].")
    if int(args.recent_n) < 1:
        raise SystemExit("--recent-n must be a positive integer.")
    features = tuple(f.strip() for f in args.features.split(",") if f.strip())
    if not features:
        raise SystemExit("--features must be a non-empty comma-separated list.")

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
        focus_features=features,
    )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if args.report:
        report_path = Path(args.report).resolve()
    else:
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        report_path = REPORT_DIR / (
            f"regime_data_quality_{args.target}_{args.horizon}m_{ts}.json"
        )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str))

    # ----- Console summary -------------------------------------------- #
    th = report["deployed_threshold"]
    src = report["threshold_source"]
    print(f"Data-quality report written to {report_path}")
    print()
    print(
        f"=== {args.target}@{args.horizon}m   runtime threshold="
        f"{th:.6f} ({src}) ==="
    )
    gr = report["group_ranges"]
    print(
        f"recent_dormant n={gr['recent_dormant']['n']}  "
        f"older_window n={gr['older_window']['n']}"
    )

    a = report["data_quality_assessment"]
    print()
    print(f"[status] {a['data_quality_status']}")
    flags = (
        "atr_quality_warning",
        "feature_null_warning",
        "feature_constant_warning",
        "timestamp_warning",
        "recent_data_sparse_warning",
    )
    for k in flags:
        print(f"  {k}: {a[k]}")
    print(f"[recommended_next_step] {a['recommended_next_step']}")

    if a["columns_missing"]:
        print(f"[missing focus features] {', '.join(a['columns_missing'])}")

    # ATR call-out: compact recent vs older summary.
    atr = report["per_feature_quality"].get("atr_bps") or {}
    atr_r = atr.get("recent_dormant") or {}
    atr_o = atr.get("older_firing_context") or {}
    print()
    print("[atr_bps recent_dormant vs older_firing_context]")
    print(
        f"  recent      n={atr_r.get('row_count')} "
        f"null_rate={(atr_r.get('null_rate') or 0):.3f} "
        f"zero_rate={(atr_r.get('zero_rate') or 0):.3f} "
        f"distinct={atr_r.get('distinct_count')} "
        f"min={atr_r.get('min')} max={atr_r.get('max')} "
        f"mean={atr_r.get('mean')}"
    )
    print(
        f"  older_firing n={atr_o.get('row_count')} "
        f"null_rate={(atr_o.get('null_rate') or 0):.3f} "
        f"zero_rate={(atr_o.get('zero_rate') or 0):.3f} "
        f"distinct={atr_o.get('distinct_count')} "
        f"min={atr_o.get('min')} max={atr_o.get('max')} "
        f"mean={atr_o.get('mean')}"
    )
    if atr_r.get("appears_imputed_default"):
        print(
            f"  WARN recent atr_bps appears imputed: "
            f"value={atr_r.get('imputed_default_value')} "
            f"share={atr_r.get('max_repeat_share')}"
        )
    if atr_r.get("appears_constant"):
        print("  WARN recent atr_bps appears constant within the group.")

    # Final summary line — descriptive, not prescriptive.
    print()
    if a["data_quality_status"] == "clean_shift_likely_real":
        print(
            "[summary] No data-quality red flags in the focus features or "
            "timestamps. The Phase 2 attribution can be read as describing "
            "real distributional shift — this is NOT a claim about edge."
        )
    elif a["data_quality_status"] == "possible_imputation_or_defaulting":
        print(
            "[summary] Imputation / null / constant pattern detected. The "
            "Phase 2 attribution should be revisited AFTER the feature "
            "pipeline is inspected — the shift may be a data-quality "
            "artifact rather than market regime change."
        )
    elif a["data_quality_status"] == "possible_stale_or_sparse_recent_data":
        print(
            "[summary] Recent rows look sparse or stale. The Phase 2 "
            "attribution may be reading thin coverage as shift; consult "
            "the event writer / collector health before trusting it."
        )
    elif a["data_quality_status"] == "insufficient_columns":
        print(
            "[summary] None of the requested focus features are in the "
            "model artifact — the audit cannot assess data quality on "
            "the columns Phase 2 surfaced."
        )
    else:
        print("[summary] Indeterminate. See per-feature detail.")

    if report["warnings"]:
        for w in report["warnings"]:
            print(f"[warning] {w}")
    print(f"[scope] {report['scope_disclosure']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
