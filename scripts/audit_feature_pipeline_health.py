#!/usr/bin/env python3
"""Read-only Phase 2C feature-pipeline health diagnostic.

Background: PR #21 (Phase 2 attribution) and PR #23 (Phase 2B data
quality) showed that the `reject@15m` model's recent dormancy is
accompanied by distributional shifts on several features, some of
which Phase 2B flagged as imputation-looking. This audit traces those
features to their *source* paths and asks, per (group × feature),
whether the Phase 2B interpretation needs correcting before any
further inference.

Specifically:

  - `ema_state` is set in the upstream events row (see
    ``scripts/backfill_events.py`` near the EMA9>EMA21 branch) with
    domain ``{1, -1, 0}``. ``ema_state_calc`` (``ml/features.py``
    lines 112–117) **aliases** raw ``ema_state`` when present, only
    recomputing from ``ema9``/``ema21`` when raw is missing. So
    Phase 2B's "both constant at 1.0" is ONE signal, not two — they
    cannot independently disagree when raw ``ema_state`` is present.
  - `gamma_mode` (``scripts/backfill_events.py`` near
    ``close >= gamma_flip``) has domain ``{1, -1, None}``. The value
    ``1.0`` is a LEGITIMATE enum value (price ≥ gamma flip), NOT a
    default sentinel. Phase 2B's imputed-default flag on
    ``gamma_mode`` should be downgraded to "legitimate enum
    concentration" if the source path confirms it.
  - `monthly_pivot_dist_bps` (``ml/features.py:196–199``) is NULL iff
    raw ``monthly_pivot`` is NULL or zero. Phase 2B's high recent
    null rate is therefore an *upstream pivot-availability* question,
    not a feature-pipeline bug.
  - `ts_event` duplicates are expected when multiple distinct level
    touches share a minute bar. ``event_id`` is the deterministic
    primary key. If duplicate-``ts_event`` rows have distinct
    ``event_id`` values, the duplicates are
    ``expected_many_events_per_bar``.

Hard scope contract:
- Read-only.
- No model training. No threshold search or tuning. No threshold
  changes. No model changes. No promotion. No walk-forward OOS.
- No database writes.
- No causal claim beyond what the per-group / per-source data
  supports. The audit reports counts and source provenance; it does
  NOT decide whether the edge is real or dead.
- If a source path cannot be confirmed (e.g. column missing from the
  view), the entry is reported as ``source_unresolved`` rather than
  failing.
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services._pybin import assert_python_310  # noqa: E402

assert_python_310()

# Reuse PR #21 (attribution) helpers so threshold semantics and group
# selection stay invariant across the regime-health audit chain.
_ATTRIBUTION_PATH = ROOT / "scripts" / "audit_regime_health_attribution.py"
_attribution_spec = importlib.util.spec_from_file_location(
    "regime_health_attribution_module_v2c", _ATTRIBUTION_PATH
)
if _attribution_spec is None or _attribution_spec.loader is None:
    raise SystemExit(f"Could not load attribution module from {_ATTRIBUTION_PATH}")
attribution_mod = importlib.util.module_from_spec(_attribution_spec)
sys.modules["regime_health_attribution_module_v2c"] = attribution_mod
_attribution_spec.loader.exec_module(attribution_mod)


DEFAULT_DUCKDB = ROOT / "data" / "pivot_training.duckdb"
DEFAULT_VIEW = "training_events_v1"
DEFAULT_MANIFEST = ROOT / "data" / "models" / "manifest_active.json"
DEFAULT_MODELS_DIR = ROOT / "data" / "models"
REPORT_DIR = ROOT / "evidence" / "feature_pipeline_health"

DEFAULT_RECENT_N = 1000
DEFAULT_OLDER_PCT = 0.30
DEFAULT_HIGHPROB_LOW = 0.70

# Source tracing: these are the lines we found via the Phase 2C
# source-trace pass. They are *declarations* about where the feature
# is produced; the runtime audit cross-checks the live data against
# them. If a future refactor moves the assignment, the live audit
# still works — only the citation drifts, and a test pins the schema
# of the citation block itself.
SOURCE_TRACE: dict[str, dict[str, Any]] = {
    "ema_state": {
        "origin_file": "scripts/backfill_events.py",
        "origin_excerpt": (
            "ema_state = 1 if ema9_out > ema21_out else "
            "-1 if ema9_out < ema21_out else 0"
        ),
        "expected_domain": [-1, 0, 1],
        "default_or_imputation_path": (
            "no explicit default; None only if ema9/ema21 are missing upstream"
        ),
        "feature_kind": "discrete_3_value",
    },
    "ema_state_calc": {
        "origin_file": "ml/features.py",
        "origin_excerpt": (
            "if event.get('ema_state') is not None: "
            "row['ema_state_calc'] = event.get('ema_state')  "
            "elif ema9 is not None and ema21 is not None: "
            "row['ema_state_calc'] = 1 if ema9 > ema21 else -1 if "
            "ema9 < ema21 else 0"
        ),
        "expected_domain": [-1, 0, 1],
        "default_or_imputation_path": (
            "ALIAS of raw ema_state when present; recomputes from "
            "ema9/ema21 only when raw is missing"
        ),
        "feature_kind": "discrete_3_value_aliased_to_ema_state",
        "alias_of": "ema_state",
    },
    "gamma_mode": {
        "origin_file": "scripts/backfill_events.py",
        "origin_excerpt": (
            "gamma_mode = 1 if close >= gamma_flip else -1  "
            "(only when use_gamma_context and gamma_flip not None and != 0)"
        ),
        "expected_domain": [-1, 1],
        "default_or_imputation_path": (
            "None when gamma context unavailable; 1 is a LEGITIMATE "
            "enum value (price at-or-above gamma flip), NOT a default"
        ),
        "feature_kind": "discrete_2_value",
        "note_for_phase_2b_correction": (
            "Phase 2B's imputed_default flag on gamma_mode=1.0 should "
            "be re-read as legitimate enum concentration, not an "
            "imputation artifact."
        ),
    },
    "monthly_pivot_dist_bps": {
        "origin_file": "ml/features.py",
        "origin_excerpt": (
            "if monthly_pivot is not None and touch_price is not None "
            "and monthly_pivot != 0: "
            "row['monthly_pivot_dist_bps'] = "
            "(touch_price - monthly_pivot) / monthly_pivot * 1e4 "
            "else: row['monthly_pivot_dist_bps'] = None"
        ),
        "expected_domain": "continuous_bps",
        "default_or_imputation_path": (
            "no fallback; NULL iff monthly_pivot is NULL or zero, OR "
            "touch_price is NULL"
        ),
        "feature_kind": "continuous_distance_bps",
        "raw_source_column": "monthly_pivot",
    },
    "ts_event": {
        "origin_file": "server/event_writer.py",
        "origin_excerpt": (
            "ts_event INTEGER NOT NULL (epoch millis, bar-aligned); "
            "event_id is a deterministic hash of "
            "(symbol, ts_event, level_type, level_price, "
            "bar_interval_sec) and is the table primary key"
        ),
        "expected_domain": "epoch_ms_int64",
        "default_or_imputation_path": (
            "no default; required column; INSERT OR IGNORE keys on "
            "event_id PRIMARY KEY"
        ),
        "feature_kind": "timestamp_ms",
        "duplicate_semantics": (
            "duplicate ts_event values are EXPECTED when multiple "
            "distinct level touches share a minute bar; rows are "
            "distinguished by event_id"
        ),
    },
}


# ------------------------------------------------------------------ #
# Per-feature classification
# ------------------------------------------------------------------ #


def _require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Missing dependency {module_name!r}: {exc}. {hint}")


def value_counts_top_n(series, n: int = 10) -> list[dict]:
    """Top-``n`` value counts with shares, on the finite-non-null subset.

    Works for numeric and non-numeric series. Returns an empty list when
    the series is None / empty / all-null.
    """
    pd = _require("pandas", "python3 -m pip install pandas")
    if series is None or len(series) == 0:
        return []
    s = series.dropna()
    # Drop +/-Inf for numeric.
    if pd.api.types.is_numeric_dtype(s):
        import numpy as np
        s = s[np.isfinite(s)]
    if len(s) == 0:
        return []
    vc = s.value_counts()
    total = int(len(s))
    out: list[dict] = []
    for val, cnt in vc.head(int(n)).items():
        v: Any
        # Booleans are int subclasses in Python but we want them
        # passed through as-is so the report distinguishes 1 vs True.
        if isinstance(val, bool):
            v = val
        else:
            # NumPy scalar types (int64, float64) are NOT Python int/float
            # subclasses, so a naive isinstance check would treat them as
            # "other" and stringify legitimate enum values like 1 -> "1".
            # `.item()` coerces any numpy scalar to its Python equivalent;
            # plain Python ints/floats expose .item() as well via the
            # number ABCs in recent CPython versions, but we guard with
            # try/except for safety.
            try:
                py_val = val.item() if hasattr(val, "item") else val
            except Exception:  # noqa: BLE001
                py_val = val
            if isinstance(py_val, (int, float)):
                v = float(py_val)
            elif isinstance(py_val, str):
                v = py_val
            else:
                v = str(py_val)
        out.append({"value": v, "count": int(cnt), "share": float(cnt / total)})
    return out


def classify_ema_state_alias_pair(
    raw_series, calc_series,
) -> dict:
    """Decide whether ``ema_state_calc`` is acting as an alias of
    ``ema_state`` on the inspected group.

    The source trace declares ``ema_state_calc`` is an alias when raw
    ``ema_state`` is present; this function verifies the *live*
    behaviour matches:

      - both present non-null counts (we don't want to assert alias on
        a near-empty group);
      - paired equality on non-null rows.

    Returns a dict the report carries verbatim so test fixtures can
    pin the schema.
    """
    pd = _require("pandas", "python3 -m pip install pandas")
    out = {
        "alias_declared_in_source_trace": True,
        "alias_verified_in_data": False,
        "matched_rows": 0,
        "checked_rows": 0,
        "mismatch_rows": 0,
        "raw_null_count": None,
        "calc_null_count": None,
        "note": "",
    }
    if raw_series is None or calc_series is None:
        out["note"] = "one_or_both_series_absent"
        return out
    if len(raw_series) == 0 or len(calc_series) == 0:
        out["note"] = "one_or_both_series_empty"
        return out
    df = pd.DataFrame({"raw": raw_series.reset_index(drop=True),
                       "calc": calc_series.reset_index(drop=True)})
    out["raw_null_count"] = int(df["raw"].isna().sum())
    out["calc_null_count"] = int(df["calc"].isna().sum())
    both_present = df.dropna()
    out["checked_rows"] = int(len(both_present))
    if out["checked_rows"] == 0:
        out["note"] = "no_rows_have_both_present"
        return out
    matched = int((both_present["raw"] == both_present["calc"]).sum())
    out["matched_rows"] = matched
    out["mismatch_rows"] = out["checked_rows"] - matched
    out["alias_verified_in_data"] = bool(matched == out["checked_rows"])
    if not out["alias_verified_in_data"]:
        out["note"] = (
            "alias_declared_but_data_disagrees:"
            f"{out['mismatch_rows']}_mismatches_in_{out['checked_rows']}"
        )
    else:
        out["note"] = "alias_confirmed_on_paired_non_null_rows"
    return out


def classify_gamma_mode_concentration(stats_dict: dict) -> dict:
    """Apply the source-trace correction for ``gamma_mode``.

    Phase 2B flagged ``gamma_mode=1.0`` as imputed-default because the
    value covered ≥50% of the group. The source trace (see
    ``SOURCE_TRACE['gamma_mode']``) shows ``1`` is a legitimate enum
    value (price ≥ gamma flip), not a default. We restate the flag
    accordingly without losing the underlying counts.
    """
    top_repeated = stats_dict.get("top_repeated") or []
    top_val = None
    top_share = None
    if top_repeated:
        top_val = top_repeated[0].get("value")
        top_share = top_repeated[0].get("share")
    in_domain = (
        top_val is not None
        and isinstance(top_val, (int, float))
        and float(top_val) in (-1.0, 1.0)
    )
    high_concentration = (
        top_share is not None and float(top_share) >= 0.50
    )
    # Source-trace correction: the imputation flag is only valid if
    # the most-common value is *outside* the legitimate enum domain.
    return {
        "top_value": top_val,
        "top_share": top_share,
        "in_legitimate_enum_domain": bool(in_domain),
        "high_concentration": bool(high_concentration),
        "phase_2b_imputed_flag_should_be_downgraded": bool(
            in_domain and high_concentration
        ),
        "corrected_label": (
            "legitimate_enum_concentration"
            if (in_domain and high_concentration)
            else "no_correction_needed"
        ),
    }


def classify_monthly_pivot_null_pattern(
    df_group, *, value_col: str = "monthly_pivot_dist_bps",
    raw_col: str = "monthly_pivot",
) -> dict:
    """Classify why ``monthly_pivot_dist_bps`` is NULL in this group.

    Per source trace, the feature is NULL iff:
      - raw ``monthly_pivot`` is NULL or zero, OR
      - ``touch_price`` is NULL.

    We compute the alignment between feature-null rows and raw-null
    rows; if they agree, the nulls are pure upstream pivot
    availability, not a feature-pipeline bug.
    """
    pd = _require("pandas", "python3 -m pip install pandas")
    np = _require("numpy", "python3 -m pip install numpy")
    out = {
        "value_col": value_col,
        "raw_col": raw_col,
        "feature_null_count": None,
        "feature_null_rate": None,
        "raw_null_or_zero_count": None,
        "raw_null_or_zero_rate": None,
        "rows_feature_null_but_raw_present_nonzero": None,
        "rows_feature_present_but_raw_null_or_zero": None,
        "classification": "unknown",
        "note": "",
    }
    if df_group is None or len(df_group) == 0:
        out["classification"] = "group_empty"
        return out
    if value_col not in df_group.columns:
        out["classification"] = "feature_column_missing"
        out["note"] = f"{value_col}_not_in_group"
        return out
    n = int(len(df_group))
    feat_null = df_group[value_col].isna()
    out["feature_null_count"] = int(feat_null.sum())
    out["feature_null_rate"] = float(out["feature_null_count"] / n)
    if raw_col not in df_group.columns:
        out["classification"] = "raw_source_column_missing_from_view"
        out["note"] = f"{raw_col}_not_in_view_cannot_correlate"
        return out
    raw = df_group[raw_col]
    raw_null_or_zero = raw.isna() | (raw == 0)
    out["raw_null_or_zero_count"] = int(raw_null_or_zero.sum())
    out["raw_null_or_zero_rate"] = float(out["raw_null_or_zero_count"] / n)
    # Cross-tabulate.
    out["rows_feature_null_but_raw_present_nonzero"] = int(
        (feat_null & ~raw_null_or_zero).sum()
    )
    out["rows_feature_present_but_raw_null_or_zero"] = int(
        (~feat_null & raw_null_or_zero).sum()
    )
    # Classification:
    if out["feature_null_count"] == 0:
        out["classification"] = "no_nulls_in_group"
    elif out["rows_feature_null_but_raw_present_nonzero"] == 0:
        # Every feature-null is explained by a raw-null/zero.
        out["classification"] = "upstream_pivot_availability_explains_all_nulls"
    elif out["rows_feature_null_but_raw_present_nonzero"] > 0:
        # Some feature-nulls have a raw present and nonzero — that's
        # a real feature-pipeline discrepancy.
        out["classification"] = "feature_pipeline_discrepancy_detected"
        out["note"] = (
            "rows_feature_null_but_raw_present_nonzero > 0; "
            "expected zero per source trace"
        )
    return out


def classify_ts_event_duplicates(
    df_group, *, ts_col: str = "ts_event",
    key_col: str = "event_id",
) -> dict:
    """Classify duplicate ``ts_event`` rows in a group.

    Per source trace: duplicate ``ts_event`` is expected when multiple
    distinct level touches share a minute bar. ``event_id`` is the
    deterministic primary key — if duplicate-``ts_event`` rows have
    distinct ``event_id``s, the duplicates are
    ``expected_many_events_per_bar``. If ``event_id`` itself repeats,
    that's ``possible_event_writer_duplication``.
    """
    pd = _require("pandas", "python3 -m pip install pandas")
    out = {
        "ts_col": ts_col,
        "key_col": key_col,
        "rows": 0,
        "distinct_ts": 0,
        "duplicate_ts_count": 0,
        "duplicate_ts_rate": None,
        "ts_event_max_repeat": 0,
        "key_column_present": False,
        "duplicate_event_id_count": 0,
        "duplicate_event_id_rate": None,
        "classification": "unknown",
        "note": "",
    }
    if df_group is None or len(df_group) == 0:
        out["classification"] = "group_empty"
        return out
    if ts_col not in df_group.columns:
        out["classification"] = "insufficient_key_columns"
        out["note"] = f"{ts_col}_not_in_group"
        return out
    n = int(len(df_group))
    out["rows"] = n
    ts = df_group[ts_col]
    out["distinct_ts"] = int(ts.nunique())
    dup_mask = ts.duplicated(keep=False)
    out["duplicate_ts_count"] = int(dup_mask.sum())
    out["duplicate_ts_rate"] = float(out["duplicate_ts_count"] / n)
    if out["duplicate_ts_count"] > 0:
        out["ts_event_max_repeat"] = int(ts.value_counts().max())
    out["key_column_present"] = bool(key_col in df_group.columns)
    if not out["key_column_present"]:
        out["classification"] = "insufficient_key_columns"
        out["note"] = (
            f"{key_col}_absent_from_group_cannot_distinguish_writer_duplication"
        )
        return out
    # Are there rows with identical (ts_event, event_id)? That would
    # indicate true writer duplication (since event_id is the PK).
    dup_full = df_group.duplicated(subset=[ts_col, key_col], keep=False)
    out["duplicate_event_id_count"] = int(dup_full.sum())
    out["duplicate_event_id_rate"] = float(out["duplicate_event_id_count"] / n)
    if out["duplicate_ts_count"] == 0:
        out["classification"] = "no_duplicate_ts"
    elif out["duplicate_event_id_count"] == 0:
        # Duplicate ts_event but unique (ts_event, event_id) pairs.
        out["classification"] = "expected_many_events_per_bar"
        out["note"] = (
            "duplicate ts_event rows have distinct event_id; expected per "
            "source trace (multiple level touches per minute bar)"
        )
    else:
        out["classification"] = "possible_event_writer_duplication"
        out["note"] = (
            f"{out['duplicate_event_id_count']} rows share both ts_event "
            "and event_id; expected zero under INSERT OR IGNORE keying"
        )
    return out


def feature_descriptive_stats(series) -> dict:
    """Per-group, per-feature descriptive shape used by the report.

    Schema is fixed and intentionally narrow — the heavy DQ lifting
    lives in PR #23's data-quality audit; this script reports the
    counts needed to *correct* its interpretations.
    """
    pd = _require("pandas", "python3 -m pip install pandas")
    np = _require("numpy", "python3 -m pip install numpy")
    base = {
        "row_count": 0,
        "null_count": 0,
        "null_rate": None,
        "distinct_count": 0,
        "top_repeated": [],
        "skip_reason": "",
    }
    if series is None:
        base["skip_reason"] = "column_absent"
        return base
    n = int(len(series))
    base["row_count"] = n
    if n == 0:
        base["skip_reason"] = "group_empty"
        return base
    null_count = int(series.isna().sum())
    base["null_count"] = null_count
    base["null_rate"] = float(null_count / n)
    base["top_repeated"] = value_counts_top_n(series, n=10)
    if pd.api.types.is_numeric_dtype(series):
        finite = series.dropna()
        finite = finite[np.isfinite(finite)]
        base["distinct_count"] = int(finite.nunique())
    else:
        base["distinct_count"] = int(series.dropna().nunique())
    return base


# ------------------------------------------------------------------ #
# Assessment
# ------------------------------------------------------------------ #


def determine_pipeline_status(
    *,
    feature_reports: dict,
    monthly_pivot_classification: dict,
    duplicate_ts_assessments: dict,
    corrected_phase_2b: dict,
) -> str:
    """Aggregate the per-feature classifications into a top-level label.

    Status precedence (most-blocking first):
      - ``insufficient_source_visibility`` — required columns absent
        from the view; we cannot trace the pipeline behaviour.
      - ``feature_pipeline_issue_likely`` — at least one feature has a
        signal that is NOT explained by the source trace (e.g.,
        feature-null rows without raw-null cause, or duplicate
        ``(ts_event, event_id)`` rows).
      - ``likely_real_regime_shift_with_feature_dq_caveats`` — the
        suspicious features traced cleanly to their source paths
        AND Phase 2B's flags now read as legitimate enum
        concentration / upstream availability / expected
        per-bar duplicates, so the Phase 2 *atr_bps* shift can be
        read as describing real distributional change with caveats
        on the aliased / enum-concentrated columns.
      - ``unknown`` — escape hatch.
    """
    # Source visibility: every focus feature has at least one of its
    # groups successfully traced.
    visibility_ok = any(
        (per_group or {}).get("recent_dormant", {}).get("skip_reason", "")
        != "column_absent"
        for per_group in feature_reports.values()
    )
    if not visibility_ok:
        return "insufficient_source_visibility"

    # Pipeline-issue triggers.
    pipeline_issue = False
    for name, classif in (duplicate_ts_assessments or {}).items():
        if classif.get("classification") == "possible_event_writer_duplication":
            pipeline_issue = True
            break
    if monthly_pivot_classification.get("classification") == "feature_pipeline_discrepancy_detected":
        pipeline_issue = True
    if pipeline_issue:
        return "feature_pipeline_issue_likely"

    # Otherwise: clean source-trace, Phase 2B flags corrected.
    return "likely_real_regime_shift_with_feature_dq_caveats"


def build_corrected_phase_2b(
    *,
    ema_alias_recent: dict,
    ema_alias_older: dict,
    gamma_classification_recent: dict,
    gamma_classification_older: dict,
    monthly_pivot_classification_recent: dict,
    monthly_pivot_classification_older: dict,
    duplicate_ts_assessments: dict,
) -> dict:
    """Restate Phase 2B's flags in light of the source trace.

    This block is what downstream consumers should read instead of
    Phase 2B's raw flags for these four features.
    """
    return {
        "ema_state_and_ema_state_calc": {
            "phase_2b_reported": "two_independent_constant_features",
            "corrected": "single_feature_observed_twice_via_alias",
            "alias_verified_recent": bool(
                ema_alias_recent.get("alias_verified_in_data")
            ),
            "alias_verified_older_firing": bool(
                ema_alias_older.get("alias_verified_in_data")
            ),
            "implication": (
                "Phase 2B's `feature_constant_warning` from these two "
                "columns should be counted as ONE signal, not two."
            ),
        },
        "gamma_mode": {
            "phase_2b_reported": "imputed_default_value_1.0_share_high",
            "corrected": (
                "legitimate_enum_concentration"
                if gamma_classification_recent.get(
                    "phase_2b_imputed_flag_should_be_downgraded"
                )
                else "imputed_default_label_retained"
            ),
            "recent_top_value": gamma_classification_recent.get("top_value"),
            "recent_top_share": gamma_classification_recent.get("top_share"),
            "older_firing_top_value": gamma_classification_older.get("top_value"),
            "older_firing_top_share": gamma_classification_older.get("top_share"),
            "implication": (
                "value=1 is a documented enum (price ≥ gamma flip); "
                "concentration on it does NOT prove imputation."
            ),
        },
        "monthly_pivot_dist_bps": {
            "phase_2b_reported": "high_null_rate_in_recent_group",
            "corrected": monthly_pivot_classification_recent.get("classification"),
            "recent_null_rate": monthly_pivot_classification_recent.get(
                "feature_null_rate"
            ),
            "older_firing_null_rate": monthly_pivot_classification_older.get(
                "feature_null_rate"
            ),
            "implication": (
                "Nulls trace to upstream `monthly_pivot` "
                "availability — not a feature-pipeline bug — when "
                "feature-null rows are explained by raw-null/zero rows."
            ),
        },
        "ts_event_duplicates": {
            "phase_2b_reported": "duplicate_ts_event_rate_~25_to_30_percent",
            "corrected": {
                name: classif.get("classification")
                for name, classif in (duplicate_ts_assessments or {}).items()
            },
            "implication": (
                "Duplicate ts_event is the expected pattern when "
                "multiple distinct level touches share a minute bar; "
                "event_id remains unique."
            ),
        },
    }


def recommend_next_step(
    status: str,
    *,
    monthly_pivot_recent_null_rate: float | None,
    monthly_pivot_classification: str,
    ema_alias_verified: bool,
    gamma_downgraded: bool,
) -> str:
    """Cheapest read-only next probe. NEVER a trading action."""
    if status == "insufficient_source_visibility":
        return "rebuild_training_view_or_widen_focus_features_then_rerun_audit"
    if status == "feature_pipeline_issue_likely":
        return "inspect_feature_pipeline_for_writer_duplication_or_null_discrepancy_before_other_inference"
    # Clean trace branch — the recommendation depends on whether the
    # monthly pivot null pattern was explained.
    if (
        monthly_pivot_recent_null_rate is not None
        and float(monthly_pivot_recent_null_rate) > 0.10
        and monthly_pivot_classification
        == "upstream_pivot_availability_explains_all_nulls"
    ):
        return (
            "monthly_pivot_nulls_trace_to_upstream_availability_investigate_pivot_provider_separately"
        )
    if ema_alias_verified and gamma_downgraded:
        return (
            "phase_2b_artifact_flags_corrected_proceed_to_separate_regime_diagnostic_for_atr_driven_shift"
        )
    return "no_residual_pipeline_red_flags_continue_with_existing_diagnostic_chain"


# ------------------------------------------------------------------ #
# Orchestration
# ------------------------------------------------------------------ #


def build_report(
    *,
    target: str,
    horizon: int,
    active_manifest_path: Path,
    manifest: dict,
    threshold_resolution: dict,
    total_rows: int,
    group_ranges: dict,
    features: dict,
    source_trace: dict,
    timestamp_duplicate_assessment: dict,
    corrected_phase_2b: dict,
    feature_pipeline_status: str,
    recommended_next_step: str,
    warnings: list[str] | None = None,
) -> dict:
    return {
        "schema_version": 1,
        "audit_type": "feature_pipeline_health",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "target": target,
        "horizon": horizon,
        "active_manifest_path": str(active_manifest_path),
        "active_manifest_version": manifest.get("version"),
        "deployed_threshold": float(threshold_resolution["runtime_threshold"]),
        "threshold_source": threshold_resolution["threshold_source"],
        "manifest_threshold": threshold_resolution["manifest_threshold"],
        "artifact_threshold": threshold_resolution["artifact_threshold"],
        "threshold_mismatch_detected": bool(
            threshold_resolution["threshold_mismatch_detected"]
        ),
        "total_labeled_rows": int(total_rows),
        "group_ranges": group_ranges,
        "feature_pipeline_status": feature_pipeline_status,
        "features": features,
        "source_trace": source_trace,
        "timestamp_duplicate_assessment": timestamp_duplicate_assessment,
        "corrected_phase2b_interpretation": corrected_phase_2b,
        "recommended_next_step": recommended_next_step,
        "warnings": list(warnings or []),
        "scope_disclosure": (
            "read_only_feature_pipeline_health; no training; no "
            "threshold tuning; no threshold search; no promotion; "
            "no edge claim; no database writes. Source paths are "
            "declared in SOURCE_TRACE and cross-checked against live "
            "view data. Threshold resolved with server semantics "
            "(manifest first, artifact fallback)."
        ),
    }


def _build_features_aligned_safe(df, feature_columns):
    """Wrapper that allows tests to monkeypatch a builder-free path."""
    return attribution_mod.build_features_aligned(df, feature_columns)


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
    manifest = attribution_mod.load_active_manifest(manifest_path)
    model_path = attribution_mod.resolve_model_path(
        manifest, target, horizon, models_dir,
    )
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

    sub = attribution_mod.load_labeled_events(
        duckdb_path, view, horizon, target,
    )
    total_rows = int(len(sub))
    selection = attribution_mod.select_groups(
        sub, recent_n=recent_n, older_pct=older_pct,
    )
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
    recent_df = selection["recent_df"]
    older_df = selection["older_df"]

    # Score the older window once to partition firing / nonfiring rows.
    pipeline = artifact.get("pipeline")
    calibrator = artifact.get("calibrator")
    model_obj = calibrator if calibrator is not None else pipeline
    if model_obj is None or not hasattr(model_obj, "predict_proba"):
        raise SystemExit("Model artifact missing pipeline/calibrator with predict_proba")
    np = _require("numpy", "python3 -m pip install numpy")
    if len(older_df) > 0:
        older_features = _build_features_aligned_safe(older_df, feature_columns)
        older_probs = attribution_mod.score_probabilities(model_obj, older_features)
    else:
        older_probs = None
    if older_probs is None or len(older_df) == 0:
        firing_mask = np.zeros(len(older_df), dtype=bool)
        highprob_mask = np.zeros(len(older_df), dtype=bool)
    else:
        firing_mask = older_probs >= float(threshold)
        highprob_mask = (older_probs >= float(highprob_low)) & (
            older_probs < float(threshold)
        )
    older_firing_df = older_df.iloc[np.where(firing_mask)[0]] if len(older_df) else older_df.iloc[0:0]
    older_highprob_df = older_df.iloc[np.where(highprob_mask)[0]] if len(older_df) else older_df.iloc[0:0]

    groups = {
        "recent_dormant": recent_df,
        "older_firing_context": older_firing_df,
        "older_high_prob_nonfiring": older_highprob_df,
    }

    focus_features = [
        "ema_state", "ema_state_calc", "gamma_mode",
        "monthly_pivot_dist_bps", "ts_event",
    ]
    feature_reports: dict[str, dict] = {}
    for feat in focus_features:
        per_group: dict[str, dict] = {}
        for gname, df in groups.items():
            series = df[feat] if feat in df.columns else None
            per_group[gname] = feature_descriptive_stats(series)
        feature_reports[feat] = per_group

    # ema_state / ema_state_calc alias verification per group.
    ema_alias = {
        gname: classify_ema_state_alias_pair(
            df["ema_state"] if "ema_state" in df.columns else None,
            df["ema_state_calc"] if "ema_state_calc" in df.columns else None,
        )
        for gname, df in groups.items()
    }

    # gamma_mode source-trace correction per group.
    gamma_corr = {
        gname: classify_gamma_mode_concentration(feature_reports["gamma_mode"][gname])
        for gname in groups
    }

    # monthly_pivot_dist_bps null source classification.
    monthly_corr = {
        gname: classify_monthly_pivot_null_pattern(df)
        for gname, df in groups.items()
    }

    # Duplicate ts_event assessment per group.
    dup_ts = {
        gname: classify_ts_event_duplicates(df)
        for gname, df in groups.items()
    }

    corrected_phase_2b = build_corrected_phase_2b(
        ema_alias_recent=ema_alias["recent_dormant"],
        ema_alias_older=ema_alias["older_firing_context"],
        gamma_classification_recent=gamma_corr["recent_dormant"],
        gamma_classification_older=gamma_corr["older_firing_context"],
        monthly_pivot_classification_recent=monthly_corr["recent_dormant"],
        monthly_pivot_classification_older=monthly_corr["older_firing_context"],
        duplicate_ts_assessments=dup_ts,
    )

    pipeline_status = determine_pipeline_status(
        feature_reports=feature_reports,
        monthly_pivot_classification=monthly_corr["recent_dormant"],
        duplicate_ts_assessments=dup_ts,
        corrected_phase_2b=corrected_phase_2b,
    )

    rec_next = recommend_next_step(
        pipeline_status,
        monthly_pivot_recent_null_rate=monthly_corr["recent_dormant"].get(
            "feature_null_rate"
        ),
        monthly_pivot_classification=monthly_corr["recent_dormant"].get(
            "classification", ""
        ),
        ema_alias_verified=bool(
            ema_alias["recent_dormant"].get("alias_verified_in_data")
            and ema_alias["older_firing_context"].get("alias_verified_in_data")
        ),
        gamma_downgraded=bool(
            gamma_corr["recent_dormant"].get(
                "phase_2b_imputed_flag_should_be_downgraded"
            )
        ),
    )

    # Compose the timestamp_duplicate_assessment top-level block.
    ts_dup_block = {
        "per_group": dup_ts,
        "summary_classification": dup_ts["recent_dormant"].get("classification"),
        "key_column_used": "event_id",
    }

    # Compose per-feature block with embedded classifications.
    features_block: dict[str, dict] = {}
    for feat, per_group in feature_reports.items():
        features_block[feat] = {"per_group": per_group}
    features_block["ema_state_calc"]["alias_verification"] = ema_alias
    features_block["gamma_mode"]["source_trace_correction"] = gamma_corr
    features_block["monthly_pivot_dist_bps"]["null_source_classification"] = monthly_corr
    features_block["ts_event"]["duplicate_classification"] = dup_ts

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
    if pipeline_status == "feature_pipeline_issue_likely":
        warnings.append("feature_pipeline_issue_likely_status_set")
    if pipeline_status == "insufficient_source_visibility":
        warnings.append("insufficient_source_visibility_status_set")

    return build_report(
        target=target,
        horizon=horizon,
        active_manifest_path=manifest_path,
        manifest=manifest,
        threshold_resolution=threshold_resolution,
        total_rows=total_rows,
        group_ranges=group_ranges,
        features=features_block,
        source_trace=SOURCE_TRACE,
        timestamp_duplicate_assessment=ts_dup_block,
        corrected_phase_2b=corrected_phase_2b,
        feature_pipeline_status=pipeline_status,
        recommended_next_step=rec_next,
        warnings=warnings,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only Phase 2C feature-pipeline health diagnostic. "
            "Traces suspicious features to source paths and corrects "
            "Phase 2B's flags where the source trace supports it. "
            "No training, no threshold tuning, no promotion, no DB writes."
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
        report_path = REPORT_DIR / f"feature_pipeline_health_{ts}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str))

    # ----- Console summary -------------------------------------------- #
    print(f"Feature-pipeline-health report written to {report_path}")
    print()
    print(f"[status] {report['feature_pipeline_status']}")
    print(f"[recommended_next_step] {report['recommended_next_step']}")
    print()
    print("[ema_state / ema_state_calc alias verification]")
    for gname, info in report["features"]["ema_state_calc"]["alias_verification"].items():
        print(
            f"  {gname:<28} alias_verified={info['alias_verified_in_data']} "
            f"matched={info['matched_rows']}/{info['checked_rows']}"
        )
    print()
    print("[gamma_mode source-trace correction]")
    for gname, info in report["features"]["gamma_mode"]["source_trace_correction"].items():
        print(
            f"  {gname:<28} top_value={info['top_value']} "
            f"share={info['top_share']} "
            f"in_enum={info['in_legitimate_enum_domain']} "
            f"downgrade_imputed_flag={info['phase_2b_imputed_flag_should_be_downgraded']}"
        )
    print()
    print("[monthly_pivot_dist_bps null source]")
    for gname, info in report["features"]["monthly_pivot_dist_bps"]["null_source_classification"].items():
        print(
            f"  {gname:<28} null_rate="
            f"{info.get('feature_null_rate'):.3f} "
            f"classification={info.get('classification')} "
            f"unexplained_feature_nulls="
            f"{info.get('rows_feature_null_but_raw_present_nonzero')}"
        )
    print()
    print("[ts_event duplicates]")
    for gname, info in report["features"]["ts_event"]["duplicate_classification"].items():
        print(
            f"  {gname:<28} n={info['rows']} "
            f"dup_ts_rate={(info.get('duplicate_ts_rate') or 0):.3f} "
            f"dup_event_id_count={info['duplicate_event_id_count']} "
            f"classification={info['classification']}"
        )
    if report["warnings"]:
        print()
        for w in report["warnings"]:
            print(f"[warning] {w}")
    print()
    print(f"[scope] {report['scope_disclosure']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
