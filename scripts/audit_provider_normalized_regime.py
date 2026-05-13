#!/usr/bin/env python3
"""Read-only Phase 2E provider-normalized regime attribution.

Phase 2D (PR #26) showed that the recent_dormant vs older_firing_context
comparison from Phase 2 / 2B is potentially confounded by a change in
the upstream data-provider mix: `marketdata.app` rows are mostly
`monthly_pivot`-clean while `Yahoo` / `Yahoo Finance` / `IBKR` rows are
not, and the recent_dormant window over-samples the latter group.

This audit re-runs the Phase 2 attribution comparison **within each
provider separately**, so we can ask:

  - Does recent dormancy still appear within ``marketdata.app`` alone?
  - Does ``atr_bps`` still show a large recent-vs-older shift inside
    one provider?
  - Does the ``gamma_mode`` regime flip survive provider control?
  - Is probability compression still present within a single
    provider's cohort?

Hard scope:
  - Read-only.
  - No training. No threshold tuning or search. No threshold changes.
  - No promotion. No walk-forward / OOS validation.
  - No database writes. No deletes. No backfill kick-off.
  - No causal claim beyond what within-provider counts support.
  - If `source` column is absent from the view, report
    ``insufficient_source_visibility`` and stop.
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

# Reuse Phase 2 attribution helpers so threshold semantics and group
# selection stay invariant across the regime-health audit chain.
_ATTR_PATH = ROOT / "scripts" / "audit_regime_health_attribution.py"
_spec = importlib.util.spec_from_file_location(
    "regime_health_attribution_module_for_provider_norm", _ATTR_PATH
)
if _spec is None or _spec.loader is None:
    raise SystemExit(f"Could not load attribution module from {_ATTR_PATH}")
attribution_mod = importlib.util.module_from_spec(_spec)
sys.modules["regime_health_attribution_module_for_provider_norm"] = attribution_mod
_spec.loader.exec_module(attribution_mod)


DEFAULT_DUCKDB = ROOT / "data" / "pivot_training.duckdb"
DEFAULT_VIEW = "training_events_v1"
DEFAULT_MANIFEST = ROOT / "data" / "models" / "manifest_active.json"
DEFAULT_MODELS_DIR = ROOT / "data" / "models"
REPORT_DIR = ROOT / "evidence" / "provider_normalized_regime"

DEFAULT_RECENT_N = 1000
DEFAULT_OLDER_PCT = 0.30
DEFAULT_HIGHPROB_LOW = 0.70
DEFAULT_SYMBOL = "SPY"
DEFAULT_MIN_GROUP_ROWS = 100
DEFAULT_MIN_FIRING_ROWS = 30

FOCUS_FEATURES = [
    "atr_bps", "gamma_mode", "ema_state", "ema_state_calc",
    "monthly_pivot_dist_bps", "price_vs_ema21_bps",
    "ema_spread_bps", "hist_break_rate",
    "distance_bps", "hist_reject_rate",
]

SOURCE_TRACE: dict[str, Any] = {
    "provider_column": {
        "origin_file": "scripts/build_duckdb_view.py",
        "view_column": "source",
        "raw_table_column": "events.source",
        "values_observed_phase_2d": [
            "marketdata.app", "Yahoo", "Yahoo Finance", "IBKR",
        ],
    },
    "smd_and_ks": {
        "origin_file": "scripts/audit_regime_health_attribution.py",
        "functions": ["standardized_mean_diff", "ks_statistic"],
        "semantics": (
            "SMD = (mean_a - mean_b) / pooled_sd, with NaN/None when "
            "either side is empty or zero-variance; KS via "
            "scipy.stats.ks_2samp.statistic, None when SciPy is "
            "unavailable"
        ),
    },
    "threshold_resolution": {
        "origin_file": "scripts/audit_regime_health_attribution.py",
        "function": "resolve_runtime_threshold",
        "semantics": (
            "manifest threshold takes precedence; artifact "
            "`optimal_threshold` is fallback; mismatch is surfaced "
            "as a warning"
        ),
    },
}


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #


def _require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Missing dependency {module_name!r}: {exc}. {hint}")


def provider_mix_for_group(df_group, *, source_col: str = "source") -> list[dict]:
    """Per-provider row count + share within a group, sorted by count
    descending. Returns an empty list when the group is empty or the
    source column is absent."""
    pd = _require("pandas", "python3 -m pip install pandas")
    if df_group is None or len(df_group) == 0 or source_col not in df_group.columns:
        return []
    n = int(len(df_group))
    vc = df_group[source_col].fillna("__null__").value_counts()
    return [
        {"source": str(k), "n": int(v), "share": float(v / n)}
        for k, v in vc.items()
    ]


def probability_summary(probs) -> dict:
    """Threshold-relative probability summary for a per-provider
    cohort. Threshold itself is passed in by caller — we report the
    raw stats here."""
    pd = _require("pandas", "python3 -m pip install pandas")
    np = _require("numpy", "python3 -m pip install numpy")
    out = {
        "n": 0, "max": None, "mean": None, "median": None,
        "p90": None, "p95": None,
    }
    if probs is None:
        return out
    arr = np.asarray(probs, dtype=float)
    arr = arr[np.isfinite(arr)]
    out["n"] = int(arr.size)
    if arr.size == 0:
        return out
    out["max"] = float(arr.max())
    out["mean"] = float(arr.mean())
    out["median"] = float(np.median(arr))
    out["p90"] = float(np.quantile(arr, 0.90))
    out["p95"] = float(np.quantile(arr, 0.95))
    return out


def within_provider_feature_comparison(
    recent_df,
    older_firing_df,
    features: list[str],
) -> dict:
    """Per-feature SMD + KS between recent_dormant and older_firing
    within a single provider's cohort. Reuses Phase 2 helpers so the
    numbers are directly comparable to the original attribution."""
    pd = _require("pandas", "python3 -m pip install pandas")
    out: dict[str, dict] = {}
    for feat in features:
        entry: dict[str, Any] = {
            "feature": feat,
            "smd": None,
            "ks": None,
            "recent_n_present": 0,
            "older_n_present": 0,
            "skip_reason": "",
        }
        if (
            recent_df is None or older_firing_df is None
            or len(recent_df) == 0 or len(older_firing_df) == 0
        ):
            entry["skip_reason"] = "empty_group"
            out[feat] = entry
            continue
        if feat not in recent_df.columns or feat not in older_firing_df.columns:
            entry["skip_reason"] = "feature_absent"
            out[feat] = entry
            continue
        a = recent_df[feat].dropna()
        b = older_firing_df[feat].dropna()
        entry["recent_n_present"] = int(len(a))
        entry["older_n_present"] = int(len(b))
        if entry["recent_n_present"] == 0 or entry["older_n_present"] == 0:
            entry["skip_reason"] = "no_non_null_rows"
            out[feat] = entry
            continue
        smd = attribution_mod.standardized_mean_diff(a, b)
        ks = attribution_mod.ks_statistic(a, b)
        entry["smd"] = (None if smd is None else float(smd))
        entry["ks"] = (None if ks is None else float(ks))
        out[feat] = entry
    return out


def survives_threshold(comp: dict, *, feature: str, smd_min_abs: float) -> bool:
    """A within-provider feature shift "survives provider control" when
    the absolute SMD ≥ ``smd_min_abs`` and is reported (not skipped)."""
    entry = comp.get(feature)
    if not entry:
        return False
    smd = entry.get("smd")
    if smd is None:
        return False
    try:
        return abs(float(smd)) >= float(smd_min_abs)
    except (TypeError, ValueError):
        return False


def probability_dormancy_survives(
    *, prob_recent: dict, prob_older: dict, threshold: float,
    drop_min: float = 0.05,
) -> bool:
    """Probability dormancy "survives provider control" when the max
    or p95 probability in the recent cohort sits at least
    ``drop_min`` below the threshold AND below the older cohort's
    median by the same margin.

    The exact thresholds are intentionally conservative — Phase 2E is
    not the place to declare an edge dead.
    """
    if not prob_recent or not prob_older:
        return False
    r_max = prob_recent.get("max")
    r_p95 = prob_recent.get("p95")
    o_med = prob_older.get("median")
    if r_max is None or r_p95 is None or o_med is None:
        return False
    try:
        below_threshold = (float(r_p95) <= float(threshold) - float(drop_min))
        below_older_median = (float(r_p95) <= float(o_med) - float(drop_min))
    except (TypeError, ValueError):
        return False
    return bool(below_threshold and below_older_median)


# ------------------------------------------------------------------ #
# Classification
# ------------------------------------------------------------------ #


def classify_status(
    *,
    columns_present: dict,
    providers_evaluated: list[dict],
    flags: dict,
) -> str:
    """Precedence:
      1. ``insufficient_source_visibility`` if `source` column missing.
      2. ``insufficient_within_provider_overlap`` if no provider has
         both recent_n and older_firing_n above the configured minima.
      3. ``regime_shift_survives_provider_control`` if at least one
         provider with adequate samples shows surviving shifts on
         multiple Phase-2 features.
      4. ``provider_mix_confounds_prior_attribution`` if no
         within-provider cohort shows surviving shifts AND
         marketdata_app_control is available.
      5. ``mixed_evidence`` otherwise.
    """
    if not columns_present.get("source", False):
        return "insufficient_source_visibility"

    adequate = [p for p in providers_evaluated if p.get("included") is True]
    if not adequate:
        return "insufficient_within_provider_overlap"

    survives_any = (
        flags.get("atr_shift_survives_provider_control")
        or flags.get("gamma_mode_shift_survives_provider_control")
        or flags.get("probability_dormancy_survives_provider_control")
    )
    survives_multiple = sum(
        1 for k in (
            "atr_shift_survives_provider_control",
            "gamma_mode_shift_survives_provider_control",
            "probability_dormancy_survives_provider_control",
        ) if flags.get(k)
    ) >= 2

    if survives_multiple:
        return "regime_shift_survives_provider_control"
    if not survives_any and flags.get("marketdata_app_control_available"):
        return "provider_mix_confounds_prior_attribution"
    return "mixed_evidence"


def recommend_next_step(status: str) -> str:
    return {
        "regime_shift_survives_provider_control": (
            "phase_2_attribution_signal_survives_provider_control_"
            "investigate_real_within_provider_drivers_separately"
        ),
        "provider_mix_confounds_prior_attribution": (
            "phase_2_attribution_signal_appears_provider_mix_driven_"
            "restrict_cohort_to_single_provider_and_re_run_phase_2_audit"
        ),
        "insufficient_within_provider_overlap": (
            "expand_data_window_to_grow_within_provider_recent_and_"
            "firing_cohorts_then_re_run_provider_normalized_audit"
        ),
        "mixed_evidence": (
            "evidence_is_mixed_widen_provider_set_or_horizon_before_"
            "drawing_attribution_conclusions"
        ),
        "insufficient_source_visibility": (
            "rebuild_training_view_to_include_source_column_and_"
            "re_run_provider_normalized_audit"
        ),
        "unknown": (
            "no_clean_classification_available_widen_diagnostic_before_action"
        ),
    }.get(status, "no_recommendation")


# ------------------------------------------------------------------ #
# Orchestration
# ------------------------------------------------------------------ #


def build_report(
    *,
    symbol: str,
    target: str,
    horizon: int,
    active_manifest_path: Path,
    manifest_version: str | None,
    threshold_resolution: dict,
    total_rows: int,
    provider_mix_summary: dict,
    within_provider_comparisons: dict,
    providers_evaluated: list[dict],
    status: str,
    flags: dict,
    min_group_rows: int,
    min_firing_rows: int,
    smd_min_abs: float,
    recommended_next_step_str: str,
    warnings: list[str] | None = None,
) -> dict:
    return {
        "schema_version": 1,
        "audit_type": "provider_normalized_regime",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "symbol": symbol,
        "target": target,
        "horizon": horizon,
        "active_manifest_path": str(active_manifest_path),
        "active_manifest_version": manifest_version,
        "deployed_threshold": float(threshold_resolution["runtime_threshold"]),
        "threshold_source": threshold_resolution["threshold_source"],
        "manifest_threshold": threshold_resolution["manifest_threshold"],
        "artifact_threshold": threshold_resolution["artifact_threshold"],
        "threshold_mismatch_detected": bool(
            threshold_resolution["threshold_mismatch_detected"]
        ),
        "total_rows_inspected": int(total_rows),
        "config": {
            "min_group_rows": int(min_group_rows),
            "min_firing_rows": int(min_firing_rows),
            "smd_min_abs": float(smd_min_abs),
        },
        "source_trace": SOURCE_TRACE,
        "provider_mix_summary": provider_mix_summary,
        "providers_evaluated": providers_evaluated,
        "within_provider_comparisons": within_provider_comparisons,
        "provider_normalized_status": status,
        "flags": flags,
        "recommended_next_step": recommended_next_step_str,
        "warnings": list(warnings or []),
        "scope_disclosure": (
            "read_only_provider_normalized_regime; no training; no "
            "threshold tuning; no threshold search; no promotion; no "
            "OOS validation; no edge claim; no database writes; no "
            "deletes; no backfill kick-off. Threshold resolved with "
            "server semantics (manifest first, artifact fallback). "
            "Phase 2 helpers reused for SMD/KS so numbers are "
            "directly comparable to the original attribution."
        ),
    }


def run_audit(
    *,
    symbol: str,
    target: str,
    horizon: int,
    manifest_path: Path,
    models_dir: Path,
    duckdb_path: Path,
    view: str,
    recent_n: int,
    older_pct: float,
    highprob_low: float,
    min_group_rows: int,
    min_firing_rows: int,
    smd_min_abs: float,
) -> dict:
    pd = _require("pandas", "python3 -m pip install pandas")
    np = _require("numpy", "python3 -m pip install numpy")
    if not duckdb_path.is_file():
        raise SystemExit(f"DuckDB not found: {duckdb_path}")

    manifest = attribution_mod.load_active_manifest(manifest_path)
    manifest_version = manifest.get("version")
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

    # Check that `source` is in the view.
    duckdb = _require("duckdb", "python3 -m pip install duckdb")
    con = duckdb.connect(str(duckdb_path), read_only=True)
    available_cols = [r[0] for r in con.execute(f"DESCRIBE {view}").fetchall()]
    columns_present = {
        "source": "source" in available_cols,
        "symbol": "symbol" in available_cols,
        "ts_event": "ts_event" in available_cols,
    }

    sub = attribution_mod.load_labeled_events(
        duckdb_path, view, horizon, target,
    )
    if symbol and "symbol" in sub.columns:
        sub = sub[sub["symbol"] == symbol].reset_index(drop=True)
    total_rows = int(len(sub))

    if not columns_present["source"] or total_rows == 0:
        # Short-circuit honestly.
        flags = {
            "marketdata_app_control_available": False,
            "atr_shift_survives_provider_control": False,
            "gamma_mode_shift_survives_provider_control": False,
            "probability_dormancy_survives_provider_control": False,
            "provider_mix_warning": True,
        }
        status = classify_status(
            columns_present=columns_present,
            providers_evaluated=[],
            flags=flags,
        )
        return build_report(
            symbol=symbol, target=target, horizon=horizon,
            active_manifest_path=manifest_path,
            manifest_version=manifest_version,
            threshold_resolution=threshold_resolution,
            total_rows=total_rows,
            provider_mix_summary={},
            within_provider_comparisons={},
            providers_evaluated=[],
            status=status, flags=flags,
            min_group_rows=min_group_rows,
            min_firing_rows=min_firing_rows,
            smd_min_abs=smd_min_abs,
            recommended_next_step_str=recommend_next_step(status),
            warnings=(
                ["source_column_missing_from_view"]
                if not columns_present["source"] else
                [f"no_rows_for_symbol:{symbol}"]
            ),
        )

    # Build the same Phase 2 groups.
    selection = attribution_mod.select_groups(
        sub, recent_n=recent_n, older_pct=older_pct,
    )
    recent_df = selection["recent_df"]
    older_df = selection["older_df"]

    # Score the older window once to partition firing / nonfiring.
    pipeline = artifact.get("pipeline")
    calibrator = artifact.get("calibrator")
    model_obj = calibrator if calibrator is not None else pipeline
    if model_obj is None or not hasattr(model_obj, "predict_proba"):
        raise SystemExit("Model artifact missing pipeline/calibrator with predict_proba")
    if len(older_df) > 0:
        older_features = attribution_mod.build_features_aligned(
            older_df, feature_columns,
        )
        older_probs = attribution_mod.score_probabilities(model_obj, older_features)
    else:
        older_probs = None
    if older_probs is None or len(older_df) == 0:
        firing_mask = np.zeros(len(older_df), dtype=bool)
        older_firing_probs = np.asarray([], dtype=float)
    else:
        firing_mask = older_probs >= float(threshold)
        older_firing_probs = np.asarray(older_probs)[firing_mask]
    older_firing_df = older_df.iloc[np.where(firing_mask)[0]] if len(older_df) else older_df.iloc[0:0]
    # Reset index so subsequent boolean masks computed against
    # older_firing_df line up with older_firing_probs by position.
    older_firing_df = older_firing_df.reset_index(drop=True)

    # Also score the recent window for probability-dormancy summaries.
    if len(recent_df) > 0:
        recent_features = attribution_mod.build_features_aligned(
            recent_df, feature_columns,
        )
        recent_probs = attribution_mod.score_probabilities(model_obj, recent_features)
    else:
        recent_probs = None

    # Provider mix summary across the three groups.
    provider_mix_summary = {
        "recent_dormant": provider_mix_for_group(recent_df),
        "older_firing_context": provider_mix_for_group(older_firing_df),
    }

    # Per-provider attribution. Iterate every provider observed in
    # either recent or older_firing; never silently drop.
    provider_values: list[str] = []
    for grp_name, grp in (("recent", recent_df), ("older_firing", older_firing_df)):
        if grp is not None and "source" in grp.columns:
            for v in grp["source"].dropna().unique().tolist():
                if v not in provider_values:
                    provider_values.append(v)
    # Also include __null__ provider if any nulls present.
    if recent_df is not None and "source" in recent_df.columns and recent_df["source"].isna().any():
        provider_values.append("__null__")

    within: dict[str, dict] = {}
    providers_evaluated: list[dict] = []
    flags = {
        "marketdata_app_control_available": False,
        "atr_shift_survives_provider_control": False,
        "gamma_mode_shift_survives_provider_control": False,
        "probability_dormancy_survives_provider_control": False,
        "provider_mix_warning": False,
    }

    for prov in provider_values:
        if prov == "__null__":
            mask_r = recent_df["source"].isna() if "source" in recent_df.columns else None
            mask_o = (
                older_firing_df["source"].isna()
                if "source" in older_firing_df.columns else None
            )
        else:
            mask_r = (recent_df["source"] == prov) if "source" in recent_df.columns else None
            mask_o = (
                (older_firing_df["source"] == prov)
                if "source" in older_firing_df.columns else None
            )
        r = recent_df[mask_r] if mask_r is not None else recent_df.iloc[0:0]
        o = older_firing_df[mask_o] if mask_o is not None else older_firing_df.iloc[0:0]
        recent_n_prov = int(len(r))
        firing_n_prov = int(len(o))
        included = bool(
            recent_n_prov >= min_group_rows
            and firing_n_prov >= min_firing_rows
        )
        if not included:
            exclude_reason = (
                "insufficient_data"
                if recent_n_prov < min_group_rows
                or firing_n_prov < min_firing_rows
                else ""
            )
            providers_evaluated.append({
                "source": prov,
                "recent_n": recent_n_prov,
                "older_firing_n": firing_n_prov,
                "included": False,
                "exclude_reason": exclude_reason,
            })
            within[prov] = {
                "recent_n": recent_n_prov,
                "older_firing_n": firing_n_prov,
                "included": False,
                "exclude_reason": exclude_reason,
            }
            continue

        # Probability summaries within this provider.
        if recent_probs is not None and len(recent_probs) > 0:
            prob_r = probability_summary(
                np.asarray(recent_probs)[mask_r.values if hasattr(mask_r, "values") else mask_r]
                if mask_r is not None else None
            )
        else:
            prob_r = probability_summary(None)
        if older_firing_probs is not None and len(older_firing_probs) > 0:
            prob_o = probability_summary(
                older_firing_probs[mask_o.values if hasattr(mask_o, "values") else mask_o]
                if mask_o is not None else None
            )
        else:
            prob_o = probability_summary(None)

        comp = within_provider_feature_comparison(r, o, FOCUS_FEATURES)
        prob_dormant_survives = probability_dormancy_survives(
            prob_recent=prob_r, prob_older=prob_o, threshold=threshold,
        )
        prov_entry = {
            "recent_n": recent_n_prov,
            "older_firing_n": firing_n_prov,
            "included": True,
            "feature_comparisons": comp,
            "probability_summary_recent": prob_r,
            "probability_summary_older_firing": prob_o,
            "probability_dormancy_survives": prob_dormant_survives,
        }
        within[prov] = prov_entry
        providers_evaluated.append({
            "source": prov,
            "recent_n": recent_n_prov,
            "older_firing_n": firing_n_prov,
            "included": True,
            "exclude_reason": "",
        })

        # Update flags. We OR across providers — i.e., a single
        # provider with adequate samples that retains the shift is
        # enough to say it survives. The flag is per-shift.
        if prov == "marketdata.app":
            flags["marketdata_app_control_available"] = True
        if survives_threshold(comp, feature="atr_bps", smd_min_abs=smd_min_abs):
            flags["atr_shift_survives_provider_control"] = True
        if survives_threshold(comp, feature="gamma_mode", smd_min_abs=smd_min_abs):
            flags["gamma_mode_shift_survives_provider_control"] = True
        if prob_dormant_survives:
            flags["probability_dormancy_survives_provider_control"] = True

    # Provider-mix warning: did the recent_dormant vs older_firing
    # provider distributions differ meaningfully?
    def _share_map(rows):
        return {r["source"]: r["share"] for r in rows}
    mix_r = _share_map(provider_mix_summary["recent_dormant"])
    mix_o = _share_map(provider_mix_summary["older_firing_context"])
    keys = set(mix_r.keys()) | set(mix_o.keys())
    max_delta = 0.0
    for k in keys:
        max_delta = max(max_delta, abs(mix_r.get(k, 0.0) - mix_o.get(k, 0.0)))
    flags["provider_mix_warning"] = bool(max_delta >= 0.20)
    flags["max_provider_share_delta_recent_vs_older_firing"] = float(max_delta)

    status = classify_status(
        columns_present=columns_present,
        providers_evaluated=providers_evaluated,
        flags=flags,
    )

    warnings: list[str] = []
    if threshold_resolution["threshold_mismatch_detected"]:
        warnings.append(
            f"threshold_mismatch:manifest={threshold_resolution['manifest_threshold']} "
            f"artifact={threshold_resolution['artifact_threshold']}"
        )
    if threshold_resolution["threshold_source"] == "artifact_fallback":
        warnings.append("manifest_missing_threshold_for_target_horizon")
    if flags["provider_mix_warning"]:
        warnings.append(
            f"provider_mix_shift_between_recent_and_older_firing_groups_max_delta={max_delta:.3f}"
        )
    if status == "insufficient_within_provider_overlap":
        warnings.append("no_provider_met_min_group_rows_and_min_firing_rows")

    return build_report(
        symbol=symbol,
        target=target,
        horizon=horizon,
        active_manifest_path=manifest_path,
        manifest_version=manifest_version,
        threshold_resolution=threshold_resolution,
        total_rows=total_rows,
        provider_mix_summary=provider_mix_summary,
        within_provider_comparisons=within,
        providers_evaluated=providers_evaluated,
        status=status,
        flags=flags,
        min_group_rows=min_group_rows,
        min_firing_rows=min_firing_rows,
        smd_min_abs=smd_min_abs,
        recommended_next_step_str=recommend_next_step(status),
        warnings=warnings,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only Phase 2E provider-normalized regime "
            "attribution. Re-runs Phase 2's recent_dormant vs "
            "older_firing_context comparison within each upstream "
            "provider separately, to test whether the Phase 2 "
            "findings survive provider-mix control. No training; no "
            "threshold tuning; no promotion; no OOS; no DB writes."
        ),
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--target", default="reject")
    parser.add_argument("--horizon", type=int, default=15)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--models-dir", default=str(DEFAULT_MODELS_DIR))
    parser.add_argument("--duckdb", default=str(DEFAULT_DUCKDB))
    parser.add_argument("--view", default=DEFAULT_VIEW)
    parser.add_argument("--recent-n", type=int, default=DEFAULT_RECENT_N)
    parser.add_argument("--older-pct", type=float, default=DEFAULT_OLDER_PCT)
    parser.add_argument("--highprob-low", type=float, default=DEFAULT_HIGHPROB_LOW)
    parser.add_argument("--min-group-rows", type=int, default=DEFAULT_MIN_GROUP_ROWS)
    parser.add_argument("--min-firing-rows", type=int, default=DEFAULT_MIN_FIRING_ROWS)
    parser.add_argument("--smd-min-abs", type=float, default=0.50)
    parser.add_argument("--report", default="")
    args = parser.parse_args(argv)

    if not (0.0 < float(args.older_pct) <= 1.0):
        raise SystemExit("--older-pct must be in (0, 1].")
    if not (0.0 <= float(args.highprob_low) <= 1.0):
        raise SystemExit("--highprob-low must be in [0, 1].")
    if int(args.recent_n) < 1:
        raise SystemExit("--recent-n must be a positive integer.")
    if int(args.min_group_rows) < 1:
        raise SystemExit("--min-group-rows must be a positive integer.")
    if int(args.min_firing_rows) < 1:
        raise SystemExit("--min-firing-rows must be a positive integer.")
    if float(args.smd_min_abs) < 0:
        raise SystemExit("--smd-min-abs must be non-negative.")

    report = run_audit(
        symbol=args.symbol,
        target=args.target,
        horizon=int(args.horizon),
        manifest_path=Path(args.manifest).resolve(),
        models_dir=Path(args.models_dir).resolve(),
        duckdb_path=Path(args.duckdb).resolve(),
        view=args.view,
        recent_n=int(args.recent_n),
        older_pct=float(args.older_pct),
        highprob_low=float(args.highprob_low),
        min_group_rows=int(args.min_group_rows),
        min_firing_rows=int(args.min_firing_rows),
        smd_min_abs=float(args.smd_min_abs),
    )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if args.report:
        report_path = Path(args.report).resolve()
    else:
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        report_path = (
            REPORT_DIR / f"provider_normalized_regime_{args.target}_{args.horizon}_{ts}.json"
        )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str))

    print(f"Provider-normalized regime report written to {report_path}")
    print()
    print(
        f"[symbol] {report['symbol']}   [target] {report['target']}@"
        f"{report['horizon']}   [threshold] {report['deployed_threshold']:.2f}"
    )
    print(f"[total_rows] {report['total_rows_inspected']}")
    print(f"[status] {report['provider_normalized_status']}")
    print(f"[recommended_next_step] {report['recommended_next_step']}")
    print()
    print("[provider mix — recent_dormant]")
    for row in report["provider_mix_summary"].get("recent_dormant", [])[:8]:
        print(f"  {row['source']:<22} n={row['n']:<6} share={row['share']:.3f}")
    print("[provider mix — older_firing_context]")
    for row in report["provider_mix_summary"].get("older_firing_context", [])[:8]:
        print(f"  {row['source']:<22} n={row['n']:<6} share={row['share']:.3f}")
    print()
    print("[providers evaluated]")
    for p in report["providers_evaluated"]:
        msg = (
            f"  {p['source']:<22} recent_n={p['recent_n']:<6} "
            f"firing_n={p['older_firing_n']:<6} included={p['included']}"
        )
        if p.get("exclude_reason"):
            msg += f" ({p['exclude_reason']})"
        print(msg)
    print()
    print("[flags]")
    for k, v in report["flags"].items():
        print(f"  {k}: {v}")
    print()
    print("[within-provider feature SMD (|smd| ≥ "
          f"{report['config']['smd_min_abs']} considered surviving)]")
    for prov, info in report["within_provider_comparisons"].items():
        if not info.get("included"):
            continue
        comps = info.get("feature_comparisons", {})
        print(f"  source={prov}")
        for feat in FOCUS_FEATURES:
            e = comps.get(feat, {})
            smd = e.get("smd")
            ks = e.get("ks")
            print(
                f"    {feat:<28} smd={smd if smd is None else round(smd,3)} "
                f"ks={ks if ks is None else round(ks,3)} "
                f"recent_n={e.get('recent_n_present')} "
                f"older_n={e.get('older_n_present')}"
            )
        pr = info.get("probability_summary_recent", {})
        po = info.get("probability_summary_older_firing", {})
        print(
            f"    [probabilities] recent: max={pr.get('max')} p95={pr.get('p95')} "
            f"median={pr.get('median')} | older_firing: median={po.get('median')} "
            f"max={po.get('max')}"
        )
        print(
            "    probability_dormancy_survives: "
            f"{info.get('probability_dormancy_survives')}"
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
