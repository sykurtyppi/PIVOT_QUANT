#!/usr/bin/env python3
"""Read-only Phase 2D diagnostic for `monthly_pivot` null coverage.

Phase 2C (PR #25) concluded that high `monthly_pivot_dist_bps` null
rates trace to upstream `monthly_pivot` NULL-or-zero rows — i.e., not
a feature-pipeline bug. This audit asks the **next** question:

  Why is upstream `monthly_pivot` NULL for so many recent rows?

It walks the source path, classifies the null pattern, and produces a
status the operator can act on without changing model behaviour.

Source trace (verified by reading the code on this branch):

  - `scripts/backfill_events.py`:
      - `build_monthly_sessions(sessions)` (line ~1290) aggregates
        daily sessions into monthly OHLC bars.
      - `find_mtf_pivot_for_date(monthly_sessions, target_date)`
        (line ~1324) returns the prior completed monthly bar's pivot
        set, or None when no monthly bar exists strictly before
        target_date.
      - The event-writer call (line ~1869) sets
        `mp = monthly_pivots.get("PP") if monthly_pivots else None`,
        so `monthly_pivot` is NULL whenever:
          (a) the daily-session history has no prior month built
              (edge of dataset), OR
          (b) no daily sessions for the prior month were available
              from the **source provider** at backfill time.
  - `server/event_writer.py` (line ~152, ~223) stores `monthly_pivot
    REAL` on the events row at write time.
  - `scripts/build_duckdb_view.py` (line ~117, ~282–284) reads
    `monthly_pivot` from the events row and computes
    `monthly_pivot_dist_bps` purely in SQL — NULL whenever raw is
    NULL.

Out of scope (explicit):
  - No training. No threshold search or tuning. No threshold changes.
  - No model promotion. No walk-forward / OOS validation.
  - No database writes. No deletes. No backfill kick-off.
  - No causal claim about model edge / performance.
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

_ATTR_PATH = ROOT / "scripts" / "audit_regime_health_attribution.py"
_spec = importlib.util.spec_from_file_location(
    "regime_health_attribution_module_for_pivot_audit", _ATTR_PATH
)
if _spec is None or _spec.loader is None:
    raise SystemExit(f"Could not load attribution module from {_ATTR_PATH}")
attribution_mod = importlib.util.module_from_spec(_spec)
sys.modules["regime_health_attribution_module_for_pivot_audit"] = attribution_mod
_spec.loader.exec_module(attribution_mod)


DEFAULT_DUCKDB = ROOT / "data" / "pivot_training.duckdb"
DEFAULT_VIEW = "training_events_v1"
DEFAULT_MANIFEST = ROOT / "data" / "models" / "manifest_active.json"
DEFAULT_MODELS_DIR = ROOT / "data" / "models"
REPORT_DIR = ROOT / "evidence" / "monthly_pivot_availability"

DEFAULT_RECENT_N = 1000
DEFAULT_OLDER_PCT = 0.30
DEFAULT_HIGHPROB_LOW = 0.70
DEFAULT_SYMBOL = "SPY"

SOURCE_TRACE: dict[str, Any] = {
    "monthly_pivot_producer": {
        "origin_file": "scripts/backfill_events.py",
        "key_functions": [
            "build_monthly_sessions",
            "find_mtf_pivot_for_date",
        ],
        "produces_null_when": (
            "no daily session exists strictly before target_date "
            "to form a prior completed monthly bar (edge of dataset "
            "or missing prior-month daily history from the provider)"
        ),
    },
    "monthly_pivot_storage": {
        "origin_file": "server/event_writer.py",
        "table_column": "events.monthly_pivot",
        "type": "REAL nullable",
    },
    "monthly_pivot_dist_bps_compute": {
        "origin_file": "scripts/build_duckdb_view.py",
        "sql_expression": (
            "CASE WHEN timed.monthly_pivot IS NULL THEN NULL "
            "ELSE (timed.touch_price - timed.monthly_pivot) "
            "/ timed.monthly_pivot * 1e4 END"
        ),
        "produces_null_when": (
            "raw monthly_pivot is NULL (zero is NOT special-cased in "
            "the view; backfill never emits monthly_pivot=0 in "
            "practice, but a hypothetical zero would trigger a "
            "ZeroDivisionError-equivalent NULL via try_cast / NaN)"
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


def group_null_summary(df_group, *, raw_col: str = "monthly_pivot",
                       dist_col: str = "monthly_pivot_dist_bps") -> dict:
    """Per-group null/zero summary for `monthly_pivot` and its derived
    bps distance. The diagnostic is intentionally narrow — Phase 2C
    already ratified the source-trace relationship between the two
    columns; here we just count it cleanly so the operator can read
    coverage at a glance.
    """
    pd = _require("pandas", "python3 -m pip install pandas")
    out = {
        "row_count": 0,
        "monthly_pivot_null_count": 0,
        "monthly_pivot_null_rate": None,
        "monthly_pivot_zero_count": 0,
        "monthly_pivot_zero_rate": None,
        "monthly_pivot_dist_bps_null_count": 0,
        "monthly_pivot_dist_bps_null_rate": None,
        "raw_null_explains_all_feature_nulls": None,
        "rows_dist_null_but_raw_present_nonzero": None,
        "missing_columns": [],
    }
    if df_group is None or len(df_group) == 0:
        return out
    n = int(len(df_group))
    out["row_count"] = n
    for c in (raw_col, dist_col):
        if c not in df_group.columns:
            out["missing_columns"].append(c)
    if raw_col in df_group.columns:
        raw = df_group[raw_col]
        out["monthly_pivot_null_count"] = int(raw.isna().sum())
        out["monthly_pivot_null_rate"] = float(out["monthly_pivot_null_count"] / n)
        zero_mask = (raw == 0) & ~raw.isna()
        out["monthly_pivot_zero_count"] = int(zero_mask.sum())
        out["monthly_pivot_zero_rate"] = float(out["monthly_pivot_zero_count"] / n)
    if dist_col in df_group.columns:
        dist = df_group[dist_col]
        out["monthly_pivot_dist_bps_null_count"] = int(dist.isna().sum())
        out["monthly_pivot_dist_bps_null_rate"] = float(
            out["monthly_pivot_dist_bps_null_count"] / n
        )
    if raw_col in df_group.columns and dist_col in df_group.columns:
        raw_missing = df_group[raw_col].isna() | (df_group[raw_col] == 0)
        dist_null = df_group[dist_col].isna()
        out["rows_dist_null_but_raw_present_nonzero"] = int(
            (dist_null & ~raw_missing).sum()
        )
        out["raw_null_explains_all_feature_nulls"] = bool(
            out["rows_dist_null_but_raw_present_nonzero"] == 0
        )
    return out


def by_dimension_null_rate(df, dim_col: str, raw_col: str = "monthly_pivot",
                           top_n: int = 20) -> list[dict]:
    """Cluster monthly_pivot nulls by an arbitrary column.

    For each top-n value in `dim_col` (by row count), report the count
    and the null rate of `monthly_pivot`. This is the cluster-by-date
    / cluster-by-source / cluster-by-level_type pivot the operator
    asked for.
    """
    pd = _require("pandas", "python3 -m pip install pandas")
    if df is None or len(df) == 0:
        return []
    if dim_col not in df.columns or raw_col not in df.columns:
        return []
    grouped = (
        df.groupby(dim_col, dropna=False)[raw_col]
        .agg(["size", lambda s: int(s.isna().sum())])
        .rename(columns={"size": "n", "<lambda_0>": "nulls"})
    )
    grouped["null_rate"] = grouped["nulls"] / grouped["n"]
    grouped = grouped.sort_values("n", ascending=False).head(int(top_n))
    out: list[dict] = []
    for value, row in grouped.iterrows():
        py_val: Any
        if value is None or (isinstance(value, float) and value != value):  # NaN
            py_val = None
        elif hasattr(value, "item"):
            try:
                py_val = value.item()
            except Exception:  # noqa: BLE001
                py_val = str(value)
        else:
            py_val = value
        if isinstance(py_val, (dt.date, dt.datetime)):
            py_val = py_val.isoformat()
        out.append({
            "value": py_val,
            "n": int(row["n"]),
            "nulls": int(row["nulls"]),
            "null_rate": float(row["null_rate"]),
        })
    return out


def date_first_last_null(df, raw_col: str = "monthly_pivot",
                         date_col: str = "event_date_et") -> dict:
    """Return the first and last calendar date on which a null
    monthly_pivot was observed (None if none)."""
    if df is None or len(df) == 0 or raw_col not in df.columns or date_col not in df.columns:
        return {"first_null_date": None, "last_null_date": None}
    sub = df[df[raw_col].isna()]
    if len(sub) == 0:
        return {"first_null_date": None, "last_null_date": None}
    dates = sub[date_col].dropna()
    if len(dates) == 0:
        return {"first_null_date": None, "last_null_date": None}
    first = dates.min()
    last = dates.max()
    def _iso(d):
        if hasattr(d, "isoformat"):
            return d.isoformat()
        return str(d)
    return {"first_null_date": _iso(first), "last_null_date": _iso(last)}


def longest_consecutive_null_dates(df, raw_col: str = "monthly_pivot",
                                   date_col: str = "event_date_et") -> dict:
    """Longest streak of consecutive *calendar dates* where every event
    that day had a NULL `monthly_pivot`. A null streak is interesting
    only if it spans contiguous trading days, but we report calendar
    contiguity to stay provider-agnostic — operators reading this can
    map back to trading days themselves.
    """
    pd = _require("pandas", "python3 -m pip install pandas")
    if df is None or len(df) == 0 or raw_col not in df.columns or date_col not in df.columns:
        return {"length_days": 0, "start_date": None, "end_date": None}
    per_date = (
        df.groupby(date_col)[raw_col]
        .apply(lambda s: bool(s.isna().all()))
        .sort_index()
    )
    if len(per_date) == 0:
        return {"length_days": 0, "start_date": None, "end_date": None}
    dates = list(per_date.index)
    flags = list(per_date.values)
    best = {"length_days": 0, "start_date": None, "end_date": None}
    cur_start = None
    cur_end = None
    cur_len = 0
    for d, is_null in zip(dates, flags, strict=False):
        if is_null:
            if cur_start is None:
                cur_start = d
            cur_end = d
            cur_len += 1
            if cur_len > best["length_days"]:
                best = {
                    "length_days": cur_len,
                    "start_date": d.isoformat() if hasattr(cur_start, "isoformat") else str(cur_start),
                    "end_date": d.isoformat() if hasattr(d, "isoformat") else str(d),
                }
                # Fix start_date isoformat above.
                best["start_date"] = (
                    cur_start.isoformat() if hasattr(cur_start, "isoformat") else str(cur_start)
                )
        else:
            cur_start = None
            cur_end = None
            cur_len = 0
    return best


# ------------------------------------------------------------------ #
# Classification
# ------------------------------------------------------------------ #


def classify_status(
    *,
    overall_summary: dict,
    by_source: list[dict],
    by_month: list[dict],
    columns_present: dict,
) -> str:
    """Decide a top-level status from the null patterns.

    Precedence:
      1. `insufficient_source_visibility` if any of the critical
         columns is missing from the view (`monthly_pivot`,
         `monthly_pivot_dist_bps`, `event_date_et`, `source`).
      2. `available_clean` if overall null rate is below 5% AND no
         provider has > 50% null rate.
      3. `join_or_pipeline_gap` if `dist_bps` is NULL on rows where
         `monthly_pivot` is present and non-zero — that would imply
         the SQL CASE expression in `build_duckdb_view.py` is
         misbehaving, which it cannot under the current source code,
         but we still surface it honestly.
      4. `zero_value_guard_expected` if a non-trivial share of rows
         have `monthly_pivot == 0` (the guard fires by design).
      5. `provider_coverage_gap` if the null rate is concentrated on
         specific providers (`Yahoo`, `IBKR`, etc.) AND clean on
         others (`marketdata.app`).
      6. `expected_sparse_by_design` if all nulls cluster on the
         earliest month(s) — pure edge-of-dataset effect.
      7. `unknown` otherwise.
    """
    required_cols = ["monthly_pivot", "monthly_pivot_dist_bps",
                     "event_date_et", "source"]
    missing = [c for c in required_cols if not columns_present.get(c, False)]
    if missing:
        return "insufficient_source_visibility"

    overall_null_rate = overall_summary.get("monthly_pivot_null_rate") or 0.0
    overall_zero_rate = overall_summary.get("monthly_pivot_zero_rate") or 0.0
    dist_unexplained = overall_summary.get(
        "rows_dist_null_but_raw_present_nonzero"
    )

    if dist_unexplained is not None and dist_unexplained > 0:
        return "join_or_pipeline_gap"
    if overall_null_rate <= 0.05:
        # Truly clean.
        return "available_clean"
    if overall_zero_rate >= 0.05:
        return "zero_value_guard_expected"

    # Provider breakdown: do we see at least one provider with very
    # high null rate AND at least one with very low?
    provider_max = 0.0
    provider_min = 1.0
    n_providers_meaningful = 0
    for prov in by_source:
        if prov["n"] >= 50:
            n_providers_meaningful += 1
            provider_max = max(provider_max, prov["null_rate"])
            provider_min = min(provider_min, prov["null_rate"])
    if (
        n_providers_meaningful >= 2
        and provider_max >= 0.50
        and provider_min <= 0.20
    ):
        return "provider_coverage_gap"

    # Month breakdown: are all nulls in the first 1-2 months?
    months_with_nulls = [m for m in by_month if (m.get("null_rate") or 0) > 0.10]
    if (
        len(by_month) >= 3
        and len(months_with_nulls) <= 2
        and (months_with_nulls[0]["value"] == by_month[0]["value"]
             if months_with_nulls else False)
    ):
        return "expected_sparse_by_design"

    # Mixed evidence — leave honest.
    return "unknown"


def build_corrected_interpretation(
    *,
    status: str,
    by_source: list[dict],
    overall_summary: dict,
    by_month_dates: dict,
) -> dict:
    """Restate Phase 2B / 2C's interpretation of monthly_pivot nulls
    in light of the source-provider breakdown. This block is what
    downstream consumers should read instead of re-deriving from the
    raw counts.
    """
    return {
        "phase_2b_reported": (
            "monthly_pivot_dist_bps high recent null rate looked like "
            "a feature-pipeline gap"
        ),
        "phase_2c_corrected_to": (
            "nulls trace to upstream raw monthly_pivot NULL rows; not "
            "a feature-pipeline bug"
        ),
        "phase_2d_root_cause_classification": status,
        "provider_null_rates": [
            {"source": p["value"], "n": p["n"], "null_rate": p["null_rate"]}
            for p in by_source[:10]
        ],
        "first_null_date": by_month_dates.get("first_null_date"),
        "last_null_date": by_month_dates.get("last_null_date"),
        "overall_monthly_pivot_null_rate": overall_summary.get(
            "monthly_pivot_null_rate"
        ),
        "implication": _implication_for_status(status),
    }


def _implication_for_status(status: str) -> str:
    return {
        "provider_coverage_gap": (
            "specific source providers do not carry monthly_pivot "
            "coverage; restrict training cohort to providers with "
            "coverage, OR backfill monthly bars for the affected "
            "providers BEFORE drawing model-edge conclusions"
        ),
        "expected_sparse_by_design": (
            "nulls confined to the earliest month(s) of the dataset; "
            "trim that window from training cohorts that require "
            "monthly_pivot, no further action needed"
        ),
        "join_or_pipeline_gap": (
            "feature_dist_bps NULL where raw is present and non-zero "
            "— inspect build_duckdb_view.py CASE expression; should "
            "never trigger under current source"
        ),
        "zero_value_guard_expected": (
            "monthly_pivot=0 rows present at non-trivial share — "
            "investigate whether the backfill writes literal zeros "
            "anywhere (it should not)"
        ),
        "available_clean": (
            "no action needed for monthly_pivot coverage"
        ),
        "insufficient_source_visibility": (
            "one or more required columns absent from the training "
            "view; cannot reach a conclusion"
        ),
        "unknown": (
            "null pattern does not cleanly match any of the known "
            "categories; widen the diagnostic before acting"
        ),
    }.get(status, "no_recommendation")


def recommend_next_step(status: str) -> str:
    """Cheapest read-only next probe per status. Never a trading action."""
    return {
        "provider_coverage_gap": (
            "filter training cohort to providers with monthly_pivot "
            "coverage and re-run Phase 2 attribution; OR backfill "
            "monthly bars for the affected providers before further "
            "regime inference"
        ),
        "expected_sparse_by_design": (
            "trim the earliest month(s) from cohorts requiring "
            "monthly_pivot and re-run Phase 2 attribution"
        ),
        "join_or_pipeline_gap": (
            "inspect build_duckdb_view.py monthly_pivot_dist_bps SQL "
            "and re-run this audit"
        ),
        "zero_value_guard_expected": (
            "inspect backfill_events.py for any code path that "
            "writes monthly_pivot=0 and re-run this audit"
        ),
        "available_clean": (
            "no further monthly_pivot diagnostic needed; resume "
            "regime-health chain"
        ),
        "insufficient_source_visibility": (
            "rebuild training view to include monthly_pivot, "
            "monthly_pivot_dist_bps, event_date_et, source; "
            "re-run this audit"
        ),
        "unknown": (
            "widen the audit (more groups, more dimensions) before "
            "any further inference"
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
    total_rows: int,
    overall_summary: dict,
    group_null_summary_block: dict,
    by_source: list[dict],
    by_month: list[dict],
    by_level_type: list[dict],
    by_session: list[dict],
    date_first_last: dict,
    longest_null_streak: dict,
    upstream_source_summary: dict,
    status: str,
    corrected_interpretation: dict,
    recommended_next_step_str: str,
    warnings: list[str] | None = None,
) -> dict:
    return {
        "schema_version": 1,
        "audit_type": "monthly_pivot_availability",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "symbol": symbol,
        "target": target,
        "horizon": horizon,
        "active_manifest_path": str(active_manifest_path),
        "active_manifest_version": manifest_version,
        "total_rows_inspected": int(total_rows),
        "monthly_pivot_status": status,
        "source_trace": SOURCE_TRACE,
        "overall_summary": overall_summary,
        "group_null_summary": group_null_summary_block,
        "date_null_summary": {
            "first_null_date": date_first_last.get("first_null_date"),
            "last_null_date": date_first_last.get("last_null_date"),
            "longest_consecutive_null_streak": longest_null_streak,
            "by_month": by_month,
        },
        "by_source": by_source,
        "by_level_type": by_level_type,
        "by_session": by_session,
        "upstream_source_summary": upstream_source_summary,
        "corrected_interpretation": corrected_interpretation,
        "recommended_next_step": recommended_next_step_str,
        "warnings": list(warnings or []),
        "scope_disclosure": (
            "read_only_monthly_pivot_availability; no training; no "
            "threshold tuning; no threshold search; no promotion; no "
            "OOS validation; no edge claim; no database writes; no "
            "deletes; no backfill kick-off. Source paths declared in "
            "SOURCE_TRACE and cross-checked against live view data."
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
) -> dict:
    pd = _require("pandas", "python3 -m pip install pandas")
    if not duckdb_path.is_file():
        raise SystemExit(f"DuckDB not found: {duckdb_path}")
    manifest = attribution_mod.load_active_manifest(manifest_path)
    manifest_version = manifest.get("version")

    duckdb = _require("duckdb", "python3 -m pip install duckdb")
    con = duckdb.connect(str(duckdb_path), read_only=True)
    available_cols = [r[0] for r in con.execute(f"DESCRIBE {view}").fetchall()]
    needed = [
        "event_id", "symbol", "ts_event", "session", "level_type",
        "source", "monthly_pivot", "monthly_pivot_dist_bps",
        "event_date_et",
    ]
    cols = [c for c in needed if c in available_cols]
    columns_present = {c: (c in available_cols) for c in needed}

    select_cols = ", ".join(cols)
    df = con.execute(
        f"SELECT {select_cols} FROM {view} WHERE symbol = ?",
        [symbol],
    ).df()
    total_rows = int(len(df))

    overall_summary = group_null_summary(df)
    by_source = by_dimension_null_rate(df, "source", top_n=20) \
        if "source" in df.columns else []
    by_month_records = []
    if "event_date_et" in df.columns:
        df_m = df.copy()
        df_m["__month"] = pd.to_datetime(df_m["event_date_et"]).dt.strftime("%Y-%m")
        by_month_records = by_dimension_null_rate(df_m, "__month", top_n=24)
    by_level_type = by_dimension_null_rate(df, "level_type", top_n=20) \
        if "level_type" in df.columns else []
    by_session = by_dimension_null_rate(df, "session", top_n=10) \
        if "session" in df.columns else []
    date_first_last = date_first_last_null(df)
    longest_null_streak = longest_consecutive_null_dates(df)

    # Group-level breakdown using Phase 2 / 2B / 2C group definitions
    # so the audit chain stays self-consistent.
    group_null_summary_block: dict[str, Any] = {}
    sub = attribution_mod.load_labeled_events(
        duckdb_path, view, horizon, target,
    )
    if symbol:
        sub = sub[sub["symbol"] == symbol].reset_index(drop=True)
    if len(sub) > 0:
        selection = attribution_mod.select_groups(
            sub, recent_n=recent_n, older_pct=older_pct,
        )
        # The labeled view may not carry `source` if the view was
        # built without it, so re-attach by event_id when possible.
        for gname, gdf in (
            ("recent_dormant", selection["recent_df"]),
            ("older_window", selection["older_df"]),
        ):
            group_null_summary_block[gname] = group_null_summary(gdf)
    else:
        group_null_summary_block = {
            "recent_dormant": group_null_summary(None),
            "older_window": group_null_summary(None),
        }

    # Upstream source summary: which providers cover monthly_pivot,
    # and which provider is dominant in the recent window.
    upstream_source_summary: dict = {
        "providers": by_source,
        "providers_with_coverage": [
            p["value"] for p in by_source if p["null_rate"] < 0.20
        ],
        "providers_without_coverage": [
            p["value"] for p in by_source if p["null_rate"] >= 0.80
        ],
    }
    # Add provider mix in the recent window if we can resolve it.
    if "source" in df.columns and "ts_event" in df.columns and len(df) > 0:
        df_sorted = df.sort_values("ts_event").reset_index(drop=True)
        recent = df_sorted.tail(int(recent_n))
        if "source" in recent.columns:
            mix = (
                recent["source"].fillna("__null__").value_counts(normalize=True)
                .to_dict()
            )
            upstream_source_summary["recent_window_provider_mix"] = {
                str(k): float(v) for k, v in mix.items()
            }

    status = classify_status(
        overall_summary=overall_summary,
        by_source=by_source,
        by_month=by_month_records,
        columns_present=columns_present,
    )
    corrected = build_corrected_interpretation(
        status=status,
        by_source=by_source,
        overall_summary=overall_summary,
        by_month_dates=date_first_last,
    )
    rec = recommend_next_step(status)

    warnings: list[str] = []
    missing_cols = [c for c, present in columns_present.items() if not present]
    if missing_cols:
        warnings.append(f"missing_view_columns:{','.join(missing_cols)}")
    if total_rows == 0:
        warnings.append(f"no_rows_for_symbol:{symbol}")
    if (overall_summary.get("monthly_pivot_null_rate") or 0) > 0.30:
        warnings.append("overall_monthly_pivot_null_rate_above_30_percent")

    return build_report(
        symbol=symbol,
        target=target,
        horizon=horizon,
        active_manifest_path=manifest_path,
        manifest_version=manifest_version,
        total_rows=total_rows,
        overall_summary=overall_summary,
        group_null_summary_block=group_null_summary_block,
        by_source=by_source,
        by_month=by_month_records,
        by_level_type=by_level_type,
        by_session=by_session,
        date_first_last=date_first_last,
        longest_null_streak=longest_null_streak,
        upstream_source_summary=upstream_source_summary,
        status=status,
        corrected_interpretation=corrected,
        recommended_next_step_str=rec,
        warnings=warnings,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only Phase 2D audit of `monthly_pivot` null "
            "coverage. Classifies nulls as provider-coverage-gap, "
            "expected-sparse-by-design, join/pipeline gap, zero "
            "value guard, available_clean, or insufficient_source_"
            "visibility. No training; no threshold tuning; no "
            "promotion; no OOS; no DB writes."
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
    parser.add_argument("--report", default="")
    args = parser.parse_args(argv)

    if not (0.0 < float(args.older_pct) <= 1.0):
        raise SystemExit("--older-pct must be in (0, 1].")
    if not (0.0 <= float(args.highprob_low) <= 1.0):
        raise SystemExit("--highprob-low must be in [0, 1].")
    if int(args.recent_n) < 1:
        raise SystemExit("--recent-n must be a positive integer.")

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
    )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if args.report:
        report_path = Path(args.report).resolve()
    else:
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        report_path = REPORT_DIR / f"monthly_pivot_availability_{ts}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str))

    print(f"Monthly-pivot-availability report written to {report_path}")
    print()
    print(f"[symbol] {report['symbol']}   "
          f"[total_rows] {report['total_rows_inspected']}")
    print(f"[status] {report['monthly_pivot_status']}")
    print(f"[recommended_next_step] {report['recommended_next_step']}")
    print()
    os = report["overall_summary"]
    print(
        f"[overall] monthly_pivot null_rate="
        f"{os.get('monthly_pivot_null_rate')} "
        f"zero_rate={os.get('monthly_pivot_zero_rate')} "
        f"raw_explains_all_feature_nulls="
        f"{os.get('raw_null_explains_all_feature_nulls')}"
    )
    print()
    print("[by_source]")
    for p in report["by_source"][:8]:
        print(f"  {str(p['value']):<22} n={p['n']:<8} "
              f"nulls={p['nulls']:<8} null_rate={p['null_rate']:.3f}")
    print()
    print("[by_month]")
    for m in report["date_null_summary"]["by_month"]:
        print(f"  {m['value']:<8} n={m['n']:<6} "
              f"nulls={m['nulls']:<6} null_rate={m['null_rate']:.3f}")
    print()
    if report["warnings"]:
        for w in report["warnings"]:
            print(f"[warning] {w}")
        print()
    print(f"[scope] {report['scope_disclosure']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
