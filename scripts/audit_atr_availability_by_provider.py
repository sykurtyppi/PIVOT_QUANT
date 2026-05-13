#!/usr/bin/env python3
"""Read-only Phase 2F audit of ``atr_bps`` availability by provider.

Phase 2E (PR #27) could not evaluate the Phase 2 ``atr_bps`` shift
within a single provider's cohort: the within-Yahoo lookup reported
``recent_n=0`` and ``older_n=0`` for ``atr_bps``. The natural next
question is whether that is:

  - a provider coverage gap (Yahoo lacks raw inputs needed for ATR),
  - a feature-join gap (raw inputs exist but never reach the view),
  - a labeled-view gap (raw inputs exist AND the feature can be
    recomputed, but the labeled view does not carry ``atr_bps`` as a
    column), or
  - a true raw-input gap (no provider can support ATR).

Source trace (verified by reading the code on this branch):

  - ``scripts/backfill_events.py``:
      - ``compute_atr(sessions, window)`` (line ~1072) produces an
        ``atr_by_date`` mapping using true-range over daily OHLC.
      - Stored on the events row as raw ``atr`` (REAL) — see core
        fields ``"atr"`` (line ~1151, ~1486) and the per-event write
        at line ~1926 (``"atr": atr_by_date.get(base["date"])``).
  - ``scripts/build_duckdb_view.py``:
      - Carries ``try_cast(atr AS DOUBLE) AS atr`` at line ~97.
      - Does NOT carry ``atr_bps`` — there is no SQL expression for
        ``atr_bps`` in the view. Consumers reading directly from
        ``training_events_v1`` will not find that column.
  - ``ml/features.py``:
      - Lines 226–233 compute ``atr_bps = atr / touch_price * 1e4``
        when both inputs are present and positive; ``None`` otherwise.
      - This is the runtime feature pipeline; it produces ``atr_bps``
        for training and live scoring, but **not** as a stored column.

Implication: ``atr_bps`` exists only inside the model's
feature-frame, never as a column of the labeled view. Phase 2E's
within-provider audit reads directly from the view, so it cannot
see ``atr_bps`` at all — this audit re-derives it on the fly using
the same formula.

Hard scope:
  - Read-only. No training. No threshold tuning/search. No threshold
    changes. No promotion. No OOS validation. No DB writes. No
    deletes. No backfill kick-off. No causal claim about edge.
  - If a column cannot be found in the view, report
    ``insufficient_source_visibility`` honestly.
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
    "regime_health_attribution_module_for_atr_audit", _ATTR_PATH
)
if _spec is None or _spec.loader is None:
    raise SystemExit(f"Could not load attribution module from {_ATTR_PATH}")
attribution_mod = importlib.util.module_from_spec(_spec)
sys.modules["regime_health_attribution_module_for_atr_audit"] = attribution_mod
_spec.loader.exec_module(attribution_mod)


DEFAULT_DUCKDB = ROOT / "data" / "pivot_training.duckdb"
DEFAULT_VIEW = "training_events_v1"
DEFAULT_MANIFEST = ROOT / "data" / "models" / "manifest_active.json"
DEFAULT_SYMBOL = "SPY"
DEFAULT_RECENT_N = 1000
DEFAULT_OLDER_PCT = 0.30
REPORT_DIR = ROOT / "evidence" / "atr_availability"


SOURCE_TRACE: dict[str, Any] = {
    "raw_atr_producer": {
        "origin_file": "scripts/backfill_events.py",
        "function": "compute_atr",
        "line_approx": 1072,
        "semantics": (
            "true-range over prior session OHLC; ATR_by_date is "
            "the trailing mean of TR over a window; effective_window "
            "adapts from min_window=2 up to the requested window"
        ),
        "stored_as": "events.atr (REAL)",
    },
    "view_atr_column": {
        "origin_file": "scripts/build_duckdb_view.py",
        "view_column": "training_events_v1.atr",
        "line_approx": 97,
        "atr_bps_in_view": False,
        "note": (
            "view exposes raw `atr` but NOT `atr_bps`; consumers "
            "that need `atr_bps` must compute it from `atr` and "
            "`touch_price` (or call into ml/features.py)"
        ),
    },
    "atr_bps_runtime_compute": {
        "origin_file": "ml/features.py",
        "lines_approx": [226, 233],
        "formula": "atr_bps = atr / touch_price * 1e4 when atr>0 and touch_price>0 else None",
        "feature_kind": "continuous_bps",
    },
}


def _require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Missing dependency {module_name!r}: {exc}. {hint}")


def compute_atr_bps(df, *, atr_col: str = "atr",
                    price_col: str = "touch_price") -> "pd.Series":  # noqa: F821
    """Apply ``ml/features.py`` formula vectorised over a DataFrame.

    Returns NaN when atr is missing/<=0 or touch_price is missing/<=0,
    exactly matching the runtime branch. Splitting this into its own
    function lets the audit and tests share one definition with no
    drift between the two."""
    pd = _require("pandas", "python3 -m pip install pandas")
    np = _require("numpy", "python3 -m pip install numpy")
    if df is None or len(df) == 0:
        return pd.Series([], dtype=float)
    if atr_col not in df.columns or price_col not in df.columns:
        return pd.Series([float("nan")] * len(df), index=df.index, dtype=float)
    atr = pd.to_numeric(df[atr_col], errors="coerce")
    price = pd.to_numeric(df[price_col], errors="coerce")
    out = pd.Series([float("nan")] * len(df), index=df.index, dtype=float)
    mask = (
        atr.notna() & price.notna() & (atr > 0) & (price > 0)
    )
    out.loc[mask] = atr[mask] / price[mask] * 1e4
    # Drop +/-Inf back to NaN.
    out.replace([np.inf, -np.inf], np.nan, inplace=True)
    return out


def availability_summary(series) -> dict:
    """Standard availability profile for a numeric series."""
    pd = _require("pandas", "python3 -m pip install pandas")
    np = _require("numpy", "python3 -m pip install numpy")
    out: dict[str, Any] = {
        "n": 0, "null_count": 0, "null_rate": None,
        "non_finite_count": 0, "non_finite_rate": None,
        "zero_count": 0, "zero_rate": None,
        "min": None, "p05": None, "median": None,
        "mean": None, "p95": None, "max": None,
    }
    if series is None or len(series) == 0:
        return out
    n = int(len(series))
    out["n"] = n
    s = pd.to_numeric(series, errors="coerce")
    out["null_count"] = int(s.isna().sum())
    out["null_rate"] = float(out["null_count"] / n)
    finite_mask = s.notna() & np.isfinite(s)
    out["non_finite_count"] = int((s.notna() & ~np.isfinite(s)).sum())
    out["non_finite_rate"] = float(out["non_finite_count"] / n)
    finite = s[finite_mask]
    out["zero_count"] = int((finite == 0).sum())
    out["zero_rate"] = float(out["zero_count"] / n)
    if len(finite) > 0:
        out["min"] = float(finite.min())
        out["p05"] = float(np.quantile(finite, 0.05))
        out["median"] = float(np.median(finite))
        out["mean"] = float(finite.mean())
        out["p95"] = float(np.quantile(finite, 0.95))
        out["max"] = float(finite.max())
    return out


def per_provider_atr(df, *, source_col: str = "source") -> list[dict]:
    """Per-provider availability for raw `atr`, computed `atr_bps`,
    and `touch_price`. Sorted by row count descending."""
    pd = _require("pandas", "python3 -m pip install pandas")
    if df is None or len(df) == 0 or source_col not in df.columns:
        return []
    df = df.copy()
    df["__atr_bps_derived"] = compute_atr_bps(df)
    out: list[dict] = []
    for prov, g in df.groupby(source_col, dropna=False):
        prov_label = str(prov) if not (isinstance(prov, float) and prov != prov) else "__null__"
        out.append({
            "source": prov_label,
            "n": int(len(g)),
            "raw_atr": availability_summary(g["atr"] if "atr" in g.columns else None),
            "atr_bps_derived": availability_summary(g["__atr_bps_derived"]),
            "touch_price": availability_summary(g["touch_price"] if "touch_price" in g.columns else None),
        })
    out.sort(key=lambda r: r["n"], reverse=True)
    return out


def by_date_atr_availability(df, *, date_col: str = "event_date_et",
                             top_n: int = 24) -> list[dict]:
    """Cluster raw atr nulls by month."""
    pd = _require("pandas", "python3 -m pip install pandas")
    if df is None or len(df) == 0 or date_col not in df.columns or "atr" not in df.columns:
        return []
    df = df.copy()
    df["__month"] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m")
    grouped = df.groupby("__month", dropna=False).agg(
        n=("atr", "size"),
        nulls=("atr", lambda s: int(s.isna().sum())),
    )
    grouped["null_rate"] = grouped["nulls"] / grouped["n"]
    grouped = grouped.sort_values("n", ascending=False).head(int(top_n))
    return [
        {"value": str(m), "n": int(row["n"]),
         "nulls": int(row["nulls"]),
         "null_rate": float(row["null_rate"])}
        for m, row in grouped.iterrows()
    ]


def group_summary(df_group) -> dict:
    """Recent/older summary block: raw atr counts and the derived
    `atr_bps` profile recomputed on the fly per Phase 2F's contract."""
    if df_group is None or len(df_group) == 0:
        return {
            "n": 0,
            "raw_atr": availability_summary(None),
            "atr_bps_derived": availability_summary(None),
        }
    derived = compute_atr_bps(df_group)
    return {
        "n": int(len(df_group)),
        "raw_atr": availability_summary(df_group["atr"] if "atr" in df_group.columns else None),
        "atr_bps_derived": availability_summary(derived),
    }


# ------------------------------------------------------------------ #
# Classification
# ------------------------------------------------------------------ #


def classify_status(
    *,
    columns_present: dict,
    overall_summary: dict,
    per_provider: list[dict],
) -> str:
    """Precedence:
      1. `insufficient_source_visibility` if `source` or `atr` is
         missing from the view.
      2. `raw_input_missing` if ALL providers have raw_atr null_rate
         ≥ 0.50 — true raw-data gap, not a feature issue.
      3. `provider_coverage_gap` if SOME providers have raw_atr
         null_rate ≥ 0.50 while others are clean.
      4. `labeled_view_gap` if raw atr is broadly available
         (raw_atr null_rate < 0.10 across all providers) AND the
         view does NOT carry `atr_bps`. This is the Phase 2E case.
      5. `feature_join_gap` if raw atr is broadly available AND the
         view DOES carry `atr_bps` AND its null rate is high while
         raw is present and positive — i.e., a real join misfire.
      6. `available_clean` otherwise.
      7. `unknown` escape.
    """
    if not (columns_present.get("source") and columns_present.get("atr")):
        return "insufficient_source_visibility"

    if per_provider:
        bads = [p for p in per_provider
                if (p["raw_atr"].get("null_rate") or 0) >= 0.50]
        goods = [p for p in per_provider
                 if (p["raw_atr"].get("null_rate") or 0) < 0.10
                 and p["n"] >= 50]
        if len(bads) == len(per_provider) and per_provider:
            return "raw_input_missing"
        if bads and goods:
            return "provider_coverage_gap"

    raw_null_rate = overall_summary.get("raw_atr_null_rate") or 0.0
    atr_bps_in_view = columns_present.get("atr_bps", False)
    if raw_null_rate < 0.10 and not atr_bps_in_view:
        return "labeled_view_gap"
    if (
        raw_null_rate < 0.10
        and atr_bps_in_view
        and overall_summary.get("atr_bps_null_rate_in_view") is not None
        and overall_summary["atr_bps_null_rate_in_view"] >= 0.20
    ):
        return "feature_join_gap"
    if raw_null_rate < 0.05:
        return "available_clean"
    return "unknown"


def build_corrected_interpretation(
    *,
    status: str,
    per_provider: list[dict],
    phase_2e_observation: str,
) -> dict:
    return {
        "phase_2e_observation": phase_2e_observation,
        "phase_2f_root_cause_classification": status,
        "yahoo_raw_atr_null_rate": next(
            (p["raw_atr"].get("null_rate") for p in per_provider
             if p["source"] == "Yahoo"), None,
        ),
        "yahoo_derived_atr_bps_null_rate": next(
            (p["atr_bps_derived"].get("null_rate") for p in per_provider
             if p["source"] == "Yahoo"), None,
        ),
        "yahoo_can_support_atr_bps_recovery": _yahoo_can_support(per_provider),
        "implication": _implication_for_status(status),
    }


def _yahoo_can_support(per_provider: list[dict]) -> bool:
    for p in per_provider:
        if p["source"] == "Yahoo":
            raw_null = p["raw_atr"].get("null_rate")
            derived_null = p["atr_bps_derived"].get("null_rate")
            if raw_null is None or derived_null is None:
                return False
            return bool(raw_null < 0.10 and derived_null < 0.10)
    return False


def _implication_for_status(status: str) -> str:
    return {
        "labeled_view_gap": (
            "raw `atr` exists in the view; `atr_bps` does not. "
            "Phase 2E's within-Yahoo `atr_bps=None` is recoverable "
            "by computing `atr / touch_price * 1e4` at audit time. "
            "Original Phase 2 atr_bps regime-shift claim can be "
            "re-tested within Yahoo using the derived column."
        ),
        "raw_input_missing": (
            "no provider carries enough raw `atr` to support ATR-"
            "based features; investigate the producer chain in "
            "backfill_events.py:compute_atr"
        ),
        "provider_coverage_gap": (
            "specific providers lack raw `atr` coverage; restrict "
            "analyses requiring ATR to the provider(s) with coverage"
        ),
        "feature_join_gap": (
            "raw `atr` is present but the in-view `atr_bps` column is "
            "broadly null; inspect the SQL expression that produces "
            "it"
        ),
        "available_clean": (
            "no action needed for ATR availability"
        ),
        "insufficient_source_visibility": (
            "view does not expose required columns; rebuild the view"
        ),
        "unknown": (
            "null pattern does not match any documented category"
        ),
    }.get(status, "no_recommendation")


def recommend_next_step(status: str) -> str:
    return {
        "labeled_view_gap": (
            "recompute_atr_bps_inline_in_provider_normalized_audit_"
            "and_re_evaluate_atr_shift_within_yahoo_using_derived_column"
        ),
        "raw_input_missing": (
            "inspect_backfill_events_compute_atr_for_missing_daily_sessions"
        ),
        "provider_coverage_gap": (
            "restrict_atr_based_analyses_to_providers_with_coverage"
        ),
        "feature_join_gap": (
            "inspect_build_duckdb_view_atr_bps_sql_expression"
        ),
        "available_clean": (
            "no_further_atr_diagnostic_needed"
        ),
        "insufficient_source_visibility": (
            "rebuild_training_view_to_carry_atr_and_source_columns"
        ),
        "unknown": (
            "widen_audit_before_drawing_conclusions"
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
    columns_present: dict,
    overall_summary: dict,
    provider_summary: list[dict],
    by_month: list[dict],
    group_summary_block: dict,
    raw_input_summary: dict,
    status: str,
    corrected_interpretation: dict,
    recommended_next_step_str: str,
    warnings: list[str] | None = None,
) -> dict:
    return {
        "schema_version": 1,
        "audit_type": "atr_availability_by_provider",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "symbol": symbol,
        "target": target,
        "horizon": horizon,
        "active_manifest_path": str(active_manifest_path),
        "active_manifest_version": manifest_version,
        "total_rows_inspected": int(total_rows),
        "atr_availability_status": status,
        "columns_present": columns_present,
        "source_trace": SOURCE_TRACE,
        "overall_summary": overall_summary,
        "provider_summary": provider_summary,
        "by_month": by_month,
        "group_summary": group_summary_block,
        "raw_input_summary": raw_input_summary,
        "corrected_interpretation": corrected_interpretation,
        "recommended_next_step": recommended_next_step_str,
        "warnings": list(warnings or []),
        "scope_disclosure": (
            "read_only_atr_availability_by_provider; no training; no "
            "threshold tuning; no threshold search; no promotion; no "
            "OOS validation; no edge claim; no database writes; no "
            "deletes. Phase 2F recomputes `atr_bps` from raw `atr` "
            "and `touch_price` using the documented formula from "
            "ml/features.py — no on-disk change."
        ),
    }


def run_audit(
    *,
    symbol: str,
    target: str,
    horizon: int,
    manifest_path: Path,
    duckdb_path: Path,
    view: str,
    recent_n: int,
    older_pct: float,
) -> dict:
    pd = _require("pandas", "python3 -m pip install pandas")
    if not duckdb_path.is_file():
        raise SystemExit(f"DuckDB not found: {duckdb_path}")
    duckdb = _require("duckdb", "python3 -m pip install duckdb")
    con = duckdb.connect(str(duckdb_path), read_only=True)
    cols_in_view = [r[0] for r in con.execute(f"DESCRIBE {view}").fetchall()]
    columns_present = {
        "atr": "atr" in cols_in_view,
        "atr_bps": "atr_bps" in cols_in_view,
        "touch_price": "touch_price" in cols_in_view,
        "source": "source" in cols_in_view,
        "symbol": "symbol" in cols_in_view,
        "event_date_et": "event_date_et" in cols_in_view,
        "ts_event": "ts_event" in cols_in_view,
    }

    manifest = attribution_mod.load_active_manifest(manifest_path)
    manifest_version = manifest.get("version")

    # Read everything we need with one query (still cheap).
    needed = [c for c in (
        "event_id", "symbol", "ts_event", "session", "level_type",
        "source", "atr", "touch_price", "event_date_et",
    ) if c in cols_in_view]
    select_cols = ", ".join(needed)
    df = con.execute(
        f"SELECT {select_cols} FROM {view} WHERE symbol = ?",
        [symbol],
    ).df()
    total_rows = int(len(df))

    derived = compute_atr_bps(df)
    overall_summary = {
        "raw_atr": availability_summary(df["atr"] if "atr" in df.columns else None),
        "atr_bps_derived": availability_summary(derived),
        "touch_price": availability_summary(
            df["touch_price"] if "touch_price" in df.columns else None
        ),
        "raw_atr_null_rate": (
            availability_summary(df["atr"] if "atr" in df.columns else None).get("null_rate")
        ),
        "atr_bps_null_rate_in_view": None,
    }

    provider_summary = per_provider_atr(df)
    by_month = by_date_atr_availability(df)

    # Group-specific summaries using Phase 2 group selection.
    sub = attribution_mod.load_labeled_events(
        duckdb_path, view, horizon, target,
    )
    if symbol and "symbol" in sub.columns:
        sub = sub[sub["symbol"] == symbol].reset_index(drop=True)
    if len(sub) > 0:
        selection = attribution_mod.select_groups(
            sub, recent_n=recent_n, older_pct=older_pct,
        )
        group_summary_block = {
            "recent_dormant": group_summary(selection["recent_df"]),
            "older_window": group_summary(selection["older_df"]),
            "whole_labeled_dataset": group_summary(sub),
        }
    else:
        group_summary_block = {
            "recent_dormant": group_summary(None),
            "older_window": group_summary(None),
            "whole_labeled_dataset": group_summary(None),
        }

    # Raw-input summary: clarify which raw inputs are present /
    # absent per provider. Phase 2F decided to surface this
    # explicitly so the operator can distinguish "no atr because no
    # raw bars" from "no atr because the producer skipped the row".
    raw_input_summary = {
        "raw_atr_in_view": columns_present["atr"],
        "touch_price_in_view": columns_present["touch_price"],
        "atr_bps_in_view": columns_present["atr_bps"],
        "per_provider_raw_atr_null_rate": [
            {"source": p["source"], "n": p["n"],
             "raw_atr_null_rate": p["raw_atr"].get("null_rate"),
             "touch_price_null_rate": p["touch_price"].get("null_rate")}
            for p in provider_summary
        ],
    }

    status = classify_status(
        columns_present=columns_present,
        overall_summary=overall_summary,
        per_provider=provider_summary,
    )

    corrected = build_corrected_interpretation(
        status=status,
        per_provider=provider_summary,
        phase_2e_observation=(
            "atr_bps not evaluable within Yahoo cohort because the "
            "labeled view does not expose atr_bps as a column"
        ),
    )
    rec = recommend_next_step(status)

    warnings: list[str] = []
    missing = [c for c in ("atr", "source", "touch_price")
               if not columns_present.get(c, False)]
    if missing:
        warnings.append(f"missing_view_columns:{','.join(missing)}")
    if status == "labeled_view_gap":
        warnings.append(
            "atr_bps_absent_from_labeled_view_but_recoverable_from_raw_atr_and_touch_price"
        )
    if total_rows == 0:
        warnings.append(f"no_rows_for_symbol:{symbol}")

    return build_report(
        symbol=symbol,
        target=target,
        horizon=horizon,
        active_manifest_path=manifest_path,
        manifest_version=manifest_version,
        total_rows=total_rows,
        columns_present=columns_present,
        overall_summary=overall_summary,
        provider_summary=provider_summary,
        by_month=by_month,
        group_summary_block=group_summary_block,
        raw_input_summary=raw_input_summary,
        status=status,
        corrected_interpretation=corrected,
        recommended_next_step_str=rec,
        warnings=warnings,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only Phase 2F audit of `atr_bps` availability by "
            "upstream provider. Classifies nulls as labeled_view_gap, "
            "raw_input_missing, provider_coverage_gap, feature_join_gap, "
            "available_clean, or insufficient_source_visibility. No "
            "training; no threshold tuning; no promotion; no OOS; no "
            "DB writes."
        ),
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--target", default="reject")
    parser.add_argument("--horizon", type=int, default=15)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--duckdb", default=str(DEFAULT_DUCKDB))
    parser.add_argument("--view", default=DEFAULT_VIEW)
    parser.add_argument("--recent-n", type=int, default=DEFAULT_RECENT_N)
    parser.add_argument("--older-pct", type=float, default=DEFAULT_OLDER_PCT)
    parser.add_argument("--report", default="")
    args = parser.parse_args(argv)

    if not (0.0 < float(args.older_pct) <= 1.0):
        raise SystemExit("--older-pct must be in (0, 1].")
    if int(args.recent_n) < 1:
        raise SystemExit("--recent-n must be a positive integer.")

    report = run_audit(
        symbol=args.symbol,
        target=args.target,
        horizon=int(args.horizon),
        manifest_path=Path(args.manifest).resolve(),
        duckdb_path=Path(args.duckdb).resolve(),
        view=args.view,
        recent_n=int(args.recent_n),
        older_pct=float(args.older_pct),
    )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if args.report:
        report_path = Path(args.report).resolve()
    else:
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        report_path = REPORT_DIR / f"atr_availability_{ts}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str))

    print(f"ATR-availability report written to {report_path}")
    print()
    print(f"[symbol] {report['symbol']}   "
          f"[total_rows] {report['total_rows_inspected']}")
    print(f"[status] {report['atr_availability_status']}")
    print(f"[recommended_next_step] {report['recommended_next_step']}")
    print()
    print(f"[atr_bps in view] {report['columns_present'].get('atr_bps')}")
    print()
    print("[per provider]")
    print(f"  {'source':<22} {'n':<8} {'raw_atr_null':<14} "
          f"{'derived_atr_bps_null':<22} {'touch_price_null':<18}")
    for p in report["provider_summary"]:
        print(
            f"  {p['source']:<22} {p['n']:<8} "
            f"{(p['raw_atr'].get('null_rate') or 0):<14.4f} "
            f"{(p['atr_bps_derived'].get('null_rate') or 0):<22.4f} "
            f"{(p['touch_price'].get('null_rate') or 0):<18.4f}"
        )
    print()
    print("[group summary — derived atr_bps]")
    for gname, g in report["group_summary"].items():
        ab = g["atr_bps_derived"]
        print(
            f"  {gname:<26} n={g['n']:<6} "
            f"null_rate={(ab.get('null_rate') or 0):.4f} "
            f"median={ab.get('median')} "
            f"p95={ab.get('p95')} "
            f"max={ab.get('max')}"
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
