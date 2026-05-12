#!/usr/bin/env python3
"""Read-only audit: held-out OOS feasibility for a single (target, horizon).

The one question this audit answers is:

  "At the already-deployed threshold for ``target@horizon``, does a clean
   chronological held-out tail slice of size S contain at least
   ``min_signals`` firings, while leaving enough rows behind to preserve
   the existing train/calib/tune sizes?"

If the answer is yes for any reported slice, a future B4 PR can implement
a single held-out OOS bootstrap. If it is no for every reported slice but
the dataset has room, B4 must use walk-forward OOS folds instead. If the
total labeled dataset is below the existing train+calib+tune floor, no
clean OOS validation is achievable from this data and the right move is
to acknowledge that.

Hard scope contract:
- Read-only. No training. No threshold search or tuning.
- The deployed threshold comes verbatim from the active manifest's model
  artifact (``optimal_threshold`` in the pickle). We never recompute it.
- Several FIXED chronological tail slices are reported — we do not pick
  the most favorable slice. Slice sizes are spec'd as percentages and
  fixed row counts up front.
- ``min_signals`` mirrors B3's ``min_signals=30`` floor; this audit does
  not invent a different number.
- No model gets retrained, scored under a different threshold, or
  promoted as a result of running this audit.

Output:
- JSON report under ``evidence/held_out_feasibility/``.
- Console summary table.
- A single recommendation: ``single_held_out_slice_feasible`` /
  ``walk_forward_oos_required`` / ``insufficient_data_for_clean_oos``.
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

DEFAULT_DUCKDB = ROOT / "data" / "pivot_training.duckdb"
DEFAULT_VIEW = "training_events_v1"
DEFAULT_MANIFEST = ROOT / "data" / "models" / "manifest_active.json"
DEFAULT_MODELS_DIR = ROOT / "data" / "models"
REPORT_DIR = ROOT / "evidence" / "held_out_feasibility"

# Mirrors B3's validator floor; do not move this independently.
MIN_SIGNALS = 30

# Fixed slice sizes. These are reported as-is; the audit does NOT pick a
# "best" size. Operators read the table and decide.
PCT_SLICES = (0.05, 0.10, 0.20, 0.30)
ROW_SLICES = (250, 500, 1000)

# Conservative floors mirroring existing train/calib/tune sizes observed in
# the most recent evidence reports (~20020 train + ~878 calib_fit + ~585 tune
# for reject@15m). The audit refuses to flag a slice "feasible" if carving
# it would push remaining rows below this combined floor.
EXISTING_FLOOR = {
    "train": 20_000,
    "calib_fit": 850,
    "tune": 580,
}
EXISTING_FLOOR_TOTAL = sum(EXISTING_FLOOR.values())


# ------------------------------------------------------------------ #
# I/O helpers
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


def load_labeled_events(duckdb_path: Path, view: str, horizon: int, target: str):
    """Read all labeled events for the (target, horizon), ordered by ts_event."""
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
    sub = df[df[target].notna()].copy()
    return sub


# ------------------------------------------------------------------ #
# Feature alignment + scoring
# ------------------------------------------------------------------ #


def build_features_aligned(df, feature_columns: list[str]):
    """Mirror the training feature-build path; reindex to model's columns.

    The deployed model's pipeline has its own imputer for missing numerics
    and one-hot for categoricals, so reindexing fills missing columns with
    NaN safely. We never strip rows here — the audit needs to score every
    row in the slice it was asked to.
    """
    pd = _require("pandas", "python3 -m pip install pandas")
    from ml.features import build_feature_row
    rows = [build_feature_row(row) for row in df.to_dict("records")]
    feature_df = pd.DataFrame(rows, index=df.index)
    return feature_df.reindex(columns=feature_columns)


def compute_slice(
    df,
    slice_size: int,
    *,
    model_obj,
    feature_columns: list[str],
    threshold: float,
    floor_total: int = EXISTING_FLOOR_TOTAL,
    min_signals: int = MIN_SIGNALS,
) -> dict[str, Any]:
    """Score a chronological tail slice of ``slice_size`` rows.

    No threshold search; the value passed in is the only one used. Slice
    is always ``df.iloc[-slice_size:]`` — the chronologically latest rows.
    """
    total = int(len(df))
    if slice_size <= 0:
        return {
            "slice_size_requested": int(slice_size),
            "available_rows": 0,
            "usable_feature_rows": 0,
            "signal_count": None,
            "signal_rate": None,
            "meets_min_signals": False,
            "remaining_rows_after_carve": total,
            "leaves_train_calib_tune_room": True,
            "skip_reason": "slice_size_not_positive",
        }
    if slice_size > total:
        return {
            "slice_size_requested": int(slice_size),
            "available_rows": 0,
            "usable_feature_rows": 0,
            "signal_count": None,
            "signal_rate": None,
            "meets_min_signals": False,
            "remaining_rows_after_carve": total,
            "leaves_train_calib_tune_room": False,
            "skip_reason": "slice_size_exceeds_total_rows",
        }

    tail = df.iloc[-int(slice_size):]
    X = build_features_aligned(tail, feature_columns)
    usable = int((~X.isna().all(axis=1)).sum())

    probs = model_obj.predict_proba(X)
    if getattr(probs, "shape", (0, 0))[1] != 2:
        return {
            "slice_size_requested": int(slice_size),
            "available_rows": int(slice_size),
            "usable_feature_rows": usable,
            "signal_count": None,
            "signal_rate": None,
            "meets_min_signals": False,
            "remaining_rows_after_carve": int(total - slice_size),
            "leaves_train_calib_tune_room": (total - slice_size) >= floor_total,
            "skip_reason": "predict_proba_unexpected_shape",
        }

    p_positive = probs[:, 1]
    signal_count = int((p_positive >= float(threshold)).sum())
    denom = int(slice_size)
    signal_rate = (signal_count / denom) if denom > 0 else None
    remaining = int(total - slice_size)
    return {
        "slice_size_requested": int(slice_size),
        "available_rows": int(slice_size),
        "usable_feature_rows": usable,
        "signal_count": signal_count,
        "signal_rate": signal_rate,
        "meets_min_signals": bool(signal_count >= int(min_signals)),
        "remaining_rows_after_carve": remaining,
        "leaves_train_calib_tune_room": bool(remaining >= int(floor_total)),
        "skip_reason": "",
    }


def determine_recommendation(
    *,
    total_rows: int,
    slices: list[dict],
    floor_total: int = EXISTING_FLOOR_TOTAL,
) -> str:
    if total_rows < floor_total:
        return "insufficient_data_for_clean_oos"
    any_feasible = any(
        bool(s.get("meets_min_signals")) and bool(s.get("leaves_train_calib_tune_room"))
        for s in slices
    )
    if any_feasible:
        return "single_held_out_slice_feasible"
    return "walk_forward_oos_required"


# ------------------------------------------------------------------ #
# Main
# ------------------------------------------------------------------ #


def build_report(
    *,
    target: str,
    horizon: int,
    active_manifest_path: Path,
    manifest: dict,
    model_path: Path,
    threshold: float,
    total_rows: int,
    slices: list[dict],
    recommendation: str,
) -> dict:
    return {
        "schema_version": 1,
        "audit_type": "held_out_feasibility",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "target": target,
        "horizon": horizon,
        "active_manifest_path": str(active_manifest_path),
        "active_manifest_version": manifest.get("version"),
        "model_path": str(model_path),
        "deployed_threshold": float(threshold),
        "min_signals_floor": int(MIN_SIGNALS),
        "existing_floor": dict(EXISTING_FLOOR),
        "existing_floor_total": int(EXISTING_FLOOR_TOTAL),
        "total_labeled_rows": int(total_rows),
        "slices": slices,
        "recommendation": recommendation,
        "scope_disclosure": (
            "read_only_audit; no training, no threshold search, no tuning, "
            "no promotion. min_signals mirrors B3 validator floor."
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only audit: held-out OOS feasibility.",
    )
    parser.add_argument("--target", default="reject")
    parser.add_argument("--horizon", type=int, default=15)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--models-dir", default=str(DEFAULT_MODELS_DIR))
    parser.add_argument("--duckdb", default=str(DEFAULT_DUCKDB))
    parser.add_argument("--view", default=DEFAULT_VIEW)
    parser.add_argument("--report", default="")
    args = parser.parse_args(argv)

    manifest_path = Path(args.manifest).resolve()
    models_dir = Path(args.models_dir).resolve()
    duckdb_path = Path(args.duckdb).resolve()

    if not duckdb_path.is_file():
        raise SystemExit(f"DuckDB not found: {duckdb_path}")

    manifest = load_active_manifest(manifest_path)
    model_path = resolve_model_path(manifest, args.target, args.horizon, models_dir)
    if not model_path.is_file():
        raise SystemExit(f"Model artifact not found: {model_path}")

    joblib = _require("joblib", "python3 -m pip install joblib")
    artifact = joblib.load(model_path)
    pipeline = artifact.get("pipeline")
    calibrator = artifact.get("calibrator")
    model_obj = calibrator if calibrator is not None else pipeline
    if model_obj is None or not hasattr(model_obj, "predict_proba"):
        raise SystemExit("Model artifact missing pipeline/calibrator with predict_proba")
    threshold = float(artifact.get("optimal_threshold"))
    feature_columns = list(artifact.get("feature_columns") or [])
    if not feature_columns:
        raise SystemExit("Model artifact missing feature_columns")

    sub = load_labeled_events(duckdb_path, args.view, args.horizon, args.target)
    total_rows = int(len(sub))

    slices: list[dict] = []
    for pct in PCT_SLICES:
        sz = max(1, int(round(total_rows * pct)))
        result = compute_slice(
            sub,
            sz,
            model_obj=model_obj,
            feature_columns=feature_columns,
            threshold=threshold,
        )
        result["slice_label"] = f"latest_{int(pct * 100)}pct"
        result["slice_pct"] = float(pct)
        slices.append(result)
    for sz in ROW_SLICES:
        result = compute_slice(
            sub,
            int(sz),
            model_obj=model_obj,
            feature_columns=feature_columns,
            threshold=threshold,
        )
        result["slice_label"] = f"latest_{int(sz)}_rows"
        result["slice_pct"] = None
        slices.append(result)

    recommendation = determine_recommendation(total_rows=total_rows, slices=slices)

    report = build_report(
        target=args.target,
        horizon=args.horizon,
        active_manifest_path=manifest_path,
        manifest=manifest,
        model_path=model_path,
        threshold=threshold,
        total_rows=total_rows,
        slices=slices,
        recommendation=recommendation,
    )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if args.report:
        report_path = Path(args.report).resolve()
    else:
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        report_path = REPORT_DIR / (
            f"held_out_feasibility_{args.target}_{args.horizon}m_{ts}.json"
        )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str))

    print(f"Held-out feasibility report written to {report_path}")
    print()
    print(
        f"=== {args.target}@{args.horizon}m   deployed threshold={threshold:.6f} ==="
    )
    print(f"Total labeled rows:          {total_rows}")
    print(f"Existing slices floor total: {EXISTING_FLOOR_TOTAL}")
    print(f"min_signals floor (B3):      {MIN_SIGNALS}")
    print()
    header = (
        f"{'slice':<22} {'rows':>6} {'usable':>7} {'signals':>8} "
        f"{'rate':>8} {'>=30':>5} {'leaves_room':>12}"
    )
    print(header)
    print("-" * len(header))
    for s in slices:
        sig = s["signal_count"]
        sig_s = str(sig) if sig is not None else "n/a"
        rate = s.get("signal_rate")
        rate_s = f"{rate:.3%}" if rate is not None else "n/a"
        print(
            f"{s['slice_label']:<22} "
            f"{s['available_rows']:>6} "
            f"{s.get('usable_feature_rows', 0):>7} "
            f"{sig_s:>8} "
            f"{rate_s:>8} "
            f"{'Y' if s['meets_min_signals'] else '-':>5} "
            f"{'Y' if s['leaves_train_calib_tune_room'] else '-':>12}"
        )
    print()
    print(f"[recommendation] {recommendation}")
    print(
        "[scope] read-only; no training, no threshold search, "
        "no tuning, no promotion."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
