#!/usr/bin/env python3
"""Read-only Phase 1 regime-health diagnostic for a (target, horizon).

Asks: at the deployed (runtime) threshold, has the model gone dormant
recently — and if so, what does the recent-event probability landscape
look like?

Three diagnostics, all on chronological recent buckets:

  1. Fire-rate by bucket: row count, signal count at runtime threshold,
     signal rate, plus min/mean/median/max probability summary.
  2. Probability distribution: full quantile profile (min, p10, p25,
     median, p75, p90, p95, p99, max) and counts above key thresholds
     (0.50, 0.60, 0.70, 0.75, 0.78, 0.79, runtime).
  3. Threshold proximity: counts within 0.01 / 0.02 / 0.05 below the
     threshold, max-probability gap to threshold, closest probability
     strictly below threshold.

Each bucket also carries three boolean flags — *not* mutually exclusive
— so the top-level ``diagnostic_status`` can be derived without losing
nuance:

  - ``dormant``                       — zero signals in the bucket.
  - ``near_threshold_tail_present``    — many recent probabilities are
                                         within 0.05 of the threshold.
  - ``probabilities_clustered_low``    — even the bucket's max
                                         probability is ≥ 0.10 below
                                         the threshold.

Top-level ``diagnostic_status`` is one of:
  - ``insufficient_data``
  - ``recent_dormancy_confirmed``
  - ``near_threshold_tail``
  - ``not_dormant``
  - ``mixed_signal``

Hard scope contract (encoded into the script):
- Read-only. No training. No threshold search or tuning. No promotion.
- Threshold is resolved with the same precedence used by
  ``server.ml_server.ModelRegistry`` and
  ``scripts/audit_held_out_feasibility.py``: manifest first, artifact
  only as fallback. Both raw values and a mismatch flag are surfaced.
- Buckets are fixed up front; this audit does NOT pick a single
  "cause." Operators read the dimensions and synthesize.
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
REPORT_DIR = ROOT / "evidence" / "regime_health"

# Fixed up front. The audit does NOT pick a most-favorable bucket.
ROW_BUCKETS = (250, 500, 1000)
PCT_BUCKETS = (0.05, 0.10, 0.20, 0.30)

# Probability thresholds tallied in the distribution view. The runtime
# threshold is appended dynamically.
PROB_BUCKETS = (0.50, 0.60, 0.70, 0.75, 0.78, 0.79)

# Proximity windows below threshold (probability units).
PROXIMITY_WINDOWS = (0.01, 0.02, 0.05)

# Per-bucket flag heuristics. Documented inline in ``_bucket_flags``.
DORMANCY_FLOOR_SIGNALS = 1
CLUSTER_LOW_GAP = 0.10
NEAR_TAIL_WINDOW = 0.05
NEAR_TAIL_MIN_COUNT = 5


# ------------------------------------------------------------------ #
# I/O helpers (mirror audit_held_out_feasibility — kept local to avoid
# cross-script imports until a third script needs them too)
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

    Manifest first; artifact only as fallback. Surfaces both raw values
    and a mismatch flag so runtime-safety substitution (PRs #9/#10/#12)
    cannot silently disagree with the artifact.
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
    """Return the positive-class probability array, or None if shape is bad.

    Single call; no threshold search, no iteration over alternatives.
    """
    probs = model_obj.predict_proba(X)
    if getattr(probs, "shape", (0, 0))[1] != 2:
        return None
    return probs[:, 1]


# ------------------------------------------------------------------ #
# Pure-logic diagnostic helpers (separately unit-testable)
# ------------------------------------------------------------------ #


def probability_summary(probs) -> dict | None:
    """Return min/mean/median/max + quantile profile for a 1-D probability array."""
    np = _require("numpy", "python3 -m pip install numpy")
    if probs is None or len(probs) == 0:
        return None
    arr = np.asarray(probs, dtype=float)
    return {
        "min": float(arr.min()),
        "mean": float(arr.mean()),
        "median": float(np.percentile(arr, 50)),
        "max": float(arr.max()),
        "p10": float(np.percentile(arr, 10)),
        "p25": float(np.percentile(arr, 25)),
        "p75": float(np.percentile(arr, 75)),
        "p90": float(np.percentile(arr, 90)),
        "p95": float(np.percentile(arr, 95)),
        "p99": float(np.percentile(arr, 99)),
    }


def counts_above(probs, prob_buckets: tuple[float, ...], runtime_threshold: float) -> dict:
    """Count of probabilities >= each tally point. Includes runtime threshold."""
    np = _require("numpy", "python3 -m pip install numpy")
    if probs is None or len(probs) == 0:
        return {f">={float(t):.2f}": 0 for t in list(prob_buckets) + [runtime_threshold]}
    arr = np.asarray(probs, dtype=float)
    counts = {}
    for t in prob_buckets:
        counts[f">={float(t):.2f}"] = int((arr >= float(t)).sum())
    counts[f">={float(runtime_threshold):.6f}_runtime"] = int(
        (arr >= float(runtime_threshold)).sum()
    )
    return counts


def proximity_stats(
    probs,
    threshold: float,
    *,
    windows: tuple[float, ...] = PROXIMITY_WINDOWS,
) -> dict:
    """Within-window counts plus max-prob gap and closest sub-threshold value.

    A probability is "within w below" iff it is strictly below the
    threshold AND within w of it (i.e. in ``[threshold-w, threshold)``).
    """
    np = _require("numpy", "python3 -m pip install numpy")
    if probs is None or len(probs) == 0:
        return {
            "within_windows": {f"within_{w}_below": 0 for w in windows},
            "max_prob": None,
            "max_prob_gap_to_threshold": None,
            "closest_prob_below_threshold": None,
        }
    arr = np.asarray(probs, dtype=float)
    within: dict[str, int] = {}
    # Tiny tolerance to absorb IEEE-754 representation noise on subtractions
    # like 0.80 - 0.79 = 0.010000000000000009. The window boundaries are
    # operator-facing (1bp, 2bp, 5bp); ulp-level differences are not
    # meaningful here and would cause off-by-one against intuition.
    _win_eps = 1e-9
    for w in windows:
        in_window = (arr < float(threshold)) & (
            (float(threshold) - arr) <= float(w) + _win_eps
        )
        within[f"within_{w}_below"] = int(in_window.sum())
    below = arr[arr < float(threshold)]
    max_p = float(arr.max())
    return {
        "within_windows": within,
        "max_prob": max_p,
        # Positive when max probability is below threshold; negative when
        # at least one event has crossed the threshold.
        "max_prob_gap_to_threshold": float(float(threshold) - max_p),
        "closest_prob_below_threshold": float(below.max()) if below.size > 0 else None,
    }


def bucket_flags(
    probs,
    threshold: float,
    signal_count: int,
    *,
    cluster_gap: float = CLUSTER_LOW_GAP,
    near_tail_window: float = NEAR_TAIL_WINDOW,
    near_tail_min_count: int = NEAR_TAIL_MIN_COUNT,
) -> dict:
    """Three non-exclusive flags per bucket.

    - ``dormant``: zero signals fired.
    - ``probabilities_clustered_low``: even the bucket's max probability
      is at least ``cluster_gap`` (default 0.10) below the threshold.
    - ``near_threshold_tail_present``: at least ``near_tail_min_count``
      probabilities are within ``near_tail_window`` (default 0.05) below
      the threshold.
    """
    np = _require("numpy", "python3 -m pip install numpy")
    if probs is None or len(probs) == 0:
        return {
            "dormant": False,
            "near_threshold_tail_present": False,
            "probabilities_clustered_low": False,
        }
    arr = np.asarray(probs, dtype=float)
    max_p = float(arr.max())
    dormant = bool(int(signal_count) < int(DORMANCY_FLOOR_SIGNALS))
    near_tail_count = int(
        ((arr < float(threshold)) & ((float(threshold) - arr) <= float(near_tail_window))).sum()
    )
    near_tail = bool(near_tail_count >= int(near_tail_min_count))
    clustered_low = bool((float(threshold) - max_p) >= float(cluster_gap))
    return {
        "dormant": dormant,
        "near_threshold_tail_present": near_tail,
        "probabilities_clustered_low": clustered_low,
    }


def compute_bucket(
    df,
    bucket_size: int,
    *,
    model_obj,
    feature_columns: list[str],
    threshold: float,
    prob_buckets: tuple[float, ...] = PROB_BUCKETS,
    proximity_windows: tuple[float, ...] = PROXIMITY_WINDOWS,
) -> dict:
    """Diagnostic view for the chronologically latest ``bucket_size`` rows.

    Single ``predict_proba`` call; the runtime threshold is the only
    comparison used. No threshold search.
    """
    total = int(len(df))
    if bucket_size <= 0:
        return {
            "available_rows": 0,
            "usable_feature_rows": 0,
            "signal_count": None,
            "signal_rate": None,
            "probability_summary": None,
            "counts_above": {},
            "threshold_proximity": None,
            "flags": {
                "dormant": False,
                "near_threshold_tail_present": False,
                "probabilities_clustered_low": False,
            },
            "skip_reason": "bucket_size_not_positive",
        }
    if bucket_size > total:
        return {
            "available_rows": 0,
            "usable_feature_rows": 0,
            "signal_count": None,
            "signal_rate": None,
            "probability_summary": None,
            "counts_above": {},
            "threshold_proximity": None,
            "flags": {
                "dormant": False,
                "near_threshold_tail_present": False,
                "probabilities_clustered_low": False,
            },
            "skip_reason": "bucket_size_exceeds_total_rows",
        }

    tail = df.iloc[-int(bucket_size):]
    X = build_features_aligned(tail, feature_columns)
    probs = score_probabilities(model_obj, X)
    if probs is None:
        return {
            "available_rows": int(bucket_size),
            "usable_feature_rows": int((~X.isna().all(axis=1)).sum()),
            "signal_count": None,
            "signal_rate": None,
            "probability_summary": None,
            "counts_above": {},
            "threshold_proximity": None,
            "flags": {
                "dormant": False,
                "near_threshold_tail_present": False,
                "probabilities_clustered_low": False,
            },
            "skip_reason": "predict_proba_unexpected_shape",
        }

    signal_count = int((probs >= float(threshold)).sum())
    summary = probability_summary(probs)
    counts = counts_above(probs, prob_buckets, float(threshold))
    proximity = proximity_stats(probs, float(threshold), windows=proximity_windows)
    flags = bucket_flags(probs, float(threshold), signal_count)
    return {
        "available_rows": int(bucket_size),
        "usable_feature_rows": int((~X.isna().all(axis=1)).sum()),
        "signal_count": signal_count,
        "signal_rate": signal_count / int(bucket_size),
        "probability_summary": summary,
        "counts_above": counts,
        "threshold_proximity": proximity,
        "flags": flags,
        "skip_reason": "",
    }


def determine_diagnostic_status(buckets: list[dict]) -> str:
    """Aggregate per-bucket flags into a single non-declarative label.

    Focuses on the three smallest (most recent) buckets that successfully
    ran. The label is *descriptive*, not prescriptive — operators read
    the per-bucket detail and decide.
    """
    ran = [b for b in buckets if not b.get("skip_reason")]
    if not ran:
        return "insufficient_data"
    smallest = sorted(ran, key=lambda b: int(b.get("available_rows", 0)))[:3]
    dormant_n = sum(1 for b in smallest if b["flags"]["dormant"])
    clustered_n = sum(1 for b in smallest if b["flags"]["probabilities_clustered_low"])
    near_tail_n = sum(1 for b in smallest if b["flags"]["near_threshold_tail_present"])

    if dormant_n == 0:
        return "not_dormant"
    if dormant_n >= 2 and clustered_n >= 2:
        return "recent_dormancy_confirmed"
    if dormant_n >= 2 and near_tail_n >= 1:
        return "near_threshold_tail"
    if dormant_n >= 2:
        return "recent_dormancy_confirmed"
    return "mixed_signal"


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
    buckets: list[dict],
    diagnostic_status: str,
    warnings: list[str] | None = None,
) -> dict:
    return {
        "schema_version": 1,
        "audit_type": "regime_health",
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
        "buckets": buckets,
        "diagnostic_status": diagnostic_status,
        "warnings": list(warnings or []),
        "scope_disclosure": (
            "read_only_regime_health_diagnostic; no training, no threshold "
            "search, no tuning, no promotion. Reports dimensions "
            "independently; does not pick a single cause. Threshold "
            "resolved with server semantics (manifest first, artifact "
            "fallback)."
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only Phase 1 regime-health diagnostic.",
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

    manifest_thr = deployed_threshold(manifest, args.target, args.horizon)
    artifact_thr = None
    raw = artifact.get("optimal_threshold")
    if raw is not None:
        try:
            artifact_thr = float(raw)
        except (TypeError, ValueError):
            artifact_thr = None
    threshold_resolution = resolve_runtime_threshold(manifest_thr, artifact_thr)
    threshold = threshold_resolution["runtime_threshold"]

    feature_columns = list(artifact.get("feature_columns") or [])
    if not feature_columns:
        raise SystemExit("Model artifact missing feature_columns")

    sub = load_labeled_events(duckdb_path, args.view, args.horizon, args.target)
    total_rows = int(len(sub))

    buckets: list[dict] = []
    # Row-count buckets first (most operationally direct).
    for sz in ROW_BUCKETS:
        result = compute_bucket(
            sub, int(sz),
            model_obj=model_obj,
            feature_columns=feature_columns,
            threshold=threshold,
        )
        result["bucket_label"] = f"latest_{int(sz)}_rows"
        result["bucket_pct"] = None
        buckets.append(result)
    # Then percentage buckets.
    for pct in PCT_BUCKETS:
        sz = max(1, int(round(total_rows * float(pct))))
        result = compute_bucket(
            sub, sz,
            model_obj=model_obj,
            feature_columns=feature_columns,
            threshold=threshold,
        )
        result["bucket_label"] = f"latest_{int(pct * 100)}pct"
        result["bucket_pct"] = float(pct)
        buckets.append(result)

    diagnostic_status = determine_diagnostic_status(buckets)

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

    report = build_report(
        target=args.target,
        horizon=args.horizon,
        active_manifest_path=manifest_path,
        manifest=manifest,
        model_path=model_path,
        threshold_resolution=threshold_resolution,
        total_rows=total_rows,
        buckets=buckets,
        diagnostic_status=diagnostic_status,
        warnings=warnings,
    )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if args.report:
        report_path = Path(args.report).resolve()
    else:
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        report_path = REPORT_DIR / (
            f"regime_health_{args.target}_{args.horizon}m_{ts}.json"
        )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str))

    # ----- Console summary -------------------------------------------- #
    print(f"Regime health report written to {report_path}")
    print()
    print(
        f"=== {args.target}@{args.horizon}m   runtime threshold="
        f"{threshold:.6f} ({threshold_resolution['threshold_source']}) ==="
    )
    print(f"Total labeled rows: {total_rows}")
    print()
    print("[1] Fire-rate by bucket")
    hdr = (
        f"  {'bucket':<22} {'rows':>5} {'sigs':>5} {'rate':>8} "
        f"{'min':>7} {'mean':>7} {'median':>7} {'max':>7}"
    )
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for b in buckets:
        if b.get("skip_reason"):
            print(f"  {b['bucket_label']:<22}  (skipped: {b['skip_reason']})")
            continue
        ps = b["probability_summary"] or {}
        print(
            f"  {b['bucket_label']:<22} "
            f"{b['available_rows']:>5} {b['signal_count']:>5} "
            f"{b['signal_rate']:>8.3%} "
            f"{ps.get('min', float('nan')):>7.4f} "
            f"{ps.get('mean', float('nan')):>7.4f} "
            f"{ps.get('median', float('nan')):>7.4f} "
            f"{ps.get('max', float('nan')):>7.4f}"
        )

    print()
    print("[2] Probability distribution — counts >= threshold tally points")
    tally_keys = [f">={t:.2f}" for t in PROB_BUCKETS]
    hdr2 = f"  {'bucket':<22} " + " ".join(f"{k:>7}" for k in tally_keys)
    print(hdr2)
    print("  " + "-" * (len(hdr2) - 2))
    for b in buckets:
        if b.get("skip_reason"):
            continue
        ca = b.get("counts_above") or {}
        cells = " ".join(f"{ca.get(k, 0):>7}" for k in tally_keys)
        print(f"  {b['bucket_label']:<22} {cells}")

    print()
    print("[3] Threshold proximity")
    hdr3 = (
        f"  {'bucket':<22} "
        f"{'≤0.01':>6} {'≤0.02':>6} {'≤0.05':>6} "
        f"{'max_p':>8} {'gap_to_thr':>11} {'closest_below':>14}"
    )
    print(hdr3)
    print("  " + "-" * (len(hdr3) - 2))
    for b in buckets:
        if b.get("skip_reason"):
            continue
        tp = b["threshold_proximity"] or {}
        w = (tp.get("within_windows") or {})
        max_p = tp.get("max_prob")
        gap = tp.get("max_prob_gap_to_threshold")
        closest = tp.get("closest_prob_below_threshold")
        print(
            f"  {b['bucket_label']:<22} "
            f"{w.get('within_0.01_below', 0):>6} "
            f"{w.get('within_0.02_below', 0):>6} "
            f"{w.get('within_0.05_below', 0):>6} "
            f"{max_p if max_p is not None else float('nan'):>8.4f} "
            f"{gap if gap is not None else float('nan'):>11.4f} "
            f"{(f'{closest:.4f}' if closest is not None else 'n/a'):>14}"
        )

    print()
    print(f"[diagnostic_status] {diagnostic_status}")
    if warnings:
        for w in warnings:
            print(f"[warning] {w}")
    print(
        "[scope] read-only diagnostic; no training, no threshold tuning, "
        "no promotion. Dimensions reported independently."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
