#!/usr/bin/env python3
"""Refit model calibration + decision thresholds without retraining base models.

This script:
1) loads active model artifacts from manifest_active.json,
2) rebuilds feature rows from the training DuckDB view for recent matured labels,
3) refits only calibrators (sigmoid/isotonic),
4) updates optimal thresholds from the calibration slice,
5) rewrites model payloads + active manifest atomically.

No new model version is created and no governance promotion is performed.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ml.calibration import ProbabilityCalibrator
from ml.features import drop_features, build_feature_row
from ml.thresholds import select_threshold, utility_bps_for_target

DEFAULT_DUCKDB = os.getenv("DUCKDB_PATH", "data/pivot_training.duckdb")
DEFAULT_VIEW = os.getenv("DUCKDB_VIEW", "training_events_v1")
DEFAULT_MODEL_DIR = os.getenv("RF_MODEL_DIR", "data/models")
DEFAULT_ACTIVE_MANIFEST = os.getenv("RF_ACTIVE_MANIFEST", "manifest_active.json").strip() or "manifest_active.json"
DEFAULT_SUMMARY_OUT = os.getenv("CALIB_REFIT_SUMMARY_PATH", "logs/calibration_refit_last.json")


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return bool(default)
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception:
        print(f"{module_name} not installed. Install with: {hint}", file=sys.stderr)
        sys.exit(1)


def choose_calibration(method: str, calib_size: int) -> str:
    if method != "auto":
        return method
    if calib_size >= 500:
        return "isotonic"
    return "sigmoid"


def load_dataframe(db_path: str, view: str, horizon: int):
    duckdb = require("duckdb", "python3 -m pip install duckdb")
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(
            f"SELECT * FROM {view} WHERE horizon_min = ? ORDER BY ts_event",
            [horizon],
        ).df()
    finally:
        con.close()
    return df


def ensure_event_date(df):
    pd = require("pandas", "python3 -m pip install pandas")
    if "event_date_et" in df.columns:
        return df
    if "ts_event" not in df.columns:
        raise ValueError("Missing ts_event in training view")
    df["event_date_et"] = (
        pd.to_datetime(df["ts_event"], unit="ms", utc=True).dt.tz_convert("America/New_York").dt.date
    )
    return df


def build_feature_dataframe(df):
    pd = require("pandas", "python3 -m pip install pandas")
    rows = [build_feature_row(row) for row in df.to_dict("records")]
    return pd.DataFrame(rows, index=df.index)


def _temp_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.tmp-{os.getpid()}-{int(time.time() * 1000)}")


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = _temp_path(path)
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def atomic_joblib_dump(joblib_module, payload: dict[str, Any], path: Path) -> None:
    tmp_path = _temp_path(path)
    try:
        joblib_module.dump(payload, tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def parse_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def parse_horizons(raw: str | None) -> list[int]:
    if not raw:
        return []
    out: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        out.append(int(token))
    return out


def split_calibration_slices(X_calib, y_calib, *, fit_fraction: float, min_fit_events: int, min_tune_events: int):
    n = len(X_calib)
    if n == 0:
        return X_calib, y_calib, X_calib, y_calib, False

    fit_n = int(round(n * fit_fraction))
    fit_n = max(int(min_fit_events), fit_n)
    fit_n = min(fit_n, max(0, n - int(min_tune_events)))

    if fit_n <= 0 or fit_n >= n:
        return X_calib, y_calib, X_calib, y_calib, False

    X_fit = X_calib.iloc[:fit_n]
    y_fit = y_calib.iloc[:fit_n]
    X_tune = X_calib.iloc[fit_n:]
    y_tune = y_calib.iloc[fit_n:]
    return X_fit, y_fit, X_tune, y_tune, True


@dataclass
class PairResult:
    target: str
    horizon: int
    status: str
    reason: str
    calib_size: int = 0
    method: str = ""
    threshold: float | None = None
    threshold_meta: dict[str, Any] | None = None


def main() -> None:
    default_trade_cost_bps = _env_float(
        "CALIB_REFIT_THRESHOLD_TRADE_COST_BPS",
        _env_float(
            "RF_THRESHOLD_TRADE_COST_BPS",
            _env_float("ML_COST_SPREAD_BPS", 0.8)
            + _env_float("ML_COST_SLIPPAGE_BPS", 0.4)
            + _env_float("ML_COST_COMMISSION_BPS", 0.1),
        ),
    )

    parser = argparse.ArgumentParser(description="Refit calibration only for active model artifacts.")
    parser.add_argument("--db", default=DEFAULT_DUCKDB)
    parser.add_argument("--view", default=DEFAULT_VIEW)
    parser.add_argument("--models-dir", default=DEFAULT_MODEL_DIR)
    parser.add_argument("--active-manifest", default=DEFAULT_ACTIVE_MANIFEST)
    parser.add_argument("--targets", default="")
    parser.add_argument("--horizons", default="")
    parser.add_argument("--calibration", choices=["auto", "isotonic", "sigmoid", "none"], default="auto")
    parser.add_argument("--calib-days", type=int, default=int(os.getenv("CALIB_REFIT_CALIB_DAYS", "5")))
    parser.add_argument("--min-calib-events", type=int, default=int(os.getenv("CALIB_REFIT_MIN_CALIB_EVENTS", "40")))
    parser.add_argument(
        "--min-threshold-events",
        type=int,
        default=int(os.getenv("CALIB_REFIT_MIN_THRESHOLD_EVENTS", "20")),
    )
    parser.add_argument("--precision-floor", type=float, default=float(os.getenv("CALIB_REFIT_PRECISION_FLOOR", "0.40")))
    parser.add_argument(
        "--threshold-objective",
        choices=["f1", "utility_bps"],
        default=os.getenv("CALIB_REFIT_THRESHOLD_OBJECTIVE", os.getenv("RF_THRESHOLD_OBJECTIVE", "utility_bps")),
    )
    parser.add_argument(
        "--threshold-min-signals",
        type=int,
        default=int(_env_float("CALIB_REFIT_THRESHOLD_MIN_SIGNALS", 10)),
    )
    parser.add_argument(
        "--threshold-trade-cost-bps",
        type=float,
        default=default_trade_cost_bps,
    )
    parser.add_argument(
        "--threshold-stability-band",
        type=float,
        default=_env_float("CALIB_REFIT_THRESHOLD_STABILITY_BAND", _env_float("RF_THRESHOLD_STABILITY_BAND", 0.0)),
    )
    parser.add_argument(
        "--retune-thresholds",
        action="store_true",
        default=_env_bool("CALIB_REFIT_RETUNE_THRESHOLDS", False),
        help="Enable threshold retuning during calibration refit.",
    )
    parser.add_argument(
        "--calib-fit-fraction",
        type=float,
        default=_env_float("CALIB_REFIT_CALIB_FIT_FRACTION", 0.6),
        help="Fraction of calibration rows used to fit calibrator before threshold tuning.",
    )
    parser.add_argument(
        "--calib-min-fit-events",
        type=int,
        default=int(_env_float("CALIB_REFIT_CALIB_MIN_FIT_EVENTS", 20)),
        help="Minimum rows reserved for calibration fitting slice.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--summary-out", default=DEFAULT_SUMMARY_OUT)
    args = parser.parse_args()

    require("numpy", "python3 -m pip install numpy")
    joblib = require("joblib", "python3 -m pip install joblib")

    model_dir = Path(args.models_dir)
    model_dir.mkdir(parents=True, exist_ok=True)
    manifest_arg = Path(args.active_manifest)
    manifest_path = manifest_arg if manifest_arg.is_absolute() else (model_dir / manifest_arg)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Active manifest not found: {manifest_path}")

    with manifest_path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)

    manifest_models = manifest.get("models", {}) if isinstance(manifest, dict) else {}
    if not isinstance(manifest_models, dict) or not manifest_models:
        raise ValueError("Invalid active manifest: missing models map")

    requested_targets = set(parse_csv(args.targets))
    requested_horizons = set(parse_horizons(args.horizons))

    pairs: list[tuple[str, int, str]] = []
    for target, horizon_map in manifest_models.items():
        if requested_targets and target not in requested_targets:
            continue
        if not isinstance(horizon_map, dict):
            continue
        for horizon_key, model_name in horizon_map.items():
            try:
                horizon = int(horizon_key)
            except (TypeError, ValueError):
                continue
            if requested_horizons and horizon not in requested_horizons:
                continue
            pairs.append((target, horizon, str(model_name)))

    if not pairs:
        raise SystemExit("No target/horizon pairs selected for calibration refit.")

    label_cols = {
        "event_id",
        "ts_event",
        "created_at",
        "event_ts_utc",
        "event_ts_et",
        "event_date_et",
        "confluence_types",
        "horizon_min",
        "return_bps",
        "mfe_bps",
        "mae_bps",
        "reject",
        "break",
        "resolution_min",
        "or_high",
        "or_low",
    }
    feature_drops = drop_features()
    all_drops = label_cols | feature_drops

    horizon_frames: dict[int, Any] = {}
    results: list[PairResult] = []
    updated_pairs = 0
    attempted_pairs = 0

    for target, horizon, model_name in pairs:
        attempted_pairs += 1
        model_path = model_dir / model_name
        if not model_path.exists():
            results.append(PairResult(target, horizon, "skipped", f"missing model file {model_name}"))
            continue

        if horizon not in horizon_frames:
            df = load_dataframe(args.db, args.view, horizon)
            if df.empty:
                horizon_frames[horizon] = None
            else:
                horizon_frames[horizon] = ensure_event_date(df.sort_values("ts_event"))
        df = horizon_frames[horizon]
        if df is None:
            results.append(PairResult(target, horizon, "skipped", "no rows for horizon"))
            continue
        if target not in df.columns:
            results.append(PairResult(target, horizon, "skipped", f"target column '{target}' missing"))
            continue

        sub = df[df[target].notna()].copy()
        if sub.empty:
            results.append(PairResult(target, horizon, "skipped", "no matured labels"))
            continue

        dates = sorted({d for d in sub["event_date_et"].tolist() if d is not None})
        if args.calib_days > 0 and dates:
            calib_dates = set(dates[-args.calib_days :])
            calib_mask = sub["event_date_et"].isin(calib_dates)
        else:
            calib_mask = sub["event_date_et"].notna()

        payload = joblib.load(model_path)
        pipeline = payload.get("pipeline")
        if pipeline is None:
            results.append(PairResult(target, horizon, "skipped", "model payload missing pipeline"))
            continue

        feature_df = build_feature_dataframe(sub)
        feature_df = feature_df.drop(columns=[c for c in all_drops if c in feature_df.columns], errors="ignore")

        feature_columns = payload.get("feature_columns") or []
        if feature_columns:
            import numpy as np

            for col in feature_columns:
                if col not in feature_df.columns:
                    feature_df[col] = np.nan
            feature_df = feature_df.loc[:, feature_columns]
        else:
            feature_df = feature_df.loc[:, feature_df.notna().any()]
            feature_columns = list(feature_df.columns)

        if feature_df.shape[1] == 0:
            results.append(PairResult(target, horizon, "skipped", "no usable features"))
            continue

        y = sub[target].astype(int)
        X_calib_all = feature_df.loc[calib_mask]
        y_calib_all = y.loc[calib_mask]

        calib_size = int(len(X_calib_all))
        class_count = len(set(y_calib_all.tolist()))
        if calib_size < args.min_calib_events or class_count < 2:
            results.append(
                PairResult(
                    target,
                    horizon,
                    "skipped",
                    f"insufficient calibration sample (n={calib_size}, classes={class_count})",
                    calib_size=calib_size,
                )
            )
            continue

        min_tune_events = int(max(args.min_threshold_events, args.threshold_min_signals)) if args.retune_thresholds else 0
        (
            X_calib_fit,
            y_calib_fit,
            X_calib_tune,
            y_calib_tune,
            split_used,
        ) = split_calibration_slices(
            X_calib_all,
            y_calib_all,
            fit_fraction=float(args.calib_fit_fraction),
            min_fit_events=int(args.calib_min_fit_events),
            min_tune_events=min_tune_events,
        )

        fit_size = int(len(X_calib_fit))
        fit_class_count = len(set(y_calib_fit.tolist()))
        if fit_size < int(args.calib_min_fit_events) or fit_class_count < 2:
            results.append(
                PairResult(
                    target,
                    horizon,
                    "skipped",
                    f"insufficient calibration fit sample (n={fit_size}, classes={fit_class_count})",
                    calib_size=fit_size,
                )
            )
            continue

        if args.calibration == "none":
            calibrator = None
            method = "none"
        else:
            method = choose_calibration(args.calibration, fit_size)
            calibrator = ProbabilityCalibrator(pipeline, method).fit(X_calib_fit, y_calib_fit)

        model_obj = calibrator if calibrator is not None else pipeline
        optimal_threshold = float(payload.get("optimal_threshold", 0.5) or 0.5)
        threshold_stability_band = float(args.threshold_stability_band)
        if args.threshold_objective == "utility_bps" and threshold_stability_band <= 0.0:
            threshold_stability_band = 0.02
        threshold_meta = {
            "objective": args.threshold_objective,
            "score": None,
            "precision": None,
            "recall": None,
            "signals": 0,
            "evaluated_candidates": 0,
            "fallback": True,
            "precision_floor": float(args.precision_floor),
            "min_signals": int(args.threshold_min_signals),
            "trade_cost_bps": float(args.threshold_trade_cost_bps),
            "stability_band": float(threshold_stability_band),
            "top_candidates": [],
            "retuned_in_refit": bool(args.retune_thresholds),
            "calibration_shared_slice": bool(not split_used),
            "calibration_fit_size": fit_size,
            "threshold_tune_size": int(len(X_calib_tune)),
            "search_enabled": bool(args.retune_thresholds),
            "search_skip_reason": "",
        }
        if not args.retune_thresholds:
            threshold_meta["search_enabled"] = False
            threshold_meta["search_skip_reason"] = "retune_disabled"
        elif calibrator is not None and not split_used:
            threshold_meta["search_enabled"] = False
            threshold_meta["search_skip_reason"] = "shared_calibration_slice"
        elif not hasattr(model_obj, "predict_proba"):
            threshold_meta["search_enabled"] = False
            threshold_meta["search_skip_reason"] = "model_missing_predict_proba"
        elif len(X_calib_tune) < int(args.min_threshold_events):
            threshold_meta["search_enabled"] = False
            threshold_meta["search_skip_reason"] = "insufficient_tuning_rows"
        elif len(set(y_calib_tune.tolist())) < 2:
            threshold_meta["search_enabled"] = False
            threshold_meta["search_skip_reason"] = "insufficient_tuning_classes"

        if threshold_meta["search_enabled"]:
            try:
                probs = model_obj.predict_proba(X_calib_tune)
                if probs.shape[1] == 2:
                    y_prob = probs[:, 1]
                    utility_values = None
                    if args.threshold_objective == "utility_bps":
                        utility_values = utility_bps_for_target(
                            sub.loc[X_calib_tune.index, "return_bps"],
                            sub.loc[X_calib_tune.index, "touch_side"],
                            target,
                            trade_cost_bps=float(args.threshold_trade_cost_bps),
                        )
                    selection = select_threshold(
                        y_calib_tune.to_numpy(),
                        y_prob,
                        objective=args.threshold_objective,
                        precision_floor=float(args.precision_floor),
                        min_signals=int(args.threshold_min_signals),
                        default_threshold=optimal_threshold,
                        utility_per_signal=utility_values,
                        stability_band=float(threshold_stability_band),
                    )
                    optimal_threshold = float(selection.threshold)
                    threshold_meta.update(
                        {
                            "score": float(selection.score),
                            "precision": float(selection.precision),
                            "recall": float(selection.recall),
                            "signals": int(selection.signals),
                            "evaluated_candidates": int(selection.evaluated_candidates),
                            "fallback": bool(selection.fallback),
                            "stability_score": float(
                                selection.stability_score
                                if selection.stability_score is not None
                                else selection.score
                            ),
                            "top_candidates": selection.top_candidates,
                        }
                    )
                else:
                    threshold_meta["search_enabled"] = False
                    threshold_meta["search_skip_reason"] = "invalid_probability_shape"
            except Exception:
                threshold_meta["search_enabled"] = False
                threshold_meta["search_skip_reason"] = "threshold_selection_exception"

        payload["calibrator"] = calibrator
        payload["calibration"] = method
        payload["optimal_threshold"] = optimal_threshold
        payload["threshold_meta"] = threshold_meta
        payload["calibration_refit"] = {
            "ts_ms": int(time.time() * 1000),
            "calib_size": calib_size,
            "calib_fit_size": fit_size,
            "calib_tune_size": int(len(X_calib_tune)),
            "method": method,
            "calib_days": int(args.calib_days),
            "precision_floor": float(args.precision_floor),
            "retune_thresholds": bool(args.retune_thresholds),
            "threshold_objective": args.threshold_objective,
            "threshold_min_signals": int(args.threshold_min_signals),
            "threshold_trade_cost_bps": float(args.threshold_trade_cost_bps),
            "threshold_stability_band": float(threshold_stability_band),
            "target": target,
            "horizon": horizon,
        }
        payload["feature_columns"] = feature_columns

        manifest.setdefault("calibration", {}).setdefault(target, {})[str(horizon)] = method
        manifest.setdefault("thresholds", {}).setdefault(target, {})[str(horizon)] = optimal_threshold
        manifest.setdefault("thresholds_meta", {}).setdefault(target, {})[str(horizon)] = threshold_meta

        if not args.dry_run:
            atomic_joblib_dump(joblib, payload, model_path)
        updated_pairs += 1
        results.append(
            PairResult(
                target,
                horizon,
                "updated",
                "calibration refit applied",
                calib_size=calib_size,
                method=method,
                threshold=optimal_threshold,
                threshold_meta=threshold_meta,
            )
        )

    if updated_pairs > 0:
        manifest["calibration_refit_ts"] = int(time.time() * 1000)
        if not args.dry_run:
            atomic_write_json(manifest_path, manifest)

    summary = {
        "status": "ok",
        "dry_run": bool(args.dry_run),
        "attempted_pairs": attempted_pairs,
        "updated_pairs": updated_pairs,
        "manifest_path": str(manifest_path),
        "db": args.db,
        "view": args.view,
        "calib_days": int(args.calib_days),
        "retune_thresholds": bool(args.retune_thresholds),
        "calib_fit_fraction": float(args.calib_fit_fraction),
        "calib_min_fit_events": int(args.calib_min_fit_events),
        "threshold_objective": args.threshold_objective,
        "threshold_min_signals": int(args.threshold_min_signals),
        "threshold_trade_cost_bps": float(args.threshold_trade_cost_bps),
        "threshold_stability_band": float(args.threshold_stability_band),
        "results": [
            {
                "target": r.target,
                "horizon": r.horizon,
                "status": r.status,
                "reason": r.reason,
                "calib_size": r.calib_size,
                "method": r.method,
                "threshold": r.threshold,
                "threshold_meta": r.threshold_meta,
            }
            for r in results
        ],
    }

    summary_out = Path(args.summary_out)
    if not summary_out.is_absolute():
        summary_out = ROOT / summary_out
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(summary_out, summary)
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
