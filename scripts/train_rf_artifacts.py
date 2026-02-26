#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ml.calibration import ProbabilityCalibrator
from ml.features import FEATURE_VERSION, build_feature_row, drop_features
from ml.thresholds import select_threshold, utility_bps_for_target

DEFAULT_DUCKDB = os.getenv("DUCKDB_PATH", "data/pivot_training.duckdb")
DEFAULT_VIEW = os.getenv("DUCKDB_VIEW", "training_events_v1")
DEFAULT_OUT_DIR = os.getenv("RF_MODEL_DIR", "data/models")
DEFAULT_METADATA_DIR = os.getenv("RF_METADATA_DIR", "metadata_runtime")
DEFAULT_CANDIDATE_MANIFEST = (
    os.getenv("RF_CANDIDATE_MANIFEST", "manifest_runtime_latest.json").strip()
    or "manifest_runtime_latest.json"
)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception:
        print(f"{module_name} not installed. Install with: {hint}", file=sys.stderr)
        sys.exit(1)


def choose_calibration(method, calib_size):
    if method != "auto":
        return method
    if calib_size >= 500:
        return "isotonic"
    return "sigmoid"


def build_pipeline(numeric_cols, categorical_cols, args):
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )
    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_cols),
            ("cat", categorical_transformer, categorical_cols),
        ]
    )

    rf = RandomForestClassifier(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth if args.max_depth > 0 else None,
        min_samples_leaf=args.min_samples_leaf,
        class_weight="balanced",
        n_jobs=-1,
        random_state=args.random_state,
    )

    return Pipeline(steps=[("prep", preprocessor), ("rf", rf)])


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
    df["event_date_et"] = pd.to_datetime(df["ts_event"], unit="ms", utc=True).dt.tz_convert(
        "America/New_York"
    ).dt.date
    return df


def build_feature_dataframe(df):
    pd = require("pandas", "python3 -m pip install pandas")
    rows = [build_feature_row(row) for row in df.to_dict("records")]
    return pd.DataFrame(rows, index=df.index)


def _parse_version_number(label: str) -> int | None:
    if not label:
        return None
    raw = str(label).strip().lower()
    if raw.startswith("v"):
        raw = raw[1:]
    if not raw.isdigit():
        return None
    return int(raw)


def _resolve_metadata_dir(out_dir: Path, raw_metadata_dir: str) -> Path:
    candidate = Path(raw_metadata_dir)
    if not candidate.is_absolute():
        candidate = out_dir / candidate
    return candidate


def next_version(out_dir: Path, metadata_dir: Path) -> str:
    version_numbers: list[int] = []

    for path in metadata_dir.glob("metadata_v*.json"):
        parsed = _parse_version_number(path.stem.replace("metadata_", ""))
        if parsed is not None:
            version_numbers.append(parsed)

    # Backward compatibility: include legacy metadata files in model root.
    for path in out_dir.glob("metadata_v*.json"):
        parsed = _parse_version_number(path.stem.replace("metadata_", ""))
        if parsed is not None:
            version_numbers.append(parsed)

    # Fallback: infer from existing model artifacts if metadata files were cleaned.
    for path in out_dir.glob("rf_*_v*.pkl"):
        stem = path.stem
        if "_v" not in stem:
            continue
        suffix = stem.rsplit("_v", 1)[-1]
        parsed = _parse_version_number(suffix)
        if parsed is not None:
            version_numbers.append(parsed)

    if not version_numbers:
        return "v001"

    return f"v{max(version_numbers) + 1:03d}"


def _temp_path(path: Path) -> Path:
    return path.with_name(
        f".{path.name}.tmp-{os.getpid()}-{int(time.time() * 1000)}"
    )


def atomic_write_json(path: Path, payload: dict) -> None:
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


def atomic_joblib_dump(joblib_module, payload: dict, path: Path) -> None:
    tmp_path = _temp_path(path)
    try:
        joblib_module.dump(payload, tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def atomic_copy_file(src: Path, dst: Path) -> None:
    tmp_path = _temp_path(dst)
    try:
        shutil.copy2(src, tmp_path)
        os.replace(tmp_path, dst)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def compute_horizon_stats(df, target, horizon):
    stats = {}
    sub = df[df["horizon_min"] == horizon]
    if sub.empty:
        return stats

    stats["sample_size"] = int(sub.shape[0])
    for label in ["reject", "break"]:
        if label not in sub.columns:
            continue
        pos = sub[sub[label] == 1]
        neg = sub[sub[label] == 0]
        stats[f"{label}_count"] = int(pos.shape[0])
        stats[f"{label}_other_count"] = int(neg.shape[0])
        stats[f"{label}_rate"] = float(pos.shape[0] / max(1, sub.shape[0]))
        for metric in ["mfe_bps", "mae_bps"]:
            stats[f"{metric}_{label}"] = float(pos[metric].mean()) if not pos.empty else None
            stats[f"{metric}_other"] = float(neg[metric].mean()) if not neg.empty else None
    return stats


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


def main() -> None:
    default_trade_cost_bps = _env_float(
        "RF_THRESHOLD_TRADE_COST_BPS",
        _env_float("ML_COST_SPREAD_BPS", 0.8)
        + _env_float("ML_COST_SLIPPAGE_BPS", 0.4)
        + _env_float("ML_COST_COMMISSION_BPS", 0.1),
    )

    parser = argparse.ArgumentParser(description="Train RF artifacts for inference server.")
    parser.add_argument("--db", default=DEFAULT_DUCKDB)
    parser.add_argument("--view", default=DEFAULT_VIEW)
    parser.add_argument("--horizons", default="5,15,30,60")
    parser.add_argument("--targets", default="reject,break")
    parser.add_argument("--calibration", choices=["auto", "isotonic", "sigmoid", "none"], default="auto")
    parser.add_argument("--calib-days", type=int, default=3)
    parser.add_argument("--min-events", type=int, default=200)
    parser.add_argument("--n-estimators", type=int, default=300)
    parser.add_argument("--max-depth", type=int, default=12)
    parser.add_argument("--min-samples-leaf", type=int, default=5)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument(
        "--threshold-objective",
        choices=["f1", "utility_bps"],
        default=os.getenv("RF_THRESHOLD_OBJECTIVE", "utility_bps"),
        help="Threshold objective: utility_bps (default, cost-aware) or f1.",
    )
    parser.add_argument(
        "--threshold-precision-floor",
        type=float,
        default=_env_float("RF_THRESHOLD_PRECISION_FLOOR", 0.40),
        help="Minimum precision required for candidate threshold selection.",
    )
    parser.add_argument(
        "--threshold-min-signals",
        type=int,
        default=int(_env_float("RF_THRESHOLD_MIN_SIGNALS", 10)),
        help="Minimum predicted positives required for threshold candidates.",
    )
    parser.add_argument(
        "--threshold-trade-cost-bps",
        type=float,
        default=default_trade_cost_bps,
        help="Per-signal cost (bps) used with utility_bps objective.",
    )
    parser.add_argument(
        "--threshold-stability-band",
        type=float,
        default=_env_float("RF_THRESHOLD_STABILITY_BAND", 0.0),
        help="Average score over +/- band around threshold to avoid knife-edge picks.",
    )
    parser.add_argument(
        "--calib-fit-fraction",
        type=float,
        default=_env_float("RF_CALIB_FIT_FRACTION", 0.6),
        help="Fraction of calibration window used to fit calibrator; remainder used for threshold tuning.",
    )
    parser.add_argument(
        "--calib-min-fit-events",
        type=int,
        default=int(_env_float("RF_CALIB_MIN_FIT_EVENTS", 20)),
        help="Minimum events reserved for calibration fitting slice.",
    )
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--metadata-dir",
        default=DEFAULT_METADATA_DIR,
        help="Directory for runtime metadata manifests (absolute or relative to --out-dir)",
    )
    parser.add_argument("--version", default=None)
    parser.add_argument(
        "--candidate-manifest",
        default=DEFAULT_CANDIDATE_MANIFEST,
        help="Runtime candidate manifest filename written into --out-dir",
    )
    parser.add_argument(
        "--allow-partial-manifest",
        action="store_true",
        default=False,
        help="Allow publishing when some target/horizon models are missing",
    )
    args = parser.parse_args()

    pd = require("pandas", "python3 -m pip install pandas")
    joblib = require("joblib", "python3 -m pip install joblib")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir = _resolve_metadata_dir(out_dir, args.metadata_dir)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    version = args.version or next_version(out_dir, metadata_dir)

    horizons = [int(h.strip()) for h in args.horizons.split(",") if h.strip()]
    targets = [t.strip() for t in args.targets.split(",") if t.strip()]

    manifest = {
        "version": version,
        "feature_version": FEATURE_VERSION,
        "models": {},
        "calibration": {},
        "thresholds_meta": {},
        "stats": {},
        "trained_end_ts": None,
    }
    trained_end_ts_max = None
    latest_aliases: list[tuple[Path, Path]] = []

    for horizon in horizons:
        df = load_dataframe(args.db, args.view, horizon)
        if df.empty:
            print(f"No rows for horizon {horizon}m. Skipping.")
            continue

        df = ensure_event_date(df)
        df = df.sort_values("ts_event")
        if not df.empty:
            horizon_end_ts = int(df["ts_event"].max())
            if trained_end_ts_max is None or horizon_end_ts > trained_end_ts_max:
                trained_end_ts_max = horizon_end_ts

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

        feature_df = build_feature_dataframe(df)
        feature_df = feature_df.drop(columns=[c for c in all_drops if c in feature_df.columns], errors="ignore")
        feature_df = feature_df.loc[:, feature_df.notna().any()]

        dates = sorted({d for d in df["event_date_et"].tolist() if d is not None})
        calib_dates = set(dates[-args.calib_days :]) if args.calib_days and dates else set()

        for target in targets:
            if target not in df.columns:
                continue
            sub = df[df[target].notna()].copy()
            if sub.empty:
                continue

            y = sub[target].astype(int)
            X = feature_df.loc[sub.index]
            calib_mask_sub = sub["event_date_et"].isin(calib_dates)

            if len(X) < args.min_events:
                print(f"Not enough events for {target} {horizon}m.")
                continue

            X_train = X.loc[~calib_mask_sub]
            y_train = y.loc[~calib_mask_sub]
            # Some features can be present overall but become fully missing in
            # the non-calibration training split. Drop them to avoid imputer warnings.
            all_null_train_cols = [col for col in X_train.columns if not X_train[col].notna().any()]
            if all_null_train_cols:
                X = X.drop(columns=all_null_train_cols)
                X_train = X_train.drop(columns=all_null_train_cols)
                print(
                    f"Dropping all-null training features for {target} {horizon}m: "
                    + ", ".join(sorted(all_null_train_cols))
                )

            if X.shape[1] == 0:
                print(f"No usable features for {target} {horizon}m after null filtering.")
                continue

            categorical_cols = [
                c for c in X.columns if not pd.api.types.is_numeric_dtype(X[c])
            ]
            numeric_cols = [c for c in X.columns if pd.api.types.is_numeric_dtype(X[c])]
            pipeline = build_pipeline(numeric_cols, categorical_cols, args)
            pipeline.fit(X_train, y_train)

            calibrator = None
            calib_method = None
            calibration_shared_slice = False
            X_calib_fit = None
            y_calib_fit = None
            X_calib_tune = None
            y_calib_tune = None
            if args.calibration != "none":
                X_calib = X.loc[calib_mask_sub]
                y_calib = y.loc[calib_mask_sub]
                (
                    X_calib_fit,
                    y_calib_fit,
                    X_calib_tune,
                    y_calib_tune,
                    split_used,
                ) = split_calibration_slices(
                    X_calib,
                    y_calib,
                    fit_fraction=float(args.calib_fit_fraction),
                    min_fit_events=int(args.calib_min_fit_events),
                    min_tune_events=int(args.threshold_min_signals),
                )
                calibration_shared_slice = not split_used
                if len(X_calib_fit) >= 20 and len(set(y_calib_fit)) == 2:
                    calib_method = choose_calibration(args.calibration, len(X_calib_fit))
                    calibrator = ProbabilityCalibrator(pipeline, calib_method).fit(X_calib_fit, y_calib_fit)

            # Compute optimal decision threshold on calibration set only.
            # Never fall back to the training set — that causes optimistic bias.
            optimal_threshold = 0.5
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
                "precision_floor": float(args.threshold_precision_floor),
                "min_signals": int(args.threshold_min_signals),
                "trade_cost_bps": float(args.threshold_trade_cost_bps),
                "stability_band": float(threshold_stability_band),
                "top_candidates": [],
                "calibration_shared_slice": bool(calibration_shared_slice),
                "calibration_fit_size": int(len(X_calib_fit)) if X_calib_fit is not None else 0,
                "threshold_tune_size": int(len(X_calib_tune)) if X_calib_tune is not None else 0,
                "search_enabled": True,
                "search_skip_reason": "",
            }
            model_obj = calibrator if calibrator is not None else pipeline
            X_calib_set = X_calib_tune if X_calib_tune is not None else X.loc[calib_mask_sub]
            y_calib_for_thresh = y_calib_tune if y_calib_tune is not None else y.loc[X_calib_set.index]
            if calibrator is not None and calibration_shared_slice:
                threshold_meta["search_enabled"] = False
                threshold_meta["search_skip_reason"] = "shared_calibration_slice"
            elif not hasattr(model_obj, "predict_proba"):
                threshold_meta["search_enabled"] = False
                threshold_meta["search_skip_reason"] = "model_missing_predict_proba"
            elif len(X_calib_set) < 20:
                threshold_meta["search_enabled"] = False
                threshold_meta["search_skip_reason"] = "insufficient_tuning_rows"

            if threshold_meta["search_enabled"]:
                try:
                    probs_calib = model_obj.predict_proba(X_calib_set)
                    if probs_calib.shape[1] == 2 and len(set(y_calib_for_thresh)) == 2:
                        y_prob_calib = probs_calib[:, 1]
                        utility_values = None
                        if args.threshold_objective == "utility_bps":
                            utility_values = utility_bps_for_target(
                                sub.loc[X_calib_set.index, "return_bps"],
                                sub.loc[X_calib_set.index, "touch_side"],
                                target,
                                trade_cost_bps=float(args.threshold_trade_cost_bps),
                            )
                        selection = select_threshold(
                            y_calib_for_thresh.to_numpy(),
                            y_prob_calib,
                            objective=args.threshold_objective,
                            precision_floor=float(args.threshold_precision_floor),
                            min_signals=int(args.threshold_min_signals),
                            default_threshold=0.5,
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
                        threshold_meta["search_skip_reason"] = "invalid_probability_shape_or_labels"
                except Exception:
                    optimal_threshold = 0.5
                    threshold_meta["search_enabled"] = False
                    threshold_meta["search_skip_reason"] = "threshold_selection_exception"

            # Compute per-feature quantile bounds for drift detection at inference.
            # Uses the full training set (not calib) since we want the broadest
            # representative range. p1/p99 gives room for natural variance while
            # catching genuine distribution shifts.
            feature_bounds = {}
            for col in numeric_cols:
                series = X_train[col].dropna()
                if len(series) >= 10:
                    feature_bounds[col] = {
                        "p1": float(series.quantile(0.01)),
                        "p99": float(series.quantile(0.99)),
                        "median": float(series.median()),
                    }

            model_name = f"rf_{target}_{horizon}m_{version}.pkl"
            model_path = out_dir / model_name
            atomic_joblib_dump(
                joblib,
                {
                    "pipeline": pipeline,
                    "calibrator": calibrator,
                    "calibration": calib_method or "none",
                    "optimal_threshold": optimal_threshold,
                    "threshold_meta": threshold_meta,
                    "feature_columns": list(X.columns),
                    "numeric_columns": numeric_cols,
                    "categorical_columns": categorical_cols,
                    "feature_bounds": feature_bounds,
                },
                model_path,
            )

            manifest["models"].setdefault(target, {})[str(horizon)] = model_name
            manifest["calibration"].setdefault(target, {})[str(horizon)] = calib_method or "none"
            manifest["thresholds"] = manifest.get("thresholds", {})
            manifest["thresholds"].setdefault(target, {})[str(horizon)] = optimal_threshold
            manifest["thresholds_meta"].setdefault(target, {})[str(horizon)] = threshold_meta
            manifest["stats"].setdefault(str(horizon), {})[target] = compute_horizon_stats(df, target, horizon)

            latest_name = f"latest_{target}_{horizon}m.pkl"
            latest_path = out_dir / latest_name
            latest_aliases.append((model_path, latest_path))

    manifest["trained_end_ts"] = trained_end_ts_max

    expected_pairs = {(target, str(horizon)) for target in targets for horizon in horizons}
    actual_pairs = set()
    for target, horizon_map in manifest["models"].items():
        for horizon_key in horizon_map.keys():
            actual_pairs.add((target, str(horizon_key)))
    missing_pairs = sorted(expected_pairs - actual_pairs)

    if not actual_pairs:
        print(
            "No model artifacts were produced. "
            f"Aborting publish to preserve existing {args.candidate_manifest}.",
            file=sys.stderr,
        )
        sys.exit(1)

    if missing_pairs and not args.allow_partial_manifest:
        missing_fmt = ", ".join(f"{t}:{h}m" for t, h in missing_pairs)
        print(
            f"Partial model set produced; aborting publish to preserve existing {args.candidate_manifest}. "
            f"Missing: {missing_fmt}. "
            "Use --allow-partial-manifest to override.",
            file=sys.stderr,
        )
        sys.exit(1)

    metadata_path = metadata_dir / f"metadata_{version}.json"
    atomic_write_json(metadata_path, manifest)

    for source_path, alias_path in latest_aliases:
        atomic_copy_file(source_path, alias_path)

    latest_manifest = out_dir / args.candidate_manifest
    # Publish latest manifest last so readers never observe a half-written
    # pointer to artifacts.
    atomic_write_json(latest_manifest, manifest)

    print(f"Saved manifest to {metadata_path}")
    print(f"Saved latest manifest to {latest_manifest}")


if __name__ == "__main__":
    main()
