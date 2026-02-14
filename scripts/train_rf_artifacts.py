#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ml.calibration import ProbabilityCalibrator
from ml.features import FEATURE_VERSION, build_feature_row, drop_features

DEFAULT_DUCKDB = os.getenv("DUCKDB_PATH", "data/pivot_training.duckdb")
DEFAULT_VIEW = os.getenv("DUCKDB_VIEW", "training_events_v1")
DEFAULT_OUT_DIR = os.getenv("RF_MODEL_DIR", "data/models")


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


def next_version(out_dir: Path) -> str:
    existing = sorted(out_dir.glob("metadata_v*.json"))
    if not existing:
        return "v001"
    last = existing[-1].stem.replace("metadata_", "")
    try:
        num = int(last.replace("v", ""))
        return f"v{num + 1:03d}"
    except ValueError:
        return "v001"


def compute_horizon_stats(df, target, horizon):
    import numpy as np

    stats = {}
    sub = df[df["horizon_min"] == horizon]
    if sub.empty:
        return stats

    for label in ["reject", "break"]:
        if label not in sub.columns:
            continue
        pos = sub[sub[label] == 1]
        neg = sub[sub[label] == 0]
        stats[f"{label}_rate"] = float(pos.shape[0] / max(1, sub.shape[0]))
        for metric in ["mfe_bps", "mae_bps"]:
            stats[f"{metric}_{label}"] = float(pos[metric].mean()) if not pos.empty else None
            stats[f"{metric}_other"] = float(neg[metric].mean()) if not neg.empty else None
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Train RF artifacts for inference server.")
    parser.add_argument("--db", default=DEFAULT_DUCKDB)
    parser.add_argument("--view", default=DEFAULT_VIEW)
    parser.add_argument("--horizons", default="5,15,60")
    parser.add_argument("--targets", default="reject,break")
    parser.add_argument("--calibration", choices=["auto", "isotonic", "sigmoid", "none"], default="auto")
    parser.add_argument("--calib-days", type=int, default=3)
    parser.add_argument("--min-events", type=int, default=200)
    parser.add_argument("--n-estimators", type=int, default=300)
    parser.add_argument("--max-depth", type=int, default=12)
    parser.add_argument("--min-samples-leaf", type=int, default=5)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--version", default=None)
    args = parser.parse_args()

    pd = require("pandas", "python3 -m pip install pandas")
    joblib = require("joblib", "python3 -m pip install joblib")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    version = args.version or next_version(out_dir)

    horizons = [int(h.strip()) for h in args.horizons.split(",") if h.strip()]
    targets = [t.strip() for t in args.targets.split(",") if t.strip()]

    manifest = {
        "version": version,
        "feature_version": FEATURE_VERSION,
        "models": {},
        "calibration": {},
        "stats": {},
        "trained_end_ts": None,
    }

    for horizon in horizons:
        df = load_dataframe(args.db, args.view, horizon)
        if df.empty:
            print(f"No rows for horizon {horizon}m. Skipping.")
            continue

        df = ensure_event_date(df)
        df = df.sort_values("ts_event")
        manifest["trained_end_ts"] = int(df["ts_event"].max()) if not df.empty else None

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
        calib_mask = df["event_date_et"].isin(calib_dates)

        for target in targets:
            if target not in df.columns:
                continue
            sub = df[df[target].notna()].copy()
            if sub.empty:
                continue

            y = sub[target].astype(int)
            X = feature_df.loc[sub.index]

            if len(X) < args.min_events:
                print(f"Not enough events for {target} {horizon}m.")
                continue

            categorical_cols = [
                c for c in X.columns if not pd.api.types.is_numeric_dtype(X[c])
            ]
            numeric_cols = [c for c in X.columns if pd.api.types.is_numeric_dtype(X[c])]
            pipeline = build_pipeline(numeric_cols, categorical_cols, args)

            X_train = X.loc[~calib_mask]
            y_train = y.loc[~calib_mask]
            pipeline.fit(X_train, y_train)

            calibrator = None
            calib_method = None
            if args.calibration != "none":
                X_calib = X.loc[calib_mask]
                y_calib = y.loc[calib_mask]
                if len(X_calib) >= 20 and len(set(y_calib)) == 2:
                    calib_method = choose_calibration(args.calibration, len(X_calib))
                    calibrator = ProbabilityCalibrator(pipeline, calib_method).fit(X_calib, y_calib)

            # Compute optimal decision threshold on calibration set
            import numpy as np
            optimal_threshold = 0.5
            model_obj = calibrator if calibrator is not None else pipeline
            if hasattr(model_obj, "predict_proba"):
                X_calib_for_thresh = X.loc[calib_mask] if len(X.loc[calib_mask]) >= 20 else X
                y_calib_for_thresh = y.loc[X_calib_for_thresh.index]
                try:
                    probs_calib = model_obj.predict_proba(X_calib_for_thresh)
                    if probs_calib.shape[1] == 2:
                        y_prob_calib = probs_calib[:, 1]
                        from sklearn.metrics import precision_recall_curve, f1_score as f1_fn
                        precisions, recalls, thresholds = precision_recall_curve(
                            y_calib_for_thresh, y_prob_calib
                        )
                        best_f1 = 0.0
                        for i, thresh in enumerate(thresholds):
                            if precisions[i] < 0.4:
                                continue
                            y_pred_t = (np.asarray(y_prob_calib) >= thresh).astype(int)
                            f1_val = float(f1_fn(y_calib_for_thresh, y_pred_t, zero_division=0))
                            if f1_val > best_f1:
                                best_f1 = f1_val
                                optimal_threshold = float(thresh)
                except Exception:
                    optimal_threshold = 0.5

            model_name = f"rf_{target}_{horizon}m_{version}.pkl"
            model_path = out_dir / model_name
            joblib.dump(
                {
                    "pipeline": pipeline,
                    "calibrator": calibrator,
                    "calibration": calib_method or "none",
                    "optimal_threshold": optimal_threshold,
                    "feature_columns": list(X.columns),
                    "numeric_columns": numeric_cols,
                    "categorical_columns": categorical_cols,
                },
                model_path,
            )

            manifest["models"].setdefault(target, {})[str(horizon)] = model_name
            manifest["calibration"].setdefault(target, {})[str(horizon)] = calib_method or "none"
            manifest["thresholds"] = manifest.get("thresholds", {})
            manifest["thresholds"].setdefault(target, {})[str(horizon)] = optimal_threshold
            manifest["stats"].setdefault(str(horizon), {})[target] = compute_horizon_stats(df, target, horizon)

            latest_name = f"latest_{target}_{horizon}m.pkl"
            latest_path = out_dir / latest_name
            latest_path.write_bytes(model_path.read_bytes())

    metadata_path = out_dir / f"metadata_{version}.json"
    with metadata_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    latest_manifest = out_dir / "manifest_latest.json"
    with latest_manifest.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    print(f"Saved manifest to {metadata_path}")
    print(f"Saved latest manifest to {latest_manifest}")


if __name__ == "__main__":
    main()
