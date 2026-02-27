#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

DEFAULT_DUCKDB = os.getenv("DUCKDB_PATH", "data/pivot_training.duckdb")
DEFAULT_VIEW = os.getenv("DUCKDB_VIEW", "training_events_v1")
DEFAULT_OUT = os.getenv("RF_METRICS_OUT", "data/exports/rf_walkforward_metrics.json")
DEFAULT_FEATURE_OUT = os.getenv("RF_FEATURE_OUT", "data/exports/rf_feature_report.json")
DEFAULT_FEATURE_CSV = os.getenv("RF_FEATURE_CSV", "data/exports/rf_feature_report.csv")
DEFAULT_CALIB_OUT = os.getenv("RF_CALIB_OUT", "data/exports/rf_calibration_curve.json")
DEFAULT_CALIB_CSV = os.getenv("RF_CALIB_CSV", "data/exports/rf_calibration_curve.csv")


def require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception:  # pragma: no cover - import guard
        print(f"{module_name} not installed. Install with: {hint}", file=sys.stderr)
        sys.exit(1)


def load_dataframe(db_path: str, view: str, horizon: int):
    duckdb = require("duckdb", "python3 -m pip install duckdb")
    pd = require("pandas", "python3 -m pip install pandas")
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


def generate_splits(dates, train_days, calib_days, test_days, max_folds, stride_days=None):
    splits = []
    if stride_days is None:
        stride_days = test_days
    if stride_days <= 0:
        raise ValueError("stride_days must be > 0")
    start = 0
    while start + train_days + calib_days + test_days <= len(dates):
        train = dates[start : start + train_days]
        calib = dates[start + train_days : start + train_days + calib_days]
        test = dates[start + train_days + calib_days : start + train_days + calib_days + test_days]
        splits.append((train, calib, test))
        start += stride_days
        if max_folds and len(splits) >= max_folds:
            break
    return splits


from ml.calibration import ProbabilityCalibrator
from ml.features import build_feature_row, drop_features


def choose_calibration(method, calib_size):
    if method != "auto":
        return method
    if calib_size >= 500:
        return "isotonic"
    return "sigmoid"


def build_pipeline(numeric_cols, categorical_cols, args):
    sklearn = require("sklearn", "python3 -m pip install scikit-learn")
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


def find_optimal_threshold(y_true, y_prob, min_precision=0.4):
    """Find the threshold that maximizes F1 with a minimum precision constraint.

    For rare-event models (break), this prevents degenerate all-negative predictions
    by finding the best threshold below the default 0.5.
    """
    import numpy as np
    from sklearn.metrics import precision_recall_curve, f1_score as f1_fn

    if y_prob is None or len(set(y_true)) < 2:
        return 0.5

    precisions, recalls, thresholds = precision_recall_curve(y_true, y_prob)
    best_threshold = 0.5
    best_f1 = 0.0

    for i, thresh in enumerate(thresholds):
        if precisions[i] < min_precision:
            continue
        # Compute F1 at this threshold
        y_pred_t = (np.asarray(y_prob) >= thresh).astype(int)
        f1_val = float(f1_fn(y_true, y_pred_t, zero_division=0))
        if f1_val > best_f1:
            best_f1 = f1_val
            best_threshold = float(thresh)

    return best_threshold


def metrics_for_fold(y_true, y_prob, y_pred, optimal_threshold=None):
    from sklearn.metrics import (
        accuracy_score,
        precision_score,
        recall_score,
        f1_score,
        roc_auc_score,
        brier_score_loss,
        log_loss,
    )
    import numpy as np

    metrics = {}
    metrics["accuracy"] = float(accuracy_score(y_true, y_pred))
    metrics["precision"] = float(precision_score(y_true, y_pred, zero_division=0))
    metrics["recall"] = float(recall_score(y_true, y_pred, zero_division=0))
    metrics["f1"] = float(f1_score(y_true, y_pred, zero_division=0))

    if y_prob is not None and len(set(y_true)) == 2:
        metrics["roc_auc"] = float(roc_auc_score(y_true, y_prob))
        metrics["brier"] = float(brier_score_loss(y_true, y_prob))
        metrics["log_loss"] = float(log_loss(y_true, y_prob, labels=[0, 1]))

        # Compute optimal threshold and metrics at that threshold
        if optimal_threshold is None:
            optimal_threshold = find_optimal_threshold(y_true, y_prob)
        metrics["optimal_threshold"] = float(optimal_threshold)

        y_pred_opt = (np.asarray(y_prob) >= optimal_threshold).astype(int)
        metrics["opt_precision"] = float(precision_score(y_true, y_pred_opt, zero_division=0))
        metrics["opt_recall"] = float(recall_score(y_true, y_pred_opt, zero_division=0))
        metrics["opt_f1"] = float(f1_score(y_true, y_pred_opt, zero_division=0))
    else:
        metrics["roc_auc"] = None
        metrics["brier"] = None
        metrics["log_loss"] = None
        metrics["optimal_threshold"] = 0.5
        metrics["opt_precision"] = None
        metrics["opt_recall"] = None
        metrics["opt_f1"] = None
    return metrics


def map_feature_groups(feature_names, categorical_cols):
    mapping = []
    cat_sorted = sorted(categorical_cols, key=len, reverse=True)
    for name in feature_names:
        if name.startswith("num__"):
            base = name[len("num__") :]
        elif name.startswith("cat__"):
            rest = name[len("cat__") :]
            base = None
            for col in cat_sorted:
                if rest == col or rest.startswith(f"{col}_"):
                    base = col
                    break
            if base is None:
                base = rest
        else:
            base = name
        mapping.append(base)
    return mapping


def compute_shap_lite(X_test, y_prob, numeric_cols, categorical_cols):
    pd = require("pandas", "python3 -m pip install pandas")
    import numpy as np

    shap_info = {}
    if y_prob is None or len(y_prob) == 0:
        return shap_info

    y_prob_series = pd.Series(y_prob, index=X_test.index)
    overall_mean = float(np.mean(y_prob))

    for col in numeric_cols:
        if col not in X_test.columns:
            continue
        series = X_test[col]
        if series.notna().sum() < 3:
            continue
        try:
            corr = series.corr(y_prob_series, method="spearman")
        except Exception:
            corr = None
        if corr is None or not np.isfinite(corr):
            continue
        shap_info[col] = {
            "direction": "positive" if corr > 0 else "negative" if corr < 0 else "flat",
            "lift": float(corr),
            "detail": f"spearman={corr:.3f}",
        }

    for col in categorical_cols:
        if col not in X_test.columns:
            continue
        series = X_test[col].astype("object")
        if series.notna().sum() < 3:
            continue
        best_lift = None
        best_cat = None
        for cat in series.dropna().unique():
            mask = series == cat
            if mask.sum() == 0:
                continue
            mean_prob = float(y_prob_series[mask].mean())
            lift = mean_prob - overall_mean
            if best_lift is None or abs(lift) > abs(best_lift):
                best_lift = lift
                best_cat = cat
        if best_lift is None:
            continue
        direction = "positive" if best_lift > 0 else "negative" if best_lift < 0 else "flat"
        shap_info[col] = {
            "direction": direction,
            "lift": float(best_lift),
            "detail": f"{col}={best_cat} lift={best_lift:.4f}",
        }

    return shap_info


def feature_importance_report(pipeline, X_test, y_prob, numeric_cols, categorical_cols, fold_idx):
    import numpy as np

    prep = pipeline.named_steps["prep"]
    rf = pipeline.named_steps["rf"]
    feature_names = prep.get_feature_names_out()
    importances = rf.feature_importances_
    base_map = map_feature_groups(feature_names, categorical_cols)

    aggregated = {}
    for base, imp in zip(base_map, importances):
        aggregated[base] = aggregated.get(base, 0.0) + float(imp)

    shap_info = compute_shap_lite(X_test, y_prob, numeric_cols, categorical_cols)

    report_rows = []
    for feature, importance in aggregated.items():
        shap_entry = shap_info.get(feature, {})
        report_rows.append(
            {
                "fold": fold_idx,
                "feature": feature,
                "importance": float(importance),
                "shap_lite_lift": shap_entry.get("lift"),
                "shap_lite_direction": shap_entry.get("direction"),
                "shap_lite_detail": shap_entry.get("detail"),
            }
        )
    return report_rows


def calibration_bins(y_true, y_prob, n_bins, fold_idx):
    import numpy as np

    rows = []
    if y_prob is None or len(y_prob) == 0:
        return rows
    y_true = np.asarray(y_true)
    y_prob = np.asarray(y_prob)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    bin_ids = np.digitize(y_prob, bins, right=True) - 1
    bin_ids = np.clip(bin_ids, 0, n_bins - 1)

    for b in range(n_bins):
        mask = bin_ids == b
        count = int(mask.sum())
        if count == 0:
            continue
        mean_pred = float(np.mean(y_prob[mask]))
        frac_pos = float(np.mean(y_true[mask]))
        rows.append(
            {
                "fold": fold_idx,
                "bin": b + 1,
                "bin_lower": float(bins[b]),
                "bin_upper": float(bins[b + 1]),
                "count": count,
                "mean_pred": mean_pred,
                "frac_pos": frac_pos,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Train RF baseline with walk-forward calibration.")
    parser.add_argument("--db", default=DEFAULT_DUCKDB, help="DuckDB path")
    parser.add_argument("--view", default=DEFAULT_VIEW, help="DuckDB view/table name")
    parser.add_argument("--horizon-min", type=int, default=15)
    parser.add_argument("--target", default="reject", choices=["reject", "break"])
    parser.add_argument("--train-days", type=int, default=30)
    parser.add_argument("--calib-days", type=int, default=5)
    parser.add_argument("--test-days", type=int, default=5)
    parser.add_argument("--max-folds", type=int, default=12)
    parser.add_argument(
        "--split-mode", choices=["rolling", "strict"], default="rolling",
        help="rolling=test-day stride, strict=non-overlapping train+calib+test windows")
    parser.add_argument("--min-events", type=int, default=200)
    parser.add_argument("--calibration", choices=["auto", "isotonic", "sigmoid", "none"], default="auto")
    parser.add_argument("--n-estimators", type=int, default=300)
    parser.add_argument("--max-depth", type=int, default=12)
    parser.add_argument("--min-samples-leaf", type=int, default=5)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--feature-out", default=DEFAULT_FEATURE_OUT)
    parser.add_argument("--feature-csv", default=DEFAULT_FEATURE_CSV)
    parser.add_argument("--calib-out", default=DEFAULT_CALIB_OUT)
    parser.add_argument("--calib-csv", default=DEFAULT_CALIB_CSV)
    parser.add_argument("--calib-bins", type=int, default=10)
    args = parser.parse_args()

    df = load_dataframe(args.db, args.view, args.horizon_min)
    if df.empty:
        print("No training rows found. Check horizon_min and view contents.")
        return

    df = ensure_event_date(df)
    df = df.sort_values("ts_event")

    if args.target not in df.columns:
        raise ValueError(f"Target '{args.target}' missing from training view.")

    df = df[df[args.target].notna()].copy()
    if df.empty:
        print("No labeled rows for target.")
        return

    # Columns that are metadata/labels, never features
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
        # Raw OR prices (we keep the bps-normalized versions)
        "or_high",
        "or_low",
    }
    # Features explicitly marked for exclusion (raw prices, duplicates, dead)
    feature_drops = drop_features()
    drop_cols = label_cols | feature_drops

    feature_df = build_feature_dataframe(df)
    feature_df = feature_df.drop(columns=[c for c in drop_cols if c in feature_df.columns], errors="ignore")
    # Drop columns that are entirely null across the dataset.
    feature_df = feature_df.loc[:, feature_df.notna().any()]
    target = df[args.target].astype(int)

    import numpy as np

    pd = require("pandas", "python3 -m pip install pandas")
    categorical_cols = [
        c for c in feature_df.columns if not pd.api.types.is_numeric_dtype(feature_df[c])
    ]
    numeric_cols = [
        c for c in feature_df.columns if pd.api.types.is_numeric_dtype(feature_df[c])
    ]

    dates = sorted({d for d in df["event_date_et"].tolist() if d is not None})
    if len(dates) < args.train_days + args.calib_days + args.test_days:
        print(
            "Not enough unique dates for walk-forward splits. "
            "Try lowering --train-days/--test-days."
        )
        return

    stride_days = (
        args.test_days
        if args.split_mode == "rolling"
        else (args.train_days + args.calib_days + args.test_days)
    )
    splits = generate_splits(
        dates, args.train_days, args.calib_days, args.test_days, args.max_folds, stride_days=stride_days
    )
    if not splits:
        print("No splits generated. Check window sizes.")
        return

    results = []
    feature_rows = []
    calib_rows = []
    for fold_idx, (train_dates, calib_dates, test_dates) in enumerate(splits, start=1):
        train_mask = df["event_date_et"].isin(train_dates)
        calib_mask = df["event_date_et"].isin(calib_dates)
        test_mask = df["event_date_et"].isin(test_dates)

        X_train = feature_df[train_mask]
        y_train = target[train_mask]
        X_calib = feature_df[calib_mask]
        y_calib = target[calib_mask]
        X_test = feature_df[test_mask]
        y_test = target[test_mask]

        # Remove columns that are entirely missing in the training fold.
        usable_cols = X_train.columns[X_train.notna().any()].tolist()
        if not usable_cols:
            continue
        X_train = X_train[usable_cols]
        X_calib = X_calib[usable_cols]
        X_test = X_test[usable_cols]

        if len(X_train) < args.min_events or len(X_test) < 20:
            continue

        fold_numeric = [c for c in usable_cols if c in numeric_cols]
        fold_categorical = [c for c in usable_cols if c in categorical_cols]
        pipeline = build_pipeline(fold_numeric, fold_categorical, args)
        pipeline.fit(X_train, y_train)

        calibrated = None
        calib_method = None
        if args.calibration != "none" and len(X_calib) >= 20 and len(set(y_calib)) == 2:
            calib_method = choose_calibration(args.calibration, len(X_calib))
            calibrator = ProbabilityCalibrator(pipeline, calib_method)
            calibrator.fit(X_calib, y_calib)
            calibrated = calibrator

        model = calibrated if calibrated is not None else pipeline

        if hasattr(model, "predict_proba"):
            probs = model.predict_proba(X_test)
            if probs.shape[1] == 2:
                y_prob = probs[:, 1]
            else:
                y_prob = None
        else:
            y_prob = None

        y_pred = (y_prob >= 0.5).astype(int) if y_prob is not None else model.predict(X_test)

        fold_metrics = metrics_for_fold(y_test, y_prob, y_pred, optimal_threshold=None)
        fold_metrics.update(
            {
                "fold": fold_idx,
                "train_start": str(train_dates[0]),
                "train_end": str(train_dates[-1]),
                "calib_start": str(calib_dates[0]),
                "calib_end": str(calib_dates[-1]),
                "test_start": str(test_dates[0]),
                "test_end": str(test_dates[-1]),
                "train_events": int(len(X_train)),
                "calib_events": int(len(X_calib)),
                "test_events": int(len(X_test)),
                "calibration": calib_method or "none",
                "positive_rate": float(np.mean(y_test)),
            }
        )
        results.append(fold_metrics)

        feature_rows.extend(
            feature_importance_report(
                pipeline,
                X_test,
                y_prob,
                fold_numeric,
                fold_categorical,
                fold_idx,
            )
        )

        calib_rows.extend(calibration_bins(y_test, y_prob, args.calib_bins, fold_idx))

    if not results:
        print("No folds met minimum event thresholds.")
        return

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2)

    avg = {}
    for key in ["accuracy", "precision", "recall", "f1", "roc_auc", "brier", "log_loss"]:
        values = [r[key] for r in results if r[key] is not None]
        avg[key] = round(sum(values) / len(values), 4) if values else None

    if feature_rows:
        out_feature = Path(args.feature_out)
        out_feature.parent.mkdir(parents=True, exist_ok=True)
        with out_feature.open("w", encoding="utf-8") as handle:
            json.dump(feature_rows, handle, indent=2)

        out_csv = Path(args.feature_csv)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        import csv

        with out_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "fold",
                    "feature",
                    "importance",
                    "shap_lite_lift",
                    "shap_lite_direction",
                    "shap_lite_detail",
                ],
            )
            writer.writeheader()
            writer.writerows(feature_rows)

    if calib_rows:
        out_calib = Path(args.calib_out)
        out_calib.parent.mkdir(parents=True, exist_ok=True)
        with out_calib.open("w", encoding="utf-8") as handle:
            json.dump(calib_rows, handle, indent=2)

        out_calib_csv = Path(args.calib_csv)
        out_calib_csv.parent.mkdir(parents=True, exist_ok=True)
        import csv

        with out_calib_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "fold",
                    "bin",
                    "bin_lower",
                    "bin_upper",
                    "count",
                    "mean_pred",
                    "frac_pos",
                ],
            )
            writer.writeheader()
            writer.writerows(calib_rows)

    print("Walk-forward results")
    print(json.dumps(avg, indent=2))
    print(f"Saved fold metrics to {out_path}")
    if feature_rows:
        print(f"Saved feature report to {out_feature}")
        print(f"Saved feature report CSV to {out_csv}")
    if calib_rows:
        print(f"Saved calibration curve to {out_calib}")
        print(f"Saved calibration curve CSV to {out_calib_csv}")


if __name__ == "__main__":
    main()
