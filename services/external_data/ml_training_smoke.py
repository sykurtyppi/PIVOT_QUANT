"""Tiny non-optimized ML training smoke for model-ready datasets.

This validates pipeline mechanics only. It does not tune hyperparameters,
optimize thresholds, promote artifacts, or make performance claims.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from services.external_data.model_ready_dataset_export import FEATURE_DATA_COLUMNS, LABEL_COLUMNS


DEFAULT_TARGET_COLUMN = "forward_return_5d"
DEFAULT_TRAIN_FRACTION = 0.7
TINY_SAMPLE_WARNING_THRESHOLD = 100
LEAKY_FEATURE_PATTERNS = ("future_", "forward_", "label", "target", "outcome")


@dataclass(frozen=True)
class MLTrainingSmokeResult:
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def run_ml_training_smoke(
    *,
    dataset_path: str | Path,
    metadata_path: str | Path | None = None,
    target_column: str = DEFAULT_TARGET_COLUMN,
    train_fraction: float = DEFAULT_TRAIN_FRACTION,
) -> MLTrainingSmokeResult:
    dataset_file = Path(dataset_path).expanduser().resolve()
    metadata_file = Path(metadata_path).expanduser().resolve() if metadata_path else _default_metadata_path(dataset_file)
    dataset = _read_dataset(dataset_file)
    metadata = _read_metadata(metadata_file)
    return run_ml_training_smoke_on_frame(
        dataset=dataset,
        dataset_path=dataset_file,
        metadata_path=metadata_file if metadata_file.exists() else None,
        dataset_metadata=metadata,
        target_column=target_column,
        train_fraction=train_fraction,
    )


def run_ml_walk_forward_smoke(
    *,
    dataset_path: str | Path,
    metadata_path: str | Path | None = None,
    target_column: str = DEFAULT_TARGET_COLUMN,
    train_window_rows: int = 30,
    test_window_rows: int = 10,
    step_rows: int = 10,
) -> MLTrainingSmokeResult:
    dataset_file = Path(dataset_path).expanduser().resolve()
    metadata_file = Path(metadata_path).expanduser().resolve() if metadata_path else _default_metadata_path(dataset_file)
    dataset = _read_dataset(dataset_file)
    metadata = _read_metadata(metadata_file)
    return run_ml_walk_forward_smoke_on_frame(
        dataset=dataset,
        dataset_path=dataset_file,
        metadata_path=metadata_file if metadata_file.exists() else None,
        dataset_metadata=metadata,
        target_column=target_column,
        train_window_rows=train_window_rows,
        test_window_rows=test_window_rows,
        step_rows=step_rows,
    )


def run_ml_walk_forward_smoke_on_frame(
    *,
    dataset: pd.DataFrame,
    dataset_path: Path | None = None,
    metadata_path: Path | None = None,
    dataset_metadata: dict[str, Any] | None = None,
    target_column: str = DEFAULT_TARGET_COLUMN,
    train_window_rows: int = 30,
    test_window_rows: int = 10,
    step_rows: int = 10,
) -> MLTrainingSmokeResult:
    if target_column != DEFAULT_TARGET_COLUMN:
        raise ValueError("PR17 walk-forward smoke supports only forward_return_5d as the training target")
    if min(train_window_rows, test_window_rows, step_rows) <= 0:
        raise ValueError("train_window_rows, test_window_rows, and step_rows must be positive")

    warnings: list[str] = [
        "ML walk-forward smoke is pipeline validation only; sample is not statistically meaningful",
        "no hyperparameter tuning, threshold optimization, promotion, or performance claim is performed",
    ]
    frame = _prepare_frame(dataset)
    feature_columns = _feature_columns(frame)
    _validate_no_label_features(feature_columns)
    label_availability = _label_availability(frame)
    required_columns = ["entry_date", target_column, *feature_columns]
    missing_required = [column for column in required_columns if column not in frame.columns]
    if missing_required:
        report = _base_report(
            status="fail",
            dataset=frame,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
            dataset_metadata=dataset_metadata,
            target_column=target_column,
            feature_columns=feature_columns,
            warnings=[*warnings, f"missing required columns: {', '.join(missing_required)}"],
            label_availability=label_availability,
        )
        report["failure_reason"] = "missing_required_columns"
        return MLTrainingSmokeResult(report=report)

    working = frame.dropna(subset=[target_column, *feature_columns]).copy()
    dropped_missing = int(len(frame) - len(working))
    if dropped_missing:
        warnings.append(f"dropped {dropped_missing} rows with missing target or feature values")
    if len(working) < TINY_SAMPLE_WARNING_THRESHOLD:
        warnings.append(f"tiny sample warning: {len(working)} usable rows below {TINY_SAMPLE_WARNING_THRESHOLD}")

    windows = _walk_forward_windows(
        working,
        train_window_rows=train_window_rows,
        test_window_rows=test_window_rows,
        step_rows=step_rows,
    )
    window_reports = [
        _evaluate_walk_forward_window(
            window_id=index + 1,
            train=window["train"],
            test=window["test"],
            feature_columns=feature_columns,
            target_column=target_column,
        )
        for index, window in enumerate(windows)
    ]
    evaluable = [window for window in window_reports if window["status"] == "evaluable"]
    non_evaluable = [window for window in window_reports if window["status"] != "evaluable"]
    if non_evaluable:
        warnings.append(f"{len(non_evaluable)} walk-forward window(s) were non-evaluable")
    if not evaluable:
        status = "fail"
    else:
        status = "warn" if warnings or non_evaluable else "pass"

    report = _base_report(
        status=status,
        dataset=working,
        dataset_path=dataset_path,
        metadata_path=metadata_path,
        dataset_metadata=dataset_metadata,
        target_column=target_column,
        feature_columns=feature_columns,
        warnings=warnings,
        label_availability=label_availability,
    )
    report.update(
        {
            "name": "ml_walk_forward_smoke",
            "training_performed": bool(evaluable),
            "model": _model_metadata(),
            "walk_forward": {
                "method": "chronological_rolling_windows",
                "train_window_rows": int(train_window_rows),
                "test_window_rows": int(test_window_rows),
                "step_rows": int(step_rows),
                "window_count": int(len(window_reports)),
                "evaluable_window_count": int(len(evaluable)),
                "non_evaluable_window_count": int(len(non_evaluable)),
                "windows": window_reports,
            },
            "aggregate": _aggregate_windows(evaluable),
            "diagnostic_summary": _diagnostic_summary(evaluable),
            "rows_dropped_missing": dropped_missing,
            "leakage_checks": {
                "labels_excluded_from_features": True,
                "all_test_rows_strictly_after_train_rows": all(window["leakage_checks"]["test_strictly_after_train"] for window in window_reports),
                "no_shuffled_splits": True,
                "future_rows_not_used_as_features": True,
            },
        }
    )
    return MLTrainingSmokeResult(report=report)


def run_ml_training_smoke_on_frame(
    *,
    dataset: pd.DataFrame,
    dataset_path: Path | None = None,
    metadata_path: Path | None = None,
    dataset_metadata: dict[str, Any] | None = None,
    target_column: str = DEFAULT_TARGET_COLUMN,
    train_fraction: float = DEFAULT_TRAIN_FRACTION,
) -> MLTrainingSmokeResult:
    if target_column != DEFAULT_TARGET_COLUMN:
        raise ValueError("PR16 smoke supports only forward_return_5d as the training target")
    if not 0 < float(train_fraction) < 1:
        raise ValueError("train_fraction must be between 0 and 1")

    warnings: list[str] = [
        "ML smoke is pipeline validation only; sample is not statistically meaningful",
        "no hyperparameter tuning, threshold optimization, promotion, or performance claim is performed",
    ]
    frame = _prepare_frame(dataset)
    feature_columns = _feature_columns(frame)
    _validate_no_label_features(feature_columns)
    label_availability = _label_availability(frame)

    required_columns = ["entry_date", target_column, *feature_columns]
    missing_required = [column for column in required_columns if column not in frame.columns]
    if missing_required:
        report = _base_report(
            status="fail",
            dataset=frame,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
            dataset_metadata=dataset_metadata,
            target_column=target_column,
            feature_columns=feature_columns,
            warnings=[*warnings, f"missing required columns: {', '.join(missing_required)}"],
            label_availability=label_availability,
        )
        report["failure_reason"] = "missing_required_columns"
        return MLTrainingSmokeResult(report=report)

    working = frame.dropna(subset=[target_column, *feature_columns]).copy()
    dropped_missing = int(len(frame) - len(working))
    if dropped_missing:
        warnings.append(f"dropped {dropped_missing} rows with missing target or feature values")
    if len(working) < TINY_SAMPLE_WARNING_THRESHOLD:
        warnings.append(f"tiny sample warning: {len(working)} usable rows below {TINY_SAMPLE_WARNING_THRESHOLD}")
    if len(working) < 4:
        report = _base_report(
            status="fail",
            dataset=working,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
            dataset_metadata=dataset_metadata,
            target_column=target_column,
            feature_columns=feature_columns,
            warnings=warnings,
            label_availability=label_availability,
        )
        report["failure_reason"] = "too_few_usable_rows"
        report["rows_dropped_missing"] = dropped_missing
        return MLTrainingSmokeResult(report=report)

    split = _chronological_split(working, train_fraction=train_fraction)
    y_train = (split["train"][target_column] > 0).astype(int)
    y_test = (split["test"][target_column] > 0).astype(int)
    train_balance = _class_balance(y_train)
    test_balance = _class_balance(y_test)
    if y_train.nunique() < 2:
        warnings.append("training target has one class only; model training skipped")
        status = "fail"
        metrics: dict[str, Any] = {}
        naive_metrics: dict[str, Any] = {}
    else:
        model = _fixed_logistic_regression_pipeline()
        model.fit(split["train"][feature_columns], y_train)
        predictions = model.predict(split["test"][feature_columns])
        probabilities = model.predict_proba(split["test"][feature_columns])[:, 1]
        metrics = _classification_metrics(y_test, predictions, probabilities)
        majority_class = int(y_train.mode().iloc[0])
        naive_predictions = pd.Series([majority_class] * len(y_test), index=y_test.index)
        naive_metrics = _classification_metrics(y_test, naive_predictions, None)
        status = "warn" if warnings else "pass"

    report = _base_report(
        status=status,
        dataset=working,
        dataset_path=dataset_path,
        metadata_path=metadata_path,
        dataset_metadata=dataset_metadata,
        target_column=target_column,
        feature_columns=feature_columns,
        warnings=warnings,
        label_availability=label_availability,
    )
    report.update(
        {
            "model": {
                **_model_metadata(),
            },
            "split": {
                "method": "chronological",
                "train_fraction": float(train_fraction),
                "train_rows": int(len(split["train"])),
                "test_rows": int(len(split["test"])),
                "train_start": _date_min(split["train"]),
                "train_end": _date_max(split["train"]),
                "test_start": _date_min(split["test"]),
                "test_end": _date_max(split["test"]),
                "test_strictly_after_train": bool(pd.to_datetime(split["train"]["entry_date"]).max() < pd.to_datetime(split["test"]["entry_date"]).min()),
                "shuffled": False,
            },
            "rows_dropped_missing": dropped_missing,
            "class_balance": {
                "train": train_balance,
                "test": test_balance,
            },
            "metrics": metrics,
            "naive_baseline": {
                "strategy": "predict_train_majority_class",
                "metrics": naive_metrics,
            },
            "leakage_checks": {
                "labels_excluded_from_features": True,
                "test_rows_strictly_after_train_rows": bool(pd.to_datetime(split["train"]["entry_date"]).max() < pd.to_datetime(split["test"]["entry_date"]).min()),
                "no_shuffled_split": True,
                "future_rows_not_used_as_features": True,
            },
        }
    )
    return MLTrainingSmokeResult(report=report)


def write_ml_training_smoke_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "ml_smoke"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    stem = (
        f"{report['symbol'].lower()}_{report['analysis_start_date']}_{report['analysis_end_date']}_"
        f"{report['name']}_{report['target_name']}"
    ).replace("/", "-")
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _model_metadata() -> dict[str, Any]:
    return {
        "type": "LogisticRegression",
        "fixed_defaults": {
            "max_iter": 1000,
            "random_state": 0,
            "threshold": 0.5,
        },
        "hyperparameter_tuning_performed": False,
        "threshold_optimization_performed": False,
        "artifact_promotion_performed": False,
    }


def _fixed_logistic_regression_pipeline() -> Pipeline:
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("classifier", LogisticRegression(max_iter=1000, random_state=0)),
        ]
    )


def _walk_forward_windows(
    frame: pd.DataFrame,
    *,
    train_window_rows: int,
    test_window_rows: int,
    step_rows: int,
) -> list[dict[str, pd.DataFrame]]:
    windows: list[dict[str, pd.DataFrame]] = []
    start = 0
    while start + train_window_rows < len(frame):
        train_end = start + train_window_rows
        test_end = min(train_end + test_window_rows, len(frame))
        if test_end <= train_end:
            break
        windows.append(
            {
                "train": frame.iloc[start:train_end].copy(),
                "test": frame.iloc[train_end:test_end].copy(),
            }
        )
        start += step_rows
    return windows


def _evaluate_walk_forward_window(
    *,
    window_id: int,
    train: pd.DataFrame,
    test: pd.DataFrame,
    feature_columns: list[str],
    target_column: str,
) -> dict[str, Any]:
    warnings: list[str] = []
    y_train = (train[target_column] > 0).astype(int)
    y_test = (test[target_column] > 0).astype(int)
    train_balance = _class_balance(y_train)
    test_balance = _class_balance(y_test)
    leakage = {
        "test_strictly_after_train": bool(pd.to_datetime(train["entry_date"]).max() < pd.to_datetime(test["entry_date"]).min())
        if not train.empty and not test.empty
        else False,
        "shuffled": False,
        "labels_excluded_from_features": True,
    }
    base = {
        "window_id": int(window_id),
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "train_start": _date_min(train),
        "train_end": _date_max(train),
        "test_start": _date_min(test),
        "test_end": _date_max(test),
        "class_balance": {
            "train": train_balance,
            "test": test_balance,
        },
        "leakage_checks": leakage,
        "warnings": warnings,
    }
    if train.empty or test.empty:
        warnings.append("empty train or test window")
        return {**base, "status": "non_evaluable", "reason": "empty_window", "metrics": {}, "naive_baseline": {}, "confusion_matrix": {}, "diagnostics": {}}
    if y_train.nunique() < 2:
        warnings.append("training target has one class only")
        return {**base, "status": "non_evaluable", "reason": "single_class_train", "metrics": {}, "naive_baseline": {}, "confusion_matrix": {}, "diagnostics": {}}
    if not leakage["test_strictly_after_train"]:
        warnings.append("test rows are not strictly after train rows")
        return {**base, "status": "non_evaluable", "reason": "invalid_chronology", "metrics": {}, "naive_baseline": {}, "confusion_matrix": {}, "diagnostics": {}}

    model = _fixed_logistic_regression_pipeline()
    model.fit(train[feature_columns], y_train)
    predictions = model.predict(test[feature_columns])
    probabilities = model.predict_proba(test[feature_columns])[:, 1]
    majority_class = int(y_train.mode().iloc[0])
    naive_predictions = pd.Series([majority_class] * len(y_test), index=y_test.index)
    metrics = _classification_metrics(y_test, predictions, probabilities)
    naive_metrics = _classification_metrics(y_test, naive_predictions, None)
    diagnostics = _window_diagnostics(
        train=train,
        test=test,
        y_train=y_train,
        y_test=y_test,
        predictions=pd.Series(predictions, index=y_test.index),
        probabilities=pd.Series(probabilities, index=y_test.index),
        naive_class=majority_class,
        feature_columns=feature_columns,
        model=model,
        metrics=metrics,
        naive_metrics=naive_metrics,
    )
    return {
        **base,
        "status": "evaluable",
        "metrics": metrics,
        "naive_baseline": {
            "strategy": "predict_train_majority_class",
            "class_choice": majority_class,
            "metrics": naive_metrics,
        },
        "confusion_matrix": diagnostics["confusion_matrix"],
        "diagnostics": diagnostics,
    }


def _aggregate_windows(windows: list[dict[str, Any]]) -> dict[str, Any]:
    metric_keys = ["accuracy", "precision", "recall", "auc"]
    averages: dict[str, float | None] = {}
    naive_averages: dict[str, float | None] = {}
    for key in metric_keys:
        values = [
            window.get("metrics", {}).get(key)
            for window in windows
            if window.get("metrics", {}).get(key) is not None
        ]
        averages[key] = float(sum(values) / len(values)) if values else None
        naive_values = [
            window.get("naive_baseline", {}).get("metrics", {}).get(key)
            for window in windows
            if window.get("naive_baseline", {}).get("metrics", {}).get(key) is not None
        ]
        naive_averages[key] = float(sum(naive_values) / len(naive_values)) if naive_values else None
    return {
        "average_metrics": averages,
        "average_naive_baseline_metrics": naive_averages,
        "window_count": int(len(windows)),
        "note": "averages are smoke diagnostics only, not performance evidence",
    }


def _window_diagnostics(
    *,
    train: pd.DataFrame,
    test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
    predictions: pd.Series,
    probabilities: pd.Series,
    naive_class: int,
    feature_columns: list[str],
    model: Pipeline,
    metrics: dict[str, Any],
    naive_metrics: dict[str, Any],
) -> dict[str, Any]:
    coefficients = _coefficient_diagnostics(model, feature_columns)
    prediction_distribution = _binary_distribution(predictions)
    probability_distribution = _numeric_distribution(probabilities)
    feature_variance = {
        "train": _feature_variance(train, feature_columns),
        "test": _feature_variance(test, feature_columns),
    }
    low_variance_train = _low_variance_features(feature_variance["train"])
    low_variance_test = _low_variance_features(feature_variance["test"])
    return {
        "class_imbalance": {
            "train": _class_balance(y_train),
            "test": _class_balance(y_test),
            "train_majority_class": naive_class,
            "train_majority_rate": _majority_rate(y_train),
            "test_majority_rate": _majority_rate(y_test),
        },
        "prediction_distribution": prediction_distribution,
        "predicted_mostly_one_class": bool(prediction_distribution["majority_rate"] is not None and prediction_distribution["majority_rate"] >= 0.8),
        "probability_distribution": probability_distribution,
        "feature_null_rates": {
            "train": _feature_null_rates(train, feature_columns),
            "test": _feature_null_rates(test, feature_columns),
        },
        "feature_variance": {
            **feature_variance,
            "low_variance_features_train": low_variance_train,
            "low_variance_features_test": low_variance_test,
        },
        "coefficients": coefficients,
        "naive_baseline_class_choice": naive_class,
        "confusion_matrix": _confusion_matrix(y_test, predictions),
        "model_vs_naive": {
            "accuracy_delta": _metric_delta(metrics, naive_metrics, "accuracy"),
            "precision_delta": _metric_delta(metrics, naive_metrics, "precision"),
            "recall_delta": _metric_delta(metrics, naive_metrics, "recall"),
            "auc_delta": _metric_delta(metrics, naive_metrics, "auc"),
        },
    }


def _diagnostic_summary(windows: list[dict[str, Any]]) -> dict[str, Any]:
    if not windows:
        return {
            "why_model_underperformed": ["no evaluable windows available"],
            "model_underperformed_naive_accuracy": None,
            "predicted_mostly_one_class_window_count": 0,
            "class_imbalance_dominated": None,
            "low_variance_feature_count": 0,
            "coefficient_direction_stability": {},
        }

    aggregate = _aggregate_windows(windows)
    model_accuracy = aggregate["average_metrics"].get("accuracy")
    naive_accuracy = aggregate["average_naive_baseline_metrics"].get("accuracy")
    underperformed = (
        model_accuracy is not None
        and naive_accuracy is not None
        and model_accuracy < naive_accuracy
    )
    predicted_mostly_one_class = [
        window
        for window in windows
        if window.get("diagnostics", {}).get("predicted_mostly_one_class")
    ]
    train_majority_rates = [
        window.get("diagnostics", {}).get("class_imbalance", {}).get("train_majority_rate")
        for window in windows
        if window.get("diagnostics", {}).get("class_imbalance", {}).get("train_majority_rate") is not None
    ]
    average_train_majority_rate = float(sum(train_majority_rates) / len(train_majority_rates)) if train_majority_rates else None
    low_variance_features = sorted(
        {
            feature
            for window in windows
            for feature in window.get("diagnostics", {}).get("feature_variance", {}).get("low_variance_features_train", [])
        }
    )
    coefficient_stability = _coefficient_direction_stability(windows)
    reasons: list[str] = []
    if underperformed:
        reasons.append("average accuracy was below the naive train-majority baseline")
    if predicted_mostly_one_class:
        reasons.append(f"model predicted mostly one class in {len(predicted_mostly_one_class)} window(s)")
    if average_train_majority_rate is not None and average_train_majority_rate >= 0.7:
        reasons.append("training windows were materially class-imbalanced, making the naive majority baseline hard to beat")
    if low_variance_features:
        reasons.append(f"{len(low_variance_features)} feature(s) had near-zero train variance in at least one window")
    if not reasons:
        reasons.append("no single dominant diagnostic cause identified; inspect per-window metrics and coefficients")
    return {
        "why_model_underperformed": reasons,
        "model_underperformed_naive_accuracy": bool(underperformed),
        "average_model_accuracy": model_accuracy,
        "average_naive_accuracy": naive_accuracy,
        "predicted_mostly_one_class_window_count": int(len(predicted_mostly_one_class)),
        "average_train_majority_rate": average_train_majority_rate,
        "class_imbalance_dominated": bool(average_train_majority_rate is not None and average_train_majority_rate >= 0.7),
        "low_variance_feature_count": int(len(low_variance_features)),
        "low_variance_features": low_variance_features,
        "coefficient_direction_stability": coefficient_stability,
        "note": "diagnostics explain fixed-smoke behavior only; they are not tuning guidance or edge evidence",
    }


def _binary_distribution(values: pd.Series) -> dict[str, Any]:
    counts = values.astype(int).value_counts().sort_index().to_dict()
    total = int(len(values))
    negative = int(counts.get(0, 0))
    positive = int(counts.get(1, 0))
    majority_count = max(negative, positive) if total else 0
    majority_class = 1 if positive >= negative else 0
    return {
        "rows": total,
        "negative": negative,
        "positive": positive,
        "positive_rate": float(positive / total) if total else None,
        "majority_class": int(majority_class) if total else None,
        "majority_rate": float(majority_count / total) if total else None,
    }


def _numeric_distribution(values: pd.Series) -> dict[str, Any]:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return {
            "rows": 0,
            "min": None,
            "p25": None,
            "median": None,
            "mean": None,
            "p75": None,
            "max": None,
        }
    return {
        "rows": int(len(numeric)),
        "min": float(numeric.min()),
        "p25": float(numeric.quantile(0.25)),
        "median": float(numeric.median()),
        "mean": float(numeric.mean()),
        "p75": float(numeric.quantile(0.75)),
        "max": float(numeric.max()),
    }


def _feature_null_rates(frame: pd.DataFrame, feature_columns: list[str]) -> dict[str, float | None]:
    if frame.empty:
        return {column: None for column in feature_columns}
    return {
        column: float(pd.to_numeric(frame[column], errors="coerce").isna().mean())
        for column in feature_columns
    }


def _feature_variance(frame: pd.DataFrame, feature_columns: list[str]) -> dict[str, float | None]:
    if frame.empty:
        return {column: None for column in feature_columns}
    variances: dict[str, float | None] = {}
    for column in feature_columns:
        numeric = pd.to_numeric(frame[column], errors="coerce").dropna()
        variances[column] = float(numeric.var()) if len(numeric) > 1 else None
    return variances


def _low_variance_features(variances: dict[str, float | None], *, threshold: float = 1e-12) -> list[str]:
    return sorted(
        column
        for column, variance in variances.items()
        if variance is not None and abs(float(variance)) <= threshold
    )


def _coefficient_diagnostics(model: Pipeline, feature_columns: list[str]) -> dict[str, Any]:
    classifier = model.named_steps["classifier"]
    raw_coefficients = classifier.coef_[0].tolist()
    by_feature = {
        feature: float(coefficient)
        for feature, coefficient in zip(feature_columns, raw_coefficients, strict=False)
    }
    sorted_features = sorted(by_feature.items(), key=lambda item: item[1])
    return {
        "intercept": float(classifier.intercept_[0]),
        "by_feature": by_feature,
        "top_positive": [
            {"feature": feature, "coefficient": coefficient}
            for feature, coefficient in reversed(sorted_features[-5:])
        ],
        "top_negative": [
            {"feature": feature, "coefficient": coefficient}
            for feature, coefficient in sorted_features[:5]
        ],
        "directions": {
            feature: _coefficient_direction(coefficient)
            for feature, coefficient in by_feature.items()
        },
    }


def _coefficient_direction(coefficient: float, *, epsilon: float = 1e-12) -> str:
    if coefficient > epsilon:
        return "positive"
    if coefficient < -epsilon:
        return "negative"
    return "zero"


def _coefficient_direction_stability(windows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    counts: dict[str, dict[str, int]] = {}
    for window in windows:
        directions = window.get("diagnostics", {}).get("coefficients", {}).get("directions", {})
        for feature, direction in directions.items():
            feature_counts = counts.setdefault(feature, {"positive": 0, "negative": 0, "zero": 0})
            feature_counts[direction] = feature_counts.get(direction, 0) + 1

    stability: dict[str, dict[str, Any]] = {}
    for feature, feature_counts in counts.items():
        total = sum(feature_counts.values())
        dominant_direction = max(feature_counts, key=feature_counts.get) if total else None
        stability[feature] = {
            "positive": int(feature_counts.get("positive", 0)),
            "negative": int(feature_counts.get("negative", 0)),
            "zero": int(feature_counts.get("zero", 0)),
            "dominant_direction": dominant_direction,
            "stability_rate": float(feature_counts.get(dominant_direction, 0) / total) if total and dominant_direction else None,
        }
    return dict(sorted(stability.items()))


def _confusion_matrix(y_true: pd.Series, y_pred: pd.Series) -> dict[str, int]:
    truth = y_true.astype(int)
    predicted = y_pred.astype(int)
    return {
        "true_positive": int(((truth == 1) & (predicted == 1)).sum()),
        "true_negative": int(((truth == 0) & (predicted == 0)).sum()),
        "false_positive": int(((truth == 0) & (predicted == 1)).sum()),
        "false_negative": int(((truth == 1) & (predicted == 0)).sum()),
    }


def _metric_delta(metrics: dict[str, Any], baseline: dict[str, Any], key: str) -> float | None:
    left = metrics.get(key)
    right = baseline.get(key)
    if left is None or right is None:
        return None
    return float(left) - float(right)


def _majority_rate(values: pd.Series) -> float | None:
    if values.empty:
        return None
    counts = values.astype(int).value_counts()
    return float(counts.max() / len(values))


def _base_report(
    *,
    status: str,
    dataset: pd.DataFrame,
    dataset_path: Path | None,
    metadata_path: Path | None,
    dataset_metadata: dict[str, Any] | None,
    target_column: str,
    feature_columns: list[str],
    warnings: list[str],
    label_availability: dict[str, int],
) -> dict[str, Any]:
    metadata = dataset_metadata or {}
    return {
        "name": "ml_training_smoke",
        "status": status,
        "symbol": metadata.get("symbol", _infer_symbol(dataset)),
        "analysis_start_date": metadata.get("analysis_start_date", _date_min(dataset)),
        "analysis_end_date": metadata.get("analysis_end_date", _date_max(dataset)),
        "read_only": True,
        "training_performed": status != "fail",
        "performance_claim": False,
        "target_name": "forward_return_5d_positive",
        "target_column": target_column,
        "target_definition": "forward_return_5d > 0",
        "feature_columns": list(feature_columns),
        "label_columns": list(LABEL_COLUMNS),
        "rows": {
            "input": int(len(dataset)),
        },
        "label_availability": label_availability,
        "dataset": {
            "path": str(dataset_path) if dataset_path else None,
            "sha256": _sha256(dataset_path) if dataset_path and dataset_path.exists() else None,
            "metadata_path": str(metadata_path) if metadata_path else None,
            "metadata_sha256": _sha256(metadata_path) if metadata_path and metadata_path.exists() else None,
            "metadata_status": metadata.get("status"),
        },
        "warnings": _dedupe(warnings),
    }


def _read_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"dataset not found: {path}")
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _read_metadata(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _default_metadata_path(dataset_path: Path) -> Path:
    stem = dataset_path.with_suffix("")
    return stem.with_name(stem.name + ".metadata.json")


def _prepare_frame(dataset: pd.DataFrame) -> pd.DataFrame:
    frame = dataset.copy()
    if "entry_date" in frame.columns:
        frame["entry_date"] = pd.to_datetime(frame["entry_date"], errors="coerce")
        frame = frame.dropna(subset=["entry_date"]).sort_values("entry_date").reset_index(drop=True)
    return frame


def _feature_columns(frame: pd.DataFrame) -> list[str]:
    return [column for column in FEATURE_DATA_COLUMNS if column in frame.columns]


def _validate_no_label_features(feature_columns: list[str]) -> None:
    leaky = []
    for column in feature_columns:
        lowered = column.lower()
        if column in LABEL_COLUMNS or any(pattern in lowered for pattern in LEAKY_FEATURE_PATTERNS):
            leaky.append(column)
    if leaky:
        raise ValueError(f"label/outcome columns cannot be used as ML smoke features: {', '.join(sorted(leaky))}")


def _chronological_split(frame: pd.DataFrame, *, train_fraction: float) -> dict[str, pd.DataFrame]:
    split_index = int(len(frame) * float(train_fraction))
    split_index = min(max(split_index, 1), len(frame) - 1)
    train = frame.iloc[:split_index].copy()
    test = frame.iloc[split_index:].copy()
    if not pd.to_datetime(train["entry_date"]).max() < pd.to_datetime(test["entry_date"]).min():
        raise ValueError("chronological split invalid: test rows must be strictly after train rows")
    return {"train": train, "test": test}


def _classification_metrics(y_true: pd.Series, y_pred: pd.Series, y_score: pd.Series | None) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
    }
    if y_score is not None and y_true.nunique() > 1:
        metrics["auc"] = float(roc_auc_score(y_true, y_score))
    else:
        metrics["auc"] = None
    return metrics


def _class_balance(values: pd.Series) -> dict[str, Any]:
    counts = values.value_counts().sort_index().to_dict()
    total = int(len(values))
    return {
        "rows": total,
        "negative": int(counts.get(0, 0)),
        "positive": int(counts.get(1, 0)),
        "positive_rate": float(counts.get(1, 0) / total) if total else None,
    }


def _label_availability(frame: pd.DataFrame) -> dict[str, int]:
    return {
        column: int(frame[column].notna().sum()) if column in frame.columns else 0
        for column in LABEL_COLUMNS
    }


def _date_min(frame: pd.DataFrame) -> str | None:
    if frame.empty or "entry_date" not in frame.columns:
        return None
    dates = pd.to_datetime(frame["entry_date"], errors="coerce").dropna()
    return dates.min().date().isoformat() if not dates.empty else None


def _date_max(frame: pd.DataFrame) -> str | None:
    if frame.empty or "entry_date" not in frame.columns:
        return None
    dates = pd.to_datetime(frame["entry_date"], errors="coerce").dropna()
    return dates.max().date().isoformat() if not dates.empty else None


def _infer_symbol(frame: pd.DataFrame) -> str | None:
    if frame.empty or "symbol" not in frame.columns:
        return None
    values = frame["symbol"].dropna().astype(str).unique().tolist()
    return values[0] if values else None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
