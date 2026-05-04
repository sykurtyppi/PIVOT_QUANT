"""Signal diagnostics for bounded model-ready datasets.

This module answers whether the current feature set appears related to the
target. It does not train models, tune thresholds, optimize hyperparameters, or
make performance claims.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.ml_training_smoke import DEFAULT_TARGET_COLUMN
from services.external_data.model_ready_dataset_export import FEATURE_DATA_COLUMNS, LABEL_COLUMNS


DEFAULT_BUCKET_COUNT = 5
DEFAULT_STABILITY_WINDOW_ROWS = 50
DEFAULT_ROLLING_WINDOW_ROWS = 20
LEAKY_FEATURE_PATTERNS = ("future_", "forward_", "label", "target", "outcome")


@dataclass(frozen=True)
class MLSignalDiagnosticsResult:
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def run_ml_signal_diagnostics(
    *,
    dataset_path: str | Path,
    metadata_path: str | Path | None = None,
    model_diagnostics_path: str | Path | None = None,
    target_column: str = DEFAULT_TARGET_COLUMN,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    stability_window_rows: int = DEFAULT_STABILITY_WINDOW_ROWS,
    rolling_window_rows: int = DEFAULT_ROLLING_WINDOW_ROWS,
) -> MLSignalDiagnosticsResult:
    dataset_file = Path(dataset_path).expanduser().resolve()
    metadata_file = Path(metadata_path).expanduser().resolve() if metadata_path else _default_metadata_path(dataset_file)
    model_file = Path(model_diagnostics_path).expanduser().resolve() if model_diagnostics_path else _default_model_diagnostics_path(dataset_file)
    dataset = _read_dataset(dataset_file)
    metadata = _read_metadata(metadata_file)
    model_diagnostics = _read_metadata(model_file)
    return run_ml_signal_diagnostics_on_frame(
        dataset=dataset,
        dataset_path=dataset_file,
        metadata_path=metadata_file if metadata_file.exists() else None,
        model_diagnostics_path=model_file if model_file.exists() else None,
        dataset_metadata=metadata,
        model_diagnostics=model_diagnostics,
        target_column=target_column,
        bucket_count=bucket_count,
        stability_window_rows=stability_window_rows,
        rolling_window_rows=rolling_window_rows,
    )


def run_ml_signal_diagnostics_on_frame(
    *,
    dataset: pd.DataFrame,
    dataset_path: Path | None = None,
    metadata_path: Path | None = None,
    model_diagnostics_path: Path | None = None,
    dataset_metadata: dict[str, Any] | None = None,
    model_diagnostics: dict[str, Any] | None = None,
    target_column: str = DEFAULT_TARGET_COLUMN,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    stability_window_rows: int = DEFAULT_STABILITY_WINDOW_ROWS,
    rolling_window_rows: int = DEFAULT_ROLLING_WINDOW_ROWS,
) -> MLSignalDiagnosticsResult:
    if target_column != DEFAULT_TARGET_COLUMN:
        raise ValueError("signal diagnostics currently support only forward_return_5d")
    if min(bucket_count, stability_window_rows, rolling_window_rows) <= 0:
        raise ValueError("bucket_count, stability_window_rows, and rolling_window_rows must be positive")

    warnings = [
        "signal diagnostics are inspection-only; no threshold tuning, hyperparameter tuning, model selection, or edge claim is performed",
    ]
    metadata = dataset_metadata or {}
    model_report = model_diagnostics or {}
    frame = _prepare_frame(dataset)
    feature_columns = _feature_columns(frame)
    _validate_no_label_features(feature_columns)

    required = ["entry_date", target_column]
    missing_required = [column for column in required if column not in frame.columns]
    if missing_required:
        report = _base_report(
            status="fail",
            frame=frame,
            metadata=metadata,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
            model_diagnostics_path=model_diagnostics_path,
            target_column=target_column,
            feature_columns=feature_columns,
            warnings=[*warnings, f"missing required columns: {', '.join(missing_required)}"],
        )
        report["failure_reason"] = "missing_required_columns"
        return MLSignalDiagnosticsResult(report=report)

    working = frame.dropna(subset=[target_column]).copy()
    if working.empty:
        report = _base_report(
            status="fail",
            frame=working,
            metadata=metadata,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
            model_diagnostics_path=model_diagnostics_path,
            target_column=target_column,
            feature_columns=feature_columns,
            warnings=[*warnings, "no rows with target values available"],
        )
        report["failure_reason"] = "no_target_rows"
        return MLSignalDiagnosticsResult(report=report)

    target_positive = (pd.to_numeric(working[target_column], errors="coerce") > 0).astype("Int64")
    target_distribution = _target_distribution(
        working,
        target_column=target_column,
        target_positive=target_positive,
        rolling_window_rows=rolling_window_rows,
    )
    lag1_autocorr = target_distribution["autocorrelation"].get("forward_return_5d_lag1")
    if lag1_autocorr is not None and abs(lag1_autocorr) >= 0.5:
        warnings.append(
            "forward_return_5d has high lag-1 autocorrelation; overlapping 5-day labels can mechanically increase serial correlation"
        )
    feature_signal_table, feature_bucket_tables, feature_stability_flags = _feature_diagnostics(
        working,
        feature_columns=feature_columns,
        target_column=target_column,
        target_positive=target_positive,
        bucket_count=bucket_count,
        stability_window_rows=stability_window_rows,
    )
    signal_strength_summary = _signal_strength_summary(feature_signal_table, feature_stability_flags)
    model_collapse = _model_collapse_diagnosis(model_report, signal_strength_summary)
    status = "warn" if warnings else "pass"

    report = _base_report(
        status=status,
        frame=working,
        metadata=metadata,
        dataset_path=dataset_path,
        metadata_path=metadata_path,
        model_diagnostics_path=model_diagnostics_path,
        target_column=target_column,
        feature_columns=feature_columns,
        warnings=warnings,
    )
    report.update(
        {
            "target_distribution": target_distribution,
            "feature_signal_table": feature_signal_table,
            "feature_bucket_tables": feature_bucket_tables,
            "feature_stability_flags": feature_stability_flags,
            "signal_strength_summary": signal_strength_summary,
            "model_collapse_diagnosis": model_collapse,
            "leakage_checks": {
                "labels_excluded_from_features": True,
                "no_label_columns_used": True,
                "no_training_performed": True,
                "no_threshold_optimization": True,
                "no_hyperparameter_tuning": True,
            },
            "explicit_warning": "no edge claim",
        }
    )
    return MLSignalDiagnosticsResult(report=report)


def write_ml_signal_diagnostics_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "ml_diagnostics"
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


def _target_distribution(
    frame: pd.DataFrame,
    *,
    target_column: str,
    target_positive: pd.Series,
    rolling_window_rows: int,
) -> dict[str, Any]:
    target_values = pd.to_numeric(frame[target_column], errors="coerce")
    by_month = frame[["entry_date"]].copy()
    by_month["month"] = pd.to_datetime(by_month["entry_date"], errors="coerce").dt.to_period("M").astype("string")
    by_month["target_positive"] = target_positive.astype(float)
    monthly = {
        str(month): {
            "rows": int(len(group)),
            "positive_rate": _safe_float(group["target_positive"].mean()),
        }
        for month, group in by_month.groupby("month", dropna=True)
    }
    rolling = []
    for start in range(0, len(frame), rolling_window_rows):
        end = min(start + rolling_window_rows, len(frame))
        chunk = frame.iloc[start:end]
        chunk_target = target_positive.iloc[start:end].astype(float)
        if chunk.empty:
            continue
        rolling.append(
            {
                "window_id": int(len(rolling) + 1),
                "start": _date_min(chunk),
                "end": _date_max(chunk),
                "rows": int(len(chunk)),
                "positive_rate": _safe_float(chunk_target.mean()),
            }
        )
    return {
        "overall": {
            "rows": int(len(frame)),
            "positive": int(target_positive.sum()),
            "negative": int((target_positive == 0).sum()),
            "positive_rate": _safe_float(target_positive.astype(float).mean()),
        },
        "monthly_positive_rate": dict(sorted(monthly.items())),
        "rolling_positive_rate": rolling,
        "autocorrelation": {
            "forward_return_5d_lag1": _safe_float(target_values.autocorr(lag=1)),
        },
        "distribution": {
            "mean": _safe_float(target_values.mean()),
            "std": _safe_float(target_values.std()),
            "skew": _safe_float(target_values.skew()),
            "min": _safe_float(target_values.min()),
            "median": _safe_float(target_values.median()),
            "max": _safe_float(target_values.max()),
        },
        "regime_segmentation": _regime_segmentation(frame, target_column=target_column, target_positive=target_positive),
    }


def _feature_diagnostics(
    frame: pd.DataFrame,
    *,
    feature_columns: list[str],
    target_column: str,
    target_positive: pd.Series,
    bucket_count: int,
    stability_window_rows: int,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    signal_rows: list[dict[str, Any]] = []
    bucket_tables: dict[str, list[dict[str, Any]]] = {}
    stability: dict[str, dict[str, Any]] = {}
    target_values = pd.to_numeric(frame[target_column], errors="coerce")
    for feature in feature_columns:
        series = pd.to_numeric(frame[feature], errors="coerce")
        buckets = _feature_bucket_table(
            feature_values=series,
            target_values=target_values,
            target_positive=target_positive,
            bucket_count=bucket_count,
        )
        bucket_tables[feature] = buckets
        bucket_separation = _bucket_separation(buckets, value_key="positive_rate")
        return_separation = _bucket_separation(buckets, value_key="mean_forward_return_5d")
        pearson = _correlation(series, target_positive.astype(float), method="pearson")
        spearman = _correlation(series, target_positive.astype(float), method="spearman")
        stability_flag = _feature_stability(
            frame,
            feature=feature,
            target_column=target_column,
            stability_window_rows=stability_window_rows,
            bucket_count=bucket_count,
        )
        stability[feature] = stability_flag
        signal_rows.append(
            {
                "feature": feature,
                "rows": int(series.notna().sum()),
                "missing_rate": _safe_float(series.isna().mean()),
                "variance": _safe_float(series.var()),
                "constant": bool(series.dropna().nunique() <= 1),
                "pearson_corr_target_positive": pearson,
                "spearman_corr_target_positive": spearman,
                "bucket_positive_rate_separation": bucket_separation,
                "bucket_mean_return_separation": return_separation,
                "stability": {
                    "relationship_stable": stability_flag["relationship_stable"],
                    "sign_flips": stability_flag["sign_flips"],
                    "window_count": stability_flag["window_count"],
                },
            }
        )
    signal_rows.sort(
        key=lambda row: max(
            abs(row["pearson_corr_target_positive"] or 0.0),
            abs(row["spearman_corr_target_positive"] or 0.0),
            abs(row["bucket_positive_rate_separation"] or 0.0),
        ),
        reverse=True,
    )
    return signal_rows, bucket_tables, stability


def _feature_bucket_table(
    *,
    feature_values: pd.Series,
    target_values: pd.Series,
    target_positive: pd.Series,
    bucket_count: int,
) -> list[dict[str, Any]]:
    data = pd.DataFrame(
        {
            "feature": pd.to_numeric(feature_values, errors="coerce"),
            "target_value": pd.to_numeric(target_values, errors="coerce"),
            "target_positive": target_positive.astype(float),
        }
    ).dropna(subset=["feature", "target_value", "target_positive"])
    if data.empty:
        return []
    unique_count = int(data["feature"].nunique())
    if unique_count <= 1:
        data["bucket_id"] = 0
        data["bucket"] = "constant"
    else:
        q = min(bucket_count, unique_count)
        data["bucket_id"] = pd.qcut(data["feature"], q=q, labels=False, duplicates="drop")
        data["bucket"] = data["bucket_id"].astype("Int64").astype("string")
    rows = []
    grouped = data.groupby("bucket_id", dropna=True, sort=True)
    for index, (bucket_id, group) in enumerate(grouped, start=1):
        rows.append(
            {
                "bucket": "constant" if unique_count <= 1 else str(int(bucket_id)),
                "bucket_index": int(index),
                "rows": int(len(group)),
                "feature_min": _safe_float(group["feature"].min()),
                "feature_max": _safe_float(group["feature"].max()),
                "p_target_positive": _safe_float(group["target_positive"].mean()),
                "positive_rate": _safe_float(group["target_positive"].mean()),
                "mean_forward_return_5d": _safe_float(group["target_value"].mean()),
            }
        )
    return rows


def _feature_stability(
    frame: pd.DataFrame,
    *,
    feature: str,
    target_column: str,
    stability_window_rows: int,
    bucket_count: int,
) -> dict[str, Any]:
    signs: list[str] = []
    windows: list[dict[str, Any]] = []
    for start in range(0, len(frame), stability_window_rows):
        end = min(start + stability_window_rows, len(frame))
        chunk = frame.iloc[start:end]
        if len(chunk) < max(10, bucket_count * 2):
            continue
        target_values = pd.to_numeric(chunk[target_column], errors="coerce")
        target_positive = (target_values > 0).astype("Int64")
        buckets = _feature_bucket_table(
            feature_values=chunk[feature],
            target_values=target_values,
            target_positive=target_positive,
            bucket_count=bucket_count,
        )
        separation = _bucket_separation(buckets, value_key="positive_rate")
        sign = _relationship_sign(separation)
        if sign != "flat":
            signs.append(sign)
        windows.append(
            {
                "window_id": int(len(windows) + 1),
                "start": _date_min(chunk),
                "end": _date_max(chunk),
                "rows": int(len(chunk)),
                "bucket_positive_rate_separation": separation,
                "sign": sign,
            }
        )
    non_flat = [sign for sign in signs if sign != "flat"]
    unique_signs = sorted(set(non_flat))
    return {
        "window_count": int(len(windows)),
        "windows": windows,
        "signs": signs,
        "sign_flips": bool(len(unique_signs) > 1),
        "relationship_stable": bool(len(unique_signs) == 1 and len(non_flat) >= 2),
    }


def _signal_strength_summary(
    feature_signal_table: list[dict[str, Any]],
    feature_stability_flags: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    ranked = [
        {
            "feature": row["feature"],
            "max_abs_correlation": max(
                abs(row["pearson_corr_target_positive"] or 0.0),
                abs(row["spearman_corr_target_positive"] or 0.0),
            ),
            "bucket_positive_rate_separation": row["bucket_positive_rate_separation"],
            "relationship_stable": feature_stability_flags.get(row["feature"], {}).get("relationship_stable"),
            "sign_flips": feature_stability_flags.get(row["feature"], {}).get("sign_flips"),
        }
        for row in feature_signal_table
    ]
    strongest_by_corr = sorted(ranked, key=lambda row: row["max_abs_correlation"], reverse=True)[:5]
    strongest_by_bucket = sorted(ranked, key=lambda row: abs(row["bucket_positive_rate_separation"] or 0.0), reverse=True)[:5]
    no_signal = [
        row["feature"]
        for row in ranked
        if row["max_abs_correlation"] < 0.05 and abs(row["bucket_positive_rate_separation"] or 0.0) < 0.05
    ]
    unstable = [
        row["feature"]
        for row in ranked
        if row["sign_flips"]
    ]
    stable_candidate_count = sum(
        1
        for row in ranked
        if row["relationship_stable"]
        and (row["max_abs_correlation"] >= 0.10 or abs(row["bucket_positive_rate_separation"] or 0.0) >= 0.15)
    )
    max_corr = max([row["max_abs_correlation"] for row in ranked], default=0.0)
    max_bucket = max([abs(row["bucket_positive_rate_separation"] or 0.0) for row in ranked], default=0.0)
    if stable_candidate_count >= 2 and (max_corr >= 0.20 or max_bucket >= 0.25):
        assessment = "MODERATE"
    elif stable_candidate_count >= 1 or max_corr >= 0.10 or max_bucket >= 0.15:
        assessment = "WEAK"
    else:
        assessment = "NONE"
    return {
        "overall_signal_strength": assessment,
        "strongest_features_by_correlation": strongest_by_corr,
        "strongest_features_by_bucket_separation": strongest_by_bucket,
        "features_with_no_signal": no_signal,
        "unstable_features": unstable,
        "stable_candidate_count": int(stable_candidate_count),
        "note": "signal strength is descriptive diagnostics only; it is not a tradable edge claim",
    }


def _model_collapse_diagnosis(
    model_report: dict[str, Any],
    signal_strength_summary: dict[str, Any],
) -> dict[str, Any]:
    diagnostic_summary = model_report.get("diagnostic_summary") or {}
    windows = model_report.get("walk_forward", {}).get("windows") or []
    probability_clustered = _probabilities_clustered_near_half(windows)
    coefficient_stability = diagnostic_summary.get("coefficient_direction_stability") or {}
    unstable_coefficients = [
        feature
        for feature, data in coefficient_stability.items()
        if data.get("stability_rate") is not None and data.get("stability_rate") < 0.75
    ]
    weak_or_missing_signal = signal_strength_summary.get("overall_signal_strength") in {"NONE", "WEAK"}
    reasons: list[str] = []
    if diagnostic_summary.get("model_underperformed_naive_accuracy"):
        reasons.append("PR19 diagnostics show average accuracy below the naive majority baseline")
    if diagnostic_summary.get("predicted_mostly_one_class_window_count", 0):
        reasons.append(f"model predicted mostly one class in {diagnostic_summary.get('predicted_mostly_one_class_window_count')} walk-forward window(s)")
    if probability_clustered["clustered_near_0_5_window_count"]:
        reasons.append(f"probabilities clustered near 0.5 in {probability_clustered['clustered_near_0_5_window_count']} window(s)")
    if unstable_coefficients:
        reasons.append(f"{len(unstable_coefficients)} coefficient direction(s) were unstable across windows")
    if weak_or_missing_signal:
        reasons.append("feature diagnostics found weak or no descriptive signal")
    if not reasons:
        reasons.append("no single collapse cause identified from available diagnostics")
    return {
        "source_model_report_available": bool(model_report),
        "why_model_predicted_mostly_one_class": reasons,
        "probabilities_clustered_near_0_5": probability_clustered,
        "coefficients_weak_or_unstable": bool(unstable_coefficients),
        "unstable_coefficient_features": unstable_coefficients,
        "no_feature_separated_classes": bool(weak_or_missing_signal),
        "signal_strength_used": signal_strength_summary.get("overall_signal_strength"),
        "note": "diagnosis is descriptive only and does not tune or change the model",
    }


def _regime_segmentation(frame: pd.DataFrame, *, target_column: str, target_positive: pd.Series) -> dict[str, dict[str, Any]]:
    regimes: dict[str, pd.Series] = {}
    if "price_momentum_20d" in frame.columns:
        momentum = pd.to_numeric(frame["price_momentum_20d"], errors="coerce")
        regimes["uptrend"] = momentum >= 0
        regimes["downtrend"] = momentum < 0
    if "realized_vol_30d" in frame.columns:
        vol = pd.to_numeric(frame["realized_vol_30d"], errors="coerce")
        median_vol = vol.median()
        if pd.notna(median_vol):
            regimes["high_vol"] = vol >= median_vol
            regimes["low_vol"] = vol < median_vol
    output: dict[str, dict[str, Any]] = {}
    target_values = pd.to_numeric(frame[target_column], errors="coerce")
    for name, mask in regimes.items():
        selected = frame.loc[mask.fillna(False)]
        selected_target = target_values.loc[selected.index]
        selected_positive = target_positive.loc[selected.index].astype(float)
        output[name] = {
            "rows": int(len(selected)),
            "positive_rate": _safe_float(selected_positive.mean()),
            "mean_forward_return_5d": _safe_float(selected_target.mean()),
        }
    return output


def _probabilities_clustered_near_half(windows: list[dict[str, Any]]) -> dict[str, Any]:
    clustered = []
    for window in windows:
        distribution = window.get("diagnostics", {}).get("probability_distribution", {})
        p25 = distribution.get("p25")
        p75 = distribution.get("p75")
        if p25 is None or p75 is None:
            continue
        is_clustered = p25 >= 0.4 and p75 <= 0.6
        if is_clustered:
            clustered.append(window.get("window_id"))
    return {
        "clustered_near_0_5_window_count": int(len(clustered)),
        "clustered_window_ids": clustered,
        "rule": "p25 >= 0.4 and p75 <= 0.6",
    }


def _bucket_separation(buckets: list[dict[str, Any]], *, value_key: str) -> float | None:
    values = [bucket.get(value_key) for bucket in buckets if bucket.get(value_key) is not None]
    if len(values) < 2:
        return None
    return float(values[-1]) - float(values[0])


def _relationship_sign(value: float | None, *, epsilon: float = 1e-9) -> str:
    if value is None or abs(value) <= epsilon:
        return "flat"
    return "positive" if value > 0 else "negative"


def _correlation(left: pd.Series, right: pd.Series, *, method: str) -> float | None:
    data = pd.DataFrame({"left": pd.to_numeric(left, errors="coerce"), "right": pd.to_numeric(right, errors="coerce")}).dropna()
    if len(data) < 2 or data["left"].nunique() <= 1 or data["right"].nunique() <= 1:
        return None
    return _safe_float(data["left"].corr(data["right"], method=method))


def _base_report(
    *,
    status: str,
    frame: pd.DataFrame,
    metadata: dict[str, Any],
    dataset_path: Path | None,
    metadata_path: Path | None,
    model_diagnostics_path: Path | None,
    target_column: str,
    feature_columns: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "name": "ml_signal_diagnostics",
        "status": status,
        "symbol": metadata.get("symbol", _infer_symbol(frame)),
        "analysis_start_date": metadata.get("analysis_start_date", _date_min(frame)),
        "analysis_end_date": metadata.get("analysis_end_date", _date_max(frame)),
        "read_only": True,
        "training_performed": False,
        "hyperparameter_tuning_performed": False,
        "threshold_optimization_performed": False,
        "performance_claim": False,
        "target_name": "forward_return_5d_positive",
        "target_column": target_column,
        "target_definition": "forward_return_5d > 0",
        "feature_columns": list(feature_columns),
        "label_columns": list(LABEL_COLUMNS),
        "rows": {
            "input": int(len(frame)),
        },
        "dataset": {
            "path": str(dataset_path) if dataset_path else None,
            "metadata_path": str(metadata_path) if metadata_path else None,
            "model_diagnostics_path": str(model_diagnostics_path) if model_diagnostics_path else None,
            "metadata_status": metadata.get("status"),
        },
        "warnings": _dedupe(warnings),
    }


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
        raise ValueError(f"label/outcome columns cannot be used as signal diagnostic features: {', '.join(sorted(leaky))}")


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


def _default_model_diagnostics_path(dataset_path: Path) -> Path:
    symbol = "spy"
    stem = dataset_path.with_suffix("").name
    parts = stem.split("_")
    if len(parts) >= 3:
        symbol = parts[0].lower()
        start = parts[1]
        end = parts[2]
    else:
        start = "unknown"
        end = "unknown"
    return (
        Path.cwd()
        / "reports"
        / "ml_smoke"
        / f"{symbol}_{start}_{end}_ml_walk_forward_smoke_forward_return_5d_positive.json"
    )


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


def _safe_float(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
