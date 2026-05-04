"""Regime and target diagnostics for bounded model-ready datasets.

This is a descriptive diagnostics layer only. It does not train models, tune
thresholds, tune hyperparameters, scan full history, or make edge claims.
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
REGIME_FEATURES = {
    "realized_vol_60d": "median_split",
    "vix_level": "median_split",
    "price_momentum_20d": "sign_split",
    "iv30_rv30_ratio": "median_split",
    "vol_term_structure_slope": "median_split",
}
TARGET_SPECS = [
    {"name": "forward_return_1d_positive", "column": "forward_return_1d", "kind": "direction", "overlap_warning": False},
    {"name": "forward_return_5d_positive", "column": "forward_return_5d", "kind": "direction", "overlap_warning": True},
    {"name": "forward_return_21d_positive", "column": "forward_return_21d", "kind": "direction", "overlap_warning": True},
    {"name": "forward_return_5d_abs_move", "column": "forward_return_5d", "kind": "absolute_move", "overlap_warning": True},
    {"name": "forward_volatility_21d", "column": "forward_volatility_21d", "kind": "continuous", "overlap_warning": True},
]


@dataclass(frozen=True)
class MLRegimeTargetDiagnosticsResult:
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def run_ml_regime_target_diagnostics(
    *,
    dataset_path: str | Path,
    metadata_path: str | Path | None = None,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    stability_window_rows: int = DEFAULT_STABILITY_WINDOW_ROWS,
    rolling_window_rows: int = DEFAULT_ROLLING_WINDOW_ROWS,
) -> MLRegimeTargetDiagnosticsResult:
    dataset_file = Path(dataset_path).expanduser().resolve()
    metadata_file = Path(metadata_path).expanduser().resolve() if metadata_path else _default_metadata_path(dataset_file)
    dataset = _read_dataset(dataset_file)
    metadata = _read_metadata(metadata_file)
    return run_ml_regime_target_diagnostics_on_frame(
        dataset=dataset,
        dataset_path=dataset_file,
        metadata_path=metadata_file if metadata_file.exists() else None,
        dataset_metadata=metadata,
        bucket_count=bucket_count,
        stability_window_rows=stability_window_rows,
        rolling_window_rows=rolling_window_rows,
    )


def run_ml_regime_target_diagnostics_on_frame(
    *,
    dataset: pd.DataFrame,
    dataset_path: Path | None = None,
    metadata_path: Path | None = None,
    dataset_metadata: dict[str, Any] | None = None,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    stability_window_rows: int = DEFAULT_STABILITY_WINDOW_ROWS,
    rolling_window_rows: int = DEFAULT_ROLLING_WINDOW_ROWS,
) -> MLRegimeTargetDiagnosticsResult:
    if min(bucket_count, stability_window_rows, rolling_window_rows) <= 0:
        raise ValueError("bucket_count, stability_window_rows, and rolling_window_rows must be positive")

    metadata = dataset_metadata or {}
    warnings = [
        "regime/target diagnostics are inspection-only; no model training, threshold tuning, hyperparameter tuning, or edge claim is performed",
        "5d and 21d forward labels overlap across adjacent entry dates and can inflate autocorrelation/stability diagnostics",
    ]
    frame = _prepare_frame(dataset)
    feature_columns = _feature_columns(frame)
    _validate_no_label_features(feature_columns)
    missing_required = [column for column in ["entry_date", DEFAULT_TARGET_COLUMN] if column not in frame.columns]
    if missing_required:
        report = _base_report(
            status="fail",
            frame=frame,
            metadata=metadata,
            dataset_path=dataset_path,
            metadata_path=metadata_path,
            feature_columns=feature_columns,
            warnings=[*warnings, f"missing required columns: {', '.join(missing_required)}"],
        )
        report["failure_reason"] = "missing_required_columns"
        return MLRegimeTargetDiagnosticsResult(report=report)

    working = frame.dropna(subset=[DEFAULT_TARGET_COLUMN]).copy()
    regime_table, missing_regime_features = _regime_table(
        working,
        feature_columns=feature_columns,
        bucket_count=bucket_count,
        stability_window_rows=stability_window_rows,
    )
    if missing_regime_features:
        warnings.append(f"missing regime feature(s): {', '.join(missing_regime_features)}")
    target_comparison = _target_comparison_table(
        working,
        feature_columns=feature_columns,
        bucket_count=bucket_count,
        stability_window_rows=stability_window_rows,
        rolling_window_rows=rolling_window_rows,
    )
    stability_summary = _stability_summary(regime_table, target_comparison)
    recommendation = _recommendation(target_comparison, stability_summary)
    report = _base_report(
        status="warn",
        frame=working,
        metadata=metadata,
        dataset_path=dataset_path,
        metadata_path=metadata_path,
        feature_columns=feature_columns,
        warnings=warnings,
    )
    report.update(
        {
            "regime_table": regime_table,
            "missing_regime_features": missing_regime_features,
            "target_comparison_table": target_comparison,
            "stability_summary": stability_summary,
            "recommendation": recommendation,
            "overlap_warning": {
                "forward_return_5d": "overlapping 5-trading-day forward labels share future returns across adjacent entry dates",
                "forward_return_21d": "overlapping 21-trading-day forward labels share future returns across adjacent entry dates",
            },
            "leakage_checks": {
                "labels_excluded_from_features": True,
                "no_training_performed": True,
                "no_threshold_optimization": True,
                "no_hyperparameter_tuning": True,
            },
            "explicit_warning": "no edge claim",
        }
    )
    return MLRegimeTargetDiagnosticsResult(report=report)


def write_ml_regime_target_diagnostics_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "ml_diagnostics"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    stem = (
        f"{report['symbol'].lower()}_{report['analysis_start_date']}_{report['analysis_end_date']}_"
        f"{report['name']}"
    ).replace("/", "-")
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _regime_table(
    frame: pd.DataFrame,
    *,
    feature_columns: list[str],
    bucket_count: int,
    stability_window_rows: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    target_values = pd.to_numeric(frame[DEFAULT_TARGET_COLUMN], errors="coerce")
    target_positive = (target_values > 0).astype("Int64")
    for feature, split_kind in REGIME_FEATURES.items():
        if feature not in frame.columns:
            missing.append(feature)
            continue
        regimes = _regime_masks(frame[feature], split_kind=split_kind)
        if not regimes:
            missing.append(feature)
            continue
        for regime_name, mask in regimes.items():
            selected = frame.loc[mask.fillna(False)].copy()
            selected_target = target_values.loc[selected.index]
            selected_positive = target_positive.loc[selected.index]
            stability = _relationship_stability(
                selected,
                target_values=selected_target,
                target_positive=selected_positive,
                feature_columns=feature_columns,
                bucket_count=bucket_count,
                stability_window_rows=stability_window_rows,
            )
            rows.append(
                {
                    "regime_feature": feature,
                    "regime": regime_name,
                    "split_kind": split_kind,
                    "rows": int(len(selected)),
                    "target_positive_rate": _safe_float(selected_positive.astype(float).mean()),
                    "mean_forward_return_5d": _safe_float(selected_target.mean()),
                    "feature_correlation_stability": stability["correlation"],
                    "bucket_separation_stability": stability["bucket_separation"],
                }
            )
    return rows, missing


def _target_comparison_table(
    frame: pd.DataFrame,
    *,
    feature_columns: list[str],
    bucket_count: int,
    stability_window_rows: int,
    rolling_window_rows: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for spec in TARGET_SPECS:
        column = spec["column"]
        if column not in frame.columns:
            rows.append(
                {
                    "target": spec["name"],
                    "source_column": column,
                    "kind": spec["kind"],
                    "status": "missing",
                    "rows": 0,
                    "overlap_warning": bool(spec["overlap_warning"]),
                }
            )
            continue
        target_values = _target_values(frame[column], kind=spec["kind"])
        target_frame = frame.loc[target_values.notna()].copy()
        target_values = target_values.loc[target_frame.index]
        direction_values = (target_values > 0).astype("Int64") if spec["kind"] == "direction" else None
        stability_target = direction_values if direction_values is not None else target_values
        stability = _relationship_stability(
            target_frame,
            target_values=target_values,
            target_positive=stability_target,
            feature_columns=feature_columns,
            bucket_count=bucket_count,
            stability_window_rows=stability_window_rows,
        )
        rolling = _rolling_target_metric(
            target_frame,
            target_values=target_values,
            target_positive=direction_values,
            kind=spec["kind"],
            rolling_window_rows=rolling_window_rows,
        )
        monthly = _monthly_target_metric(
            target_frame,
            target_values=target_values,
            target_positive=direction_values,
            kind=spec["kind"],
        )
        metric_values = [item["metric"] for item in rolling if item["metric"] is not None]
        rows.append(
            {
                "target": spec["name"],
                "source_column": column,
                "kind": spec["kind"],
                "status": "ok" if len(target_frame) else "empty",
                "rows": int(len(target_frame)),
                "positive_rate": _safe_float(direction_values.astype(float).mean()) if direction_values is not None else None,
                "mean": _safe_float(target_values.mean()),
                "std": _safe_float(target_values.std()),
                "skew": _safe_float(target_values.skew()),
                "lag1_autocorrelation": _autocorrelation(target_values, lag=1),
                "monthly_metric": monthly,
                "rolling_metric": rolling,
                "rolling_metric_std": _safe_float(pd.Series(metric_values).std()) if metric_values else None,
                "feature_correlation_stability": stability["correlation"],
                "bucket_separation_stability": stability["bucket_separation"],
                "overlap_warning": bool(spec["overlap_warning"]),
            }
        )
    return rows


def _relationship_stability(
    frame: pd.DataFrame,
    *,
    target_values: pd.Series,
    target_positive: pd.Series,
    feature_columns: list[str],
    bucket_count: int,
    stability_window_rows: int,
) -> dict[str, Any]:
    correlation_signs: dict[str, list[str]] = {feature: [] for feature in feature_columns}
    bucket_signs: dict[str, list[str]] = {feature: [] for feature in feature_columns}
    for start in range(0, len(frame), stability_window_rows):
        end = min(start + stability_window_rows, len(frame))
        chunk = frame.iloc[start:end]
        if len(chunk) < max(10, bucket_count * 2):
            continue
        chunk_target = target_positive.loc[chunk.index]
        chunk_values = target_values.loc[chunk.index]
        for feature in feature_columns:
            values = pd.to_numeric(chunk[feature], errors="coerce")
            corr = _correlation(values, chunk_target, method="spearman")
            corr_sign = _sign(corr)
            if corr_sign != "flat":
                correlation_signs[feature].append(corr_sign)
            bucket_sep = _bucket_separation(values, chunk_values, bucket_count=bucket_count)
            bucket_sign = _sign(bucket_sep)
            if bucket_sign != "flat":
                bucket_signs[feature].append(bucket_sign)
    return {
        "correlation": _sign_stability_summary(correlation_signs),
        "bucket_separation": _sign_stability_summary(bucket_signs),
    }


def _sign_stability_summary(signs_by_feature: dict[str, list[str]]) -> dict[str, Any]:
    stable: list[str] = []
    unstable: list[str] = []
    sparse: list[str] = []
    for feature, signs in signs_by_feature.items():
        unique = set(signs)
        if len(signs) < 2:
            sparse.append(feature)
        elif len(unique) == 1:
            stable.append(feature)
        else:
            unstable.append(feature)
    return {
        "stable_feature_count": int(len(stable)),
        "unstable_feature_count": int(len(unstable)),
        "sparse_feature_count": int(len(sparse)),
        "stable_features": sorted(stable),
        "unstable_features": sorted(unstable),
    }


def _regime_masks(series: pd.Series, *, split_kind: str) -> dict[str, pd.Series]:
    values = pd.to_numeric(series, errors="coerce")
    if values.dropna().empty:
        return {}
    if split_kind == "sign_split":
        return {
            "positive_or_zero": values >= 0,
            "negative": values < 0,
        }
    median = values.median()
    if pd.isna(median):
        return {}
    return {
        "high": values >= median,
        "low": values < median,
    }


def _target_values(series: pd.Series, *, kind: str) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    if kind == "absolute_move":
        return values.abs()
    return values


def _monthly_target_metric(
    frame: pd.DataFrame,
    *,
    target_values: pd.Series,
    target_positive: pd.Series | None,
    kind: str,
) -> dict[str, dict[str, Any]]:
    months = pd.to_datetime(frame["entry_date"], errors="coerce").dt.to_period("M").astype("string")
    output: dict[str, dict[str, Any]] = {}
    for month, index in months.groupby(months).groups.items():
        values = target_values.loc[index]
        positives = target_positive.loc[index] if target_positive is not None else None
        output[str(month)] = {
            "rows": int(len(values)),
            "metric": _safe_float(positives.astype(float).mean()) if kind == "direction" and positives is not None else _safe_float(values.mean()),
            "metric_name": "positive_rate" if kind == "direction" else "mean",
        }
    return dict(sorted(output.items()))


def _rolling_target_metric(
    frame: pd.DataFrame,
    *,
    target_values: pd.Series,
    target_positive: pd.Series | None,
    kind: str,
    rolling_window_rows: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for start in range(0, len(frame), rolling_window_rows):
        end = min(start + rolling_window_rows, len(frame))
        chunk = frame.iloc[start:end]
        values = target_values.loc[chunk.index]
        positives = target_positive.loc[chunk.index] if target_positive is not None else None
        rows.append(
            {
                "window_id": int(len(rows) + 1),
                "start": _date_min(chunk),
                "end": _date_max(chunk),
                "rows": int(len(chunk)),
                "metric": _safe_float(positives.astype(float).mean()) if kind == "direction" and positives is not None else _safe_float(values.mean()),
                "metric_name": "positive_rate" if kind == "direction" else "mean",
            }
        )
    return rows


def _stability_summary(regime_table: list[dict[str, Any]], target_comparison: list[dict[str, Any]]) -> dict[str, Any]:
    target_scores = []
    for row in target_comparison:
        if row.get("status") != "ok":
            continue
        corr = row["feature_correlation_stability"]
        bucket = row["bucket_separation_stability"]
        target_scores.append(
            {
                "target": row["target"],
                "stable_correlation_features": corr["stable_feature_count"],
                "unstable_correlation_features": corr["unstable_feature_count"],
                "stable_bucket_features": bucket["stable_feature_count"],
                "unstable_bucket_features": bucket["unstable_feature_count"],
                "rolling_metric_std": row["rolling_metric_std"],
                "overlap_warning": row["overlap_warning"],
            }
        )
    regime_sensitive = [
        f"{row['regime_feature']}:{row['regime']}"
        for row in regime_table
        if row.get("rows", 0) >= 20
        and (
            row["feature_correlation_stability"]["unstable_feature_count"] > row["feature_correlation_stability"]["stable_feature_count"]
            or row["bucket_separation_stability"]["unstable_feature_count"] > row["bucket_separation_stability"]["stable_feature_count"]
        )
    ]
    return {
        "target_scores": target_scores,
        "regime_sensitive_segments": regime_sensitive,
        "regime_sensitive_segment_count": int(len(regime_sensitive)),
        "note": "stability is descriptive and bounded to the analyzed dataset",
    }


def _recommendation(target_comparison: list[dict[str, Any]], stability_summary: dict[str, Any]) -> dict[str, Any]:
    ok_targets = [row for row in target_comparison if row.get("status") == "ok"]
    current = next((row for row in ok_targets if row["target"] == "forward_return_5d_positive"), None)
    if current is None or current.get("rows", 0) < 100:
        action = "needs_more_data"
        reasons = ["current 5d direction target has insufficient bounded rows for a target-change decision"]
    else:
        scored = []
        for row in ok_targets:
            corr = row["feature_correlation_stability"]["stable_feature_count"] - row["feature_correlation_stability"]["unstable_feature_count"]
            bucket = row["bucket_separation_stability"]["stable_feature_count"] - row["bucket_separation_stability"]["unstable_feature_count"]
            score = corr + bucket
            scored.append((score, row["target"]))
        scored.sort(reverse=True)
        best_score, best_target = scored[0]
        current_score = next(score for score, target in scored if target == "forward_return_5d_positive")
        best_row = next(row for row in ok_targets if row["target"] == best_target)
        if stability_summary.get("regime_sensitive_segment_count", 0) > 0:
            action = "needs_more_data"
            reasons = ["bounded diagnostics appear regime-sensitive; validate across additional years before keeping or changing target"]
            if best_target != "forward_return_5d_positive" and best_score >= current_score + 4:
                reasons.append(f"{best_target} looked more stable in this bounded year, but this is not enough to change targets")
        elif best_target != "forward_return_5d_positive" and best_score >= current_score + 4 and best_row.get("overlap_warning"):
            action = "needs_more_data"
            reasons = [f"{best_target} looked more stable, but overlapping forward labels require additional non-overlap validation before changing targets"]
        elif best_target != "forward_return_5d_positive" and best_score >= current_score + 4:
            action = "change_target"
            reasons = [f"{best_target} had materially stronger bounded stability diagnostics than forward_return_5d_positive"]
        else:
            action = "keep_target"
            reasons = ["forward_return_5d_positive was not materially worse than alternatives in bounded diagnostics"]
    return {
        "action": action,
        "allowed_values": ["keep_target", "change_target", "needs_more_data"],
        "reasons": reasons,
        "guardrail": "recommendation is diagnostic only; it does not change labels, thresholds, models, or promotion logic",
    }


def _bucket_separation(feature_values: pd.Series, target_values: pd.Series, *, bucket_count: int) -> float | None:
    data = pd.DataFrame(
        {
            "feature": pd.to_numeric(feature_values, errors="coerce"),
            "target": pd.to_numeric(target_values, errors="coerce"),
        }
    ).dropna()
    if data.empty or data["feature"].nunique() <= 1:
        return None
    q = min(bucket_count, int(data["feature"].nunique()))
    data["bucket"] = pd.qcut(data["feature"], q=q, labels=False, duplicates="drop")
    grouped = data.groupby("bucket", dropna=True, sort=True)["target"].mean()
    if len(grouped) < 2:
        return None
    return float(grouped.iloc[-1] - grouped.iloc[0])


def _correlation(left: pd.Series, right: pd.Series, *, method: str) -> float | None:
    data = pd.DataFrame({"left": pd.to_numeric(left, errors="coerce"), "right": pd.to_numeric(right, errors="coerce")}).dropna()
    if len(data) < 2 or data["left"].nunique() <= 1 or data["right"].nunique() <= 1:
        return None
    left_std = data["left"].std()
    right_std = data["right"].std()
    if pd.isna(left_std) or pd.isna(right_std) or float(left_std) == 0.0 or float(right_std) == 0.0:
        return None
    return _safe_float(data["left"].corr(data["right"], method=method))


def _autocorrelation(values: pd.Series, *, lag: int) -> float | None:
    series = pd.to_numeric(values, errors="coerce").dropna()
    if len(series) <= lag + 1 or series.nunique() <= 1:
        return None
    left = series.iloc[lag:].reset_index(drop=True)
    right = series.iloc[:-lag].reset_index(drop=True)
    if left.nunique() <= 1 or right.nunique() <= 1:
        return None
    left_std = left.std()
    right_std = right.std()
    if pd.isna(left_std) or pd.isna(right_std) or float(left_std) == 0.0 or float(right_std) == 0.0:
        return None
    return _safe_float(left.corr(right))


def _sign(value: float | None, *, epsilon: float = 1e-9) -> str:
    if value is None or abs(value) <= epsilon:
        return "flat"
    return "positive" if value > 0 else "negative"


def _base_report(
    *,
    status: str,
    frame: pd.DataFrame,
    metadata: dict[str, Any],
    dataset_path: Path | None,
    metadata_path: Path | None,
    feature_columns: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "name": "ml_regime_target_diagnostics",
        "status": status,
        "symbol": metadata.get("symbol", _infer_symbol(frame)),
        "analysis_start_date": metadata.get("analysis_start_date", _date_min(frame)),
        "analysis_end_date": metadata.get("analysis_end_date", _date_max(frame)),
        "read_only": True,
        "training_performed": False,
        "hyperparameter_tuning_performed": False,
        "threshold_optimization_performed": False,
        "performance_claim": False,
        "feature_columns": list(feature_columns),
        "label_columns": list(LABEL_COLUMNS),
        "rows": {"input": int(len(frame))},
        "dataset": {
            "path": str(dataset_path) if dataset_path else None,
            "metadata_path": str(metadata_path) if metadata_path else None,
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
        raise ValueError(f"label/outcome columns cannot be used as regime diagnostic features: {', '.join(sorted(leaky))}")


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
