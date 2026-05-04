"""Bounded multi-year diagnostics for model-ready datasets.

This module runs bounded yearly exports and descriptive diagnostics only. It
does not mutate T9 data, train models, tune thresholds, tune hyperparameters,
scan full history, or make edge claims.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.ml_regime_target_diagnostics import (
    REGIME_FEATURES,
    TARGET_SPECS,
    run_ml_regime_target_diagnostics_on_frame,
)
from services.external_data.model_ready_dataset_export import (
    FEATURE_DATA_COLUMNS,
    LABEL_COLUMNS,
    ModelReadyDatasetExport,
    build_model_ready_dataset_from_t9,
    write_model_ready_dataset_export,
)


DEFAULT_YEAR_WINDOWS = [
    {"year": "2023", "analysis_start_date": "2023-01-03", "analysis_end_date": "2023-12-29"},
    {"year": "2024", "analysis_start_date": "2024-01-02", "analysis_end_date": "2024-12-31"},
    {"year": "2025", "analysis_start_date": "2025-01-02", "analysis_end_date": "2025-12-31"},
]
DEFAULT_FEATURE_LOOKBACK_DAYS = 120
DEFAULT_LABEL_LOOKAHEAD_DAYS = 45
DEFAULT_MAX_FILES = 120
DEFAULT_DAILY_SOURCE = "yahoo"
DEFAULT_HORIZONS = ["1d", "5d", "21d"]


@dataclass(frozen=True)
class MLMultiyearDiagnosticsResult:
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def run_ml_multiyear_diagnostics(
    *,
    symbol: str = "SPY",
    root: str | Path | None = None,
    year_windows: list[dict[str, str]] | None = None,
    feature_lookback_days: int = DEFAULT_FEATURE_LOOKBACK_DAYS,
    label_lookahead_days: int = DEFAULT_LABEL_LOOKAHEAD_DAYS,
    max_files: int = DEFAULT_MAX_FILES,
    daily_source: str | None = DEFAULT_DAILY_SOURCE,
    horizons: list[str] | None = None,
) -> MLMultiyearDiagnosticsResult:
    years = year_windows or DEFAULT_YEAR_WINDOWS
    requested_horizons = horizons or DEFAULT_HORIZONS
    year_reports = []
    for window in years:
        year_reports.append(
            _build_year_report_from_t9(
                symbol=symbol,
                root=root,
                window=window,
                feature_lookback_days=feature_lookback_days,
                label_lookahead_days=label_lookahead_days,
                max_files=max_files,
                daily_source=daily_source,
                horizons=requested_horizons,
            )
        )
    return build_multiyear_diagnostics_report(
        symbol=symbol,
        year_reports=year_reports,
        config={
            "feature_lookback_days": int(feature_lookback_days),
            "label_lookahead_days": int(label_lookahead_days),
            "max_files": int(max_files),
            "daily_source": daily_source,
            "horizons": list(requested_horizons),
        },
    )


def build_multiyear_diagnostics_report(
    *,
    symbol: str,
    year_reports: list[dict[str, Any]],
    config: dict[str, Any] | None = None,
) -> MLMultiyearDiagnosticsResult:
    successful = [year for year in year_reports if year.get("status") == "ok"]
    warnings = [
        "multi-year diagnostics are inspection-only; no model training, threshold tuning, hyperparameter tuning, or edge claim is performed",
        "5d and 21d forward labels overlap across adjacent entry dates and can inflate autocorrelation/stability diagnostics",
    ]
    missing_years = [year["year"] for year in year_reports if year.get("status") != "ok"]
    if missing_years:
        warnings.append(f"missing or non-evaluable year(s): {', '.join(missing_years)}")
    cross_year = _cross_year_stability(successful)
    recommendation = _final_recommendation(successful, cross_year)
    report = {
        "name": "ml_multiyear_diagnostics",
        "status": "warn",
        "symbol": symbol.upper(),
        "read_only": True,
        "training_performed": False,
        "hyperparameter_tuning_performed": False,
        "threshold_optimization_performed": False,
        "performance_claim": False,
        "config": dict(config or {}),
        "years_requested": [year["year"] for year in year_reports],
        "year_count": int(len(year_reports)),
        "successful_year_count": int(len(successful)),
        "missing_years": missing_years,
        "year_reports": year_reports,
        "cross_year_stability": cross_year,
        "final_recommendation": recommendation,
        "warnings": _dedupe(warnings),
        "explicit_warning": "no edge claim",
    }
    return MLMultiyearDiagnosticsResult(report=report)


def write_ml_multiyear_diagnostics_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "ml_diagnostics"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    years = "-".join(report.get("years_requested") or [])
    stem = f"{report['symbol'].lower()}_{years}_{report['name']}".replace("/", "-")
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _build_year_report_from_t9(
    *,
    symbol: str,
    root: str | Path | None,
    window: dict[str, str],
    feature_lookback_days: int,
    label_lookahead_days: int,
    max_files: int,
    daily_source: str | None,
    horizons: list[str],
) -> dict[str, Any]:
    year = str(window["year"])
    try:
        export = build_model_ready_dataset_from_t9(
            symbol=symbol,
            analysis_start_date=window["analysis_start_date"],
            analysis_end_date=window["analysis_end_date"],
            feature_lookback_days=feature_lookback_days,
            label_lookahead_days=label_lookahead_days,
            root=root,
            max_files=max_files,
            daily_source=daily_source,
            horizons=horizons,
            missing_feature_policy="drop",
            missing_label_policy="flag",
        )
        paths = write_model_ready_dataset_export(export)
    except Exception as exc:
        return {
            "year": year,
            "status": "error",
            "analysis_start_date": window["analysis_start_date"],
            "analysis_end_date": window["analysis_end_date"],
            "error": str(exc),
        }
    return _build_year_report_from_export(year=year, export=export, paths=paths)


def _build_year_report_from_export(
    *,
    year: str,
    export: ModelReadyDatasetExport,
    paths: dict[str, Path] | None = None,
) -> dict[str, Any]:
    metadata = export.metadata
    dataset = export.dataset
    if dataset.empty:
        return {
            "year": year,
            "status": "missing",
            "analysis_start_date": metadata.get("analysis_start_date"),
            "analysis_end_date": metadata.get("analysis_end_date"),
            "rows": metadata.get("rows", {}),
            "reason": "model-ready dataset export produced zero rows",
            "dataset_path": str(paths["dataset"]) if paths and "dataset" in paths else metadata.get("dataset_path"),
            "metadata_path": str(paths["metadata"]) if paths and "metadata" in paths else None,
        }
    diagnostics = run_ml_regime_target_diagnostics_on_frame(
        dataset=dataset,
        dataset_metadata=metadata,
    ).report
    feature_signs = _feature_signs(dataset)
    return {
        "year": year,
        "status": "ok",
        "analysis_start_date": metadata.get("analysis_start_date"),
        "analysis_end_date": metadata.get("analysis_end_date"),
        "actual_start_date": _date_min(dataset),
        "actual_end_date": _date_max(dataset),
        "dataset_path": str(paths["dataset"]) if paths and "dataset" in paths else metadata.get("dataset_path"),
        "metadata_path": str(paths["metadata"]) if paths and "metadata" in paths else None,
        "export": {
            "status": metadata.get("status"),
            "rows": metadata.get("rows", {}),
            "fully_labeled_row_count": metadata.get("fully_labeled_row_count"),
            "drop_reasons": metadata.get("drop_reasons", {}),
            "warnings": metadata.get("warnings", []),
            "export_warnings": metadata.get("export_warnings", []),
        },
        "target_positive_rates": _target_positive_rates(dataset),
        "regime_segment_differences": _regime_segment_differences(diagnostics.get("regime_table", [])),
        "strongest_descriptive_features": _strongest_features(diagnostics.get("target_comparison_table", [])),
        "feature_signs": feature_signs,
        "feature_sign_flips": diagnostics.get("stability_summary", {}).get("target_scores", []),
        "target_comparison_results": diagnostics.get("target_comparison_table", []),
        "per_year_recommendation": diagnostics.get("recommendation", {}),
        "regime_sensitive_segment_count": diagnostics.get("stability_summary", {}).get("regime_sensitive_segment_count"),
        "warnings": diagnostics.get("warnings", []),
    }


def _cross_year_stability(year_reports: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "persisting_regimes": _persisting_regimes(year_reports),
        "stable_targets": _stable_targets(year_reports),
        "feature_sign_flips_across_years": _feature_sign_flips_across_years(year_reports),
        "target_21d_stability": _target_21d_stability(year_reports),
        "note": "cross-year stability is descriptive only and bounded to successful yearly samples",
    }


def _persisting_regimes(year_reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    effects_by_name: dict[str, list[dict[str, Any]]] = {}
    for report in year_reports:
        for effect in report.get("regime_segment_differences", []):
            effects_by_name.setdefault(effect["regime_feature"], []).append(
                {
                    "year": report["year"],
                    "difference": effect["positive_rate_difference"],
                    "sign": _sign(effect["positive_rate_difference"]),
                }
            )
    rows = []
    for feature, effects in effects_by_name.items():
        signs = [effect["sign"] for effect in effects if effect["sign"] != "flat"]
        rows.append(
            {
                "regime_feature": feature,
                "year_count": int(len(effects)),
                "effects": effects,
                "persistent": bool(len(effects) >= 2 and len(set(signs)) == 1 and bool(signs)),
            }
        )
    return sorted(rows, key=lambda row: row["regime_feature"])


def _stable_targets(year_reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_target: dict[str, list[dict[str, Any]]] = {}
    for report in year_reports:
        for row in report.get("target_comparison_results", []):
            if row.get("status") != "ok":
                continue
            by_target.setdefault(row["target"], []).append(
                {
                    "year": report["year"],
                    "stable_bucket_features": row["bucket_separation_stability"]["stable_feature_count"],
                    "unstable_bucket_features": row["bucket_separation_stability"]["unstable_feature_count"],
                    "stable_correlation_features": row["feature_correlation_stability"]["stable_feature_count"],
                    "unstable_correlation_features": row["feature_correlation_stability"]["unstable_feature_count"],
                    "overlap_warning": row["overlap_warning"],
                }
            )
    rows = []
    for target, values in by_target.items():
        stable_years = [
            value
            for value in values
            if value["stable_bucket_features"] >= value["unstable_bucket_features"]
            or value["stable_correlation_features"] >= value["unstable_correlation_features"]
        ]
        rows.append(
            {
                "target": target,
                "year_count": int(len(values)),
                "stable_year_count": int(len(stable_years)),
                "overlap_warning": any(value["overlap_warning"] for value in values),
                "stable_across_years": bool(len(values) >= 2 and len(stable_years) == len(values)),
                "year_details": values,
            }
        )
    return sorted(rows, key=lambda row: (-row["stable_year_count"], row["target"]))


def _feature_sign_flips_across_years(year_reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_feature: dict[str, list[dict[str, Any]]] = {}
    for report in year_reports:
        for feature, sign in report.get("feature_signs", {}).items():
            by_feature.setdefault(feature, []).append({"year": report["year"], "sign": sign})
    rows = []
    for feature, values in by_feature.items():
        signs = [value["sign"] for value in values if value["sign"] != "flat"]
        rows.append(
            {
                "feature": feature,
                "year_count": int(len(values)),
                "signs": values,
                "flipped": bool(len(set(signs)) > 1),
            }
        )
    return sorted(rows, key=lambda row: (not row["flipped"], row["feature"]))


def _target_21d_stability(year_reports: list[dict[str, Any]]) -> dict[str, Any]:
    target_rows = _stable_targets(year_reports)
    by_target = {row["target"]: row for row in target_rows}
    target_21d = by_target.get("forward_return_21d_positive")
    target_5d = by_target.get("forward_return_5d_positive")
    if not target_21d or not target_5d:
        return {
            "status": "insufficient_data",
            "answer": "cannot compare 21d and 5d targets across years",
        }
    return {
        "status": "ok",
        "target_21d_stable_year_count": target_21d["stable_year_count"],
        "target_5d_stable_year_count": target_5d["stable_year_count"],
        "target_21d_remains_more_stable": bool(target_21d["stable_year_count"] > target_5d["stable_year_count"]),
        "answer": "21d looked more stable across successful bounded years" if target_21d["stable_year_count"] > target_5d["stable_year_count"] else "21d was not consistently more stable than 5d across successful bounded years",
    }


def _final_recommendation(year_reports: list[dict[str, Any]], cross_year: dict[str, Any]) -> dict[str, Any]:
    if len(year_reports) < 2:
        action = "needs_more_data"
        reasons = ["fewer than two bounded years were available for cross-year target/regime validation"]
    else:
        target_21d = cross_year["target_21d_stability"]
        persistent_regimes = [row for row in cross_year["persisting_regimes"] if row["persistent"]]
        if target_21d.get("target_21d_remains_more_stable") and len(year_reports) >= 3:
            action = "consider_21d_target"
            reasons = ["21d target remained more stable across at least three successful bounded years"]
        elif persistent_regimes:
            action = "use_regime_conditioned_targets"
            reasons = [f"{len(persistent_regimes)} regime relationship(s) persisted across years"]
        else:
            action = "needs_more_data"
            reasons = ["cross-year diagnostics did not establish a persistent target or regime pattern"]
    return {
        "action": action,
        "allowed_values": ["keep_5d_target", "consider_21d_target", "use_regime_conditioned_targets", "needs_more_data"],
        "reasons": reasons,
        "guardrail": "recommendation is diagnostic only; it does not change labels, models, thresholds, or promotion logic",
    }


def _target_positive_rates(dataset: pd.DataFrame) -> dict[str, float | None]:
    output: dict[str, float | None] = {}
    for column in ["forward_return_1d", "forward_return_5d", "forward_return_21d"]:
        if column not in dataset.columns:
            output[f"{column}_positive"] = None
            continue
        values = pd.to_numeric(dataset[column], errors="coerce").dropna()
        output[f"{column}_positive"] = _safe_float((values > 0).mean()) if not values.empty else None
    return output


def _regime_segment_differences(regime_table: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_feature: dict[str, dict[str, dict[str, Any]]] = {}
    for row in regime_table:
        by_feature.setdefault(row["regime_feature"], {})[row["regime"]] = row
    pairs = {
        "realized_vol_60d": ("high", "low"),
        "vix_level": ("high", "low"),
        "price_momentum_20d": ("positive_or_zero", "negative"),
        "iv30_rv30_ratio": ("high", "low"),
        "vol_term_structure_slope": ("high", "low"),
    }
    output = []
    for feature, (left_name, right_name) in pairs.items():
        values = by_feature.get(feature, {})
        left = values.get(left_name)
        right = values.get(right_name)
        if not left or not right:
            continue
        output.append(
            {
                "regime_feature": feature,
                "left_regime": left_name,
                "right_regime": right_name,
                "left_positive_rate": left.get("target_positive_rate"),
                "right_positive_rate": right.get("target_positive_rate"),
                "positive_rate_difference": _none_safe_subtract(left.get("target_positive_rate"), right.get("target_positive_rate")),
                "left_mean_forward_return_5d": left.get("mean_forward_return_5d"),
                "right_mean_forward_return_5d": right.get("mean_forward_return_5d"),
                "mean_forward_return_5d_difference": _none_safe_subtract(left.get("mean_forward_return_5d"), right.get("mean_forward_return_5d")),
            }
        )
    return output


def _strongest_features(target_comparison_results: list[dict[str, Any]]) -> dict[str, list[str]]:
    output: dict[str, list[str]] = {}
    for row in target_comparison_results:
        if row.get("status") != "ok":
            continue
        stable = set(row["feature_correlation_stability"].get("stable_features", []))
        stable.update(row["bucket_separation_stability"].get("stable_features", []))
        output[row["target"]] = sorted(stable)[:10]
    return output


def _feature_signs(dataset: pd.DataFrame) -> dict[str, str]:
    if "forward_return_5d" not in dataset.columns:
        return {}
    target = (pd.to_numeric(dataset["forward_return_5d"], errors="coerce") > 0).astype(float)
    signs: dict[str, str] = {}
    for feature in FEATURE_DATA_COLUMNS:
        if feature not in dataset.columns:
            continue
        values = pd.to_numeric(dataset[feature], errors="coerce")
        data = pd.DataFrame({"feature": values, "target": target}).dropna()
        if len(data) < 2 or data["feature"].nunique() <= 1 or data["target"].nunique() <= 1:
            signs[feature] = "flat"
            continue
        corr = data["feature"].corr(data["target"], method="spearman")
        signs[feature] = _sign(corr)
    return signs


def _none_safe_subtract(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return float(left) - float(right)


def _sign(value: float | None, *, epsilon: float = 1e-9) -> str:
    if value is None or pd.isna(value) or abs(float(value)) <= epsilon:
        return "flat"
    return "positive" if float(value) > 0 else "negative"


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


def _safe_float(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def _dedupe(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output
