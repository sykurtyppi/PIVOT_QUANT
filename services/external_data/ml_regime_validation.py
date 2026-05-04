"""Strict train/test validation for the realized_vol_60d regime finding.

This module validates a pre-existing descriptive finding only. It does not
train models, tune thresholds, mutate T9 data, change filters, or make edge
claims.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.ml_boundary_purge import apply_boundary_purge
from services.external_data.ml_candidate_signal import build_candidate_signal_report
from services.external_data.ml_candidate_signal_paper_eval import (
    build_paper_eval_report,
)
from services.external_data.ml_candidate_signal_readiness import build_readiness_checklist
from services.external_data.ml_candidate_signal_sensitivity import build_sensitivity_report
from services.external_data.ml_regime_benchmark import (
    DEFAULT_YEAR_DATASETS,
    _date_max,
    _date_min,
    _dedupe,
    _prepare_frame,
    _read_dataset,
    _read_json,
    _safe_float,
)


DEFAULT_TRAIN_YEARS = ["2023", "2024"]
DEFAULT_TEST_YEAR = "2025"
TREND_FEATURE = "price_momentum_20d"
EXPLANATORY_VARIABLES = [
    "price_momentum_5d",
    "abs_price_momentum_5d",
    "realized_vol_20d",
]


@dataclass(frozen=True)
class MLRegimeValidationResult:
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def run_ml_regime_validation(
    *,
    symbol: str = "SPY",
    year_datasets: list[dict[str, str]] | None = None,
    train_years: list[str] | None = None,
    test_year: str = DEFAULT_TEST_YEAR,
) -> MLRegimeValidationResult:
    yearly_inputs = year_datasets or DEFAULT_YEAR_DATASETS
    loaded = _load_year_frames(yearly_inputs)
    return build_regime_validation_report(
        symbol=symbol,
        year_frames=loaded,
        train_years=train_years or DEFAULT_TRAIN_YEARS,
        test_year=test_year,
    )


def build_regime_validation_report(
    *,
    symbol: str,
    year_frames: list[dict[str, Any]],
    train_years: list[str],
    test_year: str,
) -> MLRegimeValidationResult:
    warnings = [
        "strict validation only; no model training, threshold tuning, filter change, feature addition, or edge claim is performed",
    ]
    train_items = [item for item in year_frames if str(item.get("year")) in {str(year) for year in train_years}]
    test_items = [item for item in year_frames if str(item.get("year")) == str(test_year)]
    missing_train = [year for year in train_years if str(year) not in {str(item.get("year")) for item in train_items}]
    if missing_train:
        warnings.append(f"missing train year(s): {', '.join(map(str, missing_train))}")
    if not test_items:
        warnings.append(f"missing test year: {test_year}")

    train_frame = _concat_ok_frames(train_items)
    test_frame = _concat_ok_frames(test_items)
    train_metadata = _metadata_summary(train_items)
    test_metadata = _metadata_summary(test_items)
    required = ["realized_vol_60d", TREND_FEATURE, "forward_return_5d"]
    missing_columns = sorted(set(_missing_columns(train_frame, required) + _missing_columns(test_frame, required)))
    if missing_columns:
        report = _base_report(
            symbol=symbol,
            train_years=train_years,
            test_year=test_year,
            status="fail",
            validated=False,
            degradation_warning=True,
            warnings=_dedupe(warnings + [f"missing required validation column(s): {', '.join(missing_columns)}"]),
        )
        report["missing_columns"] = missing_columns
        report["train"] = {"metadata": train_metadata}
        report["test"] = {"metadata": test_metadata}
        return MLRegimeValidationResult(report=report)

    train_working = _validation_frame(train_frame, required)
    test_working = _validation_frame(test_frame, required)

    _boundary = apply_boundary_purge(train_working, test_working)
    train_working = _boundary["purged_frame"]
    _boundary_report = _boundary["report"]

    if train_working.empty or test_working.empty:
        empty_parts = []
        if train_working.empty:
            empty_parts.append("train")
        if test_working.empty:
            empty_parts.append("test")
        report = _base_report(
            symbol=symbol,
            train_years=train_years,
            test_year=test_year,
            status="fail",
            validated=False,
            degradation_warning=True,
            warnings=_dedupe(warnings + [f"{' and '.join(empty_parts)} validation frame empty after required columns"]),
        )
        report["train"] = {"metadata": train_metadata, "rows": int(len(train_working))}
        report["test"] = {"metadata": test_metadata, "rows": int(len(test_working))}
        return MLRegimeValidationResult(report=report)

    split_value = pd.to_numeric(train_working["realized_vol_60d"], errors="coerce").median()
    train_groups = _validation_groups(train_working, split_value=split_value)
    test_groups = _validation_groups(test_working, split_value=split_value)
    train_comparisons = _comparisons_to_all_rows(train_groups)
    test_comparisons = _comparisons_to_all_rows(test_groups)
    train_test = _train_test_comparison(train_groups, test_groups, train_comparisons, test_comparisons)
    high_win_survives = _positive(test_comparisons["realized_vol_60d_high"]["positive_rate_delta"])
    low_win_survives = _negative(test_comparisons["realized_vol_60d_low"]["positive_rate_delta"])
    high_mean_survives = _positive(test_comparisons["realized_vol_60d_high"]["mean_forward_return_5d_delta"])
    low_mean_survives = _negative(test_comparisons["realized_vol_60d_low"]["mean_forward_return_5d_delta"])
    train_high_win_present = _positive(train_comparisons["realized_vol_60d_high"]["positive_rate_delta"])
    train_low_win_present = _negative(train_comparisons["realized_vol_60d_low"]["positive_rate_delta"])
    train_high_mean_present = _positive(train_comparisons["realized_vol_60d_high"]["mean_forward_return_5d_delta"])
    train_low_mean_present = _negative(train_comparisons["realized_vol_60d_low"]["mean_forward_return_5d_delta"])
    validated = bool(
        train_high_win_present
        and train_low_win_present
        and train_high_mean_present
        and train_low_mean_present
        and high_win_survives
        and low_win_survives
        and high_mean_survives
        and low_mean_survives
    )
    degradation_warning = not validated
    degradation_metric = _degradation_metric(train_comparisons, test_comparisons)
    status = "pass" if validated else "warn"
    if not all([train_high_win_present, train_low_win_present, train_high_mean_present, train_low_mean_present]):
        warnings.append("train-period realized_vol_60d relationship was not present across both win rate and mean return under the fixed bucket definition")
    if train_high_win_present and not high_win_survives:
        warnings.append("2025 high-vol regime did not preserve the train positive-rate advantage")
    if train_low_win_present and not low_win_survives:
        warnings.append("2025 low-vol regime did not preserve the train positive-rate disadvantage")
    if train_high_mean_present and not high_mean_survives:
        warnings.append("2025 high-vol regime did not preserve the train mean-return advantage")
    if train_low_mean_present and not low_mean_survives:
        warnings.append("2025 low-vol regime did not preserve the train mean-return disadvantage")

    report = _base_report(
        symbol=symbol,
        train_years=train_years,
        test_year=test_year,
        status=status,
        validated=validated,
        degradation_warning=degradation_warning,
        warnings=_dedupe(warnings),
    )

    train_ev = _add_explanatory_variables(train_working)
    test_ev = _add_explanatory_variables(test_working)

    _overextension_fragility = _overextension_fragility_diagnostics(
        train_working=train_working,
        test_working=test_working,
        split_value=split_value,
        test_year=test_year,
    )
    _late_trend_removal = _late_trend_removal_validation(
        train_working=train_working,
        test_working=test_working,
        split_value=split_value,
    )
    _candidate_signal = build_candidate_signal_report(
        train_frame=train_ev,
        test_frame=test_ev,
    )
    _paper_eval = build_paper_eval_report(
        train_frame=train_ev,
        test_frame=test_ev,
    )
    _sensitivity = build_sensitivity_report(
        train_frame=train_ev,
        test_frame=test_ev,
    )

    report.update(
        {
            "regime_definition": {
                "feature": "realized_vol_60d",
                "bucket_logic": "same as PR23: high if realized_vol_60d >= train_period_median, low otherwise",
                "split_source": "train period only",
                "split_value": _safe_float(split_value),
                "train_years": list(train_years),
                "test_year": str(test_year),
                "parameter_changes": False,
            },
            "orthogonal_conditioning": {
                "feature": TREND_FEATURE,
                "bucket_logic": "trend_positive if price_momentum_20d > 0, trend_negative if price_momentum_20d < 0",
                "threshold": 0.0,
                "threshold_tuning_performed": False,
                "zero_trend_rows_excluded": {
                    "train": int((pd.to_numeric(train_working[TREND_FEATURE], errors="coerce") == 0).sum()),
                    "test": int((pd.to_numeric(test_working[TREND_FEATURE], errors="coerce") == 0).sum()),
                },
            },
            "train": {
                "metadata": train_metadata,
                "actual_start_date": _date_min(train_working),
                "actual_end_date": _date_max(train_working),
                "benchmarks": train_groups,
                "comparisons_to_all_rows": train_comparisons,
            },
            "test": {
                "metadata": test_metadata,
                "actual_start_date": _date_min(test_working),
                "actual_end_date": _date_max(test_working),
                "benchmarks": test_groups,
                "comparisons_to_all_rows": test_comparisons,
            },
            "train_vs_test": train_test,
            "degradation_metric": degradation_metric,
            "two_dimensional_conditioning": _two_dimensional_conditioning(
                train_working=train_working,
                test_working=test_working,
                split_value=split_value,
            ),
            "time_slice_robustness": _high_vol_positive_trend_time_slices(
                test_working=test_working,
                split_value=split_value,
            ),
            "failure_explanation_diagnostics": _failure_explanation_diagnostics(
                test_working=test_working,
                split_value=split_value,
            ),
            "vol_regime_change_diagnostics": _vol_regime_change_diagnostics(
                train_working=train_working,
                test_working=test_working,
                split_value=split_value,
            ),
            "trend_maturity_diagnostics": _trend_maturity_diagnostics(
                train_working=train_working,
                test_working=test_working,
                split_value=split_value,
            ),
            "late_trend_filter_impact": _late_trend_filter_impact(
                train_working=train_working,
                test_working=test_working,
                split_value=split_value,
            ),
            "overextension_penalty_comparison": _overextension_penalty_comparison(
                train_working=train_working,
                test_working=test_working,
                split_value=split_value,
            ),
            "overextension_method_comparison": _overextension_method_comparison(
                train_working=train_working,
                test_working=test_working,
                split_value=split_value,
            ),
            "trend_maturity_independence_diagnostics": _trend_maturity_independence_diagnostics(
                train_working=train_working,
                test_working=test_working,
                split_value=split_value,
            ),
            "overextension_fragility_diagnostics": _overextension_fragility,
            "late_trend_removal_validation": _late_trend_removal,
            "candidate_signal_diagnostics": _candidate_signal,
            "paper_eval_diagnostics": _paper_eval,
            "sensitivity_diagnostics": _sensitivity,
            "boundary_purge_report": _boundary_report,
            "candidate_readiness_checklist": build_readiness_checklist(
                late_trend_removal_validation=_late_trend_removal,
                candidate_signal_diagnostics=_candidate_signal,
                overextension_fragility_diagnostics=_overextension_fragility,
                paper_eval_diagnostics=_paper_eval,
                sensitivity_diagnostics=_sensitivity,
                boundary_purge_report=_boundary_report,
            ),
            "signal_survives": validated,
        }
    )
    return MLRegimeValidationResult(report=report)


def write_ml_regime_validation_report(
    report: dict[str, Any],
    *,
    reports_dir: Path | None = None,
    stem: str | None = None,
) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "ml_diagnostics"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    if stem is None:
        years = "-".join(report.get("train_years", []) + [report.get("test_year", "test")])
        stem = f"{report['symbol'].lower()}_{years}_{report['name']}".replace("/", "-")
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _load_year_frames(yearly_inputs: list[dict[str, str]]) -> list[dict[str, Any]]:
    year_frames: list[dict[str, Any]] = []
    for item in yearly_inputs:
        dataset_path = Path(item["dataset_path"]).expanduser().resolve()
        metadata_path = Path(item.get("metadata_path", "")).expanduser().resolve() if item.get("metadata_path") else None
        if not dataset_path.exists():
            year_frames.append(
                {
                    "year": str(item["year"]),
                    "status": "missing",
                    "dataset_path": str(dataset_path),
                    "metadata_path": str(metadata_path) if metadata_path else None,
                    "reason": "dataset file missing",
                }
            )
            continue
        year_frames.append(
            {
                "year": str(item["year"]),
                "status": "ok",
                "dataset_path": str(dataset_path),
                "metadata_path": str(metadata_path) if metadata_path and metadata_path.exists() else None,
                "metadata": _read_json(metadata_path) if metadata_path and metadata_path.exists() else {},
                "frame": _read_dataset(dataset_path),
            }
        )
    return year_frames


def _base_report(
    *,
    symbol: str,
    train_years: list[str],
    test_year: str,
    status: str,
    validated: bool,
    degradation_warning: bool,
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "name": "ml_regime_validation",
        "status": status,
        "symbol": symbol.upper(),
        "read_only": True,
        "training_performed": False,
        "hyperparameter_tuning_performed": False,
        "threshold_optimization_performed": False,
        "filter_changes_performed": False,
        "feature_additions_performed": False,
        "performance_claim": False,
        "train_years": list(train_years),
        "test_year": str(test_year),
        "validated": bool(validated),
        "degradation_warning": bool(degradation_warning),
        "warnings": warnings,
        "explicit_warning": "no edge claim",
    }


def _concat_ok_frames(items: list[dict[str, Any]]) -> pd.DataFrame:
    frames = [_prepare_frame(item["frame"]) for item in items if item.get("status") == "ok" and "frame" in item]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values("entry_date").reset_index(drop=True)


def _metadata_summary(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "year": str(item.get("year")),
            "status": item.get("status"),
            "dataset_path": item.get("dataset_path"),
            "metadata_path": item.get("metadata_path"),
            "analysis_start_date": (item.get("metadata") or {}).get("analysis_start_date"),
            "analysis_end_date": (item.get("metadata") or {}).get("analysis_end_date"),
            "reason": item.get("reason"),
        }
        for item in items
    ]


def _missing_columns(frame: pd.DataFrame, required: list[str]) -> list[str]:
    return [column for column in required if column not in frame.columns]


def _validation_frame(frame: pd.DataFrame, required: list[str]) -> pd.DataFrame:
    if frame.empty:
        return frame
    working = frame.copy()
    for column in required:
        working[column] = pd.to_numeric(working[column], errors="coerce")
    return working.dropna(subset=required).reset_index(drop=True)


def _validation_groups(frame: pd.DataFrame, *, split_value: float) -> dict[str, dict[str, Any]]:
    values = pd.to_numeric(frame["realized_vol_60d"], errors="coerce")
    groups = {
        "all_rows": frame,
        "realized_vol_60d_high": frame[values >= split_value],
        "realized_vol_60d_low": frame[values < split_value],
    }
    return {name: _validation_stats(name, group) for name, group in groups.items()}


def _two_dimensional_conditioning(
    *,
    train_working: pd.DataFrame,
    test_working: pd.DataFrame,
    split_value: float,
) -> dict[str, Any]:
    train_buckets = _two_dimensional_buckets(train_working, split_value=split_value)
    test_buckets = _two_dimensional_buckets(test_working, split_value=split_value)
    train_baseline = _validation_stats("all_rows", train_working)
    test_baseline = _validation_stats("all_rows", test_working)
    train_with_deltas = _bucket_deltas_vs_baseline(train_buckets, train_baseline)
    test_with_deltas = _bucket_deltas_vs_baseline(test_buckets, test_baseline)
    comparison = _bucket_train_test_comparison(train_with_deltas, test_with_deltas)
    return {
        "status": "ok",
        "purpose": "diagnose when the fixed realized_vol_60d relationship works or fails using one orthogonal observable condition",
        "vol_definition": {
            "feature": "realized_vol_60d",
            "bucket_logic": "same as PR23/PR24 fixed train-period median split",
            "split_value": _safe_float(split_value),
        },
        "trend_definition": {
            "feature": TREND_FEATURE,
            "bucket_logic": "positive if > 0, negative if < 0",
            "threshold": 0.0,
            "parameter_optimization_performed": False,
        },
        "train_buckets": train_with_deltas,
        "test_buckets": test_with_deltas,
        "train_vs_test": comparison,
        "worked_in_train": [
            name for name, bucket in train_with_deltas.items() if bucket.get("worked_in_train")
        ],
        "survived_in_test": [
            name for name, bucket in comparison.items() if bucket.get("survived_in_test")
        ],
        "stable_buckets": [
            name for name, bucket in comparison.items() if bucket.get("stable_bucket")
        ],
        "warnings": _two_dimensional_warnings(train_with_deltas, test_with_deltas),
    }


def _two_dimensional_buckets(frame: pd.DataFrame, *, split_value: float) -> dict[str, dict[str, Any]]:
    vol = pd.to_numeric(frame["realized_vol_60d"], errors="coerce")
    trend = pd.to_numeric(frame[TREND_FEATURE], errors="coerce")
    masks = {
        "high_vol_trend_positive": (vol >= split_value) & (trend > 0),
        "high_vol_trend_negative": (vol >= split_value) & (trend < 0),
        "low_vol_trend_positive": (vol < split_value) & (trend > 0),
        "low_vol_trend_negative": (vol < split_value) & (trend < 0),
    }
    return {
        name: _validation_stats(name, frame[mask].copy())
        for name, mask in masks.items()
    }


def _bucket_deltas_vs_baseline(
    buckets: dict[str, dict[str, Any]],
    baseline: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for name, bucket in buckets.items():
        positive_delta = _none_safe_subtract(bucket.get("positive_rate_5d"), baseline.get("positive_rate_5d"))
        mean_delta = _none_safe_subtract(bucket.get("mean_forward_return_5d"), baseline.get("mean_forward_return_5d"))
        enriched = dict(bucket)
        enriched["delta_vs_baseline"] = {
            "positive_rate_delta": positive_delta,
            "mean_forward_return_5d_delta": mean_delta,
            "sample_size_delta": int(bucket.get("sample_size", 0) - baseline.get("sample_size", 0)),
        }
        enriched["worked_in_train"] = bool(_positive(positive_delta) and _positive(mean_delta))
        output[name] = enriched
    return output


def _bucket_train_test_comparison(
    train_buckets: dict[str, dict[str, Any]],
    test_buckets: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for name, train_bucket in train_buckets.items():
        test_bucket = test_buckets.get(name, {})
        train_delta = train_bucket.get("delta_vs_baseline", {})
        test_delta = test_bucket.get("delta_vs_baseline", {})
        win_preserved = _direction_preserved(train_delta.get("positive_rate_delta"), test_delta.get("positive_rate_delta"))
        mean_preserved = _direction_preserved(train_delta.get("mean_forward_return_5d_delta"), test_delta.get("mean_forward_return_5d_delta"))
        survived = bool(
            train_bucket.get("worked_in_train")
            and _positive(test_delta.get("positive_rate_delta"))
            and _positive(test_delta.get("mean_forward_return_5d_delta"))
        )
        output[name] = {
            "train_sample_size": train_bucket.get("sample_size"),
            "test_sample_size": test_bucket.get("sample_size"),
            "train_positive_rate_delta": train_delta.get("positive_rate_delta"),
            "test_positive_rate_delta": test_delta.get("positive_rate_delta"),
            "train_mean_return_delta": train_delta.get("mean_forward_return_5d_delta"),
            "test_mean_return_delta": test_delta.get("mean_forward_return_5d_delta"),
            "positive_rate_degradation": _none_safe_subtract(
                test_delta.get("positive_rate_delta"),
                train_delta.get("positive_rate_delta"),
            ),
            "mean_return_degradation": _none_safe_subtract(
                test_delta.get("mean_forward_return_5d_delta"),
                train_delta.get("mean_forward_return_5d_delta"),
            ),
            "win_rate_direction_preserved": win_preserved,
            "mean_return_direction_preserved": mean_preserved,
            "worked_in_train": bool(train_bucket.get("worked_in_train")),
            "survived_in_test": survived,
            "stable_bucket": bool(survived and win_preserved and mean_preserved),
        }
    return output


def _two_dimensional_warnings(
    train_buckets: dict[str, dict[str, Any]],
    test_buckets: dict[str, dict[str, Any]],
) -> list[str]:
    warnings: list[str] = []
    for period, buckets in [("train", train_buckets), ("test", test_buckets)]:
        empty = [name for name, bucket in buckets.items() if bucket.get("sample_size") == 0]
        if empty:
            warnings.append(f"{period} 2D bucket(s) have zero rows: {', '.join(empty)}")
    return warnings


def _high_vol_positive_trend_time_slices(
    *,
    test_working: pd.DataFrame,
    split_value: float,
) -> dict[str, Any]:
    if test_working.empty or "entry_date" not in test_working.columns:
        return {
            "status": "missing",
            "bucket": "high_vol_trend_positive",
            "reason": "test frame missing entry_date",
            "robust_across_time": False,
            "slice_instability_warning": True,
        }
    working = test_working.copy()
    working["entry_date"] = pd.to_datetime(working["entry_date"], errors="coerce")
    working = working.dropna(subset=["entry_date"])
    if working.empty:
        return {
            "status": "empty",
            "bucket": "high_vol_trend_positive",
            "reason": "test frame has no valid entry_date values",
            "robust_across_time": False,
            "slice_instability_warning": True,
        }
    slice_year = int(working["entry_date"].dt.year.mode().iloc[0])
    vol = pd.to_numeric(working["realized_vol_60d"], errors="coerce")
    trend = pd.to_numeric(working[TREND_FEATURE], errors="coerce")
    bucket_frame = working[(vol >= split_value) & (trend > 0)].copy()
    max_quarter = int(working["entry_date"].dt.quarter.max())
    slices = []
    for quarter in range(1, max_quarter + 1):
        quarter_frame = bucket_frame[bucket_frame["entry_date"].dt.quarter == quarter].copy()
        stats = _validation_stats(f"2025_Q{quarter}", quarter_frame)
        positive_slice = bool(
            stats["sample_size"] > 0
            and stats["positive_rate_5d"] is not None
            and stats["positive_rate_5d"] > 0.5
            and stats["mean_forward_return_5d"] is not None
            and stats["mean_forward_return_5d"] > 0
        )
        slices.append(
            {
                "slice": f"{slice_year}_Q{quarter}",
                "start_date": _quarter_start(slice_year, quarter),
                "end_date": _quarter_end(slice_year, quarter),
                "sample_size": stats["sample_size"],
                "win_rate_5d": stats["positive_rate_5d"],
                "mean_return_5d": stats["mean_forward_return_5d"],
                "positive_slice": positive_slice,
            }
        )
    positive_count = sum(1 for item in slices if item["positive_slice"])
    zero_row_count = sum(1 for item in slices if item["sample_size"] == 0)
    mean_returns = [item["mean_return_5d"] for item in slices if item["mean_return_5d"] is not None]
    variance = _safe_float(pd.Series(mean_returns, dtype="float64").var(ddof=0)) if mean_returns else None
    robust = bool(slices and zero_row_count == 0 and positive_count == len(slices))
    return {
        "status": "ok",
        "bucket": "high_vol_trend_positive",
        "purpose": "check whether the fixed high-vol plus positive 20d trend diagnostic is concentrated in one 2025 period",
        "definitions": {
            "vol_feature": "realized_vol_60d",
            "vol_split": "fixed train-period median from PR24/PR25",
            "vol_split_value": _safe_float(split_value),
            "trend_feature": TREND_FEATURE,
            "trend_split": "positive if > 0",
            "parameter_optimization_performed": False,
        },
        "per_slice": slices,
        "consistency_summary": {
            "slice_count": int(len(slices)),
            "positive_slice_count": int(positive_count),
            "zero_row_slice_count": int(zero_row_count),
            "consistency_score": f"{positive_count}/{len(slices)}" if slices else "0/0",
            "consistency_ratio": _safe_float(positive_count / len(slices)) if slices else None,
            "mean_return_5d_variance_across_slices": variance,
        },
        "robust_across_time": robust,
        "slice_instability_warning": not robust,
    }


def _failure_explanation_diagnostics(
    *,
    test_working: pd.DataFrame,
    split_value: float,
) -> dict[str, Any]:
    if test_working.empty or "entry_date" not in test_working.columns:
        return {
            "status": "missing",
            "reason": "test frame missing entry_date",
            "candidate_explanatory_variable": False,
        }
    working = _add_explanatory_variables(test_working)
    vol = pd.to_numeric(working["realized_vol_60d"], errors="coerce")
    trend = pd.to_numeric(working[TREND_FEATURE], errors="coerce")
    bucket = working[(vol >= split_value) & (trend > 0)].copy()
    bucket["entry_date"] = pd.to_datetime(bucket["entry_date"], errors="coerce")
    bucket = bucket.dropna(subset=["entry_date"])
    failing = bucket[bucket["entry_date"].dt.quarter == 1].copy()
    working_slices = bucket[bucket["entry_date"].dt.quarter.isin([2, 3, 4])].copy()
    variable_rows = []
    material_variables = []
    for variable in EXPLANATORY_VARIABLES:
        row = _variable_failure_comparison(variable, failing=failing, working=working_slices)
        variable_rows.append(row)
        if row["material_difference"]:
            material_variables.append(variable)
    return {
        "status": "ok",
        "purpose": "describe observable differences between failing Q1 2025 and working Q2-Q4 2025 inside the fixed high_vol_trend_positive bucket",
        "bucket": "high_vol_trend_positive",
        "failing_slice": "2025_Q1",
        "working_slices": ["2025_Q2", "2025_Q3", "2025_Q4"],
        "variables": EXPLANATORY_VARIABLES,
        "table": variable_rows,
        "materially_different_variables": material_variables,
        "candidate_explanatory_variable": bool(material_variables),
        "notes": [
            "diagnostic comparison only; no filtering or parameter optimization is performed",
            "realized_vol_20d is computed from trailing/current underlying_price rows and may be unavailable where 20 prior rows are not present",
        ],
    }


def _add_explanatory_variables(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output["entry_date"] = pd.to_datetime(output["entry_date"], errors="coerce")
    output = output.sort_values("entry_date").reset_index(drop=True)
    if "price_momentum_5d" in output.columns:
        momentum_5d = pd.to_numeric(output["price_momentum_5d"], errors="coerce")
        output["abs_price_momentum_5d"] = momentum_5d.abs()
    else:
        output["abs_price_momentum_5d"] = pd.NA
    if "realized_vol_20d" in output.columns:
        output["realized_vol_20d"] = pd.to_numeric(output["realized_vol_20d"], errors="coerce")
    elif "underlying_price" in output.columns:
        prices = pd.to_numeric(output["underlying_price"], errors="coerce")
        returns = prices.pct_change()
        output["realized_vol_20d"] = returns.rolling(20, min_periods=20).std() * (252 ** 0.5)
    else:
        output["realized_vol_20d"] = pd.NA
    output["vol_regime_change"] = pd.to_numeric(output["realized_vol_20d"], errors="coerce") - pd.to_numeric(
        output["realized_vol_60d"],
        errors="coerce",
    )
    if "distance_from_20d_mean" in output.columns:
        output["distance_from_20d_mean"] = pd.to_numeric(output["distance_from_20d_mean"], errors="coerce")
        output["trend_maturity_source"] = "distance_from_20d_mean"
    elif "underlying_price" in output.columns:
        prices = pd.to_numeric(output["underlying_price"], errors="coerce")
        rolling_mean = prices.rolling(20, min_periods=20).mean()
        rolling_std = prices.rolling(20, min_periods=20).std(ddof=0)
        output["distance_from_20d_mean"] = (prices - rolling_mean) / rolling_std.replace(0, pd.NA)
        output["trend_maturity_source"] = "distance_from_20d_mean"
    else:
        output["distance_from_20d_mean"] = pd.NA
        output["trend_maturity_source"] = "unavailable"
    return output


def _vol_regime_change_diagnostics(
    *,
    train_working: pd.DataFrame,
    test_working: pd.DataFrame,
    split_value: float,
) -> dict[str, Any]:
    train_bucket = _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(train_working), split_value=split_value)
    test_bucket = _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(test_working), split_value=split_value)
    train_table = _vol_regime_table(train_bucket)
    test_table = _vol_regime_table(test_bucket)
    q1_table = _vol_regime_table(_slice_by_quarters(test_bucket, [1]))
    q2_q4_table = _vol_regime_table(_slice_by_quarters(test_bucket, [2, 3, 4]))
    train_comparison = _expansion_vs_compression(train_table)
    test_comparison = _expansion_vs_compression(test_table)
    q1_vs_working = _q1_vs_working_comparison(q1_table, q2_q4_table)
    explains_failure = _vol_expansion_explains_failure(
        test_comparison=test_comparison,
        q1_vs_working=q1_vs_working,
    )
    return {
        "status": "ok",
        "purpose": "diagnose whether volatility expansion versus compression explains high_vol_trend_positive failure periods",
        "bucket": "high_vol_trend_positive",
        "definitions": {
            "vol_regime_change": "realized_vol_20d - realized_vol_60d",
            "vol_expansion": "vol_regime_change > 0",
            "vol_compression": "vol_regime_change <= 0",
            "parameter_optimization_performed": False,
            "filter_changes_performed": False,
        },
        "train": {
            "table": train_table,
            "expansion_vs_compression": train_comparison,
        },
        "test": {
            "table": test_table,
            "expansion_vs_compression": test_comparison,
        },
        "q1_vs_q2_q4": {
            "failing_slice": "2025_Q1",
            "working_slices": ["2025_Q2", "2025_Q3", "2025_Q4"],
            "q1_table": q1_table,
            "q2_q4_table": q2_q4_table,
            "comparison": q1_vs_working,
        },
        "vol_expansion_explains_failure": explains_failure,
        "notes": [
            "diagnostic only; failing periods are retained",
            "realized_vol_20d is preserved if present, otherwise computed from trailing/current underlying_price rows",
        ],
    }


def _fixed_high_vol_positive_trend_bucket(frame: pd.DataFrame, *, split_value: float) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    output = frame.copy()
    output["entry_date"] = pd.to_datetime(output["entry_date"], errors="coerce")
    vol = pd.to_numeric(output["realized_vol_60d"], errors="coerce")
    trend = pd.to_numeric(output[TREND_FEATURE], errors="coerce")
    return output[(vol >= split_value) & (trend > 0)].copy()


def _vol_regime_table(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
    change = pd.to_numeric(frame.get("vol_regime_change", pd.Series(dtype="float64")), errors="coerce")
    expansion = frame[change > 0].copy()
    compression = frame[change <= 0].copy()
    return {
        "vol_expansion": _vol_regime_stats("vol_expansion", expansion),
        "vol_compression": _vol_regime_stats("vol_compression", compression),
        "all_evaluable": _vol_regime_stats("all_evaluable", frame[change.notna()].copy()),
        "missing_vol_regime_change_count": int(change.isna().sum()),
    }


def _vol_regime_stats(name: str, frame: pd.DataFrame) -> dict[str, Any]:
    stats = _validation_stats(name, frame)
    change = pd.to_numeric(frame.get("vol_regime_change", pd.Series(dtype="float64")), errors="coerce").dropna()
    return {
        **stats,
        "mean_vol_regime_change": _safe_float(change.mean()) if not change.empty else None,
        "median_vol_regime_change": _safe_float(change.median()) if not change.empty else None,
    }


def _expansion_vs_compression(table: dict[str, Any]) -> dict[str, Any]:
    expansion = table["vol_expansion"]
    compression = table["vol_compression"]
    return {
        "sample_size_delta": int(expansion.get("sample_size", 0) - compression.get("sample_size", 0)),
        "win_rate_delta": _none_safe_subtract(expansion.get("positive_rate_5d"), compression.get("positive_rate_5d")),
        "mean_return_delta": _none_safe_subtract(expansion.get("mean_forward_return_5d"), compression.get("mean_forward_return_5d")),
    }


def _slice_by_quarters(frame: pd.DataFrame, quarters: list[int]) -> pd.DataFrame:
    if frame.empty or "entry_date" not in frame.columns:
        return frame.copy()
    dates = pd.to_datetime(frame["entry_date"], errors="coerce")
    return frame[dates.dt.quarter.isin(quarters)].copy()


def _q1_vs_working_comparison(
    q1_table: dict[str, Any],
    q2_q4_table: dict[str, Any],
) -> dict[str, Any]:
    q1_all = q1_table["all_evaluable"]
    working_all = q2_q4_table["all_evaluable"]
    return {
        "q1_expansion_share": _regime_share(q1_table, "vol_expansion"),
        "q2_q4_expansion_share": _regime_share(q2_q4_table, "vol_expansion"),
        "q1_minus_q2_q4_win_rate": _none_safe_subtract(q1_all.get("positive_rate_5d"), working_all.get("positive_rate_5d")),
        "q1_minus_q2_q4_mean_return": _none_safe_subtract(q1_all.get("mean_forward_return_5d"), working_all.get("mean_forward_return_5d")),
        "q1_expansion_mean_return": q1_table["vol_expansion"].get("mean_forward_return_5d"),
        "q1_compression_mean_return": q1_table["vol_compression"].get("mean_forward_return_5d"),
        "q2_q4_expansion_mean_return": q2_q4_table["vol_expansion"].get("mean_forward_return_5d"),
        "q2_q4_compression_mean_return": q2_q4_table["vol_compression"].get("mean_forward_return_5d"),
    }


def _regime_share(table: dict[str, Any], regime_name: str) -> float | None:
    total = table["all_evaluable"].get("sample_size", 0)
    if total <= 0:
        return None
    return _safe_float(table[regime_name].get("sample_size", 0) / total)


def _vol_expansion_explains_failure(
    *,
    test_comparison: dict[str, Any],
    q1_vs_working: dict[str, Any],
) -> bool:
    expansion_better = bool(
        test_comparison.get("win_rate_delta") is not None
        and test_comparison.get("mean_return_delta") is not None
        and test_comparison["win_rate_delta"] > 0
        and test_comparison["mean_return_delta"] > 0
    )
    q1_less_expansion = bool(
        q1_vs_working.get("q1_expansion_share") is not None
        and q1_vs_working.get("q2_q4_expansion_share") is not None
        and q1_vs_working["q1_expansion_share"] < q1_vs_working["q2_q4_expansion_share"]
    )
    q1_underperformed = bool(
        q1_vs_working.get("q1_minus_q2_q4_win_rate") is not None
        and q1_vs_working.get("q1_minus_q2_q4_mean_return") is not None
        and q1_vs_working["q1_minus_q2_q4_win_rate"] < 0
        and q1_vs_working["q1_minus_q2_q4_mean_return"] < 0
    )
    return bool(expansion_better and q1_less_expansion and q1_underperformed)


def _trend_maturity_diagnostics(
    *,
    train_working: pd.DataFrame,
    test_working: pd.DataFrame,
    split_value: float,
) -> dict[str, Any]:
    train_bucket = _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(train_working), split_value=split_value)
    test_bucket = _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(test_working), split_value=split_value)
    maturity_values = pd.to_numeric(train_bucket.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce").dropna()
    if maturity_values.empty:
        maturity_feature = TREND_FEATURE
        train_bucket = train_bucket.assign(distance_from_20d_mean=pd.to_numeric(train_bucket[TREND_FEATURE], errors="coerce"))
        test_bucket = test_bucket.assign(distance_from_20d_mean=pd.to_numeric(test_bucket[TREND_FEATURE], errors="coerce"))
        maturity_values = pd.to_numeric(train_bucket["distance_from_20d_mean"], errors="coerce").dropna()
    else:
        maturity_feature = "distance_from_20d_mean"
    threshold = maturity_values.quantile(0.70) if not maturity_values.empty else None
    train_table = _trend_maturity_table(train_bucket, threshold=threshold)
    test_table = _trend_maturity_table(test_bucket, threshold=threshold)
    train_comparison = _late_vs_early(train_table)
    test_comparison = _late_vs_early(test_table)
    q1_table = _trend_maturity_table(_slice_by_quarters(test_bucket, [1]), threshold=threshold)
    q2_q4_table = _trend_maturity_table(_slice_by_quarters(test_bucket, [2, 3, 4]), threshold=threshold)
    q1_vs_working = _trend_maturity_q1_vs_working(q1_table, q2_q4_table)
    time_stability = _trend_maturity_time_stability(test_bucket, threshold=threshold)
    explains_failure = _trend_maturity_explains_failure(
        test_comparison=test_comparison,
        q1_vs_working=q1_vs_working,
    )
    return {
        "status": "ok" if threshold is not None else "missing",
        "purpose": "diagnose whether trend maturity or overextension explains high_vol_trend_positive failure periods",
        "bucket": "high_vol_trend_positive",
        "definitions": {
            "preferred_variable": "distance_from_20d_mean = (price - rolling_mean_20d) / rolling_std_20d",
            "fallback_variable": TREND_FEATURE,
            "used_variable": maturity_feature,
            "late_trend": "top 30% of train-bucket maturity values",
            "early_trend": "remaining 70% of train-bucket maturity values",
            "threshold_source": "train high_vol_trend_positive bucket only",
            "threshold_value": _safe_float(threshold) if threshold is not None else None,
            "parameter_optimization_performed": False,
            "filter_changes_performed": False,
        },
        "train": {
            "table": train_table,
            "late_vs_early": train_comparison,
        },
        "test": {
            "table": test_table,
            "late_vs_early": test_comparison,
        },
        "q1_vs_q2_q4": {
            "failing_slice": "2025_Q1",
            "working_slices": ["2025_Q2", "2025_Q3", "2025_Q4"],
            "q1_table": q1_table,
            "q2_q4_table": q2_q4_table,
            "comparison": q1_vs_working,
        },
        "time_stability": time_stability,
        "trend_maturity_explains_failure": explains_failure,
        "trend_maturity_stable": time_stability["trend_maturity_stable"],
        "notes": [
            "diagnostic only; no overextension filter is introduced",
            "late/early split is fixed from the train bucket and applied unchanged to test",
        ],
    }


def _trend_maturity_table(frame: pd.DataFrame, *, threshold: float | None) -> dict[str, Any]:
    maturity = pd.to_numeric(frame.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce")
    if threshold is None:
        late = frame.iloc[0:0].copy()
        early = frame.iloc[0:0].copy()
    else:
        late = frame[maturity >= threshold].copy()
        early = frame[maturity < threshold].copy()
    return {
        "late_trend": _trend_maturity_stats("late_trend", late),
        "early_trend": _trend_maturity_stats("early_trend", early),
        "all_evaluable": _trend_maturity_stats("all_evaluable", frame[maturity.notna()].copy()),
        "missing_trend_maturity_count": int(maturity.isna().sum()),
    }


def _trend_maturity_stats(name: str, frame: pd.DataFrame) -> dict[str, Any]:
    stats = _validation_stats(name, frame)
    maturity = pd.to_numeric(frame.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce").dropna()
    return {
        **stats,
        "mean_trend_maturity": _safe_float(maturity.mean()) if not maturity.empty else None,
        "median_trend_maturity": _safe_float(maturity.median()) if not maturity.empty else None,
    }


def _late_vs_early(table: dict[str, Any]) -> dict[str, Any]:
    late = table["late_trend"]
    early = table["early_trend"]
    return {
        "sample_size_delta": int(late.get("sample_size", 0) - early.get("sample_size", 0)),
        "win_rate_delta": _none_safe_subtract(late.get("positive_rate_5d"), early.get("positive_rate_5d")),
        "mean_return_delta": _none_safe_subtract(late.get("mean_forward_return_5d"), early.get("mean_forward_return_5d")),
    }


def _trend_maturity_q1_vs_working(
    q1_table: dict[str, Any],
    q2_q4_table: dict[str, Any],
) -> dict[str, Any]:
    q1_all = q1_table["all_evaluable"]
    working_all = q2_q4_table["all_evaluable"]
    return {
        "q1_late_trend_share": _trend_maturity_share(q1_table, "late_trend"),
        "q2_q4_late_trend_share": _trend_maturity_share(q2_q4_table, "late_trend"),
        "q1_minus_q2_q4_win_rate": _none_safe_subtract(q1_all.get("positive_rate_5d"), working_all.get("positive_rate_5d")),
        "q1_minus_q2_q4_mean_return": _none_safe_subtract(q1_all.get("mean_forward_return_5d"), working_all.get("mean_forward_return_5d")),
        "q1_late_trend_mean_return": q1_table["late_trend"].get("mean_forward_return_5d"),
        "q1_early_trend_mean_return": q1_table["early_trend"].get("mean_forward_return_5d"),
        "q2_q4_late_trend_mean_return": q2_q4_table["late_trend"].get("mean_forward_return_5d"),
        "q2_q4_early_trend_mean_return": q2_q4_table["early_trend"].get("mean_forward_return_5d"),
    }


def _trend_maturity_share(table: dict[str, Any], bucket_name: str) -> float | None:
    total = table["all_evaluable"].get("sample_size", 0)
    if total <= 0:
        return None
    return _safe_float(table[bucket_name].get("sample_size", 0) / total)


def _trend_maturity_explains_failure(
    *,
    test_comparison: dict[str, Any],
    q1_vs_working: dict[str, Any],
) -> bool:
    late_worse = bool(
        test_comparison.get("win_rate_delta") is not None
        and test_comparison.get("mean_return_delta") is not None
        and test_comparison["win_rate_delta"] < 0
        and test_comparison["mean_return_delta"] < 0
    )
    q1_more_late = bool(
        q1_vs_working.get("q1_late_trend_share") is not None
        and q1_vs_working.get("q2_q4_late_trend_share") is not None
        and q1_vs_working["q1_late_trend_share"] > q1_vs_working["q2_q4_late_trend_share"]
    )
    q1_underperformed = bool(
        q1_vs_working.get("q1_minus_q2_q4_win_rate") is not None
        and q1_vs_working.get("q1_minus_q2_q4_mean_return") is not None
        and q1_vs_working["q1_minus_q2_q4_win_rate"] < 0
        and q1_vs_working["q1_minus_q2_q4_mean_return"] < 0
    )
    return bool(late_worse and q1_more_late and q1_underperformed)


def _trend_maturity_time_stability(frame: pd.DataFrame, *, threshold: float | None) -> dict[str, Any]:
    if frame.empty or "entry_date" not in frame.columns:
        return {
            "status": "missing",
            "per_quarter": [],
            "quarters_consistent": 0,
            "total_quarters": 0,
            "consistency_ratio": None,
            "trend_maturity_stable": False,
        }
    working = frame.copy()
    working["entry_date"] = pd.to_datetime(working["entry_date"], errors="coerce")
    working = working.dropna(subset=["entry_date"])
    if working.empty:
        return {
            "status": "empty",
            "per_quarter": [],
            "quarters_consistent": 0,
            "total_quarters": 0,
            "consistency_ratio": None,
            "trend_maturity_stable": False,
        }
    slice_year = int(working["entry_date"].dt.year.mode().iloc[0])
    max_quarter = int(working["entry_date"].dt.quarter.max())
    rows = []
    for quarter in range(1, max_quarter + 1):
        quarter_frame = working[working["entry_date"].dt.quarter == quarter].copy()
        table = _trend_maturity_table(quarter_frame, threshold=threshold)
        late = table["late_trend"]
        early = table["early_trend"]
        difference = _none_safe_subtract(late.get("positive_rate_5d"), early.get("positive_rate_5d"))
        early_outperforms = difference is not None and difference < 0
        sample_size = int(late.get("sample_size", 0) + early.get("sample_size", 0))
        rows.append(
            {
                "quarter": f"{slice_year}_Q{quarter}",
                "late_win_rate_5d": late.get("positive_rate_5d"),
                "early_win_rate_5d": early.get("positive_rate_5d"),
                "difference_late_minus_early": difference,
                "sample_size": sample_size,
                "late_sample_size": late.get("sample_size"),
                "early_sample_size": early.get("sample_size"),
                "early_outperforms_late": early_outperforms,
            }
        )
    evaluable = [
        row for row in rows
        if row["difference_late_minus_early"] is not None and row["late_sample_size"] > 0 and row["early_sample_size"] > 0
    ]
    quarters_consistent = sum(1 for row in evaluable if row["early_outperforms_late"])
    total_quarters = len(rows)
    consistency_ratio = quarters_consistent / total_quarters if total_quarters else None
    stable = bool(total_quarters >= 4 and quarters_consistent >= 3)
    return {
        "status": "ok",
        "definition": "early_trend outperforms late_trend when late win rate minus early win rate is negative",
        "criteria": "trend_maturity_stable is true when early_trend outperforms late_trend in at least 3 of 4 test quarters",
        "per_quarter": rows,
        "quarters_consistent": int(quarters_consistent),
        "total_quarters": int(total_quarters),
        "consistency_ratio": _safe_float(consistency_ratio),
        "trend_maturity_stable": stable,
    }


def _late_trend_filter_impact(
    *,
    train_working: pd.DataFrame,
    test_working: pd.DataFrame,
    split_value: float,
) -> dict[str, Any]:
    train_bucket = _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(train_working), split_value=split_value)
    test_bucket = _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(test_working), split_value=split_value)
    maturity_values = pd.to_numeric(train_bucket.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce").dropna()
    threshold = maturity_values.quantile(0.70) if not maturity_values.empty else None
    train_table = _late_trend_filter_impact_table(train_bucket, threshold=threshold)
    test_table = _late_trend_filter_impact_table(test_bucket, threshold=threshold)
    train_improves = _filter_improves(train_table["delta"])
    test_improves = _filter_improves(test_table["delta"])
    return {
        "status": "ok" if threshold is not None else "missing",
        "purpose": "measure the impact of excluding late_trend rows inside the fixed high_vol_trend_positive bucket",
        "bucket": "high_vol_trend_positive",
        "definitions": {
            "trend_maturity_variable": "distance_from_20d_mean",
            "late_trend": "top 30% of train-bucket maturity values, same as trend_maturity_diagnostics",
            "early_trend_only": "rows below the train-bucket late_trend threshold",
            "threshold_source": "train high_vol_trend_positive bucket only",
            "threshold_value": _safe_float(threshold) if threshold is not None else None,
            "signal_redefinition_performed": False,
            "threshold_optimization_performed": False,
            "filter_changes_performed": False,
        },
        "train": train_table,
        "test": test_table,
        "filter_improves_performance": bool(train_improves and test_improves),
        "notes": [
            "diagnostic measurement only; no live or research signal definition is changed",
            "early_trend_only uses the existing late_trend threshold from the train bucket without optimization",
        ],
    }


def _late_trend_filter_impact_table(frame: pd.DataFrame, *, threshold: float | None) -> dict[str, Any]:
    maturity = pd.to_numeric(frame.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce")
    valid_maturity = maturity.notna()
    baseline = frame[valid_maturity].copy()
    early = frame[valid_maturity & (maturity < threshold)].copy() if threshold is not None else frame.iloc[0:0].copy()
    baseline_stats = _filter_impact_stats("baseline_no_filter", baseline)
    early_stats = _filter_impact_stats("early_trend_only", early)
    return {
        "table": {
            "baseline_no_filter": baseline_stats,
            "early_trend_only": early_stats,
        },
        "delta": {
            "delta_win_rate": _none_safe_subtract(early_stats.get("win_rate_5d"), baseline_stats.get("win_rate_5d")),
            "delta_mean_return": _none_safe_subtract(early_stats.get("mean_return_5d"), baseline_stats.get("mean_return_5d")),
            "rows_removed": int(baseline_stats.get("sample_size", 0) - early_stats.get("sample_size", 0)),
        },
        "missing_trend_maturity_count": int(maturity.isna().sum()),
    }


def _filter_impact_stats(name: str, frame: pd.DataFrame) -> dict[str, Any]:
    stats = _validation_stats(name, frame)
    return {
        "scenario": name,
        "sample_size": stats["sample_size"],
        "win_rate_5d": stats["positive_rate_5d"],
        "mean_return_5d": stats["mean_forward_return_5d"],
    }


def _filter_improves(delta: dict[str, Any]) -> bool:
    return bool(
        delta.get("delta_win_rate") is not None
        and delta.get("delta_mean_return") is not None
        and delta["delta_win_rate"] > 0
        and delta["delta_mean_return"] > 0
    )


def _overextension_penalty_comparison(
    *,
    train_working: pd.DataFrame,
    test_working: pd.DataFrame,
    split_value: float,
) -> dict[str, Any]:
    train_bucket = _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(train_working), split_value=split_value)
    test_bucket = _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(test_working), split_value=split_value)
    maturity_values = pd.to_numeric(train_bucket.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce").dropna()
    threshold = maturity_values.quantile(0.70) if not maturity_values.empty else None
    train_table = _overextension_penalty_table(train_bucket, threshold=threshold)
    test_table = _overextension_penalty_table(test_bucket, threshold=threshold)
    train_preferred = _soft_penalty_preferred(train_table)
    test_preferred = _soft_penalty_preferred(test_table)
    return {
        "status": "ok" if threshold is not None else "missing",
        "purpose": "compare no adjustment, hard late-trend removal, and fixed half-weight late-trend penalty",
        "bucket": "high_vol_trend_positive",
        "definitions": {
            "trend_maturity_variable": "distance_from_20d_mean",
            "late_trend": "top 30% of train-bucket maturity values, same as trend_maturity_diagnostics",
            "threshold_source": "train high_vol_trend_positive bucket only",
            "threshold_value": _safe_float(threshold) if threshold is not None else None,
            "early_trend_weight": 1.0,
            "late_trend_weight": 0.5,
            "signal_definition_change_performed": False,
            "threshold_optimization_performed": False,
            "filter_changes_performed": False,
        },
        "train": train_table,
        "test": test_table,
        "soft_penalty_preferred": bool(train_preferred and test_preferred),
        "preference_criteria": "true only when soft penalty improves win rate and mean return versus baseline, keeps all raw rows, has larger effective sample than hard filter, and retains at least half of hard-filter improvement in both train and test",
        "notes": [
            "diagnostic only; soft weights are not applied to live scoring or research filters",
            "late-trend rows are retained at weight 0.5 only for this measurement block",
        ],
    }


def _overextension_penalty_table(frame: pd.DataFrame, *, threshold: float | None) -> dict[str, Any]:
    maturity = pd.to_numeric(frame.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce")
    valid_maturity = maturity.notna()
    baseline = frame[valid_maturity].copy()
    if threshold is None:
        hard = frame.iloc[0:0].copy()
        weights = pd.Series(dtype="float64")
    else:
        hard = frame[valid_maturity & (maturity < threshold)].copy()
        weights = pd.Series(1.0, index=baseline.index)
        weights.loc[maturity[valid_maturity] >= threshold] = 0.5
    baseline_stats = _weighted_filter_stats("baseline_no_adjustment", baseline, pd.Series(1.0, index=baseline.index))
    hard_stats = _weighted_filter_stats("hard_filter_early_trend_only", hard, pd.Series(1.0, index=hard.index))
    soft_stats = _weighted_filter_stats("soft_penalty_late_trend_half_weight", baseline, weights)
    hard_delta = _scenario_delta(hard_stats, baseline_stats)
    soft_delta = _scenario_delta(soft_stats, baseline_stats)
    return {
        "table": {
            "baseline_no_adjustment": baseline_stats,
            "hard_filter_early_trend_only": hard_stats,
            "soft_penalty_late_trend_half_weight": soft_stats,
        },
        "deltas_vs_baseline": {
            "hard_filter_early_trend_only": hard_delta,
            "soft_penalty_late_trend_half_weight": soft_delta,
        },
        "soft_retention_vs_hard_filter": {
            "win_rate_retention": _retention_ratio(soft_delta.get("delta_win_rate"), hard_delta.get("delta_win_rate")),
            "mean_return_retention": _retention_ratio(soft_delta.get("delta_mean_return"), hard_delta.get("delta_mean_return")),
        },
        "soft_penalty_preferred": _soft_penalty_preferred_from_parts(
            baseline=baseline_stats,
            hard=hard_stats,
            soft=soft_stats,
            hard_delta=hard_delta,
            soft_delta=soft_delta,
        ),
        "missing_trend_maturity_count": int(maturity.isna().sum()),
    }


def _weighted_filter_stats(name: str, frame: pd.DataFrame, weights: pd.Series) -> dict[str, Any]:
    forward_5d = pd.to_numeric(frame.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce")
    working = pd.DataFrame({"forward_return_5d": forward_5d, "weight": weights.reindex(frame.index)}).dropna()
    working = working[working["weight"] > 0]
    if working.empty:
        return {
            "scenario": name,
            "sample_size": 0,
            "effective_sample_size": 0.0,
            "kish_effective_sample_size": None,
            "weighted_win_rate_5d": None,
            "weighted_mean_return_5d": None,
        }
    total_weight = float(working["weight"].sum())
    squared_weight = float((working["weight"] ** 2).sum())
    return {
        "scenario": name,
        "sample_size": int(len(working)),
        "effective_sample_size": _safe_float(total_weight),
        "kish_effective_sample_size": _safe_float((total_weight ** 2) / squared_weight) if squared_weight > 0 else None,
        "weighted_win_rate_5d": _safe_float(((working["forward_return_5d"] > 0).astype(float) * working["weight"]).sum() / total_weight),
        "weighted_mean_return_5d": _safe_float((working["forward_return_5d"] * working["weight"]).sum() / total_weight),
    }


def _scenario_delta(scenario: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "delta_win_rate": _none_safe_subtract(scenario.get("weighted_win_rate_5d"), baseline.get("weighted_win_rate_5d")),
        "delta_mean_return": _none_safe_subtract(scenario.get("weighted_mean_return_5d"), baseline.get("weighted_mean_return_5d")),
        "effective_sample_size_delta": _none_safe_subtract(scenario.get("effective_sample_size"), baseline.get("effective_sample_size")),
        "raw_sample_size_delta": int(scenario.get("sample_size", 0) - baseline.get("sample_size", 0)),
    }


def _retention_ratio(soft_delta: float | None, hard_delta: float | None) -> float | None:
    if soft_delta is None or hard_delta is None or hard_delta <= 0:
        return None
    return _safe_float(soft_delta / hard_delta)


def _soft_penalty_preferred(table: dict[str, Any]) -> bool:
    return bool(table.get("soft_penalty_preferred"))


def _soft_penalty_preferred_from_parts(
    *,
    baseline: dict[str, Any],
    hard: dict[str, Any],
    soft: dict[str, Any],
    hard_delta: dict[str, Any],
    soft_delta: dict[str, Any],
) -> bool:
    win_retention = _retention_ratio(soft_delta.get("delta_win_rate"), hard_delta.get("delta_win_rate"))
    return_retention = _retention_ratio(soft_delta.get("delta_mean_return"), hard_delta.get("delta_mean_return"))
    return bool(
        soft_delta.get("delta_win_rate") is not None
        and soft_delta.get("delta_mean_return") is not None
        and soft_delta["delta_win_rate"] > 0
        and soft_delta["delta_mean_return"] > 0
        and win_retention is not None
        and return_retention is not None
        and win_retention >= 0.5
        and return_retention >= 0.5
        and soft.get("sample_size") == baseline.get("sample_size")
        and soft.get("effective_sample_size", 0) > hard.get("effective_sample_size", 0)
    )


def _overextension_method_comparison(
    *,
    train_working: pd.DataFrame,
    test_working: pd.DataFrame,
    split_value: float,
) -> dict[str, Any]:
    train_bucket = _add_overextension_variables(
        _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(train_working), split_value=split_value)
    )
    test_bucket = _add_overextension_variables(
        _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(test_working), split_value=split_value)
    )
    method_specs = [
        {
            "method": "distance_from_20d_mean",
            "description": "(price - rolling_mean_20d) / rolling_std_20d",
            "variable": "distance_from_20d_mean",
            "threshold_mode": "top_30_train",
        },
        {
            "method": "bollinger_style",
            "description": "(price - rolling_mean_20d) / (2 * rolling_std_20d)",
            "variable": "bollinger_style_overextension",
            "threshold_mode": "top_30_train",
        },
        {
            "method": "rsi_14",
            "description": "RSI(14), late when RSI > 70",
            "variable": "rsi_14",
            "threshold_mode": "fixed",
            "fixed_threshold": 70.0,
        },
        {
            "method": "atr_14",
            "description": "(price - rolling_mean_20d) / ATR_14",
            "variable": "atr_14_overextension",
            "threshold_mode": "top_30_train",
        },
        {
            "method": "cumulative_return_20d",
            "description": "price_momentum_20d, late when in the top 30% of train bucket",
            "variable": TREND_FEATURE,
            "threshold_mode": "top_30_train",
        },
    ]
    method_rows = []
    for spec in method_specs:
        row = _overextension_method_row(spec, train_bucket=train_bucket, test_bucket=test_bucket)
        method_rows.append(row)
    ranked = _rank_overextension_methods(method_rows)
    current_rank = next(
        (row.get("rank") for row in ranked if row.get("method") == "distance_from_20d_mean"),
        None,
    )
    return {
        "status": "ok",
        "purpose": "compare fixed overextension definitions inside the high_vol_trend_positive bucket without tuning thresholds",
        "bucket": "high_vol_trend_positive",
        "table": ranked,
        "ranking": {
            "best_performing_method": ranked[0]["method"] if ranked else None,
            "most_stable_method": _most_stable_overextension_method(method_rows),
            "current_method": "distance_from_20d_mean",
            "current_method_rank": current_rank,
            "current_method_optimal": bool(current_rank is not None and current_rank <= 2),
            "ranking_method": "average rank across train/test delta win rate and train/test delta mean return; unavailable metrics rank last",
        },
        "definitions": {
            "distance_from_20d_mean": "current method; top 30% overextended threshold from train bucket only",
            "bollinger_style": "distance_from_20d_mean divided by 2; top 30% overextended threshold from train bucket only",
            "rsi_14": "fixed overextended threshold RSI > 70; no train fitting",
            "atr_14": "requires high, low, and close columns; top 30% overextended threshold from train bucket only",
            "cumulative_return_20d": "price_momentum_20d; top 30% overextended threshold from train bucket only",
            "threshold_optimization_performed": False,
            "filter_changes_performed": False,
        },
        "notes": [
            "diagnostic comparison only; no overextension definition is replaced",
            "ATR remains unavailable unless the bounded dataset includes high, low, and close columns",
            "all top-30 thresholds are fit on train-bucket rows only and applied unchanged to test",
        ],
    }


def _add_overextension_variables(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    price = _numeric_first_available(output, ["underlying_price", "close"])
    if "distance_from_20d_mean" in output.columns:
        distance = pd.to_numeric(output["distance_from_20d_mean"], errors="coerce")
    elif price is not None:
        rolling_mean = price.rolling(20, min_periods=20).mean()
        rolling_std = price.rolling(20, min_periods=20).std(ddof=0)
        distance = (price - rolling_mean) / rolling_std.replace(0, pd.NA)
        output["distance_from_20d_mean"] = distance
    else:
        distance = pd.Series(pd.NA, index=output.index, dtype="Float64")
        output["distance_from_20d_mean"] = distance
    output["bollinger_style_overextension"] = distance / 2
    if "rsi_14" in output.columns:
        output["rsi_14"] = pd.to_numeric(output["rsi_14"], errors="coerce")
    elif price is not None:
        output["rsi_14"] = _rsi_from_price(price, window=14)
    else:
        output["rsi_14"] = pd.NA
    output["atr_14_overextension"] = _atr_overextension(output, price=price)
    return output


def _numeric_first_available(frame: pd.DataFrame, columns: list[str]) -> pd.Series | None:
    for column in columns:
        if column in frame.columns:
            return pd.to_numeric(frame[column], errors="coerce")
    return None


def _rsi_from_price(price: pd.Series, *, window: int) -> pd.Series:
    delta = price.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    return rsi.where(avg_gain.notna() & avg_loss.notna())


def _atr_overextension(frame: pd.DataFrame, *, price: pd.Series | None) -> pd.Series:
    high = _numeric_first_available(frame, ["high", "underlying_high"])
    low = _numeric_first_available(frame, ["low", "underlying_low"])
    close = _numeric_first_available(frame, ["close", "underlying_close"])
    if high is None or low is None or close is None or price is None:
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = true_range.rolling(14, min_periods=14).mean()
    rolling_mean = price.rolling(20, min_periods=20).mean()
    return (price - rolling_mean) / atr.replace(0, pd.NA)


def _overextension_method_row(
    spec: dict[str, Any],
    *,
    train_bucket: pd.DataFrame,
    test_bucket: pd.DataFrame,
) -> dict[str, Any]:
    variable = str(spec["variable"])
    threshold = _overextension_threshold(spec, train_bucket)
    train_metrics = _overextension_method_metrics(train_bucket, variable=variable, threshold=threshold)
    test_metrics = _overextension_method_metrics(test_bucket, variable=variable, threshold=threshold)
    return {
        "method": spec["method"],
        "description": spec["description"],
        "variable": variable,
        "threshold_mode": spec["threshold_mode"],
        "threshold_value": _safe_float(threshold) if threshold is not None else None,
        "status": "ok" if threshold is not None else "missing",
        "train": train_metrics,
        "test": test_metrics,
        "train_delta_win": train_metrics["delta"]["delta_win_rate"],
        "test_delta_win": test_metrics["delta"]["delta_win_rate"],
        "train_delta_return": train_metrics["delta"]["delta_mean_return"],
        "test_delta_return": test_metrics["delta"]["delta_mean_return"],
    }


def _overextension_threshold(spec: dict[str, Any], train_bucket: pd.DataFrame) -> float | None:
    if spec.get("threshold_mode") == "fixed":
        return float(spec["fixed_threshold"])
    values = pd.to_numeric(train_bucket.get(str(spec["variable"]), pd.Series(dtype="float64")), errors="coerce").dropna()
    if values.empty:
        return None
    return _safe_float(values.quantile(0.70))


def _overextension_method_metrics(
    frame: pd.DataFrame,
    *,
    variable: str,
    threshold: float | None,
) -> dict[str, Any]:
    values = pd.to_numeric(frame.get(variable, pd.Series(dtype="float64")), errors="coerce")
    valid = values.notna()
    baseline = frame[valid].copy()
    early = frame[valid & (values <= threshold)].copy() if threshold is not None else frame.iloc[0:0].copy()
    late = frame[valid & (values > threshold)].copy() if threshold is not None else frame.iloc[0:0].copy()
    baseline_stats = _filter_impact_stats("baseline_no_filter", baseline)
    early_stats = _filter_impact_stats("early_trend_only", early)
    late_stats = _filter_impact_stats("late_trend", late)
    return {
        "table": {
            "baseline_no_filter": baseline_stats,
            "early_trend_only": early_stats,
            "late_trend": late_stats,
        },
        "delta": {
            "delta_win_rate": _none_safe_subtract(early_stats.get("win_rate_5d"), baseline_stats.get("win_rate_5d")),
            "delta_mean_return": _none_safe_subtract(early_stats.get("mean_return_5d"), baseline_stats.get("mean_return_5d")),
            "rows_removed": int(baseline_stats.get("sample_size", 0) - early_stats.get("sample_size", 0)),
        },
        "missing_value_count": int(values.isna().sum()),
    }


def _rank_overextension_methods(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    metrics = ["train_delta_win", "test_delta_win", "train_delta_return", "test_delta_return"]
    rank_totals = {row["method"]: 0 for row in rows}
    for metric in metrics:
        sorted_rows = sorted(
            rows,
            key=lambda row: (
                row.get(metric) is not None,
                row.get(metric) if row.get(metric) is not None else float("-inf"),
            ),
            reverse=True,
        )
        for index, row in enumerate(sorted_rows, start=1):
            rank_totals[row["method"]] += index
    enriched = []
    for row in rows:
        output = dict(row)
        output["rank_score"] = _safe_float(rank_totals[row["method"]] / len(metrics))
        enriched.append(output)
    enriched.sort(key=lambda row: (row["rank_score"], row["method"]))
    for index, row in enumerate(enriched, start=1):
        row["rank"] = index
    return enriched


def _most_stable_overextension_method(rows: list[dict[str, Any]]) -> str | None:
    candidates = []
    for row in rows:
        win_gap = _absolute_gap(row.get("train_delta_win"), row.get("test_delta_win"))
        return_gap = _absolute_gap(row.get("train_delta_return"), row.get("test_delta_return"))
        if win_gap is None or return_gap is None:
            continue
        candidates.append((win_gap + return_gap, row["method"]))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][1]


def _absolute_gap(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return abs(float(left) - float(right))


def _trend_maturity_independence_diagnostics(
    *,
    train_working: pd.DataFrame,
    test_working: pd.DataFrame,
    split_value: float,
) -> dict[str, Any]:
    train_bucket = _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(train_working), split_value=split_value)
    test_bucket = _fixed_high_vol_positive_trend_bucket(_add_explanatory_variables(test_working), split_value=split_value)
    momentum_values = pd.to_numeric(train_bucket.get(TREND_FEATURE, pd.Series(dtype="float64")), errors="coerce").dropna()
    maturity_values = pd.to_numeric(train_bucket.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce").dropna()
    momentum_threshold = momentum_values.median() if not momentum_values.empty else None
    maturity_threshold = maturity_values.median() if not maturity_values.empty else None
    train_table = _trend_maturity_independence_table(
        train_bucket,
        momentum_threshold=momentum_threshold,
        maturity_threshold=maturity_threshold,
    )
    test_table = _trend_maturity_independence_table(
        test_bucket,
        momentum_threshold=momentum_threshold,
        maturity_threshold=maturity_threshold,
    )
    train_comparisons = _trend_maturity_independence_comparisons(train_table)
    test_comparisons = _trend_maturity_independence_comparisons(test_table)
    train_independent = _trend_maturity_separates_within_both_momentum_buckets(train_comparisons)
    test_independent = _trend_maturity_separates_within_both_momentum_buckets(test_comparisons)
    return {
        "status": "ok" if momentum_threshold is not None and maturity_threshold is not None else "missing",
        "purpose": "diagnose whether distance_from_20d_mean adds information beyond price_momentum_20d inside the fixed high_vol_trend_positive bucket",
        "bucket": "high_vol_trend_positive",
        "definitions": {
            "momentum_variable": TREND_FEATURE,
            "trend_maturity_variable": "distance_from_20d_mean",
            "momentum_split": "median split from train high_vol_trend_positive bucket only",
            "trend_maturity_split": "median split from train high_vol_trend_positive bucket only",
            "momentum_threshold": _safe_float(momentum_threshold) if momentum_threshold is not None else None,
            "trend_maturity_threshold": _safe_float(maturity_threshold) if maturity_threshold is not None else None,
            "parameter_optimization_performed": False,
            "filter_changes_performed": False,
        },
        "train": {
            "table": train_table,
            "comparisons": train_comparisons,
            "trend_maturity_independent": train_independent,
        },
        "test": {
            "table": test_table,
            "comparisons": test_comparisons,
            "trend_maturity_independent": test_independent,
        },
        "trend_maturity_independent": bool(train_independent and test_independent),
        "notes": [
            "diagnostic only; no momentum or maturity filter is introduced",
            "both split thresholds are fit on train-bucket rows only and applied unchanged to test",
            "independence requires early-vs-late trend separation inside both high-momentum and low-momentum groups in train and test",
        ],
    }


def _trend_maturity_independence_table(
    frame: pd.DataFrame,
    *,
    momentum_threshold: float | None,
    maturity_threshold: float | None,
) -> dict[str, Any]:
    momentum = pd.to_numeric(frame.get(TREND_FEATURE, pd.Series(dtype="float64")), errors="coerce")
    maturity = pd.to_numeric(frame.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce")
    missing_count = int((momentum.isna() | maturity.isna()).sum())
    if momentum_threshold is None or maturity_threshold is None:
        empty = frame.iloc[0:0].copy()
        return {
            "low_momentum_early_trend": _trend_maturity_independence_stats(
                "low_momentum_early_trend",
                empty,
                momentum_bucket="low_momentum",
                trend_bucket="early_trend",
            ),
            "low_momentum_late_trend": _trend_maturity_independence_stats(
                "low_momentum_late_trend",
                empty,
                momentum_bucket="low_momentum",
                trend_bucket="late_trend",
            ),
            "high_momentum_early_trend": _trend_maturity_independence_stats(
                "high_momentum_early_trend",
                empty,
                momentum_bucket="high_momentum",
                trend_bucket="early_trend",
            ),
            "high_momentum_late_trend": _trend_maturity_independence_stats(
                "high_momentum_late_trend",
                empty,
                momentum_bucket="high_momentum",
                trend_bucket="late_trend",
            ),
            "all_evaluable": _validation_stats("all_evaluable", empty),
            "missing_momentum_or_trend_maturity_count": missing_count,
        }
    valid = momentum.notna() & maturity.notna()
    groups = {
        "low_momentum_early_trend": (momentum < momentum_threshold) & (maturity < maturity_threshold),
        "low_momentum_late_trend": (momentum < momentum_threshold) & (maturity >= maturity_threshold),
        "high_momentum_early_trend": (momentum >= momentum_threshold) & (maturity < maturity_threshold),
        "high_momentum_late_trend": (momentum >= momentum_threshold) & (maturity >= maturity_threshold),
    }
    return {
        "low_momentum_early_trend": _trend_maturity_independence_stats(
            "low_momentum_early_trend",
            frame[groups["low_momentum_early_trend"]].copy(),
            momentum_bucket="low_momentum",
            trend_bucket="early_trend",
        ),
        "low_momentum_late_trend": _trend_maturity_independence_stats(
            "low_momentum_late_trend",
            frame[groups["low_momentum_late_trend"]].copy(),
            momentum_bucket="low_momentum",
            trend_bucket="late_trend",
        ),
        "high_momentum_early_trend": _trend_maturity_independence_stats(
            "high_momentum_early_trend",
            frame[groups["high_momentum_early_trend"]].copy(),
            momentum_bucket="high_momentum",
            trend_bucket="early_trend",
        ),
        "high_momentum_late_trend": _trend_maturity_independence_stats(
            "high_momentum_late_trend",
            frame[groups["high_momentum_late_trend"]].copy(),
            momentum_bucket="high_momentum",
            trend_bucket="late_trend",
        ),
        "all_evaluable": _validation_stats("all_evaluable", frame[valid].copy()),
        "missing_momentum_or_trend_maturity_count": missing_count,
    }


def _trend_maturity_independence_stats(
    name: str,
    frame: pd.DataFrame,
    *,
    momentum_bucket: str,
    trend_bucket: str,
) -> dict[str, Any]:
    stats = _validation_stats(name, frame)
    momentum = pd.to_numeric(frame.get(TREND_FEATURE, pd.Series(dtype="float64")), errors="coerce").dropna()
    maturity = pd.to_numeric(frame.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce").dropna()
    return {
        "momentum_bucket": momentum_bucket,
        "trend_bucket": trend_bucket,
        **stats,
        "mean_price_momentum_20d": _safe_float(momentum.mean()) if not momentum.empty else None,
        "mean_trend_maturity": _safe_float(maturity.mean()) if not maturity.empty else None,
    }


def _trend_maturity_independence_comparisons(table: dict[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for momentum_bucket in ["low_momentum", "high_momentum"]:
        early = table[f"{momentum_bucket}_early_trend"]
        late = table[f"{momentum_bucket}_late_trend"]
        win_delta = _none_safe_subtract(early.get("positive_rate_5d"), late.get("positive_rate_5d"))
        mean_delta = _none_safe_subtract(early.get("mean_forward_return_5d"), late.get("mean_forward_return_5d"))
        output[f"{momentum_bucket}_early_vs_late"] = {
            "momentum_bucket": momentum_bucket,
            "early_sample_size": early.get("sample_size"),
            "late_sample_size": late.get("sample_size"),
            "early_minus_late_win_rate": win_delta,
            "early_minus_late_mean_return": mean_delta,
            "early_outperforms_late": bool(
                win_delta is not None
                and mean_delta is not None
                and win_delta > 0
                and mean_delta > 0
                and early.get("sample_size", 0) > 0
                and late.get("sample_size", 0) > 0
            ),
        }
    return output


def _trend_maturity_separates_within_both_momentum_buckets(comparisons: dict[str, Any]) -> bool:
    return bool(
        comparisons.get("low_momentum_early_vs_late", {}).get("early_outperforms_late")
        and comparisons.get("high_momentum_early_vs_late", {}).get("early_outperforms_late")
    )


def _overextension_fragility_diagnostics(
    *,
    train_working: pd.DataFrame,
    test_working: pd.DataFrame,
    split_value: float,
    test_year: str,
    low_sample_threshold: int = 10,
    overfiltering_threshold: float = 0.50,
) -> dict[str, Any]:
    train_bucket = _fixed_high_vol_positive_trend_bucket(
        _add_explanatory_variables(train_working), split_value=split_value
    )
    test_bucket = _fixed_high_vol_positive_trend_bucket(
        _add_explanatory_variables(test_working), split_value=split_value
    )
    maturity_values = pd.to_numeric(
        train_bucket.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce"
    ).dropna()
    threshold = maturity_values.quantile(0.70) if not maturity_values.empty else None

    train_summary = _fragility_scenario_summary(
        train_bucket, threshold=threshold, low_sample_threshold=low_sample_threshold
    )
    test_summary = _fragility_scenario_summary(
        test_bucket, threshold=threshold, low_sample_threshold=low_sample_threshold
    )
    per_quarter = _fragility_per_quarter(test_bucket, threshold=threshold)

    hard_filter_q_rows = [q["hard_filter_rows"] for q in per_quarter]
    sample_size_safe = bool(
        hard_filter_q_rows and all(n >= low_sample_threshold for n in hard_filter_q_rows)
    )

    baseline_total = test_summary["baseline_no_adjustment"]["total_rows"]
    hard_total = test_summary["hard_filter_early_trend_only"]["rows_kept"]
    rows_removed_total = baseline_total - hard_total
    pct_removed = rows_removed_total / baseline_total if baseline_total > 0 else 0.0
    overfiltering_risk = bool(pct_removed > overfiltering_threshold)

    hard_mean = test_summary["hard_filter_early_trend_only"]["mean_5d_return"]
    positive_quarter_count = sum(
        1 for q in per_quarter if (q.get("hard_filter_mean_5d_return") or 0.0) > 0
    )
    fragility_warning = bool(
        hard_mean is not None and hard_mean > 0 and positive_quarter_count == 1
    )

    return {
        "status": "ok" if threshold is not None else "missing",
        "purpose": "test whether the hard overextension filter creates fragile or overly selective behavior",
        "scenarios_compared": ["baseline_no_adjustment", "hard_filter_early_trend_only"],
        "definitions": {
            "trend_maturity_variable": "distance_from_20d_mean",
            "late_trend": "top 30% of train-bucket maturity values",
            "threshold_source": "train high_vol_trend_positive bucket only",
            "threshold_value": _safe_float(threshold) if threshold is not None else None,
            "low_sample_threshold": low_sample_threshold,
            "overfiltering_threshold": overfiltering_threshold,
            "training_performed": False,
            "threshold_optimization_performed": False,
            "filter_changes_performed": False,
        },
        "train": train_summary,
        "test": test_summary,
        "per_quarter_test": per_quarter,
        "flags": {
            "sample_size_safe": bool(sample_size_safe),
            "overfiltering_risk": bool(overfiltering_risk),
            "fragility_warning": bool(fragility_warning),
        },
        "flag_criteria": {
            "sample_size_safe": f"false if any test quarter has hard_filter_rows < {low_sample_threshold}",
            "overfiltering_risk": f"true if test removes >{int(overfiltering_threshold * 100)}% of rows",
            "fragility_warning": "true if hard-filter test mean is positive and only one quarter contributes positively",
        },
    }


def _fragility_scenario_summary(
    frame: pd.DataFrame,
    *,
    threshold: float | None,
    low_sample_threshold: int = 10,
) -> dict[str, Any]:
    maturity = pd.to_numeric(
        frame.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce"
    )
    valid = maturity.notna()
    baseline = frame[valid].copy()
    early = (
        frame[valid & (maturity < threshold)].copy()
        if threshold is not None
        else frame.iloc[0:0].copy()
    )
    baseline_returns = pd.to_numeric(
        baseline.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce"
    ).dropna()
    baseline_total = int(len(baseline_returns))
    return {
        "baseline_no_adjustment": _fragility_scenario_stats(
            baseline, baseline_total=baseline_total, low_sample_threshold=low_sample_threshold
        ),
        "hard_filter_early_trend_only": _fragility_scenario_stats(
            early, baseline_total=baseline_total, low_sample_threshold=low_sample_threshold
        ),
        "missing_trend_maturity_count": int(maturity.isna().sum()),
    }


def _fragility_scenario_stats(
    frame: pd.DataFrame,
    *,
    baseline_total: int,
    low_sample_threshold: int = 10,
) -> dict[str, Any]:
    returns = pd.to_numeric(
        frame.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce"
    ).dropna()
    rows_kept = int(len(returns))
    rows_removed = baseline_total - rows_kept
    pct_removed = rows_removed / baseline_total if baseline_total > 0 else None
    quarterly = _quarterly_stats_for_frame(frame)
    q_means = [q["mean_5d_return"] for q in quarterly if q.get("mean_5d_return") is not None]
    q_rows = [q["rows"] for q in quarterly]
    variance = (
        _safe_float(pd.Series(q_means, dtype="float64").var(ddof=0))
        if len(q_means) >= 2
        else None
    )
    avg_per_quarter = _safe_float(sum(q_rows) / len(q_rows)) if q_rows else None
    min_per_quarter = min(q_rows) if q_rows else None
    num_low_sample = sum(1 for n in q_rows if n < low_sample_threshold)
    return {
        "total_rows": baseline_total,
        "rows_kept": rows_kept,
        "rows_removed": rows_removed,
        "percent_removed": _safe_float(pct_removed * 100) if pct_removed is not None else None,
        "mean_5d_return": _safe_float(returns.mean()) if not returns.empty else None,
        "win_rate": _safe_float((returns > 0).mean()) if not returns.empty else None,
        "return_variance_across_time_slices": variance,
        "avg_selected_rows_per_quarter": avg_per_quarter,
        "min_selected_rows_per_quarter": min_per_quarter,
        "num_low_sample_quarters": num_low_sample,
    }


def _quarterly_stats_for_frame(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty or "entry_date" not in frame.columns:
        return []
    working = frame.copy()
    working["entry_date"] = pd.to_datetime(working["entry_date"], errors="coerce")
    working = working.dropna(subset=["entry_date"])
    if working.empty:
        return []
    forward_5d = pd.to_numeric(
        working.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce"
    )
    working = working.assign(
        forward_5d_numeric=forward_5d,
        year_quarter=working["entry_date"].dt.to_period("Q"),
    )
    rows = []
    for period, group in working.groupby("year_quarter"):
        period_returns = group["forward_5d_numeric"].dropna()
        rows.append(
            {
                "quarter": str(period),
                "rows": int(len(period_returns)),
                "mean_5d_return": _safe_float(period_returns.mean()) if not period_returns.empty else None,
                "win_rate": _safe_float((period_returns > 0).mean()) if not period_returns.empty else None,
            }
        )
    return sorted(rows, key=lambda r: r["quarter"])


def _fragility_per_quarter(frame: pd.DataFrame, *, threshold: float | None) -> list[dict[str, Any]]:
    if frame.empty or "entry_date" not in frame.columns:
        return []
    working = frame.copy()
    working["entry_date"] = pd.to_datetime(working["entry_date"], errors="coerce")
    working = working.dropna(subset=["entry_date"])
    if working.empty:
        return []
    maturity = pd.to_numeric(
        working.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce"
    )
    valid = maturity.notna()
    baseline = working[valid].copy()
    early = (
        working[valid & (maturity < threshold)].copy()
        if threshold is not None
        else working.iloc[0:0].copy()
    )
    if baseline.empty:
        return []
    baseline = baseline.assign(year_quarter=baseline["entry_date"].dt.to_period("Q"))
    if not early.empty:
        early = early.assign(year_quarter=early["entry_date"].dt.to_period("Q"))
    quarters = sorted(baseline["year_quarter"].unique())
    rows = []
    for period in quarters:
        q_baseline = baseline[baseline["year_quarter"] == period]
        q_early = (
            early[early["year_quarter"] == period]
            if not early.empty
            else pd.DataFrame()
        )
        baseline_returns = pd.to_numeric(
            q_baseline.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce"
        ).dropna()
        early_returns = (
            pd.to_numeric(
                q_early.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce"
            ).dropna()
            if not q_early.empty
            else pd.Series(dtype="float64")
        )
        baseline_n = int(len(baseline_returns))
        early_n = int(len(early_returns))
        removed = baseline_n - early_n
        pct_removed = removed / baseline_n if baseline_n > 0 else None
        rows.append(
            {
                "quarter": str(period),
                "baseline_rows": baseline_n,
                "hard_filter_rows": early_n,
                "rows_removed": removed,
                "percent_removed": _safe_float(pct_removed * 100) if pct_removed is not None else None,
                "hard_filter_mean_5d_return": _safe_float(early_returns.mean()) if not early_returns.empty else None,
                "hard_filter_win_rate": _safe_float((early_returns > 0).mean()) if not early_returns.empty else None,
            }
        )
    return rows


def _late_trend_removal_validation(
    *,
    train_working: pd.DataFrame,
    test_working: pd.DataFrame,
    split_value: float,
) -> dict[str, Any]:
    all_train_stats = _validation_stats("all_rows_train", train_working)
    all_test_stats = _validation_stats("all_rows_test", test_working)

    train_baseline_bucket = _fixed_high_vol_positive_trend_bucket(
        _add_explanatory_variables(train_working), split_value=split_value
    )
    test_baseline_bucket = _fixed_high_vol_positive_trend_bucket(
        _add_explanatory_variables(test_working), split_value=split_value
    )

    maturity_train = pd.to_numeric(
        train_baseline_bucket.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce"
    )
    maturity_test = pd.to_numeric(
        test_baseline_bucket.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce"
    )
    maturity_values = maturity_train.dropna()
    threshold = maturity_values.quantile(0.70) if not maturity_values.empty else None

    if threshold is not None:
        train_filtered = train_baseline_bucket[maturity_train.notna() & (maturity_train < threshold)].copy()
        test_filtered = test_baseline_bucket[maturity_test.notna() & (maturity_test < threshold)].copy()
    else:
        train_filtered = train_baseline_bucket.iloc[0:0].copy()
        test_filtered = test_baseline_bucket.iloc[0:0].copy()

    train_baseline_stats = _validation_stats("high_vol_trend_positive_train", train_baseline_bucket)
    test_baseline_stats = _validation_stats("high_vol_trend_positive_test", test_baseline_bucket)
    train_filtered_stats = _validation_stats("early_trend_only_train", train_filtered)
    test_filtered_stats = _validation_stats("early_trend_only_test", test_filtered)

    b_train_win_delta = _none_safe_subtract(train_baseline_stats.get("positive_rate_5d"), all_train_stats.get("positive_rate_5d"))
    b_test_win_delta = _none_safe_subtract(test_baseline_stats.get("positive_rate_5d"), all_test_stats.get("positive_rate_5d"))
    b_train_mean_delta = _none_safe_subtract(train_baseline_stats.get("mean_forward_return_5d"), all_train_stats.get("mean_forward_return_5d"))
    b_test_mean_delta = _none_safe_subtract(test_baseline_stats.get("mean_forward_return_5d"), all_test_stats.get("mean_forward_return_5d"))

    f_train_win_delta = _none_safe_subtract(train_filtered_stats.get("positive_rate_5d"), all_train_stats.get("positive_rate_5d"))
    f_test_win_delta = _none_safe_subtract(test_filtered_stats.get("positive_rate_5d"), all_test_stats.get("positive_rate_5d"))
    f_train_mean_delta = _none_safe_subtract(train_filtered_stats.get("mean_forward_return_5d"), all_train_stats.get("mean_forward_return_5d"))
    f_test_mean_delta = _none_safe_subtract(test_filtered_stats.get("mean_forward_return_5d"), all_test_stats.get("mean_forward_return_5d"))

    b_win_train = _positive(b_train_win_delta)
    b_win_test = _positive(b_test_win_delta)
    b_mean_train = _positive(b_train_mean_delta)
    b_mean_test = _positive(b_test_mean_delta)

    f_win_train = _positive(f_train_win_delta)
    f_win_test = _positive(f_test_win_delta)
    f_mean_train = _positive(f_train_mean_delta)
    f_mean_test = _positive(f_test_mean_delta)

    baseline_validated = bool(b_win_train and b_win_test and b_mean_train and b_mean_test)
    filtered_validated = bool(f_win_train and f_win_test and f_mean_train and f_mean_test)
    late_trend_removal_fixes_signal = bool(filtered_validated)

    win_rate_change_test = _none_safe_subtract(
        test_filtered_stats.get("positive_rate_5d"), test_baseline_stats.get("positive_rate_5d")
    )
    mean_return_change_test = _none_safe_subtract(
        test_filtered_stats.get("mean_forward_return_5d"), test_baseline_stats.get("mean_forward_return_5d")
    )

    return {
        "status": "ok" if threshold is not None else "missing",
        "purpose": "test whether removing late-trend rows from high_vol_trend_positive converts a failing signal to a valid one",
        "definitions": {
            "baseline": "high_vol_trend_positive (realized_vol_60d >= train_median AND price_momentum_20d > 0)",
            "filtered": "high_vol_trend_positive AND distance_from_20d_mean < train-bucket top-30% threshold",
            "validation_criteria": "win rate delta and mean return delta versus all_rows are both positive in train AND test",
            "threshold_source": "train high_vol_trend_positive bucket only",
            "threshold_value": _safe_float(threshold) if threshold is not None else None,
            "training_performed": False,
            "threshold_optimization_performed": False,
            "filter_changes_performed": False,
        },
        "all_rows": {
            "train": {
                "sample_size": all_train_stats["sample_size"],
                "win_rate_5d": all_train_stats["positive_rate_5d"],
                "mean_return_5d": all_train_stats["mean_forward_return_5d"],
            },
            "test": {
                "sample_size": all_test_stats["sample_size"],
                "win_rate_5d": all_test_stats["positive_rate_5d"],
                "mean_return_5d": all_test_stats["mean_forward_return_5d"],
            },
        },
        "baseline_validation": {
            "scenario": "high_vol_trend_positive",
            "train": {
                "sample_size": train_baseline_stats["sample_size"],
                "win_rate_5d": train_baseline_stats["positive_rate_5d"],
                "mean_return_5d": train_baseline_stats["mean_forward_return_5d"],
                "win_rate_delta_vs_all": b_train_win_delta,
                "mean_return_delta_vs_all": b_train_mean_delta,
            },
            "test": {
                "sample_size": test_baseline_stats["sample_size"],
                "win_rate_5d": test_baseline_stats["positive_rate_5d"],
                "mean_return_5d": test_baseline_stats["mean_forward_return_5d"],
                "win_rate_delta_vs_all": b_test_win_delta,
                "mean_return_delta_vs_all": b_test_mean_delta,
            },
            "validated": bool(baseline_validated),
            "validation_checks": {
                "win_rate_positive_in_train": bool(b_win_train),
                "win_rate_positive_in_test": bool(b_win_test),
                "mean_return_positive_in_train": bool(b_mean_train),
                "mean_return_positive_in_test": bool(b_mean_test),
            },
        },
        "filtered_validation": {
            "scenario": "high_vol_trend_positive AND early_trend_only",
            "train": {
                "sample_size": train_filtered_stats["sample_size"],
                "win_rate_5d": train_filtered_stats["positive_rate_5d"],
                "mean_return_5d": train_filtered_stats["mean_forward_return_5d"],
                "win_rate_delta_vs_all": f_train_win_delta,
                "mean_return_delta_vs_all": f_train_mean_delta,
            },
            "test": {
                "sample_size": test_filtered_stats["sample_size"],
                "win_rate_5d": test_filtered_stats["positive_rate_5d"],
                "mean_return_5d": test_filtered_stats["mean_forward_return_5d"],
                "win_rate_delta_vs_all": f_test_win_delta,
                "mean_return_delta_vs_all": f_test_mean_delta,
            },
            "validated": bool(filtered_validated),
            "validation_checks": {
                "win_rate_positive_in_train": bool(f_win_train),
                "win_rate_positive_in_test": bool(f_win_test),
                "mean_return_positive_in_train": bool(f_mean_train),
                "mean_return_positive_in_test": bool(f_mean_test),
            },
        },
        "improvement_summary": {
            "baseline_rows_train": train_baseline_stats["sample_size"],
            "baseline_rows_test": test_baseline_stats["sample_size"],
            "filtered_rows_train": train_filtered_stats["sample_size"],
            "filtered_rows_test": test_filtered_stats["sample_size"],
            "rows_removed_train": int(train_baseline_stats["sample_size"] - train_filtered_stats["sample_size"]),
            "rows_removed_test": int(test_baseline_stats["sample_size"] - test_filtered_stats["sample_size"]),
            "win_rate_change_test": win_rate_change_test,
            "mean_return_change_test": mean_return_change_test,
        },
        "late_trend_removal_fixes_signal": bool(late_trend_removal_fixes_signal),
    }


def _variable_failure_comparison(
    variable: str,
    *,
    failing: pd.DataFrame,
    working: pd.DataFrame,
) -> dict[str, Any]:
    failing_values = pd.to_numeric(failing.get(variable, pd.Series(dtype="float64")), errors="coerce").dropna()
    working_values = pd.to_numeric(working.get(variable, pd.Series(dtype="float64")), errors="coerce").dropna()
    failing_stats = _distribution_stats(failing_values)
    working_stats = _distribution_stats(working_values)
    mean_difference = _none_safe_subtract(failing_stats["mean"], working_stats["mean"])
    median_difference = _none_safe_subtract(failing_stats["median"], working_stats["median"])
    pooled_std = _pooled_std(failing_values, working_values)
    standardized_mean_difference = (
        float(mean_difference) / pooled_std
        if mean_difference is not None and pooled_std is not None and pooled_std > 0
        else None
    )
    material = bool(
        standardized_mean_difference is not None
        and abs(standardized_mean_difference) >= 0.5
        and failing_stats["sample_size"] > 0
        and working_stats["sample_size"] > 0
    )
    return {
        "variable": variable,
        "failing": failing_stats,
        "working": working_stats,
        "differences": {
            "mean_difference_failing_minus_working": mean_difference,
            "median_difference_failing_minus_working": median_difference,
            "standardized_mean_difference": _safe_float(standardized_mean_difference),
        },
        "material_difference": material,
    }


def _distribution_stats(values: pd.Series) -> dict[str, Any]:
    if values.empty:
        return {
            "sample_size": 0,
            "mean": None,
            "median": None,
            "std": None,
            "p25": None,
            "p75": None,
            "min": None,
            "max": None,
        }
    return {
        "sample_size": int(len(values)),
        "mean": _safe_float(values.mean()),
        "median": _safe_float(values.median()),
        "std": _safe_float(values.std(ddof=0)),
        "p25": _safe_float(values.quantile(0.25)),
        "p75": _safe_float(values.quantile(0.75)),
        "min": _safe_float(values.min()),
        "max": _safe_float(values.max()),
    }


def _pooled_std(left: pd.Series, right: pd.Series) -> float | None:
    combined = pd.concat([left, right], ignore_index=True)
    if combined.empty:
        return None
    std = combined.std(ddof=0)
    return _safe_float(std)


def _quarter_start(year: int, quarter: int) -> str:
    month = (quarter - 1) * 3 + 1
    return f"{year}-{month:02d}-01"


def _quarter_end(year: int, quarter: int) -> str:
    month = quarter * 3
    last_day = pd.Timestamp(year=year, month=month, day=1).days_in_month
    return pd.Timestamp(year=year, month=month, day=last_day).date().isoformat()


def _validation_stats(name: str, frame: pd.DataFrame) -> dict[str, Any]:
    forward_5d = pd.to_numeric(frame.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce").dropna()
    return {
        "name": name,
        "sample_size": int(len(forward_5d)),
        "positive_rate_5d": _safe_float((forward_5d > 0).mean()) if not forward_5d.empty else None,
        "mean_forward_return_5d": _safe_float(forward_5d.mean()) if not forward_5d.empty else None,
    }


def _comparisons_to_all_rows(groups: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    baseline = groups["all_rows"]
    return {
        name: {
            "positive_rate_delta": _none_safe_subtract(group.get("positive_rate_5d"), baseline.get("positive_rate_5d")),
            "mean_forward_return_5d_delta": _none_safe_subtract(group.get("mean_forward_return_5d"), baseline.get("mean_forward_return_5d")),
            "sample_size_delta": int(group.get("sample_size", 0) - baseline.get("sample_size", 0)),
        }
        for name, group in groups.items()
        if name != "all_rows"
    }


def _train_test_comparison(
    train_groups: dict[str, dict[str, Any]],
    test_groups: dict[str, dict[str, Any]],
    train_comparisons: dict[str, dict[str, Any]],
    test_comparisons: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    group_comparisons = {}
    for name in ["all_rows", "realized_vol_60d_high", "realized_vol_60d_low"]:
        train = train_groups[name]
        test = test_groups[name]
        group_comparisons[name] = {
            "sample_size_delta_test_minus_train": int(test.get("sample_size", 0) - train.get("sample_size", 0)),
            "positive_rate_delta_test_minus_train": _none_safe_subtract(test.get("positive_rate_5d"), train.get("positive_rate_5d")),
            "mean_forward_return_5d_delta_test_minus_train": _none_safe_subtract(test.get("mean_forward_return_5d"), train.get("mean_forward_return_5d")),
        }
    regime_stability = {}
    for name in ["realized_vol_60d_high", "realized_vol_60d_low"]:
        train_delta = train_comparisons[name]["positive_rate_delta"]
        test_delta = test_comparisons[name]["positive_rate_delta"]
        regime_stability[name] = {
            "train_positive_rate_delta_vs_all": train_delta,
            "test_positive_rate_delta_vs_all": test_delta,
            "delta_degradation_test_minus_train": _none_safe_subtract(test_delta, train_delta),
            "win_rate_direction_preserved_vs_all": _direction_preserved(train_delta, test_delta),
            "train_mean_return_delta_vs_all": train_comparisons[name]["mean_forward_return_5d_delta"],
            "test_mean_return_delta_vs_all": test_comparisons[name]["mean_forward_return_5d_delta"],
            "mean_return_direction_preserved_vs_all": _direction_preserved(
                train_comparisons[name]["mean_forward_return_5d_delta"],
                test_comparisons[name]["mean_forward_return_5d_delta"],
            ),
        }
    return {
        "group_deltas": group_comparisons,
        "win_rate_stability_vs_train": regime_stability,
    }


def _degradation_metric(
    train_comparisons: dict[str, dict[str, Any]],
    test_comparisons: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    values = {}
    for name in ["realized_vol_60d_high", "realized_vol_60d_low"]:
        train_delta = train_comparisons[name]["positive_rate_delta"]
        test_delta = test_comparisons[name]["positive_rate_delta"]
        values[name] = {
            "absolute_change_in_positive_rate_delta": (
                abs(test_delta - train_delta) if train_delta is not None and test_delta is not None else None
            )
        }
    numeric = [value["absolute_change_in_positive_rate_delta"] for value in values.values() if value["absolute_change_in_positive_rate_delta"] is not None]
    return {
        "definition": "max absolute change in regime positive-rate delta versus all_rows from train to test",
        "by_regime": values,
        "max_absolute_change": _safe_float(max(numeric)) if numeric else None,
    }


def _direction_preserved(train_delta: float | None, test_delta: float | None) -> bool:
    if train_delta is None or test_delta is None:
        return False
    return bool((_positive(train_delta) and _positive(test_delta)) or (_negative(train_delta) and _negative(test_delta)))


def _positive(value: float | None) -> bool:
    return value is not None and float(value) > 0


def _negative(value: float | None) -> bool:
    return value is not None and float(value) < 0


def _none_safe_subtract(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return float(left) - float(right)
