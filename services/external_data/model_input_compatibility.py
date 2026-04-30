"""Bounded model-input compatibility audit for historical T9 features.

This module checks whether tiny historical T9 feature slices can satisfy the
clean project's current model input schema. It does not train models, tune
thresholds, or make performance claims.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.historical_feature_contract import build_historical_feature_contract_from_t9
from services.external_data.historical_label_contract import (
    DEFAULT_HORIZONS,
    build_historical_label_contract,
)
from services.external_data.model_feature_adapter import adapt_daily_features_for_model_schema
from services.external_data.model_feature_adapter import DETERMINISTIC_DAILY_FEATURES
from services.external_data.model_volatility_adapter import REAL_INPUT_VOLATILITY_FEATURES
from services.external_data.model_volatility_adapter import adapt_real_input_volatility_features_for_model_schema
from services.external_data.model_volatility_adapter import load_vix_daily_level_from_t9
from services.external_data.t9_parquet_adapter import _normalize_symbol, _parse_date


EXPECTED_FEATURE_SCHEMA: dict[str, str] = {
    "symbol": "string",
    "date": "date",
    "underlying_price": "numeric",
    "price_momentum_5d": "numeric",
    "price_momentum_20d": "numeric",
    "volume_ratio_10d": "numeric",
    "iv_rank": "numeric",
    "iv_percentile": "numeric",
    "iv30_rv30_ratio": "numeric",
    "vol_term_structure_slope": "numeric",
    "rsi_14": "numeric",
    "bb_position": "numeric",
    "vix_level": "numeric",
    "volume": "numeric",
    "realized_vol_30d": "numeric",
    "realized_vol_60d": "numeric",
}
EXPECTED_TARGET_HORIZONS = ["1d", "5d", "21d"]
EXPECTED_TARGET_COLUMNS = ["forward_return_1d", "forward_return_5d", "forward_return_21d", "forward_volatility_21d"]
SAFE_ALIASES = {
    "underlying_price": "close",
    "symbol": "underlying_symbol",
}
HIGH_NULL_RATE = 0.25


@dataclass(frozen=True)
class ModelInputCompatibilityReport:
    available_rows: pd.DataFrame
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def build_model_input_compatibility_from_t9(
    *,
    symbol: str = "SPY",
    start_date: str,
    end_date: str,
    root: str | Path | None = None,
    max_files: int = 20,
    daily_source: str | None = None,
    horizons: list[str] | None = None,
) -> ModelInputCompatibilityReport:
    normalized_symbol = _normalize_symbol(symbol)
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    feature_contract = build_historical_feature_contract_from_t9(
        symbol=normalized_symbol,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        root=root,
        max_files=max_files,
        daily_source=daily_source,
    )
    label_contract = build_historical_label_contract(
        label_ready_rows=feature_contract.label_ready_rows,
        model_ready_daily_features=feature_contract.model_ready_daily_features,
        symbol=normalized_symbol,
        start=start,
        end=end,
        horizons=horizons or DEFAULT_HORIZONS,
        source_report=feature_contract.report,
    )
    vix = load_vix_daily_level_from_t9(root=root, start=start, end=end, max_files=max_files)
    return build_model_input_compatibility_report(
        model_ready_daily_features=feature_contract.model_ready_daily_features,
        option_context_features=feature_contract.option_context_features,
        label_candidates=label_contract.label_candidates,
        vix_daily_features=vix.rows,
        symbol=normalized_symbol,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        requested_horizons=horizons or DEFAULT_HORIZONS,
        warnings=[*feature_contract.report.get("warnings", []), *label_contract.report.get("warnings", []), *vix.warnings],
        source_status={
            "feature_contract": feature_contract.report.get("status"),
            "label_contract": label_contract.report.get("status"),
            "vix_rows": int(len(vix.rows)),
        },
    )


def build_model_input_compatibility_report(
    *,
    model_ready_daily_features: pd.DataFrame,
    option_context_features: pd.DataFrame,
    label_candidates: pd.DataFrame,
    vix_daily_features: pd.DataFrame | None = None,
    symbol: str,
    start_date: str,
    end_date: str,
    requested_horizons: list[str],
    warnings: list[str] | None = None,
    source_status: dict[str, Any] | None = None,
) -> ModelInputCompatibilityReport:
    normalized_symbol = _normalize_symbol(symbol)
    available, adapter_report = _build_available_feature_rows(
        model_ready_daily_features=model_ready_daily_features,
        option_context_features=option_context_features,
        vix_daily_features=vix_daily_features,
        symbol=normalized_symbol,
    )
    feature_audit = _audit_features(available)
    label_audit = _audit_labels(label_candidates, requested_horizons=requested_horizons)
    status = _final_status(feature_audit, label_audit)
    report = {
        "name": "model_input_compatibility_smoke",
        "status": status,
        "symbol": normalized_symbol,
        "start_date": start_date,
        "end_date": end_date,
        "read_only": True,
        "training_performed": False,
        "threshold_optimization_performed": False,
        "schema_source": "services.institutional_ml_db.InstitutionalMLDatabase.get_training_dataset",
        "row_count": int(len(available)),
        "expected_feature_count": int(len(EXPECTED_FEATURE_SCHEMA)),
        "available_feature_count": int(len(available.columns)),
        "expected_features": EXPECTED_FEATURE_SCHEMA,
        "computed_feature_list": adapter_report["computed_feature_list"],
        "deterministic_computed_feature_list": adapter_report["deterministic_computed_feature_list"],
        "real_input_computed_feature_list": adapter_report["real_input_computed_feature_list"],
        "computed_non_null_counts": adapter_report["computed_non_null_counts"],
        "unavailable_due_to_insufficient_lookback": adapter_report["unavailable_due_to_insufficient_lookback"],
        "deterministic_feature_readiness": _deterministic_feature_readiness(available),
        "real_input_feature_readiness": _real_input_feature_readiness(available, adapter_report),
        "unavailable_due_to_missing_source": adapter_report["unavailable_due_to_missing_source"],
        "not_fabricated_features": adapter_report["not_fabricated_features"],
        "no_lookahead_notes": adapter_report["no_lookahead_notes"],
        "safe_aliases_applied": feature_audit["safe_aliases_applied"],
        "missing_required_features": feature_audit["missing_required_features"],
        "extra_features": feature_audit["extra_features"],
        "dtype_mismatches": feature_audit["dtype_mismatches"],
        "nullable_fields": feature_audit["nullable_fields"],
        "all_null_columns": feature_audit["all_null_columns"],
        "null_rate_warnings": feature_audit["null_rate_warnings"],
        "constant_columns": feature_audit["constant_columns"],
        "label_availability": label_audit,
        "target_horizon_compatibility": label_audit["target_horizon_compatibility"],
        "warnings": _dedupe_warnings([*(warnings or []), *feature_audit["warnings"], *label_audit["warnings"]]),
        "source_status": dict(source_status or {}),
    }
    return ModelInputCompatibilityReport(available_rows=available, report=report)


def write_model_input_compatibility_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "model_input_compatibility_smoke"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    stem = f"{report['symbol'].lower()}_{report['start_date']}_{report['end_date']}".replace("/", "-")
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def write_model_input_compatibility_extended_report(report: dict[str, Any]) -> Path:
    return write_model_input_compatibility_report(
        report,
        reports_dir=Path.cwd() / "reports" / "model_input_compatibility_smoke_extended",
    )


def _build_available_feature_rows(
    *,
    model_ready_daily_features: pd.DataFrame,
    option_context_features: pd.DataFrame,
    vix_daily_features: pd.DataFrame | None,
    symbol: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if model_ready_daily_features.empty:
        adapter_result = adapt_daily_features_for_model_schema(pd.DataFrame())
        volatility_result = adapt_real_input_volatility_features_for_model_schema(
            pd.DataFrame(),
            option_context_features=option_context_features,
            vix_daily_features=vix_daily_features,
        )
        return pd.DataFrame(), _combine_adapter_reports(adapter_result.report, volatility_result.report)
    daily = model_ready_daily_features.copy()
    daily["date"] = pd.to_datetime(daily["date"], errors="coerce").dt.date.astype("string")
    daily["symbol"] = symbol
    if "close" in daily.columns:
        daily["underlying_price"] = daily["close"]
    adapter_result = adapt_daily_features_for_model_schema(daily)
    daily = adapter_result.rows
    volatility_result = adapt_real_input_volatility_features_for_model_schema(
        daily,
        option_context_features=option_context_features,
        vix_daily_features=vix_daily_features,
    )
    daily = volatility_result.rows

    if option_context_features.empty:
        return daily, _combine_adapter_reports(adapter_result.report, volatility_result.report)

    option = option_context_features.copy()
    option["date"] = pd.to_datetime(option["date"], errors="coerce").dt.date.astype("string")
    grouped = option.groupby("date", dropna=False).agg(
        implied_volatility=("implied_volatility", "mean"),
        option_volume=("volume", "sum"),
        option_open_interest=("open_interest", "sum"),
        option_mid=("mid", "mean"),
        option_relative_spread=("relative_spread", "mean"),
    ).reset_index()
    merged = daily.merge(grouped, on="date", how="left")
    return merged, _combine_adapter_reports(adapter_result.report, volatility_result.report)


def _audit_features(frame: pd.DataFrame) -> dict[str, Any]:
    warnings: list[str] = []
    missing: list[str] = []
    aliases: dict[str, str] = {}
    for feature in EXPECTED_FEATURE_SCHEMA:
        if feature in frame.columns:
            continue
        alias = SAFE_ALIASES.get(feature)
        if alias and alias in frame.columns:
            aliases[feature] = alias
            continue
        missing.append(feature)

    dtype_mismatches = _dtype_mismatches(frame)
    nullable_fields = _nullable_fields(frame)
    all_null = [column for column in EXPECTED_FEATURE_SCHEMA if column in frame.columns and frame[column].isna().all()]
    high_null = _high_null_warnings(frame)
    constants = _constant_columns(frame)
    extra = sorted(str(column) for column in frame.columns if column not in EXPECTED_FEATURE_SCHEMA)

    if extra:
        warnings.append("historical feature rows contain extra columns not in the expected model schema")
    if dtype_mismatches:
        warnings.append("historical feature rows require dtype coercion before model input")
    if high_null:
        warnings.append("historical feature rows contain high-null expected fields")
    return {
        "missing_required_features": sorted(missing),
        "safe_aliases_applied": aliases,
        "extra_features": extra,
        "dtype_mismatches": dtype_mismatches,
        "nullable_fields": nullable_fields,
        "all_null_columns": all_null,
        "null_rate_warnings": high_null,
        "constant_columns": constants,
        "warnings": warnings,
    }


def _deterministic_feature_readiness(frame: pd.DataFrame) -> dict[str, Any]:
    feature_null_rates: dict[str, float | None] = {}
    feature_non_null_counts: dict[str, int] = {}
    if frame.empty:
        for feature in DETERMINISTIC_DAILY_FEATURES:
            feature_null_rates[feature] = None
            feature_non_null_counts[feature] = 0
        usable_rows = 0
    else:
        usable_mask = pd.Series(True, index=frame.index)
        for feature in DETERMINISTIC_DAILY_FEATURES:
            if feature not in frame.columns:
                feature_null_rates[feature] = None
                feature_non_null_counts[feature] = 0
                usable_mask &= False
                continue
            feature_null_rates[feature] = float(frame[feature].isna().mean())
            feature_non_null_counts[feature] = int(frame[feature].notna().sum())
            usable_mask &= frame[feature].notna()
        usable_rows = int(usable_mask.sum())
    return {
        "required_daily_features": list(DETERMINISTIC_DAILY_FEATURES),
        "null_rate_by_feature": feature_null_rates,
        "non_null_count_by_feature": feature_non_null_counts,
        "usable_row_count_after_required_daily_features": usable_rows,
    }


def _real_input_feature_readiness(frame: pd.DataFrame, adapter_report: dict[str, Any]) -> dict[str, Any]:
    feature_null_rates: dict[str, float | None] = {}
    feature_non_null_counts: dict[str, int] = {}
    if frame.empty:
        for feature in REAL_INPUT_VOLATILITY_FEATURES:
            feature_null_rates[feature] = None
            feature_non_null_counts[feature] = 0
        usable_rows = 0
    else:
        usable_mask = pd.Series(True, index=frame.index)
        for feature in REAL_INPUT_VOLATILITY_FEATURES:
            if feature not in frame.columns:
                feature_null_rates[feature] = None
                feature_non_null_counts[feature] = 0
                usable_mask &= False
                continue
            feature_null_rates[feature] = float(frame[feature].isna().mean())
            feature_non_null_counts[feature] = int(frame[feature].notna().sum())
            usable_mask &= frame[feature].notna()
        usable_rows = int(usable_mask.sum())
    return {
        "required_real_input_features": list(REAL_INPUT_VOLATILITY_FEATURES),
        "null_rate_by_feature": feature_null_rates,
        "non_null_count_by_feature": feature_non_null_counts,
        "usable_row_count_after_real_input_features": usable_rows,
        "source_counts": dict(adapter_report.get("source_counts", {})),
    }


def _combine_adapter_reports(daily_report: dict[str, Any], volatility_report: dict[str, Any]) -> dict[str, Any]:
    computed = [
        *daily_report.get("computed_feature_list", []),
        *volatility_report.get("computed_feature_list", []),
    ]
    computed_non_null = {
        **daily_report.get("computed_non_null_counts", {}),
        **volatility_report.get("computed_non_null_counts", {}),
    }
    insufficient = {
        **daily_report.get("unavailable_due_to_insufficient_lookback", {}),
        **volatility_report.get("unavailable_due_to_insufficient_history", {}),
    }
    no_lookahead = {
        **daily_report.get("no_lookahead_notes", {}),
        **volatility_report.get("no_lookahead_notes", {}),
    }
    daily_not_fabricated = set(daily_report.get("not_fabricated_features", [])) - set(REAL_INPUT_VOLATILITY_FEATURES)
    not_fabricated = sorted(daily_not_fabricated.union(volatility_report.get("not_fabricated_features", [])))
    return {
        "computed_feature_list": computed,
        "deterministic_computed_feature_list": list(daily_report.get("computed_feature_list", [])),
        "real_input_computed_feature_list": list(volatility_report.get("computed_feature_list", [])),
        "computed_non_null_counts": computed_non_null,
        "unavailable_due_to_insufficient_lookback": insufficient,
        "unavailable_due_to_missing_source": dict(volatility_report.get("unavailable_due_to_missing_source", {})),
        "source_counts": dict(volatility_report.get("source_counts", {})),
        "not_fabricated_features": not_fabricated,
        "no_lookahead_notes": no_lookahead,
    }


def _audit_labels(label_candidates: pd.DataFrame, *, requested_horizons: list[str]) -> dict[str, Any]:
    warnings: list[str] = []
    available = sorted(str(value) for value in label_candidates.get("horizon", pd.Series(dtype="string")).dropna().unique())
    expected = list(EXPECTED_TARGET_HORIZONS)
    requested = list(requested_horizons)
    missing_expected = [horizon for horizon in expected if horizon not in available]
    missing_requested = [horizon for horizon in requested if horizon not in available]
    counts = _count_by(label_candidates, "horizon")
    if missing_expected:
        warnings.append("not all expected model target horizons are available in historical labels")
    if missing_requested:
        warnings.append("requested smoke horizons are missing from historical labels")
    return {
        "expected_target_columns": list(EXPECTED_TARGET_COLUMNS),
        "expected_horizons": expected,
        "requested_horizons": requested,
        "available_horizons": available,
        "counts_by_horizon": counts,
        "missing_expected_horizons": missing_expected,
        "missing_requested_horizons": missing_requested,
        "target_horizon_compatibility": "fail" if missing_expected or missing_requested else "pass",
        "warnings": warnings,
    }


def _final_status(feature_audit: dict[str, Any], label_audit: dict[str, Any]) -> str:
    if feature_audit["missing_required_features"]:
        return "fail"
    if feature_audit["all_null_columns"]:
        return "fail"
    if label_audit["target_horizon_compatibility"] == "fail":
        return "fail"
    if feature_audit["dtype_mismatches"] or feature_audit["extra_features"] or feature_audit["null_rate_warnings"]:
        return "warn"
    return "pass"


def _dtype_mismatches(frame: pd.DataFrame) -> dict[str, str]:
    mismatches: dict[str, str] = {}
    for column, expected in EXPECTED_FEATURE_SCHEMA.items():
        if column not in frame.columns:
            continue
        if expected == "numeric":
            coerced = pd.to_numeric(frame[column], errors="coerce")
            if frame[column].notna().any() and coerced.notna().sum() == 0:
                mismatches[column] = "expected numeric"
        elif expected == "date":
            coerced_dates = pd.to_datetime(frame[column], errors="coerce")
            if frame[column].notna().any() and coerced_dates.notna().sum() == 0:
                mismatches[column] = "expected date"
        elif expected == "string" and not pd.api.types.is_string_dtype(frame[column]) and not pd.api.types.is_object_dtype(frame[column]):
            mismatches[column] = "expected string"
    return mismatches


def _nullable_fields(frame: pd.DataFrame) -> dict[str, int]:
    out: dict[str, int] = {}
    for column in EXPECTED_FEATURE_SCHEMA:
        if column in frame.columns:
            count = int(frame[column].isna().sum())
            if count:
                out[column] = count
    return out


def _high_null_warnings(frame: pd.DataFrame) -> dict[str, float]:
    out: dict[str, float] = {}
    if frame.empty:
        return out
    for column in EXPECTED_FEATURE_SCHEMA:
        if column not in frame.columns:
            continue
        rate = float(frame[column].isna().mean())
        if rate >= HIGH_NULL_RATE and rate < 1.0:
            out[column] = rate
    return out


def _constant_columns(frame: pd.DataFrame) -> list[str]:
    constants: list[str] = []
    for column in EXPECTED_FEATURE_SCHEMA:
        if column not in frame.columns:
            continue
        if frame[column].dropna().nunique() <= 1:
            constants.append(column)
    return constants


def _count_by(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    return {str(key): int(value) for key, value in frame[column].value_counts(dropna=False).sort_index().items()}


def _dedupe_warnings(warnings: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for warning in warnings:
        if warning in seen:
            continue
        seen.add(warning)
        out.append(warning)
    return out
