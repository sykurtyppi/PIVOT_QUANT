"""Bounded model-ready dataset export from historical compatibility rows.

This module writes tiny, reproducible feature/label artifacts for future
training work. It does not train models, optimize thresholds, or scan full
history.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.historical_feature_contract import build_historical_feature_contract_from_t9
from services.external_data.historical_label_contract import DEFAULT_HORIZONS, build_historical_label_contract
from services.external_data.model_input_compatibility import (
    EXPECTED_FEATURE_SCHEMA,
    EXPECTED_TARGET_COLUMNS,
    build_model_input_compatibility_report,
)
from services.external_data.model_volatility_adapter import load_vix_daily_level_from_t9
from services.external_data.t9_parquet_adapter import _normalize_symbol, _parse_date


FEATURE_COLUMNS = list(EXPECTED_FEATURE_SCHEMA.keys())
FEATURE_DATA_COLUMNS = [col for col in FEATURE_COLUMNS if col not in {"symbol", "date"}]
LABEL_COLUMNS = list(EXPECTED_TARGET_COLUMNS)
IDENTITY_COLUMNS = ["symbol", "entry_date"]
QUALITY_COLUMNS = ["missing_required_feature_count", "missing_required_label_count"]
EXPORT_COLUMNS = IDENTITY_COLUMNS + FEATURE_DATA_COLUMNS + LABEL_COLUMNS
LEAKY_FEATURE_PATTERNS = ("future_", "forward_", "label", "target", "outcome")


@dataclass(frozen=True)
class ModelReadyDatasetExport:
    dataset: pd.DataFrame
    metadata: dict[str, Any]

    def json_metadata(self) -> str:
        return json.dumps(self.metadata, indent=2, default=str) + "\n"


def build_model_ready_dataset_from_t9(
    *,
    symbol: str = "SPY",
    start_date: str | None = None,
    end_date: str | None = None,
    analysis_start_date: str | None = None,
    analysis_end_date: str | None = None,
    feature_lookback_days: int = 0,
    label_lookahead_days: int = 0,
    root: str | Path | None = None,
    max_files: int = 20,
    daily_source: str | None = None,
    horizons: list[str] | None = None,
    missing_feature_policy: str = "drop",
    missing_label_policy: str = "flag",
) -> ModelReadyDatasetExport:
    normalized_symbol = _normalize_symbol(symbol)
    analysis_start = _parse_date(analysis_start_date or start_date)
    analysis_end = _parse_date(analysis_end_date or end_date)
    if analysis_end < analysis_start:
        raise ValueError("analysis_end_date must be on or after analysis_start_date")
    feature_lookback_days = max(0, int(feature_lookback_days))
    label_lookahead_days = max(0, int(label_lookahead_days))
    read_start = analysis_start - timedelta(days=feature_lookback_days)
    read_end = analysis_end + timedelta(days=label_lookahead_days)

    feature_contract = build_historical_feature_contract_from_t9(
        symbol=normalized_symbol,
        start_date=read_start.isoformat(),
        end_date=read_end.isoformat(),
        root=root,
        max_files=max_files,
        daily_source=daily_source,
    )
    requested_horizons = horizons or DEFAULT_HORIZONS
    label_contract = build_historical_label_contract(
        label_ready_rows=feature_contract.label_ready_rows,
        model_ready_daily_features=feature_contract.model_ready_daily_features,
        symbol=normalized_symbol,
        start=read_start,
        end=read_end,
        horizons=requested_horizons,
        source_report=feature_contract.report,
    )
    vix = load_vix_daily_level_from_t9(root=root, start=read_start, end=read_end, max_files=max_files)
    compatibility = build_model_input_compatibility_report(
        model_ready_daily_features=feature_contract.model_ready_daily_features,
        option_context_features=feature_contract.option_context_features,
        label_candidates=label_contract.label_candidates,
        vix_daily_features=vix.rows,
        symbol=normalized_symbol,
        start_date=read_start.isoformat(),
        end_date=read_end.isoformat(),
        requested_horizons=requested_horizons,
        warnings=[*feature_contract.report.get("warnings", []), *label_contract.report.get("warnings", []), *vix.warnings],
        source_status={
            "feature_contract": feature_contract.report.get("status"),
            "label_contract": label_contract.report.get("status"),
            "vix_rows": int(len(vix.rows)),
        },
    )
    return build_model_ready_dataset_export(
        compatibility_rows=compatibility.available_rows,
        compatibility_report=compatibility.report,
        label_candidates=label_contract.label_candidates,
        symbol=normalized_symbol,
        start_date=analysis_start.isoformat(),
        end_date=analysis_end.isoformat(),
        read_start_date=read_start.isoformat(),
        read_end_date=read_end.isoformat(),
        feature_lookback_days=feature_lookback_days,
        label_lookahead_days=label_lookahead_days,
        requested_horizons=requested_horizons,
        missing_feature_policy=missing_feature_policy,
        missing_label_policy=missing_label_policy,
    )


def build_model_ready_dataset_export(
    *,
    compatibility_rows: pd.DataFrame,
    compatibility_report: dict[str, Any],
    label_candidates: pd.DataFrame,
    symbol: str,
    start_date: str,
    end_date: str,
    read_start_date: str | None = None,
    read_end_date: str | None = None,
    feature_lookback_days: int = 0,
    label_lookahead_days: int = 0,
    requested_horizons: list[str],
    missing_feature_policy: str = "drop",
    missing_label_policy: str = "flag",
) -> ModelReadyDatasetExport:
    _validate_policy("missing_feature_policy", missing_feature_policy)
    _validate_policy("missing_label_policy", missing_label_policy)
    normalized_symbol = _normalize_symbol(symbol)
    features_all = _stable_feature_frame(compatibility_rows, symbol=normalized_symbol)
    features = _filter_analysis_window(features_all, start_date=start_date, end_date=end_date)
    labels = _merge_label_frames(_pivot_label_candidates(label_candidates), _forward_volatility_21d_labels(features_all))
    dataset = features.merge(labels, on="entry_date", how="left")
    dataset = _ensure_label_columns(dataset)
    dataset = _coerce_dataset_dtypes(dataset)

    missing_features = dataset[IDENTITY_COLUMNS + FEATURE_DATA_COLUMNS].isna().sum(axis=1)
    missing_labels = dataset[LABEL_COLUMNS].isna().sum(axis=1)
    feature_drop_mask = missing_features > 0
    label_drop_mask = missing_labels > 0

    drop_reasons: dict[str, int] = {
        "missing_required_features": int(feature_drop_mask.sum()),
        "missing_required_labels": 0,
    }
    quality_columns: list[str] = []
    if missing_feature_policy == "drop":
        dataset = dataset.loc[~feature_drop_mask].copy()
        missing_features = missing_features.loc[dataset.index]
        missing_labels = missing_labels.loc[dataset.index]
    else:
        dataset["missing_required_feature_count"] = missing_features.astype("Int64")
        quality_columns.append("missing_required_feature_count")

    if missing_label_policy == "drop":
        drop_reasons["missing_required_labels"] = int(label_drop_mask.loc[dataset.index].sum())
        dataset = dataset.loc[~label_drop_mask.loc[dataset.index]].copy()
        missing_features = missing_features.loc[dataset.index]
        missing_labels = missing_labels.loc[dataset.index]
    else:
        dataset["missing_required_label_count"] = missing_labels.astype("Int64")
        quality_columns.append("missing_required_label_count")

    ordered_columns = EXPORT_COLUMNS + quality_columns
    dataset = dataset[ordered_columns].sort_values("entry_date").reset_index(drop=True)
    metadata = _metadata(
        dataset=dataset,
        compatibility_report=compatibility_report,
        symbol=normalized_symbol,
        start_date=start_date,
        end_date=end_date,
        requested_horizons=requested_horizons,
        missing_feature_policy=missing_feature_policy,
        missing_label_policy=missing_label_policy,
        rows_input=len(features),
        rows_read_input=len(features_all),
        read_start_date=read_start_date or start_date,
        read_end_date=read_end_date or end_date,
        feature_lookback_days=feature_lookback_days,
        label_lookahead_days=label_lookahead_days,
        drop_reasons=drop_reasons,
        quality_columns=quality_columns,
    )
    return ModelReadyDatasetExport(dataset=dataset, metadata=metadata)


def write_model_ready_dataset_export(export: ModelReadyDatasetExport, *, reports_dir: Path | None = None) -> dict[str, Path]:
    base = reports_dir or Path.cwd() / "reports" / "model_ready_dataset_smoke"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    stem = f"{export.metadata['symbol'].lower()}_{export.metadata['start_date']}_{export.metadata['end_date']}".replace("/", "-")
    metadata_path = base / f"{stem}.metadata.json"
    metadata_path.write_text(export.json_metadata(), encoding="utf-8")

    parquet_path = base / f"{stem}.parquet"
    csv_path = base / f"{stem}.csv"
    try:
        export.dataset.to_parquet(parquet_path, index=False)
        export.metadata["export_format"] = "parquet"
        export.metadata["dataset_path"] = str(parquet_path)
        metadata_path.write_text(export.json_metadata(), encoding="utf-8")
        return {"dataset": parquet_path, "metadata": metadata_path}
    except Exception as exc:
        export.dataset.to_csv(csv_path, index=False)
        export.metadata["export_format"] = "csv"
        export.metadata["dataset_path"] = str(csv_path)
        export.metadata["export_warnings"] = [f"parquet export unavailable; wrote CSV fallback: {exc}"]
        metadata_path.write_text(export.json_metadata(), encoding="utf-8")
        return {"dataset": csv_path, "metadata": metadata_path}


def _stable_feature_frame(rows: pd.DataFrame, *, symbol: str) -> pd.DataFrame:
    _reject_leaky_feature_columns(rows)
    frame = rows.copy()
    for column in FEATURE_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    frame["symbol"] = frame["symbol"].fillna(symbol).astype("string")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date.astype("string")
    frame = frame.drop_duplicates(subset=["symbol", "date"], keep="first")
    frame["entry_date"] = frame["date"]
    return frame[IDENTITY_COLUMNS + FEATURE_DATA_COLUMNS].reset_index(drop=True)


def _filter_analysis_window(frame: pd.DataFrame, *, start_date: str, end_date: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    dates = pd.to_datetime(frame["entry_date"], errors="coerce").dt.date
    return frame[(dates >= start) & (dates <= end)].copy().reset_index(drop=True)


def _pivot_label_candidates(label_candidates: pd.DataFrame) -> pd.DataFrame:
    if label_candidates.empty:
        return pd.DataFrame(columns=["entry_date", *LABEL_COLUMNS])
    labels = label_candidates.copy()
    labels["entry_date"] = pd.to_datetime(labels["observation_date"], errors="coerce").dt.date.astype("string")
    labels["horizon"] = labels["horizon"].astype("string")
    labels["forward_return"] = pd.to_numeric(labels["forward_return"], errors="coerce")
    labels = labels.dropna(subset=["entry_date", "horizon", "forward_return"])
    if labels.empty:
        return pd.DataFrame(columns=["entry_date", *LABEL_COLUMNS])
    pivot = labels.pivot_table(
        index="entry_date",
        columns="horizon",
        values="forward_return",
        aggfunc="first",
    ).reset_index()
    pivot.columns = [str(col) for col in pivot.columns]
    rename = {horizon: f"forward_return_{horizon}" for horizon in pivot.columns if horizon != "entry_date"}
    return pivot.rename(columns=rename)


def _merge_label_frames(returns: pd.DataFrame, volatility: pd.DataFrame) -> pd.DataFrame:
    if returns.empty:
        return volatility
    if volatility.empty:
        return returns
    return returns.merge(volatility, on="entry_date", how="outer")


def _forward_volatility_21d_labels(feature_rows: pd.DataFrame) -> pd.DataFrame:
    if feature_rows.empty or "entry_date" not in feature_rows.columns or "underlying_price" not in feature_rows.columns:
        return pd.DataFrame(columns=["entry_date", "forward_volatility_21d"])
    rows = feature_rows[["entry_date", "underlying_price"]].copy()
    rows["entry_date"] = pd.to_datetime(rows["entry_date"], errors="coerce")
    rows["underlying_price"] = pd.to_numeric(rows["underlying_price"], errors="coerce")
    rows = rows.dropna(subset=["entry_date", "underlying_price"]).sort_values("entry_date").drop_duplicates("entry_date")
    prices = rows["underlying_price"].reset_index(drop=True)
    forward_vol: list[float | Any] = []
    for index in range(len(prices)):
        future_prices = prices.iloc[index : index + 22]
        if len(future_prices) < 22:
            forward_vol.append(pd.NA)
            continue
        future_returns = future_prices.pct_change().dropna()
        forward_vol.append(float(future_returns.std() * (252 ** 0.5)) if len(future_returns) == 21 else pd.NA)
    return pd.DataFrame(
        {
            "entry_date": rows["entry_date"].dt.date.astype("string"),
            "forward_volatility_21d": forward_vol,
        }
    )


def _ensure_label_columns(dataset: pd.DataFrame) -> pd.DataFrame:
    frame = dataset.copy()
    for column in LABEL_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame


def _coerce_dataset_dtypes(dataset: pd.DataFrame) -> pd.DataFrame:
    frame = dataset.copy()
    for column in ["symbol", "entry_date", "date"]:
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    for column in FEATURE_DATA_COLUMNS + LABEL_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Float64")
    return frame


def _metadata(
    *,
    dataset: pd.DataFrame,
    compatibility_report: dict[str, Any],
    symbol: str,
    start_date: str,
    end_date: str,
    read_start_date: str,
    read_end_date: str,
    feature_lookback_days: int,
    label_lookahead_days: int,
    requested_horizons: list[str],
    missing_feature_policy: str,
    missing_label_policy: str,
    rows_input: int,
    rows_read_input: int,
    drop_reasons: dict[str, int],
    quality_columns: list[str],
) -> dict[str, Any]:
    label_null_counts = {column: int(dataset[column].isna().sum()) for column in LABEL_COLUMNS if column in dataset.columns}
    fully_labeled_rows = int(dataset[LABEL_COLUMNS].notna().all(axis=1).sum()) if not dataset.empty else 0
    feature_null_rates = _null_rates(dataset, FEATURE_COLUMNS)
    label_null_rates = _null_rates(dataset, LABEL_COLUMNS)
    warnings = list(compatibility_report.get("warnings") or [])
    if any(label_null_counts.values()):
        warnings.append("exported dataset contains missing label values")
    if drop_reasons["missing_required_features"]:
        warnings.append("rows with missing required model features were dropped")
    status = "pass"
    if int(len(dataset)) == 0 or compatibility_report.get("status") == "fail":
        status = "fail"
    elif warnings or compatibility_report.get("status") == "warn":
        status = "warn"
    return {
        "name": "model_ready_dataset_smoke",
        "status": status,
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "read_start_date": read_start_date,
        "read_end_date": read_end_date,
        "analysis_start_date": start_date,
        "analysis_end_date": end_date,
        "read_only": True,
        "training_performed": False,
        "threshold_optimization_performed": False,
        "source": {
            "compatibility_status": compatibility_report.get("status"),
            "schema_source": compatibility_report.get("schema_source"),
            "requested_horizons": list(requested_horizons),
        },
        "config": {
            "missing_feature_policy": missing_feature_policy,
            "missing_label_policy": missing_label_policy,
            "feature_lookback_days": int(feature_lookback_days),
            "label_lookahead_days": int(label_lookahead_days),
        },
        "windows": {
            "read_start_date": read_start_date,
            "read_end_date": read_end_date,
            "analysis_start_date": start_date,
            "analysis_end_date": end_date,
            "feature_lookback_days": int(feature_lookback_days),
            "label_lookahead_days": int(label_lookahead_days),
        },
        "rows": {
            "read_input": int(rows_read_input),
            "input": int(rows_input),
            "exported": int(len(dataset)),
            "dropped": int(rows_input - len(dataset)),
        },
        "drop_reasons": dict(drop_reasons),
        "feature_columns": list(FEATURE_COLUMNS),
        "label_columns": list(LABEL_COLUMNS),
        "quality_columns": list(quality_columns),
        "feature_count": int(len(FEATURE_COLUMNS)),
        "label_count": int(len(LABEL_COLUMNS)),
        "column_order": list(dataset.columns),
        "dtypes": {column: str(dtype) for column, dtype in dataset.dtypes.items()},
        "null_rates": {
            "features": feature_null_rates,
            "labels": label_null_rates,
        },
        "label_null_counts": label_null_counts,
        "fully_labeled_row_count": fully_labeled_rows,
        "compatibility_warnings": list(compatibility_report.get("warnings") or []),
        "warnings": _dedupe(warnings),
        "no_lookahead_metadata": {
            "feature_notes": dict(compatibility_report.get("no_lookahead_notes") or {}),
            "labels_separated_from_features": True,
            "future_label_columns_excluded_from_features": True,
            "feature_rows_restricted_to_analysis_window": True,
            "future_rows_used_only_for_label_construction": True,
            "forward_volatility_21d_constructed_as_label_only": True,
        },
        "leakage_checks": {
            "exported_rows_inside_analysis_window": _rows_inside_window(dataset, start_date=start_date, end_date=end_date),
            "future_label_columns_excluded_from_features": True,
            "labels_separated_from_features": True,
            "read_window_contains_feature_lookback": read_start_date <= start_date,
            "read_window_contains_label_lookahead": read_end_date >= end_date,
        },
    }


def _rows_inside_window(dataset: pd.DataFrame, *, start_date: str, end_date: str) -> bool:
    if dataset.empty:
        return True
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    dates = pd.to_datetime(dataset["entry_date"], errors="coerce").dt.date
    return bool(((dates >= start) & (dates <= end)).all())


def _null_rates(frame: pd.DataFrame, columns: list[str]) -> dict[str, float | None]:
    if frame.empty:
        return {column: None for column in columns}
    return {
        column: float(frame[column].isna().mean()) if column in frame.columns else None
        for column in columns
    }


def _reject_leaky_feature_columns(rows: pd.DataFrame) -> None:
    allowed = set(EXPECTED_FEATURE_SCHEMA)
    leaky = []
    for column in rows.columns:
        name = str(column)
        if name in allowed:
            continue
        lowered = name.lower()
        if any(pattern in lowered for pattern in LEAKY_FEATURE_PATTERNS):
            leaky.append(name)
    if leaky:
        raise ValueError(f"model-ready dataset export cannot consume leaky feature columns: {', '.join(sorted(leaky))}")


def _validate_policy(name: str, value: str) -> None:
    if value not in {"drop", "flag"}:
        raise ValueError(f"{name} must be 'drop' or 'flag'")


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
