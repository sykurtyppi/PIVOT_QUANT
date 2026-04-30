"""Tiny historical baseline report for mature label candidates.

This is a descriptive validation smoke only. It does not train, score, tune, or
walk forward.
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
from services.external_data.t9_parquet_adapter import _normalize_symbol, _parse_date


@dataclass(frozen=True)
class HistoricalBaselineReport:
    joined_rows: pd.DataFrame
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def build_historical_baseline_report_from_t9(
    *,
    symbol: str = "SPY",
    start_date: str,
    end_date: str,
    root: str | Path | None = None,
    max_files: int = 20,
    daily_source: str | None = None,
    horizons: list[str] | None = None,
) -> HistoricalBaselineReport:
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
    return build_historical_baseline_report(
        model_ready_daily_features=feature_contract.model_ready_daily_features,
        option_context_features=feature_contract.option_context_features,
        label_candidates=label_contract.label_candidates,
        symbol=normalized_symbol,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        horizons=horizons or DEFAULT_HORIZONS,
        warnings=[*feature_contract.report.get("warnings", []), *label_contract.report.get("warnings", [])],
        source_status={
            "feature_contract": feature_contract.report.get("status"),
            "label_contract": label_contract.report.get("status"),
        },
    )


def build_historical_baseline_report(
    *,
    model_ready_daily_features: pd.DataFrame,
    option_context_features: pd.DataFrame,
    label_candidates: pd.DataFrame,
    symbol: str,
    start_date: str,
    end_date: str,
    horizons: list[str],
    warnings: list[str] | None = None,
    source_status: dict[str, Any] | None = None,
) -> HistoricalBaselineReport:
    normalized_symbol = _normalize_symbol(symbol)
    joined = _join_labels_to_option_context(label_candidates, option_context_features)
    report = {
        "name": "historical_baseline_report",
        "status": "pass" if not label_candidates.empty else "warn",
        "symbol": normalized_symbol,
        "start_date": start_date,
        "end_date": end_date,
        "read_only": True,
        "training_performed": False,
        "config": {"horizons": list(horizons)},
        "rows": {
            "model_ready_daily_features": int(len(model_ready_daily_features)),
            "option_context_features": int(len(option_context_features)),
            "mature_label_candidates": int(len(label_candidates)),
            "joined_baseline_rows": int(len(joined)),
        },
        "date_coverage": {
            "daily_features": _date_coverage(model_ready_daily_features, "date"),
            "option_context_features": _date_coverage(option_context_features, "date"),
            "label_candidates_observation": _date_coverage(label_candidates, "observation_date"),
            "label_candidates_label": _date_coverage(label_candidates, "label_date"),
        },
        "mature_label_counts_by_horizon": _count_by(label_candidates, "horizon"),
        "forward_return_distribution_by_horizon": _distribution_by(label_candidates, "horizon", "forward_return"),
        "forward_return_distribution_by_option_type": _distribution_by(joined, "option_type", "forward_return"),
        "forward_return_distribution_by_moneyness_bucket": _distribution_by(joined, "moneyness_bucket", "forward_return"),
        "row_counts_by_horizon_option_type": _two_way_counts(joined, "horizon", "option_type"),
        "row_counts_by_horizon_moneyness_bucket": _two_way_counts(joined, "horizon", "moneyness_bucket"),
        "missing_values": {
            "model_ready_daily_features": _missing_value_counts(model_ready_daily_features),
            "option_context_features": _missing_value_counts(option_context_features),
            "label_candidates": _missing_value_counts(label_candidates),
            "joined_baseline_rows": _missing_value_counts(joined),
        },
        "warnings": _dedupe_warnings(warnings or []),
        "source_status": dict(source_status or {}),
        "samples": {
            "joined_baseline_rows": _sample_records(joined),
        },
    }
    if label_candidates.empty:
        report["warnings"].append("no mature label candidates were available for baseline reporting")
    return HistoricalBaselineReport(joined_rows=joined, report=report)


def write_baseline_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "historical_baseline_smoke"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    stem = f"{report['symbol'].lower()}_{report['start_date']}_{report['end_date']}".replace("/", "-")
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _join_labels_to_option_context(labels: pd.DataFrame, options: pd.DataFrame) -> pd.DataFrame:
    if labels.empty or options.empty:
        return pd.DataFrame()
    label_frame = labels.copy()
    option_frame = options.copy()
    label_frame["observation_date"] = pd.to_datetime(label_frame["observation_date"], errors="coerce").dt.date.astype("string")
    option_frame["date"] = pd.to_datetime(option_frame["date"], errors="coerce").dt.date.astype("string")
    join_keys_left = ["observation_date", "underlying_symbol", "expiration", "strike", "option_type"]
    join_keys_right = ["date", "underlying_symbol", "expiration", "strike", "option_type"]
    merged = label_frame.merge(
        option_frame,
        left_on=join_keys_left,
        right_on=join_keys_right,
        how="left",
        suffixes=("", "_option"),
    )
    merged["moneyness_bucket"] = _moneyness_bucket_series(merged.get("moneyness"))
    return merged


def _moneyness_bucket_series(values: pd.Series | None) -> pd.Series:
    if values is None:
        return pd.Series(dtype="string")
    numeric = pd.to_numeric(values, errors="coerce")
    buckets = pd.cut(
        numeric,
        bins=[float("-inf"), -0.05, -0.01, 0.01, 0.05, float("inf")],
        labels=["deep_itm_put_or_otm_call", "near_itm", "atm", "near_otm", "deep_otm_call_or_itm_put"],
    )
    return buckets.astype("string").fillna("unknown")


def _count_by(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    return {str(key): int(value) for key, value in frame[column].value_counts(dropna=False).sort_index().items()}


def _two_way_counts(frame: pd.DataFrame, row_column: str, col_column: str) -> dict[str, dict[str, int]]:
    if frame.empty or row_column not in frame.columns or col_column not in frame.columns:
        return {}
    table = pd.crosstab(frame[row_column], frame[col_column], dropna=False)
    return {
        str(index): {str(column): int(value) for column, value in row.items()}
        for index, row in table.iterrows()
    }


def _distribution_by(frame: pd.DataFrame, group_column: str, value_column: str) -> dict[str, dict[str, float | int | None]]:
    if frame.empty or group_column not in frame.columns or value_column not in frame.columns:
        return {}
    values = frame[[group_column, value_column]].copy()
    values[value_column] = pd.to_numeric(values[value_column], errors="coerce")
    values = values.dropna(subset=[value_column])
    out: dict[str, dict[str, float | int | None]] = {}
    for group, group_frame in values.groupby(group_column, dropna=False):
        series = group_frame[value_column]
        out[str(group)] = {
            "count": int(series.count()),
            "mean": _safe_float(series.mean()),
            "median": _safe_float(series.median()),
            "min": _safe_float(series.min()),
            "max": _safe_float(series.max()),
            "p25": _safe_float(series.quantile(0.25)),
            "p75": _safe_float(series.quantile(0.75)),
        }
    return out


def _safe_float(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def _date_coverage(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    if frame.empty or column not in frame.columns:
        return {"min": None, "max": None, "unique_dates": 0}
    dates = pd.to_datetime(frame[column], errors="coerce").dropna()
    if dates.empty:
        return {"min": None, "max": None, "unique_dates": 0}
    return {
        "min": dates.min().date().isoformat(),
        "max": dates.max().date().isoformat(),
        "unique_dates": int(dates.dt.date.nunique()),
    }


def _missing_value_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty:
        return {}
    return {
        str(column): int(count)
        for column, count in frame.isna().sum().items()
        if int(count) > 0
    }


def _dedupe_warnings(warnings: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for warning in warnings:
        if warning in seen:
            continue
        seen.add(warning)
        out.append(warning)
    return out


def _sample_records(frame: pd.DataFrame, *, sample_size: int = 3) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return json.loads(frame.head(sample_size).to_json(orient="records", date_format="iso"))
