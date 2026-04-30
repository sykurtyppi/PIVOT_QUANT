"""One-month walk-forward dry-run harness for bounded historical slices.

This module builds chronological train/test windows over observed trading days.
It does not train models, tune thresholds, or run a full backtest.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
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
class HistoricalWalkForwardReport:
    windows: list[dict[str, Any]]
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def build_historical_walk_forward_from_t9(
    *,
    symbol: str = "SPY",
    start_date: str,
    end_date: str,
    root: str | Path | None = None,
    max_files: int = 20,
    daily_source: str | None = None,
    horizons: list[str] | None = None,
    train_window: int = 10,
    test_window: int = 5,
    step: int = 5,
) -> HistoricalWalkForwardReport:
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
    return build_historical_walk_forward_report(
        model_ready_daily_features=feature_contract.model_ready_daily_features,
        label_candidates=label_contract.label_candidates,
        symbol=normalized_symbol,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        horizons=horizons or DEFAULT_HORIZONS,
        train_window=train_window,
        test_window=test_window,
        step=step,
        warnings=[*feature_contract.report.get("warnings", []), *label_contract.report.get("warnings", [])],
        source_status={
            "feature_contract": feature_contract.report.get("status"),
            "label_contract": label_contract.report.get("status"),
        },
    )


def build_historical_walk_forward_report(
    *,
    model_ready_daily_features: pd.DataFrame,
    label_candidates: pd.DataFrame,
    symbol: str,
    start_date: str,
    end_date: str,
    horizons: list[str],
    train_window: int,
    test_window: int,
    step: int,
    warnings: list[str] | None = None,
    source_status: dict[str, Any] | None = None,
) -> HistoricalWalkForwardReport:
    normalized_symbol = _normalize_symbol(symbol)
    train_window = _positive_int(train_window, "train_window")
    test_window = _positive_int(test_window, "test_window")
    step = _positive_int(step, "step")
    trading_days = _trading_days(model_ready_daily_features)
    windows = _build_windows(
        trading_days=trading_days,
        label_candidates=label_candidates,
        train_window=train_window,
        test_window=test_window,
        step=step,
    )
    leakage = _leakage_checks(windows, label_candidates)
    status = "fail" if leakage["status"] == "fail" else "pass"
    report = {
        "name": "historical_walk_forward_dry_run",
        "status": status,
        "symbol": normalized_symbol,
        "start_date": start_date,
        "end_date": end_date,
        "read_only": True,
        "training_performed": False,
        "threshold_optimization_performed": False,
        "config": {
            "horizons": list(horizons),
            "train_window_trading_days": train_window,
            "test_window_trading_days": test_window,
            "step_trading_days": step,
        },
        "window_count": int(len(windows)),
        "zero_row_window_count": int(sum(1 for window in windows if window["test_row_count"] == 0)),
        "total_train_rows": int(sum(window["train_row_count"] for window in windows)),
        "total_test_rows": int(sum(window["test_row_count"] for window in windows)),
        "label_coverage_by_horizon": _count_by(label_candidates, "horizon"),
        "forward_return_summary_all": _distribution(label_candidates, "forward_return"),
        "forward_return_summary_by_window": {
            window["window_id"]: window["test_forward_return_summary"] for window in windows
        },
        "warnings": _dedupe_warnings(warnings or []),
        "leakage_checks": leakage,
        "source_status": dict(source_status or {}),
        "windows": windows,
    }
    if not windows:
        report["status"] = "warn"
        report["warnings"].append("no walk-forward windows could be formed from the bounded trading-day slice")
    return HistoricalWalkForwardReport(windows=windows, report=report)


def write_walk_forward_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "historical_walk_forward_smoke"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    stem = f"{report['symbol'].lower()}_{report['start_date']}_{report['end_date']}".replace("/", "-")
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _build_windows(
    *,
    trading_days: list[str],
    label_candidates: pd.DataFrame,
    train_window: int,
    test_window: int,
    step: int,
) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    if len(trading_days) <= train_window:
        return windows
    labels = label_candidates.copy()
    if not labels.empty:
        labels["observation_date"] = pd.to_datetime(labels["observation_date"], errors="coerce").dt.date.astype("string")
    start_index = 0
    window_id = 1
    while start_index + train_window < len(trading_days):
        train_days = trading_days[start_index : start_index + train_window]
        test_days = trading_days[start_index + train_window : start_index + train_window + test_window]
        if not test_days:
            break
        train_rows = _rows_for_dates(labels, train_days)
        test_rows = _rows_for_dates(labels, test_days)
        windows.append(
            {
                "window_id": f"wf_{window_id:03d}",
                "train_start": train_days[0],
                "train_end": train_days[-1],
                "test_start": test_days[0],
                "test_end": test_days[-1],
                "train_trading_days": int(len(train_days)),
                "test_trading_days": int(len(test_days)),
                "train_row_count": int(len(train_rows)),
                "test_row_count": int(len(test_rows)),
                "train_counts_by_horizon": _count_by(train_rows, "horizon"),
                "test_counts_by_horizon": _count_by(test_rows, "horizon"),
                "test_forward_return_summary": _distribution_by(test_rows, "horizon", "forward_return"),
                "zero_row_window": bool(test_rows.empty),
            }
        )
        start_index += step
        window_id += 1
    return windows


def _leakage_checks(windows: list[dict[str, Any]], label_candidates: pd.DataFrame) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    labels = label_candidates.copy()
    if not labels.empty:
        labels["observation_date"] = pd.to_datetime(labels["observation_date"], errors="coerce").dt.date.astype("string")
        labels["label_date"] = pd.to_datetime(labels["label_date"], errors="coerce").dt.date.astype("string")
    for window in windows:
        if not window["train_end"] < window["test_start"]:
            failures.append({"window_id": window["window_id"], "reason": "train_end_not_before_test_start"})
        train_range = _date_range_set(window["train_start"], window["train_end"])
        test_range = _date_range_set(window["test_start"], window["test_end"])
        overlap = sorted(train_range.intersection(test_range))
        if overlap:
            failures.append({"window_id": window["window_id"], "reason": "test_dates_overlap_train", "overlap": overlap[:5]})
    bad_labels: list[dict[str, Any]] = []
    if not labels.empty and {"observation_date", "label_date"}.issubset(labels.columns):
        bad = labels[labels["label_date"] <= labels["observation_date"]]
        for row in bad.head(5).to_dict(orient="records"):
            bad_labels.append(
                {
                    "observation_date": row.get("observation_date"),
                    "label_date": row.get("label_date"),
                    "horizon": row.get("horizon"),
                }
            )
    if bad_labels:
        failures.append({"reason": "label_date_not_after_observation_date", "examples": bad_labels})
    return {
        "status": "fail" if failures else "pass",
        "train_end_before_test_start": not any(item.get("reason") == "train_end_not_before_test_start" for item in failures),
        "no_test_dates_inside_train": not any(item.get("reason") == "test_dates_overlap_train" for item in failures),
        "labels_have_future_dates": not bad_labels,
        "failures": failures,
    }


def _trading_days(frame: pd.DataFrame) -> list[str]:
    if frame.empty or "date" not in frame.columns:
        return []
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna().dt.date
    return [value.isoformat() for value in sorted(dates.unique())]


def _rows_for_dates(frame: pd.DataFrame, dates: list[str]) -> pd.DataFrame:
    if frame.empty or "observation_date" not in frame.columns:
        return pd.DataFrame(columns=frame.columns)
    return frame[frame["observation_date"].isin(dates)].copy()


def _date_range_set(start: str, end: str) -> set[str]:
    dates = pd.date_range(start=start, end=end, freq="D")
    return {value.date().isoformat() for value in dates}


def _positive_int(value: int, name: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"{name} must be >= 1")
    return parsed


def _count_by(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    return {str(key): int(value) for key, value in frame[column].value_counts(dropna=False).sort_index().items()}


def _distribution_by(frame: pd.DataFrame, group_column: str, value_column: str) -> dict[str, dict[str, float | int | None]]:
    if frame.empty or group_column not in frame.columns or value_column not in frame.columns:
        return {}
    out: dict[str, dict[str, float | int | None]] = {}
    for group, group_frame in frame.groupby(group_column, dropna=False):
        out[str(group)] = _distribution(group_frame, value_column)
    return out


def _distribution(frame: pd.DataFrame, value_column: str) -> dict[str, float | int | None]:
    if frame.empty or value_column not in frame.columns:
        return {"count": 0, "mean": None, "median": None, "min": None, "max": None}
    values = pd.to_numeric(frame[value_column], errors="coerce").dropna()
    if values.empty:
        return {"count": 0, "mean": None, "median": None, "min": None, "max": None}
    return {
        "count": int(values.count()),
        "mean": float(values.mean()),
        "median": float(values.median()),
        "min": float(values.min()),
        "max": float(values.max()),
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
