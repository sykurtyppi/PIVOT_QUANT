"""Deterministic rule-based baseline inside walk-forward windows.

This is benchmark plumbing only. It uses fixed, documented entry-time filters
and evaluates mature labels after selection. It does not train, tune, or search.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.historical_feature_contract import (
    build_historical_feature_contract_from_t9,
)
from services.external_data.historical_label_contract import (
    DEFAULT_HORIZONS,
    build_historical_label_contract,
)
from services.external_data.historical_walk_forward import build_historical_walk_forward_report
from services.external_data.t9_parquet_adapter import _normalize_symbol, _parse_date


FORBIDDEN_SELECTION_COLUMNS = {
    "forward_return",
    "future_underlying_close",
    "label_date",
    "label_status",
    "target",
    "outcome",
    "realized",
}
BASELINE_SELECTION_COLUMNS = [
    "moneyness",
    "bid",
    "ask",
    "mid",
    "volume",
    "open_interest",
    "relative_spread",
]


@dataclass(frozen=True)
class RuleBaselineConfig:
    max_abs_moneyness: float = 0.01
    min_volume: int = 1
    min_open_interest: int = 1
    max_relative_spread: float = 0.25
    atm_or_near_atm_only: bool = True
    allowed_moneyness_buckets: list[str] = field(default_factory=lambda: ["atm", "near_itm", "near_otm"])

    def as_dict(self) -> dict[str, Any]:
        return {
            "max_abs_moneyness": self.max_abs_moneyness,
            "min_volume": self.min_volume,
            "min_open_interest": self.min_open_interest,
            "max_relative_spread": self.max_relative_spread,
            "atm_or_near_atm_only": self.atm_or_near_atm_only,
            "allowed_moneyness_buckets": list(self.allowed_moneyness_buckets),
            "selection_columns": list(BASELINE_SELECTION_COLUMNS),
            "forbidden_selection_columns": sorted(FORBIDDEN_SELECTION_COLUMNS),
        }


@dataclass(frozen=True)
class HistoricalRuleBaselineReport:
    joined_rows: pd.DataFrame
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def build_historical_rule_baseline_from_t9(
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
    config: RuleBaselineConfig | None = None,
) -> HistoricalRuleBaselineReport:
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
    walk_forward = build_historical_walk_forward_report(
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
    return build_historical_rule_baseline_report(
        option_context_features=feature_contract.option_context_features,
        label_candidates=label_contract.label_candidates,
        walk_forward_windows=walk_forward.windows,
        symbol=normalized_symbol,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        horizons=horizons or DEFAULT_HORIZONS,
        config=config or RuleBaselineConfig(),
        warnings=walk_forward.report.get("warnings", []),
        source_status={
            **walk_forward.report.get("source_status", {}),
            "walk_forward": walk_forward.report.get("status"),
        },
    )


def build_historical_rule_baseline_report(
    *,
    option_context_features: pd.DataFrame,
    label_candidates: pd.DataFrame,
    walk_forward_windows: list[dict[str, Any]],
    symbol: str,
    start_date: str,
    end_date: str,
    horizons: list[str],
    config: RuleBaselineConfig | None = None,
    warnings: list[str] | None = None,
    source_status: dict[str, Any] | None = None,
) -> HistoricalRuleBaselineReport:
    normalized_symbol = _normalize_symbol(symbol)
    baseline_config = config or RuleBaselineConfig()
    validate_selection_columns(BASELINE_SELECTION_COLUMNS)
    joined = _join_labels_to_option_context(label_candidates, option_context_features)
    windows = [
        _summarize_window(joined, window, baseline_config) for window in walk_forward_windows
    ]
    leakage = _selection_leakage_report()
    status = "fail" if leakage["status"] == "fail" else "pass"
    if not windows:
        status = "warn"
    report = {
        "name": "historical_rule_baseline",
        "status": status,
        "symbol": normalized_symbol,
        "start_date": start_date,
        "end_date": end_date,
        "read_only": True,
        "training_performed": False,
        "threshold_optimization_performed": False,
        "performance_claim": False,
        "config": {
            "horizons": list(horizons),
            "baseline_rule": baseline_config.as_dict(),
        },
        "rows": {
            "option_context_features": int(len(option_context_features)),
            "mature_label_candidates": int(len(label_candidates)),
            "joined_rows": int(len(joined)),
        },
        "window_count": int(len(windows)),
        "zero_row_window_count": int(sum(1 for window in windows if window["test"]["non_evaluable"])),
        "train_selected_rows_total": int(sum(window["train"]["selected_rows"] for window in windows)),
        "test_selected_rows_total": int(sum(window["test"]["selected_rows"] for window in windows)),
        "leakage_checks": leakage,
        "warnings": _dedupe_warnings(warnings or []),
        "source_status": dict(source_status or {}),
        "windows": windows,
    }
    if not windows:
        report["warnings"].append("no walk-forward windows were available for rule baseline evaluation")
    return HistoricalRuleBaselineReport(joined_rows=joined, report=report)


def validate_selection_columns(columns: list[str]) -> None:
    forbidden = sorted(set(columns).intersection(FORBIDDEN_SELECTION_COLUMNS))
    if forbidden:
        raise ValueError(f"selection logic cannot use label/outcome columns: {', '.join(forbidden)}")


def write_rule_baseline_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "historical_rule_baseline_smoke"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    stem = f"{report['symbol'].lower()}_{report['start_date']}_{report['end_date']}".replace("/", "-")
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _summarize_window(
    joined: pd.DataFrame,
    window: dict[str, Any],
    config: RuleBaselineConfig,
) -> dict[str, Any]:
    train_rows = _rows_for_window(joined, window["train_start"], window["train_end"])
    test_rows = _rows_for_window(joined, window["test_start"], window["test_end"])
    return {
        "window_id": window["window_id"],
        "train_start": window["train_start"],
        "train_end": window["train_end"],
        "test_start": window["test_start"],
        "test_end": window["test_end"],
        "train": _summarize_split(train_rows, config),
        "test": _summarize_split(test_rows, config),
    }


def _summarize_split(rows: pd.DataFrame, config: RuleBaselineConfig) -> dict[str, Any]:
    eligible_mask, missing_counts = _eligible_mask(rows)
    eligible = rows[eligible_mask].copy()
    selected_mask = _selection_mask(eligible, config)
    selected = eligible[selected_mask].copy()
    missing_labels = int(pd.to_numeric(rows.get("forward_return"), errors="coerce").isna().sum()) if not rows.empty else 0
    selected_label_missing = (
        int(pd.to_numeric(selected.get("forward_return"), errors="coerce").isna().sum()) if not selected.empty else 0
    )
    summary = _return_summary(selected)
    return {
        "input_rows": int(len(rows)),
        "eligible_rows": int(len(eligible)),
        "selected_rows": int(len(selected)),
        "missing_label_count": missing_labels,
        "selected_missing_label_count": selected_label_missing,
        "non_evaluable": bool(selected.empty or selected_label_missing == len(selected)),
        "zero_row": bool(rows.empty),
        "filter_missing_counts": missing_counts,
        "counts_by_horizon": _count_by(selected, "horizon"),
        "counts_by_option_type": _count_by(selected, "option_type"),
        "forward_return": summary,
    }


def _eligible_mask(rows: pd.DataFrame) -> tuple[pd.Series, dict[str, int]]:
    if rows.empty:
        return pd.Series(dtype=bool), {}
    required = ["moneyness", "volume", "open_interest", "relative_spread", "forward_return"]
    mask = pd.Series(True, index=rows.index)
    missing_counts: dict[str, int] = {}
    for column in required:
        if column not in rows.columns:
            missing_counts[column] = int(len(rows))
            mask &= False
            continue
        values = pd.to_numeric(rows[column], errors="coerce")
        missing_counts[column] = int(values.isna().sum())
        mask &= values.notna()
    return mask, {key: value for key, value in missing_counts.items() if value}


def _selection_mask(rows: pd.DataFrame, config: RuleBaselineConfig) -> pd.Series:
    if rows.empty:
        return pd.Series(dtype=bool)
    validate_selection_columns(BASELINE_SELECTION_COLUMNS)
    moneyness = pd.to_numeric(rows["moneyness"], errors="coerce")
    volume = pd.to_numeric(rows["volume"], errors="coerce")
    open_interest = pd.to_numeric(rows["open_interest"], errors="coerce")
    relative_spread = pd.to_numeric(rows["relative_spread"], errors="coerce")
    mask = (
        moneyness.abs().le(config.max_abs_moneyness)
        & volume.ge(config.min_volume)
        & open_interest.ge(config.min_open_interest)
        & relative_spread.le(config.max_relative_spread)
    )
    if config.atm_or_near_atm_only and "moneyness_bucket" in rows.columns:
        mask &= rows["moneyness_bucket"].astype("string").isin(config.allowed_moneyness_buckets)
    return mask


def _join_labels_to_option_context(labels: pd.DataFrame, options: pd.DataFrame) -> pd.DataFrame:
    if labels.empty or options.empty:
        return pd.DataFrame()
    label_frame = labels.copy()
    option_frame = options.copy()
    label_frame["observation_date"] = pd.to_datetime(label_frame["observation_date"], errors="coerce").dt.date.astype("string")
    option_frame["date"] = pd.to_datetime(option_frame["date"], errors="coerce").dt.date.astype("string")
    merged = label_frame.merge(
        option_frame,
        left_on=["observation_date", "underlying_symbol", "expiration", "strike", "option_type"],
        right_on=["date", "underlying_symbol", "expiration", "strike", "option_type"],
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


def _rows_for_window(rows: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if rows.empty or "observation_date" not in rows.columns:
        return pd.DataFrame(columns=rows.columns)
    dates = pd.to_datetime(rows["observation_date"], errors="coerce").dt.date.astype("string")
    return rows[(dates >= start) & (dates <= end)].copy()


def _return_summary(rows: pd.DataFrame) -> dict[str, float | int | None]:
    if rows.empty or "forward_return" not in rows.columns:
        return {"sample_size": 0, "mean": None, "median": None, "win_rate": None}
    values = pd.to_numeric(rows["forward_return"], errors="coerce").dropna()
    if values.empty:
        return {"sample_size": 0, "mean": None, "median": None, "win_rate": None}
    return {
        "sample_size": int(values.count()),
        "mean": float(values.mean()),
        "median": float(values.median()),
        "win_rate": float((values > 0).mean()),
    }


def _selection_leakage_report() -> dict[str, Any]:
    forbidden = sorted(set(BASELINE_SELECTION_COLUMNS).intersection(FORBIDDEN_SELECTION_COLUMNS))
    return {
        "status": "fail" if forbidden else "pass",
        "selection_uses_entry_time_fields_only": not forbidden,
        "forward_labels_used_for_evaluation_only": not forbidden,
        "forbidden_selection_columns": forbidden,
    }


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
