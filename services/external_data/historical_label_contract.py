"""Historical label smoke contract for bounded T9 slices.

This module creates realized label candidates only when a future daily close is
available inside the bounded slice. It does not train, score, or backtest.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.historical_feature_contract import (
    LABEL_READY_COLUMNS,
    MODEL_READY_DAILY_COLUMNS,
    build_historical_feature_contract_from_t9,
)
from services.external_data.t9_parquet_adapter import _normalize_symbol, _parse_date


DEFAULT_HORIZONS = ["1d", "5d"]
LABEL_CANDIDATE_COLUMNS = [
    "observation_date",
    "label_date",
    "horizon",
    "underlying_symbol",
    "expiration",
    "strike",
    "option_type",
    "days_to_expiration",
    "underlying_close",
    "future_underlying_close",
    "forward_return",
    "label_status",
]


@dataclass(frozen=True)
class HistoricalLabelContract:
    label_candidates: pd.DataFrame
    excluded_rows: pd.DataFrame
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def build_historical_label_contract_from_t9(
    *,
    symbol: str = "SPY",
    start_date: str,
    end_date: str,
    root: str | Path | None = None,
    max_files: int = 20,
    daily_source: str | None = None,
    horizons: list[str] | None = None,
) -> HistoricalLabelContract:
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
    return build_historical_label_contract(
        label_ready_rows=feature_contract.label_ready_rows,
        model_ready_daily_features=feature_contract.model_ready_daily_features,
        symbol=normalized_symbol,
        start=start,
        end=end,
        horizons=horizons,
        source_report=feature_contract.report,
    )


def build_historical_label_contract(
    *,
    label_ready_rows: pd.DataFrame,
    model_ready_daily_features: pd.DataFrame,
    symbol: str,
    start: date,
    end: date,
    horizons: list[str] | None = None,
    source_report: dict[str, Any] | None = None,
) -> HistoricalLabelContract:
    normalized_symbol = _normalize_symbol(symbol)
    parsed_horizons = parse_horizons(horizons or DEFAULT_HORIZONS)
    checks: list[dict[str, Any]] = []
    warnings: list[str] = []
    if source_report:
        warnings.extend(source_report.get("warnings") or [])

    def add_check(name: str, status: str, detail: str, **extra: Any) -> None:
        payload = {"name": name, "status": status, "detail": detail}
        payload.update(extra)
        checks.append(payload)

    label_ready_missing = _missing_columns(label_ready_rows, LABEL_READY_COLUMNS)
    daily_missing = _missing_columns(model_ready_daily_features, MODEL_READY_DAILY_COLUMNS)
    add_check(
        "label_ready_schema",
        "fail" if label_ready_missing else "pass",
        "label-ready rows have required fields" if not label_ready_missing else "label-ready rows are missing fields",
        missing_columns=label_ready_missing,
    )
    add_check(
        "daily_feature_schema",
        "fail" if daily_missing else "pass",
        "model-ready daily features have required fields"
        if not daily_missing
        else "model-ready daily features are missing fields",
        missing_columns=daily_missing,
    )

    out_of_bounds = {
        "label_ready_rows": _dates_outside_window(label_ready_rows, "observation_date", start=start, end=end),
        "model_ready_daily_features": _dates_outside_window(model_ready_daily_features, "date", start=start, end=end),
    }
    out_of_bounds = {name: dates for name, dates in out_of_bounds.items() if dates}
    add_check(
        "date_range_bounded",
        "fail" if out_of_bounds else "pass",
        "label inputs stay inside the requested bounded window"
        if not out_of_bounds
        else "label inputs include dates outside the requested bounded window",
        out_of_bounds_dates=out_of_bounds,
    )

    blocked = _blocked_input_columns(label_ready_rows, model_ready_daily_features)
    add_check(
        "labels_not_joined_back_as_features",
        "fail" if blocked else "pass",
        "no realized/future label columns are present in feature inputs"
        if not blocked
        else "realized/future label columns must not be present in feature inputs",
        columns=blocked,
    )

    candidates, excluded = _build_label_candidates(
        label_ready_rows=label_ready_rows,
        daily_features=model_ready_daily_features,
        horizons=parsed_horizons,
    )

    label_outside = _dates_outside_window(candidates, "label_date", start=start, end=end)
    add_check(
        "future_horizon_exists_before_label",
        "fail" if label_outside else "pass",
        "every emitted label has a future close inside the bounded daily slice"
        if not label_outside
        else "emitted labels include label dates outside the bounded window",
        label_dates_outside_window=label_outside[:5],
    )

    candidate_missing = _missing_columns(candidates, LABEL_CANDIDATE_COLUMNS)
    add_check(
        "label_candidate_schema",
        "fail" if candidate_missing else "pass",
        "label candidates have required schema" if not candidate_missing else "label candidates are missing fields",
        missing_columns=candidate_missing,
    )
    add_check(
        "tail_rows_excluded",
        "pass",
        "rows without future horizon data are excluded instead of labeled",
        excluded_count=int(len(excluded)),
        excluded_by_reason=_reason_counts(excluded),
    )

    status = "fail" if any(check["status"] == "fail" for check in checks) else "pass"
    report = {
        "name": "historical_label_contract",
        "status": status,
        "symbol": normalized_symbol,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "read_only": True,
        "config": {"horizons": [spec for spec, _days in parsed_horizons]},
        "rows": {
            "label_ready_rows": int(len(label_ready_rows)),
            "label_candidates": int(len(candidates)),
            "excluded_rows": int(len(excluded)),
        },
        "mature_label_count": int(len(candidates)),
        "immature_or_excluded_count": int(len(excluded)),
        "excluded_by_reason": _reason_counts(excluded),
        "coverage": _coverage_by_horizon(candidates, excluded, parsed_horizons),
        "missing_values": {
            "label_candidates": _missing_value_counts(candidates),
            "excluded_rows": _missing_value_counts(excluded),
        },
        "warnings": warnings,
        "checks": checks,
        "samples": {
            "label_candidates": _sample_records(candidates),
            "excluded_rows": _sample_records(excluded),
        },
        "source_feature_status": source_report.get("status") if source_report else None,
    }
    return HistoricalLabelContract(label_candidates=candidates, excluded_rows=excluded, report=report)


def write_label_contract_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "historical_label_smoke"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    stem = f"{report['symbol'].lower()}_{report['start_date']}_{report['end_date']}".replace("/", "-")
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def parse_horizons(values: list[str]) -> list[tuple[str, int]]:
    parsed: list[tuple[str, int]] = []
    for value in values:
        raw = str(value).strip().lower()
        match = re.fullmatch(r"([1-9]\d*)d", raw)
        if not match:
            raise ValueError(f"unsupported horizon {value!r}; expected values like 1d or 5d")
        parsed.append((raw, int(match.group(1))))
    if not parsed:
        raise ValueError("at least one horizon is required")
    return parsed


def _build_label_candidates(
    *,
    label_ready_rows: pd.DataFrame,
    daily_features: pd.DataFrame,
    horizons: list[tuple[str, int]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidates: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    daily = daily_features.copy()
    if daily.empty or "date" not in daily.columns or "close" not in daily.columns:
        return (
            pd.DataFrame(columns=LABEL_CANDIDATE_COLUMNS),
            _exclude_all(label_ready_rows, horizons=horizons, reason="missing_daily_future_series"),
        )

    daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
    daily["close"] = pd.to_numeric(daily["close"], errors="coerce")
    daily = daily.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates("date", keep="first")
    trading_dates = daily["date"].dt.date.tolist()
    close_by_date = dict(zip(trading_dates, daily["close"].tolist(), strict=False))
    index_by_date = {value: index for index, value in enumerate(trading_dates)}

    ready = label_ready_rows.copy()
    if ready.empty:
        return pd.DataFrame(columns=LABEL_CANDIDATE_COLUMNS), pd.DataFrame(columns=_excluded_columns())
    ready["observation_date"] = pd.to_datetime(ready["observation_date"], errors="coerce").dt.date

    for row in ready.to_dict(orient="records"):
        observation_date = row.get("observation_date")
        if not isinstance(observation_date, date) or observation_date not in index_by_date:
            for horizon, _days in horizons:
                excluded.append(_excluded_row(row, horizon=horizon, reason="missing_observation_daily_close"))
            continue
        observation_close = close_by_date.get(observation_date)
        for horizon, trading_days in horizons:
            future_index = index_by_date[observation_date] + trading_days
            if future_index >= len(trading_dates):
                excluded.append(_excluded_row(row, horizon=horizon, reason="immature_missing_future_close"))
                continue
            label_date = trading_dates[future_index]
            future_close = close_by_date.get(label_date)
            if pd.isna(observation_close) or pd.isna(future_close):
                excluded.append(_excluded_row(row, horizon=horizon, reason="missing_close_value"))
                continue
            candidates.append(
                {
                    "observation_date": observation_date.isoformat(),
                    "label_date": label_date.isoformat(),
                    "horizon": horizon,
                    "underlying_symbol": row.get("underlying_symbol"),
                    "expiration": row.get("expiration"),
                    "strike": row.get("strike"),
                    "option_type": row.get("option_type"),
                    "days_to_expiration": row.get("days_to_expiration"),
                    "underlying_close": observation_close,
                    "future_underlying_close": future_close,
                    "forward_return": (future_close / observation_close) - 1,
                    "label_status": "mature",
                }
            )

    return (
        pd.DataFrame(candidates, columns=LABEL_CANDIDATE_COLUMNS),
        pd.DataFrame(excluded, columns=_excluded_columns()),
    )


def _exclude_all(label_ready_rows: pd.DataFrame, *, horizons: list[tuple[str, int]], reason: str) -> pd.DataFrame:
    excluded: list[dict[str, Any]] = []
    for row in label_ready_rows.to_dict(orient="records"):
        for horizon, _days in horizons:
            excluded.append(_excluded_row(row, horizon=horizon, reason=reason))
    return pd.DataFrame(excluded, columns=_excluded_columns())


def _excluded_row(row: dict[str, Any], *, horizon: str, reason: str) -> dict[str, Any]:
    observation_date = row.get("observation_date")
    if isinstance(observation_date, date):
        observation_date = observation_date.isoformat()
    return {
        "observation_date": observation_date,
        "horizon": horizon,
        "underlying_symbol": row.get("underlying_symbol"),
        "expiration": row.get("expiration"),
        "strike": row.get("strike"),
        "option_type": row.get("option_type"),
        "reason": reason,
    }


def _excluded_columns() -> list[str]:
    return ["observation_date", "horizon", "underlying_symbol", "expiration", "strike", "option_type", "reason"]


def _missing_columns(frame: pd.DataFrame, required: list[str]) -> list[str]:
    present = {str(column) for column in frame.columns}
    return [column for column in required if column not in present]


def _dates_outside_window(frame: pd.DataFrame, column: str, *, start: date, end: date) -> list[str]:
    if frame.empty or column not in frame.columns:
        return []
    dates = pd.to_datetime(frame[column], errors="coerce").dropna().dt.date
    return sorted(value.isoformat() for value in dates if value < start or value > end)


def _blocked_input_columns(*frames: pd.DataFrame) -> list[str]:
    allowed = set(LABEL_READY_COLUMNS + MODEL_READY_DAILY_COLUMNS)
    blocked = re.compile(r"(future|forward|fwd|target|outcome|realized|label_date|forward_return)", re.IGNORECASE)
    found: set[str] = set()
    for frame in frames:
        for column in frame.columns:
            name = str(column)
            if name in allowed:
                continue
            if blocked.search(name):
                found.add(name)
    return sorted(found)


def _reason_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty or "reason" not in frame.columns:
        return {}
    return {str(reason): int(count) for reason, count in frame["reason"].value_counts().items()}


def _coverage_by_horizon(
    candidates: pd.DataFrame,
    excluded: pd.DataFrame,
    horizons: list[tuple[str, int]],
) -> dict[str, dict[str, Any]]:
    coverage: dict[str, dict[str, Any]] = {}
    for horizon, _days in horizons:
        mature = int((candidates["horizon"] == horizon).sum()) if "horizon" in candidates.columns else 0
        immature = int((excluded["horizon"] == horizon).sum()) if "horizon" in excluded.columns else 0
        total = mature + immature
        coverage[horizon] = {
            "mature": mature,
            "excluded": immature,
            "coverage_ratio": mature / total if total else None,
        }
    return coverage


def _missing_value_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty:
        return {}
    return {
        str(column): int(count)
        for column, count in frame.isna().sum().items()
        if int(count) > 0
    }


def _sample_records(frame: pd.DataFrame, *, sample_size: int = 3) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return json.loads(frame.head(sample_size).to_json(orient="records", date_format="iso"))
