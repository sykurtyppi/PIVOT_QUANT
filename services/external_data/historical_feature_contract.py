"""Feature/label readiness contract for tiny historical T9 slices.

This module adapts normalized daily OHLCV and option-feature rows into
schema-ready feature frames. It does not train, score, label, or backtest.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.t9_inventory import resolve_t9_root
from services.external_data.t9_parquet_adapter import (
    DAILY_COLUMNS,
    OPTION_FEATURE_COLUMNS,
    load_daily_ohlcv,
    load_option_features,
    resolve_daily_source,
    _normalize_symbol,
    _parse_date,
    _select_files_for_window,
)


MODEL_READY_DAILY_COLUMNS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "return_1d",
    "intraday_range_pct",
    "close_to_open_pct",
]
OPTION_CONTEXT_COLUMNS = [
    "date",
    "underlying_symbol",
    "expiration",
    "strike",
    "option_type",
    "bid",
    "ask",
    "mid",
    "volume",
    "open_interest",
    "implied_volatility",
    "underlying_close",
    "days_to_expiration",
    "moneyness",
    "spread",
    "relative_spread",
]
LABEL_READY_COLUMNS = [
    "observation_date",
    "underlying_symbol",
    "expiration",
    "strike",
    "option_type",
    "days_to_expiration",
    "label_status",
]


@dataclass(frozen=True)
class HistoricalFeatureContract:
    model_ready_daily_features: pd.DataFrame
    option_context_features: pd.DataFrame
    label_ready_rows: pd.DataFrame
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def build_historical_feature_contract_from_t9(
    *,
    symbol: str = "SPY",
    start_date: str,
    end_date: str,
    root: str | Path | None = None,
    max_files: int = 20,
    daily_source: str | None = None,
) -> HistoricalFeatureContract:
    normalized_symbol = _normalize_symbol(symbol)
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    max_files = max(1, int(max_files))
    source_mode = resolve_daily_source(daily_source)
    t9_root = resolve_t9_root(root)

    if not t9_root.exists():
        empty = _empty_contract_report(
            symbol=normalized_symbol,
            start=start,
            end=end,
            root=t9_root,
            daily_source=source_mode,
            warning=f"T9 root does not exist: {t9_root}. Set PIVOTQUANT_T9_ROOT to the mounted drive path.",
        )
        return HistoricalFeatureContract(
            model_ready_daily_features=pd.DataFrame(columns=MODEL_READY_DAILY_COLUMNS),
            option_context_features=pd.DataFrame(columns=OPTION_CONTEXT_COLUMNS),
            label_ready_rows=pd.DataFrame(columns=LABEL_READY_COLUMNS),
            report=empty,
        )

    daily_files = _select_files_for_window(
        t9_root
        / "market_data"
        / "normalized"
        / "underlyings"
        / "daily_ohlcv"
        / f"underlying_symbol={normalized_symbol}",
        start=start,
        end=end,
        max_files=max_files,
    )
    option_feature_files = _select_files_for_window(
        t9_root
        / "market_data"
        / "research"
        / "options_features_eod"
        / f"underlying_symbol={normalized_symbol}",
        start=start,
        end=end,
        max_files=max_files,
    )

    daily = load_daily_ohlcv(
        daily_files,
        start=start,
        end=end,
        symbol=normalized_symbol,
        daily_source=source_mode,
    )
    options = load_option_features(
        option_feature_files,
        start=start,
        end=end,
        symbol=normalized_symbol,
    )
    return build_historical_feature_contract(
        daily.rows,
        options.rows,
        symbol=normalized_symbol,
        start=start,
        end=end,
        root=t9_root,
        daily_source=source_mode,
        source_warnings=[*daily.warnings, *options.warnings],
        source_metadata={
            "daily_files": [str(path) for path in daily.files[:5]],
            "option_feature_files": [str(path) for path in options.files[:5]],
            "daily_metadata": daily.metadata,
        },
    )


def build_historical_feature_contract(
    daily_rows: pd.DataFrame,
    option_rows: pd.DataFrame,
    *,
    symbol: str,
    start: date,
    end: date,
    root: Path | None = None,
    daily_source: str | None = None,
    source_warnings: list[str] | None = None,
    source_metadata: dict[str, Any] | None = None,
) -> HistoricalFeatureContract:
    checks: list[dict[str, Any]] = []
    warnings = list(source_warnings or [])
    source_mode = resolve_daily_source(daily_source)
    normalized_symbol = _normalize_symbol(symbol)

    def add_check(name: str, status: str, detail: str, **extra: Any) -> None:
        payload = {"name": name, "status": status, "detail": detail}
        payload.update(extra)
        checks.append(payload)

    daily_missing = _missing_columns(daily_rows, DAILY_COLUMNS)
    option_missing = _missing_columns(option_rows, OPTION_FEATURE_COLUMNS)
    add_check(
        "daily_input_schema",
        "fail" if daily_missing else "pass",
        "canonical daily OHLCV input has required fields" if not daily_missing else "canonical daily OHLCV input is missing fields",
        missing_columns=daily_missing,
    )
    add_check(
        "option_input_schema",
        "fail" if option_missing else "pass",
        "normalized option input has required fields" if not option_missing else "normalized option input is missing fields",
        missing_columns=option_missing,
    )

    daily_outside = _dates_outside_window(daily_rows, start=start, end=end)
    option_outside = _dates_outside_window(option_rows, start=start, end=end)
    add_check(
        "date_range_bounded",
        "fail" if daily_outside or option_outside else "pass",
        "daily and option rows stay inside the requested window"
        if not daily_outside and not option_outside
        else "input rows include dates outside the requested window",
        daily_outside_dates=daily_outside[:5],
        option_outside_dates=option_outside[:5],
    )

    duplicate_dates = _duplicate_dates(daily_rows)
    if duplicate_dates:
        warnings.append(f"duplicate canonical daily dates detected and collapsed: {', '.join(duplicate_dates[:5])}")
    add_check(
        "duplicate_daily_dates_handled",
        "pass",
        "duplicate canonical daily dates are collapsed deterministically before feature construction",
        duplicate_dates_sample=duplicate_dates[:5],
    )

    blocked = _blocked_feature_columns(daily_rows, option_rows)
    add_check(
        "no_future_or_label_like_inputs",
        "fail" if blocked else "pass",
        "no future/label-like input columns are present"
        if not blocked
        else "future/label-like columns are not allowed in feature contract inputs",
        columns=blocked,
    )

    daily_features = _build_model_ready_daily_features(daily_rows)
    option_context = _build_option_context_features(option_rows, daily_features)
    label_ready = _build_label_ready_rows(option_context)

    unaligned = _option_dates_without_daily(daily_features, option_rows)
    add_check(
        "daily_option_date_alignment",
        "fail" if unaligned else "pass",
        "each option-feature date has a model-ready daily row"
        if not unaligned
        else "option-feature dates are missing model-ready daily rows",
        unaligned_dates_sample=unaligned[:5],
    )

    generated_missing = {
        "model_ready_daily_features": _missing_columns(daily_features, MODEL_READY_DAILY_COLUMNS),
        "option_context_features": _missing_columns(option_context, OPTION_CONTEXT_COLUMNS),
        "label_ready_rows": _missing_columns(label_ready, LABEL_READY_COLUMNS),
    }
    generated_missing_flat = {
        name: missing for name, missing in generated_missing.items() if missing
    }
    add_check(
        "generated_schema",
        "fail" if generated_missing_flat else "pass",
        "generated feature/label-ready frames have required schemas"
        if not generated_missing_flat
        else "generated feature/label-ready frames are missing fields",
        missing_columns=generated_missing_flat,
    )

    add_check(
        "no_realized_labels_created",
        "pass",
        "this smoke contract creates label-ready observations only; realized labels are intentionally out of scope",
    )

    missing_values = {
        "model_ready_daily_features": _missing_value_counts(daily_features),
        "option_context_features": _missing_value_counts(option_context),
        "label_ready_rows": _missing_value_counts(label_ready),
    }
    status = "fail" if any(check["status"] == "fail" for check in checks) else "pass"
    report = {
        "name": "historical_feature_contract",
        "status": status,
        "symbol": normalized_symbol,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "t9_root": str(root) if root is not None else None,
        "read_only": True,
        "config": {"daily_source": source_mode},
        "rows": {
            "model_ready_daily_features": int(len(daily_features)),
            "option_context_features": int(len(option_context)),
            "label_ready_rows": int(len(label_ready)),
        },
        "date_ranges": {
            "model_ready_daily_features": _frame_date_range(daily_features, "date"),
            "option_context_features": _frame_date_range(option_context, "date"),
            "label_ready_rows": _frame_date_range(label_ready, "observation_date"),
        },
        "missing_values": missing_values,
        "warnings": warnings,
        "source_metadata": dict(source_metadata or {}),
        "checks": checks,
        "samples": {
            "model_ready_daily_features": _sample_records(daily_features),
            "option_context_features": _sample_records(option_context),
            "label_ready_rows": _sample_records(label_ready),
        },
    }
    return HistoricalFeatureContract(
        model_ready_daily_features=daily_features,
        option_context_features=option_context,
        label_ready_rows=label_ready,
        report=report,
    )


def write_feature_contract_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "historical_feature_smoke"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    stem = f"{report['symbol'].lower()}_{report['start_date']}_{report['end_date']}".replace("/", "-")
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _empty_contract_report(
    *,
    symbol: str,
    start: date,
    end: date,
    root: Path,
    daily_source: str,
    warning: str,
) -> dict[str, Any]:
    return {
        "name": "historical_feature_contract",
        "status": "fail",
        "symbol": symbol,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "t9_root": str(root),
        "read_only": True,
        "config": {"daily_source": daily_source},
        "rows": {
            "model_ready_daily_features": 0,
            "option_context_features": 0,
            "label_ready_rows": 0,
        },
        "date_ranges": {},
        "missing_values": {},
        "warnings": [warning],
        "source_metadata": {},
        "checks": [{"name": "t9_root_exists", "status": "fail", "detail": warning}],
        "samples": {},
    }


def _build_model_ready_daily_features(daily_rows: pd.DataFrame) -> pd.DataFrame:
    if daily_rows.empty:
        return pd.DataFrame(columns=MODEL_READY_DAILY_COLUMNS)
    frame = daily_rows.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"])
    frame = frame.sort_values(["date", "source"]).drop_duplicates(subset=["date"], keep="first")
    for column in ["open", "high", "low", "close", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["return_1d"] = frame["close"].pct_change()
    frame["intraday_range_pct"] = (frame["high"] - frame["low"]) / frame["close"]
    frame["close_to_open_pct"] = (frame["close"] / frame["open"]) - 1
    frame["date"] = frame["date"].dt.date.astype("string")
    return frame[MODEL_READY_DAILY_COLUMNS].reset_index(drop=True)


def _build_option_context_features(option_rows: pd.DataFrame, daily_features: pd.DataFrame) -> pd.DataFrame:
    if option_rows.empty:
        return pd.DataFrame(columns=OPTION_CONTEXT_COLUMNS)
    options = option_rows.copy()
    options["date"] = pd.to_datetime(options["date"], errors="coerce")
    options["expiration"] = pd.to_datetime(options["expiration"], errors="coerce")
    daily = daily_features[["date", "close"]].copy() if "date" in daily_features.columns else pd.DataFrame()
    if not daily.empty:
        daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
        daily = daily.rename(columns={"close": "underlying_close"})
    merged = options.merge(daily, on="date", how="inner")
    for column in ["strike", "bid", "ask", "mid", "volume", "open_interest", "implied_volatility", "underlying_close"]:
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
    merged["days_to_expiration"] = (merged["expiration"] - merged["date"]).dt.days
    merged["moneyness"] = (merged["strike"] / merged["underlying_close"]) - 1
    merged["spread"] = merged["ask"] - merged["bid"]
    merged["relative_spread"] = merged["spread"] / merged["mid"].where(merged["mid"] != 0)
    merged["date"] = merged["date"].dt.date.astype("string")
    merged["expiration"] = merged["expiration"].dt.date.astype("string")
    return merged[OPTION_CONTEXT_COLUMNS].sort_values(["date", "expiration", "strike", "option_type"]).reset_index(drop=True)


def _build_label_ready_rows(option_context: pd.DataFrame) -> pd.DataFrame:
    if option_context.empty:
        return pd.DataFrame(columns=LABEL_READY_COLUMNS)
    labels = pd.DataFrame()
    labels["observation_date"] = option_context["date"]
    labels["underlying_symbol"] = option_context["underlying_symbol"]
    labels["expiration"] = option_context["expiration"]
    labels["strike"] = option_context["strike"]
    labels["option_type"] = option_context["option_type"]
    labels["days_to_expiration"] = option_context["days_to_expiration"]
    labels["label_status"] = "ready_for_future_outcome_generation"
    return labels[LABEL_READY_COLUMNS].reset_index(drop=True)


def _missing_columns(frame: pd.DataFrame, required: list[str]) -> list[str]:
    present = {str(column) for column in frame.columns}
    return [column for column in required if column not in present]


def _dates_outside_window(frame: pd.DataFrame, *, start: date, end: date) -> list[str]:
    dates = _valid_date_values(frame, "date")
    return sorted(value.isoformat() for value in dates if value < start or value > end)


def _option_dates_without_daily(daily_rows: pd.DataFrame, option_rows: pd.DataFrame) -> list[str]:
    daily_dates = _valid_date_values(daily_rows, "date")
    option_dates = _valid_date_values(option_rows, "date")
    return sorted(value.isoformat() for value in option_dates if value not in daily_dates)


def _valid_date_values(frame: pd.DataFrame, column: str) -> set[date]:
    if frame.empty or column not in frame.columns:
        return set()
    values = pd.to_datetime(frame[column], errors="coerce").dropna()
    return {value.date() for value in values}


def _duplicate_dates(frame: pd.DataFrame) -> list[str]:
    if frame.empty or "date" not in frame.columns:
        return []
    dates = pd.to_datetime(frame["date"], errors="coerce").dt.date
    counts = dates.value_counts()
    return sorted(value.isoformat() for value, count in counts.items() if count > 1)


def _blocked_feature_columns(*frames: pd.DataFrame) -> list[str]:
    blocked = re.compile(r"(future|forward|fwd|next_|label|target|outcome|realized)", re.IGNORECASE)
    allowed = set(DAILY_COLUMNS + OPTION_FEATURE_COLUMNS)
    found: set[str] = set()
    for frame in frames:
        for column in frame.columns:
            name = str(column)
            if name in allowed:
                continue
            if blocked.search(name):
                found.add(name)
    return sorted(found)


def _missing_value_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty:
        return {}
    return {
        str(column): int(count)
        for column, count in frame.isna().sum().items()
        if int(count) > 0
    }


def _frame_date_range(frame: pd.DataFrame, column: str) -> dict[str, str | None]:
    if frame.empty or column not in frame.columns:
        return {"min": None, "max": None}
    dates = pd.to_datetime(frame[column], errors="coerce").dropna()
    if dates.empty:
        return {"min": None, "max": None}
    return {"min": dates.min().date().isoformat(), "max": dates.max().date().isoformat()}


def _sample_records(frame: pd.DataFrame, *, sample_size: int = 3) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return json.loads(frame.head(sample_size).to_json(orient="records", date_format="iso"))
