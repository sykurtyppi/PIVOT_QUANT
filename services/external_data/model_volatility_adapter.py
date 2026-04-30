"""Real-input IV/VIX feature adapter for bounded historical model inputs.

The adapter computes only features that can be derived from same-day option IV
rows, trailing IV history, realized volatility already present on the daily
feature frame, and real VIX daily rows. It does not fabricate unavailable IV,
VIX, macro, label, or outcome fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.t9_inventory import resolve_t9_root
from services.external_data.t9_parquet_adapter import (
    NormalizedSlice,
    load_daily_ohlcv,
    _select_files_for_window,
)


REAL_INPUT_VOLATILITY_FEATURES = [
    "iv30_rv30_ratio",
    "iv_percentile",
    "iv_rank",
    "vol_term_structure_slope",
    "vix_level",
]
FORBIDDEN_VOLATILITY_INPUT_COLUMNS = {
    "forward_return",
    "forward_return_1d",
    "forward_return_5d",
    "forward_return_21d",
    "future_underlying_close",
    "forward_volatility_21d",
    "label",
    "label_date",
    "target",
    "outcome",
    "realized",
}
NO_LOOKAHEAD_NOTES = {
    "iv30_rv30_ratio": "same-date 30D ATM IV proxy divided by realized_vol_30d already available at t",
    "iv_percentile": "current 30D IV percentile within trailing IV window through t only",
    "iv_rank": "current 30D IV rank versus trailing min/max IV window through t only",
    "vol_term_structure_slope": "same-date longer-tenor ATM IV minus shorter-tenor ATM IV",
    "vix_level": "same-date close from real ^VIX daily OHLCV rows only",
}
IV_HISTORY_WINDOW = 20


@dataclass(frozen=True)
class ModelVolatilityAdapterResult:
    rows: pd.DataFrame
    report: dict[str, Any]


def load_vix_daily_level_from_t9(
    *,
    root: str | Path | None,
    start: date,
    end: date,
    max_files: int,
) -> NormalizedSlice:
    """Load real VIX daily rows from T9 without mutating external data."""

    t9_root = resolve_t9_root(root)
    files = _select_files_for_window(
        t9_root / "market_data" / "normalized" / "underlyings" / "daily_ohlcv" / "underlying_symbol=^VIX",
        start=start,
        end=end,
        max_files=max_files,
    )
    return load_daily_ohlcv(
        files,
        start=start,
        end=end,
        symbol="^VIX",
        daily_source="yahoo",
    )


def adapt_real_input_volatility_features_for_model_schema(
    rows: pd.DataFrame,
    *,
    option_context_features: pd.DataFrame,
    vix_daily_features: pd.DataFrame | None = None,
    iv_history_window: int = IV_HISTORY_WINDOW,
) -> ModelVolatilityAdapterResult:
    _reject_forbidden_inputs(rows, option_context_features, vix_daily_features)
    if rows.empty:
        return ModelVolatilityAdapterResult(rows=rows.copy(), report=_empty_report())

    frame = rows.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.sort_values("date").reset_index(drop=True)
    frame["_date_key"] = frame["date"].dt.date.astype("string")

    source_counts: dict[str, int] = {}
    missing_source: dict[str, int] = {}
    insufficient_history: dict[str, int] = {}
    computed: list[str] = []

    iv_proxy = _build_iv_proxy(option_context_features)
    source_counts["option_iv_dates"] = int(len(iv_proxy))
    if not iv_proxy.empty:
        frame = frame.merge(iv_proxy, left_on="_date_key", right_on="date", how="left", suffixes=("", "_iv_proxy"))
        if "date_iv_proxy" in frame.columns:
            frame = frame.drop(columns=["date_iv_proxy"])

        _compute_if_missing(frame, "iv30_rv30_ratio", _iv30_rv30_ratio(frame), computed)
        _compute_if_missing(frame, "iv_percentile", _trailing_iv_percentile(frame["iv30_proxy"], iv_history_window), computed)
        _compute_if_missing(frame, "iv_rank", _trailing_iv_rank(frame["iv30_proxy"], iv_history_window), computed)
        _compute_if_missing(frame, "vol_term_structure_slope", frame["vol_term_structure_slope_proxy"], computed)
    else:
        for column in ["iv30_rv30_ratio", "iv_percentile", "iv_rank", "vol_term_structure_slope"]:
            if column not in frame.columns:
                missing_source[column] = int(len(frame))

    vix = _prepare_vix(vix_daily_features)
    source_counts["vix_dates"] = int(len(vix))
    if not vix.empty:
        frame = frame.merge(vix, left_on="_date_key", right_on="date", how="left", suffixes=("", "_vix"))
        if "date_vix" in frame.columns:
            frame = frame.drop(columns=["date_vix"])
        _compute_if_missing(frame, "vix_level", frame["vix_close"], computed)
    elif "vix_level" not in frame.columns:
        missing_source["vix_level"] = int(len(frame))

    for column in computed:
        null_count = int(frame[column].isna().sum())
        if null_count:
            insufficient_history[column] = null_count

    frame = frame.drop(columns=[col for col in ["_date_key", "iv30_proxy", "vol_term_structure_slope_proxy", "vix_close"] if col in frame.columns])
    frame["date"] = frame["date"].dt.date.astype("string")
    return ModelVolatilityAdapterResult(
        rows=frame,
        report=_adapter_report(frame, computed, insufficient_history, missing_source, source_counts),
    )


def _build_iv_proxy(option_context_features: pd.DataFrame) -> pd.DataFrame:
    if option_context_features.empty:
        return pd.DataFrame(columns=["date", "iv30_proxy", "vol_term_structure_slope_proxy"])
    options = option_context_features.copy()
    options["date"] = pd.to_datetime(options["date"], errors="coerce")
    options["dte"] = _numeric(options, "days_to_expiration")
    options["moneyness"] = _numeric(options, "moneyness")
    options["implied_volatility"] = _numeric(options, "implied_volatility")
    options = options.dropna(subset=["date", "dte", "moneyness", "implied_volatility"])
    options = options[options["implied_volatility"] > 0].copy()
    if options.empty:
        return pd.DataFrame(columns=["date", "iv30_proxy", "vol_term_structure_slope_proxy"])

    rows: list[dict[str, Any]] = []
    for day, group in options.groupby(options["date"].dt.date):
        iv30 = _bucket_iv(group, min_dte=20, max_dte=40)
        short_iv = _bucket_iv(group, min_dte=20, max_dte=40)
        long_iv = _bucket_iv(group, min_dte=50, max_dte=80)
        slope = pd.NA
        if pd.notna(short_iv) and pd.notna(long_iv):
            slope = float(long_iv) - float(short_iv)
        rows.append(
            {
                "date": str(day),
                "iv30_proxy": iv30,
                "vol_term_structure_slope_proxy": slope,
            }
        )
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def _bucket_iv(group: pd.DataFrame, *, min_dte: int, max_dte: int) -> float | Any:
    bucket = group[(group["dte"] >= min_dte) & (group["dte"] <= max_dte)].copy()
    if bucket.empty:
        return pd.NA
    bucket["abs_moneyness"] = bucket["moneyness"].abs()
    near_atm = bucket[bucket["abs_moneyness"] <= 0.03]
    if near_atm.empty:
        near_atm = bucket.sort_values("abs_moneyness").head(10)
    return float(near_atm["implied_volatility"].median())


def _prepare_vix(vix_daily_features: pd.DataFrame | None) -> pd.DataFrame:
    if vix_daily_features is None or vix_daily_features.empty:
        return pd.DataFrame(columns=["date", "vix_close"])
    vix = vix_daily_features.copy()
    if "date" not in vix.columns or "close" not in vix.columns:
        return pd.DataFrame(columns=["date", "vix_close"])
    vix["date"] = pd.to_datetime(vix["date"], errors="coerce").dt.date.astype("string")
    vix["vix_close"] = pd.to_numeric(vix["close"], errors="coerce")
    return vix[["date", "vix_close"]].dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="first")


def _iv30_rv30_ratio(frame: pd.DataFrame) -> pd.Series:
    iv30 = _numeric(frame, "iv30_proxy")
    rv30 = _numeric(frame, "realized_vol_30d")
    return iv30 / rv30.where(rv30 > 0)


def _trailing_iv_rank(iv30: pd.Series, window: int) -> pd.Series:
    iv = pd.to_numeric(iv30, errors="coerce")
    rolling_min = iv.rolling(window=window, min_periods=window).min()
    rolling_max = iv.rolling(window=window, min_periods=window).max()
    denom = rolling_max - rolling_min
    return (iv - rolling_min) / denom.replace(0, pd.NA)


def _trailing_iv_percentile(iv30: pd.Series, window: int) -> pd.Series:
    iv = pd.to_numeric(iv30, errors="coerce")

    def percentile(values: pd.Series) -> float:
        current = values.iloc[-1]
        if pd.isna(current):
            return float("nan")
        return float((values <= current).sum() / len(values) * 100.0)

    return iv.rolling(window=window, min_periods=window).apply(percentile, raw=False)


def _compute_if_missing(frame: pd.DataFrame, column: str, values: pd.Series, computed: list[str]) -> None:
    if column in frame.columns and not frame[column].isna().all():
        return
    frame[column] = values
    computed.append(column)


def _adapter_report(
    frame: pd.DataFrame,
    computed: list[str],
    insufficient_history: dict[str, int],
    missing_source: dict[str, int],
    source_counts: dict[str, int],
) -> dict[str, Any]:
    unavailable = sorted(
        column
        for column in REAL_INPUT_VOLATILITY_FEATURES
        if column not in frame.columns and column in missing_source
    )
    return {
        "computed_feature_list": list(computed),
        "computed_non_null_counts": {
            column: int(frame[column].notna().sum())
            for column in computed
            if column in frame.columns
        },
        "unavailable_due_to_insufficient_history": dict(insufficient_history),
        "unavailable_due_to_missing_source": dict(missing_source),
        "source_counts": dict(source_counts),
        "not_fabricated_features": unavailable,
        "no_lookahead_notes": {
            column: NO_LOOKAHEAD_NOTES[column]
            for column in computed
            if column in NO_LOOKAHEAD_NOTES
        },
    }


def _empty_report() -> dict[str, Any]:
    return {
        "computed_feature_list": [],
        "computed_non_null_counts": {},
        "unavailable_due_to_insufficient_history": {},
        "unavailable_due_to_missing_source": {},
        "source_counts": {"option_iv_dates": 0, "vix_dates": 0},
        "not_fabricated_features": list(REAL_INPUT_VOLATILITY_FEATURES),
        "no_lookahead_notes": {},
    }


def _reject_forbidden_inputs(*frames: pd.DataFrame | None) -> None:
    for frame in frames:
        if frame is None:
            continue
        present = {str(column) for column in frame.columns}
        forbidden = sorted(present.intersection(FORBIDDEN_VOLATILITY_INPUT_COLUMNS))
        if forbidden:
            raise ValueError(f"volatility feature adapter cannot consume label/outcome columns: {', '.join(forbidden)}")


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([pd.NA] * len(frame), dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce")
