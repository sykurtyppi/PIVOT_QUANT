"""Non-leaky daily feature adapter for bounded historical model inputs.

The adapter computes only deterministic OHLCV-derived fields. It does not use
labels, future returns, option outcomes, or unavailable IV/macro proxies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


DETERMINISTIC_DAILY_FEATURES = [
    "price_momentum_5d",
    "price_momentum_20d",
    "volume_ratio_10d",
    "realized_vol_30d",
    "realized_vol_60d",
    "rsi_14",
    "bb_position",
]
FORBIDDEN_ADAPTER_INPUT_COLUMNS = {
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
    "price_momentum_5d": "close_t / close_t-5 - 1; uses current and prior closes only",
    "price_momentum_20d": "close_t / close_t-20 - 1; uses current and prior closes only",
    "volume_ratio_10d": "volume_t / mean(volume_t-10 through volume_t-1); excludes future volume",
    "realized_vol_30d": "std(daily returns through t) over 30 observed returns; excludes future returns",
    "realized_vol_60d": "std(daily returns through t) over 60 observed returns; excludes future returns",
    "rsi_14": "Wilder-style 14-period close delta summary through t; excludes future closes",
    "bb_position": "20-period Bollinger position through t; excludes future closes",
}


@dataclass(frozen=True)
class ModelFeatureAdapterResult:
    rows: pd.DataFrame
    report: dict[str, Any]


def adapt_daily_features_for_model_schema(rows: pd.DataFrame) -> ModelFeatureAdapterResult:
    _reject_forbidden_inputs(rows)
    if rows.empty:
        return ModelFeatureAdapterResult(
            rows=rows.copy(),
            report=_adapter_report(pd.DataFrame(), [], {}),
        )

    frame = rows.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.sort_values("date").reset_index(drop=True)

    if "underlying_price" not in frame.columns and "close" in frame.columns:
        frame["underlying_price"] = frame["close"]

    close = _numeric(frame, "close")
    if close.isna().all() and "underlying_price" in frame.columns:
        close = _numeric(frame, "underlying_price")
    volume = _numeric(frame, "volume")
    returns = close.pct_change()
    added: list[str] = []

    def compute_if_missing(column: str, values: pd.Series) -> None:
        if column in frame.columns and not frame[column].isna().all():
            return
        frame[column] = values
        added.append(column)

    compute_if_missing("price_momentum_5d", close / close.shift(5) - 1)
    compute_if_missing("price_momentum_20d", close / close.shift(20) - 1)
    prior_10_volume = volume.shift(1).rolling(window=10, min_periods=10).mean()
    compute_if_missing("volume_ratio_10d", volume / prior_10_volume)
    compute_if_missing("realized_vol_30d", returns.rolling(window=30, min_periods=30).std() * (252 ** 0.5))
    compute_if_missing("realized_vol_60d", returns.rolling(window=60, min_periods=60).std() * (252 ** 0.5))
    compute_if_missing("rsi_14", _rsi(close, window=14))
    compute_if_missing("bb_position", _bb_position(close, window=20))

    frame["date"] = frame["date"].dt.date.astype("string")
    unavailable = {
        column: int(frame[column].isna().sum())
        for column in added
        if column in frame.columns and int(frame[column].isna().sum()) > 0
    }
    return ModelFeatureAdapterResult(rows=frame, report=_adapter_report(frame, added, unavailable))


def _adapter_report(frame: pd.DataFrame, added: list[str], unavailable: dict[str, int]) -> dict[str, Any]:
    return {
        "computed_feature_list": list(added),
        "computed_non_null_counts": {
            column: int(frame[column].notna().sum())
            for column in added
            if column in frame.columns
        },
        "unavailable_due_to_insufficient_lookback": unavailable,
        "not_fabricated_features": [
            "iv_rank",
            "iv_percentile",
            "iv30_rv30_ratio",
            "vol_term_structure_slope",
            "vix_level",
            "forward_return_21d",
            "forward_volatility_21d",
        ],
        "no_lookahead_notes": {
            column: NO_LOOKAHEAD_NOTES[column]
            for column in added
            if column in NO_LOOKAHEAD_NOTES
        },
    }


def _reject_forbidden_inputs(rows: pd.DataFrame) -> None:
    present = {str(column) for column in rows.columns}
    forbidden = sorted(present.intersection(FORBIDDEN_ADAPTER_INPUT_COLUMNS))
    if forbidden:
        raise ValueError(f"daily feature adapter cannot consume label/outcome columns: {', '.join(forbidden)}")


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([pd.NA] * len(frame), dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _rsi(close: pd.Series, *, window: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()
    avg_loss_safe = avg_loss.mask(avg_loss == 0)
    rs = avg_gain / avg_loss_safe
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    return rsi.where(avg_gain.notna() & avg_loss.notna())


def _bb_position(close: pd.Series, *, window: int) -> pd.Series:
    mean = close.rolling(window=window, min_periods=window).mean()
    std = close.rolling(window=window, min_periods=window).std()
    lower = mean - (2 * std)
    upper = mean + (2 * std)
    width = upper - lower
    return (close - lower) / width.replace(0, pd.NA)
