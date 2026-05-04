"""Effective sample diagnostics for option-row frames.

Option-row join results have multiple contracts per observation_date. The
forward_return is identical across all contracts for the same underlying date,
so row_count overstates the number of independent observations. These helpers
quantify the inflation and provide date-weighted return statistics.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def effective_sample_diagnostics(
    frame: pd.DataFrame,
    *,
    date_col: str,
) -> dict[str, Any]:
    """Row-count and unique-date diagnostics.

    effective_sample_warning is True when average_rows_per_date > 1,
    meaning multiple option contracts share a single underlying return.
    """
    if frame.empty or date_col not in frame.columns:
        return {
            "row_count": 0,
            "unique_entry_dates": 0,
            "average_rows_per_date": None,
            "max_rows_per_date": None,
            "effective_sample_warning": False,
        }
    row_count = int(len(frame))
    dates = pd.to_datetime(frame[date_col], errors="coerce").dt.date
    valid_mask = dates.notna()
    unique_dates = int(dates[valid_mask].nunique())
    if unique_dates == 0:
        return {
            "row_count": row_count,
            "unique_entry_dates": 0,
            "average_rows_per_date": None,
            "max_rows_per_date": None,
            "effective_sample_warning": False,
        }
    per_date_counts = dates[valid_mask].value_counts()
    max_per_date = int(per_date_counts.max())
    avg = row_count / unique_dates
    return {
        "row_count": row_count,
        "unique_entry_dates": unique_dates,
        "average_rows_per_date": round(avg, 2),
        "max_rows_per_date": max_per_date,
        "effective_sample_warning": avg > 1.0,
    }


def date_weighted_metrics(
    frame: pd.DataFrame,
    *,
    date_col: str,
    return_col: str,
) -> dict[str, Any]:
    """Date-level (deduplicated) return statistics.

    Computes one return per date (mean across contracts for that date), then
    aggregates across dates. Each trading day gets equal weight regardless of
    how many option contracts appear.
    """
    _empty: dict[str, Any] = {
        "date_weighted_win_rate": None,
        "date_weighted_mean_return": None,
        "date_weighted_median_return": None,
        "date_weighted_count": 0,
        "date_weighted_metrics_available": False,
    }
    if frame.empty or date_col not in frame.columns or return_col not in frame.columns:
        return _empty
    tmp = frame[[date_col, return_col]].copy()
    tmp["_return"] = pd.to_numeric(tmp[return_col], errors="coerce")
    tmp["_date"] = pd.to_datetime(tmp[date_col], errors="coerce").dt.date.astype("string")
    tmp = tmp.dropna(subset=["_return", "_date"])
    if tmp.empty:
        return _empty
    per_date = tmp.groupby("_date")["_return"].mean()
    count = int(len(per_date))
    return {
        "date_weighted_win_rate": float((per_date > 0).mean()),
        "date_weighted_mean_return": float(per_date.mean()),
        "date_weighted_median_return": float(per_date.median()),
        "date_weighted_count": count,
        "date_weighted_metrics_available": True,
    }
