"""Paper evaluation harness for the high_vol_trend_early_candidate signal.

Applies fixed train-derived thresholds to test-period rows and records paper
entries with outcome statistics. No live trading, execution assumptions,
slippage, commission, or edge claim.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.ml_candidate_signal import (
    apply_candidate_signal,
    derive_candidate_signal_thresholds,
)
from services.external_data.ml_regime_benchmark import _safe_float

_TREND_FEATURE = "price_momentum_20d"
_LOW_SAMPLE_THRESHOLD = 10


@dataclass(frozen=True)
class PaperEvalConfig:
    live_trading_enabled: bool = False
    execution_assumptions_included: bool = False
    slippage_mode: str = "none"
    commission_mode: str = "none"
    edge_claim: bool = False
    min_month_entries_for_warning: int = 5


def generate_paper_entries(
    frame: pd.DataFrame,
    *,
    vol_split_value: float,
    maturity_threshold: float,
) -> list[dict[str, Any]]:
    """Return one entry dict per row satisfying all three signal conditions.

    frame must have distance_from_20d_mean already computed.
    Thresholds must be derived from train only before calling here.
    """
    signal_frame = apply_candidate_signal(
        frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
    )
    entries: list[dict[str, Any]] = []
    for _, row in signal_frame.iterrows():
        entries.append(
            {
                "entry_date": str(row.get("entry_date", "")),
                "horizon": "5d",
                "forward_return_5d": _safe_float(row.get("forward_return_5d")),
                "signal_metadata": {
                    "realized_vol_60d": _safe_float(row.get("realized_vol_60d")),
                    "price_momentum_20d": _safe_float(row.get("price_momentum_20d")),
                    "distance_from_20d_mean": _safe_float(row.get("distance_from_20d_mean")),
                    "vol_split_value": vol_split_value,
                    "maturity_threshold": maturity_threshold,
                },
            }
        )
    return entries


def _quarterly_breakdown(
    frame: pd.DataFrame,
    *,
    vol_split_value: float,
    maturity_threshold: float,
) -> list[dict[str, Any]]:
    """Per-quarter signal entry count and stats; zero-entry quarters included."""
    if frame.empty or "entry_date" not in frame.columns:
        return []
    dates = pd.to_datetime(frame["entry_date"], errors="coerce")
    all_quarters = sorted(dates.dt.to_period("Q").dropna().unique())
    if not all_quarters:
        return []

    signal_frame = apply_candidate_signal(
        frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
    )
    signal_quarters = pd.to_datetime(
        signal_frame["entry_date"] if "entry_date" in signal_frame.columns else pd.Series(dtype="str"),
        errors="coerce",
    ).dt.to_period("Q")

    result: list[dict[str, Any]] = []
    for q in all_quarters:
        q_signal = signal_frame[signal_quarters == q]
        forward_5d = pd.to_numeric(
            q_signal.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce"
        ).dropna()
        entries = int(len(forward_5d))
        result.append(
            {
                "quarter": str(q),
                "entries": entries,
                "win_rate": _safe_float((forward_5d > 0).mean()) if entries > 0 else None,
                "mean_return": _safe_float(forward_5d.mean()) if entries > 0 else None,
            }
        )
    return result


def _monthly_breakdown(
    frame: pd.DataFrame,
    *,
    vol_split_value: float,
    maturity_threshold: float,
    min_month_entries_for_warning: int = 5,
) -> list[dict[str, Any]]:
    """Per-month signal entry stats; zero-entry months included from full frame.

    Each row carries low_sample=True when 0 < entries < min_month_entries_for_warning,
    so downstream callers can distinguish absent months from under-sampled ones.
    """
    if frame.empty or "entry_date" not in frame.columns:
        return []
    dates = pd.to_datetime(frame["entry_date"], errors="coerce")
    all_months = sorted(dates.dt.to_period("M").dropna().unique())
    if not all_months:
        return []

    signal_frame = apply_candidate_signal(
        frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
    )
    if signal_frame.empty or "entry_date" not in signal_frame.columns:
        signal_months = pd.Series(dtype="object")
    else:
        signal_months = pd.to_datetime(signal_frame["entry_date"], errors="coerce").dt.to_period("M")

    result: list[dict[str, Any]] = []
    for m in all_months:
        m_signal = signal_frame[signal_months == m] if not signal_frame.empty else signal_frame
        forward_5d = pd.to_numeric(
            m_signal.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce"
        ).dropna()
        entries = int(len(forward_5d))
        pos_returns = forward_5d[forward_5d > 0]
        result.append(
            {
                "month": str(m),
                "entries": entries,
                "low_sample": bool(0 < entries < min_month_entries_for_warning),
                "win_rate": _safe_float((forward_5d > 0).mean()) if entries > 0 else None,
                "mean_return": _safe_float(forward_5d.mean()) if entries > 0 else None,
                "median_return": _safe_float(forward_5d.median()) if entries > 0 else None,
                "worst_return": _safe_float(forward_5d.min()) if entries > 0 else None,
                "best_return": _safe_float(forward_5d.max()) if entries > 0 else None,
                "positive_return_sum": _safe_float(pos_returns.sum()) if not pos_returns.empty else 0.0,
            }
        )
    return result


def _stability_flags(
    monthly_breakdown: list[dict[str, Any]],
    *,
    min_month_entries_for_warning: int = 5,
) -> dict[str, Any]:
    """Derive sample-aware stability flags.

    Flags:
      low_sample_month_warning       — any month has 0 < entries < min_month_entries_for_warning
      negative_mature_month_warning  — any mature month (entries >= threshold) has
                                       win_rate < 0.5 AND mean_return < 0
      concentration_warning          — any month contributes >50% of total entries
                                       OR >50% of total positive return
      stability_flag                 — True only when negative_mature_month_warning is False,
                                       concentration_warning is False, and at least one mature
                                       month exists (cannot evaluate stability without mature data)
    """
    total_entries = sum(m["entries"] for m in monthly_breakdown)

    low_sample_month_warning = bool(
        any(0 < m["entries"] < min_month_entries_for_warning for m in monthly_breakdown)
    )

    negative_mature_month_warning = bool(
        any(
            m["entries"] >= min_month_entries_for_warning
            and m.get("win_rate") is not None
            and float(m["win_rate"]) < 0.5
            and m.get("mean_return") is not None
            and float(m["mean_return"]) < 0
            for m in monthly_breakdown
        )
    )

    entry_concentration = bool(
        total_entries > 0
        and any(m["entries"] / total_entries > 0.50 for m in monthly_breakdown)
    )

    total_positive_return = sum(
        float(m.get("positive_return_sum") or 0.0) for m in monthly_breakdown
    )
    return_concentration = bool(
        total_positive_return > 0
        and any(
            float(m.get("positive_return_sum") or 0.0) / total_positive_return > 0.50
            for m in monthly_breakdown
        )
    )

    concentration_warning = bool(entry_concentration or return_concentration)
    has_mature_month = any(
        m["entries"] >= min_month_entries_for_warning for m in monthly_breakdown
    )
    stability_flag = bool(
        not negative_mature_month_warning and not concentration_warning and has_mature_month
    )

    return {
        "stability_flag": stability_flag,
        "negative_mature_month_warning": negative_mature_month_warning,
        "low_sample_month_warning": low_sample_month_warning,
        "concentration_warning": concentration_warning,
    }


def summarize_paper_entries(
    entries: list[dict[str, Any]],
    *,
    quarterly_breakdown: list[dict[str, Any]],
    excluded_late_trend_count: int,
    low_sample_threshold: int = _LOW_SAMPLE_THRESHOLD,
) -> dict[str, Any]:
    returns = [
        e["forward_return_5d"] for e in entries if e.get("forward_return_5d") is not None
    ]
    series = pd.Series(returns, dtype="float64")
    total = len(entries)
    win_rate = _safe_float((series > 0).mean()) if total > 0 else None
    mean_return = _safe_float(series.mean()) if total > 0 else None
    median_return = _safe_float(series.median()) if total > 0 else None
    worst_return = _safe_float(series.min()) if total > 0 else None
    best_return = _safe_float(series.max()) if total > 0 else None
    low_sample_quarters = sum(
        1 for q in quarterly_breakdown if 0 < q["entries"] < low_sample_threshold
    )
    sample_size_warning = bool(total < low_sample_threshold or low_sample_quarters > 0)
    return {
        "total_paper_entries": total,
        "win_rate": win_rate,
        "mean_return": mean_return,
        "median_return": median_return,
        "worst_return": worst_return,
        "best_return": best_return,
        "excluded_late_trend_count": excluded_late_trend_count,
        "quarterly_breakdown": quarterly_breakdown,
        "sample_size_warning": sample_size_warning,
        "low_sample_threshold": low_sample_threshold,
    }


def _baseline_row_count(frame: pd.DataFrame, *, vol_split_value: float) -> int:
    vol = pd.to_numeric(
        frame.get("realized_vol_60d", pd.Series(dtype="float64")), errors="coerce"
    )
    trend = pd.to_numeric(
        frame.get(_TREND_FEATURE, pd.Series(dtype="float64")), errors="coerce"
    )
    return int(((vol >= vol_split_value) & (trend > 0)).sum())


def build_paper_eval_report(
    *,
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
) -> dict[str, Any]:
    """Build the paper evaluation report.

    train_frame and test_frame must have distance_from_20d_mean already
    computed (pass through _add_explanatory_variables before calling here).
    Thresholds are derived from train_frame only.
    """
    config = PaperEvalConfig()
    thresholds = derive_candidate_signal_thresholds(train_frame)
    vol_split_value = thresholds["vol_split_value"]
    maturity_threshold = thresholds["maturity_threshold"]

    flags = {
        "live_trading_enabled": config.live_trading_enabled,
        "execution_assumptions_included": config.execution_assumptions_included,
        "slippage_mode": config.slippage_mode,
        "commission_mode": config.commission_mode,
        "edge_claim": config.edge_claim,
    }
    definitions = {
        "training_performed": False,
        "threshold_optimization_performed": False,
        "filter_changes_performed": False,
        "live_trading_enabled": False,
        "governance_promotion_performed": False,
    }

    if vol_split_value is None or maturity_threshold is None:
        return {
            "status": "missing",
            "purpose": "paper evaluation of candidate signal; no live trading",
            "reason": "vol_split_value or maturity_threshold could not be derived from train data",
            "flags": flags,
            "definitions": definitions,
        }

    # Test period
    test_baseline_rows = _baseline_row_count(test_frame, vol_split_value=vol_split_value)
    test_signal_rows = len(
        apply_candidate_signal(
            test_frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
        )
    )
    test_excluded = test_baseline_rows - test_signal_rows
    test_entries = generate_paper_entries(
        test_frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
    )
    test_quarterly = _quarterly_breakdown(
        test_frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
    )
    test_summary = summarize_paper_entries(
        test_entries,
        quarterly_breakdown=test_quarterly,
        excluded_late_trend_count=test_excluded,
    )
    test_monthly = _monthly_breakdown(
        test_frame,
        vol_split_value=vol_split_value,
        maturity_threshold=maturity_threshold,
        min_month_entries_for_warning=config.min_month_entries_for_warning,
    )
    test_stability = _stability_flags(
        test_monthly, min_month_entries_for_warning=config.min_month_entries_for_warning
    )

    # Train period (reference only — no entries list stored)
    train_baseline_rows = _baseline_row_count(train_frame, vol_split_value=vol_split_value)
    train_signal_rows = len(
        apply_candidate_signal(
            train_frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
        )
    )
    train_excluded = train_baseline_rows - train_signal_rows
    train_entries = generate_paper_entries(
        train_frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
    )
    train_quarterly = _quarterly_breakdown(
        train_frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
    )
    train_summary = summarize_paper_entries(
        train_entries,
        quarterly_breakdown=train_quarterly,
        excluded_late_trend_count=train_excluded,
    )

    return {
        "status": "ok",
        "purpose": "paper evaluation of candidate signal; no live trading",
        "thresholds": {
            "vol_split_value": vol_split_value,
            "maturity_threshold": maturity_threshold,
            "threshold_source": "train period only",
        },
        "flags": flags,
        "train": {
            "summary": train_summary,
        },
        "test": {
            "summary": test_summary,
            "entries": test_entries,
            "stability": {
                "monthly_breakdown": test_monthly,
                "flags": test_stability,
                "flag_criteria": {
                    "negative_mature_month_warning": (
                        f"true if any month with entries >= {config.min_month_entries_for_warning}"
                        " has win_rate < 0.5 AND mean_return < 0"
                    ),
                    "low_sample_month_warning": (
                        f"true if any month has 0 < entries < {config.min_month_entries_for_warning}"
                    ),
                    "concentration_warning": "true if any month contributes >50% of total entries or >50% of total positive return",
                    "stability_flag": (
                        "true only if no negative_mature_month_warning, no concentration_warning,"
                        " and at least one mature month exists"
                    ),
                },
                "min_month_entries_for_warning": config.min_month_entries_for_warning,
                "definitions": {
                    "training_performed": False,
                    "threshold_optimization_performed": False,
                    "filter_changes_performed": False,
                    "live_trading_enabled": False,
                },
            },
        },
        "disclaimer": "no edge claim; paper evaluation only; no execution assumptions",
        "definitions": definitions,
    }


def write_paper_eval_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "ml_diagnostics"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    path = base / "candidate_signal_paper_eval.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path
