"""Walk-forward dry-run harness for bounded historical slices.

Builds chronological train/test windows over observed trading days and
optionally applies deterministic rule-baseline scoring and regime conditioning
per window. Does not train models, tune thresholds, or run a full backtest.
"""

from __future__ import annotations

import json
import math
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
from services.external_data.ml_effective_sample import date_weighted_metrics, effective_sample_diagnostics
from services.external_data.t9_parquet_adapter import _normalize_symbol, _parse_date

_REGIME_BUCKET_LABELS = {
    1: ["low_vol"],
    2: ["low_vol", "high_vol"],
    3: ["low_vol", "mid_vol", "high_vol"],
}


@dataclass(frozen=True)
class WalkForwardRuleConfig:
    """Simple deterministic entry-time filters applied per test window.

    No learned parameters. No label/outcome columns used for selection.
    """

    option_type: str = "both"  # "call"/"C", "put"/"P", or "both"
    min_open_interest: int = 0
    min_volume: int = 0
    moneyness_bucket: str | None = None  # e.g. "atm", "near_itm", "near_otm"

    def as_dict(self) -> dict[str, Any]:
        return {
            "option_type": self.option_type,
            "min_open_interest": self.min_open_interest,
            "min_volume": self.min_volume,
            "moneyness_bucket": self.moneyness_bucket,
        }


@dataclass(frozen=True)
class WalkForwardRegimeConfig:
    """Regime signal computed from information available at each train_end.

    No learned parameters. Buckets are assigned post-hoc as tertile boundaries
    across all windows' train_end vols (descriptive only, not a trading filter).
    """

    signal: str = "realized_vol_20d"  # "realized_vol_20d" or "none"
    n_buckets: int = 3
    lookback_days: int = 20

    def as_dict(self) -> dict[str, Any]:
        return {
            "signal": self.signal,
            "n_buckets": self.n_buckets,
            "lookback_days": self.lookback_days,
        }


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
    rule_config: WalkForwardRuleConfig | None = None,
    regime_config: WalkForwardRegimeConfig | None = None,
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
        option_context_features=feature_contract.option_context_features,
        symbol=normalized_symbol,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        horizons=horizons or DEFAULT_HORIZONS,
        train_window=train_window,
        test_window=test_window,
        step=step,
        rule_config=rule_config,
        regime_config=regime_config,
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
    option_context_features: pd.DataFrame | None = None,
    rule_config: WalkForwardRuleConfig | None = None,
    regime_config: WalkForwardRegimeConfig | None = None,
) -> HistoricalWalkForwardReport:
    normalized_symbol = _normalize_symbol(symbol)
    train_window = _positive_int(train_window, "train_window")
    test_window = _positive_int(test_window, "test_window")
    step = _positive_int(step, "step")
    trading_days = _trading_days(model_ready_daily_features)

    option_joined: pd.DataFrame | None = None
    rule_baseline_applied = False
    if rule_config is not None and option_context_features is not None and not option_context_features.empty:
        option_joined = _join_options_to_labels(label_candidates, option_context_features)
        rule_baseline_applied = True

    windows = _build_windows(
        trading_days=trading_days,
        label_candidates=label_candidates,
        train_window=train_window,
        test_window=test_window,
        step=step,
        option_joined=option_joined,
        rule_config=rule_config,
    )

    regime_applied = False
    if regime_config is not None and regime_config.signal != "none" and windows:
        _annotate_regime(windows, model_ready_daily_features, regime_config)
        regime_applied = True

    leakage = _leakage_checks(windows, label_candidates)
    status = "fail" if leakage["status"] == "fail" else "pass"
    report: dict[str, Any] = {
        "name": "historical_walk_forward_dry_run",
        "status": status,
        "symbol": normalized_symbol,
        "start_date": start_date,
        "end_date": end_date,
        "read_only": True,
        "training_performed": False,
        "threshold_optimization_performed": False,
        "rule_baseline_applied": rule_baseline_applied,
        "regime_applied": regime_applied,
        "config": {
            "horizons": list(horizons),
            "train_window_trading_days": train_window,
            "test_window_trading_days": test_window,
            "step_trading_days": step,
            "rule_baseline": rule_config.as_dict() if rule_config else None,
            "regime": regime_config.as_dict() if regime_config else None,
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
        "cross_window_summary": _cross_window_summary(
            windows, rule_baseline_applied, regime_applied
        ),
        "warnings": _dedupe_warnings(warnings or []),
        "leakage_checks": leakage,
        "source_status": dict(source_status or {}),
        "windows": windows,
    }
    if rule_baseline_applied:
        report["rule_baseline_total_selected"] = int(
            sum(w["rule_baseline"]["selected_rows"] for w in windows if "rule_baseline" in w)
        )
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


# ---------------------------------------------------------------------------
# Regime classification
# ---------------------------------------------------------------------------

def _annotate_regime(
    windows: list[dict[str, Any]],
    daily_frame: pd.DataFrame,
    config: WalkForwardRegimeConfig,
) -> None:
    """Compute realized vol at each train_end and assign regime buckets in-place."""
    vols: list[float | None] = [
        _compute_realized_vol(daily_frame, w["train_end"], config.lookback_days)
        for w in windows
    ]
    buckets = _assign_regime_buckets(vols, config.n_buckets)
    for window, vol, bucket in zip(windows, vols, buckets):
        window["regime"] = {
            "signal": config.signal,
            "train_end_realized_vol": vol,
            "bucket": bucket,
        }


def _compute_realized_vol(
    daily_frame: pd.DataFrame,
    train_end: str,
    lookback_days: int,
) -> float | None:
    """Annualized realized volatility using only dates <= train_end.

    Uses return_1d column if present; otherwise recomputes from close.
    Returns None when fewer than lookback_days returns are available.
    """
    if daily_frame.empty:
        return None
    frame = daily_frame.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date.astype("string")
    frame = frame[frame["date"] <= train_end].sort_values("date")
    if frame.empty:
        return None
    if "return_1d" in frame.columns:
        returns = pd.to_numeric(frame["return_1d"], errors="coerce").dropna()
    elif "close" in frame.columns:
        closes = pd.to_numeric(frame["close"], errors="coerce").dropna()
        returns = closes.pct_change().dropna()
    else:
        return None
    if len(returns) < lookback_days:
        return None
    recent = returns.iloc[-lookback_days:]
    std = float(recent.std(ddof=1))
    if math.isnan(std):
        return None
    return std * math.sqrt(252)


def _assign_regime_buckets(vols: list[float | None], n_buckets: int) -> list[str]:
    """Assign low/mid/high bucket labels using global tertiles across non-None vols.

    Bucket thresholds are computed from all windows' vols together (post-hoc
    descriptive labelling). The vol itself is strictly no-lookahead (dates <= train_end).
    """
    labels = _REGIME_BUCKET_LABELS.get(n_buckets) or [f"bucket_{i}" for i in range(n_buckets)]
    valid = [v for v in vols if v is not None]
    if not valid:
        return ["insufficient_history"] * len(vols)
    series = pd.Series(valid, dtype=float)
    quantiles = [i / n_buckets for i in range(1, n_buckets)]
    thresholds = [float(series.quantile(q)) for q in quantiles]
    result: list[str] = []
    for v in vols:
        if v is None:
            result.append("insufficient_history")
        else:
            idx = sum(1 for t in thresholds if v > t)
            result.append(labels[idx])
    return result


# ---------------------------------------------------------------------------
# Cross-window and by-regime summaries
# ---------------------------------------------------------------------------

def _cross_window_summary(
    windows: list[dict[str, Any]],
    rule_baseline_applied: bool,
    regime_applied: bool = False,
) -> dict[str, Any]:
    total_windows = len(windows)
    zero_row_count = int(sum(1 for w in windows if w["test_row_count"] == 0))
    zero_row_fraction = float(zero_row_count / total_windows) if total_windows > 0 else 0.0

    summary: dict[str, Any] = {
        "total_windows": total_windows,
        "zero_row_window_count": zero_row_count,
        "zero_row_window_fraction": zero_row_fraction,
    }

    if not rule_baseline_applied:
        return summary

    rb_windows = [w for w in windows if "rule_baseline" in w]
    evaluable = [w for w in rb_windows if not w["rule_baseline"]["non_evaluable"]]
    summary["evaluable_windows"] = len(evaluable)
    summary["non_evaluable_windows"] = len(rb_windows) - len(evaluable)
    summary["total_selected_rows"] = int(sum(w["rule_baseline"]["selected_rows"] for w in rb_windows))

    window_means: list[float | None] = [
        w["rule_baseline"]["forward_return"].get("mean") if not w["rule_baseline"]["non_evaluable"] else None
        for w in rb_windows
    ]
    summary["window_mean_returns"] = window_means

    evaluable_with_mean = [
        (
            w["window_id"],
            w["test_start"],
            w["test_end"],
            w["rule_baseline"]["forward_return"].get("mean"),
            w["rule_baseline"]["selected_rows"],
        )
        for w in evaluable
        if w["rule_baseline"]["forward_return"].get("mean") is not None
    ]
    if evaluable_with_mean:
        best = max(evaluable_with_mean, key=lambda x: x[3])
        worst = min(evaluable_with_mean, key=lambda x: x[3])
        summary["best_window"] = {
            "window_id": best[0],
            "test_start": best[1],
            "test_end": best[2],
            "mean_return": best[3],
            "selected_rows": best[4],
        }
        summary["worst_window"] = {
            "window_id": worst[0],
            "test_start": worst[1],
            "test_end": worst[2],
            "mean_return": worst[3],
            "selected_rows": worst[4],
        }
    else:
        summary["best_window"] = None
        summary["worst_window"] = None

    summary["by_horizon"] = _aggregate_by_horizon(evaluable, rb_windows)

    if regime_applied:
        summary["by_regime"] = _by_regime_summary(windows)

    return summary


def _aggregate_by_horizon(
    evaluable: list[dict[str, Any]],
    rb_windows: list[dict[str, Any]],
) -> dict[str, Any]:
    horizon_accum: dict[str, dict[str, float]] = {}
    for w in evaluable:
        for hz, hz_stats in w["rule_baseline"].get("forward_return_by_horizon", {}).items():
            n = hz_stats.get("sample_size", 0)
            mean = hz_stats.get("mean")
            win_rate = hz_stats.get("win_rate")
            if n > 0 and mean is not None and win_rate is not None:
                if hz not in horizon_accum:
                    horizon_accum[hz] = {"n": 0.0, "mean_sum": 0.0, "win_rate_sum": 0.0}
                horizon_accum[hz]["n"] += n
                horizon_accum[hz]["mean_sum"] += n * mean
                horizon_accum[hz]["win_rate_sum"] += n * win_rate

    by_horizon: dict[str, Any] = {}
    for hz in sorted(horizon_accum):
        acc = horizon_accum[hz]
        n = acc["n"]
        hz_selected = int(sum(
            w["rule_baseline"].get("counts_by_horizon", {}).get(hz, 0) for w in rb_windows
        ))
        by_horizon[hz] = {
            "selected_rows": hz_selected,
            "mean_return": float(acc["mean_sum"] / n) if n > 0 else None,
            "win_rate": float(acc["win_rate_sum"] / n) if n > 0 else None,
        }
    return by_horizon


def _by_regime_summary(windows: list[dict[str, Any]]) -> dict[str, Any]:
    """Group windows by regime bucket and aggregate rule-baseline stats per bucket."""
    regime_groups: dict[str, list[dict[str, Any]]] = {}
    for w in windows:
        bucket = (w.get("regime") or {}).get("bucket")
        if bucket is None:
            continue
        regime_groups.setdefault(bucket, []).append(w)

    out: dict[str, Any] = {}
    for bucket in sorted(regime_groups):
        bucket_windows = regime_groups[bucket]
        rb_windows = [w for w in bucket_windows if "rule_baseline" in w]
        evaluable = [w for w in rb_windows if not w["rule_baseline"]["non_evaluable"]]
        total_selected = int(sum(w["rule_baseline"]["selected_rows"] for w in rb_windows))

        evaluable_with_mean = [
            (
                w["window_id"],
                w["test_start"],
                w["test_end"],
                w["rule_baseline"]["forward_return"].get("mean"),
                w["rule_baseline"]["selected_rows"],
            )
            for w in evaluable
            if w["rule_baseline"]["forward_return"].get("mean") is not None
        ]
        best = max(evaluable_with_mean, key=lambda x: x[3]) if evaluable_with_mean else None
        worst = min(evaluable_with_mean, key=lambda x: x[3]) if evaluable_with_mean else None

        out[bucket] = {
            "total_windows": len(bucket_windows),
            "evaluable_windows": len(evaluable),
            "total_selected_rows": total_selected,
            "by_horizon": _aggregate_by_horizon(evaluable, rb_windows),
            "best_window": (
                {"window_id": best[0], "test_start": best[1], "test_end": best[2],
                 "mean_return": best[3], "selected_rows": best[4]}
                if best else None
            ),
            "worst_window": (
                {"window_id": worst[0], "test_start": worst[1], "test_end": worst[2],
                 "mean_return": worst[3], "selected_rows": worst[4]}
                if worst else None
            ),
        }
    return out


# ---------------------------------------------------------------------------
# Window building
# ---------------------------------------------------------------------------

def _build_windows(
    *,
    trading_days: list[str],
    label_candidates: pd.DataFrame,
    train_window: int,
    test_window: int,
    step: int,
    option_joined: pd.DataFrame | None = None,
    rule_config: WalkForwardRuleConfig | None = None,
) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    if len(trading_days) <= train_window:
        return windows
    labels = label_candidates.copy()
    if not labels.empty:
        labels["observation_date"] = pd.to_datetime(labels["observation_date"], errors="coerce").dt.date.astype("string")
    joined: pd.DataFrame | None = None
    if option_joined is not None and not option_joined.empty and "observation_date" in option_joined.columns:
        joined = option_joined.copy()
        joined["observation_date"] = pd.to_datetime(joined["observation_date"], errors="coerce").dt.date.astype("string")
    start_index = 0
    window_id = 1
    while start_index + train_window < len(trading_days):
        train_days = trading_days[start_index : start_index + train_window]
        test_days = trading_days[start_index + train_window : start_index + train_window + test_window]
        if not test_days:
            break
        train_rows = _rows_for_dates(labels, train_days)
        test_rows = _rows_for_dates(labels, test_days)
        window: dict[str, Any] = {
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
        if joined is not None and rule_config is not None:
            test_joined_rows = _rows_for_dates(joined, test_days)
            window["rule_baseline"] = _rule_baseline_window_summary(test_joined_rows, rule_config)
        windows.append(window)
        start_index += step
        window_id += 1
    return windows


def _rule_baseline_window_summary(rows: pd.DataFrame, config: WalkForwardRuleConfig) -> dict[str, Any]:
    eligible_rows = int(len(rows))
    selected = _apply_rule_filter(rows, config)
    selected_rows = int(len(selected))
    selection_rate = float(selected_rows / eligible_rows) if eligible_rows > 0 else None
    non_evaluable = selected.empty
    non_evaluable_reason: str | None = None
    if non_evaluable:
        non_evaluable_reason = "zero_input_rows" if rows.empty else "no_rows_pass_filters"
    values: pd.Series = pd.Series(dtype=float)
    if not selected.empty and "forward_return" in selected.columns:
        values = pd.to_numeric(selected["forward_return"], errors="coerce").dropna()

    forward_return_by_horizon: dict[str, Any] = {}
    if not selected.empty and {"forward_return", "horizon"}.issubset(selected.columns):
        for hz, grp in selected.groupby("horizon", dropna=False):
            hz_vals = pd.to_numeric(grp["forward_return"], errors="coerce").dropna()
            forward_return_by_horizon[str(hz)] = {
                "sample_size": int(hz_vals.count()),
                "mean": float(hz_vals.mean()) if not hz_vals.empty else None,
                "median": float(hz_vals.median()) if not hz_vals.empty else None,
                "win_rate": float((hz_vals > 0).mean()) if not hz_vals.empty else None,
            }

    eff = effective_sample_diagnostics(selected, date_col="observation_date")
    dw = date_weighted_metrics(selected, date_col="observation_date", return_col="forward_return")
    return {
        "data_level": "option_row",
        "eligible_rows": eligible_rows,
        "selected_rows": selected_rows,
        "selection_rate": selection_rate,
        "non_evaluable": non_evaluable,
        "non_evaluable_reason": non_evaluable_reason,
        "counts_by_horizon": _count_by(selected, "horizon"),
        "counts_by_option_type": _count_by(selected, "option_type"),
        "forward_return": {
            "sample_size": int(values.count()),
            "mean": float(values.mean()) if not values.empty else None,
            "median": float(values.median()) if not values.empty else None,
            "win_rate": float((values > 0).mean()) if not values.empty else None,
        },
        "forward_return_by_horizon": forward_return_by_horizon,
        "effective_sample": {**eff, **dw},
    }


def _apply_rule_filter(rows: pd.DataFrame, config: WalkForwardRuleConfig) -> pd.DataFrame:
    if rows.empty:
        return rows
    result = rows
    if config.option_type.lower() != "both" and "option_type" in result.columns:
        ot = config.option_type.upper()
        if ot == "CALL":
            ot = "C"
        elif ot == "PUT":
            ot = "P"
        result = result[result["option_type"].astype("string").str.upper() == ot]
    if config.min_open_interest > 0 and "open_interest" in result.columns:
        oi = pd.to_numeric(result["open_interest"], errors="coerce")
        result = result[oi.ge(config.min_open_interest).fillna(False)]
    if config.min_volume > 0 and "volume" in result.columns:
        vol = pd.to_numeric(result["volume"], errors="coerce")
        result = result[vol.ge(config.min_volume).fillna(False)]
    if config.moneyness_bucket is not None and "moneyness_bucket" in result.columns:
        result = result[result["moneyness_bucket"].astype("string") == config.moneyness_bucket]
    return result.copy()


def _join_options_to_labels(labels: pd.DataFrame, options: pd.DataFrame) -> pd.DataFrame:
    if labels.empty or options.empty:
        return pd.DataFrame()
    label_frame = labels.copy()
    option_frame = options.copy()
    label_frame["observation_date"] = (
        pd.to_datetime(label_frame["observation_date"], errors="coerce").dt.date.astype("string")
    )
    option_frame["date"] = pd.to_datetime(option_frame["date"], errors="coerce").dt.date.astype("string")
    join_cols = ["underlying_symbol", "expiration", "strike", "option_type"]
    available_join = [c for c in join_cols if c in label_frame.columns and c in option_frame.columns]
    if not available_join:
        return pd.DataFrame()
    merged = label_frame.merge(
        option_frame,
        left_on=["observation_date"] + available_join,
        right_on=["date"] + available_join,
        how="left",
        suffixes=("", "_option"),
    )
    if "moneyness" in merged.columns:
        merged["moneyness_bucket"] = _moneyness_bucket_series(merged["moneyness"])
    return merged


def _moneyness_bucket_series(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    buckets = pd.cut(
        numeric,
        bins=[float("-inf"), -0.05, -0.01, 0.01, 0.05, float("inf")],
        labels=["deep_itm_put_or_otm_call", "near_itm", "atm", "near_otm", "deep_otm_call_or_itm_put"],
    )
    return buckets.astype("string").fillna("unknown")


# ---------------------------------------------------------------------------
# Leakage checks
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
