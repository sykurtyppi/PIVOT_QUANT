"""Maturity threshold sensitivity analysis for the candidate signal.

Tests whether signal performance is robust to small changes in the quantile
used to define the late-trend threshold. No model training, threshold
optimization, live trading, or governance change.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from services.external_data.ml_candidate_signal import apply_candidate_signal
from services.external_data.ml_candidate_signal_paper_eval import (
    _monthly_breakdown,
    _stability_flags,
)
from services.external_data.ml_regime_benchmark import _safe_float

_TREND_FEATURE = "price_momentum_20d"
SENSITIVITY_QUANTILES: tuple[float, ...] = (0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85)
REFERENCE_QUANTILE: float = 0.70
_ROBUST_MIN_AGREEING: int = 4  # of the 6 non-reference quantiles


@dataclass(frozen=True)
class SensitivityConfig:
    quantiles: tuple[float, ...] = SENSITIVITY_QUANTILES
    reference_quantile: float = REFERENCE_QUANTILE
    live_trading_enabled: bool = False
    threshold_optimization_performed: bool = False
    edge_claim: bool = False


def _derive_vol_split(train_frame: pd.DataFrame) -> float | None:
    vol = pd.to_numeric(
        train_frame.get("realized_vol_60d", pd.Series(dtype="float64")), errors="coerce"
    )
    clean = vol.dropna()
    return _safe_float(clean.median()) if not clean.empty else None


def _derive_maturity_threshold(
    train_frame: pd.DataFrame,
    *,
    vol_split_value: float,
    quantile: float,
) -> float | None:
    """Derive the maturity threshold at an arbitrary quantile using train data only."""
    vol = pd.to_numeric(
        train_frame.get("realized_vol_60d", pd.Series(dtype="float64")), errors="coerce"
    )
    trend = pd.to_numeric(
        train_frame.get(_TREND_FEATURE, pd.Series(dtype="float64")), errors="coerce"
    )
    bucket = train_frame[(vol >= vol_split_value) & (trend > 0)]
    maturity = pd.to_numeric(
        bucket.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce"
    ).dropna()
    return _safe_float(maturity.quantile(quantile)) if not maturity.empty else None


def _quantile_stats(
    test_frame: pd.DataFrame,
    *,
    vol_split_value: float,
    maturity_threshold: float,
) -> dict[str, Any]:
    vol = pd.to_numeric(
        test_frame.get("realized_vol_60d", pd.Series(dtype="float64")), errors="coerce"
    )
    trend = pd.to_numeric(
        test_frame.get(_TREND_FEATURE, pd.Series(dtype="float64")), errors="coerce"
    )
    baseline_rows = int(((vol >= vol_split_value) & (trend > 0)).sum())

    signal_frame = apply_candidate_signal(
        test_frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
    )
    forward_5d = pd.to_numeric(
        signal_frame.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce"
    ).dropna()
    signal_rows = int(len(forward_5d))
    win_rate = _safe_float((forward_5d > 0).mean()) if signal_rows > 0 else None
    mean_return = _safe_float(forward_5d.mean()) if signal_rows > 0 else None

    return {
        "baseline_rows": baseline_rows,
        "signal_rows": signal_rows,
        "late_trend_excluded": baseline_rows - signal_rows,
        "win_rate": win_rate,
        "mean_return": mean_return,
    }


def _is_threshold_robust(
    sensitivity_grid: list[dict[str, Any]],
    *,
    reference_quantile: float,
) -> bool:
    """True if the reference quantile has mean_return > 0 and ≥ 4 of 6 others agree."""
    ref_items = [r for r in sensitivity_grid if abs(r["quantile"] - reference_quantile) < 1e-9]
    if not ref_items:
        return False
    ref_mean = ref_items[0].get("mean_return")
    if ref_mean is None or float(ref_mean) <= 0:
        return False
    others = [r for r in sensitivity_grid if abs(r["quantile"] - reference_quantile) >= 1e-9]
    agreeing = sum(
        1 for r in others
        if r.get("mean_return") is not None and float(r["mean_return"]) > 0
    )
    return bool(agreeing >= _ROBUST_MIN_AGREEING)


def build_sensitivity_report(
    *,
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    quantiles: tuple[float, ...] | None = None,
) -> dict[str, Any]:
    """Build the maturity threshold sensitivity report.

    train_frame and test_frame must have distance_from_20d_mean already
    computed (pass through _add_explanatory_variables before calling here).
    Thresholds are derived from train_frame only at each quantile value.
    """
    config = SensitivityConfig()
    grid_quantiles = sorted(quantiles if quantiles is not None else config.quantiles)

    flags = {
        "live_trading_enabled": config.live_trading_enabled,
        "threshold_optimization_performed": config.threshold_optimization_performed,
        "edge_claim": config.edge_claim,
    }
    definitions = {
        "training_performed": False,
        "threshold_optimization_performed": False,
        "filter_changes_performed": False,
        "live_trading_enabled": False,
        "governance_promotion_performed": False,
    }

    vol_split_value = _derive_vol_split(train_frame)
    if vol_split_value is None:
        return {
            "status": "missing",
            "purpose": "maturity threshold sensitivity analysis; no tuning or live trading",
            "reason": "vol_split_value could not be derived from train data",
            "flags": flags,
            "definitions": definitions,
        }

    sensitivity_grid: list[dict[str, Any]] = []
    for q in grid_quantiles:
        threshold = _derive_maturity_threshold(
            train_frame, vol_split_value=vol_split_value, quantile=q
        )
        if threshold is None:
            sensitivity_grid.append(
                {"quantile": q, "maturity_threshold": None, "status": "missing"}
            )
            continue
        stats = _quantile_stats(
            test_frame, vol_split_value=vol_split_value, maturity_threshold=threshold
        )
        monthly = _monthly_breakdown(
            test_frame, vol_split_value=vol_split_value, maturity_threshold=threshold
        )
        stab = _stability_flags(monthly)
        sensitivity_grid.append(
            {
                "quantile": q,
                "maturity_threshold": threshold,
                "status": "ok",
                **stats,
                "stability_flag": stab["stability_flag"],
                "negative_mature_month_warning": stab["negative_mature_month_warning"],
                "low_sample_month_warning": stab["low_sample_month_warning"],
                "concentration_warning": stab["concentration_warning"],
            }
        )

    robust = _is_threshold_robust(sensitivity_grid, reference_quantile=config.reference_quantile)

    return {
        "status": "ok",
        "purpose": "maturity threshold sensitivity analysis; no tuning or live trading",
        "reference_quantile": config.reference_quantile,
        "vol_split_value": vol_split_value,
        "sensitivity_grid": sensitivity_grid,
        "threshold_robust": robust,
        "flags": flags,
        "flag_criteria": {
            "threshold_robust": (
                "true if reference quantile (0.70) mean_return > 0"
                " and at least 4 of the other 6 quantiles also have mean_return > 0"
            ),
        },
        "disclaimer": "no edge claim; sensitivity analysis only; no threshold selection performed",
        "definitions": definitions,
    }
