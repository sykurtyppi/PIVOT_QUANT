"""Formal candidate signal specification for high_vol_trend_early_candidate.

Defines the validated filtered signal as an entry condition candidate.
No live trading, model training, threshold tuning, or governance changes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.ml_regime_benchmark import _safe_float

CANDIDATE_SIGNAL_NAME = "high_vol_trend_early_candidate"
_TREND_FEATURE = "price_momentum_20d"
_LOW_SAMPLE_THRESHOLD = 10


@dataclass(frozen=True)
class CandidateSignalSpec:
    name: str = CANDIDATE_SIGNAL_NAME
    description: str = (
        "realized_vol_60d >= train_period_median"
        " AND price_momentum_20d > 0"
        " AND distance_from_20d_mean < train_derived_late_trend_threshold"
    )
    live_trading_enabled: bool = False
    model_training_performed: bool = False
    threshold_optimization_performed: bool = False
    governance_promotion_performed: bool = False
    performance_claim: bool = False


def derive_candidate_signal_thresholds(train_frame: pd.DataFrame) -> dict[str, float | None]:
    """Derive vol_split_value and maturity_threshold from train data only.

    train_frame must have distance_from_20d_mean already computed
    (i.e. passed through _add_explanatory_variables before calling here).
    """
    vol = pd.to_numeric(
        train_frame.get("realized_vol_60d", pd.Series(dtype="float64")), errors="coerce"
    )
    vol_clean = vol.dropna()
    vol_split_value: float | None = _safe_float(vol_clean.median()) if not vol_clean.empty else None

    maturity_threshold: float | None = None
    if vol_split_value is not None:
        trend = pd.to_numeric(
            train_frame.get(_TREND_FEATURE, pd.Series(dtype="float64")), errors="coerce"
        )
        bucket_mask = (vol >= vol_split_value) & (trend > 0)
        bucket = train_frame[bucket_mask]
        maturity_values = pd.to_numeric(
            bucket.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce"
        ).dropna()
        if not maturity_values.empty:
            maturity_threshold = _safe_float(maturity_values.quantile(0.70))

    return {
        "vol_split_value": vol_split_value,
        "maturity_threshold": maturity_threshold,
    }


def apply_candidate_signal(
    frame: pd.DataFrame,
    *,
    vol_split_value: float,
    maturity_threshold: float,
) -> pd.DataFrame:
    """Return rows satisfying all three candidate signal conditions."""
    if frame.empty:
        return frame.copy()
    vol = pd.to_numeric(
        frame.get("realized_vol_60d", pd.Series(dtype="float64")), errors="coerce"
    )
    trend = pd.to_numeric(
        frame.get(_TREND_FEATURE, pd.Series(dtype="float64")), errors="coerce"
    )
    maturity = pd.to_numeric(
        frame.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce"
    )
    mask = (vol >= vol_split_value) & (trend > 0) & maturity.notna() & (maturity < maturity_threshold)
    return frame[mask].copy()


def _period_stats(
    frame: pd.DataFrame,
    *,
    vol_split_value: float,
    maturity_threshold: float,
) -> dict[str, Any]:
    vol = pd.to_numeric(
        frame.get("realized_vol_60d", pd.Series(dtype="float64")), errors="coerce"
    )
    trend = pd.to_numeric(
        frame.get(_TREND_FEATURE, pd.Series(dtype="float64")), errors="coerce"
    )
    maturity = pd.to_numeric(
        frame.get("distance_from_20d_mean", pd.Series(dtype="float64")), errors="coerce"
    )

    baseline_mask = (vol >= vol_split_value) & (trend > 0)
    baseline_rows = int(baseline_mask.sum())

    signal_mask = baseline_mask & maturity.notna() & (maturity < maturity_threshold)
    signal_frame = frame[signal_mask]
    forward_5d = pd.to_numeric(
        signal_frame.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce"
    ).dropna()

    signal_rows = int(len(forward_5d))
    late_trend_excluded = baseline_rows - int(signal_mask.sum())
    win_rate = _safe_float((forward_5d > 0).mean()) if not forward_5d.empty else None
    mean_return = _safe_float(forward_5d.mean()) if not forward_5d.empty else None
    sample_size_safe = _sample_size_safe(signal_frame, low_sample_threshold=_LOW_SAMPLE_THRESHOLD)

    return {
        "baseline_rows": baseline_rows,
        "signal_rows": signal_rows,
        "late_trend_excluded": late_trend_excluded,
        "win_rate_5d": win_rate,
        "mean_return_5d": mean_return,
        "sample_size_safe": sample_size_safe,
    }


def _sample_size_safe(frame: pd.DataFrame, *, low_sample_threshold: int) -> bool:
    if frame.empty:
        return False
    if "entry_date" not in frame.columns:
        return bool(len(frame) >= low_sample_threshold)
    dates = pd.to_datetime(frame["entry_date"], errors="coerce")
    quarters = dates.dt.to_period("Q")
    per_quarter = quarters.value_counts()
    if per_quarter.empty:
        return False
    return bool(int(per_quarter.min()) >= low_sample_threshold)


def build_candidate_signal_report(
    *,
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
) -> dict[str, Any]:
    """Build candidate signal diagnostics.

    train_frame and test_frame must have distance_from_20d_mean already
    computed (pass through _add_explanatory_variables before calling here).
    Thresholds are derived from train_frame only — no test data is used.
    """
    spec = CandidateSignalSpec()
    thresholds = derive_candidate_signal_thresholds(train_frame)
    vol_split_value = thresholds["vol_split_value"]
    maturity_threshold = thresholds["maturity_threshold"]

    if vol_split_value is None or maturity_threshold is None:
        return {
            "status": "missing",
            "purpose": "define the validated filtered signal as a formal candidate entry condition",
            "signal_name": spec.name,
            "spec": {
                "live_trading_enabled": spec.live_trading_enabled,
                "model_training_performed": spec.model_training_performed,
                "threshold_optimization_performed": spec.threshold_optimization_performed,
                "governance_promotion_performed": spec.governance_promotion_performed,
                "performance_claim": spec.performance_claim,
            },
            "reason": "vol_split_value or maturity_threshold could not be derived from train data",
            "definitions": {
                "training_performed": False,
                "threshold_optimization_performed": False,
                "filter_changes_performed": False,
                "live_trading_enabled": False,
                "governance_promotion_performed": False,
            },
        }

    train_stats = _period_stats(
        train_frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
    )
    test_stats = _period_stats(
        test_frame, vol_split_value=vol_split_value, maturity_threshold=maturity_threshold
    )

    return {
        "status": "ok",
        "purpose": "define the validated filtered signal as a formal candidate entry condition",
        "signal_name": spec.name,
        "formula": {
            "condition_1": "realized_vol_60d >= vol_split_value",
            "condition_2": "price_momentum_20d > 0",
            "condition_3": "distance_from_20d_mean < maturity_threshold",
            "operator": "AND",
            "description": spec.description,
        },
        "thresholds": {
            "vol_split_value": vol_split_value,
            "maturity_threshold": maturity_threshold,
            "threshold_source": "train period only",
            "vol_split_method": "median of realized_vol_60d in train period",
            "maturity_threshold_method": "quantile(0.70) of distance_from_20d_mean in high_vol_trend_positive train bucket",
        },
        "spec": {
            "live_trading_enabled": spec.live_trading_enabled,
            "model_training_performed": spec.model_training_performed,
            "threshold_optimization_performed": spec.threshold_optimization_performed,
            "governance_promotion_performed": spec.governance_promotion_performed,
            "performance_claim": spec.performance_claim,
        },
        "intended_interpretation": (
            "diagnostic candidate only; requires prospective validation and governance review"
            " before any live trading consideration"
        ),
        "limitations": [
            "thresholds derived from limited historical data",
            "no prospective validation has been performed",
            "no edge claim is made",
            "sample sizes may be insufficient in some periods",
        ],
        "train": train_stats,
        "test": test_stats,
        "disclaimer": "no edge claim; diagnostic and specification only",
        "definitions": {
            "training_performed": False,
            "threshold_optimization_performed": False,
            "filter_changes_performed": False,
            "live_trading_enabled": False,
            "governance_promotion_performed": False,
        },
    }


def write_candidate_signal_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "ml_diagnostics"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    stem = f"candidate_signal_{report.get('signal_name', 'unknown')}"
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path
