from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import numpy as np

ThresholdObjective = Literal["f1", "utility_bps"]
ThresholdTarget = Literal["reject", "break"]


@dataclass
class ThresholdCandidate:
    threshold: float
    score: float
    precision: float
    recall: float
    signals: int
    stability_score: float | None = None


@dataclass
class ThresholdSelection:
    threshold: float
    objective: ThresholdObjective
    score: float
    precision: float
    recall: float
    signals: int
    evaluated_candidates: int
    fallback: bool
    stability_score: float | None = None
    stability_band: float = 0.0
    top_candidates: list[dict[str, float | int]] = field(default_factory=list)


def directional_return_bps(return_bps, touch_side) -> np.ndarray:
    """Convert raw return_bps into direction-aware trade return.

    touch_side = 1 means rejection-up is favorable.
    touch_side = -1 means rejection-down is favorable.
    """
    returns = np.asarray(return_bps, dtype=float)
    side = np.asarray(touch_side, dtype=float)

    side = np.where(np.isfinite(side), side, 1.0)
    side = np.where(np.isin(side, (-1.0, 1.0)), side, np.sign(side))
    side = np.where(side == 0.0, 1.0, side)

    return returns * side


def utility_bps_for_target(
    return_bps,
    touch_side,
    target: ThresholdTarget,
    *,
    trade_cost_bps: float = 0.0,
) -> np.ndarray:
    """Compute per-signal utility for reject/break classifiers.

    reject utility: directional_return - cost
    break utility:  -directional_return - cost
    """
    directional = directional_return_bps(return_bps, touch_side)
    direction = 1.0 if target == "reject" else -1.0
    return direction * directional - float(trade_cost_bps)


def _classification_stats(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[float, float, float]:
    tp = int(np.sum((y_pred == 1) & (y_true == 1)))
    fp = int(np.sum((y_pred == 1) & (y_true == 0)))
    fn = int(np.sum((y_pred == 0) & (y_true == 1)))

    precision = (tp / (tp + fp)) if (tp + fp) > 0 else 0.0
    recall = (tp / (tp + fn)) if (tp + fn) > 0 else 0.0
    f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    return precision, recall, f1


def _candidate_thresholds(y_prob: np.ndarray, default_threshold: float, max_candidates: int = 400) -> np.ndarray:
    values = np.unique(np.clip(np.asarray(y_prob, dtype=float), 0.0, 1.0))
    if values.size == 0:
        return np.asarray([float(default_threshold)], dtype=float)

    if values.size > max_candidates:
        idx = np.linspace(0, values.size - 1, num=max_candidates, dtype=int)
        values = values[idx]

    values = np.unique(
        np.concatenate([values, np.asarray([float(default_threshold), 0.5], dtype=float)])
    )
    return np.sort(values)[::-1]


def select_threshold(
    y_true,
    y_prob,
    *,
    objective: ThresholdObjective = "f1",
    precision_floor: float = 0.4,
    min_signals: int = 10,
    default_threshold: float = 0.5,
    utility_per_signal=None,
    stability_band: float = 0.0,
    top_k: int = 5,
) -> ThresholdSelection:
    """Select a probability threshold using either F1 or cost-aware utility."""
    y_true_arr = np.asarray(y_true, dtype=int)
    y_prob_arr = np.asarray(y_prob, dtype=float)
    if y_true_arr.size == 0 or y_prob_arr.size == 0 or y_true_arr.size != y_prob_arr.size:
        raise ValueError("y_true and y_prob must be non-empty and same length")

    utility_arr = None
    if objective == "utility_bps":
        if utility_per_signal is None:
            raise ValueError("utility_per_signal is required for utility_bps objective")
        utility_arr = np.asarray(utility_per_signal, dtype=float)
        if utility_arr.size != y_prob_arr.size:
            raise ValueError("utility_per_signal must match y_prob length")

    candidates = _candidate_thresholds(y_prob_arr, float(default_threshold))
    eps = 1e-12
    evaluated = 0
    kept: list[ThresholdCandidate] = []

    for threshold in candidates:
        y_pred = (y_prob_arr >= threshold).astype(int)
        signals = int(np.sum(y_pred))
        if signals < int(min_signals):
            continue

        precision, recall, f1 = _classification_stats(y_true_arr, y_pred)
        if precision < float(precision_floor):
            continue

        evaluated += 1
        score = float(f1 if objective == "f1" else np.sum(utility_arr[y_pred == 1]))
        kept.append(
            ThresholdCandidate(
                threshold=float(threshold),
                score=score,
                precision=float(precision),
                recall=float(recall),
                signals=signals,
            )
        )

    if kept:
        band = max(0.0, float(stability_band))
        for candidate in kept:
            if band > 0.0 and len(kept) > 1:
                local_scores = [
                    c.score
                    for c in kept
                    if abs(c.threshold - candidate.threshold) <= band + eps
                ]
                candidate.stability_score = float(np.mean(local_scores)) if local_scores else candidate.score
            else:
                candidate.stability_score = candidate.score

        ranked = sorted(
            kept,
            key=lambda c: (
                float(c.stability_score if c.stability_score is not None else c.score),
                c.score,
                c.precision,
                c.signals,
            ),
            reverse=True,
        )

        best = ranked[0]
        top = [
            {
                "threshold": float(c.threshold),
                "score": float(c.score),
                "stability_score": float(c.stability_score if c.stability_score is not None else c.score),
                "precision": float(c.precision),
                "recall": float(c.recall),
                "signals": int(c.signals),
            }
            for c in ranked[: max(1, int(top_k))]
        ]

        return ThresholdSelection(
            threshold=float(best.threshold),
            objective=objective,
            score=float(best.score),
            precision=float(best.precision),
            recall=float(best.recall),
            signals=int(best.signals),
            evaluated_candidates=int(evaluated),
            fallback=False,
            stability_score=float(best.stability_score if best.stability_score is not None else best.score),
            stability_band=band,
            top_candidates=top,
        )

    y_pred = (y_prob_arr >= float(default_threshold)).astype(int)
    precision, recall, f1 = _classification_stats(y_true_arr, y_pred)
    fallback_score = float(f1)
    if objective == "utility_bps" and utility_arr is not None:
        fallback_score = float(np.sum(utility_arr[y_pred == 1]))
    fallback_signals = int(np.sum(y_pred))

    return ThresholdSelection(
        threshold=float(default_threshold),
        objective=objective,
        score=fallback_score,
        precision=float(precision),
        recall=float(recall),
        signals=fallback_signals,
        evaluated_candidates=int(evaluated),
        fallback=True,
        stability_score=fallback_score,
        stability_band=max(0.0, float(stability_band)),
        top_candidates=[
            {
                "threshold": float(default_threshold),
                "score": float(fallback_score),
                "stability_score": float(fallback_score),
                "precision": float(precision),
                "recall": float(recall),
                "signals": fallback_signals,
            }
        ],
    )
