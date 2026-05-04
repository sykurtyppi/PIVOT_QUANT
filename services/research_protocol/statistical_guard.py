"""Statistical guardrails (RESEARCH_PROTOCOL §4).

Three rules, all *enforced* (not warned):

  1. **n_eff floor.** If the lower-bound effective sample size is below
     the per-stage threshold, performance metrics are *suppressed* —
     removed from the metrics dict — and the stage cannot pass.
  2. **Bootstrap CI.** If the bootstrap confidence interval on the
     statistic of interest includes zero, the stage cannot pass.
  3. **Permutation p-value.** If the permutation p-value exceeds the
     alpha threshold, the stage cannot pass.

The combinator :func:`evaluate_statistical_validity` produces a frozen
:class:`StatisticalVerdict` that is recorded in the validation-ladder
state file for every stage 2+ entry. Reports may not display
performance metrics when ``metrics_suppressed=True`` —
:func:`suppress_metrics_if_invalid` is the single-source-of-truth
sanitizer.

Bootstrap and permutation use ``numpy.random.default_rng(rng_seed)`` so
runs are deterministic given the same seed. The Phipson-Smyth
``(count + 1) / (n_iter + 1)`` p-value floor avoids ``p == 0`` from
finite resamples.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np

from services.research_protocol.errors import StatisticalViolationError

# RESEARCH_PROTOCOL §4.2 — per-stage minimum n_eff.
N_EFF_FLOOR_BY_STAGE: dict[int, int] = {
    2: 30,
    3: 30,   # per period
    4: 30,   # per symbol
    5: 60,
    6: 100,
}
N_EFF_FLOOR_DEFAULT = 30
DEFAULT_PERMUTATION_ALPHA = 0.05

STATISTICAL_VALIDITY_KEY = "statistical_validity"

# Performance-metric keys that are suppressed when n_eff is below floor.
# Audit/diagnostic keys (registration_hash, run_timestamp,
# dataset_identifier, n_obs, n_eff, ci_*, permutation_p_value, etc.) are
# preserved.
SUPPRESSIBLE_METRIC_KEYS: frozenset[str] = frozenset({
    "win_rate",
    "mean_return",
    "median_return",
    "sharpe",
    "sortino",
    "max_drawdown",
    "win_rate_1d",
    "win_rate_5d",
    "win_rate_21d",
    "mean_return_1d",
    "mean_return_5d",
    "mean_return_21d",
    "median_return_1d",
    "median_return_5d",
    "median_return_21d",
    "filtered_test_win_rate",
    "filtered_test_mean_return",
    "filtered_train_win_rate",
    "filtered_train_mean_return",
    "baseline_test_win_rate",
    "baseline_test_mean_return",
})


@dataclass(frozen=True)
class StatisticalVerdict:
    """Combined verdict over the three statistical rules."""

    stage: int
    n_obs: int
    horizon_days: int
    n_eff: int
    n_eff_floor: int
    ci_lower: float | None
    ci_upper: float | None
    permutation_p_value: float | None
    permutation_alpha: float
    statistical_pass: bool
    metrics_suppressed: bool
    suppression_reasons: tuple[str, ...]


# --------------------------------------------------------------------- #
# n_eff
# --------------------------------------------------------------------- #


def compute_effective_sample_size(
    returns: Sequence[float],
    *,
    horizon_days: int,
) -> int:
    """Lower-bound n_eff = floor(n_obs / horizon_days).

    The conservative default (RESEARCH_PROTOCOL §4.1). Any
    autocorrelation-adjusted estimate may be reported alongside but
    never instead of this value.
    """
    if not isinstance(horizon_days, int) or isinstance(horizon_days, bool):
        raise StatisticalViolationError(
            f"horizon_days must be an int; got {horizon_days!r}"
        )
    if horizon_days < 1:
        raise StatisticalViolationError(
            f"horizon_days must be >= 1; got {horizon_days!r}"
        )
    n_obs = len(returns)
    return max(0, n_obs // horizon_days)


def n_eff_floor_for_stage(stage: int) -> int:
    """Per-stage minimum n_eff (RESEARCH_PROTOCOL §4.2)."""
    if not isinstance(stage, int) or isinstance(stage, bool):
        raise StatisticalViolationError(
            f"stage must be an int; got {stage!r}"
        )
    return N_EFF_FLOOR_BY_STAGE.get(stage, N_EFF_FLOOR_DEFAULT)


def assert_minimum_sample(
    n_eff: int,
    *,
    threshold: int,
    stage: int | None = None,
) -> None:
    """Raise :class:`StatisticalViolationError` if ``n_eff < threshold``."""
    if not isinstance(n_eff, int) or isinstance(n_eff, bool) or n_eff < 0:
        raise StatisticalViolationError(
            f"n_eff must be a non-negative int; got {n_eff!r}"
        )
    if (
        not isinstance(threshold, int)
        or isinstance(threshold, bool)
        or threshold < 0
    ):
        raise StatisticalViolationError(
            f"threshold must be a non-negative int; got {threshold!r}"
        )
    if n_eff < threshold:
        stage_clause = f" for stage {stage}" if stage is not None else ""
        raise StatisticalViolationError(
            f"n_eff={n_eff} below threshold={threshold}{stage_clause}."
            " Stage cannot pass; performance metrics will be suppressed."
        )


# --------------------------------------------------------------------- #
# Bootstrap CI (block-resampled)
# --------------------------------------------------------------------- #


def _compute_statistic(arr: np.ndarray, statistic: str) -> float:
    if arr.size == 0:
        raise StatisticalViolationError("cannot compute statistic on empty sample")
    if statistic == "mean":
        return float(arr.mean())
    if statistic == "win_rate":
        return float((arr > 0).mean())
    if statistic == "sharpe":
        std = float(arr.std(ddof=1)) if arr.size > 1 else 0.0
        return 0.0 if std == 0.0 else float(arr.mean()) / std
    raise StatisticalViolationError(
        f"unknown statistic {statistic!r}; expected one of"
        " {'mean', 'win_rate', 'sharpe'}"
    )


def compute_bootstrap_ci(
    returns: Sequence[float],
    *,
    method: str = "block",
    block_size: int,
    iterations: int = 10_000,
    confidence: float = 0.95,
    rng_seed: int = 0,
    statistic: str = "mean",
) -> tuple[float, float]:
    """Block-bootstrap confidence interval on a statistic of returns.

    Args:
      returns: sample of returns.
      method: only ``"block"`` is supported.
      block_size: contiguous block length used for resampling. Must be
        <= len(returns). For overlapping forward returns at horizon h,
        callers should use block_size >= h.
      iterations: bootstrap iteration count; must be >= 100.
      confidence: two-sided coverage in (0, 1); 0.95 is the default.
      rng_seed: integer seed for reproducibility.
      statistic: ``"mean"`` (default), ``"win_rate"``, or ``"sharpe"``.

    Returns:
      ``(lower, upper)`` tuple at the (1-confidence)/2 and
      1-(1-confidence)/2 quantiles of the bootstrap distribution.
    """
    if method != "block":
        raise StatisticalViolationError(
            f"only method='block' is supported; got {method!r}"
        )
    if not isinstance(block_size, int) or isinstance(block_size, bool):
        raise StatisticalViolationError(
            f"block_size must be an int; got {block_size!r}"
        )
    if block_size < 1:
        raise StatisticalViolationError(
            f"block_size must be >= 1; got {block_size}"
        )
    if not isinstance(iterations, int) or isinstance(iterations, bool):
        raise StatisticalViolationError(
            f"iterations must be an int; got {iterations!r}"
        )
    if iterations < 100:
        raise StatisticalViolationError(
            f"iterations must be >= 100; got {iterations}"
        )
    if not (0.0 < float(confidence) < 1.0):
        raise StatisticalViolationError(
            f"confidence must be in (0, 1); got {confidence!r}"
        )
    arr = np.asarray(returns, dtype=float)
    n = arr.size
    if n == 0:
        raise StatisticalViolationError("returns must be non-empty")
    if block_size > n:
        raise StatisticalViolationError(
            f"block_size={block_size} larger than sample size n={n}"
        )
    rng = np.random.default_rng(int(rng_seed))
    starts_max = n - block_size + 1
    n_blocks = (n + block_size - 1) // block_size
    stats = np.empty(int(iterations), dtype=float)
    for i in range(int(iterations)):
        starts = rng.integers(0, starts_max, size=n_blocks)
        chunks = np.concatenate([arr[s:s + block_size] for s in starts])
        resample = chunks[:n]
        stats[i] = _compute_statistic(resample, statistic)
    alpha = (1.0 - float(confidence)) / 2.0
    lo, hi = np.quantile(stats, [alpha, 1.0 - alpha])
    return float(lo), float(hi)


# --------------------------------------------------------------------- #
# Permutation test
# --------------------------------------------------------------------- #


def run_permutation_test(
    *,
    signal_returns: Sequence[float],
    baseline_returns: Sequence[float],
    n_iter: int = 1000,
    rng_seed: int = 0,
    one_sided: str = "greater",
) -> float:
    """Permutation test on the difference of means (signal − baseline).

    Returns the one-sided p-value with ``(count + 1) / (n_iter + 1)``
    (Phipson-Smyth) so the smallest reportable p is ``1 / (n_iter + 1)``,
    avoiding ``p == 0`` from finite resamples.

    ``one_sided="greater"`` tests ``H1: signal_mean > baseline_mean``.
    ``one_sided="less"`` tests ``H1: signal_mean < baseline_mean``.
    """
    if not isinstance(n_iter, int) or isinstance(n_iter, bool):
        raise StatisticalViolationError(
            f"n_iter must be an int; got {n_iter!r}"
        )
    if n_iter < 100:
        raise StatisticalViolationError(
            f"n_iter must be >= 100; got {n_iter}"
        )
    if one_sided not in ("greater", "less"):
        raise StatisticalViolationError(
            f"one_sided must be 'greater' or 'less'; got {one_sided!r}"
        )
    sig = np.asarray(signal_returns, dtype=float)
    base = np.asarray(baseline_returns, dtype=float)
    if sig.size == 0 or base.size == 0:
        raise StatisticalViolationError(
            "signal_returns and baseline_returns must be non-empty"
        )
    pooled = np.concatenate([sig, base])
    n_sig = sig.size
    observed = float(sig.mean()) - float(base.mean())
    rng = np.random.default_rng(int(rng_seed))
    count = 0
    for _ in range(int(n_iter)):
        perm = rng.permutation(pooled)
        diff = float(perm[:n_sig].mean()) - float(perm[n_sig:].mean())
        if one_sided == "greater":
            if diff >= observed:
                count += 1
        else:
            if diff <= observed:
                count += 1
    return (count + 1) / (int(n_iter) + 1)


# --------------------------------------------------------------------- #
# Verdict combinator + sanitizer
# --------------------------------------------------------------------- #


def evaluate_statistical_validity(
    *,
    stage: int,
    n_obs: int,
    horizon_days: int,
    ci_lower: float | None,
    ci_upper: float | None,
    permutation_p_value: float | None,
    permutation_alpha: float = DEFAULT_PERMUTATION_ALPHA,
) -> StatisticalVerdict:
    """Combine the three rules into a single verdict.

    n_eff < floor → ``metrics_suppressed=True, statistical_pass=False``.
    CI includes 0 → ``statistical_pass=False`` (metrics not suppressed
    on this rule alone, since the n_obs/n_eff is fine — the result is
    just statistically insignificant).
    p_value >= alpha → ``statistical_pass=False``.
    All-pass → ``statistical_pass=True``.
    """
    if (
        not isinstance(stage, int)
        or isinstance(stage, bool)
        or not (1 <= stage <= 6)
    ):
        raise StatisticalViolationError(
            f"stage must be int in [1, 6]; got {stage!r}"
        )
    if not isinstance(n_obs, int) or isinstance(n_obs, bool) or n_obs < 0:
        raise StatisticalViolationError(
            f"n_obs must be a non-negative int; got {n_obs!r}"
        )
    if (
        not isinstance(horizon_days, int)
        or isinstance(horizon_days, bool)
        or horizon_days < 1
    ):
        raise StatisticalViolationError(
            f"horizon_days must be a positive int; got {horizon_days!r}"
        )
    if not (0.0 < float(permutation_alpha) < 1.0):
        raise StatisticalViolationError(
            f"permutation_alpha must be in (0, 1); got {permutation_alpha!r}"
        )
    n_eff = n_obs // horizon_days
    floor = n_eff_floor_for_stage(stage)
    reasons: list[str] = []

    metrics_suppressed = n_eff < floor
    if metrics_suppressed:
        reasons.append(
            f"n_eff={n_eff} below stage_{stage}_floor={floor}"
            f" (n_obs={n_obs}, horizon_days={horizon_days})"
        )

    ci_pass = True
    if ci_lower is not None or ci_upper is not None:
        if ci_lower is None or ci_upper is None:
            raise StatisticalViolationError(
                "ci_lower and ci_upper must both be provided or both be None"
            )
        if float(ci_lower) > float(ci_upper):
            raise StatisticalViolationError(
                f"ci_lower={ci_lower!r} > ci_upper={ci_upper!r}"
            )
        if float(ci_lower) <= 0.0 <= float(ci_upper):
            reasons.append(
                f"bootstrap CI [{float(ci_lower):.6f},"
                f" {float(ci_upper):.6f}] includes 0"
            )
            ci_pass = False

    perm_pass = True
    if permutation_p_value is not None:
        p = float(permutation_p_value)
        if not (0.0 <= p <= 1.0):
            raise StatisticalViolationError(
                f"permutation_p_value must be in [0, 1]; got {permutation_p_value!r}"
            )
        if p >= float(permutation_alpha):
            reasons.append(
                f"permutation p_value={p:.4f}"
                f" >= alpha={float(permutation_alpha):.4f}"
            )
            perm_pass = False

    statistical_pass = bool(
        not metrics_suppressed and ci_pass and perm_pass
    )

    return StatisticalVerdict(
        stage=int(stage),
        n_obs=int(n_obs),
        horizon_days=int(horizon_days),
        n_eff=int(n_eff),
        n_eff_floor=int(floor),
        ci_lower=None if ci_lower is None else float(ci_lower),
        ci_upper=None if ci_upper is None else float(ci_upper),
        permutation_p_value=(
            None if permutation_p_value is None else float(permutation_p_value)
        ),
        permutation_alpha=float(permutation_alpha),
        statistical_pass=statistical_pass,
        metrics_suppressed=metrics_suppressed,
        suppression_reasons=tuple(reasons),
    )


def assert_statistical_pass(verdict: StatisticalVerdict) -> None:
    """Raise :class:`StatisticalViolationError` unless ``verdict.statistical_pass``."""
    if not verdict.statistical_pass:
        reason_text = "; ".join(verdict.suppression_reasons or ("(no reason)",))
        raise StatisticalViolationError(
            f"statistical_pass=False for stage {verdict.stage}: {reason_text}"
        )


def verdict_to_dict(verdict: StatisticalVerdict) -> dict[str, Any]:
    return {
        "stage": int(verdict.stage),
        "n_obs": int(verdict.n_obs),
        "horizon_days": int(verdict.horizon_days),
        "n_eff": int(verdict.n_eff),
        "n_eff_floor": int(verdict.n_eff_floor),
        "ci_lower": verdict.ci_lower,
        "ci_upper": verdict.ci_upper,
        "permutation_p_value": verdict.permutation_p_value,
        "permutation_alpha": float(verdict.permutation_alpha),
        "statistical_pass": bool(verdict.statistical_pass),
        "metrics_suppressed": bool(verdict.metrics_suppressed),
        "suppression_reasons": list(verdict.suppression_reasons),
    }


def _opt_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def verdict_from_dict(payload: dict[str, Any]) -> StatisticalVerdict:
    """Reconstruct a verdict by RECOMPUTING from the stored numbers.

    The ``statistical_pass`` and ``metrics_suppressed`` fields in the
    payload are NOT trusted; the verdict is derived afresh from
    ``stage``, ``n_obs``, ``horizon_days``, ``ci_*``, and
    ``permutation_p_value``. This catches a tamperer who flips
    ``statistical_pass`` while leaving the inputs alone.
    """
    if not isinstance(payload, dict):
        raise StatisticalViolationError(
            f"statistical_validity block must be a dict; got"
            f" {type(payload).__name__}"
        )
    required = (
        "stage", "n_obs", "horizon_days",
        "ci_lower", "ci_upper", "permutation_p_value",
    )
    missing = [k for k in required if k not in payload]
    if missing:
        raise StatisticalViolationError(
            f"statistical_validity block missing required keys: {missing}"
        )
    return evaluate_statistical_validity(
        stage=int(payload["stage"]),
        n_obs=int(payload["n_obs"]),
        horizon_days=int(payload["horizon_days"]),
        ci_lower=_opt_float(payload["ci_lower"]),
        ci_upper=_opt_float(payload["ci_upper"]),
        permutation_p_value=_opt_float(payload["permutation_p_value"]),
        permutation_alpha=float(
            payload.get("permutation_alpha", DEFAULT_PERMUTATION_ALPHA)
        ),
    )


def suppress_metrics_if_invalid(
    metrics: dict[str, Any],
    *,
    verdict: StatisticalVerdict,
) -> dict[str, Any]:
    """Return a sanitized copy of ``metrics`` with the verdict merged in.

    When ``verdict.metrics_suppressed`` is True, every key in
    :data:`SUPPRESSIBLE_METRIC_KEYS` is removed. Other keys (audit and
    diagnostic fields) are preserved verbatim. The verdict is always
    embedded under :data:`STATISTICAL_VALIDITY_KEY` and mirrored at the
    top level (``n_eff``, ``ci_lower``, ``ci_upper``,
    ``permutation_p_value``, ``metrics_suppressed``, ``statistical_pass``)
    so any consumer can read the verdict without descending.
    """
    if not isinstance(metrics, dict):
        raise StatisticalViolationError(
            f"metrics must be a dict; got {type(metrics).__name__}"
        )
    out: dict[str, Any] = dict(metrics)
    if verdict.metrics_suppressed:
        for key in list(out.keys()):
            if key in SUPPRESSIBLE_METRIC_KEYS:
                del out[key]
    out[STATISTICAL_VALIDITY_KEY] = verdict_to_dict(verdict)
    out["n_eff"] = int(verdict.n_eff)
    out["ci_lower"] = verdict.ci_lower
    out["ci_upper"] = verdict.ci_upper
    out["permutation_p_value"] = verdict.permutation_p_value
    out["metrics_suppressed"] = bool(verdict.metrics_suppressed)
    out["statistical_pass"] = bool(verdict.statistical_pass)
    return out
