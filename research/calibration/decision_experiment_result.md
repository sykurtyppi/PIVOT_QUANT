# Result — volatility-targeting decision experiment

Ran the frozen design in `decision_experiment_prereg.md` (committed before the backtest). 10y
adjusted SPY, 2016-09-26 → 2026-09-25, 2,453 scored days; holdout = last 30%.
Rule: `w = clip(0.15 / ann_σ̂, 0, 1.5)`, 2 bps turnover cost, 1-day timing, no financing.

## Full sample
| method | annRet | annVol | Sharpe | maxDD | ES(5%) | turn/day | CE(γ=3) |
|---|---|---|---|---|---|---|---|
| fixed | 14.2% | 18.2% | 0.78 | −41.1% | −2.8% | 0.000 | 9.2% |
| rv20 | 14.7% | 15.3% | **0.96** | **−21.2%** | −2.4% | 0.025 | 11.1% |
| ewma94 | 13.3% | 15.0% | 0.89 | −21.8% | −2.4% | 0.024 | 10.0% |

Daily net-return difference (annualized), 95% block-bootstrap CI:
- rv20 − fixed: +0.50% CI [−4.41%, +5.61%] → **no difference detected**
- ewma94 − fixed: −0.84% CI [−5.55%, +4.04%] → no difference detected
- rv20 − ewma94: +1.34% CI [+0.33%, +2.48%] → rv20 better (within the vol-targeting family)

## Holdout (last 30%, 754 days — ~2023–2026, a calm bull run)
| method | annRet | annVol | Sharpe | maxDD | CE(γ=3) |
|---|---|---|---|---|---|
| **fixed** | 20.7% | 15.2% | **1.36** | −20.8% | 17.2% |
| rv20 | 20.3% | 15.7% | 1.29 | −18.3% | 16.6% |
| ewma94 | 18.5% | 15.5% | 1.20 | −18.1% | 14.9% |

- rv20 − fixed: −0.39% CI [−6.01%, +4.92%] → no difference detected (and fixed's Sharpe is higher).

## Verdict against the frozen criteria: DOES NOT CLEAR THE BAR — stop escalating

The prereg required vol-targeting to improve Sharpe **and** CE over fixed, with the difference CI
excluding 0, **and the sign to hold on the holdout**. It fails on every leg:

- **Return-difference** CI spans 0 (full sample [−4.4%, +5.6%]).
- **Sharpe-difference** (bootstrapped directly — the metric the gate actually claims):
  full-sample ΔSharpe rv20−fixed = **+0.178, CI [−0.145, +0.509]** → not significant; holdout
  ΔSharpe = **−0.069** (fixed ahead).
- On the **prespecified holdout, fixed exposure wins** (Sharpe 1.36 vs 1.29).
- **4% financing sensitivity** (preregistered): rv20 Sharpe falls 0.96 → 0.90 and rv20−fixed return
  goes negative — the small full-sample edge erodes under a realistic leverage cost.

Per the frozen stop rule, **stop** — no robust net mean-variance decision value was shown, and the
conclusion holds on the risk-adjusted metric itself, not just the return proxy.

## What is nonetheless true (reported honestly, not as a criterion rescue)
Over the full sample, vol-targeting delivered a large, real **risk reduction**: max drawdown
−41% → −21% and annualized vol 18.2% → 15.3%, lifting full-sample Sharpe 0.78 → 0.96. But that
benefit is **regime-dependent** — it appears in the full sample (which contains the 2020 and 2022
vol spikes, consistent with de-risking through them) but **not** in the calm bull-run holdout, where
constant exposure wins. So this looks like crisis-period drawdown control, not a general utility
improvement.

## Honest limitations of this experiment
- **Inference:** the prereg's accept test bootstrapped the *return* difference, which has low power
  for a *risk-adjusted* benefit; that gap is now closed by also bootstrapping the Sharpe difference
  (above), which likewise spans 0 — so the conclusion is tested on the metric it claims.
- **Multiplicity is not formally adjusted** (3 comparisons × 2 samples). This only makes
  significance *harder* to reach, so it strengthens the null "no robust benefit"; the one nominal
  win (rv20 vs ewma94, within the vol-targeting family) is explicitly not treated as an edge.
- **The holdout is an out-of-sample regime subsample, not an overfitting guard** — no tuning or
  model selection happened, so it cannot certify against overfitting; it is a different-regime check
  (the calm 2023–26 bull), and it favors fixed exposure.
- **Block bootstrap** (block = 20) assumes rough stationarity across a decade spanning very
  different vol regimes; treat the CIs as indicative, not exact.
- Returns/metrics use the **log-return approximation** (`w·log-return`, log-return drawdowns); fine
  for daily moves at w ≤ 1.5 and applied identically to all methods, so it does not affect the
  ranking. One symbol, one horizon, one current-vintage (retroactively adjusted) data pull.
- A drawdown-/tail-averse user might value the −41%→−21% maxDD even at equal Sharpe — but that is a
  *different* objective and would need its own preregistered criterion, not a post-hoc reinterpretation.

## Note on verification
An adversarial review workflow of this experiment (backtest look-ahead, accounting, inference, and
honesty of the write-up) was launched but **could not complete — the account hit its weekly usage
limit**, so most verify passes did not run. The reviewer agents that did run raised the leads folded
in above (return-vs-Sharpe test, multiplicity, holdout framing, financing sensitivity, log-return
convention); I addressed each directly in the main thread rather than treat the aborted run as a
clean pass. The direction is unchanged: the gate fails.

## Recommendation
The gate result says stop the modeling/infrastructure escalation. The expected-move band is an
honest, standard volatility/risk estimator with **no demonstrated proprietary forecasting edge and
no robust out-of-sample decision value** on a mean-variance objective. If drawdown control is a goal
you specifically care about, that is the one thread worth a *separate, preregistered* test; otherwise
this is the place to stop.
