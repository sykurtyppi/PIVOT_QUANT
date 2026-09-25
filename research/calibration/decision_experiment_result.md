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
excluding 0, **and the sign to hold on the holdout**. It fails: the return-difference CI spans 0
on the full sample, and on the prespecified holdout fixed exposure actually wins (Sharpe 1.36 vs
1.29). Per the frozen stop rule, **stop** — no robust net mean-variance decision value was shown
out-of-sample.

## What is nonetheless true (reported honestly, not as a criterion rescue)
Over the full sample, vol-targeting delivered a large, real **risk reduction**: max drawdown
−41% → −21% and annualized vol 18.2% → 15.3%, lifting full-sample Sharpe 0.78 → 0.96. But that
benefit is **regime-dependent** — it comes from de-risking through the 2020 and 2022 vol spikes in
the full sample, and it does **not** appear in the calm bull-run holdout, where constant exposure
wins. So this is crisis-period drawdown control, not a general utility improvement.

## Honest limitations of this experiment
- **Inference power:** the prereg bootstrapped the *return* difference, which has low power to
  detect a *risk-adjusted* benefit (vol-targeting's edge is in vol/drawdown, not mean return). A
  cleaner test bootstraps the Sharpe/utility difference. But the holdout point estimates also favor
  fixed, so the "no robust benefit" conclusion does not depend on this.
- One symbol, one horizon, one current-vintage (retroactively adjusted) data pull.
- A drawdown-/tail-averse user might value the −41%→−21% maxDD even at equal Sharpe — but that is a
  *different* objective and would need its own preregistered criterion, not a post-hoc reinterpretation.

## Recommendation
The gate result says stop the modeling/infrastructure escalation. The expected-move band is an
honest, standard volatility/risk estimator with **no demonstrated proprietary forecasting edge and
no robust out-of-sample decision value** on a mean-variance objective. If drawdown control is a goal
you specifically care about, that is the one thread worth a *separate, preregistered* test; otherwise
this is the place to stop.
