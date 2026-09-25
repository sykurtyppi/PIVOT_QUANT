# Result — volatility-targeting decision experiment

Ran the frozen design in `decision_experiment_prereg.md` (committed before the backtest). 10y
adjusted SPY, **completed sessions only** through 2026-09-24, 2,452 scored days; holdout = last 30%
of scored sessions (736 days). Rule: `w = clip(0.15 / ann_σ̂, 0, 1.5)`, 2 bps turnover cost, 1-day
timing, no financing. **Simple-return P&L; wealth-based drawdowns.** Input snapshot persisted.

> **Corrected 2026-09-25.** An earlier version had four defects (flagged in audit and fixed):
> it included the current *partial* trading-day bar; it computed P&L by mixing log returns with
> simple-fraction costs; it reported *log* drawdowns as wealth drawdowns (−41%/−21% were really
> ≈ −34%/−19%); and its holdout was 754 days instead of the preregistered 736. The numbers below
> use simple-return accounting, wealth drawdowns, completed sessions, and the correct split; they
> reproduce an independent recomputation exactly. **The verdict is unchanged.**

## Full sample (2,452 days)
| method | CAGR | annVol | Sharpe | maxDD (wealth) | ES(5%) | turn/day | CE(γ=3) |
|---|---|---|---|---|---|---|---|
| fixed | 15.1% | 18.1% | 0.87 | −33.7% | −2.8% | 0.000 | 10.8% |
| rv20 | 15.7% | 15.3% | **1.04** | **−18.8%** | −2.4% | 0.025 | 12.3% |
| ewma94 | 14.2% | 14.9% | 0.97 | −19.2% | −2.3% | 0.024 | 11.1% |

- rv20 − fixed return: +0.04%/yr CI [−4.92%, +4.60%] → **no difference detected**.
- **ΔSharpe rv20 − fixed: +0.166, CI [−0.151, +0.488] → not significant.**
- rv20 − ewma94 (within the vol-targeting family): return +1.36% CI [+0.35%, +2.54%]; ΔSharpe +0.068 CI [−0.000, +0.141] — not a benchmark-beating edge.

## Holdout (last 30% of scored, 736 days — ~2023–2026, a calm bull run)
| method | CAGR | annVol | Sharpe | maxDD (wealth) | CE(γ=3) |
|---|---|---|---|---|---|
| **fixed** | 22.9% | 15.4% | **1.42** | −18.8% | 18.2% |
| rv20 | 22.1% | 15.8% | 1.35 | −16.7% | 17.5% |
| ewma94 | 19.9% | 15.5% | 1.25 | −16.5% | 15.7% |

- rv20 − fixed return: −0.56% CI [−6.54%, +4.89%]; **ΔSharpe −0.069 CI [−0.510, +0.330]** → no difference detected, and fixed's point-estimate Sharpe is higher.

## Verdict against the frozen criteria: DOES NOT CLEAR THE BAR — stop escalating

The prereg required vol-targeting to improve Sharpe **and** CE over fixed, with the difference CI
excluding 0, **and the sign to hold on the holdout**. It fails on every leg:

- **Return-difference** CI spans 0 (full sample [−4.92%, +4.60%]).
- **Sharpe-difference** (bootstrapped directly — the metric the gate actually claims):
  full-sample ΔSharpe rv20−fixed = **+0.166, CI [−0.151, +0.488]** → not significant; holdout
  ΔSharpe = **−0.069, CI [−0.510, +0.330]** (fixed ahead).
- On the **prespecified holdout, fixed exposure wins** (Sharpe 1.42 vs 1.35).
- **4% financing sensitivity** (preregistered): full-sample rv20 Sharpe falls 1.04 → 0.97 and
  rv20−fixed CAGR goes negative — the small full-sample edge erodes under a realistic leverage cost.

Per the frozen stop rule, **stop** — no robust net mean-variance decision value was shown, and the
conclusion holds on the risk-adjusted metric itself, not just the return proxy.

## What is nonetheless true (reported honestly, not as a criterion rescue)
Over the full sample, vol-targeting delivered a large, real **risk reduction**: max drawdown
−33.7% → −18.8% and annualized vol 18.1% → 15.3%, lifting full-sample Sharpe 0.87 → 1.04. But that
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
- **Accounting:** P&L is computed in simple-return space (market log return → `expm1` → scaled by
  the weight; costs are simple fractions) and drawdowns are wealth-based (compounded `1+r`); returns
  are annualized as CAGR. One symbol, one horizon, one current-vintage (retroactively adjusted) pull.
- **Preregistered outputs not fully completed (disclosed):** the accept test was evaluated via the
  return and Sharpe bootstraps, but the prereg's **CE-difference bootstrap** and the **realized-vol
  median regime breakdown** were **not** produced. Because RV20 already fails the holdout-sign
  requirement, the FAIL verdict does not depend on them — but the crisis-regime explanation above is
  therefore partly post-hoc (supported by the full-vs-holdout contrast, not a formal median split).
- A drawdown-/tail-averse user might value the −34%→−19% maxDD even at equal Sharpe — but that is a
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
