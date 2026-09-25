# Preregistration — does volatility-targeting with the forecast improve a decision?

Roadmap §4 gate (per the hermes plan). Written and frozen BEFORE running the backtest. The
decisive question is not whether WIS moves by 0.00004; it is whether sizing exposure by the
volatility forecast improves **net, risk-adjusted decision utility** over doing nothing —
otherwise the risk map has no internal use and we stop.

Honesty caveat: this is not a blind preregistration (the FKO volatility-timing result is known,
and I have seen the tournament's WIS numbers). What is frozen here is the exact decision rule,
data, split, costs, metrics, and accept/stop criteria — committed before the backtest is run.

## Data
- Yahoo split/dividend-adjusted daily SPY, `range=10y`. Current-vintage adjusted history (NOT
  point-in-time vintage) — disclosed, same caveat as the calibration/tournament notes.
- The normalized input snapshot is hashed and persisted with the run.

## Strategies compared
Exposure weight `w_t` on SPY for day t (return earned on day t), decided at the close of t-1 from
information available then. σ̂ is annualized daily-log-return volatility.
1. **fixed** — `w = 1` (always fully invested; the "do nothing" benchmark).
2. **rv20** — `w_t = clip(σ_target / σ̂^{rv20}_t, w_min, w_max)`.
3. **ewma94** — same rule with `σ̂^{ewma94}`, λ = 0.94 (the hurdle model per hermes).

## Frozen parameters
- `σ_target = 0.15` annualized. `w_min = 0.0`, `w_max = 1.5` (no shorting; leverage capped at 1.5x).
- σ̂ from a 20-obs window (rv20) / EWMA(0.94); annualized ×√252. Warmup = 60 sessions.
- **Timing:** σ̂ uses returns through t-1; `w_t` is set at t-1 close and earns day-t return. No
  same-day look-ahead.
- **Costs:** 2 bps charged on `|w_t − w_{t-1}|` (turnover), deducted from the day's return.
- **Financing:** the fraction above 1.0 (when levered) pays 0 financing in the base case; a
  sensitivity run charges 4% annualized on `max(w−1, 0)`.

## Split
- Prespecified holdout = the final 30% of scored sessions (same boundary as the tournament).
  Report full sample AND holdout. A conclusion must hold on the holdout to count.

## Metrics (net of costs)
Annualized return, annualized realized vol (vs the 15% target), Sharpe (rf = 0), max drawdown,
daily 5% expected shortfall, mean turnover, and certainty-equivalent return
`CE(γ) = mean(r_net) − (γ/2)·var(r_net)` at γ ∈ {1, 3, 5}. Regime breakdown by realized-vol median.

## Inference
- Primary comparisons (prespecified, so multiplicity is bounded): rv20 vs fixed, ewma94 vs fixed,
  rv20 vs ewma94 — on daily net returns / utility, with a seeded block-bootstrap 95% CI on the
  paired difference (block = 20). Language: a CI spanning 0 means "no difference detected," never
  "equivalent" (equivalence needs a prespecified margin; not claimed here).

## Accept / stop criteria (decided now)
- **Decision-useful** iff vol-targeting (rv20 or ewma94) improves Sharpe AND certainty-equivalent
  return (γ = 3) over `fixed`, with the bootstrap CI on the difference excluding 0, AND the sign
  holds on the holdout. Realized vol should also sit closer to the 15% target than `fixed`.
- If it does **not** clear that bar, **stop** — the risk map has no demonstrated decision value and
  no further modeling or infrastructure is justified.
- rv20 vs ewma94 is reported for information; no estimator swap is claimed a "discovered edge."

## What this does NOT establish
Directional/return alpha (a vol experiment cannot). One symbol, one horizon, one data vintage.
A GARCH/Student-t/HAR/implied-vol tournament is gated behind a PASS here and its own prereg.
