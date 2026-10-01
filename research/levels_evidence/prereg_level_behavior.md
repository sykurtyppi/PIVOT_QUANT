# Preregistration — SPY level-behavior evidence layer

**Status:** FROZEN DESIGN (written before computing any statistic). Do not edit
definitions after the first run; corrections go in an amendment section with a date.

**Author / date:** 2026-09-30.
**Purpose:** Turn `/levels` from a level calculator into an evidence system — "how
does SPY historically behave at these levels?" — without ever publishing a naked
or model-implied probability. This document freezes every outcome definition,
the estimator, the point-in-time method, the sample gating, and the baselines
*before* any number is produced, so the result cannot be reverse-fit to look good.

This mirrors the discipline that retired the ML signal and the vol-targeting gate
(see `research/calibration/`, [[forecast_band_conclusion]]): descriptive,
point-in-time, sample-gated, honestly-labelled — not a predictor dressed up as one.

---

## 1. Scope (v1)

Levels studied in v1: the **realized-volatility bands only** — `+1σ, +2σ, −1σ, −2σ`
of the **daily** horizon from `src/math/MultiHorizonVolatilityLevels.js`
(`anchor·exp(±kσ)`, σ = sample std of the trailing 20-session close-to-close log
returns, anchored to the prior session close, look-ahead-safe).

Explicitly **out of scope for v1** (do not compute or display probabilities for):
classic/Fib/Camarilla pivots, weekly/monthly horizons, options-implied expected
move, any ML classifier, any "probability of a profitable trade." Pivots stay
reference lines. These may enter later passes *only* under the same discipline.

## 2. The estimator being evaluated

For historical session `T`, the band levels are computed **point-in-time**: using
only close-to-close log returns available **through the close of session T−1**,
exactly as the live engine does. No data from session T or later touches the level.
This is a walk-forward, not an in-sample fit.

## 3. Outcome taxonomy — FROZEN definitions

Let `U1,U2` be the upper `+1σ,+2σ` levels and `L1,L2` the lower, all for session T.
"level price" `P` denotes the specific level under study.

**Daily outcomes (from the raw daily High/Low/Close of session T):**
- **Touch (upper):** `High_T ≥ U`. **Touch (lower):** `Low_T ≤ L`.
- **Close-beyond (upper):** `Close_T ≥ U`. **(lower):** `Close_T ≤ L`.
- **Either-side ±kσ touch:** touch of the upper OR lower kσ level in session T.

**Intraday outcomes (from 1-minute bars of session T; require the minute series):**
- **First side touched:** of `{U1, L1}`, the level whose first crossing minute
  timestamp is earlier. Undetermined if neither is touched.
- **Time to touch:** minutes from the first RTH bar (09:30 ET) to the first bar
  with `high ≥ U` (or `low ≤ L`). "Not touched" otherwise.
- **Post-touch rejection / acceptance @ N ∈ {15, 30, 60} min.** After the first
  touch of level `P` at minute `t*`, over the window `(t*, t*+N]`:
  - threshold `R = 0.20%` of `P` (frozen).
  - **Rejection@N:** price trades `≥ R` on the *anchor side* of `P`
    (back toward the prior close) **before** it trades `≥ R` on the *beyond side*.
  - **Acceptance@N:** price trades `≥ R` on the *beyond side* of `P` **before**
    `≥ R` on the anchor side.
  - **Undetermined@N:** neither `R` threshold is reached within N minutes.
    (Rejection, Acceptance, Undetermined are mutually exclusive and exhaustive.)
- **MFE / MAE after touch:** over `(t*, session close]`, the maximum *beyond-side*
  excursion (MFE) and maximum *anchor-side* excursion (MAE) from `P`, in percent.

Every published number cites the outcome name **and** its definition above.

## 4. Data & samples

- **Daily outcomes:** SPY daily OHLC, ~10 years (Yahoo `range=10y` ≈ 2,513 sessions;
  reconcile splits/dividends; use unadjusted OHLC for touch geometry, note the
  caveat). Adequate for touch / close-beyond.
- **Intraday outcomes:** SPY 1-minute bars. Readily available now: runtime
  `data/pivot_events.sqlite` `bar_data` (~6 months, Apr–Sep 2026, ~125 sessions).
  Larger sources for a later pass: the macbook_final salvage (2025-03 → 2026-09,
  ~1.5y) and the T9 iVolatility 1-min (2016+). **v1 intraday estimates carry a
  small sample and will frequently abstain** — this is expected and honest.

## 5. Statistics & abstention — FROZEN

- Rates reported as point estimate + **Wilson 95% interval**.
- **Sample gating:** publish an unconditional estimate only if `n ≥ 100`; a
  per-regime estimate only if bucket `n ≥ 30`. Below threshold → **abstain**
  ("insufficient comparable sessions"), show nothing rather than a fragile number.
- **Point-in-time everywhere:** any conditional estimate shown for session T uses
  only sessions `< T` (trailing, horizon-embargoed), reusing the leak-free pattern
  in `scripts/levels_product/hold_engine.py` on `feat/multihorizon-volatility-levels`.

## 6. Regime segmentation — pass 2 (buckets frozen now)

- **Volatility:** tercile of the point-in-time RV(20) used for the band (low/mid/high).
- **Gap:** `(Open_T − Close_{T−1}) / Close_{T−1}` → down `< −0.3%`, flat, up `> +0.3%`.
- **Trend:** `Close_{T−1}` vs its 50-session SMA (above / below).
- **Time-of-day** (intraday only): first 30 min / midday / final hour.
Confluence (a σ level overlapping a pivot within 0.15%) is a candidate bucket only
after it is shown to add information beyond distance + volatility (§7).

## 7. Baselines & calibration — FROZEN

The conditional (regime) estimate must beat, out-of-sample (walk-forward Brier
score on the touch/close-beyond events), all of:
1. **Unconditional** trailing rate.
2. **Volatility-bucket** rate.
3. **Theoretical driftless-Brownian one-sided touch:** `2·(1−Φ(k))` →
   `±1σ ≈ 31.7%`, `±2σ ≈ 4.6%`. (A first-cut expectation, not truth.)
"Adds information" = lower rolling out-of-sample Brier than the baseline. If a
conditioning variable (e.g. confluence) does not beat these, it is **not** shown
as changing the estimate.

## 8. What v1 surfaces in `/levels`

Secondary, expandable context under a σ level — never a bare % on the line:
> Historical: session high reached **+1σ** before the close in **27%** of
> comparable sessions (679 / 2,491) · 95% CI 26–29% · 20-session RV band,
> point-in-time · [what "reached" means]

with an explicit **"insufficient data"** state whenever §5 gating fails.

## 9. Non-goals (do not do, per the review and prior lessons)

- No normal-distribution "68%" containment label.
- No probability on classic/Fib/Camarilla lines because the lines exist.
- No ML confidence numbers; the retired RF signal is not reopened here — this layer
  is descriptive statistics, not a trained predictor.
- No "probability of success" without a defined trade, stop, target, horizon.
- No single rate that ignores regime once §7 shows regime matters.

## 10. Success / stop criteria

- **Ship the descriptive layer** if the point-in-time pipeline reproduces
  deterministically and the daily touch/close-beyond estimates are stable with
  CIs (this value does not depend on predictive lift — knowing the base rates is
  useful on its own).
- **Only claim regime-conditional value** if §7 shows the conditional estimate
  beats all three baselines out-of-sample. If it does not, keep the unconditional
  base rates and drop the regime claim — do not overfit buckets to reach a story.

## 11. Build order (after this prereg is approved)

1. Point-in-time daily event table (bands per session T from data `< T` + realized
   touch/close-beyond). Deterministic, reproducible; offline (Mac Mini).
2. Unconditional daily touch/close-beyond rates + Wilson CIs, walk-forward.
3. Intraday post-touch outcomes on the available 1-min window, heavily gated.
4. Regime segmentation + the §7 baseline/Brier comparison.
5. Surface in `/levels` as expandable secondary context with abstention.

## 12. Amendments

### 2026-10-01 — A1: multi-instrument daily layer (QQQ)

Extends the **daily** layer (touch and close-beyond; the §3 *daily* outcomes only)
from SPY to additional liquid index ETFs, beginning with **QQQ**, under the
IDENTICAL discipline and frozen definitions of this prereg — point-in-time bands
from the same `MultiHorizonVolatilityLevels` engine, Wilson CIs, §5 sample gating,
the §6 regime buckets, the §7 walk-forward Brier verdict, and §8 surfacing. **No
outcome, threshold, bucket, or estimator definition changes.**

- **Daily only.** The INTRADAY layer (§3 intraday: acceptance / rejection /
  first-side / time-to-touch) stays **SPY-only**. No true-OHLC 1-minute source
  exists for QQQ: the T9 iVolatility 1-minute lake is last/bid/ask quote snapshots,
  not OHLC bars, so it cannot drive the frozen `High_T ≥ U` touch rule. QQQ intraday
  **abstains** until a true-OHLC 1-minute feed is available.
- **Per instrument, independently.** Each instrument gets its own point-in-time
  daily snapshot and its own §7 verdict — gap conditioning must earn its
  out-of-sample edge *separately* for QQQ; it is not assumed from SPY.
- Pivots remain reference-only (§1). Per-instrument artifacts live under
  `research/levels_evidence/<symbol>/` (SPY stays at the root for compatibility).
