# SPY daily expected-move calibration — point-in-time, 10y

Roadmap §3 (first cut). Answers: *are the ±1σ / ±2σ daily expected-move bands calibrated,
out-of-sample, as they would have been known each morning?*

> **Correction (2026-09-24).** An earlier version of this note compared empirical coverage to
> the **asymptotic-normal** targets (68.3% / 95.5%) and concluded the ±2σ band showed "fat
> tails." That was wrong: the band uses `sigma` estimated from only 20 observations, so the
> correct null for a ±kσ̂ band is a finite-sample Student-t(≈19), **not** a standard normal.
> Against the right benchmark the data does **not** establish fat tails. Thanks to the
> adversarial audit of PR #69 for catching this.

## Method

For each session the daily band is reconstructed from **prior data only**: anchor = prior
session close, `sigma` = sample std (n−1) of the last 20 close-to-close **log** returns ending
at the prior session, bands = `anchor · exp(±k·sigma)` (log-symmetric, sessions-remaining = 1).
This is the daily-horizon math of `src/math/MultiHorizonVolatilityLevels.js`, verified against
the engine in `tests/forecast/calibration.test.js`. The realized close is scored for ±1σ / ±2σ
containment. CIs are a seeded circular **block bootstrap** (block = 20). The benchmark is the
**estimator-aware null**: a Monte Carlo of the exact rolling 20-obs estimator under IID Gaussian
returns (`simulateGaussianNullCoverage`), which reproduces the analytic t₁₉ coverage.

Reproduce: `node scripts/run_calibration.mjs --symbol SPY --range 10y --window 20`
(persists the normalized input snapshot under `research/calibration/snapshots/`.)

## Result (run 2026-09-24)

- Data: **2,513 adjusted sessions, 2016-09-26 → 2026-09-24**; 2,492 scored pairs.
- Yahoo re-adjusts history retroactively, so the exact `data_snapshot_sha256` differs run to run;
  the normalized input is now **persisted per run** as provenance (a hash alone is not enough).

| Band | Empirical | Estimator-aware null (t₁₉) | Asymptotic-normal ref | 95% CI | vs null |
|---|---|---|---|---|---|
| ±1σ̂ | **68.7%** | 67.0% | 68.3% | [66.5%, 70.7%] | +1.7pp |
| ±2σ̂ | **93.0%** | 94.0% | 95.5% | [91.8%, 94.2%] | −1.0pp |

- **±2σ̂ CI contains the estimator-aware null (94.0%)** → consistent with Gaussian + vol-estimation
  noise; **does not** establish fat tails.
- **±1σ̂** sits ~1.7pp above its finite-sample null (CI contains 67.0%) — broadly calibrated.
- 1σ breach asymmetry: upper 439 vs lower 341 (+3.9pp).
- By realized-vol regime (median split): low-vol 66.6% / 90.9% vs high-vol 70.8% / 95.2%.

## Reading

- The daily bands are **broadly consistent with an IID-Gaussian-with-estimated-vol null**. There
  is no clean evidence of fat tails at the daily horizon *from this experiment* — earlier claim
  retracted.
- What the study **cannot** yet separate: fat tails vs vol-estimation uncertainty vs drift. That
  needs estimator-aware nulls for competing models, not containment-vs-normal.
- The regime spread (low-vol 2σ ≈ 90.9%) is a lead, not a conclusion — it may itself be an
  estimation-noise artifact of splitting on the estimated sigma. Treat as hypothesis-generating.

## Not yet (next slices)

Estimator tournament (ATR, prior range, EWMA, HAR, Student-t) judged by **weighted interval score**
under purged validation/holdout, each against its own estimator-aware null; weekly/monthly with
overlap-aware inference; event-day conditioning (CPI/FOMC/OPEX). No estimator selection before a
locked train/validate/holdout split.
