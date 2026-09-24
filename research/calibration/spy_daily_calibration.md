# SPY daily expected-move calibration — point-in-time, 10y

Roadmap §3 (first cut). Answers: *are the ±1σ / ±2σ daily expected-move bands calibrated,
out-of-sample, as they would have been known each morning?*

## Method

For each session, the daily band is reconstructed from **prior data only**: anchor = prior
session close, `sigma` = sample std (n−1) of the last 20 close-to-close **log** returns ending
at the prior session, bands = `anchor · exp(±k·sigma)` (log-symmetric, sessions-remaining = 1).
This is the daily-horizon math of `src/math/MultiHorizonVolatilityLevels.js`, verified against
the engine in `tests/forecast/calibration.test.js`. The realized close is scored for ±1σ / ±2σ
containment. CIs are a seeded circular **block bootstrap** (block = 20) to respect serial
dependence. Data is Yahoo split/dividend-**adjusted** daily closes.

Reproduce: `node scripts/run_calibration.mjs --symbol SPY --range 10y --window 20`

## Result (run 2026-09-24)

- Data: **2,513 adjusted sessions, 2016-09-26 → 2026-09-24**; 2,492 scored forecast/outcome pairs.
- `data_snapshot_sha256: 509b902a62e7f08927f4cbfdb6a7c640a12987b118cd3d151bfb4c645fa6b5a8`
  (Yahoo re-adjusts history retroactively, so this is current-vintage, not true point-in-time —
  the forward ledger accumulates real vintages from today.)

| Band | Empirical | Nominal | Error | 95% CI |
|---|---|---|---|---|
| ±1σ | **68.7%** | 68.3% | +0.4pp | [66.5%, 70.7%] |
| ±2σ | **93.0%** | 95.5% | −2.4pp | [91.8%, 94.2%] |

- **1σ breach asymmetry:** upper 439 vs lower 341 (+3.9pp) — closes clear +1σ more often than −1σ.
- **By realized-vol regime (median split):** low-vol 66.6% / 90.9% vs high-vol 70.8% / 95.2%.

## Reading

- **±1σ is well calibrated** — empirical coverage sits on the nominal target and its CI contains
  68.3%. Usable as an expected-move / risk-map input.
- **±2σ materially undercovers** — its CI [91.8%, 94.2%] excludes the 95.5% target. The tails are
  fatter than the log-normal band assumes: the 2σ band is breached ~7% of the time, not ~4.5%.
- **The 2σ shortfall concentrates in low-vol regimes** (90.9% vs 95.2%) — in calm markets a 20-day
  RV underestimates jump risk. This is the strongest signal for the roadmap's §5 candidates
  (fat-tailed / asymmetric bands, event-risk overlays) and argues against advertising a 95% band.

## Not yet (next slices)

Weekly/monthly horizons (overlapping observations → block/purged inference); weighted interval
score vs baselines (ATR, prior range, EWMA/HAR); event-day conditioning (CPI/FOMC/OPEX);
train/validate/holdout separation before any estimator selection.
