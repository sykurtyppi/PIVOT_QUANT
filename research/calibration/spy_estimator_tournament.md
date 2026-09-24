# Does the RV band beat trivial baselines? — SPY estimator tournament

Roadmap §4. Answers the question that decides whether any of the ledger work matters:
**does the 20-day realized-volatility expected-move band actually forecast the daily move
better than trivial baselines, or is it a commodity any vol calc reproduces?**

## Method

Point-in-time, log-return space. Each morning, five volatility estimators build a ±1σ / ±2σ
band from prior returns only; the realized next-day return is scored with the **Weighted Interval
Score** (WIS) — the standard proper scoring rule for interval forecasts (rewards sharpness,
penalizes miscoverage; lower is better). WIS is in return units, so a 2016 SPY (~180) and a 2026
SPY (~760) contribute comparably. Data is Yahoo split/dividend-adjusted daily closes. A block
bootstrap gives a 95% CI on the per-day WIS *difference* vs rv20.

Estimators: `rv20` (our method), `rv60`, `ewma94` (RiskMetrics λ=0.94), `expanding` (all history —
climatology), `yesterday` (|last return|, naive).

Reproduce: `node scripts/run_tournament.mjs --symbol SPY --range 10y --holdout-frac 0.3`

## Result (run 2026-09-24; 2,513 sessions, 2016–2026)

Mean WIS (lower is better), full sample (2,452 days):

| Method | WIS | cov ±1σ | cov ±2σ | mean ±2σ width |
|---|---|---|---|---|
| ewma94 | 0.00392 | 71.4% | 94.3% | 0.0390 |
| **rv20 (ours)** | **0.00394** | 68.6% | 93.0% | 0.0379 |
| rv60 | 0.00405 | 71.6% | 93.7% | 0.0396 |
| expanding | 0.00419 | 75.8% | 93.7% | 0.0405 |
| yesterday | 0.00478 | 49.8% | 69.1% | 0.0296 |

rv20 vs each baseline (per-day WIS diff, 95% block-bootstrap CI; >0 means rv20 better):

| vs | full sample | holdout (last 30%) |
|---|---|---|
| yesterday | **rv20 better** [+0.00074, +0.00096] | **rv20 better** [+0.00067, +0.00096] |
| expanding | **rv20 better** [+0.00010, +0.00048] | **rv20 better** [+0.00004, +0.00030] |
| rv60 | rv20 better [+0.00003, +0.00022] | tie (CI spans 0) |
| ewma94 | tie (CI spans 0) | tie (CI spans 0) |

## Reading

1. **The band is a real, useful volatility forecast, not a do-nothing commodity.** It decisively
   and significantly beats the naive baseline (`yesterday`, which badly undercovers at ~50%/69%)
   and the static climatology (`expanding`, too wide and sluggish). Volatility clusters, and
   tracking it with a rolling window adds genuine value over ignoring it.
2. **But it carries no edge over a textbook vol model.** rv20 is statistically indistinguishable
   from `ewma94` (EWMA is a hair better on the point estimate, inside the noise). The specific
   "20-day simple RV" choice is arbitrary; a standard EWMA is equally good.

## What this means for the product

- The expected-move band is legitimate as an **expected-move / risk-map reference** — worth
  running a labeled forward shadow to confirm it holds live.
- It is **not a proprietary edge or an alpha source.** It is a competent, standard realized-vol
  band. Building a trading system expecting it to beat algorithmic execution would repeat the
  prior mistake. Position and use it as a commodity risk tool, honestly labeled.
- If marginal improvement is ever wanted, swap rv20 → ewma94 for free (equal-or-slightly-better,
  no added complexity). No estimator here justifies more machinery.
