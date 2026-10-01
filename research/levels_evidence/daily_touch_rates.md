# SPY realized-vol band — unconditional base rates (point-in-time)

Estimator: realized_vol band prior_close*exp(+/-k*sigma), sigma=std(ddof=1) of trailing 20 close-to-close log returns, point-in-time (data < T)
Data: SPY daily OHLC, yahoo via market proxy · snapshot `34b8ed65a2fd9c61` · sessions 2016-10-31 .. 2026-09-30
Evaluated sessions (n): **2492**

| Outcome | Rate | 95% CI (Wilson) | hits/n |
|---|---:|---|---:|
| Upper +1σ touched (session high) | 27.25% | 25.54–29.03% | 679/2492 |
| Lower −1σ touched (session low) | 26.28% | 24.59–28.05% | 655/2492 |
| Either ±1σ touched | 51.73% | 49.76–53.68% | 1289/2492 |
| Upper +2σ touched | 5.90% | 5.04–6.89% | 147/2492 |
| Lower −2σ touched | 7.99% | 6.98–9.12% | 199/2492 |
| Either ±2σ touched | 13.80% | 12.51–15.21% | 344/2492 |
| Close above +1σ | 17.42% | 15.98–18.95% | 434/2492 |
| Close below −1σ | 14.13% | 12.81–15.55% | 352/2492 |
| Close above +2σ | 3.13% | 2.52–3.89% | 78/2492 |
| Close below −2σ | 3.81% | 3.13–4.64% | 95/2492 |

Notes:
- Point-in-time: each session's band uses only close-to-close returns through the prior close.
- Touch = intraday high/low reached the level; close-beyond = the session closed past it.
- These are unconditional base rates. Regime conditioning and out-of-sample calibration vs the unconditional / vol-bucket / driftless-Brownian baselines are later steps (see prereg §7).
- Compare informally to the driftless-Brownian one-sided touch expectation: ±1σ ≈ 31.7%, ±2σ ≈ 4.6%.
