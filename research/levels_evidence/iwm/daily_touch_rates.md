# SPY realized-vol band — unconditional base rates (point-in-time)

Estimator: realized_vol band prior_close*exp(+/-k*sigma), sigma=std(ddof=1) of trailing 20 close-to-close log returns, point-in-time (data < T)
Data: IWM daily OHLC, yahoo via market proxy · snapshot `daf75fa6970f9169` · sessions 2016-11-01 .. 2026-10-01
Evaluated sessions (n): **2492**

| Outcome | Rate | 95% CI (Wilson) | hits/n |
|---|---:|---|---:|
| Upper +1σ touched (session high) | 29.57% | 27.82–31.40% | 737/2492 |
| Lower −1σ touched (session low) | 29.82% | 28.05–31.64% | 743/2492 |
| Either ±1σ touched | 57.38% | 55.43–59.31% | 1430/2492 |
| Upper +2σ touched | 5.18% | 4.37–6.12% | 129/2492 |
| Lower −2σ touched | 7.83% | 6.83–8.95% | 195/2492 |
| Either ±2σ touched | 12.92% | 11.66–14.30% | 322/2492 |
| Close above +1σ | 16.09% | 14.70–17.59% | 401/2492 |
| Close below −1σ | 15.29% | 13.93–16.76% | 381/2492 |
| Close above +2σ | 2.85% | 2.26–3.58% | 71/2492 |
| Close below −2σ | 3.53% | 2.88–4.33% | 88/2492 |

Notes:
- Point-in-time: each session's band uses only close-to-close returns through the prior close.
- Touch = intraday high/low reached the level; close-beyond = the session closed past it.
- These are unconditional base rates. Regime conditioning and out-of-sample calibration vs the unconditional / vol-bucket / driftless-Brownian baselines are later steps (see prereg §7).
- Compare informally to the driftless-Brownian one-sided touch expectation: ±1σ ≈ 31.7%, ±2σ ≈ 4.6%.
