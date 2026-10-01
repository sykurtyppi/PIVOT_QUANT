# SPY realized-vol band — unconditional base rates (point-in-time)

Estimator: realized_vol band prior_close*exp(+/-k*sigma), sigma=std(ddof=1) of trailing 20 close-to-close log returns, point-in-time (data < T)
Data: QQQ daily OHLC, yahoo via market proxy · snapshot `aafea41364824c8b` · sessions 2016-11-01 .. 2026-10-01
Evaluated sessions (n): **2492**

| Outcome | Rate | 95% CI (Wilson) | hits/n |
|---|---:|---|---:|
| Upper +1σ touched (session high) | 28.29% | 26.56–30.09% | 705/2492 |
| Lower −1σ touched (session low) | 26.73% | 25.02–28.50% | 666/2492 |
| Either ±1σ touched | 53.41% | 51.45–55.36% | 1331/2492 |
| Upper +2σ touched | 4.70% | 3.93–5.60% | 117/2492 |
| Lower −2σ touched | 7.62% | 6.65–8.73% | 190/2492 |
| Either ±2σ touched | 12.32% | 11.09–13.67% | 307/2492 |
| Close above +1σ | 17.50% | 16.05–19.04% | 436/2492 |
| Close below −1σ | 13.84% | 12.54–15.26% | 345/2492 |
| Close above +2σ | 2.81% | 2.23–3.53% | 70/2492 |
| Close below −2σ | 3.77% | 3.09–4.59% | 94/2492 |

Notes:
- Point-in-time: each session's band uses only close-to-close returns through the prior close.
- Touch = intraday high/low reached the level; close-beyond = the session closed past it.
- These are unconditional base rates. Regime conditioning and out-of-sample calibration vs the unconditional / vol-bucket / driftless-Brownian baselines are later steps (see prereg §7).
- Compare informally to the driftless-Brownian one-sided touch expectation: ±1σ ≈ 31.7%, ±2σ ≈ 4.6%.
