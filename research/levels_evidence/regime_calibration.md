# SPY vol-band — regime conditioning & out-of-sample calibration

Snapshot `34b8ed65a2fd9c61` · 2462 events · 2016-12-13 .. 2026-09-30 · burn-in 252, bucket gate n≥30.

## A. Descriptive conditional touch rates (full-sample partition)

**touch_u1** (upper +1σ touch)
- by vol: high=22.1% (n=820) · low=31.8% (n=821) · mid=27.5% (n=821)
- by trend: down=34.9% (n=654) · up=24.3% (n=1808)
- by gap: down=5.0% (n=517) · flat=17.8% (n=1243) · up=60.0% (n=702)

**touch_l1** (lower −1σ touch)
- by vol: high=24.5% (n=820) · low=30.2% (n=821) · mid=24.5% (n=821)
- by trend: down=31.5% (n=654) · up=24.6% (n=1808)
- by gap: down=67.3% (n=517) · flat=19.4% (n=1243) · up=8.7% (n=702)

## B. Out-of-sample calibration (walk-forward Brier, lower is better)

Pre-session predictors use only the prior close; **gap (at-open)** also uses the session open, so it applies once the market has opened, not the night before.

| Outcome | realized | unconditional | vol-bucket | vol×trend | gap (at-open) | Brownian |
|---|---:|---:|---:|---:|---:|---:|
| touch_u1 | 27.2% | 0.1984 | 0.1974 | 0.1928 | 0.1553 | 0.2002 |
| touch_l1 | 26.8% | 0.1965 | 0.1964 | 0.1948 | 0.1514 | 0.1986 |

**Verdict (prereg §10):**
- touch_u1: best predictor = **gap_at_open** (Brier 0.1553). Pre-session (vol×trend) beats unconditional out-of-sample. The **gap (at-open)** bucket beats it substantially — the strongest honest conditioning available once the session has opened.
- touch_l1: best predictor = **gap_at_open** (Brier 0.1514). Pre-session (vol×trend) beats unconditional out-of-sample. The **gap (at-open)** bucket beats it substantially — the strongest honest conditioning available once the session has opened.
