# SPY vol-band — regime conditioning & out-of-sample calibration

Snapshot `34b8ed65a2fd9c61` · 2462 events · 2016-12-13 .. 2026-09-30 · burn-in 252, bucket gate n≥30.

## A. Descriptive conditional rates by regime (full-sample partition)

_Full-sample (in-sample) partition, for description only. The honest, point-in-time edge is the out-of-sample Brier in Section B; do not cite a Section A bucket rate as validated without its Section B verdict._

**touch_u1** (+1σ upper touch)
- by vol: high=22.1% (n=820) · low=31.8% (n=821) · mid=27.5% (n=821)
- by trend: down=34.9% (n=654) · up=24.3% (n=1808)
- by gap: down=5.0% (n=517) · flat=17.8% (n=1243) · up=60.0% (n=702)

**touch_l1** (−1σ lower touch)
- by vol: high=24.5% (n=820) · low=30.2% (n=821) · mid=24.5% (n=821)
- by trend: down=31.5% (n=654) · up=24.6% (n=1808)
- by gap: down=67.3% (n=517) · flat=19.4% (n=1243) · up=8.7% (n=702)

**touch_u2** (+2σ upper touch)
- by vol: high=3.3% (n=820) · low=8.2% (n=821) · mid=6.1% (n=821)
- by trend: down=7.6% (n=654) · up=5.2% (n=1808)
- by gap: down=1.0% (n=517) · flat=2.3% (n=1243) · up=15.8% (n=702)

**touch_l2** (−2σ lower touch)
- by vol: high=6.2% (n=820) · low=10.4% (n=821) · mid=7.6% (n=821)
- by trend: down=10.6% (n=654) · up=7.1% (n=1808)
- by gap: down=26.9% (n=517) · flat=3.8% (n=1243) · up=1.7% (n=702)

**close_above_u1** (close above +1σ)
- by vol: high=14.8% (n=820) · low=19.2% (n=821) · mid=18.1% (n=821)
- by trend: down=22.2% (n=654) · up=15.7% (n=1808)
- by gap: down=2.9% (n=517) · flat=11.0% (n=1243) · up=39.3% (n=702)

**close_below_l1** (close below −1σ)
- by vol: high=13.8% (n=820) · low=15.3% (n=821) · mid=13.4% (n=821)
- by trend: down=18.0% (n=654) · up=12.8% (n=1808)
- by gap: down=37.7% (n=517) · flat=9.7% (n=1243) · up=4.8% (n=702)

**close_above_u2** (close above +2σ)
- by vol: high=2.0% (n=820) · low=4.5% (n=821) · mid=2.8% (n=821)
- by trend: down=4.1% (n=654) · up=2.7% (n=1808)
- by gap: down=0.4% (n=517) · flat=1.0% (n=1243) · up=8.8% (n=702)

**close_below_l2** (close below −2σ)
- by vol: high=3.2% (n=820) · low=5.4% (n=821) · mid=3.0% (n=821)
- by trend: down=5.2% (n=654) · up=3.4% (n=1808)
- by gap: down=12.4% (n=517) · flat=2.0% (n=1243) · up=0.9% (n=702)

## B. Out-of-sample calibration (walk-forward Brier, lower is better)

Pre-session predictors use only the prior close; **gap (at-open)** also uses the session open, so it applies once the market has opened, not the night before.

| Outcome | realized | unconditional | vol-bucket | vol×trend | gap (at-open) | Brownian |
|---|---:|---:|---:|---:|---:|---:|
| touch_u1 | 27.2% | 0.1984 | 0.1974 | 0.1928 | 0.1553 | 0.2002 |
| touch_l1 | 26.8% | 0.1965 | 0.1964 | 0.1948 | 0.1514 | 0.1986 |
| touch_u2 | 5.7% | 0.0535 | 0.0526 | 0.0519 | 0.0512 | 0.0535 |
| touch_l2 | 8.2% | 0.0757 | 0.0757 | 0.0752 | 0.0667 | 0.0769 |
| close_above_u1 | 17.6% | 0.1449 | 0.1449 | 0.1439 | 0.1261 | 0.1450 |
| close_below_l1 | 14.8% | 0.1262 | 0.1266 | 0.1257 | 0.1111 | 0.1259 |
| close_above_u2 | 2.9% | 0.0278 | 0.0275 | 0.0274 | 0.0277 | 0.0277 |
| close_below_l2 | 4.0% | 0.0387 | 0.0386 | 0.0385 | 0.0368 | 0.0390 |

**Verdict (prereg §10) — honest per outcome; gap applies only post-open:**
- touch_u1: best = **gap_at_open** (Brier 0.1553); gap (at-open) cuts Brier 22% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- touch_l1: best = **gap_at_open** (Brier 0.1514); gap (at-open) cuts Brier 23% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- touch_u2: best = **gap_at_open** (Brier 0.0512); gap (at-open) edges unconditional by only 4.2% — treat as no material edge; pre-session vol×trend beats unconditional.
- touch_l2: best = **gap_at_open** (Brier 0.0667); gap (at-open) cuts Brier 12% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- close_above_u1: best = **gap_at_open** (Brier 0.1261); gap (at-open) cuts Brier 13% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- close_below_l1: best = **gap_at_open** (Brier 0.1111); gap (at-open) cuts Brier 12% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- close_above_u2: best = **vol_trend** (Brier 0.0274); gap (at-open) edges unconditional by only 0.1% — treat as no material edge; pre-session vol×trend beats unconditional.
- close_below_l2: best = **gap_at_open** (Brier 0.0368); gap (at-open) edges unconditional by only 4.9% — treat as no material edge; pre-session vol×trend beats unconditional.
