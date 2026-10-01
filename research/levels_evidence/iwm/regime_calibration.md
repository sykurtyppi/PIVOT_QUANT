# SPY vol-band — regime conditioning & out-of-sample calibration

Snapshot `daf75fa6970f9169` · 2462 events · 2016-12-14 .. 2026-10-01 · burn-in 252, bucket gate n≥30.

## A. Descriptive conditional rates by regime (full-sample partition)

_Full-sample (in-sample) partition, for description only. The honest, point-in-time edge is the out-of-sample Brier in Section B; do not cite a Section A bucket rate as validated without its Section B verdict._

**touch_u1** (+1σ upper touch)
- by vol: high=25.9% (n=820) · low=31.7% (n=821) · mid=30.9% (n=821)
- by trend: down=33.9% (n=911) · up=26.9% (n=1551)
- by gap: down=7.6% (n=645) · flat=18.7% (n=965) · up=58.3% (n=852)

**touch_l1** (−1σ lower touch)
- by vol: high=24.4% (n=820) · low=33.7% (n=821) · mid=31.9% (n=821)
- by trend: down=32.3% (n=911) · up=28.7% (n=1551)
- by gap: down=62.8% (n=645) · flat=25.4% (n=965) · up=10.4% (n=852)

**touch_u2** (+2σ upper touch)
- by vol: high=2.4% (n=820) · low=7.3% (n=821) · mid=5.4% (n=821)
- by trend: down=5.4% (n=911) · up=4.8% (n=1551)
- by gap: down=1.2% (n=645) · flat=2.4% (n=965) · up=10.9% (n=852)

**touch_l2** (−2σ lower touch)
- by vol: high=4.3% (n=820) · low=10.8% (n=821) · mid=8.5% (n=821)
- by trend: down=8.8% (n=911) · up=7.4% (n=1551)
- by gap: down=20.2% (n=645) · flat=5.1% (n=965) · up=1.8% (n=852)

**close_above_u1** (close above +1σ)
- by vol: high=14.0% (n=820) · low=16.8% (n=821) · mid=17.2% (n=821)
- by trend: down=18.7% (n=911) · up=14.4% (n=1551)
- by gap: down=3.1% (n=645) · flat=9.2% (n=965) · up=33.5% (n=852)

**close_below_l1** (close below −1σ)
- by vol: high=12.7% (n=820) · low=16.9% (n=821) · mid=16.3% (n=821)
- by trend: down=18.2% (n=911) · up=13.6% (n=1551)
- by gap: down=32.9% (n=645) · flat=12.5% (n=965) · up=5.2% (n=852)

**close_above_u2** (close above +2σ)
- by vol: high=1.5% (n=820) · low=3.7% (n=821) · mid=3.3% (n=821)
- by trend: down=2.4% (n=911) · up=3.0% (n=1551)
- by gap: down=0.3% (n=645) · flat=1.1% (n=965) · up=6.6% (n=852)

**close_below_l2** (close below −2σ)
- by vol: high=2.2% (n=820) · low=4.9% (n=821) · mid=3.7% (n=821)
- by trend: down=4.2% (n=911) · up=3.2% (n=1551)
- by gap: down=9.0% (n=645) · flat=2.5% (n=965) · up=0.7% (n=852)

## B. Out-of-sample calibration (walk-forward Brier, lower is better)

Pre-session predictors use only the prior close; **gap (at-open)** also uses the session open, so it applies once the market has opened, not the night before.

| Outcome | realized | unconditional | vol-bucket | vol×trend | gap (at-open) | Brownian |
|---|---:|---:|---:|---:|---:|---:|
| touch_u1 | 29.4% | 0.2078 | 0.2075 | 0.2064 | 0.1642 | 0.2082 |
| touch_l1 | 30.5% | 0.2122 | 0.2112 | 0.2111 | 0.1708 | 0.2120 |
| touch_u2 | 5.0% | 0.0473 | 0.0470 | 0.0470 | 0.0461 | 0.0473 |
| touch_l2 | 7.9% | 0.0730 | 0.0726 | 0.0726 | 0.0679 | 0.0740 |
| close_above_u1 | 16.2% | 0.1356 | 0.1360 | 0.1357 | 0.1196 | 0.1355 |
| close_below_l1 | 15.7% | 0.1326 | 0.1328 | 0.1324 | 0.1204 | 0.1324 |
| close_above_u2 | 2.8% | 0.0269 | 0.0268 | 0.0269 | 0.0263 | 0.0269 |
| close_below_l2 | 3.6% | 0.0349 | 0.0349 | 0.0351 | 0.0341 | 0.0351 |

**Verdict (prereg §10) — honest per outcome; gap applies only post-open:**
- touch_u1: best = **gap_at_open** (Brier 0.1642); gap (at-open) cuts Brier 21% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- touch_l1: best = **gap_at_open** (Brier 0.1708); gap (at-open) cuts Brier 20% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- touch_u2: best = **gap_at_open** (Brier 0.0461); gap (at-open) edges unconditional by only 2.6% — treat as no material edge; pre-session vol×trend beats unconditional.
- touch_l2: best = **gap_at_open** (Brier 0.0679); gap (at-open) cuts Brier 7% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- close_above_u1: best = **gap_at_open** (Brier 0.1196); gap (at-open) cuts Brier 12% vs unconditional — a substantial post-open edge; pre-session conditioning does not beat unconditional (keep unconditional pre-open).
- close_below_l1: best = **gap_at_open** (Brier 0.1204); gap (at-open) cuts Brier 9% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- close_above_u2: best = **gap_at_open** (Brier 0.0263); gap (at-open) edges unconditional by only 2.2% — treat as no material edge; pre-session conditioning does not beat unconditional (keep unconditional pre-open).
- close_below_l2: best = **gap_at_open** (Brier 0.0341); gap (at-open) edges unconditional by only 2.5% — treat as no material edge; pre-session conditioning does not beat unconditional (keep unconditional pre-open).
