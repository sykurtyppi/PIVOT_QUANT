# SPY vol-band — regime conditioning & out-of-sample calibration

Snapshot `aafea41364824c8b` · 2462 events · 2016-12-14 .. 2026-10-01 · burn-in 252, bucket gate n≥30.

## A. Descriptive conditional rates by regime (full-sample partition)

_Full-sample (in-sample) partition, for description only. The honest, point-in-time edge is the out-of-sample Brier in Section B; do not cite a Section A bucket rate as validated without its Section B verdict._

**touch_u1** (+1σ upper touch)
- by vol: high=22.2% (n=820) · low=34.7% (n=821) · mid=28.1% (n=821)
- by trend: down=33.3% (n=682) · up=26.5% (n=1780)
- by gap: down=5.5% (n=638) · flat=17.2% (n=989) · up=59.0% (n=835)

**touch_l1** (−1σ lower touch)
- by vol: high=25.4% (n=820) · low=29.1% (n=821) · mid=25.6% (n=821)
- by trend: down=31.4% (n=682) · up=24.9% (n=1780)
- by gap: down=60.5% (n=638) · flat=20.6% (n=989) · up=8.0% (n=835)

**touch_u2** (+2σ upper touch)
- by vol: high=3.2% (n=820) · low=6.3% (n=821) · mid=4.5% (n=821)
- by trend: down=5.0% (n=682) · up=4.6% (n=1780)
- by gap: down=0.5% (n=638) · flat=1.7% (n=989) · up=11.4% (n=835)

**touch_l2** (−2σ lower touch)
- by vol: high=5.2% (n=820) · low=11.7% (n=821) · mid=6.0% (n=821)
- by trend: down=7.6% (n=682) · up=7.6% (n=1780)
- by gap: down=19.4% (n=638) · flat=4.7% (n=989) · up=2.2% (n=835)

**close_above_u1** (close above +1σ)
- by vol: high=14.9% (n=820) · low=20.7% (n=821) · mid=16.9% (n=821)
- by trend: down=21.0% (n=682) · up=16.2% (n=1780)
- by gap: down=3.1% (n=638) · flat=10.8% (n=989) · up=36.4% (n=835)

**close_below_l1** (close below −1σ)
- by vol: high=13.8% (n=820) · low=14.1% (n=821) · mid=13.3% (n=821)
- by trend: down=17.9% (n=682) · up=12.1% (n=1780)
- by gap: down=31.8% (n=638) · flat=9.9% (n=989) · up=4.4% (n=835)

**close_above_u2** (close above +2σ)
- by vol: high=2.1% (n=820) · low=3.4% (n=821) · mid=2.9% (n=821)
- by trend: down=3.2% (n=682) · up=2.6% (n=1780)
- by gap: down=0.3% (n=638) · flat=0.6% (n=989) · up=7.3% (n=835)

**close_below_l2** (close below −2σ)
- by vol: high=3.0% (n=820) · low=5.1% (n=821) · mid=3.2% (n=821)
- by trend: down=4.3% (n=682) · up=3.6% (n=1780)
- by gap: down=11.1% (n=638) · flat=1.8% (n=989) · up=0.5% (n=835)

## B. Out-of-sample calibration (walk-forward Brier, lower is better)

Pre-session predictors use only the prior close; **gap (at-open)** also uses the session open, so it applies once the market has opened, not the night before.

| Outcome | realized | unconditional | vol-bucket | vol×trend | gap (at-open) | Brownian |
|---|---:|---:|---:|---:|---:|---:|
| touch_u1 | 28.4% | 0.2036 | 0.2011 | 0.1994 | 0.1536 | 0.2045 |
| touch_l1 | 27.1% | 0.1977 | 0.1979 | 0.1967 | 0.1545 | 0.1996 |
| touch_u2 | 4.7% | 0.0445 | 0.0444 | 0.0445 | 0.0427 | 0.0444 |
| touch_l2 | 7.7% | 0.0711 | 0.0705 | 0.0705 | 0.0669 | 0.0720 |
| close_above_u1 | 17.5% | 0.1443 | 0.1440 | 0.1434 | 0.1257 | 0.1444 |
| close_below_l1 | 14.5% | 0.1245 | 0.1248 | 0.1241 | 0.1118 | 0.1240 |
| close_above_u2 | 2.9% | 0.0277 | 0.0278 | 0.0279 | 0.0269 | 0.0277 |
| close_below_l2 | 3.9% | 0.0375 | 0.0375 | 0.0376 | 0.0356 | 0.0377 |

**Verdict (prereg §10) — honest per outcome; gap applies only post-open:**
- touch_u1: best = **gap_at_open** (Brier 0.1536); gap (at-open) cuts Brier 25% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- touch_l1: best = **gap_at_open** (Brier 0.1545); gap (at-open) cuts Brier 22% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- touch_u2: best = **gap_at_open** (Brier 0.0427); gap (at-open) edges unconditional by only 4.0% — treat as no material edge; pre-session conditioning does not beat unconditional (keep unconditional pre-open).
- touch_l2: best = **gap_at_open** (Brier 0.0669); gap (at-open) cuts Brier 6% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- close_above_u1: best = **gap_at_open** (Brier 0.1257); gap (at-open) cuts Brier 13% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- close_below_l1: best = **gap_at_open** (Brier 0.1118); gap (at-open) cuts Brier 10% vs unconditional — a substantial post-open edge; pre-session vol×trend beats unconditional.
- close_above_u2: best = **gap_at_open** (Brier 0.0269); gap (at-open) edges unconditional by only 3.1% — treat as no material edge; pre-session conditioning does not beat unconditional (keep unconditional pre-open).
- close_below_l2: best = **gap_at_open** (Brier 0.0356); gap (at-open) edges unconditional by only 5.0% — treat as no material edge; pre-session conditioning does not beat unconditional (keep unconditional pre-open).
