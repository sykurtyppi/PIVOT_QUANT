import {
  blockBootstrapMeanCI,
  intervalScore,
  runTournament,
  TOURNAMENT_METHODS,
  weightedIntervalScore,
} from '../../src/forecast/estimatorTournament.js';

describe('interval score', () => {
  test('a realized value inside the interval scores only the width', () => {
    expect(intervalScore(-1, 1, 0, 0.05)).toBe(2); // width only, no penalty
  });
  test('a miss below the lower bound adds a coverage penalty', () => {
    const alpha = 0.05;
    // width 2 + (2/alpha)*(lower - y) = 2 + 40*(-1 - (-3)) = 2 + 80
    expect(intervalScore(-1, 1, -3, alpha)).toBeCloseTo(2 + (2 / alpha) * 2, 9);
  });
});

describe('weightedIntervalScore', () => {
  test('rewards a well-sized band over a far-too-tight one', () => {
    const y = 0.005;
    const good = weightedIntervalScore(0, 0.01, y); // y within 1σ
    const tooTight = weightedIntervalScore(0, 0.0001, y); // y is 50σ out -> huge penalty
    expect(good).toBeLessThan(tooTight);
  });
  test('also penalizes a needlessly huge band (sharpness)', () => {
    const y = 0.005;
    const tight = weightedIntervalScore(0, 0.01, y);
    const huge = weightedIntervalScore(0, 1.0, y); // absurdly wide, always covers
    expect(tight).toBeLessThan(huge);
  });
});

describe('runTournament', () => {
  // Deterministic pseudo-returns with mild volatility clustering.
  function returns(n) {
    const out = [];
    let vol = 0.01;
    for (let i = 0; i < n; i += 1) {
      vol = 0.9 * vol + 0.1 * (0.005 + 0.01 * ((i * 13) % 7) / 7);
      out.push(vol * (((i * 31) % 11) - 5) / 5);
    }
    return out;
  }

  test('produces finite WIS, valid coverage, and correct n for every method', () => {
    const r = runTournament(returns(600), { warmup: 60 });
    expect(r.n).toBe(600 - 60);
    for (const m of TOURNAMENT_METHODS) {
      const s = r.perMethod[m];
      expect(Number.isFinite(s.meanWis)).toBe(true);
      expect(s.coverage1).toBeGreaterThanOrEqual(0);
      expect(s.coverage2).toBeLessThanOrEqual(1);
      expect(s.meanWidth2).toBeGreaterThan(0);
      expect(r.perDayWis[m]).toHaveLength(r.n);
    }
  });

  test('the `from` bound restricts scoring to a holdout segment', () => {
    const r = runTournament(returns(600), { warmup: 60, from: 400 });
    expect(r.n).toBe(600 - 400);
    expect(r.indices[0]).toBe(400);
  });

  test('rejects a too-short series', () => {
    expect(() => runTournament([0.01, 0.02], { warmup: 60 })).toThrow(/at least/);
  });
});

describe('blockBootstrapMeanCI', () => {
  test('is deterministic under a seed and brackets the mean', () => {
    const diffs = Array.from({ length: 300 }, (_, i) => Math.sin(i) * 0.001);
    const a = blockBootstrapMeanCI(diffs, { seed: 3, iters: 400 });
    const b = blockBootstrapMeanCI(diffs, { seed: 3, iters: 400 });
    expect(a).toEqual(b);
    expect(a.lo).toBeLessThanOrEqual(a.mean);
    expect(a.hi).toBeGreaterThanOrEqual(a.mean);
  });
});
