import {
  blockBootstrapCI,
  dailyCoverage,
  logReturns,
  sampleStd,
  simulateGaussianNullCoverage,
  summarizeCoverage,
} from '../../src/forecast/calibration.js';
import { MultiHorizonVolatilityLevels } from '../../src/index.js';

// Contiguous weekday bars with a deterministic but varying return path.
function weekdayBars(count, start = '2024-01-01', initial = 400) {
  const bars = [];
  const cursor = new Date(`${start}T00:00:00.000Z`);
  let close = initial;
  let i = 0;
  while (bars.length < count) {
    const dow = cursor.getUTCDay();
    if (dow !== 0 && dow !== 6) {
      bars.push({ timestamp: `${cursor.toISOString().slice(0, 10)}T20:00:00.000Z`, close: Number(close.toFixed(6)) });
      // varying returns so sigma is non-degenerate
      close *= Math.exp((((i * 7) % 11) - 5) / 1000);
      i += 1;
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return bars;
}

describe('sampleStd / logReturns', () => {
  test('sampleStd uses n-1 and logReturns are close-to-close', () => {
    expect(sampleStd([1, 2, 3])).toBeCloseTo(1, 12);
    expect(logReturns([{ close: 100 }, { close: 110 }])[0]).toBeCloseTo(Math.log(1.1), 12);
  });
});

describe('dailyCoverage', () => {
  test('produces one result per scored session with consistent containment flags', () => {
    const bars = weekdayBars(60);
    const results = dailyCoverage(bars, { window: 20 });
    expect(results.length).toBe(60 - 21); // sessions from index window+1..end
    for (const r of results) {
      expect(r.within2 || !r.within1).toBe(true); // within1 implies within2
      if (r.breach1 === null) expect(r.within1).toBe(true);
      if (r.breach2 !== null) expect(r.within2).toBe(false);
    }
  });

  test('band math is faithful to the engine daily horizon (same anchor + sigma)', () => {
    const bars = weekdayBars(80);
    const window = 20;
    const results = dailyCoverage(bars, { window });
    const target = results[results.length - 1]; // last scored session
    const asOf = target.session;

    const engine = MultiHorizonVolatilityLevels.calculate(
      bars.filter((b) => b.timestamp.slice(0, 10) <= asOf),
      { symbol: 'SPY', asOf, generatedAt: `${asOf}T13:00:00.000Z`, volatilityWindows: [window], horizons: ['daily'] },
    );
    const daily = engine.horizons.daily;
    expect(daily.status).not.toBe('unavailable');
    // Prior-close anchor and the daily sigma must match the harness to numerical precision.
    expect(daily.levels.anchor).toBeCloseTo(target.anchor, 9);
    expect(daily.levels.horizonSigmaLogReturn).toBeCloseTo(target.sigma, 12);
    // And the engine's ±1σ band reproduces from the harness anchor+sigma.
    expect(target.anchor * Math.exp(target.sigma)).toBeCloseTo(daily.levels.upper1Sigma, 6);
  });

  test('rejects too-short series', () => {
    expect(() => dailyCoverage(weekdayBars(10), { window: 20 })).toThrow(/at least/);
  });
});

describe('simulateGaussianNullCoverage (estimator-aware null)', () => {
  test('a 20-obs estimator undercovers the asymptotic normal (≈67% / ≈94%, i.e. ~t19)', () => {
    const c = simulateGaussianNullCoverage({ window: 20, iters: 200000, seed: 5 });
    expect(c.coverage1).toBeGreaterThan(0.66);
    expect(c.coverage1).toBeLessThan(0.68); // well below 0.683
    expect(c.coverage2).toBeGreaterThan(0.933);
    expect(c.coverage2).toBeLessThan(0.947); // well below 0.955
  });

  test('a larger window approaches the asymptotic normal', () => {
    const c60 = simulateGaussianNullCoverage({ window: 60, iters: 200000, seed: 5 });
    expect(c60.coverage2).toBeGreaterThan(0.945); // closer to 0.955 than window=20
  });

  test('is deterministic under a fixed seed and validates window', () => {
    expect(simulateGaussianNullCoverage({ window: 20, iters: 5000, seed: 1 }))
      .toEqual(simulateGaussianNullCoverage({ window: 20, iters: 5000, seed: 1 }));
    expect(() => simulateGaussianNullCoverage({ window: 1 })).toThrow(/window/);
  });
});

describe('blockBootstrapCI', () => {
  test('rejects hostile parameters instead of hanging or returning garbage', () => {
    expect(() => blockBootstrapCI([1, 0, 1], { blockSize: 0 })).toThrow(/blockSize/);
    expect(() => blockBootstrapCI([1, 0, 1], { iters: 0 })).toThrow(/iters/);
    expect(() => blockBootstrapCI([1, 0, 1], { level: 1 })).toThrow(/level/);
  });

  test('is deterministic under a fixed seed and brackets the sample mean', () => {
    const ind = Array.from({ length: 500 }, (_, i) => (i % 3 === 0 ? 1 : 0));
    const a = blockBootstrapCI(ind, { seed: 42, iters: 500 });
    const b = blockBootstrapCI(ind, { seed: 42, iters: 500 });
    expect(a).toEqual(b); // reproducible
    expect(a.lo).toBeLessThanOrEqual(a.mean);
    expect(a.hi).toBeGreaterThanOrEqual(a.mean);
  });

  test('an all-ones indicator has coverage 1 with a degenerate CI', () => {
    const ci = blockBootstrapCI(new Array(100).fill(1), { seed: 1, iters: 200 });
    expect(ci.mean).toBe(1);
    expect(ci.lo).toBe(1);
    expect(ci.hi).toBe(1);
  });
});

describe('summarizeCoverage', () => {
  test('reports coverage, calibration error, breach asymmetry, and per-year rows', () => {
    const bars = weekdayBars(300);
    const summary = summarizeCoverage(dailyCoverage(bars, { window: 20 }), { iters: 300, window: 20, nullIters: 20000 });
    expect(summary.n).toBe(300 - 21);
    expect(summary.coverage1).toBeGreaterThan(0);
    expect(summary.coverage1).toBeLessThanOrEqual(1);
    expect(summary.calibration_error_1).toBeCloseTo(summary.coverage1 - summary.nominal1, 12);
    // Estimator-aware null is present and below the asymptotic-normal reference for window=20.
    expect(summary.estimator_null_2).toBeLessThan(summary.nominal2);
    expect(summary.calibration_error_vs_null_2).toBeCloseTo(summary.coverage2 - summary.estimator_null_2, 12);
    expect(summary.ci1.lo).toBeLessThanOrEqual(summary.coverage1 + 1e-9);
    expect(summary.ci1.hi).toBeGreaterThanOrEqual(summary.coverage1 - 1e-9);
    expect(Array.isArray(summary.per_year)).toBe(true);
    expect(summary.per_year.reduce((s, y) => s + y.n, 0)).toBe(summary.n);
  });
});
