/**
 * Point-in-time calibration harness for the expected-move bands (roadmap §3).
 *
 * For each session it reconstructs the daily band exactly as it would have been known that
 * morning — anchor = prior close, sigma = sample std of the last `window` close-to-close log
 * returns ending at the prior session, log-symmetric bands — then checks whether the realized
 * close landed inside ±1σ / ±2σ. This is the daily-horizon math of
 * src/math/MultiHorizonVolatilityLevels.js (sessionsRemaining = 1), verified against the engine
 * in the tests, computed with a rolling window so a 10y backtest is O(n).
 *
 * Nothing here reaches for data or fabricates it: callers pass the bar series. Inference that
 * uses randomness (the block bootstrap) is seeded, so results are reproducible.
 */

const NOMINAL_1SIGMA = 0.6827; // P(|Z| <= 1) for a standard normal
const NOMINAL_2SIGMA = 0.9545; // P(|Z| <= 2)

export function sampleStd(values) {
  const n = values.length;
  if (n < 2) throw new Error('sampleStd needs at least two values');
  const mean = values.reduce((s, v) => s + v, 0) / n;
  const ss = values.reduce((s, v) => s + (v - mean) ** 2, 0);
  return Math.sqrt(ss / (n - 1));
}

/** Close-to-close log returns for a sorted daily bar series (one bar per session). */
export function logReturns(bars) {
  const r = [];
  for (let i = 1; i < bars.length; i += 1) {
    if (!(bars[i].close > 0) || !(bars[i - 1].close > 0)) {
      throw new Error(`non-positive close at index ${i}`);
    }
    r.push(Math.log(bars[i].close / bars[i - 1].close));
  }
  return r;
}

/**
 * Point-in-time daily coverage series. Each element is one forecast/outcome pair with the
 * standardized realized move and containment flags.
 *
 * @param {Array<{timestamp:string, close:number}>} bars  Sorted ascending, one per session.
 * @param {object} [opts] { window=20 }
 * @returns {Array<object>}
 */
export function dailyCoverage(bars, opts = {}) {
  const window = opts.window ?? 20;
  if (!Array.isArray(bars) || bars.length < window + 2) {
    throw new Error(`need at least ${window + 2} bars for a window-${window} coverage series`);
  }
  const closes = bars.map((b) => b.close);
  const rets = logReturns(bars); // rets[k] is the return realized on session k+1
  const out = [];
  for (let d = window + 1; d < bars.length; d += 1) {
    // Returns known by the morning of session d, ending at the prior session (d-1).
    const windowRets = rets.slice(d - 1 - window, d - 1); // length === window
    const sigma = sampleStd(windowRets);
    const anchor = closes[d - 1];
    const realizedReturn = rets[d - 1]; // ln(close[d]/close[d-1])
    const z = sigma > 0 ? realizedReturn / sigma : 0;
    const absZ = Math.abs(z);
    out.push({
      session: bars[d].timestamp.slice(0, 10),
      anchor,
      realized: closes[d],
      sigma,
      z,
      within1: absZ <= 1,
      within2: absZ <= 2,
      breach1: z < -1 ? 'lower' : z > 1 ? 'upper' : null,
      breach2: z < -2 ? 'lower' : z > 2 ? 'upper' : null,
    });
  }
  return out;
}

function mulberry32(seed) {
  let a = seed >>> 0;
  return function next() {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * Circular block bootstrap CI for the mean of a 0/1 indicator, preserving serial dependence
 * (overlapping exceedances are not independent). Seeded for reproducibility.
 */
export function blockBootstrapCI(indicator, opts = {}) {
  const { blockSize = 20, iters = 2000, seed = 20260924, level = 0.95 } = opts;
  const n = indicator.length;
  if (n === 0) return { lo: NaN, hi: NaN, mean: NaN };
  const rng = mulberry32(seed);
  const means = new Array(iters);
  for (let it = 0; it < iters; it += 1) {
    let sum = 0;
    let count = 0;
    while (count < n) {
      const start = Math.floor(rng() * n);
      for (let k = 0; k < blockSize && count < n; k += 1) {
        sum += indicator[(start + k) % n];
        count += 1;
      }
    }
    means[it] = sum / n;
  }
  means.sort((x, y) => x - y);
  const loIdx = Math.floor(((1 - level) / 2) * iters);
  const hiIdx = Math.min(iters - 1, Math.floor(((1 + level) / 2) * iters));
  const mean = indicator.reduce((s, v) => s + v, 0) / n;
  return { lo: means[loIdx], hi: means[hiIdx], mean };
}

/**
 * Summarize a coverage series: empirical ±1σ/±2σ coverage with bootstrap CIs, calibration
 * error vs the nominal normal targets, upper/lower breach asymmetry, and per-year coverage.
 */
export function summarizeCoverage(results, opts = {}) {
  const n = results.length;
  if (n === 0) throw new Error('empty coverage series');
  const ind1 = results.map((r) => (r.within1 ? 1 : 0));
  const ind2 = results.map((r) => (r.within2 ? 1 : 0));
  const cov1 = ind1.reduce((s, v) => s + v, 0) / n;
  const cov2 = ind2.reduce((s, v) => s + v, 0) / n;

  const upper1 = results.filter((r) => r.breach1 === 'upper').length;
  const lower1 = results.filter((r) => r.breach1 === 'lower').length;

  const byYear = new Map();
  for (const r of results) {
    const year = r.session.slice(0, 4);
    if (!byYear.has(year)) byYear.set(year, []);
    byYear.get(year).push(r);
  }
  const perYear = [...byYear.entries()].sort().map(([year, rs]) => ({
    year,
    n: rs.length,
    coverage1: rs.filter((r) => r.within1).length / rs.length,
    coverage2: rs.filter((r) => r.within2).length / rs.length,
  }));

  return {
    n,
    coverage1: cov1,
    coverage2: cov2,
    nominal1: NOMINAL_1SIGMA,
    nominal2: NOMINAL_2SIGMA,
    calibration_error_1: cov1 - NOMINAL_1SIGMA,
    calibration_error_2: cov2 - NOMINAL_2SIGMA,
    ci1: blockBootstrapCI(ind1, opts),
    ci2: blockBootstrapCI(ind2, opts),
    breach_1sigma_upper: upper1,
    breach_1sigma_lower: lower1,
    breach_asymmetry_1: (upper1 - lower1) / n, // >0 means upper band breached more often
    per_year: perYear,
  };
}

export { NOMINAL_1SIGMA, NOMINAL_2SIGMA };
