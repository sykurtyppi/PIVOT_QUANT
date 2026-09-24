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
  if (!Number.isInteger(blockSize) || blockSize < 1) throw new Error('blockSize must be a positive integer');
  if (!Number.isInteger(iters) || iters < 1) throw new Error('iters must be a positive integer');
  if (!(level > 0 && level < 1)) throw new Error('level must be in (0, 1)');
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

function randNormal(rng) {
  // Box-Muller; guard u1 away from 0 for the log.
  let u1 = rng();
  if (u1 < 1e-12) u1 = 1e-12;
  const u2 = rng();
  return Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
}

/**
 * Estimator-aware null coverage: the coverage the band ACTUALLY achieves under an IID-Gaussian
 * null when sigma is estimated from only `window` observations. Because sigma is estimated,
 * the standardized next return is ~Student-t(window-1), not standard normal, so the true null
 * coverage of a ±kσ̂ band is well below the asymptotic 68.3% / 95.5%. This Monte Carlo simulates
 * the exact rolling estimator (draw `window` returns, standardize the next return by their
 * sample std) rather than assuming an approximation.
 *
 * For window=20 this yields ≈67.0% (±1σ̂) and ≈94.0% (±2σ̂) — the correct benchmark to judge the
 * empirical coverage against, and the reason a 93% ±2σ result does NOT by itself prove fat tails.
 *
 * @returns {{coverage1:number, coverage2:number, iters:number, window:number}}
 */
export function simulateGaussianNullCoverage(opts = {}) {
  const { window = 20, iters = 200000, seed = 20260924 } = opts;
  if (!Number.isInteger(window) || window < 2) throw new Error('window must be an integer >= 2');
  if (!Number.isInteger(iters) || iters < 1) throw new Error('iters must be a positive integer');
  const rng = mulberry32(seed);
  let within1 = 0;
  let within2 = 0;
  for (let it = 0; it < iters; it += 1) {
    let sum = 0;
    let sumsq = 0;
    for (let k = 0; k < window; k += 1) {
      const x = randNormal(rng);
      sum += x;
      sumsq += x * x;
    }
    const mean = sum / window;
    const variance = (sumsq - window * mean * mean) / (window - 1);
    const sigmaHat = Math.sqrt(Math.max(variance, 0));
    const next = randNormal(rng);
    const z = sigmaHat > 0 ? Math.abs(next / sigmaHat) : 0;
    if (z <= 1) within1 += 1;
    if (z <= 2) within2 += 1;
  }
  return { coverage1: within1 / iters, coverage2: within2 / iters, iters, window };
}

/**
 * Summarize a coverage series: empirical ±1σ/±2σ coverage with bootstrap CIs, calibration
 * error vs BOTH the asymptotic-normal target and the estimator-aware (finite-sample) null,
 * upper/lower breach asymmetry, and per-year coverage.
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

  // The correct benchmark is the estimator-aware null, not the asymptotic normal, because
  // sigma is estimated from `window` observations. Callers pass the same window used to build
  // the bands so the null matches the estimator.
  const window = opts.window ?? 20;
  const nullCov = simulateGaussianNullCoverage({ window, iters: opts.nullIters ?? 200000, seed: opts.seed ?? 20260924 });

  return {
    n,
    coverage1: cov1,
    coverage2: cov2,
    // Asymptotic-normal targets — reported for reference but NOT the right benchmark here.
    nominal1: NOMINAL_1SIGMA,
    nominal2: NOMINAL_2SIGMA,
    calibration_error_1: cov1 - NOMINAL_1SIGMA,
    calibration_error_2: cov2 - NOMINAL_2SIGMA,
    // Estimator-aware (finite-sample) null — the correct comparison for a ±kσ̂ band.
    estimator_null_1: nullCov.coverage1,
    estimator_null_2: nullCov.coverage2,
    calibration_error_vs_null_1: cov1 - nullCov.coverage1,
    calibration_error_vs_null_2: cov2 - nullCov.coverage2,
    ci1: blockBootstrapCI(ind1, opts),
    ci2: blockBootstrapCI(ind2, opts),
    breach_1sigma_upper: upper1,
    breach_1sigma_lower: lower1,
    breach_asymmetry_1: (upper1 - lower1) / n, // >0 means upper band breached more often
    per_year: perYear,
  };
}

export { NOMINAL_1SIGMA, NOMINAL_2SIGMA };
