/**
 * Estimator tournament for the daily expected-move band (roadmap §4).
 *
 * The question this answers: does the 20-day realized-volatility band actually FORECAST better
 * than trivial baselines, or is it a commodity any vol calc reproduces? Each morning, several
 * volatility estimators build a ±1σ / ±2σ band from prior data only; the realized next-day return
 * is scored with the Weighted Interval Score (WIS) — the standard proper scoring rule for interval
 * forecasts, which rewards sharpness and penalizes miscoverage in one number (lower is better).
 *
 * Everything is computed in LOG-RETURN space (median 0, band ±kσ), which is scale-invariant, so a
 * 2016 SPY at ~180 and a 2026 SPY at ~760 contribute comparably. Point-in-time throughout: the
 * estimate at session t uses only returns strictly before t.
 */

// Central-interval nominal coverages for k = 1σ and 2σ, and their interval-score alphas.
const LEVELS = [
  { k: 1, alpha: 1 - 0.6827 },
  { k: 2, alpha: 1 - 0.9545 },
];

export function sampleStd(values) {
  const n = values.length;
  if (n < 2) return 0;
  const mean = values.reduce((s, v) => s + v, 0) / n;
  const ss = values.reduce((s, v) => s + (v - mean) ** 2, 0);
  return Math.sqrt(ss / (n - 1));
}

/** Interval score for a central (1-alpha) interval [lower, upper] and realized y (lower is better). */
export function intervalScore(lower, upper, y, alpha) {
  return (upper - lower)
    + (2 / alpha) * Math.max(lower - y, 0)
    + (2 / alpha) * Math.max(y - upper, 0);
}

/**
 * Weighted Interval Score across the configured levels plus the median term.
 * @param {number} median   point forecast (0 in return space)
 * @param {number} sigma    volatility estimate
 * @param {number} y        realized value (log return)
 */
export function weightedIntervalScore(median, sigma, y) {
  const k0 = 0.5 * Math.abs(y - median);
  let sum = k0;
  for (const { k, alpha } of LEVELS) {
    const lower = median - k * sigma;
    const upper = median + k * sigma;
    sum += (alpha / 2) * intervalScore(lower, upper, y, alpha);
  }
  return sum / (LEVELS.length + 0.5);
}

export const TOURNAMENT_METHODS = ['rv20', 'rv60', 'ewma94', 'expanding', 'yesterday'];

/**
 * Run the tournament over a log-return series.
 * @param {number[]} returns   close-to-close log returns (returns[k] realized on session k+1)
 * @param {object} [opts] { warmup=60, lambda=0.94, from=0 } — `from` restricts scoring to
 *        returns index >= from (used to score only a holdout segment).
 * @returns {{perMethod:Object, perDayWis:Object, n:number, indices:number[]}}
 */
export function runTournament(returns, opts = {}) {
  const warmup = opts.warmup ?? 60;
  const lambda = opts.lambda ?? 0.94;
  const from = opts.from ?? 0;
  if (!Array.isArray(returns) || returns.length < warmup + 2) {
    throw new Error(`need at least ${warmup + 2} returns`);
  }

  const perDayWis = Object.fromEntries(TOURNAMENT_METHODS.map((m) => [m, []]));
  const stats = Object.fromEntries(TOURNAMENT_METHODS.map((m) => [m, { within1: 0, within2: 0, width2Sum: 0 }]));
  const indices = [];

  // EWMA variance state reflecting returns[0..t-1]; seeded from the warmup window.
  let ewmaVar = sampleStd(returns.slice(0, warmup)) ** 2;

  for (let t = warmup; t < returns.length; t += 1) {
    const y = returns[t];
    const sigmas = {
      rv20: sampleStd(returns.slice(t - 20, t)),
      rv60: sampleStd(returns.slice(t - 60, t)),
      ewma94: Math.sqrt(ewmaVar),
      expanding: sampleStd(returns.slice(0, t)),
      yesterday: Math.max(Math.abs(returns[t - 1]), 1e-6),
    };
    // advance EWMA to include returns[t-1]... actually returns[t-1] is already in [0..t-1];
    // update to include returns[t] for the NEXT iteration (which needs [0..t]).
    ewmaVar = lambda * ewmaVar + (1 - lambda) * returns[t] ** 2;

    if (t < from) continue;
    indices.push(t);
    for (const m of TOURNAMENT_METHODS) {
      const sigma = sigmas[m];
      perDayWis[m].push(weightedIntervalScore(0, sigma, y));
      if (Math.abs(y) <= sigma) stats[m].within1 += 1;
      if (Math.abs(y) <= 2 * sigma) stats[m].within2 += 1;
      stats[m].width2Sum += 4 * sigma; // width of the ±2σ interval
    }
  }

  const n = indices.length;
  const perMethod = Object.fromEntries(TOURNAMENT_METHODS.map((m) => {
    const wis = perDayWis[m];
    const meanWis = wis.reduce((s, v) => s + v, 0) / n;
    return [m, {
      meanWis,
      coverage1: stats[m].within1 / n,
      coverage2: stats[m].within2 / n,
      meanWidth2: stats[m].width2Sum / n,
      n,
    }];
  }));
  return { perMethod, perDayWis, n, indices };
}

function mulberry32(seed) {
  let a = seed >>> 0;
  return function next() {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * Circular block-bootstrap CI for the mean of a continuous series (e.g. per-day WIS differences),
 * preserving serial dependence. Seeded. Returns { mean, lo, hi }.
 */
export function blockBootstrapMeanCI(values, opts = {}) {
  const { blockSize = 20, iters = 2000, seed = 20260924, level = 0.95 } = opts;
  if (!Number.isInteger(blockSize) || blockSize < 1) throw new Error('blockSize must be a positive integer');
  const n = values.length;
  if (n === 0) return { mean: NaN, lo: NaN, hi: NaN };
  const rng = mulberry32(seed);
  const means = new Array(iters);
  for (let it = 0; it < iters; it += 1) {
    let sum = 0; let count = 0;
    while (count < n) {
      const start = Math.floor(rng() * n);
      for (let k = 0; k < blockSize && count < n; k += 1) { sum += values[(start + k) % n]; count += 1; }
    }
    means[it] = sum / n;
  }
  means.sort((x, y) => x - y);
  const mean = values.reduce((s, v) => s + v, 0) / n;
  return {
    mean,
    lo: means[Math.floor(((1 - level) / 2) * iters)],
    hi: means[Math.min(iters - 1, Math.floor(((1 + level) / 2) * iters))],
  };
}
