/**
 * Volatility-targeting decision backtest (roadmap §4 gate).
 *
 * Tests whether sizing SPY exposure by a volatility forecast improves NET risk-adjusted decision
 * utility over fixed exposure — the question that decides whether the risk map has any internal
 * use. Frozen design in research/calibration/decision_experiment_prereg.md.
 *
 * Timing (no look-ahead): σ̂ at t uses returns through t-1; the weight w_t is set at the close of
 * t-1 and earns day-t's return. Costs are charged on |w_t - w_{t-1}|.
 */

const ANNUALIZE = Math.sqrt(252);

export function sampleStd(values) {
  const n = values.length;
  if (n < 2) return 0;
  const mean = values.reduce((s, v) => s + v, 0) / n;
  return Math.sqrt(values.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1));
}

function clip(x, lo, hi) {
  return Math.min(hi, Math.max(lo, x));
}

/**
 * Build the daily weight series for a sizing method. Returns weights aligned to `returns` index t
 * (the weight applied to return[t]), or null before warmup.
 * @param {number[]} returns daily log returns
 * @param {object} cfg { method: 'fixed'|'rv20'|'ewma94', sigmaTarget, wMin, wMax, warmup, lambda }
 */
export function weightSeries(returns, cfg) {
  const { method, sigmaTarget = 0.15, wMin = 0, wMax = 1.5, warmup = 60, lambda = 0.94 } = cfg;
  const weights = new Array(returns.length).fill(null);
  let ewmaVar = sampleStd(returns.slice(0, warmup)) ** 2;
  for (let t = warmup; t < returns.length; t += 1) {
    let w;
    if (method === 'fixed') {
      w = 1;
    } else {
      let sigmaDaily;
      if (method === 'rv20') sigmaDaily = sampleStd(returns.slice(t - 20, t));
      else if (method === 'ewma94') sigmaDaily = Math.sqrt(ewmaVar);
      else throw new Error(`unknown method ${method}`);
      const sigmaAnn = sigmaDaily * ANNUALIZE;
      w = sigmaAnn > 0 ? clip(sigmaTarget / sigmaAnn, wMin, wMax) : wMax;
    }
    weights[t] = w;
    // advance EWMA to include return[t-1]? It already reflects [0..t-1]; update with return[t]
    // so the NEXT iteration (t+1, which needs [0..t]) is correct.
    ewmaVar = lambda * ewmaVar + (1 - lambda) * returns[t] ** 2;
  }
  return weights;
}

/**
 * Net daily strategy returns for a weight series, charging turnover cost and optional financing.
 * @returns {{netReturns:number[], turnover:number[], indices:number[]}}
 */
export function strategyReturns(returns, weights, { costPerTurn = 0.0002, financingAnnual = 0, from = 0 } = {}) {
  const financingDaily = financingAnnual / 252;
  const netReturns = [];
  const turnover = [];
  const indices = [];
  let prevW = null;
  for (let t = 0; t < returns.length; t += 1) {
    const w = weights[t];
    if (w == null) continue;
    const turn = prevW == null ? 0 : Math.abs(w - prevW);
    prevW = w;
    if (t < from) continue;
    const gross = w * returns[t];
    const cost = turn * costPerTurn + (financingDaily > 0 ? Math.max(w - 1, 0) * financingDaily : 0);
    netReturns.push(gross - cost);
    turnover.push(turn);
    indices.push(t);
  }
  return { netReturns, turnover, indices };
}

function maxDrawdown(dailyReturns) {
  let cum = 0;
  let peak = 0;
  let mdd = 0;
  for (const r of dailyReturns) {
    cum += r; // log-return cumulation
    peak = Math.max(peak, cum);
    mdd = Math.min(mdd, cum - peak);
  }
  return mdd; // <= 0
}

function expectedShortfall(dailyReturns, q = 0.05) {
  const sorted = [...dailyReturns].sort((a, b) => a - b);
  const cut = Math.max(1, Math.floor(sorted.length * q));
  const tail = sorted.slice(0, cut);
  return tail.reduce((s, v) => s + v, 0) / tail.length;
}

/** Summary metrics for a net daily-return series. */
export function summarizeStrategy(netReturns, turnover, gammas = [1, 3, 5]) {
  const n = netReturns.length;
  const mean = netReturns.reduce((s, v) => s + v, 0) / n;
  const variance = netReturns.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1);
  const vol = Math.sqrt(variance);
  const ce = Object.fromEntries(gammas.map((g) => [g, (mean - (g / 2) * variance) * 252]));
  return {
    n,
    annReturn: mean * 252,
    annVol: vol * ANNUALIZE,
    sharpe: vol > 0 ? (mean / vol) * ANNUALIZE : 0,
    maxDrawdown: maxDrawdown(netReturns),
    es5: expectedShortfall(netReturns, 0.05),
    meanTurnover: turnover.reduce((s, v) => s + v, 0) / n,
    ceReturn: ce,
  };
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

/** Annualized Sharpe of a daily-return array (rf = 0). */
export function sharpeOf(daily) {
  const n = daily.length;
  if (n < 2) return 0;
  const mean = daily.reduce((s, v) => s + v, 0) / n;
  const sd = Math.sqrt(daily.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1));
  return sd > 0 ? (mean / sd) * Math.sqrt(252) : 0;
}

/**
 * Block-bootstrap 95% CI for the DIFFERENCE in a scalar metric between two paired daily-return
 * series (e.g. ΔSharpe of A − B). Resamples day-blocks once and applies the SAME positions to both
 * series, preserving the pairing and serial dependence. This tests the risk-adjusted claim the gate
 * actually makes, rather than only the mean-return difference.
 */
export function bootstrapMetricDiff(netA, netB, metricFn, opts = {}) {
  const { blockSize = 20, iters = 2000, seed = 20260925, level = 0.95 } = opts;
  const n = netA.length;
  if (n === 0 || netB.length !== n) return { mean: NaN, lo: NaN, hi: NaN };
  const rng = mulberry32(seed);
  const diffs = new Array(iters);
  const ra = new Array(n);
  const rb = new Array(n);
  for (let it = 0; it < iters; it += 1) {
    let count = 0;
    while (count < n) {
      const start = Math.floor(rng() * n);
      for (let k = 0; k < blockSize && count < n; k += 1) {
        const idx = (start + k) % n;
        ra[count] = netA[idx];
        rb[count] = netB[idx];
        count += 1;
      }
    }
    diffs[it] = metricFn(ra) - metricFn(rb);
  }
  diffs.sort((a, b) => a - b);
  return {
    mean: metricFn(netA) - metricFn(netB),
    lo: diffs[Math.floor(((1 - level) / 2) * iters)],
    hi: diffs[Math.min(iters - 1, Math.floor(((1 + level) / 2) * iters))],
  };
}

/** Block-bootstrap 95% CI for the mean of a paired-difference series (serial-dependence aware). */
export function blockBootstrapMeanCI(values, opts = {}) {
  const { blockSize = 20, iters = 2000, seed = 20260925, level = 0.95 } = opts;
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
  means.sort((a, b) => a - b);
  const mean = values.reduce((s, v) => s + v, 0) / n;
  return { mean, lo: means[Math.floor(((1 - level) / 2) * iters)], hi: means[Math.min(iters - 1, Math.floor(((1 + level) / 2) * iters))] };
}

/**
 * Run the full experiment for a returns series and a `from` (holdout) boundary.
 * @returns {{ strategies: Object, netByMethod: Object, indices:number[] }}
 */
export function runExperiment(returns, opts = {}) {
  const { from = 0, warmup = 60, costPerTurn = 0.0002, financingAnnual = 0, sigmaTarget = 0.15, wMin = 0, wMax = 1.5 } = opts;
  const methods = ['fixed', 'rv20', 'ewma94'];
  const netByMethod = {};
  const strategies = {};
  let indices = null;
  for (const method of methods) {
    const w = weightSeries(returns, { method, sigmaTarget, wMin, wMax, warmup });
    const { netReturns, turnover, indices: idx } = strategyReturns(returns, w, { costPerTurn, financingAnnual, from });
    netByMethod[method] = netReturns;
    strategies[method] = summarizeStrategy(netReturns, turnover);
    indices = idx;
  }
  return { strategies, netByMethod, indices };
}
