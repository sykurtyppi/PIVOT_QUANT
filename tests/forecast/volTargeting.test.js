import {
  runExperiment,
  strategyReturns,
  summarizeStrategy,
  weightSeries,
} from '../../src/forecast/volTargeting.js';

// Deterministic returns with a calm first half and a volatile second half.
function regimeReturns(n = 400) {
  const out = [];
  for (let i = 0; i < n; i += 1) {
    const vol = i < n / 2 ? 0.005 : 0.02;
    out.push(vol * (((i * 17) % 9) - 4) / 4);
  }
  return out;
}

describe('weightSeries', () => {
  test('fixed is always fully invested after warmup', () => {
    const w = weightSeries(regimeReturns(120), { method: 'fixed', warmup: 60 });
    expect(w.slice(0, 60).every((x) => x === null)).toBe(true);
    expect(w.slice(60).every((x) => x === 1)).toBe(true);
  });

  test('rv20 lowers weight when volatility rises (respecting the cap)', () => {
    const w = weightSeries(regimeReturns(400), { method: 'rv20', warmup: 60, sigmaTarget: 0.15, wMin: 0, wMax: 1.5 });
    const calm = w[150]; // still calm regime
    const wild = w[380]; // volatile regime
    expect(calm).toBeLessThanOrEqual(1.5); // cap respected
    expect(wild).toBeLessThan(calm); // de-risk when vol is high
  });

  test('does not look ahead: weight at t uses only returns before t', () => {
    const base = regimeReturns(200);
    const w1 = weightSeries(base, { method: 'rv20', warmup: 60 });
    const perturbed = base.slice();
    perturbed[150] = 0.5; // change a FUTURE return
    const w2 = weightSeries(perturbed, { method: 'rv20', warmup: 60 });
    // Weights at t <= 150 must be unchanged (they cannot see return[150] or later).
    for (let t = 60; t <= 150; t += 1) expect(w2[t]).toBe(w1[t]);
  });
});

describe('strategyReturns', () => {
  test('charges turnover cost on weight changes', () => {
    const returns = [0.01, 0.01, 0.01];
    const weights = [1, 0.5, 0.5]; // one change of 0.5 at index 1
    const { netReturns } = strategyReturns(returns, weights, { costPerTurn: 0.001, from: 0 });
    // Simple-return P&L: market simple return = expm1(logret). index0: w=1, no turnover.
    expect(netReturns[0]).toBeCloseTo(Math.expm1(0.01), 12);
    // index1: w=0.5, turn 0.5 -> cost 0.0005 -> net = 0.5*expm1(0.01) - 0.0005
    expect(netReturns[1]).toBeCloseTo(0.5 * Math.expm1(0.01) - 0.0005, 12);
  });
});

describe('summarizeStrategy', () => {
  test('reports annualized metrics and certainty-equivalent returns', () => {
    const r = new Array(300).fill(0.0004); // constant simple return, zero vol
    const s = summarizeStrategy(r, new Array(300).fill(0));
    expect(s.annReturn).toBeCloseTo(1.0004 ** 252 - 1, 9); // CAGR (geometric), not mean*252
    expect(s.annVol).toBeCloseTo(0, 9);
    expect(s.ceReturn[3]).toBeCloseTo(0.0004 * 252, 6); // zero variance -> CE == annualized mean
  });
});

describe('runExperiment', () => {
  test('vol-targeting reduces realized vol vs fixed on regime-switching data', () => {
    const exp = runExperiment(regimeReturns(400), {});
    expect(exp.strategies.fixed.n).toBeGreaterThan(0);
    // rv20 should hold realized vol below the always-on fixed strategy in a high-vol regime mix.
    expect(exp.strategies.rv20.annVol).toBeLessThan(exp.strategies.fixed.annVol);
    expect(exp.netByMethod.rv20).toHaveLength(exp.strategies.rv20.n);
  });
});
