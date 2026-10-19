/**
 * Tests for fdr_correction.js — Benjamini-Hochberg FDR correction and the
 * binomial significance test used for pivot-level analysis.
 *
 * The module is a browser-global IIFE (`window.FDRCorrection = (() => …)()`)
 * with no ES exports, so we stub `window` on globalThis and load it for its
 * side effect, then read the public API off the stub. Known-answer cases pin
 * the statistics directly (this is exactly the "never fabricate significance"
 * surface the project's discipline contract cares about).
 */

describe('fdr_correction.js — FDRCorrection', () => {
  let FDR;

  beforeAll(async () => {
    globalThis.window = {};
    await import('../fdr_correction.js');
    FDR = globalThis.window.FDRCorrection;
  });

  test('exposes the documented public API', () => {
    expect(typeof FDR).toBe('object');
    expect(typeof FDR.binomialTest).toBe('function');
    expect(typeof FDR.benjaminiHochbergCorrection).toBe('function');
    expect(typeof FDR.analyzePivotSignificance).toBe('function');
    expect(typeof FDR.formatSignificanceDisplay).toBe('function');
  });

  describe('binomialTest', () => {
    test('zero trials is undefined evidence -> p = 1.0', () => {
      expect(FDR.binomialTest(0, 0)).toBe(1.0);
    });

    test('exact two-tailed value for the all-successes extreme (n=10)', () => {
      // n=10 uses the exact path (n*p == 5, not > 5). Only k=0 and k=10 have
      // PMF <= PMF(10) = 0.5^10, so p = 2/1024.
      expect(FDR.binomialTest(10, 10, 0.5)).toBeCloseTo(0.001953125, 9);
    });

    test('observing exactly the null rate gives the maximal p = 1.0', () => {
      // PMF(5) is the mode, so every other outcome has PMF <= it and all sum in.
      expect(FDR.binomialTest(5, 10, 0.5)).toBeCloseTo(1.0, 9);
    });

    test('is symmetric about the null for p = 0.5', () => {
      expect(FDR.binomialTest(2, 10)).toBeCloseTo(FDR.binomialTest(8, 10), 12);
      expect(FDR.binomialTest(0, 10)).toBeCloseTo(FDR.binomialTest(10, 10), 12);
    });

    test('large-sample normal approximation: extreme deviation is tiny, null is large', () => {
      // n=100 takes the normal-approx path (n*p = 50 > 5).
      expect(FDR.binomialTest(90, 100)).toBeLessThan(1e-4);
      expect(FDR.binomialTest(50, 100)).toBeGreaterThan(0.85);
    });

    test('always returns a probability in [0, 1]', () => {
      const cases = [[0, 0], [1, 1], [3, 7], [25, 50], [99, 100], [1, 1000]];
      for (const [s, t] of cases) {
        const p = FDR.binomialTest(s, t);
        expect(p).toBeGreaterThanOrEqual(0);
        expect(p).toBeLessThanOrEqual(1);
      }
    });
  });

  describe('benjaminiHochbergCorrection', () => {
    test('empty input returns empty results', () => {
      expect(FDR.benjaminiHochbergCorrection([])).toEqual({ qValues: [], significant: [] });
    });

    test('known worked example maps q-values back to original order', () => {
      const { qValues, significant } = FDR.benjaminiHochbergCorrection([0.001, 0.5, 0.9], 0.05);
      expect(qValues[0]).toBeCloseTo(0.003, 9);
      expect(qValues[1]).toBeCloseTo(0.75, 9);
      expect(qValues[2]).toBeCloseTo(0.9, 9);
      expect(significant).toEqual([true, false, false]);
    });

    test('q-values follow the original index even when input is unsorted', () => {
      // same multiset as above, permuted: idx0=0.9, idx1=0.001, idx2=0.5
      const { qValues, significant } = FDR.benjaminiHochbergCorrection([0.9, 0.001, 0.5], 0.05);
      expect(qValues[0]).toBeCloseTo(0.9, 9);
      expect(qValues[1]).toBeCloseTo(0.003, 9);
      expect(qValues[2]).toBeCloseTo(0.75, 9);
      expect(significant).toEqual([false, true, false]);
    });

    test('all-equal p-values at the boundary: every q equals alpha and all pass', () => {
      const { qValues, significant } = FDR.benjaminiHochbergCorrection(
        [0.05, 0.04, 0.03, 0.02, 0.01], 0.05,
      );
      for (const q of qValues) {
        expect(q).toBeCloseTo(0.05, 9);
      }
      expect(significant).toEqual([true, true, true, true, true]);
    });

    test('enforces monotone non-decreasing q-values (by sorted p) and caps at 1', () => {
      const pValues = [0.2, 0.01, 0.9, 0.04, 0.7];
      const { qValues } = FDR.benjaminiHochbergCorrection(pValues, 0.05);
      for (const q of qValues) {
        expect(q).toBeGreaterThanOrEqual(0);
        expect(q).toBeLessThanOrEqual(1);
      }
      // sort (p, q) by p ascending; q must be non-decreasing
      const paired = pValues.map((p, i) => [p, qValues[i]]).sort((a, b) => a[0] - b[0]);
      for (let i = 1; i < paired.length; i++) {
        expect(paired[i][1]).toBeGreaterThanOrEqual(paired[i - 1][1] - 1e-12);
      }
    });
  });

  describe('analyzePivotSignificance', () => {
    test('corrects across levels and summarises significance', () => {
      const out = FDR.analyzePivotSignificance({
        R1: { successes: 95, trials: 100 }, // overwhelming -> significant
        PIVOT: { successes: 50, trials: 100 }, // at the null -> not significant
        S1: { successes: 0, trials: 0 }, // no data
      }, 0.05);

      expect(out.summary.totalLevels).toBe(3);
      expect(out.summary.fdrLevel).toBe(0.05);
      expect(out.summary.significantLevels).toBe(1);

      expect(out.levels.R1.successRate).toBeCloseTo(95, 9);
      expect(out.levels.R1.significant).toBe(true);
      expect(out.levels.R1.qValue).toBeLessThanOrEqual(0.05);

      expect(out.levels.PIVOT.successRate).toBeCloseTo(50, 9);
      expect(out.levels.PIVOT.significant).toBe(false);

      expect(out.levels.S1.trials).toBe(0);
      expect(out.levels.S1.successRate).toBe(0);
      expect(out.levels.S1.pValue).toBe(1.0);
      expect(out.levels.S1.significant).toBe(false);
    });

    test('empty input yields zero levels', () => {
      const out = FDR.analyzePivotSignificance({});
      expect(out.summary.totalLevels).toBe(0);
      expect(out.summary.significantLevels).toBe(0);
      expect(out.levels).toEqual({});
    });
  });

  describe('formatSignificanceDisplay', () => {
    test('no-data level', () => {
      expect(FDR.formatSignificanceDisplay('S1', { successRate: 0, qValue: 1, significant: false, trials: 0 }))
        .toBe('S1: No data');
    });

    test('strongly significant level renders q<0.001 and the check mark', () => {
      const s = FDR.formatSignificanceDisplay('R1', { successRate: 95, qValue: 0.0005, significant: true, trials: 100 });
      expect(s).toContain('95.0%');
      expect(s).toContain('FDR q<0.001');
      expect(s).toContain('✓');
    });

    test('non-significant level renders the rounded q and the not-significant mark', () => {
      const s = FDR.formatSignificanceDisplay('PIVOT', { successRate: 50, qValue: 0.03, significant: false, trials: 100 });
      expect(s).toContain('FDR q=0.030');
      expect(s).toContain('✗ Not significant');
    });
  });
});
