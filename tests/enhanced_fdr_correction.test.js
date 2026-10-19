/**
 * Tests for enhanced_fdr_correction.js — regime-aware significance analysis:
 * one-sided binomial test, Wilson score CI, inverse-normal, monotone
 * Benjamini-Hochberg with invalid-p clamping, and the regime-aware pipeline.
 *
 * Same browser-global IIFE shape as fdr_correction.js, so we stub `window` on
 * globalThis and load for side effect. Deterministic statistics are pinned to
 * known values; the only randomised routine (permutationTest) is bounded, not
 * pinned, to stay non-flaky.
 */

describe('enhanced_fdr_correction.js — EnhancedFDRCorrection', () => {
  let E;

  beforeAll(async () => {
    globalThis.window = {};
    await import('../enhanced_fdr_correction.js');
    E = globalThis.window.EnhancedFDRCorrection;
  });

  test('exposes the documented public API', () => {
    for (const fn of [
      'analyzePivotSignificanceEnhanced', 'oneSidedBinomialTest',
      'enhancedBenjaminiHochbergCorrection', 'permutationTest', 'generateTooltip',
      'formatSignificanceDisplay', 'wilsonConfidenceInterval',
    ]) {
      expect(typeof E[fn]).toBe('function');
    }
    expect(typeof E.DEFAULT_REGIME_BASELINES).toBe('object');
  });

  describe('normalInverse (via wilsonConfidenceInterval z) and bounds', () => {
    test('Wilson CI for 50/100 matches the textbook ~[0.404, 0.596]', () => {
      const ci = E.wilsonConfidenceInterval(50, 100, 0.95);
      expect(ci.center).toBeCloseTo(0.5, 6);
      expect(ci.lower).toBeCloseTo(0.4038, 2);
      expect(ci.upper).toBeCloseTo(0.5962, 2);
    });

    test('zero trials -> uninformative [0, 1] with center 0', () => {
      expect(E.wilsonConfidenceInterval(0, 0)).toEqual({ lower: 0, upper: 1, center: 0 });
    });

    test('all-success CI stays strictly inside [0, 1] and above 0.5', () => {
      const ci = E.wilsonConfidenceInterval(10, 10, 0.95);
      expect(ci.lower).toBeGreaterThan(0);
      expect(ci.upper).toBeLessThanOrEqual(1);
      expect(ci.center).toBeGreaterThan(0.5);
      expect(ci.lower).toBeLessThanOrEqual(ci.center);
      expect(ci.center).toBeLessThanOrEqual(ci.upper);
    });
  });

  describe('oneSidedBinomialTest', () => {
    test('zero trials and out-of-range successes return 1.0', () => {
      expect(E.oneSidedBinomialTest(0, 0)).toBe(1.0);
      expect(E.oneSidedBinomialTest(12, 10)).toBe(1.0);
      expect(E.oneSidedBinomialTest(-1, 10)).toBe(1.0);
    });

    test('exact upper-tail probabilities (small n)', () => {
      // P(X >= 10 | n=10, p=0.5) = 0.5^10
      expect(E.oneSidedBinomialTest(10, 10, 0.5, true)).toBeCloseTo(0.0009765625, 10);
      // P(X >= 0) = 1
      expect(E.oneSidedBinomialTest(0, 10, 0.5, true)).toBeCloseTo(1.0, 10);
      // P(X >= 5 | n=10, p=0.5) = 1 - P(X <= 4) = 0.623047
      expect(E.oneSidedBinomialTest(5, 10, 0.5, true)).toBeCloseTo(0.623047, 5);
    });

    test('large-sample one-sided: strong over-performance is tiny', () => {
      expect(E.oneSidedBinomialTest(90, 100, 0.5, true)).toBeLessThan(1e-4);
      const atNull = E.oneSidedBinomialTest(50, 100, 0.5, true);
      expect(atNull).toBeGreaterThan(0.35);
      expect(atNull).toBeLessThan(0.65);
    });
  });

  describe('enhancedBenjaminiHochbergCorrection', () => {
    test('empty input returns empty results', () => {
      expect(E.enhancedBenjaminiHochbergCorrection([])).toEqual({ qValues: [], significant: [] });
    });

    test('known worked example maps q-values back to original order', () => {
      const { qValues, significant } = E.enhancedBenjaminiHochbergCorrection([0.001, 0.5, 0.9], 0.05);
      expect(qValues[0]).toBeCloseTo(0.003, 9);
      expect(qValues[1]).toBeCloseTo(0.75, 9);
      expect(qValues[2]).toBeCloseTo(0.9, 9);
      expect(significant).toEqual([true, false, false]);
    });

    test('fails closed on invalid p-values', () => {
      const { qValues, significant } = E.enhancedBenjaminiHochbergCorrection([NaN, -0.5, 2.0, 0.01], 0.05);
      expect(qValues[0]).toBeCloseTo(1, 9);
      expect(qValues[1]).toBeCloseTo(1, 9);
      expect(qValues[2]).toBeCloseTo(1.0, 9);
      expect(qValues[3]).toBeCloseTo(0.04, 9);
      expect(significant).toEqual([false, false, false, true]);
    });
  });

  describe('permutationTest (randomised — bounded, not pinned)', () => {
    test('zero trials returns 1.0', () => {
      expect(E.permutationTest({ successes: 0, trials: 0 }, 0.5)).toBe(1.0);
    });

    test('always returns a fraction in [0, 1]', () => {
      const p = E.permutationTest({ successes: 95, trials: 100 }, 0.5, 100);
      expect(p).toBeGreaterThanOrEqual(0);
      expect(p).toBeLessThanOrEqual(1);
    });
  });

  describe('DEFAULT_REGIME_BASELINES', () => {
    test('contains known regimes with probability-valued baselines', () => {
      const b = E.DEFAULT_REGIME_BASELINES;
      expect(b).toHaveProperty('RANGE');
      expect(b).toHaveProperty('UP-TREND');
      for (const regime of Object.keys(b)) {
        for (const v of Object.values(b[regime])) {
          expect(v).toBeGreaterThan(0);
          expect(v).toBeLessThan(1);
        }
      }
    });
  });

  describe('analyzePivotSignificanceEnhanced', () => {
    test('flags an overwhelming level and reports lift/baseline/nEff/CI', () => {
      const out = E.analyzePivotSignificanceEnhanced(
        { WIN: { successes: 95, trials: 100 } },
        'RANGE', 'weekly',
        { customBaselines: { WIN: 0.5 } },
      );
      const r = out.levels.WIN;
      expect(r.hasSufficientData).toBe(true);
      expect(r.successRate).toBeCloseTo(95, 9);
      expect(r.baseline).toBeCloseTo(50, 9);
      expect(r.lift).toBeCloseTo(45, 9);
      expect(r.nEffective).toBe(100);
      expect(r.significant).toBe(true);
      expect(r.qValue).toBeLessThanOrEqual(0.05);
      expect(r.testType).toBe('one-sided');
      expect(r.hypothesis).toContain('50.0%');

      expect(out.summary.totalLevels).toBe(1);
      expect(out.summary.validLevels).toBe(1);
      expect(out.summary.significantLevels).toBe(1);
      expect(out.summary.insufficientDataLevels).toBe(0);
      expect(out.summary.regime).toBe('RANGE');
      expect(out.familyInfo.testParameters.alpha).toBe(0.05);
    });

    test('gates levels below the sample-size floor as insufficient data', () => {
      const out = E.analyzePivotSignificanceEnhanced(
        { SMALL: { successes: 4, trials: 5 } },
        'RANGE', 'weekly',
        { customBaselines: { SMALL: 0.5 } },
      );
      const r = out.levels.SMALL;
      expect(r.hasSufficientData).toBe(false);
      expect(r.pValue).toBe(1.0);
      expect(r.qValue).toBeNull();
      expect(r.significant).toBe(false);
      expect(r.nEffective).toBe(5);
      expect(r.successRate).toBeCloseTo(80, 9);
      expect(out.summary.validLevels).toBe(0);
      expect(out.summary.insufficientDataLevels).toBe(1);
    });

    test('malformed counts cannot become significant', () => {
      const out = E.analyzePivotSignificanceEnhanced(
        { BAD: { successes: NaN, trials: 100 } },
        'RANGE', 'weekly',
        { customBaselines: { BAD: 0.5 } },
      );
      expect(out.levels.BAD.hasSufficientData).toBe(false);
      expect(out.levels.BAD.significant).toBe(false);
      expect(out.levels.BAD.pValue).toBe(1.0);
    });
  });

  describe('generateTooltip / formatSignificanceDisplay', () => {
    let out;
    beforeAll(() => {
      out = E.analyzePivotSignificanceEnhanced(
        { WIN: { successes: 95, trials: 100 }, SMALL: { successes: 4, trials: 5 } },
        'RANGE', 'weekly',
        { customBaselines: { WIN: 0.5, SMALL: 0.5 } },
      );
    });

    test('tooltip for a sufficient level includes the headline stats', () => {
      const t = E.generateTooltip('WIN', out.levels.WIN);
      expect(t).toContain('WIN Significance Analysis');
      expect(t).toContain('Success Rate: 95.0%');
      expect(t).toContain('95% CI:');
    });

    test('tooltip for an insufficient level explains the minimum', () => {
      const t = E.generateTooltip('SMALL', out.levels.SMALL);
      expect(t).toContain('Insufficient data');
      expect(t).toContain('Minimum required');
    });

    test('display line: significant level shows q<0.001 and the check mark', () => {
      const s = E.formatSignificanceDisplay('WIN', out.levels.WIN);
      expect(s).toBe('WIN: 95.0% (FDR q<0.001) ✓');
    });

    test('display line: insufficient level reports the sample size', () => {
      const s = E.formatSignificanceDisplay('SMALL', out.levels.SMALL);
      expect(s).toBe('SMALL: Insufficient data (n=5)');
    });
  });
});
