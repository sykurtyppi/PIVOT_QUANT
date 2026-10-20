/**
 * Tests for enhanced_fdr_correction.js — regime-aware significance analysis:
 * one-sided binomial test, Wilson score CI, inverse-normal, monotone
 * Benjamini-Hochberg with invalid-p clamping, and the regime-aware pipeline.
 *
 * Same browser-global IIFE shape as fdr_correction.js, so we stub `window` on
 * globalThis and load for side effect. Deterministic statistics are pinned to
 * known values; the aggregate-count permutation helper intentionally fails
 * closed because protocol-compliant block permutation requires raw labels.
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
      expect(atNull).toBeCloseTo(0.5398, 3);
    });

    test('malformed direct-call contracts fail closed', () => {
      for (const baseline of [null, -1, 2, 'bad', NaN]) {
        expect(E.oneSidedBinomialTest(1, 100, baseline, true)).toBe(1.0);
      }
      expect(E.oneSidedBinomialTest(1.5, 100, 0.5, true)).toBe(1.0);
      expect(E.oneSidedBinomialTest(1, 100.5, 0.5, true)).toBe(1.0);
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

    test('invalid alpha cannot manufacture significance', () => {
      const out = E.enhancedBenjaminiHochbergCorrection([0], 2);
      expect(out.significant).toEqual([false]);
    });

    test('sparse p-value arrays fail closed without throwing', () => {
      const sparse = [];
      sparse[1] = 0.01;
      const out = E.enhancedBenjaminiHochbergCorrection(sparse, 0.05);
      expect(out.qValues).toHaveLength(2);
      expect(out.significant).toEqual([false, true]);
    });
  });

  describe('permutationTest', () => {
    test('fails closed because aggregate counts cannot perform protocol block permutation', () => {
      expect(E.permutationTest({ successes: 0, trials: 0 }, 0.5)).toBeNull();
      expect(E.permutationTest({ successes: 95, trials: 100 }, 0.5, 10000)).toBeNull();
      expect(E.permutationTest({ successes: 95, trials: 100 }, 0.5, 0)).toBeNull();
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
        { customBaselines: { WIN: 0.5 }, overlapHorizon: 1 },
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
        { customBaselines: { SMALL: 0.5 }, overlapHorizon: 1 },
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
        { customBaselines: { BAD: 0.5 }, overlapHorizon: 1 },
      );
      expect(out.levels.BAD.hasSufficientData).toBe(false);
      expect(out.levels.BAD.significant).toBe(false);
      expect(out.levels.BAD.pValue).toBe(1.0);
    });

    test('malformed alpha, baselines, and level objects fail closed', () => {
      const invalidAlpha = E.analyzePivotSignificanceEnhanced(
        { WIN: { successes: 100, trials: 100 } },
        'RANGE', 'weekly',
        { alpha: 2, customBaselines: { WIN: 0.5 }, overlapHorizon: 1 },
      );
      expect(invalidAlpha.levels.WIN.significant).toBe(false);

      const invalidBaseline = E.analyzePivotSignificanceEnhanced(
        { WIN: { successes: 100, trials: 100 }, BAD: null },
        'RANGE', 'weekly',
        { customBaselines: { WIN: NaN, BAD: 0.5 }, overlapHorizon: 1 },
      );
      expect(invalidBaseline.levels.WIN.hasSufficientData).toBe(false);
      expect(invalidBaseline.levels.WIN.significant).toBe(false);
      expect(invalidBaseline.levels.BAD.hasSufficientData).toBe(false);

      const zeroBaseline = E.analyzePivotSignificanceEnhanced(
        { ZERO: { successes: 1, trials: 100 } },
        'RANGE', 'weekly',
        { customBaselines: { ZERO: 0 }, overlapHorizon: 1 },
      );
      expect(zeroBaseline.levels.ZERO.baseline).toBe(0);
      expect(E.analyzePivotSignificanceEnhanced(null).summary.totalLevels).toBe(0);
    });

    test('malformed levels remain in the predeclared BH family as p=1', () => {
      const out = E.analyzePivotSignificanceEnhanced(
        { A: { successes: 16, trials: 20 }, BAD: null },
        'RANGE', 'weekly',
        { customBaselines: { A: 0.56, BAD: 0.5 }, overlapHorizon: 1 },
      );
      expect(out.levels.A.pValue).toBeLessThan(0.05);
      expect(out.levels.A.qValue).toBeGreaterThan(0.05);
      expect(out.levels.A.significant).toBe(false);
    });

    test('absent levels remain in the declared BH family as p=1', () => {
      const familyMembers = ['R3', 'R2', 'R1', 'PIVOT', 'S1', 'S2', 'S3'];
      const out = E.analyzePivotSignificanceEnhanced(
        { R1: { successes: 62, trials: 100 } },
        'RANGE', 'weekly',
        { overlapHorizon: 1 },
      );

      expect(out.levels.R1.pValue).toBeLessThan(0.05);
      expect(out.levels.R1.qValue).toBeGreaterThan(0.05);
      expect(out.levels.R1.significant).toBe(false);
      expect(out.summary.totalLevels).toBe(7);
      expect(out.summary.insufficientDataLevels).toBe(6);
      expect(out.familyInfo.familyMembers).toEqual(familyMembers);
    });

    test('overlap horizon supplies the conservative n_obs / h lower bound', () => {
      const out = E.analyzePivotSignificanceEnhanced(
        { WIN: { successes: 95, trials: 100 } },
        'RANGE', 'weekly',
        { customBaselines: { WIN: 0.5 }, overlapHorizon: 5 },
      );
      expect(out.levels.WIN.nEffective).toBe(20);
      const missing = E.analyzePivotSignificanceEnhanced(
        { WIN: { successes: 95, trials: 100 } },
        'RANGE', 'weekly',
        { customBaselines: { WIN: 0.5 } },
      );
      expect(missing.levels.WIN.nEffective).toBe(0);
      expect(missing.levels.WIN.hasSufficientData).toBe(false);
    });

    test('overlap correction uses effective observations for inference', () => {
      const out = E.analyzePivotSignificanceEnhanced(
        { WIN: { successes: 60, trials: 100 } },
        'RANGE', 'weekly',
        { customBaselines: { WIN: 0.5 }, overlapHorizon: 5 },
      );
      const r = out.levels.WIN;
      expect(r.nEffective).toBe(20);
      expect(r.successesEffective).toBe(12);
      expect(r.pValue).toBeGreaterThan(0.1);
      expect(r.pValue).toBeLessThan(0.25);
      expect(r.confidenceInterval.lower).toBeCloseTo(
        E.wilsonConfidenceInterval(12, 20, 0.95).lower * 100,
        12,
      );
      expect(r.significant).toBe(false);
    });

    test('two-sided overlap inference retains the observed proportion', () => {
      const out = E.analyzePivotSignificanceEnhanced(
        { WIN: { successes: 29, trials: 100 } },
        'RANGE', 'weekly',
        { customBaselines: { WIN: 0.5 }, overlapHorizon: 5, oneSided: false },
      );
      const r = out.levels.WIN;
      expect(r.nEffective).toBe(20);
      expect(r.successesEffective).toBeCloseTo(5.8, 12);
      expect(r.confidenceInterval.center).toBeGreaterThan(29);
      expect(r.confidenceInterval.center).toBeLessThan(50);
      expect(r.pValue).toBeGreaterThan(0.05);
      expect(r.significant).toBe(false);
    });

    test('empty custom baselines fail closed without deriving family from results', () => {
      const sparse = E.analyzePivotSignificanceEnhanced(
        { A: { successes: 60, trials: 100 } },
        'RANGE', 'weekly',
        { customBaselines: {}, overlapHorizon: 1 },
      );
      const expanded = E.analyzePivotSignificanceEnhanced(
        { A: { successes: 60, trials: 100 }, B: null },
        'RANGE', 'weekly',
        { customBaselines: {}, overlapHorizon: 1 },
      );
      expect(sparse.summary.totalLevels).toBe(0);
      expect(expanded.summary.totalLevels).toBe(0);
      expect(sparse.familyInfo.familyMembers).toEqual([]);
      expect(expanded.familyInfo.familyMembers).toEqual([]);
    });

    test('unsafe explicit family members fail closed', () => {
      const out = E.analyzePivotSignificanceEnhanced(
        JSON.parse('{"__proto__":{"successes":100,"trials":100}}'),
        'RANGE', 'weekly',
        { familyMembers: ['__proto__'], overlapHorizon: 1 },
      );
      expect(out.summary.totalLevels).toBe(0);
      expect(out.summary.validLevels).toBe(0);
      expect(Object.prototype.hasOwnProperty.call(out.levels, '__proto__')).toBe(false);
      expect(JSON.stringify(out.levels)).toBe('{}');
    });
  });

  describe('generateTooltip / formatSignificanceDisplay', () => {
    let out;
    beforeAll(() => {
      out = E.analyzePivotSignificanceEnhanced(
        { WIN: { successes: 95, trials: 100 }, SMALL: { successes: 4, trials: 5 } },
        'RANGE', 'weekly',
        { customBaselines: { WIN: 0.5, SMALL: 0.5 }, overlapHorizon: 1 },
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
