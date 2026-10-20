/**
 * @jest-environment jsdom
 */
/**
 * Tests for volatility_analysis.js — rolling realized volatility, percentile
 * ranking, regime classification, per-volatility subfamily splitting/analysis,
 * and the matrix/styles DOM builders. Browser-global IIFE; consumes
 * EnhancedFDRCorrection for the subfamily analysis (loaded here).
 */

describe('volatility_analysis.js — VolatilityAnalysis (jsdom)', () => {
  let V;

  beforeAll(async () => {
    await import('../enhanced_fdr_correction.js'); // window.EnhancedFDRCorrection
    await import('../volatility_analysis.js');
    V = window.VolatilityAnalysis;
  });

  test('exposes the public API', () => {
    for (const fn of ['classifyVolatility', 'calculateRollingVolatility', 'runVolatilitySubfamilyAnalysis', 'splitDataByVolatility', 'analyzePivotSubfamily', 'createVolatilityMatrix', 'generateMockPriceData', 'addVolatilityStyles', 'calculatePercentiles', 'splitLevelByVolatility', 'createVolMatrixRow']) {
      expect(typeof V[fn]).toBe('function');
    }
  });

  describe('calculateRollingVolatility', () => {
    const bars = (closes) => closes.map((c) => ({ close: c }));

    test('pads the first windowSize-1 entries with null', () => {
      const vols = V.calculateRollingVolatility(bars(Array(20).fill(100)), 20);
      expect(vols).toHaveLength(20);
      for (let i = 0; i < 19; i++) expect(vols[i]).toBeNull();
      expect(vols[19]).not.toBeNull();
    });

    test('constant prices have zero realized volatility', () => {
      const vols = V.calculateRollingVolatility(bars(Array(6).fill(100)), 5);
      expect(vols[5]).toBeCloseTo(0, 12);
    });

    test('varying prices have positive volatility', () => {
      const vols = V.calculateRollingVolatility(bars([100, 110, 100, 115, 95, 120]), 5);
      expect(vols[5]).toBeGreaterThan(0);
    });
  });

  describe('calculatePercentiles', () => {
    test('ranks valid values and preserves nulls', () => {
      expect(V.calculatePercentiles([null, 1, 2, 3, 4])).toEqual([null, 25, 50, 75, 100]);
    });
  });

  describe('classifyVolatility', () => {
    test('nulls the warm-up window and classifies the rest', () => {
      const data = Array(25).fill(0).map(() => ({ close: 100 }));
      const out = V.classifyVolatility(data, 20);
      expect(out).toHaveLength(25);
      expect(out[0]).toEqual({ regime: null, value: null, percentile: null });
      // constant prices -> vol 0 -> percentile 100 -> 'High'
      expect(out[24].regime).toBe('High');
      expect(out[24].percentile).toBe(100);
      for (let i = 20; i < 25; i++) {
        expect(['Low', 'Normal', 'High']).toContain(out[i].regime);
      }
    });
  });

  describe('splitDataByVolatility', () => {
    test('splits each level into three equal-trial volatility buckets', () => {
      const splits = V.splitDataByVolatility({ R1: { successes: 30, trials: 90 } }, {});
      for (const regime of ['Low', 'Normal', 'High']) {
        expect(splits[regime].R1.trials).toBe(30); // floor(90 / 3)
        expect(splits[regime].R1.successes).toBeGreaterThanOrEqual(0);
        expect(splits[regime].R1.successes).toBeLessThanOrEqual(30);
      }
    });

    test('levels with too few trials per bucket are dropped', () => {
      const splits = V.splitDataByVolatility({ R1: { successes: 1, trials: 2 } }, {});
      expect(splits.Low.R1).toBeUndefined(); // floor(2/3) = 0 trials -> excluded
    });
  });

  test('runVolatilitySubfamilyAnalysis returns the family/summary shape', () => {
    const out = V.runVolatilitySubfamilyAnalysis(
      { R1: { successes: 30, trials: 90 }, R2: { successes: 60, trials: 90 } },
      'RANGE', {}, {},
    );
    expect(out.summary.regime).toBe('RANGE');
    expect(Object.keys(out.families).sort()).toEqual(['High', 'Low', 'Normal']);
  });

  describe('createVolatilityMatrix / createVolMatrixRow', () => {
    const results = {
      families: {
        Low: {
          levels: { R1: { hasSufficientData: true, significant: true, qValue: 0.0005, successRate: 70, baseline: 50, lift: 20, nEffective: 40 } },
          summary: { hasData: true, significantLevels: 1, validLevels: 1 },
        },
        Normal: { levels: {}, summary: { hasData: false, significantLevels: 0, validLevels: 0 } },
        High: { levels: {}, summary: { hasData: false, significantLevels: 0, validLevels: 0 } },
      },
    };

    test('matrix renders three regime rows and the regime title', () => {
      const m = V.createVolatilityMatrix(results, 'RANGE');
      expect(m.className).toBe('volatility-matrix enhanced');
      expect(m.querySelectorAll('.matrix-row')).toHaveLength(3);
      expect(m.textContent).toContain('Volatility × RANGE Matrix');
    });

    test('a significant row cell shows rate, q and lift', () => {
      const rowHtml = V.createVolMatrixRow('Low', ['R1'], results);
      expect(rowHtml).toContain('Low Vol');
      expect(rowHtml).toContain('1/1 sig');
      expect(rowHtml).toContain('vol-matrix-cell significant');
      expect(rowHtml).toContain('70%');
      expect(rowHtml).toContain('q=<0.001'); // 0.0005 < 0.001
    });
  });

  test('addVolatilityStyles injects the stylesheet once (idempotent)', () => {
    V.addVolatilityStyles();
    V.addVolatilityStyles();
    expect(document.querySelectorAll('#volatility-analysis-styles')).toHaveLength(1);
  });

  test('generateMockPriceData returns n OHLC bars with positive closes', () => {
    const data = V.generateMockPriceData(10);
    expect(data).toHaveLength(10);
    for (const bar of data) {
      expect(bar.close).toBeGreaterThan(0);
      expect(typeof bar.open).toBe('number');
      expect(typeof bar.high).toBe('number');
      expect(typeof bar.low).toBe('number');
    }
  });
});
