/**
 * Tests for advanced_analytics.js — level scoring, walk-forward stability,
 * window stats, binomial test, volatility split, alert rules/evaluation,
 * bias, ATR stops, sparkline geometry, and the data-integrity hash.
 *
 * Browser-global IIFE (`window.AdvancedAnalytics = (() => …)()`), loaded via a
 * globalThis.window stub + dynamic import; pure logic, no DOM.
 */

describe('advanced_analytics.js — AdvancedAnalytics', () => {
  let A;

  beforeAll(async () => {
    globalThis.window = {};
    await import('../advanced_analytics.js');
    A = globalThis.window.AdvancedAnalytics;
  });

  test('exposes the documented public API', () => {
    for (const fn of [
      'computeLevelScore', 'calculateWalkForwardStability', 'generateTopPicks',
      'splitByVolatility', 'createAnalysisMetadata', 'createAlertRules',
      'evaluateAlerts', 'determineBias', 'calculateATRStops', 'generateSparklineData',
      'calculateWindowStats', 'calculateBinomialTest', 'calculateSHA1', 'generateComputationId',
    ]) {
      expect(typeof A[fn]).toBe('function');
    }
  });

  describe('computeLevelScore', () => {
    test('non-finite inputs and too-small samples score 0', () => {
      expect(A.computeLevelScore({ q: NaN, lift: 10, ciWidth: 10, nEff: 50 })).toBe(0);
      expect(A.computeLevelScore({ q: 0.001, lift: 30, ciWidth: 0, nEff: 5 })).toBe(0);
    });

    test('a maximal input clamps to 1', () => {
      expect(A.computeLevelScore({ q: 0, lift: 30, ciWidth: 0, nEff: 100 })).toBeCloseTo(1, 9);
    });

    test('weighted combination matches the documented formula', () => {
      // sig .5 (.4) + effect .5 (.3) + precision .5 (.2) + sample 0 (.1) + dir .1 = 0.55
      expect(A.computeLevelScore({ q: 0.05, lift: 15, ciWidth: 20, nEff: 10 })).toBeCloseTo(0.55, 9);
    });

    test('positive lift earns the direction bonus over equal negative lift', () => {
      const pos = A.computeLevelScore({ q: 0.1, lift: 30, ciWidth: 40, nEff: 100 });
      const neg = A.computeLevelScore({ q: 0.1, lift: -30, ciWidth: 40, nEff: 100 });
      expect(pos).toBeCloseTo(0.5, 9);
      expect(neg).toBeCloseTo(0.4, 9);
    });
  });

  describe('calculateBinomialTest', () => {
    test('zero trials -> 1.0; at the null -> ~1; extreme -> ~0; bounded', () => {
      expect(A.calculateBinomialTest(0, 0, 0.5)).toBe(1.0);
      expect(A.calculateBinomialTest(50, 100, 0.5)).toBeCloseTo(1.0, 4);
      expect(A.calculateBinomialTest(90, 100, 0.5)).toBeLessThan(1e-4);
      const p = A.calculateBinomialTest(7, 10, 0.5);
      expect(p).toBeGreaterThanOrEqual(0);
      expect(p).toBeLessThanOrEqual(1);
    });
  });

  describe('calculateWindowStats', () => {
    test('rejects windows below 10 trials', () => {
      expect(A.calculateWindowStats(Array(9).fill({ success: true })).isValid).toBe(false);
    });

    test('computes success rate and lift against the 50% baseline', () => {
      const data = [...Array(7).fill({ success: true }), ...Array(3).fill({ success: false })];
      const s = A.calculateWindowStats(data);
      expect(s.isValid).toBe(true);
      expect(s.successes).toBe(7);
      expect(s.trials).toBe(10);
      expect(s.successRate).toBeCloseTo(70, 9);
      expect(s.lift).toBeCloseTo(20, 9);
      expect(s.baseline).toBeCloseTo(50, 9);
    });
  });

  describe('calculateWalkForwardStability', () => {
    test('too-short series is UNKNOWN', () => {
      const ts = Array(10).fill({ success: true });
      expect(A.calculateWalkForwardStability(ts, 10, 5)).toEqual({ variance: null, trend: null, stability: 'UNKNOWN' });
    });

    test('fewer than 3 valid windows is INSUFFICIENT', () => {
      const ts = Array(15).fill({ success: true });
      expect(A.calculateWalkForwardStability(ts, 10, 5).stability).toBe('INSUFFICIENT');
    });

    test('a steady series is STABLE with zero variance and flat trend', () => {
      const ts = Array(25).fill({ success: true });
      const out = A.calculateWalkForwardStability(ts, 10, 5);
      expect(out.stability).toBe('STABLE');
      expect(out.variance).toBeCloseTo(0, 9);
      expect(out.trend).toBeCloseTo(0, 9);
      expect(out.liftSeries.length).toBe(4);
    });
  });

  describe('determineBias', () => {
    test('resistance/support invert reversal vs breakout on lift sign', () => {
      expect(A.determineBias('R1', 5)).toBe('reversal');
      expect(A.determineBias('R1', -5)).toBe('breakout');
      expect(A.determineBias('S1', 5)).toBe('reversal');
      expect(A.determineBias('S1', -5)).toBe('breakout');
    });
    test('pivot maps to directional bias', () => {
      expect(A.determineBias('PIVOT', 5)).toBe('bullish_bias');
      expect(A.determineBias('PIVOT', -5)).toBe('bearish_bias');
    });
  });

  describe('calculateATRStops', () => {
    test('uses explicit price ATR and refuses to fabricate it from confidence data', () => {
      expect(A.calculateATRStops({}, 'reversal', 2)).toBeNull();
      const stops = A.calculateATRStops({ atr: 1.25 }, 'reversal', 2);
      expect(stops.stop_distance).toBeCloseTo(2.5, 9);
      expect(stops.target_distance).toBeCloseTo(3.75, 9);
      expect(stops.risk_reward_ratio).toBe(1.5);
      expect(stops.bias).toBe('reversal');
      expect(stops.atr_multiplier).toBe(2);
    });
  });

  describe('generateSparklineData', () => {
    test('degenerate series returns an empty path', () => {
      expect(A.generateSparklineData([1])).toEqual({ path: '', points: [] });
    });

    test('maps a two-point series into SVG coordinates', () => {
      const s = A.generateSparklineData([0, 10], 60, 20);
      expect(s.min).toBe(0);
      expect(s.max).toBe(10);
      expect(s.points[0]).toMatchObject({ x: 0, y: 20, value: 0 });
      expect(s.points[1]).toMatchObject({ x: 60, y: 0, value: 10 });
      expect(s.path).toBe('M 0.0 20.0 L 60.0 0.0');
    });
  });

  describe('splitByVolatility', () => {
    test('buckets points by percentile and skips points without volatility', () => {
      const out = A.splitByVolatility(
        ['a', 'b', 'c', 'd'],
        [{ percentile: 10 }, { percentile: 33 }, { percentile: 66 }, { percentile: 90 }],
      );
      expect(out.Low).toEqual(['a']);
      expect(out.Normal).toEqual(['b', 'c']); // 33 and 66 both fall in [33, 67)
      expect(out.High).toEqual(['d']);

      const skipped = A.splitByVolatility(['x'], [undefined]);
      expect(skipped).toEqual({ Low: [], Normal: [], High: [] });
    });
  });

  describe('createAlertRules / evaluateAlerts', () => {
    test('rules carry the documented thresholds', () => {
      const r = A.createAlertRules();
      expect(r.significance.strong_signal).toMatchObject({ q_threshold: 0.01, lift_threshold: 15, min_nEff: 30 });
      expect(r.volume.high_conviction.nEff_threshold).toBe(50);
      expect(r.stability.deteriorating.variance_threshold).toBe(150);
    });

    test('a strong result fires HIGH/MEDIUM/LOW significance alerts with a bias', () => {
      const rules = A.createAlertRules();
      const result = { qValue: 0.005, lift: 20, nEffective: 40, confidenceInterval: { upper: 50, lower: 30 } };
      const alerts = A.evaluateAlerts('R1', result, null, rules);
      expect(alerts).toHaveLength(3);
      const strong = alerts.find((a) => a.level === 'strong_signal');
      expect(strong.priority).toBe('HIGH');
      expect(strong.bias).toBe('reversal');
    });

    test('a non-significant result fires nothing', () => {
      const rules = A.createAlertRules();
      const result = { qValue: 0.5, lift: 1, nEffective: 40, confidenceInterval: { upper: 50, lower: 30 } };
      expect(A.evaluateAlerts('R1', result, null, rules)).toEqual([]);
    });

    test('stability direction cannot fire contradictory alerts', () => {
      const rules = A.createAlertRules();
      const result = { qValue: 1, lift: 0, nEffective: 40, confidenceInterval: { upper: 50, lower: 30 } };
      const up = A.evaluateAlerts('R1', result, { variance: 200, trend: 3 }, rules);
      expect(up.filter((a) => a.type === 'stability').map((a) => a.level)).toEqual(['improving']);
      const down = A.evaluateAlerts('R1', result, { variance: 200, trend: -3 }, rules);
      expect(down.filter((a) => a.type === 'stability').map((a) => a.level)).toEqual(['deteriorating']);
    });
  });

  describe('generateTopPicks', () => {
    const fdrResults = {
      levels: {
        R1: { hasSufficientData: true, qValue: 0.001, lift: 25, confidenceInterval: { upper: 60, lower: 50 }, nEffective: 80, successRate: 75, baseline: 50 },
        R2: { hasSufficientData: true, qValue: 0.2, lift: 5, confidenceInterval: { upper: 70, lower: 40 }, nEffective: 15, successRate: 55, baseline: 50 },
        R3: { hasSufficientData: false, qValue: 0.001, lift: 25, confidenceInterval: { upper: 60, lower: 50 }, nEffective: 4, successRate: 75, baseline: 50 },
      },
    };

    test('ranks sufficient levels by score and excludes insufficient ones', () => {
      const picks = A.generateTopPicks(fdrResults, 'RANGE', 2);
      expect(picks).toHaveLength(2);
      expect(picks[0].level).toBe('R1');
      expect(picks[0].score).toBeGreaterThan(picks[1].score);
      expect(picks.map((p) => p.level)).not.toContain('R3');
    });

    test('honours maxPicks', () => {
      expect(A.generateTopPicks(fdrResults, 'RANGE', 1)).toHaveLength(1);
    });

    test('preserves an exact q-value of zero', () => {
      const exact = JSON.parse(JSON.stringify(fdrResults));
      exact.levels.R1.qValue = 0;
      const tiny = JSON.parse(JSON.stringify(fdrResults));
      tiny.levels.R1.qValue = Number.MIN_VALUE;
      expect(A.generateTopPicks(exact, 'RANGE', 1)[0].score)
        .toBeCloseTo(A.generateTopPicks(tiny, 'RANGE', 1)[0].score, 12);
    });
  });

  describe('calculateSHA1 / generateComputationId', () => {
    test('hash is deterministic, input-sensitive, and hex-formatted', () => {
      const a = A.calculateSHA1([{ close: 1 }, { close: 2 }]);
      const again = A.calculateSHA1([{ close: 1 }, { close: 2 }]);
      const other = A.calculateSHA1([{ close: 1 }, { close: 3 }]);
      expect(a).toBe(again);
      expect(a).not.toBe(other);
      expect(a).toMatch(/^[0-9a-f]+$/);
      expect(a.length).toBeGreaterThanOrEqual(8);
    });

    test('computation id is a non-empty string', () => {
      const id = A.generateComputationId();
      expect(typeof id).toBe('string');
      expect(id.length).toBeGreaterThan(5);
    });
  });
});
