/**
 * Tests for advanced_pivot_engine.js — ATR (Wilder), standard/Fibonacci/
 * Camarilla pivots, ATR zones, pivot-strength analysis, and formatting.
 *
 * Browser-global IIFE (`window.advancedPivotEngine = (() => …)()`), loaded via a
 * globalThis.window stub + dynamic import; pure math, no DOM.
 */

describe('advanced_pivot_engine.js — advancedPivotEngine', () => {
  let E;

  beforeAll(async () => {
    globalThis.window = {};
    await import('../advanced_pivot_engine.js');
    E = globalThis.window.advancedPivotEngine;
  });

  const flat = (n) => Array.from({ length: n }, () => ({ high: 11, low: 10, close: 10, volume: 100 }));

  test('exposes the documented public API', () => {
    for (const fn of ['calculateAdvancedPivots', 'calculateATR', 'getZoneConfiguration', 'formatPivotData', 'analyzePivotStrength']) {
      expect(typeof E[fn]).toBe('function');
    }
  });

  describe('calculateATR (Wilder)', () => {
    test('returns null below period + 1 candles', () => {
      expect(E.calculateATR(flat(2), 2)).toBeNull();
    });

    test('constant true range yields that range', () => {
      // every bar high-low=1, flat closes -> every TR=1 -> ATR=1
      expect(E.calculateATR(flat(3), 2)).toBeCloseTo(1, 9);
    });

    test('a close-to-high gap drives the true range', () => {
      const candles = [
        { high: 10, low: 10, close: 10 },
        { high: 15, low: 14, close: 14 }, // tr = max(1, |15-10|, |14-10|) = 5
      ];
      expect(E.calculateATR(candles, 1)).toBeCloseTo(5, 9);
    });
  });

  describe('calculateAdvancedPivots', () => {
    test('insufficient data returns null', () => {
      expect(E.calculateAdvancedPivots(null)).toBeNull();
      expect(E.calculateAdvancedPivots(flat(2), 'standard', 2)).toBeNull();
    });

    test('standard pivots from the latest bar (H=110 L=90 C=100)', () => {
      const candles = [...flat(2), { high: 110, low: 90, close: 100, volume: 100 }];
      const out = E.calculateAdvancedPivots(candles, 'standard', 2);
      expect(out.type).toBe('standard');
      const p = out.pivotLevels;
      expect(p.PIVOT).toBeCloseTo(100, 9);
      expect(p.R1).toBeCloseTo(110, 9);
      expect(p.R2).toBeCloseTo(120, 9);
      expect(p.R3).toBeCloseTo(130, 9);
      expect(p.S1).toBeCloseTo(90, 9);
      expect(p.S2).toBeCloseTo(80, 9);
      expect(p.S3).toBeCloseTo(70, 9);
      // < 20 candles -> no gamma-flip level is added
      expect(p).not.toHaveProperty('GAMMA_FLIP');
    });

    test('20+ candles add a finite GAMMA_FLIP level (VWAP proxy)', () => {
      const out = E.calculateAdvancedPivots(flat(20), 'standard', 14);
      expect(out.pivotLevels).toHaveProperty('GAMMA_FLIP');
      expect(Number.isFinite(out.pivotLevels.GAMMA_FLIP)).toBe(true);
      expect(typeof out.gammaFlip).toBe('string');
    });

    test('fibonacci and camarilla variants compute their documented levels', () => {
      const candles = [...flat(2), { high: 110, low: 90, close: 100, volume: 100 }];
      const fib = E.calculateAdvancedPivots(candles, 'fibonacci', 2).pivotLevels;
      expect(fib['R1.000']).toBeCloseTo(120, 9); // pivot + range (100 + 20)
      expect(fib['R0.618']).toBeCloseTo(112.36, 9); // 100 + 0.618*20
      expect(fib['S1.000']).toBeCloseTo(80, 9);

      const cam = E.calculateAdvancedPivots(candles, 'camarilla', 2).pivotLevels;
      expect(cam.PIVOT).toBeCloseTo(100, 9); // camarilla pivot = close
      expect(cam.R1).toBeCloseTo(100 + (20 * 1.1) / 12, 9);
      expect(cam.S4).toBeCloseTo(89, 9); // 100 - 20*1.1/2
    });
  });

  describe('getZoneConfiguration', () => {
    test('known type returns its colour set; unknown falls back to standard', () => {
      expect(E.getZoneConfiguration('standard').colors).toHaveProperty('resistance');
      expect(E.getZoneConfiguration('nonsense')).toEqual(E.getZoneConfiguration('standard'));
    });
  });

  describe('formatPivotData', () => {
    test('null input returns null', () => {
      expect(E.formatPivotData(null)).toBeNull();
    });

    test('sorts price-descending and classifies level types', () => {
      const advancedData = {
        pivotLevels: { R1: 110, S1: 90, PIVOT: 100 },
        strength: {
          R1: { strength: 50, reliability: 'Medium', distanceFromCurrent: '1.00' },
          S1: { strength: 30, reliability: 'Low', distanceFromCurrent: '-1.00' },
          PIVOT: { strength: 0, reliability: 'Low', distanceFromCurrent: '0.00' },
        },
        atrZones: {
          R1: { zones: { normal: { high: 111, low: 109 } } },
          S1: { zones: { normal: { high: 91, low: 89 } } },
          PIVOT: { zones: { normal: { high: 101, low: 99 } } },
        },
      };
      const out = E.formatPivotData(advancedData);
      expect(out.map((r) => r.label)).toEqual(['R1', 'PIVOT', 'S1']);
      expect(out[0]).toMatchObject({ label: 'R1', price: '110.00', type: 'resistance', strength: 50, reliability: 'Medium' });
      expect(out[2].type).toBe('support');
    });
  });

  describe('analyzePivotStrength', () => {
    test('a far-away level registers no touches and low reliability', () => {
      const candles = Array.from({ length: 10 }, () => ({ high: 101, low: 99, close: 100 }));
      const out = E.analyzePivotStrength({ R1: 1000 }, candles, 100);
      expect(out.R1.touches).toBe(0);
      expect(out.R1.strength).toBe(0);
      expect(out.R1.reliability).toBe('Low');
      expect(out.R1.distanceFromCurrent).toBe('900.00');
    });

    test('a straddling level registers touches and reports Medium reliability', () => {
      // every bar's [low, high] brackets 100 -> a touch each bar; flat closes
      // never cross back, so bounces stay 0 (strength 0) but touches >= 2.
      const candles = Array.from({ length: 10 }, () => ({ high: 101, low: 99, close: 100 }));
      const out = E.analyzePivotStrength({ PIVOT: 100 }, candles, 100);
      expect(out.PIVOT.touches).toBe(10);
      expect(out.PIVOT.strength).toBe(0);
      expect(out.PIVOT.reliability).toBe('Medium');
      expect(out.PIVOT.distanceFromCurrent).toBe('0.00');
    });
  });
});
