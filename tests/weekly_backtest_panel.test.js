/**
 * @jest-environment jsdom
 */
/**
 * Tests for weekly_backtest_panel.js — focused on the deterministic,
 * non-orchestration surface: computePriorWeekPivots (weekly pivot math over the
 * ISO-week aggregation) with its data guards, and the exports' fail-closed
 * "run the backtest first" path. The runWeeklyBacktest orchestration (depends on
 * window.backtestEngine + URL.createObjectURL, unimplemented in jsdom) is left
 * to an integration layer, not unit-tested here.
 */

// daily bar at 12:00 UTC on the given date (month is 1-based for readability)
const bar = (y, m, d, o, h, l, c) => ({
  timestamp: Date.UTC(y, m - 1, d, 12) / 1000,
  date: `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`,
  open: o, high: h, low: l, close: c,
});

describe('weekly_backtest_panel.js — weeklyBacktestPanel (jsdom)', () => {
  let W;

  beforeAll(async () => {
    await import('../weekly_backtest_panel.js');
    W = window.weeklyBacktestPanel;
  });

  test('exposes the public API', () => {
    expect(typeof W.computePriorWeekPivots).toBe('function');
    expect(typeof W.exportWeeklyCSV).toBe('function');
    expect(typeof W.exportWeeklyReport).toBe('function');
    expect(W.state).toBeDefined();
  });

  describe('computePriorWeekPivots', () => {
    // Prior week (Jun 4-7 2024, one ISO week): high 112, low 88, close 106 (last bar).
    // Current week (Jun 18-20 2024, a later ISO week) is excluded as the latest.
    const twoWeeks = [
      bar(2024, 6, 4, 100, 110, 90, 100),
      bar(2024, 6, 5, 100, 112, 95, 105),
      bar(2024, 6, 6, 105, 108, 88, 102),
      bar(2024, 6, 7, 102, 111, 92, 106), // last prior-week bar -> close 106
      bar(2024, 6, 18, 115, 120, 100, 115),
      bar(2024, 6, 19, 115, 121, 101, 116),
      bar(2024, 6, 20, 116, 122, 102, 118),
    ];

    test('derives pivots from the prior (second-to-last) week H/L/C', () => {
      const r = W.computePriorWeekPivots(twoWeeks);
      // pivot = (112 + 88 + 106) / 3 = 102 ; range = 24
      expect(r.high).toBe(112);
      expect(r.low).toBe(88);
      expect(r.close).toBe(106);
      expect(r.pivot).toBeCloseTo(102, 9);
      expect(r.range).toBe(24);
      expect(r.R1).toBeCloseTo(116, 9); // 2*102 - 88
      expect(r.R2).toBeCloseTo(126, 9); // 102 + 24
      expect(r.R3).toBeCloseTo(140, 9); // 2*102 - 88 + 24
      expect(r.S1).toBeCloseTo(92, 9); // 2*102 - 112
      expect(r.S2).toBeCloseTo(78, 9); // 102 - 24
      expect(r.S3).toBeCloseTo(68, 9); // 2*102 - 112 - 24
    });

    test('caches the result on state.priorWeekData', () => {
      const r = W.computePriorWeekPivots(twoWeeks);
      expect(r).toBe(W.state.priorWeekData);
    });

    test('throws below 7 bars', () => {
      expect(() => W.computePriorWeekPivots([])).toThrow('Insufficient data');
      expect(() => W.computePriorWeekPivots(twoWeeks.slice(0, 6))).toThrow('Insufficient data');
    });

    test('throws when all bars fall in a single week', () => {
      // Jun 3-9 2024 is one ISO week (Mon-Sun) -> only one aggregated week
      const oneWeek = [3, 4, 5, 6, 7, 8, 9].map((d) => bar(2024, 6, d, 100, 110, 90, 100));
      expect(() => W.computePriorWeekPivots(oneWeek)).toThrow('at least 2 weeks');
    });
  });

  describe('exports fail closed before a backtest has run', () => {
    beforeEach(() => {
      W.state.weeklyResults = null;
    });

    test('exportWeeklyCSV alerts and does not reach the download path', () => {
      const alert = jest.spyOn(window, 'alert').mockImplementation(() => {});
      try {
        expect(() => W.exportWeeklyCSV()).not.toThrow();
        expect(alert).toHaveBeenCalledWith('Run weekly backtest first.');
      } finally {
        alert.mockRestore();
      }
    });

    test('exportWeeklyReport alerts and does not reach the download path', () => {
      const alert = jest.spyOn(window, 'alert').mockImplementation(() => {});
      try {
        expect(() => W.exportWeeklyReport()).not.toThrow();
        expect(alert).toHaveBeenCalledWith('Run weekly backtest first.');
      } finally {
        alert.mockRestore();
      }
    });
  });
});
