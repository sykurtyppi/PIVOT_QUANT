/**
 * @jest-environment jsdom
 */
/**
 * Tests for enhanced_pivot_display.js — the regime-aware significance table,
 * summary panel, legend/controls, styles, and the complete-display assembler.
 *
 * Browser-global IIFE; builds DOM from a fdrResults shape. Deterministic, so
 * cell formatting and structure are pinned directly. The complete display is
 * exercised both with the real EnhancedFDRCorrection engine and with it absent
 * (fallback view).
 */

const level = (over = {}) => ({
  successRate: 63, successes: 63, trials: 100, baseline: 50, lift: 13,
  nEffective: 80, hasSufficientData: true, significant: true,
  confidenceInterval: { lower: 55, upper: 71 },
  pValue: 0.001, qValue: 0.005, permutationPValue: 0.02,
  ...over,
});

const SIG = level();
const NOTSIG = level({ significant: false, lift: -5 });
const INSUF = level({
  hasSufficientData: false, significant: false, trials: 5, nEffective: 4,
  successRate: 80, successes: 4, lift: 30, confidenceInterval: { lower: 0, upper: 100 },
  pValue: null, qValue: null, permutationPValue: null,
});

const fdrResults = (over = {}) => ({
  levels: { R3: SIG, R2: NOTSIG, R1: INSUF, PIVOT: SIG, S1: NOTSIG, S2: INSUF, S3: INSUF, ...(over.levels || {}) },
  summary: {
    regime: 'RANGE', timeframe: 'weekly', significantLevels: 2, validLevels: 4,
    insufficientDataLevels: 3, totalLevels: 7, testType: 'one-sided', fdrLevel: 0.05,
    ...(over.summary || {}),
  },
});

describe('enhanced_pivot_display.js — EnhancedPivotDisplay (jsdom)', () => {
  let D;

  beforeAll(async () => {
    await import('../enhanced_fdr_correction.js'); // window.EnhancedFDRCorrection, for the complete-display test
    await import('../enhanced_pivot_display.js');
    D = window.EnhancedPivotDisplay;
  });

  test('exposes the public API', () => {
    for (const fn of ['createCompleteEnhancedDisplay', 'createEnhancedLevelReliabilityTable', 'createLegendAndControls', 'createSummaryPanel', 'addEnhancedStyles']) {
      expect(typeof D[fn]).toBe('function');
    }
  });

  describe('createEnhancedLevelReliabilityTable', () => {
    test('one row per present level, with the significance-driven row class', () => {
      const t = D.createEnhancedLevelReliabilityTable(fdrResults(), 'RANGE', 'weekly', false, null);
      const rows = t.querySelectorAll('tbody tr');
      expect(rows).toHaveLength(7);
      expect(t.querySelectorAll('thead th')).toHaveLength(7); // no Debug column
      // order is R3,R2,R1,PIVOT,S1,S2,S3
      expect(rows[0].classList.contains('significant')).toBe(true); // R3 sig
      expect(rows[1].classList.contains('not-significant')).toBe(true); // R2
      expect(rows[2].classList.contains('insufficient-data')).toBe(true); // R1 insuf
    });

    test('skips levels absent from fdrResults.levels', () => {
      const r = { levels: { R1: SIG, S1: NOTSIG, S3: INSUF }, summary: fdrResults().summary };
      const t = D.createEnhancedLevelReliabilityTable(r, 'RANGE', 'weekly', false, null);
      expect(t.querySelectorAll('tbody tr')).toHaveLength(3);
    });

    test('formats the significant row cells exactly', () => {
      const t = D.createEnhancedLevelReliabilityTable(fdrResults(), 'RANGE', 'weekly', false, null);
      const r0 = t.querySelectorAll('tbody tr')[0]; // R3 (sig)
      expect(r0.querySelector('.success-rate').textContent).toBe('63.0% (63/100)');
      expect(r0.querySelector('.baseline').textContent).toBe('50.0%');
      const lift = r0.querySelector('.lift');
      expect(lift.textContent).toBe('+13.0%');
      expect(lift.classList.contains('positive')).toBe(true);
      expect(r0.querySelector('.confidence-interval').textContent).toBe('[55.0%, 71.0%]');
      expect(r0.querySelector('.sample-size').textContent).toBe('100 (80)');
      expect(r0.querySelector('.significance-cell .sig-pass').textContent).toBe('✓');
    });

    test('negative lift and insufficient-data markers', () => {
      const t = D.createEnhancedLevelReliabilityTable(fdrResults(), 'RANGE', 'weekly', false, null);
      const rows = t.querySelectorAll('tbody tr');
      const lift = rows[1].querySelector('.lift'); // R2 notSig lift -5
      expect(lift.textContent).toBe('-5.0%');
      expect(lift.classList.contains('negative')).toBe(true);
      expect(rows[1].querySelector('.significance-cell .sig-fail').textContent).toBe('✗');
      // R1 insufficient
      expect(rows[2].querySelector('.significance-cell .sig-gray').textContent).toBe('—');
      expect(rows[2].querySelector('.sample-size').classList.contains('insufficient')).toBe(true);
    });

    test('debug mode adds the column and p/q/p_perm (or N/A)', () => {
      const t = D.createEnhancedLevelReliabilityTable(fdrResults(), 'RANGE', 'weekly', true, null);
      expect(t.querySelectorAll('thead th')).toHaveLength(8);
      const rows = t.querySelectorAll('tbody tr');
      expect(rows[0].querySelector('.debug-info').textContent).toBe('p=0.0010, q=0.0050, p_perm=0.0200');
      expect(rows[2].querySelector('.debug-info').textContent).toBe('N/A'); // insufficient
    });
  });

  test('createSummaryPanel renders regime, counts, test type and FDR level', () => {
    const p = D.createSummaryPanel(fdrResults());
    expect(p.className).toBe('significance-summary-panel');
    const txt = p.textContent;
    expect(txt).toContain('RANGE');
    expect(txt).toContain('weekly');
    expect(txt).toContain('ONE-SIDED');
    expect(txt).toContain('FDR = 5.0%');
  });

  describe('createLegendAndControls', () => {
    test('renders both toggles; one-sided leaves the test-mode box unchecked', () => {
      const c = D.createLegendAndControls(fdrResults(), false, null);
      expect(c.querySelector('#test-mode-toggle').checked).toBe(false);
      expect(c.querySelector('#debug-mode-toggle')).not.toBeNull();
      expect(c.querySelector('.legend-note').textContent).toContain('RANGE');
    });

    test('two-sided checks the test-mode box; onToggle fires on change', () => {
      const onToggle = jest.fn();
      const c = D.createLegendAndControls(fdrResults({ summary: { ...fdrResults().summary, testType: 'two-sided' } }), true, onToggle);
      expect(c.querySelector('#test-mode-toggle').checked).toBe(true);
      const debugBox = c.querySelector('#debug-mode-toggle');
      debugBox.checked = false;
      debugBox.dispatchEvent(new window.Event('change'));
      expect(onToggle).toHaveBeenCalledWith('debugMode', false);
    });
  });

  test('addEnhancedStyles injects the stylesheet once (idempotent)', () => {
    D.addEnhancedStyles();
    D.addEnhancedStyles();
    expect(document.querySelectorAll('#enhanced-significance-styles')).toHaveLength(1);
  });

  describe('createCompleteEnhancedDisplay', () => {
    test('assembles summary + table + legend with the real engine', () => {
      const container = D.createCompleteEnhancedDisplay(
        { R3: { successes: 95, trials: 100 }, PIVOT: { successes: 50, trials: 100 } },
        'RANGE', 'weekly', {},
      );
      expect(container.className).toBe('enhanced-significance-display');
      expect(container.querySelector('.significance-summary-panel')).not.toBeNull();
      expect(container.querySelector('table.level-reliability-table.enhanced')).not.toBeNull();
      expect(container.querySelector('.significance-legend-container')).not.toBeNull();
    });

    test('falls back to an all-insufficient view when the engine is absent', () => {
      const saved = window.EnhancedFDRCorrection;
      delete window.EnhancedFDRCorrection;
      const warn = jest.spyOn(console, 'warn').mockImplementation(() => {});
      try {
        const container = D.createCompleteEnhancedDisplay({}, 'RANGE', 'weekly', {});
        const rows = container.querySelectorAll('table tbody tr');
        expect(rows).toHaveLength(7); // all seven levels present in the fallback
        expect(container.querySelectorAll('tbody tr.insufficient-data')).toHaveLength(7);
        expect(warn).toHaveBeenCalled();
      } finally {
        warn.mockRestore();
        window.EnhancedFDRCorrection = saved;
      }
    });
  });
});
