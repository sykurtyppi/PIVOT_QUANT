/**
 * @jest-environment jsdom
 */
/**
 * Tests for pivot_fdr_integration.js — wires the FDR engine into the pivot
 * table display. First DOM module under the jsdom harness (per-file docblock
 * above; the default env stays node for every other suite).
 *
 * The module is a browser-global IIFE that registers a DOMContentLoaded handler
 * at load, so it needs a real `document` (jsdom provides it). Its render path
 * also calls host globals (findConfluenceZones/getStrength) that live in the
 * main calculator, so those are stubbed here.
 */

describe('pivot_fdr_integration.js — PivotFDRIntegration (jsdom)', () => {
  let PFI;

  beforeAll(async () => {
    await import('../fdr_correction.js'); // sets window.FDRCorrection (the engine it consults)
    await import('../pivot_fdr_integration.js'); // sets window.PivotFDRIntegration
    PFI = window.PivotFDRIntegration;
  });

  test('exposes the public API', () => {
    expect(typeof PFI.initialize).toBe('function');
    expect(typeof PFI.displayResultsWithFDR).toBe('function');
    expect(typeof PFI.testWithSampleData).toBe('function');
    expect(PFI.SAMPLE_HISTORICAL_DATA).toBeDefined();
  });

  test('SAMPLE_HISTORICAL_DATA carries the seven standard levels', () => {
    const d = PFI.SAMPLE_HISTORICAL_DATA;
    expect(Object.keys(d).sort()).toEqual(['PIVOT', 'R1', 'R2', 'R3', 'S1', 'S2', 'S3']);
    for (const k of Object.keys(d)) {
      expect(d[k].trials).toBe(52);
      expect(d[k].successes).toBeLessThanOrEqual(52);
    }
  });

  test('initialize injects FDR styles once and swaps window.displayResults', () => {
    const original = function originalDisplayResults() {};
    window.displayResults = original;
    PFI.initialize();
    expect(document.getElementById('fdr-styles')).not.toBeNull();
    expect(window.originalDisplayResults).toBe(original);
    expect(window.displayResults).toBe(PFI.displayResultsWithFDR);
    PFI.initialize(); // idempotent: the style element is not duplicated
    expect(document.querySelectorAll('#fdr-styles').length).toBe(1);
  });

  describe('displayResultsWithFDR rendering', () => {
    const data = {
      standard: { R3: 110, R2: 108, R1: 105, Pivot: 100, S1: 95, S2: 92, S3: 90 },
      camarilla: {},
      fibonacci: {},
      current: 100,
    };

    beforeEach(() => {
      document.body.innerHTML = '<div id="tables-container"></div>';
      window.currentPivotType = 'standard';
      // host-app globals the render path calls directly
      globalThis.findConfluenceZones = () => [];
      globalThis.getStrength = () => 'strength-strong';
    });

    test('renders a 7-row pivot table with significance cells and an FDR summary', () => {
      PFI.displayResultsWithFDR(data);
      const container = document.getElementById('tables-container');
      const rows = container.querySelectorAll('table.pivot-table tbody tr');
      expect(rows).toHaveLength(7);
      // rows are sorted by price descending, so R3 (110) is first
      expect(rows[0].querySelector('.level-label').textContent).toBe('R3');
      expect(container.querySelectorAll('.significance-cell')).toHaveLength(7);
      const summary = container.querySelector('.fdr-summary');
      expect(summary).not.toBeNull();
      expect(summary.textContent).toContain('levels significant');
    });

    test('distance cell marks a level at the current price', () => {
      PFI.displayResultsWithFDR(data);
      // Pivot (100) == current (100) -> |distance| < 5 -> "AT PRICE"
      const atPrice = document.querySelector('.distance-cell.at-price');
      expect(atPrice).not.toBeNull();
      expect(atPrice.textContent).toContain('AT PRICE');
    });

    test('falls back to No-data cells without throwing when the FDR engine is absent', () => {
      const saved = window.FDRCorrection;
      delete window.FDRCorrection;
      const warn = jest.spyOn(console, 'warn').mockImplementation(() => {});
      try {
        expect(() => PFI.displayResultsWithFDR(data)).not.toThrow();
        expect(document.querySelectorAll('.significance-cell.no-data')).toHaveLength(7);
        expect(warn).toHaveBeenCalled();
      } finally {
        warn.mockRestore();
        window.FDRCorrection = saved;
      }
    });
  });
});
