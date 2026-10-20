/**
 * @jest-environment jsdom
 */
/**
 * Tests for advanced_ui_components.js — UI builders for the analytics panels.
 * Mix of pure HTML-string helpers (deterministic) and DOM-element builders that
 * consume window.AdvancedAnalytics (stubbed here for control). Browser-global IIFE.
 */

describe('advanced_ui_components.js — AdvancedUIComponents (jsdom)', () => {
  let UI;

  beforeAll(async () => {
    await import('../advanced_ui_components.js');
    UI = window.AdvancedUIComponents;
  });

  test('exposes the public API', () => {
    for (const fn of ['createTopPicksCard', 'createVolatilityMatrix', 'createWalkForwardSummary', 'createAlertsPanel', 'createStabilityBadge', 'createSparklineDisplay', 'addAdvancedStyles', 'createAlertItem', 'createWalkForwardRow']) {
      expect(typeof UI[fn]).toBe('function');
    }
  });

  describe('createStabilityBadge (pure string)', () => {
    test('null / null-variance -> UNKNOWN badge', () => {
      const s = UI.createStabilityBadge(null);
      expect(s).toContain('stability-badge unknown');
      expect(s).toContain('UNKNOWN');
    });

    test('trend sign drives the arrow and class', () => {
      const up = UI.createStabilityBadge({ variance: 5.53, stability: 'STABLE', trend: 2.0 });
      expect(up).toContain('stability-badge stable');
      expect(up).toContain('Variance: 5.5');
      expect(up).toContain('Trend: 2.00');
      expect(up).toContain('↗');
      expect(UI.createStabilityBadge({ variance: 1, stability: 'DRIFTY', trend: -2 })).toContain('↘');
      expect(UI.createStabilityBadge({ variance: 1, stability: 'MODERATE', trend: 0 })).toContain('→');
    });
  });

  describe('createAlertItem (pure string)', () => {
    test('significance alert carries bias, CI and stops', () => {
      const s = UI.createAlertItem({
        type: 'significance', priority: 'HIGH', level_name: 'R2', message: 'strong', bias: 'LONG',
        confidence_interval: { lower: 58.0, upper: 70.0 },
        stops: { stop_distance: 1.234, target_distance: 2.468, risk_reward_ratio: 2 },
      });
      expect(s).toContain('alert-item high');
      expect(s).toContain('📊');
      expect(s).toContain('R2');
      expect(s).toContain('Bias: <strong>LONG</strong>');
      expect(s).toContain('CI: [58.0%, 70.0%]');
      // stop/target/R:R render on separate lines in the markup
      expect(s).toContain('Stop: 1.23');
      expect(s).toContain('Target: 2.47');
      expect(s).toContain('(2:1 R:R)');
    });

    test('stability alert shows variance/trend and no bias block', () => {
      const s = UI.createAlertItem({
        type: 'stability', priority: 'MEDIUM', level_name: 'S1', message: 'drift', variance: 3.14, trend: -0.5,
      });
      expect(s).toContain('alert-item medium');
      expect(s).toContain('⚠️');
      expect(s).toContain('Variance: 3.1');
      expect(s).toContain('Trend: -0.50');
      expect(s).not.toContain('Bias:');
    });

    test('significance alert without ATR stops renders safely (preserved from #85)', () => {
      const s = UI.createAlertItem({
        type: 'significance', priority: 'HIGH', level_name: 'R1', message: 'signal',
        bias: 'reversal', confidence_interval: { lower: 30, upper: 50 }, stops: null,
      });
      expect(s).toContain('Stops unavailable');
      expect(s).not.toContain('undefined');
    });
  });

  test('createWalkForwardRow with null stability -> No data + UNKNOWN badge', () => {
    const s = UI.createWalkForwardRow('PIVOT', null);
    expect(s).toContain('PIVOT');
    expect(s).toContain('No data');
    expect(s).toContain('stability-badge unknown');
  });

  test('createSparklineDisplay with <2 points -> insufficient-data placeholder', () => {
    expect(UI.createSparklineDisplay({ liftSeries: [1] }, 'R1')).toContain('Insufficient data for trend');
  });

  describe('disabled stub builders', () => {
    test('createVolatilityMatrix is a disabled stub', () => {
      const el = UI.createVolatilityMatrix([], 'trend', '5m');
      expect(el.className).toBe('volatility-matrix disabled');
      expect(el.textContent).toContain('disabled');
    });

    test('createWalkForwardSummary is a disabled stub', () => {
      const el = UI.createWalkForwardSummary(null, null);
      expect(el.className).toBe('walk-forward-summary disabled');
      expect(el.textContent).toContain('disabled');
    });
  });

  describe('DOM builders consuming AdvancedAnalytics (stubbed)', () => {
    test('createTopPicksCard empty-picks branch', () => {
      window.AdvancedAnalytics = { generateTopPicks: () => [] };
      const card = UI.createTopPicksCard({}, 'high_vol', null);
      expect(card.className).toBe('top-picks-card');
      expect(card.innerHTML).toContain('No Significant Levels');
      expect(card.innerHTML).toContain('high_vol');
    });

    test('createTopPicksCard with one pick renders a pick item and score', () => {
      window.AdvancedAnalytics = {
        generateTopPicks: () => [{
          level: 'R1', score: 0.42,
          result: { successRate: 63.2, lift: 5.4, qValue: 0.0005, confidenceInterval: { lower: 55.1, upper: 71.3 } },
        }],
      };
      const card = UI.createTopPicksCard({}, 'trend', null);
      expect(card.querySelectorAll('.pick-item')).toHaveLength(1);
      expect(card.textContent).toContain('Strong Level');
      expect(card.textContent).toContain('Score: 42');
    });

    test('createAlertsPanel with no alerts', () => {
      window.AdvancedAnalytics = { createAlertRules: () => ({}), evaluateAlerts: () => [] };
      const panel = UI.createAlertsPanel({ levels: { R1: { hasSufficientData: true } } }, 'trend', null);
      expect(panel.className).toBe('alerts-panel');
      expect(panel.textContent).toContain('No active alerts');
    });
  });

  test('addAdvancedStyles injects the stylesheet once (idempotent)', () => {
    UI.addAdvancedStyles();
    UI.addAdvancedStyles();
    expect(document.querySelectorAll('#advanced-ui-styles')).toHaveLength(1);
  });

  test('generateMockTimeSeries returns n structurally-valid points', () => {
    const series = UI.generateMockTimeSeries('R2', 5);
    expect(series).toHaveLength(5);
    for (const p of series) {
      expect(typeof p.success).toBe('boolean');
      expect(p.successProb).toBeGreaterThanOrEqual(0.2);
      expect(p.successProb).toBeLessThanOrEqual(0.9);
    }
  });
});
