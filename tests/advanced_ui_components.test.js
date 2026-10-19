describe('advanced_ui_components.js — alert rendering', () => {
  let UI;

  beforeAll(async () => {
    globalThis.window = {};
    await import('../advanced_ui_components.js');
    UI = globalThis.window.AdvancedUIComponents;
  });

  test('significance alerts without ATR stops render safely', () => {
    const html = UI.createAlertItem({
      type: 'significance', priority: 'HIGH', level_name: 'R1',
      message: 'signal', bias: 'reversal',
      confidence_interval: { lower: 30, upper: 50 },
      stops: null,
    });
    expect(html).toContain('Stops unavailable');
    expect(html).not.toContain('undefined');
  });
});