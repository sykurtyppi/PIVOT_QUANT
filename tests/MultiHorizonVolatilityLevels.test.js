import { MultiHorizonVolatilityLevels, QuantPivot } from '../src/index.js';

function weekdayBars(start, end, initialClose = 100, logReturn = 0.005) {
  const bars = [];
  const cursor = new Date(`${start}T00:00:00.000Z`);
  const last = new Date(`${end}T00:00:00.000Z`);
  let close = initialClose;
  let direction = 1;

  while (cursor <= last) {
    const day = cursor.getUTCDay();
    if (day !== 0 && day !== 6) {
      bars.push({ timestamp: cursor.toISOString(), close });
      close *= Math.exp(direction * logReturn);
      direction *= -1;
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }

  return bars;
}

describe('MultiHorizonVolatilityLevels', () => {
  const generatedAt = '2026-09-21T21:00:00.000Z';

  test('calculates deterministic log-symmetric daily sigma levels from prior-session data', () => {
    const closes = [
      100,
      100 * Math.exp(-0.01),
      100,
      100 * Math.exp(-0.01),
      100,
    ];
    const dates = ['2026-09-15', '2026-09-16', '2026-09-17', '2026-09-18', '2026-09-21'];
    const bars = dates.map((date, index) => ({ timestamp: `${date}T20:00:00.000Z`, close: closes[index] }));

    const result = MultiHorizonVolatilityLevels.calculate(bars, {
      symbol: 'QQQ',
      asOf: '2026-09-22',
      generatedAt,
      volatilityWindows: [4],
      horizons: ['daily'],
    });

    const sampleDailyVolatility = Math.sqrt(0.0004 / 3);
    const anchor = 100;

    expect(result.schemaVersion).toBe('1.0.0');
    expect(result.horizons.daily.anchor).toEqual({
      close: anchor,
      timestamp: '2026-09-21T20:00:00.000Z',
      rule: 'prior_session_close',
    });
    expect(result.horizons.daily.sessionsRemaining).toBe(1);
    expect(result.horizons.daily.selectedVolatilityWindow).toBe(4);
    expect(result.horizons.daily.realizedVolatility.daily).toBeCloseTo(sampleDailyVolatility, 12);
    expect(result.horizons.daily.levels.upper1Sigma).toBeCloseTo(
      anchor * Math.exp(sampleDailyVolatility),
      10
    );
    expect(result.horizons.daily.levels.lower1Sigma).toBeCloseTo(
      anchor * Math.exp(-sampleDailyVolatility),
      10
    );
    expect(result.horizons.daily.levels.upper2Sigma).toBeCloseTo(
      anchor * Math.exp(2 * sampleDailyVolatility),
      10
    );
    expect(
      result.horizons.daily.levels.upper1Sigma * result.horizons.daily.levels.lower1Sigma
    ).toBeCloseTo(anchor ** 2, 8);
  });

  test('uses the prior completed session, week, and month closes as distinct anchors', () => {
    const bars = weekdayBars('2026-08-20', '2026-09-21');
    const byDate = Object.fromEntries(bars.map(bar => [bar.timestamp.slice(0, 10), bar.close]));

    const result = MultiHorizonVolatilityLevels.calculate(bars, {
      symbol: 'SPY',
      asOf: '2026-09-22',
      generatedAt,
      volatilityWindows: [5],
    });

    expect(result.horizons.daily.anchor.close).toBe(byDate['2026-09-21']);
    expect(result.horizons.daily.anchor.rule).toBe('prior_session_close');
    expect(result.horizons.weekly.anchor.close).toBe(byDate['2026-09-18']);
    expect(result.horizons.weekly.anchor.rule).toBe('prior_completed_week_close');
    expect(result.horizons.monthly.anchor.close).toBe(byDate['2026-08-31']);
    expect(result.horizons.monthly.anchor.rule).toBe('prior_completed_month_close');
  });

  test('does not use as-of or later bars in anchors or volatility estimates', () => {
    const base = weekdayBars('2026-08-20', '2026-09-21');
    const future = [
      ...base,
      { timestamp: '2026-09-22T20:00:00.000Z', close: 1000 },
      { timestamp: '2026-09-23T20:00:00.000Z', close: 10 },
    ];
    const options = {
      symbol: 'QQQ',
      asOf: '2026-09-22',
      generatedAt,
      volatilityWindows: [5],
    };

    const withoutFuture = MultiHorizonVolatilityLevels.calculate(base, options);
    const withFuture = MultiHorizonVolatilityLevels.calculate(future, options);

    expect(withFuture.horizons).toEqual(withoutFuture.horizons);
    expect(withFuture.provenance.calculationDataThrough).toBe('2026-09-21T00:00:00.000Z');
    expect(withFuture.provenance.inputDataThrough).toBe('2026-09-23T20:00:00.000Z');
  });

  test('does not use bars published after generatedAt', () => {
    const bars = weekdayBars('2026-08-20', '2026-09-21').map(bar => ({
      ...bar,
      timestamp: bar.timestamp.replace('T00:00:00.000Z', 'T20:00:00.000Z'),
    }));

    const result = MultiHorizonVolatilityLevels.calculate(bars, {
      symbol: 'QQQ',
      asOf: '2026-09-22',
      generatedAt: '2026-09-18T21:00:00.000Z',
      volatilityWindows: [5],
    });

    expect(result.horizons.daily.status).toBe('unavailable');
    expect(result.horizons.daily.reason).toBe('missing_prior_period_close');
    expect(result.horizons.weekly.anchor.timestamp).toBe('2026-09-18T20:00:00.000Z');
    expect(result.provenance.calculationDataThrough).toBe('2026-09-18T20:00:00.000Z');
  });

  test('ignores post-cutoff bar contents and session validity', () => {
    const base = weekdayBars('2026-08-20', '2026-09-21');
    const options = {
      symbol: 'QQQ',
      asOf: '2026-09-22',
      generatedAt,
      volatilityWindows: [5],
    };
    const result = MultiHorizonVolatilityLevels.calculate(
      [
        ...base,
        { timestamp: '2026-09-26T20:00:00.000Z', close: Number.NaN },
      ],
      options
    );

    expect(result.horizons).toEqual(
      MultiHorizonVolatilityLevels.calculate(base, options).horizons
    );
    expect(result.provenance.inputDataThrough).toBe('2026-09-26T20:00:00.000Z');
  });

  test('marks a horizon unavailable instead of silently using a stale anchor', () => {
    const result = MultiHorizonVolatilityLevels.calculate(
      weekdayBars('2026-07-01', '2026-07-31'),
      {
        symbol: 'SPY',
        asOf: '2026-09-22',
        generatedAt,
        volatilityWindows: [5],
      }
    );

    for (const horizon of Object.values(result.horizons)) {
      expect(horizon).toMatchObject({
        status: 'unavailable',
        reason: 'missing_prior_period_close',
        selectedVolatilityWindow: null,
        quality: { status: 'unavailable', requestedWindow: 5, usedWindow: null },
      });
      expect(horizon.levels).toBeNull();
    }
  });

  test('counts weekday sessions remaining and excludes supplied market holidays', () => {
    const bars = weekdayBars('2026-08-10', '2026-09-22');

    const result = MultiHorizonVolatilityLevels.calculate(bars, {
      symbol: 'SPY',
      asOf: '2026-09-23',
      generatedAt: '2026-09-22T21:00:00.000Z',
      volatilityWindows: [5],
      holidays: ['2026-09-25'],
    });

    expect(result.horizons.daily.sessionsRemaining).toBe(1);
    expect(result.horizons.weekly.sessionsRemaining).toBe(2);
    expect(result.horizons.monthly.sessionsRemaining).toBe(5);
    expect(result.methodology.sessionCalendar).toBe('weekday_with_explicit_holidays');
  });

  test('falls back to the first window with enough point-in-time observations and exposes quality', () => {
    const bars = weekdayBars('2026-09-08', '2026-09-21');

    const result = MultiHorizonVolatilityLevels.calculate(bars, {
      symbol: 'SMH',
      asOf: '2026-09-22',
      generatedAt,
      volatilityWindows: [20, 5],
      horizons: ['daily'],
    });

    expect(result.horizons.daily.selectedVolatilityWindow).toBe(5);
    expect(result.horizons.daily.quality.status).toBe('fallback');
    expect(result.horizons.daily.quality.requestedWindow).toBe(20);
    expect(result.horizons.daily.quality.usedWindow).toBe(5);
    expect(result.horizons.daily.volatilityEstimates['20'].status).toBe('insufficient_data');
    expect(result.horizons.daily.volatilityEstimates['5'].status).toBe('available');
  });

  test.each([
    [[{ timestamp: '2026-09-21T20:00:00.000Z', close: 0 }], 'positive finite close'],
    [
      [
        { timestamp: '2026-09-21T20:00:00.000Z', close: 100 },
        { timestamp: '2026-09-18T20:00:00.000Z', close: 101 },
      ],
      'strictly increasing',
    ],
    [
      [
        { timestamp: '2026-09-21T20:00:00.000Z', close: 100 },
        { timestamp: '2026-09-21T20:00:00.000Z', close: 101 },
      ],
      'strictly increasing',
    ],
    [[{ timestamp: '2026-09-19T20:00:00.000Z', close: 100 }], 'trading session'],
    [[{ timestamp: '2026-09-21T20:00:00', close: 100 }], 'explicit timezone'],
    [
      [
        { timestamp: '2026-09-18T15:00:00.000Z', close: 100 },
        { timestamp: '2026-09-18T20:00:00.000Z', close: 101 },
      ],
      'one close per UTC session date',
    ],
  ])('rejects invalid input bars %#', (bars, message) => {
    expect(() =>
      MultiHorizonVolatilityLevels.calculate(bars, {
        symbol: 'SPY',
        asOf: '2026-09-22',
        generatedAt,
      })
    ).toThrow(message);
  });

  test('never emits non-finite JSON values for extreme but finite closes', () => {
    const dates = ['2026-09-15', '2026-09-16', '2026-09-17', '2026-09-18', '2026-09-21'];
    const bars = dates.map((date, index) => ({
      timestamp: `${date}T20:00:00.000Z`,
      close: index % 2 === 0 ? 1e-300 : 1e300,
    }));

    const result = MultiHorizonVolatilityLevels.calculate(bars, {
      symbol: 'SPY',
      asOf: '2026-09-22',
      generatedAt,
      volatilityWindows: [4],
      horizons: ['daily'],
    });

    expect(result.horizons.daily).toMatchObject({
      status: 'unavailable',
      reason: 'non_finite_projected_levels',
      levels: null,
      quality: { status: 'unavailable', requestedWindow: 4, usedWindow: 4 },
    });
    expect(JSON.parse(JSON.stringify(result))).toEqual(result);
  });

  test('is available through the public QuantPivot API and returns a stable JSON contract', () => {
    const bars = weekdayBars('2026-08-20', '2026-09-21');
    const quantPivot = new QuantPivot({}, 'testing');

    try {
      const result = quantPivot.calculateVolatilityLevels(bars, {
        symbol: 'SPY',
        asOf: '2026-09-22',
        generatedAt,
        volatilityWindows: [5],
      });
      const serialized = JSON.parse(JSON.stringify(result));

      expect(serialized.symbol).toBe('SPY');
      expect(serialized.asOf).toBe('2026-09-22');
      expect(serialized.generatedAt).toBe(generatedAt);
      expect(serialized.provenance).toMatchObject({
        priceField: 'close',
        returnType: 'log',
        annualizationSessions: 252,
      });
      expect(Object.keys(serialized.horizons)).toEqual(['daily', 'weekly', 'monthly']);
    } finally {
      quantPivot.dispose();
    }
  });
});
