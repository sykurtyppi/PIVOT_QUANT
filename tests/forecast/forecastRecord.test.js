import {
  buildForecastRecords,
  canonicalStringify,
  sha256Hex,
} from '../../src/forecast/forecastRecord.js';

const LINEAGE = {
  softwareSha: '31c4fdb',
  dataSource: 'yahoo_proxy',
  dataSnapshotHash: 'a'.repeat(64),
  dataThrough: '2026-09-23T20:00:00.000Z',
  priceAdjustment: 'split_and_dividend_adjusted',
  calendarVersion: 'nyse-rulegen-1.0.0',
  generatedAt: '2026-09-24T12:15:00.000Z',
  version: 1,
};

function engineResult(overrides = {}) {
  return {
    schemaVersion: '1.0.0',
    symbol: 'SPY',
    asOf: '2026-09-24',
    generatedAt: LINEAGE.generatedAt,
    methodology: { name: 'realized_volatility_log_symmetric' },
    provenance: { priceField: 'close' },
    horizons: {
      daily: {
        status: 'available',
        sessionsRemaining: 1,
        anchor: { close: 666, timestamp: '2026-09-23T20:00:00.000Z', rule: 'prior_session_close' },
        selectedVolatilityWindow: 20,
        realizedVolatility: { daily: 0.01, annualized: 0.16, returnObservations: 20 },
        levels: { anchor: 666, lower2Sigma: 653, lower1Sigma: 659, upper1Sigma: 673, upper2Sigma: 679, horizonSigmaLogReturn: 0.01 },
        quality: { status: 'complete', requestedWindow: 20, usedWindow: 20 },
      },
      weekly: {
        status: 'available',
        sessionsRemaining: 3,
        anchor: { close: 664, timestamp: '2026-09-22T20:00:00.000Z', rule: 'prior_completed_week_close' },
        selectedVolatilityWindow: 60,
        realizedVolatility: { daily: 0.011, annualized: 0.17, returnObservations: 60 },
        levels: { anchor: 664, lower2Sigma: 640, lower1Sigma: 652, upper1Sigma: 676, upper2Sigma: 688, horizonSigmaLogReturn: 0.02 },
        quality: { status: 'fallback', requestedWindow: 20, usedWindow: 60 },
      },
      monthly: {
        status: 'unavailable',
        sessionsRemaining: 6,
        anchor: { close: 660, timestamp: '2026-08-31T20:00:00.000Z', rule: 'prior_completed_month_close' },
        selectedVolatilityWindow: null,
        reason: 'insufficient_volatility_history',
        levels: null,
        quality: { status: 'unavailable', requestedWindow: 20, usedWindow: null },
      },
      ...overrides,
    },
  };
}

describe('canonicalStringify', () => {
  test('is key-order independent at every depth', () => {
    const a = { b: 1, a: { d: 2, c: [3, { f: 4, e: 5 }] } };
    const b = { a: { c: [3, { e: 5, f: 4 }], d: 2 }, b: 1 };
    expect(canonicalStringify(a)).toBe(canonicalStringify(b));
    expect(sha256Hex(a)).toBe(sha256Hex(b));
  });
});

describe('buildForecastRecords', () => {
  test('emits one record per horizon with mapped levels', () => {
    const records = buildForecastRecords(engineResult(), LINEAGE);
    expect(records.map((r) => r.horizon)).toEqual(['daily', 'weekly', 'monthly']);

    const daily = records.find((r) => r.horizon === 'daily');
    expect(daily.forecast_id).toBe('SPY-2026-09-24-daily-v1');
    expect(daily.level_type).toBe('expected_move_band');
    expect(daily.outcome_forecast).toBe('close_containment');
    expect(daily.levels).toEqual({ lower_2: 653, lower_1: 659, upper_1: 673, upper_2: 679 });
    expect(daily.anchor).toBe(666);
    expect(daily.estimator).toBe('20_session_close_to_close_rv');
    expect(daily.quality.fallback).toBe(false);
    expect(daily.content_hash).toMatch(/^[0-9a-f]{64}$/);
  });

  test('prominently labels a fallback window', () => {
    const weekly = buildForecastRecords(engineResult(), LINEAGE).find((r) => r.horizon === 'weekly');
    expect(weekly.quality.status).toBe('fallback');
    expect(weekly.quality.fallback).toBe(true);
    expect(weekly.estimator).toBe('60_session_close_to_close_rv');
  });

  test('marks unavailable horizons with null levels and explicit unavailable_fields', () => {
    const monthly = buildForecastRecords(engineResult(), LINEAGE).find((r) => r.horizon === 'monthly');
    expect(monthly.levels).toBeNull();
    expect(monthly.anchor).toBeNull();
    expect(monthly.estimator).toBeNull();
    expect(monthly.quality.status).toBe('unavailable');
    expect(monthly.unavailable_fields).toEqual(['anchor', 'levels', 'realized_volatility']);
  });

  test('content_hash is stable across rebuilds and independent of wall-clock fields', () => {
    const first = buildForecastRecords(engineResult(), LINEAGE);
    const laterClock = buildForecastRecords(engineResult(), {
      ...LINEAGE, generatedAt: '2026-09-24T18:00:00.000Z', ingestedAt: '2026-09-24T17:59:00.000Z',
    });
    first.forEach((rec, i) => expect(rec.content_hash).toBe(laterClock[i].content_hash));
  });

  test('content_hash changes when a level changes', () => {
    const base = buildForecastRecords(engineResult(), LINEAGE).find((r) => r.horizon === 'daily');
    const bumped = engineResult();
    bumped.horizons.daily.levels.upper2Sigma = 680;
    const changed = buildForecastRecords(bumped, LINEAGE).find((r) => r.horizon === 'daily');
    expect(changed.content_hash).not.toBe(base.content_hash);
  });

  test('content_hash covers each lineage/content field that determines the forecast', () => {
    const daily = () => buildForecastRecords(engineResult(), LINEAGE).find((r) => r.horizon === 'daily');
    const base = daily().content_hash;
    // Lineage fields that MUST change the hash.
    for (const lineageOverride of [
      { softwareSha: 'deadbeef' },
      { dataSnapshotHash: 'b'.repeat(64) },
      { calendarVersion: 'nyse-rulegen-9.9.9' },
    ]) {
      const changed = buildForecastRecords(engineResult(), { ...LINEAGE, ...lineageOverride })
        .find((r) => r.horizon === 'daily');
      expect(changed.content_hash).not.toBe(base);
    }
    // Content fields that MUST change the hash.
    const withAnchor = engineResult();
    withAnchor.horizons.daily.levels.anchor = 667;
    expect(buildForecastRecords(withAnchor, LINEAGE).find((r) => r.horizon === 'daily').content_hash).not.toBe(base);

    const withSigma = engineResult();
    withSigma.horizons.daily.levels.horizonSigmaLogReturn = 0.02;
    expect(buildForecastRecords(withSigma, LINEAGE).find((r) => r.horizon === 'daily').content_hash).not.toBe(base);
  });

  test('content_hash is invariant to non-determining lineage (source, wall clock)', () => {
    const base = buildForecastRecords(engineResult(), LINEAGE).find((r) => r.horizon === 'daily').content_hash;
    for (const override of [
      { dataSource: 'stooq' },
      { generatedAt: '2027-01-01T00:00:00.000Z' },
      { ingestedAt: '2027-01-01T00:00:00.000Z' },
    ]) {
      const rec = buildForecastRecords(engineResult(), { ...LINEAGE, ...override }).find((r) => r.horizon === 'daily');
      expect(rec.content_hash).toBe(base);
    }
  });

  test('distinguishes the two unavailable shapes by quality reason', () => {
    const a = engineResult();
    a.horizons.monthly.reason = 'insufficient_volatility_history';
    const b = engineResult();
    b.horizons.monthly.reason = 'non_finite_projected_levels';
    const ha = buildForecastRecords(a, LINEAGE).find((r) => r.horizon === 'monthly').content_hash;
    const hb = buildForecastRecords(b, LINEAGE).find((r) => r.horizon === 'monthly').content_hash;
    expect(ha).not.toBe(hb);
  });

  test('requires every lineage key', () => {
    const keys = ['softwareSha', 'dataSource', 'dataSnapshotHash', 'dataThrough', 'priceAdjustment', 'calendarVersion', 'generatedAt'];
    for (const key of keys) {
      expect(() => buildForecastRecords(engineResult(), { ...LINEAGE, [key]: '' }))
        .toThrow(new RegExp(`${key} is required`));
    }
  });
});
