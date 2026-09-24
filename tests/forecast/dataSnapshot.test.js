import { buildDataSnapshot, DataQualityError } from '../../src/forecast/dataSnapshot.js';

const GENERATED_AT = '2026-09-24T12:15:00.000Z';

// Bars for a handful of sessions ending on the required prior session (2026-09-23).
function goodBars() {
  return [
    { timestamp: '2026-09-18T20:00:00.000Z', close: 660 },
    { timestamp: '2026-09-21T20:00:00.000Z', close: 662 },
    { timestamp: '2026-09-22T20:00:00.000Z', close: 664 },
    { timestamp: '2026-09-23T20:00:00.000Z', close: 666 },
  ];
}

describe('buildDataSnapshot', () => {
  test('accepts clean bars and produces a deterministic snapshot hash', () => {
    const a = buildDataSnapshot(goodBars(), { symbol: 'SPY', asOf: '2026-09-24', generatedAt: GENERATED_AT });
    const b = buildDataSnapshot(goodBars(), { symbol: 'SPY', asOf: '2026-09-24', generatedAt: GENERATED_AT });
    expect(a.dataSnapshotHash).toBe(b.dataSnapshotHash);
    expect(a.dataSnapshotHash).toMatch(/^[0-9a-f]{64}$/);
    expect(a.priorSession).toBe('2026-09-23');
    expect(a.dataThrough).toBe('2026-09-23T20:00:00.000Z');
    expect(a.snapshot.bar_count).toBe(4);
  });

  test('excludes look-ahead bars after generatedAt', () => {
    const bars = goodBars().concat({ timestamp: '2026-09-24T20:00:00.000Z', close: 999 });
    const snap = buildDataSnapshot(bars, { symbol: 'SPY', asOf: '2026-09-24', generatedAt: GENERATED_AT });
    expect(snap.snapshot.bar_count).toBe(4);
    expect(snap.dataThrough).toBe('2026-09-23T20:00:00.000Z');
  });

  test('fails closed when the required prior-session close is missing', () => {
    const bars = goodBars().filter((b) => !b.timestamp.startsWith('2026-09-23'));
    expect(() => buildDataSnapshot(bars, { symbol: 'SPY', asOf: '2026-09-24', generatedAt: GENERATED_AT }))
      .toThrow(expect.objectContaining({ code: 'missing_prior_session' }));
  });

  test('rejects duplicate sessions', () => {
    const bars = goodBars().concat({ timestamp: '2026-09-23T21:00:00.000Z', close: 667 });
    expect(() => buildDataSnapshot(bars, { symbol: 'SPY', asOf: '2026-09-24', generatedAt: GENERATED_AT }))
      .toThrow(expect.objectContaining({ code: 'duplicate_bar' }));
  });

  test('rejects partial bars (missing/invalid close)', () => {
    const bars = goodBars();
    bars[1] = { timestamp: bars[1].timestamp, close: null };
    expect(() => buildDataSnapshot(bars, { symbol: 'SPY', asOf: '2026-09-24', generatedAt: GENERATED_AT }))
      .toThrow(expect.objectContaining({ code: 'partial_bar' }));
  });

  test('rejects unadjusted price data', () => {
    expect(() => buildDataSnapshot(goodBars(), {
      symbol: 'SPY', asOf: '2026-09-24', generatedAt: GENERATED_AT, adjusted: false,
    })).toThrow(expect.objectContaining({ code: 'unadjusted_data' }));
  });

  test('rejects a stale feed (newest bar far older than generatedAt)', () => {
    // Prior session 2026-09-23 is present, but the run is dated ~2 weeks later: the newest
    // available bar is > maxFeedAgeDays old relative to generatedAt -> stale feed.
    expect(() => buildDataSnapshot(goodBars(), {
      symbol: 'SPY', asOf: '2026-09-24', generatedAt: '2026-10-08T12:15:00.000Z', maxFeedAgeDays: 5,
    })).toThrow(expect.objectContaining({ code: 'stale_feed' }));
  });

  test('rejects a non-session asOf', () => {
    // A prior-session bar exists but asOf is Christmas.
    const bars = [
      { timestamp: '2026-12-23T20:00:00.000Z', close: 700 },
      { timestamp: '2026-12-24T20:00:00.000Z', close: 701 },
    ];
    expect(() => buildDataSnapshot(bars, {
      symbol: 'SPY', asOf: '2026-12-25', generatedAt: '2026-12-25T12:15:00.000Z',
    })).toThrow(DataQualityError);
  });

  test('empty bars are rejected', () => {
    expect(() => buildDataSnapshot([], { symbol: 'SPY', asOf: '2026-09-24', generatedAt: GENERATED_AT }))
      .toThrow(expect.objectContaining({ code: 'empty_bars' }));
  });

  test('excludes the asOf session own close even for an after-close run (session look-ahead bound)', () => {
    // The asOf session close is stamped 20:00Z; a run at 21:00Z must NOT admit it, or the
    // snapshot hash would depend on the hour the job fired (breaking reproducibility).
    const bars = goodBars().concat({ timestamp: '2026-09-24T20:00:00.000Z', close: 999 });
    const snap = buildDataSnapshot(bars, { symbol: 'SPY', asOf: '2026-09-24', generatedAt: '2026-09-24T21:00:00.000Z' });
    expect(snap.snapshot.bar_count).toBe(4);
    expect(snap.dataThrough).toBe('2026-09-23T20:00:00.000Z');
    expect(snap.snapshot.bars.some((b) => b.close === 999)).toBe(false);
  });

  test('snapshot hash is independent of run time before vs after the asOf close', () => {
    const bars = goodBars().concat({ timestamp: '2026-09-24T20:00:00.000Z', close: 999 });
    const early = buildDataSnapshot(bars, { symbol: 'SPY', asOf: '2026-09-24', generatedAt: '2026-09-24T12:15:00.000Z' });
    const late = buildDataSnapshot(bars, { symbol: 'SPY', asOf: '2026-09-24', generatedAt: '2026-09-24T21:00:00.000Z' });
    expect(early.dataSnapshotHash).toBe(late.dataSnapshotHash);
  });

  test('rejects each missing/invalid option and degenerate input', () => {
    const base = { symbol: 'SPY', asOf: '2026-09-24', generatedAt: GENERATED_AT };
    expect(() => buildDataSnapshot(goodBars(), { ...base, asOf: undefined }))
      .toThrow(expect.objectContaining({ code: 'missing_as_of' }));
    expect(() => buildDataSnapshot(goodBars(), { ...base, generatedAt: undefined }))
      .toThrow(expect.objectContaining({ code: 'missing_generated_at' }));
    expect(() => buildDataSnapshot(goodBars(), { ...base, generatedAt: 'not-a-time' }))
      .toThrow(expect.objectContaining({ code: 'bad_generated_at' }));
    // All bars at/after asOf -> nothing level-determining remains.
    const futureOnly = [{ timestamp: '2026-09-24T20:00:00.000Z', close: 700 }];
    expect(() => buildDataSnapshot(futureOnly, base))
      .toThrow(expect.objectContaining({ code: 'no_usable_bars' }));
    // Non-object bar and unparseable timestamp are partial bars.
    expect(() => buildDataSnapshot([...goodBars(), 42], base))
      .toThrow(expect.objectContaining({ code: 'partial_bar' }));
    expect(() => buildDataSnapshot([...goodBars(), { timestamp: 'nope', close: 1 }], base))
      .toThrow(expect.objectContaining({ code: 'partial_bar' }));
  });

  test('rejects zero and negative closes (close must be positive)', () => {
    for (const bad of [0, -5]) {
      const bars = goodBars();
      bars[2] = { timestamp: bars[2].timestamp, close: bad };
      expect(() => buildDataSnapshot(bars, { symbol: 'SPY', asOf: '2026-09-24', generatedAt: GENERATED_AT }))
        .toThrow(expect.objectContaining({ code: 'partial_bar' }));
    }
  });

  test('derives the unadjusted gate from a non-adjusted price_adjustment label', () => {
    expect(() => buildDataSnapshot(goodBars(), {
      symbol: 'SPY', asOf: '2026-09-24', generatedAt: GENERATED_AT, priceAdjustment: 'raw_close',
    })).toThrow(expect.objectContaining({ code: 'unadjusted_data' }));
  });
});
