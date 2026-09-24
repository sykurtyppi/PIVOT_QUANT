import { appendFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { dirname, join } from 'path';

import { computeContentHash, computeRecordHash } from '../../src/forecast/forecastRecord.js';
import {
  appendForecast,
  appendForecasts,
  findForecast,
  LedgerImmutabilityError,
  LedgerIntegrityError,
  readForecasts,
} from '../../src/forecast/ledgerStore.js';

// A realistic, schema-complete, hash-valid forecast record. Identity is (symbol, session, horizon).
function record(overrides = {}) {
  const base = {
    schema_version: '1.0.0',
    forecast_id: 'SPY-2026-09-24-daily-v1',
    version: 'v1',
    symbol: 'SPY',
    target_session: '2026-09-24',
    horizon: 'daily',
    software_sha: '31c4fdb',
    data_source: 'yahoo_proxy',
    data_snapshot_hash: 'a'.repeat(64),
    data_through: '2026-09-23T20:00:00.000Z',
    price_adjustment: 'split_and_dividend_adjusted',
    calendar_version: 'nyse-rulegen-1.0.0',
    generated_at: '2026-09-24T12:15:00.000Z',
    anchor: 666,
    estimator: '20_session_close_to_close_rv',
    horizon_sigma_log_return: 0.01,
    levels: { lower_2: 653, lower_1: 659, upper_1: 673, upper_2: 679 },
    quality: { status: 'complete', reason: null },
    ...overrides,
  };
  if (!('content_hash' in overrides)) {
    base.content_hash = computeContentHash(base);
    base.record_hash = computeRecordHash(base);
  }
  return base;
}

// Same identity, different content (a conflicting republish).
const conflicting = (id) => record({ forecast_id: id, anchor: 667 });
// A different identity (different horizon), so it legitimately coexists.
const weekly = (over = {}) => record({ forecast_id: 'SPY-2026-09-24-weekly-v1', horizon: 'weekly', ...over });

describe('ledgerStore (append-only)', () => {
  let dir;
  let ledger;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'ledger-'));
    ledger = join(dir, 'nested', 'forecasts.jsonl');
  });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  test('writes a new forecast and reads it back', () => {
    const res = appendForecast(ledger, record());
    expect(res).toMatchObject({ written: true, idempotent: false });
    expect(readForecasts(ledger)).toHaveLength(1);
    expect(findForecast(ledger, 'SPY-2026-09-24-daily-v1').symbol).toBe('SPY');
  });

  test('re-appending a byte-identical record is an idempotent no-op', () => {
    appendForecast(ledger, record());
    const res = appendForecast(ledger, record());
    expect(res).toMatchObject({ written: false, idempotent: true });
    expect(readForecasts(ledger)).toHaveLength(1);
    expect(readFileSync(ledger, 'utf8').trim().split('\n')).toHaveLength(1);
  });

  test('same forecast_id with different content is refused', () => {
    appendForecast(ledger, record());
    expect(() => appendForecast(ledger, conflicting('SPY-2026-09-24-daily-v1')))
      .toThrow(LedgerImmutabilityError);
    expect(readForecasts(ledger)).toHaveLength(1);
  });

  test('republishing the same identity under a new version is refused', () => {
    appendForecast(ledger, record({ forecast_id: 'SPY-2026-09-24-daily-v1', version: 'v1' }));
    // Different forecast_id (v2) but SAME (symbol, session, horizon) identity, different content.
    expect(() => appendForecast(ledger, record({ forecast_id: 'SPY-2026-09-24-daily-v2', version: 'v2', anchor: 670 })))
      .toThrow(LedgerImmutabilityError);
    expect(readForecasts(ledger)).toHaveLength(1);
  });

  test('an explicit linked supersede is allowed and retains the original', () => {
    appendForecast(ledger, record({ forecast_id: 'SPY-2026-09-24-daily-v1' }));
    const corrected = record({ forecast_id: 'SPY-2026-09-24-daily-v2', version: 'v2', anchor: 670, supersedes: 'SPY-2026-09-24-daily-v1' });
    const res = appendForecast(ledger, corrected, { allowSupersede: true });
    expect(res.written).toBe(true);
    expect(readForecasts(ledger)).toHaveLength(2); // original retained
  });

  test('different identities (horizons) coexist', () => {
    appendForecast(ledger, record());
    appendForecast(ledger, weekly());
    expect(readForecasts(ledger)).toHaveLength(2);
  });

  test('reading a missing ledger yields an empty list', () => {
    expect(readForecasts(join(dir, 'nope.jsonl'))).toEqual([]);
  });

  test('rejects records without forecast_id / content_hash', () => {
    expect(() => appendForecast(ledger, { symbol: 'SPY' })).toThrow(/forecast_id and content_hash/);
  });

  test('refuses a record whose content_hash does not match its content', () => {
    expect(() => appendForecast(ledger, record({ content_hash: 'f'.repeat(64) })))
      .toThrow(LedgerIntegrityError);
  });

  test('verifies content_hash on read and fails closed on tampering', () => {
    appendForecast(ledger, record());
    // Tamper with a persisted level without updating the hash.
    const tampered = readFileSync(ledger, 'utf8').replace('"anchor":666', '"anchor":999');
    writeFileSync(ledger, tampered, 'utf8');
    expect(() => readForecasts(ledger)).toThrow(LedgerIntegrityError);
  });

  test('tolerates a torn trailing line and still appends', () => {
    appendForecast(ledger, record());
    appendFileSync(ledger, '{"forecast_id":"SPY-2026-09-24-weekly-v1","content_ha', 'utf8');
    expect(readForecasts(ledger)).toHaveLength(1); // torn tail ignored
    const res = appendForecast(ledger, weekly());
    expect(res.written).toBe(true);
    expect(readForecasts(ledger)).toHaveLength(2);
  });

  test('a malformed non-final line is still a hard error', () => {
    mkdirSync(dirname(ledger), { recursive: true });
    writeFileSync(ledger, `not json\n${JSON.stringify(record())}\n`, 'utf8');
    expect(() => readForecasts(ledger)).toThrow(/not valid JSON/);
  });

  test('findForecast throws when the ledger holds duplicate ids', () => {
    mkdirSync(dirname(ledger), { recursive: true });
    const line = JSON.stringify(record());
    writeFileSync(ledger, `${line}\n${line}\n`, 'utf8');
    expect(() => findForecast(ledger, 'SPY-2026-09-24-daily-v1')).toThrow(LedgerIntegrityError);
  });

  test('appendForecasts is all-or-nothing when one record conflicts', () => {
    appendForecast(ledger, record()); // daily exists
    const batch = [weekly(), conflicting('SPY-2026-09-24-daily-v1')]; // weekly new, daily conflicts
    expect(() => appendForecasts(ledger, batch)).toThrow(LedgerImmutabilityError);
    expect(findForecast(ledger, 'SPY-2026-09-24-weekly-v1')).toBeNull(); // nothing written
    expect(readForecasts(ledger)).toHaveLength(1);
  });

  test('appendForecasts writes a clean multi-record batch together', () => {
    const res = appendForecasts(ledger, [record(), weekly()]);
    expect(res.every((r) => r.written)).toBe(true);
    expect(readForecasts(ledger)).toHaveLength(2);
  });

  test('a single batch cannot introduce a duplicate identity', () => {
    // Two different records sharing one (symbol, session, horizon) identity in ONE call.
    const a = record({ forecast_id: 'SPY-2026-09-24-daily-v1' });
    const b = record({ forecast_id: 'SPY-2026-09-24-daily-v2', version: 'v2', anchor: 670 });
    expect(() => appendForecasts(ledger, [a, b])).toThrow(LedgerImmutabilityError);
    expect(readForecasts(ledger)).toEqual([]); // nothing written
  });

  test('a batch with two byte-identical records writes a single row', () => {
    const res = appendForecasts(ledger, [record(), record()]);
    expect(res[0].written).toBe(true);
    expect(res[1].idempotent).toBe(true);
    expect(readForecasts(ledger)).toHaveLength(1);
  });

  test('refuses a record missing required schema fields', () => {
    const incomplete = record();
    delete incomplete.data_source; // drop a required provenance field
    expect(() => appendForecast(ledger, incomplete)).toThrow(LedgerIntegrityError);
  });

  test('the envelope hash catches tampering with provenance fields', () => {
    appendForecast(ledger, record());
    // Change a NON-level provenance field (outside content_hash) without updating record_hash.
    const tampered = readFileSync(ledger, 'utf8').replace('"yahoo_proxy"', '"attacker_feed"');
    writeFileSync(ledger, tampered, 'utf8');
    expect(() => readForecasts(ledger)).toThrow(LedgerIntegrityError);
  });
});
