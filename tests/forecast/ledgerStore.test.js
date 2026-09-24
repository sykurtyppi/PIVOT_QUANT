import { appendFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { dirname, join } from 'path';

import {
  appendForecast,
  appendForecasts,
  findForecast,
  LedgerImmutabilityError,
  LedgerIntegrityError,
  readForecasts,
} from '../../src/forecast/ledgerStore.js';

function record(overrides = {}) {
  return {
    forecast_id: 'SPY-2026-09-24-daily-v1',
    content_hash: 'a'.repeat(64),
    symbol: 'SPY',
    ...overrides,
  };
}

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
    expect(readForecasts(ledger)).toHaveLength(1); // not duplicated
    expect(readFileSync(ledger, 'utf8').trim().split('\n')).toHaveLength(1);
  });

  test('re-appending a different record for the same id is refused', () => {
    appendForecast(ledger, record());
    expect(() => appendForecast(ledger, record({ content_hash: 'b'.repeat(64) })))
      .toThrow(LedgerImmutabilityError);
    expect(readForecasts(ledger)).toHaveLength(1); // original untouched
  });

  test('different forecast_ids coexist', () => {
    appendForecast(ledger, record());
    appendForecast(ledger, record({ forecast_id: 'SPY-2026-09-24-weekly-v1', content_hash: 'c'.repeat(64) }));
    expect(readForecasts(ledger)).toHaveLength(2);
  });

  test('reading a missing ledger yields an empty list', () => {
    expect(readForecasts(join(dir, 'nope.jsonl'))).toEqual([]);
  });

  test('rejects records without forecast_id / content_hash', () => {
    expect(() => appendForecast(ledger, { symbol: 'SPY' })).toThrow(/forecast_id and content_hash/);
  });

  test('tolerates a torn trailing line and still appends', () => {
    appendForecast(ledger, record());
    // Simulate an interrupted append: a partial JSON fragment with no trailing newline.
    appendFileSync(ledger, '{"forecast_id":"SPY-2026-09-24-weekly-v1","content_ha', 'utf8');
    expect(readForecasts(ledger)).toHaveLength(1); // torn tail ignored, not a hard error
    const res = appendForecast(ledger, record({ forecast_id: 'SPY-2026-09-24-weekly-v1', content_hash: 'd'.repeat(64) }));
    expect(res.written).toBe(true);
    expect(readForecasts(ledger)).toHaveLength(2);
  });

  test('a malformed non-final line is still a hard error', () => {
    mkdirSync(dirname(ledger), { recursive: true });
    writeFileSync(ledger, 'not json\n' + JSON.stringify(record()) + '\n', 'utf8');
    expect(() => readForecasts(ledger)).toThrow(/not valid JSON/);
  });

  test('findForecast throws when the ledger holds duplicate ids', () => {
    mkdirSync(dirname(ledger), { recursive: true });
    const line = JSON.stringify(record());
    writeFileSync(ledger, `${line}\n${line}\n`, 'utf8');
    expect(() => findForecast(ledger, 'SPY-2026-09-24-daily-v1')).toThrow(LedgerIntegrityError);
  });

  test('appendForecasts is all-or-nothing when one record conflicts', () => {
    appendForecast(ledger, record({ forecast_id: 'A' }));
    const batch = [
      record({ forecast_id: 'B', content_hash: 'b'.repeat(64) }), // new
      record({ forecast_id: 'A', content_hash: 'x'.repeat(64) }), // conflicts with existing A
    ];
    expect(() => appendForecasts(ledger, batch)).toThrow(LedgerImmutabilityError);
    // B must NOT have been written — the batch aborted before any append.
    expect(findForecast(ledger, 'B')).toBeNull();
    expect(readForecasts(ledger)).toHaveLength(1);
  });

  test('appendForecasts writes a clean multi-record batch together', () => {
    const res = appendForecasts(ledger, [
      record({ forecast_id: 'A' }),
      record({ forecast_id: 'B', content_hash: 'b'.repeat(64) }),
    ]);
    expect(res.every((r) => r.written)).toBe(true);
    expect(readForecasts(ledger)).toHaveLength(2);
  });
});
