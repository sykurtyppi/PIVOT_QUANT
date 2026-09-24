import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

import { publishForecast } from '../../src/forecast/publishForecast.js';
import { DataQualityError } from '../../src/forecast/dataSnapshot.js';
import { readForecasts } from '../../src/forecast/ledgerStore.js';
import { holidaysInRange } from '../../src/forecast/nyseCalendar.js';

// Deterministic daily weekday/session bars from 2026-01-02 through a given last session.
function sessionBars(lastSession) {
  const holidays = new Set(holidaysInRange('2026-01-01', lastSession));
  const bars = [];
  let close = 700;
  const cursor = new Date('2026-01-02T00:00:00.000Z');
  const last = new Date(`${lastSession}T00:00:00.000Z`);
  while (cursor <= last) {
    const dow = cursor.getUTCDay();
    const key = cursor.toISOString().slice(0, 10);
    if (dow !== 0 && dow !== 6 && !holidays.has(key)) {
      bars.push({ timestamp: `${key}T20:00:00.000Z`, close: Number(close.toFixed(4)) });
      close *= Math.exp((key.charCodeAt(9) % 2 ? 1 : -1) * 0.008);
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return bars;
}

const BASE = {
  symbol: 'SPY',
  asOf: '2026-09-24',
  generatedAt: '2026-09-24T12:15:00.000Z',
  softwareSha: '31c4fdbca180b8202bf544ac1f056200f5bea9d0',
  dataSource: 'yahoo_proxy',
};

describe('publishForecast (integration)', () => {
  let dir;
  let ledger;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'publish-'));
    ledger = join(dir, 'forecasts.jsonl');
  });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  test('publishes per-horizon records with full lineage', () => {
    const bars = sessionBars('2026-09-23');
    const { records, writes } = publishForecast({ ...BASE, bars, ledgerPath: ledger });
    expect(records.map((r) => r.horizon)).toEqual(['daily', 'weekly', 'monthly']);
    expect(writes.every((w) => w.written)).toBe(true);

    const daily = records.find((r) => r.horizon === 'daily');
    expect(daily.software_sha).toBe(BASE.softwareSha);
    expect(daily.data_source).toBe('yahoo_proxy');
    expect(daily.calendar_version).toBe('nyse-rulegen-1.0.0');
    expect(daily.data_snapshot_hash).toMatch(/^[0-9a-f]{64}$/);
    expect(daily.quality.status).toBe('complete');
    expect(readForecasts(ledger)).toHaveLength(3);
  });

  test('re-running from the same snapshot + SHA is byte-reproducible and idempotent', () => {
    const bars = sessionBars('2026-09-23');
    const first = publishForecast({ ...BASE, bars, ledgerPath: ledger });
    // Later wall-clock, same data + SHA -> identical content, no new rows.
    const second = publishForecast({ ...BASE, bars, generatedAt: '2026-09-24T18:30:00.000Z', ledgerPath: ledger });
    first.records.forEach((rec, i) => expect(rec.content_hash).toBe(second.records[i].content_hash));
    expect(second.writes.every((w) => w.idempotent && !w.written)).toBe(true);
    expect(readForecasts(ledger)).toHaveLength(3); // not doubled
  });

  test('fails closed when the prior session is missing (does not write)', () => {
    const bars = sessionBars('2026-09-23').filter((b) => !b.timestamp.startsWith('2026-09-23'));
    expect(() => publishForecast({ ...BASE, bars, ledgerPath: ledger }))
      .toThrow(expect.objectContaining({ code: 'missing_prior_session' }));
    expect(readForecasts(ledger)).toEqual([]);
  });

  test('rejects an asOf that is not an NYSE session', () => {
    const bars = sessionBars('2026-11-25'); // through the day before Thanksgiving
    expect(() => publishForecast({ ...BASE, bars, asOf: '2026-11-26', generatedAt: '2026-11-26T12:15:00.000Z', ledgerPath: ledger }))
      .toThrow(DataQualityError);
  });

  test('persist:false assembles records without touching the ledger', () => {
    const bars = sessionBars('2026-09-23');
    const { records, writes } = publishForecast({ ...BASE, bars, ledgerPath: ledger, persist: false });
    expect(records).toHaveLength(3);
    expect(writes).toEqual([]);
    expect(readForecasts(ledger)).toEqual([]);
  });

  test('a caller-supplied holidays param cannot shift the forecast (calendar is authoritative)', () => {
    const bars = sessionBars('2026-09-23');
    const clean = publishForecast({ ...BASE, bars, ledgerPath: ledger, persist: false });
    // Inject a bogus holidays field (a real session falsely marked holiday). It must be ignored.
    const injected = publishForecast({ ...BASE, bars, holidays: ['2026-09-23', '2026-09-18'], ledgerPath: ledger, persist: false });
    clean.records.forEach((rec, i) => expect(rec.content_hash).toBe(injected.records[i].content_hash));
  });

  test('holiday between asOf and month end is counted out of monthly sessionsRemaining', () => {
    // asOf Wed 2026-11-25; Thanksgiving Thu 11-26 sits between asOf and month end (11-30).
    // Remaining monthly sessions = 11-25, 11-27, 11-30 = 3 (Thanksgiving + weekend excluded).
    const bars = sessionBars('2026-11-24');
    const { records } = publishForecast({
      ...BASE, bars, asOf: '2026-11-25', generatedAt: '2026-11-25T12:15:00.000Z', ledgerPath: ledger, persist: false,
    });
    const monthly = records.find((r) => r.horizon === 'monthly');
    expect(monthly.sessions_remaining).toBe(3);
  });

  test('fails closed (writes nothing) on every data-quality gate', () => {
    const good = sessionBars('2026-09-23');
    const cases = [
      { name: 'missing_prior_session', args: { bars: good.filter((b) => !b.timestamp.startsWith('2026-09-23')) } },
      { name: 'duplicate_bar', args: { bars: good.concat({ timestamp: '2026-09-23T21:00:00.000Z', close: 1 }) } },
      { name: 'unadjusted_data', args: { bars: good, priceAdjustment: 'raw_close' } },
      { name: 'non_session_as_of', args: { bars: sessionBars('2026-11-25'), asOf: '2026-11-26', generatedAt: '2026-11-26T12:15:00.000Z' } },
    ];
    for (const c of cases) {
      expect(() => publishForecast({ ...BASE, ...c.args, ledgerPath: ledger }))
        .toThrow(expect.objectContaining({ code: c.name }));
      expect(readForecasts(ledger)).toEqual([]); // nothing was written by any failing gate
    }
  });
});
