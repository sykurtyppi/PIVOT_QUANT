import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

import {
  appendOutcome,
  readOutcomes,
  scoreForecast,
  scoreMaturedForecasts,
  ScoreboardImmutabilityError,
} from '../../src/forecast/scoreForecasts.js';

function record(overrides = {}) {
  return {
    forecast_id: 'SPY-2026-09-24-daily-v1',
    version: 'v1',
    symbol: 'SPY',
    target_session: '2026-09-24',
    horizon: 'daily',
    outcome_forecast: 'close_containment',
    software_sha: '31c4fdb',
    content_hash: 'a'.repeat(64),
    anchor: 666,
    horizon_sigma_log_return: 0.01,
    levels: { lower_2: 653, lower_1: 659, upper_1: 673, upper_2: 679 },
    ...overrides,
  };
}

describe('scoreForecast', () => {
  test('close inside ±1σ is within both bands', () => {
    const o = scoreForecast(record(), 666);
    expect(o.within_1sigma).toBe(true);
    expect(o.within_2sigma).toBe(true);
    expect(o.breach_1sigma_side).toBeNull();
    expect(Math.abs(o.z_score)).toBeLessThan(1e-9);
  });

  test('close between 1σ and 2σ breaches 1σ upper but holds 2σ', () => {
    const o = scoreForecast(record(), 676);
    expect(o.within_1sigma).toBe(false);
    expect(o.within_2sigma).toBe(true);
    expect(o.breach_1sigma_side).toBe('upper');
    expect(o.breach_2sigma_side).toBeNull();
    expect(o.z_score).toBeGreaterThan(1);
  });

  test('close beyond 2σ lower breaches both', () => {
    const o = scoreForecast(record(), 640);
    expect(o.within_1sigma).toBe(false);
    expect(o.within_2sigma).toBe(false);
    expect(o.breach_1sigma_side).toBe('lower');
    expect(o.breach_2sigma_side).toBe('lower');
  });

  test('boundary close exactly on the band is contained (inclusive)', () => {
    const o = scoreForecast(record(), 673); // == upper_1
    expect(o.within_1sigma).toBe(true);
  });

  test('unavailable forecast (null levels) is not scored', () => {
    expect(scoreForecast(record({ levels: null, anchor: null }), 666)).toBeNull();
  });

  test('rejects an invalid realized close', () => {
    expect(() => scoreForecast(record(), -1)).toThrow(/positive finite/);
  });
});

describe('scoreboard (append-only)', () => {
  let dir;
  let board;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'score-'));
    board = join(dir, 'nested', 'scoreboard.jsonl');
  });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  test('appends an outcome and reads it back', () => {
    const o = scoreForecast(record(), 666, { realizedSession: '2026-09-24', scoredAt: '2026-09-25T00:00:00Z' });
    expect(appendOutcome(board, o)).toMatchObject({ written: true });
    expect(readOutcomes(board)).toHaveLength(1);
  });

  test('re-scoring the same result is idempotent', () => {
    const o = scoreForecast(record(), 666);
    appendOutcome(board, o);
    expect(appendOutcome(board, o)).toMatchObject({ written: false, idempotent: true });
    expect(readOutcomes(board)).toHaveLength(1);
  });

  test('a different result for the same forecast is refused (no retroactive rewrite)', () => {
    appendOutcome(board, scoreForecast(record(), 666));
    expect(() => appendOutcome(board, scoreForecast(record(), 640)))
      .toThrow(ScoreboardImmutabilityError);
    expect(readOutcomes(board)).toHaveLength(1);
  });
});

describe('scoreMaturedForecasts', () => {
  let dir;
  let board;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'matured-'));
    board = join(dir, 'scoreboard.jsonl');
  });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  test('scores matured forecasts, skips unmatured / unavailable / already-scored', () => {
    const forecasts = [
      record({ forecast_id: 'A', target_session: '2026-09-24' }),
      record({ forecast_id: 'B', target_session: '2026-09-25' }), // not matured
      record({ forecast_id: 'C', levels: null, anchor: null }),    // unavailable
    ];
    const realized = { A: { close: 666, session: '2026-09-24' } };
    const lookup = (r) => realized[r.forecast_id] || null;

    const first = scoreMaturedForecasts(forecasts, lookup, { scoreboardPath: board, scoredAt: 't' });
    expect(first.scored.map((o) => o.forecast_id)).toEqual(['A']);
    expect(first.skipped).toEqual(expect.arrayContaining([
      { forecast_id: 'B', reason: 'not_matured' },
      { forecast_id: 'C', reason: 'unavailable_forecast' },
    ]));

    // Re-run: A now already scored, nothing new written.
    const second = scoreMaturedForecasts(forecasts, lookup, { scoreboardPath: board, scoredAt: 't' });
    expect(second.scored).toHaveLength(0);
    expect(second.skipped).toEqual(expect.arrayContaining([{ forecast_id: 'A', reason: 'already_scored' }]));
    expect(readOutcomes(board)).toHaveLength(1);
  });
});
