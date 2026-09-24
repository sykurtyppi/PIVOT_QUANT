/**
 * Outcome scoring for the prospective shadow record (roadmap §8).
 *
 * Each published forecast asserts a close-containment claim: the realized close should sit
 * inside the ±1σ / ±2σ bands. This module scores a matured forecast against its realized
 * close and appends the result to an append-only scoreboard. The scoreboard is never
 * retroactively rewritten — a re-score that agrees is idempotent, a re-score that disagrees
 * is refused — so the forward track record cannot be quietly improved after the fact.
 *
 * The realized-close lookup is injected, so this logic is deterministic and unit-testable
 * and does not itself reach for market data.
 */

import { appendFileSync, existsSync, mkdirSync, readFileSync } from 'fs';
import { dirname } from 'path';

import { sha256Hex } from './forecastRecord.js';

export const SCOREBOARD_SCHEMA_VERSION = '1.0.0';

export class ScoreboardImmutabilityError extends Error {
  constructor(message, forecastId) {
    super(message);
    this.name = 'ScoreboardImmutabilityError';
    this.forecastId = forecastId;
  }
}

/**
 * Score one forecast record against a realized close.
 * Returns null when the record cannot be scored (unavailable levels).
 *
 * @param {object} record         An immutable forecast record from the ledger.
 * @param {number} realizedClose  The realized close for the record's target outcome.
 * @param {object} [meta]         { realizedSession, scoredAt } lineage for the outcome row.
 */
export function scoreForecast(record, realizedClose, meta = {}) {
  if (!record || record.levels == null || record.anchor == null) {
    return null; // unavailable horizons are recorded as un-scoreable, never as a pass/fail
  }
  if (typeof realizedClose !== 'number' || !Number.isFinite(realizedClose) || realizedClose <= 0) {
    throw new Error(`realizedClose must be a positive finite number, got ${realizedClose}`);
  }

  const { lower_1, upper_1, lower_2, upper_2 } = record.levels;
  const within1 = realizedClose >= lower_1 && realizedClose <= upper_1;
  const within2 = realizedClose >= lower_2 && realizedClose <= upper_2;

  let breach1 = null;
  if (realizedClose < lower_1) breach1 = 'lower';
  else if (realizedClose > upper_1) breach1 = 'upper';
  let breach2 = null;
  if (realizedClose < lower_2) breach2 = 'lower';
  else if (realizedClose > upper_2) breach2 = 'upper';

  // Standardized move in the band's own units (log-return / horizon sigma), when defined.
  const sigma = record.horizon_sigma_log_return;
  const zScore = sigma && sigma > 0 ? Math.log(realizedClose / record.anchor) / sigma : null;

  const outcome = {
    schema_version: SCOREBOARD_SCHEMA_VERSION,
    forecast_id: record.forecast_id,
    version: record.version,
    symbol: record.symbol,
    target_session: record.target_session,
    horizon: record.horizon,
    outcome_forecast: record.outcome_forecast,
    software_sha: record.software_sha,
    forecast_content_hash: record.content_hash,
    anchor: record.anchor,
    realized_close: realizedClose,
    realized_session: meta.realizedSession ?? null,
    within_1sigma: within1,
    within_2sigma: within2,
    breach_1sigma_side: breach1,
    breach_2sigma_side: breach2,
    z_score: zScore,
    scored_at: meta.scoredAt ?? null,
  };
  outcome.outcome_hash = sha256Hex({
    forecast_id: outcome.forecast_id,
    forecast_content_hash: outcome.forecast_content_hash,
    realized_close: outcome.realized_close,
    within_1sigma: outcome.within_1sigma,
    within_2sigma: outcome.within_2sigma,
  });
  return outcome;
}

export function readOutcomes(scoreboardPath) {
  if (!existsSync(scoreboardPath)) return [];
  const outcomes = [];
  readFileSync(scoreboardPath, 'utf8').split('\n').forEach((line, index) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    try {
      outcomes.push(JSON.parse(trimmed));
    } catch (err) {
      throw new Error(`scoreboard ${scoreboardPath} line ${index + 1} is not valid JSON: ${err.message}`);
    }
  });
  return outcomes;
}

/**
 * Append one outcome. Idempotent on an identical re-score (same outcome_hash); a different
 * outcome for the same forecast_id is refused — the scoreboard is never rewritten.
 */
export function appendOutcome(scoreboardPath, outcome) {
  if (!outcome || typeof outcome.forecast_id !== 'string' || typeof outcome.outcome_hash !== 'string') {
    throw new Error('appendOutcome requires an outcome with forecast_id and outcome_hash');
  }
  const existing = readOutcomes(scoreboardPath).find((o) => o.forecast_id === outcome.forecast_id);
  if (existing) {
    if (existing.outcome_hash === outcome.outcome_hash) {
      return { written: false, idempotent: true, outcome: existing };
    }
    throw new ScoreboardImmutabilityError(
      `outcome for ${outcome.forecast_id} already scored with a different result ` +
        `(${existing.outcome_hash} != ${outcome.outcome_hash}); the scoreboard is append-only`,
      outcome.forecast_id,
    );
  }
  const dir = dirname(scoreboardPath);
  if (dir && !existsSync(dir)) mkdirSync(dir, { recursive: true });
  appendFileSync(scoreboardPath, `${JSON.stringify(outcome)}\n`, 'utf8');
  return { written: true, idempotent: false, outcome };
}

/**
 * Score every matured, not-yet-scored forecast that a realized close is available for.
 *
 * @param {object[]} forecasts        Records from the ledger.
 * @param {(record:object)=>({close:number, session?:string}|null)} realizedLookup
 *        Returns the realized close for a record, or null if the outcome has not matured.
 * @param {object} opts { scoreboardPath, scoredAt }
 * @returns {{scored:object[], skipped:object[]}}
 */
export function scoreMaturedForecasts(forecasts, realizedLookup, opts) {
  if (!opts || !opts.scoreboardPath) throw new Error('scoreMaturedForecasts requires opts.scoreboardPath');
  const alreadyScored = new Set(readOutcomes(opts.scoreboardPath).map((o) => o.forecast_id));
  const scored = [];
  const skipped = [];

  for (const record of forecasts) {
    if (alreadyScored.has(record.forecast_id)) {
      skipped.push({ forecast_id: record.forecast_id, reason: 'already_scored' });
      continue;
    }
    if (record.levels == null) {
      skipped.push({ forecast_id: record.forecast_id, reason: 'unavailable_forecast' });
      continue;
    }
    const realized = realizedLookup(record);
    if (!realized) {
      skipped.push({ forecast_id: record.forecast_id, reason: 'not_matured' });
      continue;
    }
    const outcome = scoreForecast(record, realized.close, {
      realizedSession: realized.session ?? null,
      scoredAt: opts.scoredAt ?? null,
    });
    const write = appendOutcome(opts.scoreboardPath, outcome);
    scored.push(write.outcome);
  }
  return { scored, skipped };
}
