/**
 * Outcome scoring for the prospective shadow record (roadmap §8).
 *
 * Each published forecast asserts a close-containment claim: the realized close should sit
 * inside the ±1σ / ±2σ bands. This module verifies a matured forecast still matches its stored
 * content_hash, scores it against its realized close, and appends the result to an append-only
 * scoreboard. Writes take the same cross-process lock and durable two-phase commit as the
 * forecast ledger, so concurrent scorers cannot lose or corrupt outcomes; the scoreboard is
 * never retroactively rewritten (a re-score that agrees is idempotent, one that disagrees is
 * refused).
 *
 * The realized-close lookup is injected, so this logic is deterministic and unit-testable and
 * does not itself reach for market data.
 */

import { sha256Hex, verifyForecastContentHash } from './forecastRecord.js';
import { appendLinesDurable, readJsonl, truncateTornTail, withLock } from './fileLock.js';

export const SCOREBOARD_SCHEMA_VERSION = '1.0.0';

export class ScoreboardImmutabilityError extends Error {
  constructor(message, forecastId) {
    super(message);
    this.name = 'ScoreboardImmutabilityError';
    this.forecastId = forecastId;
  }
}

export class ForecastIntegrityError extends Error {
  constructor(message, forecastId) {
    super(message);
    this.name = 'ForecastIntegrityError';
    this.forecastId = forecastId;
  }
}

function outcomeHash(outcome) {
  return sha256Hex({
    forecast_id: outcome.forecast_id,
    forecast_content_hash: outcome.forecast_content_hash,
    realized_close: outcome.realized_close,
    within_1sigma: outcome.within_1sigma,
    within_2sigma: outcome.within_2sigma,
  });
}

/**
 * Score one forecast record against a realized close. Returns null when the record cannot be
 * scored (unavailable levels). Throws if the record's content_hash no longer matches its content.
 */
export function scoreForecast(record, realizedClose, meta = {}) {
  if (!record || record.levels == null || record.anchor == null) {
    return null; // unavailable horizons are recorded as un-scoreable, never as a pass/fail
  }
  if (!verifyForecastContentHash(record)) {
    throw new ForecastIntegrityError(
      `refusing to score ${record.forecast_id}: content_hash does not match its content`,
      record.forecast_id,
    );
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
  outcome.outcome_hash = outcomeHash(outcome);
  return outcome;
}

function outcomeVerifier(outcome, line) {
  if (typeof outcome?.outcome_hash !== 'string' || outcomeHash(outcome) !== outcome.outcome_hash) {
    throw new ScoreboardImmutabilityError(
      `scoreboard row on line ${line} (${outcome?.forecast_id}) fails outcome_hash verification`,
      outcome?.forecast_id,
    );
  }
}

export function readOutcomes(scoreboardPath, { verify = true } = {}) {
  return readJsonl(scoreboardPath, verify ? { verify: outcomeVerifier } : {});
}

/**
 * Append one outcome under the exclusive lock. Idempotent on an identical re-score (same
 * outcome_hash); a different outcome for the same forecast_id is refused.
 */
export function appendOutcome(scoreboardPath, outcome) {
  if (!outcome || typeof outcome.forecast_id !== 'string' || typeof outcome.outcome_hash !== 'string') {
    throw new Error('appendOutcome requires an outcome with forecast_id and outcome_hash');
  }
  return withLock(scoreboardPath, () => {
    truncateTornTail(scoreboardPath);
    const existing = readOutcomes(scoreboardPath).find((o) => o.forecast_id === outcome.forecast_id);
    if (existing) {
      if (existing.outcome_hash === outcome.outcome_hash) return { written: false, idempotent: true, outcome: existing };
      throw new ScoreboardImmutabilityError(
        `outcome for ${outcome.forecast_id} already scored with a different result; the scoreboard is append-only`,
        outcome.forecast_id,
      );
    }
    appendLinesDurable(scoreboardPath, [JSON.stringify(outcome)]);
    return { written: true, idempotent: false, outcome };
  });
}

/**
 * Score every matured, not-yet-scored forecast that a realized close is available for.
 *
 * @param {object[]} forecasts        Records from the ledger.
 * @param {(record:object)=>({close:number, session?:string}|null)} realizedLookup
 * @param {object} opts { scoreboardPath, scoredAt }
 * @returns {{scored:object[], skipped:object[]}}
 */
export function scoreMaturedForecasts(forecasts, realizedLookup, opts) {
  if (!opts || !opts.scoreboardPath) throw new Error('scoreMaturedForecasts requires opts.scoreboardPath');
  const alreadyScored = new Set(readOutcomes(opts.scoreboardPath).map((o) => o.forecast_id));
  const scored = [];
  const skipped = [];
  for (const record of forecasts) {
    if (alreadyScored.has(record.forecast_id)) { skipped.push({ forecast_id: record.forecast_id, reason: 'already_scored' }); continue; }
    if (record.levels == null) { skipped.push({ forecast_id: record.forecast_id, reason: 'unavailable_forecast' }); continue; }
    const realized = realizedLookup(record);
    if (!realized) { skipped.push({ forecast_id: record.forecast_id, reason: 'not_matured' }); continue; }
    const outcome = scoreForecast(record, realized.close, { realizedSession: realized.session ?? null, scoredAt: opts.scoredAt ?? null });
    scored.push(appendOutcome(opts.scoreboardPath, outcome).outcome);
  }
  return { scored, skipped };
}
