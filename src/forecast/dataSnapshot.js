/**
 * Input-data validation, snapshotting, and hashing for the forecast ledger.
 *
 * The ledger must fail closed on bad inputs rather than publish a confident forecast off
 * stale, duplicated, partial, or unadjusted data. This module normalizes a set of daily
 * bars into a canonical snapshot, hashes it, and enforces the plan's data-quality gates,
 * including the hard one: the immediately prior trading session's close must be present.
 */

import { canonicalStringify, sha256Hex } from './forecastRecord.js';
import { isTradingSession, priorTradingSession } from './nyseCalendar.js';

const DAY_MS = 86_400_000;

export class DataQualityError extends Error {
  constructor(message, code) {
    super(message);
    this.name = 'DataQualityError';
    this.code = code;
  }
}

// Price-adjustment labels the ledger accepts as genuinely adjusted. Anything else is
// treated as unadjusted and refused, so the recorded label cannot disagree with the gate.
const ADJUSTED_LABELS = new Set(['split_and_dividend_adjusted', 'split_adjusted', 'total_return']);

// Session key from a bar timestamp, using UTC exactly like the engine's dateKey and the
// NYSE calendar — a string slice would misassign an offset-bearing timestamp to the wrong
// session and let the prior-session gate pass on a bar the engine counts differently.
function dateKeyOf(iso) {
  const dt = new Date(iso);
  if (Number.isNaN(dt.getTime())) throw new DataQualityError(`invalid timestamp: ${iso}`, 'partial_bar');
  return dt.toISOString().slice(0, 10);
}

/**
 * Validate and snapshot daily bars as of a target session.
 *
 * @param {Array<{timestamp:string, close:number, open?:number, high?:number, low?:number}>} bars
 * @param {object} opts
 * @param {string} opts.asOf                 Target session YYYY-MM-DD.
 * @param {string} opts.generatedAt          ISO publication time; bars after it are rejected.
 * @param {string} [opts.priceAdjustment='split_and_dividend_adjusted'] Recorded adjustment label;
 *        anything outside the adjusted allowlist is treated as unadjusted and refused.
 * @param {boolean} [opts.adjusted]          Explicit override of the label-derived adjustment gate.
 * @param {number} [opts.maxFeedAgeDays=5]   Max age of the newest bar vs generatedAt before the
 *        feed is considered stale (covers a long weekend plus a holiday).
 * @returns {{snapshot:object, dataSnapshotHash:string, dataThrough:string,
 *            priorSession:string, bars:Array}}
 */
export function buildDataSnapshot(bars, opts) {
  if (!opts || typeof opts.asOf !== 'string') {
    throw new DataQualityError('buildDataSnapshot requires opts.asOf', 'missing_as_of');
  }
  if (typeof opts.generatedAt !== 'string') {
    throw new DataQualityError('buildDataSnapshot requires opts.generatedAt', 'missing_generated_at');
  }
  if (!Array.isArray(bars) || bars.length === 0) {
    throw new DataQualityError('bars must be a non-empty array', 'empty_bars');
  }

  // Adjustment status is derived from the recorded price_adjustment label unless an
  // explicit boolean overrides it, so the production entry point (which passes a label,
  // not a bool) is actually gated — an unadjusted feed cannot slip through by omission.
  const priceAdjustment = opts.priceAdjustment || 'split_and_dividend_adjusted';
  const adjusted = opts.adjusted != null ? opts.adjusted !== false : ADJUSTED_LABELS.has(priceAdjustment);
  if (!adjusted) {
    // Unadjusted history silently corrupts split/dividend crossings — refuse it.
    throw new DataQualityError(
      `refusing non-adjusted price data (price_adjustment=${priceAdjustment}); ` +
        'ledger requires split/dividend adjusted closes',
      'unadjusted_data',
    );
  }

  // Validate asOf is a supported NYSE session up front, so a malformed / holiday /
  // out-of-support asOf fails cleanly here rather than as an opaque calendar error deeper in.
  let asOfIsSession;
  try {
    asOfIsSession = isTradingSession(opts.asOf);
  } catch (err) {
    throw new DataQualityError(`asOf ${opts.asOf} is not a usable NYSE session: ${err.message}`, 'non_session_as_of');
  }
  if (!asOfIsSession) {
    throw new DataQualityError(`asOf ${opts.asOf} is not an NYSE trading session`, 'non_session_as_of');
  }

  const generatedMs = Date.parse(opts.generatedAt);
  if (!Number.isFinite(generatedMs)) {
    throw new DataQualityError(`generatedAt is not a valid timestamp: ${opts.generatedAt}`, 'bad_generated_at');
  }

  // Normalize + reject partial/malformed bars. A bar is level-determining only if its
  // SESSION is strictly before asOf (point-in-time bound, independent of wall-clock) AND
  // its instant is at/before generatedAt. The session bound is the load-bearing one: it
  // keeps the target session's own close out of the snapshot even for an after-close run,
  // so the data_snapshot_hash is reproducible regardless of the hour the job fires.
  const normalized = [];
  const seenSessions = new Set();
  for (let i = 0; i < bars.length; i += 1) {
    const bar = bars[i];
    if (bar == null || typeof bar !== 'object') {
      throw new DataQualityError(`bars[${i}] must be an object`, 'partial_bar');
    }
    if (typeof bar.timestamp !== 'string' || Number.isNaN(Date.parse(bar.timestamp))) {
      throw new DataQualityError(`bars[${i}].timestamp is invalid`, 'partial_bar');
    }
    const ms = Date.parse(bar.timestamp);
    const session = dateKeyOf(bar.timestamp);
    if (session >= opts.asOf) continue; // session look-ahead bound: nothing at/after asOf
    if (ms > generatedMs) continue; // instant look-ahead bound: nothing after publication
    if (typeof bar.close !== 'number' || !Number.isFinite(bar.close) || bar.close <= 0) {
      throw new DataQualityError(`bars[${i}] has a non-positive/invalid close`, 'partial_bar');
    }
    if (seenSessions.has(session)) {
      throw new DataQualityError(`duplicate bar for session ${session}`, 'duplicate_bar');
    }
    seenSessions.add(session);
    normalized.push({ timestamp: bar.timestamp, session, close: bar.close, ms });
  }

  if (normalized.length === 0) {
    throw new DataQualityError('no level-determining bars before asOf at/through generatedAt', 'no_usable_bars');
  }

  normalized.sort((a, b) => a.ms - b.ms);
  const newest = normalized[normalized.length - 1];

  // Hard gate: the immediately prior NYSE trading session close must be present.
  const requiredPrior = priorTradingSession(opts.asOf);
  if (!seenSessions.has(requiredPrior)) {
    throw new DataQualityError(
      `missing required prior-session close for ${requiredPrior} (asOf ${opts.asOf})`,
      'missing_prior_session',
    );
  }

  // Feed staleness: the newest available bar must not be too old relative to the
  // publication instant. This measures ACTUAL data age (generatedAt - newest bar), so a
  // stalled feed whose latest bar is days behind is caught — unlike a bound derived purely
  // from asOf, which can say nothing about the data.
  const maxFeedAgeDays = opts.maxFeedAgeDays ?? 5;
  if (generatedMs - newest.ms > maxFeedAgeDays * DAY_MS) {
    throw new DataQualityError(
      `stale feed: newest bar ${newest.session} is > ${maxFeedAgeDays}d before generatedAt ${opts.generatedAt}`,
      'stale_feed',
    );
  }

  const snapshotBars = normalized.map((bar) => ({ timestamp: bar.timestamp, close: bar.close }));
  const snapshot = {
    symbol: (opts.symbol || '').toUpperCase() || null,
    as_of: opts.asOf,
    price_adjustment: opts.priceAdjustment || 'split_and_dividend_adjusted',
    bar_count: snapshotBars.length,
    data_from: normalized[0].timestamp,
    data_through: newest.timestamp,
    bars: snapshotBars,
  };

  return {
    snapshot,
    dataSnapshotHash: sha256Hex(canonicalStringify(snapshot)),
    dataThrough: newest.timestamp,
    priorSession: requiredPrior,
    bars: snapshotBars,
  };
}
