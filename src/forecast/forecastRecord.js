/**
 * Immutable forecast record assembly + deterministic hashing.
 *
 * Turns one MultiHorizonVolatilityLevels result into per-horizon records that carry the
 * full lineage the ledger promises: what is being forecast, the exact software SHA, the
 * hashed input snapshot, the calendar version, the estimator, and every explicitly
 * unavailable field. A `content_hash` covers only the level-determining content (not
 * wall-clock fields), so "re-run from the same snapshot + SHA yields byte-equivalent
 * levels" is a checkable property, not a hope.
 */

import { createHash } from 'crypto';

export const FORECAST_RECORD_SCHEMA_VERSION = '1.0.0';

/**
 * Deterministic JSON: object keys are emitted in sorted order at every depth, so the
 * same logical content always serializes to the same bytes. Arrays keep their order.
 */
export function canonicalStringify(value) {
  return JSON.stringify(sortDeep(value));
}

function sortDeep(value) {
  if (Array.isArray(value)) return value.map(sortDeep);
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = sortDeep(value[key]);
    }
    return out;
  }
  return value;
}

export function sha256Hex(input) {
  const text = typeof input === 'string' ? input : canonicalStringify(input);
  return createHash('sha256').update(text, 'utf8').digest('hex');
}

function estimatorLabel(usedWindow) {
  if (usedWindow == null) return null;
  return `${usedWindow}_session_close_to_close_rv`;
}

/**
 * Build immutable per-horizon forecast records from an engine result.
 *
 * @param {object} engineResult  Output of MultiHorizonVolatilityLevels.calculate.
 * @param {object} lineage
 * @param {string} lineage.softwareSha       Git SHA the levels were computed at.
 * @param {string} lineage.dataSource        e.g. 'yahoo_proxy'.
 * @param {string} lineage.dataSnapshotHash  sha256 of the exact input bar snapshot.
 * @param {string} lineage.dataThrough       ISO timestamp of the newest input bar.
 * @param {string} lineage.priceAdjustment   e.g. 'split_and_dividend_adjusted'.
 * @param {string} lineage.calendarVersion   Authoritative calendar identifier.
 * @param {string} lineage.generatedAt       ISO publication timestamp (wall clock).
 * @param {string} lineage.ingestedAt        ISO time the source data was fetched.
 * @param {number|string} [lineage.version=1] Forecast version series (revisions restart it).
 * @returns {object[]} One record per requested horizon.
 */
export function buildForecastRecords(engineResult, lineage) {
  const required = [
    'softwareSha', 'dataSource', 'dataSnapshotHash', 'dataThrough',
    'priceAdjustment', 'calendarVersion', 'generatedAt',
  ];
  for (const key of required) {
    if (!lineage || lineage[key] == null || lineage[key] === '') {
      throw new Error(`buildForecastRecords: lineage.${key} is required`);
    }
  }

  const version = lineage.version == null ? 1 : lineage.version;
  const versionLabel = String(version).startsWith('v') ? String(version) : `v${version}`;
  const symbol = engineResult.symbol;
  const asOf = engineResult.asOf;

  return Object.entries(engineResult.horizons).map(([horizon, result]) => {
    const available = result.status !== 'unavailable' && result.levels != null;
    const quality = result.quality || {};
    const usedWindow = quality.usedWindow ?? result.selectedVolatilityWindow ?? null;

    const levels = available
      ? {
          lower_2: result.levels.lower2Sigma,
          lower_1: result.levels.lower1Sigma,
          upper_1: result.levels.upper1Sigma,
          upper_2: result.levels.upper2Sigma,
        }
      : null;

    const unavailableFields = available ? [] : ['anchor', 'levels', 'realized_volatility'];

    const record = {
      schema_version: FORECAST_RECORD_SCHEMA_VERSION,
      forecast_id: `${symbol}-${asOf}-${horizon}-${versionLabel}`,
      version: versionLabel,
      symbol,
      // The session the forecast is published FOR; `horizon` sets the containment window.
      target_session: asOf,
      horizon,
      level_type: 'expected_move_band',
      // These bands support only the close-containment claim — never labeled support/resistance.
      outcome_forecast: 'close_containment',
      sessions_remaining: result.sessionsRemaining ?? null,

      // Lineage / reproducibility.
      generated_at: lineage.generatedAt,
      ingested_at: lineage.ingestedAt ?? null,
      software_sha: lineage.softwareSha,
      data_source: lineage.dataSource,
      data_snapshot_hash: lineage.dataSnapshotHash,
      data_through: lineage.dataThrough,
      price_adjustment: lineage.priceAdjustment,
      calendar_version: lineage.calendarVersion,

      // Forecast content. When unavailable, anchor fields are nulled together so the record
      // stays self-consistent (no dangling anchor_session/anchor_rule beside a null anchor).
      anchor: available ? result.levels.anchor : null,
      anchor_rule: available ? (result.anchor?.rule ?? null) : null,
      anchor_session: available ? (result.anchor?.timestamp ?? null) : null,
      estimator: available ? estimatorLabel(usedWindow) : null,
      horizon_sigma_log_return: available ? result.levels.horizonSigmaLogReturn : null,
      realized_volatility: available ? result.realizedVolatility : null,
      levels,

      quality: {
        status: quality.status ?? 'unavailable',
        // Fallback (a non-primary window) is prominently labeled, never silent.
        fallback: quality.status === 'fallback',
        requested_window: quality.requestedWindow ?? null,
        used_window: usedWindow,
        reason: available ? null : (result.reason ?? 'unavailable'),
      },
      unavailable_fields: unavailableFields,

      methodology: engineResult.methodology,
      provenance: engineResult.provenance,
    };

    record.content_hash = computeContentHash(record);
    // #5: an envelope hash over the ENTIRE record (provenance + chronology included) so no
    // stored field can be altered without detection. content_hash stays the reproducibility hash
    // (levels only); record_hash is the tamper-evidence hash (everything).
    record.record_hash = computeRecordHash(record);
    return record;
  });
}

// Required fields (and coarse types) every persisted forecast record must carry.
const REQUIRED_FORECAST_FIELDS = {
  schema_version: 'string', forecast_id: 'string', symbol: 'string', target_session: 'string',
  horizon: 'string', software_sha: 'string', data_source: 'string', data_snapshot_hash: 'string',
  data_through: 'string', price_adjustment: 'string', calendar_version: 'string',
  generated_at: 'string', content_hash: 'string',
};

/**
 * Canonical content hash for a forecast record: covers exactly the level-determining content
 * (and lineage that determines it), excluding wall-clock/ingestion fields. Used both to stamp a
 * new record and to VERIFY a stored one, so the two can never drift apart.
 */
export function computeContentHash(record) {
  return sha256Hex({
    forecast_id: record.forecast_id,
    symbol: record.symbol,
    target_session: record.target_session,
    horizon: record.horizon,
    software_sha: record.software_sha,
    data_snapshot_hash: record.data_snapshot_hash,
    calendar_version: record.calendar_version,
    anchor: record.anchor,
    estimator: record.estimator,
    horizon_sigma_log_return: record.horizon_sigma_log_return,
    levels: record.levels,
    quality_status: record.quality?.status ?? null,
    // Distinguishes the two unavailable shapes (insufficient history vs non-finite levels)
    // so re-runs that differ only in why a horizon is unavailable hash differently.
    quality_reason: record.quality?.reason ?? null,
  });
}

/**
 * Envelope hash over the whole record (every field except the two hash fields themselves), so any
 * change to provenance or chronology — data_source, price_adjustment, data_through, timestamps —
 * is detectable, not just a change to the levels.
 */
export function computeRecordHash(record) {
  const { content_hash: _ch, record_hash: _rh, ...rest } = record; // exclude the hash fields
  return sha256Hex(rest);
}

/** Coarse schema check: every required field present with the right primitive type. */
export function validateForecastRecord(record) {
  if (!record || typeof record !== 'object') return { ok: false, reason: 'not an object' };
  for (const [field, type] of Object.entries(REQUIRED_FORECAST_FIELDS)) {
    if (typeof record[field] !== type) return { ok: false, reason: `missing/invalid ${field}` };
  }
  if (record.quality == null || typeof record.quality !== 'object') return { ok: false, reason: 'missing quality' };
  return { ok: true };
}

/**
 * True iff a stored record still matches its content_hash AND its record_hash (when present) AND
 * satisfies the schema — the full read/pre-score integrity check.
 */
export function verifyForecastContentHash(record) {
  if (!validateForecastRecord(record).ok) return false;
  if (computeContentHash(record) !== record.content_hash) return false;
  if (typeof record.record_hash === 'string' && computeRecordHash(record) !== record.record_hash) return false;
  return true;
}
