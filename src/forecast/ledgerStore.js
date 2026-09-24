/**
 * Append-only forecast ledger store (JSONL).
 *
 * Immutability: a forecast, once written for its identity, is never overwritten. A re-run that
 * produces a byte-identical record (same content_hash) is an idempotent no-op; a re-run that
 * produces a DIFFERENT record for the same identity aborts. Writes are serialized across
 * processes by an exclusive lock and committed two-phase (validate every record, then append
 * all durably), reads recompute and VERIFY each record's content_hash (tamper/corruption
 * detection) and detect duplicate identities, and a torn trailing line is recovered rather than
 * bricking future writes.
 *
 * Identity is (symbol, target_session, horizon) — NOT the versioned forecast_id — so a forecast
 * cannot be silently republished under a bumped version after its outcome is known. A genuine
 * pre-outcome correction must be an explicit superseding record (allowSupersede), which is
 * itself appended and never deletes the original.
 */

import { verifyForecastContentHash } from './forecastRecord.js';
import { appendLinesDurable, readJsonl, truncateTornTail, withLock } from './fileLock.js';

export class LedgerImmutabilityError extends Error {
  constructor(message, forecastId) {
    super(message);
    this.name = 'LedgerImmutabilityError';
    this.forecastId = forecastId;
  }
}

export class LedgerIntegrityError extends Error {
  constructor(message, forecastId) {
    super(message);
    this.name = 'LedgerIntegrityError';
    this.forecastId = forecastId;
  }
}

function verifier(record, line) {
  if (!verifyForecastContentHash(record)) {
    throw new LedgerIntegrityError(
      `forecast record on line ${line} (${record?.forecast_id}) fails content_hash verification`,
      record?.forecast_id,
    );
  }
}

/** Identity that must be unique regardless of version. */
function identityOf(record) {
  return `${record.symbol}::${record.target_session}::${record.horizon}`;
}

/**
 * Read + verify every forecast record. Set { verify: false } only for raw inspection; the
 * default recomputes each content_hash and throws on any mismatch.
 */
export function readForecasts(ledgerPath, { verify = true } = {}) {
  return readJsonl(ledgerPath, verify ? { verify: verifier } : {});
}

/** Find a record by forecast_id, throwing if the ledger holds more than one for that id. */
export function findForecast(ledgerPath, forecastId) {
  const matches = readForecasts(ledgerPath).filter((r) => r.forecast_id === forecastId);
  if (matches.length > 1) {
    throw new LedgerIntegrityError(
      `ledger contains ${matches.length} records for ${forecastId}; append-only invariant violated`,
      forecastId,
    );
  }
  return matches[0] || null;
}

/**
 * Two-phase append of one or more records under an exclusive cross-process lock.
 *  - New identity, or same identity with identical content_hash -> written / idempotent.
 *  - Same identity with different content aborts the WHOLE batch, unless the record sets
 *    `supersedes` (the superseded forecast_id) and `allowSupersede` is passed — then it is
 *    appended as an explicit, linked correction (the original is retained).
 * @returns {Array<{written:boolean, idempotent:boolean, record:object}>} one per input record.
 */
export function appendForecasts(ledgerPath, records, { allowSupersede = false } = {}) {
  const list = Array.isArray(records) ? records : [records];
  for (const record of list) {
    if (!record || typeof record.forecast_id !== 'string' || typeof record.content_hash !== 'string') {
      throw new Error('appendForecasts requires records with forecast_id and content_hash');
    }
    if (!verifyForecastContentHash(record)) {
      throw new LedgerIntegrityError(`record ${record.forecast_id} content_hash does not match its content`, record.forecast_id);
    }
  }

  return withLock(ledgerPath, () => {
    truncateTornTail(ledgerPath); // recover from a crashed prior append before writing
    const existing = readForecasts(ledgerPath);
    const byId = new Map(existing.map((r) => [r.forecast_id, r]));
    const byIdentity = new Map(existing.map((r) => [identityOf(r), r]));

    // Phase 1: validate every record against the ledger AND against already-accepted records in
    // this same batch (each accepted record is registered before the next is checked), so a
    // single "atomic" call cannot itself introduce a duplicate id or identity. Abort on any
    // conflict — nothing is written.
    const plan = [];
    for (const record of list) {
      const identity = identityOf(record);
      const sameId = byId.get(record.forecast_id);
      if (sameId) {
        if (sameId.content_hash === record.content_hash) { plan.push({ record, existing: sameId }); continue; }
        throw new LedgerImmutabilityError(
          `forecast ${record.forecast_id} already exists with a different content_hash; the ledger is append-only`,
          record.forecast_id,
        );
      }
      const sameIdentity = byIdentity.get(identity);
      if (sameIdentity) {
        const isLinkedSupersede = allowSupersede && record.supersedes === sameIdentity.forecast_id;
        if (!isLinkedSupersede) {
          throw new LedgerImmutabilityError(
            `a forecast already exists for ${identity} (${sameIdentity.forecast_id}); ` +
              'republishing under a new version is refused. Pass allowSupersede with record.supersedes to correct it before the outcome.',
            record.forecast_id,
          );
        }
      }
      // Register the accepted record so a later record in this batch conflicts against it.
      byId.set(record.forecast_id, record);
      byIdentity.set(identity, record);
      plan.push({ record, existing: null });
    }

    // Phase 2: append everything new in one durable write.
    const toWrite = plan.filter((p) => !p.existing);
    if (toWrite.length) appendLinesDurable(ledgerPath, toWrite.map((p) => JSON.stringify(p.record)));
    return plan.map((p) => (p.existing
      ? { written: false, idempotent: true, record: p.existing }
      : { written: true, idempotent: false, record: p.record }));
  });
}

/** Append a single record (convenience wrapper over the two-phase batch append). */
export function appendForecast(ledgerPath, record, opts) {
  return appendForecasts(ledgerPath, [record], opts)[0];
}
