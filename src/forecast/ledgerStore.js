/**
 * Append-only forecast ledger store (JSONL).
 *
 * Immutability is the whole point: a forecast, once written for a (forecast_id), is never
 * overwritten. A re-run that produces a byte-identical record (same content_hash) is an
 * idempotent no-op; a re-run that produces a DIFFERENT record for the same forecast_id is
 * a hard error, because it means something that should have been frozen has changed.
 *
 * Concurrency: appends are serialized with an exclusive on-disk lock, and a whole batch of
 * horizons commits two-phase (validate every record, then append all) so a conflict on a
 * later horizon cannot leave earlier horizons half-written. Reads tolerate a torn trailing
 * line (an interrupted append) instead of bricking every future write, and detect a
 * duplicated forecast_id rather than silently returning the first copy.
 */

import { appendFileSync, closeSync, existsSync, mkdirSync, openSync, readFileSync, statSync, unlinkSync, writeFileSync } from 'fs';
import { dirname } from 'path';

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

/**
 * Parse the ledger into records. Throws on a malformed line UNLESS it is the final line and
 * the file does not end in a newline — that is treated as an interrupted (torn) append and
 * ignored, so a crash mid-write cannot poison every subsequent read/append.
 */
export function readForecasts(ledgerPath) {
  if (!existsSync(ledgerPath)) return [];
  const text = readFileSync(ledgerPath, 'utf8');
  if (text === '') return [];
  const endsWithNewline = text.endsWith('\n');
  const lines = text.split('\n');
  if (lines[lines.length - 1] === '') lines.pop(); // drop the empty tail after a trailing newline
  const records = [];
  lines.forEach((line, index) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    const isLastLine = index === lines.length - 1;
    try {
      records.push(JSON.parse(trimmed));
    } catch (err) {
      if (isLastLine && !endsWithNewline) return; // torn final append: ignore incomplete tail
      throw new Error(`ledger ${ledgerPath} line ${index + 1} is not valid JSON: ${err.message}`);
    }
  });
  return records;
}

/** Find a record by id, throwing if the ledger somehow contains more than one for that id. */
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

const LOCK_STALE_MS = 30_000;

function acquireLock(ledgerPath, { maxWaitMs = 5000 } = {}) {
  const lockPath = `${ledgerPath}.lock`;
  const dir = dirname(lockPath);
  if (dir && !existsSync(dir)) mkdirSync(dir, { recursive: true });
  const deadline = Date.now() + maxWaitMs;
  for (;;) {
    try {
      closeSync(openSync(lockPath, 'wx')); // O_CREAT | O_EXCL
      return lockPath;
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      try {
        if (Date.now() - statSync(lockPath).mtimeMs > LOCK_STALE_MS) {
          unlinkSync(lockPath); // steal a stale lock left by a crashed writer
          continue;
        }
      } catch { /* lock vanished between stat and now; retry */ }
      if (Date.now() >= deadline) {
        throw new Error(`could not acquire ledger lock ${lockPath} within ${maxWaitMs}ms`);
      }
      const spinUntil = Date.now() + 25; // brief bounded spin; the lock is held only per-commit
      while (Date.now() < spinUntil) { /* wait */ }
    }
  }
}

function releaseLock(lockPath) {
  try { unlinkSync(lockPath); } catch { /* already gone */ }
}

/**
 * If the ledger ends mid-line (a crashed append left a partial record with no trailing
 * newline), drop that incomplete tail before writing, so the new record starts on a clean
 * line instead of being concatenated onto the fragment. Must run under the lock.
 */
function truncateTornTail(ledgerPath) {
  if (!existsSync(ledgerPath)) return;
  const text = readFileSync(ledgerPath, 'utf8');
  if (text === '' || text.endsWith('\n')) return;
  const lastNewline = text.lastIndexOf('\n');
  writeFileSync(ledgerPath, lastNewline < 0 ? '' : text.slice(0, lastNewline + 1), 'utf8');
}

/**
 * Two-phase append of one or more records under an exclusive lock.
 *  - Every record's forecast_id must be absent, or present with an identical content_hash.
 *  - A present id with a different content_hash aborts the WHOLE batch (nothing is written).
 *  - Otherwise all new records are appended together in a single write.
 * @returns {Array<{written:boolean, idempotent:boolean, record:object}>} one per input record.
 */
export function appendForecasts(ledgerPath, records) {
  const list = Array.isArray(records) ? records : [records];
  for (const record of list) {
    if (!record || typeof record.forecast_id !== 'string' || typeof record.content_hash !== 'string') {
      throw new Error('appendForecasts requires records with forecast_id and content_hash');
    }
  }
  const dir = dirname(ledgerPath);
  if (dir && !existsSync(dir)) mkdirSync(dir, { recursive: true });

  const lockPath = acquireLock(ledgerPath);
  try {
    truncateTornTail(ledgerPath); // recover from a crashed prior append before writing
    const existingById = new Map();
    for (const rec of readForecasts(ledgerPath)) existingById.set(rec.forecast_id, rec);

    // Guard against a batch that itself carries two different records for one id.
    const batchHashes = new Map();
    for (const record of list) {
      const prev = batchHashes.get(record.forecast_id);
      if (prev != null && prev !== record.content_hash) {
        throw new LedgerImmutabilityError(
          `batch contains conflicting records for ${record.forecast_id}`,
          record.forecast_id,
        );
      }
      batchHashes.set(record.forecast_id, record.content_hash);
    }

    // Phase 1: validate every record against the ledger; abort on any conflict (write nothing).
    const plan = list.map((record) => {
      const existing = existingById.get(record.forecast_id);
      if (existing && existing.content_hash !== record.content_hash) {
        throw new LedgerImmutabilityError(
          `forecast ${record.forecast_id} already exists with a different content_hash ` +
            `(${existing.content_hash} != ${record.content_hash}); the ledger is append-only`,
          record.forecast_id,
        );
      }
      return { record, existing };
    });

    // Phase 2: append everything new in one write.
    const toWrite = plan.filter((p) => !p.existing);
    if (toWrite.length) {
      appendFileSync(ledgerPath, `${toWrite.map((p) => JSON.stringify(p.record)).join('\n')}\n`, 'utf8');
    }
    return plan.map((p) => (p.existing
      ? { written: false, idempotent: true, record: p.existing }
      : { written: true, idempotent: false, record: p.record }));
  } finally {
    releaseLock(lockPath);
  }
}

/** Append a single record (convenience wrapper over the two-phase batch append). */
export function appendForecast(ledgerPath, record) {
  return appendForecasts(ledgerPath, [record])[0];
}
