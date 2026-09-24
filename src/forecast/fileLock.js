/**
 * Cross-process advisory lock + durable append for the append-only JSONL ledgers.
 *
 * Both the forecast ledger and the outcome scoreboard are read-validate-append sequences that
 * must be serialized across processes (a scheduler run can overlap a manual retry). This module
 * gives them one exclusive lock (atomic O_EXCL create, with a stale-lock steal for a crashed
 * holder) and a durable append that fsyncs the file and its parent directory, plus torn-tail
 * recovery so a crash mid-append cannot brick future writes.
 */

import {
  appendFileSync, closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, statSync, unlinkSync, writeFileSync,
} from 'fs';
import { dirname } from 'path';

const LOCK_STALE_MS = 30_000;

function acquireLock(targetPath, { maxWaitMs = 5000 } = {}) {
  const lockPath = `${targetPath}.lock`;
  const dir = dirname(lockPath);
  if (dir && !existsSync(dir)) mkdirSync(dir, { recursive: true });
  const deadline = Date.now() + maxWaitMs;
  for (;;) {
    try {
      closeSync(openSync(lockPath, 'wx')); // O_CREAT | O_EXCL: atomic across processes
      return lockPath;
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      try {
        if (Date.now() - statSync(lockPath).mtimeMs > LOCK_STALE_MS) {
          unlinkSync(lockPath); // steal a lock left by a crashed writer
          continue;
        }
      } catch { /* lock vanished between stat and now; retry */ }
      if (Date.now() >= deadline) throw new Error(`could not acquire lock ${lockPath} within ${maxWaitMs}ms`);
      const spinUntil = Date.now() + 25; // brief bounded spin; the lock is held only per-commit
      while (Date.now() < spinUntil) { /* wait */ }
    }
  }
}

function releaseLock(lockPath) {
  try { unlinkSync(lockPath); } catch { /* already gone */ }
}

/** Run `fn` while holding the exclusive lock for `targetPath`. */
export function withLock(targetPath, fn, opts) {
  const lockPath = acquireLock(targetPath, opts);
  try {
    return fn();
  } finally {
    releaseLock(lockPath);
  }
}

/**
 * If the file ends mid-line (a crashed append left a partial record with no trailing newline),
 * drop that incomplete tail so the next record starts clean. Call under the lock.
 */
export function truncateTornTail(path) {
  if (!existsSync(path)) return;
  const text = readFileSync(path, 'utf8');
  if (text === '' || text.endsWith('\n')) return;
  const lastNewline = text.lastIndexOf('\n');
  writeFileSync(path, lastNewline < 0 ? '' : text.slice(0, lastNewline + 1), 'utf8');
}

/** Append newline-terminated lines and fsync the file + parent directory for crash durability. */
export function appendLinesDurable(path, lines) {
  if (!lines.length) return;
  const dir = dirname(path);
  if (dir && !existsSync(dir)) mkdirSync(dir, { recursive: true });
  const payload = `${lines.join('\n')}\n`;
  const fd = openSync(path, 'a');
  try {
    appendFileSync(fd, payload, 'utf8');
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  // fsync the directory so the (possibly new) file entry is durable too.
  try {
    const dfd = openSync(dir || '.', 'r');
    try { fsyncSync(dfd); } finally { closeSync(dfd); }
  } catch { /* some platforms disallow dir fsync; file fsync above still holds */ }
}

/**
 * Parse a JSONL file into records, tolerating a torn final line (no trailing newline) but
 * throwing on any other malformed line. Optionally verify each record and throw on failure.
 */
export function readJsonl(path, { verify } = {}) {
  if (!existsSync(path)) return [];
  const text = readFileSync(path, 'utf8');
  if (text === '') return [];
  const endsWithNewline = text.endsWith('\n');
  const lines = text.split('\n');
  if (lines[lines.length - 1] === '') lines.pop();
  const records = [];
  lines.forEach((line, index) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    const isLastLine = index === lines.length - 1;
    let record;
    try {
      record = JSON.parse(trimmed);
    } catch (err) {
      if (isLastLine && !endsWithNewline) return; // torn final append: ignore incomplete tail
      throw new Error(`${path} line ${index + 1} is not valid JSON: ${err.message}`);
    }
    if (verify) verify(record, index + 1);
    records.push(record);
  });
  return records;
}
