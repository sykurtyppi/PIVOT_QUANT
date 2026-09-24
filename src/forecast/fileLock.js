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
  appendFileSync, closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, statSync, unlinkSync, writeFileSync, writeSync,
} from 'fs';
import { randomBytes } from 'crypto';
import { dirname } from 'path';

// A very long belt-and-suspenders age. The PRIMARY staleness test is owner-process liveness, so
// a lock is never stolen from a live owner merely because it has been held a while (a long fsync,
// a big ledger, a scheduling delay); only a dead owner's lock — or an absurdly old one — is stolen.
const LOCK_ANCIENT_MS = 600_000;

function ownerAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0); // signal 0 = liveness probe, delivers nothing
    return true;
  } catch (err) {
    return err.code === 'EPERM'; // exists but owned by another user
  }
}

function readLockHolder(lockPath) {
  try {
    return JSON.parse(readFileSync(lockPath, 'utf8'));
  } catch {
    return null;
  }
}

/**
 * Acquire an exclusive lease on `targetPath`, returning {lockPath, token}. The lock file records
 * the owner pid and a random token. A held lock is stolen only when its owner process is gone (or
 * the lock is absurdly old), never merely because a fixed timeout elapsed — so a live owner keeps
 * mutual exclusion through long I/O. Release removes the lock only if it still carries our token.
 */
function acquireLock(targetPath, { maxWaitMs = 5000 } = {}) {
  const lockPath = `${targetPath}.lock`;
  const dir = dirname(lockPath);
  if (dir && !existsSync(dir)) mkdirSync(dir, { recursive: true });
  const token = randomBytes(12).toString('hex');
  const deadline = Date.now() + maxWaitMs;
  for (;;) {
    try {
      const fd = openSync(lockPath, 'wx'); // O_CREAT | O_EXCL: the atomic arbiter across processes
      try {
        writeSync(fd, JSON.stringify({ pid: process.pid, token, ts: Date.now() }));
        fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
      return { lockPath, token };
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      const holder = readLockHolder(lockPath);
      let mtimeMs = Date.now();
      try { mtimeMs = statSync(lockPath).mtimeMs; } catch { continue; /* vanished; retry create */ }
      const ownerGone = !holder || !ownerAlive(holder.pid);
      const ancient = Date.now() - mtimeMs > LOCK_ANCIENT_MS;
      if (ownerGone || ancient) {
        // Remove the dead owner's lock; the O_EXCL create is the real arbiter, so two concurrent
        // stealers cannot both win — the loser just re-evaluates on the next iteration.
        try { unlinkSync(lockPath); } catch { /* another stealer got there first */ }
        continue;
      }
      if (Date.now() >= deadline) {
        throw new Error(`could not acquire lock ${lockPath} within ${maxWaitMs}ms (held by live pid ${holder?.pid})`);
      }
      const spinUntil = Date.now() + 25; // brief bounded spin; the lock is held only per-commit
      while (Date.now() < spinUntil) { /* wait */ }
    }
  }
}

function releaseLock(lease) {
  const holder = readLockHolder(lease.lockPath);
  if (holder && holder.token === lease.token) {
    try { unlinkSync(lease.lockPath); } catch { /* already gone */ }
  }
  // else: a later owner holds it now (we were stolen from as a presumed-dead owner) — leave it.
}

/** Run `fn` while holding the exclusive lease for `targetPath`. */
export function withLock(targetPath, fn, opts) {
  const lease = acquireLock(targetPath, opts);
  try {
    return fn();
  } finally {
    releaseLock(lease);
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
