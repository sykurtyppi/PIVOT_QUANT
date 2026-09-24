import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

import { withLock } from '../../src/forecast/fileLock.js';

describe('fileLock', () => {
  let dir;
  let target;
  beforeEach(() => { dir = mkdtempSync(join(tmpdir(), 'lock-')); target = join(dir, 'ledger.jsonl'); });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  test('runs the critical section and releases the lock afterward', () => {
    let ran = false;
    withLock(target, () => { ran = true; expect(existsSync(`${target}.lock`)).toBe(true); });
    expect(ran).toBe(true);
    expect(existsSync(`${target}.lock`)).toBe(false);
  });

  test('does NOT steal a lock held by a live owner (times out instead)', () => {
    // A lock owned by THIS (alive) process, held far longer than the old 30s window would allow.
    writeFileSync(`${target}.lock`, JSON.stringify({ pid: process.pid, token: 'other', ts: Date.now() - 120_000 }), 'utf8');
    expect(() => withLock(target, () => {}, { maxWaitMs: 150 })).toThrow(/could not acquire/);
    // The live owner's lock is untouched.
    expect(JSON.parse(readFileSync(`${target}.lock`, 'utf8')).token).toBe('other');
  });

  test('reclaims a lock whose owner process is gone', () => {
    // A pid that is (almost certainly) not a live process -> ownerAlive is false -> stealable.
    writeFileSync(`${target}.lock`, JSON.stringify({ pid: 2_147_483_646, token: 'dead', ts: Date.now() }), 'utf8');
    let ran = false;
    withLock(target, () => { ran = true; }, { maxWaitMs: 1000 });
    expect(ran).toBe(true);
    expect(existsSync(`${target}.lock`)).toBe(false);
  });
});
