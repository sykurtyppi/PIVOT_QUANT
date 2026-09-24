import { spawn } from 'child_process';
import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

import { readOutcomes } from '../../src/forecast/scoreForecasts.js';

// jest runs from the repo root (rootDir); resolve the worker relative to it (import.meta is
// unavailable under the babel-jest CommonJS transform).
const WORKER = join(process.cwd(), 'tests', 'forecast', 'helpers', 'concurrent_score_worker.mjs');

function runWorker(scoreboardPath, close) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [WORKER, scoreboardPath, String(close)], { stdio: 'ignore' });
    child.on('exit', (code) => resolve(code));
  });
}

describe('multiprocess scoreboard concurrency', () => {
  let dir;
  beforeEach(() => { dir = mkdtempSync(join(tmpdir(), 'stress-')); });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));

  test('conflicting concurrent writers: exactly one outcome survives, the rest are refused', async () => {
    const scoreboard = join(dir, 'scoreboard.jsonl');
    const N = 16;
    // Each worker scores the same forecast against a DIFFERENT realized close -> conflicting outcomes.
    const codes = await Promise.all(Array.from({ length: N }, (_, i) => runWorker(scoreboard, 640 + i)));

    const written = codes.filter((c) => c === 0).length;
    const refused = codes.filter((c) => c === 2).length;
    const errored = codes.filter((c) => c !== 0 && c !== 2).length;

    expect(errored).toBe(0);
    expect(written).toBe(1); // exactly one winner
    expect(refused).toBe(N - 1); // all others cleanly refused, none lost silently

    const outcomes = readOutcomes(scoreboard); // also re-verifies every outcome_hash
    expect(outcomes).toHaveLength(1);
  }, 30000);

  test('identical concurrent writers are all idempotent and leave one row', async () => {
    const scoreboard = join(dir, 'scoreboard.jsonl');
    const N = 16;
    const codes = await Promise.all(Array.from({ length: N }, () => runWorker(scoreboard, 666)));
    expect(codes.every((c) => c === 0)).toBe(true); // identical -> idempotent, never refused
    expect(readOutcomes(scoreboard)).toHaveLength(1);
  }, 30000);
});
