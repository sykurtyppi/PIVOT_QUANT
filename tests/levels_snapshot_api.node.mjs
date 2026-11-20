import assert from 'node:assert/strict';
import { test } from 'node:test';
import http from 'node:http';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { handleLevelsSnapshot, readLevelsSnapshot } from '../server/levels_snapshot.js';

test('GET route is no-store, rejects query paths/duplicates and write methods', async () => {
  const calls = [];
  const server = http.createServer((req, res) => handleLevelsSnapshot(req, res,
    new URL(req.url, 'http://localhost'), { read: async (symbol) => {
      calls.push(symbol);
      return { status: 'unavailable', symbol, reason: 'missing' };
    } }));
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    const base = `http://127.0.0.1:${server.address().port}/api/levels/snapshot`;
    for (const query of ['?symbol=../SPY', '?symbol=spy', '?symbol=SPY&path=/etc/passwd', '?symbol=SPY&symbol=QQQ', '']) {
      const response = await fetch(base + query);
      assert.equal(response.status, 400);
      assert.equal(response.headers.get('cache-control'), 'no-store');
    }
    assert.equal((await fetch(base + '?symbol=SPY', { method: 'POST' })).status, 405);
    const response = await fetch(base + '?symbol=SPY');
    assert.equal(response.status, 200);
    assert.equal((await response.json()).reason, 'missing');
    assert.deepEqual(calls, ['SPY']);
  } finally { await new Promise((resolve) => server.close(resolve)); }
});

test('real CLI is hermetic with trusted artifact root and missing snapshots', async () => {
  const root = await fs.mkdtemp(path.join(process.env.TMPDIR || os.tmpdir(), 'levels-api-'));
  try {
    const result = await readLevelsSnapshot('SPY', { env: { ...process.env, LEVELS_SNAPSHOT_DIR: root } });
    assert.equal(result.status, 'unavailable');
    assert.equal(result.reason, 'missing');
    assert.equal(result.symbol, 'SPY');
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});

test('real CLI returns exact validated synthetic artifact then rejects tampering', async () => {
  const root = await fs.mkdtemp(path.join(process.env.TMPDIR || os.tmpdir(), 'levels-api-'));
  try {
    const code = `import sys,json\nfrom pathlib import Path\nfrom datetime import datetime,timedelta\nsys.path[:0]=['scripts','scripts/levels_product']\nfrom read_snapshot import target_session\nfrom level_snapshot import build_snapshot,write_snapshot,UTC,ET,_session_close_utc\nfrom trading_calendar import is_trading_day\nfrom backfill_events import calculate_pivots\nsource=target_session(datetime.now(UTC))-timedelta(days=1)\nwhile not is_trading_day(source): source-=timedelta(days=1)\nsnapshot=build_snapshot(symbol='SPY',source_session_date=source,source_ohlc={'high':605,'low':595,'close':601},generated_at_utc=_session_close_utc(source),raw_levels=calculate_pivots(605,595,601),evidence={})\nwrite_snapshot(snapshot,Path(sys.argv[1]))\nprint(json.dumps(snapshot))`;
    const expected = JSON.parse(execFileSync('./scripts/_pybin_exec.sh', ['-c', code, root], { encoding: 'utf8' }));
    const options = { env: { ...process.env, LEVELS_SNAPSHOT_DIR: root } };
    const result = await readLevelsSnapshot('SPY', options);
    assert.equal(result.status, 'available');
    assert.deepEqual(result.snapshot, expected);
    expected.levels[0].price += 1;
    await fs.writeFile(path.join(root, 'levels_snapshot_SPY_latest.json'), JSON.stringify(expected));
    const invalid = await readLevelsSnapshot('SPY', options);
    assert.equal(invalid.reason, 'invalid');
    assert.ok(!invalid.snapshot);
  } finally { await fs.rm(root, { recursive: true, force: true }); }
});

test('subprocess failure/timeout/oversized output fail closed; no data fallback', async () => {
  for (const fail of [Object.assign(new Error('timeout'), { killed: true }), new Error('maxBuffer')]) {
    const result = await readLevelsSnapshot('QQQ', { run: (_file, _args, options, callback) => {
      assert.equal(options.timeout, 5000);
      assert.equal(options.maxBuffer, 524288);
      callback(fail);
    } });
    assert.equal(result.reason, 'reader_failed');
    assert.equal(result.status, 'unavailable');
    assert.ok(!result.snapshot);
  }
});

test('reader saturation does not spawn and capacity recovers after success and failure', async () => {
  const callbacks = [];
  const run = (_file, _args, _options, callback) => callbacks.push(callback);
  const held = Array.from({ length: 4 }, () => readLevelsSnapshot('SPY', { run }));
  try {
    assert.equal(callbacks.length, 4);
    assert.equal((await readLevelsSnapshot('SPY', { run })).reason, 'reader_busy');
    assert.equal(callbacks.length, 4, 'busy request must not spawn');
    callbacks[0](null, JSON.stringify({ status: 'unavailable', symbol: 'SPY', reason: 'missing' }));
    assert.equal((await held[0]).reason, 'missing');
    const replacement = readLevelsSnapshot('SPY', { run });
    held.push(replacement);
    assert.equal(callbacks.length, 5, 'successful completion releases capacity');
    callbacks[1](new Error('synthetic timeout'));
    assert.equal((await held[1]).reason, 'reader_failed');
    const recovered = readLevelsSnapshot('SPY', { run });
    held.push(recovered);
    assert.equal(callbacks.length, 6, 'failed completion releases capacity');
  } finally {
    for (const callback of callbacks.slice(2)) callback(new Error('cleanup'));
    await Promise.all(held);
  }
  assert.equal((await readLevelsSnapshot('SPY', { run: () => { throw new Error('spawn failure'); } })).reason, 'reader_failed');
  assert.equal((await readLevelsSnapshot('SPY', { run: (_f, _a, _o, cb) => cb(null,
    JSON.stringify({ status: 'unavailable', symbol: 'SPY', reason: 'missing' })) })).reason, 'missing');
});

test('resolver wrapper and fixed CLI arguments; never shell or query paths', async () => {
  let called = false;
  const result = await readLevelsSnapshot('SPY', { env: { PYTHON_BIN: '/trusted/python' }, run: (file, args, options, callback) => {
    called = true;
    assert.ok(file.endsWith('/scripts/_pybin_exec.sh'));
    assert.deepEqual(args.slice(-2), ['--symbol', 'SPY']);
    assert.equal(options.env.PYTHON_BIN, '/trusted/python');
    assert.equal(options.shell, false);
    callback(null, JSON.stringify({ status: 'unavailable', symbol: 'SPY', reason: 'missing' }));
  } });
  assert.ok(called);
  assert.equal(result.reason, 'missing');
  assert.equal((await readLevelsSnapshot('../SPY')).reason, 'invalid_symbol');
});
