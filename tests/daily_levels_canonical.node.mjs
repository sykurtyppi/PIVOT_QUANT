import assert from 'node:assert/strict';
import { test } from 'node:test';
import fs from 'node:fs';
import vm from 'node:vm';
import { JSDOM } from 'jsdom';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { handleLevelsSnapshot, readLevelsSnapshot } from '../server/levels_snapshot.js';

const html = fs.readFileSync(new URL('../daily_levels.html', import.meta.url), 'utf8');
const flush = () => new Promise((resolve) => setImmediate(resolve));
function setup() {
  const dom = new JSDOM(html);
  const pending = [];
  const context = vm.createContext({
    document: dom.window.document, console, URLSearchParams, Date, setInterval: () => {},
    isTradingSession: () => true, nextTradingSession: (d) => d, holidaysInRange: () => [],
    MultiHorizonVolatilityLevels: { calculate: () => null },
    fetch: (url) => new Promise((resolve, reject) => pending.push({ url, resolve, reject })),
  });
  let script = html.match(/<script type="module">([\s\S]*?)<\/script>/)[1];
  script = script.replace(/^import .*;$/gm, '').replace(/^load(?:Evidence|Regime|LevelProbabilities|Intraday)?\(\);$/gm, '');
  vm.runInContext(script, context);
  // Keep the real async loaders/guards and canonical DOM renderer; isolate unrelated analytics.
  vm.runInContext(`renderAll = () => { $('content').hidden = false; $('price').textContent = lastData.data.symbol; };
    renderEvidence = () => { $('ev-body').textContent = evidence.marker; };`, context);
  const run = (code) => vm.runInContext(code, context);
  const response = (value) => ({ ok: true, json: async () => value });
  return { dom, pending, run, response };
}
test('synthetic publication, mocked delivery, HTTP reader and DOM preserve one identity', async () => {
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR || os.tmpdir(), 'canonical-day-'));
  let server;
  try {
    const code = `import sys,json
from pathlib import Path
from datetime import datetime,timedelta
from unittest.mock import patch
sys.path[:0]=['scripts','scripts/levels_product']
from read_snapshot import target_session
from level_snapshot import build_snapshot,write_snapshot,UTC,_session_close_utc
from trading_calendar import is_trading_day
from backfill_events import calculate_pivots
import publish
source=target_session(datetime.now(UTC))-timedelta(days=1)
while not is_trading_day(source): source-=timedelta(days=1)
captured=_session_close_utc(source)
snapshot=build_snapshot(symbol='SPY',source_session_date=source,source_ohlc={'high':605,'low':595,'close':601},generated_at_utc=captured,raw_levels=calculate_pivots(605,595,601),evidence={})
root=Path(sys.argv[1])
write_snapshot(snapshot,root)
track={'symbol':'SPY','report_session_date':source.isoformat(),'as_of_utc':captured,'horizons':{}}
briefing,_=publish.load_or_build_briefing(snapshot,track,root,captured_at_utc=captured)
with patch.object(publish,'datetime',wraps=datetime) as clock, patch.object(publish.notify,'post',return_value=True) as transport:
    clock.now.return_value=datetime.fromisoformat(captured.replace('Z','+00:00'))
    delivery=publish.deliver_briefing(briefing,out_dir=root,channel='webhook',dry_run=False)
    assert transport.call_count==1
receipt=json.loads(next(root.glob('morning_delivery_*.json')).read_text())
print(json.dumps({'snapshot':snapshot,'briefing':briefing,'receipt':receipt,'delivery':delivery}))`;
    const fixture = JSON.parse(execFileSync('./scripts/_pybin_exec.sh', ['-c', code, root], { encoding: 'utf8' }));
    assert.equal(fixture.briefing.snapshot_id, fixture.snapshot.snapshot_id);
    assert.equal(fixture.receipt.snapshot_id, fixture.snapshot.snapshot_id);
    assert.deepEqual(fixture.delivery, [true, 'sent']);
    server = http.createServer((req, res) => handleLevelsSnapshot(req, res,
      new URL(req.url, 'http://localhost'), { read: (symbol) => readLevelsSnapshot(symbol,
        { env: { ...process.env, LEVELS_SNAPSHOT_DIR: root } }) }));
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const { dom, pending, run } = setup();
    run('load()');
    const request = pending.find((p) => p.url.startsWith('/api/levels/snapshot'));
    const response = await fetch(`http://127.0.0.1:${server.address().port}${request.url}`);
    assert.equal(response.headers.get('cache-control'), 'no-store');
    request.resolve(response);
    pending.find((p) => p.url.startsWith('/api/market')).reject(new Error('synthetic provider outage'));
    await flush();
    await flush();
    const panel = dom.window.document.getElementById('canonical-map');
    assert.equal(panel.closest('[hidden]'), null, 'canonical map survives live-feed outage');
    assert.ok(panel.textContent.includes(fixture.snapshot.snapshot_id));
    for (const level of fixture.snapshot.levels) {
      const row = panel.querySelector(`[data-level-id="${level.level_id}"]`);
      assert.equal(row.textContent, `${level.label}  ${level.price.toFixed(2)}`);
      assert.ok(fixture.briefing.body.includes(level.price.toFixed(2)));
    }
    assert.equal(dom.window.document.getElementById('content').hidden, true);
  } finally {
    if (server?.listening) await new Promise((resolve) => server.close(resolve));
    fs.rmSync(root, { recursive: true, force: true });
  }
});

function snapshot(symbol = 'SPY') {
  return { status: 'available', symbol, snapshot: {
    symbol, snapshot_id: 'lvs_fixture', session_date: '2026-11-16', generated_at_utc: '2026-11-13T21:01:00Z',
    source: { kind: 'completed_rth_ohlc', session_date: '2026-11-13', as_of_utc: '2026-11-13T21:00:00Z', freshness: 'current_prior_session' },
    calculation: { family: 'classic_floor_pivots', version: 'fixture:v1' },
    levels: [{ label: 'M4', price: 605.67, level_id: 'lvl_fixture', validity: { expires_at_utc: '2026-11-16T21:00:00Z' } }],
  } };
}
function market(symbol) {
  return { symbol, candles: Array.from({ length: 35 }, (_, i) => ({ time: 1790000000 + i * 86400, close: 600 })) };
}

test('canonical map renders exact artifact values and provenance, separate from exploration', async () => {
  const { dom, pending, run, response } = setup();
  run('load()');
  const canonical = pending.find((p) => p.url.startsWith('/api/levels/snapshot'));
  assert.ok(canonical, 'canonical API requested');
  canonical.resolve(response(snapshot()));
  await flush();
  const text = dom.window.document.getElementById('canonical-map').textContent;
  for (const expected of ['605.67', 'M4', 'lvs_fixture', '2026-11-16', '2026-11-13', 'completed_rth_ohlc', 'classic_floor_pivots', 'fixture:v1']) assert.ok(text.includes(expected), expected);
  assert.ok(html.includes('Live / exploratory analytics'));
  assert.equal(dom.window.document.getElementById('canonical-map').getAttribute('aria-busy'), 'false');
});

for (const oldFirst of [true, false]) test(`symbol switch blocks stale success/error/finally; oldFirst=${oldFirst}`, async () => {
  const { dom, pending, run, response } = setup();
  run('load(); loadEvidence()');
  const old = [...pending];
  run("selectSymbol('QQQ')");
  const current = pending.filter((p) => !old.includes(p));
  const finishOld = async () => {
    old.find((p) => p.url.startsWith('/api/market')).reject(new Error('old market failure'));
    old.find((p) => p.url.startsWith('/api/levels/snapshot'))?.resolve(response(snapshot('SPY')));
    old.find((p) => p.url.includes('daily_touch_rates')).resolve(response({ marker: 'OLD SPY' }));
    await flush();
  };
  if (oldFirst) {
    await finishOld();
    assert.equal(dom.window.document.getElementById('canonical-map').getAttribute('aria-busy'), 'true');
    assert.ok(!dom.window.document.getElementById('status').textContent.includes('old market failure'));
  }
  current.find((p) => p.url.startsWith('/api/market')).resolve(response(market('QQQ')));
  current.find((p) => p.url.startsWith('/api/levels/snapshot'))?.resolve(response(snapshot('QQQ')));
  current.find((p) => p.url.includes('daily_touch_rates')).resolve(response({ marker: 'CURRENT QQQ' }));
  await flush();
  if (!oldFirst) await finishOld();
  assert.equal(dom.window.document.getElementById('price').textContent, 'QQQ');
  assert.equal(dom.window.document.getElementById('ev-body').textContent, 'CURRENT QQQ');
  assert.ok(!dom.window.document.getElementById('canonical-map').textContent.includes('SPY'));
});

test('late old-symbol market success cannot recalculate the newly selected instrument', async () => {
  const { dom, pending, run, response } = setup();
  run('load()');
  const oldMarket = pending.find((p) => p.url.startsWith('/api/market'));
  run("selectSymbol('QQQ')");
  pending.filter((p) => p.url.startsWith('/api/market')).at(-1).resolve(response(market('QQQ')));
  await flush();
  oldMarket.resolve(response(market('SPY')));
  await flush();
  assert.equal(run('lastData.data.symbol'), 'QQQ');
  assert.equal(dom.window.document.getElementById('price').textContent, 'QQQ');
});

test('wrong-symbol payload and unavailable response never become canonical map', async () => {
  const { dom, pending, run, response } = setup();
  run('load()');
  pending.find((p) => p.url.startsWith('/api/levels/snapshot'))?.resolve(response(snapshot('QQQ')));
  pending.find((p) => p.url.startsWith('/api/market')).resolve(response(market('QQQ')));
  await flush();
  assert.ok(dom.window.document.getElementById('canonical-status').textContent.includes('symbol_mismatch'));
  assert.equal(dom.window.document.getElementById('canonical-levels').textContent, '');
  assert.equal(dom.window.document.getElementById('content').hidden, true);
  run('load()');
  pending.filter((p) => p.url.startsWith('/api/levels/snapshot')).at(-1).resolve(response({ status: 'unavailable', symbol: 'SPY', reason: 'stale' }));
  await flush();
  assert.ok(dom.window.document.getElementById('canonical-status').textContent.includes('stale'));
  assert.equal(dom.window.document.getElementById('canonical-levels').textContent, '');
});

test('same-symbol refresh generation rejects late success and late error', async () => {
  const { dom, pending, run, response } = setup();
  run('load(); load()');
  const maps = pending.filter((p) => p.url.startsWith('/api/levels/snapshot'));
  assert.equal(maps.length, 2);
  maps[1].resolve(response(snapshot()));
  await flush();
  maps[0].reject(new Error('old failure'));
  await flush();
  assert.ok(dom.window.document.getElementById('canonical-map').textContent.includes('lvs_fixture'));
  assert.equal(dom.window.document.getElementById('canonical-map').getAttribute('aria-busy'), 'false');
});
