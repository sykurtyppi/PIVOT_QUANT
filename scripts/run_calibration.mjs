#!/usr/bin/env node
/**
 * Point-in-time calibration study for the expected-move bands (roadmap §3, first cut).
 *
 * Fetches ~10y of split/dividend-adjusted daily SPY, reconstructs each morning's daily band
 * from prior data only, and reports empirical ±1σ / ±2σ close-containment vs the nominal
 * normal targets, with seeded block-bootstrap CIs, per-year coverage, breach asymmetry, and a
 * high/low realized-volatility regime split. The exact input series is hashed for provenance.
 *
 * Usage: node scripts/run_calibration.mjs [--symbol SPY] [--range 10y] [--window 20]
 *
 * This is a RESEARCH backfill: it pulls Yahoo's current-vintage adjusted history directly (not
 * point-in-time vintage). The forward ledger accumulates true vintages from today.
 */

import { createHash } from 'crypto';
import { mkdir, writeFile } from 'fs/promises';

import { dailyCoverage, summarizeCoverage } from '../src/forecast/calibration.js';
import { mapMarketResponseToBars } from '../src/forecast/dataSourceYahoo.js';

function parseArgs(argv) {
  const a = {};
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i].startsWith('--')) { a[argv[i].slice(2)] = argv[i + 1]; i += 1; }
  }
  return a;
}

async function fetchYahoo(symbol, range) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=${range}&interval=1d`;
  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (calibration-research)' } });
  if (!res.ok) throw new Error(`Yahoo returned HTTP ${res.status}`);
  const j = await res.json();
  const r = j?.chart?.result?.[0];
  const ts = r?.timestamp || [];
  const adj = r?.indicators?.adjclose?.[0]?.adjclose || [];
  const raw = r?.indicators?.quote?.[0]?.close || [];
  const candles = ts.map((t, i) => ({ time: t, close: raw[i], adjclose: adj[i] }))
    .filter((c) => Number.isFinite(c.adjclose) && c.adjclose > 0);
  return { candles };
}

const pct = (x) => `${(x * 100).toFixed(1)}%`;
const pp = (x) => `${x >= 0 ? '+' : ''}${(x * 100).toFixed(1)}pp`;

function regimeSplit(results) {
  const sigmas = results.map((r) => r.sigma).sort((a, b) => a - b);
  const median = sigmas[Math.floor(sigmas.length / 2)];
  const low = results.filter((r) => r.sigma <= median);
  const high = results.filter((r) => r.sigma > median);
  const cov = (rs, key) => rs.filter((r) => r[key]).length / rs.length;
  return {
    median_daily_sigma: median,
    low_vol: { n: low.length, coverage1: cov(low, 'within1'), coverage2: cov(low, 'within2') },
    high_vol: { n: high.length, coverage1: cov(high, 'within1'), coverage2: cov(high, 'within2') },
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const symbol = (args.symbol || 'SPY').toUpperCase();
  const range = args.range || '10y';
  const window = Number(args.window || 20);
  if (!Number.isInteger(window) || window < 2) throw new Error('--window must be an integer >= 2');

  const retrievedAt = new Date().toISOString();
  const response = await fetchYahoo(symbol, range);
  const bars = mapMarketResponseToBars(response);
  const snapshotHash = createHash('sha256')
    .update(JSON.stringify(bars.map((b) => [b.timestamp, b.close]))).digest('hex');

  // #7: persist the immutable normalized snapshot so this run is independently reproducible
  // and never overwritten (Yahoo re-adjusts history, so a hash alone is not provenance).
  const snapDir = new URL('../research/calibration/snapshots/', import.meta.url);
  await mkdir(snapDir, { recursive: true });
  const snapPath = new URL(`${symbol}_${range}_${snapshotHash.slice(0, 12)}.json`, snapDir);
  await writeFile(snapPath, JSON.stringify({
    symbol, range, retrieved_at: retrievedAt, provider: 'yahoo_v8_chart',
    price_adjustment: 'split_and_dividend_adjusted', bar_count: bars.length,
    data_snapshot_sha256: snapshotHash, bars,
  }, null, 0));

  const results = dailyCoverage(bars, { window });
  const s = summarizeCoverage(results, { window });
  const regimes = regimeSplit(results);

  console.log(`\n=== Point-in-time daily calibration — ${symbol} (window=${window}) ===`);
  console.log(`data: ${bars.length} adjusted sessions ${bars[0].timestamp.slice(0, 10)} .. ${bars[bars.length - 1].timestamp.slice(0, 10)}`);
  console.log(`data_snapshot_sha256: ${snapshotHash}`);
  console.log(`snapshot persisted: research/calibration/snapshots/${symbol}_${range}_${snapshotHash.slice(0, 12)}.json`);
  console.log(`scored forecast/outcome pairs: ${s.n}\n`);

  console.log('close containment (daily horizon) — empirical vs the ESTIMATOR-AWARE null');
  console.log(`(bands use sigma from ${window} obs, so the correct null is a ~t${window - 1}, not the asymptotic normal):`);
  console.log(`  +/-1sigma: empirical ${pct(s.coverage1)}  95% CI [${pct(s.ci1.lo)}, ${pct(s.ci1.hi)}]`);
  console.log(`            estimator-aware null ${pct(s.estimator_null_1)} (asymptotic-normal ref ${pct(s.nominal1)})  -> vs null ${pp(s.calibration_error_vs_null_1)}`);
  console.log(`  +/-2sigma: empirical ${pct(s.coverage2)}  95% CI [${pct(s.ci2.lo)}, ${pct(s.ci2.hi)}]`);
  console.log(`            estimator-aware null ${pct(s.estimator_null_2)} (asymptotic-normal ref ${pct(s.nominal2)})  -> vs null ${pp(s.calibration_error_vs_null_2)}`);
  const nullInCi2 = s.estimator_null_2 >= s.ci2.lo && s.estimator_null_2 <= s.ci2.hi;
  console.log(`  interpretation: the +/-2sigma CI ${nullInCi2 ? 'CONTAINS' : 'excludes'} the estimator-aware null` +
    `${nullInCi2 ? ' -> consistent with Gaussian+estimation noise, does NOT establish fat tails' : ' -> evidence beyond estimation noise'}.`);
  console.log(`  1sigma breaches: upper ${s.breach_1sigma_upper}, lower ${s.breach_1sigma_lower} (asymmetry ${pp(s.breach_asymmetry_1)})`);

  console.log('\nby realized-volatility regime (median split):');
  console.log(`  low-vol  (n=${regimes.low_vol.n}):  1s ${pct(regimes.low_vol.coverage1)}  2s ${pct(regimes.low_vol.coverage2)}`);
  console.log(`  high-vol (n=${regimes.high_vol.n}):  1s ${pct(regimes.high_vol.coverage1)}  2s ${pct(regimes.high_vol.coverage2)}`);

  console.log('\nper-year coverage:');
  for (const y of s.per_year) {
    console.log(`  ${y.year}: n=${String(y.n).padStart(3)}  1s ${pct(y.coverage1).padStart(6)}  2s ${pct(y.coverage2).padStart(6)}`);
  }
  console.log('');
}

main().catch((err) => { console.error(`calibration failed: ${err.message}`); process.exit(1); });
