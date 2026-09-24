#!/usr/bin/env node
/**
 * Estimator tournament (roadmap §4): does the 20-day RV band actually forecast the daily move
 * better than trivial baselines? Fetches ~10y adjusted daily SPY, computes point-in-time daily
 * bands for several volatility estimators, and scores each with the Weighted Interval Score
 * (lower = better). Reports mean WIS, coverage, and average ±2σ width per method, plus a seeded
 * block-bootstrap 95% CI on the per-day WIS DIFFERENCE (baseline − rv20): a CI strictly above 0
 * means rv20 is significantly better than that baseline.
 *
 * Usage: node scripts/run_tournament.mjs [--symbol SPY] [--range 10y] [--holdout-frac 0.3]
 */

import { mapMarketResponseToBars } from '../src/forecast/dataSourceYahoo.js';
import { logReturns } from '../src/forecast/calibration.js';
import {
  blockBootstrapMeanCI, runTournament, TOURNAMENT_METHODS,
} from '../src/forecast/estimatorTournament.js';

function parseArgs(argv) {
  const a = {};
  for (let i = 0; i < argv.length; i += 1) if (argv[i].startsWith('--')) { a[argv[i].slice(2)] = argv[i + 1]; i += 1; }
  return a;
}

async function fetchYahoo(symbol, range) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=${range}&interval=1d`;
  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (tournament-research)' } });
  if (!res.ok) throw new Error(`Yahoo returned HTTP ${res.status}`);
  const r = (await res.json())?.chart?.result?.[0];
  const ts = r?.timestamp || [];
  const adj = r?.indicators?.adjclose?.[0]?.adjclose || [];
  const raw = r?.indicators?.quote?.[0]?.close || [];
  return { candles: ts.map((t, i) => ({ time: t, close: raw[i], adjclose: adj[i] })).filter((c) => Number.isFinite(c.adjclose) && c.adjclose > 0) };
}

const f = (x, d = 4) => x.toFixed(d);

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const symbol = (args.symbol || 'SPY').toUpperCase();
  const range = args.range || '10y';
  const holdoutFrac = args['holdout-frac'] !== undefined ? Number(args['holdout-frac']) : 0.3;

  const bars = mapMarketResponseToBars(await fetchYahoo(symbol, range));
  const returns = logReturns(bars);
  const holdoutFrom = Math.floor(returns.length * (1 - holdoutFrac));

  const full = runTournament(returns, { warmup: 60 });
  const holdout = runTournament(returns, { warmup: 60, from: holdoutFrom });

  const report = (label, res) => {
    console.log(`\n=== ${label} (${res.n} days) — mean WIS (lower is better) ===`);
    const rows = TOURNAMENT_METHODS.map((m) => ({ m, ...res.perMethod[m] }))
      .sort((a, b) => a.meanWis - b.meanWis);
    for (const row of rows) {
      const flag = row.m === 'rv20' ? '  <- our method' : '';
      console.log(`  ${row.m.padEnd(10)} WIS ${f(row.meanWis, 5)}  cov1 ${f(row.coverage1 * 100, 1)}%  cov2 ${f(row.coverage2 * 100, 1)}%  width2 ${f(row.meanWidth2, 4)}${flag}`);
    }
    // Bootstrap CI on the per-day WIS improvement of rv20 vs each baseline (baseline - rv20).
    console.log('  rv20 improvement over each baseline (per-day WIS diff, 95% block-bootstrap CI):');
    for (const m of TOURNAMENT_METHODS) {
      if (m === 'rv20') continue;
      const diff = res.perDayWis[m].map((v, i) => v - res.perDayWis.rv20[i]); // >0 => rv20 better
      const ci = blockBootstrapMeanCI(diff, { seed: 20260924 });
      const verdict = ci.lo > 0 ? 'rv20 BETTER' : ci.hi < 0 ? 'rv20 WORSE' : 'tie (CI spans 0)';
      console.log(`    vs ${m.padEnd(10)} mean ${f(ci.mean, 6)}  CI [${f(ci.lo, 6)}, ${f(ci.hi, 6)}]  -> ${verdict}`);
    }
  };

  console.log(`data: ${bars.length} adjusted sessions ${bars[0].timestamp.slice(0, 10)} .. ${bars[bars.length - 1].timestamp.slice(0, 10)}`);
  report('FULL SAMPLE', full);
  report(`HOLDOUT (last ${Math.round(holdoutFrac * 100)}%)`, holdout);
  console.log('');
}

main().catch((err) => { console.error(`tournament failed: ${err.message}`); process.exit(1); });
