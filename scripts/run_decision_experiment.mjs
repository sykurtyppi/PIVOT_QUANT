#!/usr/bin/env node
/**
 * Volatility-targeting decision experiment (roadmap §4 gate). Runs the frozen design in
 * research/calibration/decision_experiment_prereg.md: does sizing SPY exposure by a vol forecast
 * improve net risk-adjusted decision utility over fixed exposure and over the EWMA hurdle?
 *
 * Usage: node scripts/run_decision_experiment.mjs [--range 10y] [--holdout-frac 0.3] [--financing 0]
 */

import { createHash } from 'crypto';

import { mapMarketResponseToBars } from '../src/forecast/dataSourceYahoo.js';
import { logReturns } from '../src/forecast/calibration.js';
import { runExperiment, blockBootstrapMeanCI } from '../src/forecast/volTargeting.js';

function parseArgs(argv) {
  const a = {};
  for (let i = 0; i < argv.length; i += 1) if (argv[i].startsWith('--')) { a[argv[i].slice(2)] = argv[i + 1]; i += 1; }
  return a;
}

async function fetchYahoo(symbol, range) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=${range}&interval=1d`;
  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (decision-research)' } });
  if (!res.ok) throw new Error(`Yahoo returned HTTP ${res.status}`);
  const r = (await res.json())?.chart?.result?.[0];
  const ts = r?.timestamp || [];
  const adj = r?.indicators?.adjclose?.[0]?.adjclose || [];
  const raw = r?.indicators?.quote?.[0]?.close || [];
  return { candles: ts.map((t, i) => ({ time: t, close: raw[i], adjclose: adj[i] })).filter((c) => Number.isFinite(c.adjclose) && c.adjclose > 0) };
}

const f = (x, d = 2) => (Number.isFinite(x) ? x.toFixed(d) : 'n/a');
const pct = (x, d = 1) => `${f(x * 100, d)}%`;

function reportBlock(label, exp) {
  console.log(`\n=== ${label} (${exp.strategies.fixed.n} days) ===`);
  console.log('  method     annRet   annVol  Sharpe   maxDD    ES5     turn/day  CE(γ=3)');
  for (const m of ['fixed', 'rv20', 'ewma94']) {
    const s = exp.strategies[m];
    console.log(`  ${m.padEnd(9)} ${pct(s.annReturn).padStart(7)} ${pct(s.annVol).padStart(7)} ${f(s.sharpe).padStart(6)} ${pct(s.maxDrawdown).padStart(7)} ${pct(s.es5).padStart(7)} ${f(s.meanTurnover, 3).padStart(8)} ${pct(s.ceReturn[3]).padStart(8)}`);
  }
  const diffCI = (a, b) => {
    const d = exp.netByMethod[a].map((v, i) => v - exp.netByMethod[b][i]); // daily net-return diff a-b
    return blockBootstrapMeanCI(d);
  };
  console.log('  daily net-return difference (annualized), 95% block-bootstrap CI:');
  for (const [a, b] of [['rv20', 'fixed'], ['ewma94', 'fixed'], ['rv20', 'ewma94']]) {
    const ci = diffCI(a, b);
    const verdict = ci.lo > 0 ? `${a} BETTER` : ci.hi < 0 ? `${a} WORSE` : 'no difference detected';
    console.log(`    ${a} - ${b}: ${pct(ci.mean * 252, 2)}  CI [${pct(ci.lo * 252, 2)}, ${pct(ci.hi * 252, 2)}]  -> ${verdict}`);
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const symbol = (args.symbol || 'SPY').toUpperCase();
  const range = args.range || '10y';
  const holdoutFrac = args['holdout-frac'] !== undefined ? Number(args['holdout-frac']) : 0.3;
  const financingAnnual = args.financing !== undefined ? Number(args.financing) : 0;

  const bars = mapMarketResponseToBars(await fetchYahoo(symbol, range));
  const returns = logReturns(bars);
  const snapshotHash = createHash('sha256').update(JSON.stringify(bars.map((b) => [b.timestamp, b.close]))).digest('hex');
  const holdoutFrom = Math.floor(returns.length * (1 - holdoutFrac));

  console.log(`data: ${bars.length} adjusted sessions ${bars[0].timestamp.slice(0, 10)} .. ${bars[bars.length - 1].timestamp.slice(0, 10)}`);
  console.log(`data_snapshot_sha256: ${snapshotHash}`);
  console.log('rule: w = clip(0.15 / ann_sigma_hat, 0, 1.5); 2bps turnover cost; 1-day timing; financing=' + financingAnnual);

  reportBlock('FULL SAMPLE', runExperiment(returns, { financingAnnual }));
  reportBlock(`HOLDOUT (last ${Math.round(holdoutFrac * 100)}%)`, runExperiment(returns, { from: holdoutFrom, financingAnnual }));
  console.log('');
}

main().catch((err) => { console.error(`decision experiment failed: ${err.message}`); process.exit(1); });
