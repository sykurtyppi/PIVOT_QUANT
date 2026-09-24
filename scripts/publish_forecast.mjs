#!/usr/bin/env node
/**
 * Publish today's immutable point-in-time volatility-levels forecast to the ledger.
 *
 * Fetches split/dividend-adjusted daily bars from the running yahoo_proxy, pins the exact
 * git SHA, and writes an append-only, hashed forecast record per horizon. Fails closed on any
 * data-quality problem (missing prior session, stale feed, unadjusted data, ...).
 *
 * Usage:
 *   node scripts/publish_forecast.mjs [--symbol SPY] [--as-of YYYY-MM-DD] [--range 10y]
 *       [--proxy http://127.0.0.1:3000] [--ledger data/forecast_ledger/forecasts.jsonl]
 *       [--version 1] [--auth-header 'Cookie: ...'] [--dry-run]
 *
 *   --dry-run assembles and prints the records without writing them.
 */

import { execSync } from 'child_process';

import { fetchDailyBars, YAHOO_PRICE_ADJUSTMENT } from '../src/forecast/dataSourceYahoo.js';
import { isTradingSession, nextTradingSession } from '../src/forecast/nyseCalendar.js';
import { parseCliArgs } from '../src/forecast/parseCliArgs.js';
import { publishForecast, DEFAULT_LEDGER_PATH } from '../src/forecast/publishForecast.js';

const BOOLEAN_FLAGS = ['dry-run', 'production', 'allow-dirty'];
const VALUE_FLAGS = ['symbol', 'as-of', 'range', 'proxy', 'ledger', 'version', 'auth-header'];

function parseArgs(argv) {
  return parseCliArgs(argv, { booleanFlags: BOOLEAN_FLAGS, valueFlags: VALUE_FLAGS });
}

function resolveSoftwareSha({ allowDirty = false } = {}) {
  let sha;
  try {
    sha = execSync('git rev-parse HEAD', { encoding: 'utf8' }).trim();
  } catch {
    throw new Error('could not resolve git SHA; run inside the repository');
  }
  // #8: a clean SHA must not label a modified working tree. Refuse a dirty tree in production.
  const dirty = execSync('git status --porcelain', { encoding: 'utf8' }).trim();
  if (dirty && !allowDirty) {
    throw new Error('working tree is dirty; refusing to publish a clean SHA over local modifications (use --allow-dirty for research only)');
  }
  return { sha, dirty: Boolean(dirty) };
}

function todayUtcDateKey() {
  return new Date().toISOString().slice(0, 10);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const symbol = (args.symbol || 'SPY').toUpperCase();
  const range = args.range || '10y';
  const proxyBaseUrl = args.proxy || process.env.YAHOO_PROXY_URL || 'http://127.0.0.1:3000';
  const ledgerPath = args.ledger || DEFAULT_LEDGER_PATH;
  const version = args.version || 1;

  // Target session: explicit --as-of, else today if it is a session, else the next session.
  let asOf = args['as-of'];
  if (!asOf) {
    const today = todayUtcDateKey();
    asOf = isTradingSession(today) ? today : nextTradingSession(today);
  }

  // --auth-header "Name: value" (e.g. "Cookie: dash_auth=..."), split on the first colon.
  const headers = {};
  if (args['auth-header']) {
    const idx = args['auth-header'].indexOf(':');
    if (idx > 0) headers[args['auth-header'].slice(0, idx).trim()] = args['auth-header'].slice(idx + 1).trim();
  }

  const production = Boolean(args.production);
  const { sha: softwareSha } = resolveSoftwareSha({ allowDirty: Boolean(args['allow-dirty']) && !production });
  const generatedAt = new Date().toISOString();

  console.error(`[publish] symbol=${symbol} asOf=${asOf} sha=${softwareSha.slice(0, 12)} mode=${production ? 'production' : 'research'} dryRun=${!!args['dry-run']}`);

  const { bars, ingestedAt } = await fetchDailyBars({ proxyBaseUrl, symbol, range, headers });
  console.error(`[publish] fetched ${bars.length} adjusted daily bars (ingestedAt=${ingestedAt})`);

  const result = publishForecast({
    bars,
    symbol,
    asOf,
    generatedAt,
    softwareSha,
    dataSource: 'yahoo_proxy',
    priceAdjustment: YAHOO_PRICE_ADJUSTMENT,
    ingestedAt,
    version,
    ledgerPath,
    persist: !args['dry-run'],
    mode: production ? 'production' : 'research',
    // In production the library stamps the publication time from its own trusted clock and
    // overrides the generatedAt above, so a stale caller timestamp cannot be backdated in.
  });

  for (const record of result.records) {
    const w = args['dry-run'] ? 'dry-run' : (result.writes.find((x) => x.record.forecast_id === record.forecast_id)?.idempotent ? 'idempotent' : 'written');
    const bands = record.levels
      ? `[${record.levels.lower_2}, ${record.levels.lower_1}, ${record.levels.upper_1}, ${record.levels.upper_2}]`
      : `unavailable(${record.quality.reason})`;
    console.log(`${record.forecast_id}  ${record.quality.status}  anchor=${record.anchor}  ${bands}  ${w}`);
  }
  console.error(`[publish] data_snapshot_hash=${result.dataSnapshotHash} priorSession=${result.priorSession}`);
}

main().catch((err) => {
  console.error(`[publish] FAILED (${err.code || 'error'}): ${err.message}`);
  process.exit(1);
});
