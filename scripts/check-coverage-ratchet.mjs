#!/usr/bin/env node
/**
 * JS coverage ratchet.
 *
 * Jest's `coverageThreshold.global` (package.json) is a coarse floor set a few
 * points below the real number. This ratchet is the tight gate: it compares the
 * freshly-measured src/** totals against a committed high-water mark
 * (.github/coverage-baseline.json) and FAILS if any metric drops more than
 * TOLERANCE_PCT below it — so coverage cannot silently regress between the
 * coarse floor and the real number.
 *
 * Usage:
 *   node scripts/check-coverage-ratchet.mjs            # check (CI + local)
 *   node scripts/check-coverage-ratchet.mjs --update   # ratchet the baseline up
 *
 * Both modes read coverage/coverage-summary.json, so run `npm test` (which runs
 * jest --coverage, emitting the json-summary reporter) first. `npm run
 * coverage:check` and `npm run coverage:baseline` wrap the two modes.
 *
 * Because the `js` CI job is a REQUIRED check, the gate must never redden on
 * jitter alone. The suite includes fast-check property tests (tests/properties/*)
 * that explore different random inputs each run; branch coverage was observed to
 * swing <=0.17pp over 5 runs (statements/lines <=0.05pp, functions 0). Two
 * defences combine: the committed baseline is the CONSERVATIVE MINIMUM observed
 * over several runs (not a lucky peak), and TOLERANCE_PCT (0.75) sits ~4x above
 * the observed swing. So a spurious failure needs a seed far outside anything
 * seen, while a real regression (adding untested code typically drops coverage
 * well over 0.75pp) still fails. A genuine improvement is adopted by re-running
 * with --update and committing the new baseline.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const METRICS = ['statements', 'branches', 'functions', 'lines'];
const TOLERANCE_PCT = 0.75;

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), '..');
const SUMMARY_PATH = join(repoRoot, 'coverage', 'coverage-summary.json');
const BASELINE_PATH = join(repoRoot, '.github', 'coverage-baseline.json');

function fail(message) {
  console.error(`coverage-ratchet: ${message}`);
  process.exit(1);
}

function readJson(path, what) {
  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch (err) {
    fail(`could not read ${what} at ${path} (${err.code || err.message}).`);
    return undefined; // unreachable; fail() exits
  }
}

function currentPercents() {
  const summary = readJson(
    SUMMARY_PATH,
    'the jest coverage summary — run `npm test` first so jest emits it'
  );
  const total = summary && summary.total;
  if (!total) fail('coverage-summary.json has no "total" block.');
  const out = {};
  for (const m of METRICS) {
    const pct = total[m] && total[m].pct;
    if (typeof pct !== 'number' || Number.isNaN(pct)) {
      fail(`coverage-summary.json total.${m}.pct is missing or not a number.`);
    }
    out[m] = pct;
  }
  return out;
}

const isUpdate = process.argv.includes('--update');
const current = currentPercents();

if (isUpdate) {
  const baseline = {
    _comment:
      'JS coverage high-water mark over src/** (see scripts/check-coverage-ratchet.mjs). Regenerate with `npm run coverage:baseline`; never hand-edit upward past the measured number.',
    tolerance_pct: TOLERANCE_PCT,
    metrics: current,
  };
  writeFileSync(BASELINE_PATH, `${JSON.stringify(baseline, null, 2)}\n`, 'utf8');
  console.log(`coverage-ratchet: baseline written to ${BASELINE_PATH}`);
  for (const m of METRICS) console.log(`  ${m.padEnd(11)} ${current[m].toFixed(2)}%`);
  process.exit(0);
}

const baseline = readJson(
  BASELINE_PATH,
  'the coverage baseline — create it with `npm run coverage:baseline`'
);
const base = baseline && baseline.metrics;
if (!base) fail('coverage-baseline.json has no "metrics" block.');
for (const m of METRICS) {
  if (typeof base[m] !== 'number' || Number.isNaN(base[m])) {
    fail(`coverage-baseline.json metrics.${m} is missing or not a number — regenerate with \`npm run coverage:baseline\`.`);
  }
}
const tolerance = typeof baseline.tolerance_pct === 'number' ? baseline.tolerance_pct : TOLERANCE_PCT;

const regressions = [];
const improvements = [];
console.log(`coverage-ratchet: src/** coverage vs baseline (tolerance ${tolerance}%)`);
for (const m of METRICS) {
  const now = current[m];
  const was = base[m];
  const delta = now - was;
  const floor = was - tolerance;
  const status = now < floor ? 'REGRESSED' : 'ok';
  console.log(
    `  ${m.padEnd(11)} now ${now.toFixed(2)}%  baseline ${was.toFixed(2)}%  delta ${delta >= 0 ? '+' : ''}${delta.toFixed(2)}  ${status}`
  );
  if (now < floor) regressions.push({ m, now, was });
  if (delta > tolerance) improvements.push({ m, now, was });
}

if (regressions.length) {
  console.error('\ncoverage-ratchet: FAIL — coverage regressed below baseline:');
  for (const r of regressions) {
    console.error(`  ${r.m}: ${r.now.toFixed(2)}% < ${r.was.toFixed(2)}% - ${tolerance}% tolerance`);
  }
  console.error('\nAdd tests to recover, or — if the drop is intended (e.g. code removed) —');
  console.error('re-run `npm run coverage:baseline` and commit the new baseline with justification.');
  process.exit(1);
}

if (improvements.length) {
  console.log(
    `\ncoverage-ratchet: coverage improved beyond tolerance on ${improvements
      .map((i) => i.m)
      .join(', ')}. Consider \`npm run coverage:baseline\` to ratchet the baseline up.`
  );
}
console.log('\ncoverage-ratchet: OK');
