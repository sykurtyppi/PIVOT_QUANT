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
 * TOLERANCE_PCT absorbs small run-to-run jitter. The suite includes fast-check
 * property tests (tests/properties/*), which explore different random inputs
 * each run and swing branch/line coverage by up to ~0.2pp; the tolerance is set
 * a few times above that. A real regression larger than it fails the build; a
 * genuine improvement is adopted by re-running with --update and committing the
 * new baseline.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const METRICS = ['statements', 'branches', 'functions', 'lines'];
const TOLERANCE_PCT = 0.5;

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
