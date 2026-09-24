// Worker for the multiprocess concurrency stress test. Builds a valid forecast record, scores
// it against a caller-supplied realized close, and appends the outcome to a shared scoreboard.
// Exit 0 = written or idempotent; 2 = refused (immutability conflict); 3 = unexpected error.
import { computeContentHash } from '../../../src/forecast/forecastRecord.js';
import { appendOutcome, scoreForecast, ScoreboardImmutabilityError } from '../../../src/forecast/scoreForecasts.js';

const [, , scoreboardPath, closeStr] = process.argv;

const record = {
  forecast_id: 'SPY-2026-09-24-daily-v1',
  version: 'v1',
  symbol: 'SPY',
  target_session: '2026-09-24',
  horizon: 'daily',
  outcome_forecast: 'close_containment',
  software_sha: '31c4fdb',
  data_snapshot_hash: 'a'.repeat(64),
  calendar_version: 'nyse-rulegen-1.0.0',
  estimator: '20_session_close_to_close_rv',
  anchor: 666,
  horizon_sigma_log_return: 0.01,
  levels: { lower_2: 653, lower_1: 659, upper_1: 673, upper_2: 679 },
  quality: { status: 'complete', reason: null },
};
record.content_hash = computeContentHash(record);

try {
  const outcome = scoreForecast(record, Number(closeStr), { realizedSession: '2026-09-24' });
  appendOutcome(scoreboardPath, outcome);
  process.exit(0);
} catch (err) {
  process.exit(err instanceof ScoreboardImmutabilityError ? 2 : 3);
}
