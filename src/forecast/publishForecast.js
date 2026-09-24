/**
 * Orchestrate a single immutable forecast publication:
 *   validate + snapshot data (fail closed) -> authoritative calendar -> volatility engine
 *   -> immutable per-horizon records -> append-only ledger.
 *
 * The data source is injected as `bars` so this is deterministic and unit-testable; a thin
 * yahoo_proxy adapter (dataSourceYahoo.js) supplies bars in production.
 */

import { MultiHorizonVolatilityLevels } from '../math/MultiHorizonVolatilityLevels.js';
import { buildDataSnapshot } from './dataSnapshot.js';
import { buildForecastRecords } from './forecastRecord.js';
import { CALENDAR_VERSION, holidaysInRange } from './nyseCalendar.js';
import { appendForecasts } from './ledgerStore.js';

const DEFAULT_LEDGER_PATH = 'data/forecast_ledger/forecasts.jsonl';

/**
 * Latest period end that any requested horizon can reach from asOf: the Friday of asOf's ISO
 * week (weekly) and the last calendar day of asOf's month (monthly). The authoritative holiday
 * range must extend to here — not stop at asOf — or a holiday between asOf and the period end is
 * omitted and the engine over-counts sessionsRemaining, shifting the weekly/monthly bands.
 */
function periodEndBound(asOfKey) {
  const d = new Date(`${asOfKey}T00:00:00.000Z`);
  const dow = d.getUTCDay();
  const daysSinceMonday = dow === 0 ? 6 : dow - 1;
  const friday = new Date(d.getTime());
  friday.setUTCDate(d.getUTCDate() - daysSinceMonday + 4);
  const monthEnd = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0));
  const later = friday.getTime() > monthEnd.getTime() ? friday : monthEnd;
  return later.toISOString().slice(0, 10);
}

/**
 * @param {object} params
 * @param {Array} params.bars                 Daily bars ({timestamp, close, ...}).
 * @param {string} params.symbol              e.g. 'SPY'.
 * @param {string} params.asOf                Target session YYYY-MM-DD.
 * @param {string} params.generatedAt         ISO publication timestamp.
 * @param {string} params.softwareSha         Git SHA the code is running at.
 * @param {string} params.dataSource          e.g. 'yahoo_proxy'.
 * @param {string} [params.priceAdjustment='split_and_dividend_adjusted']
 * @param {string} [params.ingestedAt]        ISO time the data was fetched.
 * @param {number|string} [params.version=1]  Forecast version series.
 * @param {string[]} [params.horizons]        Defaults to the engine's default horizons.
 * @param {number[]} [params.volatilityWindows]
 * @param {string} [params.ledgerPath]        Ledger JSONL path.
 * @param {boolean} [params.persist=true]     When false, assemble records without writing.
 * @returns {{records:object[], dataSnapshotHash:string, priorSession:string, writes:object[]}}
 */
export function publishForecast(params) {
  const {
    bars, symbol, asOf, generatedAt, softwareSha, dataSource,
    priceAdjustment, // #5: no default — the caller/adapter must assert adjustment explicitly
    ingestedAt = null,
    version = 1,
    horizons,
    volatilityWindows,
    ledgerPath = DEFAULT_LEDGER_PATH,
    persist = true,
    mode = 'research',
    nowIso = null,
    publicationCutoff = null,
    allowSupersede = false,
    clockSkewMs = 120000,
  } = params || {};

  // #5: adjustment is mandatory, so nothing is ever silently labeled "adjusted".
  for (const [key, val] of Object.entries({ symbol, asOf, generatedAt, softwareSha, dataSource, priceAdjustment })) {
    if (!val) throw new Error(`publishForecast: ${key} is required`);
  }

  // #1: in production, refuse backdating and post-cutoff publication so a forecast cannot be
  // manufactured for an already-known session and stored as if it were prospective. (Republishing
  // under a bumped version is refused at the ledger layer by (symbol, target_session, horizon)
  // identity regardless of mode.)
  if (mode === 'production') {
    if (!nowIso) throw new Error('publishForecast: production mode requires nowIso (a trusted clock)');
    const nowMs = Date.parse(nowIso);
    if (!Number.isFinite(nowMs)) throw new Error(`publishForecast: nowIso is not a valid timestamp: ${nowIso}`);
    if (asOf < nowIso.slice(0, 10)) {
      throw new Error(`publishForecast: production refuses a past asOf ${asOf} (now ${nowIso.slice(0, 10)}); backdating is not allowed`);
    }
    if (Date.parse(generatedAt) > nowMs + clockSkewMs) {
      throw new Error(`publishForecast: production refuses generatedAt ${generatedAt} in the future vs the trusted clock ${nowIso}`);
    }
    const cutoff = publicationCutoff || `${asOf}T13:30:00.000Z`; // default: US cash-open (~9:30 ET)
    if (Date.parse(generatedAt) >= Date.parse(cutoff)) {
      throw new Error(`publishForecast: production refuses generatedAt ${generatedAt} at/after the publication cutoff ${cutoff}`);
    }
  }

  // Validate + snapshot + hash the exact input data (fail closed on bad data).
  const snapshot = buildDataSnapshot(bars, {
    symbol, asOf, generatedAt, priceAdjustment,
  });

  // Authoritative NYSE holidays from the input span through the furthest period end any
  // horizon reaches -> the engine's calendar, so callers cannot supply (or omit) holidays
  // and silently shift anchors or session counts.
  const fromKey = snapshot.snapshot.data_from.slice(0, 10);
  const holidays = holidaysInRange(fromKey, periodEndBound(asOf));

  // Feed the engine the SAME bounded, validated snapshot bars the hash was taken over, so the
  // snapshot and the levels are computed from an identical point-in-time input set.
  const engineResult = MultiHorizonVolatilityLevels.calculate(snapshot.bars, {
    symbol,
    asOf,
    generatedAt,
    holidays,
    ...(horizons ? { horizons } : {}),
    ...(volatilityWindows ? { volatilityWindows } : {}),
  });

  const records = buildForecastRecords(engineResult, {
    softwareSha,
    dataSource,
    dataSnapshotHash: snapshot.dataSnapshotHash,
    dataThrough: snapshot.dataThrough,
    priceAdjustment,
    calendarVersion: CALENDAR_VERSION,
    generatedAt,
    ingestedAt,
    version,
  });

  // Two-phase commit: all horizons validate against the ledger before any is written, so a
  // conflict on one horizon can never leave the others half-published.
  const writes = persist ? appendForecasts(ledgerPath, records, { allowSupersede }) : [];

  return {
    records,
    dataSnapshotHash: snapshot.dataSnapshotHash,
    priorSession: snapshot.priorSession,
    writes,
  };
}

export { DEFAULT_LEDGER_PATH };
