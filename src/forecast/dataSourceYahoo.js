/**
 * Daily-bar data source backed by the repo's yahoo_proxy (server/yahoo_proxy.js).
 *
 * Decision: the ledger uses Yahoo's SPLIT + DIVIDEND ADJUSTED close (`adjclose`), because
 * close-to-close log returns must not be polluted by the ~0.3-0.4% ex-dividend gaps that the
 * raw close carries. The proxy now passes `adjclose` through; this adapter requires it and
 * FAILS CLOSED if it is missing, rather than silently falling back to the raw (unadjusted)
 * close. Yahoo re-adjusts history retroactively, so this series is point-in-time only once the
 * ledger snapshots and hashes it at publish time — which is exactly what publishForecast does.
 *
 * `mapMarketResponseToBars` is a pure function (unit-tested); `fetchDailyBars` is the thin,
 * network-facing wrapper around the proxy.
 */

export const YAHOO_PRICE_ADJUSTMENT = 'split_and_dividend_adjusted';

/**
 * Keep only bars for sessions strictly before the current UTC date, dropping the current
 * (possibly still-open, partial) session's bar. Yahoo returns a live partial bar intraday; using
 * it makes results non-reproducible and scores an unfinished close. Deterministic given `nowMs`.
 */
export function completedSessionsOnly(bars, { nowMs = Date.now() } = {}) {
  const todayUtc = new Date(nowMs).toISOString().slice(0, 10);
  return bars.filter((b) => b.timestamp.slice(0, 10) < todayUtc);
}

/**
 * Map a yahoo_proxy /api/market response into ledger bars ({timestamp, close}) using the
 * adjusted close. Deterministic and side-effect free.
 *
 * @param {object} response         Parsed proxy JSON: { candles: [{time, close, adjclose, ...}] }.
 * @param {object} [opts]
 * @param {boolean} [opts.requireAdjusted=true] Throw if any candle lacks an adjusted close.
 * @returns {Array<{timestamp:string, close:number}>} sorted ascending by time.
 */
export function mapMarketResponseToBars(response, opts = {}) {
  const requireAdjusted = opts.requireAdjusted !== false;
  const candles = response && Array.isArray(response.candles) ? response.candles : null;
  if (!candles || candles.length === 0) {
    throw new Error('yahoo_proxy response has no candles');
  }

  const bars = candles.map((candle, index) => {
    if (candle == null || typeof candle !== 'object') {
      throw new Error(`candle[${index}] is not an object`);
    }
    const timeSec = candle.time;
    if (!Number.isFinite(timeSec)) {
      throw new Error(`candle[${index}].time is not a unix timestamp`);
    }
    const adjusted = candle.adjclose;
    if (requireAdjusted && !(typeof adjusted === 'number' && Number.isFinite(adjusted) && adjusted > 0)) {
      // Fail closed: the ledger requires adjusted closes; a raw-only feed is refused upstream.
      throw new Error(`candle[${index}] (time=${timeSec}) has no usable adjusted close (adjclose)`);
    }
    const close = requireAdjusted ? adjusted : candle.close;
    if (!(typeof close === 'number' && Number.isFinite(close) && close > 0)) {
      throw new Error(`candle[${index}] has no usable close`);
    }
    return { timestamp: new Date(timeSec * 1000).toISOString(), close };
  });

  bars.sort((a, b) => (a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0));
  return bars;
}

/**
 * Fetch daily adjusted SPY (or other symbol) bars from a running yahoo_proxy.
 *
 * @param {object} params
 * @param {string} [params.proxyBaseUrl='http://127.0.0.1:3000']
 * @param {string} [params.symbol='SPY']
 * @param {string} [params.range='10y']
 * @param {Record<string,string>} [params.headers]  e.g. an auth cookie/header the proxy needs.
 * @param {typeof fetch} [params.fetchImpl=globalThis.fetch]  Injectable for testing.
 * @returns {Promise<{bars:Array, ingestedAt:string, source:string, dataSourceLabel:string}>}
 */
export async function fetchDailyBars(params = {}) {
  const {
    proxyBaseUrl = 'http://127.0.0.1:3000',
    symbol = 'SPY',
    range = '10y',
    headers = {},
    fetchImpl = globalThis.fetch,
  } = params;
  if (typeof fetchImpl !== 'function') {
    throw new Error('fetchDailyBars requires a fetch implementation (Node >=18 or fetchImpl)');
  }

  const url = `${proxyBaseUrl.replace(/\/$/, '')}/api/market` +
    `?source=yahoo&symbol=${encodeURIComponent(symbol)}&range=${encodeURIComponent(range)}&interval=1d`;
  const ingestedAt = new Date().toISOString();
  const res = await fetchImpl(url, { headers });
  if (!res.ok) {
    throw new Error(`yahoo_proxy /api/market returned HTTP ${res.status}`);
  }
  const response = await res.json();
  const bars = mapMarketResponseToBars(response);
  return { bars, ingestedAt, source: 'yahoo_proxy', dataSourceLabel: 'yahoo_proxy' };
}
