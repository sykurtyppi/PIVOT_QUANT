import {
  completedSessionsOnly, mapMarketResponseToBars, fetchDailyBars, YAHOO_PRICE_ADJUSTMENT,
} from '../../src/forecast/dataSourceYahoo.js';

// Unix seconds for a couple of session dates.
const t = (iso) => Math.floor(Date.parse(iso) / 1000);

describe('completedSessionsOnly', () => {
  const bars = [
    { timestamp: '2026-09-23T13:30:00.000Z', close: 662 },
    { timestamp: '2026-09-24T13:30:00.000Z', close: 664 },
    { timestamp: '2026-09-25T13:30:00.000Z', close: 999 }, // current (partial) session
  ];
  test('drops the current UTC-date bar (the possibly-partial session)', () => {
    const nowMs = Date.parse('2026-09-25T17:44:00.000Z'); // mid-session
    const kept = completedSessionsOnly(bars, { nowMs });
    expect(kept.map((b) => b.timestamp.slice(0, 10))).toEqual(['2026-09-23', '2026-09-24']);
  });
  test('keeps a bar once its date is strictly before the current UTC date', () => {
    const nowMs = Date.parse('2026-09-26T00:00:00.000Z'); // next UTC day
    const kept = completedSessionsOnly(bars, { nowMs });
    expect(kept).toHaveLength(3); // 09-25 now complete
  });
});

describe('mapMarketResponseToBars', () => {
  test('uses the adjusted close and sorts ascending by time', () => {
    const response = {
      candles: [
        { time: t('2026-09-23T13:30:00Z'), close: 670, adjclose: 666, volume: 1 },
        { time: t('2026-09-21T13:30:00Z'), close: 665, adjclose: 662, volume: 1 },
      ],
    };
    const bars = mapMarketResponseToBars(response);
    expect(bars.map((b) => b.close)).toEqual([662, 666]); // adjusted, sorted by time
    expect(bars[0].timestamp < bars[1].timestamp).toBe(true);
    expect(bars[1].timestamp).toBe(new Date(t('2026-09-23T13:30:00Z') * 1000).toISOString());
  });

  test('fails closed when a candle lacks an adjusted close', () => {
    const response = { candles: [{ time: t('2026-09-23T13:30:00Z'), close: 670, adjclose: null }] };
    expect(() => mapMarketResponseToBars(response)).toThrow(/adjusted close/);
  });

  test('can fall back to raw close only when explicitly allowed', () => {
    const response = { candles: [{ time: t('2026-09-23T13:30:00Z'), close: 670, adjclose: null }] };
    const bars = mapMarketResponseToBars(response, { requireAdjusted: false });
    expect(bars[0].close).toBe(670);
  });

  test('rejects an empty or malformed response', () => {
    expect(() => mapMarketResponseToBars({ candles: [] })).toThrow(/no candles/);
    expect(() => mapMarketResponseToBars({})).toThrow(/no candles/);
  });

  test('exposes the adjusted-price label the ledger records', () => {
    expect(YAHOO_PRICE_ADJUSTMENT).toBe('split_and_dividend_adjusted');
  });
});

describe('fetchDailyBars', () => {
  test('calls the proxy /api/market and maps adjusted bars', async () => {
    let calledUrl = null;
    const fakeFetch = async (url) => {
      calledUrl = url;
      return {
        ok: true,
        json: async () => ({
          candles: [
            { time: t('2026-09-22T13:30:00Z'), close: 668, adjclose: 664 },
            { time: t('2026-09-23T13:30:00Z'), close: 670, adjclose: 666 },
          ],
        }),
      };
    };
    const { bars, source } = await fetchDailyBars({ proxyBaseUrl: 'http://127.0.0.1:3000', symbol: 'SPY', fetchImpl: fakeFetch });
    expect(calledUrl).toContain('/api/market?source=yahoo&symbol=SPY&range=10y&interval=1d');
    expect(bars.map((b) => b.close)).toEqual([664, 666]);
    expect(source).toBe('yahoo_proxy');
  });

  test('throws on a non-OK proxy response', async () => {
    const fakeFetch = async () => ({ ok: false, status: 502 });
    await expect(fetchDailyBars({ fetchImpl: fakeFetch })).rejects.toThrow(/HTTP 502/);
  });
});
