import http from 'http';
import https from 'https';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const HOST = process.env.HOST || '127.0.0.1';
const PORT = Number(process.env.PORT || 3000);
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 15000);
const CACHE_MAX_SIZE = Number(process.env.CACHE_MAX_SIZE || 50);
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 5);
const BASE_DELAY_MS = Number(process.env.BASE_DELAY_MS || 800);
const MAX_DELAY_MS = Number(process.env.MAX_DELAY_MS || 8000);
const MAX_BODY_BYTES = Number(process.env.MAX_BODY_BYTES || 1048576); // 1 MB

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '..');
const DASHBOARD_FILE = path.join(ROOT_DIR, 'production_pivot_dashboard.html');
const LOCAL_CHART_PATH = path.join(
  ROOT_DIR,
  'node_modules',
  'lightweight-charts',
  'dist',
  'lightweight-charts.standalone.production.js'
);
const EXPORT_DIR = path.join(ROOT_DIR, 'data', 'exports');
const METRICS_FILE = path.join(EXPORT_DIR, 'rf_walkforward_metrics.json');
const CALIB_FILE = path.join(EXPORT_DIR, 'rf_calibration_curve.json');
const SQLITE_DB = path.join(ROOT_DIR, 'data', 'pivot_events.sqlite');

const symbolMap = new Map([
  ['SPX', '^GSPC'],
  ['SPY', 'SPY'],
]);

const cache = new Map();

/**
 * LRU cache eviction: remove expired entries first, then oldest if over limit.
 */
function evictCache() {
  const now = Date.now();
  // Phase 1: remove expired entries
  for (const [key, entry] of cache.entries()) {
    if (now - entry.timestamp >= CACHE_TTL_MS) {
      cache.delete(key);
    }
  }
  // Phase 2: if still over limit, remove oldest entries (LRU)
  while (cache.size > CACHE_MAX_SIZE) {
    const oldestKey = cache.keys().next().value;
    cache.delete(oldestKey);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function tryParseJson(text, defaultValue = null) {
  if (typeof text !== 'string') {
    return { ok: false, value: defaultValue };
  }
  const trimmed = text.trim();
  if (!trimmed) {
    return { ok: true, value: defaultValue };
  }
  try {
    return { ok: true, value: JSON.parse(trimmed) };
  } catch (_error) {
    return { ok: false, value: defaultValue };
  }
}

function fetchLocalJson(url) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    const req = client.get(url, (res) => {
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        const statusCode = res.statusCode || 0;
        const parsed = tryParseJson(data);
        if (statusCode >= 200 && statusCode < 300) {
          if (parsed.ok) {
            resolve(parsed.value);
          } else {
            reject({
              statusCode,
              message: `Invalid JSON from GET ${url}`,
              body: data,
            });
          }
          return;
        }

        reject({
          statusCode,
          message: `HTTP ${statusCode}`,
          body: parsed.ok ? parsed.value : data,
        });
      });
    });
    req.on('error', (error) => reject({ statusCode: 0, message: error.message, error }));
    req.setTimeout(5000, () => {
      req.destroy(new Error(`GET timeout to ${url}`));
    });
  });
}

function fetchLocalJsonPost(url, payload) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(payload || {});
    const req = http.request(
      url,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(data),
        },
      },
      (res) => {
        let body = '';
        res.on('data', (chunk) => {
          body += chunk;
        });
        res.on('end', () => {
          const statusCode = res.statusCode || 0;
          const parsed = tryParseJson(body, {});
          if (statusCode >= 200 && statusCode < 300) {
            if (parsed.ok) {
              resolve(parsed.value);
            } else {
              reject({
                statusCode,
                message: `Invalid JSON from upstream POST ${url}`,
                body,
              });
            }
            return;
          }

          reject({
            statusCode,
            message: `HTTP ${statusCode}`,
            body: parsed.ok ? parsed.value : body,
          });
        });
      }
    );
    req.on('error', (error) => reject({ statusCode: 0, message: error.message, error }));
    req.setTimeout(5000, () => req.destroy(new Error(`POST timeout to ${url}`)));
    req.write(data);
    req.end();
  });
}

function isRetryableStatus(status) {
  return [429, 500, 502, 503, 504].includes(status);
}

function formatYmd(epochSeconds, timeZone) {
  const date = new Date(epochSeconds * 1000);
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  const parts = Object.fromEntries(formatter.formatToParts(date).map((p) => [p.type, p.value]));
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function mapSymbol(rawSymbol) {
  const symbol = (rawSymbol || 'SPX').toUpperCase().trim();
  return symbolMap.get(symbol) || symbol;
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        headers: {
          'Accept': 'application/json',
          'Accept-Encoding': 'identity',
          'User-Agent': 'PivotQuantDashboard/1.0',
        },
      },
      (res) => {
        let data = '';
        res.on('data', (chunk) => {
          data += chunk;
        });
        res.on('end', () => {
          const status = res.statusCode || 0;
          if (status >= 200 && status < 300) {
            try {
              resolve(JSON.parse(data));
            } catch (error) {
              reject({ statusCode: status, message: 'Invalid JSON', error });
            }
            return;
          }

          if (status >= 300 && status < 400 && res.headers.location) {
            fetchJson(res.headers.location)
              .then(resolve)
              .catch(reject);
            return;
          }

          reject({
            statusCode: status,
            message: `HTTP ${status}`,
            body: data,
          });
        });
      }
    );

    req.setTimeout(12000, () => {
      req.destroy(new Error('Request timeout'));
    });

    req.on('error', (error) => reject({ statusCode: 0, message: error.message, error }));
  });
}

async function fetchWithRetry(url) {
  let attempt = 0;
  let lastError;

  while (attempt <= MAX_RETRIES) {
    attempt += 1;
    try {
      const payload = await fetchJson(url);
      return { payload, attempts: attempt };
    } catch (error) {
      lastError = error;
      const status = error.statusCode || 0;
      if (!isRetryableStatus(status) || attempt > MAX_RETRIES) {
        throw error;
      }

      const delay = Math.min(MAX_DELAY_MS, BASE_DELAY_MS * Math.pow(2, attempt - 1));
      const jitter = delay * (0.7 + Math.random() * 0.6);
      await sleep(jitter);
    }
  }

  throw lastError;
}

function parseYahooPayload(payload, requestedSymbol, yahooSymbol) {
  const result = payload?.chart?.result?.[0];
  if (!result) {
    throw new Error('Invalid Yahoo Finance response payload');
  }

  const meta = result.meta || {};
  const timestamps = result.timestamp || [];
  const quote = result.indicators?.quote?.[0] || {};

  const candles = [];
  for (let i = 0; i < timestamps.length; i += 1) {
    const open = quote.open?.[i];
    const high = quote.high?.[i];
    const low = quote.low?.[i];
    const close = quote.close?.[i];
    const volume = quote.volume?.[i];

    if (
      Number.isFinite(open) &&
      Number.isFinite(high) &&
      Number.isFinite(low) &&
      Number.isFinite(close)
    ) {
      candles.push({
        time: timestamps[i],
        open: Number(open),
        high: Number(high),
        low: Number(low),
        close: Number(close),
        volume: Number.isFinite(volume) ? volume : 0,
      });
    }
  }

  if (candles.length === 0) {
    throw new Error('Yahoo Finance returned no valid candles');
  }

  const marketState = meta.marketState || 'UNKNOWN';
  const timeZone = meta.exchangeTimezoneName || 'America/New_York';
  const now = Math.floor(Date.now() / 1000);
  const regularEnd = meta.currentTradingPeriod?.regular?.end;

  let isLastSessionComplete = marketState === 'CLOSED';
  if (Number.isFinite(regularEnd)) {
    isLastSessionComplete = now >= regularEnd;
  }

  const lastIndex = candles.length - 1;
  let usedIndex = lastIndex;
  const lastDate = formatYmd(candles[lastIndex].time, timeZone);
  const todayDate = formatYmd(now, timeZone);

  if (!isLastSessionComplete && lastIndex > 0 && lastDate === todayDate) {
    usedIndex = lastIndex - 1;
  }

  const usedCandle = candles[usedIndex];

  return {
    symbol: requestedSymbol,
    yahooSymbol,
    currency: meta.currency || 'USD',
    exchangeName: meta.exchangeName || 'UNKNOWN',
    marketState,
    currentPrice: meta.regularMarketPrice || candles[lastIndex].close,
    previousClose: meta.previousClose || candles[Math.max(0, lastIndex - 1)].close,
    candles,
    session: {
      usedIndex,
      usedDate: formatYmd(usedCandle.time, timeZone),
      isLastSessionComplete,
      timeZone,
    },
    meta: {
      regularMarketTime: meta.regularMarketTime,
      regularMarketPrice: meta.regularMarketPrice,
      regularMarketVolume: meta.regularMarketVolume,
    },
  };
}

async function getYahooData({ symbol, range = '3mo', interval = '1d' }) {
  const requestedSymbol = (symbol || 'SPX').toUpperCase().trim();
  const yahooSymbol = mapSymbol(requestedSymbol);
  const cacheKey = `${yahooSymbol}|${range}|${interval}`;
  const cached = cache.get(cacheKey);

  if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
    return { ...cached.data, fetch: { fromCache: true, attempts: 0 } };
  }

  const hosts = ['query1.finance.yahoo.com', 'query2.finance.yahoo.com'];
  const errors = [];
  let payload = null;
  let attempts = 0;

  for (const host of hosts) {
    const url = `https://${host}/v8/finance/chart/${encodeURIComponent(
      yahooSymbol
    )}?range=${range}&interval=${interval}`;

    try {
      const result = await fetchWithRetry(url);
      payload = result.payload;
      attempts += result.attempts;
      break;
    } catch (error) {
      errors.push({ host, message: error.message, status: error.statusCode || 0 });
    }
  }

  if (!payload) {
    const message = errors.length ? JSON.stringify(errors) : 'Unknown Yahoo error';
    throw new Error(`Yahoo Finance failed: ${message}`);
  }

  const parsed = parseYahooPayload(payload, requestedSymbol, yahooSymbol);

  const response = {
    ...parsed,
    dataSource: 'Yahoo Finance',
    fetch: { fromCache: false, attempts },
    asOf: new Date().toISOString(),
  };

  cache.set(cacheKey, { timestamp: Date.now(), data: response });
  evictCache();
  return response;
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'no-store',
  });
  res.end(JSON.stringify(payload, null, 2));
}

function sendProxyError(res, error, fallbackError, fallbackStatus = 502) {
  const upstreamStatus = Number(error?.statusCode || 0);
  const statusCode = upstreamStatus >= 400 && upstreamStatus < 600 ? upstreamStatus : fallbackStatus;

  if (error?.body && typeof error.body === 'object' && !Array.isArray(error.body)) {
    sendJson(res, statusCode, error.body);
    return;
  }

  const payload = {
    error: fallbackError,
    message: error?.message || String(error),
  };
  if (typeof error?.body === 'string' && error.body.trim()) {
    payload.upstreamBody = error.body;
  }
  sendJson(res, statusCode, payload);
}

function sendFile(res, filePath) {
  fs.readFile(filePath, (error, data) => {
    if (error) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found');
      return;
    }
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store',
      Pragma: 'no-cache',
      Expires: '0',
    });
    res.end(data);
  });
}

function sendJs(res, filePath) {
  fs.readFile(filePath, (error, data) => {
    if (error) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found');
      return;
    }
    res.writeHead(200, {
      'Content-Type': 'application/javascript; charset=utf-8',
      'Cache-Control': 'no-store',
      'Access-Control-Allow-Origin': '*',
    });
    res.end(data);
  });
}

/**
 * Read request body with a size limit to prevent memory abuse.
 */
function readBody(req, maxBytes = MAX_BODY_BYTES) {
  return new Promise((resolve, reject) => {
    let data = '';
    let bytes = 0;
    req.on('data', (chunk) => {
      bytes += chunk.length;
      if (bytes > maxBytes) {
        req.destroy(new Error('Request body too large'));
        reject(new Error(`Request body exceeds ${maxBytes} bytes`));
        return;
      }
      data += chunk;
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

function readJsonFile(filePath) {
  if (!fs.existsSync(filePath)) return null;
  const raw = fs.readFileSync(filePath, 'utf8');
  return JSON.parse(raw);
}

function average(values) {
  const clean = values.filter((value) => Number.isFinite(value));
  if (!clean.length) return null;
  return clean.reduce((acc, value) => acc + value, 0) / clean.length;
}

function computeEce(rows) {
  if (!rows?.length) return null;
  let total = 0;
  let weighted = 0;
  rows.forEach((row) => {
    const count = Number(row.count);
    const meanPred = Number(row.mean_pred);
    const fracPos = Number(row.frac_pos);
    if (!Number.isFinite(count) || !Number.isFinite(meanPred) || !Number.isFinite(fracPos)) return;
    total += count;
    weighted += Math.abs(meanPred - fracPos) * count;
  });
  if (!total) return null;
  return weighted / total;
}

function summarizeMlMetrics(metrics, calibRows) {
  if (!Array.isArray(metrics) || metrics.length === 0) {
    return {
      status: 'empty',
      folds: 0,
    };
  }

  const sorted = [...metrics].sort((a, b) => (a.fold || 0) - (b.fold || 0));
  const latest = sorted[sorted.length - 1];
  const window = sorted.slice(-3);
  const seriesWindow = sorted.slice(-10);

  const calibByFold = new Map();
  if (Array.isArray(calibRows)) {
    calibRows.forEach((row) => {
      const fold = row.fold ?? 0;
      if (!calibByFold.has(fold)) {
        calibByFold.set(fold, []);
      }
      calibByFold.get(fold).push(row);
    });
  }

  const latestEce = computeEce(calibByFold.get(latest.fold) || []);
  const rollingEce = average(
    window.map((row) => computeEce(calibByFold.get(row.fold) || []))
  );

  return {
    status: 'ok',
    folds: sorted.length,
    latest: {
      fold: latest.fold,
      test_start: latest.test_start,
      test_end: latest.test_end,
      roc_auc: latest.roc_auc,
      brier: latest.brier,
    },
    rolling: {
      roc_auc: average(window.map((row) => row.roc_auc)),
      brier: average(window.map((row) => row.brier)),
    },
    calibration: {
      latest_ece: latestEce,
      rolling_ece: rollingEce,
    },
    series: {
      roc_auc: seriesWindow
        .map((row) => ({ fold: row.fold, value: row.roc_auc }))
        .filter((row) => Number.isFinite(row.value)),
      brier: seriesWindow
        .map((row) => ({ fold: row.fold, value: row.brier }))
        .filter((row) => Number.isFinite(row.value)),
      ece: seriesWindow
        .map((row) => ({
          fold: row.fold,
          value: computeEce(calibByFold.get(row.fold) || []),
        }))
        .filter((row) => Number.isFinite(row.value)),
    },
    updated_at: new Date().toISOString(),
  };
}

/**
 * Query level statistics from SQLite, including Week 2 features:
 * VPOC, multi-TF confluence, level aging, and historical accuracy.
 */
async function queryLevelStats(symbol, limit) {
  // Use dynamic import for better-sqlite3 (optional dependency)
  let Database;
  try {
    const mod = await import('better-sqlite3');
    Database = mod.default;
  } catch (_err) {
    // Fallback: return data from the latest export files
    return queryLevelStatsFromExports(symbol, limit);
  }

  if (!fs.existsSync(SQLITE_DB)) {
    return { error: 'Database not found', levels: [] };
  }

  const db = new Database(SQLITE_DB, { readonly: true });

  try {
    const rows = db.prepare(`
      SELECT
        te.level_type,
        te.level_price,
        te.touch_price,
        te.distance_bps,
        te.confluence_count,
        te.ema_state,
        te.vwap_dist_bps,
        te.atr,
        te.vpoc,
        te.vpoc_dist_bps,
        te.volume_at_level,
        te.mtf_confluence,
        te.mtf_confluence_types,
        te.weekly_pivot,
        te.monthly_pivot,
        te.level_age_days,
        te.hist_reject_rate,
        te.hist_break_rate,
        te.hist_sample_size,
        te.ts_event,
        el5.reject AS reject_5m,
        el5.break AS break_5m,
        el15.reject AS reject_15m,
        el15.break AS break_15m,
        el60.reject AS reject_60m,
        el60.break AS break_60m
      FROM touch_events te
      LEFT JOIN event_labels el5
        ON te.event_id = el5.event_id AND el5.horizon_min = 5
      LEFT JOIN event_labels el15
        ON te.event_id = el15.event_id AND el15.horizon_min = 15
      LEFT JOIN event_labels el60
        ON te.event_id = el60.event_id AND el60.horizon_min = 60
      WHERE te.symbol = ?
      ORDER BY te.ts_event DESC
      LIMIT ?
    `).all(symbol, limit);

    // Aggregate by level_type for summary stats
    const byType = {};
    for (const row of rows) {
      const lt = row.level_type;
      if (!byType[lt]) {
        byType[lt] = {
          level_type: lt,
          events: [],
          avg_vpoc_dist_bps: null,
          avg_mtf_confluence: 0,
          avg_level_age: 0,
          avg_hist_reject_rate: null,
          avg_hist_break_rate: null,
          total_volume_at_level: 0,
        };
      }
      byType[lt].events.push(row);
    }

    const summary = Object.values(byType).map((group) => {
      const events = group.events;
      const n = events.length;
      const vpocDists = events.map((e) => e.vpoc_dist_bps).filter(Number.isFinite);
      const mtfConfs = events.map((e) => e.mtf_confluence || 0);
      const ages = events.map((e) => e.level_age_days || 0);
      const rejectRates = events.map((e) => e.hist_reject_rate).filter(Number.isFinite);
      const breakRates = events.map((e) => e.hist_break_rate).filter(Number.isFinite);
      const volumes = events.map((e) => e.volume_at_level || 0);

      return {
        level_type: group.level_type,
        event_count: n,
        latest_price: events[0]?.level_price,
        avg_vpoc_dist_bps: vpocDists.length
          ? vpocDists.reduce((a, b) => a + b, 0) / vpocDists.length
          : null,
        avg_mtf_confluence: mtfConfs.reduce((a, b) => a + b, 0) / n,
        avg_level_age: ages.reduce((a, b) => a + b, 0) / n,
        avg_hist_reject_rate: rejectRates.length
          ? rejectRates.reduce((a, b) => a + b, 0) / rejectRates.length
          : null,
        avg_hist_break_rate: breakRates.length
          ? breakRates.reduce((a, b) => a + b, 0) / breakRates.length
          : null,
        total_volume_at_level: volumes.reduce((a, b) => a + b, 0),
        reject_rate_5m: average(events.map((e) => e.reject_5m).filter((v) => v !== null)),
        break_rate_5m: average(events.map((e) => e.break_5m).filter((v) => v !== null)),
        reject_rate_15m: average(events.map((e) => e.reject_15m).filter((v) => v !== null)),
        break_rate_15m: average(events.map((e) => e.break_15m).filter((v) => v !== null)),
        reject_rate_60m: average(events.map((e) => e.reject_60m).filter((v) => v !== null)),
        break_rate_60m: average(events.map((e) => e.break_60m).filter((v) => v !== null)),
      };
    });

    return {
      symbol,
      updated_at: new Date().toISOString(),
      total_events: rows.length,
      level_summary: summary,
      recent_events: rows.slice(0, 20),
    };
  } finally {
    db.close();
  }
}

function queryLevelStatsFromExports(symbol, _limit) {
  // Fallback when better-sqlite3 isn't available: read from CSV exports
  const eventsPath = path.join(EXPORT_DIR, 'touch_events.csv');
  if (!fs.existsSync(eventsPath)) {
    return { symbol, error: 'No export data available', level_summary: [] };
  }
  return {
    symbol,
    note: 'Install better-sqlite3 for live DB queries. Showing static export data.',
    level_summary: [],
  };
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host}`);

  if (url.pathname === '/api/market') {
    try {
      const source = (url.searchParams.get('source') || 'yahoo').toLowerCase();
      const symbol = url.searchParams.get('symbol');
      const range = url.searchParams.get('range') || '3mo';
      const interval = url.searchParams.get('interval') || '1d';

      if (source === 'ibkr') {
        const marketUrl = `http://127.0.0.1:5001/market?symbol=${encodeURIComponent(
          symbol || 'SPX'
        )}&range=${encodeURIComponent(range)}&interval=${encodeURIComponent(interval)}`;
        const data = await fetchLocalJson(marketUrl);
        sendJson(res, 200, data);
      } else {
        const data = await getYahooData({
          symbol,
          range,
          interval,
        });
        sendJson(res, 200, data);
      }
    } catch (error) {
      sendJson(res, 500, {
        error: 'Data fetch failed',
        message: error?.message || String(error),
      });
    }
    return;
  }

  if (url.pathname === '/api/gamma') {
    try {
      const symbol = url.searchParams.get('symbol') || 'SPX';
      const expiry = url.searchParams.get('expiry') || 'front';
      const limit = url.searchParams.get('limit') || '60';
      const gammaUrl = `http://127.0.0.1:5001/gamma?symbol=${encodeURIComponent(
        symbol
      )}&expiry=${encodeURIComponent(expiry)}&limit=${encodeURIComponent(limit)}`;

      const data = await fetchLocalJson(gammaUrl);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Gamma bridge unavailable');
    }
    return;
  }

  if (url.pathname === '/api/ib/market') {
    try {
      const symbol = url.searchParams.get('symbol') || 'SPX';
      const interval = url.searchParams.get('interval') || '1d';
      const range = url.searchParams.get('range') || '3mo';
      const ibUrl = `http://127.0.0.1:5001/market?symbol=${encodeURIComponent(
        symbol
      )}&interval=${encodeURIComponent(interval)}&range=${encodeURIComponent(range)}`;

      const data = await fetchLocalJson(ibUrl);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'IBKR market bridge unavailable');
    }
    return;
  }

  if (url.pathname === '/api/ib/spot') {
    try {
      const symbol = url.searchParams.get('symbol') || 'SPX';
      const ibUrl = `http://127.0.0.1:5001/spot?symbol=${encodeURIComponent(symbol)}`;
      const data = await fetchLocalJson(ibUrl);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'IBKR spot bridge unavailable');
    }
    return;
  }

  if (url.pathname === '/api/ml/metrics') {
    try {
      const metrics = readJsonFile(METRICS_FILE);
      const calib = readJsonFile(CALIB_FILE);
      const summary = summarizeMlMetrics(metrics, calib);
      if (summary.status === 'empty') {
        sendJson(res, 404, {
          error: 'ML metrics unavailable',
          message: 'Run the training script to generate metrics.',
        });
        return;
      }
      sendJson(res, 200, summary);
    } catch (error) {
      sendJson(res, 500, {
        error: 'ML metrics failed',
        message: error?.message || String(error),
      });
    }
    return;
  }

  if (url.pathname === '/api/ml/health') {
    try {
      const data = await fetchLocalJson('http://127.0.0.1:5003/health');
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'ML health unavailable');
    }
    return;
  }

  if (url.pathname === '/api/live/health') {
    try {
      const data = await fetchLocalJson('http://127.0.0.1:5004/health');
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Live collector health unavailable');
    }
    return;
  }

  if (url.pathname === '/api/ml/reload') {
    if (req.method !== 'POST') {
      sendJson(res, 405, { error: 'Method not allowed' });
      return;
    }
    try {
      const data = await fetchLocalJsonPost('http://127.0.0.1:5003/reload', {});
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'ML reload unavailable');
    }
    return;
  }

  if (url.pathname === '/api/ml/score') {
    if (req.method !== 'POST') {
      sendJson(res, 405, { error: 'Method not allowed' });
      return;
    }
    try {
      const body = await readBody(req);
      const payload = body ? JSON.parse(body) : {};
      const data = await fetchLocalJsonPost('http://127.0.0.1:5003/score', payload);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'ML score unavailable');
    }
    return;
  }

  if (url.pathname === '/api/events') {
    try {
      const body = await readBody(req);
      const payload = body ? JSON.parse(body) : {};
      const writerUrl = 'http://127.0.0.1:5002/events';
      const data = await fetchLocalJsonPost(writerUrl, payload);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Event writer unavailable');
    }
    return;
  }

  if (url.pathname === '/api/bars') {
    try {
      const body = await readBody(req);
      const payload = body ? JSON.parse(body) : {};
      const writerUrl = 'http://127.0.0.1:5002/bars';
      const data = await fetchLocalJsonPost(writerUrl, payload);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Bar writer unavailable');
    }
    return;
  }

  if (url.pathname === '/api/daily-candles') {
    try {
      const symbol = url.searchParams.get('symbol') || 'SPY';
      const limit = Math.min(Number(url.searchParams.get('limit') || 200), 500);
      const writerUrl = `http://127.0.0.1:5002/daily-candles?symbol=${encodeURIComponent(
        symbol
      )}&limit=${limit}`;
      const data = await fetchLocalJson(writerUrl);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Daily candle aggregation unavailable');
    }
    return;
  }

  if (url.pathname === '/api/levels') {
    try {
      const symbol = url.searchParams.get('symbol') || 'SPX';
      const limit = Math.min(Number(url.searchParams.get('limit') || 50), 200);
      const levelStats = await queryLevelStats(symbol, limit);
      sendJson(res, 200, levelStats);
    } catch (error) {
      sendJson(res, 500, {
        error: 'Level stats query failed',
        message: error?.message || String(error),
      });
    }
    return;
  }

  if (url.pathname === '/static/lightweight-charts.js') {
    if (fs.existsSync(LOCAL_CHART_PATH)) {
      sendJs(res, LOCAL_CHART_PATH);
    } else {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('lightweight-charts not installed');
    }
    return;
  }

  if (url.pathname === '/' || url.pathname === '/production_pivot_dashboard.html') {
    sendFile(res, DASHBOARD_FILE);
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

server.listen(PORT, HOST, () => {
  /* eslint-disable-next-line no-console */
  console.log(`Pivot dashboard server running at http://${HOST}:${PORT}`);
});

// --- Graceful shutdown & crash guards ---

process.on('uncaughtException', (err) => {
  console.error('[FATAL] Uncaught exception:', err);
  server.close(() => process.exit(1));
  setTimeout(() => process.exit(1), 3000);
});

process.on('unhandledRejection', (reason) => {
  console.error('[WARN] Unhandled rejection:', reason);
});

function shutdown(signal) {
  console.log(`\n[${signal}] Shutting down gracefully...`);
  server.close(() => {
    console.log('Server closed.');
    process.exit(0);
  });
  setTimeout(() => {
    console.error('Forced shutdown after timeout.');
    process.exit(1);
  }, 5000);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
