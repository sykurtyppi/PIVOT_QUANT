import http from 'http';
import https from 'https';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const HOST = process.env.HOST || '127.0.0.1';
const PORT = Number(process.env.PORT || 3000);
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 15000);
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 5);
const BASE_DELAY_MS = Number(process.env.BASE_DELAY_MS || 800);
const MAX_DELAY_MS = Number(process.env.MAX_DELAY_MS || 8000);

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

const symbolMap = new Map([
  ['SPX', '^GSPC'],
  ['SPY', 'SPY'],
]);

const cache = new Map();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
        if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
          try {
            resolve(JSON.parse(data));
          } catch (error) {
            reject(new Error('Invalid JSON from gamma bridge'));
          }
        } else {
          reject(new Error(`Gamma bridge HTTP ${res.statusCode || 0}`));
        }
      });
    });
    req.on('error', (error) => reject(error));
    req.setTimeout(5000, () => {
      req.destroy(new Error('Gamma bridge timeout'));
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
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
            try {
              resolve(JSON.parse(body));
            } catch (error) {
              reject(new Error('Invalid JSON from writer'));
            }
          } else {
            reject(new Error(`Writer HTTP ${res.statusCode || 0}`));
          }
        });
      }
    );
    req.on('error', reject);
    req.setTimeout(5000, () => req.destroy(new Error('Writer timeout')));
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

function sendFile(res, filePath) {
  fs.readFile(filePath, (error, data) => {
    if (error) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
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
      sendJson(res, 502, {
        error: 'Gamma bridge unavailable',
        message: error?.message || String(error),
      });
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
      sendJson(res, 502, {
        error: 'IBKR market bridge unavailable',
        message: error?.message || String(error),
      });
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
      sendJson(res, 502, {
        error: 'IBKR spot bridge unavailable',
        message: error?.message || String(error),
      });
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

  if (url.pathname === '/api/events') {
    try {
      const body = await new Promise((resolve, reject) => {
        let data = '';
        req.on('data', (chunk) => {
          data += chunk;
        });
        req.on('end', () => resolve(data));
        req.on('error', reject);
      });
      const payload = body ? JSON.parse(body) : {};
      const writerUrl = 'http://127.0.0.1:5002/events';
      const data = await fetchLocalJsonPost(writerUrl, payload);
      sendJson(res, 200, data);
    } catch (error) {
      sendJson(res, 502, {
        error: 'Event writer unavailable',
        message: error?.message || String(error),
      });
    }
    return;
  }

  if (url.pathname === '/api/bars') {
    try {
      const body = await new Promise((resolve, reject) => {
        let data = '';
        req.on('data', (chunk) => {
          data += chunk;
        });
        req.on('end', () => resolve(data));
        req.on('error', reject);
      });
      const payload = body ? JSON.parse(body) : {};
      const writerUrl = 'http://127.0.0.1:5002/bars';
      const data = await fetchLocalJsonPost(writerUrl, payload);
      sendJson(res, 200, data);
    } catch (error) {
      sendJson(res, 502, {
        error: 'Bar writer unavailable',
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
