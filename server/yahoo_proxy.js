import http from 'http';
import https from 'https';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import { buildConversionSnapshot, convertLevels, normalizeInstrument } from './level_converter.js';

const HOST = process.env.HOST || '127.0.0.1';
const PORT = Number(process.env.PORT || 3000);
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 15000);
const CACHE_MAX_SIZE = Number(process.env.CACHE_MAX_SIZE || 50);
const LEVEL_CONVERTER_SNAPSHOT_TTL_MS = Number(
  process.env.LEVEL_CONVERTER_SNAPSHOT_TTL_MS || 60000
);
const LEVEL_CONVERTER_RESULT_TTL_MS = Number(
  process.env.LEVEL_CONVERTER_RESULT_TTL_MS || 30000
);
const LEVEL_CONVERTER_CACHE_MAX_SIZE = Number(
  process.env.LEVEL_CONVERTER_CACHE_MAX_SIZE || 120
);
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
const ENV_FILE = path.join(ROOT_DIR, '.env');
const BACKUP_STATE_FILE = path.join(ROOT_DIR, 'logs', 'backup_state.json');
const HOST_HEALTH_STATE_FILE = path.join(ROOT_DIR, 'logs', 'host_health_state.json');
const HEALTH_ALERT_STATE_FILE = path.join(ROOT_DIR, 'logs', 'health_alert_state.json');
const REPORT_DELIVERY_STATE_FILE = path.join(ROOT_DIR, 'logs', 'report_delivery_state.json');
const REPORT_DELIVERY_LOG_FILE = path.join(ROOT_DIR, 'logs', 'report_delivery.log');
const HEALTH_ALERT_LOG_FILE = path.join(ROOT_DIR, 'logs', 'health_alert.log');
const RESTORE_DRILL_LOG_FILE = path.join(ROOT_DIR, 'logs', 'restore_drill.log');

const symbolMap = new Map([
  ['SPX', '^GSPC'],
  ['SPY', 'SPY'],
  ['US500', '^GSPC'],
  ['US 500', '^GSPC'],
  ['S&P500', '^GSPC'],
  ['S&P 500', '^GSPC'],
  ['SP500', '^GSPC'],
  ['ES', 'ES=F'],
]);
const YAHOO_HOSTS = ['query1.finance.yahoo.com', 'query2.finance.yahoo.com'];

const cache = new Map();
const levelConversionSnapshotCache = new Map();
const levelConversionResultCache = new Map();
const WRITE_ENDPOINTS = new Set(['/api/ml/reload', '/api/ml/score', '/api/events', '/api/bars']);
const RESPONSE_SECURITY_HEADERS = Object.freeze({
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'Referrer-Policy': 'no-referrer',
  'Permissions-Policy': 'camera=(), microphone=(), geolocation=()',
});
const FORM_URLENCODED = 'application/x-www-form-urlencoded';
const ENV_FILE_VALUES = loadEnvMap(ENV_FILE);
const SECURITY = buildSecurityConfig(process.env, ENV_FILE_VALUES);

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

function readTimedCache(map, key, ttlMs) {
  const entry = map.get(key);
  if (!entry) return null;
  const ageMs = Date.now() - entry.timestamp;
  if (ageMs >= ttlMs) {
    map.delete(key);
    return null;
  }
  return { data: entry.data, ageMs };
}

function writeTimedCache(map, key, data, ttlMs, maxSize) {
  map.set(key, { timestamp: Date.now(), data });
  const now = Date.now();
  for (const [entryKey, entryValue] of map.entries()) {
    if (now - entryValue.timestamp >= ttlMs) {
      map.delete(entryKey);
    }
  }
  while (map.size > maxSize) {
    const oldestKey = map.keys().next().value;
    map.delete(oldestKey);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseBool(value, fallback = false) {
  if (value == null || value === '') return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return fallback;
}

function readSetting(procEnv, fileEnv, key, fallback = '') {
  if (typeof procEnv?.[key] === 'string' && procEnv[key] !== '') {
    return procEnv[key];
  }
  if (typeof fileEnv?.[key] === 'string' && fileEnv[key] !== '') {
    return fileEnv[key];
  }
  return fallback;
}

function buildSecurityConfig(procEnv = process.env, fileEnv = {}) {
  const authPassword = readSetting(
    procEnv,
    fileEnv,
    'DASH_AUTH_PASSWORD',
    readSetting(procEnv, fileEnv, 'DASH_AUTH_PASS', '')
  ).trim();
  const authEnabledFlag = parseBool(readSetting(procEnv, fileEnv, 'DASH_AUTH_ENABLED', ''), false);
  const authCredentialsConfigured = authPassword.length > 0;
  return {
    authEnabled: authEnabledFlag || authCredentialsConfigured,
    authCredentialsConfigured,
    authPassword,
    authCookieName: readSetting(procEnv, fileEnv, 'DASH_AUTH_COOKIE_NAME', 'pq_dash_auth').trim() || 'pq_dash_auth',
    authCookieTtlSec: Math.max(
      300,
      Number(readSetting(procEnv, fileEnv, 'DASH_AUTH_COOKIE_TTL_SEC', '2592000')) || 2592000
    ),
    authSessionPath: '/',
    authPageTitle: readSetting(procEnv, fileEnv, 'DASH_AUTH_PAGE_TITLE', 'PivotQuant Dashboard Access'),
    authPageSubtitle: readSetting(procEnv, fileEnv, 'DASH_AUTH_PAGE_SUBTITLE', 'Enter password to continue.'),
    authBypassLocal: parseBool(readSetting(procEnv, fileEnv, 'DASH_AUTH_LOCAL_BYPASS', 'true'), true),
    writeEndpointsLocalOnly: parseBool(
      readSetting(procEnv, fileEnv, 'DASH_WRITE_ENDPOINTS_LOCAL_ONLY', 'true'),
      true
    ),
    authCookieSecure: parseBool(readSetting(procEnv, fileEnv, 'DASH_AUTH_COOKIE_SECURE', 'true'), true),
  };
}

function withSecurityHeaders(headers = {}) {
  return {
    ...RESPONSE_SECURITY_HEADERS,
    ...headers,
  };
}

function normalizeRemoteAddress(value) {
  if (typeof value !== 'string') return '';
  if (value.startsWith('::ffff:')) {
    return value.slice(7);
  }
  return value;
}

function isLoopbackAddress(addr) {
  const normalized = normalizeRemoteAddress(addr);
  return normalized === '127.0.0.1' || normalized === '::1';
}

function isLoopbackRequest(req) {
  const remoteAddress = req?.socket?.remoteAddress || '';
  if (!isLoopbackAddress(remoteAddress)) {
    return false;
  }
  // Funnel/Serve traffic often arrives through a local proxy with forwarded headers.
  const forwardedFor = String(req?.headers?.['x-forwarded-for'] || '').trim();
  return forwardedFor.length === 0;
}

function requestIsSecure(req) {
  if (Boolean(req?.socket?.encrypted)) return true;
  const forwardedProtoRaw = String(req?.headers?.['x-forwarded-proto'] || '');
  if (!forwardedProtoRaw) return false;
  const forwardedProto = forwardedProtoRaw.split(',')[0].trim().toLowerCase();
  return forwardedProto === 'https';
}

function safeEqual(left, right) {
  const leftBuf = Buffer.from(String(left || ''), 'utf8');
  const rightBuf = Buffer.from(String(right || ''), 'utf8');
  if (leftBuf.length !== rightBuf.length) {
    return false;
  }
  return crypto.timingSafeEqual(leftBuf, rightBuf);
}

function parseCookies(req) {
  const cookieHeader = String(req?.headers?.cookie || '');
  const out = {};
  if (!cookieHeader) return out;
  const pairs = cookieHeader.split(';');
  for (const pair of pairs) {
    const idx = pair.indexOf('=');
    if (idx <= 0) continue;
    const key = pair.slice(0, idx).trim();
    const value = pair.slice(idx + 1).trim();
    if (!key) continue;
    out[key] = value;
  }
  return out;
}

function parseTokenParts(token) {
  const raw = String(token || '').trim();
  if (!raw) return null;
  const parts = raw.split('.');
  if (parts.length !== 3) return null;
  const exp = Number(parts[0]);
  const nonce = parts[1];
  const sig = parts[2];
  if (!Number.isFinite(exp) || !nonce || !sig) return null;
  return { exp, nonce, sig, payload: `${exp}.${nonce}` };
}

function signPayload(payload, secret) {
  return crypto.createHmac('sha256', secret).update(payload).digest('base64url');
}

function createSessionToken(secret, ttlSec) {
  const exp = Math.floor(Date.now() / 1000) + Math.max(300, Number(ttlSec) || 2592000);
  const nonce = crypto.randomBytes(18).toString('base64url');
  const payload = `${exp}.${nonce}`;
  const sig = signPayload(payload, secret);
  return `${payload}.${sig}`;
}

function hasValidSession(req) {
  if (!SECURITY.authEnabled) return true;
  if (!SECURITY.authCredentialsConfigured) return false;
  const cookies = parseCookies(req);
  const token = cookies[SECURITY.authCookieName];
  const parts = parseTokenParts(token);
  if (!parts) return false;
  const now = Math.floor(Date.now() / 1000);
  if (parts.exp < now) return false;
  const expectedSig = signPayload(parts.payload, SECURITY.authPassword);
  return safeEqual(parts.sig, expectedSig);
}

function encodeHtml(text) {
  return String(text || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function renderLoginPage(nextPath, message = '') {
  const safeNext = nextPath && nextPath.startsWith('/') ? nextPath : '/';
  const safeMessage = message ? `<div class="msg">${encodeHtml(message)}</div>` : '';
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${encodeHtml(SECURITY.authPageTitle)}</title>
  <style>
    :root { color-scheme: dark; }
    body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; background: #0b0f1a; color: #e2e8f0; display: grid; place-items: center; min-height: 100vh; }
    .card { width: min(92vw, 420px); background: #0f172a; border: 1px solid rgba(148,163,184,.2); border-radius: 14px; padding: 20px; box-shadow: 0 10px 40px rgba(0,0,0,.35); }
    h1 { margin: 0 0 6px; font-size: 20px; }
    p { margin: 0 0 16px; color: #94a3b8; font-size: 13px; }
    label { display: block; font-size: 12px; color: #94a3b8; margin-bottom: 6px; }
    input { width: 100%; box-sizing: border-box; border-radius: 10px; border: 1px solid rgba(148,163,184,.3); background: #0b1222; color: #e2e8f0; padding: 10px 12px; font-size: 14px; }
    button { margin-top: 12px; width: 100%; border: 0; border-radius: 10px; padding: 10px 12px; font-size: 14px; font-weight: 600; color: #081024; background: linear-gradient(135deg,#60a5fa,#22d3ee); cursor: pointer; }
    .msg { margin-bottom: 12px; font-size: 12px; color: #fca5a5; }
  </style>
</head>
<body>
  <form class="card" method="POST" action="/auth/login">
    <h1>${encodeHtml(SECURITY.authPageTitle)}</h1>
    <p>${encodeHtml(SECURITY.authPageSubtitle)}</p>
    ${safeMessage}
    <input type="hidden" name="next" value="${encodeHtml(safeNext)}" />
    <label for="password">Password</label>
    <input id="password" name="password" type="password" required autofocus />
    <button type="submit">Continue</button>
  </form>
</body>
</html>`;
}

function sendLoginPage(res, nextPath, message = '', statusCode = 200) {
  res.writeHead(
    statusCode,
    withSecurityHeaders({
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store',
    })
  );
  res.end(renderLoginPage(nextPath, message));
}

function parseFormBody(body) {
  const params = new URLSearchParams(String(body || ''));
  return {
    password: String(params.get('password') || ''),
    next: String(params.get('next') || '/'),
  };
}

function setSessionCookieHeaders(res, req, token) {
  const secure = SECURITY.authCookieSecure && requestIsSecure(req);
  const cookieParts = [
    `${SECURITY.authCookieName}=${token}`,
    `Path=${SECURITY.authSessionPath}`,
    `Max-Age=${SECURITY.authCookieTtlSec}`,
    'HttpOnly',
    'SameSite=Lax',
  ];
  if (secure) {
    cookieParts.push('Secure');
  }
  res.setHeader('Set-Cookie', cookieParts.join('; '));
}

function clearSessionCookieHeaders(res, req) {
  const cookieParts = [
    `${SECURITY.authCookieName}=`,
    `Path=${SECURITY.authSessionPath}`,
    'Max-Age=0',
    'HttpOnly',
    'SameSite=Lax',
  ];
  if (SECURITY.authCookieSecure && requestIsSecure(req)) {
    cookieParts.push('Secure');
  }
  res.setHeader('Set-Cookie', cookieParts.join('; '));
}

async function handleAuthRoutes(req, res, url) {
  const isAuthPath = url.pathname === '/auth/login' || url.pathname === '/auth/logout';
  if (!isAuthPath) return false;
  if (!SECURITY.authEnabled) {
    sendJson(res, 404, { error: 'Not found' });
    return true;
  }
  if (url.pathname === '/auth/logout') {
    if (req.method !== 'POST') {
      methodNotAllowed(res, 'POST');
      return true;
    }
    clearSessionCookieHeaders(res, req);
    sendJson(res, 200, { status: 'ok' });
    return true;
  }
  if (url.pathname === '/auth/login') {
    if (req.method === 'GET') {
      const nextPath = url.searchParams.get('next') || '/';
      sendLoginPage(res, nextPath);
      return true;
    }
    if (req.method !== 'POST') {
      methodNotAllowed(res, 'POST');
      return true;
    }
    const contentType = String(req.headers['content-type'] || '').toLowerCase();
    if (!contentType.includes(FORM_URLENCODED)) {
      sendLoginPage(res, '/', 'Unsupported form encoding.', 400);
      return true;
    }
    const body = await readBody(req, 8 * 1024);
    const form = parseFormBody(body);
    const nextPath = form.next && form.next.startsWith('/') ? form.next : '/';
    if (!safeEqual(form.password, SECURITY.authPassword)) {
      sendLoginPage(res, nextPath, 'Invalid password.', 401);
      return true;
    }
    const token = createSessionToken(SECURITY.authPassword, SECURITY.authCookieTtlSec);
    setSessionCookieHeaders(res, req, token);
    res.writeHead(
      302,
      withSecurityHeaders({
        Location: nextPath,
        'Cache-Control': 'no-store',
      })
    );
    res.end();
    return true;
  }
  return false;
}

function isAuthorizedRequest(req) {
  if (!SECURITY.authEnabled) return true;
  if (!SECURITY.authCredentialsConfigured) return false;
  return hasValidSession(req);
}

function normalizeMethod(req) {
  return String(req.method || 'GET').toUpperCase();
}

function methodAllowed(req, expected) {
  const method = normalizeMethod(req);
  if (expected === 'GET') {
    return method === 'GET' || method === 'HEAD';
  }
  return method === expected;
}

function methodNotAllowed(res, allowedMethod) {
  sendJson(res, 405, {
    error: 'Method not allowed',
    allowed: allowedMethod,
  });
}

function isApiPath(pathname) {
  return typeof pathname === 'string' && pathname.startsWith('/api/');
}

function shouldUseLoginPage(req, url) {
  if (isApiPath(url.pathname)) return false;
  const accept = String(req.headers.accept || '').toLowerCase();
  return accept.includes('text/html') || url.pathname === '/' || url.pathname === '/production_pivot_dashboard.html';
}

function redirectToLogin(res, url) {
  const nextPath = `${url.pathname}${url.search || ''}`;
  const location = `/auth/login?next=${encodeURIComponent(nextPath)}`;
  res.writeHead(
    302,
    withSecurityHeaders({
      Location: location,
      'Cache-Control': 'no-store',
    })
  );
  res.end();
}

function sendUnauthorizedJson(res) {
  sendJson(res, 401, {
    error: 'Authentication required',
    message: 'Sign in with the dashboard password.',
  });
}

function sendLoginRequired(res, req, url) {
  if (shouldUseLoginPage(req, url)) {
    redirectToLogin(res, url);
    return;
  }
  sendUnauthorizedJson(res);
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

function mapOptionsSymbol(rawSymbol) {
  const symbol = (rawSymbol || 'SPY').toUpperCase().trim();
  if (
    symbol === 'SPX' ||
    symbol === 'US500' ||
    symbol === 'US 500' ||
    symbol === 'S&P500' ||
    symbol === 'S&P 500' ||
    symbol === 'SP500' ||
    symbol === '^GSPC' ||
    symbol === 'ES' ||
    symbol === 'ES=F'
  ) {
    return 'SPY';
  }
  return symbol;
}

function isMonthlyExpiry(expiryYmd) {
  const value = String(expiryYmd || '');
  if (!/^\d{8}$/.test(value)) return false;
  const year = Number(value.slice(0, 4));
  const month = Number(value.slice(4, 6));
  const day = Number(value.slice(6, 8));
  const dt = new Date(Date.UTC(year, month - 1, day));
  const weekday = dt.getUTCDay();
  return weekday === 5 && day >= 15 && day <= 21;
}

function pickOptionsExpiry(expirations, mode) {
  const normalized = Array.from(
    new Set(
      (Array.isArray(expirations) ? expirations : [])
        .map((item) => String(item))
        .filter((item) => /^\d{8}$/.test(item))
    )
  ).sort();
  if (!normalized.length) return null;

  const today = formatYmd(Math.floor(Date.now() / 1000), 'America/New_York').replace(/-/g, '');
  const safeMode = String(mode || 'front').toLowerCase();

  if (safeMode === '0dte') {
    return normalized.includes(today) ? today : normalized[0];
  }

  if (safeMode === 'monthly') {
    const monthly = normalized.filter((exp) => isMonthlyExpiry(exp) && exp >= today);
    if (monthly.length) return monthly[0];
  }

  if (safeMode === 'all') {
    return normalized[0];
  }

  const front = normalized.find((exp) => exp >= today);
  return front || normalized[0];
}

function normalizeYahooContracts(contracts) {
  if (!Array.isArray(contracts)) return [];
  return contracts
    .map((contract) => ({
      strike: toNumber(contract?.strike, null),
      iv: toNumber(contract?.impliedVolatility, null),
      oi: Math.max(0, toNumber(contract?.openInterest, 0)),
    }))
    .filter((contract) => Number.isFinite(contract.strike));
}

function selectClosestContracts(contracts, spot, limit) {
  const safeLimit = Math.max(1, Math.min(300, Number(limit) || 60));
  if (!Array.isArray(contracts) || contracts.length <= safeLimit || !Number.isFinite(spot)) {
    return Array.isArray(contracts) ? contracts : [];
  }
  return [...contracts]
    .sort((left, right) => Math.abs(left.strike - spot) - Math.abs(right.strike - spot))
    .slice(0, safeLimit);
}

function nearestIvByStrike(contracts, spot) {
  if (!Array.isArray(contracts) || !contracts.length || !Number.isFinite(spot)) return null;
  let best = null;
  for (const contract of contracts) {
    if (!Number.isFinite(contract?.iv)) continue;
    const distance = Math.abs(contract.strike - spot);
    if (!best || distance < best.distance) {
      best = { iv: contract.iv, distance };
    }
  }
  return best ? best.iv : null;
}

function summarizeYahooOptionWindow(optionResult, limit) {
  const selected = optionResult?.options?.[0] || {};
  const quote = optionResult?.quote || {};
  const spot =
    toNumber(quote?.regularMarketPrice, null) ??
    toNumber(quote?.regularMarketPreviousClose, null) ??
    null;

  const calls = selectClosestContracts(normalizeYahooContracts(selected.calls), spot, limit);
  const puts = selectClosestContracts(normalizeYahooContracts(selected.puts), spot, limit);
  const totalContracts = calls.length + puts.length;

  let oiCall = 0;
  let oiPut = 0;
  let withIV = 0;
  let withOI = 0;
  let callWall = null;
  let putWall = null;
  const oiByStrike = new Map();

  for (const call of calls) {
    const oi = call.oi || 0;
    oiCall += oi;
    if (Number.isFinite(call.iv)) withIV += 1;
    if (oi > 0) withOI += 1;
    if (!callWall || oi > callWall.oi) {
      callWall = { strike: call.strike, oi };
    }
    oiByStrike.set(call.strike, (oiByStrike.get(call.strike) || 0) + oi);
  }

  for (const put of puts) {
    const oi = put.oi || 0;
    oiPut += oi;
    if (Number.isFinite(put.iv)) withIV += 1;
    if (oi > 0) withOI += 1;
    if (!putWall || oi > putWall.oi) {
      putWall = { strike: put.strike, oi };
    }
    oiByStrike.set(put.strike, (oiByStrike.get(put.strike) || 0) + oi);
  }

  const totalOi = oiCall + oiPut;
  const topOi = [...oiByStrike.values()]
    .sort((left, right) => right - left)
    .slice(0, 5)
    .reduce((acc, value) => acc + value, 0);
  const oiConcentration = totalOi > 0 ? (topOi / totalOi) * 100 : null;

  let pin = null;
  for (const [strike, oi] of oiByStrike.entries()) {
    if (!pin || oi > pin.oi) {
      pin = { strike, oi };
    }
  }

  const atmCallIv = nearestIvByStrike(calls, spot);
  const atmPutIv = nearestIvByStrike(puts, spot);
  const atmIV =
    Number.isFinite(atmCallIv) && Number.isFinite(atmPutIv)
      ? (atmCallIv + atmPutIv) / 2
      : Number.isFinite(atmCallIv)
        ? atmCallIv
        : Number.isFinite(atmPutIv)
          ? atmPutIv
          : null;

  return {
    spot,
    totalContracts,
    withIV,
    withOI,
    oiCall,
    oiPut,
    oiConcentration,
    totalOi,
    callWall,
    putWall,
    pin,
    atmIV,
    skew25d: null,
  };
}

async function fetchYahooOptionsResult(symbol, expiryYmd = null) {
  const errors = [];
  let attempts = 0;
  const dateQuery = expiryYmd ? `?date=${encodeURIComponent(String(expiryYmd))}` : '';
  const targetUrls = YAHOO_HOSTS.map(
    (host) => `https://${host}/v7/finance/options/${encodeURIComponent(symbol)}${dateQuery}`
  );
  const requestUrls = [];
  for (const targetUrl of targetUrls) {
    requestUrls.push({ label: 'direct', url: targetUrl });
  }
  for (const targetUrl of targetUrls) {
    requestUrls.push({
      label: 'allorigins',
      url: `https://api.allorigins.win/raw?url=${encodeURIComponent(targetUrl)}`,
    });
  }

  for (const request of requestUrls) {
    try {
      const result = await fetchWithRetry(request.url);
      attempts += result.attempts;
      const optionResult = result?.payload?.optionChain?.result?.[0];
      if (optionResult) {
        return { optionResult, attempts };
      }
      errors.push({
        host: request.url,
        source: request.label,
        message: 'Invalid Yahoo options payload',
        status: 0,
      });
    } catch (error) {
      errors.push({
        host: request.url,
        source: request.label,
        message: error?.message || String(error),
        status: error?.statusCode || 0,
      });
    }
  }

  const message = errors.length ? JSON.stringify(errors) : 'Unknown Yahoo options error';
  throw new Error(`Yahoo options failed: ${message}`);
}

function buildWallPayload(strike, oi, maxOi) {
  if (!Number.isFinite(strike)) return null;
  const safeOi = Math.max(0, toNumber(oi, 0));
  return {
    price: strike,
    gex: 0,
    strength: maxOi > 0 ? Math.round((safeOi / maxOi) * 100) : 0,
  };
}

async function fetchYahooGammaFallback({ symbol, expiryMode = 'front', limit = 60 }) {
  const optionSymbol = mapOptionsSymbol(symbol);
  const firstFetch = await fetchYahooOptionsResult(optionSymbol);
  const first = firstFetch.optionResult;
  const expiries = (Array.isArray(first?.expirationDates) ? first.expirationDates : [])
    .map((value) => String(value))
    .filter((value) => /^\d{8}$/.test(value))
    .sort();
  const selectedExpiry = pickOptionsExpiry(expiries, expiryMode);

  let selectedResult = first;
  let attempts = firstFetch.attempts;
  if (selectedExpiry && String(first?.expirationDate || '') !== selectedExpiry) {
    const selectedFetch = await fetchYahooOptionsResult(optionSymbol, selectedExpiry);
    selectedResult = selectedFetch.optionResult;
    attempts += selectedFetch.attempts;
  }

  const summary = summarizeYahooOptionWindow(selectedResult, limit);
  const today = formatYmd(Math.floor(Date.now() / 1000), 'America/New_York').replace(/-/g, '');
  let zeroDteShare = null;
  if (expiries.includes(today)) {
    if (selectedExpiry === today && summary.totalOi > 0) {
      zeroDteShare = 100;
    } else if (selectedExpiry && selectedExpiry !== today) {
      try {
        const todayFetch = await fetchYahooOptionsResult(optionSymbol, today);
        attempts += todayFetch.attempts;
        const todaySummary = summarizeYahooOptionWindow(todayFetch.optionResult, limit);
        const denom = summary.totalOi + todaySummary.totalOi;
        if (denom > 0) {
          zeroDteShare = (todaySummary.totalOi / denom) * 100;
        }
      } catch (_error) {
        zeroDteShare = null;
      }
    }
  }

  const maxWallOi = Math.max(
    toNumber(summary.callWall?.oi, 0),
    toNumber(summary.putWall?.oi, 0),
    toNumber(summary.pin?.oi, 0)
  );

  return {
    source: 'Yahoo',
    symbol: (symbol || 'SPY').toUpperCase().trim(),
    spot: summary.spot,
    expiryMode,
    generatedAt: new Date().toISOString(),
    gammaFlip: null,
    callWall: buildWallPayload(summary.callWall?.strike, summary.callWall?.oi, maxWallOi),
    putWall: buildWallPayload(summary.putWall?.strike, summary.putWall?.oi, maxWallOi),
    pin: buildWallPayload(summary.pin?.strike, summary.pin?.oi, maxWallOi),
    usedOpenInterest: true,
    stats: {
      totalContracts: summary.totalContracts,
      withGreeks: summary.withIV,
      withIV: summary.withIV,
      withOI: summary.withOI,
      oiCall: summary.oiCall,
      oiPut: summary.oiPut,
      oiConcentration: Number.isFinite(summary.oiConcentration) ? summary.oiConcentration : null,
      zeroDteShare: Number.isFinite(zeroDteShare) ? zeroDteShare : null,
      atmIV: summary.atmIV,
      skew25d: summary.skew25d,
      expiries,
    },
    fetch: {
      attempts,
    },
  };
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

  const hosts = YAHOO_HOSTS;
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

function extractCandleClose(data, preferSessionClose = false) {
  const candles = Array.isArray(data?.candles) ? data.candles : [];
  if (!candles.length) return null;
  if (preferSessionClose) {
    const rawIdx = toNumber(data?.session?.usedIndex, candles.length - 1);
    const idx = Math.max(0, Math.min(candles.length - 1, Math.floor(rawIdx)));
    const close = toNumber(candles[idx]?.close, null);
    if (Number.isFinite(close)) return close;
  }
  const lastClose = toNumber(candles[candles.length - 1]?.close, null);
  if (Number.isFinite(lastClose)) return lastClose;
  return null;
}

async function fetchInstrumentReferencePrice(instrument, mode) {
  const normalized = normalizeInstrument(instrument, 'SPY');
  const marketSymbol = normalized === 'US500' ? 'SPX' : normalized;
  const safeMode = mode === 'live' ? 'live' : 'prior_close';

  if (safeMode === 'live') {
    try {
      const spotUrl = `http://127.0.0.1:5001/spot?symbol=${encodeURIComponent(marketSymbol)}`;
      const spot = await fetchLocalJson(spotUrl);
      const spotPrice = toNumber(spot?.spot ?? spot?.currentPrice, null);
      if (Number.isFinite(spotPrice)) {
        return {
          price: spotPrice,
          source: 'IBKR spot',
          asOf: spot?.generatedAt || new Date().toISOString(),
        };
      }
    } catch (_error) {
      // Fallback to Yahoo snapshot below.
    }
  }

  const yahooData = await getYahooData({
    symbol: marketSymbol,
    range: safeMode === 'live' ? '1d' : '5d',
    interval: safeMode === 'live' ? '1m' : '1d',
  });

  let price = null;
  if (safeMode === 'prior_close') {
    price = extractCandleClose(yahooData, true);
  }
  if (!Number.isFinite(price)) {
    price = toNumber(yahooData?.currentPrice, null);
  }
  if (!Number.isFinite(price)) {
    price = extractCandleClose(yahooData, false);
  }
  if (!Number.isFinite(price)) {
    throw new Error(`Unable to resolve price for ${marketSymbol}`);
  }

  return {
    price,
    source: safeMode === 'prior_close' ? 'Yahoo prior close' : 'Yahoo',
    asOf: yahooData?.asOf || new Date().toISOString(),
  };
}

function buildLevelConversionCacheKey(payload) {
  const levels = Array.isArray(payload?.levels) ? payload.levels : [];
  const compactLevels = levels.map((level) => {
    if (!level || typeof level !== 'object') {
      const numeric = toNumber(level, null);
      return [numeric];
    }
    return [
      String(level.label || ''),
      String(level.type || ''),
      toNumber(level.value, null),
      toNumber(level.distance, null),
      toNumber(level.strength, null),
      toNumber(level.touches, null),
      toNumber(level.bounces, null),
      toNumber(level.breaks, null),
    ];
  });

  return JSON.stringify({
    from: normalizeInstrument(payload?.from || payload?.fromInstrument || 'SPY', 'SPY'),
    to: normalizeInstrument(payload?.to || payload?.toInstrument || 'SPX', 'SPX'),
    mode: payload?.mode === 'live' ? 'live' : 'prior_close',
    esBasisMode: payload?.esBasisMode !== false,
    levels: compactLevels,
  });
}

async function getLevelConversionSnapshot(mode) {
  const safeMode = mode === 'live' ? 'live' : 'prior_close';
  const cacheKey = `snapshot:${safeMode}`;
  const cached = readTimedCache(
    levelConversionSnapshotCache,
    cacheKey,
    LEVEL_CONVERTER_SNAPSHOT_TTL_MS
  );
  if (cached) {
    return { snapshot: cached.data, cache: { hit: true, ageMs: cached.ageMs } };
  }

  const [spyRef, spxRef] = await Promise.all([
    fetchInstrumentReferencePrice('SPY', safeMode),
    fetchInstrumentReferencePrice('SPX', safeMode),
  ]);

  let esRef = null;
  try {
    esRef = await fetchInstrumentReferencePrice('ES', safeMode);
  } catch (_error) {
    esRef = null;
  }

  const asOfEpoch = [spyRef?.asOf, spxRef?.asOf, esRef?.asOf]
    .map((value) => Date.parse(value || ''))
    .filter((value) => Number.isFinite(value));
  const asOf = asOfEpoch.length ? new Date(Math.max(...asOfEpoch)).toISOString() : new Date().toISOString();
  const source = [...new Set([spyRef.source, spxRef.source, esRef?.source].filter(Boolean))].join(', ');

  const snapshot = buildConversionSnapshot({
    mode: safeMode,
    source,
    asOf,
    esBasisMode: true,
    prices: {
      SPY: spyRef.price,
      SPX: spxRef.price,
      US500: spxRef.price,
      ES: Number.isFinite(esRef?.price) ? esRef.price : spxRef.price,
    },
  });

  writeTimedCache(
    levelConversionSnapshotCache,
    cacheKey,
    snapshot,
    LEVEL_CONVERTER_SNAPSHOT_TTL_MS,
    LEVEL_CONVERTER_CACHE_MAX_SIZE
  );
  return { snapshot, cache: { hit: false, ageMs: 0 } };
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, withSecurityHeaders({
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  }));
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
      res.writeHead(404, withSecurityHeaders({ 'Content-Type': 'text/plain' }));
      res.end('Not found');
      return;
    }
    res.writeHead(200, withSecurityHeaders({
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store',
      Pragma: 'no-cache',
      Expires: '0',
    }));
    res.end(data);
  });
}

function sendJs(res, filePath) {
  fs.readFile(filePath, (error, data) => {
    if (error) {
      res.writeHead(404, withSecurityHeaders({ 'Content-Type': 'text/plain' }));
      res.end('Not found');
      return;
    }
    res.writeHead(200, withSecurityHeaders({
      'Content-Type': 'application/javascript; charset=utf-8',
      'Cache-Control': 'no-store',
    }));
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

function readJsonFileSafe(filePath, fallback = null) {
  try {
    const value = readJsonFile(filePath);
    return value == null ? fallback : value;
  } catch (_error) {
    return fallback;
  }
}

function loadEnvMap(filePath) {
  const env = {};
  if (!fs.existsSync(filePath)) return env;
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/);
  for (const raw of lines) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const idx = line.indexOf('=');
    if (idx <= 0) continue;
    const key = line.slice(0, idx).trim();
    const value = line.slice(idx + 1).trim().replace(/^['"]|['"]$/g, '');
    if (key) env[key] = value;
  }
  return env;
}

function parseCsv(raw) {
  if (typeof raw !== 'string' || !raw.trim()) return [];
  return raw
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function toNumber(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function ageMinutes(tsMs) {
  const value = toNumber(tsMs, null);
  if (!Number.isFinite(value) || value <= 0) return null;
  return Math.max(0, Math.round((Date.now() - value) / 60000));
}

function readTailLines(filePath, maxLines = 120) {
  if (!fs.existsSync(filePath)) return [];
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/).filter(Boolean);
  if (lines.length <= maxLines) return lines;
  return lines.slice(-maxLines);
}

function parseLogTimestamp(line) {
  if (typeof line !== 'string') return null;
  const match = line.match(/^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]/);
  if (!match) return null;
  return match[1];
}

function summarizeReportDelivery(logLines) {
  const summary = {
    status: 'unknown',
    timestamp: null,
    line: '',
  };
  for (let idx = logLines.length - 1; idx >= 0; idx -= 1) {
    const line = logLines[idx];
    if (!line) continue;
    if (line.includes('DONE  daily_report_send')) {
      summary.status = 'ok';
      summary.timestamp = parseLogTimestamp(line);
      summary.line = line;
      return summary;
    }
    if (line.includes('WARN notification skipped')) {
      summary.status = 'warning';
      summary.timestamp = parseLogTimestamp(line);
      summary.line = line;
      return summary;
    }
    if (line.includes('ERROR notification send failed') || line.includes('ERROR daily report generation failed')) {
      summary.status = 'error';
      summary.timestamp = parseLogTimestamp(line);
      summary.line = line;
      return summary;
    }
  }
  return summary;
}

function summarizeHealthAlert(logLines) {
  const summary = {
    status: 'unknown',
    timestamp: null,
    line: '',
  };
  for (let idx = logLines.length - 1; idx >= 0; idx -= 1) {
    const line = logLines[idx];
    if (!line) continue;
    if (line.includes('notify ok')) {
      summary.status = 'ok';
      summary.timestamp = parseLogTimestamp(line);
      summary.line = line;
      return summary;
    }
    if (line.includes('notify failed')) {
      summary.status = 'error';
      summary.timestamp = parseLogTimestamp(line);
      summary.line = line;
      return summary;
    }
  }
  return summary;
}

function summarizeRestoreDrill(logLines) {
  const summary = {
    status: 'unknown',
    timestamp: null,
    line: '',
    snapshot: '',
    report: '',
  };
  for (let idx = logLines.length - 1; idx >= 0; idx -= 1) {
    const line = logLines[idx];
    if (!line) continue;
    if (line.includes('restore drill ok snapshot=')) {
      summary.status = 'ok';
      summary.timestamp = parseLogTimestamp(line);
      summary.line = line;
      const match = line.match(/snapshot=([0-9_]+)/);
      if (match) summary.snapshot = match[1];
      const reportMatch = line.match(/report=([A-Za-z0-9_.-]+)/);
      if (reportMatch) summary.report = reportMatch[1];
      return summary;
    }
    if (line.includes('restore drill failed')) {
      summary.status = 'failed';
      summary.timestamp = parseLogTimestamp(line);
      summary.line = line;
      return summary;
    }
    if (line.includes('restore drill skipped: lock busy')) {
      summary.status = 'skipped_lock_busy';
      summary.timestamp = parseLogTimestamp(line);
      summary.line = line;
      return summary;
    }
  }
  return summary;
}

async function readOpsStatusRows() {
  let Database;
  try {
    const mod = await import('better-sqlite3');
    Database = mod.default;
  } catch (_err) {
    return {};
  }

  if (!fs.existsSync(SQLITE_DB)) {
    return {};
  }

  const db = new Database(SQLITE_DB, { readonly: true });
  try {
    const exists = db.prepare(
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ops_status' LIMIT 1"
    ).get();
    if (!exists) return {};
    const rows = db.prepare('SELECT key, value FROM ops_status').all();
    const out = {};
    rows.forEach((row) => {
      out[row.key] = row.value;
    });
    return out;
  } finally {
    db.close();
  }
}

async function queryOpsStatus() {
  const env = loadEnvMap(ENV_FILE);
  const ops = await readOpsStatusRows();
  const backupState = readJsonFileSafe(BACKUP_STATE_FILE, {});
  const hostState = readJsonFileSafe(HOST_HEALTH_STATE_FILE, {});
  const alertState = readJsonFileSafe(HEALTH_ALERT_STATE_FILE, {});
  const reportState = readJsonFileSafe(REPORT_DELIVERY_STATE_FILE, {});
  const reportLog = summarizeReportDelivery(readTailLines(REPORT_DELIVERY_LOG_FILE, 200));
  const alertLog = summarizeHealthAlert(readTailLines(HEALTH_ALERT_LOG_FILE, 200));
  const drillLog = summarizeRestoreDrill(readTailLines(RESTORE_DRILL_LOG_FILE, 200));

  const backupLastRunMs = toNumber(ops.backup_last_run_ms, toNumber(backupState.last_run_ms, null));
  const restoreLastRunMs = toNumber(ops.backup_restore_last_run_ms, null);
  const hostLastRunMs = toNumber(ops.host_health_last_run_ms, toNumber(hostState.checked_at_ms, null));

  const reportChannels = parseCsv(
    env.ML_REPORT_NOTIFY_CHANNELS || process.env.ML_REPORT_NOTIFY_CHANNELS || ''
  );
  const alertChannels = parseCsv(
    env.ML_ALERT_NOTIFY_CHANNELS
      || process.env.ML_ALERT_NOTIFY_CHANNELS
      || env.ML_REPORT_NOTIFY_CHANNELS
      || process.env.ML_REPORT_NOTIFY_CHANNELS
      || ''
  );

  return {
    generated_at: new Date().toISOString(),
    backup: {
      status: ops.backup_last_status || backupState.last_status || 'unknown',
      snapshot: ops.backup_last_snapshot || backupState.last_snapshot || '',
      last_run_ms: backupLastRunMs,
      age_min: ageMinutes(backupLastRunMs),
      removed_count: toNumber(ops.backup_last_removed_count, 0),
      error: ops.backup_last_error || '',
    },
    restore_drill: {
      status: ops.backup_restore_last_status || drillLog.status || 'unknown',
      snapshot: ops.backup_restore_last_snapshot || drillLog.snapshot || '',
      last_run_ms: restoreLastRunMs,
      age_min: ageMinutes(restoreLastRunMs),
      error: ops.backup_restore_last_error || '',
      last_timestamp: drillLog.timestamp,
      last_line: drillLog.line,
    },
    host_health: {
      status: ops.host_health_last_status || hostState.status || 'unknown',
      last_run_ms: hostLastRunMs,
      age_min: ageMinutes(hostLastRunMs),
      warn_count: toNumber(ops.host_health_warn_count, 0),
      crit_count: toNumber(ops.host_health_crit_count, 0),
      disk_free_pct: toNumber(ops.host_health_disk_free_pct, null),
      db_growth_mb_per_day: toNumber(ops.host_health_db_growth_mb_per_day, null),
      error: ops.host_health_last_error || '',
    },
    alerts: {
      daily_report: {
        channels: reportChannels,
        last_status: reportLog.status,
        last_timestamp: reportLog.timestamp,
        last_line: reportLog.line,
        sent_keys: Object.keys(reportState?.sent || {}).length,
      },
      immediate: {
        channels: alertChannels,
        last_status: alertLog.status,
        last_timestamp: alertLog.timestamp,
        last_line: alertLog.line,
        services: alertState?.services || {},
      },
    },
  };
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
  const hostHeader = req.headers.host || `127.0.0.1:${PORT}`;
  const url = new URL(req.url || '/', `http://${hostHeader}`);
  const requestIsLocal = isLoopbackRequest(req);

  if (url.pathname === '/health') {
    sendJson(res, 200, {
      status: 'ok',
      auth_enabled: SECURITY.authEnabled,
      auth_credentials_configured: SECURITY.authCredentialsConfigured,
      auth_method: 'password_cookie',
      write_endpoints_local_only: SECURITY.writeEndpointsLocalOnly,
    });
    return;
  }

  if (await handleAuthRoutes(req, res, url)) {
    return;
  }

  if (SECURITY.authEnabled) {
    if (!SECURITY.authCredentialsConfigured) {
      sendJson(res, 500, {
        error: 'Authentication misconfigured',
        message: 'DASH_AUTH_PASSWORD is required when DASH_AUTH_ENABLED=true',
      });
      return;
    }
    const localBypassAllowed = SECURITY.authBypassLocal && requestIsLocal;
    if (!localBypassAllowed && !isAuthorizedRequest(req)) {
      sendLoginRequired(res, req, url);
      return;
    }
  }

  if (SECURITY.writeEndpointsLocalOnly && WRITE_ENDPOINTS.has(url.pathname) && !requestIsLocal) {
    sendJson(res, 403, {
      error: 'Forbidden',
      message: 'This endpoint is restricted to local requests.',
    });
    return;
  }

  if (url.pathname === '/api/market') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
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
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
    try {
      const symbol = url.searchParams.get('symbol') || 'SPX';
      const expiry = url.searchParams.get('expiry') || 'front';
      const limit = url.searchParams.get('limit') || '60';
      const source = (url.searchParams.get('source') || 'auto').toLowerCase();
      const gammaUrl = `http://127.0.0.1:5001/gamma?symbol=${encodeURIComponent(
        symbol
      )}&expiry=${encodeURIComponent(expiry)}&limit=${encodeURIComponent(limit)}`;

      let data = null;
      let bridgeError = null;
      if (source !== 'yahoo') {
        try {
          data = await fetchLocalJson(gammaUrl);
        } catch (error) {
          bridgeError = error;
          if (source === 'ibkr') {
            throw error;
          }
        }
      }

      if (!data) {
        try {
          data = await fetchYahooGammaFallback({ symbol, expiryMode: expiry, limit });
          if (bridgeError?.message) {
            data.fallback = { mode: 'yahoo', reason: bridgeError.message };
          }
        } catch (fallbackError) {
          if (bridgeError) {
            throw {
              statusCode: bridgeError?.statusCode || fallbackError?.statusCode || 502,
              message: `IBKR gamma unavailable (${bridgeError?.message || 'error'}); Yahoo fallback unavailable (${fallbackError?.message || 'error'})`,
            };
          }
          throw fallbackError;
        }
      }

      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Gamma bridge unavailable');
    }
    return;
  }

  if (url.pathname === '/api/ib/market') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
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
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
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
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
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
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
    try {
      const data = await fetchLocalJson('http://127.0.0.1:5003/health');
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'ML health unavailable');
    }
    return;
  }

  if (url.pathname === '/api/live/health') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
    try {
      const data = await fetchLocalJson('http://127.0.0.1:5004/health');
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Live collector health unavailable');
    }
    return;
  }

  if (url.pathname === '/api/ops/status') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
    try {
      const data = await queryOpsStatus();
      sendJson(res, 200, data);
    } catch (error) {
      sendJson(res, 500, {
        error: 'Ops status unavailable',
        message: error?.message || String(error),
      });
    }
    return;
  }

  if (url.pathname === '/api/ml/reload') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
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
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
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
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return;
    }
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
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return;
    }
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
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
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

  if (url.pathname === '/api/levels/convert') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return;
    }
    try {
      const body = await readBody(req);
      const payload = body ? JSON.parse(body) : {};
      const levels = Array.isArray(payload?.levels) ? payload.levels : [];
      if (levels.length > 1000) {
        sendJson(res, 413, {
          error: 'Too many levels',
          message: 'Maximum 1000 levels per conversion request.',
        });
        return;
      }

      const fromRequested = String(payload?.from || payload?.fromInstrument || 'SPY');
      const toRequested = String(payload?.to || payload?.toInstrument || 'SPX');
      const fromInstrument = normalizeInstrument(fromRequested, 'SPY');
      const toInstrument = normalizeInstrument(toRequested, 'SPX');
      const mode = payload?.mode === 'live' ? 'live' : 'prior_close';
      const esBasisMode = payload?.esBasisMode !== false;

      const cacheKey = buildLevelConversionCacheKey({
        levels,
        from: fromInstrument,
        to: toInstrument,
        mode,
        esBasisMode,
      });
      const cached = readTimedCache(
        levelConversionResultCache,
        cacheKey,
        LEVEL_CONVERTER_RESULT_TTL_MS
      );
      if (cached) {
        const conversion = cached.data?.conversion
          ? {
              ...cached.data.conversion,
              cache: {
                ...(cached.data.conversion.cache || {}),
                hit: true,
                ageMs: cached.ageMs,
              },
            }
          : null;
        sendJson(res, 200, {
          ...cached.data,
          conversion,
        });
        return;
      }

      const snapshotResult = await getLevelConversionSnapshot(mode);
      const converted = convertLevels({
        levels,
        fromInstrument,
        toInstrument,
        snapshot: snapshotResult.snapshot,
        esBasisMode,
      });

      const response = {
        status: 'ok',
        levels: converted.levels,
        conversion: {
          ...converted.metadata,
          fromRequested,
          toRequested,
          fromInstrument,
          toInstrument,
          levelCount: levels.length,
          cache: {
            hit: false,
            ageMs: 0,
            snapshotHit: snapshotResult.cache.hit,
            snapshotAgeMs: snapshotResult.cache.ageMs,
          },
        },
      };

      writeTimedCache(
        levelConversionResultCache,
        cacheKey,
        response,
        LEVEL_CONVERTER_RESULT_TTL_MS,
        LEVEL_CONVERTER_CACHE_MAX_SIZE
      );
      sendJson(res, 200, response);
    } catch (error) {
      const statusCode = error instanceof SyntaxError ? 400 : 500;
      sendJson(res, statusCode, {
        error: 'Level conversion failed',
        message: error?.message || String(error),
      });
    }
    return;
  }

  if (url.pathname === '/api/levels') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
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
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
    if (fs.existsSync(LOCAL_CHART_PATH)) {
      sendJs(res, LOCAL_CHART_PATH);
    } else {
      res.writeHead(404, withSecurityHeaders({ 'Content-Type': 'text/plain' }));
      res.end('lightweight-charts not installed');
    }
    return;
  }

  if (url.pathname === '/' || url.pathname === '/production_pivot_dashboard.html') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return;
    }
    sendFile(res, DASHBOARD_FILE);
    return;
  }

  res.writeHead(404, withSecurityHeaders({ 'Content-Type': 'text/plain' }));
  res.end('Not found');
});

server.listen(PORT, HOST, () => {
  /* eslint-disable-next-line no-console */
  console.log(`Pivot dashboard server running at http://${HOST}:${PORT}`);
  /* eslint-disable-next-line no-console */
  console.log(
    `[security] auth_enabled=${SECURITY.authEnabled} auth_credentials_configured=${SECURITY.authCredentialsConfigured} auth_local_bypass=${SECURITY.authBypassLocal} write_endpoints_local_only=${SECURITY.writeEndpointsLocalOnly}`
  );
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
