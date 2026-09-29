export async function handleMarketRoutes(req, res, url, deps) {
  const {
    fs,
    ROOT_DIR,
    LOCAL_CHART_PATH,
    methodAllowed,
    methodNotAllowed,
    sendJson,
    sendProxyError,
    fetchLocalJson,
    fetchLocalJsonPost,
    getYahooData,
    fetchYahooGammaFallback,
    readBody,
    readTimedCache,
    writeTimedCache,
    levelConversionResultCache,
    LEVEL_CONVERTER_RESULT_TTL_MS,
    LEVEL_CONVERTER_CACHE_MAX_SIZE,
    buildLevelConversionCacheKey,
    normalizeInstrument,
    getLevelConversionSnapshot,
    convertLevels,
    queryLevelStats,
  } = deps;

  if (url.pathname === '/api/market') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
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
    return true;
  }

  if (url.pathname === '/api/gamma') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const symbol = url.searchParams.get('symbol') || 'SPX';
      const expiry = url.searchParams.get('expiry') || '90dte';
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
    return true;
  }

  if (url.pathname === '/api/ib/market') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
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
    return true;
  }

  if (url.pathname === '/api/ib/spot') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const symbol = url.searchParams.get('symbol') || 'SPX';
      const ibUrl = `http://127.0.0.1:5001/spot?symbol=${encodeURIComponent(symbol)}`;
      const data = await fetchLocalJson(ibUrl);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'IBKR spot bridge unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/ml/reload') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return true;
    }
    try {
      const data = await fetchLocalJsonPost('http://127.0.0.1:5003/reload', {});
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'ML reload unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/ml/score') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return true;
    }
    try {
      const body = await readBody(req);
      const payload = body ? JSON.parse(body) : {};
      const data = await fetchLocalJsonPost('http://127.0.0.1:5003/score', payload);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'ML score unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/events') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return true;
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
    return true;
  }

  if (url.pathname === '/api/bars') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return true;
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
    return true;
  }

  if (url.pathname === '/api/daily-candles') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
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
    return true;
  }

  if (url.pathname === '/api/levels/convert') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return true;
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
        return true;
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
        return true;
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
    return true;
  }

  if (url.pathname === '/api/levels') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
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
    return true;
  }

  return false;
}
