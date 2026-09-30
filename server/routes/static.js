export async function handleStaticRoutes(req, res, url, deps) {
  const LIGHTWEIGHT_CHARTS_FALLBACK_URL =
    'https://cdn.jsdelivr.net/npm/lightweight-charts@5.1.0/dist/lightweight-charts.standalone.production.js';
  const {
    fs,
    DASHBOARD_FILE,
    DASHBOARD_RUNTIME_FILE,
    WORKSPACE_REQUESTS_FILE,
    RESEARCH_WORKSPACE_FILE,
    MODELS_WORKSPACE_FILE,
    GOVERNANCE_WORKSPACE_FILE,
    REPLAY_WORKSPACE_FILE,
    OPS_WORKSPACE_FILE,
    DAILY_LEVELS_FILE,
    VOLATILITY_LEVELS_JS,
    NYSE_CALENDAR_JS,
    TOUCH_RATES_FILE,
    LOCAL_CHART_PATH,
    methodAllowed,
    methodNotAllowed,
    sendJs,
    sendJsonFile,
    sendFile,
    withSecurityHeaders,
  } = deps;

  if (url.pathname === '/static/lightweight-charts.js') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    if (fs.existsSync(LOCAL_CHART_PATH)) {
      sendJs(res, LOCAL_CHART_PATH);
    } else {
      res.writeHead(302, withSecurityHeaders({
        Location: LIGHTWEIGHT_CHARTS_FALLBACK_URL,
        'Cache-Control': 'no-store',
      }));
      res.end();
    }
    return true;
  }

  if (url.pathname === '/app/shared/dashboard_runtime.js') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendJs(res, DASHBOARD_RUNTIME_FILE);
    return true;
  }

  if (url.pathname === '/app/shared/workspace_requests.js') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendJs(res, WORKSPACE_REQUESTS_FILE);
    return true;
  }

  if (url.pathname === '/app/research/index.js') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendJs(res, RESEARCH_WORKSPACE_FILE);
    return true;
  }

  if (url.pathname === '/app/models/index.js') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendJs(res, MODELS_WORKSPACE_FILE);
    return true;
  }

  if (url.pathname === '/app/governance/index.js') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendJs(res, GOVERNANCE_WORKSPACE_FILE);
    return true;
  }

  if (url.pathname === '/app/replay/index.js') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendJs(res, REPLAY_WORKSPACE_FILE);
    return true;
  }

  if (url.pathname === '/app/ops/index.js') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendJs(res, OPS_WORKSPACE_FILE);
    return true;
  }

  if (url.pathname === '/app/levels/volatility_levels.js') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendJs(res, VOLATILITY_LEVELS_JS);
    return true;
  }

  if (url.pathname === '/app/levels/nyse_calendar.js') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendJs(res, NYSE_CALENDAR_JS);
    return true;
  }

  if (url.pathname === '/app/levels/touch_rates.json') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendJsonFile(res, TOUCH_RATES_FILE);
    return true;
  }

  if (url.pathname === '/levels' || url.pathname === '/daily_levels.html') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendFile(res, DAILY_LEVELS_FILE);
    return true;
  }

  if (url.pathname === '/' || url.pathname === '/production_pivot_dashboard.html') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    sendFile(res, DASHBOARD_FILE);
    return true;
  }

  return false;
}
