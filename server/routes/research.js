export async function handleResearchRoutes(req, res, url, deps) {
  const {
    RESEARCH_API_PORT,
    methodAllowed,
    methodNotAllowed,
    sendJson,
    sendProxyError,
    fetchLocalJson,
    fetchLocalJsonPost,
    readBody,
  } = deps;

  if (url.pathname === '/api/research/health') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const data = await fetchLocalJson(`http://127.0.0.1:${RESEARCH_API_PORT}/health`);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Research API unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/research/metadata') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const data = await fetchLocalJson(`http://127.0.0.1:${RESEARCH_API_PORT}/metadata`);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Research metadata unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/research/slice-query') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return true;
    }
    try {
      const body = await readBody(req);
      const payload = body ? JSON.parse(body) : {};
      const data = await fetchLocalJsonPost(`http://127.0.0.1:${RESEARCH_API_PORT}/slice-query`, payload);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Research slice query unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/research/expectancy-map') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return true;
    }
    try {
      const body = await readBody(req);
      const payload = body ? JSON.parse(body) : {};
      const data = await fetchLocalJsonPost(`http://127.0.0.1:${RESEARCH_API_PORT}/expectancy-map`, payload);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Research expectancy map unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/research/walkforward') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return true;
    }
    try {
      const body = await readBody(req);
      const payload = body ? JSON.parse(body) : {};
      const data = await fetchLocalJsonPost(`http://127.0.0.1:${RESEARCH_API_PORT}/walkforward`, payload);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Research walkforward unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/research/cohort-drilldown') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return true;
    }
    try {
      const body = await readBody(req);
      const payload = body ? JSON.parse(body) : {};
      const data = await fetchLocalJsonPost(`http://127.0.0.1:${RESEARCH_API_PORT}/cohort-drilldown`, payload);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Research cohort drilldown unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/research/replay-day') {
    if (!methodAllowed(req, 'POST')) {
      methodNotAllowed(res, 'POST');
      return true;
    }
    try {
      const body = await readBody(req);
      const payload = body ? JSON.parse(body) : {};
      const data = await fetchLocalJsonPost(`http://127.0.0.1:${RESEARCH_API_PORT}/replay-day`, payload);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Research replay day unavailable');
    }
    return true;
  }

  return false;
}
