export async function handleModelRoutes(req, res, url, deps) {
  const {
    MODEL_API_PORT,
    methodAllowed,
    methodNotAllowed,
    sendJson,
    sendProxyError,
    fetchLocalJson,
    fetchLocalJsonPost,
    readBody,
  } = deps;

  if (url.pathname === '/api/models/registry') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const data = await fetchLocalJson(`http://127.0.0.1:${MODEL_API_PORT}/registry`);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Model registry unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/models/health') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const data = await fetchLocalJson(`http://127.0.0.1:${MODEL_API_PORT}/health`);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Model API unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/models/diagnostics') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const modelId = url.searchParams.get('id') || '';
      const encoded = encodeURIComponent(modelId);
      const data = await fetchLocalJson(`http://127.0.0.1:${MODEL_API_PORT}/diagnostics?id=${encoded}`);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Model diagnostics unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/models/benchmarks') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const modelId = url.searchParams.get('id') || '';
      const encoded = encodeURIComponent(modelId);
      const data = await fetchLocalJson(`http://127.0.0.1:${MODEL_API_PORT}/benchmarks?id=${encoded}`);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Model benchmarks unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/models/baseline-compare') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const modelId = url.searchParams.get('id') || '';
      const encoded = encodeURIComponent(modelId);
      const data = await fetchLocalJson(`http://127.0.0.1:${MODEL_API_PORT}/baseline-compare?id=${encoded}`);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Model baseline comparison unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/models/decision') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const modelId = url.searchParams.get('id') || '';
      const encoded = encodeURIComponent(modelId);
      const data = await fetchLocalJson(`http://127.0.0.1:${MODEL_API_PORT}/decision?id=${encoded}`);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Model decision panel unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/models/review-log') {
    if (req.method === 'GET') {
      try {
        const modelId = url.searchParams.get('id') || '';
        const limit = url.searchParams.get('limit') || '';
        const verdict = url.searchParams.get('verdict') || '';
        const reviewer = url.searchParams.get('reviewer') || '';
        const params = new URLSearchParams();
        if (modelId) params.set('id', modelId);
        if (limit) params.set('limit', limit);
        if (verdict) params.set('verdict', verdict);
        if (reviewer) params.set('reviewer', reviewer);
        const suffix = params.toString() ? `?${params.toString()}` : '';
        const data = await fetchLocalJson(`http://127.0.0.1:${MODEL_API_PORT}/review-log${suffix}`);
        sendJson(res, 200, data);
      } catch (error) {
        sendProxyError(res, error, 'Model review log unavailable');
      }
      return true;
    }
    if (req.method === 'POST') {
      try {
        const body = await readBody(req);
        const payload = body ? JSON.parse(body) : {};
        const data = await fetchLocalJsonPost(`http://127.0.0.1:${MODEL_API_PORT}/review-log`, payload);
        sendJson(res, 200, data);
      } catch (error) {
        sendProxyError(res, error, 'Model review recording failed');
      }
      return true;
    }
    methodNotAllowed(res, 'GET, POST');
    return true;
  }

  if (url.pathname === '/api/models/review-compare') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const reviewIdA = url.searchParams.get('review_id_a') || '';
      const reviewIdB = url.searchParams.get('review_id_b') || '';
      const params = new URLSearchParams();
      if (reviewIdA) params.set('review_id_a', reviewIdA);
      if (reviewIdB) params.set('review_id_b', reviewIdB);
      const suffix = params.toString() ? `?${params.toString()}` : '';
      const data = await fetchLocalJson(`http://127.0.0.1:${MODEL_API_PORT}/review-compare${suffix}`);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Model review comparison unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/models/governance-summary') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const data = await fetchLocalJson(`http://127.0.0.1:${MODEL_API_PORT}/governance-summary`);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Model governance summary unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/models/governance-workspace') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    try {
      const limit = url.searchParams.get('limit') || '';
      const params = new URLSearchParams();
      if (limit) params.set('limit', limit);
      const suffix = params.toString() ? `?${params.toString()}` : '';
      const data = await fetchLocalJson(`http://127.0.0.1:${MODEL_API_PORT}/governance-workspace${suffix}`);
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Model governance workspace unavailable');
    }
    return true;
  }

  return false;
}
