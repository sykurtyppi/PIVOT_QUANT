(function attachDashboardRuntime(global) {
  function getApiOrigin(port = 3000) {
    const raw = global.location && global.location.origin;
    if (!raw || raw === 'null') return `http://127.0.0.1:${port}`;
    if (port === 3000) return raw;
    return raw.replace(/:\d+$/, '') + `:${port}`;
  }

  global.PQDashboardRuntime = Object.freeze({
    getApiOrigin,
  });
})(window);
