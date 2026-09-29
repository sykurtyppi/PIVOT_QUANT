export async function handleRuntimeRoutes(req, res, url, deps) {
  const {
    SECURITY,
    RUNTIME_ARCHITECTURE,
    requestIsLocal,
    METRICS_FILE,
    CALIB_FILE,
    ACTIVE_MANIFEST_FILE,
    getAuthAuditSnapshot,
    methodAllowed,
    methodNotAllowed,
    sendJson,
    sendProxyError,
    buildAuthSessionsSnapshot,
    readJsonFileWithMetaAsync,
    summarizeMlMetrics,
    fetchLocalJson,
    readMlDashboardHealth,
    queryOpsStatus,
    path,
    ROOT_DIR,
  } = deps;

  const localOnly = () => {
    if (requestIsLocal) return false;
    sendJson(res, 403, {
      error: 'Forbidden',
      message: 'This endpoint is restricted to local requests.',
    });
    return true;
  };

  if (url.pathname === '/health') {
    sendJson(res, 200, { status: 'ok' });
    return true;
  }

  if (url.pathname === '/api/runtime/health') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    if (localOnly()) {
      return true;
    }
    const authAudit = getAuthAuditSnapshot();
    sendJson(res, 200, {
      status: 'ok',
      auth_enabled: SECURITY.authEnabled,
      auth_credentials_configured: SECURITY.authCredentialsConfigured,
      auth_method: 'password_cookie',
      auth_service_token_configured: SECURITY.authServiceTokenConfigured,
      auth_service_token_scope: 'local_api_only',
      auth_password_policy_enforced: SECURITY.authPasswordPolicyEnforced,
      auth_password_strong_enough: SECURITY.authPasswordStrongEnough,
      auth_password_min_length: SECURITY.authPasswordMinLength,
      auth_local_bypass: SECURITY.authBypassLocal,
      auth_cookie_secure: SECURITY.authCookieSecure,
      auth_bind_host: SECURITY.bindHost,
      auth_bind_is_loopback: SECURITY.bindIsLoopback,
      auth_policy_ok: SECURITY.authPolicyOk,
      auth_policy_issues: SECURITY.authPolicyIssues,
      auth_rate_limit_enabled: SECURITY.authRateLimitEnabled,
      auth_rate_limit_window_sec: SECURITY.authRateLimitWindowSec,
      auth_rate_limit_max_attempts: SECURITY.authRateLimitMaxAttempts,
      auth_rate_limit_lockout_sec: SECURITY.authRateLimitLockoutSec,
      write_endpoints_local_only: SECURITY.writeEndpointsLocalOnly,
      runtime_architecture_mode: RUNTIME_ARCHITECTURE.runtime_mode,
      runtime_architecture_governance_state: RUNTIME_ARCHITECTURE.runtime_governance_state,
      runtime_dashboard_uses_src_library: RUNTIME_ARCHITECTURE.dashboard_uses_src_library,
      runtime_dashboard_script_count: RUNTIME_ARCHITECTURE.dashboard_script_count,
      ...authAudit,
    });
    return true;
  }

  if (url.pathname === '/api/security/sessions') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    if (localOnly()) {
      return true;
    }
    sendJson(res, 200, buildAuthSessionsSnapshot());
    return true;
  }

  if (url.pathname === '/api/runtime/architecture') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    if (localOnly()) {
      return true;
    }
    sendJson(res, 200, { status: 'ok', ...RUNTIME_ARCHITECTURE });
    return true;
  }

  if (url.pathname === '/api/ml/metrics') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    if (localOnly()) {
      return true;
    }
    try {
      const [metricsSource, calibSource, manifestSource] = await Promise.all([
        readJsonFileWithMetaAsync(METRICS_FILE),
        readJsonFileWithMetaAsync(CALIB_FILE),
        readJsonFileWithMetaAsync(ACTIVE_MANIFEST_FILE),
      ]);
      const latestSourceMs = Math.max(
        Number(metricsSource?.mtimeMs) || 0,
        Number(calibSource?.mtimeMs) || 0
      );
      const sourceFiles = {
        metrics: {
          path: path.relative(ROOT_DIR, String(metricsSource?.filePath || METRICS_FILE)),
          mtime_ms: metricsSource?.mtimeMs ?? null,
          size_bytes: metricsSource?.sizeBytes ?? null,
        },
        calibration: {
          path: path.relative(ROOT_DIR, String(calibSource?.filePath || CALIB_FILE)),
          mtime_ms: calibSource?.mtimeMs ?? null,
          size_bytes: calibSource?.sizeBytes ?? null,
        },
        active_manifest: {
          path: path.relative(ROOT_DIR, String(manifestSource?.filePath || ACTIVE_MANIFEST_FILE)),
          mtime_ms: manifestSource?.mtimeMs ?? null,
          size_bytes: manifestSource?.sizeBytes ?? null,
        },
      };
      const manifestPayload =
        manifestSource?.data && typeof manifestSource.data === 'object'
          ? manifestSource.data
          : null;
      const summary = summarizeMlMetrics(metricsSource?.data, calibSource?.data, {
        updatedAtMs: latestSourceMs > 0 ? latestSourceMs : null,
        sourceFiles,
        activeModelVersion: manifestPayload?.version ?? null,
        activeModelTrainedEndTs: manifestPayload?.trained_end_ts ?? null,
      });
      if (summary.status === 'empty') {
        sendJson(res, 404, {
          error: 'ML metrics unavailable',
          message: 'Run the training script to generate metrics.',
          updated_at: summary.updated_at,
          stale_seconds: summary.stale_seconds,
          source_files: summary.source_files,
          active_model_version: summary.active_model_version,
          active_model_trained_end_ts: summary.active_model_trained_end_ts,
        });
        return true;
      }
      sendJson(res, 200, summary);
    } catch (error) {
      sendJson(res, 500, {
        error: 'ML metrics failed',
        message: error?.message || String(error),
      });
    }
    return true;
  }

  if (url.pathname === '/api/ml/health') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    if (localOnly()) {
      return true;
    }
    try {
      const [data, dashboardHealth] = await Promise.all([
        fetchLocalJson('http://127.0.0.1:5003/health'),
        readMlDashboardHealth(),
      ]);
      sendJson(res, 200, {
        ...(data && typeof data === 'object' ? data : {}),
        dashboard_health: dashboardHealth,
      });
    } catch (error) {
      sendProxyError(res, error, 'ML health unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/live/health') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    if (localOnly()) {
      return true;
    }
    try {
      const data = await fetchLocalJson('http://127.0.0.1:5004/health');
      sendJson(res, 200, data);
    } catch (error) {
      sendProxyError(res, error, 'Live collector health unavailable');
    }
    return true;
  }

  if (url.pathname === '/api/ops/status') {
    if (!methodAllowed(req, 'GET')) {
      methodNotAllowed(res, 'GET');
      return true;
    }
    if (localOnly()) {
      return true;
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
    return true;
  }

  return false;
}
