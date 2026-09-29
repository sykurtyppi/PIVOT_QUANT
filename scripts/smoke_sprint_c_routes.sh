#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:3000}"

check_ok() {
  local path="$1"
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' "${BASE_URL}${path}")"
  if [[ "${code}" != "200" ]]; then
    echo "[FAIL] ${path} -> HTTP ${code}"
    return 1
  fi
  echo "[OK]   ${path}"
}

check_chart_asset() {
  local path="/static/lightweight-charts.js"
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' "${BASE_URL}${path}")"
  if [[ "${code}" != "200" && "${code}" != "302" ]]; then
    echo "[FAIL] ${path} -> HTTP ${code}"
    return 1
  fi
  echo "[OK]   ${path} -> HTTP ${code}"
}

echo "Sprint C smoke against ${BASE_URL}"

echo
echo "Dashboard module assets"
check_ok "/app/shared/dashboard_runtime.js"
check_ok "/app/shared/workspace_requests.js"
check_ok "/app/research/index.js"
check_ok "/app/models/index.js"
check_ok "/app/governance/index.js"
check_ok "/app/replay/index.js"
check_ok "/app/ops/index.js"

echo
echo "Static shell"
check_ok "/"
check_ok "/production_pivot_dashboard.html"
check_chart_asset

echo
echo "Proxy route families"
check_ok "/health"
check_ok "/api/runtime/architecture"
check_ok "/api/research/health"
check_ok "/api/models/health"
check_ok "/api/ops/status"

echo
echo "Sprint C smoke passed"
