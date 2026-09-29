#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_URL="${UI_SMOKE_BASE_URL:-http://127.0.0.1:3000}"

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "[FAIL] Required command not found: ${cmd}" >&2
    exit 1
  fi
}

require_cmd node
require_cmd bash
require_cmd curl

PLAYWRIGHT_CLI="${ROOT_DIR}/node_modules/@playwright/test/cli.js"
if [[ ! -f "${PLAYWRIGHT_CLI}" ]]; then
  echo "[FAIL] @playwright/test is not installed." >&2
  echo "       Run 'npm install' in ${ROOT_DIR}, then 'npx playwright install chromium'." >&2
  exit 1
fi

health_code="$(curl -sS -o /dev/null -w '%{http_code}' "${BASE_URL}/health" || true)"
if [[ "${health_code}" != "200" ]]; then
  echo "[FAIL] Dashboard/proxy is not reachable at ${BASE_URL} (health returned ${health_code:-unreachable})." >&2
  echo "       Start it with 'bash server/run_all.sh' or 'npm run dashboard:server', then rerun UI smoke." >&2
  exit 1
fi

echo "UI smoke against ${BASE_URL}"
cd "${ROOT_DIR}"
UI_SMOKE_BASE_URL="${BASE_URL}" \
  node "${PLAYWRIGHT_CLI}" test tests/ui/dashboard.smoke.spec.js --workers=1 --reporter=line

