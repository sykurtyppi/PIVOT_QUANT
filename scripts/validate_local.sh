#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_URL="${VALIDATE_BASE_URL:-http://127.0.0.1:3000}"

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "[FAIL] Required command not found: ${cmd}" >&2
    exit 1
  fi
}

resolve_python_bin() {
  if [[ -n "${PYTHON_BIN:-}" && -x "${PYTHON_BIN}" ]]; then
    echo "${PYTHON_BIN}"
    return 0
  fi

  local candidates=(
    "${ROOT_DIR}/.venv313/bin/python"
    "${ROOT_DIR}/.venv313/bin/python3"
    "${ROOT_DIR}/.venv/bin/python"
    "${ROOT_DIR}/.venv/bin/python3"
  )

  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -x "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  done

  echo "[FAIL] No supported Python interpreter found. Set PYTHON_BIN or install .venv313/.venv." >&2
  exit 1
}

run_step() {
  local label="$1"
  shift
  echo
  echo "==> ${label}"
  "$@"
}

check_dashboard_reachable() {
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' "${BASE_URL}/health" || true)"
  if [[ "${code}" != "200" ]]; then
    echo "[FAIL] Dashboard/proxy is not reachable at ${BASE_URL} (health returned ${code:-unreachable})." >&2
    echo "       Start it with 'bash server/run_all.sh' or 'npm run dashboard:server', then rerun validation." >&2
    exit 1
  fi
}

require_cmd bash
require_cmd curl
PYTHON_EXEC="$(resolve_python_bin)"

export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="${ROOT_DIR}"

echo "PivotQuant local validation"
echo "Root: ${ROOT_DIR}"
echo "Python: ${PYTHON_EXEC}"
echo "Base URL: ${BASE_URL}"

run_step "Shell syntax checks" \
  bash -n \
  "${ROOT_DIR}/server/run_all.sh" \
  "${ROOT_DIR}/server/run_model_api.sh" \
  "${ROOT_DIR}/server/run_research_api.sh" \
  "${ROOT_DIR}/scripts/smoke_sprint_c_routes.sh"

run_step "Research mart build tests" \
  "${PYTHON_EXEC}" "${ROOT_DIR}/tests/python/test_build_research_marts.py"

run_step "Research API tests" \
  "${PYTHON_EXEC}" "${ROOT_DIR}/tests/python/test_research_api.py"

run_step "Model API tests" \
  "${PYTHON_EXEC}" "${ROOT_DIR}/tests/python/test_model_api.py"

run_step "Model context backfill tests" \
  "${PYTHON_EXEC}" "${ROOT_DIR}/tests/python/test_backfill_model_context.py"

run_step "Dashboard route/module smoke prerequisite" check_dashboard_reachable

run_step "Sprint C route/module smoke" \
  bash "${ROOT_DIR}/scripts/smoke_sprint_c_routes.sh" "${BASE_URL}"

echo
echo "Local validation passed"
