#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OVERALL_RESULT="PASS"
WARN_REASONS=()
DASHBOARD_UNREACHABLE=0
START="$(date +%s)"

log_info() { echo "[INFO] $1"; }
log_warn() { echo "[WARN] $1"; }
log_fail() { echo "[FAIL] $1"; }

mark_warn() {
  local reason="$1"
  if [[ "${OVERALL_RESULT}" == "PASS" ]]; then
    OVERALL_RESULT="WARN"
  fi
  WARN_REASONS+=("${reason}")
}

mark_fail() {
  local reason="$1"
  OVERALL_RESULT="FAIL"
  log_fail "${reason}"
}

print_section() {
  local name="$1"
  echo
  echo "=== ${name} ==="
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

  local candidate=""
  for candidate in "${candidates[@]}"; do
    if [[ -x "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  done

  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi
  return 1
}

join_warn_reasons() {
  local seen='|'
  local joined=''
  local reason=''
  for reason in "${WARN_REASONS[@]}"; do
    if [[ -z "${reason}" ]]; then
      continue
    fi
    case "${seen}" in
      *"|${reason}|"*)
        continue
        ;;
    esac
    seen="${seen}${reason}|"
    if [[ -n "${joined}" ]]; then
      joined="${joined}; ${reason}"
    else
      joined="${reason}"
    fi
  done
  printf '%s' "${joined}"
}

run_validation() {
  print_section "VALIDATION"
  local output=""
  local exit_code=0
  set +e
  output="$(bash "${ROOT_DIR}/scripts/validate_local.sh" 2>&1)"
  exit_code=$?
  set -e
  printf '%s\n' "${output}"
  if [[ ${exit_code} -eq 0 ]]; then
    echo "[PASS] Local validation passed"
  else
    if printf '%s\n' "${output}" | grep -q "Dashboard/proxy is not reachable"; then
      DASHBOARD_UNREACHABLE=1
    fi
    mark_fail "Local validation failed"
  fi
}

run_integrity() {
  print_section "INTEGRITY"
  local integrity_script="${ROOT_DIR}/scripts/verify_release_integrity.py"
  if [[ ! -f "${integrity_script}" ]]; then
    mark_fail "Integrity script is missing: ${integrity_script}"
    return
  fi
  if [[ -z "${PYTHON_EXEC}" ]]; then
    mark_fail "No supported Python interpreter found for integrity verification"
    return
  fi

  local output=""
  local exit_code=0
  local integrity_result=""

  set +e
  output="$(cd "${ROOT_DIR}" && PYTHONDONTWRITEBYTECODE=1 "${PYTHON_EXEC}" "${integrity_script}" 2>&1)"
  exit_code=$?
  set -e

  if [[ -n "${output}" ]]; then
    printf '%s\n' "${output}" | sed '/^\[RESULT\] /d'
  fi

  integrity_result="$(printf '%s\n' "${output}" | awk '/^\[RESULT\] / {print $2}' | tail -n 1)"
  if [[ ${exit_code} -ne 0 || "${integrity_result}" == "FAIL" ]]; then
    mark_fail "Integrity verification failed"
    return
  fi

  if [[ "${integrity_result}" == "WARN" ]]; then
    mark_warn "integrity warnings present"
    return
  fi

  if [[ "${integrity_result}" == "PASS" ]]; then
    echo "[PASS] Integrity verification passed"
    return
  fi

  log_warn "Integrity verification result was not parseable"
  mark_warn "integrity result unreadable"
}

run_operational_preflight() {
  print_section "OPERATIONAL PREFLIGHT"
  local preflight_script="${ROOT_DIR}/scripts/operational_preflight.py"
  if [[ ! -f "${preflight_script}" ]]; then
    mark_fail "Operational preflight script is missing: ${preflight_script}"
    return
  fi
  if [[ -z "${PYTHON_EXEC}" ]]; then
    mark_fail "No supported Python interpreter found for operational preflight"
    return
  fi

  local output=""
  local exit_code=0
  local preflight_result=""

  set +e
  output="$(cd "${ROOT_DIR}" && PYTHONDONTWRITEBYTECODE=1 "${PYTHON_EXEC}" "${preflight_script}" --require-services 2>&1)"
  exit_code=$?
  set -e

  if [[ -n "${output}" ]]; then
    printf '%s\n' "${output}" | sed '/^\[RESULT\] /d'
  fi

  preflight_result="$(printf '%s\n' "${output}" | awk '/^\[RESULT\] / {print $2}' | tail -n 1)"
  if [[ ${exit_code} -ne 0 || "${preflight_result}" == "FAIL" ]]; then
    mark_fail "Operational preflight failed"
    return
  fi

  if [[ "${preflight_result}" == "WARN" ]]; then
    mark_warn "operational preflight warnings present"
    return
  fi

  if [[ "${preflight_result}" == "PASS" ]]; then
    echo "[PASS] Operational preflight passed"
    return
  fi

  log_warn "Operational preflight result was not parseable"
  mark_warn "operational preflight result unreadable"
}

run_ui_smoke() {
  print_section "UI SMOKE"

  if [[ "${DASHBOARD_UNREACHABLE}" -eq 1 ]]; then
    echo "[SKIP] UI smoke skipped because dashboard/proxy is unreachable"
    return
  fi

  local smoke_script="${ROOT_DIR}/scripts/run_ui_smoke.sh"
  local playwright_cli="${ROOT_DIR}/node_modules/@playwright/test/cli.js"

  if [[ ! -f "${smoke_script}" ]]; then
    echo "[SKIP] UI smoke (script not available)"
    mark_warn "ui smoke skipped"
    return
  fi

  if ! command -v node >/dev/null 2>&1; then
    echo "[SKIP] UI smoke (node not installed)"
    mark_warn "ui smoke skipped"
    return
  fi

  if [[ ! -f "${playwright_cli}" ]]; then
    echo "[SKIP] UI smoke (Playwright not installed)"
    mark_warn "ui smoke skipped"
    return
  fi

  local output=""
  local exit_code=0

  set +e
  output="$(bash "${smoke_script}" 2>&1)"
  exit_code=$?
  set -e

  printf '%s\n' "${output}"

  if [[ ${exit_code} -eq 0 ]]; then
    echo "[PASS] UI smoke passed"
    return
  fi

  log_warn "UI smoke failed"
  mark_warn "ui smoke failed"
}

run_validation
PYTHON_EXEC="$(resolve_python_bin || true)"
run_integrity
run_operational_preflight
run_ui_smoke

echo
END="$(date +%s)"
log_info "Completed in $((END - START))s"

if [[ "${OVERALL_RESULT}" == "WARN" ]]; then
  warn_reason="$(join_warn_reasons)"
  if [[ -n "${warn_reason}" ]]; then
    echo "[RESULT] WARN (${warn_reason})"
  else
    echo "[RESULT] WARN"
  fi
elif [[ "${OVERALL_RESULT}" == "FAIL" ]]; then
  echo "[RESULT] FAIL"
else
  echo "[RESULT] PASS"
fi

if [[ "${OVERALL_RESULT}" == "FAIL" ]]; then
  exit 1
fi
exit 0
