#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
LOCK_DIR="${LOG_DIR}/run_all.lock"
mkdir -p "${LOG_DIR}"

PIDS=()
OPTIONAL_PIDS=()
STARTUP_TIMEOUT_SEC="${STARTUP_TIMEOUT_SEC:-120}"
MONITOR_INTERVAL_SEC="${MONITOR_INTERVAL_SEC:-20}"
PIVOT_DB="${PIVOT_DB:-${ROOT_DIR}/data/pivot_events.sqlite}"
LIVE_COLLECTOR_ENABLED="${LIVE_COLLECTOR_ENABLED:-1}"
LIVE_COLLECTOR_ACTIVE=0
CLEANUP_DONE=0
LOCK_OWNED=0

is_listening() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  return 1
}

resolve_python() {
  if [[ -x "${ROOT_DIR}/.venv/bin/python3" ]]; then
    printf '%s' "${ROOT_DIR}/.venv/bin/python3"
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi
  return 1
}

acquire_lock() {
  local existing_pid=""
  while true; do
    if mkdir "${LOCK_DIR}" 2>/dev/null; then
      printf '%s\n' "$$" > "${LOCK_DIR}/pid"
      LOCK_OWNED=1
      return 0
    fi

    existing_pid=""
    if [[ -f "${LOCK_DIR}/pid" ]]; then
      existing_pid="$(cat "${LOCK_DIR}/pid" 2>/dev/null || true)"
    fi

    if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" >/dev/null 2>&1; then
      echo "Another stack supervisor is already running (pid ${existing_pid}). Waiting..."
      while kill -0 "${existing_pid}" >/dev/null 2>&1; do
        sleep "${MONITOR_INTERVAL_SEC}"
      done
      continue
    fi

    rm -rf "${LOCK_DIR}" 2>/dev/null || true
  done
}

release_lock() {
  if [[ "${LOCK_OWNED}" -ne 1 ]]; then
    return 0
  fi
  if [[ -d "${LOCK_DIR}" ]]; then
    rm -rf "${LOCK_DIR}" 2>/dev/null || true
  fi
  LOCK_OWNED=0
}

cleanup() {
  if [[ "${CLEANUP_DONE}" -eq 1 ]]; then
    return 0
  fi
  CLEANUP_DONE=1

  if [[ "${#PIDS[@]}" -gt 0 ]]; then
    echo "Stopping managed services..."
  fi
  for pid in "${PIDS[@]}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done
  release_lock
}

on_exit() {
  cleanup
}

on_signal() {
  cleanup
  exit 0
}

trap on_exit EXIT
trap on_signal INT TERM

die() {
  local message="$1"
  echo "[ERROR] ${message}"
  exit 1
}

is_truthy() {
  local value="${1:-}"
  case "${value}" in
    1|[Tt][Rr][Uu][Ee]|[Yy][Ee][Ss]|[Oo][Nn])
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

start_service() {
  local name="$1"
  local port="$2"
  local optional="${3:-false}"
  shift 3
  if [[ -n "${port}" ]] && is_listening "${port}"; then
    echo "${name} already running on port ${port}. Skipping."
    return 0
  fi
  echo "Starting ${name}..."
  "$@" > "${LOG_DIR}/${name}.log" 2>&1 &
  local pid=$!
  PIDS+=("${pid}")
  if [[ "${optional}" == "true" ]]; then
    OPTIONAL_PIDS+=("${pid}")
  fi
  if [[ -n "${port}" ]]; then
    echo "${name} PID ${pid} (port ${port}, log: ${LOG_DIR}/${name}.log)"
  else
    echo "${name} PID ${pid} (log: ${LOG_DIR}/${name}.log)"
  fi
}

is_optional_pid() {
  local pid="$1"
  local candidate
  for candidate in "${OPTIONAL_PIDS[@]}"; do
    if [[ "${candidate}" == "${pid}" ]]; then
      return 0
    fi
  done
  return 1
}

remove_pid() {
  local pid="$1"
  local next_pids=()
  local next_optional=()
  local candidate

  for candidate in "${PIDS[@]}"; do
    if [[ "${candidate}" != "${pid}" ]]; then
      next_pids+=("${candidate}")
    fi
  done
  for candidate in "${OPTIONAL_PIDS[@]}"; do
    if [[ "${candidate}" != "${pid}" ]]; then
      next_optional+=("${candidate}")
    fi
  done

  PIDS=("${next_pids[@]}")
  OPTIONAL_PIDS=("${next_optional[@]}")
}

wait_for_port() {
  local port="$1"
  local timeout="${2:-$STARTUP_TIMEOUT_SEC}"
  local elapsed=0

  while [[ "${elapsed}" -lt "${timeout}" ]]; do
    if is_listening "${port}"; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  return 1
}

wait_for_http() {
  local url="$1"
  local timeout="${2:-$STARTUP_TIMEOUT_SEC}"
  local elapsed=0

  while [[ "${elapsed}" -lt "${timeout}" ]]; do
    if curl -fsS --max-time 2 "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  return 1
}

verify_service() {
  local name="$1"
  local port="$2"
  local health_url="${3:-}"

  if ! wait_for_port "${port}"; then
    die "${name} failed to bind to port ${port} within ${STARTUP_TIMEOUT_SEC}s. Check ${LOG_DIR}/${name}.log"
  fi

  if [[ -n "${health_url}" ]] && ! wait_for_http "${health_url}"; then
    die "${name} did not pass health check (${health_url}) within ${STARTUP_TIMEOUT_SEC}s. Check ${LOG_DIR}/${name}.log"
  fi

  echo "${name} ready on port ${port}"
}

quick_check_service() {
  local name="$1"
  local port="$2"
  local health_url="${3:-}"

  if ! is_listening "${port}"; then
    echo "[monitor] ${name} is not listening on ${port}"
    return 1
  fi

  if [[ -n "${health_url}" ]] && ! curl -fsS --max-time 2 "${health_url}" >/dev/null 2>&1; then
    echo "[monitor] ${name} health failed: ${health_url}"
    return 1
  fi

  return 0
}

quick_check_live_collector() {
  local url="http://127.0.0.1:5004/health"
  local body=""
  if ! body="$(curl -fsS --max-time 2 "${url}" 2>/dev/null)"; then
    echo "[monitor] live_collector health fetch failed: ${url}"
    return 1
  fi

  if printf '%s' "${body}" | grep -Eq '"status"[[:space:]]*:[[:space:]]*"(ok|starting)"'; then
    return 0
  fi

  echo "[monitor] live_collector unhealthy payload: ${body}"
  return 1
}

monitor_stack() {
  if [[ "${#PIDS[@]}" -eq 0 ]]; then
    echo "No new services were started by this supervisor. Monitoring existing stack (${MONITOR_INTERVAL_SEC}s interval)."
  else
    echo "Monitoring stack health (${MONITOR_INTERVAL_SEC}s interval)."
  fi

  while true; do
    quick_check_service "event_writer" "5002" "http://127.0.0.1:5002/health" || die "event_writer health check failed"
    quick_check_service "gamma_bridge" "5001" || echo "[WARN] gamma_bridge not responding on 5001 (IBKR may be offline)"
    quick_check_service "ml_server" "5003" "http://127.0.0.1:5003/health" || die "ml_server health check failed"
    quick_check_service "dashboard" "3000" "http://127.0.0.1:3000/" || die "dashboard health check failed"
    if [[ "${LIVE_COLLECTOR_ACTIVE}" -eq 1 ]]; then
      quick_check_service "live_collector" "5004" "http://127.0.0.1:5004/health" || die "live_collector health check failed"
      quick_check_live_collector || echo "[WARN] live_collector status degraded"
    fi

    for pid in "${PIDS[@]}"; do
      if ! kill -0 "${pid}" >/dev/null 2>&1; then
        if is_optional_pid "${pid}"; then
          echo "[WARN] Optional managed service exited (pid ${pid}). Continuing."
          remove_pid "${pid}"
          continue
        fi
        die "A managed service process exited unexpectedly (pid ${pid})"
      fi
    done

    sleep "${MONITOR_INTERVAL_SEC}"
  done
}

run_db_migrations() {
  local py
  if ! py="$(resolve_python)"; then
    die "No python3 interpreter available for DB migrations."
  fi

  echo "Running DB migrations on ${PIVOT_DB}..."
  if ! "${py}" "${ROOT_DIR}/scripts/migrate_db.py" --db "${PIVOT_DB}" >> "${LOG_DIR}/db_migrate.log" 2>&1; then
    die "DB migration failed. Check ${LOG_DIR}/db_migrate.log"
  fi
}

cd "${ROOT_DIR}"
acquire_lock
run_db_migrations

if is_truthy "${LIVE_COLLECTOR_ENABLED}"; then
  LIVE_COLLECTOR_ACTIVE=1
fi

start_service "event_writer" "5002" "false" bash server/run_event_writer.sh
start_service "gamma_bridge" "5001" "true" bash server/run_gamma_bridge.sh
start_service "ml_server" "5003" "false" bash server/run_ml_server.sh
start_service "dashboard" "3000" "false" node server/yahoo_proxy.js

verify_service "event_writer" "5002" "http://127.0.0.1:5002/health"
# gamma_bridge is optional — depends on IBKR Gateway which may not run
# on weekends/holidays. Dashboard falls back to Yahoo automatically.
if ! wait_for_port "5001"; then
  echo "[WARN] gamma_bridge did not start (IBKR Gateway may be offline). Continuing without it."
else
  echo "gamma_bridge ready on port 5001"
fi
verify_service "ml_server" "5003" "http://127.0.0.1:5003/health"
verify_service "dashboard" "3000" "http://127.0.0.1:3000/"
if [[ "${LIVE_COLLECTOR_ACTIVE}" -eq 1 ]]; then
  # Start collector only after core services are confirmed ready to avoid first-cycle score race.
  start_service "live_collector" "5004" "false" bash server/run_live_collector.sh
  verify_service "live_collector" "5004" "http://127.0.0.1:5004/health"
  quick_check_live_collector || echo "[WARN] live_collector reported degraded status at startup"
else
  echo "[WARN] live_collector disabled via LIVE_COLLECTOR_ENABLED=${LIVE_COLLECTOR_ENABLED}"
fi

echo "All services ready."
echo "Press Ctrl+C to stop."
monitor_stack
