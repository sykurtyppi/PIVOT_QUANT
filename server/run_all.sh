#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

PIDS=()

is_listening() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  return 1
}

cleanup() {
  echo "Stopping services..."
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done
  exit 0
}

trap cleanup INT TERM

start_service() {
  local name="$1"
  local port="$2"
  shift 2
  if [ -n "${port}" ] && is_listening "${port}"; then
    echo "${name} already running on port ${port}. Skipping."
    return 0
  fi
  echo "Starting ${name}..."
  "$@" > "${LOG_DIR}/${name}.log" 2>&1 &
  local pid=$!
  PIDS+=("${pid}")
  if [ -n "${port}" ]; then
    echo "${name} PID ${pid} (port ${port}, log: ${LOG_DIR}/${name}.log)"
  else
    echo "${name} PID ${pid} (log: ${LOG_DIR}/${name}.log)"
  fi
}

cd "${ROOT_DIR}"

start_service "event_writer" "5002" bash server/run_event_writer.sh
start_service "gamma_bridge" "5001" bash server/run_gamma_bridge.sh
start_service "ml_server" "5003" bash server/run_ml_server.sh
start_service "dashboard" "3000" node server/yahoo_proxy.js

echo "All services checked. Press Ctrl+C to stop."
wait
