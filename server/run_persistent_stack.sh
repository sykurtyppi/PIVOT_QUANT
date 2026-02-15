#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

detect_lan_ip() {
  local iface ip
  iface="$(route get default 2>/dev/null | awk '/interface:/{print $2; exit}')"
  if [[ -n "${iface:-}" ]]; then
    ip="$(ipconfig getifaddr "${iface}" 2>/dev/null || true)"
    if [[ -n "${ip:-}" ]]; then
      printf '%s' "${ip}"
      return 0
    fi
  fi

  for iface in en0 en1; do
    ip="$(ipconfig getifaddr "${iface}" 2>/dev/null || true)"
    if [[ -n "${ip:-}" ]]; then
      printf '%s' "${ip}"
      return 0
    fi
  done

  return 1
}

LOCAL_HOSTNAME="$(scutil --get LocalHostName 2>/dev/null || true)"
LAN_IP="$(detect_lan_ip || true)"

export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-3000}"
export ML_SERVER_BIND="${ML_SERVER_BIND:-0.0.0.0}"
export ML_SERVER_PORT="${ML_SERVER_PORT:-5003}"

# Keep internal writer/bridge private unless explicitly overridden.
export EVENT_WRITER_BIND="${EVENT_WRITER_BIND:-127.0.0.1}"
export IB_BRIDGE_BIND="${IB_BRIDGE_BIND:-127.0.0.1}"

if [[ -z "${ML_CORS_ORIGINS:-}" ]]; then
  origins=("http://localhost:3000" "http://127.0.0.1:3000")
  if [[ -n "${LOCAL_HOSTNAME}" ]]; then
    origins+=("http://${LOCAL_HOSTNAME}.local:3000")
  fi
  if [[ -n "${LAN_IP}" ]]; then
    origins+=("http://${LAN_IP}:3000")
  fi
  ML_CORS_ORIGINS="$(IFS=,; echo "${origins[*]}")"
fi
export ML_CORS_ORIGINS

echo "[run_persistent_stack] Root: ${ROOT_DIR}"
echo "[run_persistent_stack] Dashboard bind: ${HOST}:${PORT}"
echo "[run_persistent_stack] ML bind: ${ML_SERVER_BIND}:${ML_SERVER_PORT}"
if [[ -n "${LAN_IP}" ]]; then
  echo "[run_persistent_stack] LAN URL: http://${LAN_IP}:${PORT}"
fi
if [[ -n "${LOCAL_HOSTNAME}" ]]; then
  echo "[run_persistent_stack] mDNS URL: http://${LOCAL_HOSTNAME}.local:${PORT}"
fi
echo "[run_persistent_stack] ML_CORS_ORIGINS=${ML_CORS_ORIGINS}"

cd "${ROOT_DIR}"

# Keep the system awake for 24/7 operation while services run.
exec /usr/bin/caffeinate -ims /bin/bash server/run_all.sh
