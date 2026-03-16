#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"
ENV_FILE="${ROOT_DIR}/.env"

load_env_file() {
  local env_path="$1"
  local raw line key value
  [[ -f "${env_path}" ]] || return 0

  while IFS= read -r raw || [[ -n "${raw}" ]]; do
    line="${raw#"${raw%%[![:space:]]*}"}"
    [[ -z "${line}" ]] && continue
    [[ "${line:0:1}" == "#" ]] && continue
    [[ "${line}" == "export "* ]] && line="${line#export }"
    [[ "${line}" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue

    key="${line%%=*}"
    value="${line#*=}"
    # Accept quoted .env values (single/double) without exporting quote chars.
    if [[ "${#value}" -ge 2 ]]; then
      if [[ "${value:0:1}" == "\"" && "${value: -1}" == "\"" ]]; then
        value="${value:1:${#value}-2}"
      elif [[ "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
        value="${value:1:${#value}-2}"
      fi
    fi
    export "${key}=${value}"
  done < "${env_path}"
}

# Load .env with strict key=value parsing (non-executing) to avoid launchd
# crashes from malformed prose/comment lines.
load_env_file "${ENV_FILE}"

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
export STARTUP_TIMEOUT_SEC="${STARTUP_TIMEOUT_SEC:-120}"
export LIVE_COLLECTOR_ENABLED="${LIVE_COLLECTOR_ENABLED:-1}"
export LIVE_COLLECTOR_BIND="${LIVE_COLLECTOR_BIND:-127.0.0.1}"
export LIVE_COLLECTOR_PORT="${LIVE_COLLECTOR_PORT:-5004}"
export LIVE_COLLECTOR_SYMBOLS="${LIVE_COLLECTOR_SYMBOLS:-SPY}"
export LIVE_COLLECTOR_RANGE="${LIVE_COLLECTOR_RANGE:-2d}"
export LIVE_COLLECTOR_SOURCE="${LIVE_COLLECTOR_SOURCE:-yahoo}"
export LIVE_COLLECTOR_POLL_SEC="${LIVE_COLLECTOR_POLL_SEC:-45}"
export LIVE_COLLECTOR_SCORE_ENABLED="${LIVE_COLLECTOR_SCORE_ENABLED:-1}"

# Keep internal writer/bridge private unless explicitly overridden.
export EVENT_WRITER_BIND="${EVENT_WRITER_BIND:-127.0.0.1}"
export IB_BRIDGE_BIND="${IB_BRIDGE_BIND:-127.0.0.1}"

is_truthy() {
  local value="${1:-}"
  case "${value,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

is_loopback_bind() {
  local host_value="${1:-}"
  case "${host_value,,}" in
    localhost|127.0.0.1|::1|127.*) return 0 ;;
    *) return 1 ;;
  esac
}

if is_truthy "${DASH_AUTH_ENABLED:-false}"; then
  DASH_AUTH_PASSWORD="${DASH_AUTH_PASSWORD:-${DASH_AUTH_PASS:-}}"
  DASH_AUTH_MIN_PASSWORD_LEN="${DASH_AUTH_MIN_PASSWORD_LEN:-20}"
  DASH_AUTH_ENFORCE_STRONG_PASSWORD="${DASH_AUTH_ENFORCE_STRONG_PASSWORD:-true}"

  if is_truthy "${DASH_AUTH_ENFORCE_STRONG_PASSWORD}"; then
    if [[ "${#DASH_AUTH_PASSWORD}" -lt "${DASH_AUTH_MIN_PASSWORD_LEN}" ]]; then
      echo "[run_persistent_stack] ERROR: DASH_AUTH_PASSWORD length (${#DASH_AUTH_PASSWORD}) must be >= ${DASH_AUTH_MIN_PASSWORD_LEN} when DASH_AUTH_ENFORCE_STRONG_PASSWORD=true."
      exit 1
    fi
  fi

  DASH_AUTH_LOCAL_BYPASS="${DASH_AUTH_LOCAL_BYPASS:-}"
  if [[ -z "${DASH_AUTH_LOCAL_BYPASS}" ]]; then
    if is_loopback_bind "${HOST}"; then
      DASH_AUTH_LOCAL_BYPASS="true"
    else
      DASH_AUTH_LOCAL_BYPASS="false"
    fi
  fi

  if ! is_loopback_bind "${HOST}" && is_truthy "${DASH_AUTH_LOCAL_BYPASS}"; then
    echo "[run_persistent_stack] ERROR: DASH_AUTH_LOCAL_BYPASS=true is not allowed when HOST is non-loopback (${HOST})."
    exit 1
  fi
fi

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
echo "[run_persistent_stack] Live collector: enabled=${LIVE_COLLECTOR_ENABLED} bind=${LIVE_COLLECTOR_BIND}:${LIVE_COLLECTOR_PORT} symbols=${LIVE_COLLECTOR_SYMBOLS} range=${LIVE_COLLECTOR_RANGE} source=${LIVE_COLLECTOR_SOURCE} poll=${LIVE_COLLECTOR_POLL_SEC}s score_enabled=${LIVE_COLLECTOR_SCORE_ENABLED}"
echo "[run_persistent_stack] Startup timeout: ${STARTUP_TIMEOUT_SEC}s"
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
