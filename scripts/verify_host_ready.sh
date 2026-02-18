#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

LABEL_DASH="com.pivotquant.dashboard"
LABEL_RETRAIN="com.pivotquant.retrain"
LABEL_DAILY="com.pivotquant.daily_report"
LABEL_HEALTH_ALERT="com.pivotquant.health_alert"
LABEL_BACKUP="com.pivotquant.nightly_backup"
LABEL_RESTORE_DRILL="com.pivotquant.restore_drill"
LABEL_HOST_HEALTH="com.pivotquant.host_health"
UID_NUM="$(id -u)"
TARGET_DASH="gui/${UID_NUM}/${LABEL_DASH}"
TARGET_RETRAIN="gui/${UID_NUM}/${LABEL_RETRAIN}"
TARGET_DAILY="gui/${UID_NUM}/${LABEL_DAILY}"
TARGET_HEALTH_ALERT="gui/${UID_NUM}/${LABEL_HEALTH_ALERT}"
TARGET_BACKUP="gui/${UID_NUM}/${LABEL_BACKUP}"
TARGET_RESTORE_DRILL="gui/${UID_NUM}/${LABEL_RESTORE_DRILL}"
TARGET_HOST_HEALTH="gui/${UID_NUM}/${LABEL_HOST_HEALTH}"

ok() { printf '[OK] %s\n' "$*"; }
warn() { printf '[WARN] %s\n' "$*"; }
err() { printf '[ERR] %s\n' "$*"; }

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

check_launch_agent() {
  local target="$1" label="$2"
  if launchctl print "${target}" >/dev/null 2>&1; then
    ok "LaunchAgent loaded: ${label}"
  else
    warn "LaunchAgent not loaded: ${label}"
  fi
}

check_listen() {
  local port="$1" expected="$2"
  local lines binds
  lines="$(lsof -nP -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -z "${lines}" ]]; then
    err "Nothing listening on TCP ${port}"
    return
  fi
  binds="$(printf '%s\n' "${lines}" | awk 'NR>1{print $9}' | sort -u)"
  if printf '%s\n' "${binds}" | grep -qE "0\\.0\\.0\\.0:${port}|\\*:${port}|\\[::\\]:${port}"; then
    ok "Port ${port} listening on all interfaces"
  elif printf '%s\n' "${binds}" | grep -q "127.0.0.1:${port}"; then
    warn "Port ${port} is loopback-only (127.0.0.1). Remote Mac cannot reach it."
  else
    warn "Port ${port} listening on unexpected bind: ${binds}"
  fi
  if [[ -n "${expected}" ]] && ! printf '%s\n' "${lines}" | grep -q "${expected}"; then
    warn "Port ${port} listener does not appear to be ${expected}"
  fi
}

check_http_root() {
  local url="$1"
  if curl -fsS --max-time 3 -o /dev/null "${url}"; then
    ok "HTTP reachable: ${url}"
  else
    err "HTTP failed: ${url}"
  fi
}

check_http_json_field() {
  local url="$1" field="$2"
  local body
  body="$(curl -fsS --max-time 3 "${url}" 2>/dev/null || true)"
  if [[ -z "${body}" ]]; then
    err "HTTP failed: ${url}"
    return
  fi
  if printf '%s' "${body}" | grep -q "\"${field}\""; then
    ok "Endpoint healthy: ${url}"
  else
    warn "Endpoint responded but '${field}' field not found: ${url}"
  fi
}

echo "=== PivotQuant Host Readiness Check ==="
echo

HOSTNAME_LOCAL="$(scutil --get LocalHostName 2>/dev/null || true)"
LAN_IP="$(detect_lan_ip || true)"
CONSOLE_USER="$(stat -f%Su /dev/console 2>/dev/null || true)"

echo "Host: $(hostname)"
echo "Console user: ${CONSOLE_USER:-unknown}"
echo "LocalHostName: ${HOSTNAME_LOCAL:-unknown}"
echo "LAN IP: ${LAN_IP:-unknown}"
echo

echo "LaunchAgents:"
check_launch_agent "${TARGET_DASH}" "${LABEL_DASH}"
check_launch_agent "${TARGET_RETRAIN}" "${LABEL_RETRAIN}"
check_launch_agent "${TARGET_DAILY}" "${LABEL_DAILY}"
check_launch_agent "${TARGET_HEALTH_ALERT}" "${LABEL_HEALTH_ALERT}"
check_launch_agent "${TARGET_BACKUP}" "${LABEL_BACKUP}"
check_launch_agent "${TARGET_RESTORE_DRILL}" "${LABEL_RESTORE_DRILL}"
check_launch_agent "${TARGET_HOST_HEALTH}" "${LABEL_HOST_HEALTH}"
echo

echo "Ports:"
check_listen 3000 "node"
check_listen 5003 "python"
check_listen 5002 "python"
check_listen 5001 "python"
check_listen 5004 "python"
echo

echo "HTTP checks:"
check_http_root "http://127.0.0.1:3000/"
check_http_json_field "http://127.0.0.1:5003/health" "status"
check_http_json_field "http://127.0.0.1:5004/health" "status"
if [[ -n "${LAN_IP:-}" ]]; then
  check_http_root "http://${LAN_IP}:3000/"
fi
echo

echo "Power/awake state:"
if pgrep -fl "caffeinate.*server/run_all.sh" >/dev/null 2>&1; then
  ok "caffeinate process found for service stack"
else
  warn "No matching caffeinate process found"
fi
echo

echo "Firewall:"
if /usr/libexec/ApplicationFirewall/socketfilterfw --getglobalstate >/tmp/pq_fw_state.$$ 2>/dev/null; then
  FW_STATE="$(cat /tmp/pq_fw_state.$$)"
  rm -f /tmp/pq_fw_state.$$
  echo "${FW_STATE}"
  warn "If remote access fails, allow incoming for node and python3 in Firewall settings."
else
  warn "Could not read firewall state (permission-restricted)."
fi
echo

echo "Open from MacBook Pro:"
if [[ -n "${LAN_IP:-}" ]]; then
  echo "  http://${LAN_IP}:3000"
fi
if [[ -n "${HOSTNAME_LOCAL:-}" ]]; then
  echo "  http://${HOSTNAME_LOCAL}.local:3000"
fi
