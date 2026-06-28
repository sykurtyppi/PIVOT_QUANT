#!/usr/bin/env bash
# Install (or remove) the levels-product launchd agents:
#   com.pivotquant.levels_daily     — weekdays pre-open: morning post + track record
#   com.pivotquant.levels_intraday  — every 2 min: confluence alert poller (no-ops
#                                     outside RTH since touches are only logged then)
#
# Usage:
#   bash install_levels_product_agents.sh install     # (default)
#   bash install_levels_product_agents.sh uninstall
#
# Webhook delivery: set LEVELS_PRODUCT_WEBHOOK_URL in the environment the agents
# inherit (or edit the plists). Unset = dry-run to the logs.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LP_DIR="${ROOT_DIR}/scripts/levels_product"
LOG_DIR="${ROOT_DIR}/logs/levels_product"
LA_DIR="${HOME}/Library/LaunchAgents"
UID_NUM="$(id -u)"
ACTION="${1:-install}"
DAILY_HOUR="${LEVELS_DAILY_HOUR:-8}"
DAILY_MINUTE="${LEVELS_DAILY_MINUTE:-15}"
INTRADAY_INTERVAL="${LEVELS_INTRADAY_INTERVAL_SEC:-120}"
WEBHOOK="${LEVELS_PRODUCT_WEBHOOK_URL:-}"
mkdir -p "${LOG_DIR}" "${LA_DIR}"

# resolve a >=3.10 interpreter for the intraday agent (mirrors the daily script)
PY="${PYTHON_BIN:-}"
if [[ -z "${PY}" ]]; then
  for cand in "${ROOT_DIR}/.venv313/bin/python" "${ROOT_DIR}/.venv/bin/python"; do
    [[ -x "${cand}" ]] && { PY="${cand}"; break; }
  done
fi
[[ -z "${PY}" ]] && { echo "no project python found"; exit 1; }

xml_escape() {  # escape & < > " so an operator-set webhook can't inject plist XML/keys
  local s="$1"
  s="${s//&/&amp;}"; s="${s//</&lt;}"; s="${s//>/&gt;}"; s="${s//\"/&quot;}"
  printf '%s' "${s}"
}

daily_label="com.pivotquant.levels_daily"
intraday_label="com.pivotquant.levels_intraday"

unload() {
  local label="$1"
  launchctl bootout "gui/${UID_NUM}/${label}" 2>/dev/null || true
  rm -f "${LA_DIR}/${label}.plist"
}

if [[ "${ACTION}" == "uninstall" ]]; then
  unload "${daily_label}"; unload "${intraday_label}"
  echo "uninstalled levels-product agents"
  exit 0
fi

webhook_env_block() {
  [[ -z "${WEBHOOK}" ]] && return 0
  local esc; esc="$(xml_escape "${WEBHOOK}")"
  cat <<EOF
    <key>LEVELS_PRODUCT_WEBHOOK_URL</key><string>${esc}</string>
EOF
}

weekday_block() {  # hour/minute weekdays 1-5
  local hour="$1" minute="$2" out="" d
  for d in 1 2 3 4 5; do
    out+="    <dict><key>Weekday</key><integer>${d}</integer><key>Hour</key><integer>${hour}</integer><key>Minute</key><integer>${minute}</integer></dict>
"
  done
  printf '%s' "${out}"
}

# --- daily agent (weekday morning) ---
cat > "${LA_DIR}/${daily_label}.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>${daily_label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${LP_DIR}/run_levels_product_daily.sh</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
$(webhook_env_block)  </dict>
  <key>StartCalendarInterval</key>
  <array>
$(weekday_block "${DAILY_HOUR}" "${DAILY_MINUTE}")  </array>
  <key>StandardOutPath</key><string>${LOG_DIR}/launchd_daily.out</string>
  <key>StandardErrorPath</key><string>${LOG_DIR}/launchd_daily.err</string>
  <key>WorkingDirectory</key><string>${ROOT_DIR}</string>
</dict></plist>
EOF

# --- intraday agent (interval poller) ---
cat > "${LA_DIR}/${intraday_label}.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>${intraday_label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PY}</string>
    <string>${LP_DIR}/intraday_alert.py</string>
    <string>--symbol</string><string>SPY</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
$(webhook_env_block)  </dict>
  <key>StartInterval</key><integer>${INTRADAY_INTERVAL}</integer>
  <key>StandardOutPath</key><string>${LOG_DIR}/launchd_intraday.out</string>
  <key>StandardErrorPath</key><string>${LOG_DIR}/launchd_intraday.err</string>
  <key>WorkingDirectory</key><string>${ROOT_DIR}</string>
</dict></plist>
EOF

chmod 600 "${LA_DIR}/${daily_label}.plist" "${LA_DIR}/${intraday_label}.plist"  # plist holds the webhook secret
unload "${daily_label}"; unload "${intraday_label}"
launchctl bootstrap "gui/${UID_NUM}" "${LA_DIR}/${daily_label}.plist"
launchctl bootstrap "gui/${UID_NUM}" "${LA_DIR}/${intraday_label}.plist"
echo "installed:"
echo "  ${daily_label}     weekdays ${DAILY_HOUR}:$(printf '%02d' "${DAILY_MINUTE}") (morning post + track record)"
echo "  ${intraday_label}  every ${INTRADAY_INTERVAL}s (confluence alert poller)"
[[ -z "${WEBHOOK}" ]] && echo "  NOTE: LEVELS_PRODUCT_WEBHOOK_URL unset → agents dry-run to ${LOG_DIR}"
