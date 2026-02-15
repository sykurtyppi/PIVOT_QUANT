#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.pivotquant.retrain"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}" "${HOME}/Library/LaunchAgents"

cat > "${PLIST_PATH}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>

  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${ROOT_DIR}/scripts/run_retrain_cycle.sh</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>

  <key>RunAtLoad</key>
  <false/>

  <!-- Retrain every 6 hours -->
  <key>StartInterval</key>
  <integer>21600</integer>

  <key>StandardOutPath</key>
  <string>${LOG_DIR}/retrain.launchd.out.log</string>

  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/retrain.launchd.err.log</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
</dict>
</plist>
PLIST

UID_NUM="$(id -u)"
TARGET="gui/${UID_NUM}/${LABEL}"

if launchctl print "${TARGET}" >/dev/null 2>&1; then
  echo "Existing ${LABEL} detected. Restarting with new plist..."
  launchctl bootout "gui/${UID_NUM}" "${PLIST_PATH}" >/dev/null 2>&1 || true
fi

launchctl bootstrap "gui/${UID_NUM}" "${PLIST_PATH}"
launchctl enable "${TARGET}"
launchctl kickstart -k "${TARGET}" >/dev/null 2>&1 || true

echo "Installed ${LABEL}"
echo "Plist: ${PLIST_PATH}"
echo "Use:"
echo "  launchctl print ${TARGET}"
echo "  tail -f ${LOG_DIR}/retrain.log ${LOG_DIR}/retrain.launchd.err.log"
