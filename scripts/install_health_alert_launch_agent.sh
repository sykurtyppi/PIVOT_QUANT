#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.pivotquant.health_alert"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="${ROOT_DIR}/logs"
UID_NUM="$(id -u)"
TARGET="gui/${UID_NUM}/${LABEL}"
CHECK_INTERVAL_SEC="${ML_ALERT_CHECK_INTERVAL_SEC:-60}"

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
    <string>${ROOT_DIR}/scripts/run_health_alert_watchdog.sh</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>

  <key>RunAtLoad</key>
  <true/>

  <key>StartInterval</key>
  <integer>${CHECK_INTERVAL_SEC}</integer>

  <key>StandardOutPath</key>
  <string>${LOG_DIR}/health_alert.launchd.out.log</string>

  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/health_alert.launchd.err.log</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
</dict>
</plist>
PLIST

chmod 644 "${PLIST_PATH}"
chmod 755 "${ROOT_DIR}/scripts/run_health_alert_watchdog.sh"
xattr -dr com.apple.quarantine "${PLIST_PATH}" "${ROOT_DIR}" >/dev/null 2>&1 || true
plutil -lint "${PLIST_PATH}" >/dev/null

launchctl bootout "${TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/${UID_NUM}" "${PLIST_PATH}" >/dev/null 2>&1 || true
launchctl remove "${LABEL}" >/dev/null 2>&1 || true
launchctl disable "${TARGET}" >/dev/null 2>&1 || true
launchctl enable "${TARGET}" >/dev/null 2>&1 || true

if ! launchctl bootstrap "gui/${UID_NUM}" "${PLIST_PATH}" >/dev/null 2>&1; then
  echo "First health alert bootstrap failed; retrying..."
  launchctl bootout "${TARGET}" >/dev/null 2>&1 || true
  launchctl bootout "gui/${UID_NUM}" "${PLIST_PATH}" >/dev/null 2>&1 || true
  launchctl remove "${LABEL}" >/dev/null 2>&1 || true
  launchctl enable "${TARGET}" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/${UID_NUM}" "${PLIST_PATH}"
fi

launchctl kickstart -k "${TARGET}" >/dev/null 2>&1 || true

if ! launchctl print "${TARGET}" >/dev/null 2>&1; then
  echo "[ERROR] LaunchAgent did not load: ${TARGET}" >&2
  exit 1
fi

echo "Installed ${LABEL}"
echo "Plist: ${PLIST_PATH}"
echo "Use:"
echo "  launchctl print ${TARGET}"
echo "  tail -f ${LOG_DIR}/health_alert.log ${LOG_DIR}/health_alert.launchd.err.log"

