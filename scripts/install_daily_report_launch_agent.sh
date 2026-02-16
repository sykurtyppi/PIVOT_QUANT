#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.pivotquant.daily_report"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="${ROOT_DIR}/logs"
UID_NUM="$(id -u)"
TARGET="gui/${UID_NUM}/${LABEL}"
SCHEDULE_MODE="${1:-${ML_REPORT_SCHEDULE_MODE:-close}}"
CLOSE_HOUR="${ML_REPORT_CLOSE_HOUR:-17}"
CLOSE_MINUTE="${ML_REPORT_CLOSE_MINUTE:-10}"
MORNING_HOUR="${ML_REPORT_MORNING_HOUR:-8}"
MORNING_MINUTE="${ML_REPORT_MORNING_MINUTE:-5}"

mkdir -p "${LOG_DIR}" "${HOME}/Library/LaunchAgents"

build_schedule_block() {
  case "${SCHEDULE_MODE}" in
    close)
      cat <<EOF
  <key>StartCalendarInterval</key>
  <array>
    <dict>
      <key>Weekday</key><integer>1</integer>
      <key>Hour</key><integer>${CLOSE_HOUR}</integer>
      <key>Minute</key><integer>${CLOSE_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>2</integer>
      <key>Hour</key><integer>${CLOSE_HOUR}</integer>
      <key>Minute</key><integer>${CLOSE_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>3</integer>
      <key>Hour</key><integer>${CLOSE_HOUR}</integer>
      <key>Minute</key><integer>${CLOSE_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>4</integer>
      <key>Hour</key><integer>${CLOSE_HOUR}</integer>
      <key>Minute</key><integer>${CLOSE_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>5</integer>
      <key>Hour</key><integer>${CLOSE_HOUR}</integer>
      <key>Minute</key><integer>${CLOSE_MINUTE}</integer>
    </dict>
  </array>
EOF
      ;;
    morning)
      cat <<EOF
  <key>StartCalendarInterval</key>
  <array>
    <dict>
      <key>Weekday</key><integer>1</integer>
      <key>Hour</key><integer>${MORNING_HOUR}</integer>
      <key>Minute</key><integer>${MORNING_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>2</integer>
      <key>Hour</key><integer>${MORNING_HOUR}</integer>
      <key>Minute</key><integer>${MORNING_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>3</integer>
      <key>Hour</key><integer>${MORNING_HOUR}</integer>
      <key>Minute</key><integer>${MORNING_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>4</integer>
      <key>Hour</key><integer>${MORNING_HOUR}</integer>
      <key>Minute</key><integer>${MORNING_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>5</integer>
      <key>Hour</key><integer>${MORNING_HOUR}</integer>
      <key>Minute</key><integer>${MORNING_MINUTE}</integer>
    </dict>
  </array>
EOF
      ;;
    both)
      cat <<EOF
  <key>StartCalendarInterval</key>
  <array>
    <dict>
      <key>Weekday</key><integer>1</integer>
      <key>Hour</key><integer>${MORNING_HOUR}</integer>
      <key>Minute</key><integer>${MORNING_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>2</integer>
      <key>Hour</key><integer>${MORNING_HOUR}</integer>
      <key>Minute</key><integer>${MORNING_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>3</integer>
      <key>Hour</key><integer>${MORNING_HOUR}</integer>
      <key>Minute</key><integer>${MORNING_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>4</integer>
      <key>Hour</key><integer>${MORNING_HOUR}</integer>
      <key>Minute</key><integer>${MORNING_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>5</integer>
      <key>Hour</key><integer>${MORNING_HOUR}</integer>
      <key>Minute</key><integer>${MORNING_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>1</integer>
      <key>Hour</key><integer>${CLOSE_HOUR}</integer>
      <key>Minute</key><integer>${CLOSE_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>2</integer>
      <key>Hour</key><integer>${CLOSE_HOUR}</integer>
      <key>Minute</key><integer>${CLOSE_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>3</integer>
      <key>Hour</key><integer>${CLOSE_HOUR}</integer>
      <key>Minute</key><integer>${CLOSE_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>4</integer>
      <key>Hour</key><integer>${CLOSE_HOUR}</integer>
      <key>Minute</key><integer>${CLOSE_MINUTE}</integer>
    </dict>
    <dict>
      <key>Weekday</key><integer>5</integer>
      <key>Hour</key><integer>${CLOSE_HOUR}</integer>
      <key>Minute</key><integer>${CLOSE_MINUTE}</integer>
    </dict>
  </array>
EOF
      ;;
    *)
      echo "[ERROR] Invalid schedule mode '${SCHEDULE_MODE}'. Use: close | morning | both" >&2
      exit 1
      ;;
  esac
}

SCHEDULE_BLOCK="$(build_schedule_block)"

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
    <string>${ROOT_DIR}/scripts/run_daily_report_send.sh</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>

  <key>RunAtLoad</key>
  <false/>

${SCHEDULE_BLOCK}

  <key>StandardOutPath</key>
  <string>${LOG_DIR}/daily_report.launchd.out.log</string>

  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/daily_report.launchd.err.log</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
</dict>
</plist>
PLIST

chmod 644 "${PLIST_PATH}"
chmod 755 "${ROOT_DIR}/scripts/run_daily_report_send.sh"
xattr -dr com.apple.quarantine "${PLIST_PATH}" "${ROOT_DIR}" >/dev/null 2>&1 || true
plutil -lint "${PLIST_PATH}" >/dev/null

launchctl bootout "${TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/${UID_NUM}" "${PLIST_PATH}" >/dev/null 2>&1 || true
launchctl remove "${LABEL}" >/dev/null 2>&1 || true
launchctl disable "${TARGET}" >/dev/null 2>&1 || true
launchctl enable "${TARGET}" >/dev/null 2>&1 || true

if ! launchctl bootstrap "gui/${UID_NUM}" "${PLIST_PATH}" >/dev/null 2>&1; then
  echo "First daily report bootstrap failed; retrying..."
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

echo "Installed ${LABEL} (mode=${SCHEDULE_MODE})"
echo "Plist: ${PLIST_PATH}"
echo "Use:"
echo "  launchctl print ${TARGET}"
echo "  tail -f ${LOG_DIR}/report_delivery.log ${LOG_DIR}/daily_report.launchd.err.log"
