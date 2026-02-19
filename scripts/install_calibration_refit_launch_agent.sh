#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.pivotquant.calibration_refit"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="${ROOT_DIR}/logs"
UID_NUM="$(id -u)"
TARGET="gui/${UID_NUM}/${LABEL}"

CALIB_REFIT_HOUR="${CALIB_REFIT_HOUR:-23}"
CALIB_REFIT_MINUTE="${CALIB_REFIT_MINUTE:-45}"
CALIB_REFIT_WEEKDAYS="${CALIB_REFIT_WEEKDAYS:-1,2,3,4,5}"
CALIB_REFIT_KICKSTART_NOW="${CALIB_REFIT_KICKSTART_NOW:-false}"

mkdir -p "${LOG_DIR}" "${HOME}/Library/LaunchAgents"

is_truthy() {
  local raw="${1:-}"
  local lowered
  lowered="$(printf '%s' "${raw}" | tr '[:upper:]' '[:lower:]')"
  case "${lowered}" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

build_schedule_xml() {
  local csv="$1"
  local hour="$2"
  local minute="$3"
  local IFS=','
  local entry=""
  local weekday=""
  local output=""

  for entry in ${csv}; do
    weekday="$(printf '%s' "${entry}" | tr -d '[:space:]')"
    if [[ -z "${weekday}" ]]; then
      continue
    fi
    if [[ ! "${weekday}" =~ ^[0-7]$ ]]; then
      echo "[ERROR] Invalid CALIB_REFIT_WEEKDAYS value '${weekday}' (expected 0-7)." >&2
      exit 1
    fi
    output+="    <dict>\n"
    output+="      <key>Weekday</key><integer>${weekday}</integer>\n"
    output+="      <key>Hour</key><integer>${hour}</integer>\n"
    output+="      <key>Minute</key><integer>${minute}</integer>\n"
    output+="    </dict>\n"
  done

  if [[ -z "${output}" ]]; then
    echo "[ERROR] CALIB_REFIT_WEEKDAYS produced no schedule entries." >&2
    exit 1
  fi

  printf '%b' "${output}"
}

SCHEDULE_XML="$(build_schedule_xml "${CALIB_REFIT_WEEKDAYS}" "${CALIB_REFIT_HOUR}" "${CALIB_REFIT_MINUTE}")"

{
  cat <<PLIST_HEADER
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>

  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${ROOT_DIR}/scripts/run_calibration_refit.sh</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>

  <key>RunAtLoad</key>
  <false/>

  <key>StartCalendarInterval</key>
  <array>
PLIST_HEADER
  printf '%s' "${SCHEDULE_XML}"
  cat <<PLIST_FOOTER
  </array>

  <key>StandardOutPath</key>
  <string>${LOG_DIR}/calibration_refit.launchd.out.log</string>

  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/calibration_refit.launchd.err.log</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
</dict>
</plist>
PLIST_FOOTER
} > "${PLIST_PATH}"

chmod 644 "${PLIST_PATH}"
chmod 755 "${ROOT_DIR}/scripts/run_calibration_refit.sh"
chmod 755 "${ROOT_DIR}/scripts/refit_calibration.py"
xattr -dr com.apple.quarantine "${PLIST_PATH}" "${ROOT_DIR}" >/dev/null 2>&1 || true
plutil -lint "${PLIST_PATH}" >/dev/null

launchctl bootout "${TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/${UID_NUM}" "${PLIST_PATH}" >/dev/null 2>&1 || true
launchctl remove "${LABEL}" >/dev/null 2>&1 || true
launchctl disable "${TARGET}" >/dev/null 2>&1 || true
launchctl enable "${TARGET}" >/dev/null 2>&1 || true

if ! launchctl bootstrap "gui/${UID_NUM}" "${PLIST_PATH}" >/dev/null 2>&1; then
  echo "First calibration_refit bootstrap failed; retrying..."
  launchctl bootout "${TARGET}" >/dev/null 2>&1 || true
  launchctl bootout "gui/${UID_NUM}" "${PLIST_PATH}" >/dev/null 2>&1 || true
  launchctl remove "${LABEL}" >/dev/null 2>&1 || true
  launchctl enable "${TARGET}" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/${UID_NUM}" "${PLIST_PATH}"
fi

if is_truthy "${CALIB_REFIT_KICKSTART_NOW}"; then
  launchctl kickstart -k "${TARGET}" >/dev/null 2>&1 || true
fi

if ! launchctl print "${TARGET}" >/dev/null 2>&1; then
  echo "[ERROR] LaunchAgent did not load: ${TARGET}" >&2
  exit 1
fi

echo "Installed ${LABEL}"
echo "Plist: ${PLIST_PATH}"
echo "Schedule: weekdays ${CALIB_REFIT_WEEKDAYS} at ${CALIB_REFIT_HOUR}:$(printf '%02d' "${CALIB_REFIT_MINUTE}")"
echo "Use:"
echo "  launchctl print ${TARGET}"
echo "  tail -f ${LOG_DIR}/calibration_refit.log ${LOG_DIR}/calibration_refit.launchd.err.log"
if ! is_truthy "${CALIB_REFIT_KICKSTART_NOW}"; then
  echo "Note: calibration refit agent is installed without immediate kickstart; it runs on schedule."
fi
