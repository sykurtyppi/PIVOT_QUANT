#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="${ROOT_DIR}/.venv/bin/python3"

if [ -x "${VENV_PY}" ]; then
  exec "${VENV_PY}" "${ROOT_DIR}/server/live_event_collector.py"
fi

if command -v python3 >/dev/null 2>&1; then
  exec python3 "${ROOT_DIR}/server/live_event_collector.py"
fi

echo "python3 not found. Please install Python 3 and try again." >&2
exit 1
