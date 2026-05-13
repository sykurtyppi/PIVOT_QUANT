#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

source "${ROOT_DIR}/scripts/_pybin.sh"

exec "${PYTHON_BIN}" "${ROOT_DIR}/server/live_event_collector.py"
