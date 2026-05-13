#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Resolve a guaranteed >=3.10 Python via the shared helper.
# The helper sources PYTHON_BIN; aborts with a clear error if none found.
source "${ROOT_DIR}/scripts/_pybin.sh"

exec "${PYTHON_BIN}" "${ROOT_DIR}/server/ml_server.py"
