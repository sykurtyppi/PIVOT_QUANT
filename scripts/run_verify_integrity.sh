#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

resolve_python_bin() {
  if [[ -n "${PYTHON_BIN:-}" && -x "${PYTHON_BIN}" ]]; then
    echo "${PYTHON_BIN}"
    return 0
  fi

  local candidates=(
    "${ROOT_DIR}/.venv313/bin/python"
    "${ROOT_DIR}/.venv313/bin/python3"
    "${ROOT_DIR}/.venv/bin/python"
    "${ROOT_DIR}/.venv/bin/python3"
  )

  local candidate=""
  for candidate in "${candidates[@]}"; do
    if [[ -x "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  done

  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi

  echo "[FAIL] No supported Python interpreter found for verify_release_integrity.py" >&2
  return 1
}

PYTHON_EXEC="$(resolve_python_bin)"
PYTHONDONTWRITEBYTECODE=1 "${PYTHON_EXEC}" "${ROOT_DIR}/scripts/verify_release_integrity.py"
