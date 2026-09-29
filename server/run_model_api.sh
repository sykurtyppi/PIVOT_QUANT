#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

resolve_python() {
  local candidate=""
  local candidates=()

  if [[ -n "${PYTHON_BIN:-}" ]]; then
    candidates+=("${PYTHON_BIN}")
  fi
  candidates+=(
    "${ROOT_DIR}/.venv313/bin/python"
    "${ROOT_DIR}/.venv313/bin/python3"
    "${ROOT_DIR}/.venv/bin/python"
    "${ROOT_DIR}/.venv/bin/python3"
  )

  for candidate in "${candidates[@]}"; do
    if [[ -n "${candidate}" && -x "${candidate}" ]]; then
      printf '%s' "${candidate}"
      return 0
    fi
  done

  return 1
}

if PYTHON_EXE="$(resolve_python)"; then
  exec "${PYTHON_EXE}" "${ROOT_DIR}/server/model_api.py"
fi

echo "No supported Python interpreter found for model_api. Set PYTHON_BIN or create ${ROOT_DIR}/.venv313 or ${ROOT_DIR}/.venv." >&2
exit 1
