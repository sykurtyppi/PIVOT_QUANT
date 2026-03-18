#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="${ROOT_DIR}/.venv/bin/python3"
ENV_FILE="${ROOT_DIR}/.env"

load_env_file() {
  local env_path="$1"
  local raw line key value
  [[ -f "${env_path}" ]] || return 0

  while IFS= read -r raw || [[ -n "${raw}" ]]; do
    line="${raw#"${raw%%[![:space:]]*}"}"
    [[ -z "${line}" ]] && continue
    [[ "${line:0:1}" == "#" ]] && continue
    [[ "${line}" == "export "* ]] && line="${line#export }"
    [[ "${line}" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue

    key="${line%%=*}"
    value="${line#*=}"
    # Accept quoted .env values without exporting quote characters.
    if [[ "${#value}" -ge 2 ]]; then
      if [[ "${value:0:1}" == "\"" && "${value: -1}" == "\"" ]]; then
        value="${value:1:${#value}-2}"
      elif [[ "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
        value="${value:1:${#value}-2}"
      fi
    fi
    export "${key}=${value}"
  done < "${env_path}"
}

load_env_file "${ENV_FILE}"

if [ -x "${VENV_PY}" ]; then
  exec "${VENV_PY}" "${ROOT_DIR}/server/ibkr_gamma_bridge.py"
fi

if command -v python3 >/dev/null 2>&1; then
  exec python3 "${ROOT_DIR}/server/ibkr_gamma_bridge.py"
fi

echo "python3 not found. Please install Python 3 and try again." >&2
exit 1
