#!/usr/bin/env bash
# Shared shell-side Python resolver. Source from any wrapper script that
# needs to run Python with a guaranteed >= 3.10 interpreter.
#
# Mirrors services/_pybin.py and scripts/run_retrain_evidence_pack.py's
# resolve_training_python(). Precedence:
#
#   1. ${PYTHON_BIN} env var (operator override), if executable
#   2. ${ROOT_DIR}/.venv313/bin/python, if present
#   3. ${ROOT_DIR}/.venv/bin/python (or python3), if present
#   4. system python3, only if its version is >= 3.10
#
# Outputs:
#   sets PYTHON_BIN  - absolute path to a verified >= 3.10 interpreter
# Sources a single file rather than copy-pasting the same 6-line
# resolution-and-fallback block into every wrapper script. Encodes the
# version check that was missing from sibling wrappers.
#
# Aborts (exit 1) with a clear error if no >= 3.10 interpreter is reachable.
#
# Usage:
#   ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
#   source "${ROOT_DIR}/scripts/_pybin.sh"
#   "${PYTHON_BIN}" my_script.py

set -o pipefail

if [[ -z "${ROOT_DIR:-}" ]]; then
  # The caller forgot to set ROOT_DIR; derive it from this file's location.
  ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi

# Probe an interpreter for "python -c '...' returns version_info >= (3,10)".
# Returns 0 if the path is executable AND >= 3.10; 1 otherwise.
_pybin_is_310_or_newer() {
  local exe="$1"
  if [[ -z "${exe}" ]]; then return 1; fi
  if [[ ! -x "${exe}" ]]; then return 1; fi
  "${exe}" -c 'import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)' \
    >/dev/null 2>&1
}

_pybin_tried=()

# 1. Caller-provided override.
if [[ -n "${PYTHON_BIN:-}" ]]; then
  if _pybin_is_310_or_newer "${PYTHON_BIN}"; then
    export PYTHON_BIN
    return 0 2>/dev/null || exit 0
  fi
  _pybin_tried+=("PYTHON_BIN=${PYTHON_BIN} (rejected: not executable or <3.10)")
fi

# 2. .venv313/bin/python
_candidate="${ROOT_DIR}/.venv313/bin/python"
if _pybin_is_310_or_newer "${_candidate}"; then
  PYTHON_BIN="${_candidate}"
  export PYTHON_BIN
  return 0 2>/dev/null || exit 0
fi
_pybin_tried+=(".venv313/bin/python=${_candidate} (not present or <3.10)")

# 3. .venv/bin/python (prefer python3 then python)
for _candidate in "${ROOT_DIR}/.venv/bin/python3" "${ROOT_DIR}/.venv/bin/python"; do
  if _pybin_is_310_or_newer "${_candidate}"; then
    PYTHON_BIN="${_candidate}"
    export PYTHON_BIN
    return 0 2>/dev/null || exit 0
  fi
  _pybin_tried+=(".venv/bin/$(basename "${_candidate}")=${_candidate} (not present or <3.10)")
done

# 4. System python3 (only if >= 3.10).
_candidate="$(command -v python3 2>/dev/null || true)"
if [[ -n "${_candidate}" ]] && _pybin_is_310_or_newer "${_candidate}"; then
  PYTHON_BIN="${_candidate}"
  export PYTHON_BIN
  return 0 2>/dev/null || exit 0
fi
if [[ -n "${_candidate}" ]]; then
  _pybin_tried+=("system python3=${_candidate} (<3.10)")
else
  _pybin_tried+=("system python3 (not found on PATH)")
fi

echo "ERROR: could not resolve a Python >= 3.10. Tried:" >&2
for _line in "${_pybin_tried[@]}"; do
  echo "  - ${_line}" >&2
done
echo "Set PYTHON_BIN, create .venv/ with a 3.10+ interpreter, or install python3.10+." >&2
return 1 2>/dev/null || exit 1
