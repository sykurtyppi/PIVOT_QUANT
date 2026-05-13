#!/usr/bin/env bash
# Tiny exec wrapper: resolve a >=3.10 Python via scripts/_pybin.sh, then
# exec it with all forwarded arguments.
#
# Usage:
#   scripts/_pybin_exec.sh scripts/foo.py --arg
#   scripts/_pybin_exec.sh -m unittest discover -s tests/python -p test_x.py
#
# Why this exists: npm scripts in package.json cannot themselves source
# helpers or branch on environment. Routing them through this wrapper
# gives the same precedence (PYTHON_BIN -> .venv313 -> .venv -> system
# python3 with a version probe) every shell wrapper in the project uses.
# Hard-coding ``./.venv/bin/python`` in npm scripts would silently
# bypass PYTHON_BIN overrides and .venv313 and would not version-probe
# the venv interpreter.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/_pybin.sh"

exec "${PYTHON_BIN}" "$@"
