#!/usr/bin/env bash
# Single-shot level-alert watcher, invoked every ~1 min by the launch agent
# (com.pivotquant.levels-alerts). Self-gates to RTH; exits quietly when closed.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
PY="$ROOT/.venv313/bin/python"
[ -x "$PY" ] || PY="$(command -v python3 || echo /usr/bin/python3)"
exec "$PY" scripts/levels_alerts/watcher.py
