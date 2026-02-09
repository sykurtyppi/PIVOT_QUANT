#!/usr/bin/env bash
set -euo pipefail

if [ -x ".venv/bin/python" ]; then
  exec ./.venv/bin/python server/event_writer.py
fi

if command -v python3 >/dev/null 2>&1; then
  exec python3 server/event_writer.py
fi

echo "python3 not found. Please install Python 3 and try again." >&2
exit 1
