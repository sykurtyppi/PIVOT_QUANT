#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ -x "${ROOT_DIR}/.venv/bin/python3" ]]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python3"
elif [[ -x "${ROOT_DIR}/.venv/bin/python" ]]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON="$(command -v python3)"
else
  echo "python3 not found" >&2
  exit 1
fi

OUT_DEFAULT="${ROOT_DIR}/logs/reports/weekly_policy_review_$(TZ=America/New_York date +%F).md"
HAS_OUTPUT=0
for arg in "$@"; do
  case "${arg}" in
    --output|--output=*) HAS_OUTPUT=1 ;;
  esac
done

CMD=("${PYTHON}" "scripts/weekly_policy_review.py")
if [[ "${HAS_OUTPUT}" -eq 0 ]]; then
  CMD+=("--output" "${OUT_DEFAULT}")
fi
CMD+=("$@")

"${CMD[@]}"
if [[ "${HAS_OUTPUT}" -eq 0 ]]; then
  echo "Wrote ${OUT_DEFAULT}"
fi
