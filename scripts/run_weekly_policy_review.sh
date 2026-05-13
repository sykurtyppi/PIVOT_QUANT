#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if ! source "${ROOT_DIR}/scripts/_pybin.sh"; then
  echo "no python3.10+ found" >&2
  exit 1
fi
PYTHON="${PYTHON_BIN}"

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
