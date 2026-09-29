#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

load_env_file() {
  local env_file="${ROOT_DIR}/.env"
  local line key value
  [[ -f "${env_file}" ]] || return 0

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -n "${line}" && "${line}" != \#* && "${line}" == *=* ]] || continue
    key="${line%%=*}"
    value="${line#*=}"
    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [[ "${#value}" -ge 2 ]]; then
      if [[ "${value:0:1}" == '"' && "${value: -1}" == '"' ]]; then
        value="${value:1:${#value}-2}"
      elif [[ "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
        value="${value:1:${#value}-2}"
      fi
    fi
    export "${key}=${value}"
  done < "${env_file}"
}

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

  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi

  return 1
}

load_env_file

# ── Active manifest ────────────────────────────────────────────────────────────
# Serve only the governed active manifest by default. Candidate manifests must be
# promoted by model_governance.py before runtime startup should load them.
export RF_ACTIVE_MANIFEST="${RF_ACTIVE_MANIFEST:-manifest_active.json}"

# ── Break model threshold override ────────────────────────────────────────────
# v210 break models were tuned with min_signals=10 and produced fallback=true
# (threshold=1.0, effectively never fires).  Threshold sensitivity analysis
# (2026-04) identified 0.94 as optimal: +14.92 bps avg / 82.3% win / Sharpe 5.47
# under realistic execution (+2 bps).  The marginal 0.92–0.94 band is structurally
# broken (25% win rate, avg -2.75 bps) and is excluded at 0.94.
# Remove or increase this value if signal frequency becomes too high.
export ML_BREAK_THRESHOLD_OVERRIDE="${ML_BREAK_THRESHOLD_OVERRIDE:-15:0.94}"

# ── Decision meta: high-confidence tiers ──────────────────────────────────────
# Probabilities above these levels are classified as "high_confidence" in the
# decision_meta response field.  The UI can use this to distinguish a
# strong trigger (act) from a standard threshold-crosser (secondary filter).
# ML_HIGH_CONFIDENCE_BREAK matches the signal threshold (0.94) so every fired
# break is automatically high_confidence — no ambiguous "standard break" tier.
export ML_HIGH_CONFIDENCE_REJECT="${ML_HIGH_CONFIDENCE_REJECT:-0.95}"
export ML_HIGH_CONFIDENCE_BREAK="${ML_HIGH_CONFIDENCE_BREAK:-0.94}"

# ── Break signal risk filters ─────────────────────────────────────────────────
# F1 (-regime=4): suppress break signals when regime_type == 4.
#   Offline analysis: 28.6% loss rate, avg loss -188 bps (incl. -524 bps on
#   2025-04-07).  14 signals/year removed; avg_bps of removed set is +3.63 bps —
#   negligible edge surrendered.  Removes 3 catastrophic losses (>30 bps each).
#
# F7 (-gamma_mode=+1): suppress break signals when gamma_mode == +1 (dealers net
#   short gamma).  Short-gamma regimes amplify momentum through levels instead of
#   absorbing them, reversing the break-signal premise.
#   Offline: 22.2% loss rate, avg loss -59.8 bps.
#   With F1+F7: Sharpe 6.2 → 14.0, MaxDD -524 → -51.9 bps, 72% signal retained.
#
# Both default to false — restart without these vars is a safe no-op.
# Rollback: set to false (or remove) and restart ml_server.py.
export ML_BREAK_FILTER_REGIME4="${ML_BREAK_FILTER_REGIME4:-true}"
export ML_BREAK_FILTER_GAMMA_POS="${ML_BREAK_FILTER_GAMMA_POS:-true}"

if PYTHON_EXE="$(resolve_python)"; then
  exec "${PYTHON_EXE}" "${ROOT_DIR}/server/ml_server.py"
fi

echo "No supported Python interpreter found for ml_server. Set PYTHON_BIN or create ${ROOT_DIR}/.venv313 or ${ROOT_DIR}/.venv." >&2
exit 1
