# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Shape

PIVOT_QUANT has **two coexisting halves**:

1. **JavaScript pivot-engine library** under `src/`, plus dashboard/UI modules at the repo root (`advanced_*.js`, `alert_system.js`, etc.). Tests live in `tests/*.test.js`. This is the "institutional-grade pivot point analysis" surface described in `README.md`.
2. **Python ML+research stack** under `server/`, `ml/`, `scripts/`, `services/`. This is the live trading-research system described in `SYSTEM.md` — data collection, training, calibration, threshold selection, governance, and serving.

`SYSTEM.md` and `docs/RESEARCH_PROTOCOL*.md` are the authoritative references for the Python half. **Read both before changing anything in `scripts/`, `server/`, `ml/`, or `services/`.**

## Commands

### Tests

```bash
# JS suite (Jest)
npm test                                          # full suite
npm test -- --testNamePattern="<pattern>"          # subset by test name
npm run test:performance                          # perf tests only

# Python ops smoke (most ML/research tests live here)
.venv/bin/python -m unittest discover -s tests/python -p test_ops_smoke.py
.venv/bin/python -m unittest discover -s tests/python -p test_ops_smoke.py -k <substring>
# NOTE: `tests/` has no __init__.py — dotted-form `python -m unittest tests.python.test_ops_smoke.X` fails.
#       Always use `unittest discover` with the path-and-pattern form.

# Research protocol unit tests
.venv/bin/python -m unittest discover -s tests/unit/test_research_protocol

# Other Python suites
.venv/bin/python -m unittest discover -s tests/python -p test_prediction_log_reliability.py
.venv/bin/python -m unittest discover -s tests/python -p test_register_candidate.py
```

### Lint / Build / Format (JS only)

```bash
npm run lint                # eslint src/ + tests/**/*.test.js
npm run lint:fix
npm run format              # prettier
npm run build               # rollup
npm run validate            # lint + test
```

### Python Interpreter

**The project Python is `.venv/bin/python` (3.11.14).** System `python3` on the dev Mac is Apple's CommandLineTools 3.9.6, which cannot import `scripts/train_rf_artifacts.py` (PEP 604 `int | None` annotations). When invoking Python directly, always prefer `.venv/bin/python`. Scripts that spawn subprocesses must resolve a ≥3.10 interpreter explicitly (see `scripts/run_retrain_evidence_pack.py:resolve_training_python` for the standard pattern: `PYTHON_BIN` env → `.venv313/bin/python` → `.venv/bin/python` → `sys.executable` if ≥3.10 → fail clearly).

### Common Pipeline Commands

```bash
bash server/run_all.sh                            # foreground stack (proxy + collector + ml_server + event_writer)
bash server/run_persistent_stack.sh               # persistent 24/7 (with caffeinate)
bash scripts/run_retrain_cycle.sh                 # backfill → labels → export → duckdb → train → governance → reload → daily report
.venv/bin/python scripts/run_retrain_evidence_pack.py --out-dir /tmp/pq_evidence_$(date +%s) --report evidence/retrain_$(date +%s).json
.venv/bin/python scripts/audit_held_out_feasibility.py --target reject --horizon 15
.venv/bin/python scripts/audit_regime_health_attribution.py --target reject --horizon 15
```

### CI

GitHub Actions runs three jobs on push to `main` and on PRs: `JS Lint and Test`, `Python Fast Tests`, `Python Ops Smoke`. All three must be green before merging. See `.github/workflows/ci.yml`.

## Architecture — Python Half

### Data Flow

```
yahoo_proxy (:3000) → live_event_collector (:5004) → SQLite (data/pivot_events.sqlite)
                                                           ↓
                                  build_labels → export_parquet → DuckDB (data/pivot_training.duckdb)
                                                           ↓
                                                  train_rf_artifacts.py
                                                           ↓
                                          manifest_runtime_latest.json (candidate)
                                                           ↓
                                                  model_governance.py
                                                           ↓
                                          manifest_active.json (served by ml_server :5003)
```

### Key Modules

- **`ml/thresholds.py`** — `select_threshold()` is the threshold-selection engine. Returns a `ThresholdSelection` dataclass; the chosen threshold is `threshold`, the per-signal utility array at that threshold is `score_observations`. `NO_SIGNAL_THRESHOLD = float(np.nextafter(1.0, 2.0))` is the sentinel that disables a (target, horizon) — any `y_prob >= NO_SIGNAL_THRESHOLD` is always False.
- **`ml/calibration.py`** — `ProbabilityCalibrator` wraps a fitted pipeline with isotonic or sigmoid calibration. Inference uses `calibrator if present else pipeline`.
- **`ml/features.py`** — `build_feature_row(row)` and `FEATURE_VERSION` define the canonical feature surface. Any feature change must bump `FEATURE_VERSION`.
- **`server/ml_server.py`** — FastAPI service on `:5003`. `ModelRegistry` loads `manifest_active.json` and reloads on `POST /reload`. **Threshold resolution precedence:** `manifest['thresholds'][target][horizon]` first; `artifact['optimal_threshold']` only as fallback. `_apply_runtime_threshold_safety()` substitutes the no-signal sentinel for any `utility_bps` threshold whose `score` is non-finite, ≤0, or marked `fallback=True`. **Any audit or diagnostic must mirror this precedence exactly** — see `scripts/audit_held_out_feasibility.py:resolve_runtime_threshold` for the reference implementation. The server also consults `serving_state` (see below) before answering `/score`; when not active, every event in the request is short-circuited with a structured dormant response and no manifest/threshold/model state is mutated.
- **`server/serving_state.py`** — third-axis serving-state registry. Reads `<RF_MODEL_DIR>/serving_state.json`; written by `scripts/set_serving_state.py`. Missing file → default `active`. Unparseable / schema-invalid file → `dormant_data_quality` (fail-closed). `ServingStateRegistry.is_active()` is what `/score` consults; `snapshot()` powers the `serving_state` block in `/health`. D1 is manual-only — no audit auto-wiring, no auto-clear, no expiry enforcement.
- **`scripts/train_rf_artifacts.py`** — orchestrates the train→calibrate→threshold-select pipeline. Writes `manifest_runtime_latest.json` (candidate) into `--out-dir`. Capture-only fields in `thresholds_meta[target][horizon]`: `score_observations`, `score_observations_source` (always `"threshold_tune_slice"`), `signals_on_tune_slice`, `train_purge` diagnostic. Post-hoc `apply_threshold_risk_guards()` can substitute to `NO_SIGNAL_THRESHOLD`; when it does, `score_observations` is cleared so the manifest's observations always refer to the threshold that ships.
- **`scripts/run_retrain_evidence_pack.py`** — the evidence-pack engine. Runs `train_rf_artifacts.py` into an isolated `--out-dir`, then emits a structured JSON report under `evidence/`. Top-level `candidate_readiness` block classifies the candidate into one of four states (`full_family_ready` / `partial_ready` / `degraded_candidate` / `not_ready`) and emits a `promotion_disposition` (one of `ready_full_family`, `full_family_in_sample_validated`, `partial_oos_validated`, `partial_in_sample_validated`, `hold_pending_oos_validation`, `hold_pending_statistical_validation`, `blocked_not_ready`). **`promotion_ready=True` requires `oos_validation_passed=True`, which in turn requires `oos_validation_coverage_complete=True` (OOS covers every mechanically-viable horizon).** In-sample (`threshold_tune_slice`) evidence is real but cannot promote; it lands in `full_family_in_sample_validated` / `partial_in_sample_validated`. Mixed-scope candidates (some horizons OOS, others in-sample) land in `hold_pending_oos_validation` because OOS coverage is incomplete. Statistical validation runs only on mechanically-viable horizons via bootstrap CI + one-sample sign-flip permutation. The scope of each validation result comes from `score_observations_source`; recognized OOS sources are `held_out_slice`, `walk_forward_fold`, or anything matching the `oos_` prefix — anything else (including typos) is treated as in-sample.
- **`scripts/audit_held_out_feasibility.py`** / **`scripts/audit_regime_health_attribution.py`** / **`scripts/audit_regime_data_quality.py`** — read-only diagnostic scripts that mirror serving threshold semantics (manifest-first, artifact fallback) and report fixed chronological slices/groups without picking favorable slices.
- **`services/research_protocol/`** — the protocol-enforcement subsystem. `registration.py`, `kill_list.py`, `protocol_guard.py`, `validation_ladder.py`, `statistical_guard.py`, `replication_guard.py`, `audit_logger.py`, `trial_budget.py`. See `docs/RESEARCH_PROTOCOL_ENFORCEMENT.md`.

### Evidence-Pack Chain (read this before extending it)

The promotion pipeline is a chain of layered policy gates, each shipped as a separate PR:

| Layer | What it gates | Where it lives |
|---|---|---|
| B1 | Training mechanics — produces a candidate manifest | `train_rf_artifacts.py` + `run_retrain_evidence_pack.py` |
| B2 | Candidate readiness — viable vs blocked horizons; runtime/publish agreement; purge diagnostic | `run_retrain_evidence_pack.classify_candidate_readiness` |
| B3 | Statistical validation — bootstrap CI + sign-flip permutation on per-signal utilities | `run_retrain_evidence_pack._validate_horizon_statistically` |
| Promotion | Final promotion disposition | `run_retrain_evidence_pack._compute_promotion_disposition` |

**Invariants:**

- Promotion is a **second axis** on top of readiness, AND statistical validation is itself split by scope. Even `full_family_ready` is not promotable without `oos_validation_passed == True`; in-sample-only evidence (`validation_scope == "in_sample"`) supports `*_in_sample_validated` dispositions but never `promotion_ready=True`. `oos_validation_passed == True` itself requires `oos_validation_coverage_complete == True` — OOS must cover EVERY mechanically-viable horizon, not just the OOS subset. A mixed-scope candidate (some horizons OOS, some in-sample) reports `oos_validation_passed=None` and lands in `hold_pending_oos_validation`. Never collapse the two axes — automation downstream relies on the separation.
- Statistical validation runs **only** on mechanically-viable horizons. The safety chain's verdict is upstream of statistics; a blocked horizon must not be validated even if observations exist.
- `train_rows_purged == 0` is NOT a failure. The `train_purge` diagnostic has explicit states: `valid_noop` (purge enabled, ran, found nothing), `valid_purged` (>0 rows dropped), `disabled` (operator opt-out), `invalid` (broken diagnostic). Only `invalid` blocks readiness.
- `runtime_safety_dry_run.would_neutralize_count > 0` is a hard FAIL (`not_ready` / `blocked_not_ready`). Artifact/serve divergence breaks promotion reproducibility — the sha256 of the on-disk manifest must represent what actually serves.
- **Serving state is a third axis on top of readiness and promotion.** Readiness ≠ promotion ≠ serving state. A promoted model can be paused via `data/models/serving_state.json` without losing its promoted status or changing its manifest. The serving-state gate (`server/serving_state.py`) reads the file and short-circuits `/score` with a structured dormant response when `state != "active"`. **`scripts/set_serving_state.py` is the only writer.** Audits do NOT auto-flip serving state in D1 — that's D3+. Dormancy is "stop emitting signals," not "demote the model," not "retrain," not "rewrite thresholds."

### Serving-state phase status
| Phase | Status | What it covers |
|---|---|---|
| D1 | **shipped** (PR #29) | Manual file-flag gate. Operator-only writes via `set_serving_state.py`. `/score` short-circuit + dormant response. `/health.serving_state` exposes current state. |
| D2 | **shipped** (PR #30) | Observability only. `serving_state_changed` audit event on every CLI write. Process-local counters (`transitions_count_in_process`, `dormant_requests_count_in_process`, `dormant_requests_count_since_state_set`, `last_blocked_at_ms`, `last_loaded_at_ms`) exposed under `/health.serving_state.observability`. Sampled `predict_blocked_dormant` audit event (rate-and-time gated via `ML_SERVING_DORMANT_LOG_SAMPLE_N` default 100 + `ML_SERVING_DORMANT_LOG_MIN_INTERVAL_SEC` default 60). **No behavior change.** Both event types live in `reports/research_protocol/audit_log.jsonl`. |
| D3 | **future** | Opt-in audit-script automation: an audit can call `set_serving_state.py` on failure via `--write-serving-state-on-fail`. Requires explicit flag + `--expires-at` + `--triggering-audit`. Subprocess boundary preserved so the CLI remains the only writer. |
| D4 | **deferred** | Auto-clear, expiry enforcement at runtime, sticky cooldown. Only after D2+D3 have produced enough audit-log data to choose anti-flap parameters from. |

**Operator runbook:** `docs/SERVING_STATE_RUNBOOK.md` — step-by-step for inspecting `/health.serving_state`, pausing / resuming via the CLI, verifying audit events, troubleshooting invalid files, and the `--force` rules. Read it before flipping live serving state.

### Discipline Contract (the most important rule)

This codebase has been hardened over many PRs against a specific failure mode: a previous candidate (`high_vol_trend_early_candidate`) showed 80.9% test win rate on 2025 and 41.2% on 2022 with the same frozen filter — i.e., the "edge" was a regime-favored artifact that survived six diagnostic modules and an LLM-assisted audit. Every audit script, every gate, every test in this repository is written under the assumption that the next change (human or model) is equally prone to fooling itself. Specifically:

- **Never fabricate significance.** If raw per-signal observations aren't available, the validator must report `insufficient_data` — not a hand-waved p-value from aggregate scores.
- **Slice sizes are fixed up front.** Audits report several chronological tail slices; they don't pick the most favorable one. See `audit_held_out_feasibility.py:PCT_SLICES`/`ROW_SLICES` and `audit_regime_health_attribution.py:DEFAULT_RECENT_N`/`DEFAULT_OLDER_PCT`.
- **Report dimensions independently.** A diagnostic does not pick a single "cause" — it produces a multi-dimensional picture and lets the reader synthesize.
- **In-sample evidence must be labeled.** When evidence comes from the same slice that selected a parameter (e.g. `score_observations` from the threshold-tune slice), the manifest records the source via `score_observations_source: "threshold_tune_slice"` so downstream readers cannot mistake it for OOS.
- **Threshold resolution = manifest first, artifact fallback.** Any audit that compares against "the deployed threshold" must use this precedence and surface a mismatch flag.
- **No promotion as a side effect.** Audit and diagnostic scripts must be read-only across DuckDB and model artifacts. They write only to `evidence/` (gitignored except `.gitkeep`).

## Workflow

### Review Feedback Protocol

When applying review feedback (Codex, PR comments, etc.) for a bug fix:
1. Search the full codebase for ALL paths that exhibit the same bug family (env vars, CLI flags, pass-through args, config overrides).
2. List every override path found before editing.
3. Only then apply fixes — do not ship partial fixes that leave sibling paths broken.

## Operations

### Long-Running Processes

- Before restarting any fetcher/backfill/server, check if it's actually dead (`ps`, port check, log tail).
- After restart, compute and report an ETA based on current throughput.
- Audit skip/resume logic for correctness before kicking off long runs.

### Disk Hygiene

Evidence-pack runs write ~400 MB of model artifacts to `/tmp/pq_evidence_<run_id>/` per invocation. The dev Mac's user data partition runs near 100% capacity, so clean `/tmp/pq_evidence_*` between repeated runs:

```bash
rm -rf /tmp/pq_evidence_*
df -h /private/tmp
```

JSON reports in `evidence/` are tiny and should be preserved — they're gitignored (except `.gitkeep`) but are the authoritative artifacts for the next planning step.

### Threshold Override Paths

Several paths can substitute or override the deployed threshold. When debugging "why isn't the model firing," check ALL of them:

- `manifest['thresholds'][target][horizon]` — primary, set by training.
- `ModelRegistry._apply_runtime_threshold_safety()` at server load time — substitutes to `NO_SIGNAL_THRESHOLD` for non-finite / non-positive / fallback utility scores.
- `apply_threshold_risk_guards()` in `train_rf_artifacts.py` — substitutes at training publish time.
- `ml.thresholds.select_threshold(enforce_min_score=, enforce_no_fallback=)` — strict-selector flags that emit the sentinel.
- `RF_THRESHOLD_NO_TRADE_THRESHOLD`, `RF_THRESHOLD_DISABLE_ON_NONPOSITIVE_UTILITY`, `RF_THRESHOLD_DISABLE_ON_FALLBACK` env vars.

A real "model dormant" investigation requires running `scripts/audit_regime_health_attribution.py` AND inspecting the manifest's `thresholds_meta` for guard reasons.

## Symbol Policy

Active retrain symbol defaults to `SPY` via `RETRAIN_SYMBOLS`. Most validation/diagnostic scripts hard-default to `--target reject --horizon 15` for `reject@15m`, which is the surviving viable horizon as of the latest evidence pack. Other (target, horizon) pairs are mechanically blocked (`no_signal_substituted`) per the strict selector — see the most recent report under `evidence/`.
