---
name: pivotquant-projection-accuracy-audit
description: Use this skill in the Pivot Quant repo to audit only projection, scoring, model accuracy, label alignment, calibration, model artifact, research mart, and governance drift risks. Do not use for generic security, style, UI polish, formatting, or non-projection refactors.
---

# Pivot Quant Projection Accuracy Audit

## Description

Audit projection/model accuracy risks in this repository. The audit is read-only and should identify ways live scoring, labels, features, calibration, thresholds, research marts, or governance state can produce misleading projections.

Hard rule: do not modify files during this audit.

## When To Use

Use when the user asks for:
- projection accuracy review
- model/scorer risk audit
- leakage or label alignment audit
- calibration, threshold, model registry, or governance reconciliation review
- research mart freshness or training/runtime parity review

## Scope

Focus only on projection-impacting issues:
- data leakage and feature/target contamination
- label window and horizon alignment
- train/eval/calibration split mistakes
- current-session or future-bar leakage
- stale, missing, or mismatched model artifacts
- `prediction_log` versus actual scoring mismatch
- threshold fallback behavior and silent default paths
- governance/reconciliation drift
- SQLite/DuckDB mart freshness and lineage
- `event_date_et`, session, `ts_event`, timestamp, and timezone alignment
- scorer runtime behavior versus training assumptions
- silent model, analog, manifest, or calibration fallback paths

## Out Of Scope

Ignore unless it directly changes projection correctness:
- style, formatting, naming, lint-only concerns
- generic security issues
- UI polish
- trading strategy quality opinions
- broad refactors
- frontend issues that do not affect scoring inputs, displayed scores, or review decisions

## Repo Files To Inspect

Start with the smallest relevant set, then expand only when evidence requires it:
- Runtime scorer: `server/ml_server.py`, `server/run_ml_server.sh`
- Model registry/API/governance: `server/model_api.py`, `server/model_metadata.py`, `server/model_registry_snapshot.py`, `server/model_review_store.py`
- Event and label pipeline: `server/event_writer.py`, `server/live_event_collector.py`, `scripts/build_labels.py`, `scripts/reconcile_predictions.py`, `scripts/score_unscored_touch_events.py`
- Training and artifacts: `scripts/train_rf.py`, `scripts/train_rf_artifacts.py`, `scripts/refit_calibration.py`, `ml/features.py`, `ml/calibration.py`, `ml/thresholds.py`
- Research marts: `scripts/build_duckdb_view.py`, `scripts/export_parquet.py`, `scripts/build_research_marts.py`, `server/research_api.py`, `research/marts/schema.sql`, `research/marts/cost_models.json`
- Dashboard paths that can affect scoring input or displayed decisions: `production_pivot_dashboard.html`, `app/models/index.js`, `app/governance/index.js`, `app/replay/index.js`, `app/research/index.js`, `app/ops/index.js`
- Validation and ops checks: `scripts/operational_preflight.py`, `scripts/verify_release_integrity.py`, `scripts/release_readiness.sh`, `tests/python/`
- Data/artifact paths when present: `data/pivot_events.sqlite`, `data/pivot_training*.duckdb`, `data/research_marts/last_build.json`, `data/models/manifest_active.json`, `data/models/manifest_runtime_latest.json`, `data/models/model_registry.json`, `data/models/metadata_runtime/`, `data/t9_experiments/`, `data/model_lab/review_store.sqlite`

## Required Audit Procedure

1. Confirm repo context with `pwd`, `git status --short`, and targeted `rg --files`; do not clean or revert anything.
2. Map the scoring path from dashboard or collector event creation to `server/ml_server.py` `/score`, prediction logging, and downstream governance/research views.
3. Map the training path from SQLite events and labels to DuckDB views, research marts, model training, metadata, manifests, and active runtime artifacts.
4. Compare training feature construction in `ml/features.py` and training scripts against runtime scoring feature assumptions in `server/ml_server.py`.
5. Check label timing: `ts_event`, `bar_data.ts`, `event_date_et`, session boundaries, horizon minutes, touch-bar exclusion, and sufficient-forward-bars rules.
6. Check split boundaries: training/calibration/test windows, walk-forward behavior, threshold selection, model context metadata, and candidate versus active manifest promotion.
7. Check artifact integrity: manifest paths, model files, metadata context, feature version, target/horizon coverage, calibration objects, threshold maps, and fallback defaults.
8. Check prediction parity: whether `prediction_log`, shadow emission logs, review APIs, and dashboard decisions represent the same model version, feature version, thresholds, policy mode, and score values served by `/score`.
9. Check freshness and lineage: DuckDB path, `last_build.json`, cost model version, source SQLite, active registry roots, and stale model/mart handling.
10. Report only findings with a clear projection impact. Include exact file and line references.

## Output Format

Use this order:

1. **Findings**
   - `Severity - Title`
   - `File:line`
   - `Evidence`
   - `Projection impact`
   - `Suggested fix`
2. **Open Questions**
3. **Audit Coverage**
4. **Not Reviewed**

If no issues are found, say so and list residual risk or test gaps.

## Severity Definitions

- **Critical**: Demonstrated future-data leakage, train/test contamination, or runtime/training mismatch likely to invalidate live projection decisions.
- **High**: Strong evidence of stale or wrong artifacts, wrong labels, wrong thresholds, or scoring/logging mismatch that can materially mislead live decisions.
- **Medium**: Plausible accuracy degradation or governance drift with bounded blast radius or missing proof of live impact.
- **Low**: Minor projection observability, freshness, or validation weakness that could hide future issues.

## False-Positive Control Rules

- Do not flag a risk only because a fallback exists; prove it can be reached silently or produces misleading projection behavior.
- Do not assume data leakage; identify the field, join, timestamp, or split boundary that permits it.
- Do not report missing tests unless tied to a concrete accuracy failure mode.
- Distinguish offline research-only behavior from live scoring behavior.
- Do not treat stale data as a finding unless the repo lacks detection, surfaces misleading freshness, or uses stale artifacts for live scoring.
- Prefer one precise finding over broad speculation.
