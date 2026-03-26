# PivotQuant ML Stack Audit Reconciliation

Date: 2026-03-26  
Workspace: `/Users/tristanalejandro/PIVOT_QUANT`  
Git commit: `f6ce365`  
Interpreter: `/Users/tristanalejandro/PIVOT_QUANT/.venv/bin/python3` (`3.13.7`)  
Generated from current code and local data only

## Scope

This document reconciles three different inputs:

1. Historical production forensics for the March 2026 Air deployment (`v175` / `v209` / `v211`)
2. A broad code audit that mixed real issues with stale findings
3. The current workspace as it exists on disk today

The key discipline here is simple: historical incident analysis is useful, but it is not the same thing as a current-state code audit.

## Environment Reality Check

The current workspace does not match the historical Air environment described in the forensic report.

Current local model state:

- `data/models/manifest_active.json`: `v035`
- `data/models/manifest_runtime_latest.json`: `v037`
- `data/models/model_registry.json`:
  - `active_version: v035`
  - `candidate_version: v037`
  - `last_action: rejected`

Current local prediction-log sample:

- `prediction_log` contains `v029` and `v010`
- it does not contain the historical `v175`, `v209`, or `v211` rows discussed in the Air incident write-up

Conclusion:

- The March Air report should be treated as a valid historical incident analysis for that host and time window.
- It should not be treated as proof that the current workspace has the same active defects.

## Executive Summary

The historical forensic report got the important production diagnosis right: the live system can look operational while still being structurally unable to trade. That part was strong.

The broad code audit also found one real class of issue: runtime-version inconsistency across launch paths and Python modules.

However, several high-severity claims in the code audit are stale against the current source tree. The most important example is the `ts_prediction` UPSERT claim: in the current code, `ts_prediction` is already preserved on conflict and is not being overwritten.

The best path forward is:

1. Keep the historical incident report as incident history, not as a live defect list.
2. Treat runtime-version consistency as the main code-quality fix from this review cycle.
3. Use a formal Mini offline validation checklist for the current candidate before any governance or promotion changes.

## What The Reports Got Right

### 1. The historical Air forensic report identified the right failure mode

The strongest point in the March production report is the distinction between:

- reporting state such as stale / kill-switch / blocked
- the underlying mechanical reason a model could not produce usable signals

That is the correct forensic frame. It prevents a false fix such as clearing staleness while leaving the signal path structurally dead.

### 2. Horizon-by-horizon utility analysis is the right governance lens

The production report correctly focused on per-horizon utility, guard behavior, and signal viability instead of relying on generic classifier metrics alone.

That is the right standard for this system.

### 3. The code audit was directionally right about runtime consistency

The audit correctly surfaced that Python-version assumptions were inconsistent across the stack. That class of issue was real and worth fixing.

## Resolved In Current Working Tree

These items are now resolved in the current working tree, based on direct source inspection and the patches applied during this review.

### 1. Runtime annotation compatibility was inconsistent

Problem:

- multiple first-party modules used `X | Y` type syntax without `from __future__ import annotations`
- that creates fragile import behavior whenever an older interpreter path is used

Resolution applied:

- added `from __future__ import annotations` to:
  - `server/ml_server.py`
  - `scripts/train_rf_artifacts.py`
  - `scripts/model_governance.py`
  - `scripts/nightly_backup.py`
  - `scripts/reconcile_predictions.py`
  - `scripts/refit_calibration.py`
  - `server/event_writer.py`
  - `server/ibkr_gamma_bridge.py`
  - `server/live_event_collector.py`

Validation:

- `python3 -m py_compile` passed for the patched Python files

Status:

- fixed in working tree
- still needs normal deploy validation on the target host(s)

### 2. Daily report sender version gate was too rigid

Problem:

- `scripts/run_daily_report_send.sh` had a hardcoded Python `>=3.10` assertion
- this made the launcher stricter than the repo needed to be

Resolution applied:

- replaced the hardcoded check with `PIVOT_PYTHON_MIN_VERSION`
- default set to `3.9`
- invalid values fail clearly

Validation:

- `bash -n scripts/run_daily_report_send.sh` passed

Status:

- fixed in working tree
- still needs normal deploy validation on the target host(s)

## Already Fixed Or Stale Findings

These claims appeared in the pasted audit material but do not hold against the current source tree.

### 1. `ts_prediction` overwritten on UPSERT

Status: already fixed in current code

Current `prediction_log` UPSERT behavior in `server/ml_server.py` preserves the original signal/probability payload and does not update `ts_prediction` in the conflict clause. The current comment and SQL are aligned.

Implication:

- the historical claim should not be carried forward as a live defect without re-checking the exact deployed commit

### 2. Weekly lag profile uses UTC date instead of ET date

Status: already fixed in current code

Current `scripts/weekly_policy_review.py` converts event timestamps to ET in Python using `astimezone(ET_TZ).date()`.

### 3. `ml/features.py` silently mishandles MTF type payloads

Status: already fixed in current code

Current code checks `isinstance(types_list, list)` before iterating, which is the right guard for this case.

### 4. `ml/features.py` lacks final `inf` / `nan` sanitization

Status: already fixed in current code

Current code normalizes `nan` and `inf` values to `None` before returning the feature row.

### 5. SQLite prediction-log contention is only logged at debug level

Status: stale claim against current code

Current `server/ml_server.py` already routes repeated contention warnings through a warning-level path.

### 6. All-null calibration-column bug in `train_rf_artifacts.py`

Status: not reproduced in current code

The current code drops all-null columns before deriving `X_calib = X.loc[calib_mask_sub]`, so the specific misalignment claim from the audit does not reproduce from source inspection.

### 7. `.env.example` still uses zero support floors and tiny SQLite timeouts

Status: stale claim against current repo defaults

Current repo defaults already include:

- `MODEL_GOV_MIN_POSITIVE_SAMPLES=20`
- `PREDICTION_LOG_CONNECT_TIMEOUT_SEC=0.5`
- `PREDICTION_LOG_BUSY_TIMEOUT_MS=500`

## Still Open

These are the items that still matter after reconciliation.

### 1. Environment parity must be treated as a first-class control

The biggest process gap was not code. It was mixing:

- historical Air findings
- current local workspace state
- generic repo audit conclusions

without a hard evidence banner.

Required practice going forward:

- every report should state host, git commit, Python path/version, active manifest, candidate manifest, and report timestamp

This is a professional-control issue, not a cosmetic issue.

### 2. Current candidate quality still needs proper offline validation

In this workspace, the live question is not `v209` or `v211`. It is the current candidate recorded in:

- active: `v035`
- candidate: `v037`

The candidate is currently rejected for threshold/governance reasons. That means the next high-value task is not speculative calibration work. It is a disciplined offline validation of the current candidate.

### 3. Calibration-window tuning is a medium-priority model-quality question

`RF_CALIB_DAYS=10` may be a reasonable candidate for later review, but it is not the top-priority issue from this reconciliation.

This should stay behind:

- runtime consistency
- governance validation
- candidate signal viability

## What Was Missing From The Original Reports

### 1. Explicit status labels

Every finding should have been marked as one of:

- historical only
- still open
- already fixed
- fixed in working tree but not yet deployed

Without those labels, a strong report becomes harder to operationalize.

### 2. A single source-of-truth evidence banner

The reports needed a standard header with:

- host
- git commit
- Python executable and version
- active manifest version
- candidate manifest version
- database path used

### 3. A forward plan tied to rollback criteria

Several recommendations suggested governance or deployment changes without a tightly bounded validation checklist. That is fixable, and the attached Mini checklist fills that gap.

## Recommended Next Moves

### Immediate

1. Treat the runtime-version patch set as ready for normal review and deployment.
2. Stop using the March Air report as a proxy for current repo state.
3. Use the Mini offline validation checklist for `v037` before any governance changes.

### Near-Term

1. Add a standard evidence banner template to future audit and incident reports.
2. Add regression tests for:
   - launcher Python selection
   - conflict-row timestamp semantics
   - ET day bucketing
   - feature-row sanitization

### Defer For Now

1. calibration-selection research
2. alternate calibrator experiments
3. broader score-function experimentation

Those are optimization projects. The current priority remains runtime discipline and candidate validation.

## Validation Performed During This Reconciliation

- inspected current manifests and registry
- checked local prediction-log version distribution
- confirmed current `prediction_log` UPSERT semantics
- confirmed current ET bucketing in `weekly_policy_review.py`
- confirmed current MTF type guarding in `ml/features.py`
- confirmed current `inf` / `nan` sanitization in `ml/features.py`
- confirmed current repo timeout/support-floor defaults
- patched runtime-version consistency issues
- ran `python3 -m py_compile` on patched Python modules
- ran `bash -n scripts/run_daily_report_send.sh`

## Bottom Line

The clean professional conclusion is:

- the historical incident report was useful and largely correct for its own host and time window
- the broad code audit contained a mix of real issues and stale claims
- the real code-quality improvement from this pass was runtime-version consistency
- the next meaningful model decision should be based on a formal offline validation of the current candidate, not on historical `v175` / `v209` narratives or on speculative calibration changes
