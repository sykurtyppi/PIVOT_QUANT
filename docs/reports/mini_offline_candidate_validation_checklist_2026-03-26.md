# Mini Offline Candidate Validation Checklist

Date: 2026-03-26  
Workspace: `/Users/tristanalejandro/PIVOT_QUANT`

## Purpose

This checklist is for validating the current candidate on the Mac Mini before any governance or production changes.

Current local state at time of writing:

- active model: `v035`
- candidate model: `v037`

Use this checklist as a runbook. Do not skip steps and do not promote from partial evidence.

## 1. Snapshot The Test Environment

Record these before running anything:

- host name
- git commit
- Python executable and version
- active manifest version
- candidate manifest version
- model registry `last_action` and `last_reason`
- database path under test

Recommended commands:

```bash
hostname
git rev-parse --short HEAD
.venv/bin/python3 -V
python3 - <<'PY'
import json, pathlib
for p in [
    "data/models/manifest_active.json",
    "data/models/manifest_runtime_latest.json",
    "data/models/model_registry.json",
]:
    path = pathlib.Path(p)
    print(f"\n== {p} ==")
    d = json.loads(path.read_text())
    for k in ("version", "active_version", "candidate_version", "last_action", "last_reason"):
        if k in d:
            print(f"{k}: {d.get(k)}")
PY
```

Pass condition:

- snapshot is complete and saved with the test notes

## 2. Confirm Candidate Artifact Integrity

Check that the candidate manifest, model files, and calibration metadata are all present and internally consistent.

Verify:

- candidate manifest exists
- referenced model artifacts exist for all configured horizons
- calibration metadata exists
- threshold metadata is present

Pass condition:

- no missing artifact paths
- no JSON parse failures
- no broken references between manifest and model files

## 3. Run Governance Status Without Changing State

Run governance status against the candidate and capture the exact reasons for pass or reject.

Recommended command:

```bash
.venv/bin/python3 scripts/model_governance.py \
  --models-dir data/models \
  --candidate-manifest manifest_runtime_latest.json \
  --active-manifest manifest_active.json \
  --prev-active-manifest manifest_active_prev.json \
  --state-file model_registry.json \
  status
```

Record:

- candidate version
- active version
- required horizons
- threshold utility floor
- exact reject reasons

Pass condition:

- governance output matches the manifest/registry story

## 4. Score The Candidate On Recent Real Events

This is the most important validation step.

Use recent stored touch events and score them with the candidate model without changing production state. The goal is to observe what the candidate would actually have emitted on recent data.

Minimum sample:

- last 5 trading days if available
- last 10 trading days preferred

Record by horizon:

- signal count
- signal rate
- average predicted probability for emitted signals
- threshold used
- reject vs break mix
- abstain rate

Pass condition:

- candidate emits a plausible number of signals
- no obviously dead horizon is being mistaken for a viable one
- no horizon floods the stream with low-quality signals

## 5. Evaluate Cost-Aware Utility On The Replay Window

For every emitted candidate signal, compute realized outcome and cost-aware utility using the same conventions as governance/training.

Record by horizon:

- utility sum
- utility per signal
- TP / FP counts if applicable
- precision
- recall where meaningful

Do not accept a horizon based only on classifier confidence or AUC.

Pass condition:

- the horizon intended for promotion shows positive or at least acceptable guarded utility
- known-bad horizons remain non-viable or guarded out

## 6. Inspect Threshold Meta And Guard Behavior

For each horizon, inspect:

- selected threshold
- fallback flag
- guard applied flag
- guard reason
- stability score
- search enabled / skip reason

Questions to answer:

- is the candidate viable because of a real tuned threshold, or only because of fallback behavior?
- is a positive point estimate contradicted by a materially negative stability score?
- are any horizons surviving only because the support is too small?

Pass condition:

- intended live horizons are supported by tuned thresholds, not by accidental fallback behavior
- guarded horizons are clearly identified as no-trade

## 7. Compare Candidate Against Active On The Same Replay Window

This comparison should be direct and like-for-like.

Compare:

- active emitted signals
- candidate emitted signals
- utility by horizon
- abstain rate
- coverage
- calibration quality where available

Required metrics:

- `utility_bps`
- Brier
- ECE if available
- log-loss if available

Pass condition:

- candidate improves or at least justifiably trades off against the active model on the target horizon
- no hidden regression appears in the intended live path

## 8. Check Operational Safety Before Any Promotion

Before recommending any production change, confirm:

- candidate manifest loads cleanly
- reload path succeeds
- model files are readable on the target machine
- no new runtime-version mismatch exists
- logging and health endpoints still behave normally after load

Pass condition:

- deployment mechanics are boring
- no manual workaround is required

## 9. Promotion Recommendation Format

Every Mini validation should end with one of these decisions:

- `REJECT`
- `SHADOW ONLY`
- `PROMOTE WITH CAUTION`
- `PROMOTE`

The decision must include:

- target horizon(s)
- main evidence
- main risk
- rollback trigger

## 10. Required Rollback Triggers

If the candidate is promoted or shadowed, pre-commit rollback criteria before deployment.

Suggested rollback triggers:

- live signal precision materially below replay expectation
- live utility materially below replay expectation
- unexpected fallback-driven horizon activation
- excessive signal flood
- health or reload instability

## Recommended Deliverables

The Mini validation should produce three artifacts:

1. a one-page summary
2. a metrics table by horizon
3. a recommendation with rollback criteria

## Professional Standard

Do not change governance, required horizons, or utility floors until this checklist is complete for the current candidate.

If the replay evidence is weak or mixed, the correct answer is to reject or shadow, not to rationalize a promotion.
