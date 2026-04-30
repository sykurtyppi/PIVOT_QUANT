# ML Forensic Review Prompt

Use this brief for a forensic, evidence-first review of the live ML trading stack in `PIVOT_QUANT`.

## Prompt

```text
You are doing a forensic, evidence-first review of the live ML trading stack in PIVOT_QUANT.

Environment
- Repo root: /Users/tristanalejandro/PIVOT_QUANT
- Research DB if present: /Volumes/T9/pivotquant/PIVOT_QUANT/data/pivot_events.sqlite
- Production runtime artifacts may exist in:
  - data/models/manifest_active.json
  - data/models/manifest_runtime_latest.json
  - data/models/manifest_active_prev.json
  - data/models/model_registry.json

Mission
Determine why live production produced effectively zero live trades for the week ending March 25, 2026, even though the stack remained operational and daily reports showed nontrivial horizon metrics.

Hard constraints
- Do not make code changes.
- Do not push anything.
- Do not recommend production changes without strong evidence.
- Treat code, DB, manifests, and logs as primary evidence.
- Be skeptical. Verify every important claim.
- If parity is incomplete, stop and say so clearly before drawing conclusions.

Critical instruction: parity first
Before any analysis, verify whether this machine is a faithful mirror of production runtime state.

You must check:
1. git revision
2. presence and versions of:
   - data/models/manifest_active.json
   - data/models/manifest_runtime_latest.json
   - data/models/model_registry.json
3. whether the DB being used is the real research/runtime DB
4. whether relevant env/policy settings are present
5. whether the manifests/model files match the current production state

If parity is incomplete, output this first:
- PARITY STATUS: INCOMPLETE
- Missing items
- Why conclusions would be unreliable
- Exact next artifacts needed

Do not continue to model conclusions if parity is incomplete.

Primary question
Why is live production still on stale model v175 and producing zero tradeable signals, and which of these is the dominant cause:
1. stale active model
2. weak candidate quality
3. governance too strict
4. thresholding collapsing to no-trade
5. live suppressions / policy overlays
6. combination of the above

Secondary question
Is there enough evidence to justify a narrower promotion path such as:
- required horizons = 5,15
- required horizons = 5 only

Known context to verify, not assume
- Production active model is v175.
- Daily reports on March 23, March 24, and March 25, 2026 all show KILL-SWITCH / BLOCKED / DO NOT TRADE.
- The system is operational: dashboard, collector, scoring, and gamma generally work.
- Gamma coverage exists on touch events.
- Production generated effectively zero tradeable matured signals for the week.
- Candidate promotion appears to be blocked repeatedly by governance.
- Analog promotion gate has been passing, but that has not translated into live trading utility.
- There is concern that short-horizon reject signal may exist while broader horizon requirements block promotion.

You must inspect
1. Runtime manifests and registry
- data/models/manifest_active.json
- data/models/manifest_runtime_latest.json
- data/models/manifest_active_prev.json
- data/models/model_registry.json

2. ML server and live scoring behavior
- server/ml_server.py
- threshold loading
- signal classification
- analog blend
- disagreement guard
- regime policy
- ATR overlays
- OR breakout reject filter
- quality flag generation
- stale model surfacing

3. Training / artifacts / governance
- scripts/train_rf.py
- scripts/train_rf_artifacts.py
- scripts/model_governance.py
- scripts/run_retrain_cycle.sh
- related helper scripts

4. Reporting / reconciliation
- scripts/generate_daily_ml_report.py
- scripts/send_daily_report.py
- scripts/reconcile_predictions.py
- logs/reports/ml_daily_2026-03-23.md
- logs/reports/ml_daily_2026-03-24.md
- logs/reports/ml_daily_2026-03-25.md
- logs/retrain.log

5. DB-backed runtime evidence
Use the real DB if available. Prefer:
- /Volumes/T9/pivotquant/PIVOT_QUANT/data/pivot_events.sqlite

Questions you must answer

1. What exactly is v175 doing in production right now?
For each target/horizon:
- active threshold
- fallback status
- guard status
- whether it is structurally capable of emitting live trades

2. Why does v175 emit zero trades?
Quantify the contribution of:
- pinned no-trade thresholds
- weak scores
- suppressions / overlays
- stale active artifact
- any other blocker

3. How did v175 perform live this week by horizon?
For 5m / 15m / 30m / 60m, and reject/break separately where applicable:
- live signal counts
- no-edge rate
- precision / recall if any live signals exist
- AUC / Brier / ECE
- MFE / MAE
- cost-aware expectancy where possible
- whether the model ranks setups but fails to cross threshold

4. Are the March 23, 24, and 25 daily reports supported by DB evidence?
- validate them against DB and logs
- distinguish observed vs inferred
- call out any misleading interpretations precisely

5. What blocks candidate promotion?
- identify exact governance gate(s)
- show exact failed target/horizon combinations
- show threshold utility guard behavior
- determine whether candidates are genuinely weak or whether one bad horizon blocks otherwise-usable short-horizon signal

6. Evaluate the “5m-only” thesis carefully
- Does evidence support a 5m-only production lane?
- Is 15m strong enough to include?
- Should 30m/60m stop being promotion blockers?
- Should break be excluded from promotion gating for now?
- Back this with numbers, not narrative.

7. Counterfactual offline scenarios
Estimate what would likely have happened this week under:
- current governance
- required horizons = 5,15
- required horizons = 5 only

For each scenario, estimate:
- whether a candidate would likely promote
- likely signal count
- likely cost-aware tradeability
- biggest risks introduced

Methodology requirements
- Use exact file paths and line references when citing code.
- Use exact dates: March 23, 2026; March 24, 2026; March 25, 2026.
- Clearly label every claim as one of:
  - Observed from code/DB/logs
  - Inferred from evidence
- If exact replay is not possible, say exactly why and provide the strongest approximation available.
- Do not generalize from a single report.

Deliverable format

1. Findings first
Ordered by importance.
For each finding include:
- title
- why it matters
- evidence
- file references and/or DB/log outputs

2. Root-cause ranking
Rank:
- stale active model
- candidate weakness
- governance strictness
- thresholds
- suppressions/overlays
- reporting artifacts

3. Horizon-by-horizon diagnosis table
- 5m / 15m / 30m / 60m
- Reject and Break separately if needed

4. Governance decision analysis
State clearly whether governance is:
- correct
- too strict
- or misaligned with production goals

5. Recommendation
Separate:
- what to test offline on the Mini
- what, if anything, should change in production later

If you recommend “5 only” or “5,15”, explain why that is institutionally responsible and what specific risks it introduces.

Quality bar
- No vague summary.
- No hand-waving.
- No unproven claims.
- If evidence is mixed, say so.
- If parity is incomplete, stop and explain exactly what is missing.
```
