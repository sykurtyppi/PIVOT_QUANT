# PivotQuant Model Governance

## Scope
This policy controls promotion of trained ML artifacts from `candidate` to `active` for live scoring.

## Lifecycle
1. `train_rf_artifacts.py` publishes `data/models/manifest_latest.json` (candidate).
2. `scripts/model_governance.py evaluate` applies promotion gates.
3. If gates pass, candidate is promoted to `data/models/manifest_active.json`.
4. `ml_server.py` serves active manifest by default.
5. Rollback is available through `scripts/model_governance.py rollback`.

## Promotion Gates
The evaluator enforces:
- Required targets exist (default: `reject,break`).
- Required horizons exist (default: `5,15,60`).
- All referenced model artifacts exist on disk.
- Thresholds exist and are bounded `[0,1]`.
- Candidate `trained_end_ts` is not older than active.
- Feature-version changes are blocked by default.
- Candidate MFE/MAE cannot regress beyond configured tolerances.

Config (from `.env`):
- `MODEL_GOV_REQUIRED_TARGETS`
- `MODEL_GOV_REQUIRED_HORIZONS`
- `MODEL_GOV_MIN_TRAINED_END_DELTA_MS`
- `MODEL_GOV_MAX_MFE_REGRESSION_BPS`
- `MODEL_GOV_MAX_MAE_WORSENING_BPS`
- `MODEL_GOV_ALLOW_FEATURE_VERSION_CHANGE`
- `MODEL_GOV_FORCE_PROMOTE` (emergency override)

## State and Audit Trail
`scripts/model_governance.py` writes:
- `data/models/model_registry.json` (history and last decision)
- `data/models/manifest_active_prev.json` (rollback source)
- ops keys in SQLite `ops_status`:
  - `model_active_version`
  - `model_candidate_version`
  - `model_governance_last_action`
  - `model_governance_last_reason`
  - `model_governance_last_checked_ms`

## Operations
Status:
```bash
python3 scripts/model_governance.py --models-dir data/models status
```

Evaluate:
```bash
python3 scripts/model_governance.py --models-dir data/models --ops-db data/pivot_events.sqlite evaluate
```

Rollback to previous active:
```bash
python3 scripts/model_governance.py --models-dir data/models rollback
```

Rollback to explicit version:
```bash
python3 scripts/model_governance.py --models-dir data/models rollback --to-version v010
```

## Integration
`scripts/run_retrain_cycle.sh` now executes governance after training and before `/reload`.
This keeps retrain autonomous while preventing unsafe candidate promotion.

## Design References
- NIST AI RMF 1.0: governance and lifecycle controls for AI risk management.  
  https://www.nist.gov/publications/artificial-intelligence-risk-management-framework-ai-rmf-10
- Google Cloud MLOps guidance: continuous training with model validation before deployment.  
  https://cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning
- Google Cloud quality guidelines: deployment-time validation checks for model artifacts and interfaces.  
  https://docs.cloud.google.com/architecture/guidelines-for-developing-high-quality-ml-solutions
- MLflow Model Registry: model lifecycle/versioning and controlled promotion workflows.  
  https://mlflow.org/docs/2.21.3/model-registry/
