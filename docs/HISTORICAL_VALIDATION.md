# Historical Validation

This repository can inspect historical PivotQuant data on an external Samsung T9
drive without mutating the source files.

## Read-Only T9 Inventory

Run:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 python scripts/inspect_external_data.py --symbol SPY --max-files 200
```

For JSON output:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 python scripts/inspect_external_data.py --symbol SPY --max-files 200 --json
```

The inventory command checks likely locations for:

- SPY daily OHLCV parquet
- SPY option chain parquet
- SPY option feature parquet
- SQLite candidates
- raw intraday JSON candidates

The command is metadata-only. It reports paths, existence, capped file counts,
sample schemas, cheap row estimates when `duckdb` is available, and date ranges
from parquet metadata or partition/file names. It does not copy, move, delete,
rewrite, or migrate any T9 data.

If the T9 drive is not mounted, the command exits non-zero with a clear warning.

## Validation Direction

The first validation step is discovery only. Later steps should add small,
date-bounded smoke tests before any full historical walk-forward run. Historical
backtests must remain walk-forward and avoid lookahead leakage: train/calibrate
only on data available before the test window, then evaluate out-of-sample.

## Historical Smoke Adapter

Run a tiny date-bounded read of daily OHLCV and option-feature parquet:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_historical_smoke_test.py \
  --symbol SPY \
  --start-date 2024-01-02 \
  --end-date 2024-01-05 \
  --max-files 20
```

The smoke adapter normalizes:

- Daily OHLCV to `date`, `open`, `high`, `low`, `close`, `volume`, `source`.
- Option features to `date`, `underlying_symbol`, `expiration`, `strike`,
  `option_type`, `bid`, `ask`, `mid`, `volume`, `open_interest`,
  `implied_volatility`.

Reports are written only under the local repo path:

```text
reports/historical_smoke/
```

This is still not a walk-forward harness. It is a small data-readiness check that
proves the clean repo can read a bounded T9 slice safely before broader
validation work begins.

## Historical Contract Smoke

The smoke adapter now applies a narrow validation contract on the normalized
slice. It does not train, score, label, or backtest. It only verifies that a
tiny date-bounded input slice is structurally safe for future validation work.

Daily OHLCV source selection is deterministic:

```bash
PIVOTQUANT_DAILY_SOURCE=yahoo      # default
PIVOTQUANT_DAILY_SOURCE=ivolatility
PIVOTQUANT_DAILY_SOURCE=auto
```

`yahoo` is the default because it is explicit and stable. `auto` chooses a
source using fixed precedence (`yahoo`, then `ivolatility`, then alphabetical
fallback). When multiple daily sources exist for the same date, the report keeps
only the selected canonical source and records duplicate-source metadata.

The contract checks:

- Required daily fields exist: `date`, `open`, `high`, `low`, `close`,
  `volume`, `source`.
- Required option-feature fields exist: `date`, `underlying_symbol`,
  `expiration`, `strike`, `option_type`, `bid`, `ask`, `mid`, `volume`,
  `open_interest`, `implied_volatility`.
- Daily and option rows are inside the requested date window.
- Every option-feature date has a selected daily OHLCV row.
- Future/label-like columns are not present in normalized feature frames.

This contract intentionally stops before full walk-forward validation. The next
step should adapt these normalized slices into the new project feature/label
interfaces on a tiny historical window before any full-history scan.

## Historical Feature Contract Smoke

Run the bounded feature/label-readiness adapter:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_historical_feature_smoke.py \
  --symbol SPY \
  --start-date 2024-01-02 \
  --end-date 2024-01-05 \
  --max-files 20 \
  --daily-source yahoo
```

This command reads the same normalized bounded T9 slice and produces a contract
report for three schema-ready outputs:

- `model_ready_daily_features`: canonical daily OHLCV plus same-day/prior-day
  derived fields such as `return_1d`, `intraday_range_pct`, and
  `close_to_open_pct`.
- `option_context_features`: normalized option rows joined to same-date
  underlying close, with context fields such as `days_to_expiration`,
  `moneyness`, `spread`, and `relative_spread`.
- `label_ready_rows`: observation rows that are ready for a later realized-label
  builder. This smoke does not create realized labels.

The feature contract validates:

- No input rows fall outside the requested date window.
- No option date is used unless a selected canonical daily row exists for that
  date.
- Required generated schemas are present.
- Missing values are reported rather than hidden.
- Duplicate daily dates are collapsed deterministically.
- Future/label-like input columns are rejected so feature construction does not
  accidentally use realized outcomes.

Reports are written only under:

```text
reports/historical_feature_smoke/
```

This remains a tiny validation adapter. It is not a model trainer, a label
builder, or a walk-forward backtest.

## Historical Label Contract Smoke

Run the bounded label-builder smoke:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_historical_label_smoke.py \
  --symbol SPY \
  --start-date 2024-01-02 \
  --end-date 2024-01-10 \
  --max-files 20 \
  --daily-source yahoo \
  --horizons 1d,5d
```

The label smoke consumes `label_ready_rows` plus canonical daily features from
the feature contract. It emits realized label candidates only when the requested
future trading-day horizon has an observed close inside the bounded daily slice.
Rows at the tail of the window, or rows missing an observation/future close, are
excluded and counted by reason.

The label contract validates:

- Future horizon data exists before a label candidate is emitted.
- Final/immature rows without future data are excluded instead of labeled.
- Label outputs are not joined back into feature inputs.
- Input and label dates remain inside the bounded date range.
- Missing data and coverage by horizon are reported clearly.

Reports are written only under:

```text
reports/historical_label_smoke/
```

This is still not a walk-forward backtest. It is a bounded no-leakage label
smoke that proves the next validation layer can distinguish mature labels from
immature observations.

## Historical Baseline Smoke

Run the first tiny end-to-end descriptive baseline:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_historical_baseline_smoke.py \
  --symbol SPY \
  --start-date 2024-01-02 \
  --end-date 2024-01-31 \
  --max-files 20 \
  --daily-source yahoo \
  --horizons 1d,5d
```

The baseline smoke consumes:

- `model_ready_daily_features`
- `option_context_features`
- mature label candidates from the label contract

It reports descriptive metrics only:

- Row counts and date coverage.
- Mature label counts by horizon.
- Forward-return distributions by horizon.
- Forward-return distributions by option type.
- Forward-return distributions by moneyness bucket.
- Missing-value summaries and warnings.

This smoke deliberately performs no ML training, no threshold tuning, no
governance promotion, and no walk-forward evaluation. Its purpose is to verify
that the bounded historical pipeline can produce coherent mature outcomes before
the project scales into a one-month walk-forward validation PR.

Reports are written only under:

```text
reports/historical_baseline_smoke/
```

## Historical Walk-Forward Dry-Run Smoke

Run the first bounded walk-forward skeleton:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_historical_walk_forward_smoke.py \
  --symbol SPY \
  --start-date 2024-01-02 \
  --end-date 2024-01-31 \
  --max-files 20 \
  --daily-source yahoo \
  --horizons 1d,5d \
  --train-window 10 \
  --test-window 5 \
  --step 5
```

The walk-forward smoke uses observed trading days from the bounded canonical
daily slice. It emits chronological windows with:

- Train/test trading-day ranges.
- Train/test mature-label row counts.
- Label coverage by horizon.
- Forward-return summaries by test window.
- Explicit zero-row windows when a test window has no mature labels.
- No-leakage checks: `train_end < test_start`, no test dates inside train, and
  emitted labels have future label dates.

This remains a dry run. It performs no model training, no threshold
optimization, and no full-history scan. Its purpose is to prove the
walk-forward calendar/window skeleton before adding any RF or governance logic.

Reports are written only under:

```text
reports/historical_walk_forward_smoke/
```

## Historical Rule-Baseline Smoke

Run deterministic rule baselines inside the bounded walk-forward windows:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_historical_rule_baseline_smoke.py \
  --symbol SPY \
  --start-date 2024-01-02 \
  --end-date 2024-01-31 \
  --max-files 20 \
  --daily-source yahoo \
  --horizons 1d,5d \
  --train-window 10 \
  --test-window 5 \
  --step 5
```

The rule baseline is not alpha search. It applies fixed, documented entry-time
filters only:

- `abs(moneyness) <= 0.01`
- `volume >= 1`
- `open_interest >= 1`
- `relative_spread <= 0.25`
- moneyness bucket limited to `atm`, `near_itm`, or `near_otm`

Train and test summaries are reported separately for every window. The report
includes eligible rows, selected rows, forward-return mean/median, win rate,
sample size, missing-label count, and a non-evaluable flag for zero-row or
label-missing windows.

The rule baseline intentionally does not train models, optimize thresholds, or
make performance claims. Forward labels are used only after selection for
evaluation, and selection is guarded so label/outcome columns cannot be used as
entry filters.

Reports are written only under:

```text
reports/historical_rule_baseline_smoke/
```

## Model Input Compatibility Smoke

Run the bounded schema/readiness audit:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_model_input_compatibility_smoke.py \
  --symbol SPY \
  --start-date 2024-01-02 \
  --end-date 2024-01-31 \
  --max-files 20 \
  --daily-source yahoo \
  --horizons 1d,5d
```

The audit compares the bounded historical feature rows against the clean
project's current model input contract from
`InstitutionalMLDatabase.get_training_dataset()`. It reports:

- Deterministic OHLCV-derived features computed by the bounded adapter.
- Rows unavailable due to insufficient lookback history.
- Missing required model features.
- Extra historical features.
- Dtype mismatches and coercion needs.
- Nullable, high-null, constant, and all-null fields.
- Label availability by horizon.
- Target/horizon compatibility.

Status semantics:

- `fail`: required model features are missing, required fields are all-null, or
  required target horizons are unavailable.
- `warn`: required fields exist, but extra columns, coercions, or high-null
  fields need attention.
- `pass`: required inputs are available and type-compatible.

This is a schema/readiness check only. It performs no ML training, no threshold
optimization, no alpha search, and no full-history scan.

The bounded adapter may compute only non-leaky daily features from observed
OHLCV rows:

- `price_momentum_5d`: current close versus close 5 observed rows earlier.
- `price_momentum_20d`: current close versus close 20 observed rows earlier.
- `volume_ratio_10d`: current volume versus the prior 10 observed volumes.
- `realized_vol_30d` / `realized_vol_60d`: realized volatility through the
  current row only.
- `rsi_14` and `bb_position`: current/prior close based technical features when
  enough lookback exists.

The daily adapter does not fabricate unavailable IV, term-structure, VIX/macro,
or 21-day target fields. Those remain missing or all-null until real bounded
inputs exist.

When real bounded inputs are present, the compatibility layer may also compute:

- `iv30_rv30_ratio`: same-date 30D ATM IV proxy divided by `realized_vol_30d`.
- `iv_percentile`: current 30D IV percentile inside the trailing IV window
  through the entry date only.
- `iv_rank`: current 30D IV rank versus trailing min/max IV through the entry
  date only.
- `vol_term_structure_slope`: same-date longer-tenor ATM IV minus shorter-tenor
  ATM IV.
- `vix_level`: same-date close from real `^VIX` daily OHLCV rows.

If VIX rows, option IV rows, or the required tenors are missing, the report keeps
those fields unavailable rather than fabricating values.

Reports are written only under:

```text
reports/model_input_compatibility_smoke/
```

## Extended Model Input Compatibility Smoke

Run the extended bounded readiness audit when you need enough history for
lookback-heavy daily features and 21-day labels:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_model_input_compatibility_extended_smoke.py \
  --symbol SPY \
  --start-date 2023-11-01 \
  --end-date 2024-01-31 \
  --max-files 20 \
  --max-days 130 \
  --daily-source yahoo \
  --horizons 1d,5d,21d
```

The extended smoke still reads a bounded slice only. It reports:

- Null rate by deterministic daily feature.
- Insufficient-lookback count by feature.
- Usable row count after deterministic daily features are available.
- Real-input IV/VIX feature source counts and null rates.
- Unavailable-source counts for IV/VIX fields that cannot be constructed.
- 21-day label availability separately from feature availability.
- True missing schema fields that remain unavailable.

This command is still a readiness check, not a model run. It does not train,
optimize thresholds, scan full history, or fabricate unavailable fields.

Reports are written only under:

```text
reports/model_input_compatibility_smoke_extended/
```

## Model-Ready Dataset Smoke

After compatibility passes or warns for only explainable reasons, export a tiny
feature/label dataset artifact:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_model_ready_dataset_smoke.py \
  --symbol SPY \
  --analysis-start-date 2024-01-02 \
  --analysis-end-date 2024-01-31 \
  --feature-lookback-days 120 \
  --label-lookahead-days 45 \
  --max-files 20 \
  --max-days 220 \
  --daily-source yahoo \
  --horizons 1d,5d,21d
```

The dataset export consumes the compatibility layer and writes:

- Required model feature columns only, in stable order.
- `symbol` and `entry_date` identity columns.
- Label columns separated from features:
  `forward_return_1d`, `forward_return_5d`, `forward_return_21d`, and
  `forward_volatility_21d` when available.
- A JSON metadata sidecar with provenance, compatibility warnings,
  no-lookahead notes, row counts, drop reasons, dtypes, and null rates.

The export separates window semantics:

- The read window may include prior rows for feature lookback and future rows for
  label construction.
- The analysis window controls which `entry_date` rows are exported.
- Future rows are never exported as features for an earlier analysis row.
- Label columns stay separate from feature columns and are never fed back into
  feature construction.
- `forward_volatility_21d` is constructed as a label-only field from future
  daily returns inside the label lookahead window when enough future rows exist.

Rows with missing required model features are dropped by default. Rows with
missing labels are retained and flagged by default so short bounded windows can
still produce a reproducible feature artifact without pretending labels exist.
Use `--missing-label-policy drop` when you deliberately need only fully labeled
rows.

The writer uses Parquet when the local Python environment has a supported
engine; otherwise it writes CSV fallback plus the same JSON metadata sidecar.

Reports are written only under:

```text
reports/model_ready_dataset_smoke/
```

This is still not a model run. It performs no ML training, no threshold
optimization, no performance claim, and no full-history scan.
