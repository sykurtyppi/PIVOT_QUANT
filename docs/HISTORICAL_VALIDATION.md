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

## Unit Test Suite

Run the focused ML pipeline test suite (historical validation and candidate
signal work only — 142 tests, always expected to be clean):

```bash
python -m unittest discover -s tests/unit/test_services -p "test_ml_*.py"
```

Expected: `OK` — zero failures, zero errors.

This pattern covers every `test_ml_*.py` file: boundary purge, candidate signal,
paper eval, readiness checklist, governance, effective sample, multiyear
diagnostics, regime/target diagnostics, signal diagnostics, and training smoke.

**Full suite (includes two known pre-existing failures):**

```bash
python -m unittest discover -s tests/unit/test_services
```

Expected: `FAILED (failures=1, errors=1)` — the two failures are unrelated to
this pipeline. See [`docs/KNOWN_TEST_FAILURES.md`](KNOWN_TEST_FAILURES.md) for
details, reproducers, root causes, and recommended fixes. Do not count them as
regressions. Any new failure in `test_ml_*.py` is a regression and must be
investigated.

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

Run the bounded walk-forward skeleton with optional rule-baseline scoring and
regime conditioning. The example below covers a 3-month window, adds a 20-day
realized-volatility regime signal, and prints only the cross-window summary:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_historical_walk_forward_smoke.py \
  --symbol SPY \
  --start-date 2024-01-02 \
  --end-date 2024-03-31 \
  --max-files 60 \
  --daily-source yahoo \
  --horizons 1d,5d \
  --train-window-days 20 \
  --test-window-days 5 \
  --step-days 5 \
  --option-type both \
  --min-open-interest 100 \
  --min-volume 1 \
  --regime-signal realized_vol_20d \
  --regime-lookback-days 20 \
  --regime-buckets 3 \
  --summary-only
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

When `--option-type`, `--min-open-interest`, or `--min-volume` are supplied,
each test window also reports deterministic rule-baseline scoring:

- Eligible rows (joined label+option rows in the test window).
- Selected rows after applying entry-time filters.
- Selection rate.
- Forward-return mean, median, and win rate for selected rows.
- Count by horizon and option type.
- Non-evaluable flag with reason for zero-row or no-selection windows.

When `--regime-signal realized_vol_20d` is supplied, each window also reports
a regime bucket computed strictly from canonical daily close prices with
dates ≤ `train_end`:

- `train_end_realized_vol`: annualized standard deviation of the last
  `--regime-lookback-days` daily log returns available at `train_end`.
- `bucket`: `low_vol`, `mid_vol`, or `high_vol` (or `insufficient_history`
  when fewer returns are available than `--regime-lookback-days`).

Bucket thresholds are global tertiles across all windows' train-end vols.
This is a post-hoc descriptive label, not a forward-looking trading filter.
The vol value itself is strictly no-lookahead (dates ≤ `train_end` only).

The report always includes a `cross_window_summary` section with:

- `total_windows`: total walk-forward windows formed.
- `evaluable_windows`: windows where at least one row passes all filters.
- `non_evaluable_windows`: windows with zero selected rows.
- `zero_row_window_count` / `zero_row_window_fraction`: windows where the test
  period had no mature label rows at all.
- `total_selected_rows`: sum of selected rows across all evaluable windows.
- `by_horizon`: per-horizon selected row count, weighted-mean return, and
  weighted win rate across evaluable windows.
- `window_mean_returns`: ordered list of per-window mean returns (None for
  non-evaluable windows).
- `best_window` / `worst_window`: the evaluable window with the highest and
  lowest mean return, respectively.
- `by_regime` (when `--regime-signal` is active): per-bucket aggregation of
  total windows, evaluable windows, selected rows, per-horizon mean return and
  win rate, and best/worst window within each regime bucket.

Rule-baseline filters and regime signals are entry-time only. Label and outcome
columns (`forward_return`, `label_date`, etc.) are never used for selection or
regime assignment. No parameters are learned. No thresholds are optimized.

Available CLI flags:

| Flag | Default | Description |
|---|---|---|
| `--option-type` | `both` | `call`, `put`, or `both` |
| `--min-open-interest` | `0` | Minimum open interest |
| `--min-volume` | `0` | Minimum volume |
| `--moneyness-bucket` | none | Restrict to `atm`, `near_itm`, or `near_otm` |
| `--regime-signal` | `none` | `realized_vol_20d` or `none` |
| `--regime-lookback-days` | `20` | Trading-day lookback for realized vol |
| `--regime-buckets` | `3` | Number of vol buckets (1, 2, or 3) |
| `--summary-only` | off | Print cross-window summary only |

This remains a dry run. It performs no model training, no threshold
optimization, and no full-history scan.

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

## Cross-Period Validation

Audit-Fix PR5 adds a configurable train/test split so the **frozen** candidate
signal (no logic, threshold, or filter changes) can be re-evaluated on a
different market regime, and a `cross_period_validated` decision aggregating
two or more independent runs.

### Why

The audit verdict on the original 2023+2024 → 2025 run flagged that the test
year (2025, strong bull market) is favorable to a long-biased filter and that
the candidate's test win rate (80.9%) materially exceeded its train win rate
(68.5%). A single out-of-sample observation cannot distinguish "stable signal"
from "regime-favorable artifact." Cross-period validation is a falsification
attempt — a different regime (e.g., 2022 bear market) should either confirm or
break the candidate.

### Source modules

- `services/external_data/ml_regime_benchmark.discover_year_datasets`
- `services/external_data/ml_cross_period_validation.aggregate_cross_period_validation`
- `scripts/run_ml_regime_validation_cross_period.py`

### Executed cross-period: `2021_partial → 2022`

The originally-specified `2020+2021 → 2022` split was not executable because the
T9 SPY options-features tree only covers **2021-04 onwards** — there are no SPY
option feature parquet files for 2020, and 2021 is partial (April–December).
The closest-to-spec alternative was used, with the partial-coverage fact
explicitly recorded in the run's `data_coverage` block instead of being papered
over.

Generate datasets (T9 mounted; `flag` policy keeps rows with missing IV-derived
features that the candidate signal does not use):

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python \
  -m scripts.run_model_ready_dataset_smoke \
  --symbol SPY \
  --analysis-start-date 2021-04-05 --analysis-end-date 2021-12-31 \
  --feature-lookback-days 120 --label-lookahead-days 45 \
  --max-files 100 --max-days 560 --daily-source yahoo \
  --horizons 1d,5d,21d --missing-feature-policy flag

PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python \
  -m scripts.run_model_ready_dataset_smoke \
  --symbol SPY \
  --analysis-start-date 2022-01-03 --analysis-end-date 2022-12-30 \
  --feature-lookback-days 120 --label-lookahead-days 45 \
  --max-files 100 --max-days 560 --daily-source yahoo \
  --horizons 1d,5d,21d --missing-feature-policy flag
```

Run cross-period validation with full data-coverage disclosure:

```bash
.venv/bin/python scripts/run_ml_regime_validation_cross_period.py \
  --train-years 2021 --test-year 2022 \
  --train-coverage-start 2021-04-05 --train-coverage-end 2021-12-31 \
  --train-is-partial \
  --data-coverage-note "T9 SPY options-features coverage begins 2021-04; train uses 2021 partial (Apr-Dec). IV features (iv_rank, iv_percentile, iv30_rv30_ratio, vol_term_structure_slope) unavailable in 2021/2022 and are not used by the frozen candidate signal." \
  --period-label "train=2021_partial; test=2022"
```

The script auto-discovers the datasets, runs the regime-validation pipeline,
attaches a `data_coverage` block to the new-period report, writes the new
period report to
`reports/ml_diagnostics/spy_2021-2022_ml_regime_validation_cross_period.json`,
aggregates with the existing 2023+2024 → 2025 report (preserved verbatim), and
writes the cross-period decision to
`reports/ml_diagnostics/ml_cross_period_validation.json`.

### Result of executed run

`cross_period_validated = false`. Headline numbers from the recorded artifacts:

| period | filtered train win5d | filtered test win5d | filtered test mean5d | sample_size_safe (train, test) | paper_ready |
|---|---|---|---|---|---|
| baseline 2023+2024 → 2025 | 68.5% (n=124) | 80.9% (n=68) | +1.01% | (False, True) | True |
| **2021_partial → 2022** | 69.7% (n=33) | **41.2%** (n=51) | **−0.52%** | (False, False) | **False** |

The 2022 bear-market regime produced a sub-50% test win rate and negative mean
forward 5-day return for the filtered candidate. Both train and test failed the
sample-size safety floor on this run — train because 2021 is partial
(Apr–Dec only, 33 filtered rows), test because the 2022 candidate population
itself is small after filtering.

### What this means for the candidate — falsification recorded

The frozen `high_vol_trend_early_candidate` signal has been **falsified for
cross-period generalization**. The result has been recorded as a permanent fact
about the signal in
`services/external_data/ml_candidate_signal_readiness.FALSIFICATION_RECORD`:

```python
FALSIFICATION_RECORD = {
    "candidate_falsified": True,
    "falsification_period": "train=2021_partial; test=2022",
    "filtered_test_win_rate": 0.4117647058823529,
    "filtered_test_mean_return": -0.005174632785788908,
    "baseline_period": "train=2023+2024; test=2025",
    "baseline_filtered_test_win_rate": 0.8088235294117647,
    "baseline_filtered_test_mean_return": 0.010125475505699285,
    "reason": "failed bear-regime cross-period validation: ...",
    "tune_or_repair_prohibited": True,
    ...
}
```

This record overrides any subsequent readiness checklist:

- `candidate_status` is now **`falsified_cross_period`** for any input.
- `candidate_ready_for_paper_observation` is **False** even when every per-run
  diagnostic criterion passes. The raw per-run pass/fail is still computed and
  surfaced as `criteria_pass_pre_falsification` for audit history.
- `governance_flags.prospective_paper_observation_allowed` is **False**.
- `governance_flags.live_integration_allowed` and `edge_claim_allowed` remain
  **False** (now triple-gated by snooping metadata, prospective-validation
  requirement, and falsification).

#### Why 2023+2024 → 2025 looked good

The original baseline reported a filtered test win rate of 80.9% and mean 5d
return of +1.01% on 2025. With hindsight, 2025 was a strong-trend bull regime
that systematically rewarded a long-biased trend filter. The signal's apparent
edge was regime-favored, not generalizing.

#### Why 2022 invalidates the candidate

2022 was a pronounced bear regime. The same frozen filter — applied unchanged
on 2021_partial → 2022 — produced a 41.2% test win rate and a −0.52% mean 5d
return on 51 filtered observations. The fact that `realized_vol_60d ≥ median ∧
price_momentum_20d > 0 ∧ distance_from_20d_mean < q70` shows positive results
in a bull year and negative results in a bear year is exactly the signature of
a regime-conditional artifact rather than a market microstructure edge.

#### Do not tune or repair this candidate on 2022

Adjusting thresholds, swapping features, or retraining anything in response to
2022 results is **explicitly prohibited** by `FALSIFICATION_RECORD`:

> The honest move is to record the falsification and stop. Tuning in response
> to a failed cross-period test is the multiple-testing / overfitting failure
> mode the snooping metadata was designed to gate.

Any further work on long-biased trend signals should restart from
pre-registered hypotheses with a multiple-testing adjustment, on data that was
not used during the PR23–PR42 exploration.

#### Audit-history preservation

The original 2023+2024 → 2025 regime-validation report
(`reports/ml_diagnostics/spy_2023-2024-2025_ml_regime_validation.json`,
generated 2026-05-01) is **not** modified. It captures the readiness checklist
as it was *before* the falsification record existed and remains the
authoritative record of the original baseline run for audit purposes. The
cross-period aggregate
(`reports/ml_diagnostics/ml_cross_period_validation.json`) records
`cross_period_validated=false` and is the authoritative falsification artifact.

### Cross-period decision logic

`cross_period_validated` is `true` only if **every** input period independently
reports `candidate_ready_for_paper_observation = true`. With fewer than two
periods the result is unconditionally `false` — a single period is by
definition not cross-period.

This decision **does not authorize live integration**. The
`SNOOPING_METADATA.live_integration_allowed` and
`governance_flags.edge_claim_allowed` constraints from the readiness checklist
are unchanged and remain `false`.

### What the frozen signal logic does NOT change

Cross-period validation reuses these unchanged:

- `realized_vol_60d >= train_period_median`
- `price_momentum_20d > 0`
- `distance_from_20d_mean < quantile(0.70)` of the high-vol-trend-positive
  train bucket
- All readiness criteria, governance flags, and snooping metadata
- All boundary purge logic
- The default 2023+2024 → 2025 run (preserved verbatim)

The only configurable inputs are `--train-years`, `--test-year`, and
`--datasets-dir`. No threshold optimization, no filter tuning, no signal
modification.

## Effective Sample Size and Option-Row Independence

Option-row join results (baseline and walk-forward) produce one row per option
contract per observation date. Because `forward_return` is derived solely from
the underlying close, all contracts sharing the same date have an identical
return. Row count therefore overstates the number of independent observations.

### Flags in every option-row report

| Field | Location | Meaning |
|---|---|---|
| `data_level` | top-level | `"option_row"` — not date-level |
| `option_row_independence_warning` | top-level | `true` when average contracts per date > 1 |
| `date_weighted_metrics_available` | top-level | `true` when return data is present |
| `effective_sample` | top-level and per-split | See structure below |

### `effective_sample` structure

```json
{
  "row_count": 65794,
  "unique_entry_dates": 21,
  "average_rows_per_date": 3133.0,
  "max_rows_per_date": 4512,
  "effective_sample_warning": true,
  "date_weighted_win_rate": 0.571,
  "date_weighted_mean_return": 0.0031,
  "date_weighted_median_return": 0.0025,
  "date_weighted_count": 21,
  "date_weighted_metrics_available": true
}
```

`date_weighted_*` metrics give each trading day equal weight regardless of how
many option contracts appear on that date. These are the primary return metrics
for evaluating option-row reports — the raw mean/median/win_rate based on row
count are inflated by contract multiplicity and should not be interpreted as
independent-observation statistics.

### Where it appears

- **`historical_baseline_report`**: `effective_sample` at report top-level,
  applied to the full joined frame.
- **`historical_rule_baseline`**: `effective_sample` at report top-level (full
  joined frame) and inside each window split (`train.effective_sample`,
  `test.effective_sample`), applied to the selected rows after entry-time
  filtering.
- **`historical_walk_forward` rule-baseline windows**: `effective_sample` inside
  each window's `rule_baseline` dict, applied to the selected rows.

### Candidate signal reports are date-level

Reports produced by `ml_candidate_signal`, `ml_candidate_signal_paper_eval`, and
the readiness checklist operate on the model-ready dataset, which is one row per
`entry_date`. These reports carry `"data_level": "date"` and are not subject to
option-row inflation. The `effective_sample_warning` does not apply to them.

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

To verify the same export contract across month boundaries, run the bounded
multi-month smoke:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_model_ready_dataset_multimonth_smoke.py
```

By default this reads only the bounded window needed for `SPY` from the
January-March 2024 analysis window:

- Analysis window: `2024-01-02` through `2024-03-29`.
- Feature lookback: `120` calendar days.
- Label lookahead: `45` calendar days.
- Maximum read window cap: `280` calendar days.

The multi-month metadata includes `monthly_summary` plus schema-stability checks
for column order, feature columns, label columns, and unexpected columns.

This is still not a model run. It performs no ML training, no threshold
optimization, no performance claim, and no full-history scan.

To verify the same export contract over one bounded calendar year, run:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_model_ready_dataset_oneyear_smoke.py
```

By default this keeps the scan bounded:

- Analysis window: `2023-01-03` through `2023-12-29`.
- Feature lookback: `120` calendar days for 60-day realized volatility and
  trailing IV-rank readiness.
- Label lookahead: `45` calendar days for 21-trading-day label readiness.
- Maximum read window cap: `560` calendar days.
- Maximum file cap: `100`.

The one-year smoke is still a reproducibility/readiness check only. It should
be used to inspect row counts, dropped rows, fully labeled rows, monthly
coverage, schema stability, null rates, and leakage checks before any broader
walk-forward modeling work.

## Tiny ML Training Smoke

After a bounded model-ready dataset exists, run a non-optimized ML pipeline
smoke:

```bash
.venv/bin/python scripts/run_ml_training_smoke.py \
  --dataset-path reports/model_ready_dataset_smoke/spy_2024-01-02_2024-03-29.csv \
  --metadata-path reports/model_ready_dataset_smoke/spy_2024-01-02_2024-03-29.metadata.json
```

The ML smoke trains one fixed-default `LogisticRegression` model on one target:

```text
forward_return_5d > 0
```

It uses a chronological train/test split only. It does not shuffle, tune
hyperparameters, optimize thresholds, promote artifacts, write registry entries,
or make performance claims. The report includes sample sizes, class balance,
accuracy, precision/recall, AUC when valid, naive-majority baseline metrics, and
explicit warnings that the sample is too small for statistical conclusions.

Reports are written only under:

```text
reports/ml_smoke/
```

To validate the same fixed-model mechanics across multiple chronological
windows, run:

```bash
.venv/bin/python scripts/run_ml_walk_forward_smoke.py \
  --dataset-path reports/model_ready_dataset_smoke/spy_2024-01-02_2024-03-29.csv \
  --metadata-path reports/model_ready_dataset_smoke/spy_2024-01-02_2024-03-29.metadata.json \
  --train-window-rows 30 \
  --test-window-rows 10 \
  --step-rows 10
```

The walk-forward smoke reports per-window train/test rows, class balance,
classification metrics, naive-majority baseline metrics, leakage checks, and
non-evaluable window reasons. Aggregates are simple averages for smoke
diagnostics only. They are not edge evidence.

The walk-forward report also includes diagnostic sections to explain fixed-smoke
behavior when it underperforms the naive baseline. These diagnostics include
per-window class imbalance, prediction distribution, probability distribution,
feature null rates, feature variance, coefficient directions, naive baseline
class choice, confusion matrix, and a compact `diagnostic_summary`. This is
inspection-only output. It must not be treated as threshold tuning,
hyperparameter tuning, model selection, or evidence of edge.

After the one-year dataset export exists, run the same fixed-model
walk-forward mechanics against that bounded dataset:

```bash
.venv/bin/python scripts/run_ml_walk_forward_smoke.py \
  --dataset-path reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.csv \
  --metadata-path reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.metadata.json \
  --train-window-rows 120 \
  --test-window-rows 20 \
  --step-rows 20
```

This remains a smoke test: fixed-default `LogisticRegression`, one target
(`forward_return_5d > 0`), chronological windows only, no tuning, no threshold
optimization, no promotion, and no edge claim.

## ML Signal Diagnostics

After the one-year dataset export and one-year walk-forward smoke exist, run
the signal diagnostics:

```bash
.venv/bin/python scripts/run_ml_signal_diagnostics.py \
  --dataset-path reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.csv \
  --metadata-path reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.metadata.json \
  --model-diagnostics-path reports/ml_smoke/spy_2023-01-03_2023-12-29_ml_walk_forward_smoke_forward_return_5d_positive.json
```

This command does not train a model. It asks whether the existing target
(`forward_return_5d > 0`) appears descriptively related to the current feature
set. The report includes:

- Target distribution: overall positive rate, monthly positive rate, rolling
  positive rate, lag-1 autocorrelation, return distribution, and simple regime
  segmentation.
- Feature diagnostics: Pearson/Spearman correlation with the binary target,
  quantile-bucket positive rates, quantile-bucket mean forward returns, and
  relationship-stability flags across chronological windows.
- Signal summary: strongest descriptive features, features with no signal,
  unstable features, and an overall `NONE` / `WEAK` / `MODERATE` assessment.
- Model-collapse diagnosis: uses the existing walk-forward smoke diagnostics to
  explain whether prediction collapse looks related to one-class predictions,
  probability clustering near 0.5, unstable coefficients, or weak feature/target
  separation.

Reports are written under:

```text
reports/ml_diagnostics/
```

## Multi-Year Diagnostics

After the bounded one-year diagnostics are working, run the multi-year
diagnostics smoke:

```bash
PIVOTQUANT_T9_ROOT=/Volumes/T9 .venv/bin/python scripts/run_ml_multiyear_diagnostics.py
```

The default bounded windows are:

- `2023-01-03` through `2023-12-29`
- `2024-01-02` through `2024-12-31` if data exists
- `2025-01-02` through `2025-12-31` if data exists

Each year is exported and evaluated separately. Missing or non-evaluable years
are reported explicitly instead of being fabricated or silently skipped. The
report includes, per year:

- Exported rows and fully labeled rows.
- Target positive rates for 1d, 5d, and 21d direction targets.
- Regime segment differences.
- Strongest descriptive features.
- Feature sign flips.
- Target comparison results.

The cross-year section reports:

- Which regime relationships persist across years.
- Which targets are stable across years.
- Which features flip signs across years.
- Whether the 21d target remains more stable or was specific to one bounded
  sample.

The final recommendation is one of:

- `keep_5d_target`
- `consider_21d_target`
- `use_regime_conditioned_targets`
- `needs_more_data`

This is still diagnostics only. It does not change labels, train models, tune
thresholds, tune hyperparameters, scan full history, or make an edge claim.

## Realized-Volatility Regime Benchmark

After the bounded yearly model-ready datasets exist, run the fixed
`realized_vol_60d` benchmark:

```bash
.venv/bin/python scripts/run_ml_regime_benchmark.py
```

This command reads the bounded yearly model-ready CSVs for 2023, 2024, and
2025. It does not read T9, train models, tune thresholds, tune hyperparameters,
or change strategy behavior.

For each year and overall it compares:

- All rows baseline.
- `realized_vol_60d` high, split by the year median.
- `realized_vol_60d` low, split by the year median.

Each group reports sample size, `forward_return_5d > 0` positive rate, mean and
median `forward_return_5d`, deltas versus the all-rows baseline, small-sample
warnings, and target summaries for 1d direction, 5d direction, 21d direction,
and 21d forward volatility.

The stability summary reports whether high-vol or low-vol conditioning persists
by year, improves versus the all-rows baseline, is directionally stable, and has
a descriptive effect size large enough to justify further validation. This is a
benchmark diagnostic only; it is not an edge claim and does not alter any model
or runtime path.

This is signal forensics only. It must not be used as threshold tuning,
hyperparameter tuning, model selection, or evidence of tradable edge.

## Realized-Volatility Regime Validation

After the PR23 benchmark report exists, run the strict train/test validation:

```bash
.venv/bin/python scripts/run_ml_regime_validation.py
```

This is a validation-only command. It reads the bounded yearly model-ready CSVs
for 2023, 2024, and 2025, trains no model, tunes no parameter, changes no
filter, and adds no features.

The validation contract is fixed:

- Train period: 2023 and 2024.
- Test period: 2025 only.
- Regime feature: `realized_vol_60d`.
- Bucket logic: same as PR23, high when `realized_vol_60d` is greater than or
  equal to the median and low otherwise.
- Split source: the 2023-2024 train-period median only.

The report computes only sample size, `forward_return_5d > 0` positive rate,
mean `forward_return_5d`, train-vs-test deltas, win-rate stability versus the
train period, and a degradation metric. It emits:

- `validated`: true only when the 2023-2024 high-vol advantage and low-vol
  disadvantage both survive in 2025 for win rate and mean return.
- `degradation_warning`: true when that strict direction check fails.

This report is not an edge claim. It only answers whether the previously
observed `realized_vol_60d` descriptive relationship survives one fixed
out-of-sample year under unchanged bucket logic.

The same report also includes a 2D diagnostic conditioning table. This adds one
observable, fixed second variable:

- Vol bucket: the unchanged `realized_vol_60d` train-period median split.
- Trend bucket: `price_momentum_20d > 0` versus `price_momentum_20d < 0`.

The 2D table reports train and test sample size, 5d win rate, mean 5d return,
deltas versus the all-rows baseline, train-vs-test degradation, and a
`stable_bucket` flag per combination. This is diagnostic only: it does not
change filters, tune a threshold, add a feature to a model, or select a
strategy.

The report also includes a time-slice robustness check for the one bucket that
survived the 2D diagnostic: `high_vol_trend_positive`. The 2025 test period is
split into calendar quarters, and each available quarter reports sample size,
5d win rate, and mean 5d return. `robust_across_time` is true only if every
available quarter has rows and is positive on both win rate and mean return.
`slice_instability_warning` flags concentration in one period or a failed
quarter. This does not change the signal definition or add any new filter.

When a time slice fails, the report includes failure-explanation diagnostics
for the same fixed `high_vol_trend_positive` bucket. It compares failing
`2025_Q1` against working `2025_Q2` through `2025_Q4` using observable
variables only:

- `price_momentum_5d`
- `abs_price_momentum_5d`
- trailing `realized_vol_20d` computed from `underlying_price`

The comparison reports means, distribution summaries, differences, and whether
any variable is a candidate explanatory variable. This is for diagnosis only:
the failing period is not removed, no filter is introduced, and no threshold is
optimized.

A volatility-regime-change diagnostic then tests whether volatility expansion
versus compression explains the same failure. It defines:

- `vol_regime_change = realized_vol_20d - realized_vol_60d`
- `vol_expansion`: `vol_regime_change > 0`
- `vol_compression`: `vol_regime_change <= 0`

For the fixed `high_vol_trend_positive` bucket, the report compares expansion
and compression in train and test, then compares `2025_Q1` against `2025_Q2`
through `2025_Q4`. The flag `vol_expansion_explains_failure` is diagnostic
only; it does not introduce a volatility-expansion filter or change the signal.

The trend-maturity diagnostic tests whether overextension explains the same
failure. It prefers:

- `distance_from_20d_mean = (price - rolling_mean_20d) / rolling_std_20d`

If that cannot be computed, it falls back to the existing `price_momentum_20d`.
Inside the fixed `high_vol_trend_positive` bucket, the top 30% of train-period
maturity values are labeled `late_trend`; all remaining rows are
`early_trend`. The train threshold is then applied unchanged to test and to
`2025_Q1` versus `2025_Q2` through `2025_Q4`. The flag
`trend_maturity_explains_failure` is diagnostic only; it does not create an
overextension filter or alter the signal.

The trend-maturity section also includes a quarter-level stability check over
the 2025 test period. For each quarter it reports late-trend win rate,
early-trend win rate, the late-minus-early difference, and sample size.
`trend_maturity_stable` is true only when early trend outperforms late trend in
at least three of the four test quarters. This checks whether the maturity
effect is persistent or concentrated in one period.

The report then measures the impact of excluding late-trend rows without
changing the signal definition. Inside the same fixed
`high_vol_trend_positive` bucket and using the same train-derived late-trend
threshold, it compares:

- `baseline_no_filter`
- `early_trend_only`

For train and test, the table reports sample size, 5d win rate, mean 5d return,
and the deltas from removing late-trend rows. The flag
`filter_improves_performance` is true only if the diagnostic removal improves
both win rate and mean return in train and test. This is still measurement
only: it does not change any live, research, or governance filter.

The soft-penalty diagnostic compares three non-live scenarios inside that same
bucket:

- `baseline_no_adjustment`: all rows at weight `1.0`.
- `hard_filter_early_trend_only`: late-trend rows removed.
- `soft_penalty_late_trend_half_weight`: all rows retained, but late-trend rows
  receive weight `0.5`.

For train and test it reports raw sample size, effective weighted sample size,
weighted 5d win rate, and weighted mean 5d return. The flag
`soft_penalty_preferred` is true only if the soft penalty improves both metrics
versus baseline, keeps all raw rows, has a larger effective sample than the
hard filter, and retains at least half of the hard-filter improvement in train
and test. This is diagnostic only and does not apply weights to scoring.

The overextension-method comparison then asks whether the current maturity
definition is robust or easily replaced. It compares fixed definitions inside
the same `high_vol_trend_positive` bucket:

- `distance_from_20d_mean`, the current method.
- Bollinger-style distance: `(price - rolling_mean_20d) / (2 * rolling_std_20d)`.
- `rsi_14`, with a fixed late-trend threshold of `RSI > 70`.
- ATR-style distance: `(price - rolling_mean_20d) / ATR_14`, only when real
  high, low, and close inputs exist.
- `cumulative_return_20d`, using `price_momentum_20d`.

For train-fitted methods, late trend is the top 30% of train-bucket values and
the threshold is applied unchanged to test. The report ranks methods by train
and test deltas in win rate and mean return versus the no-filter baseline.
`current_method_optimal` is true when `distance_from_20d_mean` ranks in the top
two. This ranking is diagnostic only and does not replace the current method.

The report also tests whether trend maturity is additive or just another view
of existing momentum. Inside the same fixed `high_vol_trend_positive` bucket,
it splits both `price_momentum_20d` and `distance_from_20d_mean` by medians fit
on train rows only. This creates four diagnostic groups:

- `low_momentum_early_trend`
- `low_momentum_late_trend`
- `high_momentum_early_trend`
- `high_momentum_late_trend`

For train and test, the report compares early versus late trend separately
inside low-momentum and high-momentum rows. The flag
`trend_maturity_independent` is true only when early-versus-late separation
appears inside both momentum buckets in train and test. This remains diagnostic
only: it does not introduce a momentum filter, a trend-maturity filter, or a
new trading rule.

The report also includes an overextension fragility diagnostic that tests
whether the hard overextension filter creates fragile or overly selective
behavior. It uses the same `high_vol_trend_positive` bucket and the same
train-derived `distance_from_20d_mean` late-trend threshold (top 30%) as the
soft-penalty and filter-impact sections. It compares two scenarios:

- `baseline_no_adjustment`: all rows retained.
- `hard_filter_early_trend_only`: late-trend rows removed.

For train and test, the diagnostic reports:

- `total_rows`, `rows_kept`, `rows_removed`, `percent_removed`.
- `mean_5d_return`, `win_rate`.
- `return_variance_across_time_slices`: variance of per-calendar-quarter mean
  returns, measuring whether performance is stable or concentrated.
- `avg_selected_rows_per_quarter`, `min_selected_rows_per_quarter`,
  `num_low_sample_quarters`: sample-size stability across time periods.

For the test period, a per-quarter table reports baseline rows, hard-filter
rows, percent removed, hard-filter mean 5d return, and hard-filter win rate for
each available calendar quarter.

Three flags summarize the safety assessment:

- `sample_size_safe`: `false` if any test quarter has fewer than 10 hard-filter
  rows after applying the late-trend exclusion.
- `overfiltering_risk`: `true` if the hard filter removes more than 50% of test
  rows overall.
- `fragility_warning`: `true` if the hard-filter test mean return is positive
  but only one calendar quarter contributes a positive mean return (performance
  is concentrated).

This diagnostic is read-only. It does not train, tune thresholds, change
filters, add features, or alter any live, research, or governance behavior.
The flags are informational: they inform whether the hard-filter improvement
seen in prior runs is robust enough to consider integration, not a decision
gate.

The report also includes a late-trend removal validation that applies strict
train/test validation criteria (same as PR24) to both the baseline and the
filtered scenario:

- `baseline`: `high_vol_trend_positive` — the same 2D bucket as PR25.
- `filtered`: `high_vol_trend_positive AND early_trend_only` — baseline rows
  after late-trend exclusion using the train-derived top-30% threshold.

For each scenario, the signal is `validated` only when both conditions hold:

- The win rate delta versus all rows is positive in train AND test.
- The mean return delta versus all rows is positive in train AND test.

The report emits:

- `baseline_validation`: sample sizes, win rate, mean return, per-period
  deltas versus all rows, per-check validation flags, and `validated`.
- `filtered_validation`: the same fields for the early-trend-only scenario.
- `improvement_summary`: rows removed from train and test, test win rate
  change, and test mean return change from baseline to filtered.
- `late_trend_removal_fixes_signal`: `true` when the filtered scenario
  passes strict validation. This flag answers the core question: does
  removing overextended trend convert a failing signal into a valid one?

No training, threshold optimization, or filter change is performed. The
threshold is fixed from the train bucket and applied unchanged to test.

### Candidate Signal Specification (`candidate_signal_diagnostics`)

PR37 defines the validated filtered signal as a formal entry condition
candidate. No live trading is enabled, no model is trained, and no governance
promotion is performed.

**Signal name:** `high_vol_trend_early_candidate`

**Formula (three conditions, all must be true):**

```
realized_vol_60d >= vol_split_value          -- high-vol regime
AND price_momentum_20d > 0                   -- positive trend direction
AND distance_from_20d_mean < maturity_threshold  -- early trend (not overextended)
```

**Required inputs:**

- `realized_vol_60d`: 60-day realized volatility of the underlying.
- `price_momentum_20d`: 20-day price momentum (sign encodes trend direction).
- `distance_from_20d_mean`: z-score distance of current price from its 20-day
  rolling mean (pre-computed by `_add_explanatory_variables`).
- `forward_return_5d`: 5-day forward return for outcome evaluation.

**Threshold source (train period only, no test data used):**

- `vol_split_value` = median of `realized_vol_60d` across all train rows.
- `maturity_threshold` = quantile(0.70) of `distance_from_20d_mean` within
  the `high_vol_trend_positive` train bucket
  (`realized_vol_60d >= vol_split_value AND price_momentum_20d > 0`).
- Late-trend rows are those where `distance_from_20d_mean >= maturity_threshold`
  (top 30% of the bucket). The candidate signal excludes these.

**Intended interpretation:**

Diagnostic candidate only. The signal specification defines the exact entry
condition that survived strict PR24-style validation after removing overextended
trend rows. It requires prospective validation and governance review before any
live trading consideration.

**Limitations:**

- Thresholds are derived from limited historical data (2023–2024 train period).
- No prospective validation has been performed.
- No edge claim is made.
- Sample sizes may be insufficient in some quarters.

**Report fields (`candidate_signal_diagnostics`):**

- `status`: `"ok"` if thresholds could be derived, `"missing"` otherwise.
- `signal_name`: `"high_vol_trend_early_candidate"`.
- `formula`: the three conditions with operator.
- `thresholds`: `vol_split_value`, `maturity_threshold`, `threshold_source`,
  `vol_split_method`, `maturity_threshold_method`.
- `spec`: guard flags — `live_trading_enabled`, `model_training_performed`,
  `threshold_optimization_performed`, `governance_promotion_performed`,
  `performance_claim` — all `false`.
- `train` / `test`: per-period stats — `baseline_rows` (high_vol_trend_positive
  before maturity filter), `signal_rows` (after maturity filter), `late_trend_excluded`,
  `win_rate_5d`, `mean_return_5d`, `sample_size_safe` (all quarters ≥ 10 rows).
- `disclaimer`: `"no edge claim; diagnostic and specification only"`.
- `definitions`: repeated flags confirming no training, tuning, or live use.

**Key invariants tested:**

- `CandidateSignalSpec()` is immutable; all guard flags default to `False`.
- `derive_candidate_signal_thresholds` uses only train data; the maturity
  threshold does not change when the test frame is substituted.
- `apply_candidate_signal` applies fixed thresholds without re-deriving them
  from test data (no test leakage).
- Changing the test frame alters test-period stats but never the thresholds or
  train-period stats.

### Paper Evaluation Harness (`paper_eval_diagnostics`)

PR38 adds a paper evaluation harness that applies the fixed candidate signal to
the test period and records one entry per qualifying row. No live trading, no
execution assumptions, no slippage, no commission, and no edge claim.

**Source module:** `services/external_data/ml_candidate_signal_paper_eval.py`

**Inputs:**

- `train_frame` — pre-processed frame (distance_from_20d_mean already computed).
- `test_frame` — same pre-processing requirement.
- Thresholds derived from `train_frame` only via `derive_candidate_signal_thresholds`.

**Paper entry fields (one dict per qualifying test row):**

| Field | Description |
|---|---|
| `entry_date` | Row date string |
| `horizon` | Fixed `"5d"` |
| `forward_return_5d` | Outcome for that entry |
| `signal_metadata` | `realized_vol_60d`, `price_momentum_20d`, `distance_from_20d_mean`, `vol_split_value`, `maturity_threshold` |

**Evaluation summary fields (`test.summary`):**

| Field | Description |
|---|---|
| `total_paper_entries` | Total rows selected by the signal in the test period |
| `win_rate` | Fraction of entries with `forward_return_5d > 0` |
| `mean_return` | Mean of `forward_return_5d` across entries |
| `median_return` | Median of `forward_return_5d` |
| `best_return` | Maximum single-entry return |
| `worst_return` | Minimum single-entry return |
| `excluded_late_trend_count` | Rows in `high_vol_trend_positive` baseline excluded by the maturity filter |
| `quarterly_breakdown` | Per-quarter list (see below) |
| `sample_size_warning` | `true` when total < 10 or any non-zero quarter has < 10 entries |

**Quarterly breakdown** (list ordered by calendar quarter):

Each item includes `quarter` (e.g. `"2025Q1"`), `entries`, `win_rate`, and
`mean_return`. Zero-entry quarters are always included with `entries=0` and
`win_rate`/`mean_return` set to `null`. This ensures the report reflects the
full test period, not just periods with signal activity.

**Explicit flags (`flags` key):**

```json
{
  "live_trading_enabled": false,
  "execution_assumptions_included": false,
  "slippage_mode": "none",
  "commission_mode": "none",
  "edge_claim": false
}
```

**Report structure:**

- `status`: `"ok"` or `"missing"`.
- `thresholds`: `vol_split_value`, `maturity_threshold`, `threshold_source`.
- `flags`: the five explicit flags above.
- `train.summary`: evaluation summary for the train period (reference only).
- `test.summary`: evaluation summary for the test period.
- `test.entries`: full list of paper entry dicts.
- `disclaimer`: `"no edge claim; paper evaluation only; no execution assumptions"`.
- `definitions`: repeated safety flags (`training_performed`,
  `threshold_optimization_performed`, `filter_changes_performed`,
  `live_trading_enabled`, `governance_promotion_performed`) all `false`.

**Key invariants tested:**

- Only rows satisfying all three signal conditions appear in `test.entries`;
  rows failing any single condition (low vol, negative trend, late trend) are
  absent.
- Thresholds are derived from `train_frame` only; substituting a different test
  frame does not change `thresholds`, `threshold_source`, or train summary.
- All five live-trading / execution flags are `false`; `PaperEvalConfig()` has
  `slippage_mode = "none"` and `commission_mode = "none"` by construction.
- Every calendar quarter present in the test frame appears in
  `quarterly_breakdown`, including quarters with zero signal entries.

### Prospective Stability Diagnostics (`test.stability`)

PR39 extends the paper evaluation harness with monthly sub-window analysis of
the test period. PR41 adds sample-awareness so that months with too few entries
do not trigger mature-month warnings. No live trading, model training, threshold
tuning, or governance change is performed.

**Monthly breakdown** (`test.stability.monthly_breakdown`):

Every calendar month present in the test frame is included in the list, even
when the signal selected no rows for that month. Per-month fields:

| Field | Description |
|---|---|
| `month` | `"YYYY-MM"` period string |
| `entries` | Signal rows selected in that month |
| `low_sample` | `true` when `0 < entries < min_month_entries_for_warning` (under-sampled, not absent) |
| `win_rate` | `null` when `entries = 0` |
| `mean_return` | `null` when `entries = 0` |
| `median_return` | `null` when `entries = 0` |
| `worst_return` | `null` when `entries = 0` |
| `best_return` | `null` when `entries = 0` |
| `positive_return_sum` | Sum of positive `forward_return_5d` values; `0.0` when `entries = 0` |

Zero-entry months are **absent** (`low_sample = false`). Only months with
`0 < entries < min_month_entries_for_warning` are considered under-sampled
(`low_sample = true`).

**Stability flags** (`test.stability.flags`):

| Flag | Criterion |
|---|---|
| `low_sample_month_warning` | `true` if any month has `0 < entries < min_month_entries_for_warning` (default 5) |
| `negative_mature_month_warning` | `true` if any **mature** month (`entries ≥ min_month_entries_for_warning`) has `win_rate < 0.5` **AND** `mean_return < 0` |
| `concentration_warning` | `true` if any month contributes `>50%` of total entries **OR** `>50%` of total positive return |
| `stability_flag` | `true` only when `negative_mature_month_warning` is `false`, `concentration_warning` is `false`, **and at least one mature month exists** |

**`min_month_entries_for_warning`** is documented as a top-level key in the
`test.stability` block (default: `5`). It separates absent months (entries=0)
from under-sampled months (1–4 entries) from mature months (≥5 entries).

**Flag criteria rationale:**

- `negative_mature_month_warning` only fires for mature months, so a single
  losing trade in a one-entry month cannot falsely suppress `stability_flag`.
- `low_sample_month_warning` is informational: it flags under-sampled months
  without penalising the signal unless a mature negative month also exists.
- `stability_flag` requires at least one mature month to be meaningful — if all
  entries are in under-sampled months, there is insufficient evidence to claim
  stability.
- `concentration_warning` catches two distinct risks: entry-count skew (most
  trades clustered in one calendar month) and return-attribution skew (most
  positive return earned in a single lucky month).
- `stability_flag = True` is a necessary but not sufficient condition for
  signal confidence; it does not constitute an edge claim.

**Stability block definitions** (always present):

```json
{
  "training_performed": false,
  "threshold_optimization_performed": false,
  "filter_changes_performed": false,
  "live_trading_enabled": false
}
```

**Report location:** `test.stability` inside the `paper_eval_diagnostics` key
of the full validation report.

**Key invariants tested:**

- `negative_mature_month_warning` fires only for months with `entries ≥ 5`
  that have `win_rate < 0.5` AND `mean_return < 0`; low-sample months are
  excluded from the mature-warning check.
- `low_sample_month_warning` fires whenever any month has `0 < entries < 5`.
- `concentration_warning` fires when one month holds `> 50%` of total entries
  (confirmed via entry-count ratio) or `> 50%` of total positive return
  (confirmed via `positive_return_sum` ratio).
- Every calendar month in the test frame appears in `monthly_breakdown`; months
  with no signal entries have `entries = 0`, `low_sample = false`,
  `win_rate = null`, `mean_return = null`, and `positive_return_sum = 0.0`.
- All top-level execution flags (`live_trading_enabled`,
  `execution_assumptions_included`, `slippage_mode`, `commission_mode`,
  `edge_claim`) remain unchanged from PR38 defaults.

### Maturity Threshold Sensitivity Analysis (`sensitivity_diagnostics`)

PR40 tests whether the signal is robust to the specific quantile chosen to
define the late-trend threshold. Seven quantile values are swept using
**train-only** data for each threshold derivation. No threshold is selected or
promoted; the analysis is purely diagnostic.

**Source module:** `services/external_data/ml_candidate_signal_sensitivity.py`

**Quantile grid:** `[0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85]`  
**Reference quantile:** `0.70` (the fixed candidate-signal choice from PR37)

**Per-quantile fields (`sensitivity_grid` list, one entry per quantile):**

| Field | Description |
|---|---|
| `quantile` | The quantile value swept |
| `maturity_threshold` | `distance_from_20d_mean` threshold derived from train at that quantile |
| `baseline_rows` | Rows in `high_vol_trend_positive` bucket in the test frame |
| `signal_rows` | Rows satisfying `distance < maturity_threshold` in the test frame |
| `late_trend_excluded` | `baseline_rows − signal_rows` |
| `win_rate` | Fraction with `forward_return_5d > 0`; `null` when `signal_rows = 0` |
| `mean_return` | Mean `forward_return_5d`; `null` when `signal_rows = 0` |
| `stability_flag` | PR39/PR41 monthly stability flag applied at this threshold |
| `negative_mature_month_warning` | PR41 mature-month negative-direction flag at this threshold |
| `low_sample_month_warning` | PR41 under-sampled month flag at this threshold |
| `concentration_warning` | PR39 monthly concentration flag at this threshold |

**Top-level output fields:**

| Field | Description |
|---|---|
| `threshold_robust` | See criterion below |
| `reference_quantile` | `0.70` |
| `vol_split_value` | Train-period median of `realized_vol_60d` (same across all quantiles) |

**`threshold_robust` criterion:**

`true` if the reference quantile (0.70) has `mean_return > 0` **and** at
least 4 of the remaining 6 quantiles also have `mean_return > 0`. A result of
`true` indicates the sign of the signal's mean return is stable across nearby
threshold choices; it does not constitute an edge claim.

**Monotonicity invariant:**

Because `distance < threshold` widens as the threshold rises, `signal_rows` is
non-decreasing as quantile increases. This is tested explicitly and serves as
an internal consistency check on the threshold derivation.

**Explicit flags:**

```json
{
  "live_trading_enabled": false,
  "threshold_optimization_performed": false,
  "edge_claim": false
}
```

**Key invariants tested:**

- `signal_rows` is non-decreasing as quantile increases (monotonicity).
- The reference quantile (`0.70`) always appears in `sensitivity_grid`.
- `threshold_optimization_performed = false` and `live_trading_enabled = false`
  in both flags and definitions; disclaimer contains `"no threshold selection"`.
- `threshold_robust = True` when all quantiles share positive `mean_return`;
  `threshold_robust = False` when all quantiles have negative `mean_return`.

### Train/Test Boundary Purge (`boundary_purge_report`)

Audit-Fix PR1 adds an explicit boundary contamination check and purge before any
threshold derivation or performance measurement. Train rows whose forward label
extends into the test period are removed. No ML, no tuning, no T9 mutation.

**Source module:** `services/external_data/ml_boundary_purge.py`

**Why it is needed:** For a train entry on date E with horizon H, `label_date =
E + H business days`. If `label_date >= test_start`, the outcome uses test-period
prices and contaminates train performance evidence. Example: a 2024-12-26 entry
with `forward_return_5d` uses the 2025-01-02 close, which is in the test set.

**Embargo:** `MAX_EVALUATED_HORIZON_BDAYS = 5` (the candidate signal horizon).
Business days are counted with `pd.offsets.BDay` (weekdays only, no market-holiday
adjustment). For the 2023–2024 train / 2025 test split, this purges the 4 trading
days between 2024-12-26 and 2024-12-31 inclusive (labels reach 2025-01-02 or later).

**Purge report fields:**

| Field | Description |
|---|---|
| `boundary_label_overlap_detected` | `true` if any train row had `label_date >= test_start` before purge |
| `boundary_purge_applied` | `true` if the contaminated rows were removed |
| `train_rows_before_purge` | Train row count before purge |
| `train_rows_after_purge` | Train row count after purge |
| `rows_purged` | Rows removed |
| `max_label_date_retained` | Latest `label_date` among retained train rows |
| `test_start` | Earliest `entry_date` in the test frame (ISO string) |
| `embargo_horizon_bdays` | Business days used as the embargo window |

**Threshold derivation order:** the purge is applied before `split_value` and
`maturity_threshold` are derived, so all thresholds come from the contamination-free
train frame.

**`label_date` column in exports:** `model_ready_dataset_export.py` now pivots
`label_date` per horizon from `label_candidates` and emits `label_date_1d`,
`label_date_5d`, and `label_date_21d` columns in new exports. Existing CSVs that
predate this change do not have these columns; the purge falls back to computing
`entry_date + H business days` automatically.

**Readiness gate:** `candidate_readiness_checklist` includes a `boundary_clean`
criterion. It is `false` — and readiness fails — when `boundary_label_overlap_detected
= true` AND `boundary_purge_applied = false`. If the purge was applied (or no
overlap was detected), `boundary_clean = true`.

**Key invariants tested (15 tests in `test_ml_boundary_purge.py`):**

- Rows with `entry_date + 5 bdays >= test_start` are removed.
- Rows with `entry_date + 5 bdays < test_start` are fully retained.
- The test frame is not altered by `apply_boundary_purge`.
- `derive_candidate_signal_thresholds` called on the purged frame produces a
  different `maturity_threshold` than on the original when the purged row has an
  extreme `distance_from_20d_mean` value.
- When `label_date_5d` column is present, it is used directly instead of the fallback.
- `boundary_clean = false` when overlap is detected and purge is not applied.
- `boundary_clean = true` when overlap is detected and purge IS applied.
- Empty train frame returns `status = "skipped"` without raising.

### Candidate Signal Readiness Checklist (`candidate_readiness_checklist`)

Audit-Fix PR1 adds `boundary_clean` as the tenth readiness criterion. PR42
aggregates results from the preceding diagnostic layers into a single
paper-observation readiness decision. Audit-Fix PR3 adds snooping-risk metadata
and a three-state governance classification that prevents the candidate from ever
being promoted to live integration through this checklist alone.

No data processing, ML, tuning, or live trading is performed — inputs are
pre-computed diagnostic dicts.

**Source module:** `services/external_data/ml_candidate_signal_readiness.py`

**Top-level decision fields:**

| Field | Type | Description |
|---|---|---|
| `candidate_ready_for_paper_observation` | `bool` | `true` only when all ten criteria below pass |
| `candidate_status` | `str` | `exploratory_paper_candidate` or `blocked` — never `ready_for_live` |

**Governance flags** (`governance_flags`):

| Flag | Value | Rationale |
|---|---|---|
| `edge_claim_allowed` | always `false` | No pre-registration; in-sample results only |
| `live_integration_allowed` | always `false` | `pre_registered=false`, `multiple_testing_adjustment_applied=false`, `prospective_validation_required=true` |
| `prospective_paper_observation_allowed` | `true` when candidate ready, else `false` | Controlled paper observation is the ceiling status |

**Snooping-risk metadata** (`snooping_metadata`):

| Field | Value | Meaning |
|---|---|---|
| `diagnostics_explored_count` | `6` | Number of diagnostic modules examined before signal was frozen |
| `candidate_discovered_after_diagnostics` | `true` | Signal was selected post-exploration, not pre-registered |
| `pre_registered` | `false` | No hypothesis was registered before data inspection |
| `multiple_testing_adjustment_applied` | `false` | No Bonferroni / FDR correction applied |
| `prospective_validation_required` | `true` | Forward-looking paper observation must precede any edge claim |

These fields are frozen constants. The `candidate_status` can never reach
`ready_for_live` while any of `pre_registered`, `multiple_testing_adjustment_applied`
remain `false` or `prospective_validation_required` remains `true`.

**Governance interpretation:**
> The `high_vol_trend_early_candidate` signal is **exploratory**. All diagnostic
> criteria may pass, but the signal was discovered after examining six diagnostic
> modules without pre-registration. Paper observation under frozen conditions is
> the maximum permissible activity. No edge claim is made. No live integration
> is permitted from this pipeline.

**Ten criteria (all must hold for `true`):**

| Criterion | Source | Pass condition |
|---|---|---|
| `filtered_validated` | `late_trend_removal_validation` | filtered signal beats `all_rows` on win rate and mean return in both train and test |
| `sample_size_safe` | `candidate_signal_diagnostics` | all test-period quarters have ≥ 10 signal entries |
| `overfiltering_risk` | `overextension_fragility_diagnostics` | `false` — late-trend filter removes ≤ 50% of baseline rows |
| `fragility_warning` | `overextension_fragility_diagnostics` | `false` — positive return not concentrated in a single quarter |
| `stability_flag` | `paper_eval_diagnostics` | `true` — no mature-negative month, no concentration, ≥ 1 mature month |
| `concentration_warning` | `paper_eval_diagnostics` | `false` — no month holds > 50% of entries or > 50% of positive return |
| `threshold_robust` | `sensitivity_diagnostics` | reference quantile has `mean_return > 0` and ≥ 4 of 6 others agree |
| `boundary_clean` | `boundary_purge_report` | `true` if no overlap detected, or if overlap was detected AND purge was applied |
| `live_trading_enabled` | `ReadinessConfig` | always `false` |
| `edge_claim` | `ReadinessConfig` | always `false` |

`boundary_clean = false` (readiness fails) when `boundary_label_overlap_detected = true`
AND `boundary_purge_applied = false` — i.e., contaminated train labels were not removed.

**Frozen signal definition** (`frozen_signal_definition`):

The readiness checklist includes a frozen specification of the signal as of the
PR42 validation run. This section is documentation only and does not enable live
trading or constitute an edge claim.

| Condition | Feature | Operator | Threshold source |
|---|---|---|---|
| 1 | `realized_vol_60d` | `>=` | median of `realized_vol_60d` across train period rows |
| 2 | `price_momentum_20d` | `>` | fixed zero; no derivation required |
| 3 | `distance_from_20d_mean` | `<` | `quantile(0.70)` of `distance_from_20d_mean` in the high-vol positive-trend train bucket |

All three conditions are joined by `AND`. Thresholds are derived from the train
period only. The `freeze_note` field states: any change to conditions, features,
or threshold derivation method requires a new full validation run before further
paper observation.

**Conservative defaults for missing inputs:**

If any sub-report is absent or contains `null` values, `_extract_criteria`
defaults conservatively:

- Positive-sense criteria (`filtered_validated`, `sample_size_safe`,
  `stability_flag`, `threshold_robust`) default to `false`.
- Warning-sense criteria (`overfiltering_risk`, `fragility_warning`,
  `concentration_warning`) default to `true`.

This ensures a missing diagnostic can never produce a spurious ready=True.

**Sample-size caveats** (`sample_size_caveats` list):

- Paper eval train period may have low-sample quarters (`sample_size_warning`).
- Test period monthly breakdown may have `low_sample` months (entries < 5).
- `sample_size_safe` evaluates test-period quarters only (threshold: ≥ 10).
- Signal observation in low-sample months should be interpreted with caution.

**Definitions block** (always present, all `false`):

```json
{
  "training_performed": false,
  "threshold_optimization_performed": false,
  "filter_changes_performed": false,
  "live_trading_enabled": false,
  "governance_promotion_performed": false
}
```

**Key invariants tested (38 tests across two test files):**

*`test_ml_candidate_signal_readiness.py` (16 tests — diagnostic criteria):*
- All ten criteria passing → `candidate_ready_for_paper_observation = true`.
- Each criterion failing individually → `false`, with all others held passing.
- `live_trading_enabled` and `edge_claim` are always `false` in both `flags` and
  `criteria`.
- All five `definitions` keys are always `false`.
- `frozen_signal_definition` contains the correct threshold sources and
  `freeze_note`.
- Empty or `null` sub-report inputs default conservatively to not-ready.
- `criteria` dict contains exactly the ten expected keys, each typed `bool`.
- Disclaimer contains `"edge claim"`.

*`test_ml_candidate_signal_governance.py` (22 tests — governance and snooping):*
- `candidate_status` is never `ready_for_live` for any criteria outcome.
- `candidate_status` is `exploratory_paper_candidate` when all criteria pass.
- `candidate_status` is `blocked` when any criterion fails.
- `live_integration_allowed` is always `false` (both via helper and full report).
- `edge_claim_allowed` is always `false` (both via helper and full report).
- No multiple-testing adjustment → `live_integration_allowed = false` even when
  all other criteria pass.
- `prospective_paper_observation_allowed` matches `candidate_ready_for_paper_observation`.
- `snooping_metadata` contains all six required keys with correct values.
- `DIAGNOSTICS_EXPLORED_COUNT = 6`.
- `snooping_metadata` is invariant across criteria outcomes (it describes process,
  not result).
- Disclaimer mentions `"exploratory"`, `"no statistical edge claim"`, and
  `"prospective"`.

## Regime And Target Diagnostics

After the one-year model-ready dataset exists, run regime and target
diagnostics:

```bash
.venv/bin/python scripts/run_ml_regime_target_diagnostics.py \
  --dataset-path reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.csv \
  --metadata-path reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.metadata.json
```

This command does not train models and does not change labels. It asks whether
the observed `forward_return_5d > 0` signal appears regime-dependent and
whether alternate targets look descriptively more stable.

Regime diagnostics segment the bounded dataset by:

- `realized_vol_60d` high/low.
- `vix_level` high/low.
- `price_momentum_20d` positive/negative.
- `iv30_rv30_ratio` high/low.
- `vol_term_structure_slope` high/low.

Each segment reports sample size, `forward_return_5d > 0` positive rate, mean
`forward_return_5d`, feature-correlation stability, and feature-bucket
separation stability.

Target diagnostics compare:

- `forward_return_1d > 0`.
- `forward_return_5d > 0`.
- `forward_return_21d > 0`.
- Absolute `forward_return_5d` move.
- `forward_volatility_21d`.

The report includes a conservative recommendation:

- `keep_target`
- `change_target`
- `needs_more_data`

The recommendation is diagnostic only. It does not alter training labels,
thresholds, model selection, registry behavior, or promotion logic. The report
explicitly warns that 5-day and 21-day forward labels overlap across adjacent
entry dates and can inflate autocorrelation or stability diagnostics.

Reports are written under:

```text
reports/ml_diagnostics/
```
