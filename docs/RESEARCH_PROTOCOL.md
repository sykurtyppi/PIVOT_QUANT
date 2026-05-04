# PivotQuant Research Protocol

**Status:** v1, drafted 2026-05-04 following the falsification of
`high_vol_trend_early_candidate` on the 2021_partial → 2022 cross-period run.

**Audience:** Anyone proposing, implementing, or reviewing a new candidate
signal in this repository.

**Posture:** Skeptical by default. The previous candidate produced a 80.9%
test win rate on 2025 and a 41.2% test win rate on 2022 with the same frozen
filter — i.e., the original "edge" was a regime-favored artifact that survived
six diagnostic modules and an LLM-assisted audit. This protocol is written
under the assumption that the next researcher (human or model) is equally
prone to overfitting and equally good at convincing themselves otherwise. The
purpose of every section below is to make it harder to lie to oneself, not
easier to ship a signal.

This document is **process design only**. It does not propose a signal, a
feature, or a target. Implementing the rules below is a precondition for any
future candidate work.

---

## 0. Failure-Detection First

Read this before reading anything else.

The protocol is structured as a *falsification ladder*: every stage exists to
kill the candidate, not to confirm it. A signal that "passes" a stage has
merely failed to be killed yet. Confirmation bias must be assumed at every
step.

The following inversions of normal research instinct are mandatory:

- A passing result is the *least* informative outcome at every stage.
- A near-passing result that becomes passing after a small adjustment is
  *evidence against* the candidate, not evidence in its favor.
- "It almost worked, but…" is the canonical signature of overfitting.
- A failed result on a held-out period is *information*, not a problem to be
  fixed.
- The desire to retry, retune, or re-test a killed candidate is itself a
  signal that the discipline is working — and a signal to harden the kill,
  not to relax it.

If, while applying this protocol, you find yourself reasoning "I just need to
adjust X and re-run Stage Y," you have already left the protocol. Stop. File
a new pre-registration as a new hypothesis or close the candidate.

---

## 1. Pre-Registration Template

Every candidate **must** be pre-registered before *any* statistic is computed
on validation or holdout data. The pre-registration is a frozen JSON document
written to `reports/research_protocol/registrations/{candidate_id}.json` and
hashed with SHA256. The hash is referenced in every downstream artifact.

The registration **must** contain the following fields. None may be added,
removed, or modified after the file is committed.

### 1.1 Hypothesis (`hypothesis`)

A statement of the **economic or mechanistic** reason the signal might exist.
Statistical patterns observed in past data are forbidden as the hypothesis.
The hypothesis must reference a real-world mechanism — order flow,
microstructure friction, dealer hedging, calendar effects, behavioral
overreaction — that is independently testable.

| Acceptable | Rejected |
|---|---|
| "Post-FOMC announcement, dealer gamma unwinding compresses near-dated IV by ≥X bp on average; we expect short-vega put spreads opened the morning after FOMC to capture this." | "When realized vol is high and trend is up and we're not too late, returns look good." |
| "Friday-to-Monday weekend decay should be larger for stocks with high overnight gap variance because option market makers cannot rebalance over the weekend." | "We noticed in 2024 that..." |

The hypothesis text must include:
- The mechanism (one paragraph).
- Why the mechanism *should* produce a positive expected return.
- Why the mechanism *might fail* (this section is mandatory; missing it
  invalidates the registration).
- Citation to at least one external source (paper, dealer note, regulatory
  filing) that supports the mechanism, or an explicit acknowledgment that no
  such source exists.

### 1.2 Feature definitions (`features`)

Every feature used by the candidate must be defined as a deterministic
function with the following fully-specified components:

```json
{
  "name": "feature_name",
  "input_columns": ["..."],
  "transformation": "exact algorithm — e.g., 'rolling_std(returns, window=60, min_periods=60)'",
  "lookback_window_days": 60,
  "min_periods": 60,
  "missing_data_policy": "drop | flag | forward_fill_max_2d",
  "clipping": {"lower": null, "upper": null},
  "scaling": "none | z_score_train | log",
  "leakage_guard": "explicit statement that this feature uses no future information"
}
```

Forbidden:
- Features whose computation depends on the test or holdout period.
- Features defined as "approximately X" without an exact algorithm.
- Features whose missing-data handling is unspecified.
- Features whose `min_periods` is less than the lookback window
  (would-be-NaN values cannot be silently filled in).

### 1.3 Threshold logic (`thresholds`)

Thresholds must be one of the following — and **only** one of the following:

- **Fixed numeric:** `RSI > 70`, `IV-RV ratio > 1.0`. The numeric value is
  registered with the hypothesis and cannot be changed.
- **Train-derived rule:** `quantile(p)` of a feature, computed on train data
  only, where `p` is fixed in the registration. The train period and the
  quantile are both pre-specified. Test/holdout data is never consulted.
- **External rule:** a threshold sourced from a published paper, dealer
  research, or regulatory rule. Must be cited.

Forbidden:
- Threshold sweeps where the "best" threshold is selected after looking at
  test data.
- Thresholds described as "approximately" or "around X."
- Thresholds whose derivation method changes (e.g. starting with median and
  switching to `quantile(0.70)` because results were better — this is the
  exact mode that produced the falsified candidate).
- Threshold reuse from a previously-killed candidate without a new
  pre-registration that explicitly accounts for the prior trial in the
  multiple-testing budget.

### 1.4 Allowed transformations (`transformations`)

The pre-registration must list every transformation applied to inputs and
must declare any transformation not listed as forbidden:

```json
{
  "allowed": [
    "log of strictly-positive features (named explicitly)",
    "z-score using train mean/std only",
    "monotonic clipping at registered percentiles"
  ],
  "forbidden_unless_explicitly_listed": [
    "any non-monotonic transformation",
    "any transformation derived from test data",
    "feature interaction terms not registered"
  ]
}
```

### 1.5 Forbidden changes after registration (`forbidden_changes`)

Once the pre-registration is committed, the following changes invalidate the
candidate and require a new registration with a new candidate ID:

- Any change to a feature definition, threshold value, or threshold
  derivation method.
- Adding, removing, or reordering filter conditions.
- Adding regime conditioning, time-of-day filtering, or asset-class
  filtering.
- Changing the forward-return horizon, label definition, or universe.
- Changing the missing-data policy.
- Changing the train/test/holdout split.

A "small" change to a registered candidate is, in this protocol, **not** a
small change. It is a new candidate that consumes one slot in the
multiple-testing budget. The fact that this seems strict is the point.

### 1.6 Falsification criteria (`falsification`)

The pre-registration must list, in advance, the conditions under which the
candidate is dead. These cannot be relaxed after results are observed.

Mandatory falsification criteria:
- Stage 2 (single OOS): `mean_return ≤ 0` OR `win_rate < 0.55` OR
  `n_eff < 30`.
- Stage 3 (cross-period): `cross_period_validated = false` per the existing
  aggregator (any single period fails).
- Stage 4 (cross-symbol): sign flip on any tested same-liquidity-class
  symbol, OR magnitude divergence > 1σ from the median across symbols.
- Stage 5 (robustness): permutation p-value > `0.01 / N_trials` (Bonferroni)
  OR Sharpe 95% CI includes zero.
- Stage 6 (paper): realized vs. expected divergence > 1σ over the minimum
  observation window.

The researcher may add stricter criteria. They may not weaken these.

---

## 2. Dataset Governance Plan

Three datasets, three different access policies. The names are reserved
across the project; no other name may be used to refer to these splits.

### 2.1 Research dataset (contaminated)

Everything that has been *touched* by any prior PR, including PR23–PR42 and
the falsification PR. As of 2026-05-04, this means:

- All SPY data on T9 from 2021-04 through 2025-12.
- All cached parquet exports under `reports/model_ready_dataset_smoke/`.
- The filtered candidate construction methodology itself.

Permitted use: **hypothesis generation only**. No statistic computed on the
research dataset is reportable. Plots, exploratory diagnostics, and "does
this idea even make sense" sanity checks are fine. Anything past Stage 1 is
forbidden on this dataset.

### 2.2 Validation dataset (untouched, used once per candidate)

Used at Stages 2 and 3. **One pass per candidate.** If the candidate fails
any stage, it is dead and the validation dataset is *not* re-used by a
modified version of the same candidate.

Given current T9 limitations (SPY options data starts 2021-04), the
recommended composition is:

- **Cross-symbol panel:** Three to five non-SPY large-cap symbols from
  `/Volumes/T9/market_data/research/options_features_eod/` chosen *before*
  the candidate's pre-registration is committed. Liquidity-tiered to
  approximate SPY (e.g. QQQ, IWM, AAPL, MSFT — or a comparable set selected
  by an objective criterion such as ADV rank). The exact symbols are
  pre-registered.
- **Held-back temporal slice:** A continuous 6-month window from the
  research period that has *not* been used in any prior PR's reported
  results. This window must be pre-registered and may not be inspected.

### 2.3 Final holdout dataset (never used until the end)

Used at Stage 5 (robustness) and only after Stages 2–4 pass. **One pass per
candidate, ever.** If the candidate fails on the holdout, it is dead. If it
passes, it advances to Stage 6.

Composition options, in order of preference:

1. **Backfill pre-2021 IV from iVolatility raw tree.**
   `/Volumes/T9/market_data/raw/ivolatility/iv_surfaces` is unprocessed.
   Building a separate ingestion pipeline for 2018–2020 would create a
   genuine pre-2021 holdout that is structurally untouched by this project's
   exploration. **Recommended.** Estimated work: a separate engineering PR,
   not blocking protocol adoption.
2. **Symbol-disjoint holdout.** A symbol family completely absent from the
   research dataset (e.g., a small-cap or sector-specific name) reserved
   for first-and-only use at Stage 5.
3. **Future paper-traded out-of-sample window.** The protocol's Stage 6
   accumulation period serves as a temporal holdout that no historical
   exploration could have contaminated.

Pre-2021 backfill is the highest-quality option. The other two are
acceptable substitutes; a candidate's pre-registration must declare which
holdout class it will use.

### 2.4 Leakage-prevention rules

- The validation and holdout datasets are read-only at the filesystem level;
  any code that opens them must declare its stage in a runtime header that
  is logged.
- Any code path that reads validation or holdout data must include the
  candidate's pre-registration hash. Reading validation data with no
  registration hash is a runtime error.
- Cross-period thresholds, normalizations, or any other train-derived
  quantity must be computed on training data only. Re-derivation on the
  validation set is forbidden.

---

## 3. Validation Ladder

Six stages, sequential, gated. A candidate cannot enter Stage N+1 until
Stage N is recorded as passed in `reports/research_protocol/ladder/`. Each
stage has explicit pass criteria, allowed metrics, and blocking conditions.

### Stage 1 — In-sample sanity check

| | |
|---|---|
| Purpose | Verify the candidate is implementable. |
| Dataset | Research only. |
| Allowed metrics | Anything; for diagnostics only. |
| Pass criterion | Implementation reproduces the registered rules deterministically (re-runs produce identical output). |
| Block | Implementation cannot reproduce the registered rules → fix code or revise registration before proceeding. A registration revision creates a new candidate ID. |
| Output | "Implementation passes." No claim about edge. |

### Stage 2 — Single-period out-of-sample

| | |
|---|---|
| Purpose | Minimum viability. Does the signal survive *any* held-out period? |
| Dataset | Validation; one held-back temporal slice. |
| Allowed metrics | Mean return, win rate, Sharpe, n_eff, sample-size flags. No bucket conditioning. No regime stratification. |
| Pass criteria | All of: `mean_return > 0`, `win_rate ≥ 0.55`, `n_eff ≥ 30`, `Sharpe ≥ 0.5`, `sample_size_safe = true`. |
| Block | Any criterion fails → candidate falsified. Or `n_eff < 30` → result is *suppressed entirely*; stage cannot pass on a too-small sample. |

### Stage 3 — Cross-period validation

| | |
|---|---|
| Purpose | Regime independence. The previous candidate failed *here*. |
| Dataset | Validation; at least two distinct periods covering structurally different vol regimes (e.g., one bear, one bull) AND different trend regimes. |
| Allowed metrics | Per-period: mean return, win rate, Sharpe, n_eff. Aggregate: `cross_period_validated`. |
| Pass criterion | `cross_period_validated = true` per `services/external_data/ml_cross_period_validation.aggregate_cross_period_validation` — i.e., every period independently meets Stage 2 criteria. |
| Block | Any single period fails → candidate is permanently falsified and triggers the kill-switch in §6. |

### Stage 4 — Cross-symbol validation

| | |
|---|---|
| Purpose | Microstructure independence. Is the result SPY-specific? |
| Dataset | Validation; ≥3 pre-registered symbols of comparable liquidity. |
| Allowed metrics | Per-symbol: mean return, win rate, Sharpe, n_eff. Aggregate: sign agreement and magnitude agreement. |
| Pass criteria | All of: every tested symbol shows mean return of the same sign as the registration's predicted direction; magnitude across symbols stays within ±1σ of the cross-symbol median; `n_eff ≥ 30` per symbol. |
| Block | Sign flip on any single comparable symbol → candidate falsified. Magnitude divergence > 1σ on any symbol → candidate falsified. |

### Stage 5 — Robustness

| | |
|---|---|
| Purpose | Result is not driven by a lucky path or lucky parameter draw. |
| Dataset | Final holdout. One pass, ever. |
| Allowed metrics | Block-bootstrap CI on Sharpe / mean return / win rate; permutation p-value. |
| Pass criteria | All of: 95% bootstrap CI on Sharpe excludes zero; permutation p-value `< 0.01 / N_trials` (Bonferroni-adjusted across the candidate's lifetime trials); deflated Sharpe > 0 at α = 0.05. |
| Block | Any criterion fails → candidate falsified. Bootstrap CI includes zero → candidate falsified even if point estimate is positive. |

### Stage 6 — Paper observation

| | |
|---|---|
| Purpose | Genuinely prospective out-of-sample accumulation. |
| Dataset | Live data, paper-traded forward in time. |
| Minimum window | 90 trading days *or* `n_eff ≥ 100` for the candidate's horizon, whichever is later. |
| Allowed metrics | Realized mean return, win rate, Sharpe, drawdown. Plotted alongside the Stage 5 estimates with their CIs. |
| Pass criteria | All realized metrics fall within ±1σ of the Stage 5 estimates over the observation window. |
| Block | Realized metric outside ±1σ → candidate falsified. Realized drawdown exceeds Stage 5 95% bootstrap floor → candidate falsified. |

Even after Stage 6 passes, the candidate is **not** authorized for live
capital. Live integration requires a separate governance review outside this
protocol.

---

## 4. Statistical Safeguards

### 4.1 Effective sample size for overlapping returns

Forward returns at horizon `h` are overlapping: observation `t` and
observation `t+1` share `h-1` days of underlying movement. Naive row counts
overstate independent observations. The protocol uses:

```
n_eff ≈ n_obs / (h)                                       (lower bound)
n_eff_adj = n_obs * (1 - 2 * sum_{k=1..h-1} (1 - k/h) * ρ_k)   (Newey-West)
```

where `ρ_k` is the lag-k autocorrelation of the residual. The lower bound is
the conservative default; `n_eff_adj` may be reported alongside but never
*instead of* the lower bound.

### 4.2 Minimum n_eff thresholds

| Stage | Minimum n_eff | Action below threshold |
|---|---|---|
| Stage 2 | 30 | Suppress all metrics; stage cannot pass. |
| Stage 3 (per period) | 30 | Suppress that period's metrics; stage cannot pass. |
| Stage 4 (per symbol) | 30 | That symbol does not contribute to pass criterion. |
| Stage 5 | 60 | Stage cannot pass. |
| Stage 6 | 100 | Window must extend until threshold met. |

"Suppress" means the metrics are not displayed in the candidate's report,
not merely flagged. A reader of the report cannot see the unreliable
estimate and cannot anchor on it.

### 4.3 Permutation test protocol

For Stages 3 and 5:

- **Block size** = forward-return horizon `h`. Smaller blocks underestimate
  autocorrelation; larger blocks reduce permutation power.
- **Iterations** ≥ 10,000.
- **Test statistic** = the candidate's filtered mean return on the test
  period.
- **Null** = labels (forward returns) shuffled within blocks; signal mask
  unchanged.
- **One-sided** in the direction of the registered hypothesis. Two-sided
  permutation invites ex-post rationalization of the wrong-sign case.
- **Decision threshold** = `p < 0.01 / N_trials` where `N_trials` is the
  cumulative number of candidates the researcher has registered to date,
  including failed ones. See §8.

### 4.4 Bootstrap confidence intervals

For Stage 5:

- Block bootstrap with block size `h`.
- Resamples ≥ 10,000.
- 95% CI on Sharpe, mean return, and win rate.
- All three CIs are reported; the candidate cannot pass on Sharpe alone.
- **No re-running** of the bootstrap with different seeds in search of a
  more favorable CI. Seed is registered; the run is deterministic.

### 4.5 Multiple-testing adjustment

Three layers, applied jointly:

1. **Bonferroni at the program level.** The p-value threshold for any
   permutation test is `α / N_trials` where `N_trials` is the total number
   of candidates registered (passed or failed) in the program's lifetime.
   `N_trials` resets only when an externally-audited validation reset is
   declared; researcher self-resets are not permitted.
2. **Benjamini-Hochberg for batched tests.** Within a single candidate's
   robustness suite, the BH procedure controls FDR at 0.05.
3. **Deflated Sharpe Ratio** (Bailey & López de Prado, 2014). The reported
   Sharpe is adjusted for higher moments and number of trials; the deflated
   value, not the raw value, is the headline metric.

---

## 5. Execution Realism Layer

A candidate that survives the statistical protocol but cannot be traded is
not a signal.

### 5.1 Entry timing

- Signal is computed using data **available as of the close of day t**.
- Earliest legal entry is **the open of day t+1**, or later if the candidate
  registration specifies a delayed entry.
- Close-to-close return calculations are forbidden as the headline metric.
  Open-to-open or VWAP-based fills are required.
- For intraday candidates, the registration must specify the exact entry
  timestamp rule and a minimum pre-entry observation window.

### 5.2 Slippage model

- Per-side slippage = `0.5 × (ask − bid)` at the entry/exit timestamp.
- For options: `0.5 × (ask − bid)` plus an additional 5% adverse fill on
  high-spread contracts (relative spread > 10%).
- For equities: `max(0.5 × spread, 0.5 cents)`.

### 5.3 Commission

- Options: $0.65 per contract per side (Tastytrade-class retail rate). May
  be lowered with documentation of an actual broker rate.
- Equities: $0 base, $0.0035/share regulatory fee.
- Round-trip costs are deducted from per-trade returns *before* any metric
  is computed.

### 5.4 Liquidity floor

- Options: open interest ≥ 100, daily volume ≥ 50 on the entry day.
- Equities: ADV ≥ $5M.

A candidate cannot pass any stage if its entries violate the liquidity
floor. Entries below the floor are dropped, and the resulting `n_eff` is
recomputed against the post-drop sample.

### 5.5 Invalid by execution alone

A candidate is dead — regardless of statistical performance — if any of:

- Net round-trip cost (slippage + commission) > 30% of registered expected
  per-trade return.
- Required entry size > 5% of average daily volume in the underlying or
  option contract.
- Realized fill prices in any backtest deviate from quoted mid by more than
  the slippage model would predict.

---

## 6. Governance + Kill Switch

### 6.1 Permanent-kill conditions

A candidate is **permanently killed** — and added to a project-level kill
list at `reports/research_protocol/kill_list.json` — if any of:

- Stage 3 fails (`cross_period_validated = false`).
- Stage 4 fails (cross-symbol sign flip or magnitude divergence).
- Stage 5 fails (permutation p > threshold, bootstrap CI includes zero, or
  deflated Sharpe ≤ 0).
- Stage 6 deviates beyond ±1σ.
- Execution-realism failure per §5.5.

The kill list records: candidate ID, registration hash, kill date, kill
stage, kill reason, and the artifact paths supporting the decision.

### 6.2 No-revival rule

Once on the kill list, the candidate is dead. The following are forbidden:

- Re-running a killed candidate after a code change.
- Registering a "variant" of a killed candidate (same hypothesis, different
  threshold or feature set) without first incrementing `N_trials` for the
  multiple-testing budget. Variants count as new trials, not new candidates.
- Re-using the validation or holdout datasets for the killed candidate's
  variants.
- Removing a candidate from the kill list. The list is append-only.

A new candidate must differ from any killed candidate on at least one of:
- The hypothesized economic mechanism (different mechanism = different
  candidate).
- The asset class.
- The forward-return horizon by a factor of ≥ 2.

A candidate that differs only in threshold value, lookback window, or
feature engineering detail is a *variant*, not a new candidate, and is
forbidden.

### 6.3 The "I just want to try one more thing" gate

If the researcher proposes a change to a killed or near-failing candidate
that they believe addresses the kill reason, the proposal must be:

1. Filed as a new pre-registration with a new candidate ID.
2. Counted in `N_trials` for Bonferroni adjustment.
3. Run against a *different* validation slice than the killed candidate
   used (the original slice is now contaminated with respect to the new
   variant).

This is the rule that would have prevented the previous failure most
directly. The PR23–PR42 path was a sequence of "one more thing" adjustments,
none of which were registered separately, and `diagnostics_explored_count`
was understated as 6 when the true count of distinct trial choices was much
higher.

---

## 7. Auditability Requirements

Every artifact produced under this protocol must be reproducible from
recorded metadata alone.

### 7.1 Dataset fingerprinting

Every dataset used in any stage must be fingerprinted:

```json
{
  "path": "reports/.../spy_2022-01-03_2022-12-30.parquet",
  "sha256": "9f...e3",
  "row_count": 251,
  "column_set_hash": "1a...9c",
  "min_date": "2022-01-03",
  "max_date": "2022-12-30",
  "fingerprinted_at": "2026-05-04T18:22:41Z"
}
```

A candidate's artifact directory must contain one such fingerprint per
dataset consumed. Re-running with a dataset whose SHA256 has changed
invalidates the prior result.

### 7.2 Frozen signal definition hash

The pre-registration document is hashed (SHA256). The hash is recorded in:

- The pre-registration file itself (computed over canonical JSON, then
  written as the file's `registration_hash` field on a second pass).
- Every downstream report (Stage 2–6 artifacts).
- The kill list, on permanent kill.

A change in the hash means a new candidate. Hashes never change in place.

### 7.3 Reproducibility metadata

Each stage's artifact must record:

- `git_commit_sha`: the commit at run time.
- `git_status_clean`: bool; runs against a dirty tree must be flagged.
- `python_version`, `pandas_version`, `numpy_version`.
- `random_seed`: fixed; specified in the pre-registration.
- `command_line`: the exact CLI invocation.
- `stage_input_dataset_fingerprints`: per §7.1.
- `wall_clock_runtime_seconds`.

These metadata are written to
`reports/research_protocol/ladder/{candidate_id}/stage_{n}.metadata.json`
beside the stage's report.

### 7.4 Reproducibility test

For every passing stage, a third party must be able to:

1. `git checkout <git_commit_sha>`.
2. Restore datasets matching the recorded SHA256s.
3. Run the recorded `command_line`.
4. Receive byte-identical artifacts (modulo timestamps).

Failure to reproduce invalidates the candidate's progress through the
ladder.

---

## 8. Researcher Degrees-of-Freedom Control

The previous failure was as much a tracking failure as a statistical one.
This section closes that gap.

### 8.1 New hypothesis vs. modification

A change to a candidate creates a **new hypothesis** (and a new
pre-registration, a new candidate ID, and a `+1` to `N_trials`) if it
involves any of:

- A different stated economic mechanism.
- A different feature set, even if the new features are correlated with
  the old.
- A different forward-return horizon.
- A different threshold derivation method (median → quantile, fixed →
  rule-derived, etc.).
- A different filter logic (different number or order of conjunctions).
- A different symbol family or asset class.

A change that is **forbidden as an in-place modification** but counted as
a new trial includes:

- Threshold value change with the same derivation method.
- Lookback window change.
- Missing-data policy change.
- Adding or removing a single condition.

There is no such thing as a "minor tweak" under this protocol. A tweak is a
new trial.

### 8.2 Attempt budget

- **Maximum 3 candidates per quarter.** This budget is enforced at the
  program level, not per researcher. The intent is to make selectivity
  costly so that low-conviction ideas are not registered.
- Each candidate consumes one slot regardless of outcome.
- A candidate killed at Stage 1 (implementation failure) does not consume a
  slot, but only because no validation data was touched.
- A candidate killed at any later stage **does** consume a slot.

### 8.3 `diagnostics_explored_count` and `N_trials` tracking

The previous candidate's `SNOOPING_METADATA.diagnostics_explored_count` was
6, reflecting six distinct diagnostic *modules*. The actual number of
researcher choices made along the PR23–PR42 path was an order of magnitude
higher. The protocol corrects this:

- `N_trials` increments on every committed pre-registration, every variant,
  and every retried run on a held-out dataset.
- `N_trials` is logged in `reports/research_protocol/n_trials.json` and is
  append-only.
- The Bonferroni denominator in Stage 5 is the current value of `N_trials`
  at the time the stage is run. Re-running Stage 5 after additional trials
  have been registered tightens the threshold, not loosens it.
- A protocol audit annually reconciles `N_trials` against the git log of
  registrations. Discrepancies trigger a rebuild of the multiple-testing
  budget from the registration log.

### 8.4 Mandatory cooldown

After a candidate is killed at Stage 3 or later, the researcher may not
register a new candidate in the same hypothesis family for **30 days**. The
purpose of the cooldown is to break the immediate "try the next thing"
reflex that produced the previous failure.

---

## 9. Implementation Checklist

The following items are the minimum module/file structure required to
operationalize the protocol. They are listed for future implementation; do
not implement them yet without a separate, explicit task.

```
docs/RESEARCH_PROTOCOL.md                              [this file]
reports/research_protocol/
  registrations/{candidate_id}.json                    [§1]
  ladder/{candidate_id}/stage_{1..6}.json              [§3]
  ladder/{candidate_id}/stage_{1..6}.metadata.json     [§7.3]
  kill_list.json                                       [§6.1, append-only]
  n_trials.json                                        [§8.3, append-only]
services/research_protocol/
  registration.py
    - load_registration(path) -> Registration         [validates schema]
    - hash_registration(registration) -> str          [§7.2]
  fingerprint.py
    - fingerprint_dataset(path) -> DatasetFingerprint [§7.1]
  ladder.py
    - run_stage(candidate_id, stage, ...)             [§3, gated]
    - assert_prior_stages_passed(candidate_id, stage) [gating]
  kill_list.py
    - record_kill(candidate_id, stage, reason, ...)   [§6.1, append-only]
    - assert_not_killed(candidate_id)                 [§6.2]
  n_eff.py
    - n_eff_lower_bound(n_obs, horizon) -> float      [§4.1]
    - n_eff_newey_west(returns, horizon) -> float     [§4.1]
  permutation.py
    - block_permutation_pvalue(...)                   [§4.3]
  bootstrap.py
    - block_bootstrap_ci(...)                         [§4.4]
  multiple_testing.py
    - bonferroni_threshold(alpha, n_trials)           [§4.5]
    - benjamini_hochberg(pvalues, alpha)              [§4.5]
    - deflated_sharpe(returns, n_trials)              [§4.5]
  execution.py
    - apply_slippage(fills, mids, spreads)            [§5.2]
    - apply_commission(fills)                         [§5.3]
    - liquidity_floor(rows)                           [§5.4]
tests/unit/test_research_protocol/
  test_registration_schema.py
  test_registration_hash_invariance.py
  test_fingerprint_reproducibility.py
  test_ladder_gating.py
  test_kill_list_append_only.py
  test_n_eff_calculations.py
  test_permutation_p_value_distribution.py
  test_bootstrap_seed_determinism.py
  test_multiple_testing_thresholds.py
  test_execution_invalidation.py
```

---

## 10. Minimal Rules That Would Have Prevented the Previous Failure

The previous candidate (`high_vol_trend_early_candidate`) was discovered on
2023+2024 → 2025 with a filtered test win rate of 80.9% and would have been
prevented by any one of the following rules. The protocol bundles all of
them; even adopting a subset would harden future work.

1. **Mandatory pre-registration with frozen hash before any reportable
   statistic is computed.** PR42 had no such gate. The candidate was
   selected after observing diagnostics across PR23–PR42, which is the
   textbook discovery-after-search failure mode.
2. **Mandatory cross-period validation on a regime-different period before
   any "ready" status.** The original baseline reported `paper_ready=True`
   on a single bull-regime period. Stage 3 of this protocol would have
   required the 2022 bear-regime run before that flag could be raised — and
   2022 would have killed the candidate on first contact.
3. **Mandatory `n_eff` calculation, not row count.** The 80.9% test win
   rate was on `n=68` filtered observations with a 5-day overlapping
   horizon. The lower-bound `n_eff ≈ 68/5 ≈ 14`, well below the Stage 2
   minimum of 30. The result would have been suppressed entirely.
4. **Mandatory permutation test.** With `N_trials ≥ 7` (PR23–PR42 was at
   least seven distinct trials), the Bonferroni-adjusted p-threshold is
   `0.01 / 7 ≈ 0.0014`. A block-permutation test on 68 observations is
   unlikely to clear that bar; the result would have failed Stage 5.
5. **Mandatory cross-symbol check.** The candidate was tested only on SPY.
   A 3-symbol cross-symbol panel would have flagged the SPY-2025
   specificity directly.
6. **Honest `N_trials` accounting.** The `diagnostics_explored_count = 6`
   recorded in `SNOOPING_METADATA` undercounted the true number of
   researcher choices by at least an order of magnitude. The protocol's
   `N_trials` is append-only and reconciled against the registration log,
   making understatement structurally difficult.
7. **The 30-day cooldown.** The protocol's cooldown explicitly forbids
   exactly the reflex that produced the failed candidate: "we noticed
   late-trend rows hurt us, let's filter them out, re-run, and call it
   ready." That sequence consumes one trial per attempt under this protocol
   and would have exhausted the quarterly budget before the candidate was
   pre-registered as the canonical version.

Any future candidate must demonstrate, in its pre-registration, that it
is constructed to be killed by these seven rules — not to circumvent them.

---

## 11. What This Protocol Does Not Do

To be explicit about scope:

- This protocol does not propose a signal, a feature, or a hypothesis.
- It does not guarantee that a candidate that passes Stage 6 has a real
  edge. It guarantees only that *if* it does, the evidence is admissible.
- It does not eliminate researcher bias. It makes bias more expensive.
- It is not a substitute for governance review before live capital is
  allocated. It is a precondition for that review.
- Adopting the protocol does not retroactively legitimize any prior result.
  Past results stand as audit history, not as evidence.

If this protocol feels burdensome, that is correct. The previous candidate
took six months of research and produced a falsified signal. The protocol's
overhead is small relative to the cost of *not* having had it in place.
