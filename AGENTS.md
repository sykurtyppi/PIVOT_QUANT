# AGENTS.md — PIVOT_QUANT

Per-repo facts for the Hermes review agent (adversarial quant reviewer). This file is
the repo-specific layer under the global SOUL.md identity and the shared six-check review
skill (look-ahead bias, point-in-time violations, survivorship, overfitting/multiple-testing,
cost/execution realism, regime cherry-picking). Report findings as a severity-ranked list
citing exact lines — never a prose summary.

> **Audit provenance.** Verified against `main` at commit `6d1db89` on 2026-07-16. Package
> versions, line numbers, sample counts, and "currently surviving" observations are
> point-in-time snapshots — re-verify any specific line / number / version against the
> current tree before relying on it. The structural invariants (data providers, risk tier,
> the classes of pitfall) are durable; the exact citations are not.

## Orientation

Two-headed repo. `README.md` sells a polished "institutional pivot-point JS library"
(`src/core/QuantPivotEngine.js`, `docs/api.md`, Docker/K8s) — that is **aspirational
framing, largely not the live system**. The real system is the Python ML stack documented
in `SYSTEM.md` + `CLAUDE.md` + `docs/`. **`SYSTEM.md`/`CLAUDE.md` describe the real system; `README.md` is aspirational marketing.
Use the former to orient, but still verify their claims against the code before relying on them.** It detects pivot events on SPY, scores reject/break
outcomes at 5/15/30/60-minute horizons with a scikit-learn RandomForest, logs
predictions/outcomes, retrains every 6 hours, and hot-reloads models into a 24×7 server.
Outputs are labeled "live tradeable signal" and drive human trading decisions.

## 1. Stack & dependencies

- **Python (the live ML platform)** — `requirements-runtime.txt`: `scikit-learn==1.5.2`,
  `numpy==1.26.4`, `pandas==2.2.3`, `duckdb==1.1.3`, `fastapi==0.115.12`, `uvicorn==0.30.6`,
  `joblib==1.4.2`, `ib-insync==0.9.86`. Interpreter pinned to `.venv/bin/python` (3.11; PEP-604
  annotations break on 3.9). Model = RandomForest (`scripts/train_rf_artifacts.py`, `train_rf.py`).
- **JavaScript (dashboard + significance display)** — Node ≥16 ES modules, Rollup, Jest/Babel,
  ESLint/Prettier; runtime dep effectively just `lightweight-charts`. Root `*.js` are browser
  globals loaded by `production_pivot_dashboard.html`. `server/yahoo_proxy.js` is a Node
  market-data proxy on `:3000`.
- **Data store** — SQLite `data/pivot_events.sqlite` (`prediction_log`, `event_labels`).
  Training views built in DuckDB (`build_duckdb_view.py`).
- **Process topology** (`SYSTEM.md`, `server/run_all.sh`): `yahoo_proxy.js:3000` →
  `live_event_collector.py:5004` (poll+feature build, 45s, SPY) → SQLite; `ml_server.py`
  (FastAPI) `:5003` serves `/score`, `/health`, `/reload`; `event_writer.py:5002`; optional
  `ibkr_gamma_bridge.py:5001`.
- **Deployment** — `docs/REMOTE_24X7_SETUP.md`: runs 24×7 on a personal MacBook Air via macOS
  LaunchAgent `com.pivotquant.dashboard` + `caffeinate`, binds `0.0.0.0`, optionally exposed to
  the public internet via **Tailscale Funnel** with one shared `DASH_AUTH_PASSWORD`. Not cloud
  infra despite README K8s claims. Models are pickles loaded via `joblib.load` on `/reload`
  (`server/ml_server.py:138`, annotated in-code as "an RCE primitive", token-gated).

## 2. Risk tier — Research/backtesting (production-serving)

No auto-execution path was found — grep finds no order router or broker execution call (strong evidence, not exhaustive proof); IBKR is used
for data ingest only. The system **scores/predicts/alerts**; a human trades. Per the fleet
convention it is classed **Research/backtesting**: overfitting and multiple-testing are the
primary risks, so ask whether OOS / walk-forward validation backs any new result.

**But it is production-serving** — continuous 6-hourly retrain + hot-reload of pickled models
+ 24×7 (optionally public) serving, with outputs represented as tradeable. So on top of the
research checks, watch the production-ML surface: label leakage on retrain, train/serve skew,
promotion discipline, model governance. Every numerical/signal finding here is **flag-only,
never a silent correction**, even though style/infra may be fixed directly (§4).

## 3. Known pitfalls specific to this repo (verified from code)

1. **The anti-overfitting machinery does not guard the live path.** `scripts/run_retrain_cycle.sh`
   (the 6-hourly LaunchAgent) runs train → `scripts/model_governance.py` → `POST /reload`. It does
   **not** call `ml/walk_forward_oos.py`, the `services/research_protocol/` guard, or
   `run_retrain_evidence_pack.py`. `docs/RESEARCH_PROTOCOL_ENFORCEMENT.md` confirms enforcement is
   "opt-in … Default behavior is unchanged (legacy non-enforcing)." So a model can be tuned in-sample
   and hot-swapped live gated only by mechanical checks (targets/horizons/bounds/MFE-MAE) with no OOS
   or statistical validation. **This is the single highest-value thing to scrutinize on any retrain/
   promotion change.**
2. **Horizon labeling is inherently look-ahead-shaped.** `scripts/build_labels.py::label_event`
   scans forward bars to `ts_event + horizon` to decide reject/break. Leakage defense rests on
   `forward_bars_after_touch` (excludes the touch bar, `ts > ts_event`) and `normalize_bar_interval`
   (refuses rows with missing/zero `bar_interval_sec`). Verify features never read post-touch data.
3. **Leakage defense is a hand-maintained drop-list.** `ml/features.py` `DROP_FEATURES` (keyed to
   `FEATURE_VERSION="v3"`) drops raw prices/EMAs/VWAP/VPOC as "temporal leakage." Any new raw feature
   that slips past the list is a silent leak.
4. **Train/serve skew in the prior-shift correction.** `ml/label_shift.py::correct_prior_shift`
   (Saerens/Lipton) is applied **only at serve time** (`ml_server.py:~2979`), not during
   training/threshold selection. Thresholds are chosen on uncorrected probabilities but serving
   corrects them — deployed threshold and corrected probability live in different probability spaces.
   `rolling_class_rate` docstring records a real "CRITICAL-1" prior-circularity bug (reverted 2026-05-31).
5. **Self-labeling retrain loop.** The system scores events → `prediction_log` → matures to
   `event_labels` → retrains on them every 6h (`score_unscored_touch_events.py` runs post-reload).
   Watch for training on its own recent predictions and for expanding-window retrain reusing the tail
   that also tunes thresholds.
6. **Regime fragility is the documented original sin.** `CLAUDE.md` records candidate
   `high_vol_trend_early_candidate`: **80.9% win rate on 2025 vs 41.2% on 2022 with the same frozen
   filter** — a regime artifact that survived six diagnostic modules and an LLM audit. Per
   `CLAUDE.md` Symbol Policy, only **`reject@15m` currently survives**; other (target,horizon) pairs
   are mechanically blocked (`no_signal_substituted`). The "5 horizons × reject/break" story collapses
   to one live signal in practice.
7. **Options/gamma features are dead in normal operation.** `ml/features.py` `DROP_FEATURES` marks
   `iv_rv_state`, `gamma_confidence`, `oi_concentration_top5`, `zero_dte_share` as "100% null from
   Yahoo-only collection." `advanced_pivot_engine.js::calculateGammaFlip` is a **VWAP proxy mislabeled
   as gamma** — a representation issue to flag, not silently edit.
8. **`atr_bps` is not in the training view.** `build_duckdb_view.py` carries raw `atr` only; `atr_bps`
   exists only in the runtime feature-frame (`ml/features.py:226-233`). Audits reading the view can't
   see it — see `scripts/audit_atr_availability_by_provider.py`.
9. **Browser-side FDR stats are separate and heuristic.** `enhanced_fdr_correction.js` tests against
   hand-chosen `DEFAULT_REGIME_BASELINES` (null ≠ flat 50%), FDR-corrects only levels passing
   `minN`/`minNEff` (family size is post-filter), applies heuristic `nEff` clustering discounts, and
   uses an **unseeded `Math.random()` permutation test** (non-reproducible). This is display-layer,
   distinct from the Python `statistical_guard`.
10. **Hot-reload has a stale-state window.** `docs/SERVING_STATE_RUNBOOK.md`: serving-state file is
    read only at startup and on `POST /reload`; a plain `/score` does not refresh it. Pause takes two
    steps and there is a window where `/score` serves stale state. `ModelRegistry` uses an RLock;
    "transient empty model map during reload" is commented at `ml_server.py:784`.
11. **`WEEKLY_BACKTEST_PATCH_SUMMARY.md` references archived/absent files** and
    `pivot_fdr_integration.js` contains **hardcoded demo data** (`SAMPLE_HISTORICAL_DATA`, `R3: 45/52`),
    not live output. Don't cite it as a live backtest result.

## 4. What the agent may fix directly vs only flag

**Default posture is read-only.** During a review-only task, report proposed changes as
findings and do not edit; post inline PR comments only as the configured review bot or when
explicitly asked, not merely because a PR exists. Fixes apply only when explicitly
authorized — and even then, numerical / signal / statistical changes require focused
before/after validation and human review, never a silent edit. "Low-risk" is not risk-free:
UI, scheduler, deploy, CORS, and DB code can still be consequential — treat every item below
as a candidate, not standing authorization.


**Flag only — never auto-fix (signal / labeling / calibration / threshold / model core; changes
alter what ships as a tradeable signal):**
`ml/thresholds.py`, `ml/calibration.py`, `ml/label_shift.py`, `ml/walk_forward_oos.py`,
`ml/threshold_overrides.py`, `ml/features.py` (esp. `DROP_FEATURES`), `scripts/build_labels.py`,
`scripts/train_rf_artifacts.py`, `scripts/model_governance.py`, `scripts/run_retrain_evidence_pack.py`,
`scripts/run_retrain_cycle.sh`, `server/ml_server.py` (scoring/threshold/prior-shift/reload),
`server/serving_state.py`, `services/research_protocol/*`, and the FDR/statistics JS
(`fdr_correction.js`, `enhanced_fdr_correction.js`) when it drives displayed significance.

**Low-risk — only if a fix is explicitly requested, (UI / chart / infra / ops — no effect on the numbers a trader acts on):**
`production_pivot_dashboard.html`, `advanced_ui_components.js`, `enhanced_chart_engine.js`,
`enhanced_pivot_display.js`, `professional_ui_system.js`, `help_system.js`; build/lint/test config
(`rollup.config.js`, `eslint*`, `package.json` scripts); `docs/*` prose; LaunchAgent install shells,
backup/health-check scripts; report-delivery transport in `send_daily_report.py` (transport only, not
thresholds). Caveat: the VWAP-proxy-labeled-as-gamma issue is a correctness flag, not a safe silent edit.

## 5. PR etiquette

Deliver findings as **inline review comments anchored to exact lines**, severity-ranked, framed as
"here is why this result might be wrong." Do **not** post full-file rewrites or restructure code unless
explicitly asked to push a fix commit. For flag-only areas, comment and stop — do not edit. When a change
touches the retrain/promotion/serving path, state the concrete leakage/skew/regime scenario it could
produce, not a generic caution.
