# Root Module Classification

**Status:** classification + recommendation only (no gate change applied here — see [Recommendations](#recommendations)).
**Date:** 2026-10-21
**Scope:** the 15 root-level browser `*.js` modules (repo-root `*.js`, excluding the build configs `eslint.config.js` and `rollup.config.js`).

## Why this exists

The JS test-coverage campaign added per-file coverage floors to several root modules. Before trusting those floors as a *production* safety gate, we had to answer one question with hard evidence: **which root modules does the live product actually load?** The coverage denominator should reflect live code, not a prior generation of browser experiments kept alive only by archived test harnesses.

Two prior claims asserted these modules were live and had to be tested against the evidence, not assumed:

- `server/yahoo_proxy.js` `buildRuntimeArchitectureSnapshot()` (`runtime_mode: 'dashboard_globals'`, note at `:269`) asserts *"Primary production runtime is rooted in `production_pivot_dashboard.html` and root-level browser modules."*
- An external audit ("hermes") described the chart/alert/help/professional-UI modules as "genuinely-active."

## Method

A module is **live** only if some live surface loads, imports, serves, or executes it. The only live surfaces are:

- the two HTML files the server actually serves — `production_pivot_dashboard.html` (routes `/`, `/production_pivot_dashboard.html`) and `daily_levels.html` (routes `/levels`, `/daily_levels.html`);
- a `server/*.js` route or static handler;
- the `src/` library (the rollup build) or a Node CLI/script that actually runs.

Each module was classified by an independent agent that enumerated every inbound reference repo-wide (file:line) and searched for any live load path (including dynamic `loadScript` / `createElement('script')` / `import()`, service workers, server concat/bundling). Each verdict was then **adversarially re-checked** in the opposite direction (for every "legacy" verdict, an agent tried to prove it live). **No verdict was overturned; all 15 held at high confidence.**

### Live-surface facts (independently re-verified for this doc)

- `production_pivot_dashboard.html` references **none** of the 15 module basenames or their `window.*` globals — a combined grep returns **0 matches**. Its only dynamic script path (`loadScript` → `ensureChartLibrary`, ~`:3070`) loads **only** `/static/lightweight-charts.js` + two CDN fallbacks. It gets its data from server JSON APIs.
- `daily_levels.html` is `<script type="module">` importing **only** three `/app/levels/*.js` assets, which the server resolves into `src/` (not root modules):
  - `/app/levels/volatility_levels.js` → `src/math/MultiHorizonVolatilityLevels.js` (**a different file** from root `volatility_analysis.js`)
  - `/app/levels/nyse_calendar.js` → `src/forecast/nyseCalendar.js`
  - `/app/levels/gate.js` → `src/levels/gate.js`
- `server/yahoo_proxy.js` serves JS via an **explicit route whitelist ending in a hard 404** (`:3699`): `/static/lightweight-charts.js`, the three `/app/levels/*.js` above, several `/app/levels/*.json` data files, and the two HTML pages. **There is no catch-all static route and no root-`*.js` mount**, so no root module can be served.
- `rollup.config.js` builds **only** `src/index.js` → `dist/`. No root module is in the build.

## Result

**14 of 15 root modules are `legacy`; 1 is an `example`. Zero are active.** Every inbound reference to these modules comes from `archive/legacy_html/*.html` harnesses (served by no route), the Jest coverage config in `package.json`, their own tests, documentation prose, or another already-dead root module — or there are no inbound references at all.

| Module | Class | In gate now | Surface | One-line evidence |
|---|---|---|---|---|
| `fdr_correction.js` | legacy | ✅ (floor **85/95**) | pure-logic | Benjamini–Hochberg IIFE; loaded only by `archive/legacy_html/test_fdr_implementation.html:170` + the dead `pivot_fdr_integration.js` + demo `integration_example.js` |
| `enhanced_fdr_correction.js` | legacy | ✅ (60/85) | pure-logic | stat IIFE; archive HTML only + two themselves-dead root consumers |
| `advanced_analytics.js` | legacy | ✅ (72/88) | pure-logic | archive `test_complete_advanced_system.html` + dead consumer `advanced_ui_components.js` + coverage config + its test |
| `advanced_pivot_engine.js` | legacy | ✅ (72/90) | pure-logic | loaded only by 4 archive harnesses (e.g. `V3PIVOT_CALCULATOR.html:399`) |
| `pivot_fdr_integration.js` | legacy | ✅ (60/85) | dom-builder | monkey-patches `window.displayResults`; intended host `V3_PIVOT_CALCULATOR_PRO.html` **does not exist** |
| `enhanced_pivot_display.js` | legacy | ✅ (62/88) | dom-builder | depends on dead `EnhancedFDRCorrection`; archive HTML only |
| `advanced_ui_components.js` | legacy | ✅ (45/72) | dom-builder | only non-self/test ref is `archive/legacy_html/test_complete_advanced_system.html:323` |
| `volatility_analysis.js` | legacy | ✅ (62/90) | mixed | archive harness only; the `/app/levels/volatility_levels.js` route serves a **different** `src/` file |
| `weekly_backtest_panel.js` | legacy | ✅ (**8/12**) | mixed | documented `<script>` integration into `V3PIVOT_CALCULATOR.html` never landed; the near-zero floor already signalled this |
| `alert_system.js` | legacy | ❌ | timer/dom glue | self-instantiates `window.alertSystem` on `DOMContentLoaded`; nothing live instantiates it; only refs are `CLAUDE.md:9` prose + a Python source-text assertion |
| `enhanced_chart_engine.js` | legacy | ❌ | timer/fetch/chart-lib glue | loaded only by 3 archive harnesses; not unit-testable |
| `help_system.js` | legacy | ❌ | dom/timer/localStorage glue | **zero** inbound references repo-wide; not unit-testable |
| `performance_tracker.js` | legacy | ❌ | dom/timer glue | **zero** inbound references; 60s `setInterval`, auto-instantiates on `DOMContentLoaded`; not unit-testable |
| `professional_ui_system.js` | legacy | ❌ | dom-lifecycle/timer glue | consumed only as the runtime global `window.professionalUI` by the dead `alert_system`/`help_system`/`performance_tracker` cluster |
| `integration_example.js` | **example** | ❌ | demo | self-labeled demo scaffolding; sole function is a never-invoked `_calculatePivotsWithFDR`; targets the non-existent `V3_PIVOT_CALCULATOR_PRO.html` |

The nine ✅ modules sit in `collectCoverageFrom` (`package.json:117–125`) with per-file floors (`:135–143`) — **unit floors gating code no live surface runs.**

## Reconciliation of the prior claims

**The evidence is correct; the doc-string and the hermes claim are both false (stale) as applied to these 15 modules.** The `runtime_mode: 'dashboard_globals'` note describes an intended/historical architecture — a dashboard built from root-level browser globals — that the actually-served HTML abandoned. The real live JS surface today is the `src/` library (served via `/app/levels/` and the rollup entry) plus the two HTML pages' own inline scripts. The root `*.js` modules are an orphaned prior generation surviving only in `archive/legacy_html/*.html` harnesses, the Jest config, and their tests.

This is the discipline-contract failure mode in reverse: an authoritative-sounding self-audit string nearly licensed calling 14 dead modules "active." Per the contract, no module was upgraded on the strength of that string or the hermes assertion — a cited live load path was required, and none exists for any of the 15.

## Impact on the coverage campaign

The per-file floors added for these modules are **regression tests of stat/engine logic, not a production-coverage gate** — they enforce coverage on code that never executes in any served surface. The tests are not worthless as library-level regression guards, but they were mis-targeted as a production gate, and the green numbers give a false sense of production safety. Some floors are high (`fdr_correction` 85/95, `advanced_pivot_engine` 72/90, `volatility_analysis` 62/90) and partly prop up the global threshold.

## Recommendations

These are **direction-changing** and are left for a human to land explicitly (per the discipline contract — no archival/gate edit as a side effect of a classification). Suggested staging, lowest-risk first:

1. **Coverage gate (low risk, do first, its own PR).** Drop the nine root-module entries from `collectCoverageFrom` (`package.json:117–125`) and the nine per-file `coverageThreshold` entries (`:135–143`), leaving only `src/**/*.js` (+ the `!src/**/*.test.js` exclusion). Then **recalibrate the global floor** (`:129–134`, currently branches 45 / functions 60 / lines 58 / statements 58): with the legacy modules gone the global number is computed over `src/**` alone and will shift — re-measure and set the floor from the real `src/**` number rather than carrying the inflated legacy figure.
2. **Archive the files + retire their tests (bigger, separate reviewed PR).** Moving the `.js` files into `archive/` **will break**, and must be handled in the same change: the nine `tests/*.test.js` that import them, and the Python source-text assertions in `tests/python/test_ops_smoke.py` (`:5936` and the `test_fdr_display_modules_guard_missing_global_engines` test) that `read_text()` `alert_system.js`, `pivot_fdr_integration.js`, `enhanced_pivot_display.js`, and `enhanced_fdr_correction.js`. Either relocate/retire those tests or keep the files in place and only do step 1.
3. **Fix the misleading self-audit.** Update `server/yahoo_proxy.js` `buildRuntimeArchitectureSnapshot()` (note at `~:269`) to describe the real `src/`-backed surface, or it will re-mislead the next audit.
4. **Update `CLAUDE.md`** "Project Shape" — its JS-half description (`advanced_*.js`, `alert_system.js`, … as the live "institutional-grade pivot point analysis surface") should note these root modules are legacy/archived, not live.

## Caveat

The entire finding rests on `production_pivot_dashboard.html` having zero root-module references and the server serving only two HTML files via an explicit whitelist. Both were re-verified directly. **If a future served page or server route is added that loads a root module, re-classify that module to `active-*` before removing its coverage.**
