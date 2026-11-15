# Canonical levels workflow — first increment

## Goal and boundary

Keep the existing deterministic levels engine and immutable product artifacts. Expose the exact scheduled morning snapshot to the dashboard, explicitly separate it from exploratory/live browser calculations, and prevent instrument-switch races. Do not present reconstructed historical touches as prospective or delivered-alert performance.

This increment is not a deployment, a completed prospective evaluation, or a backup/restore clearance. It does not incorporate unrelated runtime/listener, candle-alignment or snapshot-retry branches. No operator database, environment file or real notification is needed for verification.

## Morning delivery window

A frozen briefing may be newly delivered from its recorded capture time through the session's regular exchange open (09:30 America/New_York), inclusive. The trading calendar and timezone rules determine the UTC boundary; a fixed UTC hour is not used. This preserves the artifact's existing pre-market capture policy at delivery time as well.

The check occurs under the existing per-briefing/channel lock immediately before the transport call. A late first delivery raises `ValueError`, does not invoke the provider and does not write a successful receipt. A previously confirmed receipt still returns `already-delivered` after the window without sending again. Historical artifacts remain available; dry-run rendering remains possible after expiry. The change does not claim exactly-once provider delivery after a crash between remote acceptance and receipt persistence.

## Acceptance checks

- The dashboard shows canonical snapshot values and identity without recomputing them, or explicitly reports canonical data unavailable.
- Browser exploratory/live calculations remain separately labeled.
- Older instrument responses cannot overwrite a newer selection, including stale errors and loading-state updates.
- Missing, invalid, stale and future snapshots fail closed at the read boundary.
- New briefing delivery after session open is rejected; delivery before capture is rejected; the exact open boundary is covered.
- A successful delivery retry after the window is still idempotent.
- Tests use synthetic files and mocked providers, not operator evidence or real notifications.

## Canonical API and dashboard

`GET /api/levels/snapshot?symbol=SPY` runs behind the dashboard's existing authentication gate. It accepts one filename-safe uppercase symbol and no other query parameters; methods other than GET are rejected. Responses are `no-store`. The subprocess bridge uses the existing Python resolver (`PYTHON_BIN`, repository virtualenv, compatible system Python), a five-second timeout, bounded output and at most four concurrent readers.

The Python reader validates the stored artifact with `level_snapshot.load_snapshot`; it never computes substitute levels or opens an operator database. It reads `evidence/levels_product/levels_snapshot_SYMBOL_latest.json`, or a trusted process-level `LEVELS_SNAPSHOT_DIR` override. Query parameters cannot select a directory. Reads are bounded to 256 KiB and reject escaping symlinks and non-regular files.

Clock policy targets today's exchange session before close, then the next session at/after close or on non-trading days. Existing holiday/early-close tables and New York timezone rules apply. Calendar dates outside the existing 2025–2027 coverage fail closed. Missing, malformed, stale, future, oversized and wrong-symbol artifacts return explicit unavailable states.

The dashboard displays exact stored prices, snapshot identity, source session/as-of/freshness, calculation version and expiry in a separate canonical panel. Live/exploratory analytics remain separately labeled. The canonical panel remains usable during a live-market-feed outage. Selection/load generations guard late successes, errors and loading-state updates.

## Verification — local, isolated

Delivery regressions were added before the fix and reproduced expired first sends and pre-capture sends reaching the mocked transport. API/UI tests initially failed before their modules and panel existed. Integration also updated older page-contract assertions to distinguish the safe snapshot route from the retired `/api/levels` RF surface, without allowing that retired surface.

Verified on Node 24.22.1 and the repository Python 3.11 virtualenv:

- Full Python smoke suite: 894 tests, OK, two skips.
- Research-protocol suite: 336 tests, OK, one skip.
- Jest: 18 suites, 328 tests passed.
- `npm run test:canonical`: 13 API/DOM/integration tests passed, including reader saturation and capacity recovery after success, callback failure and synchronous spawn failure.
- Standard JS lint, explicit lint for the new helper/Node tests, proxy syntax, build, coverage ratchet and `git diff --check` passed.

The synthetic integration exercises snapshot writing, frozen briefing construction, mocked transport plus receipt, the HTTP read handler and the actual page's DOM renderer. It checks matching snapshot/level identities and prices across these surfaces even when the live feed fails. It uses only temporary artifacts and an ephemeral loopback listener. It is not a real-browser visual test or a full proxy/service-stack test. Restore and intraday-alert delivery are outside that integrated fixture.

`npm run test:canonical` is wired into CI with explicit Python dependencies and lint. Hosted CI has not been run for these uncommitted changes. Independent scoped review found no blocking security or logic defects. Its concurrency-coverage suggestion was addressed and rerun successfully. Full-proxy authentication-dispatch regression remains follow-up; current route placement was independently reviewed and parent-confirmed below the authentication gate. Agent scratch logs use `pivot-canonical-*-final.log`; these are local verification evidence, not committed product data.

## Follow-up work (not completed by this increment)

1. Establish prospective forecast commitment before outcome maturity and historical label-completeness gating.
2. Include authoritative product state, immutable snapshots/briefings and delivery receipts in backup/restore coverage, verified with an isolated restore.
3. Add an end-to-end synthetic day spanning snapshot, HTTP read, UI display, intraday alert identity, mocked delivery and restoration.
4. Run a clean prospective observation period against predeclared baselines before making usefulness or alpha claims.
