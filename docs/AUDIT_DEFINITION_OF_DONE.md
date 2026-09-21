# Broad Audit Definition of Done

A broad audit is complete only when every applicable item below is evidenced. A source inventory, test-count summary, or green CI run is not completion.

## Scope and revisions

- [ ] Record every repository, branch, local SHA, remote head SHA, dirty state, and review time.
- [ ] State each user/product question separately; do not collapse runtime, quantitative, product, launch, and architecture conclusions.
- [ ] Identify excluded paths and residual limitations.

## Implementation and evidence

- [ ] Trace each material output from external input through normalization, storage, computation, API, and UI.
- [ ] Exercise at least one deterministic golden case end to end and inspect the terminal API/UI output.
- [ ] Reproduce each confirmed failure with a local fixture or controlled test; label plausible unreproduced issues **suspected**.
- [ ] Verify point-in-time availability, time boundaries, null/error handling, and degraded-provider behavior.
- [ ] Check selection bias, leakage, calibration, trial count, and transaction costs for quantitative claims.
- [ ] Run installation from the authoritative lockfile in an isolated checkout and record exact commands/results.
- [ ] Run subsystem-specific tests plus the repository's full required gates. A narrow green check clears only that scope.
- [ ] Start relevant services and exercise a realistic workflow. Live-provider evidence is additive, never a substitute for deterministic fixtures.

## Product and launch

- [ ] State who the user is, which decision the output changes, and whether uncertainty/data quality are visible at decision time.
- [ ] Probe malformed, stale, sparse, missing, and internally inconsistent inputs.
- [ ] Evaluate authentication, authorization, secret handling, telemetry, disclosures, reproducibility, support burden, hosted-mode behavior, and rollback.
- [ ] Give separate private-demo, limited-beta, and public-launch verdicts where applicable.

## Completion and reporting

- [ ] Rank findings with severity and confidence as separate fields; include concrete failure path and remediation.
- [ ] Re-check historical ledger findings at the reviewed SHA; an old finding is only a lead.
- [ ] Re-fetch the remote head and re-run change-sensitive checks if it moved.
- [ ] Answer every original question explicitly, including architecture questions only after the primary user job is established.
- [ ] Verify requested external actions by remote observation; otherwise label them **unverified**.
