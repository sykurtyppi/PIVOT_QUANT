# Repository Operating Rules

These rules apply to the entire repository.

## Environment and installation

- Work in an isolated checkout or worktree; never repurpose a user's active checkout.
- Use `npm ci`; the root `package-lock.json` is authoritative for JavaScript dependencies.
- For a clean checkout, create the repository-local Python 3.11 environment and install the pinned runtime/smoke dependencies before running Python checks:
  ```bash
  python3.11 -m venv .venv
  .venv/bin/python -m pip install -r requirements-runtime.txt
  ```
  `scripts/_pybin_exec.sh` (and therefore `npm run ml:test:smoke`) follows the repository resolver order: an explicit `PYTHON_BIN`, then `.venv313`, then `.venv`, then a compatible system `python3`. In a clean checkout with no higher-precedence override, it selects the `.venv` created above.
- Python utilities must run through `scripts/_pybin_exec.sh` and their documented environment. Do not assume a bare `python` command or mutate the operator's environment.
- Do not commit generated builds, local databases, logs, reports, model artifacts, or credentials unless the file is explicitly versioned and its provenance is documented.

## Canonical checks

- JavaScript: `npm run lint && npm test && npm run build`
- Python smoke suite: `npm run ml:test:smoke`
- Performance-sensitive changes: `npm run benchmark` and the relevant stress/performance target with recorded hardware/runtime context.
- Security/operations checks in CI remain required; a narrow test result clears only its path.

## Quantitative and operational boundaries

- Use frozen inputs and deterministic clocks for regressions. Validate timestamp order, session/calendar assumptions, feature availability, costs, slippage, and trial counts.
- Never train, reconcile, or report against future labels or mutable live data without an explicit point-in-time boundary.
- Local tests use temporary databases and directories; never write to operator datasets, launch agents, scheduled jobs, notification endpoints, or production model paths.
- Public performance claims require reproducible evidence at the reviewed SHA, including sample size, selection process, costs, uncertainty, and genuine forward/OOS status.

## Product and architecture

- Establish the primary user job before proposing integration with another analytical product.
- Shared shell/design/platform services and merged analytical engines are separate decisions. Merge engines only when inputs, semantics, validation, failure policy, release cadence, and ownership are demonstrably compatible.
- Deployments, launch-agent changes, messages, migrations, and model promotion require explicit authorization, rollback coverage, and remote/state verification.
- Broad audits must satisfy `docs/AUDIT_DEFINITION_OF_DONE.md`; inventory is not completion.
