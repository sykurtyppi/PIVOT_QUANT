# Dashboard runtime enforcement and occupied-port recovery

## Scope and policy

The dashboard's approved Node range remains `package.json` → `engines.node`
(currently `>=24.22.1 <25`); `.nvmrc` and `.node-version` both select `24.22.1`.
This change does not set a new security patch floor or update these selectors.
`server/node_runtime.js` reads that package policy. It accepts its existing
`>=major.minor.patch <major` format and rejects unsupported runtimes or an
unrecognized policy format. A future policy-format change must update the
validator and its regression tests together.

`server/yahoo_proxy.js` imports the validator before dashboard initialization and
listening. Direct invocation (`node server/yahoo_proxy.js`, including the npm
`dashboard:server` script) therefore cannot bypass the policy. `run_all.sh` uses
the same validator with its selected `NODE_BIN`, rather than keeping a separate
shell version comparison.

## Occupied dashboard port: refuse, do not adopt

The launcher requires `lsof` and rejects an occupied dashboard port (normally
3000) before migrations or service starts. It does **not** trust runtime strings,
HTTP success, missing metadata, or even a listener claiming an approved runtime.
It neither adopts nor terminates an existing dashboard listener.

Read-only diagnosis:

```bash
lsof -nP -iTCP:3000 -sTCP:LISTEN
```

Inspect the reported PID and identify its actual owning supervisor. With separate
authorization, stop/restart that listener through its owner and select an approved
Node executable for the next launcher run (`NODE_BIN=/absolute/path/to/node`).
Do not kill all Node processes or remove an unrelated service to free the port.
Do not restart a production stack as part of running these regressions.

The launcher checks again when starting the dashboard. For a listener racing the
startup, dashboard readiness additionally requires the exact dashboard child PID
started by this launcher to own the listening port, pass HTTP health, and still
own the port after health. Failure prevents `All services ready.` and cleanup
remains limited to this launcher's managed children. Existing reuse/health
behavior for the bridge, event writer, ML server, and collector is unchanged.
These startup checks are not generalized process attestation or a guarantee
against every possible post-readiness runtime failure.

## Isolated regression checks

Provision the repository's locked JavaScript dependencies using the selected
approved Node and npm, and Python 3.11 smoke dependencies in the worktree only:

```bash
# First set PATH to an installed approved Node and the intended npm executable.
node --version
npm --version
npm ci
python3.11 -m venv .venv
.venv/bin/python -m pip install -r .github/ci/requirements-smoke.txt
```

Use a scratch directory outside the checkout and explicit interpreter selection:

```bash
export TMPDIR=/absolute/path/to/isolated/scratch
export NODE_BIN=/absolute/path/to/approved/node
export NODE22_BIN=/absolute/path/to/unsupported/node22
export PYTHON_BIN="$PWD/.venv/bin/python"
scripts/_pybin_exec.sh -m unittest discover -s tests/python -p test_dashboard_runtime.py -v
npm run lint
npm test -- --runInBand
npm run build
npm run coverage:check
npm run ml:test:smoke
./node_modules/.bin/eslint server/node_runtime.js server/yahoo_proxy.js
node --check server/node_runtime.js
node --check server/yahoo_proxy.js
bash -n server/run_all.sh
```

The new regression module copies the real dashboard entrypoint, runtime helper,
package policy, and launcher into temporary fixture trees. It substitutes only
fixture service ports/paths, stubs migrations and unrelated services, scrubs
operator service/database environment variables, and disables the collector.
Listeners bind ephemeral loopback ports. Cleanup terminates recorded child PIDs
and fixture-owned HTTP servers, never discovers and kills an external PID.

Tests cover the actual Node 22 entrypoint and launcher, actual Node 22 listener
adoption bypass, supported startup, policy boundaries, metadata-independent
occupied-port refusal, a bind race, shell syntax, and shutdown ownership. Only
version-boundary cases simulate `process.versions.node` using a fixture preload;
actual Node 22 cases use a separate executable. Set `NODE22_BIN` explicitly when
available: three actual-Node-22 cases skip without it (or a `node22` on PATH), so a
passing run with skips is not equivalent evidence. `NODE_BIN` must resolve to an
installed approved runtime.

## Local evidence and remaining gates

The corrective work was exercised on Darwin arm64 with Node `v24.22.1`, npm
`11.19.1`, Python `3.11.15`, and a separate actual Node `v22.23.3`. Logs are
external scratch artifacts (`pivot-runtime-*`), not repository fixtures or a
production rollout record. The PR records exact check results.

The canonical lint command excludes this server entrypoint. Explicit server
ESLint currently reports the existing unused `readTailLines` function plus six
console warnings in `yahoo_proxy.js`; the baseline file at the parent revision
has the same findings. The new helper is lint-clean. Do not represent the
explicit server lint as green or silently expand this fix into unrelated
cleanup. Jest's coverage ratchet covers `src/**`, not dashboard startup; the
subprocess regressions provide the dashboard-specific evidence.

Independent review, exact-head CI/security checks, deployment authorization,
and any production restart/runtime verification remain separate gates. This
runbook grants no permission to restart, merge, deploy, promote models, alter
launch agents, or stop an existing listener.
