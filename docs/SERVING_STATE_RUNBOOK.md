# Serving-State Operations Runbook

Use this when you need to pause live serving, resume it, verify what state
the server is actually in, or remediate a corrupt control-plane file.

Audience: on-call / ops. Assumes the ML server is running on
`http://127.0.0.1:5003` (the default). Adjust the host:port if your
deployment differs.

---

## 0) What serving state is

Serving state is the **third axis** of the model lifecycle, alongside
readiness and promotion:

| Axis | Question it answers | Where it lives |
|---|---|---|
| **Readiness** | "Is this candidate artifact mechanically/statistically acceptable?" | Evidence pack (`scripts/run_retrain_evidence_pack.py`) |
| **Promotion** | "Is this candidate allowed to become the active manifest?" | Promotion disposition in the same evidence pack |
| **Serving state** | "Should the currently-active, already-promoted model be answering live predictions **right now**?" | `data/models/serving_state.json` + `server/serving_state.py` |

A promoted model can be paused without losing its promoted status or
changing its manifest. Pausing is **not** demotion, retraining, or
threshold tuning — it just stops `/score` from emitting signals.

## A) Current phase status

| Phase | Status | What it covers |
|---|---|---|
| **D1** | **shipped** | Manual file-flag gate. Operator-only writes via `scripts/set_serving_state.py`. `/score` short-circuit + structured dormant response. `/health.serving_state` exposes current state. |
| **D2** | **shipped** | Observability only. `serving_state_changed` audit event on every CLI write. Process-local counters under `/health.serving_state.observability`. Sampled `predict_blocked_dormant` audit events. **No behavior change.** |
| **D3** | **not active** | Future. Audit-script opt-in automation. Audits stay diagnostic-only today. |
| **D4** | **deferred** | Future. Auto-clear, expiry enforcement at runtime, sticky cooldown. Not implemented; `expires_at` is informational only today. |

## B) First three commands (read-only)

```bash
# Health snapshot — top-level serving_state block.
curl -fsS http://127.0.0.1:5003/health | jq '.serving_state'

# Last few serving-state audit events.
tail -n 50 reports/research_protocol/audit_log.jsonl \
  | grep -E '"serving_state_changed"|"predict_blocked_dormant"' \
  | tail -n 10

# Current on-disk state file (if any).
test -f data/models/serving_state.json \
  && cat data/models/serving_state.json \
  || echo "no serving_state.json on disk (loader default = active)"
```

## C) Expected healthy default when no file exists

When `data/models/serving_state.json` does not exist, the loader
synthesizes a default — this is normal and does NOT mean serving is
paused. Look for these markers in `/health.serving_state`:

```json
{
  "schema_version": 1,
  "state": "active",
  "source": "default_missing_file",
  "reason": "serving_state_missing_default_active",
  "load_error": null,
  "since_ts": null,
  "observability": {
    "transitions_count_in_process": 0,
    "dormant_requests_count_in_process": 0,
    "dormant_requests_count_since_state_set": 0,
    "last_blocked_at_ms": null,
    "last_loaded_at_ms": <some int>,
    "dormant_log_sample_n": 100,
    "dormant_log_min_interval_ms": 60000
  }
}
```

The `observability` sub-block is process-local: counters reset on every
server restart. The audit log is the durable record.

## D) Pause live serving ⚠️ **affects live serving**

`scripts/set_serving_state.py` writes `data/models/serving_state.json`.
The running server **does not** auto-detect the new file — the registry
loads `serving_state.json` only at process startup and on `POST /reload`.
A normal `/score` request does **not** refresh the cached state. Until
one of those two triggers fires, `/score` continues to answer with
whatever state was cached at the previous load.

The pause therefore takes effect in two steps:

1. Run the CLI to write the file.
2. Run `POST /reload` (or restart the process) so `ServingStateRegistry`
   re-reads the file. After that, every `/score` request short-circuits
   to the dormant response.

```bash
# ⚠️ LIVE step 1: writes the pause file. The running server does
# NOT pick this up yet; /score still answers normally until step 2.
.venv/bin/python scripts/set_serving_state.py \
  --state dormant_manual_pause \
  --reason "<short human reason>" \
  --triggering-audit "<optional path to evidence/.../foo.json>" \
  --expires-at "<ISO-8601 or epoch-ms — strongly encouraged>"

# ⚠️ LIVE step 2: tells the server to reload serving_state.json.
# Pause becomes effective when this returns. (Alternative: restart
# the ml_server process.)
curl -fsS -X POST http://127.0.0.1:5003/reload >/dev/null

# Verify it took effect.
curl -fsS http://127.0.0.1:5003/health | jq '.serving_state | {state, source, reason, since_ts}'
```

Expected `/health.serving_state` after pausing:

| Field | Value |
|---|---|
| `state` | `"dormant_manual_pause"` |
| `source` | `"file"` |
| `reason` | the string you passed |
| `since_ts` | epoch ms of the write |
| `load_error` | `null` |

The CLI also emits a `serving_state_changed` audit-log line — see
section **F**.

## E) Resume active ⚠️ **affects live serving**

Same two-step shape as section D — the CLI write does not flip the
running server on its own; `POST /reload` is what activates it.

```bash
# ⚠️ LIVE step 1: writes the active-state file. /score still
# returns dormant responses until step 2.
.venv/bin/python scripts/set_serving_state.py \
  --state active \
  --reason "<reason for resuming, e.g. regime review complete>"

# ⚠️ LIVE step 2: tells the server to reload serving_state.json.
# Once this returns, /score answers normally again.
curl -fsS -X POST http://127.0.0.1:5003/reload >/dev/null

curl -fsS http://127.0.0.1:5003/health | jq '.serving_state.state'
# -> "active"
```

Resuming **never** requires `--force`. The dormant→active transition
is always allowed.

## F) When `--force` is required

`--force` is required **only** for one specific transition:

| Prior state | Target state | `--force` required? |
|---|---|---|
| missing | any | No |
| valid `active` | any dormant | No |
| valid `dormant_X` | same `dormant_X` (refresh) | No |
| valid `dormant_X` | `active` | No |
| **valid `dormant_X`** | **`dormant_Y` (different dormant)** | **Yes** |
| invalid prior file (any reason) | any | No |

If you see the error

```
Refusing to overwrite existing dormant state 'dormant_X' with 'dormant_Y';
pass --force to confirm.
```

…you're being asked to confirm a *cross-dormant* change (e.g. operator
pause → audit-fail pause). Re-run with `--force` if intentional.

## G) Verify `serving_state_changed` audit events

Every successful CLI write emits one `serving_state_changed` event to
`reports/research_protocol/audit_log.jsonl`. Inspect the most recent
event:

```bash
tail -r reports/research_protocol/audit_log.jsonl 2>/dev/null \
  | grep '"serving_state_changed"' | head -n 1 | jq '.'
# (on Linux: tac instead of `tail -r`)
```

Fields in `metadata` you care about:

| Field | Meaning |
|---|---|
| `from_state` | previous on-disk state, or loader-equivalent if the prior file was invalid |
| `from_state_source` | `"file"` (valid prior) / `"missing"` / `"invalid"` |
| `to_state` | the state just written |
| `reason` | operator-supplied reason |
| `triggering_audit` | optional audit-report path that motivated the change |
| `set_by` | `user@hostname` of whoever ran the CLI |
| `manifest_version_when_set` | active manifest version at write time |
| `expires_at` | epoch ms or `null`; **informational only in D1/D2** |
| `since_ts` / `written_at_ms` | epoch ms |
| `state_path` / `state_path_sha256` | the file written + its sha256 |
| `forced` | whether `--force` was passed |

## H) Verify dormant `/score` responses + `predict_blocked_dormant` sampling

When serving is dormant, every `/score` request returns a structured
dormant response with `signal: null`. Quick check (this **does** send a
real request — fine while dormant, returns a no-signal payload):

```bash
curl -fsS -X POST http://127.0.0.1:5003/score \
  -H 'content-type: application/json' \
  -d '{"event": {"symbol": "SPY", "horizon_min": 15}}' | jq '.'
```

Expected response shape:

```json
{
  "signal": null,
  "blocked_reason": "serving_dormant",
  "serving_state": "dormant_manual_pause",
  "serving_state_reason": "...",
  "serving_state_since_ts": 1700000000000,
  "serving_state_expires_at": 1700001000000,
  "serving_state_triggering_audit": null,
  "manifest_version": "v415",
  "manifest_version_when_set": "v415"
}
```

The dormant response intentionally does **not** include probability,
threshold, or any prediction-internal field.

`predict_blocked_dormant` events are **sampled**, not per-request:

- The **first** dormant request after every state transition always
  emits one event (so the audit log captures the start of every
  dormant episode).
- After that, the next event fires only when **both** gates are
  satisfied:
  - rate: at least `ML_SERVING_DORMANT_LOG_SAMPLE_N` dormant requests
    since the last emit (default `100`)
  - time: at least `ML_SERVING_DORMANT_LOG_MIN_INTERVAL_SEC` since
    the last emit (default `60`)

To inspect:

```bash
grep '"predict_blocked_dormant"' reports/research_protocol/audit_log.jsonl \
  | tail -n 5 | jq '{event_type, decision, metadata}'
```

Cross-check the live counters at `/health.serving_state.observability`:

```bash
curl -fsS http://127.0.0.1:5003/health \
  | jq '.serving_state.observability'
```

`dormant_requests_count_in_process` ticks every request; the audit
events are the sampled subset.

## I) Confirm nothing else changed

After any pause/resume, verify that **only** the serving-state file
moved. Manifests, model artifacts, threshold configs, evidence reports,
and DBs must be byte-stable:

```bash
git status --short data/models scripts evidence reports/research_protocol \
  | grep -vE 'serving_state\.json|audit_log\.jsonl' \
  || echo "no other tracked changes (expected)"
```

`serving_state.json` is gitignored (operational state, not source).
`audit_log.jsonl` is also a tracked artifact path but lives under
`reports/research_protocol/` which is gitignored.

## J) Safety warnings (the gate is not a substitute for these)

Do **not** use serving state to:

- **Demote a model.** Promotion lives in `manifest_active.json`. Pause
  is reversible; demotion is a manifest rewrite via
  `scripts/model_governance.py`.
- **Retrain.** Retraining is a separate, governance-gated event under
  `scripts/run_retrain_cycle.sh`.
- **Change thresholds.** Threshold values live in the manifest and the
  model pickles. Use `scripts/refit_calibration.py` for threshold
  refits, not the serving gate.
- **Test in production.** The gate is one-bit; there is no "partial
  shadow" mode in D1/D2.
- **Indicate model failure.** A model emitting weak signals is normal;
  pausing should reflect a specific operator decision (regime concern,
  data-quality concern, ops review), not "I think it's bad."

## K) Troubleshooting an invalid/corrupt `serving_state.json`

If the file is unparseable or schema-invalid, the loader **fails
closed**: it treats the file as `dormant_data_quality` regardless of
what state value the corrupt bytes claim. This is intentional — an
unparseable control-plane file must not silently allow serving.

You'll see this in `/health.serving_state`:

```json
{
  "state": "dormant_data_quality",
  "source": "invalid_file",
  "reason": "serving_state_invalid",
  "load_error": "<the parse / schema error>"
}
```

To remediate, just write a valid file and trigger a reload. **`--force`
is NOT required** when the prior file is invalid:

```bash
# ⚠️ LIVE step 1: replaces the corrupt file with a fresh valid record.
# The server is still serving its cached dormant_data_quality state
# until step 2 reloads.
.venv/bin/python scripts/set_serving_state.py \
  --state active \
  --reason "remediating corrupt serving_state.json"

# ⚠️ LIVE step 2: reload so the server drops its cached
# dormant_data_quality and re-reads the fresh valid record.
curl -fsS -X POST http://127.0.0.1:5003/reload >/dev/null
```

The audit event for this remediation records `from_state_source="invalid"`
and the loader-equivalent `from_state="dormant_data_quality"`, so the
audit trail reflects what the server **would have honored** at the
moment of remediation — not the literal corrupt bytes.

## L) `expires_at` is informational only

You can (and should) pass `--expires-at` when pausing — but **the
runtime does not enforce it in D1/D2.** The field is recorded in the
state file and in the `serving_state_changed` audit event so operators
can see the intended review date, but the server does **not**
auto-clear when `expires_at` passes.

If you want the state cleared, an operator must run the same two-step
sequence as section E (CLI write + `POST /reload`):

```bash
# ⚠️ LIVE step 1: writes the active-state file.
.venv/bin/python scripts/set_serving_state.py \
  --state active --reason "expiry review complete"

# ⚠️ LIVE step 2: reload so the server picks it up.
curl -fsS -X POST http://127.0.0.1:5003/reload >/dev/null
```

Auto-clear + runtime expiry enforcement are **D4**, deferred until
D2 + D3 have produced enough operational data to choose anti-flap
parameters from.

## M) Environment knobs

| Variable | Default | What it controls |
|---|---|---|
| `ML_SERVING_DORMANT_LOG_SAMPLE_N` | `100` | Min dormant requests between sampled `predict_blocked_dormant` emits. |
| `ML_SERVING_DORMANT_LOG_MIN_INTERVAL_SEC` | `60` | Min seconds between sampled emits per state. |
| `RF_MODEL_DIR` | `data/models` | Directory the registry resolves `serving_state.json` against. |
| `PIVOTQUANT_RESEARCH_PROTOCOL_ROOT` | `reports/research_protocol` | Where the audit log lives. |

The first two are observability-only — they affect audit-event volume,
not the gate's pass/block decision.

## N) Related references

- `server/serving_state.py` — registry + observability classes.
- `scripts/set_serving_state.py` — the **only** writer of `serving_state.json`.
- `services/research_protocol/audit_logger.py` — where the audit
  events are appended (JSONL, atomic appends with an OS advisory lock).
- `CLAUDE.md` — three-axis invariants and discipline contract.
