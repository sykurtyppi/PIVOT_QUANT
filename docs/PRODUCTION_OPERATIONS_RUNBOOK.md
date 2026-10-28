# PIVOT QUANT Production Operations Runbook

This is the authoritative operating procedure for the current macOS production deployment. It replaces generic deployment checklists with evidence gates: a component is healthy only when its registration, execution, output, freshness, and downstream effect are independently proven.

## 1. Scope and safety boundaries

The production product is the deterministic levels workflow. The predictive ML system remains governed research infrastructure; service uptime or fresh model files do not establish economic edge.

Default production paths on the current host:

```bash
export PIVOT_DEPLOY=/Users/tristanalejandro/PIVOT_QUANT-main-deploy
export PIVOT_DATA=/Users/tristanalejandro/PIVOT_QUANT/data
export PIVOT_UID="$(id -u)"
```

Before running commands, confirm these paths describe the intended host. Never print `.env`, credentials, webhook URLs, cookies, or message-recipient values into an evidence bundle.

Operational invariants:

- Preserve pre-change evidence before changing launchd jobs, deployment state, databases, model paths, or scheduler ownership.
- Treat endpoint reachability, launchd registration, process execution, durable output, external publication, and semantic freshness as separate claims.
- Do not retrain or promote a model merely because it is old, reachable, or operational.
- Do not rewrite historical event identities or silently advance/discard blocked events.
- Monitoring and observation are read-only. They must not restart services, retrain, promote, rewrite events, mutate databases, or change schedulers.
- A manual run proves only that manual path. It does not prove the next scheduled run.
- A successful report generation does not prove external publication.
- A zero exit status does not override missing, stale, incorrectly dated, or semantically invalid output.

## 2. Current ownership model

The production reporting path deliberately separates generation from publication:

1. `com.pivotquant.daily_report` runs `scripts/run_daily_report_send.sh` with `ML_REPORT_GENERATION_ONLY=true`.
2. The wrapper applies the trading-day gate, generates the exact-date report, and attempts the gamma snapshot. Gamma failure is non-fatal.
3. The approved external publisher independently validates and publishes the exact-date report. Repository code must not fall back to placeholder email, iMessage, or webhook configuration in generation-only mode.
4. `com.pivotquant.health_alert` remains the 60-second service watchdog.
5. A temporary bounded, read-only end-of-session observer complements the watchdog while the release is evaluated. It is not a replacement scheduler.

Do not retire either report scheduler or transfer ownership until the consolidation gate in section 9 is satisfied and rollback is prepared.

launchd calendar intervals use the Mac's local timezone. Hermes cron expressions are UTC-based in the current configuration. Re-evaluate their relative ordering before every daylight-saving transition; a weekday cron expression also requires the exchange-calendar gate because it cannot identify US market holidays.

## 3. Evidence model

Use this sequence whenever validating an operational claim:

1. **Configuration:** the plist/script contains the intended settings.
2. **Registration:** launchd has loaded the intended job and environment.
3. **Execution:** the run count advances and the completed run has the expected exit status.
4. **Persistence:** the expected artifact or database state exists.
5. **Freshness and semantics:** the artifact has the correct trading date, age, heading, and content state.
6. **Publication:** the external delivery system records a successful payload for that exact date.
7. **Ordinary scheduling:** at least one non-forced scheduled run produces the same result.

Do not collapse these into a single “green” status.

### 3.1 Evidence contracts

Each claim below has its own source, freshness condition, interpretation, and safe response:

- **Deployment identity** — Source: `git rev-parse HEAD`, `.deploy/deployed-source-sha`, and `git status` including untracked files. Freshness: capture in the same observation. Pass: SHA values match and the worktree status is empty. Fail: preserve the diff and process evidence; reconstruct the intended state in a clean worktree rather than resetting blindly. Do not run a blind `git clean` on production.
- **launchd registration** — Source: `launchctl print gui/$UID/com.pivotquant.daily_report`. Freshness: capture after every plist change/reload and during the observation. Pass: the loaded path and required environment match the intended plist. Fail: preserve the plist and loaded state, then reload from the logged-in user's normal Terminal; do not compensate by changing source defaults.
- **Generator execution** — Source: launchd `runs`, `state`, `last exit code`, and `logs/report_delivery.log`. Freshness: the run count must advance for the session being claimed. Pass: the completed exact-date trading-session run exits `0` and records generation-only success. Fail: preserve stdout/stderr and wrapper logs before changing dependencies or configuration.
- **Report artifact** — Source: `logs/reports/ml_daily_YYYY-MM-DD.md`. Freshness: exact exchange date and no more than 180 minutes old at end-of-session observation. Pass: expected heading and readable nonempty content. Fail: classify as generation/freshness failure; do not publish an older `latest` artifact.
- **Gamma snapshot** — Source: the same wrapper log and gamma history/output named by the run. Freshness: the attempt belongs to the exact generator run. Pass: snapshot success and data-sanity result are recorded. Warning: gamma is unavailable but the report path completed. Response: diagnose the selected interpreter and tracked gamma requirements without reclassifying report generation as failed.
- **External publication** — Source: the exact-date Hermes publisher output. Freshness: no more than 180 minutes old at end-of-session observation. Pass: successful report payload exists for the same exchange date and contains no alert marker. Fail: classify as publication failure; do not enable repository legacy transports as an unreviewed fallback.
- **Watchdog and services** — Source: `logs/health_alert_state.json`, watchdog logs, and launchd registration. Freshness: watchdog state no more than 3.5 minutes old. Pass: collector and ML are both `up` with current status. Fail: distinguish stale monitoring evidence from a down service, preserve evidence, and restart only an identified owner if intervention is approved.
- **SQLite integrity** — Source: read-only `PRAGMA quick_check` for both production databases. Freshness: current observation and after any storage incident or restore. Pass: both return `ok`. Fail: stop mutation paths that could worsen damage, preserve the files, and test restoration in a temporary path.
- **Outcome maturation** — Source: read-only event/prediction/label counts for the prior trading session, by governed horizon. Freshness: after the next morning's levels cycle and horizon grace period. Pass: no unexplained overdue labels and qualified labels exist when due predicted events exist. Fail: preserve affected event identities and investigate the label path; never infer maturation from builder exit status or rewrite history.
- **Ordinary scheduling** — Source: non-forced launchd and Hermes records. Freshness: each claimed exchange session. Pass: generation and publication complete without manual kickstart. Fail: keep schedulers separate and the observation window open; a manual smoke run cannot close this claim.

## 4. Routine read-only production check

Run from the deployment checkout:

```bash
cd "$PIVOT_DEPLOY"

printf '%s\n' '--- deployment identity ---'
git rev-parse HEAD
cat .deploy/deployed-source-sha
git status --porcelain --untracked-files=all

printf '%s\n' '--- launchd summary ---'
bash scripts/launch_agent_status.sh

printf '%s\n' '--- report generator registration ---'
launchctl print "gui/${PIVOT_UID}/com.pivotquant.daily_report" \
  | grep -E 'path =|state =|runs =|last exit code|ML_REPORT_GENERATION_ONLY|ML_REPORT_REPORT_DATE_MODE|ML_REPORT_SCHEDULE_MODE'

printf '%s\n' '--- recent report evidence ---'
tail -n 80 logs/report_delivery.log
tail -n 80 logs/daily_report.launchd.err.log

printf '%s\n' '--- watchdog evidence ---'
tail -n 80 logs/health_alert.log
tail -n 80 logs/health_alert.launchd.err.log
```

Expected evidence:

- Git `HEAD` equals `.deploy/deployed-source-sha` and the full worktree status is empty.
- The loaded report job contains `ML_REPORT_GENERATION_ONLY = true`.
- The latest completed report run exits `0` on a trading day.
- `logs/report_delivery.log` contains an exact-date generation success such as `DONE  daily_report_generate_only`.
- No legacy email, iMessage, or webhook send attempt occurs in generation-only mode.
- The watchdog state is recent and reports the collector and ML service as up.

The current bounded observer is the executable acceptance check for report freshness, publisher evidence, watchdog age and service state, prior-session label maturation, event-database integrity, and tracked deployment identity. The separate `git status --untracked-files=all` check above remains required for full-worktree drift:

```bash
export HERMES_HOME=/Users/tristanalejandro/.hermes
"$HERMES_HOME/hermes-agent/venv/bin/python" \
  "$HERMES_HOME/scripts/pivot_quant_production_observer.py"
```

The command is read-only. An ordinary session passes only when its first line is `[OK]`; retain every warning or failure line. The scheduled observer's durable output is under `$HERMES_HOME/cron/output/7ba0c4e460a5/`. Exact-date publication evidence is under `$HERMES_HOME/cron/output/c18786f257c1/`. The current observer runs at `10 23 * * 1-5` UTC with five bounded invocations; invocation count is not session count. Verify the IDs and schedules in Hermes before relying on them after any automation change.

For SQLite integrity, use read-only connections against the live data files:

```bash
python3 - <<'PY'
import os
import sqlite3
from pathlib import Path

for name in ("pivot_events.sqlite", "levels_product.sqlite"):
    path = Path(os.environ["PIVOT_DATA"]) / name
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    try:
        result = conn.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        conn.close()
    print(f"{name}: {result}")
PY
```

Both results must be `ok`. This proves file integrity, not data freshness or label completeness.

## 5. Daily-report acceptance criteria

For each ordinary trading session, record all of the following:

- Exchange date and check time in `America/New_York`.
- Generator launchd run count and last exit status.
- Exact artifact: `logs/reports/ml_daily_YYYY-MM-DD.md`.
- Artifact heading: `# Daily ML Report - YYYY-MM-DD`.
- Artifact modification age at observation time.
- Gamma snapshot result, including an explicit warning if unavailable.
- Publisher evidence for the same exchange date.
- Watchdog age and collector/ML service state.
- Read-only SQLite `quick_check` results.
- Matured event/label counts for the prior trading session.
- Deployment `HEAD`, source marker, and full-worktree drift result.

Classify outcomes as:

- **OK:** every required layer is present, current, and semantically correct.
- **WARNING:** the core report path succeeded but a non-blocking condition needs follow-up, such as non-fatal gamma unavailability or a pre-baseline label session.
- **ALERT:** generation, exact-date validation, freshness, publication, database integrity, service freshness, deployment identity, or matured-label expectations fail.
- **SKIP:** the exchange calendar says the date is not a trading day. A weekday alone is not sufficient.

## 6. Exact-SHA deployment procedure

### 6.1 Preserve pre-change evidence

Create a timestamped evidence directory outside the repository and capture only non-secret state:

```bash
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE="$HOME/.hermes/cache/scratch/pivot_deploy_${STAMP}"
mkdir -p "$EVIDENCE"
chmod 700 "$EVIDENCE"
umask 077

cd "$PIVOT_DEPLOY"
git rev-parse HEAD > "$EVIDENCE/deployed-head.txt"
git status --porcelain --untracked-files=all > "$EVIDENCE/worktree-status.txt"
cp .deploy/deployed-source-sha "$EVIDENCE/deployed-source-sha.txt"
PLIST="$HOME/Library/LaunchAgents/com.pivotquant.daily_report.plist"
shasum -a 256 "$PLIST" > "$EVIDENCE/daily-report-plist.sha256"
for key in \
  Label \
  EnvironmentVariables.ML_REPORT_GENERATION_ONLY \
  EnvironmentVariables.ML_REPORT_REPORT_DATE_MODE \
  EnvironmentVariables.ML_REPORT_SCHEDULE_MODE; do
  printf '%s=' "$key" >> "$EVIDENCE/daily-report-plist-allowlist.txt"
  plutil -extract "$key" raw -o - "$PLIST" \
    >> "$EVIDENCE/daily-report-plist-allowlist.txt"
  printf '\n' >> "$EVIDENCE/daily-report-plist-allowlist.txt"
done
launchctl print "gui/${PIVOT_UID}/com.pivotquant.daily_report" | awk '
  /^[[:space:]]*(path|state|runs|last exit code)[[:space:]]*=/ { print }
  /^[[:space:]]*(ML_REPORT_GENERATION_ONLY|ML_REPORT_REPORT_DATE_MODE|ML_REPORT_SCHEDULE_MODE)[[:space:]]*=>/ { print }
' > "$EVIDENCE/daily-report-launchctl-allowlist.txt"
tail -n 200 logs/report_delivery.log > "$EVIDENCE/report-delivery-tail.txt"
```

The procedure writes only an allowlist of non-secret plist/runtime fields and restricts the directory to the operator. Review the bundle before sharing it; never add `.env`, full environment dumps, cookies, recipient values, webhook URLs, or connection strings.

### 6.2 Verify the candidate

Deploy only a reviewed commit already present on the protected integration branch:

```bash
export CANDIDATE_SHA='<40-character-reviewed-sha>'

git fetch origin --tags
test "$(git rev-parse origin/main)" = "$CANDIDATE_SHA"
test -z "$(git status --porcelain --untracked-files=all)"
git cat-file -e "${CANDIDATE_SHA}^{commit}"
```

Validate the candidate itself in a clean detached worktree. Reusing the deployed checkout for these tests would exercise the old revision:

```bash
CANDIDATE_WORKTREE="$HOME/.hermes/cache/scratch/pivot_candidate_${CANDIDATE_SHA}"
test ! -e "$CANDIDATE_WORKTREE"
git worktree add --detach "$CANDIDATE_WORKTREE" "$CANDIDATE_SHA"
cleanup_candidate() {
  git -C "$PIVOT_DEPLOY" worktree remove --force "$CANDIDATE_WORKTREE" \
    >/dev/null 2>&1 || true
}
trap cleanup_candidate EXIT

(
  cd "$CANDIDATE_WORKTREE"
  export PYTHON_BIN="$PIVOT_DEPLOY/.venv313/bin/python"
  export PYTHONDONTWRITEBYTECODE=1
  bash -n scripts/run_daily_report_send.sh
  ./scripts/_pybin_exec.sh -m py_compile scripts/send_daily_report.py
  ./scripts/_pybin_exec.sh -m unittest discover \
    -s tests -p 'test_daily_report_dedup.py' -v
  ./scripts/_pybin_exec.sh -m unittest discover \
    -s tests -p 'test_report_delivery_transport.py' -v
)

cleanup_candidate
trap - EXIT
```

Run the full CI-equivalent suites when source behavior, dependencies, shared scripts, or runtime contracts changed. The reused interpreter verifies the candidate against the current production runtime. If tracked dependency declarations changed, instead create a candidate-local environment from those declarations and verify that environment before deployment.

### 6.3 Deploy and identify

```bash
git reset --hard "$CANDIDATE_SHA"
printf '%s\n' "$CANDIDATE_SHA" > .deploy/deployed-source-sha

test "$(git rev-parse HEAD)" = "$CANDIDATE_SHA"
test "$(cat .deploy/deployed-source-sha)" = "$CANDIDATE_SHA"
test -z "$(git status --porcelain --untracked-files=all)"
```

Do not copy an old virtual environment, `node_modules`, generated output, ignored application state, or an unreviewed patch into a clean deployment. Install runtime dependencies from tracked lock or requirements files and verify them with the relevant package manager.

### 6.4 Reload launchd safely

If the plist or its environment changed, reloading is mandatory; editing the file does not update an already loaded job.

From the logged-in user’s normal macOS Terminal:

```bash
launchctl bootout "gui/$(id -u)" \
  "$HOME/Library/LaunchAgents/com.pivotquant.daily_report.plist" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" \
  "$HOME/Library/LaunchAgents/com.pivotquant.daily_report.plist"
```

Then verify the loaded environment rather than trusting a silent command:

```bash
launchctl print "gui/$(id -u)/com.pivotquant.daily_report" \
  | grep -E 'path =|state =|runs =|last exit code|ML_REPORT_GENERATION_ONLY|ML_REPORT_REPORT_DATE_MODE|ML_REPORT_SCHEDULE_MODE'
```

### 6.5 Exercise and validate

A forced run is a smoke test only:

```bash
before_runs="$(launchctl print "gui/${PIVOT_UID}/com.pivotquant.daily_report" | awk '/runs =/{print $3; exit}')"
started_at="$(date +%s)"
launchctl kickstart -k "gui/${PIVOT_UID}/com.pivotquant.daily_report"

deadline=$((SECONDS + 600))
while (( SECONDS < deadline )); do
  job="$(launchctl print "gui/${PIVOT_UID}/com.pivotquant.daily_report")"
  runs="$(printf '%s\n' "$job" | awk '/runs =/{print $3; exit}')"
  state="$(printf '%s\n' "$job" | awk '/state =/{print $3; exit}')"
  if [[ "$runs" =~ ^[0-9]+$ ]] \
    && (( runs > before_runs )) \
    && [[ "$state" != "running" ]]; then
    break
  fi
  sleep 2
done

test "$runs" -gt "$before_runs"
test "$state" != "running"
last_exit="$(printf '%s\n' "$job" | awk -F'= ' '/last exit code =/{print $2; exit}')"
test "$last_exit" = "0"

report_date="$(TZ=America/New_York date +%F)"
report="logs/reports/ml_daily_${report_date}.md"
test -s "$report"
test "$(stat -f %m "$report")" -ge "$started_at"
test "$(sed -n '1p' "$report")" = "# Daily ML Report - ${report_date}"

printf '%s\n' "$job" \
  | grep -E 'state =|runs =|last exit code|ML_REPORT_GENERATION_ONLY'
tail -n 100 logs/report_delivery.log
```

The bounded loop proves that the new run completed, exited successfully, and wrote a new exact-date artifact. Separately verify the gamma attempt, absence of legacy transport attempts, and deployment identity. Then observe the next ordinary scheduled generator and publisher runs before declaring steady-state scheduling proven.

### 6.6 Tag only after runtime proof

Create a release tag only after the exact SHA is deployed and the runtime checks above pass:

```bash
export RELEASE_TAG='<approved-release-tag>'
git tag -a "$RELEASE_TAG" "$CANDIDATE_SHA" -m '<release description>'
git push origin "refs/tags/$RELEASE_TAG"
test "$(git ls-remote --tags origin "refs/tags/${RELEASE_TAG}^{}" | awk '{print $1}')" = "$CANDIDATE_SHA"
```

## 7. Incident decision paths

### Report job is loaded but behavior reflects old settings

Likely cause: the plist changed on disk but the loaded launchd environment is stale.

1. Preserve `launchctl print`, the plist, logs, deployment `HEAD`, and marker.
2. Compare only the required non-secret environment keys.
3. Reload with `bootout` and `bootstrap` from the logged-in user’s Terminal.
4. Re-read `launchctl print` and exercise one smoke run.
5. Do not change source defaults to compensate for stale runtime registration.

### Report was generated but not published

1. Validate the exact-date artifact and freshness.
2. Check external publisher output for the same exchange date.
3. Treat this as a publication failure, not a generation failure.
4. Do not enable legacy transports as an unreviewed workaround.

### Gamma snapshot failed

1. Confirm the report itself completed and remained publishable.
2. Inspect the gamma error without changing report-delivery success semantics.
3. Verify the selected project interpreter and install only tracked gamma requirements.
4. Run `python -m pip check` and a direct import check.
5. Re-run a smoke test and capture the new gamma result.

### Watchdog or collector evidence is stale

1. Check `logs/health_alert_state.json`, watchdog logs, and launchd registration.
2. Distinguish stale observer evidence from an actually down service.
3. Use health endpoints as another layer, not as proof of fresh collection or persistence.
4. Preserve evidence before any restart. Restart only the affected, identified owner.

### SQLite integrity fails

1. Stop automated mutation paths that could worsen the incident.
2. Preserve the database and related logs without opening the live file read-write.
3. Use the latest complete backup and follow `docs/INCIDENT_RUNBOOK.md` and `docs/OPS_RESILIENCE.md`.
4. Restore into a temporary path and run `PRAGMA quick_check` before considering replacement.
5. Do not infer that a successful backup job guarantees a restorable snapshot.

### Deployment drift or marker mismatch

1. Preserve `git status`, `HEAD`, marker, and running-job evidence.
2. Do not reset blindly if the drift may contain unreviewed production changes.
3. Reconstruct the intended state in a clean worktree, review it, and deploy an exact SHA.
4. Update the marker only as part of the verified deployment transaction.

## 8. Rollback procedure

Rollback is an exact-SHA deployment, not a file-by-file repair:

1. Preserve current evidence.
2. Select the last runtime-proven SHA; do not assume the previous commit is safe.
3. Confirm the rollback SHA exists on the remote and record why it is selected.
4. Reset the deployment checkout to that SHA and update the source marker.
5. Restore tracked runtime requirements for that revision.
6. Reload any changed launchd plist.
7. Repeat registration, execution, persistence, freshness, and publication checks.
8. Keep the failed candidate and evidence available for diagnosis.

Model rollback is separate. Follow `docs/INCIDENT_RUNBOOK.md` and the model-governance controls; do not use code rollback as an implicit model-promotion decision.

## 9. Scheduler-consolidation gate

The generation LaunchAgent and external publisher remain intentionally separate during the observation window. Consolidation is allowed only when all of the following are true:

- 3–5 clean ordinary trading sessions have been observed after the relevant fix.
- Holiday or `SKIP` invocations are not counted as clean trading sessions.
- Exact-date generation and publication succeeded for every counted session.
- Watchdog and collector evidence remained fresh.
- Matured prior-session labels had no unexplained overdue gaps.
- Both SQLite databases passed integrity checks.
- Deployment `HEAD`, source marker, and full worktree status remained consistent.
- Daylight-saving behavior and schedule ownership are explicitly accounted for.
- The selected authoritative scheduler has a tested rollback path.
- The retired scheduler can be restored without reconstructing secrets or undocumented state.

Record the decision, owner, rollback command, and first post-consolidation observation. Do not infer five clean sessions merely because a job was configured to repeat five times.

## 10. Evidence record template

Use one record per intervention or observed session:

```text
Date/time (UTC and America/New_York):
Operator:
Reason / expected outcome:
Target host and deployment path:
Pre-change HEAD / marker / full worktree status:
Candidate or rollback SHA:
Changed files or runtime dependencies:
Launchd configuration and loaded-registration evidence:
Manual run evidence:
Exact-date artifact and freshness:
Gamma result:
External publication evidence:
Watchdog / collector evidence:
SQLite integrity:
Matured label evidence:
Ordinary scheduled-run evidence:
Outcome: OK | WARNING | ALERT | SKIP
Remaining risk / next observation:
Rollback SHA and command:
Evidence bundle path:
```

Never place secrets in this record.

## 11. Current release baseline

The report-delivery safety release established on 2026-10-28 is:

- Source and deployment SHA: `fe328f07365fc4dcd05586ffad6bec7e855a69a4`
- Release tag: `report-delivery-2026-10-28`
- Report generator policy: `ML_REPORT_GENERATION_ONLY=true`
- External publication: separate approved publisher
- Health monitoring: existing 60-second watchdog plus a temporary bounded read-only observer

This baseline is historical evidence, not a permanently hard-coded target. Future deployments must record and verify their own exact SHA and release evidence.
