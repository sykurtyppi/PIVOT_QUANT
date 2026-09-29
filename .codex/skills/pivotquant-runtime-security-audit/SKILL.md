---
name: pivotquant-runtime-security-audit
description: Use this skill in the Pivot Quant repo to audit only runtime security, local-network exposure, auth, input validation, process management, launch scripts, endpoint safety, secrets/config leakage, and operational safety risks. Do not use for model accuracy, trading methodology, style, or UI polish unless it creates security risk.
---

# Pivot Quant Runtime Security Audit

## Description

Audit runtime, local-network, auth, input-validation, process-management, and operational safety risks in this repository. The audit is read-only and should focus on risks that could expose services, mutate state, crash local services, leak secrets/config, or break operational assumptions.

Hard rule: do not modify files during this audit.

## When To Use

Use when the user asks for:
- runtime security audit
- local network exposure review
- auth or endpoint safety review
- input validation and crash-risk audit
- launch script, process management, or operational safety review
- secrets/config leakage review

## Scope

Focus only on runtime/ops safety issues:
- externally bound services and binding defaults
- unauthenticated sensitive endpoints
- reload, score, event, bar, review, and write endpoints
- open redirects and auth bypasses
- request body size and payload validation
- bad input causing uncaught exceptions or process crashes
- unsafe subprocess, shell, LaunchAgent, or path usage
- secrets/config leakage through `.env`, logs, health/debug endpoints, or UI
- health/debug endpoints that expose sensitive deployment posture
- launch scripts and assumptions about loopback-only versus LAN-accessible services
- local proxy behavior across `server/yahoo_proxy.js` route modules and Python sidecars

## Out Of Scope

Ignore unless it creates a security or operational safety issue:
- model accuracy or projection quality
- trading strategy quality
- statistical methodology
- UI polish
- formatting and style
- generic refactors

## Repo Files To Inspect

Start with these files and directories:
- Dashboard/proxy/auth: `server/yahoo_proxy.js`, `server/routes/runtime.js`, `server/routes/market.js`, `server/routes/models.js`, `server/routes/research.js`, `server/routes/static.js`
- Sensitive services: `server/ml_server.py`, `server/event_writer.py`, `server/live_event_collector.py`, `server/research_api.py`, `server/model_api.py`, `server/ibkr_gamma_bridge.py`
- Launch/process scripts: `server/run_all.sh`, `server/run_persistent_stack.sh`, `server/run_ml_server.sh`, `server/run_event_writer.sh`, `server/run_live_collector.sh`, `server/run_research_api.sh`, `server/run_model_api.sh`, `server/run_gamma_bridge.sh`
- Operational scripts: `scripts/install_*launch_agent*.sh`, `scripts/uninstall_*launch_agent*.sh`, `scripts/verify_host_ready.sh`, `scripts/operational_preflight.py`, `scripts/release_readiness.sh`, `scripts/stress_ml_reload_score.py`, `scripts/send_daily_report.py`, `scripts/health_alert_watchdog.py`, `scripts/slo_monitor.py`
- Config/docs/tests: `.env.example`, `SYSTEM.md`, `docs/REMOTE_24X7_SETUP.md`, `docs/OPS_RESILIENCE.md`, `tests/python/`, `tests/ui/`
- Frontend only when security-relevant: `production_pivot_dashboard.html`, `app/ops/index.js`, `app/models/index.js`, `app/governance/index.js`, `app/shared/`
- Runtime state paths when referenced: `data/pivot_events.sqlite`, `data/model_lab/review_store.sqlite`, `logs/`, `data/models/*.json`

## Required Audit Procedure

1. Confirm repo context with `pwd`, `git status --short`, and targeted `rg --files`; do not clean or revert anything.
2. Enumerate service bind defaults and ports for dashboard/proxy, ML server, event writer, live collector, research API, model API, and IBKR bridge.
3. Identify all externally reachable endpoints and classify them as read-only, write, reload, score, review, operational, health/debug, or static.
4. Trace dashboard auth and local-only controls in `server/yahoo_proxy.js`, including route order, `WRITE_ENDPOINTS`, service-token behavior, login/logout, redirects, cookies, forwarded headers, and loopback checks.
5. Check direct sidecar exposure: determine whether Python services enforce auth themselves or rely only on bind address/proxy assumptions.
6. Check payload handling on POST/write endpoints for body limits, JSON parse errors, schema validation, type coercion, path handling, and uncaught exceptions.
7. Review launch scripts for binding defaults, inherited `.env` behavior, shell quoting, process cleanup, PID/lock handling, and accidental LAN exposure.
8. Review secrets/config handling: `.env.example`, logs, health responses, debug endpoints, dashboard-rendered config, report/webhook scripts, and service state files.
9. Review subprocess/shell usage for untrusted input, unsafe interpolation, and destructive commands.
10. Report only findings with a plausible runtime, network, auth, validation, process, or operational safety impact. Include exact file and line references.

## Output Format

Use this order:

1. **Findings**
   - `Severity - Title`
   - `File:line`
   - `Evidence`
   - `Runtime/security impact`
   - `Suggested fix`
2. **Open Questions**
3. **Audit Coverage**
4. **Not Reviewed**

If no issues are found, say so and list residual risk or test gaps.

## Severity Definitions

- **Critical**: Remote or LAN-accessible path can mutate state, reload models, write data, expose secrets, or execute commands without effective auth.
- **High**: Sensitive endpoint, service, secret, or process control can be abused under realistic local-network or misconfiguration conditions.
- **Medium**: Bad input, route ordering, launch defaults, or config exposure can crash services, bypass intended controls, or leak operational posture.
- **Low**: Defense-in-depth, observability, or validation gap with limited immediate impact.

## False-Positive Control Rules

- Do not flag a service as exposed only because it has a port; prove bind/default/launch path can make it reachable beyond intended clients.
- Do not treat CORS as authentication or network access control.
- Do not report theoretical shell injection without showing untrusted input reaches a shell or subprocess boundary.
- Do not report health/debug exposure unless it reveals sensitive state, auth posture, paths, tokens, model/ops status, or helps attack a write path.
- Distinguish loopback-only dev assumptions from documented/persistent LAN deployment scripts.
- Do not include model accuracy findings; hand those to `pivotquant-projection-accuracy-audit`.
