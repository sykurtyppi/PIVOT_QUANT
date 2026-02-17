#!/usr/bin/env python3
"""Stateful service watchdog with immediate email alerts.

Checks ML and live collector health endpoints and notifies only on:
  - DOWN transition
  - UP recovery transition

Optional repeated DOWN reminders can be enabled via ML_ALERT_REPEAT_MIN.
"""

from __future__ import annotations

import argparse
import json
import os
import smtplib
import socket
import time
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib import error, request


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_STATE_FILE = ROOT / "logs" / "health_alert_state.json"
DEFAULT_LOG_FILE = ROOT / "logs" / "health_alert.log"


def parse_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            os.environ.setdefault(key, value)


def log_line(log_path: Path, message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {message}"
    print(line)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"services": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"services": {}}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def check_service(name: str, url: str, timeout_sec: float) -> dict[str, Any]:
    started = time.time()
    req = request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "PivotQuantHealthWatchdog/1.0",
        },
    )

    try:
        with request.urlopen(req, timeout=timeout_sec) as response:
            body_raw = response.read().decode("utf-8", errors="replace")
            http_status = int(getattr(response, "status", 200))
    except error.HTTPError as exc:
        latency_ms = int((time.time() - started) * 1000)
        return {
            "service": name,
            "up": False,
            "status": "http_error",
            "reason": f"HTTP {exc.code}",
            "http_status": int(exc.code),
            "latency_ms": latency_ms,
            "url": url,
        }
    except (error.URLError, TimeoutError, OSError) as exc:
        latency_ms = int((time.time() - started) * 1000)
        reason = getattr(exc, "reason", None)
        reason_text = str(reason) if reason is not None else str(exc)
        return {
            "service": name,
            "up": False,
            "status": "unreachable",
            "reason": reason_text or "Connection failed",
            "http_status": 0,
            "latency_ms": latency_ms,
            "url": url,
        }

    latency_ms = int((time.time() - started) * 1000)
    if http_status < 200 or http_status >= 300:
        return {
            "service": name,
            "up": False,
            "status": "http_error",
            "reason": f"HTTP {http_status}",
            "http_status": http_status,
            "latency_ms": latency_ms,
            "url": url,
        }

    try:
        payload = json.loads(body_raw or "{}")
    except json.JSONDecodeError:
        return {
            "service": name,
            "up": False,
            "status": "invalid_json",
            "reason": "Health endpoint returned non-JSON body",
            "http_status": http_status,
            "latency_ms": latency_ms,
            "url": url,
        }

    status_value = str(payload.get("status", "")).strip().lower()
    if name == "ml":
        up_statuses = {"ok", "stale", "degraded", "healthy", "checking"}
    else:
        up_statuses = {"ok", "degraded", "starting"}

    up = status_value in up_statuses
    reason = (
        f"status={status_value or '--'}"
        if up
        else f"Unexpected health status: {status_value or '--'}"
    )
    return {
        "service": name,
        "up": up,
        "status": status_value or "unknown",
        "reason": reason,
        "http_status": http_status,
        "latency_ms": latency_ms,
        "url": url,
    }


def resolve_channels() -> list[str]:
    raw = os.getenv("ML_ALERT_NOTIFY_CHANNELS", "").strip()
    if not raw:
        raw = os.getenv("ML_REPORT_NOTIFY_CHANNELS", "email").strip()
    return [c.lower() for c in parse_csv(raw)]


def send_email(subject: str, text_body: str, dry_run: bool) -> tuple[bool, str]:
    recipients_raw = os.getenv("ML_ALERT_EMAIL_TO", "").strip() or os.getenv("ML_REPORT_EMAIL_TO", "").strip()
    recipients = parse_csv(recipients_raw)
    sender = os.getenv("ML_ALERT_EMAIL_FROM", "").strip() or os.getenv("ML_REPORT_EMAIL_FROM", "").strip()
    host = os.getenv("ML_REPORT_SMTP_HOST", "").strip()
    port = int((os.getenv("ML_REPORT_SMTP_PORT", "587").strip() or "587"))
    username = os.getenv("ML_REPORT_SMTP_USER", "").strip()
    password = os.getenv("ML_REPORT_SMTP_PASS", "").strip()
    use_tls = os.getenv("ML_REPORT_SMTP_USE_TLS", "true").strip().lower() in {"1", "true", "yes", "on"}

    if not recipients:
        return False, "email: no recipients configured (ML_ALERT_EMAIL_TO / ML_REPORT_EMAIL_TO)"
    if not host:
        return False, "email: ML_REPORT_SMTP_HOST not set"
    if not sender:
        sender = username
    if not sender:
        return False, "email: sender not configured (ML_ALERT_EMAIL_FROM / ML_REPORT_EMAIL_FROM / ML_REPORT_SMTP_USER)"

    if dry_run:
        return True, f"email dry-run to {', '.join(recipients)}"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(text_body)

    try:
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            if use_tls:
                smtp.starttls()
            if username and password:
                smtp.login(username, password)
            smtp.send_message(msg)
        return True, f"email sent to {', '.join(recipients)}"
    except smtplib.SMTPDataError as exc:
        raw = exc.smtp_error.decode("utf-8", errors="replace") if isinstance(exc.smtp_error, bytes) else str(exc.smtp_error)
        return False, f"email SMTPDataError {exc.smtp_code} ({raw})"
    except (smtplib.SMTPException, OSError) as exc:
        return False, f"email send failed ({exc})"


def notify(subject: str, body: str, dry_run: bool) -> tuple[bool, str]:
    channels = resolve_channels()
    if "email" not in channels:
        return False, "notifications skipped (email channel not enabled)"
    return send_email(subject, body, dry_run=dry_run)


def should_repeat_down_alert(previous: dict[str, Any], repeat_min: int, now_ts: int) -> bool:
    if repeat_min <= 0:
        return False
    last_alert_ts = int(previous.get("last_alert_ts") or 0)
    if last_alert_ts <= 0:
        return True
    return (now_ts - last_alert_ts) >= repeat_min * 60


def build_subject(prefix: str, service_name: str, state: str) -> str:
    return f"{prefix} {service_name.upper()} {state}"


def build_body(
    host: str,
    checked_at: str,
    result: dict[str, Any],
    prior: dict[str, Any],
    state_label: str,
) -> str:
    lines = [
        "PivotQuant Immediate Service Alert",
        "",
        f"State: {state_label}",
        f"Service: {result['service']}",
        f"Host: {host}",
        f"Checked: {checked_at}",
        f"Endpoint: {result['url']}",
        f"HTTP: {result['http_status']}",
        f"Status: {result['status']}",
        f"Reason: {result['reason']}",
        f"Latency: {result['latency_ms']} ms",
    ]
    if prior:
        previous_state = prior.get("state")
        previous_reason = prior.get("last_reason")
        if previous_state:
            lines.append(f"Previous state: {previous_state}")
        if previous_reason:
            lines.append(f"Previous reason: {previous_reason}")
    lines.append("")
    lines.append("This alert was generated by scripts/health_alert_watchdog.py")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PivotQuant immediate health alerts")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
    parser.add_argument("--log-file", default=str(DEFAULT_LOG_FILE))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=float(os.getenv("ML_ALERT_TIMEOUT_SEC", "4")),
    )
    parser.add_argument(
        "--repeat-min",
        type=int,
        default=int(os.getenv("ML_ALERT_REPEAT_MIN", "0")),
        help="If >0, send reminder while still DOWN every N minutes.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env_file = Path(args.env_file).expanduser()
    load_env_file(env_file)

    log_file = Path(args.log_file).expanduser()
    state_file = Path(args.state_file).expanduser()
    host = socket.gethostname()
    checked_at = now_iso_utc()
    now_ts = int(time.time())

    services = {
        "ml": os.getenv("ML_ALERT_ML_HEALTH_URL", "http://127.0.0.1:5003/health").strip(),
        "collector": os.getenv("ML_ALERT_COLLECTOR_HEALTH_URL", "http://127.0.0.1:5004/health").strip(),
    }
    subject_prefix = os.getenv("ML_ALERT_SUBJECT_PREFIX", "[ALERT]").strip() or "[ALERT]"
    recovery_prefix = os.getenv("ML_ALERT_RECOVERY_PREFIX", "[RECOVERED]").strip() or "[RECOVERED]"

    state = load_state(state_file)
    service_state = state.setdefault("services", {})

    summaries: list[str] = []
    changed = False

    for name, url in services.items():
        result = check_service(name=name, url=url, timeout_sec=args.timeout_sec)
        summaries.append(f"{name}={result['status']}{'' if result['up'] else '(!)'}")

        previous = service_state.get(name, {})
        previous_state = str(previous.get("state") or "unknown")
        current_state = "up" if result["up"] else "down"

        send = False
        subject = ""
        body = ""
        alert_kind = ""

        if current_state == "down":
            if previous_state != "down":
                send = True
                alert_kind = "down_transition"
                subject = build_subject(subject_prefix, name, "DOWN")
                body = build_body(host, checked_at, result, previous, "DOWN")
            elif should_repeat_down_alert(previous, args.repeat_min, now_ts):
                send = True
                alert_kind = "down_reminder"
                subject = build_subject(subject_prefix, name, "STILL DOWN")
                body = build_body(host, checked_at, result, previous, "STILL DOWN")
        elif previous_state == "down":
            send = True
            alert_kind = "recovery"
            subject = build_subject(recovery_prefix, name, "UP")
            body = build_body(host, checked_at, result, previous, "RECOVERED")

        if send:
            ok, msg = notify(subject=subject, body=body, dry_run=args.dry_run)
            changed = True
            if ok:
                log_line(log_file, f"{name}: {alert_kind} notify ok ({msg})")
            else:
                log_line(log_file, f"{name}: {alert_kind} notify failed ({msg})")
            if ok:
                previous["last_alert_ts"] = now_ts

        if previous_state != current_state:
            changed = True
            previous["last_change_ts"] = now_ts

        previous["state"] = current_state
        previous["last_status"] = result["status"]
        previous["last_reason"] = result["reason"]
        previous["last_check_ts"] = now_ts
        previous["last_url"] = result["url"]
        previous["last_http_status"] = result["http_status"]
        previous["last_latency_ms"] = result["latency_ms"]
        if current_state == "down":
            previous["last_down_ts"] = now_ts
        elif previous_state == "down":
            previous["last_recovered_ts"] = now_ts

        service_state[name] = previous

    state["checked_at"] = checked_at
    state["host"] = host
    save_state(state_file, state)

    if changed or args.dry_run:
        log_line(log_file, f"health check: {'; '.join(summaries)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

