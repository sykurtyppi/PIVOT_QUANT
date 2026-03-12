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
import sqlite3
import smtplib
import socket
import subprocess
import time
from datetime import datetime, time as dt_time, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib import error, request

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_STATE_FILE = ROOT / "logs" / "health_alert_state.json"
DEFAULT_LOG_FILE = ROOT / "logs" / "health_alert.log"
ET_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc


def parse_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def to_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def to_float(value: object, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def is_market_hours_et(now_utc: datetime) -> bool:
    now_et = now_utc.astimezone(ET_TZ)
    if now_et.weekday() >= 5:
        return False
    open_time = dt_time(hour=9, minute=30)
    close_time = dt_time(hour=16, minute=0)
    t = now_et.time()
    return t >= open_time and t < close_time


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


def count_unscored_live_events(db_path: str, lookback_minutes: int) -> tuple[int | None, str | None]:
    lookback_ms = max(1, int(lookback_minutes)) * 60 * 1000
    min_ts_ms = int(time.time() * 1000) - lookback_ms
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM touch_events te
                WHERE te.ts_event >= ?
                  AND NOT EXISTS (
                    SELECT 1
                    FROM prediction_log pl
                    WHERE pl.event_id = te.event_id
                      AND COALESCE(pl.is_preview, 0) = 0
                  )
                """,
                (min_ts_ms,),
            ).fetchone()
            return int(row[0] or 0), None
        finally:
            conn.close()
    except sqlite3.Error as exc:
        return None, str(exc)


def check_service(
    name: str,
    url: str,
    timeout_sec: float,
    *,
    ml_score_latency_max_ms: float,
    ml_score_min_success_count: int,
) -> dict[str, Any]:
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
    score_last_duration_ms = None
    score_success_count = None
    score_latency_breached = False
    score_latency_reason = ""
    if name == "ml" and ml_score_latency_max_ms > 0:
        score_payload = payload.get("score")
        if isinstance(score_payload, dict):
            score_last_duration_ms = to_float(score_payload.get("last_duration_ms"))
            score_success_count = to_int(score_payload.get("success_count"), 0)
            if (
                score_last_duration_ms is not None
                and score_success_count >= max(0, ml_score_min_success_count)
                and score_last_duration_ms > ml_score_latency_max_ms
            ):
                score_latency_breached = True
                score_latency_reason = (
                    f"score.last_duration_ms={score_last_duration_ms:.3f} "
                    f"exceeds threshold={ml_score_latency_max_ms:.3f} "
                    f"(success_count={score_success_count})"
                )

    return {
        "service": name,
        "up": up,
        "status": status_value or "unknown",
        "reason": reason,
        "http_status": http_status,
        "latency_ms": latency_ms,
        "url": url,
        "score_last_duration_ms": score_last_duration_ms,
        "score_success_count": score_success_count,
        "score_latency_breached": score_latency_breached,
        "score_latency_reason": score_latency_reason,
        "score_latency_threshold_ms": ml_score_latency_max_ms if ml_score_latency_max_ms > 0 else None,
    }


def resolve_channels() -> list[str]:
    raw = os.getenv("ML_ALERT_NOTIFY_CHANNELS", "").strip()
    if not raw:
        raw = os.getenv("ML_REPORT_NOTIFY_CHANNELS", "email").strip()
    return [c.lower() for c in parse_csv(raw)]


def email_failover_trigger(message: str) -> bool:
    lowered = message.lower()
    return (
        "535" in lowered
        or "smtpauthenticationerror" in lowered
        or "5.7.8" in lowered
        or "smtpdataerror 550" in lowered
        or "5.4.5" in lowered
        or "sending limit" in lowered
    )


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


def escape_applescript(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def send_imessage(message_text: str, dry_run: bool) -> tuple[bool, str]:
    recipients = parse_csv(
        os.getenv("ML_ALERT_IMESSAGE_TO", "").strip() or os.getenv("ML_REPORT_IMESSAGE_TO", "").strip()
    )
    if not recipients:
        return False, "imessage: recipients not configured"
    if dry_run:
        return True, f"imessage dry-run to {', '.join(recipients)}"

    failures: list[str] = []
    for recipient in recipients:
        script = [
            'tell application "Messages"',
            "set targetService to 1st service whose service type = iMessage",
            f'set targetParticipant to participant "{escape_applescript(recipient)}" of targetService',
            f'send "{escape_applescript(message_text)}" to targetParticipant',
            "end tell",
        ]
        result = subprocess.run(
            ["osascript", *sum([["-e", line] for line in script], [])],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            failures.append(f"{recipient}: {(result.stderr or '').strip() or 'osascript failed'}")

    if failures:
        return False, "imessage failed: " + "; ".join(failures)
    return True, f"imessage sent to {', '.join(recipients)}"


def send_webhook(subject: str, body: str, dry_run: bool) -> tuple[bool, str]:
    url = os.getenv("ML_ALERT_WEBHOOK_URL", "").strip() or os.getenv("ML_REPORT_WEBHOOK_URL", "").strip()
    if not url:
        return False, "webhook: URL not configured"
    payload = json.dumps({"subject": subject, "body": body}).encode("utf-8")
    if dry_run:
        return True, f"webhook dry-run to {url}"
    req = request.Request(
        url=url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=30) as resp:  # noqa: S310
            if resp.status < 200 or resp.status >= 300:
                return False, f"webhook HTTP {resp.status}"
    except error.URLError as exc:
        return False, f"webhook failed ({exc})"
    return True, "webhook delivered"


def notify(subject: str, body: str, dry_run: bool) -> tuple[bool, str]:
    channels = resolve_channels()
    if not channels:
        return False, "notifications skipped (no channels)"

    attempted: list[str] = []
    errors: list[str] = []
    email_fail_msg = ""

    for channel in channels:
        if channel == "email":
            ok, msg = send_email(subject, body, dry_run=dry_run)
            attempted.append("email")
            if ok:
                return True, msg
            errors.append(msg)
            email_fail_msg = msg
        elif channel == "webhook":
            ok, msg = send_webhook(subject, body, dry_run=dry_run)
            attempted.append("webhook")
            if ok:
                return True, msg
            errors.append(msg)
        elif channel == "imessage":
            message_text = f"{subject}\n\n{body}"
            ok, msg = send_imessage(message_text, dry_run=dry_run)
            attempted.append("imessage")
            if ok:
                return True, msg
            errors.append(msg)

    # Automatic failover path for common Gmail failures.
    if email_fail_msg and email_failover_trigger(email_fail_msg):
        fallback_channels = [
            c.strip().lower()
            for c in parse_csv(os.getenv("ML_ALERT_FAILOVER_CHANNELS", "webhook,imessage"))
            if c.strip()
        ]
        for channel in fallback_channels:
            if channel in attempted:
                continue
            if channel == "webhook":
                ok, msg = send_webhook(subject, body, dry_run=dry_run)
                if ok:
                    return True, f"failover {msg}"
                errors.append(msg)
            elif channel == "imessage":
                message_text = f"{subject}\n\n{body}"
                ok, msg = send_imessage(message_text, dry_run=dry_run)
                if ok:
                    return True, f"failover {msg}"
                errors.append(msg)

    return False, "; ".join(errors) if errors else "notify failed"


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
    if result.get("score_last_duration_ms") is not None:
        lines.append(f"Score last duration: {result['score_last_duration_ms']:.3f} ms")
    if result.get("score_latency_threshold_ms") is not None:
        lines.append(f"Score duration threshold: {result['score_latency_threshold_ms']:.3f} ms")
    if result.get("score_success_count") is not None:
        lines.append(f"Score success count: {result['score_success_count']}")
    if result.get("collector_unscored_count") is not None:
        lines.append(
            "Collector unscored (lookback): "
            f"{result['collector_unscored_count']} "
            f"(threshold={result.get('collector_unscored_threshold')}, "
            f"lookback_min={result.get('collector_unscored_lookback_min')}, "
            f"market_hours_only={result.get('collector_unscored_market_hours_only')})"
        )
    if result.get("collector_unscored_streak") is not None:
        lines.append(
            f"Collector unscored streak: {result['collector_unscored_streak']}/"
            f"{result.get('collector_unscored_streak_threshold')}"
        )
    if result.get("collector_unscored_query_error"):
        lines.append(f"Collector unscored query error: {result['collector_unscored_query_error']}")
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
    parser.add_argument(
        "--notify-subject",
        default="",
        help="Optional one-shot custom alert subject. If provided with --notify-body, health checks are skipped.",
    )
    parser.add_argument(
        "--notify-body",
        default="",
        help="Optional one-shot custom alert body for --notify-subject mode.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env_file = Path(args.env_file).expanduser()
    load_env_file(env_file)

    log_file = Path(args.log_file).expanduser()
    state_file = Path(args.state_file).expanduser()
    host = socket.gethostname()
    now_dt_utc = datetime.now(timezone.utc)
    checked_at = now_dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    now_ts = int(time.time())

    if args.notify_subject.strip() and args.notify_body.strip():
        ok, msg = notify(subject=args.notify_subject.strip(), body=args.notify_body.strip(), dry_run=args.dry_run)
        if ok:
            log_line(log_file, f"custom_alert notify ok ({msg})")
            return 0
        log_line(log_file, f"custom_alert notify failed ({msg})")
        return 1

    services = {
        "ml": os.getenv("ML_ALERT_ML_HEALTH_URL", "http://127.0.0.1:5003/health").strip(),
        "collector": os.getenv("ML_ALERT_COLLECTOR_HEALTH_URL", "http://127.0.0.1:5004/health").strip(),
    }
    subject_prefix = os.getenv("ML_ALERT_SUBJECT_PREFIX", "[ALERT]").strip() or "[ALERT]"
    recovery_prefix = os.getenv("ML_ALERT_RECOVERY_PREFIX", "[RECOVERED]").strip() or "[RECOVERED]"
    ml_score_latency_max_ms = max(
        0.0, float(os.getenv("ML_ALERT_ML_SCORE_LAST_DURATION_MAX_MS", "0") or "0")
    )
    ml_score_min_success_count = max(
        0, int(os.getenv("ML_ALERT_ML_SCORE_MIN_SUCCESS_COUNT", "5") or "5")
    )
    ml_score_consecutive_fails = max(
        1, int(os.getenv("ML_ALERT_ML_SCORE_CONSECUTIVE_FAILS", "3") or "3")
    )
    service_consecutive_fails = max(
        1, int(os.getenv("ML_ALERT_CONSECUTIVE_FAILS", "2") or "2")
    )
    collector_unscored_guard_enabled = to_bool(
        os.getenv("ML_ALERT_COLLECTOR_UNSCORED_GUARD", "true"),
        default=True,
    )
    collector_unscored_max = max(
        0, int(os.getenv("ML_ALERT_COLLECTOR_UNSCORED_MAX", "0") or "0")
    )
    collector_unscored_lookback_min = max(
        1, int(os.getenv("ML_ALERT_COLLECTOR_UNSCORED_LOOKBACK_MIN", "120") or "120")
    )
    collector_unscored_consecutive_fails = max(
        1, int(os.getenv("ML_ALERT_COLLECTOR_UNSCORED_CONSECUTIVE_FAILS", "3") or "3")
    )
    collector_unscored_market_hours_only = to_bool(
        os.getenv("ML_ALERT_COLLECTOR_UNSCORED_MARKET_HOURS_ONLY", "true"),
        default=True,
    )
    pivot_db_path = os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite")).strip() or str(
        ROOT / "data" / "pivot_events.sqlite"
    )

    state = load_state(state_file)
    service_state = state.setdefault("services", {})

    summaries: list[str] = []
    changed = False

    for name, url in services.items():
        result = check_service(
            name=name,
            url=url,
            timeout_sec=args.timeout_sec,
            ml_score_latency_max_ms=ml_score_latency_max_ms,
            ml_score_min_success_count=ml_score_min_success_count,
        )
        previous = service_state.get(name, {})
        if not isinstance(previous, dict):
            previous = {}

        if name == "ml":
            streak = to_int(previous.get("ml_score_latency_streak"), 0)
            if bool(result.get("score_latency_breached")):
                streak += 1
                previous["ml_score_latency_streak"] = streak
                if streak >= ml_score_consecutive_fails:
                    result["up"] = False
                    result["status"] = "latency_regressed"
                    base_reason = str(result.get("score_latency_reason") or "score latency threshold exceeded")
                    result["reason"] = (
                        f"{base_reason}; streak={streak}/{ml_score_consecutive_fails}"
                    )
            else:
                previous["ml_score_latency_streak"] = 0
        elif name == "collector":
            previous_streak = to_int(previous.get("collector_unscored_streak"), 0)
            guard_window_open = (not collector_unscored_market_hours_only) or is_market_hours_et(now_dt_utc)
            result["collector_unscored_market_hours_only"] = collector_unscored_market_hours_only
            result["collector_unscored_lookback_min"] = collector_unscored_lookback_min
            result["collector_unscored_threshold"] = collector_unscored_max
            result["collector_unscored_streak_threshold"] = collector_unscored_consecutive_fails
            result["collector_unscored_streak"] = previous_streak

            if collector_unscored_guard_enabled and guard_window_open and bool(result.get("up")):
                unscored_count, unscored_error = count_unscored_live_events(
                    db_path=pivot_db_path,
                    lookback_minutes=collector_unscored_lookback_min,
                )
                result["collector_unscored_count"] = unscored_count
                result["collector_unscored_query_error"] = unscored_error
                if unscored_error:
                    previous["collector_unscored_streak"] = 0
                    result["collector_unscored_streak"] = 0
                elif unscored_count is not None and unscored_count > collector_unscored_max:
                    next_streak = previous_streak + 1
                    previous["collector_unscored_streak"] = next_streak
                    result["collector_unscored_streak"] = next_streak
                    if next_streak >= collector_unscored_consecutive_fails:
                        result["up"] = False
                        result["status"] = "scoring_lagging"
                        result["reason"] = (
                            f"unscored_count={unscored_count} exceeds threshold={collector_unscored_max} "
                            f"(lookback={collector_unscored_lookback_min}m); "
                            f"streak={next_streak}/{collector_unscored_consecutive_fails}"
                        )
                        result["skip_pending_downgrade"] = True
                else:
                    previous["collector_unscored_streak"] = 0
                    result["collector_unscored_streak"] = 0
            else:
                previous["collector_unscored_streak"] = 0
                result["collector_unscored_streak"] = 0

        raw_is_down = not bool(result["up"])
        down_streak = to_int(previous.get("down_streak"), 0)
        if raw_is_down:
            down_streak += 1
        else:
            down_streak = 0
        previous["down_streak"] = down_streak
        if raw_is_down and down_streak < service_consecutive_fails and not bool(result.get("skip_pending_downgrade")):
            result["up"] = True
            base_status = str(result.get("status") or "unknown")
            base_reason = str(result.get("reason") or "health check failed")
            result["status"] = f"{base_status}_pending"
            result["reason"] = (
                f"{base_reason}; down_streak={down_streak}/{service_consecutive_fails}"
            )

        summaries.append(f"{name}={result['status']}{'' if result['up'] else '(!)'}")

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
