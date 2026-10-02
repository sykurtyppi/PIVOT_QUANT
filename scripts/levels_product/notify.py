#!/usr/bin/env python3
"""Tiny webhook notifier for the levels product (Slack + Discord compatible).

Posts to LEVELS_PRODUCT_WEBHOOK_URL. The payload shape is auto-detected from the
URL: Slack incoming webhooks (hooks.slack.com) get {"text": <mrkdwn>} with
**bold** rewritten to Slack *bold*; Discord (or any generic JSON-POST endpoint)
gets {"content": <markdown>}. If the env var is unset, it prints to stdout
instead — so every publisher works in dry-run with zero config. stdlib only.
"""
from __future__ import annotations

import json
import os
import smtplib
import urllib.error
import urllib.request
from email.message import EmailMessage
from pathlib import Path

WEBHOOK_ENV = "LEVELS_PRODUCT_WEBHOOK_URL"
# reuse the EXISTING daily-report SMTP contract so levels email "just works" with
# the operator's already-configured mail setup (same .env, same vars).
_REPO = Path(__file__).resolve().parents[2]


def _load_env_file(path=None):
    """Best-effort load KEY=VALUE lines from .env into os.environ (no override).

    Mirrors how send_daily_report.py sources its SMTP config so the levels email
    uses the same credentials without the operator configuring anything new.
    """
    p = Path(path or os.getenv("ML_REPORT_ENV_FILE") or (_REPO / ".env"))
    if not p.is_file():
        return
    for raw in p.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        if key.startswith("export "):
            key = key[len("export "):].strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def _env_bool(name, default):
    v = (os.getenv(name) or "").strip().lower()
    return default if v == "" else v in ("1", "true", "yes", "on")


def email_post(subject: str, body: str, *, dry_run: bool = False) -> bool:
    """Send the levels post by email using the daily-report SMTP env contract.

    Returns True if sent (or dry-run), False on missing config / failure. Never
    raises — a mail failure must not crash the daily job. The password is read
    from env only; nothing is logged.
    """
    _load_env_file()
    host = (os.getenv("ML_REPORT_SMTP_HOST") or "").strip()
    port = int((os.getenv("ML_REPORT_SMTP_PORT") or "587").strip() or "587")
    user = (os.getenv("ML_REPORT_SMTP_USER") or "").strip()
    password = (os.getenv("ML_REPORT_SMTP_PASS") or "").strip()
    sender = (os.getenv("ML_REPORT_EMAIL_FROM") or user).strip()
    recipients = [r.strip() for r in (os.getenv("ML_REPORT_EMAIL_TO") or "").split(",") if r.strip()]
    use_tls = _env_bool("ML_REPORT_SMTP_USE_TLS", True)
    if not (host and sender and recipients):
        print("[notify] email not configured (need ML_REPORT_SMTP_HOST/EMAIL_FROM/EMAIL_TO); skipped")
        return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)
    if dry_run:
        print(f"[notify] email DRY RUN to {', '.join(recipients)} — subject: {subject}")
        return True
    try:
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            if use_tls:
                smtp.starttls()
            if user:
                smtp.login(user, password)
            smtp.send_message(msg)
    except (smtplib.SMTPException, OSError) as exc:
        print(f"[notify] email delivery failed ({type(exc).__name__}); skipped")
        return False
    print(f"[notify] email sent to {', '.join(recipients)}")
    return True


def _slack_text(content: str) -> str:
    """Convert Discord-style **bold** to Slack mrkdwn *bold*."""
    import re
    return re.sub(r"\*\*(.+?)\*\*", r"*\1*", content)


def post(content: str, *, username: str = "PivotQuant Levels", timeout: float = 8.0) -> bool:
    """Best-effort deliver. Returns True only for confirmed HTTP success.

    NEVER raises: a webhook error must not crash the daily pipeline or the
    intraday poller. Delivery failures are logged and returned to the caller so
    stateful publishers can retain an event for retry rather than silently
    checkpointing an undelivered alert.
    """
    url = (os.getenv(WEBHOOK_ENV) or "").strip()
    if not url:
        print("─" * 60)
        print(f"[DRY RUN — set {WEBHOOK_ENV} to deliver (Slack or Discord webhook)]\n")
        print(content)
        print("─" * 60)
        return False
    if "hooks.slack.com" in url:
        # Slack incoming webhook: {"text": ...} with Slack mrkdwn (*bold*, not **bold**)
        body = json.dumps({"text": _slack_text(content)}).encode()
    else:
        # Discord (or generic) webhook: {"content": ...}
        body = json.dumps({"content": content, "username": username}).encode()
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (operator-set webhook)
            return 200 <= resp.status < 300
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        # do not echo the URL (it is a bearer credential)
        print(f"[notify] webhook delivery failed ({type(exc).__name__}); skipped")
        return False
