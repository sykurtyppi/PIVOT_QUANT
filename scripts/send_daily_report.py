#!/usr/bin/env python3
"""Send a generated daily ML report to configured channels.

Supported channels:
  - email (SMTP)
  - iMessage (macOS Messages via osascript)
  - webhook (generic JSON POST)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import smtplib
import subprocess
import sys
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib import error, request

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG_FILES = [
    ROOT / "logs" / "retrain.log",
    ROOT / "logs" / "ml_server.log",
    ROOT / "logs" / "live_collector.log",
    ROOT / "logs" / "dashboard.log",
]


def parse_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send ML daily report notifications.")
    parser.add_argument("--report", required=True, help="Path to markdown report file.")
    parser.add_argument(
        "--channel",
        action="append",
        choices=["email", "imessage", "webhook"],
        help="Channel(s) to use. If omitted, uses ML_REPORT_NOTIFY_CHANNELS env.",
    )
    parser.add_argument(
        "--env-file",
        default=os.getenv("ML_REPORT_ENV_FILE", str(ROOT / ".env")),
        help="Optional .env file with notification vars (default: ./ .env).",
    )
    parser.add_argument("--subject-prefix", default="PivotQuant Daily Report")
    parser.add_argument(
        "--log-tail-lines",
        type=int,
        default=int(os.getenv("ML_REPORT_LOG_TAIL_LINES", "80")),
        help="How many lines to include from each log in email body.",
    )
    parser.add_argument(
        "--include-log-tails",
        action="store_true",
        default=env_bool("ML_REPORT_INCLUDE_LOG_TAILS", True),
        help="Include tails from key runtime logs in email body.",
    )
    parser.add_argument(
        "--log-file",
        action="append",
        default=[],
        help="Optional log file path(s). If omitted, uses ML_REPORT_LOG_FILES or defaults.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print actions only.")
    return parser.parse_args()


def load_env_file(path: str) -> None:
    env_path = Path(path).expanduser()
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if not key:
            continue
        os.environ.setdefault(key, value)


def read_report(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def resolve_log_files(cli_logs: list[str]) -> list[Path]:
    if cli_logs:
        return [Path(item).expanduser().resolve() for item in cli_logs]
    raw = os.getenv("ML_REPORT_LOG_FILES", "").strip()
    if raw:
        return [Path(item).expanduser().resolve() for item in parse_csv(raw)]
    return DEFAULT_LOG_FILES


def tail_text(path: Path, lines: int) -> str:
    if not path.exists():
        return f"[missing] {path}"
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if lines <= 0:
        return "\n".join(content)
    return "\n".join(content[-lines:])


def build_log_tail_section(log_files: list[Path], lines: int) -> str:
    sections: list[str] = []
    for log_file in log_files:
        body = tail_text(log_file, lines)
        sections.append(f"[{log_file}]\n{body}")
    return "\n\n".join(sections)


def extract_line_value(markdown: str, label: str) -> str | None:
    pattern = re.compile(rf"^- {re.escape(label)}:\s*(.+)$", re.MULTILINE)
    match = pattern.search(markdown)
    if not match:
        return None
    value = match.group(1).strip()
    value = value.replace("**", "").strip("`")
    return value


def build_summary(markdown: str, report_path: Path) -> tuple[str, str]:
    report_date = "unknown"
    header = markdown.splitlines()[0].strip() if markdown else ""
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", header)
    if date_match:
        report_date = date_match.group(1)

    health = extract_line_value(markdown, "Health State") or "unknown"
    model = extract_line_value(markdown, "Model") or "unknown"
    sample = extract_line_value(markdown, "Scored predictions (latest per event)") or "0"
    stale = extract_line_value(markdown, "Model Staleness") or "unknown"

    subject = f"{report_date} | {health} | {model}"
    summary_lines = [
        f"PivotQuant Daily ML Report: {report_date}",
        f"Health: {health}",
        f"Model: {model}",
        f"Model staleness: {stale}",
        f"Scored predictions: {sample}",
        f"Report file: {report_path}",
    ]
    return subject, "\n".join(summary_lines)


def send_email(
    recipients: list[str],
    subject: str,
    body_summary: str,
    report_text: str,
    log_tail_text: str | None,
    dry_run: bool,
) -> tuple[bool, str]:
    host = os.getenv("ML_REPORT_SMTP_HOST", "").strip()
    port = int(os.getenv("ML_REPORT_SMTP_PORT", "587").strip() or "587")
    username = os.getenv("ML_REPORT_SMTP_USER", "").strip()
    password = os.getenv("ML_REPORT_SMTP_PASS", "").strip()
    sender = os.getenv("ML_REPORT_EMAIL_FROM", username).strip()
    use_tls = env_bool("ML_REPORT_SMTP_USE_TLS", True)

    if not recipients:
        return False, "email: ML_REPORT_EMAIL_TO is empty"
    if not host:
        return False, "email: ML_REPORT_SMTP_HOST not set"
    if not sender:
        return False, "email: ML_REPORT_EMAIL_FROM or ML_REPORT_SMTP_USER must be set"

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    parts = [body_summary, "--- Full report ---", report_text]
    if log_tail_text:
        parts.extend(["--- Runtime log tails ---", log_tail_text])
    message.set_content("\n\n".join(parts))

    if dry_run:
        return True, f"email: dry-run to {', '.join(recipients)}"

    with smtplib.SMTP(host, port, timeout=30) as smtp:
        if use_tls:
            smtp.starttls()
        if username:
            smtp.login(username, password)
        smtp.send_message(message)
    return True, f"email: sent to {', '.join(recipients)}"


def escape_applescript(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def send_imessage(
    recipients: list[str],
    message_text: str,
    dry_run: bool,
) -> tuple[bool, str]:
    if not recipients:
        return False, "imessage: ML_REPORT_IMESSAGE_TO is empty"

    if dry_run:
        return True, f"imessage: dry-run to {', '.join(recipients)}"

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
            stderr = (result.stderr or "").strip()
            failures.append(f"{recipient}: {stderr or 'osascript failed'}")

    if failures:
        return False, "imessage: " + "; ".join(failures)
    return True, f"imessage: sent to {', '.join(recipients)}"


def send_webhook(
    url: str,
    subject: str,
    body_summary: str,
    report_path: Path,
    dry_run: bool,
) -> tuple[bool, str]:
    if not url:
        return False, "webhook: ML_REPORT_WEBHOOK_URL not set"

    payload: dict[str, Any] = {
        "subject": subject,
        "summary": body_summary,
        "report_path": str(report_path),
    }
    encoded = json.dumps(payload).encode("utf-8")

    if dry_run:
        return True, f"webhook: dry-run to {url}"

    req = request.Request(
        url=url,
        data=encoded,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=30) as resp:  # noqa: S310 (env-controlled URL)
            if resp.status < 200 or resp.status >= 300:
                return False, f"webhook: HTTP {resp.status}"
    except error.URLError as exc:
        return False, f"webhook: {exc}"
    return True, "webhook: delivered"


def main() -> int:
    args = parse_args()
    load_env_file(args.env_file)

    report_path = Path(args.report).expanduser().resolve()
    if not report_path.exists():
        print(f"[notify] report not found: {report_path}", file=sys.stderr)
        return 2

    report_text = read_report(report_path)
    suffix, summary = build_summary(report_text, report_path)
    subject = f"{args.subject_prefix} | {suffix}"
    imessage_summary = summary.replace("\n", " | ")
    log_tail_text: str | None = None
    if args.include_log_tails:
        log_tail_text = build_log_tail_section(resolve_log_files(args.log_file), args.log_tail_lines)

    channels = args.channel or parse_csv(os.getenv("ML_REPORT_NOTIFY_CHANNELS"))
    channels = [c.strip().lower() for c in channels if c.strip()]
    if not channels:
        print("[notify] skipped (no channels configured)")
        return 0

    successes = 0
    attempts = 0

    if "email" in channels:
        attempts += 1
        email_to = parse_csv(os.getenv("ML_REPORT_EMAIL_TO"))
        ok, msg = send_email(email_to, subject, summary, report_text, log_tail_text, args.dry_run)
        print(f"[notify] {msg}")
        successes += 1 if ok else 0

    if "imessage" in channels:
        attempts += 1
        imessage_to = parse_csv(os.getenv("ML_REPORT_IMESSAGE_TO"))
        ok, msg = send_imessage(imessage_to, imessage_summary, args.dry_run)
        print(f"[notify] {msg}")
        successes += 1 if ok else 0

    if "webhook" in channels:
        attempts += 1
        webhook_url = os.getenv("ML_REPORT_WEBHOOK_URL", "").strip()
        ok, msg = send_webhook(webhook_url, subject, summary, report_path, args.dry_run)
        print(f"[notify] {msg}")
        successes += 1 if ok else 0

    if attempts > 0 and successes == 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
