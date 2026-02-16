#!/usr/bin/env python3
"""Send a generated daily ML report to configured channels.

Supported channels:
  - email (SMTP)
  - iMessage (macOS Messages via osascript)
  - webhook (generic JSON POST)
"""

from __future__ import annotations

import argparse
from collections import Counter
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
NOISE_PATTERNS = [
    re.compile(r'GET /health HTTP/1\.1" 200 OK'),
    re.compile(r'^INFO:\s+127\.0\.0\.1:\d+\s+-\s+"GET /health'),
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
        "--email-style",
        choices=["compact", "full"],
        default=os.getenv("ML_REPORT_EMAIL_STYLE", "compact").strip().lower() or "compact",
        help="Email rendering style. compact is operator-friendly; full includes full inline report.",
    )
    parser.add_argument(
        "--anomaly-limit",
        type=int,
        default=int(os.getenv("ML_REPORT_ANOMALY_LIMIT", "6")),
        help="Max anomaly lines shown in compact email digest.",
    )
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


def clean_inline_markdown(value: str) -> str:
    return value.replace("**", "").replace("`", "").strip()


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


def build_log_tails(log_files: list[Path], lines: int) -> dict[str, str]:
    tails: dict[str, str] = {}
    for log_file in log_files:
        tails[str(log_file)] = tail_text(log_file, lines)
    return tails


def build_log_tail_section(log_tails: dict[str, str]) -> str:
    sections: list[str] = []
    for path, body in log_tails.items():
        sections.append(f"[{path}]\n{body}")
    return "\n\n".join(sections)


def extract_line_value(markdown: str, label: str) -> str | None:
    pattern = re.compile(rf"^- {re.escape(label)}:\s*(.+)$", re.MULTILINE)
    match = pattern.search(markdown)
    if not match:
        return None
    return clean_inline_markdown(match.group(1))


def parse_report_context(markdown: str, report_path: Path) -> dict[str, str]:
    report_date = "unknown"
    header = markdown.splitlines()[0].strip() if markdown else ""
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", header)
    if date_match:
        report_date = date_match.group(1)

    return {
        "report_date": report_date,
        "generated": extract_line_value(markdown, "Generated") or "unknown",
        "window": extract_line_value(markdown, "Window (ET)") or "unknown",
        "health": extract_line_value(markdown, "Health State") or "unknown",
        "model": extract_line_value(markdown, "Model") or "unknown",
        "staleness": extract_line_value(markdown, "Model Staleness") or "unknown",
        "scored": extract_line_value(markdown, "Scored predictions (latest per event)") or "0",
        "unique_events": extract_line_value(markdown, "Unique events scored") or "0",
        "labeled_rows": extract_line_value(markdown, "Labeled prediction rows (matured horizons)") or "0",
        "report_path": str(report_path),
    }


def build_short_summary(context: dict[str, str]) -> str:
    lines = [
        f"PivotQuant Daily ML Report: {context['report_date']}",
        f"Health: {context['health']}",
        f"Model: {context['model']}",
        f"Model staleness: {context['staleness']}",
        f"Scored predictions: {context['scored']}",
        f"Report file: {context['report_path']}",
    ]
    return "\n".join(lines)


def extract_section_bullets(markdown: str, header: str) -> list[str]:
    lines = markdown.splitlines()
    try:
        start = lines.index(header)
    except ValueError:
        return []

    bullets: list[str] = []
    for line in lines[start + 1 :]:
        if line.startswith("## "):
            break
        if line.startswith("- "):
            bullets.append(clean_inline_markdown(line[2:]))
    return bullets


def parse_horizon_snapshots(markdown: str) -> list[str]:
    lines = markdown.splitlines()
    snapshots: list[str] = []

    try:
        start = lines.index("## Horizon Metrics")
    except ValueError:
        return snapshots

    for line in lines[start + 1 :]:
        if line.startswith("## "):
            break
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 18:
            continue
        if cells[0] in {"Horizon", "---"}:
            continue

        horizon = cells[0]
        n = cells[1]
        if n in {"0", "--", ""}:
            continue
        auc_r, auc_b = cells[10], cells[11]
        brier_r, brier_b = cells[6], cells[7]
        ece_r, ece_b = cells[8], cells[9]
        snapshots.append(
            f"{horizon}: N={n}, AUC(R/B)={auc_r}/{auc_b}, "
            f"Brier(R/B)={brier_r}/{brier_b}, ECE(R/B)={ece_r}/{ece_b}"
        )
    return snapshots


def is_noise_line(line: str) -> bool:
    return any(pattern.search(line) for pattern in NOISE_PATTERNS)


def is_anomaly_line(line: str) -> bool:
    text = line.strip()
    if not text or is_noise_line(text):
        return False

    lowered = text.lower()
    keywords = (
        "warn",
        "warning",
        "error",
        "exception",
        "traceback",
        "failed",
        "timed out",
        "timeout",
        "kill-switch",
        "degrading",
        "stale",
    )
    return any(keyword in lowered for keyword in keywords)


def normalize_anomaly_line(line: str) -> str:
    text = line.strip()
    text = re.sub(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+\[(INFO|WARNING|ERROR)\]\s*", "", text)
    text = re.sub(r"^INFO:\s*", "", text)
    text = re.sub(r"^WARNING:\s*", "", text)
    text = re.sub(r"^ERROR:\s*", "", text)
    return text


def build_anomaly_digest(log_tails: dict[str, str], limit: int) -> list[str]:
    counts: Counter[str] = Counter()
    for path, body in log_tails.items():
        source = Path(path).name
        for line in body.splitlines():
            if not is_anomaly_line(line):
                continue
            normalized = normalize_anomaly_line(line)
            key = f"[{source}] {normalized}"
            counts[key] += 1

    if not counts:
        return ["No warnings/errors detected in selected log tails."]

    digest: list[str] = []
    for line, count in counts.most_common(max(1, limit)):
        if count > 1:
            digest.append(f"{line} (x{count})")
        else:
            digest.append(line)
    return digest


def build_compact_email_body(
    context: dict[str, str],
    horizon_snapshots: list[str],
    health_notes: list[str],
    anomaly_digest: list[str],
    report_path: Path,
    include_log_tails: bool,
) -> str:
    lines: list[str] = []
    lines.append(f"PivotQuant Daily ML Report ({context['report_date']})")
    lines.append("")
    lines.append("Executive Summary")
    lines.append(f"- Health: {context['health']}")
    lines.append(f"- Model: {context['model']}")
    lines.append(f"- Staleness: {context['staleness']}")
    lines.append(f"- Scored Predictions: {context['scored']}")
    lines.append(f"- Unique Events Scored: {context['unique_events']}")
    lines.append(f"- Labeled Rows (matured): {context['labeled_rows']}")
    lines.append(f"- Window (ET): {context['window']}")
    lines.append("")

    lines.append("Top Health Notes")
    if health_notes:
        for note in health_notes[:3]:
            lines.append(f"- {note}")
    else:
        lines.append("- None")
    lines.append("")

    lines.append("Horizon Snapshot")
    if horizon_snapshots:
        for snapshot in horizon_snapshots:
            lines.append(f"- {snapshot}")
    else:
        lines.append("- No matured horizon metrics yet (N=0).")
    lines.append("")

    lines.append("Anomaly Digest")
    for line in anomaly_digest:
        lines.append(f"- {line}")
    lines.append("")

    lines.append("Attachments")
    lines.append(f"- Full report: {report_path.name}")
    if include_log_tails:
        lines.append("- Runtime log digest: runtime_log_digest.txt")
    lines.append("")
    lines.append("Reference Path")
    lines.append(f"- {report_path}")
    return "\n".join(lines)


def build_full_email_body(summary: str, report_text: str, log_tail_text: str | None) -> str:
    parts = [summary, "--- Full report ---", report_text]
    if log_tail_text:
        parts.extend(["--- Runtime log tails ---", log_tail_text])
    return "\n\n".join(parts)


def send_email(
    recipients: list[str],
    subject: str,
    body_text: str,
    report_text: str,
    report_path: Path,
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
    message.set_content(body_text)
    message.add_attachment(report_text, subtype="markdown", filename=report_path.name)
    if log_tail_text:
        message.add_attachment(log_tail_text, subtype="plain", filename="runtime_log_digest.txt")

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
    context = parse_report_context(report_text, report_path)
    summary = build_short_summary(context)
    subject = (
        f"{args.subject_prefix} | [{context['health']}] {context['report_date']} "
        f"| Stale {context['staleness']} | Scored {context['scored']}"
    )
    imessage_summary = (
        f"PivotQuant {context['report_date']} | {context['health']} | "
        f"stale {context['staleness']} | scored {context['scored']}"
    )

    log_tails: dict[str, str] = {}
    log_tail_text: str | None = None
    if args.include_log_tails:
        log_tails = build_log_tails(resolve_log_files(args.log_file), args.log_tail_lines)
        log_tail_text = build_log_tail_section(log_tails)

    health_notes = extract_section_bullets(report_text, "## Health Notes")
    horizon_snapshots = parse_horizon_snapshots(report_text)
    anomaly_digest = build_anomaly_digest(log_tails, args.anomaly_limit)
    compact_body = build_compact_email_body(
        context=context,
        horizon_snapshots=horizon_snapshots,
        health_notes=health_notes,
        anomaly_digest=anomaly_digest,
        report_path=report_path,
        include_log_tails=bool(log_tail_text),
    )
    full_body = build_full_email_body(summary, report_text, log_tail_text)
    email_body = compact_body if args.email_style == "compact" else full_body

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
        ok, msg = send_email(
            email_to,
            subject,
            email_body,
            report_text,
            report_path,
            log_tail_text,
            args.dry_run,
        )
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
