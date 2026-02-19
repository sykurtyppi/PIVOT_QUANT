#!/usr/bin/env python3
"""Scheduled pre-open and post-open checks with one alert per phase/day."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import error, request
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_STATE_FILE = ROOT / "logs" / "session_routine_state.json"
DEFAULT_LOG_FILE = ROOT / "logs" / "session_routine.log"
DEFAULT_DB = ROOT / "data" / "pivot_events.sqlite"
DEFAULT_OPS_STATUS_URL = "http://127.0.0.1:3000/api/ops/status"
DEFAULT_GAMMA_URL = "http://127.0.0.1:5001/gamma?symbol=SPY&expiry=front&limit=10"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PivotQuant session routine checks.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
    parser.add_argument("--log-file", default=str(DEFAULT_LOG_FILE))
    parser.add_argument("--db-path", default=str(DEFAULT_DB))
    parser.add_argument(
        "--force-phase",
        choices=["preopen", "postopen"],
        default="",
        help="Run a specific phase immediately (ignores ET window).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore per-day phase dedupe and force execution.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--no-notify",
        action="store_true",
        help="Compute and log checks but skip notification dispatch.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=float(os.getenv("ML_ALERT_TIMEOUT_SEC", "4")),
    )
    return parser.parse_args()


def parse_bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


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


def log_line(path: Path, message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {message}"
    print(line)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"daily": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"daily": {}}
    if not isinstance(payload, dict):
        return {"daily": {}}
    payload.setdefault("daily", {})
    return payload


def save_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fetch_json(url: str, timeout_sec: float) -> tuple[int, dict[str, Any] | None, str]:
    req = request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "PivotQuantSessionRoutine/1.0",
        },
    )
    try:
        with request.urlopen(req, timeout=timeout_sec) as resp:
            status = int(getattr(resp, "status", 200))
            body = resp.read().decode("utf-8", errors="replace")
        if status < 200 or status >= 300:
            return status, None, f"HTTP {status}"
        try:
            return status, json.loads(body or "{}"), ""
        except json.JSONDecodeError:
            return status, None, "invalid JSON"
    except error.HTTPError as exc:
        return int(exc.code), None, f"HTTP {exc.code}"
    except (error.URLError, TimeoutError, OSError) as exc:
        reason = getattr(exc, "reason", None)
        return 0, None, str(reason) if reason is not None else str(exc)


def service_is_up(service_name: str, status_value: str) -> bool:
    normalized = (status_value or "").strip().lower()
    if service_name == "ml":
        return normalized in {"ok", "stale", "degraded", "healthy", "checking"}
    return normalized in {"ok", "degraded", "starting"}


def in_window(now_et: datetime, hour: int, minute: int, window_min: int) -> bool:
    target = now_et.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return target <= now_et < (target + timedelta(minutes=max(1, window_min)))


def fmt_ms_et(ms_value: int | None, tz_name: str) -> str:
    if not ms_value:
        return "--"
    dt = datetime.fromtimestamp(ms_value / 1000.0, ZoneInfo(tz_name))
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def run_flow_query(db_path: Path, lookback_min: int) -> dict[str, int | None]:
    cutoff_ms = int(time.time() * 1000) - max(1, lookback_min) * 60 * 1000
    query = """
        SELECT
            (SELECT COUNT(*) FROM bar_data WHERE ts >= ?) AS bars_lookback,
            (SELECT COUNT(*) FROM touch_events WHERE ts_event >= ?) AS events_lookback,
            (SELECT COUNT(*) FROM prediction_log WHERE ts_prediction >= ? AND COALESCE(is_preview,0)=0) AS pred_live_lookback,
            (SELECT COUNT(*) FROM prediction_log WHERE ts_prediction >= ? AND COALESCE(is_preview,0)=1) AS pred_preview_lookback,
            (SELECT MAX(ts) FROM bar_data) AS last_bar_ms,
            (SELECT MAX(ts_event) FROM touch_events) AS last_event_ms,
            (SELECT MAX(ts_prediction) FROM prediction_log) AS last_pred_ms
    """
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(query, (cutoff_ms, cutoff_ms, cutoff_ms, cutoff_ms)).fetchone()
    finally:
        conn.close()
    return {
        "bars_lookback": int(row[0] or 0),
        "events_lookback": int(row[1] or 0),
        "pred_live_lookback": int(row[2] or 0),
        "pred_preview_lookback": int(row[3] or 0),
        "last_bar_ms": int(row[4]) if row[4] is not None else None,
        "last_event_ms": int(row[5]) if row[5] is not None else None,
        "last_pred_ms": int(row[6]) if row[6] is not None else None,
    }


def resolve_python() -> str:
    venv_python = ROOT / ".venv" / "bin" / "python3"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def notify_via_alert_pipeline(
    env_file: Path,
    subject: str,
    body: str,
    dry_run: bool,
) -> tuple[bool, str]:
    cmd = [
        resolve_python(),
        str(ROOT / "scripts" / "health_alert_watchdog.py"),
        "--env-file",
        str(env_file),
        "--notify-subject",
        subject,
        "--notify-body",
        body,
    ]
    if dry_run:
        cmd.append("--dry-run")
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    output = (proc.stdout or "") + (proc.stderr or "")
    output = output.strip() or "no output"
    return proc.returncode == 0, output


def build_preopen_result(now_et: datetime, timeout_sec: float) -> dict[str, str]:
    ml_url = os.getenv("ML_ALERT_ML_HEALTH_URL", "http://127.0.0.1:5003/health").strip()
    collector_url = os.getenv("ML_ALERT_COLLECTOR_HEALTH_URL", "http://127.0.0.1:5004/health").strip()
    ops_url = os.getenv("ML_SESSION_OPS_STATUS_URL", DEFAULT_OPS_STATUS_URL).strip()
    gamma_url = os.getenv("ML_SESSION_GAMMA_URL", DEFAULT_GAMMA_URL).strip()

    ml_http, ml_payload, ml_reason = fetch_json(ml_url, timeout_sec)
    collector_http, collector_payload, collector_reason = fetch_json(collector_url, timeout_sec)
    ops_http, ops_payload, ops_reason = fetch_json(ops_url, timeout_sec)
    gamma_http, _, gamma_reason = fetch_json(gamma_url, timeout_sec)

    ml_status = str((ml_payload or {}).get("status") or "unreachable")
    collector_status = str((collector_payload or {}).get("status") or "unreachable")
    ml_ok = service_is_up("ml", ml_status)
    collector_ok = service_is_up("collector", collector_status)

    backup_status = str(((ops_payload or {}).get("backup") or {}).get("status") or "unknown")
    drill_status = str(((ops_payload or {}).get("restore_drill") or {}).get("status") or "unknown")
    host_status = str(((ops_payload or {}).get("host_health") or {}).get("status") or "unknown")

    ops_statuses = [backup_status.lower(), drill_status.lower(), host_status.lower()]
    blocking_ops = any(x in {"critical", "failed", "down", "error"} for x in ops_statuses)
    warning_ops = any(x in {"warning", "unknown"} for x in ops_statuses)

    level = "ok"
    if not (ml_ok and collector_ok) or blocking_ops:
        level = "fail"
    elif warning_ops:
        level = "warn"

    gamma_state = f"HTTP {gamma_http}" if gamma_http else gamma_reason or "unreachable"
    if gamma_http >= 200 and gamma_http < 300:
        gamma_state = f"HTTP {gamma_http} (ok)"
    elif gamma_http == 502:
        gamma_state = f"HTTP 502 ({gamma_reason or 'gamma unavailable'})"

    lines = [
        "PivotQuant Session Routine",
        "",
        "Phase: PREOPEN (09:20 ET target)",
        f"Checked (ET): {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"Host: {socket.gethostname()}",
        f"ML: {ml_status} (http={ml_http or 0}, reason={ml_reason or 'status'})",
        f"Collector: {collector_status} (http={collector_http or 0}, reason={collector_reason or 'status'})",
        (
            f"Ops: backup={backup_status}, drill={drill_status}, host={host_status} "
            f"(ops_http={ops_http or 0}, reason={ops_reason or 'status'})"
        ),
        f"Gamma bridge: {gamma_state} (non-blocking)",
        "",
        f"Result: {level.upper()}",
        "",
        "Generated by scripts/session_routine_check.py",
    ]
    return {"level": level, "body": "\n".join(lines)}


def build_postopen_result(now_et: datetime, db_path: Path, timeout_sec: float) -> dict[str, str]:
    ml_url = os.getenv("ML_ALERT_ML_HEALTH_URL", "http://127.0.0.1:5003/health").strip()
    collector_url = os.getenv("ML_ALERT_COLLECTOR_HEALTH_URL", "http://127.0.0.1:5004/health").strip()

    lookback_min = int(os.getenv("ML_SESSION_LOOKBACK_MIN", "15"))
    min_bars = int(os.getenv("ML_SESSION_POSTOPEN_MIN_BARS", "1"))
    min_events = int(os.getenv("ML_SESSION_POSTOPEN_MIN_EVENTS", "0"))
    min_live_preds = int(os.getenv("ML_SESSION_POSTOPEN_MIN_LIVE_PREDS", "0"))

    flow = run_flow_query(db_path=db_path, lookback_min=lookback_min)
    ml_http, ml_payload, ml_reason = fetch_json(ml_url, timeout_sec)
    collector_http, collector_payload, collector_reason = fetch_json(collector_url, timeout_sec)

    ml_status = str((ml_payload or {}).get("status") or "unreachable")
    collector_status = str((collector_payload or {}).get("status") or "unreachable")
    ml_ok = service_is_up("ml", ml_status)
    collector_ok = service_is_up("collector", collector_status)

    bars = int(flow["bars_lookback"] or 0)
    events = int(flow["events_lookback"] or 0)
    pred_live = int(flow["pred_live_lookback"] or 0)
    pred_preview = int(flow["pred_preview_lookback"] or 0)

    level = "ok"
    hard_fail = (not ml_ok) or (not collector_ok) or (bars < min_bars)
    soft_warn = (events < min_events) or (pred_live < min_live_preds) or (events == 0 and pred_live == 0)
    if hard_fail:
        level = "fail"
    elif soft_warn:
        level = "warn"

    tz_name = os.getenv("ML_SESSION_ET_TZ", "America/New_York").strip() or "America/New_York"
    lines = [
        "PivotQuant Session Routine",
        "",
        "Phase: POSTOPEN (09:40 ET target)",
        f"Checked (ET): {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"Host: {socket.gethostname()}",
        f"ML: {ml_status} (http={ml_http or 0}, reason={ml_reason or 'status'})",
        f"Collector: {collector_status} (http={collector_http or 0}, reason={collector_reason or 'status'})",
        f"Flow (last {lookback_min}m): bars={bars}, events={events}, pred_live={pred_live}, pred_preview={pred_preview}",
        f"Thresholds: min_bars={min_bars}, min_events={min_events}, min_live_preds={min_live_preds}",
        f"Last bar: {fmt_ms_et(flow['last_bar_ms'], tz_name)}",
        f"Last event: {fmt_ms_et(flow['last_event_ms'], tz_name)}",
        f"Last pred: {fmt_ms_et(flow['last_pred_ms'], tz_name)}",
        "",
        f"Result: {level.upper()}",
        "",
        "Generated by scripts/session_routine_check.py",
    ]
    return {"level": level, "body": "\n".join(lines)}


def phase_subject(phase: str, level: str) -> str:
    check_prefix = os.getenv("ML_SESSION_CHECK_PREFIX", "[CHECK]").strip() or "[CHECK]"
    alert_prefix = os.getenv("ML_ALERT_SUBJECT_PREFIX", "[ALERT]").strip() or "[ALERT]"
    phase_name = phase.upper()
    if level == "fail":
        return f"{alert_prefix} {phase_name} FLOW CHECK FAILED"
    if level == "warn":
        return f"{check_prefix} {phase_name} FLOW CHECK WARN"
    return f"{check_prefix} {phase_name} FLOW CHECK OK"


def prune_state(state: dict[str, Any], today_et: datetime.date, keep_days: int = 14) -> None:
    daily = state.setdefault("daily", {})
    keep_after = today_et - timedelta(days=keep_days)
    keys_to_remove: list[str] = []
    for key in daily:
        try:
            day = datetime.strptime(key, "%Y-%m-%d").date()
        except ValueError:
            keys_to_remove.append(key)
            continue
        if day < keep_after:
            keys_to_remove.append(key)
    for key in keys_to_remove:
        daily.pop(key, None)


def should_notify_for_level(level: str, notify_on_ok: bool) -> bool:
    if level in {"fail", "warn"}:
        return True
    return notify_on_ok


def main() -> int:
    args = parse_args()

    env_file = Path(args.env_file).expanduser()
    load_env_file(env_file)

    state_file = Path(args.state_file).expanduser()
    log_file = Path(args.log_file).expanduser()
    db_path = Path(args.db_path).expanduser()
    db_path = Path(os.getenv("PIVOT_DB", str(db_path))).expanduser()

    et_tz_name = os.getenv("ML_SESSION_ET_TZ", "America/New_York").strip() or "America/New_York"
    now_et = datetime.now(ZoneInfo(et_tz_name))
    today_key = now_et.strftime("%Y-%m-%d")
    state = load_state(state_file)
    prune_state(state, now_et.date())
    daily = state.setdefault("daily", {})
    day_state = daily.setdefault(today_key, {})

    notify_on_ok_default = parse_bool(os.getenv("ML_SESSION_NOTIFY_ON_OK"), True)
    notify_on_ok_preopen = parse_bool(
        os.getenv("ML_SESSION_PREOPEN_NOTIFY_ON_OK"),
        notify_on_ok_default,
    )
    notify_on_ok_postopen = parse_bool(
        os.getenv("ML_SESSION_POSTOPEN_NOTIFY_ON_OK"),
        notify_on_ok_default,
    )

    phase_windows = {
        "preopen": (
            int(os.getenv("ML_SESSION_PREOPEN_HOUR", "9")),
            int(os.getenv("ML_SESSION_PREOPEN_MINUTE", "20")),
        ),
        "postopen": (
            int(os.getenv("ML_SESSION_POSTOPEN_HOUR", "9")),
            int(os.getenv("ML_SESSION_POSTOPEN_MINUTE", "40")),
        ),
    }
    window_min = int(os.getenv("ML_SESSION_WINDOW_MIN", "10"))

    due_phases: list[str] = []
    if args.force_phase:
        due_phases = [args.force_phase]
    else:
        if now_et.weekday() >= 5:
            return 0
        for phase, (hour, minute) in phase_windows.items():
            if in_window(now_et, hour=hour, minute=minute, window_min=window_min):
                due_phases.append(phase)

    if not due_phases:
        return 0

    any_notify_fail = False
    for phase in due_phases:
        if not args.force and phase in day_state:
            continue

        if phase == "preopen":
            result = build_preopen_result(now_et=now_et, timeout_sec=args.timeout_sec)
            notify_on_ok = notify_on_ok_preopen
        else:
            result = build_postopen_result(now_et=now_et, db_path=db_path, timeout_sec=args.timeout_sec)
            notify_on_ok = notify_on_ok_postopen

        level = result["level"]
        subject = phase_subject(phase=phase, level=level)
        should_notify = should_notify_for_level(level=level, notify_on_ok=notify_on_ok)
        notified = False

        if should_notify and not args.no_notify:
            ok, notify_msg = notify_via_alert_pipeline(
                env_file=env_file,
                subject=subject,
                body=result["body"],
                dry_run=args.dry_run,
            )
            if ok:
                notified = True
                log_line(log_file, f"{phase}: notify ok ({notify_msg})")
            else:
                any_notify_fail = True
                log_line(log_file, f"{phase}: notify failed ({notify_msg})")
        else:
            log_line(
                log_file,
                f"{phase}: check complete (level={level}, notify={'skipped' if not should_notify else 'disabled'})",
            )

        day_state[phase] = {
            "checked_at_utc": now_iso_utc(),
            "checked_at_et": now_et.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "level": level,
            "subject": subject,
            "notified": notified,
            "dry_run": bool(args.dry_run),
        }

    state["last_run_utc"] = now_iso_utc()
    state["host"] = socket.gethostname()
    save_state(state_file, state)

    return 1 if any_notify_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
