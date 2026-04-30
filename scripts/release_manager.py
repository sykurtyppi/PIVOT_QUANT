#!/usr/bin/env python3
"""Release gate/promote manager for staging -> production discipline."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, request

ROOT = Path(__file__).resolve().parents[1]

try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(ROOT / ".env", override=False)
except ImportError:
    pass

from audit_log import append_event, detect_commit_hash

DEFAULT_DB = Path(os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite")))
DEFAULT_RELEASE_DIR = ROOT / "logs" / "releases"
DEFAULT_PYTHON = str((ROOT / ".venv" / "bin" / "python3"))


def now_ms() -> int:
    return int(time.time() * 1000)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def select_python() -> str:
    if Path(DEFAULT_PYTHON).exists():
        return DEFAULT_PYTHON
    return sys.executable


def run_cmd(cmd: list[str], cwd: Path, timeout_sec: int = 900) -> dict[str, Any]:
    start = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout_sec,
    )
    duration_ms = int((time.time() - start) * 1000)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "duration_ms": duration_ms,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def set_ops_status(db_path: Path, pairs: dict[str, str]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_status (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at INTEGER NOT NULL
            )
            """
        )
        ts = now_ms()
        for key, value in pairs.items():
            conn.execute(
                """
                INSERT INTO ops_status(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE
                  SET value = excluded.value,
                      updated_at = excluded.updated_at
                """,
                (key, value, ts),
            )
        conn.commit()
    finally:
        conn.close()


def check_http_health(url: str, timeout_sec: float) -> dict[str, Any]:
    started = time.time()
    req = request.Request(url, headers={"Accept": "application/json"})
    try:
        with request.urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            status = int(getattr(resp, "status", 200))
    except error.HTTPError as exc:
        return {
            "ok": False,
            "status": int(exc.code),
            "latency_ms": int((time.time() - started) * 1000),
            "reason": f"HTTP {exc.code}",
        }
    except Exception as exc:  # pragma: no cover
        return {
            "ok": False,
            "status": 0,
            "latency_ms": int((time.time() - started) * 1000),
            "reason": str(exc),
        }

    payload = {}
    try:
        payload = json.loads(body or "{}")
    except json.JSONDecodeError:
        payload = {}
    status_value = str(payload.get("status", "")).strip().lower()
    ok = (200 <= status < 300) and status_value in {"ok", "degraded", "analog_degraded", "stale", "starting"}
    return {
        "ok": ok,
        "status": status,
        "latency_ms": int((time.time() - started) * 1000),
        "reason": f"status={status_value or '--'}",
    }


def evaluate_gate(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    commit = args.commit.strip() or detect_commit_hash(ROOT)
    checks: list[dict[str, Any]] = []
    failures = 0
    warnings = 0
    python_bin = select_python()

    if not args.allow_dirty:
        git_state = run_cmd(["git", "status", "--porcelain"], ROOT, timeout_sec=30)
        dirty = bool((git_state["stdout"] or "").strip())
        checks.append(
            {
                "name": "git_clean",
                "required": True,
                "ok": not dirty and git_state["returncode"] == 0,
                "details": "working tree clean" if not dirty else "working tree has local changes",
                "cmd": git_state["cmd"],
                "returncode": git_state["returncode"],
            }
        )
        if dirty or git_state["returncode"] != 0:
            failures += 1

    smoke = run_cmd(
        [python_bin, "-m", "unittest", "discover", "-s", "tests/python", "-p", "test_*.py", "-v"],
        ROOT,
        timeout_sec=1200,
    )
    checks.append(
        {
            "name": "ops_smoke_tests",
            "required": True,
            "ok": smoke["returncode"] == 0,
            "cmd": smoke["cmd"],
            "returncode": smoke["returncode"],
            "duration_ms": smoke["duration_ms"],
            "stdout_tail": "\n".join((smoke["stdout"] or "").splitlines()[-40:]),
            "stderr_tail": "\n".join((smoke["stderr"] or "").splitlines()[-20:]),
        }
    )
    if smoke["returncode"] != 0:
        failures += 1

    bash_check = run_cmd(
        [
            "/bin/bash",
            "-n",
            "scripts/run_retrain_cycle.sh",
            "scripts/run_daily_report_send.sh",
            "scripts/run_calibration_refit.sh",
            "scripts/install_ops_resilience_launch_agents.sh",
        ],
        ROOT,
        timeout_sec=60,
    )
    checks.append(
        {
            "name": "bash_syntax",
            "required": True,
            "ok": bash_check["returncode"] == 0,
            "cmd": bash_check["cmd"],
            "returncode": bash_check["returncode"],
            "stderr_tail": "\n".join((bash_check["stderr"] or "").splitlines()[-20:]),
        }
    )
    if bash_check["returncode"] != 0:
        failures += 1

    py_compile = run_cmd(
        [
            python_bin,
            "-m",
            "py_compile",
            "scripts/model_governance.py",
            "scripts/release_manager.py",
            "scripts/slo_monitor.py",
            "scripts/full_restore_drill.py",
            "scripts/audit_log.py",
        ],
        ROOT,
        timeout_sec=60,
    )
    checks.append(
        {
            "name": "python_compile",
            "required": True,
            "ok": py_compile["returncode"] == 0,
            "cmd": py_compile["cmd"],
            "returncode": py_compile["returncode"],
            "stderr_tail": "\n".join((py_compile["stderr"] or "").splitlines()[-20:]),
        }
    )
    if py_compile["returncode"] != 0:
        failures += 1

    if not args.skip_health:
        for name, url in [
            ("ml_health", args.ml_health_url),
            ("collector_health", args.collector_health_url),
            ("ops_status", args.ops_status_url),
        ]:
            health = check_http_health(url, timeout_sec=args.health_timeout_sec)
            optional = name == "ops_status" and args.environment == "staging"
            ok = bool(health["ok"])
            checks.append(
                {
                    "name": name,
                    "required": not optional,
                    "ok": ok,
                    "url": url,
                    "http_status": health["status"],
                    "latency_ms": health["latency_ms"],
                    "reason": health["reason"],
                }
            )
            if not ok:
                if optional:
                    warnings += 1
                else:
                    failures += 1

    status = "pass" if failures == 0 else "fail"
    report = {
        "status": status,
        "checked_at": now_iso(),
        "checked_at_ms": now_ms(),
        "environment": args.environment,
        "commit": commit,
        "failures": failures,
        "warnings": warnings,
        "checks": checks,
    }
    return (0 if status == "pass" else 1), report


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def latest_gate_report(release_dir: Path, source_env: str) -> Path | None:
    if not release_dir.exists():
        return None
    candidates = sorted(release_dir.glob(f"gate_{source_env}_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def run_promote(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    commit = args.commit.strip() or detect_commit_hash(ROOT)
    release_dir = Path(args.release_dir).expanduser()
    gate_report_path = Path(args.gate_report).expanduser() if args.gate_report else latest_gate_report(release_dir, args.source_environment)
    if gate_report_path is None or not gate_report_path.exists():
        return 1, {"status": "error", "message": "No gate report found for promotion"}

    try:
        gate_report = json.loads(gate_report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return 1, {"status": "error", "message": f"Invalid gate report JSON: {gate_report_path}"}

    gate_status = str(gate_report.get("status") or "").lower()
    gate_commit = str(gate_report.get("commit") or "")
    gate_env = str(gate_report.get("environment") or "")
    if gate_status != "pass":
        return 1, {"status": "error", "message": f"Gate report is not pass: {gate_status}", "gate_report": str(gate_report_path)}
    if gate_commit != commit:
        return 1, {"status": "error", "message": f"Gate commit mismatch ({gate_commit} != {commit})", "gate_report": str(gate_report_path)}
    if gate_env != args.source_environment:
        return 1, {"status": "error", "message": f"Gate env mismatch ({gate_env} != {args.source_environment})", "gate_report": str(gate_report_path)}

    promoted_at_ms = now_ms()
    promotion_id = f"{args.target_environment}-{commit}-{promoted_at_ms}"
    record = {
        "promotion_id": promotion_id,
        "promoted_at": now_iso(),
        "promoted_at_ms": promoted_at_ms,
        "source_environment": args.source_environment,
        "target_environment": args.target_environment,
        "commit": commit,
        "gate_report": str(gate_report_path),
        "actor": os.getenv("USER", "unknown"),
        "host": os.getenv("HOSTNAME", ""),
    }
    release_dir.mkdir(parents=True, exist_ok=True)
    promotions_file = release_dir / "promotions.jsonl"
    with promotions_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")

    result = {
        "status": "ok",
        "action": "promote",
        "promotion": record,
    }
    return 0, result


def load_ops_status(db_path: Path) -> dict[str, str]:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ops_status' LIMIT 1"
        ).fetchone()
        if not row:
            return {}
        rows = conn.execute("SELECT key, value FROM ops_status").fetchall()
        return {str(k): str(v) for k, v in rows}
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Release discipline tooling.")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--release-dir", default=str(DEFAULT_RELEASE_DIR))

    sub = parser.add_subparsers(dest="command", required=True)

    gate = sub.add_parser("gate", help="Run release gate checks")
    gate.add_argument("--environment", choices=["staging", "production"], default="staging")
    gate.add_argument("--commit", default="")
    gate.add_argument("--allow-dirty", action="store_true")
    gate.add_argument("--skip-health", action="store_true")
    gate.add_argument("--health-timeout-sec", type=float, default=4.0)
    gate.add_argument("--ml-health-url", default="http://127.0.0.1:5003/health")
    gate.add_argument("--collector-health-url", default="http://127.0.0.1:5004/health")
    gate.add_argument("--ops-status-url", default="http://127.0.0.1:3000/api/ops/status")
    gate.add_argument("--output", default="")

    promote = sub.add_parser("promote", help="Promote a passed gate release")
    promote.add_argument("--source-environment", choices=["staging", "production"], default="staging")
    promote.add_argument("--target-environment", choices=["staging", "production"], default="production")
    promote.add_argument("--commit", default="")
    promote.add_argument("--gate-report", default="")

    sub.add_parser("status", help="Show release status")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser()
    release_dir = Path(args.release_dir).expanduser()

    if args.command == "gate":
        code, report = evaluate_gate(args)
        commit = str(report.get("commit") or "")
        report_name = args.output.strip()
        if report_name:
            report_path = Path(report_name).expanduser()
        else:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = release_dir / f"gate_{args.environment}_{stamp}_{commit or 'unknown'}.json"
        write_report(report_path, report)

        set_ops_status(
            db_path,
            {
                "release_gate_last_status": report["status"],
                "release_gate_last_env": args.environment,
                "release_gate_last_commit": commit,
                "release_gate_last_run_ms": str(report.get("checked_at_ms", now_ms())),
                "release_gate_last_report": str(report_path),
                "release_gate_last_error": "" if code == 0 else f"{report.get('failures', 0)} gate check(s) failed",
            },
        )
        append_event(
            db_path=db_path,
            event_type="release_gate",
            source="release_manager",
            actor=os.getenv("USER", "unknown"),
            host=os.getenv("HOSTNAME", ""),
            commit_hash=commit,
            message=f"release gate {report['status']} ({args.environment})",
            details={
                "environment": args.environment,
                "status": report["status"],
                "failures": report.get("failures", 0),
                "warnings": report.get("warnings", 0),
                "report_path": str(report_path),
            },
        )
        print(json.dumps({"status": report["status"], "report_path": str(report_path), "report": report}, indent=2))
        return code

    if args.command == "promote":
        code, result = run_promote(args)
        commit = args.commit.strip() or detect_commit_hash(ROOT)
        if code == 0:
            promotion = result.get("promotion", {})
            set_ops_status(
                db_path,
                {
                    "release_active_commit": str(promotion.get("commit") or ""),
                    "release_active_env": str(promotion.get("target_environment") or ""),
                    "release_last_promoted_ms": str(promotion.get("promoted_at_ms") or now_ms()),
                    "release_last_promotion_id": str(promotion.get("promotion_id") or ""),
                    "release_last_source_env": str(promotion.get("source_environment") or ""),
                    "release_last_gate_report": str(promotion.get("gate_report") or ""),
                    "release_last_error": "",
                },
            )
            append_event(
                db_path=db_path,
                event_type="release_promote",
                source="release_manager",
                actor=os.getenv("USER", "unknown"),
                host=os.getenv("HOSTNAME", ""),
                commit_hash=commit,
                message=f"release promoted {args.source_environment}->{args.target_environment}",
                details=result.get("promotion", {}),
            )
        else:
            set_ops_status(
                db_path,
                {
                    "release_last_error": str(result.get("message") or "release promotion failed"),
                },
            )
            append_event(
                db_path=db_path,
                event_type="release_promote_failed",
                source="release_manager",
                actor=os.getenv("USER", "unknown"),
                host=os.getenv("HOSTNAME", ""),
                commit_hash=commit,
                message=str(result.get("message") or "release promotion failed"),
                details={
                    "source_environment": args.source_environment,
                    "target_environment": args.target_environment,
                    "gate_report": args.gate_report,
                },
            )

        print(json.dumps(result, indent=2))
        return code

    if args.command == "status":
        ops = load_ops_status(db_path)
        latest_gate = latest_gate_report(release_dir, "staging")
        promotions_file = release_dir / "promotions.jsonl"
        last_promotion = None
        if promotions_file.exists():
            lines = promotions_file.read_text(encoding="utf-8", errors="replace").splitlines()
            if lines:
                try:
                    last_promotion = json.loads(lines[-1])
                except json.JSONDecodeError:
                    last_promotion = {"raw": lines[-1]}

        print(
            json.dumps(
                {
                    "status": "ok",
                    "release_dir": str(release_dir),
                    "ops": {
                        "release_gate_last_status": ops.get("release_gate_last_status", "unknown"),
                        "release_gate_last_env": ops.get("release_gate_last_env", ""),
                        "release_gate_last_commit": ops.get("release_gate_last_commit", ""),
                        "release_gate_last_report": ops.get("release_gate_last_report", ""),
                        "release_active_commit": ops.get("release_active_commit", ""),
                        "release_active_env": ops.get("release_active_env", ""),
                        "release_last_promoted_ms": ops.get("release_last_promoted_ms", ""),
                        "release_last_error": ops.get("release_last_error", ""),
                    },
                    "latest_gate_report": str(latest_gate) if latest_gate else "",
                    "last_promotion": last_promotion,
                },
                indent=2,
            )
        )
        return 0

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
