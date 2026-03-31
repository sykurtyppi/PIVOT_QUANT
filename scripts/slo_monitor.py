#!/usr/bin/env python3
"""Evaluate explicit SLO budgets and persist pass/warn/fail state."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from datetime import datetime, time as dtime, timezone
from pathlib import Path
from typing import Any
from urllib import error, request

from audit_log import append_event

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_DB = Path(os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite")))
DEFAULT_LOG_FILE = ROOT / "logs" / "slo_monitor.log"
DEFAULT_STATE_FILE = ROOT / "logs" / "slo_state.json"
DEFAULT_REPORT_FILE = ROOT / "logs" / "slo_last.json"
DEFAULT_REPORT_DELIVERY_LOG = ROOT / "logs" / "report_delivery.log"
DEFAULT_HEALTH_ALERT_LOG = ROOT / "logs" / "health_alert.log"
ET_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc


def now_ms() -> int:
    return int(time.time() * 1000)


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PivotQuant SLO checks.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    parser.add_argument("--db-path", default=str(DEFAULT_DB))
    parser.add_argument("--log-file", default=str(DEFAULT_LOG_FILE))
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
    parser.add_argument("--report-file", default=str(DEFAULT_REPORT_FILE))
    parser.add_argument("--report-delivery-log", default=str(DEFAULT_REPORT_DELIVERY_LOG))
    parser.add_argument("--health-alert-log", default=str(DEFAULT_HEALTH_ALERT_LOG))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


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


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


def fetch_health(url: str, timeout_sec: float) -> dict[str, Any]:
    started = time.time()
    req = request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "PivotQuantSloMonitor/1.0"},
    )
    try:
        with request.urlopen(req, timeout=timeout_sec) as response:
            body = response.read().decode("utf-8", errors="replace")
            status_code = int(getattr(response, "status", 200))
    except error.HTTPError as exc:
        return {
            "ok": False,
            "http_status": int(exc.code),
            "latency_ms": int((time.time() - started) * 1000),
            "reason": f"HTTP {exc.code}",
        }
    except Exception as exc:  # pragma: no cover
        return {
            "ok": False,
            "http_status": 0,
            "latency_ms": int((time.time() - started) * 1000),
            "reason": str(exc),
        }

    payload = {}
    try:
        payload = json.loads(body or "{}")
    except json.JSONDecodeError:
        payload = {}
    status = str(payload.get("status", "")).strip().lower()
    ok = (200 <= status_code < 300) and status in {"ok", "degraded", "analog_degraded", "stale", "starting"}
    return {
        "ok": ok,
        "http_status": status_code,
        "latency_ms": int((time.time() - started) * 1000),
        "status": status,
        "reason": f"status={status or '--'}",
    }


def to_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def percentile(values: list[float], p: float) -> float | None:
    clean = sorted(v for v in values if isinstance(v, (float, int)))
    if not clean:
        return None
    if len(clean) == 1:
        return float(clean[0])
    rank = max(0.0, min(1.0, p)) * (len(clean) - 1)
    low = int(rank)
    high = min(len(clean) - 1, low + 1)
    frac = rank - low
    return float(clean[low] * (1 - frac) + clean[high] * frac)


def market_is_open_et(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    t = now.time()
    return dtime(9, 30) <= t < dtime(16, 0)


def parse_log_timestamp_ms(line: str) -> int | None:
    if len(line) < 21 or line[0] != "[":
        return None
    try:
        dt = datetime.strptime(line[1:20], "%Y-%m-%d %H:%M:%S")
        return int(dt.replace(tzinfo=ET_TZ).timestamp() * 1000)
    except ValueError:
        return None


def get_last_report_success_ms(path: Path) -> int | None:
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    for line in reversed(lines):
        if "DONE  daily_report_send" not in line:
            continue
        ts = parse_log_timestamp_ms(line)
        if ts is not None:
            return ts
    return None


def count_down_events_last_24h(path: Path, now_ts_ms: int) -> int:
    if not path.exists():
        return 0
    cutoff = now_ts_ms - (24 * 60 * 60 * 1000)
    count = 0
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "down_transition" not in line:
            continue
        ts = parse_log_timestamp_ms(line)
        if ts is not None and ts >= cutoff:
            count += 1
    return count


def get_db_max_ts(conn: sqlite3.Connection, table: str, col: str) -> int | None:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    if not row:
        return None
    value = conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()[0]
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def get_ops_status(conn: sqlite3.Connection) -> dict[str, str]:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ops_status' LIMIT 1"
    ).fetchone()
    if not row:
        return {}
    rows = conn.execute("SELECT key, value FROM ops_status").fetchall()
    return {str(key): str(value) for key, value in rows}


def set_ops_status(conn: sqlite3.Connection, pairs: dict[str, str]) -> None:
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


def budget_entry(name: str, value: float | int | str, target: float | int | str, status: str, reason: str) -> dict[str, Any]:
    return {
        "name": name,
        "value": value,
        "target": target,
        "status": status,
        "reason": reason,
    }


def run() -> int:
    args = parse_args()
    load_env_file(Path(args.env_file).expanduser())

    db_path = Path(args.db_path).expanduser()
    log_file = Path(args.log_file).expanduser()
    state_file = Path(args.state_file).expanduser()
    report_file = Path(args.report_file).expanduser()
    report_delivery_log = Path(args.report_delivery_log).expanduser()
    health_alert_log = Path(args.health_alert_log).expanduser()

    window_hours = int(os.getenv("SLO_WINDOW_HOURS", "24"))
    min_sample_count = int(os.getenv("SLO_MIN_SAMPLE_COUNT", "24"))
    ml_uptime_target = to_float(os.getenv("SLO_ML_UPTIME_MIN_PCT_24H", "99.0"))
    collector_uptime_target = to_float(os.getenv("SLO_COLLECTOR_UPTIME_MIN_PCT_24H", "99.0"))
    ml_p95_target_ms = to_float(os.getenv("SLO_ML_P95_LATENCY_MAX_MS", "2000"))
    collector_p95_target_ms = to_float(os.getenv("SLO_COLLECTOR_P95_LATENCY_MAX_MS", "2000"))
    pred_age_open_sec = to_float(os.getenv("SLO_MAX_PREDICTION_AGE_SEC_OPEN", "300"))
    pred_age_closed_sec = to_float(os.getenv("SLO_MAX_PREDICTION_AGE_SEC_CLOSED", "3600"))
    retrain_age_hours = to_float(os.getenv("SLO_MAX_RETRAIN_AGE_HOURS", "8"))
    report_age_hours = to_float(os.getenv("SLO_MAX_DAILY_REPORT_AGE_HOURS", "30"))
    alert_down_max = int(to_float(os.getenv("SLO_MAX_ALERT_DOWN_EVENTS_24H", "6")))
    health_timeout_sec = to_float(os.getenv("SLO_HEALTH_TIMEOUT_SEC", "4"))

    now_ts_ms = now_ms()
    now_et = datetime.now(ET_TZ)
    window_cutoff_ms = now_ts_ms - (window_hours * 60 * 60 * 1000)

    ml_health_url = os.getenv("ML_ALERT_ML_HEALTH_URL", "http://127.0.0.1:5003/health").strip()
    collector_health_url = os.getenv("ML_ALERT_COLLECTOR_HEALTH_URL", "http://127.0.0.1:5004/health").strip()

    ml_health = fetch_health(ml_health_url, timeout_sec=health_timeout_sec)
    collector_health = fetch_health(collector_health_url, timeout_sec=health_timeout_sec)

    state = load_json(state_file, default={})
    samples = state.get("samples", [])
    if not isinstance(samples, list):
        samples = []
    sample = {
        "ts_ms": now_ts_ms,
        "ml_up": bool(ml_health["ok"]),
        "ml_latency_ms": int(ml_health["latency_ms"]),
        "collector_up": bool(collector_health["ok"]),
        "collector_latency_ms": int(collector_health["latency_ms"]),
    }
    samples.append(sample)
    samples = [s for s in samples if isinstance(s, dict) and int(s.get("ts_ms", 0)) >= window_cutoff_ms]

    sample_count = len(samples)
    ml_up_count = sum(1 for s in samples if s.get("ml_up"))
    collector_up_count = sum(1 for s in samples if s.get("collector_up"))
    ml_uptime_pct = (ml_up_count / sample_count * 100.0) if sample_count else 0.0
    collector_uptime_pct = (collector_up_count / sample_count * 100.0) if sample_count else 0.0
    ml_p95 = percentile([float(s.get("ml_latency_ms", 0)) for s in samples if s.get("ml_up")], 0.95)
    collector_p95 = percentile(
        [float(s.get("collector_latency_ms", 0)) for s in samples if s.get("collector_up")],
        0.95,
    )

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        ops = get_ops_status(conn)
        last_prediction_ts = get_db_max_ts(conn, "prediction_log", "ts_prediction")
    finally:
        conn.close()

    prediction_age_sec = None
    if last_prediction_ts is not None:
        prediction_age_sec = max(0.0, (now_ts_ms - last_prediction_ts) / 1000.0)

    retrain_last_end_ms = int(float(ops.get("retrain_last_end_ms", "0") or 0))
    retrain_last_status = str(ops.get("retrain_last_status", "unknown")).lower()
    retrain_age_sec = max(0.0, (now_ts_ms - retrain_last_end_ms) / 1000.0) if retrain_last_end_ms > 0 else None

    report_last_success_ms = get_last_report_success_ms(report_delivery_log)
    report_age_sec = max(0.0, (now_ts_ms - report_last_success_ms) / 1000.0) if report_last_success_ms else None
    alert_down_events_24h = count_down_events_last_24h(health_alert_log, now_ts_ms)

    budgets: list[dict[str, Any]] = []
    fail_count = 0
    warn_count = 0

    if sample_count < min_sample_count:
        budgets.append(
            budget_entry(
                "sample_count_24h",
                sample_count,
                f">={min_sample_count}",
                "warn",
                "Insufficient sample window for stable uptime percentile checks.",
            )
        )
        warn_count += 1
    else:
        budgets.append(
            budget_entry(
                "sample_count_24h",
                sample_count,
                f">={min_sample_count}",
                "pass",
                "Sample window sufficient.",
            )
        )

    ml_uptime_status = "pass" if ml_uptime_pct >= ml_uptime_target else "fail"
    budgets.append(
        budget_entry(
            "ml_uptime_pct_24h",
            round(ml_uptime_pct, 2),
            ml_uptime_target,
            ml_uptime_status,
            f"ML endpoint sampled at {ml_uptime_pct:.2f}% availability.",
        )
    )
    fail_count += 1 if ml_uptime_status == "fail" else 0

    collector_uptime_status = "pass" if collector_uptime_pct >= collector_uptime_target else "fail"
    budgets.append(
        budget_entry(
            "collector_uptime_pct_24h",
            round(collector_uptime_pct, 2),
            collector_uptime_target,
            collector_uptime_status,
            f"Collector endpoint sampled at {collector_uptime_pct:.2f}% availability.",
        )
    )
    fail_count += 1 if collector_uptime_status == "fail" else 0

    ml_latency_status = "pass" if (ml_p95 is not None and ml_p95 <= ml_p95_target_ms) else "fail"
    budgets.append(
        budget_entry(
            "ml_p95_latency_ms_24h",
            round(ml_p95, 1) if ml_p95 is not None else "n/a",
            ml_p95_target_ms,
            ml_latency_status,
            "ML p95 latency over successful checks.",
        )
    )
    fail_count += 1 if ml_latency_status == "fail" else 0

    collector_latency_status = (
        "pass" if (collector_p95 is not None and collector_p95 <= collector_p95_target_ms) else "fail"
    )
    budgets.append(
        budget_entry(
            "collector_p95_latency_ms_24h",
            round(collector_p95, 1) if collector_p95 is not None else "n/a",
            collector_p95_target_ms,
            collector_latency_status,
            "Collector p95 latency over successful checks.",
        )
    )
    fail_count += 1 if collector_latency_status == "fail" else 0

    prediction_age_threshold = pred_age_open_sec if market_is_open_et(now_et) else pred_age_closed_sec
    if prediction_age_sec is None:
        pred_status = "fail"
        pred_reason = "No prediction timestamp available."
    else:
        pred_status = "pass" if prediction_age_sec <= prediction_age_threshold else "fail"
        pred_reason = f"Prediction age is {prediction_age_sec:.1f}s."
    budgets.append(
        budget_entry(
            "prediction_freshness_sec",
            round(prediction_age_sec, 1) if prediction_age_sec is not None else "n/a",
            prediction_age_threshold,
            pred_status,
            pred_reason,
        )
    )
    fail_count += 1 if pred_status == "fail" else 0

    if retrain_age_sec is None:
        retrain_status = "fail"
        retrain_reason = "No retrain completion timestamp found."
    else:
        max_retrain_age_sec = retrain_age_hours * 3600.0
        retrain_status = "pass" if retrain_last_status == "ok" and retrain_age_sec <= max_retrain_age_sec else "fail"
        retrain_reason = f"Last retrain status={retrain_last_status}, age={retrain_age_sec/3600.0:.2f}h."
    budgets.append(
        budget_entry(
            "retrain_freshness_hours",
            round((retrain_age_sec or 0.0) / 3600.0, 3) if retrain_age_sec is not None else "n/a",
            retrain_age_hours,
            retrain_status,
            retrain_reason,
        )
    )
    fail_count += 1 if retrain_status == "fail" else 0

    if report_age_sec is None:
        report_status = "warn"
        report_reason = "No successful daily_report_send found in report_delivery.log."
        warn_count += 1
    else:
        max_report_age_sec = report_age_hours * 3600.0
        report_status = "pass" if report_age_sec <= max_report_age_sec else "fail"
        report_reason = f"Last report send age={report_age_sec/3600.0:.2f}h."
        if report_status == "fail":
            fail_count += 1
    budgets.append(
        budget_entry(
            "daily_report_age_hours",
            round((report_age_sec or 0.0) / 3600.0, 3) if report_age_sec is not None else "n/a",
            report_age_hours,
            report_status,
            report_reason,
        )
    )

    alert_status = "pass" if alert_down_events_24h <= alert_down_max else "fail"
    budgets.append(
        budget_entry(
            "alert_down_events_24h",
            alert_down_events_24h,
            f"<={alert_down_max}",
            alert_status,
            "Number of DOWN transitions over the last 24h.",
        )
    )
    fail_count += 1 if alert_status == "fail" else 0

    overall = "ok"
    if fail_count > 0:
        overall = "fail"
    elif warn_count > 0:
        overall = "warn"

    report = {
        "status": overall,
        "checked_at": now_iso_utc(),
        "checked_at_ms": now_ts_ms,
        "window_hours": window_hours,
        "sample_count": sample_count,
        "metrics": {
            "ml_uptime_pct_24h": round(ml_uptime_pct, 4),
            "collector_uptime_pct_24h": round(collector_uptime_pct, 4),
            "ml_p95_latency_ms_24h": round(ml_p95, 3) if ml_p95 is not None else None,
            "collector_p95_latency_ms_24h": round(collector_p95, 3) if collector_p95 is not None else None,
            "prediction_age_sec": round(prediction_age_sec, 3) if prediction_age_sec is not None else None,
            "retrain_age_sec": round(retrain_age_sec, 3) if retrain_age_sec is not None else None,
            "daily_report_age_sec": round(report_age_sec, 3) if report_age_sec is not None else None,
            "alert_down_events_24h": alert_down_events_24h,
            "ml_health": ml_health,
            "collector_health": collector_health,
        },
        "budgets": budgets,
    }

    set_pairs = {
        "slo_last_status": overall,
        "slo_last_run_ms": str(now_ts_ms),
        "slo_window_samples": str(sample_count),
        "slo_ml_uptime_pct_24h": f"{ml_uptime_pct:.4f}",
        "slo_collector_uptime_pct_24h": f"{collector_uptime_pct:.4f}",
        "slo_ml_p95_latency_ms_24h": "" if ml_p95 is None else f"{ml_p95:.3f}",
        "slo_collector_p95_latency_ms_24h": "" if collector_p95 is None else f"{collector_p95:.3f}",
        "slo_prediction_age_sec": "" if prediction_age_sec is None else f"{prediction_age_sec:.3f}",
        "slo_retrain_age_sec": "" if retrain_age_sec is None else f"{retrain_age_sec:.3f}",
        "slo_daily_report_age_sec": "" if report_age_sec is None else f"{report_age_sec:.3f}",
        "slo_alert_down_events_24h": str(alert_down_events_24h),
        "slo_last_error": "; ".join(item["name"] for item in budgets if item["status"] == "fail"),
    }

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        set_ops_status(conn, set_pairs)
        conn.commit()
    finally:
        conn.close()

    previous_status = str(state.get("last_status") or "")
    if overall != previous_status:
        try:
            append_event(
                db_path=db_path,
                event_type="slo_status_change",
                source="slo_monitor",
                actor=os.getenv("USER", "unknown"),
                host=os.getenv("HOSTNAME", ""),
                commit_hash="",
                message=f"SLO status changed {previous_status or 'unknown'} -> {overall}",
                details={
                    "previous_status": previous_status or "unknown",
                    "current_status": overall,
                    "fail_count": fail_count,
                    "warn_count": warn_count,
                },
            )
        except Exception:
            pass

    state["last_status"] = overall
    state["last_checked_ms"] = now_ts_ms
    state["samples"] = samples

    summary = (
        f"slo status={overall} samples={sample_count} ml_uptime={ml_uptime_pct:.2f}% "
        f"collector_uptime={collector_uptime_pct:.2f}% pred_age_sec="
        f"{'n/a' if prediction_age_sec is None else f'{prediction_age_sec:.1f}'}"
    )
    log_line(log_file, summary)

    if not args.dry_run:
        save_json(state_file, state)
        save_json(report_file, report)

    return 0 if overall in {"ok", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(run())
