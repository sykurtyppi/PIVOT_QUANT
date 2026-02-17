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
import sqlite3
import smtplib
import subprocess
import sys
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib import error, request

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite"))
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

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

ET_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc


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
        "--db",
        default=os.getenv("PIVOT_DB", DEFAULT_DB),
        help="SQLite DB path used for freshness/throughput stats.",
    )
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


def parse_report_day(report_date_str: str) -> date:
    try:
        return datetime.strptime(report_date_str, "%Y-%m-%d").date()
    except ValueError:
        return datetime.now(ET_TZ).date()


def et_day_bounds_ms(report_day: date) -> tuple[int, int]:
    start_dt = datetime.combine(report_day, dtime.min, tzinfo=ET_TZ)
    end_dt = start_dt + timedelta(days=1)
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text or text == "--":
        return None
    match = re.search(r"-?\d+(\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def format_age_from_ms(ts_ms: int | None) -> str:
    if ts_ms is None or ts_ms <= 0:
        return "--"
    age_sec = max(0.0, time.time() - (ts_ms / 1000.0))
    if age_sec < 60:
        return f"{int(age_sec)}s"
    if age_sec < 3600:
        return f"{int(age_sec // 60)}m"
    if age_sec < 86400:
        return f"{age_sec / 3600.0:.1f}h"
    return f"{age_sec / 86400.0:.1f}d"


def format_ts_et(ts_ms: int | None) -> str:
    if ts_ms is None or ts_ms <= 0:
        return "--"
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(ET_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def get_market_session_context() -> dict[str, str]:
    now_et = datetime.now(ET_TZ)
    weekday = now_et.weekday() < 5
    current_time = now_et.time()
    market_open = dtime(9, 30)
    market_close = dtime(16, 0)
    premarket_open = dtime(4, 0)
    afterhours_close = dtime(20, 0)

    market_state = "CLOSED"
    session = "off-hours"
    if weekday:
        if market_open <= current_time < market_close:
            market_state = "OPEN"
            session = "regular"
        elif premarket_open <= current_time < market_open:
            market_state = "CLOSED"
            session = "premarket"
        elif market_close <= current_time < afterhours_close:
            market_state = "CLOSED"
            session = "after-hours"
        else:
            market_state = "CLOSED"
            session = "off-hours"
    else:
        session = "weekend"

    return {
        "market_state": market_state,
        "session": session,
        "now_et": now_et.strftime("%Y-%m-%d %H:%M:%S %Z"),
    }


def connect_db_if_exists(db_path: str) -> sqlite3.Connection | None:
    path = Path(db_path).expanduser()
    if not path.exists():
        return None
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def get_count_between(conn: sqlite3.Connection, table: str, col: str, start_ms: int, end_ms: int) -> int:
    return int(
        conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {col} >= ? AND {col} < ?",
            (start_ms, end_ms),
        ).fetchone()[0]
    )


def get_max_ts(conn: sqlite3.Connection, table: str, col: str) -> int | None:
    row = conn.execute(f"SELECT MAX({col}) AS v FROM {table}").fetchone()
    if not row:
        return None
    value = row["v"] if isinstance(row, sqlite3.Row) else row[0]
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def fetch_db_progress(
    db_path: str,
    report_day: date,
) -> dict[str, Any]:
    start_ms, end_ms = et_day_bounds_ms(report_day)
    stats: dict[str, Any] = {
        "db_path": db_path,
        "bars_today": None,
        "events_today": None,
        "predictions_today": None,
        "predictions_live_today": None,
        "predictions_preview_today": None,
        "last_bar_ts": None,
        "last_event_ts": None,
        "last_prediction_ts": None,
    }

    conn = connect_db_if_exists(db_path)
    if conn is None:
        stats["error"] = f"DB not found: {db_path}"
        return stats

    try:
        if table_exists(conn, "bar_data"):
            stats["bars_today"] = get_count_between(conn, "bar_data", "ts", start_ms, end_ms)
            stats["last_bar_ts"] = get_max_ts(conn, "bar_data", "ts")

        if table_exists(conn, "touch_events"):
            stats["events_today"] = get_count_between(conn, "touch_events", "ts_event", start_ms, end_ms)
            stats["last_event_ts"] = get_max_ts(conn, "touch_events", "ts_event")

        if table_exists(conn, "prediction_log"):
            stats["predictions_today"] = get_count_between(conn, "prediction_log", "ts_prediction", start_ms, end_ms)
            pred_cols = {r[1] for r in conn.execute("PRAGMA table_info(prediction_log)").fetchall()}
            if "is_preview" in pred_cols:
                live = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM prediction_log
                        WHERE ts_prediction >= ? AND ts_prediction < ? AND COALESCE(is_preview, 0) = 0
                        """,
                        (start_ms, end_ms),
                    ).fetchone()[0]
                )
                preview = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM prediction_log
                        WHERE ts_prediction >= ? AND ts_prediction < ? AND COALESCE(is_preview, 0) = 1
                        """,
                        (start_ms, end_ms),
                    ).fetchone()[0]
                )
                stats["predictions_live_today"] = live
                stats["predictions_preview_today"] = preview
            stats["last_prediction_ts"] = get_max_ts(conn, "prediction_log", "ts_prediction")
    finally:
        conn.close()

    return stats


def parse_calibration_drift(markdown: str) -> list[dict[str, str | float | None]]:
    rows: list[dict[str, str | float | None]] = []
    lines = markdown.splitlines()
    try:
        start = lines.index("## Calibration Drift vs Trailing 20 Reports")
    except ValueError:
        return rows

    for line in lines[start + 1 :]:
        if line.startswith("## "):
            break
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 7 or cells[0] in {"Horizon", "---"}:
            continue
        rows.append(
            {
                "horizon": cells[0],
                "mfe_delta": parse_float(cells[5]),
                "mae_delta": parse_float(cells[6]),
                "mfe_raw": cells[5],
                "mae_raw": cells[6],
            }
        )
    return rows


def build_mfe_mae_summary(drift_rows: list[dict[str, str | float | None]]) -> list[str]:
    lines: list[str] = []
    for row in drift_rows:
        horizon = str(row["horizon"])
        mfe_raw = str(row["mfe_raw"])
        mae_raw = str(row["mae_raw"])
        if mfe_raw == "--" and mae_raw == "--":
            continue
        lines.append(f"{horizon}: MFE Δ={mfe_raw}, MAE Δ={mae_raw}")

    if not lines:
        return ["No MFE/MAE drift deltas available yet."]
    return lines


def parse_retrain_status(log_tails: dict[str, str]) -> dict[str, str]:
    retrain_text = ""
    for path, body in log_tails.items():
        if Path(path).name == "retrain.log":
            retrain_text = body
            break

    result = {
        "last_cycle": "unknown",
        "reload_status": "unknown",
        "next_expected": "unknown",
    }
    if not retrain_text:
        return result

    ts_re = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+(.*)$")
    last_cycle_dt: datetime | None = None
    last_reload_call_dt: datetime | None = None
    last_reload_warn_dt: datetime | None = None

    for raw in retrain_text.splitlines():
        match = ts_re.match(raw.strip())
        if not match:
            continue
        ts_str, msg = match.groups()
        try:
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if "Retrain cycle complete." in msg:
            last_cycle_dt = dt
        if "Reloading ML server models..." in msg:
            last_reload_call_dt = dt
        if "WARN: ML server reload failed" in msg:
            last_reload_warn_dt = dt

    if last_cycle_dt is not None:
        result["last_cycle"] = last_cycle_dt.strftime("%Y-%m-%d %H:%M:%S")
        result["next_expected"] = (last_cycle_dt + timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")

    if last_reload_call_dt is None:
        result["reload_status"] = "unknown"
    elif last_reload_warn_dt is None or last_reload_call_dt > last_reload_warn_dt:
        result["reload_status"] = "ok"
    else:
        result["reload_status"] = "failed"

    return result


def fetch_ops_status(db_path: str) -> dict[str, str]:
    conn = connect_db_if_exists(db_path)
    if conn is None:
        return {}
    try:
        if not table_exists(conn, "ops_status"):
            return {}
        rows = conn.execute("SELECT key, value FROM ops_status").fetchall()
        return {str(r["key"]): str(r["value"]) if r["value"] is not None else "" for r in rows}
    finally:
        conn.close()


def ms_to_local_str(value: str | None) -> str:
    if not value:
        return "unknown"
    try:
        ts_ms = int(float(value))
    except (TypeError, ValueError):
        return "unknown"
    return format_ts_et(ts_ms)


def build_retrain_status(ops_status: dict[str, str], log_status: dict[str, str]) -> dict[str, str]:
    if not ops_status:
        return log_status

    last_end = ms_to_local_str(ops_status.get("retrain_last_end_ms"))
    reload_status = ops_status.get("reload_last_status", "").strip().lower() or "unknown"
    retrain_state = ops_status.get("retrain_state", "").strip().lower()
    retrain_last_status = ops_status.get("retrain_last_status", "").strip().lower()

    next_expected = "unknown"
    try:
        end_ms = int(float(ops_status.get("retrain_last_end_ms", "")))
        next_expected = format_ts_et(end_ms + 6 * 60 * 60 * 1000)
    except (TypeError, ValueError):
        pass

    state_hint = ""
    if retrain_state == "running":
        state_hint = " (running)"
    elif retrain_last_status == "failed":
        state_hint = " (last cycle failed)"

    return {
        "last_cycle": f"{last_end}{state_hint}",
        "reload_status": reload_status,
        "next_expected": next_expected,
    }


def build_action_flags(
    context: dict[str, str],
    db_progress: dict[str, Any],
    retrain_status: dict[str, str],
    failures_today: dict[str, int] | None = None,
    impact: dict[str, Any] | None = None,
) -> list[str]:
    flags: list[str] = []

    health = context.get("health", "").upper()
    if health == "KILL-SWITCH":
        flags.append("Keep live execution disabled until freshness and scoring recover.")

    staleness_hours = parse_float(context.get("staleness"))
    if staleness_hours is not None and staleness_hours >= 72:
        flags.append("Trigger retrain now (model staleness exceeds 72h).")

    predictions_today = db_progress.get("predictions_today")
    if isinstance(predictions_today, int) and predictions_today == 0:
        flags.append("Investigate scoring pipeline: 0 predictions logged in report window.")

    if failures_today and failures_today.get("failures", 0) > 0:
        flags.append(
            f"Resolve scoring errors ({failures_today.get('failures', 0)} failures, "
            f"{failures_today.get('timeouts', 0)} timeouts today)."
        )

    if impact and impact.get("avg_net") is not None and float(impact["avg_net"]) < 0:
        flags.append("Net expectancy is negative after costs; keep risk limits tight or disable execution.")

    if retrain_status.get("reload_status") == "failed":
        flags.append("Fix /reload failures before next trading session.")

    if not flags:
        flags.append("No critical actions detected.")
    return flags


def check_http(url: str, expect_json_status: bool = False) -> str:
    try:
        with request.urlopen(url, timeout=1.5) as resp:  # noqa: S310 (local URL)
            if resp.status < 200 or resp.status >= 300:
                return f"down (HTTP {resp.status})"
            if expect_json_status:
                raw = resp.read().decode("utf-8", errors="replace")
                try:
                    payload = json.loads(raw)
                    if isinstance(payload, dict) and "status" in payload:
                        return f"up ({payload.get('status')})"
                except json.JSONDecodeError:
                    return "up (invalid json)"
            return "up"
    except Exception:
        return "down"


def parse_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_delta(current: float | int | None, previous: float | int | None, better_if_lower: bool = False) -> str:
    if current is None or previous is None:
        return "--"
    delta = float(current) - float(previous)
    if abs(delta) < 1e-9:
        return "flat"
    up = delta > 0
    arrow = "▲" if up else "▼"
    direction_good = (not up and better_if_lower) or (up and not better_if_lower)
    verdict = "better" if direction_good else "worse"
    return f"{arrow} {delta:+.1f} ({verdict})"


def extract_report_dir(path: Path) -> Path:
    return path.parent


def find_previous_report(report_path: Path, current_date: str) -> Path | None:
    report_dir = extract_report_dir(report_path)
    candidates = sorted(report_dir.glob("ml_daily_*.md"))
    prev: Path | None = None
    for candidate in candidates:
        stem = candidate.stem  # ml_daily_YYYY-MM-DD
        parts = stem.split("_")
        if len(parts) < 3:
            continue
        day = parts[-1]
        if day < current_date:
            prev = candidate
    return prev


def score_failure_keywords(line: str) -> tuple[bool, bool]:
    lowered = line.lower()
    failure = "collector scoring failed" in lowered or "score request failed" in lowered
    timeout = "timed out" in lowered or "timeout" in lowered
    return failure, timeout


def count_score_failures_by_day(log_files: list[Path], report_day: date) -> dict[str, int]:
    target_prefix = report_day.strftime("%Y-%m-%d")
    stats = {"failures": 0, "timeouts": 0}
    watch = {"live_collector.log", "retrain.log"}
    for path in log_files:
        if path.name not in watch or not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8", errors="replace") as fh:
                for raw in fh:
                    if not raw.startswith(target_prefix):
                        continue
                    failure, timeout = score_failure_keywords(raw)
                    if failure:
                        stats["failures"] += 1
                    if timeout:
                        stats["timeouts"] += 1
        except OSError:
            continue
    return stats


def compute_impact_stats(db_path: str, report_day: date, include_preview: bool = False) -> dict[str, Any]:
    spread = float(os.getenv("ML_COST_SPREAD_BPS", "0.8"))
    slippage = float(os.getenv("ML_COST_SLIPPAGE_BPS", "0.4"))
    commission = float(os.getenv("ML_COST_COMMISSION_BPS", "0.1"))
    total_cost = spread + slippage + commission

    result: dict[str, Any] = {
        "cost_model": {"spread": spread, "slippage": slippage, "commission": commission, "total": total_cost},
        "signals": 0,
        "avg_gross": None,
        "avg_net": None,
        "win_rate_net": None,
        "by_horizon": {},
    }

    conn = connect_db_if_exists(db_path)
    if conn is None:
        result["error"] = f"DB not found: {db_path}"
        return result

    try:
        if not all(table_exists(conn, t) for t in ("prediction_log", "event_labels", "touch_events")):
            result["error"] = "Required tables missing for impact stats."
            return result

        pred_cols = {r[1] for r in conn.execute("PRAGMA table_info(prediction_log)").fetchall()}
        has_preview = "is_preview" in pred_cols
        preview_filter = ""
        if has_preview and not include_preview:
            preview_filter = "AND COALESCE(lp.is_preview, 0) = 0"

        start_ms, end_ms = et_day_bounds_ms(report_day)
        rows = conn.execute(
            f"""
            WITH latest_pred AS (
                SELECT *
                FROM (
                    SELECT pl.*,
                           ROW_NUMBER() OVER (
                             PARTITION BY pl.event_id
                             ORDER BY pl.ts_prediction DESC
                           ) AS rn
                    FROM prediction_log pl
                )
                WHERE rn = 1
            )
            SELECT
                te.ts_event,
                el.horizon_min,
                el.return_bps,
                lp.signal_5m,
                lp.signal_15m,
                lp.signal_60m
            FROM event_labels el
            JOIN touch_events te ON te.event_id = el.event_id
            JOIN latest_pred lp ON lp.event_id = el.event_id
            WHERE te.ts_event >= ? AND te.ts_event < ?
              {preview_filter}
            """,
            (start_ms, end_ms),
        ).fetchall()

        gross_vals: list[float] = []
        net_vals: list[float] = []
        by_h: dict[int, list[tuple[float, float]]] = {5: [], 15: [], 60: []}

        for row in rows:
            horizon = int(row["horizon_min"])
            if horizon == 5:
                signal = (row["signal_5m"] or "").lower()
            elif horizon == 15:
                signal = (row["signal_15m"] or "").lower()
            elif horizon == 60:
                signal = (row["signal_60m"] or "").lower()
            else:
                continue

            if signal not in {"reject", "break"}:
                continue

            gross = parse_float_or_none(row["return_bps"])
            if gross is None:
                continue
            net = gross - total_cost
            gross_vals.append(gross)
            net_vals.append(net)
            by_h.setdefault(horizon, []).append((gross, net))

        result["signals"] = len(gross_vals)
        if gross_vals:
            result["avg_gross"] = sum(gross_vals) / len(gross_vals)
            result["avg_net"] = sum(net_vals) / len(net_vals)
            wins = sum(1 for n in net_vals if n > 0)
            result["win_rate_net"] = wins / len(net_vals)

        by_h_summary: dict[int, dict[str, Any]] = {}
        for horizon, pairs in by_h.items():
            if not pairs:
                by_h_summary[horizon] = {"n": 0, "avg_net": None}
                continue
            n = len(pairs)
            avg_net = sum(net for _, net in pairs) / n
            by_h_summary[horizon] = {"n": n, "avg_net": avg_net}
        result["by_horizon"] = by_h_summary
    finally:
        conn.close()

    return result


def build_trend_summary(
    context: dict[str, str],
    previous_context: dict[str, str] | None,
    db_current: dict[str, Any],
    db_previous: dict[str, Any],
    fails_current: dict[str, int],
    fails_previous: dict[str, int],
) -> list[str]:
    lines: list[str] = []

    cur_stale = parse_float(context.get("staleness"))
    prev_stale = parse_float(previous_context.get("staleness")) if previous_context else None
    lines.append(f"Staleness: {format_delta(cur_stale, prev_stale, better_if_lower=True)}")

    lines.append(
        "Bars today vs prior: "
        f"{format_delta(parse_float_or_none(db_current.get('bars_today')), parse_float_or_none(db_previous.get('bars_today')))}"
    )
    lines.append(
        "Events today vs prior: "
        f"{format_delta(parse_float_or_none(db_current.get('events_today')), parse_float_or_none(db_previous.get('events_today')))}"
    )
    lines.append(
        "Predictions today vs prior: "
        f"{format_delta(parse_float_or_none(db_current.get('predictions_today')), parse_float_or_none(db_previous.get('predictions_today')))}"
    )
    lines.append(
        "Score failures today vs prior: "
        f"{format_delta(fails_current.get('failures'), fails_previous.get('failures'), better_if_lower=True)}"
    )
    return lines


def build_impact_lines(impact: dict[str, Any]) -> list[str]:
    if impact.get("error"):
        return [f"Impact stats unavailable: {impact['error']}"]

    cost = impact["cost_model"]
    lines = [
        "Cost model (bps): "
        f"spread={float(cost['spread']):.2f}, "
        f"slippage={float(cost['slippage']):.2f}, "
        f"commission={float(cost['commission']):.2f} "
        f"(total={float(cost['total']):.2f})",
        f"Tradeable matured signals: {impact['signals']}",
    ]

    if impact.get("avg_gross") is None:
        lines.append("No matured tradeable returns yet for impact calculation.")
        return lines

    lines.append(f"Avg gross return: {impact['avg_gross']:.2f} bps")
    lines.append(f"Avg net return: {impact['avg_net']:.2f} bps")
    win_rate = impact.get("win_rate_net")
    if win_rate is not None:
        lines.append(f"Net win rate: {win_rate * 100:.1f}%")

    by_horizon = impact.get("by_horizon", {})
    for horizon in (5, 15, 60):
        h = by_horizon.get(horizon, {})
        n = h.get("n", 0)
        avg_net = h.get("avg_net")
        if n == 0 or avg_net is None:
            lines.append(f"{horizon}m: N=0")
        else:
            lines.append(f"{horizon}m: N={n}, avg net={avg_net:.2f} bps")
    return lines

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


def count_score_failures(log_tails: dict[str, str]) -> int:
    total = 0
    for path, body in log_tails.items():
        name = Path(path).name
        if name not in {"live_collector.log", "retrain.log"}:
            continue
        for line in body.splitlines():
            lowered = line.lower()
            if "collector scoring failed" in lowered or "score request failed" in lowered:
                total += 1
    return total


def build_compact_email_body(
    context: dict[str, str],
    market_context: dict[str, str],
    service_snapshot: dict[str, str],
    db_progress: dict[str, Any],
    retrain_status: dict[str, str],
    horizon_snapshots: list[str],
    mfe_mae_summary: list[str],
    health_notes: list[str],
    anomaly_digest: list[str],
    action_flags: list[str],
    score_failures_tail: int,
    score_timeouts_today: int,
    trend_lines: list[str],
    impact_lines: list[str],
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

    lines.append("Market & Session")
    lines.append(f"- Now (ET): {market_context['now_et']}")
    lines.append(f"- Market: {market_context['market_state']}")
    lines.append(f"- Session: {market_context['session']}")
    lines.append("")

    lines.append("Service Snapshot")
    lines.append(f"- Dashboard API :3000: {service_snapshot['dashboard']}")
    lines.append(f"- ML Server :5003: {service_snapshot['ml']}")
    lines.append(f"- Live Collector :5004: {service_snapshot['collector']}")
    lines.append("")

    lines.append("Data Freshness")
    lines.append(f"- Last bar: {format_ts_et(db_progress.get('last_bar_ts'))} (age {format_age_from_ms(db_progress.get('last_bar_ts'))})")
    lines.append(f"- Last event: {format_ts_et(db_progress.get('last_event_ts'))} (age {format_age_from_ms(db_progress.get('last_event_ts'))})")
    lines.append(
        f"- Last prediction: {format_ts_et(db_progress.get('last_prediction_ts'))} "
        f"(age {format_age_from_ms(db_progress.get('last_prediction_ts'))})"
    )
    if db_progress.get("error"):
        lines.append(f"- DB note: {db_progress['error']}")
    lines.append("")

    lines.append("Daily Throughput")
    bars_today = db_progress.get("bars_today")
    events_today = db_progress.get("events_today")
    predictions_today = db_progress.get("predictions_today")
    lines.append(f"- Bars ingested today: {bars_today if bars_today is not None else '--'}")
    lines.append(f"- Events detected today: {events_today if events_today is not None else '--'}")
    lines.append(f"- Predictions logged today: {predictions_today if predictions_today is not None else '--'}")
    live_count = db_progress.get("predictions_live_today")
    preview_count = db_progress.get("predictions_preview_today")
    eligible = events_today if isinstance(events_today, int) else None
    scored_live = live_count if isinstance(live_count, int) else (
        predictions_today if isinstance(predictions_today, int) else None
    )
    unscored = None
    if isinstance(eligible, int) and isinstance(scored_live, int):
        unscored = max(eligible - scored_live, 0)

    if live_count is not None or preview_count is not None:
        lines.append(
            f"- Live vs Preview predictions: "
            f"{live_count if live_count is not None else '--'} / {preview_count if preview_count is not None else '--'}"
        )
    lines.append(f"- Eligible events today: {eligible if eligible is not None else '--'}")
    lines.append(f"- Scored live today: {scored_live if scored_live is not None else '--'}")
    lines.append(f"- Unscored eligible today: {unscored if unscored is not None else '--'}")
    lines.append(f"- Score failures (log tail): {score_failures_tail}")
    lines.append(f"- Score timeouts (today): {score_timeouts_today}")
    lines.append("")

    lines.append("Retrain & Reload")
    lines.append(f"- Last retrain cycle: {retrain_status['last_cycle']}")
    lines.append(f"- Last /reload status: {retrain_status['reload_status']}")
    lines.append(f"- Next expected retrain (~6h): {retrain_status['next_expected']}")
    lines.append("")

    lines.append("MFE/MAE Drift")
    for line in mfe_mae_summary[:3]:
        lines.append(f"- {line}")
    lines.append("")

    lines.append("Trend vs Prior Day")
    for line in trend_lines:
        lines.append(f"- {line}")
    lines.append("")

    lines.append("Impact (Cost-Aware)")
    for line in impact_lines:
        lines.append(f"- {line}")
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

    lines.append("Action Flags")
    for line in action_flags[:3]:
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
    report_day = parse_report_day(context["report_date"])
    db_progress = fetch_db_progress(args.db, report_day)
    db_prev = fetch_db_progress(args.db, report_day - timedelta(days=1))
    market_context = get_market_session_context()
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
    resolved_log_files = resolve_log_files(args.log_file)

    service_snapshot = {
        "dashboard": check_http("http://127.0.0.1:3000/"),
        "ml": check_http("http://127.0.0.1:5003/health", expect_json_status=True),
        "collector": check_http("http://127.0.0.1:5004/health", expect_json_status=True),
    }
    retrain_status = build_retrain_status(fetch_ops_status(args.db), parse_retrain_status(log_tails))
    health_notes = extract_section_bullets(report_text, "## Health Notes")
    horizon_snapshots = parse_horizon_snapshots(report_text)
    mfe_mae_summary = build_mfe_mae_summary(parse_calibration_drift(report_text))
    anomaly_digest = build_anomaly_digest(log_tails, args.anomaly_limit)
    score_failures_tail = count_score_failures(log_tails)
    failures_today = count_score_failures_by_day(resolved_log_files, report_day)
    failures_prev = count_score_failures_by_day(resolved_log_files, report_day - timedelta(days=1))
    previous_report_path = find_previous_report(report_path, context["report_date"])
    previous_context: dict[str, str] | None = None
    if previous_report_path and previous_report_path.exists():
        previous_context = parse_report_context(read_report(previous_report_path), previous_report_path)
    trend_lines = build_trend_summary(context, previous_context, db_progress, db_prev, failures_today, failures_prev)
    impact_stats = compute_impact_stats(args.db, report_day)
    impact_lines = build_impact_lines(impact_stats)
    action_flags = build_action_flags(context, db_progress, retrain_status, failures_today, impact_stats)
    compact_body = build_compact_email_body(
        context=context,
        market_context=market_context,
        service_snapshot=service_snapshot,
        db_progress=db_progress,
        retrain_status=retrain_status,
        horizon_snapshots=horizon_snapshots,
        mfe_mae_summary=mfe_mae_summary,
        health_notes=health_notes,
        anomaly_digest=anomaly_digest,
        action_flags=action_flags,
        score_failures_tail=score_failures_tail,
        score_timeouts_today=failures_today.get("timeouts", 0),
        trend_lines=trend_lines,
        impact_lines=impact_lines,
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
