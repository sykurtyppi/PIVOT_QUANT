#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

ET_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc
DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
DEFAULT_OUTPUT = "logs/reports/weekend_deep_audit_latest.md"
DEFAULT_PREDICTION_BASIS = (
    os.getenv("ML_WEEKEND_AUDIT_PREDICTION_BASIS", "first") or "first"
).strip().lower()
if DEFAULT_PREDICTION_BASIS not in {"first", "latest"}:
    DEFAULT_PREDICTION_BASIS = "first"
DEFAULT_COST_BPS = float(os.getenv("ML_COST_SPREAD_BPS", "0.8")) + float(
    os.getenv("ML_COST_SLIPPAGE_BPS", "0.4")
) + float(os.getenv("ML_COST_COMMISSION_BPS", "0.1"))


@dataclass(frozen=True)
class SnapshotInfo:
    snapshot_date: date
    with_greeks: int
    with_iv: int
    with_oi: int
    gamma_flip: float | None
    oi_concentration_top5: float | None
    zero_dte_share: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Weekend deep-dive audit for gamma carry impact, regime policy attribution, "
            "and calibration stability."
        )
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path")
    parser.add_argument("--start-date", required=True, help="ET start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="ET end date inclusive (YYYY-MM-DD)")
    parser.add_argument("--symbol", default="SPY", help="Symbol filter (default: SPY)")
    parser.add_argument(
        "--prediction-basis",
        choices=("first", "latest"),
        default=DEFAULT_PREDICTION_BASIS,
        help=(
            "Prediction row basis for attribution rows "
            "(first=execution fidelity, latest=replay view)."
        ),
    )
    parser.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS, help="Roundtrip cost model in bps")
    parser.add_argument(
        "--calibration-lookback-days",
        type=int,
        default=14,
        help="Number of recent report days to summarize from daily_ml_metrics",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Markdown output path")
    return parser.parse_args()


def parse_day(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def et_bounds_ms(start_day: date, end_day: date) -> tuple[int, int]:
    start_dt = datetime.combine(start_day, time.min, tzinfo=ET_TZ)
    end_dt = datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=ET_TZ)
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def mean_or_none(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def std_or_none(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    return statistics.pstdev(values)


def safe_round(value: float | None, digits: int = 3) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def parse_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def prediction_order_sql(prediction_basis: str) -> str:
    basis = str(prediction_basis or "first").strip().lower()
    if basis == "first":
        return "ASC"
    if basis == "latest":
        return "DESC"
    raise ValueError(f"Unsupported prediction_basis={prediction_basis!r}")


def prediction_basis_label(prediction_basis: str) -> str:
    basis = str(prediction_basis or "first").strip().lower()
    if basis == "latest":
        return "latest prediction per event"
    return "first prediction per event"


def load_snapshots(conn: sqlite3.Connection, symbol: str, end_day: date) -> dict[date, SnapshotInfo]:
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                symbol,
                snapshot_date,
                with_greeks,
                with_iv,
                with_oi,
                gamma_flip,
                oi_concentration_top5,
                zero_dte_share,
                ts_collected_ms,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, snapshot_date
                    ORDER BY ts_collected_ms DESC
                ) AS rn
            FROM gamma_snapshots
            WHERE symbol = ?
              AND snapshot_date <= ?
        )
        SELECT
            snapshot_date,
            with_greeks,
            with_iv,
            with_oi,
            gamma_flip,
            oi_concentration_top5,
            zero_dte_share
        FROM ranked
        WHERE rn = 1
        ORDER BY snapshot_date ASC
        """,
        (symbol.upper(), end_day.strftime("%Y-%m-%d")),
    ).fetchall()
    out: dict[date, SnapshotInfo] = {}
    for row in rows:
        d = parse_day(str(row["snapshot_date"]))
        out[d] = SnapshotInfo(
            snapshot_date=d,
            with_greeks=int(row["with_greeks"] or 0),
            with_iv=int(row["with_iv"] or 0),
            with_oi=int(row["with_oi"] or 0),
            gamma_flip=float(row["gamma_flip"]) if row["gamma_flip"] is not None else None,
            oi_concentration_top5=(
                float(row["oi_concentration_top5"]) if row["oi_concentration_top5"] is not None else None
            ),
            zero_dte_share=float(row["zero_dte_share"]) if row["zero_dte_share"] is not None else None,
        )
    return out


def previous_provider_snapshot(
    snapshots_by_day: dict[date, SnapshotInfo],
    d: date,
) -> SnapshotInfo | None:
    prior_days = [k for k in snapshots_by_day.keys() if k <= d]
    prior_days.sort(reverse=True)
    for day_key in prior_days:
        snap = snapshots_by_day[day_key]
        if snap.with_greeks > 0 and snap.gamma_flip is not None:
            return snap
    return None


def near_eq(a: float | None, b: float | None, tol: float = 1e-6) -> bool:
    if a is None or b is None:
        return False
    return abs(a - b) <= tol


def classify_gamma_source(
    event_flip: float | None,
    event_day: date,
    snapshots_by_day: dict[date, SnapshotInfo],
) -> str:
    if event_flip is None:
        return "none"

    same_day = snapshots_by_day.get(event_day)
    prev_provider = previous_provider_snapshot(snapshots_by_day, event_day)

    if (
        same_day is not None
        and same_day.with_greeks > 0
        and same_day.gamma_flip is not None
        and near_eq(event_flip, same_day.gamma_flip)
    ):
        return "fresh_same_day"

    if prev_provider is not None and near_eq(event_flip, prev_provider.gamma_flip):
        if prev_provider.snapshot_date < event_day:
            return "carry_prev_day"
        return "fresh_same_day"

    if same_day is not None and same_day.with_oi > 0 and same_day.with_greeks == 0:
        return "oi_only_context"

    return "other_gamma"


def load_event_rows(
    conn: sqlite3.Connection,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT event_id, symbol, ts_event, gamma_mode, gamma_flip, rv_regime, regime_type
        FROM touch_events
        WHERE symbol = ?
          AND ts_event >= ?
          AND ts_event < ?
        ORDER BY ts_event ASC
        """,
        (symbol.upper(), start_ms, end_ms),
    ).fetchall()
    return [dict(r) for r in rows]


def load_labels_by_event(
    conn: sqlite3.Connection,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            te.event_id,
            te.ts_event,
            el.horizon_min,
            el.reject,
            el.break,
            el.return_bps,
            el.mfe_bps,
            el.mae_bps
        FROM touch_events te
        JOIN event_labels el ON el.event_id = te.event_id
        WHERE te.symbol = ?
          AND te.ts_event >= ?
          AND te.ts_event < ?
        ORDER BY te.ts_event ASC
        """,
        (symbol.upper(), start_ms, end_ms),
    ).fetchall()
    return [dict(r) for r in rows]


def load_selected_predictions(
    conn: sqlite3.Connection,
    symbol: str,
    start_ms: int,
    end_ms: int,
    prediction_basis: str,
) -> list[dict[str, Any]]:
    pred_order = prediction_order_sql(prediction_basis)
    pred_cols = {r["name"] for r in conn.execute("PRAGMA table_info(prediction_log)").fetchall()}
    has_preview = "is_preview" in pred_cols
    preview_filter = "AND COALESCE(lp.is_preview, 0) = 0" if has_preview else ""
    rows = conn.execute(
        f"""
        WITH selected_pred AS (
            SELECT *
            FROM (
                SELECT
                    pl.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY pl.event_id
                        ORDER BY pl.ts_prediction {pred_order}
                    ) AS rn
                FROM prediction_log pl
            )
            WHERE rn = 1
        )
        SELECT
            te.event_id,
            te.ts_event,
            lp.regime_policy_mode,
            lp.trade_regime,
            lp.selected_policy,
            lp.regime_policy_json,
            lp.signal_5m,
            lp.signal_15m,
            lp.signal_30m,
            lp.signal_60m
        FROM selected_pred lp
        JOIN touch_events te ON te.event_id = lp.event_id
        WHERE te.symbol = ?
          AND te.ts_event >= ?
          AND te.ts_event < ?
          {preview_filter}
        ORDER BY te.ts_event ASC
        """,
        (symbol.upper(), start_ms, end_ms),
    ).fetchall()
    return [dict(r) for r in rows]


def load_daily_calibration(
    conn: sqlite3.Connection,
    lookback_days: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM daily_ml_metrics
        WHERE report_date IN (
            SELECT DISTINCT report_date
            FROM daily_ml_metrics
            ORDER BY report_date DESC
            LIMIT ?
        )
        ORDER BY report_date ASC, horizon_min ASC
        """,
        (max(1, lookback_days),),
    ).fetchall()
    return [dict(r) for r in rows]


def build_markdown(
    *,
    symbol: str,
    start_day: date,
    end_day: date,
    cost_bps: float,
    event_rows: list[dict[str, Any]],
    label_rows: list[dict[str, Any]],
    pred_rows: list[dict[str, Any]],
    prediction_basis: str,
    snapshots: dict[date, SnapshotInfo],
    calibration_rows: list[dict[str, Any]],
) -> str:
    event_gamma_class: dict[str, str] = {}
    event_day_map: dict[str, date] = {}
    class_counts: dict[str, int] = defaultdict(int)
    gamma_mode_counts: dict[str, int] = defaultdict(int)

    for event in event_rows:
        event_id = str(event["event_id"])
        dt_et = datetime.fromtimestamp(int(event["ts_event"]) / 1000, tz=timezone.utc).astimezone(ET_TZ)
        event_day = dt_et.date()
        event_day_map[event_id] = event_day
        source_class = classify_gamma_source(
            event_flip=float(event["gamma_flip"]) if event["gamma_flip"] is not None else None,
            event_day=event_day,
            snapshots_by_day=snapshots,
        )
        event_gamma_class[event_id] = source_class
        class_counts[source_class] += 1
        gamma_mode_counts[str(event.get("gamma_mode"))] += 1

    # Label-only impact by gamma source and horizon.
    label_stats: dict[tuple[str, int], dict[str, list[float] | int]] = defaultdict(
        lambda: {"n": 0, "reject": 0, "break": 0, "mfe": [], "mae": [], "ret": []}
    )
    for row in label_rows:
        event_id = str(row["event_id"])
        source_class = event_gamma_class.get(event_id, "none")
        horizon = int(row["horizon_min"])
        key = (source_class, horizon)
        bucket = label_stats[key]
        bucket["n"] = int(bucket["n"]) + 1
        bucket["reject"] = int(bucket["reject"]) + int(row["reject"] or 0)
        bucket["break"] = int(bucket["break"]) + int(row["break"] or 0)
        if isinstance(row["mfe_bps"], (int, float)):
            cast = bucket["mfe"]
            assert isinstance(cast, list)
            cast.append(float(row["mfe_bps"]))
        if isinstance(row["mae_bps"], (int, float)):
            cast = bucket["mae"]
            assert isinstance(cast, list)
            cast.append(float(row["mae_bps"]))
        if isinstance(row["return_bps"], (int, float)):
            cast = bucket["ret"]
            assert isinstance(cast, list)
            cast.append(float(row["return_bps"]))

    # Prediction impact attribution.
    pred_by_event: dict[str, dict[str, Any]] = {str(r["event_id"]): r for r in pred_rows}
    impact_by_source_h: dict[tuple[str, int], dict[str, list[float] | int]] = defaultdict(
        lambda: {"trades": 0, "gross": [], "net": [], "wins": 0}
    )
    impact_by_regime_atr_h: dict[tuple[str, str, int], dict[str, list[float] | int]] = defaultdict(
        lambda: {"trades": 0, "gross": [], "net": [], "wins": 0}
    )
    divergence_by_regime_atr: dict[tuple[str, str], int] = defaultdict(int)
    divergence_by_horizon: dict[int, int] = defaultdict(int)

    for row in label_rows:
        event_id = str(row["event_id"])
        pred = pred_by_event.get(event_id)
        if pred is None:
            continue
        horizon = int(row["horizon_min"])
        signal = pred.get(f"signal_{horizon}m")
        if signal not in ("reject", "break"):
            continue
        ret = row.get("return_bps")
        if not isinstance(ret, (int, float)):
            continue
        gross = float(ret) if signal == "reject" else -float(ret)
        net = gross - cost_bps
        source_class = event_gamma_class.get(event_id, "none")
        sb = impact_by_source_h[(source_class, horizon)]
        sb["trades"] = int(sb["trades"]) + 1
        gross_list = sb["gross"]
        net_list = sb["net"]
        assert isinstance(gross_list, list)
        assert isinstance(net_list, list)
        gross_list.append(gross)
        net_list.append(net)
        if net > 0:
            sb["wins"] = int(sb["wins"]) + 1

        regime = str(pred.get("trade_regime") or "unknown").strip().lower() or "unknown"
        payload = parse_json(pred.get("regime_policy_json"))
        atr_zone = str(payload.get("atr_zone") or "unknown").strip().lower() or "unknown"
        rb = impact_by_regime_atr_h[(regime, atr_zone, horizon)]
        rb["trades"] = int(rb["trades"]) + 1
        rg = rb["gross"]
        rn = rb["net"]
        assert isinstance(rg, list)
        assert isinstance(rn, list)
        rg.append(gross)
        rn.append(net)
        if net > 0:
            rb["wins"] = int(rb["wins"]) + 1

        # Divergence attribution from policy payload.
        diffs = payload.get("signal_diffs")
        if isinstance(diffs, dict):
            key = f"signal_{horizon}m"
            diff_obj = diffs.get(key)
            if isinstance(diff_obj, dict):
                if diff_obj.get("baseline") != diff_obj.get("regime"):
                    divergence_by_regime_atr[(regime, atr_zone)] += 1
                    divergence_by_horizon[horizon] += 1

    # Calibration stability across daily_ml_metrics.
    cal_by_h: dict[int, dict[str, list[float]]] = defaultdict(
        lambda: {"brier_r": [], "brier_b": [], "ece_r": [], "ece_b": [], "avg_ret": []}
    )
    report_dates: set[str] = set()
    for row in calibration_rows:
        h = int(row["horizon_min"])
        report_dates.add(str(row["report_date"]))
        for src_key, dst_key in (
            ("brier_reject", "brier_r"),
            ("brier_break", "brier_b"),
            ("ece_reject", "ece_r"),
            ("ece_break", "ece_b"),
            ("avg_return_bps", "avg_ret"),
        ):
            value = row.get(src_key)
            if isinstance(value, (int, float)):
                cal_by_h[h][dst_key].append(float(value))

    lines: list[str] = []
    lines.append(f"# Weekend Deep Audit ({symbol})")
    lines.append("")
    lines.append(f"- Window (ET): {start_day.isoformat()} -> {end_day.isoformat()}")
    lines.append(f"- Cost model: {cost_bps:.2f} bps")
    lines.append(f"- Events audited: {len(event_rows)}")
    lines.append(f"- Labeled rows audited: {len(label_rows)}")
    lines.append(f"- Prediction basis: {prediction_basis_label(prediction_basis)}")
    lines.append(f"- Predictions audited: {len(pred_rows)}")
    lines.append("")

    lines.append("## Gamma Freshness & Carry")
    lines.append("")
    lines.append("| Source Class | Events | Share % |")
    lines.append("|---|---:|---:|")
    total_events = len(event_rows) or 1
    for source in ("fresh_same_day", "carry_prev_day", "oi_only_context", "other_gamma", "none"):
        c = class_counts.get(source, 0)
        lines.append(f"| {source} | {c} | {c * 100.0 / total_events:.2f} |")
    lines.append("")
    lines.append("Gamma mode distribution:")
    for k in sorted(gamma_mode_counts.keys()):
        lines.append(f"- gamma_mode={k}: {gamma_mode_counts[k]}")
    lines.append("")

    lines.append("### Label Outcomes by Gamma Source × Horizon")
    lines.append("")
    lines.append("| Source | Horizon | N | Reject % | Break % | Avg Return (bps) | Avg MFE | Avg MAE |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for (source, horizon) in sorted(label_stats.keys(), key=lambda x: (x[0], x[1])):
        b = label_stats[(source, horizon)]
        n = int(b["n"])
        reject = int(b["reject"])
        brk = int(b["break"])
        mfe = mean_or_none(b["mfe"]) if isinstance(b["mfe"], list) else None
        mae = mean_or_none(b["mae"]) if isinstance(b["mae"], list) else None
        ret = mean_or_none(b["ret"]) if isinstance(b["ret"], list) else None
        lines.append(
            f"| {source} | {horizon}m | {n} | "
            f"{(reject * 100.0 / n) if n else 0:.2f} | {(brk * 100.0 / n) if n else 0:.2f} | "
            f"{safe_round(ret, 3)} | {safe_round(mfe, 3)} | {safe_round(mae, 3)} |"
        )
    lines.append("")

    lines.append("### Trade Impact by Gamma Source × Horizon")
    lines.append("")
    lines.append("| Source | Horizon | Trades | Avg Gross | Avg Net | Net Win % |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for (source, horizon) in sorted(impact_by_source_h.keys(), key=lambda x: (x[0], x[1])):
        b = impact_by_source_h[(source, horizon)]
        trades = int(b["trades"])
        gross = mean_or_none(b["gross"]) if isinstance(b["gross"], list) else None
        net = mean_or_none(b["net"]) if isinstance(b["net"], list) else None
        wins = int(b["wins"])
        win_rate = (wins * 100.0 / trades) if trades else 0.0
        lines.append(
            f"| {source} | {horizon}m | {trades} | {safe_round(gross, 3)} | {safe_round(net, 3)} | {win_rate:.2f} |"
        )
    lines.append("")

    lines.append("## Regime Policy Attribution")
    lines.append("")
    lines.append("### Trades by Trade Regime × ATR Zone × Horizon")
    lines.append("")
    lines.append("| Regime | ATR Zone | Horizon | Trades | Avg Gross | Avg Net | Net Win % |")
    lines.append("|---|---|---:|---:|---:|---:|---:|")
    for key in sorted(impact_by_regime_atr_h.keys()):
        regime, atr_zone, horizon = key
        b = impact_by_regime_atr_h[key]
        trades = int(b["trades"])
        gross = mean_or_none(b["gross"]) if isinstance(b["gross"], list) else None
        net = mean_or_none(b["net"]) if isinstance(b["net"], list) else None
        wins = int(b["wins"])
        win_rate = (wins * 100.0 / trades) if trades else 0.0
        lines.append(
            f"| {regime} | {atr_zone} | {horizon}m | {trades} | {safe_round(gross, 3)} | {safe_round(net, 3)} | {win_rate:.2f} |"
        )
    lines.append("")

    lines.append("### Shadow Divergences")
    lines.append("")
    total_div = sum(divergence_by_horizon.values())
    lines.append(f"- Total divergence instances (rows): {total_div}")
    for h in (5, 15, 30, 60):
        lines.append(f"- Horizon {h}m divergences: {divergence_by_horizon.get(h, 0)}")
    lines.append("")
    lines.append("| Regime | ATR Zone | Divergences |")
    lines.append("|---|---|---:|")
    for (regime, atr_zone), count in sorted(divergence_by_regime_atr.items()):
        lines.append(f"| {regime} | {atr_zone} | {count} |")
    lines.append("")

    lines.append("## Snapshot Quality")
    lines.append("")
    lines.append("| Snapshot Date | with_greeks | with_iv | with_oi | gamma_flip | oi_concentration_top5 | zero_dte_share |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for d in sorted(k for k in snapshots.keys() if start_day <= k <= end_day):
        s = snapshots[d]
        lines.append(
            f"| {d.isoformat()} | {s.with_greeks} | {s.with_iv} | {s.with_oi} | "
            f"{safe_round(s.gamma_flip, 3)} | {safe_round(s.oi_concentration_top5, 4)} | {safe_round(s.zero_dte_share, 4)} |"
        )
    lines.append("")

    lines.append("## Calibration Stability (daily_ml_metrics)")
    lines.append("")
    lines.append(f"- Report days sampled: {len(report_dates)}")
    lines.append("")
    lines.append("| Horizon | Metric | Mean | StdDev | N |")
    lines.append("|---:|---|---:|---:|---:|")
    for horizon in sorted(cal_by_h.keys()):
        metrics = cal_by_h[horizon]
        for key, label in (
            ("brier_r", "Brier Reject"),
            ("brier_b", "Brier Break"),
            ("ece_r", "ECE Reject"),
            ("ece_b", "ECE Break"),
            ("avg_ret", "Avg Return (bps)"),
        ):
            vals = metrics[key]
            lines.append(
                f"| {horizon}m | {label} | {safe_round(mean_or_none(vals), 4)} | {safe_round(std_or_none(vals), 4)} | {len(vals)} |"
            )
    lines.append("")

    lines.append("## Risk Notes")
    lines.append("")
    if class_counts.get("carry_prev_day", 0) > 0 and class_counts.get("fresh_same_day", 0) == 0:
        lines.append("- High carry dependency: no fresh same-day gamma in audit window.")
    if class_counts.get("none", 0) > 0:
        lines.append("- Some events still lack gamma context entirely.")
    if total_div == 0:
        lines.append("- Shadow policy shows zero divergences in this window; check if policy payload is present on live rows.")
    if not report_dates:
        lines.append("- No daily_ml_metrics rows available for stability analysis.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    start_day = parse_day(args.start_date)
    end_day = parse_day(args.end_date)
    if end_day < start_day:
        raise SystemExit("end-date must be >= start-date")

    start_ms, end_ms = et_bounds_ms(start_day, end_day)
    conn = connect(args.db)
    try:
        snapshots = load_snapshots(conn, args.symbol, end_day=end_day)
        events = load_event_rows(conn, args.symbol, start_ms, end_ms)
        labels = load_labels_by_event(conn, args.symbol, start_ms, end_ms)
        preds = load_selected_predictions(
            conn,
            args.symbol,
            start_ms,
            end_ms,
            args.prediction_basis,
        )
        cal = load_daily_calibration(conn, args.calibration_lookback_days)
    finally:
        conn.close()

    markdown = build_markdown(
        symbol=args.symbol.upper(),
        start_day=start_day,
        end_day=end_day,
        cost_bps=float(args.cost_bps),
        event_rows=events,
        label_rows=labels,
        pred_rows=preds,
        prediction_basis=args.prediction_basis,
        snapshots=snapshots,
        calibration_rows=cal,
    )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(markdown + "\n", encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
