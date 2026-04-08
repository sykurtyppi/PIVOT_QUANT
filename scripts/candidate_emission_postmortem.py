#!/usr/bin/env python3
"""Decompose a candidate emission/preview cohort into policy and regime pockets.

This is the institutional follow-up after a candidate preview run fails or looks
promising: use the same decomposition every time so we can make consistent
go/no-go decisions instead of relying on one-off terminal snippets.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ml.regime_semantics import favored_side_for_trade_regime

DEFAULT_DB = Path(ROOT / "data" / "pivot_events.sqlite")
DEFAULT_OUT_DIR = ROOT / "logs" / "reports" / "research"
ET = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class PostmortemRow:
    event_id: str
    ts_event: int
    ts_prediction: int
    event_day_et: str
    regime_bucket: str
    selected_policy: str
    runtime_signal: str
    chosen_side: str
    realized_utility: float
    return_bps: float
    model_side_prob: float | None
    threshold: float | None
    analog_side_prob: float | None
    disagreement: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Post-mortem a candidate preview/live cohort.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite DB path.")
    parser.add_argument("--symbol", default="SPY", help="Symbol to analyze.")
    parser.add_argument("--horizon", type=int, default=60, help="Single horizon in minutes.")
    parser.add_argument(
        "--source",
        choices=("live", "preview", "all"),
        default="preview",
        help="Prediction source filter.",
    )
    parser.add_argument(
        "--prediction-basis",
        choices=("first", "latest"),
        default="first",
        help="Which prediction row to use per event.",
    )
    parser.add_argument("--model-version", default="", help="Optional model_version filter.")
    parser.add_argument(
        "--start-date",
        default="",
        help="Optional ET start date in YYYY-MM-DD (inclusive, based on touch-event time).",
    )
    parser.add_argument(
        "--end-date",
        default="",
        help="Optional ET end date in YYYY-MM-DD (exclusive, based on touch-event time).",
    )
    parser.add_argument("--cost-bps", type=float, default=1.3, help="Per-trade cost in bps.")
    parser.add_argument(
        "--max-pred-lag-hours",
        type=float,
        default=6.0,
        help="Maximum prediction lag from event time for live/all rows.",
    )
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory.")
    return parser.parse_args()


def validate_db_path(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {db_path}")
    if not db_path.is_file():
        raise FileNotFoundError(f"SQLite DB path is not a file: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()

    required = {"prediction_log", "touch_events", "event_labels"}
    missing = sorted(required - tables)
    if missing:
        raise RuntimeError(
            f"SQLite DB at {db_path} is missing required tables: {', '.join(missing)}"
        )


def source_filter_sql(source: str) -> str:
    if source == "all":
        return ""
    if source == "live":
        return "AND COALESCE(pl.is_preview, 0) = 0"
    return "AND COALESCE(pl.is_preview, 0) = 1"


def regime_bucket_from_event(regime_type_value: Any, trade_regime_value: Any) -> str:
    trade_regime = str(trade_regime_value or "").strip().lower()
    if trade_regime in {"compression", "expansion", "neutral"}:
        return trade_regime
    try:
        regime_type = int(regime_type_value) if regime_type_value is not None else None
    except (TypeError, ValueError):
        regime_type = None
    if regime_type in (1, 2, 4):
        return "expansion"
    if regime_type == 3:
        return "compression"
    return "neutral"


def chosen_side(bucket: str) -> str:
    return favored_side_for_trade_regime(bucket)


def et_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(ET).strftime("%Y-%m-%d")


def parse_et_date_window(start_date: str, end_date: str) -> tuple[int | None, int | None]:
    def _to_ms(raw: str) -> int | None:
        raw = raw.strip()
        if not raw:
            return None
        dt_local = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=ET)
        return int(dt_local.astimezone(timezone.utc).timestamp() * 1000)

    start_ms = _to_ms(start_date)
    end_ms = _to_ms(end_date)
    if start_ms is not None and end_ms is not None and end_ms <= start_ms:
        raise ValueError("--end-date must be later than --start-date")
    return start_ms, end_ms


def summarize_utils(utils: list[float]) -> dict[str, float | int | None]:
    if not utils:
        return {
            "rows_n": 0,
            "avg_utility": None,
            "total_utility": 0.0,
            "win_rate_pct": None,
        }
    return {
        "rows_n": len(utils),
        "avg_utility": float(statistics.fmean(utils)),
        "total_utility": float(sum(utils)),
        "win_rate_pct": 100.0 * sum(1 for u in utils if u > 0) / len(utils),
    }


def load_rows(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    horizon: int,
    source: str,
    prediction_basis: str,
    model_version: str,
    cost_bps: float,
    max_pred_lag_hours: float,
    start_ms: int | None,
    end_ms: int | None,
) -> tuple[list[PostmortemRow], dict[str, Any]]:
    pred_order = "ASC" if prediction_basis == "first" else "DESC"
    source_sql = source_filter_sql(source)
    model_sql = ""
    params: list[Any] = [symbol]
    if model_version.strip():
        model_sql = "AND COALESCE(pl.model_version, '') = ?"
        params.append(model_version.strip())
    date_sql = ""
    if start_ms is not None:
        date_sql += " AND te.ts_event >= ?"
        params.append(start_ms)
    if end_ms is not None:
        date_sql += " AND te.ts_event < ?"
        params.append(end_ms)
    params.append(horizon)

    signal_col = f"signal_{horizon}m"
    prob_reject_col = f"prob_reject_{horizon}m"
    prob_break_col = f"prob_break_{horizon}m"
    threshold_reject_col = f"threshold_reject_{horizon}m"
    threshold_break_col = f"threshold_break_{horizon}m"
    analog_reject_col = f"analog_best_reject_prob"
    analog_break_col = f"analog_best_break_prob"

    sql = f"""
        WITH scoped_pred AS (
            SELECT *
            FROM (
                SELECT
                    pl.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY pl.event_id
                        ORDER BY pl.ts_prediction {pred_order}
                    ) AS rn
                FROM prediction_log pl
                JOIN touch_events te
                  ON te.event_id = pl.event_id
                WHERE te.symbol = ?
                  {source_sql}
                  {model_sql}
                  {date_sql}
            )
            WHERE rn = 1
        )
        SELECT
            te.event_id,
            te.ts_event,
            te.regime_type,
            sp.ts_prediction,
            sp.trade_regime,
            sp.selected_policy,
            sp.{signal_col} AS runtime_signal,
            sp.{prob_reject_col} AS prob_reject,
            sp.{prob_break_col} AS prob_break,
            sp.{threshold_reject_col} AS threshold_reject,
            sp.{threshold_break_col} AS threshold_break,
            sp.{analog_reject_col} AS analog_reject,
            sp.{analog_break_col} AS analog_break,
            sp.analog_best_disagreement,
            el.return_bps
        FROM scoped_pred sp
        JOIN touch_events te
          ON te.event_id = sp.event_id
        JOIN event_labels el
          ON el.event_id = te.event_id
         AND el.horizon_min = ?
        ORDER BY te.ts_event
    """

    fetched = conn.execute(sql, params).fetchall()
    max_lag_ms = int(max_pred_lag_hours * 3600 * 1000)
    apply_lag_filter = source != "preview"

    meta = {
        "prediction_rows_joined": len(fetched),
        "prediction_basis": prediction_basis,
        "source": source,
        "model_version_filter": model_version or None,
        "start_date_filter_et": et_day(start_ms) if start_ms is not None else None,
        "end_date_filter_et_exclusive": et_day(end_ms) if end_ms is not None else None,
        "max_pred_lag_hours": max_pred_lag_hours,
        "max_pred_lag_applied": apply_lag_filter,
        "dropped_for_lag": 0,
        "dropped_for_abstain": 0,
    }

    rows: list[PostmortemRow] = []
    for fetched_row in fetched:
        (
            event_id,
            ts_event,
            regime_type_value,
            ts_prediction,
            trade_regime,
            selected_policy,
            runtime_signal,
            prob_reject,
            prob_break,
            threshold_reject,
            threshold_break,
            analog_reject,
            analog_break,
            disagreement,
            return_bps,
        ) = fetched_row

        ts_event = int(ts_event)
        ts_prediction = int(ts_prediction)
        lag_ms = ts_prediction - ts_event
        if apply_lag_filter and (lag_ms < 0 or lag_ms > max_lag_ms):
            meta["dropped_for_lag"] += 1
            continue

        bucket = regime_bucket_from_event(regime_type_value, trade_regime)
        side = chosen_side(bucket)
        if side == "abstain":
            meta["dropped_for_abstain"] += 1
            continue

        ret = float(return_bps)
        reject_utility = ret - cost_bps
        break_utility = -ret - cost_bps
        realized = reject_utility if side == "reject" else break_utility

        model_prob = float(prob_reject) if side == "reject" and prob_reject is not None else (
            float(prob_break) if side == "break" and prob_break is not None else None
        )
        threshold = float(threshold_reject) if side == "reject" and threshold_reject is not None else (
            float(threshold_break) if side == "break" and threshold_break is not None else None
        )
        analog_prob = float(analog_reject) if side == "reject" and analog_reject is not None else (
            float(analog_break) if side == "break" and analog_break is not None else None
        )

        rows.append(
            PostmortemRow(
                event_id=str(event_id),
                ts_event=ts_event,
                ts_prediction=ts_prediction,
                event_day_et=et_day(ts_event),
                regime_bucket=bucket,
                selected_policy=str(selected_policy or "").strip().lower() or "unknown",
                runtime_signal=str(runtime_signal or "").strip().lower() or "unknown",
                chosen_side=side,
                realized_utility=realized,
                return_bps=ret,
                model_side_prob=model_prob,
                threshold=threshold,
                analog_side_prob=analog_prob,
                disagreement=float(disagreement) if disagreement is not None else None,
            )
        )

    if rows:
        meta["first_event_day_et"] = rows[0].event_day_et
        meta["last_event_day_et"] = rows[-1].event_day_et
    else:
        meta["first_event_day_et"] = None
        meta["last_event_day_et"] = None
    return rows, meta


def build_group_summary(rows: list[PostmortemRow], key_fn) -> dict[str, dict[str, float | int | None]]:
    grouped: dict[str, list[float]] = {}
    for row in rows:
        key = key_fn(row)
        grouped.setdefault(key, []).append(row.realized_utility)
    return {key: summarize_utils(utils) for key, utils in sorted(grouped.items())}


def build_summary_lines(payload: dict[str, Any]) -> list[str]:
    lines = []
    lines.append(
        f"symbol={payload['symbol']} horizon={payload['horizon']} source={payload['source']} "
        f"prediction_basis={payload['prediction_basis']} model_version={payload['model_version_filter']}"
    )
    lines.append(
        f"coverage={payload['coverage']['first_event_day_et']} -> {payload['coverage']['last_event_day_et']} "
        f"eligible_rows={payload['coverage']['eligible_rows']} "
        f"joined_rows={payload['coverage']['prediction_rows_joined']}"
    )
    if payload["coverage"].get("start_date_filter_et") or payload["coverage"].get("end_date_filter_et_exclusive"):
        lines.append(
            f"filter_window_et={payload['coverage'].get('start_date_filter_et')} -> "
            f"{payload['coverage'].get('end_date_filter_et_exclusive')}"
        )
    lines.append(
        f"lag_applied={payload['coverage']['max_pred_lag_applied']} "
        f"dropped_for_lag={payload['coverage']['dropped_for_lag']} "
        f"dropped_for_abstain={payload['coverage']['dropped_for_abstain']}"
    )
    overall = payload["overall"]
    lines.append(
        f"overall: avg={overall['avg_utility']} total={overall['total_utility']} "
        f"win_rate={overall['win_rate_pct']}"
    )

    for section in ("by_policy", "by_bucket", "by_policy_bucket", "by_runtime_signal"):
        lines.append(f"{section}:")
        groups = payload[section]
        if not groups:
            lines.append("  <none>")
            continue
        for key, summary in groups.items():
            lines.append(
                f"  {key}: rows={summary['rows_n']} avg={summary['avg_utility']} "
                f"total={summary['total_utility']} win_rate={summary['win_rate_pct']}"
            )
    return lines


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    start_ms, end_ms = parse_et_date_window(args.start_date, args.end_date)

    validate_db_path(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        rows, meta = load_rows(
            conn,
            symbol=args.symbol,
            horizon=args.horizon,
            source=args.source,
            prediction_basis=args.prediction_basis,
            model_version=args.model_version,
            cost_bps=args.cost_bps,
            max_pred_lag_hours=args.max_pred_lag_hours,
            start_ms=start_ms,
            end_ms=end_ms,
        )
    finally:
        conn.close()

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "symbol": args.symbol,
        "horizon": args.horizon,
        "source": args.source,
        "prediction_basis": args.prediction_basis,
        "model_version_filter": args.model_version or None,
        "cost_bps": args.cost_bps,
        "coverage": {
            **meta,
            "eligible_rows": len(rows),
        },
        "overall": summarize_utils([row.realized_utility for row in rows]),
        "by_policy": build_group_summary(rows, lambda row: row.selected_policy),
        "by_bucket": build_group_summary(rows, lambda row: row.regime_bucket),
        "by_policy_bucket": build_group_summary(
            rows,
            lambda row: f"{row.selected_policy}|{row.regime_bucket}",
        ),
        "by_runtime_signal": build_group_summary(rows, lambda row: row.runtime_signal),
        "sample_rows": [asdict(row) for row in rows[:25]],
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{args.symbol.lower()}_h{args.horizon}_candidate_emission_postmortem"
    if args.source != "all":
        stem += f"_{args.source}"
    if args.model_version.strip():
        stem += f"_{args.model_version.strip()}"
    if args.start_date or args.end_date:
        start_token = args.start_date or "open"
        end_token = args.end_date or "open"
        stem += f"_{start_token}_to_{end_token}"

    json_path = out_dir / f"{stem}.json"
    txt_path = out_dir / f"{stem}_summary.txt"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    summary_lines = build_summary_lines(payload)
    txt_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(f"wrote {json_path}")
    print(f"wrote {txt_path}")
    print()
    print("\n".join(summary_lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
