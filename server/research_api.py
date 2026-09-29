from __future__ import annotations

import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


def require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception as exc:  # pragma: no cover - import guard
        raise SystemExit(f"{module_name} not installed. Install with: {hint}") from exc


DUCKDB = require("duckdb", "python3 -m pip install duckdb")


def _default_duckdb_path() -> Path:
    raw = os.getenv("DUCKDB_PATH")
    if raw:
        return Path(raw).expanduser()
    return ROOT / "data" / "pivot_training.duckdb"


def _default_lineage_path() -> Path:
    raw = os.getenv("RESEARCH_LINEAGE_PATH")
    if raw:
        return Path(raw).expanduser()
    marts_dir = os.getenv("RESEARCH_MARTS_DIR")
    if marts_dir:
        return Path(marts_dir).expanduser() / "last_build.json"
    return ROOT / "data" / "research_marts" / "last_build.json"


DUCKDB_PATH = _default_duckdb_path()
LINEAGE_PATH = _default_lineage_path()
HOST = os.getenv("RESEARCH_API_BIND", "127.0.0.1")
PORT = int(os.getenv("RESEARCH_API_PORT", "5005"))
MAX_STALE_HOURS = max(1.0, float(os.getenv("RESEARCH_MARTS_MAX_AGE_HOURS", "72")))

ALLOWED_DIMENSIONS = {
    "symbol": "eb.symbol",
    "event_date_et": "eb.event_date_et",
    "horizon_min": "el.horizon_min",
    "regime_bucket": "eb.regime_bucket",
    "level_family": "eb.level_family",
    "tod_bucket": "eb.tod_bucket",
    "atr_zone": "eb.atr_zone",
    "confluence_bucket": "eb.confluence_bucket",
}


class SliceFilters(BaseModel):
    regime_bucket: list[str] = Field(default_factory=list)
    level_family: list[str] = Field(default_factory=list)
    tod_bucket: list[str] = Field(default_factory=list)
    atr_zone: list[str] = Field(default_factory=list)
    confluence_bucket: list[str] = Field(default_factory=list)
    require_weekly_confluence: bool | None = None
    require_monthly_confluence: bool | None = None


class SliceQueryRequest(BaseModel):
    symbol: str = "SPY"
    date_from: str | None = None
    date_to: str | None = None
    horizons: list[int] = Field(default_factory=list)
    filters: SliceFilters = Field(default_factory=SliceFilters)
    group_by: list[str] = Field(default_factory=list)
    include_daily_curve: bool = True
    include_distribution: bool = True


class ExpectancyMapRequest(BaseModel):
    symbol: str = "SPY"
    date_from: str | None = None
    date_to: str | None = None
    horizon: int = 15
    axis_x: str = "regime_bucket"
    axis_y: str = "tod_bucket"
    metric: str = "avg_reject_net_bps"
    filters: SliceFilters = Field(default_factory=SliceFilters)


class WalkforwardRequest(BaseModel):
    symbol: str = "SPY"
    horizon: int = 15
    date_from: str | None = None
    date_to: str | None = None
    filters: SliceFilters = Field(default_factory=SliceFilters)
    train_days: int = 180
    test_days: int = 20
    step_days: int = 20
    baseline: str = "slice_expectancy"


class CohortDrilldownRequest(BaseModel):
    symbol: str = "SPY"
    date_from: str | None = None
    date_to: str | None = None
    horizons: list[int] = Field(default_factory=list)
    filters: SliceFilters = Field(default_factory=SliceFilters)
    limit: int = 10
    baseline: str = "reject_net_bps"


class ReplayDayRequest(BaseModel):
    symbol: str = "SPY"
    event_date: str
    primary_horizon: int = 15
    horizons: list[int] = Field(default_factory=lambda: [5, 15, 30, 60])
    limit: int = 10


SliceFilters.model_rebuild()
SliceQueryRequest.model_rebuild()
ExpectancyMapRequest.model_rebuild()
WalkforwardRequest.model_rebuild()
CohortDrilldownRequest.model_rebuild()
ReplayDayRequest.model_rebuild()


app = FastAPI(title="PIVOT_QUANT Research API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:3000", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _connect():
    return DUCKDB.connect(str(DUCKDB_PATH), read_only=True)


def _lineage_payload() -> dict[str, Any]:
    if not LINEAGE_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Research lineage missing at {LINEAGE_PATH}. Run scripts/build_research_marts.py first.",
        )
    payload = json.loads(LINEAGE_PATH.read_text(encoding="utf-8"))
    built_at = payload.get("built_at_utc")
    if built_at:
        # Coarse staleness check without adding heavy datetime parsing dependencies.
        payload["age_hours"] = round((time.time() - LINEAGE_PATH.stat().st_mtime) / 3600.0, 2)
        payload["is_stale"] = payload["age_hours"] > MAX_STALE_HOURS
    else:
        payload["age_hours"] = None
        payload["is_stale"] = False
    return payload


def _ensure_research_ready(*, fail_on_stale: bool = False) -> dict[str, Any]:
    payload = _lineage_payload()
    if fail_on_stale and payload.get("is_stale"):
        raise HTTPException(
            status_code=503,
            detail=(
                f"Research marts are stale ({payload.get('age_hours')}h > {MAX_STALE_HOURS}h). "
                "Rebuild with scripts/build_research_marts.py."
            ),
        )
    return payload


def _placeholders(values: list[Any]) -> str:
    return ", ".join("?" for _ in values)


def _build_where(
    symbol: str,
    date_from: str | None,
    date_to: str | None,
    horizons: list[int],
    filters: SliceFilters,
) -> tuple[str, list[Any]]:
    clauses = ["eb.symbol = ?"]
    params: list[Any] = [symbol.upper()]

    if date_from:
        clauses.append("eb.event_date_et >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        clauses.append("eb.event_date_et <= CAST(? AS DATE)")
        params.append(date_to)
    if horizons:
        clean_horizons = [int(h) for h in horizons if int(h) in {5, 15, 30, 60}]
        if clean_horizons:
            clauses.append(f"el.horizon_min IN ({_placeholders(clean_horizons)})")
            params.extend(clean_horizons)

    for field_name in ("regime_bucket", "level_family", "tod_bucket", "atr_zone", "confluence_bucket"):
        values = getattr(filters, field_name, None) or []
        clean_values = [str(value) for value in values if str(value).strip()]
        if clean_values:
            clauses.append(f"eb.{field_name} IN ({_placeholders(clean_values)})")
            params.extend(clean_values)

    if filters.require_weekly_confluence is True:
        clauses.append("eb.has_weekly_confluence = 1")
    if filters.require_monthly_confluence is True:
        clauses.append("eb.has_monthly_confluence = 1")

    return " AND ".join(clauses), params


def _rows_to_dicts(cursor) -> list[dict[str, Any]]:
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def _sanitize_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, float):
            out[key] = round(value, 6)
        elif hasattr(value, "isoformat"):
            out[key] = value.isoformat()
        elif value is None:
            out[key] = None
        else:
            out[key] = value
    return out


def _fetch_daily_reject_slice_rows(con, where_sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cursor = con.execute(
        f"""
        SELECT
            eb.event_date_et,
            COUNT(*) AS rows,
            SUM(el.reject_net_bps) AS utility_sum,
            SUM(CASE WHEN el.reject_net_bps > 0 THEN 1 ELSE 0 END) AS win_rows,
            SUM(el.mfe_bps) AS mfe_sum,
            SUM(el.mae_bps) AS mae_sum
        FROM pq_research.mart_event_base eb
        JOIN pq_research.mart_event_labels el
          ON eb.event_id = el.event_id
        WHERE {where_sql}
        GROUP BY eb.event_date_et
        ORDER BY eb.event_date_et
        """,
        params,
    )
    return _rows_to_dicts(cursor)


def _fetch_trading_calendar_dates(
    con,
    *,
    symbol: str,
    date_from: str | None,
    date_to: str | None,
) -> list[date]:
    clauses = ["symbol = ?"]
    params: list[Any] = [symbol.upper()]
    if date_from:
        clauses.append("event_date_et >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        clauses.append("event_date_et <= CAST(? AS DATE)")
        params.append(date_to)
    cursor = con.execute(
        f"""
        SELECT event_date_et
        FROM pq_research.mart_trading_calendar
        WHERE {' AND '.join(clauses)}
        ORDER BY event_date_et
        """,
        params,
    )
    rows = []
    for row in _rows_to_dicts(cursor):
        event_date = row.get("event_date_et")
        if isinstance(event_date, str):
            event_date = date.fromisoformat(event_date)
        rows.append(event_date)
    return rows


def _calendarize_daily_rows(
    calendar_dates: list[date],
    daily_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    row_by_date: dict[date, dict[str, Any]] = {}
    for row in daily_rows:
        event_date = row.get("event_date_et")
        if isinstance(event_date, str):
            event_date = date.fromisoformat(event_date)
        if isinstance(event_date, date):
            row_by_date[event_date] = {**row, "event_date_et": event_date}

    calendarized_rows: list[dict[str, Any]] = []
    for event_date in calendar_dates:
        row = row_by_date.get(event_date)
        if row is not None:
            calendarized_rows.append(row)
            continue
        calendarized_rows.append(
            {
                "event_date_et": event_date,
                "rows": 0,
                "utility_sum": 0.0,
                "win_rows": 0,
                "mfe_sum": 0.0,
                "mae_sum": 0.0,
            }
        )
    return calendarized_rows


def _window_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "days": 0,
            "rows": 0,
            "avg_reject_net_bps": None,
            "win_rate_reject": None,
            "avg_mfe_bps": None,
            "avg_mae_bps": None,
            "mean_daily_reject_net_bps": None,
            "positive_days": 0,
            "best_day_reject_net_bps": None,
            "worst_day_reject_net_bps": None,
        }

    total_rows = int(sum(int(row.get("rows") or 0) for row in rows))
    utility_sum = float(sum(float(row.get("utility_sum") or 0.0) for row in rows))
    win_rows = float(sum(float(row.get("win_rows") or 0.0) for row in rows))
    mfe_sum = float(sum(float(row.get("mfe_sum") or 0.0) for row in rows))
    mae_sum = float(sum(float(row.get("mae_sum") or 0.0) for row in rows))
    daily_avgs = [
        (float(row.get("utility_sum") or 0.0) / float(row.get("rows") or 1.0))
        for row in rows
        if float(row.get("rows") or 0.0) > 0
    ]

    return {
        "days": len(rows),
        "rows": total_rows,
        "avg_reject_net_bps": (utility_sum / total_rows) if total_rows else None,
        "win_rate_reject": (win_rows / total_rows) if total_rows else None,
        "avg_mfe_bps": (mfe_sum / total_rows) if total_rows else None,
        "avg_mae_bps": (mae_sum / total_rows) if total_rows else None,
        "mean_daily_reject_net_bps": (sum(daily_avgs) / len(daily_avgs)) if daily_avgs else None,
        "positive_days": sum(1 for value in daily_avgs if value > 0),
        "best_day_reject_net_bps": max(daily_avgs) if daily_avgs else None,
        "worst_day_reject_net_bps": min(daily_avgs) if daily_avgs else None,
    }


def _cohort_day_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    day_rows: list[dict[str, Any]] = []
    for row in rows:
        row_count = int(row.get("rows") or 0)
        utility_sum = float(row.get("utility_sum") or 0.0)
        win_rows = float(row.get("win_rows") or 0.0)
        mfe_sum = float(row.get("mfe_sum") or 0.0)
        mae_sum = float(row.get("mae_sum") or 0.0)
        avg_reject = (utility_sum / row_count) if row_count else None
        day_rows.append(
            {
                "event_date_et": row.get("event_date_et"),
                "rows": row_count,
                "avg_reject_net_bps": avg_reject,
                "win_rate_reject": (win_rows / row_count) if row_count else None,
                "avg_mfe_bps": (mfe_sum / row_count) if row_count else None,
                "avg_mae_bps": (mae_sum / row_count) if row_count else None,
            }
        )
    return [_sanitize_row(row) for row in day_rows]


@app.get("/health")
def health() -> dict[str, Any]:
    lineage = _ensure_research_ready(fail_on_stale=False)
    return {
        "status": "ok" if not lineage.get("is_stale") else "stale",
        "active_db": lineage.get("source_db"),
        "active_duckdb": lineage.get("duckdb_path"),
        "marts_built_at": lineage.get("built_at_utc"),
        "mart_row_counts": lineage.get("mart_counts", {}),
        "lineage": lineage,
    }


@app.get("/metadata")
def metadata() -> dict[str, Any]:
    lineage = _ensure_research_ready(fail_on_stale=False)
    con = _connect()
    try:
        date_row = con.execute(
            """
            SELECT
                MIN(event_date_et) AS min_date,
                MAX(event_date_et) AS max_date
            FROM pq_research.mart_event_base
            """
        ).fetchone()
        def distinct_values(column: str) -> list[str]:
            rows = con.execute(
                f"SELECT DISTINCT {column} FROM pq_research.mart_event_base WHERE {column} IS NOT NULL ORDER BY {column}"
            ).fetchall()
            return [str(row[0]) for row in rows]

        symbols = distinct_values("symbol")
        horizons = [int(row[0]) for row in con.execute(
            "SELECT DISTINCT horizon_min FROM pq_research.mart_event_labels ORDER BY horizon_min"
        ).fetchall()]
        return {
            "symbols": symbols,
            "horizons": horizons,
            "date_range": {
                "min_date": str(date_row[0]) if date_row and date_row[0] is not None else None,
                "max_date": str(date_row[1]) if date_row and date_row[1] is not None else None,
            },
            "dimensions": {
                "regime_bucket": distinct_values("regime_bucket"),
                "level_family": distinct_values("level_family"),
                "tod_bucket": distinct_values("tod_bucket"),
                "atr_zone": distinct_values("atr_zone"),
                "confluence_bucket": distinct_values("confluence_bucket"),
            },
            "lineage": {
                "source_db": lineage.get("source_db"),
                "duckdb_path": lineage.get("duckdb_path"),
                "marts_built_at": lineage.get("built_at_utc"),
            },
        }
    finally:
        con.close()


@app.post("/slice-query")
def slice_query(request: SliceQueryRequest) -> dict[str, Any]:
    lineage = _ensure_research_ready(fail_on_stale=False)
    where_sql, params = _build_where(
        symbol=request.symbol,
        date_from=request.date_from,
        date_to=request.date_to,
        horizons=request.horizons,
        filters=request.filters,
    )

    group_by = [field for field in request.group_by if field in ALLOWED_DIMENSIONS]
    con = _connect()
    try:
        base_from = """
            FROM pq_research.mart_event_base eb
            JOIN pq_research.mart_event_labels el
              ON eb.event_id = el.event_id
        """
        summary_cursor = con.execute(
            f"""
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT eb.event_date_et) AS days,
                AVG(el.reject_net_bps) AS avg_reject_net_bps,
                AVG(CASE WHEN el.reject_net_bps > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_reject,
                AVG(el.mfe_bps) AS avg_mfe_bps,
                AVG(el.mae_bps) AS avg_mae_bps
            {base_from}
            WHERE {where_sql}
            """,
            params,
        )
        summary_rows = _rows_to_dicts(summary_cursor)
        summary = _sanitize_row(summary_rows[0] if summary_rows else {})

        groups: list[dict[str, Any]] = []
        if group_by:
            select_dims = ", ".join(f"{ALLOWED_DIMENSIONS[field]} AS {field}" for field in group_by)
            group_dims = ", ".join(ALLOWED_DIMENSIONS[field] for field in group_by)
            group_cursor = con.execute(
                f"""
                SELECT
                    {select_dims},
                    COUNT(*) AS rows,
                    AVG(el.reject_net_bps) AS avg_reject_net_bps,
                    AVG(CASE WHEN el.reject_net_bps > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_reject,
                    AVG(el.mfe_bps) AS avg_mfe_bps,
                    AVG(el.mae_bps) AS avg_mae_bps
                {base_from}
                WHERE {where_sql}
                GROUP BY {group_dims}
                ORDER BY rows DESC
                LIMIT 250
                """,
                params,
            )
            groups = [_sanitize_row(row) for row in _rows_to_dicts(group_cursor)]

        daily_curve: list[dict[str, Any]] = []
        if request.include_daily_curve:
            daily_cursor = con.execute(
                f"""
                SELECT
                    eb.event_date_et,
                    COUNT(*) AS rows,
                    AVG(el.reject_net_bps) AS avg_reject_net_bps,
                    AVG(CASE WHEN el.reject_net_bps > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_reject
                {base_from}
                WHERE {where_sql}
                GROUP BY eb.event_date_et
                ORDER BY eb.event_date_et
                """,
                params,
            )
            daily_curve = [_sanitize_row(row) for row in _rows_to_dicts(daily_cursor)]

        distribution: dict[str, Any] | None = None
        if request.include_distribution:
            dist_cursor = con.execute(
                f"""
                SELECT
                    QUANTILE_CONT(el.reject_net_bps, 0.05) AS p05,
                    QUANTILE_CONT(el.reject_net_bps, 0.50) AS p50,
                    QUANTILE_CONT(el.reject_net_bps, 0.95) AS p95
                {base_from}
                WHERE {where_sql}
                """,
                params,
            )
            dist_rows = _rows_to_dicts(dist_cursor)
            distribution = _sanitize_row(dist_rows[0] if dist_rows else {})

        return {
            "summary": summary,
            "groups": groups,
            "daily_curve": daily_curve,
            "distribution": distribution,
            "lineage": {
                "source_db": lineage.get("source_db"),
                "duckdb_path": lineage.get("duckdb_path"),
                "marts_built_at": lineage.get("built_at_utc"),
            },
        }
    finally:
        con.close()


@app.post("/expectancy-map")
def expectancy_map(request: ExpectancyMapRequest) -> dict[str, Any]:
    lineage = _ensure_research_ready(fail_on_stale=False)
    if request.axis_x not in ALLOWED_DIMENSIONS or request.axis_y not in ALLOWED_DIMENSIONS:
        raise HTTPException(status_code=400, detail="Unsupported axis selection")
    metric_map = {
        "avg_reject_net_bps": "AVG(el.reject_net_bps)",
        "win_rate_reject": "AVG(CASE WHEN el.reject_net_bps > 0 THEN 1.0 ELSE 0.0 END)",
        "avg_break_net_bps": "AVG(el.break_net_bps)",
        "win_rate_break": "AVG(CASE WHEN el.break_net_bps > 0 THEN 1.0 ELSE 0.0 END)",
        "avg_mfe_bps": "AVG(el.mfe_bps)",
        "avg_mae_bps": "AVG(el.mae_bps)",
    }
    metric_sql = metric_map.get(request.metric)
    if metric_sql is None:
        raise HTTPException(status_code=400, detail="Unsupported metric")

    filters = request.filters
    where_sql, params = _build_where(
        symbol=request.symbol,
        date_from=request.date_from,
        date_to=request.date_to,
        horizons=[request.horizon],
        filters=filters,
    )
    con = _connect()
    try:
        cursor = con.execute(
            f"""
            SELECT
                {ALLOWED_DIMENSIONS[request.axis_x]} AS axis_x,
                {ALLOWED_DIMENSIONS[request.axis_y]} AS axis_y,
                COUNT(*) AS rows,
                {metric_sql} AS metric_value
            FROM pq_research.mart_event_base eb
            JOIN pq_research.mart_event_labels el
              ON eb.event_id = el.event_id
            WHERE {where_sql}
            GROUP BY 1, 2
            ORDER BY 1, 2
            """,
            params,
        )
        cells = [_sanitize_row(row) for row in _rows_to_dicts(cursor)]
        return {
            "cells": cells,
            "metric": request.metric,
            "lineage": {
                "source_db": lineage.get("source_db"),
                "duckdb_path": lineage.get("duckdb_path"),
                "marts_built_at": lineage.get("built_at_utc"),
            },
        }
    finally:
        con.close()


@app.post("/walkforward")
def walkforward(request: WalkforwardRequest) -> dict[str, Any]:
    lineage = _ensure_research_ready(fail_on_stale=False)
    if request.baseline != "slice_expectancy":
        raise HTTPException(status_code=400, detail="Unsupported baseline")
    if request.horizon not in {5, 15, 30, 60}:
        raise HTTPException(status_code=400, detail="Unsupported horizon")
    if request.train_days < 2 or request.test_days < 1 or request.step_days < 1:
        raise HTTPException(status_code=400, detail="Invalid walkforward window configuration")

    where_sql, params = _build_where(
        symbol=request.symbol,
        date_from=request.date_from,
        date_to=request.date_to,
        horizons=[request.horizon],
        filters=request.filters,
    )

    con = _connect()
    try:
        calendar_dates = _fetch_trading_calendar_dates(
            con,
            symbol=request.symbol,
            date_from=request.date_from,
            date_to=request.date_to,
        )
        daily_rows = _fetch_daily_reject_slice_rows(con, where_sql, params)
    finally:
        con.close()

    if not calendar_dates:
        return {
            "windows": [],
            "aggregate": {
                "windows": 0,
                "windows_with_test_rows": 0,
                "windows_without_test_rows": 0,
                "calendar_days": 0,
                "mean_test_avg_reject_net_bps": None,
                "median_test_avg_reject_net_bps": None,
                "positive_windows": 0,
                "positive_window_rate": None,
                "mean_test_win_rate_reject": None,
                "mean_train_avg_reject_net_bps": None,
                "total_test_rows": 0,
                "best_window": None,
                "worst_window": None,
            },
            "lineage": {
                "source_db": lineage.get("source_db"),
                "duckdb_path": lineage.get("duckdb_path"),
                "marts_built_at": lineage.get("built_at_utc"),
            },
        }

    normalized_rows = _calendarize_daily_rows(calendar_dates, daily_rows)

    windows: list[dict[str, Any]] = []
    total_days = len(normalized_rows)
    cursor = 0
    while cursor + request.train_days + request.test_days <= total_days:
        train_rows = normalized_rows[cursor : cursor + request.train_days]
        test_rows = normalized_rows[cursor + request.train_days : cursor + request.train_days + request.test_days]
        train_stats = _window_summary(train_rows)
        test_stats = _window_summary(test_rows)
        window_payload = {
            "train_start": train_rows[0]["event_date_et"].isoformat(),
            "train_end": train_rows[-1]["event_date_et"].isoformat(),
            "test_start": test_rows[0]["event_date_et"].isoformat(),
            "test_end": test_rows[-1]["event_date_et"].isoformat(),
            "train_days": train_stats["days"],
            "test_days": test_stats["days"],
            "rows_train": train_stats["rows"],
            "rows_test": test_stats["rows"],
            "train_avg_reject_net_bps": train_stats["avg_reject_net_bps"],
            "test_avg_reject_net_bps": test_stats["avg_reject_net_bps"],
            "test_win_rate_reject": test_stats["win_rate_reject"],
            "test_avg_mfe_bps": test_stats["avg_mfe_bps"],
            "test_avg_mae_bps": test_stats["avg_mae_bps"],
            "test_positive_days": test_stats["positive_days"],
            "test_mean_daily_reject_net_bps": test_stats["mean_daily_reject_net_bps"],
            "test_best_day_reject_net_bps": test_stats["best_day_reject_net_bps"],
            "test_worst_day_reject_net_bps": test_stats["worst_day_reject_net_bps"],
        }
        windows.append(_sanitize_row(window_payload))
        cursor += request.step_days

    windows_with_test_rows = sum(1 for window in windows if int(window.get("rows_test") or 0) > 0)
    test_avgs = [
        float(window["test_avg_reject_net_bps"])
        for window in windows
        if window.get("test_avg_reject_net_bps") is not None
    ]
    train_avgs = [
        float(window["train_avg_reject_net_bps"])
        for window in windows
        if window.get("train_avg_reject_net_bps") is not None
    ]
    test_wins = [
        float(window["test_win_rate_reject"])
        for window in windows
        if window.get("test_win_rate_reject") is not None
    ]
    positive_windows = sum(1 for value in test_avgs if value > 0)
    sorted_test_avgs = sorted(test_avgs)
    median_test_avg = None
    if sorted_test_avgs:
        mid = len(sorted_test_avgs) // 2
        if len(sorted_test_avgs) % 2:
            median_test_avg = sorted_test_avgs[mid]
        else:
            median_test_avg = (sorted_test_avgs[mid - 1] + sorted_test_avgs[mid]) / 2.0

    ranked_windows = [window for window in windows if window.get("test_avg_reject_net_bps") is not None]
    best_window = (
        max(ranked_windows, key=lambda row: float(row.get("test_avg_reject_net_bps") or float("-inf")), default=None)
        if ranked_windows
        else None
    )
    worst_window = (
        min(ranked_windows, key=lambda row: float(row.get("test_avg_reject_net_bps") or float("inf")), default=None)
        if ranked_windows
        else None
    )

    aggregate = _sanitize_row(
        {
            "windows": len(windows),
            "windows_with_test_rows": windows_with_test_rows,
            "windows_without_test_rows": len(windows) - windows_with_test_rows,
            "calendar_days": len(calendar_dates),
            "mean_test_avg_reject_net_bps": (sum(test_avgs) / len(test_avgs)) if test_avgs else None,
            "median_test_avg_reject_net_bps": median_test_avg,
            "positive_windows": positive_windows,
            "positive_window_rate": (positive_windows / len(test_avgs)) if test_avgs else None,
            "mean_test_win_rate_reject": (sum(test_wins) / len(test_wins)) if test_wins else None,
            "mean_train_avg_reject_net_bps": (sum(train_avgs) / len(train_avgs)) if train_avgs else None,
            "total_test_rows": int(sum(int(window.get("rows_test") or 0) for window in windows)),
            "best_window": best_window,
            "worst_window": worst_window,
        }
    )

    return {
        "windows": windows,
        "aggregate": aggregate,
        "lineage": {
            "source_db": lineage.get("source_db"),
            "duckdb_path": lineage.get("duckdb_path"),
            "marts_built_at": lineage.get("built_at_utc"),
        },
    }


@app.post("/cohort-drilldown")
def cohort_drilldown(request: CohortDrilldownRequest) -> dict[str, Any]:
    lineage = _ensure_research_ready(fail_on_stale=False)
    if request.baseline != "reject_net_bps":
        raise HTTPException(status_code=400, detail="Unsupported drilldown baseline")
    limit = max(1, min(50, int(request.limit or 10)))

    where_sql, params = _build_where(
        symbol=request.symbol,
        date_from=request.date_from,
        date_to=request.date_to,
        horizons=request.horizons,
        filters=request.filters,
    )

    con = _connect()
    try:
        daily_rows = _fetch_daily_reject_slice_rows(con, where_sql, params)
    finally:
        con.close()

    cohort_rows = _cohort_day_rows(daily_rows)
    best_days = sorted(
        cohort_rows,
        key=lambda row: float(row.get("avg_reject_net_bps") if row.get("avg_reject_net_bps") is not None else float("-inf")),
        reverse=True,
    )[:limit]
    worst_days = sorted(
        cohort_rows,
        key=lambda row: float(row.get("avg_reject_net_bps") if row.get("avg_reject_net_bps") is not None else float("inf")),
    )[:limit]
    recent_days = sorted(
        cohort_rows,
        key=lambda row: row.get("event_date_et") or "",
        reverse=True,
    )[:limit]

    stability = _sanitize_row(
        {
            "days": len(cohort_rows),
            "positive_days": sum(1 for row in cohort_rows if float(row.get("avg_reject_net_bps") or 0.0) > 0),
            "negative_days": sum(1 for row in cohort_rows if float(row.get("avg_reject_net_bps") or 0.0) < 0),
            "best_day_reject_net_bps": best_days[0]["avg_reject_net_bps"] if best_days else None,
            "worst_day_reject_net_bps": worst_days[0]["avg_reject_net_bps"] if worst_days else None,
        }
    )

    return {
        "best_days": best_days,
        "worst_days": worst_days,
        "recent_days": recent_days,
        "stability": stability,
        "lineage": {
            "source_db": lineage.get("source_db"),
            "duckdb_path": lineage.get("duckdb_path"),
            "marts_built_at": lineage.get("built_at_utc"),
        },
    }


@app.post("/replay-day")
def replay_day(request: ReplayDayRequest) -> dict[str, Any]:
    lineage = _ensure_research_ready(fail_on_stale=False)
    if request.primary_horizon not in {5, 15, 30, 60}:
        raise HTTPException(status_code=400, detail="Unsupported primary horizon")
    clean_horizons = [int(h) for h in request.horizons if int(h) in {5, 15, 30, 60}]
    if not clean_horizons:
        clean_horizons = [request.primary_horizon]
    limit = max(1, min(25, int(request.limit or 10)))

    con = _connect()
    try:
        base_params = [request.symbol.upper(), request.event_date]
        summary_cursor = con.execute(
            """
            SELECT
                COUNT(*) AS rows,
                AVG(el.reject_net_bps) AS avg_reject_net_bps,
                AVG(CASE WHEN el.reject_net_bps > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_reject,
                AVG(el.mfe_bps) AS avg_mfe_bps,
                AVG(el.mae_bps) AS avg_mae_bps
            FROM pq_research.mart_event_base eb
            JOIN pq_research.mart_event_labels el
              ON eb.event_id = el.event_id
            WHERE eb.symbol = ?
              AND eb.event_date_et = CAST(? AS DATE)
              AND el.horizon_min = ?
            """,
            [*base_params, request.primary_horizon],
        )
        summary = _sanitize_row((_rows_to_dicts(summary_cursor) or [{}])[0])

        by_horizon_cursor = con.execute(
            f"""
            SELECT
                el.horizon_min,
                COUNT(*) AS rows,
                AVG(el.reject_net_bps) AS avg_reject_net_bps,
                AVG(CASE WHEN el.reject_net_bps > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_reject,
                AVG(el.mfe_bps) AS avg_mfe_bps,
                AVG(el.mae_bps) AS avg_mae_bps
            FROM pq_research.mart_event_base eb
            JOIN pq_research.mart_event_labels el
              ON eb.event_id = el.event_id
            WHERE eb.symbol = ?
              AND eb.event_date_et = CAST(? AS DATE)
              AND el.horizon_min IN ({_placeholders(clean_horizons)})
            GROUP BY el.horizon_min
            ORDER BY el.horizon_min
            """,
            [*base_params, *clean_horizons],
        )
        by_horizon = [_sanitize_row(row) for row in _rows_to_dicts(by_horizon_cursor)]

        by_tod_cursor = con.execute(
            """
            SELECT
                eb.tod_bucket,
                COUNT(*) AS rows,
                AVG(el.reject_net_bps) AS avg_reject_net_bps,
                AVG(CASE WHEN el.reject_net_bps > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_reject
            FROM pq_research.mart_event_base eb
            JOIN pq_research.mart_event_labels el
              ON eb.event_id = el.event_id
            WHERE eb.symbol = ?
              AND eb.event_date_et = CAST(? AS DATE)
              AND el.horizon_min = ?
            GROUP BY eb.tod_bucket
            ORDER BY rows DESC, eb.tod_bucket
            """,
            [*base_params, request.primary_horizon],
        )
        by_tod = [_sanitize_row(row) for row in _rows_to_dicts(by_tod_cursor)]

        by_level_family_cursor = con.execute(
            """
            SELECT
                eb.level_family,
                COUNT(*) AS rows,
                AVG(el.reject_net_bps) AS avg_reject_net_bps,
                AVG(CASE WHEN el.reject_net_bps > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_reject
            FROM pq_research.mart_event_base eb
            JOIN pq_research.mart_event_labels el
              ON eb.event_id = el.event_id
            WHERE eb.symbol = ?
              AND eb.event_date_et = CAST(? AS DATE)
              AND el.horizon_min = ?
            GROUP BY eb.level_family
            ORDER BY rows DESC, eb.level_family
            """,
            [*base_params, request.primary_horizon],
        )
        by_level_family = [_sanitize_row(row) for row in _rows_to_dicts(by_level_family_cursor)]

        top_events_cursor = con.execute(
            """
            SELECT
                eb.event_id,
                eb.event_ts_et,
                eb.tod_bucket,
                eb.level_family,
                eb.touch_side,
                eb.distance_bps,
                el.reject_net_bps,
                el.mfe_bps,
                el.mae_bps
            FROM pq_research.mart_event_base eb
            JOIN pq_research.mart_event_labels el
              ON eb.event_id = el.event_id
            WHERE eb.symbol = ?
              AND eb.event_date_et = CAST(? AS DATE)
              AND el.horizon_min = ?
            ORDER BY el.reject_net_bps DESC, eb.event_ts_et
            LIMIT ?
            """,
            [*base_params, request.primary_horizon, limit],
        )
        top_events = [_sanitize_row(row) for row in _rows_to_dicts(top_events_cursor)]

        worst_events_cursor = con.execute(
            """
            SELECT
                eb.event_id,
                eb.event_ts_et,
                eb.tod_bucket,
                eb.level_family,
                eb.touch_side,
                eb.distance_bps,
                el.reject_net_bps,
                el.mfe_bps,
                el.mae_bps
            FROM pq_research.mart_event_base eb
            JOIN pq_research.mart_event_labels el
              ON eb.event_id = el.event_id
            WHERE eb.symbol = ?
              AND eb.event_date_et = CAST(? AS DATE)
              AND el.horizon_min = ?
            ORDER BY el.reject_net_bps ASC, eb.event_ts_et
            LIMIT ?
            """,
            [*base_params, request.primary_horizon, limit],
        )
        worst_events = [_sanitize_row(row) for row in _rows_to_dicts(worst_events_cursor)]
    finally:
        con.close()

    return {
        "summary": {
            **summary,
            "event_date": request.event_date,
            "primary_horizon": request.primary_horizon,
        },
        "by_horizon": by_horizon,
        "by_tod": by_tod,
        "by_level_family": by_level_family,
        "top_events": top_events,
        "worst_events": worst_events,
        "lineage": {
            "source_db": lineage.get("source_db"),
            "duckdb_path": lineage.get("duckdb_path"),
            "marts_built_at": lineage.get("built_at_utc"),
        },
    }


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
