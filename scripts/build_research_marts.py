#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

from research_source import load_trading_calendar_rows


def _default_duckdb_path() -> Path:
    raw = os.getenv("DUCKDB_PATH")
    if raw:
        return Path(raw).expanduser()
    return ROOT / "data" / "pivot_training.duckdb"


def _default_marts_dir() -> Path:
    raw = os.getenv("RESEARCH_MARTS_DIR")
    if raw:
        return Path(raw).expanduser()
    return ROOT / "data" / "research_marts"


def _default_cost_models_path() -> Path:
    raw = os.getenv("RESEARCH_COST_MODELS_PATH")
    if raw:
        return Path(raw).expanduser()
    return ROOT / "research" / "marts" / "cost_models.json"


def _default_cost_model_version() -> str:
    return str(os.getenv("RESEARCH_COST_MODEL_VERSION") or "rt_cost_v1").strip() or "rt_cost_v1"


def _default_source_db_path() -> Path:
    raw = os.getenv("PIVOT_DB")
    if raw:
        return Path(raw).expanduser()
    return ROOT / "data" / "pivot_events.sqlite"


DUCKDB_PATH = _default_duckdb_path()
MARTS_DIR = _default_marts_dir()
SCHEMA_PATH = ROOT / "research" / "marts" / "schema.sql"
LINEAGE_PATH = MARTS_DIR / "last_build.json"
COST_MODELS_PATH = _default_cost_models_path()
COST_MODEL_VERSION = _default_cost_model_version()
SOURCE_DB_PATH = _default_source_db_path()
REQUIRED_MART_COLUMNS: dict[str, set[str]] = {
    "mart_event_base": {"event_id", "symbol", "event_date_et"},
    "mart_event_labels": {"event_id", "horizon_min", "reject_net_bps", "break_net_bps"},
    "mart_slice_expectancy_daily": {"symbol", "event_date_et", "horizon_min", "regime_bucket", "rows_n"},
    "mart_slice_expectancy_rollup": {"symbol", "horizon_min", "regime_bucket", "rows_n"},
}


def require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception as exc:  # pragma: no cover - import guard
        raise SystemExit(f"{module_name} not installed. Install with: {hint}") from exc


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _fetchone_dict(con, sql: str) -> dict[str, Any]:
    row = con.execute(sql).fetchone()
    if row is None:
        return {}
    columns = [col[0] for col in con.description]
    return dict(zip(columns, row))


def _assert_training_view_exists(con) -> None:
    exists = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'training_events_v1'
        """
    ).fetchone()[0]
    if not exists:
        raise RuntimeError(
            f"training_events_v1 not found in DuckDB at {DUCKDB_PATH}. "
            "Run scripts/build_duckdb_view.py first."
        )


def _read_schema_sql() -> str:
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Research mart schema not found: {SCHEMA_PATH}")
    return SCHEMA_PATH.read_text(encoding="utf-8")


def _load_cost_models(path: Path) -> dict[str, Any]:
    path = path.expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Research cost model registry not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Research cost model registry must be a JSON object: {path}")
    return payload


def _resolve_cost_model(path: Path, version: str) -> dict[str, Any]:
    registry = _load_cost_models(path)
    payload = registry.get(version)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unknown research cost model version: {version}")

    required = ("spread_bps", "slippage_bps", "commission_bps", "trade_cost_bps")
    missing = [key for key in required if key not in payload]
    if missing:
        raise RuntimeError(f"Research cost model {version} is missing required keys: {', '.join(missing)}")

    try:
        spread = float(payload["spread_bps"])
        slippage = float(payload["slippage_bps"])
        commission = float(payload["commission_bps"])
        trade_cost = float(payload["trade_cost_bps"])
    except Exception as exc:
        raise RuntimeError(f"Research cost model {version} has non-numeric fields") from exc

    if abs((spread + slippage + commission) - trade_cost) > 1e-9:
        raise RuntimeError(
            f"Research cost model {version} has inconsistent totals: "
            f"spread+slippage+commission={spread + slippage + commission} trade_cost_bps={trade_cost}"
        )

    return {
        "version": version,
        "label": str(payload.get("label") or version),
        "spread_bps": spread,
        "slippage_bps": slippage,
        "commission_bps": commission,
        "trade_cost_bps": trade_cost,
    }


def _source_db_metadata(path: Path) -> dict[str, Any]:
    path = path.expanduser()
    exists = path.exists()
    stat = path.stat() if exists else None
    mtime_utc = (
        datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if stat is not None
        else None
    )
    return {
        "source_db": str(path),
        "source_db_exists": exists,
        "source_db_size_bytes": int(stat.st_size) if stat is not None else None,
        "source_db_mtime_utc": mtime_utc,
    }


def _write_build_config(
    con,
    *,
    built_at_utc: str,
    schema_path: Path,
    source_db_path: Path,
    cost_model: dict[str, Any],
) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS pq_research")
    con.execute(
        """
        CREATE OR REPLACE TABLE pq_research.mart_build_config (
            built_at_utc VARCHAR,
            schema_path VARCHAR,
            source_db_path VARCHAR,
            cost_model_version VARCHAR,
            spread_bps DOUBLE,
            slippage_bps DOUBLE,
            commission_bps DOUBLE,
            trade_cost_bps DOUBLE
        )
        """
    )
    con.execute(
        """
        INSERT INTO pq_research.mart_build_config (
            built_at_utc,
            schema_path,
            source_db_path,
            cost_model_version,
            spread_bps,
            slippage_bps,
            commission_bps,
            trade_cost_bps
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            built_at_utc,
            str(schema_path),
            str(source_db_path),
            str(cost_model["version"]),
            float(cost_model["spread_bps"]),
            float(cost_model["slippage_bps"]),
            float(cost_model["commission_bps"]),
            float(cost_model["trade_cost_bps"]),
        ],
    )


def _write_trading_calendar(con, rows: list[dict[str, Any]]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS pq_research")
    con.execute(
        """
        CREATE OR REPLACE TABLE pq_research.mart_trading_calendar (
            symbol VARCHAR,
            event_date_et DATE,
            calendar_bar_interval_sec INTEGER,
            bar_count INTEGER,
            min_ts BIGINT,
            max_ts BIGINT
        )
        """
    )
    if not rows:
        return
    con.executemany(
        """
        INSERT INTO pq_research.mart_trading_calendar (
            symbol,
            event_date_et,
            calendar_bar_interval_sec,
            bar_count,
            min_ts,
            max_ts
        ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?)
        """,
        [
            [
                str(row["symbol"]),
                str(row["event_date_et"]),
                int(row["calendar_bar_interval_sec"]),
                int(row["bar_count"]),
                int(row["min_ts"]),
                int(row["max_ts"]),
            ]
            for row in rows
        ],
    )


def _assert_mart_schema_contract(con) -> None:
    for table_name, required_columns in REQUIRED_MART_COLUMNS.items():
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'pq_research' AND table_name = ?
            """,
            [table_name],
        ).fetchall()
        available = {str(row[0]) for row in rows}
        missing = required_columns - available
        if missing:
            missing_text = ", ".join(sorted(missing))
            raise RuntimeError(f"Missing required columns in pq_research.{table_name}: {missing_text}")


def build_research_marts(
    *,
    duckdb_path: Path = DUCKDB_PATH,
    marts_dir: Path = MARTS_DIR,
    lineage_path: Path = LINEAGE_PATH,
    cost_models_path: Path = COST_MODELS_PATH,
    cost_model_version: str = COST_MODEL_VERSION,
    source_db_path: Path = SOURCE_DB_PATH,
) -> dict[str, Any]:
    duckdb = require("duckdb", "python3 -m pip install duckdb")

    duckdb_path = duckdb_path.expanduser()
    marts_dir = marts_dir.expanduser()
    lineage_path = lineage_path.expanduser()
    cost_models_path = cost_models_path.expanduser()
    source_db_path = source_db_path.expanduser()
    built_at_utc = _utc_now_iso()
    cost_model = _resolve_cost_model(cost_models_path, cost_model_version)
    source_db_info = _source_db_metadata(source_db_path)
    trading_calendar_rows = load_trading_calendar_rows(source_db_path)

    marts_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(duckdb_path))
    try:
        _assert_training_view_exists(con)
        _write_build_config(
            con,
            built_at_utc=built_at_utc,
            schema_path=SCHEMA_PATH,
            source_db_path=source_db_path,
            cost_model=cost_model,
        )
        _write_trading_calendar(con, trading_calendar_rows)
        con.execute(_read_schema_sql())
        _assert_mart_schema_contract(con)

        mart_counts = {
            "mart_trading_calendar": int(con.execute("SELECT COUNT(*) FROM pq_research.mart_trading_calendar").fetchone()[0]),
            "mart_event_base": int(con.execute("SELECT COUNT(*) FROM pq_research.mart_event_base").fetchone()[0]),
            "mart_event_labels": int(con.execute("SELECT COUNT(*) FROM pq_research.mart_event_labels").fetchone()[0]),
            "mart_slice_expectancy_daily": int(
                con.execute("SELECT COUNT(*) FROM pq_research.mart_slice_expectancy_daily").fetchone()[0]
            ),
            "mart_slice_expectancy_rollup": int(
                con.execute("SELECT COUNT(*) FROM pq_research.mart_slice_expectancy_rollup").fetchone()[0]
            ),
        }
        date_range = _fetchone_dict(
            con,
            """
            SELECT
                MIN(event_date_et) AS min_event_date_et,
                MAX(event_date_et) AS max_event_date_et
            FROM pq_research.mart_event_base
            """,
        )
        horizons = [
            int(row[0])
            for row in con.execute(
                "SELECT DISTINCT horizon_min FROM pq_research.mart_event_labels ORDER BY horizon_min"
            ).fetchall()
        ]
        symbols = [
            str(row[0])
            for row in con.execute(
                "SELECT DISTINCT symbol FROM pq_research.mart_event_base ORDER BY symbol"
            ).fetchall()
        ]
        build_config = _fetchone_dict(
            con,
            """
            SELECT
                built_at_utc,
                schema_path,
                source_db_path,
                cost_model_version,
                spread_bps,
                slippage_bps,
                commission_bps,
                trade_cost_bps
            FROM pq_research.mart_build_config
            LIMIT 1
            """,
        )
    finally:
        con.close()

    lineage = {
        "status": "ok",
        "built_at_utc": built_at_utc,
        "source_db": str(source_db_path),
        "duckdb_path": str(duckdb_path),
        "schema_path": str(SCHEMA_PATH),
        "cost_models_path": str(cost_models_path),
        "marts_dir": str(marts_dir),
        "mart_counts": mart_counts,
        "symbols": symbols,
        "horizons": horizons,
        "cost_model": {
            "version": str(build_config.get("cost_model_version") or cost_model["version"]),
            "spread_bps": float(build_config.get("spread_bps"))
            if build_config.get("spread_bps") is not None
            else float(cost_model["spread_bps"]),
            "slippage_bps": float(build_config.get("slippage_bps"))
            if build_config.get("slippage_bps") is not None
            else float(cost_model["slippage_bps"]),
            "commission_bps": float(build_config.get("commission_bps"))
            if build_config.get("commission_bps") is not None
            else float(cost_model["commission_bps"]),
            "trade_cost_bps": float(build_config.get("trade_cost_bps"))
            if build_config.get("trade_cost_bps") is not None
            else float(cost_model["trade_cost_bps"]),
        },
        "source_db_metadata": source_db_info,
        "date_range": {
            "min_event_date_et": str(date_range.get("min_event_date_et")) if date_range.get("min_event_date_et") is not None else None,
            "max_event_date_et": str(date_range.get("max_event_date_et")) if date_range.get("max_event_date_et") is not None else None,
        },
    }

    lineage_path.write_text(json.dumps(lineage, indent=2), encoding="utf-8")
    return lineage


def main() -> None:
    lineage = build_research_marts()
    print(json.dumps(lineage, indent=2))


if __name__ == "__main__":
    main()
