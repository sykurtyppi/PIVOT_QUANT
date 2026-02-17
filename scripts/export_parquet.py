#!/usr/bin/env python3
import os
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
OUT_DIR = Path(os.getenv("EXPORT_DIR", "data/exports"))
PIP_INSTALL = f"{sys.executable} -m pip install"


def require(module_name: str, pip_package: str):
    try:
        return __import__(module_name)
    except Exception:
        print(
            f"{module_name} not installed. Install with: {PIP_INSTALL} {pip_package}",
            file=sys.stderr,
        )
        sys.exit(1)


def export_table(conn: sqlite3.Connection, duckdb_con, table: str, out_path: Path) -> None:
    pd = require("pandas", "pandas")
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb_con.register("tmp_df", df)
    duckdb_con.execute(
        f"COPY tmp_df TO '{out_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')"
    )
    duckdb_con.unregister("tmp_df")


def main() -> None:
    duckdb = require("duckdb", "duckdb")
    conn = sqlite3.connect(DEFAULT_DB)
    con = duckdb.connect()
    try:
        export_table(conn, con, "touch_events", OUT_DIR / "touch_events.parquet")
        export_table(conn, con, "event_labels", OUT_DIR / "event_labels.parquet")
    finally:
        conn.close()
        con.close()
    print(f"Exported parquet to {OUT_DIR}")


if __name__ == "__main__":
    main()
