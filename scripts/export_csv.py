#!/usr/bin/env python3
import csv
import os
import sqlite3
from pathlib import Path

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
OUT_DIR = Path(os.getenv("EXPORT_DIR", "data/exports"))


def export_table(conn: sqlite3.Connection, table: str, out_path: Path) -> None:
    cur = conn.execute(f"SELECT * FROM {table}")
    columns = [desc[0] for desc in cur.description]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(cur.fetchall())


def main() -> None:
    conn = sqlite3.connect(DEFAULT_DB)
    export_table(conn, "touch_events", OUT_DIR / "touch_events.csv")
    export_table(conn, "event_labels", OUT_DIR / "event_labels.csv")
    conn.close()
    print(f"Exported CSVs to {OUT_DIR}")


if __name__ == "__main__":
    main()
