#!/usr/bin/env python3
"""Initialize (or migrate) the PivotQuant SQLite database."""

from __future__ import annotations

import os

from migrate_db import LATEST_SCHEMA_VERSION, migrate_db

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")


if __name__ == "__main__":
    summary = migrate_db(DEFAULT_DB, target_version=LATEST_SCHEMA_VERSION, verbose=True)
    print(
        f"Initialized DB at {summary['db']} "
        f"(schema v{summary['to_version']})"
    )
