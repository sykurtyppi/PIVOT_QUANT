"""Read-only Samsung T9 historical-data inventory.

This module intentionally performs metadata discovery only. It never copies,
moves, deletes, or writes to external data paths.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_T9_ROOT = Path("/Volumes/T9")
DEFAULT_MAX_FILES = 200


@dataclass
class InventorySection:
    name: str
    path: str
    exists: bool
    file_count: int
    files_sample: list[str] = field(default_factory=list)
    sample_schema: list[dict[str, str]] = field(default_factory=list)
    row_estimate: int | None = None
    date_range: dict[str, str | None] | None = None
    notes: list[str] = field(default_factory=list)


def resolve_t9_root(root: str | Path | None = None) -> Path:
    raw = root if root is not None else os.getenv("PIVOTQUANT_T9_ROOT")
    return Path(raw).expanduser() if raw else DEFAULT_T9_ROOT


def build_t9_inventory(
    *,
    symbol: str = "SPY",
    root: str | Path | None = None,
    max_files: int = DEFAULT_MAX_FILES,
) -> dict[str, Any]:
    """Build a read-only inventory of likely T9 data sources."""
    normalized_symbol = _normalize_symbol(symbol)
    t9_root = resolve_t9_root(root)
    max_files = max(1, int(max_files))

    report: dict[str, Any] = {
        "t9_root": str(t9_root),
        "root_exists": t9_root.exists(),
        "symbol": normalized_symbol,
        "max_files": max_files,
        "read_only": True,
        "sections": {},
        "warnings": [],
    }
    if not t9_root.exists():
        report["warnings"].append(
            f"T9 root does not exist: {t9_root}. Set PIVOTQUANT_T9_ROOT to the mounted drive path."
        )
        return report

    sections = {
        "daily_ohlcv_parquet": _parquet_section(
            name="SPY daily OHLCV parquet",
            base=t9_root
            / "market_data"
            / "normalized"
            / "underlyings"
            / "daily_ohlcv"
            / f"underlying_symbol={normalized_symbol}",
            patterns=["*.parquet"],
            max_files=max_files,
            date_column="trade_date",
        ),
        "option_chain_parquet": _parquet_section(
            name="SPY option chain parquet",
            base=t9_root
            / "market_data"
            / "normalized"
            / "options"
            / "chains_eod"
            / f"underlying_symbol={normalized_symbol}",
            patterns=["*.parquet"],
            max_files=max_files,
            date_column="trade_date",
        ),
        "option_feature_parquet": _parquet_section(
            name="SPY option feature parquet",
            base=t9_root
            / "market_data"
            / "research"
            / "options_features_eod"
            / f"underlying_symbol={normalized_symbol}",
            patterns=["*.parquet"],
            max_files=max_files,
            date_column="trade_date",
        ),
        "raw_intraday_json": _json_section(
            name="SPY raw intraday JSON candidates",
            base=t9_root / "market_data" / "raw" / "ivolatility" / "intraday_stock_prices",
            symbol=normalized_symbol,
            max_files=max_files,
        ),
        "sqlite_candidates": _sqlite_section(
            name="SQLite candidates",
            root=t9_root,
            symbol=normalized_symbol,
            max_files=max_files,
        ),
    }
    report["sections"] = {key: asdict(section) for key, section in sections.items()}
    return report


def _normalize_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise ValueError("symbol must not be empty")
    if not re.match(r"^[A-Z0-9._-]{1,16}$", normalized):
        raise ValueError(f"unsupported symbol value: {symbol!r}")
    return normalized


def _limited_files(base: Path, patterns: list[str], max_files: int) -> list[Path]:
    if not base.exists():
        return []

    matches: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = sorted(
            d for d in dirnames if d not in {".Spotlight-V100", ".Trashes", ".fseventsd"}
        )
        for filename in sorted(filenames):
            if any(fnmatch.fnmatch(filename, pattern) for pattern in patterns):
                matches.append(Path(dirpath) / filename)
                if len(matches) >= max_files:
                    return matches
    return matches


def _parquet_section(
    *,
    name: str,
    base: Path,
    patterns: list[str],
    max_files: int,
    date_column: str,
) -> InventorySection:
    files = _limited_files(base, patterns, max_files)
    section = InventorySection(
        name=name,
        path=str(base),
        exists=base.exists(),
        file_count=len(files),
        files_sample=[str(path) for path in files[:5]],
    )
    if len(files) >= max_files:
        section.notes.append(f"file listing capped at max_files={max_files}")
    if not base.exists():
        section.notes.append("expected path missing")
        return section
    if not files:
        section.notes.append("no matching parquet files found within scan limit")
        return section

    section.date_range = _date_range_from_paths(files)
    duckdb_module = _try_import_duckdb()
    if duckdb_module is None:
        section.notes.append("duckdb not installed; parquet schema and row estimates skipped")
        return section

    schema, schema_note = _parquet_schema(duckdb_module, files[0])
    section.sample_schema = schema
    if schema_note:
        section.notes.append(schema_note)

    row_estimate, row_note = _parquet_row_estimate(duckdb_module, files)
    section.row_estimate = row_estimate
    if row_note:
        section.notes.append(row_note)

    metadata_range, range_note = _parquet_date_range_from_metadata(
        duckdb_module, files, date_column=date_column
    )
    if metadata_range is not None:
        section.date_range = metadata_range
    if range_note:
        section.notes.append(range_note)
    return section


def _json_section(*, name: str, base: Path, symbol: str, max_files: int) -> InventorySection:
    patterns = [f"*{symbol.lower()}*.json", f"*{symbol.upper()}*.json"]
    files = _limited_files(base, patterns, max_files)
    section = InventorySection(
        name=name,
        path=str(base),
        exists=base.exists(),
        file_count=len(files),
        files_sample=[str(path) for path in files[:5]],
        date_range=_date_range_from_paths(files),
    )
    if len(files) >= max_files:
        section.notes.append(f"file listing capped at max_files={max_files}")
    if not base.exists():
        section.notes.append("expected path missing")
        return section
    if not files:
        section.notes.append("no matching raw intraday JSON files found within scan limit")
        return section

    schema, note = _json_sample_schema(files[0])
    section.sample_schema = schema
    if note:
        section.notes.append(note)
    return section


def _sqlite_section(*, name: str, root: Path, symbol: str, max_files: int) -> InventorySection:
    files = _limited_sqlite_candidates(root, symbol=symbol, max_files=max_files)
    section = InventorySection(
        name=name,
        path=str(root),
        exists=root.exists(),
        file_count=len(files),
        files_sample=[str(path) for path in files[:10]],
    )
    if len(files) >= max_files:
        section.notes.append(f"file listing capped at max_files={max_files}")
    if not files:
        section.notes.append("no SQLite candidates found within scan limit")
        return section

    schema_notes: list[str] = []
    for candidate in files:
        schema, note = _sqlite_sample_schema(candidate)
        if schema:
            section.sample_schema = schema
            section.notes.append(f"sample_schema_source={candidate}")
            break
        if note:
            schema_notes.append(f"{candidate}: {note}")
    if not section.sample_schema and schema_notes:
        section.notes.append("no readable SQLite schema sample found")
        section.notes.extend(schema_notes[:3])
    return section


def _limited_sqlite_candidates(root: Path, *, symbol: str, max_files: int) -> list[Path]:
    matches: list[Path] = []
    symbol_lower = symbol.lower()
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(
            d for d in dirnames if d not in {".Spotlight-V100", ".Trashes", ".fseventsd"}
        )
        for filename in sorted(filenames):
            lowered = filename.lower()
            if not lowered.endswith((".sqlite", ".db")):
                continue
            full_path = Path(dirpath) / filename
            path_text = str(full_path).lower()
            if symbol_lower in path_text or "pivot" in path_text or "earnings" in path_text:
                matches.append(full_path)
                if len(matches) >= max_files:
                    return matches
    return matches


def _try_import_duckdb():
    try:
        import duckdb  # type: ignore

        return duckdb
    except Exception:
        return None


def _sql_string(value: Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _parquet_schema(duckdb_module, path: Path) -> tuple[list[dict[str, str]], str | None]:
    try:
        con = duckdb_module.connect(":memory:")
        try:
            rows = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet({_sql_string(path)})"
            ).fetchall()
        finally:
            con.close()
    except Exception as exc:
        return [], f"could not read parquet schema from sample file: {exc}"
    return [
        {"name": str(row[0]), "type": str(row[1])}
        for row in rows
    ], None


def _parquet_row_estimate(duckdb_module, files: list[Path]) -> tuple[int | None, str | None]:
    total = 0
    inspected = 0
    for path in files:
        try:
            con = duckdb_module.connect(":memory:")
            try:
                value = con.execute(
                    f"""
                    SELECT COALESCE(SUM(row_group_num_rows), 0)
                    FROM (
                        SELECT DISTINCT file_name, row_group_id, row_group_num_rows
                        FROM parquet_metadata({_sql_string(path)})
                    )
                    """
                ).fetchone()[0]
            finally:
                con.close()
            total += int(value or 0)
            inspected += 1
        except Exception as exc:
            return (total if inspected else None), f"row estimate stopped at {inspected} files: {exc}"
    return total, None


def _parquet_date_range_from_metadata(
    duckdb_module,
    files: list[Path],
    *,
    date_column: str,
) -> tuple[dict[str, str | None] | None, str | None]:
    min_values: list[str] = []
    max_values: list[str] = []
    inspected = 0
    for path in files:
        try:
            con = duckdb_module.connect(":memory:")
            try:
                row = con.execute(
                    f"""
                    SELECT MIN(stats_min_value), MAX(stats_max_value)
                    FROM parquet_metadata({_sql_string(path)})
                    WHERE path_in_schema = ?
                    """,
                    [date_column],
                ).fetchone()
            finally:
                con.close()
            inspected += 1
            if row and row[0] is not None:
                min_values.append(str(row[0])[:10])
            if row and row[1] is not None:
                max_values.append(str(row[1])[:10])
        except Exception as exc:
            return None, f"date-range metadata stopped at {inspected} files: {exc}"
    if not min_values or not max_values:
        return None, None
    return {"min": min(min_values), "max": max(max_values)}, None


def _date_range_from_paths(files: list[Path]) -> dict[str, str | None] | None:
    values: list[str] = []
    year_month_re = re.compile(r"year=(\d{4})/month=(\d{2})")
    date_re = re.compile(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})")
    for path in files:
        text = str(path)
        ym_match = year_month_re.search(text)
        if ym_match:
            values.append(f"{ym_match.group(1)}-{ym_match.group(2)}")
            continue
        date_match = date_re.search(text)
        if date_match:
            values.append(
                f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
            )
    if not values:
        return None
    return {"min": min(values), "max": max(values)}


def _json_sample_schema(path: Path) -> tuple[list[dict[str, str]], str | None]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [], f"could not parse sample JSON schema: {exc}"

    sample = payload
    if isinstance(payload, list) and payload:
        sample = payload[0]
    if isinstance(payload, dict):
        for key in ("data", "results", "rows"):
            value = payload.get(key)
            if isinstance(value, list) and value:
                sample = value[0]
                break
    if not isinstance(sample, dict):
        return [{"name": "<root>", "type": type(sample).__name__}], None
    return [
        {"name": str(key), "type": type(value).__name__}
        for key, value in list(sample.items())[:40]
    ], None


def _sqlite_sample_schema(path: Path) -> tuple[list[dict[str, str]], str | None]:
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except Exception as exc:
        return [], f"could not open SQLite candidate read-only: {exc}"

    try:
        table_rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name LIMIT 8"
        ).fetchall()
        schema: list[dict[str, str]] = []
        for (table_name,) in table_rows:
            columns = conn.execute(f"PRAGMA table_info({_quote_identifier(table_name)})").fetchall()
            column_text = ", ".join(f"{col[1]} {col[2]}" for col in columns[:12])
            schema.append({"name": str(table_name), "type": column_text or "table"})
        return schema, None
    except Exception as exc:
        return [], f"could not read SQLite schema: {exc}"
    finally:
        conn.close()


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'
