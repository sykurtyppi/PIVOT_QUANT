"""Read-only T9 parquet smoke adapter.

The adapter loads small, date-bounded slices only. It never writes to, copies,
or mutates files under the external T9 root.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from services.external_data.t9_inventory import DEFAULT_T9_ROOT, resolve_t9_root


DAILY_COLUMNS = ["date", "open", "high", "low", "close", "volume", "source"]
OPTION_FEATURE_COLUMNS = [
    "date",
    "underlying_symbol",
    "expiration",
    "strike",
    "option_type",
    "bid",
    "ask",
    "mid",
    "volume",
    "open_interest",
    "implied_volatility",
]
DAILY_SOURCE_ENV = "PIVOTQUANT_DAILY_SOURCE"
DEFAULT_DAILY_SOURCE = "yahoo"
DAILY_SOURCE_CHOICES = {"yahoo", "ivolatility", "auto"}
DAILY_SOURCE_PRECEDENCE = ["yahoo", "ivolatility"]


@dataclass
class NormalizedSlice:
    name: str
    rows: pd.DataFrame
    files: list[Path]
    missing_columns: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def summary(self, *, sample_size: int = 3) -> dict[str, Any]:
        date_range = _frame_date_range(self.rows)
        return {
            "name": self.name,
            "row_count": int(len(self.rows)),
            "date_range": date_range,
            "file_count": int(len(self.files)),
            "files_sample": [str(path) for path in self.files[:5]],
            "missing_columns": list(self.missing_columns),
            "warnings": list(self.warnings),
            "metadata": dict(self.metadata),
            "sample_rows": _sample_records(self.rows, sample_size=sample_size),
        }


def load_historical_smoke_slice(
    *,
    symbol: str = "SPY",
    start_date: str,
    end_date: str,
    root: str | Path | None = None,
    max_files: int = 20,
    daily_source: str | None = None,
) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    max_files = max(1, int(max_files))
    t9_root = resolve_t9_root(root)
    source_mode = resolve_daily_source(daily_source)

    report: dict[str, Any] = {
        "symbol": normalized_symbol,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "t9_root": str(t9_root),
        "root_exists": t9_root.exists(),
        "read_only": True,
        "config": {
            "daily_source": source_mode,
            "daily_source_env": DAILY_SOURCE_ENV,
        },
        "warnings": [],
        "sections": {},
    }
    if not t9_root.exists():
        report["warnings"].append(
            f"T9 root does not exist: {t9_root}. Set PIVOTQUANT_T9_ROOT to the mounted drive path."
        )
        return report

    daily_files = _select_files_for_window(
        t9_root
        / "market_data"
        / "normalized"
        / "underlyings"
        / "daily_ohlcv"
        / f"underlying_symbol={normalized_symbol}",
        start=start,
        end=end,
        max_files=max_files,
    )
    option_feature_files = _select_files_for_window(
        t9_root
        / "market_data"
        / "research"
        / "options_features_eod"
        / f"underlying_symbol={normalized_symbol}",
        start=start,
        end=end,
        max_files=max_files,
    )

    daily = load_daily_ohlcv(
        daily_files,
        start=start,
        end=end,
        symbol=normalized_symbol,
        daily_source=source_mode,
    )
    options = load_option_features(
        option_feature_files,
        start=start,
        end=end,
        symbol=normalized_symbol,
    )

    report["sections"] = {
        "daily_ohlcv": daily.summary(),
        "option_features": options.summary(),
    }
    report["sections"]["historical_contract"] = validate_historical_smoke_contract(
        daily.rows,
        options.rows,
        start=start,
        end=end,
    )
    return report


def load_daily_ohlcv(
    files: list[Path],
    *,
    start: date,
    end: date,
    symbol: str,
    daily_source: str | None = None,
) -> NormalizedSlice:
    warnings: list[str] = []
    source_mode = resolve_daily_source(daily_source)
    if not files:
        return NormalizedSlice(
            name="daily_ohlcv",
            rows=pd.DataFrame(columns=DAILY_COLUMNS),
            files=[],
            missing_columns=list(DAILY_COLUMNS),
            warnings=["no daily OHLCV parquet files found for requested date window"],
            metadata={"source_mode": source_mode, "selected_source": None},
        )

    raw, read_warnings = _read_parquet_window(files, start=start, end=end, date_candidates=["trade_date", "date"])
    warnings.extend(read_warnings)
    if raw.empty:
        return NormalizedSlice(
            name="daily_ohlcv",
            rows=pd.DataFrame(columns=DAILY_COLUMNS),
            files=files,
            missing_columns=[],
            warnings=warnings + ["no daily OHLCV rows found in requested date window"],
        )

    frame = pd.DataFrame()
    frame["date"] = _date_series(raw, ["trade_date", "date"])
    frame["open"] = _numeric_series(raw, ["open", "open_price", "Open", "open_10000"], scale_for_suffix="_10000")
    frame["high"] = _numeric_series(raw, ["high", "high_price", "High", "high_10000"], scale_for_suffix="_10000")
    frame["low"] = _numeric_series(raw, ["low", "low_price", "Low", "low_10000"], scale_for_suffix="_10000")
    frame["close"] = _numeric_series(raw, ["close", "close_price", "Close", "close_10000"], scale_for_suffix="_10000")
    frame["volume"] = _numeric_series(raw, ["volume", "Volume"])
    frame["source"] = _string_series(raw, ["vendor", "source", "source_path"], default="unknown")
    if "underlying_symbol" in raw.columns:
        frame = frame[raw["underlying_symbol"].astype(str).str.upper().eq(symbol)]

    frame = _filter_normalized_dates(frame, start=start, end=end)
    duplicate_summary = _duplicate_daily_source_summary(frame)
    if duplicate_summary["duplicate_date_count"]:
        warnings.append(
            "duplicate daily OHLCV source rows detected; selected one canonical source "
            f"for {duplicate_summary['duplicate_date_count']} date(s)"
        )
    selected_source = _choose_daily_source(frame, source_mode=source_mode)
    if selected_source:
        frame = frame[frame["source"].astype("string").str.lower().eq(selected_source)]
    elif not frame.empty:
        warnings.append(f"no daily rows matched configured source mode: {source_mode}")
        frame = frame.iloc[0:0].copy()
    missing = [col for col in DAILY_COLUMNS if frame[col].isna().all()]
    return NormalizedSlice(
        name="daily_ohlcv",
        rows=frame[DAILY_COLUMNS].sort_values("date").reset_index(drop=True),
        files=files,
        missing_columns=missing,
        warnings=warnings,
        metadata={
            "source_mode": source_mode,
            "selected_source": selected_source,
            "available_sources": duplicate_summary["available_sources"],
            "duplicate_date_count": duplicate_summary["duplicate_date_count"],
            "duplicate_dates_sample": duplicate_summary["duplicate_dates_sample"],
        },
    )


def load_option_features(
    files: list[Path],
    *,
    start: date,
    end: date,
    symbol: str,
) -> NormalizedSlice:
    warnings: list[str] = []
    if not files:
        return NormalizedSlice(
            name="option_features",
            rows=pd.DataFrame(columns=OPTION_FEATURE_COLUMNS),
            files=[],
            missing_columns=list(OPTION_FEATURE_COLUMNS),
            warnings=["no option feature parquet files found for requested date window"],
        )

    raw, read_warnings = _read_parquet_window(files, start=start, end=end, date_candidates=["trade_date", "date"])
    warnings.extend(read_warnings)
    if raw.empty:
        return NormalizedSlice(
            name="option_features",
            rows=pd.DataFrame(columns=OPTION_FEATURE_COLUMNS),
            files=files,
            missing_columns=[],
            warnings=warnings + ["no option feature rows found in requested date window"],
        )

    frame = pd.DataFrame()
    frame["date"] = _date_series(raw, ["trade_date", "date"])
    frame["underlying_symbol"] = _string_series(raw, ["underlying_symbol", "underlying", "symbol"], default=symbol).str.upper()
    frame["expiration"] = _date_series(raw, ["expiry", "expiration", "expiration_date"])
    frame["strike"] = _numeric_series(raw, ["strike", "strike_price", "strike_10000"], scale_for_suffix="_10000")
    frame["option_type"] = _option_type_series(raw)
    frame["bid"] = _numeric_series(raw, ["bid", "bid_price", "bid_10000"], scale_for_suffix="_10000")
    frame["ask"] = _numeric_series(raw, ["ask", "ask_price", "ask_10000"], scale_for_suffix="_10000")
    frame["mid"] = _numeric_series(raw, ["mid", "mark", "mid_10000"], scale_for_suffix="_10000")
    frame["volume"] = _numeric_series(raw, ["volume", "Volume"])
    frame["open_interest"] = _numeric_series(raw, ["open_interest", "openInterest", "oi"])
    frame["implied_volatility"] = _numeric_series(
        raw,
        ["implied_volatility", "iv", "preiv", "iv_1000000"],
        scale_for_suffix="_1000000",
    )
    frame = frame[frame["underlying_symbol"].eq(symbol)]
    frame = _filter_normalized_dates(frame, start=start, end=end)
    missing = [col for col in OPTION_FEATURE_COLUMNS if frame[col].isna().all()]
    return NormalizedSlice(
        name="option_features",
        rows=frame[OPTION_FEATURE_COLUMNS].sort_values(["date", "expiration", "strike"]).reset_index(drop=True),
        files=files,
        missing_columns=missing,
        warnings=warnings,
    )


def write_smoke_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "historical_smoke"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    stem = (
        f"{report['symbol'].lower()}_{report['start_date']}_{report['end_date']}"
        .replace("/", "-")
        .replace(":", "-")
    )
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def resolve_daily_source(value: str | None = None) -> str:
    raw = value if value is not None else os.environ.get(DAILY_SOURCE_ENV, DEFAULT_DAILY_SOURCE)
    source = str(raw or DEFAULT_DAILY_SOURCE).strip().lower()
    if source not in DAILY_SOURCE_CHOICES:
        choices = ", ".join(sorted(DAILY_SOURCE_CHOICES))
        raise ValueError(f"unsupported daily source {raw!r}; expected one of: {choices}")
    return source


def validate_historical_smoke_contract(
    daily_rows: pd.DataFrame,
    option_rows: pd.DataFrame,
    *,
    start: date,
    end: date,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def add_check(name: str, status: str, detail: str, **extra: Any) -> None:
        payload = {"name": name, "status": status, "detail": detail}
        payload.update(extra)
        checks.append(payload)

    daily_missing = _missing_required_columns(daily_rows, DAILY_COLUMNS)
    option_missing = _missing_required_columns(option_rows, OPTION_FEATURE_COLUMNS)
    add_check(
        "daily_required_columns",
        "fail" if daily_missing else "pass",
        "daily OHLCV normalized schema is complete" if not daily_missing else "daily OHLCV schema is incomplete",
        missing_columns=daily_missing,
    )
    add_check(
        "option_required_columns",
        "fail" if option_missing else "pass",
        "option feature normalized schema is complete" if not option_missing else "option feature schema is incomplete",
        missing_columns=option_missing,
    )

    add_check(
        "daily_rows_present",
        "fail" if daily_rows.empty else "pass",
        "daily OHLCV rows are present" if not daily_rows.empty else "no daily OHLCV rows available for contract window",
        row_count=int(len(daily_rows)),
    )
    add_check(
        "option_rows_present",
        "fail" if option_rows.empty else "pass",
        "option feature rows are present" if not option_rows.empty else "no option feature rows available for contract window",
        row_count=int(len(option_rows)),
    )

    daily_outside = _dates_outside_window(daily_rows, start=start, end=end)
    option_outside = _dates_outside_window(option_rows, start=start, end=end)
    add_check(
        "daily_date_window",
        "fail" if daily_outside else "pass",
        "daily dates are inside requested window" if not daily_outside else "daily rows include dates outside requested window",
        outside_dates_sample=daily_outside[:5],
    )
    add_check(
        "option_date_window",
        "fail" if option_outside else "pass",
        "option feature dates are inside requested window" if not option_outside else "option rows include dates outside requested window",
        outside_dates_sample=option_outside[:5],
    )

    unaligned_option_dates = _option_dates_without_daily(daily_rows, option_rows)
    add_check(
        "daily_option_date_alignment",
        "fail" if unaligned_option_dates else "pass",
        "each option feature date has a selected daily OHLCV row"
        if not unaligned_option_dates
        else "option feature dates are missing selected daily OHLCV rows",
        unaligned_dates_sample=unaligned_option_dates[:5],
    )

    future_like = _future_or_label_like_columns(daily_rows, option_rows)
    add_check(
        "no_future_or_label_like_feature_columns",
        "fail" if future_like else "pass",
        "no future/label-like columns are present in normalized feature frames"
        if not future_like
        else "future/label-like columns must not be used as smoke-contract features",
        columns=future_like,
    )

    status = "fail" if any(check["status"] == "fail" for check in checks) else "pass"
    return {
        "name": "historical_feature_label_contract",
        "status": status,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "checks": checks,
    }


def _normalize_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise ValueError("symbol must not be empty")
    if not re.match(r"^[A-Z0-9._-]{1,16}$", normalized):
        raise ValueError(f"unsupported symbol value: {symbol!r}")
    return normalized


def _parse_date(value: str) -> date:
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _select_files_for_window(base: Path, *, start: date, end: date, max_files: int) -> list[Path]:
    if not base.exists():
        return []
    out: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
        for filename in sorted(filenames):
            if not filename.endswith(".parquet"):
                continue
            path = Path(dirpath) / filename
            if not _file_overlaps_window(path, start=start, end=end):
                continue
            out.append(path)
            if len(out) >= max_files:
                return out
    return out


def _file_overlaps_window(path: Path, *, start: date, end: date) -> bool:
    text = str(path)
    month_match = re.search(r"year=(\d{4})/month=(\d{2})", text)
    if month_match:
        year = int(month_match.group(1))
        month = int(month_match.group(2))
        month_start = date(year, month, 1)
        month_end = date(year + int(month == 12), 1 if month == 12 else month + 1, 1)
        return month_start <= end and month_end > start

    date_match = re.search(r"(20\d{2})[-_](\d{2})(?:[-_](\d{2}))?", path.name)
    if date_match:
        year = int(date_match.group(1))
        month = int(date_match.group(2))
        day = int(date_match.group(3) or "1")
        file_date = date(year, month, day)
        return start <= file_date <= end or (day == 1 and file_date.replace(day=1) <= end)
    return True


def _read_parquet_window(
    files: list[Path],
    *,
    start: date,
    end: date,
    date_candidates: list[str],
) -> tuple[pd.DataFrame, list[str]]:
    warnings: list[str] = []
    try:
        return _read_parquet_window_duckdb_module(
            files,
            start=start,
            end=end,
            date_candidates=date_candidates,
        ), warnings
    except Exception as exc:
        warnings.append(f"duckdb Python read unavailable: {exc}")

    try:
        return _read_parquet_window_duckdb_cli(
            files,
            start=start,
            end=end,
            date_candidates=date_candidates,
        ), warnings
    except Exception as exc:
        warnings.append(f"duckdb CLI read unavailable: {exc}")

    try:
        frame = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
        date_col = _first_existing(frame, date_candidates)
        if date_col is not None:
            dates = pd.to_datetime(frame[date_col], errors="coerce").dt.date
            frame = frame[(dates >= start) & (dates <= end)]
        return frame, warnings
    except Exception as exc:
        warnings.append(f"pandas parquet read unavailable: {exc}")
        return pd.DataFrame(), warnings


def _read_parquet_window_duckdb_module(
    files: list[Path],
    *,
    start: date,
    end: date,
    date_candidates: list[str],
) -> pd.DataFrame:
    import duckdb  # type: ignore

    con = duckdb.connect(":memory:")
    try:
        file_sql = _duckdb_file_list_sql(files)
        schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet({file_sql})").fetchall()
        date_col = _first_name_in_schema(schema, date_candidates)
        where = ""
        params: list[Any] = []
        if date_col:
            where = f" WHERE CAST({_quote_identifier(date_col)} AS DATE) BETWEEN ? AND ?"
            params = [start.isoformat(), end.isoformat()]
        return con.execute(f"SELECT * FROM read_parquet({file_sql}){where}", params).df()
    finally:
        con.close()


def _read_parquet_window_duckdb_cli(
    files: list[Path],
    *,
    start: date,
    end: date,
    date_candidates: list[str],
) -> pd.DataFrame:
    duckdb_bin = shutil.which("duckdb")
    if not duckdb_bin:
        raise RuntimeError("duckdb CLI not found")

    file_sql = _duckdb_file_list_sql(files)
    schema_sql = f"DESCRIBE SELECT * FROM read_parquet({file_sql})"
    schema_result = subprocess.run(
        [duckdb_bin, "-csv", "-c", schema_sql],
        check=True,
        capture_output=True,
        text=True,
    )
    schema_rows = list(csv.DictReader(io.StringIO(schema_result.stdout)))
    date_col = _first_name_in_schema(
        [(row.get("column_name"),) for row in schema_rows],
        date_candidates,
    )
    where = ""
    if date_col:
        where = (
            f" WHERE CAST({_quote_identifier(date_col)} AS DATE) "
            f"BETWEEN DATE '{start.isoformat()}' AND DATE '{end.isoformat()}'"
        )
    data_sql = f"SELECT * FROM read_parquet({file_sql}){where}"
    data_result = subprocess.run(
        [duckdb_bin, "-csv", "-c", data_sql],
        check=True,
        capture_output=True,
        text=True,
    )
    if not data_result.stdout.strip():
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(data_result.stdout))


def _duckdb_file_list_sql(files: list[Path]) -> str:
    if not files:
        raise ValueError("no parquet files supplied")
    return "[" + ", ".join(_sql_literal(str(path)) for path in files) + "]"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _first_name_in_schema(schema_rows: list[Any], candidates: list[str]) -> str | None:
    names = [str(row[0]) for row in schema_rows if row and row[0] is not None]
    lowered = {name.lower(): name for name in names}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def _first_existing(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    lowered = {str(col).lower(): str(col) for col in frame.columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def _date_series(frame: pd.DataFrame, candidates: list[str]) -> pd.Series:
    col = _first_existing(frame, candidates)
    if col is None:
        return pd.Series([None] * len(frame), dtype="object")
    return pd.to_datetime(frame[col], errors="coerce").dt.date.astype("string")


def _numeric_series(
    frame: pd.DataFrame,
    candidates: list[str],
    *,
    scale_for_suffix: str | None = None,
) -> pd.Series:
    col = _first_existing(frame, candidates)
    if col is None:
        return pd.Series([pd.NA] * len(frame), dtype="Float64")
    values = pd.to_numeric(frame[col], errors="coerce")
    if scale_for_suffix and str(col).endswith(scale_for_suffix):
        scale = 10_000.0 if scale_for_suffix == "_10000" else 1_000_000.0
        values = values / scale
    return values


def _string_series(frame: pd.DataFrame, candidates: list[str], *, default: str) -> pd.Series:
    col = _first_existing(frame, candidates)
    if col is None:
        return pd.Series([default] * len(frame), dtype="string")
    return frame[col].fillna(default).astype("string")


def _option_type_series(frame: pd.DataFrame) -> pd.Series:
    col = _first_existing(frame, ["option_type", "call_put", "side"])
    if col is None:
        return pd.Series([pd.NA] * len(frame), dtype="string")
    values = frame[col].astype("string").str.lower()
    return values.replace(
        {
            "c": "call",
            "p": "put",
            "call": "call",
            "put": "put",
        }
    )


def _filter_normalized_dates(frame: pd.DataFrame, *, start: date, end: date) -> pd.DataFrame:
    if frame.empty or "date" not in frame.columns:
        return frame
    dates = pd.to_datetime(frame["date"], errors="coerce").dt.date
    return frame[(dates >= start) & (dates <= end)].copy()


def _frame_date_range(frame: pd.DataFrame) -> dict[str, str | None]:
    if frame.empty or "date" not in frame.columns:
        return {"min": None, "max": None}
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if dates.empty:
        return {"min": None, "max": None}
    return {
        "min": dates.min().date().isoformat(),
        "max": dates.max().date().isoformat(),
    }


def _sample_records(frame: pd.DataFrame, *, sample_size: int) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    sample = frame.head(sample_size).copy()
    return json.loads(sample.to_json(orient="records", date_format="iso"))


def _choose_daily_source(frame: pd.DataFrame, *, source_mode: str) -> str | None:
    if frame.empty or "source" not in frame.columns:
        return None
    available = sorted(
        source
        for source in frame["source"].astype("string").str.lower().dropna().unique().tolist()
        if source
    )
    if not available:
        return None
    if source_mode != "auto":
        return source_mode if source_mode in available else None
    for preferred in DAILY_SOURCE_PRECEDENCE:
        if preferred in available:
            return preferred
    return available[0]


def _duplicate_daily_source_summary(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty or "date" not in frame.columns or "source" not in frame.columns:
        return {
            "available_sources": [],
            "duplicate_date_count": 0,
            "duplicate_dates_sample": [],
        }
    working = frame[["date", "source"]].dropna().copy()
    working["source"] = working["source"].astype("string").str.lower()
    available = sorted(source for source in working["source"].dropna().unique().tolist() if source)
    source_counts = working.groupby("date")["source"].nunique()
    duplicate_dates = source_counts[source_counts > 1].index.astype(str).tolist()
    return {
        "available_sources": available,
        "duplicate_date_count": int(len(duplicate_dates)),
        "duplicate_dates_sample": duplicate_dates[:5],
    }


def _missing_required_columns(frame: pd.DataFrame, required: list[str]) -> list[str]:
    present = {str(col) for col in frame.columns}
    return [col for col in required if col not in present]


def _dates_outside_window(frame: pd.DataFrame, *, start: date, end: date) -> list[str]:
    dates = _valid_date_values(frame)
    return sorted(value.isoformat() for value in dates if value < start or value > end)


def _option_dates_without_daily(daily_rows: pd.DataFrame, option_rows: pd.DataFrame) -> list[str]:
    daily_dates = _valid_date_values(daily_rows)
    option_dates = _valid_date_values(option_rows)
    return sorted(value.isoformat() for value in option_dates if value not in daily_dates)


def _valid_date_values(frame: pd.DataFrame) -> set[date]:
    if frame.empty or "date" not in frame.columns:
        return set()
    values = pd.to_datetime(frame["date"], errors="coerce").dropna()
    return {value.date() for value in values}


def _future_or_label_like_columns(*frames: pd.DataFrame) -> list[str]:
    blocked = re.compile(r"(future|forward|fwd|next_|label|target|outcome|realized)", re.IGNORECASE)
    allowed = set(DAILY_COLUMNS + OPTION_FEATURE_COLUMNS)
    found: set[str] = set()
    for frame in frames:
        for column in frame.columns:
            name = str(column)
            if name in allowed:
                continue
            if blocked.search(name):
                found.add(name)
    return sorted(found)
