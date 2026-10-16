#!/usr/bin/env python3
"""Canonical versioned contract for deterministic levels-product snapshots.

The snapshot is the single handoff between morning level generation, human
publication, intraday alerts, UI consumers, and future outcome scorekeeping.
Identifiers deliberately exclude generation time and evidence annotations so a
rerun from the same completed-session inputs produces the same identities.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from backfill_events import calculate_pivots
from trading_calendar import is_trading_day, session_close_et

CONTRACT = "pivotquant.levels_snapshot"
CONTRACT_VERSION = 1
CALCULATION_FAMILY = "classic_floor_pivots"
CALCULATION_VERSION = "backfill_events.calculate_pivots:v1"
ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
SYMBOL_PATTERN = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,31}")
PIVOT_LABELS = frozenset(
    {"R3", "R2", "R1", "M4", "M2", "PP", "M1", "M3", "S1", "S2", "S3"}
)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _digest(prefix: str, value: Any) -> str:
    return f"{prefix}_{hashlib.sha256(_canonical_bytes(value)).hexdigest()[:24]}"


def _json_value(value: Any) -> Any:
    """Return a stable JSON-native copy (including stringified mapping keys)."""
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))


def next_trading_day(source_session_date: date) -> date:
    candidate = source_session_date + timedelta(days=1)
    while not is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def _session_close_utc(session_date: date) -> str:
    local_close = datetime.combine(session_date, session_close_et(session_date), tzinfo=ET)
    return local_close.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _require_canonical_utc(name: str, value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a canonical UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be a canonical UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be a canonical UTC timestamp")
    if parsed.astimezone(UTC).isoformat().replace("+00:00", "Z") != value:
        raise ValueError(f"{name} must be a canonical UTC timestamp ending in Z")
    return parsed


def _expected_pivots(source_ohlc: Mapping[str, Any]) -> dict[str, float]:
    return {
        label: round(float(price), 2)
        for label, price in calculate_pivots(
            float(source_ohlc["high"]),
            float(source_ohlc["low"]),
            float(source_ohlc["close"]),
        ).items()
    }


def _validate_pivot_inputs(source_ohlc: Mapping[str, Any]) -> None:
    values = {key: float(source_ohlc[key]) for key in ("high", "low", "close")}
    if not all(math.isfinite(value) and value > 0 for value in values.values()):
        raise ValueError("source OHLC values must be finite and positive")
    if values["high"] < values["low"] or not values["low"] <= values["close"] <= values["high"]:
        raise ValueError("source OHLC values are inconsistent")


def _validate_pivot_levels(raw_levels: Mapping[str, Any], source_ohlc: Mapping[str, Any]) -> None:
    if set(raw_levels) != PIVOT_LABELS:
        raise ValueError("pivot labels must contain the complete classic floor-pivot set")
    expected = _expected_pivots(source_ohlc)
    actual = {str(label): round(float(price), 2) for label, price in raw_levels.items()}
    if actual != expected:
        raise ValueError("levels do not match the declared pivot calculation")


def _identity_core(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "contract": snapshot["contract"],
        "contract_version": snapshot["contract_version"],
        "symbol": snapshot["symbol"],
        "session_date": snapshot["session_date"],
        "source": snapshot["source"],
        "calculation": snapshot["calculation"],
        "reference_price": snapshot["reference_price"],
        "levels": [
            {
                key: level[key]
                for key in (
                    "level_id",
                    "label",
                    "price",
                    "side",
                    "distance_from_reference_bps",
                    "confluence_components",
                    "validity",
                )
            }
            for level in snapshot["levels"]
        ],
    }


def _level_identity_core(
    *, symbol: str, session_date: str, label: str, price: float
) -> dict[str, Any]:
    return {
        "contract": CONTRACT,
        "contract_version": CONTRACT_VERSION,
        "symbol": symbol,
        "session_date": session_date,
        "calculation_family": CALCULATION_FAMILY,
        "calculation_version": CALCULATION_VERSION,
        "label": label,
        "price": price,
    }


def build_snapshot(
    *,
    symbol: str,
    source_session_date: date,
    source_ohlc: Mapping[str, float],
    generated_at_utc: str,
    raw_levels: Mapping[str, float],
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Build one deterministic levels snapshot from a completed RTH session."""
    normalized_symbol = symbol.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(normalized_symbol):
        raise ValueError("symbol must be a filename-safe market identifier")
    required_ohlc = {"high", "low", "close"}
    if set(source_ohlc) != required_ohlc:
        raise ValueError("source_ohlc must contain exactly high, low, and close")
    if not is_trading_day(source_session_date):
        raise ValueError("source_session_date must be a trading session")
    _validate_pivot_inputs(source_ohlc)
    _validate_pivot_levels(raw_levels, source_ohlc)

    target_session = next_trading_day(source_session_date)
    target_session_text = target_session.isoformat()
    reference_price = round(float(source_ohlc["close"]), 2)
    if reference_price <= 0:
        raise ValueError("source close must be positive")

    levels = []
    for label, raw_price in raw_levels.items():
        price = round(float(raw_price), 2)
        if price <= 0:
            raise ValueError(f"level {label} price must be positive")
        level_id = _digest(
            "lvl",
            _level_identity_core(
                symbol=normalized_symbol,
                session_date=target_session_text,
                label=str(label),
                price=price,
            ),
        )
        levels.append(
            {
                "level_id": level_id,
                "label": str(label),
                "price": price,
                "side": "resistance" if price >= reference_price else "support",
                "distance_from_reference_bps": round(
                    (price - reference_price) / reference_price * 10_000,
                    1,
                ),
                "confluence_components": [],
                "validity": {
                    "session_date": target_session_text,
                    "expires_at_utc": _session_close_utc(target_session),
                },
            }
        )
    levels.sort(key=lambda item: (-item["price"], item["label"]))

    snapshot: dict[str, Any] = {
        "contract": CONTRACT,
        "contract_version": CONTRACT_VERSION,
        "snapshot_id": "",
        "product": "deterministic_levels",
        "symbol": normalized_symbol,
        "session_date": target_session_text,
        "generated_at_utc": generated_at_utc,
        "source": {
            "kind": "completed_rth_ohlc",
            "session_date": source_session_date.isoformat(),
            "as_of_utc": _session_close_utc(source_session_date),
            "freshness": "current_prior_session",
            "ohlc": {
                "high": float(source_ohlc["high"]),
                "low": float(source_ohlc["low"]),
                "close": float(source_ohlc["close"]),
            },
        },
        "calculation": {
            "family": CALCULATION_FAMILY,
            "version": CALCULATION_VERSION,
        },
        "reference_price": reference_price,
        "levels": levels,
        "evidence": _json_value(evidence),
    }
    snapshot["snapshot_id"] = _digest("lvs", _identity_core(snapshot))
    validate_snapshot(snapshot)
    return snapshot


def validate_snapshot(snapshot: Mapping[str, Any]) -> None:
    """Fail closed when a snapshot is malformed or its identity no longer matches."""
    if snapshot.get("contract") != CONTRACT:
        raise ValueError(f"unsupported levels contract: {snapshot.get('contract')!r}")
    if snapshot.get("contract_version") != CONTRACT_VERSION:
        raise ValueError(f"unsupported levels contract version: {snapshot.get('contract_version')!r}")
    if not snapshot.get("levels"):
        raise ValueError("levels snapshot contains no levels")
    if snapshot.get("source", {}).get("freshness") != "current_prior_session":
        raise ValueError("levels snapshot source is not current_prior_session")

    generated_at = _require_canonical_utc(
        "generated_at_utc", snapshot.get("generated_at_utc")
    )
    source = snapshot["source"]
    source_as_of = _require_canonical_utc("source.as_of_utc", source.get("as_of_utc"))
    try:
        source_session_date = date.fromisoformat(source["session_date"])
        snapshot_session_date = date.fromisoformat(snapshot["session_date"])
    except (TypeError, ValueError) as exc:
        raise ValueError("source and snapshot session dates must be ISO dates") from exc
    if not is_trading_day(source_session_date):
        raise ValueError("source.session_date must be a trading session")
    if next_trading_day(source_session_date) != snapshot_session_date:
        raise ValueError("snapshot session_date is not the next trading session")
    if source["as_of_utc"] != _session_close_utc(source_session_date):
        raise ValueError("source.as_of_utc does not match the source session close")
    target_expiry = datetime.fromisoformat(
        _session_close_utc(snapshot_session_date).replace("Z", "+00:00")
    )
    if generated_at < source_as_of or generated_at > target_expiry:
        raise ValueError(
            "snapshot generation time must be after source close and before target expiry"
        )
    if snapshot.get("calculation") != {
        "family": CALCULATION_FAMILY,
        "version": CALCULATION_VERSION,
    }:
        raise ValueError("snapshot calculation metadata is unsupported")
    if not SYMBOL_PATTERN.fullmatch(str(snapshot.get("symbol", ""))):
        raise ValueError("snapshot symbol is not filename-safe")
    source_ohlc = source.get("ohlc", {})
    if set(source_ohlc) != {"high", "low", "close"}:
        raise ValueError("source OHLC must contain exactly high, low, and close")
    _validate_pivot_inputs(source_ohlc)

    seen_ids: set[str] = set()
    seen_labels: set[str] = set()
    reference_price = float(snapshot["reference_price"])
    normalized_source_close = round(float(source_ohlc["close"]), 2)
    if (not math.isfinite(reference_price) or reference_price <= 0
            or reference_price != normalized_source_close):
        raise ValueError(
            "reference_price must equal the positive normalized source close"
        )
    expected_expiry = _session_close_utc(snapshot_session_date)
    for level in snapshot["levels"]:
        expected_level_id = _digest(
            "lvl",
            _level_identity_core(
                symbol=snapshot["symbol"],
                session_date=snapshot["session_date"],
                label=level["label"],
                price=level["price"],
            ),
        )
        if level.get("level_id") != expected_level_id:
            raise ValueError(f"level_id mismatch for {level.get('label')}")
        if expected_level_id in seen_ids:
            raise ValueError(f"duplicate level_id for {level.get('label')}")
        seen_ids.add(expected_level_id)
        if level["label"] in seen_labels:
            raise ValueError(f"duplicate level label: {level.get('label')}")
        seen_labels.add(level["label"])
        expected_side = "resistance" if float(level["price"]) >= reference_price else "support"
        if level.get("side") != expected_side:
            raise ValueError(f"level side does not match price for {level.get('label')}")
        expected_distance = round(
            (float(level["price"]) - reference_price) / reference_price * 10_000,
            1,
        )
        if level.get("distance_from_reference_bps") != expected_distance:
            raise ValueError(f"level distance does not match price for {level.get('label')}")
        if level.get("validity", {}).get("session_date") != snapshot["session_date"]:
            raise ValueError(f"invalid session validity for {level.get('label')}")
        if level.get("validity", {}).get("expires_at_utc") != expected_expiry:
            raise ValueError(f"invalid expiry for {level.get('label')}")

    actual_pivots = {level["label"]: level["price"] for level in snapshot["levels"]}
    _validate_pivot_levels(actual_pivots, source_ohlc)

    expected_snapshot_id = _digest("lvs", _identity_core(snapshot))
    if snapshot.get("snapshot_id") != expected_snapshot_id:
        raise ValueError("snapshot_id does not match snapshot contents")


def load_snapshot(path: Path) -> dict[str, Any]:
    try:
        snapshot = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read levels snapshot {path}: {exc}") from exc
    validate_snapshot(snapshot)
    return snapshot


def _create_immutable_json(path: Path, payload: Mapping[str, Any]) -> bool:
    """Atomically create ``path`` without replacing an existing artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp_name, path)
        except FileExistsError:
            return False
        return True
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def _atomic_latest_link(latest: Path, immutable: Path) -> None:
    """Atomically point latest at an already durable immutable artifact."""
    latest.parent.mkdir(parents=True, exist_ok=True)
    temp = latest.parent / f".{latest.name}.{os.getpid()}.tmp"
    try:
        temp.unlink(missing_ok=True)
        temp.symlink_to(immutable.name)
        os.replace(temp, latest)
    finally:
        temp.unlink(missing_ok=True)


def write_snapshot(snapshot: Mapping[str, Any], out_dir: Path) -> tuple[Path, Path]:
    """Create an immutable artifact and a monotonic, atomic latest pointer."""
    validate_snapshot(snapshot)
    immutable = out_dir / f"levels_snapshot_{snapshot['symbol']}_{snapshot['session_date']}.json"
    latest = out_dir / f"levels_snapshot_{snapshot['symbol']}_latest.json"
    if not _create_immutable_json(immutable, snapshot):
        existing = load_snapshot(immutable)
        if existing["snapshot_id"] != snapshot["snapshot_id"]:
            raise ValueError(
                f"immutable snapshot already exists with different contents: {immutable}"
            )

    lock_path = out_dir / f".{latest.name}.lock"
    with lock_path.open("a+b") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            if latest.exists() or latest.is_symlink():
                current = load_snapshot(latest)
                if current["session_date"] > snapshot["session_date"]:
                    return immutable, latest
                if (current["session_date"] == snapshot["session_date"]
                        and current["snapshot_id"] != snapshot["snapshot_id"]):
                    raise ValueError(
                        "latest snapshot conflicts with immutable session artifact"
                    )

            _atomic_latest_link(latest, immutable)
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    return immutable, latest
