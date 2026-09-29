#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from server.model_metadata import ModelMetadataError, validate_model_context


DEFAULT_REGISTRY_ROOT = Path(
    os.getenv("MODEL_REGISTRY_ROOT") or ROOT / "data" / "t9_experiments"
).expanduser()
DEFAULT_LINEAGE_PATH = Path(
    os.getenv("RESEARCH_LINEAGE_PATH") or ROOT / "data" / "research_marts" / "last_build.json"
).expanduser()
DEFAULT_COST_MODELS_PATH = (ROOT / "research" / "marts" / "cost_models.json").expanduser()
DEFAULT_TRAINING_VIEW = os.getenv("DUCKDB_VIEW", "training_events_v1").strip() or "training_events_v1"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object in {path}")
    return payload


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}-{int(time.time() * 1000)}")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _normalize_targets(values: Any) -> list[str]:
    if isinstance(values, dict):
        iterable = values.keys()
    elif isinstance(values, (list, tuple, set)):
        iterable = values
    else:
        iterable = []
    out: list[str] = []
    seen: set[str] = set()
    for value in iterable:
        text = str(value or "").strip().lower()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _collect_targets(*payloads: dict[str, Any]) -> list[str]:
    inferred_sets: list[list[str]] = []
    for payload in payloads:
        for candidate in (
            payload.get("model_context"),
            payload.get("thresholds_meta"),
            payload.get("thresholds"),
            payload.get("stats"),
        ):
            targets = _normalize_targets(candidate)
            if targets:
                inferred_sets.append(targets)
                break
    if not inferred_sets:
        raise RuntimeError("Could not infer targets from metadata or manifest")
    first = inferred_sets[0]
    for other in inferred_sets[1:]:
        if other != first:
            raise RuntimeError(f"Inconsistent targets across artifact files: {first} vs {other}")
    return first


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _collect_trade_costs(*payloads: dict[str, Any]) -> list[float]:
    values: list[float] = []
    for payload in payloads:
        direct_candidates = [
            payload.get("threshold_trade_cost_bps"),
            ((payload.get("model_context") or {}).get("trade_cost_bps") if isinstance(payload.get("model_context"), dict) else None),
        ]
        for candidate in direct_candidates:
            value = _to_float(candidate)
            if value is not None:
                values.append(value)
        thresholds_meta = payload.get("thresholds_meta") or {}
        if isinstance(thresholds_meta, dict):
            for horizon_map in thresholds_meta.values():
                if not isinstance(horizon_map, dict):
                    continue
                for meta in horizon_map.values():
                    if not isinstance(meta, dict):
                        continue
                    value = _to_float(meta.get("trade_cost_bps"))
                    if value is not None:
                        values.append(value)
    unique = sorted({round(value, 12) for value in values})
    return [float(value) for value in unique]


def _load_cost_models(path: Path) -> dict[str, dict[str, Any]]:
    payload = _load_json(path)
    out: dict[str, dict[str, Any]] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            out[str(key)] = value
    if not out:
        raise RuntimeError(f"No cost models found in {path}")
    return out


def _resolve_cost_model_version(
    *,
    explicit_version: str | None,
    trade_cost_bps: float,
    lineage: dict[str, Any],
    cost_models: dict[str, dict[str, Any]],
) -> str:
    if explicit_version:
        candidate = cost_models.get(explicit_version)
        if not isinstance(candidate, dict):
            raise RuntimeError(f"Unknown cost model version {explicit_version!r}")
        configured_trade_cost = _to_float(candidate.get("trade_cost_bps"))
        if configured_trade_cost is None or abs(configured_trade_cost - trade_cost_bps) > 1e-9:
            raise RuntimeError(
                f"Cost model {explicit_version!r} trade_cost_bps={configured_trade_cost} does not match artifact trade_cost_bps={trade_cost_bps}"
            )
        return explicit_version

    lineage_cost = lineage.get("cost_model") or {}
    lineage_version = str(lineage_cost.get("version") or "").strip() or None
    lineage_trade_cost = _to_float(lineage_cost.get("trade_cost_bps"))
    if lineage_version:
        candidate = cost_models.get(lineage_version)
        configured_trade_cost = _to_float((candidate or {}).get("trade_cost_bps"))
        if configured_trade_cost is not None and abs(configured_trade_cost - trade_cost_bps) <= 1e-9:
            return lineage_version
        if lineage_trade_cost is not None and abs(lineage_trade_cost - trade_cost_bps) <= 1e-9 and lineage_version in cost_models:
            return lineage_version

    matches = [
        version
        for version, payload in cost_models.items()
        if _to_float(payload.get("trade_cost_bps")) is not None
        and abs(float(payload["trade_cost_bps"]) - trade_cost_bps) <= 1e-9
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise RuntimeError(f"No cost model matches artifact trade_cost_bps={trade_cost_bps}")
    raise RuntimeError(f"Ambiguous cost model for trade_cost_bps={trade_cost_bps}: {matches}")


def _infer_default_symbol(explicit_symbol: str | None, lineage: dict[str, Any]) -> str:
    if explicit_symbol:
        symbol = str(explicit_symbol).strip().upper()
        if symbol:
            return symbol

    lineage_symbols = [str(value).strip().upper() for value in (lineage.get("symbols") or []) if str(value).strip()]
    lineage_symbols = sorted(dict.fromkeys(lineage_symbols))
    if len(lineage_symbols) == 1:
        return lineage_symbols[0]

    source_candidates = [
        str(lineage.get("source_db") or ""),
        str((lineage.get("source_db_metadata") or {}).get("source_db") or ""),
        str(lineage.get("duckdb_path") or ""),
    ]
    known_symbols = ("SPY", "SPX", "QQQ", "IWM", "DIA", "NDX", "RUT")
    matches: set[str] = set()
    for raw_value in source_candidates:
        value = raw_value.lower()
        for symbol in known_symbols:
            token = symbol.lower()
            if any(marker in value for marker in (f"_{token}_", f"/{token}_", f"_{token}.", f"/{token}.")):
                matches.add(symbol)
    if len(matches) == 1:
        return next(iter(matches))
    raise RuntimeError("Could not infer default_symbol safely; pass --default-symbol explicitly")


def _resolve_source_duckdb_path(explicit_path: str | None, lineage: dict[str, Any]) -> str:
    candidate = str(explicit_path or lineage.get("duckdb_path") or "").strip()
    if not candidate:
        raise RuntimeError("Could not resolve source_duckdb_path; pass --source-duckdb-path explicitly")
    return str(Path(candidate).expanduser())


def _resolve_training_view(explicit_view: str | None) -> str:
    value = str(explicit_view or DEFAULT_TRAINING_VIEW).strip()
    if not value:
        raise RuntimeError("Could not resolve training_view")
    return value


def _build_model_context(
    *,
    targets: list[str],
    default_symbol: str,
    cost_model_version: str,
    trade_cost_bps: float,
    training_view: str,
    source_duckdb_path: str,
) -> dict[str, Any]:
    return validate_model_context(
        {
            "metadata_mode": "explicit",
            "symbols": [default_symbol],
            "default_symbol": default_symbol,
            "targets": targets,
            "cost_model_version": cost_model_version,
            "trade_cost_bps": float(trade_cost_bps),
            "training_view": training_view,
            "source_duckdb_path": source_duckdb_path,
        }
    )


def backfill_registry(
    *,
    registry_root: Path,
    lineage_path: Path,
    cost_models_path: Path,
    default_symbol: str | None = None,
    source_duckdb_path: str | None = None,
    training_view: str | None = None,
    cost_model_version: str | None = None,
    write: bool = False,
) -> dict[str, Any]:
    lineage = _load_json(lineage_path)
    cost_models = _load_cost_models(cost_models_path)

    resolved_symbol = _infer_default_symbol(default_symbol, lineage)
    resolved_source_duckdb_path = _resolve_source_duckdb_path(source_duckdb_path, lineage)
    resolved_training_view = _resolve_training_view(training_view)

    metadata_files = sorted(registry_root.rglob("metadata_*.json"))
    summary: dict[str, Any] = {
        "registry_root": str(registry_root),
        "lineage_path": str(lineage_path),
        "cost_models_path": str(cost_models_path),
        "write": bool(write),
        "artifacts_seen": len(metadata_files),
        "updated": [],
        "skipped": [],
        "failed": [],
    }

    for metadata_path in metadata_files:
        manifest_path = metadata_path.parent.parent / "manifest_runtime_latest.json"
        artifact_id = str(metadata_path.parent.parent.relative_to(registry_root))
        try:
            metadata_payload = _load_json(metadata_path)
            manifest_payload = _load_json(manifest_path)

            metadata_context = metadata_payload.get("model_context")
            manifest_context = manifest_payload.get("model_context")
            if isinstance(metadata_context, dict) and isinstance(manifest_context, dict):
                validate_model_context(metadata_context)
                validate_model_context(manifest_context)
                summary["skipped"].append({
                    "artifact": artifact_id,
                    "reason": "model_context_present",
                })
                continue

            targets = _collect_targets(metadata_payload, manifest_payload)
            trade_costs = _collect_trade_costs(metadata_payload, manifest_payload)
            if not trade_costs:
                raise RuntimeError("Could not infer trade_cost_bps")
            if len(trade_costs) != 1:
                raise RuntimeError(f"Inconsistent trade_cost_bps values: {trade_costs}")
            trade_cost_bps = trade_costs[0]

            resolved_cost_model_version = _resolve_cost_model_version(
                explicit_version=cost_model_version,
                trade_cost_bps=trade_cost_bps,
                lineage=lineage,
                cost_models=cost_models,
            )
            model_context = _build_model_context(
                targets=targets,
                default_symbol=resolved_symbol,
                cost_model_version=resolved_cost_model_version,
                trade_cost_bps=trade_cost_bps,
                training_view=resolved_training_view,
                source_duckdb_path=resolved_source_duckdb_path,
            )

            updated_metadata = {**metadata_payload, "model_context": model_context}
            updated_manifest = {**manifest_payload, "model_context": model_context}

            if write:
                _write_json_atomic(metadata_path, updated_metadata)
                _write_json_atomic(manifest_path, updated_manifest)

            summary["updated"].append(
                {
                    "artifact": artifact_id,
                    "metadata_path": str(metadata_path),
                    "manifest_path": str(manifest_path),
                    "model_context": model_context,
                }
            )
        except (RuntimeError, ModelMetadataError, FileNotFoundError, json.JSONDecodeError) as exc:
            summary["failed"].append(
                {
                    "artifact": artifact_id,
                    "metadata_path": str(metadata_path),
                    "manifest_path": str(manifest_path),
                    "error": str(exc),
                }
            )

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill model_context into legacy model metadata/manifests.")
    parser.add_argument("--registry-root", default=str(DEFAULT_REGISTRY_ROOT))
    parser.add_argument("--research-lineage-path", default=str(DEFAULT_LINEAGE_PATH))
    parser.add_argument("--cost-models-path", default=str(DEFAULT_COST_MODELS_PATH))
    parser.add_argument("--default-symbol", default=os.getenv("MODEL_DEFAULT_SYMBOL", "").strip().upper() or None)
    parser.add_argument("--source-duckdb-path", default=os.getenv("DUCKDB_PATH") or None)
    parser.add_argument("--training-view", default=DEFAULT_TRAINING_VIEW)
    parser.add_argument("--cost-model-version", default=os.getenv("RESEARCH_COST_MODEL_VERSION") or None)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    summary = backfill_registry(
        registry_root=Path(args.registry_root).expanduser(),
        lineage_path=Path(args.research_lineage_path).expanduser(),
        cost_models_path=Path(args.cost_models_path).expanduser(),
        default_symbol=args.default_symbol,
        source_duckdb_path=args.source_duckdb_path,
        training_view=args.training_view,
        cost_model_version=args.cost_model_version,
        write=bool(args.write),
    )
    print(json.dumps(summary, indent=2))
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
