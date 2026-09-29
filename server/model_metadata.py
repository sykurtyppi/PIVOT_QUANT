from __future__ import annotations

import os
from pathlib import Path
from typing import Any


DEFAULT_LEGACY_SYMBOL = os.getenv("MODEL_DEFAULT_SYMBOL", "SPY").strip().upper() or "SPY"
DEFAULT_RESEARCH_COST_MODEL_VERSION = os.getenv("RESEARCH_COST_MODEL_VERSION", "rt_cost_v1").strip() or "rt_cost_v1"
DEFAULT_TRAINING_VIEW = os.getenv("DUCKDB_VIEW", "training_events_v1").strip() or "training_events_v1"


class ModelMetadataError(ValueError):
    pass


def allow_legacy_model_metadata() -> bool:
    raw = str(os.getenv("ALLOW_LEGACY_MODEL_METADATA", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _normalize_strings(values: Any, *, upper: bool = False) -> list[str]:
    if isinstance(values, str):
        candidate_values = [values]
    elif isinstance(values, (list, tuple, set)):
        candidate_values = list(values)
    else:
        return []
    normalized: list[str] = []
    for value in candidate_values:
        text = str(value or "").strip()
        if not text:
            continue
        normalized.append(text.upper() if upper else text.lower())
    deduped: list[str] = []
    seen: set[str] = set()
    for value in normalized:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _infer_targets(payload: dict[str, Any]) -> list[str]:
    candidate_maps = [payload.get("thresholds_meta"), payload.get("thresholds"), payload.get("stats")]
    targets: list[str] = []
    for candidate_map in candidate_maps:
        if not isinstance(candidate_map, dict):
            continue
        for key in candidate_map.keys():
            text = str(key or "").strip().lower()
            if text:
                targets.append(text)
    return _normalize_strings(targets)


def _infer_trade_cost_bps(payload: dict[str, Any], fallback_trade_cost_bps: float | None) -> float | None:
    candidates = [
        payload.get("threshold_trade_cost_bps"),
        (payload.get("lineage") or {}).get("threshold_trade_cost_bps") if isinstance(payload.get("lineage"), dict) else None,
        ((payload.get("model_context") or {}).get("trade_cost_bps") if isinstance(payload.get("model_context"), dict) else None),
        fallback_trade_cost_bps,
    ]
    for candidate in candidates:
        value = _as_float(candidate)
        if value is not None:
            return value
    return None


def normalize_model_context(
    payload: dict[str, Any],
    *,
    fallback_symbol: str | None = None,
    fallback_cost_model_version: str | None = None,
    fallback_trade_cost_bps: float | None = None,
    fallback_training_view: str | None = None,
    fallback_source_duckdb_path: str | None = None,
    allow_legacy: bool | None = None,
) -> dict[str, Any]:
    model_context = payload.get("model_context")
    if isinstance(model_context, dict):
        symbols = _normalize_strings(model_context.get("symbols"), upper=True)
        default_symbol = str(model_context.get("default_symbol") or "").strip().upper() or None
        if default_symbol and default_symbol not in symbols:
            symbols = [*symbols, default_symbol]
        if not default_symbol and len(symbols) == 1:
            default_symbol = symbols[0]

        targets = _normalize_strings(model_context.get("targets"))
        cost_model_version = str(
            model_context.get("cost_model_version")
            or fallback_cost_model_version
            or DEFAULT_RESEARCH_COST_MODEL_VERSION
        ).strip()
        trade_cost_bps = _infer_trade_cost_bps(payload, fallback_trade_cost_bps)
        training_view = str(
            model_context.get("training_view")
            or fallback_training_view
            or DEFAULT_TRAINING_VIEW
        ).strip()
        source_duckdb_path = str(
            model_context.get("source_duckdb_path")
            or fallback_source_duckdb_path
            or ""
        ).strip()

        return {
            "metadata_mode": "explicit",
            "symbols": symbols,
            "default_symbol": default_symbol,
            "targets": targets,
            "cost_model_version": cost_model_version,
            "trade_cost_bps": trade_cost_bps,
            "training_view": training_view,
            "source_duckdb_path": str(Path(source_duckdb_path).expanduser()) if source_duckdb_path else None,
        }

    use_legacy = allow_legacy_model_metadata() if allow_legacy is None else bool(allow_legacy)
    if not use_legacy:
        raise ModelMetadataError("Artifact metadata missing model_context and legacy compatibility is disabled")

    inferred_symbol = str(fallback_symbol or DEFAULT_LEGACY_SYMBOL).strip().upper() or DEFAULT_LEGACY_SYMBOL
    inferred_targets = _infer_targets(payload)
    inferred_training_view = str(fallback_training_view or DEFAULT_TRAINING_VIEW).strip() or DEFAULT_TRAINING_VIEW
    inferred_source_duckdb = str(fallback_source_duckdb_path or "").strip()

    return {
        "metadata_mode": "legacy_inferred",
        "symbols": [inferred_symbol],
        "default_symbol": inferred_symbol,
        "targets": inferred_targets,
        "cost_model_version": str(fallback_cost_model_version or DEFAULT_RESEARCH_COST_MODEL_VERSION).strip() or DEFAULT_RESEARCH_COST_MODEL_VERSION,
        "trade_cost_bps": _infer_trade_cost_bps(payload, fallback_trade_cost_bps),
        "training_view": inferred_training_view,
        "source_duckdb_path": str(Path(inferred_source_duckdb).expanduser()) if inferred_source_duckdb else None,
    }


def validate_model_context(model_context: dict[str, Any]) -> dict[str, Any]:
    symbols = _normalize_strings(model_context.get("symbols"), upper=True)
    default_symbol = str(model_context.get("default_symbol") or "").strip().upper() or None
    targets = _normalize_strings(model_context.get("targets"))
    metadata_mode = str(model_context.get("metadata_mode") or "").strip() or "explicit"
    cost_model_version = str(model_context.get("cost_model_version") or "").strip()
    trade_cost_bps = _as_float(model_context.get("trade_cost_bps"))
    training_view = str(model_context.get("training_view") or "").strip()
    source_duckdb_path = str(model_context.get("source_duckdb_path") or "").strip()

    if not symbols:
        raise ModelMetadataError("Model context must include at least one symbol")
    if not default_symbol:
        if len(symbols) == 1:
            default_symbol = symbols[0]
        else:
            raise ModelMetadataError("Model context must include default_symbol when multiple symbols are present")
    if default_symbol not in symbols:
        raise ModelMetadataError("Model context default_symbol must be present in symbols")
    if not targets:
        raise ModelMetadataError("Model context must include at least one target")
    if not cost_model_version:
        raise ModelMetadataError("Model context must include cost_model_version")
    if trade_cost_bps is None:
        raise ModelMetadataError("Model context must include trade_cost_bps")
    if not training_view:
        raise ModelMetadataError("Model context must include training_view")
    if not source_duckdb_path:
        raise ModelMetadataError("Model context must include source_duckdb_path")

    return {
        "metadata_mode": metadata_mode,
        "symbols": symbols,
        "default_symbol": default_symbol,
        "targets": targets,
        "cost_model_version": cost_model_version,
        "trade_cost_bps": trade_cost_bps,
        "training_view": training_view,
        "source_duckdb_path": str(Path(source_duckdb_path).expanduser()),
    }


def resolve_model_context(
    payload: dict[str, Any],
    *,
    fallback_symbol: str | None = None,
    fallback_cost_model_version: str | None = None,
    fallback_trade_cost_bps: float | None = None,
    fallback_training_view: str | None = None,
    fallback_source_duckdb_path: str | None = None,
    allow_legacy: bool | None = None,
) -> dict[str, Any]:
    return validate_model_context(
        normalize_model_context(
            payload,
            fallback_symbol=fallback_symbol,
            fallback_cost_model_version=fallback_cost_model_version,
            fallback_trade_cost_bps=fallback_trade_cost_bps,
            fallback_training_view=fallback_training_view,
            fallback_source_duckdb_path=fallback_source_duckdb_path,
            allow_legacy=allow_legacy,
        )
    )
