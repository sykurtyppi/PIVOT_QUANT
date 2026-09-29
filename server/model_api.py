from __future__ import annotations

import hashlib
import json
import os
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from server.model_metadata import ModelMetadataError, resolve_model_context
from server.model_registry_snapshot import RegistryRecord, RegistrySnapshot, get_registry_snapshot
from server.model_review_store import (
    append_review as append_review_store_entry,
    get_review as get_review_store_entry,
    import_reviews as import_review_entries,
    list_reviews as list_store_reviews,
    read_jsonl_reviews,
    review_count as review_store_count,
)


def require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception as exc:  # pragma: no cover - import guard
        raise SystemExit(f"{module_name} not installed. Install with: {hint}") from exc


DUCKDB = require("duckdb", "python3 -m pip install duckdb")


def _default_registry_roots() -> list[Path]:
    raw = os.getenv("MODEL_REGISTRY_ROOTS") or os.getenv("MODEL_REGISTRY_ROOT")
    if raw:
        parts = [part.strip() for part in raw.replace(";", ",").split(",") if part.strip()]
        return [Path(part).expanduser() for part in parts]
    return [ROOT / "data" / "models", ROOT / "data" / "t9_experiments"]


def _default_duckdb_path() -> Path:
    raw = os.getenv("DUCKDB_PATH")
    if raw:
        return Path(raw).expanduser()
    return ROOT / "data" / "pivot_training.duckdb"


def _default_research_lineage_path() -> Path:
    raw = os.getenv("RESEARCH_LINEAGE_PATH")
    if raw:
        return Path(raw).expanduser()
    marts_dir = os.getenv("RESEARCH_MARTS_DIR")
    if marts_dir:
        return Path(marts_dir).expanduser() / "last_build.json"
    return ROOT / "data" / "research_marts" / "last_build.json"


def _default_review_log_path() -> Path:
    raw = os.getenv("MODEL_REVIEW_LOG_PATH")
    if raw:
        return Path(raw).expanduser()
    return ROOT / "data" / "model_lab" / "review_log.jsonl"


def _default_review_store_path() -> Path:
    raw = os.getenv("MODEL_REVIEW_DB_PATH")
    if raw:
        return Path(raw).expanduser()
    return ROOT / "data" / "model_lab" / "review_store.sqlite"


def _default_review_backend() -> str:
    raw = str(os.getenv("MODEL_REVIEW_BACKEND", "sqlite")).strip().lower()
    return raw if raw in {"sqlite", "jsonl"} else "sqlite"


REGISTRY_ROOTS = _default_registry_roots()
DUCKDB_PATH = _default_duckdb_path()
RESEARCH_LINEAGE_PATH = _default_research_lineage_path()
REVIEW_LOG_PATH = _default_review_log_path()
REVIEW_STORE_PATH = _default_review_store_path()
REVIEW_BACKEND = _default_review_backend()
HOST = os.getenv("MODEL_API_BIND", "127.0.0.1")
PORT = int(os.getenv("MODEL_API_PORT", "5006"))


app = FastAPI(title="PIVOT_QUANT Model API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:3000", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ReviewLogRequest(BaseModel):
    id: str = Field(..., min_length=1)
    reviewer: str = Field(default="model_lab")
    note: str = Field(default="", max_length=2000)


def _iso_from_epoch_ms(value: Any) -> str | None:
    try:
        if value is None:
            return None
        ts = float(value) / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _registry_roots() -> list[Path]:
    return [path for path in REGISTRY_ROOTS if path.exists()]

def _safe_repo_relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path.resolve())


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return None
    return None


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _connect_research():
    if not DUCKDB_PATH.exists():
        raise HTTPException(status_code=503, detail=f"DuckDB not found at {DUCKDB_PATH}")
    return DUCKDB.connect(str(DUCKDB_PATH), read_only=True)


def _read_research_lineage() -> dict[str, Any] | None:
    if not RESEARCH_LINEAGE_PATH.exists():
        return None
    try:
        payload = json.loads(RESEARCH_LINEAGE_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return None
    return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _review_storage_lineage() -> dict[str, Any]:
    return {
        "review_backend": REVIEW_BACKEND,
        "review_log_path": str(REVIEW_LOG_PATH),
        "review_store_path": str(REVIEW_STORE_PATH),
    }


def _bootstrap_sqlite_reviews_from_jsonl() -> dict[str, int] | None:
    if REVIEW_BACKEND != "sqlite":
        return None
    if review_store_count(REVIEW_STORE_PATH) > 0:
        return None
    legacy_entries = read_jsonl_reviews(REVIEW_LOG_PATH)
    if not legacy_entries:
        return None
    return import_review_entries(REVIEW_STORE_PATH, legacy_entries)


def _review_log_entries() -> list[dict[str, Any]]:
    if REVIEW_BACKEND == "jsonl":
        return read_jsonl_reviews(REVIEW_LOG_PATH)
    _bootstrap_sqlite_reviews_from_jsonl()
    entries = list_store_reviews(REVIEW_STORE_PATH)
    if entries:
        return entries
    return read_jsonl_reviews(REVIEW_LOG_PATH)


def _append_review_log(entry_body: dict[str, Any]) -> dict[str, Any]:
    if REVIEW_BACKEND == "jsonl":
        prev_hash = ""
        existing_entries = read_jsonl_reviews(REVIEW_LOG_PATH)
        if existing_entries:
            prev_hash = str(existing_entries[-1].get("entry_hash") or "")
        body = {**entry_body, "prev_hash": prev_hash}
        entry_hash = hashlib.sha256(_canonical_json(body).encode("utf-8")).hexdigest()
        review_id = f"{body['recorded_at_utc'].replace(':', '').replace('-', '')}_{entry_hash[:12]}"
        entry = {**body, "review_id": review_id, "entry_hash": entry_hash}
        REVIEW_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with REVIEW_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=True) + "\n")
        return entry
    _bootstrap_sqlite_reviews_from_jsonl()
    return append_review_store_entry(REVIEW_STORE_PATH, entry_body)


def _find_review_entry(review_id: str) -> dict[str, Any]:
    if REVIEW_BACKEND == "sqlite":
        _bootstrap_sqlite_reviews_from_jsonl()
        entry = get_review_store_entry(REVIEW_STORE_PATH, review_id)
        if entry:
            return entry
    for entry in read_jsonl_reviews(REVIEW_LOG_PATH):
        if str(entry.get("review_id") or "") == review_id:
            return entry
    raise HTTPException(status_code=404, detail="Review entry not found")


def _parse_rank_pair(value: Any) -> tuple[int | None, int | None]:
    text = str(value or "").strip()
    if "/" not in text:
        return None, None
    left, right = text.split("/", 1)
    try:
        return int(left), int(right)
    except Exception:
        return None, None


def _extract_horizon_details(payload: dict[str, Any]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    thresholds = payload.get("thresholds") or {}
    threshold_meta = payload.get("thresholds_meta") or {}
    calibration = payload.get("calibration") or {}

    for target, horizon_map in threshold_meta.items():
        if not isinstance(horizon_map, dict):
            continue
        for horizon, meta in horizon_map.items():
            if not isinstance(meta, dict):
                continue
            threshold_value = ((thresholds.get(target) or {}) if isinstance(thresholds.get(target), dict) else {}).get(str(horizon))
            calib_value = ((calibration.get(target) or {}) if isinstance(calibration.get(target), dict) else {}).get(str(horizon))
            details.append(
                {
                    "target": str(target),
                    "horizon": int(horizon),
                    "threshold": threshold_value,
                    "calibration": calib_value or "none",
                    "guard_applied": bool(meta.get("guard_applied")),
                    "guard_reason": meta.get("guard_reason"),
                    "objective": meta.get("objective"),
                    "score": meta.get("score"),
                    "precision": meta.get("precision"),
                    "recall": meta.get("recall"),
                    "signals": meta.get("signals"),
                    "selected_utility_avg": meta.get("selected_utility_avg"),
                    "selected_utility_sum": meta.get("selected_utility_sum"),
                    "selected_tp_count": meta.get("selected_tp_count"),
                    "selected_fp_count": meta.get("selected_fp_count"),
                    "tune_prob_utility_corr_all": meta.get("tune_prob_utility_corr_all"),
                    "tune_prob_utility_corr_pos": meta.get("tune_prob_utility_corr_pos"),
                    "top_candidates": meta.get("top_candidates") or [],
                }
            )
    return sorted(details, key=lambda row: (row["target"], row["horizon"]))


def _extract_shadow_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    shadow_policies = payload.get("shadow_policies") or {}
    if not isinstance(shadow_policies, dict):
        return rows

    for policy_name, policy_payload in shadow_policies.items():
        if not isinstance(policy_payload, dict):
            continue
        for side in ("reject", "break"):
            side_payload = policy_payload.get(side)
            if not isinstance(side_payload, dict):
                continue
            rows.append(
                {
                    "policy_name": str(policy_name),
                    "side": side,
                    "horizon": int(side_payload.get("horizon") or policy_payload.get("horizon") or 0),
                    "scope": policy_payload.get("scope"),
                    "status": side_payload.get("status"),
                    "reason": side_payload.get("reason"),
                    "reference_threshold": side_payload.get("reference_threshold"),
                    "margin_cutoff": side_payload.get("margin_cutoff"),
                    "eligible_rows": side_payload.get("eligible_rows"),
                    "emitted_rows": side_payload.get("emitted_rows"),
                    "emitted_positive_rate": side_payload.get("emitted_positive_rate"),
                    "emitted_utility_avg": side_payload.get("emitted_utility_avg"),
                    "emitted_utility_sum": side_payload.get("emitted_utility_sum"),
                    "percentile_cutoff": side_payload.get("percentile_cutoff") or policy_payload.get("percentile_cutoff"),
                }
            )
    return sorted(rows, key=lambda row: (row["policy_name"], row["side"], row["horizon"]))


def _extract_stats_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    stats = payload.get("stats") or {}
    if not isinstance(stats, dict):
        return rows

    for horizon_key, target_map in stats.items():
        if not isinstance(target_map, dict):
            continue
        for target, target_stats in target_map.items():
            if not isinstance(target_stats, dict):
                continue

            positive_rate = _as_float(target_stats.get(f"{target}_rate"))
            sample_size = target_stats.get("sample_size")
            mfe_pos = _as_float(target_stats.get(f"mfe_bps_{target}"))
            mfe_other = _as_float(target_stats.get(f"mfe_bps_{target}_other"))
            mae_pos = _as_float(target_stats.get(f"mae_bps_{target}"))
            mae_other = _as_float(target_stats.get(f"mae_bps_{target}_other"))
            mfe_gap = (mfe_pos - mfe_other) if mfe_pos is not None and mfe_other is not None else None
            mae_advantage = (abs(mae_other) - abs(mae_pos)) if mae_pos is not None and mae_other is not None else None

            best_regime = None
            best_score = None
            regime_rows: list[dict[str, Any]] = []
            by_regime = target_stats.get("by_regime") or {}
            if isinstance(by_regime, dict):
                for regime_name, regime_stats in by_regime.items():
                    if not isinstance(regime_stats, dict):
                        continue
                    regime_mfe = _as_float(regime_stats.get(f"mfe_bps_{target}"))
                    regime_mfe_other = _as_float(regime_stats.get(f"mfe_bps_{target}_other"))
                    regime_mae = _as_float(regime_stats.get(f"mae_bps_{target}"))
                    regime_mae_other = _as_float(regime_stats.get(f"mae_bps_{target}_other"))
                    regime_mfe_gap = (regime_mfe - regime_mfe_other) if regime_mfe is not None and regime_mfe_other is not None else None
                    regime_mae_adv = (abs(regime_mae_other) - abs(regime_mae)) if regime_mae is not None and regime_mae_other is not None else None
                    separation_score = (regime_mfe_gap or 0.0) + (regime_mae_adv or 0.0) if (regime_mfe_gap is not None or regime_mae_adv is not None) else None
                    regime_rows.append(
                        {
                            "regime_bucket": str(regime_name),
                            "sample_size": regime_stats.get("sample_size"),
                            "sample_share": regime_stats.get("sample_share"),
                            "positive_rate": regime_stats.get(f"{target}_rate"),
                            "mfe_gap_bps": regime_mfe_gap,
                            "mae_advantage_bps": regime_mae_adv,
                            "separation_score": separation_score,
                        }
                    )
                eligible_rows = [
                    row for row in regime_rows
                    if row.get("separation_score") is not None
                    and ((_as_float(row.get("sample_share")) or 0.0) >= 0.05 or float(row.get("sample_size") or 0) >= 50)
                ]
                candidate_rows = eligible_rows if eligible_rows else [row for row in regime_rows if row.get("separation_score") is not None]
                if candidate_rows:
                    best_row = max(candidate_rows, key=lambda row: row["separation_score"])
                    best_regime = str(best_row["regime_bucket"])
                    best_score = best_row["separation_score"]

            rows.append(
                {
                    "target": str(target),
                    "horizon": int(horizon_key),
                    "sample_size": sample_size,
                    "positive_rate": positive_rate,
                    "mfe_gap_bps": mfe_gap,
                    "mae_advantage_bps": mae_advantage,
                    "best_regime_by_separation": best_regime,
                    "best_regime_separation_score": best_score,
                    "regime_rows": sorted(regime_rows, key=lambda row: (row["separation_score"] or float("-inf")), reverse=True),
                }
            )
    return sorted(rows, key=lambda row: (row["target"], row["horizon"]))


def _build_registry_entry(
    metadata_path: Path,
    payload: dict[str, Any],
    candidate_version: str | None = None,
) -> dict[str, Any]:
    version = str(payload.get("version") or metadata_path.stem.replace("metadata_", ""))
    horizon_details = _extract_horizon_details(payload)
    guarded_pairs = sum(1 for detail in horizon_details if detail.get("guard_applied"))
    utility_values = [detail.get("selected_utility_avg") for detail in horizon_details if detail.get("selected_utility_avg") is not None]
    trained_end_ts = payload.get("trained_end_ts")

    return {
        "id": _safe_repo_relative(metadata_path),
        "path": str(metadata_path),
        "family": metadata_path.parents[1].name if len(metadata_path.parents) >= 2 else metadata_path.parent.name,
        "version": version,
        "feature_version": payload.get("feature_version"),
        "trained_end_ts": trained_end_ts,
        "trained_end_utc": _iso_from_epoch_ms(trained_end_ts),
        "tune_date_range": payload.get("tune_date_range") or {},
        "pair_count": len(horizon_details),
        "guarded_pairs": guarded_pairs,
        "active_pairs": max(0, len(horizon_details) - guarded_pairs),
        "mean_selected_utility_avg": (sum(float(v) for v in utility_values) / len(utility_values)) if utility_values else None,
        "status": "blocked" if guarded_pairs and guarded_pairs == len(horizon_details) else ("guarded" if guarded_pairs else "candidate"),
        "is_family_latest": candidate_version == version if candidate_version else False,
        "candidate_manifest_path": _safe_repo_relative(metadata_path.parent.parent / "manifest_runtime_latest.json"),
    }


def _registry_snapshot() -> RegistrySnapshot:
    return get_registry_snapshot(
        roots=_registry_roots(),
        build_entry=_build_registry_entry,
        extract_horizon_details=_extract_horizon_details,
    )


def _snapshot_record_to_dict(record: RegistryRecord) -> dict[str, Any]:
    return {
        "metadata_path": record.metadata_path,
        "manifest_path": record.manifest_path,
        "manifest_payload": record.manifest_payload,
        "manifest_version": record.manifest_version,
        "payload": record.payload,
        "entry": record.entry,
        "horizon_details": record.horizon_details,
    }


def _load_registry_records(snapshot: RegistrySnapshot | None = None) -> list[dict[str, Any]]:
    active_snapshot = snapshot or _registry_snapshot()
    return [_snapshot_record_to_dict(record) for record in active_snapshot.records]


def _registry_entries(snapshot: RegistrySnapshot | None = None) -> list[dict[str, Any]]:
    return [record["entry"] for record in _load_registry_records(snapshot)]


def _registry_record_by_id(registry_id: str, snapshot: RegistrySnapshot | None = None) -> dict[str, Any]:
    active_snapshot = snapshot or _registry_snapshot()
    clean_id = str(registry_id or "").strip()
    record = active_snapshot.records_by_id.get(clean_id)
    if record is not None:
        return _snapshot_record_to_dict(record)

    resolved_id = str((ROOT / clean_id).resolve())
    for snapshot_record in active_snapshot.records:
        metadata_path = snapshot_record.metadata_path
        if clean_id in {str(metadata_path), str(metadata_path.resolve()), resolved_id}:
            return _snapshot_record_to_dict(snapshot_record)
    raise HTTPException(status_code=404, detail="Model artifact not found in configured registry roots")


def _research_cost_model_context() -> dict[str, Any]:
    research_lineage = _read_research_lineage() or {}
    cost_model = research_lineage.get("cost_model")
    if not isinstance(cost_model, dict):
        cost_model = {}
    return {
        "cost_model_version": str(cost_model.get("version") or os.getenv("RESEARCH_COST_MODEL_VERSION", "rt_cost_v1")).strip() or "rt_cost_v1",
        "trade_cost_bps": _as_float(cost_model.get("trade_cost_bps")),
        "source_duckdb_path": str(DUCKDB_PATH),
        "training_view": os.getenv("DUCKDB_VIEW", "training_events_v1"),
    }


def _normalize_path_string(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return str(Path(text).expanduser())


def _lineage_match_summary(model_context: dict[str, Any], research_lineage: dict[str, Any] | None) -> dict[str, Any]:
    research_lineage = research_lineage if isinstance(research_lineage, dict) else {}
    active_duckdb_path = _normalize_path_string(DUCKDB_PATH)
    model_duckdb_path = _normalize_path_string(model_context.get("source_duckdb_path"))
    active_cost_model_version = str(
        ((research_lineage.get("cost_model") or {}) if isinstance(research_lineage.get("cost_model"), dict) else {}).get("version")
        or os.getenv("RESEARCH_COST_MODEL_VERSION", "rt_cost_v1")
    ).strip() or "rt_cost_v1"
    model_cost_model_version = str(model_context.get("cost_model_version") or "").strip() or None
    source_duckdb_matches = (
        active_duckdb_path == model_duckdb_path if active_duckdb_path is not None and model_duckdb_path is not None else None
    )
    cost_model_matches = (
        active_cost_model_version == model_cost_model_version
        if active_cost_model_version is not None and model_cost_model_version is not None
        else None
    )
    return {
        "source_duckdb_matches": source_duckdb_matches,
        "cost_model_matches": cost_model_matches,
        "active_duckdb_path": active_duckdb_path,
        "model_duckdb_path": model_duckdb_path,
        "active_cost_model_version": active_cost_model_version,
        "model_cost_model_version": model_cost_model_version,
    }


def _requires_model_context_backfill(model_context: dict[str, Any]) -> bool:
    return str(model_context.get("metadata_mode") or "").strip().lower() != "explicit"


def _preferred_regime_baseline_value(summary: dict[str, Any]) -> float | None:
    preferred_generic = _as_float(summary.get("preferred_regime_avg_net_bps"))
    if preferred_generic is not None:
        return preferred_generic
    return _as_float(summary.get("preferred_regime_avg_reject_net_bps"))


def _resolve_record_model_context(record: dict[str, Any]) -> dict[str, Any]:
    lineage_context = _research_cost_model_context()
    try:
        return resolve_model_context(
            record.get("payload") or {},
            fallback_symbol=os.getenv("MODEL_DEFAULT_SYMBOL", "SPY"),
            fallback_cost_model_version=lineage_context.get("cost_model_version"),
            fallback_trade_cost_bps=lineage_context.get("trade_cost_bps"),
            fallback_training_view=lineage_context.get("training_view"),
            fallback_source_duckdb_path=lineage_context.get("source_duckdb_path"),
        )
    except ModelMetadataError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _baseline_metric_fields_for_target(target: str) -> tuple[str, str]:
    normalized = str(target or "").strip().lower()
    if normalized == "reject":
        return "avg_reject_net_bps", "win_rate_reject"
    if normalized == "break":
        return "avg_break_net_bps", "win_rate_break"
    raise HTTPException(status_code=422, detail=f"Unsupported model target for evaluation: {target}")


def _preferred_target_for_horizon(horizon_rows: list[dict[str, Any]], preferred_horizon: Any, model_context: dict[str, Any]) -> str | None:
    candidate_rows = [row for row in horizon_rows if row.get("horizon") == preferred_horizon]
    supported_targets = [str(target).lower() for target in (model_context.get("targets") or [])]
    for target in supported_targets:
        if any(str(row.get("target") or "").lower() == target for row in candidate_rows):
            return target
    if candidate_rows:
        return str(candidate_rows[0].get("target") or "").lower() or None
    return supported_targets[0] if supported_targets else None


def _median_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def _build_benchmarks(
    registry_id: str,
    *,
    snapshot: RegistrySnapshot | None = None,
    selected_record: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_snapshot = snapshot or _registry_snapshot()
    selected = selected_record or _registry_record_by_id(registry_id, snapshot=active_snapshot)
    selected_entry = selected["entry"]
    selected_payload = selected["payload"]
    selected_details = selected["horizon_details"]
    selected_stats = _extract_stats_rows(selected_payload)
    stats_by_key = {(row["target"], row["horizon"]): row for row in selected_stats}
    shadow_rows = _extract_shadow_rows(selected_payload)

    all_records = _load_registry_records(active_snapshot)
    overall_utilities = [
        {
            "id": record["entry"]["id"],
            "version": record["entry"]["version"],
            "family": record["entry"]["family"],
            "utility": _as_float(record["entry"].get("mean_selected_utility_avg")),
        }
        for record in all_records
        if _as_float(record["entry"].get("mean_selected_utility_avg")) is not None
    ]
    overall_sorted = sorted(overall_utilities, key=lambda row: row["utility"], reverse=True)
    selected_overall_utility = _as_float(selected_entry.get("mean_selected_utility_avg"))
    selected_overall_rank = None
    if selected_overall_utility is not None:
        for index, row in enumerate(overall_sorted, start=1):
            if row["id"] == selected_entry["id"]:
                selected_overall_rank = index
                break

    horizon_rows: list[dict[str, Any]] = []
    for detail in selected_details:
        key = (detail["target"], detail["horizon"])
        global_peers: list[dict[str, Any]] = []
        family_peers: list[dict[str, Any]] = []
        for record in all_records:
            entry = record["entry"]
            for peer_detail in record["horizon_details"]:
                if (peer_detail.get("target"), peer_detail.get("horizon")) != key:
                    continue
                peer_row = {
                    "id": entry["id"],
                    "version": entry["version"],
                    "family": entry["family"],
                    "utility": _as_float(peer_detail.get("selected_utility_avg")),
                    "precision": _as_float(peer_detail.get("precision")),
                    "guard_applied": bool(peer_detail.get("guard_applied")),
                }
                global_peers.append(peer_row)
                if entry.get("family") == selected_entry.get("family"):
                    family_peers.append(peer_row)

        def _rank_bundle(rows: list[dict[str, Any]]) -> dict[str, Any]:
            comparable = [row for row in rows if row["utility"] is not None]
            comparable_sorted = sorted(comparable, key=lambda row: row["utility"], reverse=True)
            selected_rank = None
            for index, row in enumerate(comparable_sorted, start=1):
                if row["id"] == selected_entry["id"]:
                    selected_rank = index
                    break
            best_row = comparable_sorted[0] if comparable_sorted else None
            utilities = [row["utility"] for row in comparable_sorted]
            positive_share = (sum(1 for value in utilities if value > 0) / len(utilities)) if utilities else None
            return {
                "peer_count": len(comparable_sorted),
                "selected_rank": selected_rank,
                "median_utility_avg": _median_or_none(utilities),
                "best_version": best_row["version"] if best_row else None,
                "best_utility_avg": best_row["utility"] if best_row else None,
                "positive_share": positive_share,
            }

        global_bundle = _rank_bundle(global_peers)
        family_bundle = _rank_bundle(family_peers)
        baseline = stats_by_key.get(key) or {}
        selected_utility = _as_float(detail.get("selected_utility_avg"))
        median_utility = global_bundle.get("median_utility_avg")
        delta_vs_median = (selected_utility - median_utility) if selected_utility is not None and median_utility is not None else None

        horizon_rows.append(
            {
                "target": detail["target"],
                "horizon": detail["horizon"],
                "selected_status": "guarded" if detail.get("guard_applied") else "active",
                "selected_utility_avg": selected_utility,
                "selected_precision": _as_float(detail.get("precision")),
                "selected_recall": _as_float(detail.get("recall")),
                "selected_signals": detail.get("signals"),
                "selected_corr_all": _as_float(detail.get("tune_prob_utility_corr_all")),
                "global_peer_count": global_bundle["peer_count"],
                "global_rank": global_bundle["selected_rank"],
                "global_median_utility_avg": global_bundle["median_utility_avg"],
                "global_best_version": global_bundle["best_version"],
                "global_best_utility_avg": global_bundle["best_utility_avg"],
                "global_positive_share": global_bundle["positive_share"],
                "family_peer_count": family_bundle["peer_count"],
                "family_rank": family_bundle["selected_rank"],
                "family_median_utility_avg": family_bundle["median_utility_avg"],
                "family_best_version": family_bundle["best_version"],
                "family_best_utility_avg": family_bundle["best_utility_avg"],
                "delta_vs_global_median": delta_vs_median,
                "baseline_positive_rate": baseline.get("positive_rate"),
                "baseline_sample_size": baseline.get("sample_size"),
                "baseline_mfe_gap_bps": baseline.get("mfe_gap_bps"),
                "baseline_mae_advantage_bps": baseline.get("mae_advantage_bps"),
                "best_regime_by_separation": baseline.get("best_regime_by_separation"),
                "best_regime_separation_score": baseline.get("best_regime_separation_score"),
            }
        )

    preferred_horizon = None
    if horizon_rows:
        preferred_row = max(
            horizon_rows,
            key=lambda row: (
                1 if row.get("selected_status") == "active" else 0,
                row.get("selected_utility_avg") if row.get("selected_utility_avg") is not None else float("-inf"),
            ),
        )
        preferred_horizon = preferred_row.get("horizon")

    return {
        "summary": {
            "registry_rank": selected_overall_rank,
            "registry_peer_count": len(overall_sorted),
            "registry_median_utility_avg": _median_or_none([row["utility"] for row in overall_sorted]),
            "registry_best_version": overall_sorted[0]["version"] if overall_sorted else None,
            "registry_best_utility_avg": overall_sorted[0]["utility"] if overall_sorted else None,
            "shadow_policy_count": len([row for row in shadow_rows if row.get("status") == "ok"]),
        },
        "horizon_rows": sorted(horizon_rows, key=lambda row: (row["target"], row["horizon"])),
        "shadow_rows": shadow_rows,
        "research_handoff": {
            "date_from": (selected_payload.get("tune_date_range") or {}).get("min_event_date_et"),
            "date_to": (selected_payload.get("tune_date_range") or {}).get("max_event_date_et"),
            "replay_date": (selected_payload.get("tune_date_range") or {}).get("max_event_date_et"),
            "preferred_horizon": preferred_horizon,
            "suggested_regime_bucket": next((row.get("best_regime_by_separation") for row in horizon_rows if row.get("horizon") == preferred_horizon and row.get("best_regime_by_separation")), None),
        },
    }


def _baseline_summary_for_window(
    *,
    symbol: str,
    date_from: str | None,
    date_to: str | None,
    horizon: int,
    regime_bucket: str | None = None,
) -> dict[str, Any]:
    clauses = ["symbol = ?", "horizon_min = ?"]
    params: list[Any] = [symbol.upper(), int(horizon)]
    if date_from:
        clauses.append("event_date_et >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        clauses.append("event_date_et <= CAST(? AS DATE)")
        params.append(date_to)
    if regime_bucket:
        clauses.append("regime_bucket = ?")
        params.append(regime_bucket)

    where_sql = " AND ".join(clauses)
    con = _connect_research()
    try:
        row = con.execute(
            f"""
            SELECT
                COUNT(*) AS days,
                SUM(rows_n) AS rows,
                SUM(avg_reject_net_bps * rows_n) / NULLIF(SUM(rows_n), 0) AS avg_reject_net_bps,
                SUM(win_rate_reject * rows_n) / NULLIF(SUM(rows_n), 0) AS win_rate_reject,
                SUM(avg_break_net_bps * rows_n) / NULLIF(SUM(rows_n), 0) AS avg_break_net_bps,
                SUM(win_rate_break * rows_n) / NULLIF(SUM(rows_n), 0) AS win_rate_break,
                SUM(avg_mfe_bps * rows_n) / NULLIF(SUM(rows_n), 0) AS avg_mfe_bps,
                SUM(avg_mae_bps * rows_n) / NULLIF(SUM(rows_n), 0) AS avg_mae_bps,
                AVG(avg_reject_net_bps) AS mean_daily_reject_net_bps,
                AVG(avg_break_net_bps) AS mean_daily_break_net_bps,
                SUM(CASE WHEN avg_reject_net_bps > 0 THEN 1 ELSE 0 END) AS positive_days,
                MIN(avg_reject_net_bps) AS worst_day_reject_net_bps,
                MAX(avg_reject_net_bps) AS best_day_reject_net_bps,
                MIN(avg_break_net_bps) AS worst_day_break_net_bps,
                MAX(avg_break_net_bps) AS best_day_break_net_bps
            FROM pq_research.mart_slice_expectancy_daily
            WHERE {where_sql}
            """,
            params,
        ).fetchone()
    finally:
        con.close()

    if not row:
        return {
            "days": 0,
            "rows": 0,
            "avg_reject_net_bps": None,
            "win_rate_reject": None,
            "avg_break_net_bps": None,
            "win_rate_break": None,
            "avg_mfe_bps": None,
            "avg_mae_bps": None,
            "mean_daily_reject_net_bps": None,
            "mean_daily_break_net_bps": None,
            "positive_days": 0,
            "worst_day_reject_net_bps": None,
            "best_day_reject_net_bps": None,
            "worst_day_break_net_bps": None,
            "best_day_break_net_bps": None,
        }

    columns = [
        "days",
        "rows",
        "avg_reject_net_bps",
        "win_rate_reject",
        "avg_break_net_bps",
        "win_rate_break",
        "avg_mfe_bps",
        "avg_mae_bps",
        "mean_daily_reject_net_bps",
        "mean_daily_break_net_bps",
        "positive_days",
        "worst_day_reject_net_bps",
        "best_day_reject_net_bps",
        "worst_day_break_net_bps",
        "best_day_break_net_bps",
    ]
    out: dict[str, Any] = {}
    for key, value in zip(columns, row):
        if isinstance(value, float):
            out[key] = round(value, 6)
        else:
            out[key] = value
    return out


def _build_baseline_compare(
    registry_id: str,
    *,
    snapshot: RegistrySnapshot | None = None,
    record: dict[str, Any] | None = None,
    benchmarks: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_snapshot = snapshot or _registry_snapshot()
    record = record or _registry_record_by_id(registry_id, snapshot=active_snapshot)
    model_context = _resolve_record_model_context(record)
    benchmarks = benchmarks or _build_benchmarks(
        registry_id,
        snapshot=active_snapshot,
        selected_record=record,
    )
    handoff = benchmarks.get("research_handoff") or {}
    date_from = handoff.get("date_from")
    date_to = handoff.get("date_to")
    symbol = str(model_context.get("default_symbol") or "").upper()
    research_lineage = _read_research_lineage()
    lineage_match = _lineage_match_summary(model_context, research_lineage)

    baseline_rows: list[dict[str, Any]] = []
    for row in benchmarks.get("horizon_rows") or []:
        selected_utility = _as_float(row.get("selected_utility_avg"))
        target = str(row.get("target") or "").lower()
        metric_field, win_rate_field = _baseline_metric_fields_for_target(target)
        all_regimes = _baseline_summary_for_window(
            symbol=symbol,
            date_from=date_from,
            date_to=date_to,
            horizon=int(row["horizon"]),
            regime_bucket=None,
        )
        baseline_avg = _as_float(all_regimes.get(metric_field))
        all_regimes.update(
            {
                "target": row["target"],
                "horizon": row["horizon"],
                "baseline_name": "all_regimes_window",
                "regime_bucket": None,
                "baseline_metric_name": metric_field,
                "baseline_win_rate_name": win_rate_field,
                "baseline_avg_net_bps": baseline_avg,
                "baseline_win_rate": _as_float(all_regimes.get(win_rate_field)),
                "model_selected_utility_avg": selected_utility,
                "delta_vs_model_selected_avg": (selected_utility - baseline_avg)
                if selected_utility is not None and baseline_avg is not None
                else None,
            }
        )
        baseline_rows.append(all_regimes)

        best_regime = row.get("best_regime_by_separation")
        if best_regime:
            regime_summary = _baseline_summary_for_window(
                symbol=symbol,
                date_from=date_from,
                date_to=date_to,
                horizon=int(row["horizon"]),
                regime_bucket=str(best_regime),
            )
            regime_avg = _as_float(regime_summary.get(metric_field))
            regime_summary.update(
                {
                    "target": row["target"],
                    "horizon": row["horizon"],
                    "baseline_name": "best_regime_window",
                    "regime_bucket": str(best_regime),
                    "baseline_metric_name": metric_field,
                    "baseline_win_rate_name": win_rate_field,
                    "baseline_avg_net_bps": regime_avg,
                    "baseline_win_rate": _as_float(regime_summary.get(win_rate_field)),
                    "model_selected_utility_avg": selected_utility,
                    "delta_vs_model_selected_avg": (selected_utility - regime_avg)
                    if selected_utility is not None and regime_avg is not None
                    else None,
                }
            )
            baseline_rows.append(regime_summary)

    preferred_horizon = handoff.get("preferred_horizon")
    preferred_target = _preferred_target_for_horizon(benchmarks.get("horizon_rows") or [], preferred_horizon, model_context)
    preferred_rows = [
        row for row in baseline_rows
        if row.get("horizon") == preferred_horizon and str(row.get("target") or "").lower() == str(preferred_target or "").lower()
    ]
    preferred_regime_row = next((row for row in preferred_rows if row.get("baseline_name") == "best_regime_window"), None)
    preferred_all_row = next((row for row in preferred_rows if row.get("baseline_name") == "all_regimes_window"), None)
    preferred_all_avg = preferred_all_row.get("baseline_avg_net_bps") if preferred_all_row else None
    preferred_regime_avg = preferred_regime_row.get("baseline_avg_net_bps") if preferred_regime_row else None

    return {
        "summary": {
            "symbol": symbol,
            "metadata_mode": model_context.get("metadata_mode"),
            "requires_model_context_backfill": _requires_model_context_backfill(model_context),
            "supported_targets": model_context.get("targets") or [],
            "preferred_target": preferred_target,
            "date_from": date_from,
            "date_to": date_to,
            "preferred_horizon": preferred_horizon,
            "preferred_regime": handoff.get("suggested_regime_bucket"),
            "preferred_all_regimes_avg_net_bps": preferred_all_avg,
            "preferred_regime_avg_net_bps": preferred_regime_avg,
            "preferred_all_regimes_avg_reject_net_bps": preferred_all_avg if preferred_target == "reject" else None,
            "preferred_regime_avg_reject_net_bps": preferred_regime_avg if preferred_target == "reject" else None,
            "preferred_regime_rows": preferred_regime_row.get("rows") if preferred_regime_row else None,
            "preferred_regime_positive_days": preferred_regime_row.get("positive_days") if preferred_regime_row else None,
            "source_duckdb_matches": lineage_match.get("source_duckdb_matches"),
            "cost_model_matches": lineage_match.get("cost_model_matches"),
            "lineage_match": lineage_match,
        },
        "rows": baseline_rows,
        "lineage": {
            "duckdb_path": str(DUCKDB_PATH),
            "research_lineage_path": str(RESEARCH_LINEAGE_PATH),
            "source_db": research_lineage.get("source_db") if research_lineage else None,
            "marts_built_at": research_lineage.get("built_at_utc") if research_lineage else None,
        },
    }


def _build_decision_summary(
    registry_id: str,
    *,
    snapshot: RegistrySnapshot | None = None,
    record: dict[str, Any] | None = None,
    benchmarks: dict[str, Any] | None = None,
    baseline_compare: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_snapshot = snapshot or _registry_snapshot()
    record = record or _registry_record_by_id(registry_id, snapshot=active_snapshot)
    model_context = _resolve_record_model_context(record)
    benchmarks = benchmarks or _build_benchmarks(
        registry_id,
        snapshot=active_snapshot,
        selected_record=record,
    )
    baseline_compare = baseline_compare or _build_baseline_compare(
        registry_id,
        snapshot=active_snapshot,
        record=record,
        benchmarks=benchmarks,
    )
    lineage_match = ((baseline_compare.get("summary") or {}).get("lineage_match") or {})

    model = record["entry"]
    preferred_horizon = benchmarks.get("research_handoff", {}).get("preferred_horizon")
    preferred_target = str((baseline_compare.get("summary") or {}).get("preferred_target") or "").lower() or None
    horizon_rows = benchmarks.get("horizon_rows") or []
    preferred_row = next(
        (
            row for row in horizon_rows
            if row.get("horizon") == preferred_horizon and str(row.get("target") or "").lower() == str(preferred_target or "").lower()
        ),
        None,
    )
    if not preferred_row:
        preferred_row = next((row for row in horizon_rows if row.get("horizon") == preferred_horizon), None)
    baseline_rows = baseline_compare.get("rows") or []
    preferred_window = next(
        (
            row for row in baseline_rows
            if row.get("horizon") == preferred_horizon
            and row.get("baseline_name") == "all_regimes_window"
            and str(row.get("target") or "").lower() == str(preferred_target or "").lower()
        ),
        None,
    )
    preferred_regime = next(
        (
            row for row in baseline_rows
            if row.get("horizon") == preferred_horizon
            and row.get("baseline_name") == "best_regime_window"
            and str(row.get("target") or "").lower() == str(preferred_target or "").lower()
        ),
        None,
    )
    shadow_rows = benchmarks.get("shadow_rows") or []
    preferred_shadow = next(
        (
            row for row in shadow_rows
            if str(row.get("side") or "").lower() == str(preferred_target or "").lower()
            and int(row.get("horizon") or 0) == int(preferred_horizon or 0)
            and row.get("status") == "ok"
        ),
        None,
    )

    def rule_result(rule_id: str, label: str, passed: bool, actual: Any, threshold: str, note: str) -> dict[str, Any]:
        return {
            "rule_id": rule_id,
            "label": label,
            "passed": bool(passed),
            "actual": actual,
            "threshold": threshold,
            "note": note,
        }

    rules: list[dict[str, Any]] = []

    active_horizon = bool(preferred_row) and preferred_row.get("selected_status") == "active"
    rules.append(
        rule_result(
            "active_horizon",
            "Has an active preferred horizon",
            active_horizon,
            preferred_row.get("selected_status") if preferred_row else None,
            "status must be active",
            "Guarded horizons cannot be promoted regardless of relative ranking.",
        )
    )

    selected_utility = _as_float(preferred_row.get("selected_utility_avg")) if preferred_row else None
    rules.append(
        rule_result(
            "positive_utility",
            "Preferred horizon utility is positive",
            selected_utility is not None and selected_utility > 0,
            selected_utility,
            "> 0 bps",
            "The selected cohort must be economically positive after cost.",
        )
    )

    corr_all = _as_float(preferred_row.get("selected_corr_all")) if preferred_row else None
    rules.append(
        rule_result(
            "positive_corr",
            "Probability-to-utility correlation is positive",
            corr_all is not None and corr_all > 0,
            corr_all,
            "> 0",
            "Ranking quality must at least slope in the right direction.",
        )
    )

    global_rank = preferred_row.get("global_rank") if preferred_row else None
    global_peer_count = preferred_row.get("global_peer_count") if preferred_row else None
    top_half = (
        global_rank is not None
        and global_peer_count is not None
        and int(global_rank) <= max(1, int(global_peer_count) // 2 + int(global_peer_count) % 2)
    )
    rules.append(
        rule_result(
            "top_half_rank",
            "Ranks in the top half of comparable peers",
            bool(top_half),
            f"{global_rank}/{global_peer_count}" if global_rank is not None and global_peer_count is not None else None,
            "top-half peer rank",
            "A challenger should outperform at least half of comparable artifacts.",
        )
    )

    delta_vs_window = _as_float(preferred_window.get("delta_vs_model_selected_avg")) if preferred_window else None
    rules.append(
        rule_result(
            "beats_window_baseline",
            "Beats same-window unconditional slice",
            delta_vs_window is not None and delta_vs_window > 0,
            delta_vs_window,
            "> 0 bps",
            "The model should improve on the simple same-window baseline, not just repackage it.",
        )
    )

    delta_vs_regime = _as_float(preferred_regime.get("delta_vs_model_selected_avg")) if preferred_regime else None
    rules.append(
        rule_result(
            "beats_regime_baseline",
            "Beats preferred-regime slice baseline",
            delta_vs_regime is not None and delta_vs_regime > 0,
            delta_vs_regime,
            "> 0 bps",
            "The model should add value beyond merely selecting the historically favorable regime.",
        )
    )

    shadow_utility = _as_float(preferred_shadow.get("emitted_utility_avg")) if preferred_shadow else None
    shadow_ok = preferred_shadow is not None and shadow_utility is not None and shadow_utility > 0
    rules.append(
        rule_result(
            "shadow_support",
            "Shadow policy supports the preferred horizon",
            shadow_ok,
            shadow_utility,
            "> 0 bps shadow utility",
            "Shadow margin policies should agree directionally with the challenger before promotion.",
        )
    )

    passed_count = sum(1 for rule in rules if rule["passed"])
    total_count = len(rules)

    if not active_horizon or (selected_utility is not None and selected_utility <= 0):
        verdict = "blocked"
    elif passed_count == total_count:
        verdict = "challenger_pass"
    else:
        verdict = "research_only"

    headline = {
        "blocked": "Blocked: the candidate fails core promotion gates.",
        "research_only": "Research only: the candidate is interesting but does not clear all challenger rules.",
        "challenger_pass": "Challenger pass: the candidate clears the current institutional ruleset.",
    }[verdict]

    return {
        "summary": {
            "verdict": verdict,
            "headline": headline,
            "metadata_mode": model_context.get("metadata_mode"),
            "requires_model_context_backfill": _requires_model_context_backfill(model_context),
            "evaluation_symbol": model_context.get("default_symbol"),
            "evaluation_target": preferred_target,
            "supported_targets": model_context.get("targets") or [],
            "source_duckdb_matches": lineage_match.get("source_duckdb_matches"),
            "cost_model_matches": lineage_match.get("cost_model_matches"),
            "lineage_match": lineage_match,
            "passed_rules": passed_count,
            "total_rules": total_count,
            "preferred_horizon": preferred_horizon,
            "preferred_regime": benchmarks.get("research_handoff", {}).get("suggested_regime_bucket"),
            "model_status": model.get("status"),
            "selected_utility_avg": selected_utility,
            "selected_corr_all": corr_all,
            "peer_rank": f"{global_rank}/{global_peer_count}" if global_rank is not None and global_peer_count is not None else None,
            "delta_vs_window_baseline": delta_vs_window,
            "delta_vs_regime_baseline": delta_vs_regime,
            "shadow_utility_avg": shadow_utility,
        },
        "rules": rules,
    }


def _build_review_snapshot(registry_id: str, *, reviewer: str, note: str = "") -> dict[str, Any]:
    snapshot = _registry_snapshot()
    record = _registry_record_by_id(registry_id, snapshot=snapshot)
    benchmarks = _build_benchmarks(registry_id, snapshot=snapshot, selected_record=record)
    baseline_compare = _build_baseline_compare(
        registry_id,
        snapshot=snapshot,
        record=record,
        benchmarks=benchmarks,
    )
    decision = _build_decision_summary(
        registry_id,
        snapshot=snapshot,
        record=record,
        benchmarks=benchmarks,
        baseline_compare=baseline_compare,
    )
    research_lineage = _read_research_lineage()

    return {
        "recorded_at_utc": _utc_now(),
        "reviewer": (reviewer or "model_lab").strip() or "model_lab",
        "note": (note or "").strip(),
        "model": record["entry"],
        "decision_summary": decision.get("summary") or {},
        "benchmark_summary": benchmarks.get("summary") or {},
        "baseline_summary": baseline_compare.get("summary") or {},
        "research_handoff": benchmarks.get("research_handoff") or {},
        "raw_paths": {
            "metadata_path": str(record["metadata_path"]),
            "manifest_path": str(record["metadata_path"].parent.parent / "manifest_runtime_latest.json"),
            "review_log_path": str(REVIEW_LOG_PATH),
            "review_store_path": str(REVIEW_STORE_PATH),
        },
        "lineage": {
            "registry_roots": [str(path) for path in _registry_roots()],
            "duckdb_path": str(DUCKDB_PATH),
            "research_lineage_path": str(RESEARCH_LINEAGE_PATH),
            "source_db": research_lineage.get("source_db") if research_lineage else None,
            "marts_built_at": research_lineage.get("built_at_utc") if research_lineage else None,
            **_review_storage_lineage(),
        },
    }


def _build_review_compare(review_id_a: str, review_id_b: str) -> dict[str, Any]:
    review_a = _find_review_entry(review_id_a)
    review_b = _find_review_entry(review_id_b)

    summary_a = review_a.get("decision_summary") or {}
    summary_b = review_b.get("decision_summary") or {}
    benchmark_a = review_a.get("benchmark_summary") or {}
    benchmark_b = review_b.get("benchmark_summary") or {}
    baseline_a = review_a.get("baseline_summary") or {}
    baseline_b = review_b.get("baseline_summary") or {}
    preferred_regime_baseline_a = _preferred_regime_baseline_value(baseline_a)
    preferred_regime_baseline_b = _preferred_regime_baseline_value(baseline_b)

    rank_a, peer_count_a = _parse_rank_pair(summary_a.get("peer_rank"))
    rank_b, peer_count_b = _parse_rank_pair(summary_b.get("peer_rank"))

    rows = [
        {
            "metric": "Verdict",
            "value_a": summary_a.get("verdict"),
            "value_b": summary_b.get("verdict"),
            "delta": "changed" if summary_a.get("verdict") != summary_b.get("verdict") else "same",
        },
        {
            "metric": "Rules Passed",
            "value_a": f"{summary_a.get('passed_rules')}/{summary_a.get('total_rules')}",
            "value_b": f"{summary_b.get('passed_rules')}/{summary_b.get('total_rules')}",
            "delta": (
                (int(summary_b.get("passed_rules") or 0) - int(summary_a.get("passed_rules") or 0))
                if summary_a.get("passed_rules") is not None and summary_b.get("passed_rules") is not None
                else None
            ),
        },
        {
            "metric": "Selected Utility Avg",
            "value_a": summary_a.get("selected_utility_avg"),
            "value_b": summary_b.get("selected_utility_avg"),
            "delta": (
                (_as_float(summary_b.get("selected_utility_avg")) - _as_float(summary_a.get("selected_utility_avg")))
                if _as_float(summary_a.get("selected_utility_avg")) is not None and _as_float(summary_b.get("selected_utility_avg")) is not None
                else None
            ),
        },
        {
            "metric": "Peer Rank",
            "value_a": summary_a.get("peer_rank"),
            "value_b": summary_b.get("peer_rank"),
            "delta": ((rank_a - rank_b) if rank_a is not None and rank_b is not None else None),
        },
        {
            "metric": "Window Baseline Delta",
            "value_a": summary_a.get("delta_vs_window_baseline"),
            "value_b": summary_b.get("delta_vs_window_baseline"),
            "delta": (
                (_as_float(summary_b.get("delta_vs_window_baseline")) - _as_float(summary_a.get("delta_vs_window_baseline")))
                if _as_float(summary_a.get("delta_vs_window_baseline")) is not None and _as_float(summary_b.get("delta_vs_window_baseline")) is not None
                else None
            ),
        },
        {
            "metric": "Regime Baseline Delta",
            "value_a": summary_a.get("delta_vs_regime_baseline"),
            "value_b": summary_b.get("delta_vs_regime_baseline"),
            "delta": (
                (_as_float(summary_b.get("delta_vs_regime_baseline")) - _as_float(summary_a.get("delta_vs_regime_baseline")))
                if _as_float(summary_a.get("delta_vs_regime_baseline")) is not None and _as_float(summary_b.get("delta_vs_regime_baseline")) is not None
                else None
            ),
        },
        {
            "metric": "Registry Rank",
            "value_a": benchmark_a.get("registry_rank"),
            "value_b": benchmark_b.get("registry_rank"),
            "delta": (
                (int(benchmark_a.get("registry_rank") or 0) - int(benchmark_b.get("registry_rank") or 0))
                if benchmark_a.get("registry_rank") is not None and benchmark_b.get("registry_rank") is not None
                else None
            ),
        },
        {
            "metric": "Preferred Regime Baseline",
            "value_a": preferred_regime_baseline_a,
            "value_b": preferred_regime_baseline_b,
            "delta": (
                (preferred_regime_baseline_b - preferred_regime_baseline_a)
                if preferred_regime_baseline_a is not None and preferred_regime_baseline_b is not None
                else None
            ),
        },
    ]

    return {
        "review_a": review_a,
        "review_b": review_b,
        "summary": {
            "same_model": ((review_a.get("model") or {}).get("id") == (review_b.get("model") or {}).get("id")),
            "verdict_changed": summary_a.get("verdict") != summary_b.get("verdict"),
            "review_id_a": review_id_a,
            "review_id_b": review_id_b,
            "recorded_at_a": review_a.get("recorded_at_utc"),
            "recorded_at_b": review_b.get("recorded_at_utc"),
            "peer_pool_a": peer_count_a,
            "peer_pool_b": peer_count_b,
        },
        "rows": rows,
        **_review_storage_lineage(),
    }


def _latest_reviews_by_model(reviews: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for review in reviews:
        model_id = str((review.get("model") or {}).get("id") or "").strip()
        if not model_id:
            continue
        latest[model_id] = review
    return latest


def _build_governance_summary(
    *,
    snapshot: RegistrySnapshot | None = None,
    reviews: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    active_snapshot = snapshot or _registry_snapshot()
    reviews = reviews if reviews is not None else _review_log_entries()
    registry_entries = _registry_entries(active_snapshot)
    latest_review = reviews[-1] if reviews else None
    latest_registry = registry_entries[0] if registry_entries else None
    verdict_counts: dict[str, int] = {}
    reviewed_model_ids: set[str] = set()
    for review in reviews:
        verdict = str((review.get("decision_summary") or {}).get("verdict") or "unknown")
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
        model_id = str((review.get("model") or {}).get("id") or "")
        if model_id:
            reviewed_model_ids.add(model_id)

    latest_registry_review = None
    if latest_registry:
        latest_registry_review = next(
            (
                review for review in reversed(reviews)
                if (review.get("model") or {}).get("id") == latest_registry.get("id")
            ),
            None,
        )

    return {
        **_review_storage_lineage(),
        "review_entries": len(reviews),
        "reviewed_models": len(reviewed_model_ids),
        "verdict_counts": verdict_counts,
        "latest_review": latest_review,
        "latest_registry_model": latest_registry,
        "latest_registry_review": latest_registry_review,
        "latest_registry_reviewed": latest_registry_review is not None,
    }


def _build_governance_workspace(limit: int = 12) -> dict[str, Any]:
    snapshot = _registry_snapshot()
    reviews = _review_log_entries()
    latest_reviews = _latest_reviews_by_model(reviews)
    registry_entries = _registry_entries(snapshot)
    summary = _build_governance_summary(snapshot=snapshot, reviews=reviews)
    review_state_counts: dict[str, int] = {
        "pending_review": 0,
        "needs_refresh": 0,
        "challenger_pass": 0,
        "research_only": 0,
        "blocked": 0,
    }
    queue_rows: list[dict[str, Any]] = []

    for entry in registry_entries:
        model_id = str(entry.get("id") or "")
        record = _registry_record_by_id(model_id, snapshot=snapshot)
        benchmarks = _build_benchmarks(model_id, snapshot=snapshot, selected_record=record)
        baseline_compare = _build_baseline_compare(
            model_id,
            snapshot=snapshot,
            record=record,
            benchmarks=benchmarks,
        )
        decision = _build_decision_summary(
            model_id,
            snapshot=snapshot,
            record=record,
            benchmarks=benchmarks,
            baseline_compare=baseline_compare,
        )
        decision_summary = decision.get("summary") or {}
        handoff = benchmarks.get("research_handoff") or {}
        latest_review = latest_reviews.get(model_id)
        latest_review_summary = (latest_review or {}).get("decision_summary") or {}

        if not latest_review:
            review_state = "pending_review"
        elif (
            latest_review_summary.get("verdict") != decision_summary.get("verdict")
            or latest_review_summary.get("passed_rules") != decision_summary.get("passed_rules")
            or latest_review_summary.get("total_rules") != decision_summary.get("total_rules")
        ):
            review_state = "needs_refresh"
        else:
            review_state = str(decision_summary.get("verdict") or "reviewed")

        review_state_counts[review_state] = review_state_counts.get(review_state, 0) + 1

        queue_rows.append(
            {
                "id": model_id,
                "version": entry.get("version"),
                "family": entry.get("family"),
                "status": entry.get("status"),
                "trained_end_ts": entry.get("trained_end_ts"),
                "trained_end_utc": entry.get("trained_end_utc"),
                "mean_selected_utility_avg": entry.get("mean_selected_utility_avg"),
                "review_state": review_state,
                "latest_review_verdict": latest_review_summary.get("verdict"),
                "latest_reviewed_at_utc": (latest_review or {}).get("recorded_at_utc"),
                "latest_reviewer": (latest_review or {}).get("reviewer"),
                "latest_review_id": (latest_review or {}).get("review_id"),
                "preferred_horizon": decision_summary.get("preferred_horizon"),
                "preferred_regime": decision_summary.get("preferred_regime"),
                "selected_utility_avg": decision_summary.get("selected_utility_avg"),
                "selected_corr_all": decision_summary.get("selected_corr_all"),
                "peer_rank": decision_summary.get("peer_rank"),
                "metadata_mode": decision_summary.get("metadata_mode"),
                "requires_model_context_backfill": decision_summary.get("requires_model_context_backfill"),
                "evaluation_symbol": decision_summary.get("evaluation_symbol"),
                "evaluation_target": decision_summary.get("evaluation_target"),
                "source_duckdb_matches": decision_summary.get("source_duckdb_matches"),
                "cost_model_matches": decision_summary.get("cost_model_matches"),
                "lineage_match": decision_summary.get("lineage_match"),
                "delta_vs_window_baseline": decision_summary.get("delta_vs_window_baseline"),
                "delta_vs_regime_baseline": decision_summary.get("delta_vs_regime_baseline"),
                "review_rules_passed": decision_summary.get("passed_rules"),
                "review_rules_total": decision_summary.get("total_rules"),
                "research_handoff": handoff,
            }
        )

    review_priority = {
        "pending_review": 0,
        "needs_refresh": 1,
        "research_only": 2,
        "challenger_pass": 3,
        "blocked": 4,
    }
    queue_rows = sorted(
        queue_rows,
        key=lambda row: (
            review_priority.get(str(row.get("review_state") or ""), 99),
            -(int(row.get("trained_end_ts") or 0)),
        ),
    )
    queue_total_count = len(queue_rows)
    queue_page = queue_rows[:limit]

    activity_rows: list[dict[str, Any]] = []
    for review in reversed(reviews[-12:]):
        decision_summary = review.get("decision_summary") or {}
        activity_rows.append(
            {
                "review_id": review.get("review_id"),
                "recorded_at_utc": review.get("recorded_at_utc"),
                "version": (review.get("model") or {}).get("version"),
                "model_id": (review.get("model") or {}).get("id"),
                "verdict": decision_summary.get("verdict"),
                "passed_rules": decision_summary.get("passed_rules"),
                "total_rules": decision_summary.get("total_rules"),
                "reviewer": review.get("reviewer"),
                "note": review.get("note"),
            }
        )

    latest_registry_model = summary.get("latest_registry_model") or {}
    latest_registry_queue_row = next(
        (row for row in queue_rows if row.get("id") == latest_registry_model.get("id")),
        None,
    )

    return {
        "summary": {
            **summary,
            "queue_count": queue_total_count,
            "queue_total_count": queue_total_count,
            "queue_page_count": len(queue_page),
            "queue_limit": int(limit),
            "pending_review_count": review_state_counts.get("pending_review", 0),
            "needs_refresh_count": review_state_counts.get("needs_refresh", 0),
            "challenger_pass_count": review_state_counts.get("challenger_pass", 0),
            "research_only_count": review_state_counts.get("research_only", 0),
            "blocked_count": review_state_counts.get("blocked", 0),
            "latest_review_verdict": ((summary.get("latest_review") or {}).get("decision_summary") or {}).get("verdict"),
            "latest_reviewed_at_utc": (summary.get("latest_review") or {}).get("recorded_at_utc"),
            "latest_registry_version": (summary.get("latest_registry_model") or {}).get("version"),
            "latest_registry_review_state": latest_registry_queue_row.get("review_state") if latest_registry_queue_row else None,
        },
        "queue": queue_page,
        "latest_reviews": activity_rows,
        "lineage": {
            "registry_roots": [str(path) for path in _registry_roots()],
            "duckdb_path": str(DUCKDB_PATH),
            "research_lineage_path": str(RESEARCH_LINEAGE_PATH),
            **_review_storage_lineage(),
        },
    }


@app.get("/health")
def health() -> dict[str, Any]:
    snapshot = _registry_snapshot()
    roots = [str(path) for path in _registry_roots()]
    return {
        "status": "ok",
        "registry_roots": roots,
        "models_found": len(snapshot.metadata_files),
        **_review_storage_lineage(),
        "review_entries": len(_review_log_entries()),
    }


@app.get("/registry")
def registry(limit: int = Query(default=50, ge=1, le=200)) -> dict[str, Any]:
    snapshot = _registry_snapshot()
    entries = _registry_entries(snapshot)[:limit]
    return {
        "models": entries,
        "registry_roots": [str(path) for path in _registry_roots()],
    }


@app.get("/diagnostics")
def diagnostics(id: str = Query(..., min_length=1)) -> dict[str, Any]:
    snapshot = _registry_snapshot()
    record = _registry_record_by_id(id, snapshot=snapshot)
    metadata_path = record["metadata_path"]
    payload = record["payload"]
    registry_entry = record["entry"]
    horizon_details = record["horizon_details"]
    return {
        "model": registry_entry,
        "metadata": {
            "version": payload.get("version"),
            "feature_version": payload.get("feature_version"),
            "trained_end_ts": payload.get("trained_end_ts"),
            "trained_end_utc": _iso_from_epoch_ms(payload.get("trained_end_ts")),
            "tune_date_range": payload.get("tune_date_range") or {},
            "gamma_context": payload.get("gamma_context"),
            "shadow_policies": payload.get("shadow_policies"),
            "stats": payload.get("stats"),
        },
        "horizon_details": horizon_details,
        "raw_paths": {
            "metadata_path": str(metadata_path),
            "manifest_path": str(metadata_path.parent.parent / "manifest_runtime_latest.json"),
        },
    }


@app.get("/benchmarks")
def benchmarks(id: str = Query(..., min_length=1)) -> dict[str, Any]:
    snapshot = _registry_snapshot()
    record = _registry_record_by_id(id, snapshot=snapshot)
    return {
        "model": record["entry"],
        "benchmarks": _build_benchmarks(id, snapshot=snapshot, selected_record=record),
        "raw_paths": {
            "metadata_path": str(record["metadata_path"]),
            "manifest_path": str(record["metadata_path"].parent.parent / "manifest_runtime_latest.json"),
        },
    }


@app.get("/baseline-compare")
def baseline_compare(id: str = Query(..., min_length=1)) -> dict[str, Any]:
    snapshot = _registry_snapshot()
    record = _registry_record_by_id(id, snapshot=snapshot)
    benchmarks = _build_benchmarks(id, snapshot=snapshot, selected_record=record)
    return {
        "model": record["entry"],
        "baseline_compare": _build_baseline_compare(
            id,
            snapshot=snapshot,
            record=record,
            benchmarks=benchmarks,
        ),
        "raw_paths": {
            "metadata_path": str(record["metadata_path"]),
            "manifest_path": str(record["metadata_path"].parent.parent / "manifest_runtime_latest.json"),
        },
    }


@app.get("/decision")
def decision(id: str = Query(..., min_length=1)) -> dict[str, Any]:
    snapshot = _registry_snapshot()
    record = _registry_record_by_id(id, snapshot=snapshot)
    benchmarks = _build_benchmarks(id, snapshot=snapshot, selected_record=record)
    baseline_payload = _build_baseline_compare(
        id,
        snapshot=snapshot,
        record=record,
        benchmarks=benchmarks,
    )
    return {
        "model": record["entry"],
        "decision": _build_decision_summary(
            id,
            snapshot=snapshot,
            record=record,
            benchmarks=benchmarks,
            baseline_compare=baseline_payload,
        ),
        "raw_paths": {
            "metadata_path": str(record["metadata_path"]),
            "manifest_path": str(record["metadata_path"].parent.parent / "manifest_runtime_latest.json"),
        },
    }


@app.get("/review-log")
def review_log(
    id: str | None = Query(default=None),
    verdict: str | None = Query(default=None),
    reviewer: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    entries = _review_log_entries()
    id = id if isinstance(id, str) else None
    verdict = verdict if isinstance(verdict, str) else None
    reviewer = reviewer if isinstance(reviewer, str) else None
    if id:
        entries = [entry for entry in entries if (entry.get("model") or {}).get("id") == id]
    if verdict:
        entries = [entry for entry in entries if (entry.get("decision_summary") or {}).get("verdict") == verdict]
    if reviewer:
        reviewer_lower = reviewer.strip().lower()
        entries = [entry for entry in entries if reviewer_lower in str(entry.get("reviewer") or "").lower()]
    entries = list(reversed(entries[-limit:]))
    return {
        "reviews": entries,
        **_review_storage_lineage(),
    }


@app.post("/review-log")
def record_review(request: ReviewLogRequest) -> dict[str, Any]:
    entry_body = _build_review_snapshot(request.id, reviewer=request.reviewer, note=request.note)
    entry = _append_review_log(entry_body)
    return {
        "status": "ok",
        "review": entry,
        **_review_storage_lineage(),
    }


@app.get("/review-compare")
def review_compare(
    review_id_a: str = Query(..., min_length=1),
    review_id_b: str = Query(..., min_length=1),
) -> dict[str, Any]:
    return _build_review_compare(review_id_a, review_id_b)


@app.get("/governance-summary")
def governance_summary() -> dict[str, Any]:
    return _build_governance_summary()


@app.get("/governance-workspace")
def governance_workspace(limit: int = Query(default=12, ge=1, le=50)) -> dict[str, Any]:
    return _build_governance_workspace(limit)


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
