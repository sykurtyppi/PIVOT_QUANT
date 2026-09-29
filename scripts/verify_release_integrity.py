#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _resolve_python_bin() -> str | None:
    override = os.getenv("PYTHON_BIN")
    if override:
        override_path = Path(override).expanduser()
        if override_path.is_file() and os.access(override_path, os.X_OK):
            return str(override_path)

    candidates = [
        ROOT / ".venv313" / "bin" / "python",
        ROOT / ".venv313" / "bin" / "python3",
        ROOT / ".venv" / "bin" / "python",
        ROOT / ".venv" / "bin" / "python3",
    ]
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    fallback = shutil.which("python3")
    return fallback


def _maybe_reexec_with_resolved_python() -> None:
    if os.getenv("PQ_INTEGRITY_PYTHON_RESOLVED") == "1":
        return
    target = _resolve_python_bin()
    if not target:
        return
    target_path = Path(target)
    try:
        current_path = Path(sys.executable)
        if current_path.exists() and target_path.exists() and current_path.samefile(target_path):
            return
    except Exception:
        if str(Path(sys.executable)) == str(target_path):
            return
    env = dict(os.environ)
    env["PQ_INTEGRITY_PYTHON_RESOLVED"] = "1"
    os.execve(
        str(target_path),
        [str(target_path), str(Path(__file__).resolve()), *sys.argv[1:]],
        env,
    )


_maybe_reexec_with_resolved_python()


def _env_path(name: str, default: Path) -> Path:
    raw = os.getenv(name)
    return Path(raw).expanduser() if raw else default


RESEARCH_LINEAGE_PATH = _env_path("RESEARCH_LINEAGE_PATH", ROOT / "data" / "research_marts" / "last_build.json")
MODEL_REGISTRY_ROOT = _env_path("MODEL_REGISTRY_ROOT", ROOT / "data" / "t9_experiments")
MODEL_REVIEW_LOG_PATH = _env_path("MODEL_REVIEW_LOG_PATH", ROOT / "data" / "model_lab" / "review_log.jsonl")
MODEL_REVIEW_DB_PATH = _env_path("MODEL_REVIEW_DB_PATH", ROOT / "data" / "model_lab" / "review_store.sqlite")
MODEL_REVIEW_BACKEND = str(os.getenv("MODEL_REVIEW_BACKEND", "sqlite")).strip().lower()


@dataclass
class CategoryResult:
    name: str
    status: str
    skips: list[str]
    warnings: list[str]
    failures: list[str]
    meta: dict[str, Any]


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _extract_horizons(payload: dict[str, Any]) -> list[int]:
    horizon_values: set[int] = set()

    thresholds_meta = payload.get("thresholds_meta")
    if isinstance(thresholds_meta, dict):
        for target_map in thresholds_meta.values():
            if not isinstance(target_map, dict):
                continue
            for horizon in target_map.keys():
                parsed = _as_int(horizon)
                if parsed is not None:
                    horizon_values.add(parsed)

    stats = payload.get("stats")
    if isinstance(stats, dict):
        for horizon in stats.keys():
            parsed = _as_int(horizon)
            if parsed is not None:
                horizon_values.add(parsed)

    return sorted(horizon_values)


def _derive_registry_status(payload: dict[str, Any], horizons: list[int]) -> str | None:
    threshold_meta = payload.get("thresholds_meta")
    if not isinstance(threshold_meta, dict):
        return "candidate" if horizons else None

    pair_count = 0
    guarded_pairs = 0
    for target_map in threshold_meta.values():
        if not isinstance(target_map, dict):
            continue
        for meta in target_map.values():
            if not isinstance(meta, dict):
                continue
            pair_count += 1
            if bool(meta.get("guard_applied")):
                guarded_pairs += 1

    if pair_count <= 0:
        return "candidate" if horizons else None
    if guarded_pairs == pair_count:
        return "blocked"
    if guarded_pairs > 0:
        return "guarded"
    return "candidate"


def _resolve_status(*, warnings: list[str], failures: list[str]) -> str:
    if failures:
        return "FAIL"
    if warnings:
        return "WARN"
    return "PASS"


def check_research_integrity() -> CategoryResult:
    skips: list[str] = []
    warnings: list[str] = []
    failures: list[str] = []

    if not RESEARCH_LINEAGE_PATH.exists():
        failures.append(f"Research lineage artifact missing: {RESEARCH_LINEAGE_PATH}")
        return CategoryResult("Research integrity", "FAIL", skips, warnings, failures, {})

    lineage = _read_json(RESEARCH_LINEAGE_PATH)
    if not lineage:
        failures.append(f"Malformed JSON in research lineage: {RESEARCH_LINEAGE_PATH}")
        return CategoryResult("Research integrity", "FAIL", skips, warnings, failures, {})

    required_fields = [
        "status",
        "built_at_utc",
        "duckdb_path",
        "mart_counts",
        "symbols",
        "horizons",
        "cost_model",
        "date_range",
    ]
    for field in required_fields:
        if field not in lineage:
            failures.append(f"Missing lineage field: {field}")

    mart_counts = lineage.get("mart_counts")
    required_marts = [
        "mart_trading_calendar",
        "mart_event_base",
        "mart_event_labels",
        "mart_slice_expectancy_daily",
        "mart_slice_expectancy_rollup",
    ]
    if not isinstance(mart_counts, dict):
        failures.append("mart_counts is missing or malformed")
    else:
        for mart_name in required_marts:
            count = mart_counts.get(mart_name)
            if not isinstance(count, int):
                failures.append(f"{mart_name} count missing or non-integer")
            elif count <= 0:
                failures.append(f"{mart_name} has non-positive row count: {count}")

    symbols = lineage.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        failures.append("symbols list missing or empty")

    horizons = lineage.get("horizons")
    if not isinstance(horizons, list) or not horizons:
        failures.append("horizons list missing or empty")

    cost_model = lineage.get("cost_model")
    if not isinstance(cost_model, dict):
        failures.append("cost_model block missing or malformed")
    else:
        if not cost_model.get("version"):
            failures.append("cost_model.version missing")
        if cost_model.get("trade_cost_bps") is None:
            failures.append("cost_model.trade_cost_bps missing")

    date_range = lineage.get("date_range")
    if not isinstance(date_range, dict):
        failures.append("date_range block missing or malformed")
    else:
        min_date = str(date_range.get("min_event_date_et") or "")
        max_date = str(date_range.get("max_event_date_et") or "")
        if not min_date or not max_date:
            failures.append("date_range min/max missing")
        else:
            try:
                if datetime.fromisoformat(min_date) > datetime.fromisoformat(max_date):
                    failures.append("date_range min_event_date_et is after max_event_date_et")
            except Exception:
                failures.append("date_range min/max are not valid ISO dates")

    source_meta = lineage.get("source_db_metadata")
    if isinstance(source_meta, dict) and not bool(source_meta.get("source_db_exists", True)):
        warnings.append("source_db_metadata.source_db_exists=false")

    duckdb_path_raw = lineage.get("duckdb_path")
    duckdb_path = Path(str(duckdb_path_raw)).expanduser() if duckdb_path_raw else None
    if duckdb_path is None or not duckdb_path.exists():
        failures.append(f"DuckDB path from lineage is missing: {duckdb_path_raw}")
    else:
        try:
            import duckdb  # type: ignore
        except Exception:
            skips.append("duckdb not installed; schema-level research integrity checks skipped")
        else:
            con = duckdb.connect(str(duckdb_path), read_only=True)
            try:
                table_rows = con.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'pq_research'
                    """
                ).fetchall()
                table_names = {str(row[0]) for row in table_rows}
                for mart_name in required_marts:
                    if mart_name not in table_names:
                        failures.append(f"Missing pq_research table: {mart_name}")

                required_columns_by_table = {
                    "mart_event_base": {"event_id", "symbol", "event_date_et"},
                    "mart_event_labels": {"event_id", "horizon_min", "reject_net_bps", "break_net_bps"},
                    "mart_slice_expectancy_daily": {
                        "symbol",
                        "event_date_et",
                        "horizon_min",
                        "regime_bucket",
                        "rows_n",
                    },
                    "mart_slice_expectancy_rollup": {"symbol", "horizon_min", "regime_bucket", "rows_n"},
                }
                for table_name, required_columns in required_columns_by_table.items():
                    col_rows = con.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'pq_research' AND table_name = ?
                        """,
                        [table_name],
                    ).fetchall()
                    column_names = {str(row[0]) for row in col_rows}
                    missing = required_columns - column_names
                    if missing:
                        failures.append(
                            f"Missing required columns in pq_research.{table_name}: {', '.join(sorted(missing))}"
                        )
            finally:
                con.close()

    status = _resolve_status(warnings=warnings, failures=failures)
    return CategoryResult(
        "Research integrity",
        status,
        skips,
        warnings,
        failures,
        {"lineage_path": str(RESEARCH_LINEAGE_PATH)},
    )


def check_model_registry_integrity() -> CategoryResult:
    skips: list[str] = []
    warnings: list[str] = []
    failures: list[str] = []

    roots = [path for path in [MODEL_REGISTRY_ROOT] if path.exists()]
    if not roots:
        failures.append(f"Model registry root not found: {MODEL_REGISTRY_ROOT}")
        return CategoryResult("Model registry integrity", "FAIL", skips, warnings, failures, {"latest_model_ids": []})

    metadata_files = sorted({path for root in roots for path in root.rglob("metadata_*.json")})
    if not metadata_files:
        failures.append(f"No metadata_*.json files found under: {', '.join(str(root) for root in roots)}")
        return CategoryResult("Model registry integrity", "FAIL", skips, warnings, failures, {"latest_model_ids": []})

    family_versions: dict[Path, set[str]] = {}
    family_latest_model_ids: list[str] = []
    model_count = 0

    required_model_context_fields = {
        "symbols",
        "default_symbol",
        "targets",
        "cost_model_version",
        "trade_cost_bps",
        "training_view",
        "source_duckdb_path",
    }

    for metadata_path in metadata_files:
        payload = _read_json(metadata_path)
        if payload is None:
            failures.append(f"Invalid metadata JSON: {metadata_path}")
            continue

        model_count += 1
        try:
            model_id = str(metadata_path.resolve().relative_to(ROOT.resolve()))
        except Exception:
            model_id = str(metadata_path.resolve())

        version = str(payload.get("version") or "").strip()
        if not version:
            failures.append(f"Invalid registry entry: missing version ({metadata_path})")
            continue

        if payload.get("trained_end_ts") is None:
            failures.append(f"Metadata missing trained_end_ts: {metadata_path}")

        horizons = _extract_horizons(payload)
        if not horizons:
            failures.append(f"Invalid registry entry: no detectable horizons ({metadata_path})")

        status = _derive_registry_status(payload, horizons)
        if not model_id:
            failures.append(f"Invalid registry entry: missing model_id ({metadata_path})")
        if status is None:
            failures.append(f"Invalid registry entry: missing status ({metadata_path})")

        model_context = payload.get("model_context")
        if not isinstance(model_context, dict):
            warnings.append(f"Metadata missing model_context (legacy/incomplete): {metadata_path}")
        else:
            missing_ctx = sorted(required_model_context_fields - set(model_context.keys()))
            if missing_ctx:
                warnings.append(
                    f"Metadata model_context missing fields ({', '.join(missing_ctx)}): {metadata_path}"
                )

        family_dir = metadata_path.parent.parent
        family_versions.setdefault(family_dir, set()).add(version)

    for family_dir, versions in sorted(family_versions.items(), key=lambda item: str(item[0])):
        manifest_path = family_dir / "manifest_runtime_latest.json"
        if not manifest_path.exists():
            failures.append(f"Missing manifest_runtime_latest.json: {manifest_path}")
            continue
        manifest = _read_json(manifest_path)
        if manifest is None:
            failures.append(f"Invalid manifest JSON: {manifest_path}")
            continue
        manifest_version = str(manifest.get("version") or "").strip()
        if not manifest_version:
            failures.append(f"Manifest missing version: {manifest_path}")
            continue
        if manifest_version not in versions:
            warnings.append(
                f"Manifest version '{manifest_version}' not found in family metadata files: {family_dir}"
            )
        else:
            matching_metadata = family_dir / "metadata_runtime" / f"metadata_{manifest_version}.json"
            if matching_metadata.exists():
                try:
                    model_id = str(matching_metadata.resolve().relative_to(ROOT.resolve()))
                except Exception:
                    model_id = str(matching_metadata.resolve())
                family_latest_model_ids.append(model_id)

    status = _resolve_status(warnings=warnings, failures=failures)
    return CategoryResult(
        "Model registry integrity",
        status,
        skips,
        warnings,
        failures,
        {
            "registry_roots": [str(root) for root in roots],
            "model_count": model_count,
            "latest_model_ids": sorted(set(family_latest_model_ids)),
        },
    )


def _verify_jsonl_hash_chain(entries: list[dict[str, Any]]) -> tuple[bool, str | None]:
    try:
        import hashlib
        from server.model_review_store import canonical_json  # type: ignore
    except Exception:
        return False, "could_not_import_hash_helpers"

    previous_hash = ""
    for index, entry in enumerate(entries, start=1):
        expected_prev = str(entry.get("prev_hash") or "")
        if expected_prev != previous_hash:
            return False, f"prev_hash_mismatch_at_position_{index}"
        entry_hash = str(entry.get("entry_hash") or "")
        if not entry_hash:
            return False, f"missing_entry_hash_at_position_{index}"
        body = dict(entry)
        body.pop("entry_hash", None)
        body.pop("review_id", None)
        computed = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        if computed != entry_hash:
            return False, f"entry_hash_mismatch_at_position_{index}"
        previous_hash = entry_hash
    return True, None


def check_governance_integrity(latest_model_ids: list[str]) -> CategoryResult:
    skips: list[str] = []
    warnings: list[str] = []
    failures: list[str] = []
    reviewed_model_ids: set[str] = set()
    review_count = 0

    backend = MODEL_REVIEW_BACKEND if MODEL_REVIEW_BACKEND in {"sqlite", "jsonl"} else "sqlite"

    if backend == "sqlite":
        if not MODEL_REVIEW_DB_PATH.exists():
            if MODEL_REVIEW_LOG_PATH.exists():
                warnings.append(
                    f"SQLite review store missing but legacy JSONL exists: {MODEL_REVIEW_DB_PATH}"
                )
            else:
                skips.append(f"No governance review store found at {MODEL_REVIEW_DB_PATH}; governance checks skipped")
            status = _resolve_status(warnings=warnings, failures=failures)
            return CategoryResult(
                "Governance store integrity",
                status,
                skips,
                warnings,
                failures,
                {"review_count": review_count, "reviewed_model_ids": sorted(reviewed_model_ids), "backend": backend},
            )

        conn = sqlite3.connect(str(MODEL_REVIEW_DB_PATH))
        conn.row_factory = sqlite3.Row
        try:
            table_row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='review_entries'"
            ).fetchone()
            if table_row is None:
                failures.append(f"Missing review_entries table in {MODEL_REVIEW_DB_PATH}")
            else:
                rows = conn.execute(
                    """
                    SELECT review_id, recorded_at_utc, reviewer, note, model_id, decision_verdict, payload_json
                    FROM review_entries
                    ORDER BY sequence_id ASC
                    """
                ).fetchall()
                review_count = len(rows)
                review_ids: set[str] = set()
                for idx, row in enumerate(rows, start=1):
                    review_id = str(row["review_id"] or "").strip()
                    if not review_id:
                        failures.append(f"Review entry missing review_id at row {idx}")
                        continue
                    if review_id in review_ids:
                        failures.append(f"Duplicate review_id detected: {review_id}")
                    review_ids.add(review_id)

                    if not str(row["recorded_at_utc"] or "").strip():
                        failures.append(f"Review entry missing recorded_at_utc: {review_id}")
                    if not str(row["reviewer"] or "").strip():
                        failures.append(f"Review entry missing reviewer: {review_id}")

                    model_id = str(row["model_id"] or "").strip()
                    decision = str(row["decision_verdict"] or "").strip()
                    if not model_id:
                        failures.append(f"Review entry missing model_id: {review_id}")
                    if not decision:
                        failures.append(f"Review entry missing decision_verdict: {review_id}")
                    if model_id:
                        reviewed_model_ids.add(model_id)

                    payload = None
                    try:
                        payload = json.loads(str(row["payload_json"]))
                    except Exception:
                        failures.append(f"Malformed payload_json in review entry: {review_id}")
                    if isinstance(payload, dict):
                        if not isinstance(payload.get("model"), dict):
                            failures.append(f"Review payload missing model block: {review_id}")
                        if not isinstance(payload.get("decision_summary"), dict):
                            failures.append(f"Review payload missing decision_summary block: {review_id}")

                try:
                    from server.model_review_store import verify_hash_chain  # type: ignore
                except Exception as exc:
                    if review_count > 0:
                        failures.append(
                            "Governance hash-chain verifier unavailable for non-empty sqlite review store: "
                            f"{type(exc).__name__}: {exc}"
                        )
                    else:
                        skips.append("Governance hash-chain verifier unavailable and store is empty; chain check skipped")
                else:
                    try:
                        chain = verify_hash_chain(MODEL_REVIEW_DB_PATH)
                    except Exception as exc:
                        if review_count > 0:
                            failures.append(
                                "Governance hash-chain verification failed for non-empty sqlite review store: "
                                f"{type(exc).__name__}: {exc}"
                            )
                        else:
                            skips.append("Governance hash-chain verification failed on empty sqlite store; chain check skipped")
                    else:
                        if not bool(chain.get("valid")):
                            failures.append(
                                f"Review hash chain invalid ({chain.get('reason')}) at position {chain.get('position')}"
                            )
        finally:
            conn.close()
    else:
        if not MODEL_REVIEW_LOG_PATH.exists():
            skips.append(f"No JSONL review log found at {MODEL_REVIEW_LOG_PATH}; governance checks skipped")
            status = _resolve_status(warnings=warnings, failures=failures)
            return CategoryResult(
                "Governance store integrity",
                status,
                skips,
                warnings,
                failures,
                {"review_count": review_count, "reviewed_model_ids": sorted(reviewed_model_ids), "backend": backend},
            )

        entries: list[dict[str, Any]] = []
        for raw_line in MODEL_REVIEW_LOG_PATH.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                failures.append("Malformed JSON line in review log")
                continue
            if not isinstance(payload, dict):
                failures.append("Non-object review entry in review log")
                continue
            entries.append(payload)

        review_count = len(entries)
        review_ids: set[str] = set()
        for idx, entry in enumerate(entries, start=1):
            review_id = str(entry.get("review_id") or "").strip()
            if not review_id:
                failures.append(f"Review entry missing review_id at position {idx}")
                continue
            if review_id in review_ids:
                failures.append(f"Duplicate review_id detected: {review_id}")
            review_ids.add(review_id)

            if not str(entry.get("recorded_at_utc") or "").strip():
                failures.append(f"Review entry missing recorded_at_utc: {review_id}")
            if not str(entry.get("reviewer") or "").strip():
                failures.append(f"Review entry missing reviewer: {review_id}")

            model = entry.get("model") if isinstance(entry.get("model"), dict) else {}
            model_id = str(model.get("id") or "").strip()
            if not model_id:
                failures.append(f"Review entry missing model.id: {review_id}")
            else:
                reviewed_model_ids.add(model_id)

            decision_summary = entry.get("decision_summary") if isinstance(entry.get("decision_summary"), dict) else {}
            if not str(decision_summary.get("verdict") or "").strip():
                failures.append(f"Review entry missing decision_summary.verdict: {review_id}")

        chain_valid, chain_reason = _verify_jsonl_hash_chain(entries)
        if not chain_valid:
            if chain_reason == "could_not_import_hash_helpers":
                if review_count > 0:
                    failures.append("Governance hash-chain helpers unavailable for non-empty JSONL review log")
                else:
                    skips.append("Governance hash-chain helpers unavailable and JSONL review log is empty; chain check skipped")
            else:
                failures.append(f"JSONL hash chain validation failed: {chain_reason}")

    if latest_model_ids:
        missing_reviews = sorted(set(latest_model_ids) - reviewed_model_ids)
        if missing_reviews:
            warnings.append(f"Governance missing reviews for {len(missing_reviews)} latest model(s)")

    status = _resolve_status(warnings=warnings, failures=failures)
    return CategoryResult(
        "Governance store integrity",
        status,
        skips,
        warnings,
        failures,
        {"review_count": review_count, "reviewed_model_ids": sorted(reviewed_model_ids), "backend": backend},
    )


def _print_category(result: CategoryResult) -> None:
    print(f"[{result.status}] {result.name}")
    for skip in result.skips:
        print(f"[SKIP] {skip}")
    for warning in result.warnings:
        print(f"[WARN] {warning}")
    for failure in result.failures:
        print(f"[FAIL] {failure}")


def main() -> int:
    research = check_research_integrity()
    model = check_model_registry_integrity()
    governance = check_governance_integrity(model.meta.get("latest_model_ids", []))

    for result in [research, model, governance]:
        _print_category(result)

    has_fail = any(result.status == "FAIL" for result in [research, model, governance])
    has_warn = any(result.status == "WARN" for result in [research, model, governance])
    final = "FAIL" if has_fail else ("WARN" if has_warn else "PASS")
    print(f"[RESULT] {final}")
    return 1 if has_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
