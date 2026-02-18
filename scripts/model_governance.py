#!/usr/bin/env python3
"""PivotQuant model governance controller.

Implements a conservative candidate -> active promotion flow:
1) train_rf_artifacts.py writes a candidate manifest (manifest_latest.json)
2) this script evaluates promotion gates
3) if accepted, candidate is promoted to manifest_active.json
4) serving code reads manifest_active.json (fallback-safe)

Also supports rollback to previous/explicit model versions.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_MODELS_DIR = Path(os.getenv("RF_MODEL_DIR", "data/models"))
DEFAULT_CANDIDATE_MANIFEST = os.getenv("RF_CANDIDATE_MANIFEST", "manifest_latest.json")
DEFAULT_ACTIVE_MANIFEST = os.getenv("RF_ACTIVE_MANIFEST", "manifest_active.json")
DEFAULT_PREV_ACTIVE_MANIFEST = os.getenv("RF_PREV_ACTIVE_MANIFEST", "manifest_active_prev.json")
DEFAULT_STATE_FILE = os.getenv("RF_GOVERNANCE_STATE", "model_registry.json")
DEFAULT_REQUIRED_TARGETS = os.getenv("MODEL_GOV_REQUIRED_TARGETS", "reject,break")
DEFAULT_REQUIRED_HORIZONS = os.getenv("MODEL_GOV_REQUIRED_HORIZONS", "5,15,60")
DEFAULT_MIN_TRAINED_END_DELTA_MS = int(os.getenv("MODEL_GOV_MIN_TRAINED_END_DELTA_MS", "0"))
DEFAULT_MAX_MFE_REGRESSION_BPS = float(os.getenv("MODEL_GOV_MAX_MFE_REGRESSION_BPS", "1.5"))
DEFAULT_MAX_MAE_WORSENING_BPS = float(os.getenv("MODEL_GOV_MAX_MAE_WORSENING_BPS", "2.0"))
DEFAULT_ALLOW_FEATURE_VERSION_CHANGE = os.getenv(
    "MODEL_GOV_ALLOW_FEATURE_VERSION_CHANGE", "false"
).strip().lower() in {"1", "true", "yes", "y", "on"}
DEFAULT_DB = os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite"))
STATE_SCHEMA_VERSION = 1
MAX_HISTORY = 200


@dataclass
class GateConfig:
    required_targets: list[str]
    required_horizons: list[int]
    min_trained_end_delta_ms: int
    max_mfe_regression_bps: float
    max_mae_worsening_bps: float
    allow_feature_version_change: bool


def now_ms() -> int:
    return int(time.time() * 1000)


def _tmp_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.tmp-{os.getpid()}-{now_ms()}")


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(path)
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def atomic_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(dst)
    try:
        shutil.copy2(src, tmp)
        os.replace(tmp, dst)
    finally:
        if tmp.exists():
            tmp.unlink()


def parse_csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_horizons(value: str) -> list[int]:
    out: list[int] = []
    for raw in parse_csv_list(value):
        out.append(int(raw))
    return out


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing manifest: {path}")
    payload = load_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid manifest payload at {path}")
    return payload


def version_of(manifest: dict[str, Any]) -> str:
    value = manifest.get("version")
    return str(value) if value is not None else "unknown"


def empty_state() -> dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "active_version": None,
        "previous_active_version": None,
        "candidate_version": None,
        "last_action": "none",
        "last_reason": "",
        "last_checked_at_ms": 0,
        "last_promoted_at_ms": 0,
        "history": [],
    }


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return empty_state()
    payload = load_json(path)
    if not isinstance(payload, dict):
        return empty_state()
    payload.setdefault("schema_version", STATE_SCHEMA_VERSION)
    payload.setdefault("history", [])
    return payload


def push_history(state: dict[str, Any], entry: dict[str, Any]) -> None:
    history = state.setdefault("history", [])
    if not isinstance(history, list):
        history = []
        state["history"] = history
    history.append(entry)
    if len(history) > MAX_HISTORY:
        del history[: len(history) - MAX_HISTORY]


def validate_manifest(
    manifest: dict[str, Any],
    models_dir: Path,
    gates: GateConfig,
) -> list[str]:
    errors: list[str] = []
    models = manifest.get("models")
    thresholds = manifest.get("thresholds", {})
    if not isinstance(models, dict):
        return ["manifest.models missing or invalid"]

    for target in gates.required_targets:
        horizon_map = models.get(target)
        if not isinstance(horizon_map, dict):
            errors.append(f"manifest.models.{target} missing")
            continue
        for horizon in gates.required_horizons:
            key = str(horizon)
            filename = horizon_map.get(key)
            if not filename:
                errors.append(f"missing model file mapping for {target}:{horizon}m")
                continue
            path = models_dir / str(filename)
            if not path.exists():
                errors.append(f"missing model artifact for {target}:{horizon}m ({path.name})")
            threshold = thresholds.get(target, {}).get(key)
            thr = to_float(threshold)
            if thr is None:
                errors.append(f"missing threshold for {target}:{horizon}m")
            elif thr < 0.0 or thr > 1.0:
                errors.append(f"invalid threshold for {target}:{horizon}m ({thr})")

    trained_end_ts = manifest.get("trained_end_ts")
    try:
        if trained_end_ts is not None and int(trained_end_ts) <= 0:
            errors.append("trained_end_ts must be positive when present")
    except (TypeError, ValueError):
        errors.append("trained_end_ts is invalid")

    return errors


def evaluate_gates(
    active: dict[str, Any],
    candidate: dict[str, Any],
    gates: GateConfig,
) -> list[str]:
    failures: list[str] = []

    active_feature = active.get("feature_version")
    candidate_feature = candidate.get("feature_version")
    if (
        active_feature is not None
        and candidate_feature is not None
        and not gates.allow_feature_version_change
        and str(active_feature) != str(candidate_feature)
    ):
        failures.append(
            f"feature_version change blocked ({active_feature} -> {candidate_feature})"
        )

    active_end = to_float(active.get("trained_end_ts"))
    candidate_end = to_float(candidate.get("trained_end_ts"))
    if active_end is not None and candidate_end is not None:
        required = active_end + gates.min_trained_end_delta_ms
        if candidate_end < required:
            failures.append(
                f"candidate trained_end_ts not newer enough ({int(candidate_end)} < {int(required)})"
            )

    active_stats = active.get("stats", {})
    candidate_stats = candidate.get("stats", {})
    for horizon in gates.required_horizons:
        horizon_key = str(horizon)
        active_h = active_stats.get(horizon_key, {}) if isinstance(active_stats, dict) else {}
        cand_h = candidate_stats.get(horizon_key, {}) if isinstance(candidate_stats, dict) else {}

        for target in gates.required_targets:
            active_block = active_h.get(target, {}) if isinstance(active_h, dict) else {}
            cand_block = cand_h.get(target, {}) if isinstance(cand_h, dict) else {}
            if not isinstance(active_block, dict) or not isinstance(cand_block, dict):
                continue

            if target == "reject":
                mfe_key = "mfe_bps_reject"
                mae_key = "mae_bps_reject"
            else:
                mfe_key = "mfe_bps_break"
                mae_key = "mae_bps_break"

            active_mfe = to_float(active_block.get(mfe_key))
            cand_mfe = to_float(cand_block.get(mfe_key))
            if active_mfe is not None and cand_mfe is not None:
                if cand_mfe < (active_mfe - gates.max_mfe_regression_bps):
                    failures.append(
                        f"{target}:{horizon}m {mfe_key} regressed "
                        f"{active_mfe:.2f} -> {cand_mfe:.2f} (>{gates.max_mfe_regression_bps:.2f} bps)"
                    )

            active_mae = to_float(active_block.get(mae_key))
            cand_mae = to_float(cand_block.get(mae_key))
            if active_mae is not None and cand_mae is not None:
                # MAE is typically negative bps. More negative is worse.
                if cand_mae < (active_mae - gates.max_mae_worsening_bps):
                    failures.append(
                        f"{target}:{horizon}m {mae_key} worsened "
                        f"{active_mae:.2f} -> {cand_mae:.2f} (>{gates.max_mae_worsening_bps:.2f} bps)"
                    )

    return failures


def _ops_set(db_path: str, pairs: dict[str, str]) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_status (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at INTEGER NOT NULL
            );
            """
        )
        ts = now_ms()
        for key, value in pairs.items():
            conn.execute(
                """
                INSERT INTO ops_status(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE
                  SET value = excluded.value,
                      updated_at = excluded.updated_at
                """,
                (key, value, ts),
            )
        conn.commit()
    finally:
        conn.close()


def _metadata_manifest(models_dir: Path, version: str) -> Path:
    return models_dir / f"metadata_{version}.json"


def _persist_state_and_ops(
    state_path: Path,
    state: dict[str, Any],
    ops_db: str | None,
    result: dict[str, Any],
) -> None:
    atomic_write_json(state_path, state)
    if ops_db:
        _ops_set(
            ops_db,
            {
                "model_active_version": str(result.get("active_version") or ""),
                "model_candidate_version": str(result.get("candidate_version") or ""),
                "model_governance_last_action": str(result.get("action") or ""),
                "model_governance_last_reason": str(result.get("reason") or ""),
                "model_governance_last_checked_ms": str(now_ms()),
            },
        )


def cmd_status(args: argparse.Namespace) -> int:
    models_dir = Path(args.models_dir)
    candidate_path = models_dir / args.candidate_manifest
    active_path = models_dir / args.active_manifest
    prev_path = models_dir / args.prev_active_manifest
    state_path = models_dir / args.state_file
    state = load_state(state_path)

    candidate_version = None
    if candidate_path.exists():
        candidate_version = version_of(load_manifest(candidate_path))
    active_version = None
    if active_path.exists():
        active_version = version_of(load_manifest(active_path))

    out = {
        "status": "ok",
        "models_dir": str(models_dir),
        "candidate_manifest": str(candidate_path),
        "active_manifest": str(active_path),
        "prev_active_manifest": str(prev_path),
        "state_file": str(state_path),
        "candidate_exists": candidate_path.exists(),
        "active_exists": active_path.exists(),
        "previous_exists": prev_path.exists(),
        "candidate_version": candidate_version,
        "active_version": active_version,
        "state": state,
    }
    print(json.dumps(out, indent=2))
    return 0


def cmd_evaluate(args: argparse.Namespace) -> int:
    models_dir = Path(args.models_dir)
    candidate_path = models_dir / args.candidate_manifest
    active_path = models_dir / args.active_manifest
    prev_path = models_dir / args.prev_active_manifest
    state_path = models_dir / args.state_file

    gates = GateConfig(
        required_targets=parse_csv_list(args.required_targets),
        required_horizons=parse_horizons(args.required_horizons),
        min_trained_end_delta_ms=args.min_trained_end_delta_ms,
        max_mfe_regression_bps=args.max_mfe_regression_bps,
        max_mae_worsening_bps=args.max_mae_worsening_bps,
        allow_feature_version_change=args.allow_feature_version_change,
    )
    state = load_state(state_path)

    candidate = load_manifest(candidate_path)
    candidate_version = version_of(candidate)

    result: dict[str, Any] = {
        "status": "ok",
        "action": "no_change",
        "promoted": False,
        "active_version": state.get("active_version"),
        "candidate_version": candidate_version,
        "reason": "",
        "gate_failures": [],
        "paths": {
            "candidate_manifest": str(candidate_path),
            "active_manifest": str(active_path),
            "prev_active_manifest": str(prev_path),
            "state_file": str(state_path),
        },
        "gates": asdict(gates),
    }

    manifest_errors = validate_manifest(candidate, models_dir, gates)
    if manifest_errors:
        reason = "candidate manifest validation failed"
        result.update(
            {
                "action": "rejected",
                "reason": reason,
                "gate_failures": manifest_errors,
                "active_version": state.get("active_version"),
            }
        )
        state.update(
            {
                "candidate_version": candidate_version,
                "last_action": "rejected",
                "last_reason": reason + ": " + "; ".join(manifest_errors),
                "last_checked_at_ms": now_ms(),
            }
        )
        push_history(
            state,
            {
                "ts_ms": now_ms(),
                "action": "rejected",
                "candidate_version": candidate_version,
                "active_version": state.get("active_version"),
                "reason": state["last_reason"],
            },
        )
        _persist_state_and_ops(state_path, state, args.ops_db, result)
        print(json.dumps(result))
        return 0

    # Bootstrap: first accepted candidate becomes active.
    if not active_path.exists():
        atomic_copy(candidate_path, active_path)
        state.update(
            {
                "active_version": candidate_version,
                "previous_active_version": None,
                "candidate_version": candidate_version,
                "last_action": "bootstrap",
                "last_reason": "initialized active manifest from candidate",
                "last_checked_at_ms": now_ms(),
                "last_promoted_at_ms": now_ms(),
            }
        )
        push_history(
            state,
            {
                "ts_ms": now_ms(),
                "action": "bootstrap",
                "candidate_version": candidate_version,
                "active_version": candidate_version,
                "reason": state["last_reason"],
            },
        )
        result.update(
            {
                "action": "bootstrap",
                "promoted": True,
                "active_version": candidate_version,
                "reason": state["last_reason"],
            }
        )
        _persist_state_and_ops(state_path, state, args.ops_db, result)
        print(json.dumps(result))
        return 0

    active = load_manifest(active_path)
    active_version = version_of(active)
    result["active_version"] = active_version

    if candidate_version == active_version:
        state.update(
            {
                "candidate_version": candidate_version,
                "active_version": active_version,
                "last_action": "no_change",
                "last_reason": "candidate version equals active version",
                "last_checked_at_ms": now_ms(),
            }
        )
        push_history(
            state,
            {
                "ts_ms": now_ms(),
                "action": "no_change",
                "candidate_version": candidate_version,
                "active_version": active_version,
                "reason": state["last_reason"],
            },
        )
        result["reason"] = state["last_reason"]
        _persist_state_and_ops(state_path, state, args.ops_db, result)
        print(json.dumps(result))
        return 0

    gate_failures = evaluate_gates(active, candidate, gates)
    if gate_failures and not args.force_promote:
        reason = "candidate rejected by governance gates"
        result.update(
            {
                "action": "rejected",
                "promoted": False,
                "reason": reason,
                "gate_failures": gate_failures,
                "active_version": active_version,
            }
        )
        state.update(
            {
                "candidate_version": candidate_version,
                "active_version": active_version,
                "last_action": "rejected",
                "last_reason": reason + ": " + "; ".join(gate_failures),
                "last_checked_at_ms": now_ms(),
            }
        )
        push_history(
            state,
            {
                "ts_ms": now_ms(),
                "action": "rejected",
                "candidate_version": candidate_version,
                "active_version": active_version,
                "reason": state["last_reason"],
            },
        )
        _persist_state_and_ops(state_path, state, args.ops_db, result)
        print(json.dumps(result))
        return 0

    if active_path.exists():
        atomic_copy(active_path, prev_path)
    atomic_copy(candidate_path, active_path)

    state.update(
        {
            "candidate_version": candidate_version,
            "previous_active_version": active_version,
            "active_version": candidate_version,
            "last_action": "promoted",
            "last_reason": "candidate promoted to active",
            "last_checked_at_ms": now_ms(),
            "last_promoted_at_ms": now_ms(),
        }
    )
    push_history(
        state,
        {
            "ts_ms": now_ms(),
            "action": "promoted",
            "candidate_version": candidate_version,
            "active_version": candidate_version,
            "previous_active_version": active_version,
            "reason": state["last_reason"],
            "forced": bool(args.force_promote),
        },
    )
    result.update(
        {
            "action": "promoted",
            "promoted": True,
            "active_version": candidate_version,
            "reason": state["last_reason"],
            "gate_failures": gate_failures,
        }
    )
    _persist_state_and_ops(state_path, state, args.ops_db, result)
    print(json.dumps(result))
    return 0


def cmd_rollback(args: argparse.Namespace) -> int:
    models_dir = Path(args.models_dir)
    active_path = models_dir / args.active_manifest
    prev_path = models_dir / args.prev_active_manifest
    state_path = models_dir / args.state_file
    state = load_state(state_path)

    if not active_path.exists():
        raise FileNotFoundError(f"Active manifest not found: {active_path}")
    active_manifest = load_manifest(active_path)
    active_version = version_of(active_manifest)

    target_version = args.to_version or state.get("previous_active_version")
    target_path: Path | None = None
    if target_version:
        explicit = _metadata_manifest(models_dir, str(target_version))
        if explicit.exists():
            target_path = explicit
    if target_path is None and prev_path.exists():
        target_path = prev_path
    if target_path is None:
        raise FileNotFoundError(
            "No rollback candidate found. Provide --to-version or ensure manifest_active_prev.json exists."
        )

    target_manifest = load_manifest(target_path)
    target_version = version_of(target_manifest)
    if target_version == active_version:
        out = {
            "status": "ok",
            "action": "no_change",
            "reason": "rollback target is already active",
            "active_version": active_version,
            "target_version": target_version,
        }
        print(json.dumps(out))
        return 0

    atomic_copy(active_path, prev_path)
    atomic_copy(target_path, active_path)

    state.update(
        {
            "previous_active_version": active_version,
            "active_version": target_version,
            "last_action": "rollback",
            "last_reason": f"rolled back from {active_version} to {target_version}",
            "last_checked_at_ms": now_ms(),
            "last_promoted_at_ms": now_ms(),
        }
    )
    push_history(
        state,
        {
            "ts_ms": now_ms(),
            "action": "rollback",
            "active_version": target_version,
            "previous_active_version": active_version,
            "reason": state["last_reason"],
        },
    )
    result = {
        "status": "ok",
        "action": "rollback",
        "active_version": target_version,
        "candidate_version": state.get("candidate_version"),
        "reason": state["last_reason"],
    }
    _persist_state_and_ops(state_path, state, args.ops_db, result)
    print(json.dumps(result))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Model governance controller")
    parser.add_argument("--models-dir", default=str(DEFAULT_MODELS_DIR))
    parser.add_argument("--candidate-manifest", default=DEFAULT_CANDIDATE_MANIFEST)
    parser.add_argument("--active-manifest", default=DEFAULT_ACTIVE_MANIFEST)
    parser.add_argument("--prev-active-manifest", default=DEFAULT_PREV_ACTIVE_MANIFEST)
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--ops-db", default=DEFAULT_DB)

    sub = parser.add_subparsers(dest="command", required=True)

    status_cmd = sub.add_parser("status", help="Show governance status")
    status_cmd.set_defaults(func=cmd_status)

    eval_cmd = sub.add_parser("evaluate", help="Evaluate candidate and promote if gates pass")
    eval_cmd.add_argument("--required-targets", default=DEFAULT_REQUIRED_TARGETS)
    eval_cmd.add_argument("--required-horizons", default=DEFAULT_REQUIRED_HORIZONS)
    eval_cmd.add_argument(
        "--min-trained-end-delta-ms",
        type=int,
        default=DEFAULT_MIN_TRAINED_END_DELTA_MS,
    )
    eval_cmd.add_argument(
        "--max-mfe-regression-bps",
        type=float,
        default=DEFAULT_MAX_MFE_REGRESSION_BPS,
    )
    eval_cmd.add_argument(
        "--max-mae-worsening-bps",
        type=float,
        default=DEFAULT_MAX_MAE_WORSENING_BPS,
    )
    eval_cmd.add_argument(
        "--allow-feature-version-change",
        action="store_true",
        default=DEFAULT_ALLOW_FEATURE_VERSION_CHANGE,
    )
    eval_cmd.add_argument("--force-promote", action="store_true", default=False)
    eval_cmd.set_defaults(func=cmd_evaluate)

    rollback_cmd = sub.add_parser("rollback", help="Rollback active manifest")
    rollback_cmd.add_argument("--to-version", default=None, help="Version label like v010")
    rollback_cmd.set_defaults(func=cmd_rollback)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.func(args))
    except Exception as exc:  # pragma: no cover
        print(json.dumps({"status": "error", "message": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
