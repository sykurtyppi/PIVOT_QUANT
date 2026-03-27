#!/usr/bin/env python3
"""PivotQuant model governance controller.

Implements a conservative candidate -> active promotion flow:
1) train_rf_artifacts.py writes a runtime candidate manifest
2) this script evaluates promotion gates
3) if accepted, candidate is promoted to manifest_active.json
4) serving code reads manifest_active.json (fallback-safe)

Also supports rollback to previous/explicit model versions.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_MODELS_DIR = Path(os.getenv("RF_MODEL_DIR", "data/models"))
DEFAULT_METADATA_DIR = os.getenv("RF_METADATA_DIR", "metadata_runtime").strip() or "metadata_runtime"
DEFAULT_CANDIDATE_MANIFEST = (
    os.getenv("RF_CANDIDATE_MANIFEST", "manifest_runtime_latest.json").strip()
    or "manifest_runtime_latest.json"
)
DEFAULT_ACTIVE_MANIFEST = os.getenv("RF_ACTIVE_MANIFEST", "manifest_active.json")
DEFAULT_PREV_ACTIVE_MANIFEST = os.getenv("RF_PREV_ACTIVE_MANIFEST", "manifest_active_prev.json")
DEFAULT_STATE_FILE = os.getenv("RF_GOVERNANCE_STATE", "model_registry.json")
DEFAULT_REQUIRED_TARGETS = os.getenv("MODEL_GOV_REQUIRED_TARGETS", "reject,break")
DEFAULT_REQUIRED_HORIZONS = os.getenv("MODEL_GOV_REQUIRED_HORIZONS", "5,15,60")
DEFAULT_MIN_TRAINED_END_DELTA_MS = int(os.getenv("MODEL_GOV_MIN_TRAINED_END_DELTA_MS", "0"))
DEFAULT_MAX_MFE_REGRESSION_BPS = float(os.getenv("MODEL_GOV_MAX_MFE_REGRESSION_BPS", "1.5"))
DEFAULT_MAX_MAE_WORSENING_BPS = float(os.getenv("MODEL_GOV_MAX_MAE_WORSENING_BPS", "2.0"))
DEFAULT_MIN_TOTAL_SAMPLES = int(os.getenv("MODEL_GOV_MIN_TOTAL_SAMPLES", "0"))
DEFAULT_MIN_POSITIVE_SAMPLES = int(os.getenv("MODEL_GOV_MIN_POSITIVE_SAMPLES", "0"))
DEFAULT_MIN_POSITIVE_SAMPLES_REJECT = int(
    os.getenv("MODEL_GOV_MIN_POSITIVE_SAMPLES_REJECT", str(DEFAULT_MIN_POSITIVE_SAMPLES))
)
DEFAULT_MIN_POSITIVE_SAMPLES_BREAK = int(
    os.getenv("MODEL_GOV_MIN_POSITIVE_SAMPLES_BREAK", str(DEFAULT_MIN_POSITIVE_SAMPLES))
)
DEFAULT_ALLOW_FEATURE_VERSION_CHANGE = os.getenv(
    "MODEL_GOV_ALLOW_FEATURE_VERSION_CHANGE", "false"
).strip().lower() in {"1", "true", "yes", "y", "on"}
DEFAULT_REGIME_AWARE = os.getenv(
    "MODEL_GOV_REGIME_AWARE", "false"
).strip().lower() in {"1", "true", "yes", "y", "on"}
DEFAULT_REGIME_BUCKETS = os.getenv(
    "MODEL_GOV_REGIME_BUCKETS", "compression,expansion,neutral"
)
DEFAULT_REGIME_MIN_TOTAL_SAMPLES = int(
    os.getenv("MODEL_GOV_REGIME_MIN_TOTAL_SAMPLES", "0")
)
DEFAULT_REGIME_MIN_POSITIVE_SAMPLES = int(
    os.getenv("MODEL_GOV_REGIME_MIN_POSITIVE_SAMPLES", "0")
)
DEFAULT_REGIME_MIN_POSITIVE_SAMPLES_REJECT = int(
    os.getenv(
        "MODEL_GOV_REGIME_MIN_POSITIVE_SAMPLES_REJECT",
        str(DEFAULT_REGIME_MIN_POSITIVE_SAMPLES),
    )
)
DEFAULT_REGIME_MIN_POSITIVE_SAMPLES_BREAK = int(
    os.getenv(
        "MODEL_GOV_REGIME_MIN_POSITIVE_SAMPLES_BREAK",
        str(DEFAULT_REGIME_MIN_POSITIVE_SAMPLES),
    )
)
DEFAULT_REGIME_MIN_COMPARED_BUCKETS = int(
    os.getenv("MODEL_GOV_REGIME_MIN_COMPARED_BUCKETS", "1")
)
DEFAULT_ENFORCE_THRESHOLD_UTILITY_GUARD = os.getenv(
    "MODEL_GOV_ENFORCE_THRESHOLD_UTILITY_GUARD", "true"
).strip().lower() in {"1", "true", "yes", "y", "on"}
DEFAULT_THRESHOLD_UTILITY_TARGETS = os.getenv(
    "MODEL_GOV_THRESHOLD_UTILITY_TARGETS", "reject"
)
DEFAULT_THRESHOLD_UTILITY_MIN_SCORE = float(
    os.getenv("MODEL_GOV_THRESHOLD_UTILITY_MIN_SCORE", "0.0")
)
DEFAULT_ENFORCE_LIVE_EMISSION_GATE = os.getenv(
    "MODEL_GOV_ENFORCE_LIVE_EMISSION_GATE", "false"
).strip().lower() in {"1", "true", "yes", "y", "on"}
DEFAULT_EMISSION_LOOKBACK_DAYS = int(os.getenv("MODEL_GOV_EMISSION_LOOKBACK_DAYS", "5"))
DEFAULT_EMISSION_MAX_PRED_LAG_HOURS = float(
    os.getenv("MODEL_GOV_EMISSION_MAX_PRED_LAG_HOURS", "6.0")
)
DEFAULT_EMISSION_PREDICTION_BASIS = (
    os.getenv("MODEL_GOV_EMISSION_PREDICTION_BASIS", "first").strip().lower() or "first"
)
if DEFAULT_EMISSION_PREDICTION_BASIS not in {"first", "latest"}:
    DEFAULT_EMISSION_PREDICTION_BASIS = "first"
DEFAULT_EMISSION_SOURCE = (
    os.getenv("MODEL_GOV_EMISSION_SOURCE", "preview").strip().lower() or "preview"
)
if DEFAULT_EMISSION_SOURCE not in {"live", "preview", "all"}:
    DEFAULT_EMISSION_SOURCE = "preview"
DEFAULT_EMISSION_MIN_ROWS = int(os.getenv("MODEL_GOV_EMISSION_MIN_ROWS", "25"))
DEFAULT_EMISSION_MIN_COVERAGE = float(os.getenv("MODEL_GOV_EMISSION_MIN_COVERAGE", "0.9"))
DEFAULT_EMISSION_MIN_SIGNALS = int(os.getenv("MODEL_GOV_EMISSION_MIN_SIGNALS", "1"))
DEFAULT_EMISSION_MAX_ABSTAIN_RATE = float(
    os.getenv("MODEL_GOV_EMISSION_MAX_ABSTAIN_RATE", "0.98")
)
DEFAULT_EMISSION_SYMBOLS = os.getenv("MODEL_GOV_EMISSION_SYMBOLS", "")
DEFAULT_EMISSION_DB = os.getenv("MODEL_GOV_EMISSION_DB", "").strip()
DEFAULT_DB = os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite"))
STATE_SCHEMA_VERSION = 1
MAX_HISTORY = 200
LEGACY_CANDIDATE_MANIFEST = "manifest_latest.json"


@dataclass
class GateConfig:
    required_targets: list[str]
    required_horizons: list[int]
    min_trained_end_delta_ms: int
    max_mfe_regression_bps: float
    max_mae_worsening_bps: float
    min_total_samples: int
    min_positive_samples_reject: int
    min_positive_samples_break: int
    allow_feature_version_change: bool
    regime_aware: bool = False
    regime_buckets: list[str] = field(
        default_factory=lambda: ["compression", "expansion", "neutral"]
    )
    regime_min_total_samples: int = 0
    regime_min_positive_samples_reject: int = 0
    regime_min_positive_samples_break: int = 0
    regime_min_compared_buckets: int = 1
    enforce_threshold_utility_guard: bool = False
    threshold_utility_targets: list[str] = field(default_factory=lambda: ["reject"])
    threshold_utility_min_score: float = 0.0
    enforce_live_emission_gate: bool = False
    emission_lookback_days: int = 5
    emission_max_pred_lag_hours: float = 6.0
    emission_prediction_basis: str = "first"
    emission_source: str = "preview"
    emission_min_rows: int = 25
    emission_min_coverage: float = 0.9
    emission_min_signals: int = 1
    emission_max_abstain_rate: float = 0.98
    emission_symbols: list[str] = field(default_factory=list)


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


def resolve_candidate_manifest_path(models_dir: Path, configured_name: str) -> Path:
    configured_path = models_dir / configured_name
    if configured_path.exists():
        return configured_path
    if configured_path.name != LEGACY_CANDIDATE_MANIFEST:
        legacy_path = models_dir / LEGACY_CANDIDATE_MANIFEST
        if legacy_path.exists():
            return legacy_path
    return configured_path


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _regime_positive_floor(gates: GateConfig, target: str) -> int:
    if target == "reject":
        return int(gates.regime_min_positive_samples_reject)
    return int(gates.regime_min_positive_samples_break)


def _regime_bucket_blocks(block: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw = block.get("by_regime")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for bucket, payload in raw.items():
        if isinstance(bucket, str) and isinstance(payload, dict):
            out[bucket] = payload
    return out


def _regime_support_ok(
    *,
    bucket_block: dict[str, Any],
    target: str,
    gates: GateConfig,
) -> tuple[bool, str | None]:
    sample_size = to_int(bucket_block.get("sample_size"))
    min_total = int(gates.regime_min_total_samples)
    if min_total > 0 and (sample_size is None or sample_size < min_total):
        return False, f"sample_size<{min_total}"

    min_positive = _regime_positive_floor(gates, target)
    if min_positive > 0:
        pos_key = f"{target}_count"
        pos_count = to_int(bucket_block.get(pos_key))
        if pos_count is None or pos_count < min_positive:
            return False, f"{pos_key}<{min_positive}"

    return True, None


def _is_metric_regression(
    *,
    active_value: float,
    candidate_value: float,
    tolerance: float,
) -> bool:
    return candidate_value < (active_value - tolerance)


def _evaluate_regime_metric(
    *,
    target: str,
    horizon: int,
    metric_key: str,
    failure_label: str,
    tolerance: float,
    active_block: dict[str, Any],
    candidate_block: dict[str, Any],
    gates: GateConfig,
) -> tuple[bool, str | None, list[str]]:
    """Evaluate one metric with optional regime-aware waivers.

    Returns:
      (failed, message_if_failed, skip_messages)
    """
    active_metric = to_float(active_block.get(metric_key))
    candidate_metric = to_float(candidate_block.get(metric_key))
    if active_metric is None or candidate_metric is None:
        missing: list[str] = []
        if active_metric is None:
            missing.append("active")
        if candidate_metric is None:
            missing.append("candidate")
        return (
            False,
            None,
            [
                f"{target}:{horizon}m skipped {metric_key} regression gate "
                f"(missing {'/'.join(missing)} metric)"
            ],
        )

    aggregate_failed = _is_metric_regression(
        active_value=active_metric,
        candidate_value=candidate_metric,
        tolerance=tolerance,
    )
    if not aggregate_failed:
        return False, None, []

    # Default behavior: fail on aggregate regression.
    aggregate_message = (
        f"{target}:{horizon}m {metric_key} {failure_label} "
        f"{active_metric:.2f} -> {candidate_metric:.2f} (>{tolerance:.2f} bps)"
    )

    if not gates.regime_aware:
        return True, aggregate_message, []

    active_regimes = _regime_bucket_blocks(active_block)
    candidate_regimes = _regime_bucket_blocks(candidate_block)
    if not candidate_regimes:
        # Candidate has no per-regime stats — cannot waive aggregate gate.
        return True, aggregate_message, []
    if not active_regimes:
        # Active predates by_regime stats (bootstrap case). Waive the aggregate
        # gate so regime-aware governance is not blocked by legacy manifests.
        skip_msg = (
            f"{target}:{horizon}m {metric_key} regime-aware gate waived "
            f"(active_no_regime_data; aggregate {active_metric:.2f} -> "
            f"{candidate_metric:.2f})"
        )
        return False, aggregate_message, [skip_msg]

    bucket_failures: list[str] = []
    bucket_skips: list[str] = []
    compared = 0
    for bucket in gates.regime_buckets:
        active_bucket = active_regimes.get(bucket)
        candidate_bucket = candidate_regimes.get(bucket)
        if active_bucket is None or candidate_bucket is None:
            bucket_skips.append(f"{bucket}:missing_bucket")
            continue
        active_ok, active_reason = _regime_support_ok(
            bucket_block=active_bucket,
            target=target,
            gates=gates,
        )
        candidate_ok, candidate_reason = _regime_support_ok(
            bucket_block=candidate_bucket,
            target=target,
            gates=gates,
        )
        if not active_ok or not candidate_ok:
            reason = (
                f"{bucket}:support(active={active_reason or 'ok'},"
                f"candidate={candidate_reason or 'ok'})"
            )
            bucket_skips.append(reason)
            continue

        active_bucket_metric = to_float(active_bucket.get(metric_key))
        candidate_bucket_metric = to_float(candidate_bucket.get(metric_key))
        if active_bucket_metric is None or candidate_bucket_metric is None:
            bucket_skips.append(f"{bucket}:missing_metric")
            continue

        compared += 1
        if _is_metric_regression(
            active_value=active_bucket_metric,
            candidate_value=candidate_bucket_metric,
            tolerance=tolerance,
        ):
            bucket_failures.append(
                f"{target}:{horizon}m {metric_key} {failure_label} in {bucket} "
                f"{active_bucket_metric:.2f} -> {candidate_bucket_metric:.2f} "
                f"(>{tolerance:.2f} bps)"
            )

    if compared < max(1, int(gates.regime_min_compared_buckets)):
        return True, aggregate_message, [
            f"{target}:{horizon}m regime-aware check skipped "
            f"(compared_buckets={compared} < min_compared={max(1, int(gates.regime_min_compared_buckets))}; "
            f"details={';'.join(bucket_skips) if bucket_skips else 'none'})"
        ]

    if bucket_failures:
        details = "; ".join(bucket_failures)
        return True, details, []

    major_active_bucket = max(
        active_regimes.items(),
        key=lambda item: to_int(item[1].get("sample_size")) or 0,
    )[0]
    major_candidate_bucket = max(
        candidate_regimes.items(),
        key=lambda item: to_int(item[1].get("sample_size")) or 0,
    )[0]
    skip_detail = (
        f"{target}:{horizon}m {metric_key} aggregate {failure_label} waived by regime-aware check "
        f"(compared_buckets={compared}, major_bucket={major_active_bucket}->{major_candidate_bucket})"
    )
    if bucket_skips:
        skip_detail += f"; skipped={';'.join(bucket_skips)}"
    return False, None, [skip_detail]


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


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except sqlite3.Error:
        return set()
    return {str(row[1]) for row in rows}


def _prediction_source_filter_sql(pred_cols: set[str], source: str) -> tuple[str, list[Any]]:
    normalized = (source or "preview").strip().lower()
    if normalized not in {"live", "preview", "all"}:
        raise ValueError(f"Unsupported emission source: {source!r}")
    if normalized == "all":
        return "", []
    if "is_preview" not in pred_cols:
        if normalized == "preview":
            return "AND 1 = 0", []
        return "", []
    return "AND COALESCE(pl.is_preview, 0) = ?", [1 if normalized == "preview" else 0]


def push_history(state: dict[str, Any], entry: dict[str, Any]) -> None:
    history = state.setdefault("history", [])
    if not isinstance(history, list):
        history = []
        state["history"] = history
    history.append(entry)
    if len(history) > MAX_HISTORY:
        del history[: len(history) - MAX_HISTORY]


def _history_entry(
    *,
    action: str,
    reason: str,
    gates: GateConfig | None = None,
    gate_failures: list[str] | None = None,
    gate_skips: list[str] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "ts_ms": now_ms(),
        "action": action,
        "reason": reason,
    }
    if gates is not None:
        entry["gate_config"] = asdict(gates)
    if gate_failures is not None:
        entry["gate_failures"] = [str(item) for item in gate_failures]
    if gate_skips is not None:
        entry["gate_skips"] = [str(item) for item in gate_skips]
    entry.update(extra)
    return entry


def _format_horizon_labels(horizons: list[int]) -> str:
    return ",".join(f"{horizon}m" for horizon in horizons)


def harden_break_fallback_thresholds(
    manifest: dict[str, Any],
    *,
    no_trade_threshold: float = 1.0,
    applied_at_ms: int | None = None,
) -> tuple[dict[str, Any], list[int]]:
    threshold_value = float(no_trade_threshold)
    if threshold_value < 0.0 or threshold_value > 1.0:
        raise ValueError(f"no_trade_threshold must be within [0.0, 1.0], got {threshold_value}")

    hardened = copy.deepcopy(manifest)
    thresholds = hardened.get("thresholds")
    if not isinstance(thresholds, dict):
        return hardened, []

    break_thresholds = thresholds.get("break")
    if not isinstance(break_thresholds, dict):
        return hardened, []

    thresholds_meta = hardened.get("thresholds_meta")
    break_meta = thresholds_meta.get("break", {}) if isinstance(thresholds_meta, dict) else {}
    if not isinstance(break_meta, dict):
        break_meta = {}

    hardened_horizons: list[int] = []
    override_details: dict[str, dict[str, Any]] = {}
    for horizon_key, raw_threshold in list(break_thresholds.items()):
        threshold = to_float(raw_threshold)
        if threshold is None or threshold >= threshold_value:
            continue
        meta = break_meta.get(horizon_key, {})
        if not isinstance(meta, dict) or not bool(meta.get("fallback")):
            continue
        break_thresholds[horizon_key] = threshold_value
        horizon = to_int(horizon_key)
        if horizon is not None:
            hardened_horizons.append(horizon)
        override_details[str(horizon_key)] = {
            "from": threshold,
            "to": threshold_value,
            "reason": "fallback_threshold",
        }

    if not override_details:
        return hardened, []

    runtime_overrides = hardened.get("runtime_overrides")
    if not isinstance(runtime_overrides, dict):
        runtime_overrides = {}
        hardened["runtime_overrides"] = runtime_overrides

    rollback_overrides = runtime_overrides.get("rollback")
    if not isinstance(rollback_overrides, dict):
        rollback_overrides = {}
        runtime_overrides["rollback"] = rollback_overrides

    rollback_overrides.update(
        {
            "source_version": version_of(manifest),
            "applied_at_ms": int(applied_at_ms or now_ms()),
            "break_threshold_overrides": override_details,
            "break_fallbacks_hardened": sorted(hardened_horizons),
        }
    )
    return hardened, sorted(hardened_horizons)


def evaluate_live_emission_gate(
    db_path: str,
    *,
    candidate_version: str,
    gates: GateConfig,
) -> tuple[list[str], dict[str, Any]]:
    db = Path(db_path).expanduser()
    failures: list[str] = []
    summary: dict[str, Any] = {
        "db_path": str(db),
        "candidate_version": candidate_version,
        "source": gates.emission_source,
        "prediction_basis": gates.emission_prediction_basis,
        "lookback_days": int(gates.emission_lookback_days),
        "max_pred_lag_hours": float(gates.emission_max_pred_lag_hours),
        "min_rows": int(gates.emission_min_rows),
        "min_coverage": float(gates.emission_min_coverage),
        "min_signals": int(gates.emission_min_signals),
        "max_abstain_rate": float(gates.emission_max_abstain_rate),
        "symbols": list(gates.emission_symbols),
    }

    if not db.exists():
        failures.append(f"candidate emission gate data source missing ({db})")
        summary["status"] = "missing_db"
        return failures, summary

    conn = sqlite3.connect(str(db))
    try:
        conn.row_factory = sqlite3.Row
        if not _table_exists(conn, "touch_events"):
            failures.append("candidate emission gate requires touch_events table")
            summary["status"] = "missing_touch_events"
            return failures, summary
        if not _table_exists(conn, "prediction_log"):
            failures.append("candidate emission gate requires prediction_log table")
            summary["status"] = "missing_prediction_log"
            return failures, summary

        pred_cols = _table_columns(conn, "prediction_log")
        required_cols = {"event_id", "ts_prediction", "model_version", "best_horizon", "abstain"}
        missing_cols = sorted(required_cols - pred_cols)
        if missing_cols:
            failures.append(
                "candidate emission gate requires prediction_log columns: "
                + ", ".join(missing_cols)
            )
            summary["status"] = "missing_prediction_columns"
            summary["missing_prediction_columns"] = missing_cols
            return failures, summary

        has_event_labels = _table_exists(conn, "event_labels")
        labeled_signal_sql = "0 AS labeled_signal_rows"
        if has_event_labels:
            labeled_signal_sql = """
                SUM(
                    CASE
                        WHEN sp.best_horizon IS NOT NULL
                         AND COALESCE(sp.abstain, 0) = 0
                         AND EXISTS (
                             SELECT 1
                             FROM event_labels el
                             WHERE el.event_id = sp.event_id
                               AND el.horizon_min = sp.best_horizon
                         )
                        THEN 1 ELSE 0
                    END
                ) AS labeled_signal_rows
            """

        anchor_row = conn.execute(
            "SELECT MAX(ts_event) AS max_ts_event FROM touch_events"
        ).fetchone()
        anchor_ts_ms = to_int(anchor_row["max_ts_event"] if anchor_row else None) or now_ms()
        window_end_ms = anchor_ts_ms + 1
        window_start_ms = anchor_ts_ms - max(0, int(gates.emission_lookback_days)) * 86_400_000
        max_pred_lag_ms = int(max(0.0, float(gates.emission_max_pred_lag_hours)) * 3600 * 1000)
        pred_order = "ASC" if gates.emission_prediction_basis == "first" else "DESC"
        source_filter_sql, source_params = _prediction_source_filter_sql(pred_cols, gates.emission_source)
        symbol_filter_sql = ""
        symbol_params: list[Any] = []
        emission_symbols = [str(symbol).strip().upper() for symbol in gates.emission_symbols if str(symbol).strip()]
        touch_cols = _table_columns(conn, "touch_events")
        if emission_symbols:
            if "symbol" not in touch_cols:
                failures.append("candidate emission gate symbol filtering requires touch_events.symbol")
                summary["status"] = "missing_touch_symbol"
                return failures, summary
            symbol_placeholders = ",".join("?" for _ in emission_symbols)
            symbol_filter_sql = f"AND UPPER(COALESCE(te.symbol, '')) IN ({symbol_placeholders})"
            symbol_params.extend(emission_symbols)

        lag_filter_sql = """
                      AND (pl.ts_prediction - st.ts_event) >= 0
                      AND (pl.ts_prediction - st.ts_event) <= ?
        """
        lag_filter_params: list[Any] = [max_pred_lag_ms]
        max_pred_lag_applied = True
        if gates.emission_source == "preview":
            # Preview/shadow scoring is retrospective by design: rows are
            # written "now" for recent historical touch events, so applying a
            # live lag filter would incorrectly discard every candidate row.
            lag_filter_sql = ""
            lag_filter_params = []
            max_pred_lag_applied = False

        summary.update(
            {
                "anchor_ts_ms": anchor_ts_ms,
                "window_start_ts_ms": window_start_ms,
                "window_end_ts_ms": window_end_ms,
                "max_pred_lag_applied": max_pred_lag_applied,
            }
        )

        params: list[Any] = [window_start_ms, window_end_ms]
        params.extend(symbol_params)
        params.append(candidate_version)
        params.extend(source_params)
        params.extend(lag_filter_params)
        row = conn.execute(
            f"""
            WITH scoped_touch AS (
                SELECT te.event_id, te.ts_event
                FROM touch_events te
                WHERE te.ts_event >= ?
                  AND te.ts_event < ?
                  {symbol_filter_sql}
            ),
            selected_pred AS (
                SELECT *
                FROM (
                    SELECT
                        pl.event_id,
                        pl.ts_prediction,
                        pl.best_horizon,
                        pl.abstain,
                        ROW_NUMBER() OVER (
                            PARTITION BY pl.event_id
                            ORDER BY pl.ts_prediction {pred_order}
                        ) AS rn
                    FROM scoped_touch st
                    JOIN prediction_log pl ON pl.event_id = st.event_id
                    WHERE COALESCE(pl.model_version, '') = ?
                      {source_filter_sql}
                      {lag_filter_sql}
                )
                WHERE rn = 1
            )
            SELECT
                (SELECT COUNT(*) FROM scoped_touch) AS touch_rows,
                COUNT(*) AS selected_rows,
                SUM(
                    CASE
                        WHEN sp.best_horizon IS NOT NULL AND COALESCE(sp.abstain, 0) = 0
                        THEN 1 ELSE 0
                    END
                ) AS signal_rows,
                SUM(
                    CASE
                        WHEN sp.best_horizon IS NULL OR COALESCE(sp.abstain, 0) = 1
                        THEN 1 ELSE 0
                    END
                ) AS abstain_rows,
                {labeled_signal_sql},
                SUM(CASE WHEN sp.best_horizon = 5  AND COALESCE(sp.abstain, 0) = 0 THEN 1 ELSE 0 END) AS signal_5m_rows,
                SUM(CASE WHEN sp.best_horizon = 15 AND COALESCE(sp.abstain, 0) = 0 THEN 1 ELSE 0 END) AS signal_15m_rows,
                SUM(CASE WHEN sp.best_horizon = 30 AND COALESCE(sp.abstain, 0) = 0 THEN 1 ELSE 0 END) AS signal_30m_rows,
                SUM(CASE WHEN sp.best_horizon = 60 AND COALESCE(sp.abstain, 0) = 0 THEN 1 ELSE 0 END) AS signal_60m_rows
            FROM selected_pred sp
            """,
            params,
        ).fetchone()

        touch_rows = int((row["touch_rows"] if row else 0) or 0)
        selected_rows = int((row["selected_rows"] if row else 0) or 0)
        signal_rows = int((row["signal_rows"] if row else 0) or 0)
        abstain_rows = int((row["abstain_rows"] if row else 0) or 0)
        labeled_signal_rows = int((row["labeled_signal_rows"] if row else 0) or 0)
        coverage = (selected_rows / touch_rows) if touch_rows > 0 else None
        abstain_rate = (abstain_rows / selected_rows) if selected_rows > 0 else 1.0
        signal_counts = {
            "5": int((row["signal_5m_rows"] if row else 0) or 0),
            "15": int((row["signal_15m_rows"] if row else 0) or 0),
            "30": int((row["signal_30m_rows"] if row else 0) or 0),
            "60": int((row["signal_60m_rows"] if row else 0) or 0),
        }

        summary.update(
            {
                "status": "ok",
                "has_event_labels": has_event_labels,
                "touch_rows": touch_rows,
                "selected_rows": selected_rows,
                "coverage": round(coverage, 6) if coverage is not None else None,
                "signal_rows": signal_rows,
                "abstain_rows": abstain_rows,
                "abstain_rate": round(abstain_rate, 6),
                "labeled_signal_rows": labeled_signal_rows,
                "signal_horizon_counts": signal_counts,
            }
        )

        if touch_rows <= 0:
            failures.append("candidate emission gate found no touch_events in the evaluation window")
            summary["status"] = "no_touch_events"
            return failures, summary

        if selected_rows < int(gates.emission_min_rows):
            failures.append(
                f"candidate emission gate selected_rows {selected_rows} < "
                f"min_rows {int(gates.emission_min_rows)}"
            )

        min_coverage = max(0.0, min(1.0, float(gates.emission_min_coverage)))
        if coverage is None or coverage < min_coverage:
            failures.append(
                f"candidate emission gate coverage {coverage if coverage is not None else 0.0:.3f} < "
                f"min_coverage {min_coverage:.3f}"
            )

        if signal_rows < int(gates.emission_min_signals):
            failures.append(
                f"candidate emission gate signal_rows {signal_rows} < "
                f"min_signals {int(gates.emission_min_signals)}"
            )

        max_abstain_rate = max(0.0, min(1.0, float(gates.emission_max_abstain_rate)))
        if abstain_rate > max_abstain_rate:
            failures.append(
                f"candidate emission gate abstain_rate {abstain_rate:.3f} > "
                f"max_abstain_rate {max_abstain_rate:.3f}"
            )

        return failures, summary
    finally:
        conn.close()


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
) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    skips: list[str] = []

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

    if gates.enforce_threshold_utility_guard:
        candidate_thresholds = candidate.get("thresholds", {})
        candidate_thresholds_meta = candidate.get("thresholds_meta", {})
        if not isinstance(candidate_thresholds, dict):
            candidate_thresholds = {}
        if not isinstance(candidate_thresholds_meta, dict):
            candidate_thresholds_meta = {}

        for horizon in gates.required_horizons:
            horizon_key = str(horizon)
            for target in gates.threshold_utility_targets:
                target_thresholds = candidate_thresholds.get(target, {})
                target_meta_map = candidate_thresholds_meta.get(target, {})
                if not isinstance(target_thresholds, dict):
                    target_thresholds = {}
                if not isinstance(target_meta_map, dict):
                    target_meta_map = {}

                threshold = to_float(target_thresholds.get(horizon_key))
                target_meta = target_meta_map.get(horizon_key, {})
                if not isinstance(target_meta, dict):
                    target_meta = {}

                if threshold is None:
                    failures.append(
                        f"{target}:{horizon}m threshold utility guard check failed (missing threshold)"
                    )
                    continue

                objective = str(target_meta.get("objective") or "")
                if objective != "utility_bps":
                    failures.append(
                        f"{target}:{horizon}m threshold utility guard check failed (objective={objective or 'missing'})"
                    )
                    continue

                guard_applied = bool(target_meta.get("guard_applied"))
                guard_reason = str(target_meta.get("guard_reason") or "").strip()
                if guard_applied:
                    failures.append(
                        f"{target}:{horizon}m threshold utility guard applied ({guard_reason or 'unspecified'})"
                    )
                    continue

                score = to_float(target_meta.get("score"))
                if score is None:
                    failures.append(
                        f"{target}:{horizon}m threshold utility guard check failed (missing threshold score)"
                    )
                    continue

                if score <= float(gates.threshold_utility_min_score):
                    failures.append(
                        f"{target}:{horizon}m threshold utility score {score:.3f} <= "
                        f"min_score {float(gates.threshold_utility_min_score):.3f}"
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

            active_sample = to_int(active_block.get("sample_size"))
            cand_sample = to_int(cand_block.get("sample_size"))
            if active_sample is None:
                active_sample = to_int(active_h.get("sample_size")) if isinstance(active_h, dict) else None
            if cand_sample is None:
                cand_sample = to_int(cand_h.get("sample_size")) if isinstance(cand_h, dict) else None
            if gates.min_total_samples > 0:
                if active_sample is None or cand_sample is None:
                    skips.append(
                        f"{target}:{horizon}m skipped regression gates "
                        f"(missing sample_size; min_total={gates.min_total_samples})"
                    )
                    continue
                if active_sample < gates.min_total_samples or cand_sample < gates.min_total_samples:
                    skips.append(
                        f"{target}:{horizon}m skipped regression gates "
                        f"(sample_size active={active_sample} candidate={cand_sample} "
                        f"< min_total={gates.min_total_samples})"
                    )
                    continue

            required_positive = (
                gates.min_positive_samples_reject
                if target == "reject"
                else gates.min_positive_samples_break
            )
            if required_positive > 0:
                pos_key = f"{target}_count"
                active_pos = to_int(active_block.get(pos_key))
                cand_pos = to_int(cand_block.get(pos_key))
                if active_pos is None or cand_pos is None:
                    skips.append(
                        f"{target}:{horizon}m skipped regression gates "
                        f"(missing {pos_key}; min_positive={required_positive})"
                    )
                    continue
                if active_pos < required_positive or cand_pos < required_positive:
                    skips.append(
                        f"{target}:{horizon}m skipped regression gates "
                        f"({pos_key} active={active_pos} candidate={cand_pos} "
                        f"< min_positive={required_positive})"
                    )
                    continue

            if target == "reject":
                mfe_key = "mfe_bps_reject"
                mae_key = "mae_bps_reject"
            else:
                mfe_key = "mfe_bps_break"
                mae_key = "mae_bps_break"

            mfe_failed, mfe_message, mfe_skips = _evaluate_regime_metric(
                target=target,
                horizon=horizon,
                metric_key=mfe_key,
                failure_label="regressed",
                tolerance=gates.max_mfe_regression_bps,
                active_block=active_block,
                candidate_block=cand_block,
                gates=gates,
            )
            if mfe_failed and mfe_message:
                failures.append(mfe_message)
            if mfe_skips:
                skips.extend(mfe_skips)

            # MAE is typically negative bps. More negative is worse.
            mae_failed, mae_message, mae_skips = _evaluate_regime_metric(
                target=target,
                horizon=horizon,
                metric_key=mae_key,
                failure_label="worsened",
                tolerance=gates.max_mae_worsening_bps,
                active_block=active_block,
                candidate_block=cand_block,
                gates=gates,
            )
            if mae_failed and mae_message:
                failures.append(mae_message)
            if mae_skips:
                skips.extend(mae_skips)

    return failures, skips


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


def _resolve_metadata_dir(models_dir: Path, raw_metadata_dir: str) -> Path:
    candidate = Path(raw_metadata_dir)
    if not candidate.is_absolute():
        candidate = models_dir / candidate
    return candidate


def _metadata_manifest_candidates(
    models_dir: Path, metadata_dir: Path, version: str
) -> list[Path]:
    preferred = metadata_dir / f"metadata_{version}.json"
    legacy = models_dir / f"metadata_{version}.json"
    if preferred == legacy:
        return [preferred]
    return [preferred, legacy]


def _persist_state_and_ops(
    state_path: Path,
    state: dict[str, Any],
    ops_db: str | None,
    result: dict[str, Any],
) -> None:
    reason = str(result.get("reason") or "")
    gate_failures = result.get("gate_failures")
    if isinstance(gate_failures, list):
        details = [str(item).strip() for item in gate_failures if str(item).strip()]
        if details:
            reason = f"{reason}: {'; '.join(details)}" if reason else "; ".join(details)

    atomic_write_json(state_path, state)
    if ops_db:
        _ops_set(
            ops_db,
            {
                "model_active_version": str(result.get("active_version") or ""),
                "model_candidate_version": str(result.get("candidate_version") or ""),
                "model_governance_last_action": str(result.get("action") or ""),
                "model_governance_last_reason": reason,
                "model_governance_last_checked_ms": str(now_ms()),
            },
        )


def cmd_status(args: argparse.Namespace) -> int:
    models_dir = Path(args.models_dir)
    metadata_dir = _resolve_metadata_dir(models_dir, args.metadata_dir)
    candidate_path = resolve_candidate_manifest_path(models_dir, args.candidate_manifest)
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
        "metadata_dir": str(metadata_dir),
        "candidate_manifest_configured": str(models_dir / args.candidate_manifest),
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
    metadata_dir = _resolve_metadata_dir(models_dir, args.metadata_dir)
    candidate_path = resolve_candidate_manifest_path(models_dir, args.candidate_manifest)
    active_path = models_dir / args.active_manifest
    prev_path = models_dir / args.prev_active_manifest
    state_path = models_dir / args.state_file

    gates = GateConfig(
        required_targets=parse_csv_list(args.required_targets),
        required_horizons=parse_horizons(args.required_horizons),
        min_trained_end_delta_ms=args.min_trained_end_delta_ms,
        max_mfe_regression_bps=args.max_mfe_regression_bps,
        max_mae_worsening_bps=args.max_mae_worsening_bps,
        min_total_samples=args.min_total_samples,
        min_positive_samples_reject=max(
            int(args.min_positive_samples), int(args.min_positive_samples_reject)
        ),
        min_positive_samples_break=max(
            int(args.min_positive_samples), int(args.min_positive_samples_break)
        ),
        allow_feature_version_change=args.allow_feature_version_change,
        regime_aware=bool(args.regime_aware),
        regime_buckets=parse_csv_list(args.regime_buckets) or [
            "compression",
            "expansion",
            "neutral",
        ],
        regime_min_total_samples=int(args.regime_min_total_samples),
        regime_min_positive_samples_reject=max(
            int(args.regime_min_positive_samples),
            int(args.regime_min_positive_samples_reject),
        ),
        regime_min_positive_samples_break=max(
            int(args.regime_min_positive_samples),
            int(args.regime_min_positive_samples_break),
        ),
        regime_min_compared_buckets=max(1, int(args.regime_min_compared_buckets)),
        enforce_threshold_utility_guard=bool(args.enforce_threshold_utility_guard),
        threshold_utility_targets=parse_csv_list(args.threshold_utility_targets) or ["reject"],
        threshold_utility_min_score=float(args.threshold_utility_min_score),
        enforce_live_emission_gate=bool(
            getattr(args, "enforce_live_emission_gate", DEFAULT_ENFORCE_LIVE_EMISSION_GATE)
        ),
        emission_lookback_days=max(
            0, int(getattr(args, "emission_lookback_days", DEFAULT_EMISSION_LOOKBACK_DAYS))
        ),
        emission_max_pred_lag_hours=max(
            0.0,
            float(
                getattr(
                    args,
                    "emission_max_pred_lag_hours",
                    DEFAULT_EMISSION_MAX_PRED_LAG_HOURS,
                )
            ),
        ),
        emission_prediction_basis=(
            str(
                getattr(
                    args,
                    "emission_prediction_basis",
                    DEFAULT_EMISSION_PREDICTION_BASIS,
                )
            ).strip().lower()
            or "first"
        ),
        emission_source=(
            str(getattr(args, "emission_source", DEFAULT_EMISSION_SOURCE)).strip().lower()
            or "preview"
        ),
        emission_min_rows=max(
            0, int(getattr(args, "emission_min_rows", DEFAULT_EMISSION_MIN_ROWS))
        ),
        emission_min_coverage=max(
            0.0,
            min(
                1.0,
                float(getattr(args, "emission_min_coverage", DEFAULT_EMISSION_MIN_COVERAGE)),
            ),
        ),
        emission_min_signals=max(
            0, int(getattr(args, "emission_min_signals", DEFAULT_EMISSION_MIN_SIGNALS))
        ),
        emission_max_abstain_rate=max(
            0.0,
            min(
                1.0,
                float(
                    getattr(
                        args,
                        "emission_max_abstain_rate",
                        DEFAULT_EMISSION_MAX_ABSTAIN_RATE,
                    )
                ),
            ),
        ),
        emission_symbols=[
            token.strip().upper()
            for token in parse_csv_list(
                str(getattr(args, "emission_symbols", DEFAULT_EMISSION_SYMBOLS))
            )
            if token.strip()
        ],
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
        "gate_skips": [],
        "paths": {
            "metadata_dir": str(metadata_dir),
            "candidate_manifest_configured": str(models_dir / args.candidate_manifest),
            "candidate_manifest": str(candidate_path),
            "active_manifest": str(active_path),
            "prev_active_manifest": str(prev_path),
            "state_file": str(state_path),
        },
        "gates": asdict(gates),
    }

    emission_db = (
        str(getattr(args, "emission_db", "") or args.ops_db or "").strip() or DEFAULT_DB
    ).strip()
    result["paths"]["emission_db"] = emission_db
    emission_gate_failures: list[str] = []
    emission_gate_summary: dict[str, Any] = {
        "status": "disabled",
        "candidate_version": candidate_version,
        "db_path": emission_db,
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
            _history_entry(
                action="rejected",
                reason=state["last_reason"],
                gates=gates,
                gate_failures=manifest_errors,
                candidate_version=candidate_version,
                active_version=state.get("active_version"),
            ),
        )
        _persist_state_and_ops(state_path, state, args.ops_db, result)
        print(json.dumps(result))
        return 0

    if gates.enforce_live_emission_gate:
        emission_gate_failures, emission_gate_summary = evaluate_live_emission_gate(
            emission_db,
            candidate_version=candidate_version,
            gates=gates,
        )
    result["emission_gate"] = emission_gate_summary

    # Bootstrap: first accepted candidate becomes active.
    if not active_path.exists():
        if emission_gate_failures and not args.force_promote:
            reason = "candidate rejected by governance gates"
            result.update(
                {
                    "action": "rejected",
                    "reason": reason,
                    "gate_failures": emission_gate_failures,
                    "active_version": state.get("active_version"),
                }
            )
            state.update(
                {
                    "candidate_version": candidate_version,
                    "last_action": "rejected",
                    "last_reason": reason + ": " + "; ".join(emission_gate_failures),
                    "last_checked_at_ms": now_ms(),
                }
            )
            push_history(
                state,
                _history_entry(
                    action="rejected",
                    reason=state["last_reason"],
                    gates=gates,
                    gate_failures=emission_gate_failures,
                    candidate_version=candidate_version,
                    active_version=state.get("active_version"),
                    emission_gate=emission_gate_summary,
                ),
            )
            _persist_state_and_ops(state_path, state, args.ops_db, result)
            print(json.dumps(result))
            return 0
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
            _history_entry(
                action="bootstrap",
                reason=state["last_reason"],
                gates=gates,
                candidate_version=candidate_version,
                active_version=candidate_version,
                emission_gate=emission_gate_summary,
            ),
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
            _history_entry(
                action="no_change",
                reason=state["last_reason"],
                gates=gates,
                candidate_version=candidate_version,
                active_version=active_version,
            ),
        )
        result["reason"] = state["last_reason"]
        _persist_state_and_ops(state_path, state, args.ops_db, result)
        print(json.dumps(result))
        return 0

    gate_failures, gate_skips = evaluate_gates(active, candidate, gates)
    combined_gate_failures = list(gate_failures)
    combined_gate_failures.extend(emission_gate_failures)
    if combined_gate_failures and not args.force_promote:
        reason = "candidate rejected by governance gates"
        result.update(
            {
                "action": "rejected",
                "promoted": False,
                "reason": reason,
                "gate_failures": combined_gate_failures,
                "gate_skips": gate_skips,
                "active_version": active_version,
            }
        )
        state.update(
            {
                "candidate_version": candidate_version,
                "active_version": active_version,
                "last_action": "rejected",
                "last_reason": reason + ": " + "; ".join(combined_gate_failures),
                "last_checked_at_ms": now_ms(),
            }
        )
        push_history(
            state,
            _history_entry(
                action="rejected",
                reason=state["last_reason"],
                gates=gates,
                gate_failures=combined_gate_failures,
                gate_skips=gate_skips,
                candidate_version=candidate_version,
                active_version=active_version,
                emission_gate=emission_gate_summary,
            ),
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
        _history_entry(
            action="promoted",
            reason=state["last_reason"],
            gates=gates,
            gate_failures=combined_gate_failures,
            gate_skips=gate_skips,
            candidate_version=candidate_version,
            active_version=candidate_version,
            previous_active_version=active_version,
            forced=bool(args.force_promote),
            emission_gate=emission_gate_summary,
        ),
    )
    result.update(
        {
            "action": "promoted",
            "promoted": True,
            "active_version": candidate_version,
            "reason": state["last_reason"],
            "gate_failures": combined_gate_failures,
            "gate_skips": gate_skips,
        }
    )
    _persist_state_and_ops(state_path, state, args.ops_db, result)
    print(json.dumps(result))
    return 0


def cmd_rollback(args: argparse.Namespace) -> int:
    models_dir = Path(args.models_dir)
    metadata_dir = _resolve_metadata_dir(models_dir, args.metadata_dir)
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
        for explicit in _metadata_manifest_candidates(models_dir, metadata_dir, str(target_version)):
            if explicit.exists():
                target_path = explicit
                break
    if (
        target_path is None
        and args.harden_break_fallbacks
        and target_version is not None
        and str(target_version) == active_version
    ):
        target_path = active_path
    if target_path is None and prev_path.exists():
        target_path = prev_path
    if target_path is None:
        raise FileNotFoundError(
            "No rollback candidate found. Provide --to-version or ensure manifest_active_prev.json exists."
        )

    target_manifest = load_manifest(target_path)
    target_version = version_of(target_manifest)
    hardened_break_horizons: list[int] = []
    write_manifest = target_manifest
    if args.harden_break_fallbacks:
        source_manifest = target_manifest
        if target_version == active_version:
            source_manifest = active_manifest
        write_manifest, hardened_break_horizons = harden_break_fallback_thresholds(
            source_manifest,
            no_trade_threshold=args.no_trade_threshold,
            applied_at_ms=now_ms(),
        )

    if target_version == active_version and not hardened_break_horizons:
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
    if hardened_break_horizons:
        atomic_write_json(active_path, write_manifest)
    else:
        atomic_copy(target_path, active_path)

    action = "rollback"
    reason = f"rolled back from {active_version} to {target_version}"
    if target_version == active_version and hardened_break_horizons:
        action = "hardened_active"
        reason = (
            f"hardened active {active_version} break fallback horizons "
            f"({_format_horizon_labels(hardened_break_horizons)})"
        )
    elif hardened_break_horizons:
        reason += (
            f"; hardened break fallback horizons "
            f"({_format_horizon_labels(hardened_break_horizons)})"
        )

    state.update(
        {
            "previous_active_version": (
                active_version
                if target_version != active_version
                else state.get("previous_active_version")
            ),
            "active_version": target_version,
            "last_action": action,
            "last_reason": reason,
            "last_checked_at_ms": now_ms(),
            "last_promoted_at_ms": now_ms(),
        }
    )
    push_history(
        state,
        _history_entry(
            action=action,
            reason=state["last_reason"],
            active_version=target_version,
            previous_active_version=active_version,
            candidate_version=state.get("candidate_version"),
            hardened_break_horizons=hardened_break_horizons,
            no_trade_threshold=float(args.no_trade_threshold),
        ),
    )
    result = {
        "status": "ok",
        "action": action,
        "active_version": target_version,
        "candidate_version": state.get("candidate_version"),
        "reason": state["last_reason"],
        "hardened_break_horizons": hardened_break_horizons,
        "no_trade_threshold": float(args.no_trade_threshold),
    }
    _persist_state_and_ops(state_path, state, args.ops_db, result)
    print(json.dumps(result))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Model governance controller")
    parser.add_argument("--models-dir", default=str(DEFAULT_MODELS_DIR))
    parser.add_argument(
        "--metadata-dir",
        default=DEFAULT_METADATA_DIR,
        help="Directory for metadata manifests (absolute or relative to --models-dir)",
    )
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
        "--min-total-samples",
        type=int,
        default=DEFAULT_MIN_TOTAL_SAMPLES,
    )
    eval_cmd.add_argument(
        "--min-positive-samples",
        type=int,
        default=DEFAULT_MIN_POSITIVE_SAMPLES,
        help="Global minimum positive-label count for MAE/MFE regression gates.",
    )
    eval_cmd.add_argument(
        "--min-positive-samples-reject",
        type=int,
        default=DEFAULT_MIN_POSITIVE_SAMPLES_REJECT,
    )
    eval_cmd.add_argument(
        "--min-positive-samples-break",
        type=int,
        default=DEFAULT_MIN_POSITIVE_SAMPLES_BREAK,
    )
    eval_cmd.add_argument(
        "--allow-feature-version-change",
        action="store_true",
        default=DEFAULT_ALLOW_FEATURE_VERSION_CHANGE,
    )
    eval_cmd.add_argument(
        "--regime-aware",
        action="store_true",
        default=DEFAULT_REGIME_AWARE,
        help=(
            "Evaluate MAE/MFE regressions within configured regime buckets and waive "
            "aggregate regressions when no supported bucket regresses."
        ),
    )
    eval_cmd.add_argument(
        "--regime-buckets",
        default=DEFAULT_REGIME_BUCKETS,
        help="Comma-separated buckets for regime-aware checks (e.g. compression,expansion,neutral).",
    )
    eval_cmd.add_argument(
        "--regime-min-total-samples",
        type=int,
        default=DEFAULT_REGIME_MIN_TOTAL_SAMPLES,
        help="Minimum per-bucket sample_size required before comparing bucket metrics.",
    )
    eval_cmd.add_argument(
        "--regime-min-positive-samples",
        type=int,
        default=DEFAULT_REGIME_MIN_POSITIVE_SAMPLES,
        help="Global minimum per-bucket positive-label count for regime-aware regression checks.",
    )
    eval_cmd.add_argument(
        "--regime-min-positive-samples-reject",
        type=int,
        default=DEFAULT_REGIME_MIN_POSITIVE_SAMPLES_REJECT,
    )
    eval_cmd.add_argument(
        "--regime-min-positive-samples-break",
        type=int,
        default=DEFAULT_REGIME_MIN_POSITIVE_SAMPLES_BREAK,
    )
    eval_cmd.add_argument(
        "--regime-min-compared-buckets",
        type=int,
        default=DEFAULT_REGIME_MIN_COMPARED_BUCKETS,
        help="Minimum number of supported buckets required to waive aggregate regressions.",
    )
    eval_cmd.add_argument(
        "--enforce-threshold-utility-guard",
        action="store_true",
        default=DEFAULT_ENFORCE_THRESHOLD_UTILITY_GUARD,
        help=(
            "Reject candidate promotion when threshold utility guard is applied or "
            "utility score is below --threshold-utility-min-score for configured targets/horizons."
        ),
    )
    eval_cmd.add_argument(
        "--no-enforce-threshold-utility-guard",
        dest="enforce_threshold_utility_guard",
        action="store_false",
        help="Disable threshold utility guard checks in governance promotion evaluation.",
    )
    eval_cmd.add_argument(
        "--threshold-utility-targets",
        default=DEFAULT_THRESHOLD_UTILITY_TARGETS,
        help="Comma-separated targets for threshold utility guard checks (default: reject).",
    )
    eval_cmd.add_argument(
        "--threshold-utility-min-score",
        type=float,
        default=DEFAULT_THRESHOLD_UTILITY_MIN_SCORE,
        help="Minimum acceptable threshold utility score for configured targets/horizons.",
    )
    eval_cmd.add_argument(
        "--enforce-live-emission-gate",
        action="store_true",
        default=DEFAULT_ENFORCE_LIVE_EMISSION_GATE,
        help=(
            "Reject promotion when the candidate does not emit enough recent "
            "signals in live-like prediction rows."
        ),
    )
    eval_cmd.add_argument(
        "--no-enforce-live-emission-gate",
        dest="enforce_live_emission_gate",
        action="store_false",
        help="Disable recent live-emission gate checks during governance evaluation.",
    )
    eval_cmd.add_argument(
        "--emission-db",
        default=DEFAULT_EMISSION_DB,
        help=(
            "SQLite data source used for candidate emission checks. "
            "Defaults to MODEL_GOV_EMISSION_DB or --ops-db."
        ),
    )
    eval_cmd.add_argument(
        "--emission-lookback-days",
        type=int,
        default=DEFAULT_EMISSION_LOOKBACK_DAYS,
        help="Lookback window used for candidate emission checks.",
    )
    eval_cmd.add_argument(
        "--emission-max-pred-lag-hours",
        type=float,
        default=DEFAULT_EMISSION_MAX_PRED_LAG_HOURS,
        help="Maximum prediction lag allowed between ts_event and ts_prediction.",
    )
    eval_cmd.add_argument(
        "--emission-prediction-basis",
        default=DEFAULT_EMISSION_PREDICTION_BASIS,
        choices=["first", "latest"],
        help="Whether to evaluate first or latest prediction row per event.",
    )
    eval_cmd.add_argument(
        "--emission-source",
        default=DEFAULT_EMISSION_SOURCE,
        choices=["live", "preview", "all"],
        help="Prediction source used for candidate emission checks.",
    )
    eval_cmd.add_argument(
        "--emission-min-rows",
        type=int,
        default=DEFAULT_EMISSION_MIN_ROWS,
        help="Minimum recent prediction rows required before emission checks can pass.",
    )
    eval_cmd.add_argument(
        "--emission-min-coverage",
        type=float,
        default=DEFAULT_EMISSION_MIN_COVERAGE,
        help="Minimum selected_rows / touch_rows coverage required for emission checks.",
    )
    eval_cmd.add_argument(
        "--emission-min-signals",
        type=int,
        default=DEFAULT_EMISSION_MIN_SIGNALS,
        help="Minimum recent non-abstaining signal rows required for candidate promotion.",
    )
    eval_cmd.add_argument(
        "--emission-max-abstain-rate",
        type=float,
        default=DEFAULT_EMISSION_MAX_ABSTAIN_RATE,
        help="Maximum acceptable abstain rate in the candidate emission window.",
    )
    eval_cmd.add_argument(
        "--emission-symbols",
        default=DEFAULT_EMISSION_SYMBOLS,
        help=(
            "Optional comma-separated symbol universe for candidate emission checks. "
            "Use this to align governance with the candidate shadow-scoring universe."
        ),
    )
    eval_cmd.add_argument("--force-promote", action="store_true", default=False)
    eval_cmd.set_defaults(func=cmd_evaluate)

    rollback_cmd = sub.add_parser("rollback", help="Rollback active manifest")
    rollback_cmd.add_argument("--to-version", default=None, help="Version label like v010")
    rollback_cmd.add_argument(
        "--harden-break-fallbacks",
        action="store_true",
        default=False,
        help=(
            "Clamp fallback-derived break thresholds to --no-trade-threshold while "
            "rolling back or hardening the currently active version."
        ),
    )
    rollback_cmd.add_argument(
        "--no-trade-threshold",
        type=float,
        default=1.0,
        help="Threshold used when hardening fallback-derived break horizons.",
    )
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
