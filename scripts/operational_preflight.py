#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "pivot_events.sqlite"
DEFAULT_ENV = ROOT / ".env"
VALID_HORIZONS = {5, 15, 30, 60}
MODEL_CONTEXT_REQUIRED_FIELDS = {
    "symbols",
    "default_symbol",
    "targets",
    "cost_model_version",
    "trade_cost_bps",
    "training_view",
    "source_duckdb_path",
}
CONTROLLED_SHADOW_TARGETS = {"reject", "break"}


@dataclass
class CheckResult:
    status: str
    message: str


@dataclass
class ManifestContract:
    path: Path
    payload: dict[str, Any]
    version: str
    model_context: dict[str, Any]
    model_targets: dict[str, set[int]]
    shadow_policy_name: str | None
    shadow_policy_horizon: int | None


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not key.replace("_", "").isalnum() or key[0].isdigit():
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _runtime_env(env_file: Path = DEFAULT_ENV) -> dict[str, str]:
    values = _read_env_file(env_file)
    values.update({key: value for key, value in os.environ.items() if value is not None})
    return values


def _resolve_path(raw: str | None, *, base: Path = ROOT) -> Path | None:
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = base / path
    return path


def resolve_manifest_path(env: dict[str, str]) -> Path:
    explicit = _resolve_path(env.get("RF_MANIFEST_PATH"))
    if explicit is not None:
        return explicit

    model_dir = _resolve_path(env.get("RF_MODEL_DIR") or "data/models")
    if model_dir is None:
        model_dir = ROOT / "data" / "models"

    active_name = (env.get("RF_ACTIVE_MANIFEST") or "manifest_active.json").strip() or "manifest_active.json"
    candidate_name = (env.get("RF_CANDIDATE_MANIFEST") or "manifest_runtime_latest.json").strip()
    candidate_name = candidate_name or "manifest_runtime_latest.json"
    active_path = model_dir / active_name
    if active_path.exists():
        return active_path
    candidate_path = model_dir / candidate_name
    if candidate_path.exists():
        return candidate_path
    legacy_path = model_dir / "manifest_latest.json"
    if legacy_path.exists():
        return legacy_path
    return candidate_path


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


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _env_int(name: str, default: int) -> int:
    parsed = _as_int(os.getenv(name))
    return parsed if parsed is not None else default


def _env_float(name: str, default: float) -> float:
    parsed = _as_float(os.getenv(name))
    return parsed if parsed is not None else default


def _fmt_ts(ms: int | None) -> str:
    if ms is None:
        return "n/a"
    return time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ms / 1000.0))


def _manifest_model_dir(manifest_path: Path, env: dict[str, str]) -> Path:
    configured = _resolve_path(env.get("RF_MODEL_DIR") or "")
    if configured is not None:
        return configured
    return manifest_path.parent


def _model_artifact_path(model_dir: Path, raw_path: Any) -> Path | None:
    if not isinstance(raw_path, str) or not raw_path.strip():
        return None
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = model_dir / candidate
    return candidate


def _target_horizon_map(models_payload: Any) -> dict[str, set[int]]:
    out: dict[str, set[int]] = {}
    if not isinstance(models_payload, dict):
        return out
    for target, horizons in models_payload.items():
        if isinstance(horizons, dict):
            horizon_values = horizons.keys()
        elif isinstance(horizons, list):
            horizon_values = horizons
        else:
            continue
        target_key = str(target).strip().lower()
        out[target_key] = {
            int_horizon
            for int_horizon in (_as_int(horizon) for horizon in horizon_values)
            if int_horizon in VALID_HORIZONS
        }
    return out


def _coverage_label(model_targets: dict[str, set[int]]) -> str:
    parts = []
    for target in sorted(model_targets):
        horizons = ",".join(str(h) for h in sorted(model_targets[target])) or "none"
        parts.append(f"{target}=[{horizons}]")
    return "; ".join(parts) if parts else "none"


def _validate_model_context(model_context: Any) -> tuple[dict[str, Any], list[str]]:
    if not isinstance(model_context, dict):
        return {}, sorted(MODEL_CONTEXT_REQUIRED_FIELDS)
    missing = [
        field
        for field in sorted(MODEL_CONTEXT_REQUIRED_FIELDS)
        if model_context.get(field) in (None, "", [])
    ]
    return model_context, missing


def _manifest_lineage_present(manifest: dict[str, Any]) -> bool:
    return any(
        manifest.get(field) not in (None, "", {})
        for field in (
            "trained_end_ts",
            "created_at",
            "created_at_utc",
            "generated_at_utc",
            "build_id",
            "tune_date_range",
        )
    )


def load_manifest_contract(env: dict[str, str]) -> tuple[ManifestContract | None, list[CheckResult]]:
    results: list[CheckResult] = []
    manifest_path = resolve_manifest_path(env)
    if not manifest_path.exists():
        return None, [CheckResult("FAIL", f"Runtime manifest missing: {manifest_path}; set RF_MODEL_DIR/RF_ACTIVE_MANIFEST or publish the runtime manifest first")]

    manifest = _read_json(manifest_path)
    if manifest is None:
        return None, [CheckResult("FAIL", f"Runtime manifest is not valid JSON object: {manifest_path}; republish the artifact manifest")]

    version = str(manifest.get("version") or "").strip()
    if not version:
        results.append(CheckResult("FAIL", "Runtime manifest missing required version; republish artifact metadata with a version/build id"))
        version = "unknown"
    results.append(CheckResult("PASS", f"Runtime manifest readable: {manifest_path} (version={version})"))

    model_targets = manifest.get("models")
    if not isinstance(model_targets, dict) or not model_targets:
        results.append(CheckResult("FAIL", "Runtime manifest has no models block; republish artifact with target/horizon model paths"))
        model_targets = {}
    else:
        targets = sorted(str(target) for target in model_targets.keys())
        results.append(CheckResult("PASS", f"Runtime manifest model targets: {', '.join(targets)}"))

    model_context, missing_context = _validate_model_context(manifest.get("model_context"))
    if missing_context:
        results.append(
            CheckResult(
                "FAIL",
                "Runtime manifest missing model_context fields: "
                f"{', '.join(missing_context)}; backfill/republish artifact metadata before shadow/live use",
            )
        )
    else:
        results.append(
            CheckResult(
                "PASS",
                "Runtime manifest model_context explicit: "
                f"default_symbol={model_context.get('default_symbol')}; targets={','.join(str(t) for t in model_context.get('targets', []))}",
            )
        )

    if manifest.get("feature_version") in (None, ""):
        results.append(CheckResult("FAIL", "Runtime manifest missing feature_version; republish artifact metadata"))
    if not _manifest_lineage_present(manifest):
        results.append(CheckResult("FAIL", "Runtime manifest missing lineage timestamp/build metadata; include trained_end_ts, tune_date_range, created_at, or build_id"))
    else:
        results.append(CheckResult("PASS", "Runtime manifest lineage metadata present"))

    model_dir = _manifest_model_dir(manifest_path, env)
    target_horizons = _target_horizon_map(model_targets)
    expected_targets = {
        str(target).strip().lower()
        for target in (model_context.get("targets") or CONTROLLED_SHADOW_TARGETS)
        if str(target).strip()
    }
    expected_targets = expected_targets or CONTROLLED_SHADOW_TARGETS
    missing_targets = sorted(target for target in expected_targets if target not in target_horizons)
    if missing_targets:
        results.append(
            CheckResult(
                "FAIL",
                f"Runtime manifest missing target coverage for: {', '.join(missing_targets)}; publish models for every approved target",
            )
        )
    else:
        results.append(CheckResult("PASS", f"Runtime manifest target/horizon coverage: {_coverage_label(target_horizons)}"))

    if isinstance(model_targets, dict):
        for target, horizons in model_targets.items():
            if not isinstance(horizons, dict):
                results.append(CheckResult("FAIL", f"Runtime manifest models.{target} is not a horizon map"))
                continue
            for horizon, raw_model_path in horizons.items():
                int_horizon = _as_int(horizon)
                artifact_path = _model_artifact_path(model_dir, raw_model_path)
                if int_horizon not in VALID_HORIZONS:
                    results.append(CheckResult("FAIL", f"Runtime manifest has invalid horizon {horizon!r} for target {target}"))
                    continue
                if artifact_path is None:
                    results.append(CheckResult("FAIL", f"Runtime manifest missing model artifact path for {target} {int_horizon}m"))
                elif not artifact_path.exists():
                    results.append(
                        CheckResult(
                            "FAIL",
                            f"Runtime model artifact missing for {target} {int_horizon}m: {artifact_path}; restore or republish model files",
                        )
                    )

    shadow_mode = (env.get("ML_MODEL_SIDE_MARGIN_SHADOW_MODE") or "log").strip().lower()
    if shadow_mode == "off":
        results.append(CheckResult("SKIP", "Model-side-margin shadow emission disabled by config"))
        return ManifestContract(
            path=manifest_path,
            payload=manifest,
            version=version,
            model_context=model_context,
            model_targets=target_horizons,
            shadow_policy_name=None,
            shadow_policy_horizon=None,
        ), results
    if shadow_mode != "log":
        results.append(CheckResult("FAIL", f"Invalid ML_MODEL_SIDE_MARGIN_SHADOW_MODE={shadow_mode}"))
        return None, results

    policy_name = (env.get("ML_MODEL_SIDE_MARGIN_SHADOW_POLICY_NAME") or "model_side_margin_v1").strip()
    policy_name = policy_name or "model_side_margin_v1"
    shadow_policies = manifest.get("shadow_policies")
    policy = shadow_policies.get(policy_name) if isinstance(shadow_policies, dict) else None
    if not isinstance(policy, dict):
        results.append(
            CheckResult(
                "FAIL",
                f"Runtime manifest missing shadow policy '{policy_name}'; live shadow emissions will be ineligible",
            )
        )
        return ManifestContract(
            path=manifest_path,
            payload=manifest,
            version=version,
            model_context=model_context,
            model_targets=target_horizons,
            shadow_policy_name=policy_name,
            shadow_policy_horizon=None,
        ), results

    horizon = _as_int(policy.get("horizon"))
    if horizon not in VALID_HORIZONS:
        results.append(CheckResult("FAIL", f"Shadow policy '{policy_name}' has invalid horizon: {horizon}"))
    else:
        results.append(CheckResult("PASS", f"Shadow policy '{policy_name}' configured for {horizon}m"))

    for side in ("reject", "break"):
        side_policy = policy.get(side)
        if not isinstance(side_policy, dict):
            results.append(CheckResult("FAIL", f"Shadow policy '{policy_name}' missing {side} side config"))
            continue
        side_models = model_targets.get(side)
        if not isinstance(side_models, dict) or str(horizon) not in side_models:
            results.append(CheckResult("FAIL", f"Shadow policy '{policy_name}' needs {side} {horizon}m model"))
        if isinstance(side_policy.get("status"), str) and side_policy.get("status") not in {"ok", "ready", "active", "enabled"}:
            results.append(
                CheckResult(
                    "FAIL",
                    f"Shadow policy '{policy_name}' {side} side is not ready (status={side_policy.get('status')}); rerun artifact training/backfill",
                )
            )
        if _as_float(side_policy.get("reference_threshold")) is None:
            results.append(CheckResult("FAIL", f"Shadow policy '{policy_name}' {side} reference_threshold missing"))
        if _as_float(side_policy.get("margin_cutoff")) is None:
            results.append(CheckResult("FAIL", f"Shadow policy '{policy_name}' {side} margin_cutoff missing"))
    return ManifestContract(
        path=manifest_path,
        payload=manifest,
        version=version,
        model_context=model_context,
        model_targets=target_horizons,
        shadow_policy_name=policy_name,
        shadow_policy_horizon=horizon,
    ), results


def check_runtime_manifest(env: dict[str, str]) -> list[CheckResult]:
    _, results = load_manifest_contract(env)
    return results


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        [table_name],
    ).fetchone()
    return row is not None


def _scalar(conn: sqlite3.Connection, sql: str, params: list[Any] | None = None) -> Any:
    row = conn.execute(sql, params or []).fetchone()
    return row[0] if row else None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _max_required_horizon(contract: ManifestContract | None) -> int:
    if contract is not None and contract.shadow_policy_horizon in VALID_HORIZONS:
        return int(contract.shadow_policy_horizon)
    horizons: set[int] = set()
    if contract is not None:
        for target_horizons in contract.model_targets.values():
            horizons.update(int(h) for h in target_horizons if h in VALID_HORIZONS)
    return max(horizons or VALID_HORIZONS)


def _check_label_pipeline(
    conn: sqlite3.Connection,
    *,
    predictions: int,
    labels: int | None,
    contract: ManifestContract | None,
) -> list[CheckResult]:
    results: list[CheckResult] = []
    build_labels_path = ROOT / "scripts" / "build_labels.py"
    if build_labels_path.exists():
        results.append(CheckResult("PASS", f"Label builder available: {build_labels_path}"))
    else:
        results.append(CheckResult("FAIL", f"Label builder missing: {build_labels_path}; restore scripts/build_labels.py"))

    if predictions <= 0:
        return results

    if not _has_table(conn, "touch_events"):
        results.append(CheckResult("FAIL", "prediction_log has rows but touch_events table is missing; labels cannot be joined to events"))
        return results

    pred_cols = _table_columns(conn, "prediction_log")
    touch_cols = _table_columns(conn, "touch_events")
    if "event_id" not in pred_cols or "ts_prediction" not in pred_cols:
        results.append(CheckResult("FAIL", "prediction_log missing event_id/ts_prediction columns required for label maturity checks"))
        return results
    if "event_id" not in touch_cols or "ts_event" not in touch_cols:
        results.append(CheckResult("FAIL", "touch_events missing event_id/ts_event columns required for label maturity checks"))
        return results

    max_bar_ts = None
    if _has_table(conn, "bar_data") and "ts" in _table_columns(conn, "bar_data"):
        max_bar_ts = _as_int(_scalar(conn, "SELECT MAX(ts) FROM bar_data"))
    reference_ts = max_bar_ts or int(time.time() * 1000)
    required_horizon = _max_required_horizon(contract)
    maturity_ms = int(required_horizon) * 60 * 1000
    mature_predictions = int(
        _scalar(
            conn,
            """
            SELECT COUNT(DISTINCT pl.event_id)
            FROM prediction_log pl
            JOIN touch_events te ON te.event_id = pl.event_id
            WHERE (? - te.ts_event) >= ?
            """,
            [reference_ts, maturity_ms],
        )
        or 0
    )
    if mature_predictions <= 0:
        results.append(
            CheckResult(
                "WARN",
                f"prediction_log has rows but no events are mature for {required_horizon}m labels yet",
            )
        )
        return results

    if labels is None:
        results.append(
            CheckResult(
                "FAIL",
                f"{mature_predictions} prediction event(s) are mature for {required_horizon}m labels but event_labels table is missing; run scripts/build_labels.py",
            )
        )
        return results
    if labels == 0:
        results.append(
            CheckResult(
                "FAIL",
                f"{mature_predictions} prediction event(s) are mature for {required_horizon}m labels but event_labels has 0 rows; run scripts/build_labels.py --horizons 5 15 60 --incremental",
            )
        )
        return results

    labeled_mature = int(
        _scalar(
            conn,
            """
            SELECT COUNT(DISTINCT pl.event_id)
            FROM prediction_log pl
            JOIN touch_events te ON te.event_id = pl.event_id
            JOIN event_labels el
              ON el.event_id = pl.event_id
             AND el.horizon_min = ?
            WHERE (? - te.ts_event) >= ?
            """,
            [required_horizon, reference_ts, maturity_ms],
        )
        or 0
    )
    if labeled_mature == 0:
        results.append(
            CheckResult(
                "FAIL",
                f"{mature_predictions} prediction event(s) are mature for {required_horizon}m labels but none have matching event_labels rows; run label builder and verify horizons",
            )
        )
    elif labeled_mature < mature_predictions:
        results.append(
            CheckResult(
                "WARN",
                f"{mature_predictions - labeled_mature} mature prediction event(s) lack {required_horizon}m labels; run incremental label builder",
            )
        )
    else:
        results.append(CheckResult("PASS", f"Mature predictions have matching {required_horizon}m labels: {labeled_mature}/{mature_predictions}"))
    return results


def check_runtime_database(db_path: Path, contract: ManifestContract | None = None) -> list[CheckResult]:
    if not db_path.exists():
        return [CheckResult("WARN", f"Runtime SQLite DB missing: {db_path}")]

    results: list[CheckResult] = [CheckResult("PASS", f"Runtime SQLite DB readable: {db_path}")]
    conn = sqlite3.connect(str(db_path))
    try:
        for table_name in ("bar_data", "touch_events", "prediction_log", "shadow_emission_log", "event_labels"):
            if not _has_table(conn, table_name):
                results.append(CheckResult("WARN", f"Runtime table missing: {table_name}"))

        labels: int | None = None
        if _has_table(conn, "prediction_log"):
            predictions = int(_scalar(conn, "SELECT COUNT(*) FROM prediction_log") or 0)
            results.append(CheckResult("PASS" if predictions else "WARN", f"prediction_log rows: {predictions}"))
        else:
            predictions = 0

        if _has_table(conn, "event_labels"):
            labels = int(_scalar(conn, "SELECT COUNT(*) FROM event_labels") or 0)
            if predictions and labels == 0:
                results.append(CheckResult("WARN", "event_labels has 0 rows; checking whether prediction rows are mature"))
            else:
                results.append(CheckResult("PASS" if labels else "WARN", f"event_labels rows: {labels}"))
        elif predictions:
            results.append(CheckResult("FAIL", "prediction_log has rows but event_labels table is missing; run scripts/build_labels.py after events mature"))

        if _has_table(conn, "shadow_emission_log"):
            shadow_rows = int(_scalar(conn, "SELECT COUNT(*) FROM shadow_emission_log") or 0)
            eligible_rows = int(_scalar(conn, "SELECT COALESCE(SUM(eligible), 0) FROM shadow_emission_log") or 0)
            emit_rows = int(_scalar(conn, "SELECT COALESCE(SUM(shadow_emit), 0) FROM shadow_emission_log") or 0)
            shadow_cols = _table_columns(conn, "shadow_emission_log")
            repair_payload = (
                contract.payload.get("operational_parity_repair")
                if contract is not None and isinstance(contract.payload, dict)
                else None
            )
            repair_ts = _as_int(repair_payload.get("repaired_at_ms")) if isinstance(repair_payload, dict) else None
            post_repair_rows = None
            if repair_ts is not None and "created_at_ms" in shadow_cols:
                post_repair_rows = int(
                    _scalar(
                        conn,
                        "SELECT COUNT(*) FROM shadow_emission_log WHERE created_at_ms >= ?",
                        [repair_ts],
                    )
                    or 0
                )
            only_legacy_shadow_rows = bool(shadow_rows and eligible_rows == 0 and repair_ts is not None and post_repair_rows == 0)
            if shadow_rows:
                results.append(
                    CheckResult(
                        "PASS" if eligible_rows else ("WARN" if only_legacy_shadow_rows else "FAIL"),
                        (
                            f"shadow_emission_log rows: {shadow_rows}; eligible={eligible_rows}; emits={emit_rows}"
                            + (
                                "; all rows predate repaired shadow policy config"
                                if only_legacy_shadow_rows
                                else ""
                            )
                        ),
                    )
                )
                top_reason = conn.execute(
                    """
                    SELECT COALESCE(ineligibility_reason, ''), COUNT(*)
                    FROM shadow_emission_log
                    WHERE COALESCE(eligible, 0) = 0
                    GROUP BY COALESCE(ineligibility_reason, '')
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                    """
                ).fetchone()
                if top_reason:
                    reason, count = top_reason
                    status = (
                        "FAIL"
                        if reason == "missing_policy_config" and eligible_rows == 0 and not only_legacy_shadow_rows
                        else "WARN"
                    )
                    results.append(CheckResult(status, f"top shadow ineligibility reason: {reason or 'unknown'} ({count})"))
            else:
                results.append(CheckResult("WARN", "shadow_emission_log has 0 rows"))
        results.extend(_check_label_pipeline(conn, predictions=predictions, labels=labels, contract=contract))
    finally:
        conn.close()
    return results


def check_scoring_freshness(db_path: Path, contract: ManifestContract | None = None) -> list[CheckResult]:
    """Detect a silently-stalled scorer: events being detected but not scored.

    The primary signal is an *event-relative* backlog — matured ``touch_events`` with
    no row in ``prediction_log``. This is robust to market-closed periods: when no new
    events arrive the backlog stays flat, so it does not false-alarm overnight or on
    weekends the way a wall-clock "time since last score" check would. This is the exact
    failure ``check_runtime_database`` misses, because that check reports ``prediction_log``
    green on row count alone while the newest prediction may be months stale.

    A "settle" window (anchored to the newest detected event, not wall-clock) excludes the
    freshest events a healthy scorer legitimately may not have reached yet. Wall-clock
    market-data staleness is reported separately at lower severity, since bar age grows
    naturally outside trading hours.

    Env overrides: ``SCORING_SETTLE_MINUTES`` (default 10), ``SCORING_BACKLOG_WARN``
    (default 5), ``SCORING_BACKLOG_FAIL`` (default 50), ``MARKET_DATA_STALE_MINUTES``
    (default 120).
    """
    if not db_path.exists():
        return [CheckResult("SKIP", f"Scoring-freshness: runtime DB missing: {db_path}")]

    settle_ms = int(_env_float("SCORING_SETTLE_MINUTES", 10.0) * 60_000)
    backlog_warn = _env_int("SCORING_BACKLOG_WARN", 5)
    backlog_fail = _env_int("SCORING_BACKLOG_FAIL", 50)
    market_stale_minutes = _env_float("MARKET_DATA_STALE_MINUTES", 120.0)

    results: list[CheckResult] = []
    if backlog_warn > backlog_fail:
        results.append(
            CheckResult(
                "WARN",
                f"Scoring backlog thresholds misordered (WARN {backlog_warn} > FAIL {backlog_fail}); "
                "clamping WARN down to FAIL so the early-warning tier stays reachable",
            )
        )
        backlog_warn = backlog_fail
    conn = sqlite3.connect(str(db_path))
    try:
        if not (_has_table(conn, "touch_events") and _has_table(conn, "prediction_log")):
            return [CheckResult("SKIP", "Scoring-freshness: touch_events/prediction_log not both present")]

        te_cols = _table_columns(conn, "touch_events")
        pl_cols = _table_columns(conn, "prediction_log")
        if "event_id" not in te_cols or "ts_event" not in te_cols or "event_id" not in pl_cols:
            return [CheckResult("WARN", "Scoring-freshness: touch_events/prediction_log missing event_id/ts_event columns")]

        newest_event = _as_int(_scalar(conn, "SELECT MAX(ts_event) FROM touch_events"))
        if newest_event is None:
            return [CheckResult("SKIP", "Scoring-freshness: touch_events is empty")]
        newest_pred_event = _as_int(
            _scalar(
                conn,
                "SELECT MAX(t.ts_event) FROM touch_events t "
                "JOIN prediction_log p ON p.event_id = t.event_id",
            )
        )

        mature_cutoff = newest_event - settle_ms
        unscored_mature = _as_int(
            _scalar(
                conn,
                "SELECT COUNT(*) FROM touch_events t "
                "WHERE t.ts_event <= ? "
                "AND NOT EXISTS (SELECT 1 FROM prediction_log p WHERE p.event_id = t.event_id)",
                [mature_cutoff],
            )
        ) or 0

        # An empty (or fully-unlinked) prediction_log is NOT itself a stall: a
        # just-restarted or market-closed-startup system legitimately has no scored
        # events yet. The stall verdict is driven by the *matured backlog*, so a cold
        # start with nothing past the settle window passes, while events that matured
        # long ago and were never scored still FAIL — whether or not any prediction
        # row exists. The ~4.5-month real stall leaves old prediction rows, so it lands
        # in the else-label with a huge backlog either way.
        never_scored = newest_pred_event is None
        if never_scored:
            gap_label = "no prediction_log row joins to any detected event yet"
        else:
            gap_label = (
                f"newest scored event {_fmt_ts(newest_pred_event)} vs "
                f"newest detected event {_fmt_ts(newest_event)}"
            )

        if unscored_mature >= backlog_fail:
            verb = "Scoring stalled" if never_scored else "Scoring backlog"
            results.append(
                CheckResult(
                    "FAIL",
                    f"{verb}: {unscored_mature} matured touch_events unscored; {gap_label}",
                )
            )
        elif unscored_mature >= backlog_warn:
            results.append(
                CheckResult(
                    "WARN",
                    f"Scoring backlog building: {unscored_mature} matured touch_events unscored; {gap_label}",
                )
            )
        else:
            cold = "; scorer has not written any prediction yet" if never_scored else ""
            results.append(
                CheckResult(
                    "PASS",
                    f"Scoring current: {unscored_mature} matured touch_events unscored; {gap_label}{cold}",
                )
            )

        if _has_table(conn, "bar_data") and "ts" in _table_columns(conn, "bar_data"):
            newest_bar = _as_int(_scalar(conn, "SELECT MAX(ts) FROM bar_data"))
            if newest_bar is not None:
                bar_age_min = (int(time.time() * 1000) - newest_bar) / 60_000.0
                if bar_age_min > market_stale_minutes:
                    results.append(
                        CheckResult(
                            "WARN",
                            f"Market data age {bar_age_min:.0f}m exceeds {market_stale_minutes:.0f}m "
                            f"(newest bar {_fmt_ts(newest_bar)}); expected overnight/weekends, "
                            "investigate if during trading hours",
                        )
                    )
                else:
                    results.append(
                        CheckResult(
                            "PASS",
                            f"Market data fresh: newest bar {_fmt_ts(newest_bar)} ({bar_age_min:.0f}m old)",
                        )
                    )
    finally:
        conn.close()
    return results


def _fetch_json(url: str, *, timeout: float = 2.0) -> dict[str, Any] | None:
    with urlopen(url, timeout=timeout) as response:
        if not 200 <= int(response.status) < 300:
            return None
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else None


def _same_path(left: str | None, right: Path | str | None) -> bool:
    if not left or right is None:
        return False
    try:
        return Path(left).expanduser().resolve() == Path(str(right)).expanduser().resolve()
    except Exception:
        return str(left) == str(right)


def check_ml_server_payload_parity(contract: ManifestContract, payload: dict[str, Any]) -> list[CheckResult]:
    results: list[CheckResult] = []
    manifest_path = payload.get("manifest_path")
    if not _same_path(str(manifest_path) if manifest_path else None, contract.path):
        results.append(
            CheckResult(
                "FAIL",
                f"ml_server active manifest mismatch: health reports {manifest_path}, preflight resolved {contract.path}",
            )
        )
    manifest = payload.get("manifest")
    if not isinstance(manifest, dict):
        results.append(CheckResult("FAIL", "ml_server health does not expose loaded manifest payload"))
        manifest = {}
    health_version = str(manifest.get("version") or "").strip()
    if health_version != contract.version:
        results.append(
            CheckResult(
                "FAIL",
                f"ml_server model version mismatch: health={health_version or 'missing'} manifest={contract.version}",
            )
        )

    readiness = payload.get("runtime_readiness")
    if isinstance(readiness, dict):
        if not bool(readiness.get("model_context_present")):
            results.append(CheckResult("FAIL", "ml_server runtime_readiness reports missing model_context"))
        if not bool(readiness.get("shadow_policy_ready")) and contract.shadow_policy_name:
            results.append(CheckResult("FAIL", "ml_server runtime_readiness reports shadow policy not ready"))
        readiness_coverage = _target_horizon_map(readiness.get("target_horizon_coverage"))
        if readiness_coverage and readiness_coverage != contract.model_targets:
            results.append(
                CheckResult(
                    "FAIL",
                    f"ml_server target/horizon coverage mismatch: health={_coverage_label(readiness_coverage)} manifest={_coverage_label(contract.model_targets)}",
                )
            )
    else:
        results.append(CheckResult("WARN", "ml_server health lacks runtime_readiness block; upgrade server/ml_server.py before relying on health parity"))

    health_models = _target_horizon_map(payload.get("models"))
    missing_loaded = {
        target: sorted(horizons - health_models.get(target, set()))
        for target, horizons in contract.model_targets.items()
        if horizons - health_models.get(target, set())
    }
    if missing_loaded:
        results.append(
            CheckResult(
                "FAIL",
                f"ml_server has not loaded all manifest models: {missing_loaded}; reload or fix missing artifacts",
            )
        )
    if not any(result.status == "FAIL" for result in results):
        results.append(CheckResult("PASS", "ml_server runtime manifest/version/context/coverage matches preflight manifest"))
    return results


def check_model_api_payload_parity(
    contract: ManifestContract,
    registry_payload: dict[str, Any],
    baseline_payload: dict[str, Any] | None = None,
) -> list[CheckResult]:
    results: list[CheckResult] = []
    models = registry_payload.get("models")
    if not isinstance(models, list):
        return [CheckResult("FAIL", "model_api /registry payload missing models list")]
    matching = [
        model
        for model in models
        if isinstance(model, dict) and str(model.get("version") or "").strip() == contract.version
    ]
    if not matching:
        return [
            CheckResult(
                "FAIL",
                f"model_api registry does not contain active runtime version {contract.version}; backfill/register the active artifact or update runtime manifest",
            )
        ]
    results.append(CheckResult("PASS", f"model_api registry contains active runtime version {contract.version}"))

    if baseline_payload is not None:
        baseline_compare = baseline_payload.get("baseline_compare")
        if isinstance(baseline_compare, dict):
            baseline_summary = baseline_compare.get("summary")
            if not isinstance(baseline_summary, dict):
                baseline_summary = baseline_compare
            metadata_mode = baseline_summary.get("metadata_mode")
            if metadata_mode != "explicit":
                results.append(
                    CheckResult(
                        "FAIL",
                        f"model_api active artifact resolves with metadata_mode={metadata_mode}; backfill explicit model_context",
                    )
                )
            supported_targets = {
                str(target).strip().lower()
                for target in (baseline_summary.get("supported_targets") or [])
                if str(target).strip()
            }
            context_targets = {
                str(target).strip().lower()
                for target in (contract.model_context.get("targets") or [])
                if str(target).strip()
            }
            if context_targets and supported_targets and supported_targets != context_targets:
                results.append(
                    CheckResult(
                        "FAIL",
                        f"model_api supported targets {sorted(supported_targets)} do not match runtime model_context targets {sorted(context_targets)}",
                    )
                )
        else:
            results.append(CheckResult("WARN", "model_api baseline-compare payload missing baseline_compare block; target/context parity could not be fully checked"))

    if not any(result.status == "FAIL" for result in results):
        results.append(CheckResult("PASS", "model_api registry/runtime parity checks passed"))
    return results


def check_service_parity(contract: ManifestContract | None, require_services: bool) -> list[CheckResult]:
    if contract is None:
        return [CheckResult("SKIP", "Service parity skipped because runtime manifest contract is invalid")]

    ml_server_url = (os.getenv("ML_SERVER_HEALTH_URL") or "http://127.0.0.1:5003/health").strip()
    model_api_url = (os.getenv("MODEL_API_URL") or "http://127.0.0.1:5006").strip().rstrip("/")
    results: list[CheckResult] = []

    try:
        ml_payload = _fetch_json(ml_server_url)
    except (OSError, URLError, json.JSONDecodeError):
        results.append(CheckResult("FAIL" if require_services else "SKIP", f"ml_server parity check unreachable: {ml_server_url}"))
    else:
        if ml_payload is None:
            results.append(CheckResult("FAIL", f"ml_server health did not return valid JSON: {ml_server_url}"))
        else:
            results.extend(check_ml_server_payload_parity(contract, ml_payload))

    registry_url = f"{model_api_url}/registry"
    try:
        registry_payload = _fetch_json(registry_url)
    except (OSError, URLError, json.JSONDecodeError):
        results.append(CheckResult("FAIL" if require_services else "SKIP", f"model_api registry parity check unreachable: {registry_url}"))
    else:
        if registry_payload is None:
            results.append(CheckResult("FAIL", f"model_api /registry did not return valid JSON: {registry_url}"))
        else:
            baseline_payload = None
            models = registry_payload.get("models") if isinstance(registry_payload, dict) else None
            matching_id = None
            if isinstance(models, list):
                for model in models:
                    if isinstance(model, dict) and str(model.get("version") or "").strip() == contract.version:
                        matching_id = str(model.get("id") or "").strip()
                        break
            if matching_id:
                baseline_url = f"{model_api_url}/baseline-compare?id={quote(matching_id, safe='')}"
                try:
                    baseline_payload = _fetch_json(baseline_url, timeout=5.0)
                except (OSError, URLError, json.JSONDecodeError):
                    results.append(CheckResult("WARN", f"model_api baseline parity check unreachable: {baseline_url}"))
            results.extend(check_model_api_payload_parity(contract, registry_payload, baseline_payload))

    return results


def check_services(require_services: bool) -> list[CheckResult]:
    endpoints = [
        ("dashboard/proxy", "http://127.0.0.1:3000/health"),
        ("ml_server", "http://127.0.0.1:5003/health"),
        ("model_api", "http://127.0.0.1:5006/health"),
        ("research_api", "http://127.0.0.1:5005/health"),
    ]
    results: list[CheckResult] = []
    for name, url in endpoints:
        try:
            with urlopen(url, timeout=2) as response:
                status = int(response.status)
        except (OSError, URLError):
            results.append(
                CheckResult("FAIL" if require_services else "SKIP", f"{name} health unreachable: {url}")
            )
            continue
        if 200 <= status < 300:
            results.append(CheckResult("PASS", f"{name} health OK: {url}"))
        else:
            results.append(CheckResult("FAIL", f"{name} health returned HTTP {status}: {url}"))
    return results


def _print_results(results: list[CheckResult]) -> str:
    final = "PASS"
    for result in results:
        print(f"[{result.status}] {result.message}")
        if result.status == "FAIL":
            final = "FAIL"
        elif result.status == "WARN" and final == "PASS":
            final = "WARN"
    print(f"[RESULT] {final}")
    return final


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local operational preflight checks.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Runtime SQLite DB path")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV), help="Runtime .env file")
    parser.add_argument(
        "--require-services",
        action="store_true",
        help="Treat unreachable local services as failures instead of skipped offline checks.",
    )
    args = parser.parse_args()

    env = _runtime_env(Path(args.env_file).expanduser())
    db_path = _resolve_path(args.db) or DEFAULT_DB
    results: list[CheckResult] = []
    contract, manifest_results = load_manifest_contract(env)
    results.extend(manifest_results)
    results.extend(check_runtime_database(db_path, contract=contract))
    results.extend(check_scoring_freshness(db_path, contract=contract))
    results.extend(check_services(bool(args.require_services)))
    results.extend(check_service_parity(contract, bool(args.require_services)))
    final = _print_results(results)
    return 1 if final == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
