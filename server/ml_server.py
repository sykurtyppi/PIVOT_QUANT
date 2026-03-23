import asyncio
import heapq
import json
import logging
import math
import os
import queue
import sqlite3
import sys
import time
import threading
import atexit
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_horizon_float_map(name: str) -> dict[int, float]:
    raw = os.getenv(name, "")
    out: dict[int, float] = {}
    if not raw:
        return out
    for token in raw.split(","):
        token = token.strip()
        if not token or ":" not in token:
            continue
        key, value = token.split(":", 1)
        key_norm = key.strip().lower().rstrip("m")
        try:
            horizon = int(key_norm)
            value_f = float(value.strip())
        except Exception:
            continue
        if horizon not in {5, 15, 30, 60}:
            continue
        out[horizon] = max(0.0, min(1.0, value_f))
    return out


def _env_n_jobs(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw.strip())
    except Exception:
        return default
    if value == 0:
        return default
    if value < -1:
        return -1
    return value


def _parse_reject_or_breakout_filter_rules(raw: str) -> dict[int, set[int]]:
    rules: dict[int, set[int]] = {}
    if not raw:
        return rules
    for token in raw.replace(";", ",").split(","):
        token = token.strip()
        if not token or ":" not in token:
            continue
        horizon_raw, values_raw = token.split(":", 1)
        horizon_norm = horizon_raw.strip().lower().rstrip("m")
        if not horizon_norm.isdigit():
            continue
        horizon = int(horizon_norm)
        if horizon not in {5, 15, 30, 60}:
            continue
        values: set[int] = set()
        for value_token in values_raw.replace("/", "|").split("|"):
            value_token = value_token.strip()
            if not value_token or not value_token.lstrip("-").isdigit():
                continue
            value = int(value_token)
            if value in {-1, 0, 1}:
                values.add(value)
        if values:
            rules[horizon] = values
    return rules


ROOT = Path(__file__).resolve().parents[1]
ANALOG_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import pandas as pd
import joblib

from ml.features import build_feature_row, collect_missing, FEATURE_VERSION

log = logging.getLogger("ml_server")
_missing_threshold_warnings: set[tuple[str, int, str]] = set()

MODEL_DIR = Path(os.getenv("RF_MODEL_DIR", "data/models"))
RF_MANIFEST_PATH = os.getenv("RF_MANIFEST_PATH", "").strip()
RF_ACTIVE_MANIFEST = os.getenv("RF_ACTIVE_MANIFEST", "manifest_active.json").strip() or "manifest_active.json"
RF_CANDIDATE_MANIFEST = (
    os.getenv("RF_CANDIDATE_MANIFEST", "manifest_runtime_latest.json").strip()
    or "manifest_runtime_latest.json"
)
LEGACY_CANDIDATE_MANIFEST = "manifest_latest.json"
HOST = os.getenv("ML_SERVER_BIND", "127.0.0.1")
PORT = int(os.getenv("ML_SERVER_PORT", "5003"))
STALE_MODEL_HOURS = int(os.getenv("STALE_MODEL_HOURS", "48"))
ML_SHADOW_HORIZONS = {
    int(h.strip())
    for h in os.getenv("ML_SHADOW_HORIZONS", "30").split(",")
    if h.strip().isdigit()
}
if not ML_SHADOW_HORIZONS:
    ML_SHADOW_HORIZONS = {30}
ML_REGIME_POLICY_MODE = (os.getenv("ML_REGIME_POLICY_MODE", "shadow") or "shadow").strip().lower()
if ML_REGIME_POLICY_MODE not in {"off", "shadow", "active"}:
    ML_REGIME_POLICY_MODE = "shadow"
ML_REJECT_OR_BREAKOUT_FILTER_MODE = (
    os.getenv("ML_REJECT_OR_BREAKOUT_FILTER_MODE", "off") or "off"
).strip().lower()
if ML_REJECT_OR_BREAKOUT_FILTER_MODE not in {"off", "shadow", "active"}:
    ML_REJECT_OR_BREAKOUT_FILTER_MODE = "off"
ML_REJECT_OR_BREAKOUT_FILTER_HORIZONS = {
    int(h.strip())
    for h in os.getenv("ML_REJECT_OR_BREAKOUT_FILTER_HORIZONS", "15").split(",")
    if h.strip().isdigit() and int(h.strip()) in {5, 15, 30, 60}
}
if not ML_REJECT_OR_BREAKOUT_FILTER_HORIZONS:
    ML_REJECT_OR_BREAKOUT_FILTER_HORIZONS = {15}
ML_REJECT_OR_BREAKOUT_FILTER_BLOCK_VALUES = {
    int(v.strip())
    for v in os.getenv("ML_REJECT_OR_BREAKOUT_FILTER_BLOCK_VALUES", "-1").split(",")
    if v.strip().lstrip("-").isdigit() and int(v.strip()) in {-1, 0, 1}
}
if not ML_REJECT_OR_BREAKOUT_FILTER_BLOCK_VALUES:
    ML_REJECT_OR_BREAKOUT_FILTER_BLOCK_VALUES = {-1}
ML_REJECT_OR_BREAKOUT_FILTER_RULES = _parse_reject_or_breakout_filter_rules(
    os.getenv("ML_REJECT_OR_BREAKOUT_FILTER_RULES", "")
)
ML_REGIME_THRESHOLD_MAX_DELTA = max(
    0.0,
    min(0.20, float(os.getenv("ML_REGIME_THRESHOLD_MAX_DELTA", "0.05"))),
)
ML_REGIME_COMPRESSION_REJECT_DELTA = float(os.getenv("ML_REGIME_COMPRESSION_REJECT_DELTA", "-0.02"))
ML_REGIME_COMPRESSION_BREAK_DELTA = float(os.getenv("ML_REGIME_COMPRESSION_BREAK_DELTA", "0.02"))
ML_REGIME_EXPANSION_REJECT_DELTA = float(os.getenv("ML_REGIME_EXPANSION_REJECT_DELTA", "0.02"))
ML_REGIME_EXPANSION_BREAK_DELTA = float(os.getenv("ML_REGIME_EXPANSION_BREAK_DELTA", "-0.02"))
ML_ATR_ZONE_ULTRA_MAX = max(
    0.0,
    min(0.50, float(os.getenv("ML_ATR_ZONE_ULTRA_MAX", "0.05"))),
)
ML_ATR_ZONE_NEAR_MAX = max(
    ML_ATR_ZONE_ULTRA_MAX,
    min(0.75, float(os.getenv("ML_ATR_ZONE_NEAR_MAX", "0.10"))),
)
ML_ATR_ZONE_MID_MAX = max(
    ML_ATR_ZONE_NEAR_MAX,
    min(1.50, float(os.getenv("ML_ATR_ZONE_MID_MAX", "0.20"))),
)
ML_ATR_COMPRESSION_ULTRA_REJECT_DELTA = float(
    os.getenv("ML_ATR_COMPRESSION_ULTRA_REJECT_DELTA", "0.02")
)
ML_ATR_COMPRESSION_ULTRA_BREAK_DELTA = float(
    os.getenv("ML_ATR_COMPRESSION_ULTRA_BREAK_DELTA", "-0.01")
)
ML_ATR_COMPRESSION_NEAR_REJECT_DELTA = float(
    os.getenv("ML_ATR_COMPRESSION_NEAR_REJECT_DELTA", "-0.01")
)
ML_ATR_COMPRESSION_NEAR_BREAK_DELTA = float(
    os.getenv("ML_ATR_COMPRESSION_NEAR_BREAK_DELTA", "0.01")
)
ML_ATR_EXPANSION_ULTRA_REJECT_DELTA = float(
    os.getenv("ML_ATR_EXPANSION_ULTRA_REJECT_DELTA", "0.03")
)
ML_ATR_EXPANSION_ULTRA_BREAK_DELTA = float(
    os.getenv("ML_ATR_EXPANSION_ULTRA_BREAK_DELTA", "-0.02")
)
ML_ATR_EXPANSION_NEAR_REJECT_DELTA = float(
    os.getenv("ML_ATR_EXPANSION_NEAR_REJECT_DELTA", "0.01")
)
ML_ATR_EXPANSION_NEAR_BREAK_DELTA = float(
    os.getenv("ML_ATR_EXPANSION_NEAR_BREAK_DELTA", "-0.01")
)
ML_FEATURE_DRIFT_MIN_FEATURES = max(
    1,
    int(os.getenv("ML_FEATURE_DRIFT_MIN_FEATURES", "2")),
)
ML_FEATURE_DRIFT_IGNORE_COLUMNS = {
    col.strip()
    for col in os.getenv(
        "ML_FEATURE_DRIFT_IGNORE_COLUMNS",
        "hist_sample_size,regime_type",
    ).split(",")
    if col.strip()
}
ML_REGIME_GUARD_EXPANSION_NEAR_MODE = (
    os.getenv("ML_REGIME_GUARD_EXPANSION_NEAR_MODE", "shadow") or "shadow"
).strip().lower()
if ML_REGIME_GUARD_EXPANSION_NEAR_MODE not in {"off", "shadow", "active"}:
    ML_REGIME_GUARD_EXPANSION_NEAR_MODE = "shadow"
ML_REGIME_GUARD_EXPANSION_NEAR_STRATEGY = (
    os.getenv("ML_REGIME_GUARD_EXPANSION_NEAR_STRATEGY", "no_trade") or "no_trade"
).strip().lower()
if ML_REGIME_GUARD_EXPANSION_NEAR_STRATEGY not in {"no_trade", "tighten"}:
    ML_REGIME_GUARD_EXPANSION_NEAR_STRATEGY = "no_trade"
ML_REGIME_GUARD_EXPANSION_NEAR_REJECT_DELTA = float(
    os.getenv("ML_REGIME_GUARD_EXPANSION_NEAR_REJECT_DELTA", "0.03")
)
ML_REGIME_GUARD_EXPANSION_NEAR_BREAK_DELTA = float(
    os.getenv("ML_REGIME_GUARD_EXPANSION_NEAR_BREAK_DELTA", "0.03")
)
PREDICTION_LOG_DB = Path(os.getenv(
    "PREDICTION_LOG_DB",
    str(ROOT / "data" / "pivot_events.sqlite"),
))
_PREDICTION_LOG_LOCAL = threading.local()
_PREDICTION_LOG_SCHEMA_READY = False
_PREDICTION_LOG_SCHEMA_LOCK = threading.Lock()
SCORE_MAX_BATCH_EVENTS = max(1, int(os.getenv("ML_SCORE_MAX_BATCH_EVENTS", "256")))
SCORE_MAX_BODY_BYTES = max(1024, int(os.getenv("ML_SCORE_MAX_BODY_BYTES", "262144")))
ML_SCORE_MAX_IN_FLIGHT = max(1, int(os.getenv("ML_SCORE_MAX_IN_FLIGHT", "2")))
ML_SCORE_ANALOG_DISABLE_IN_FLIGHT = max(
    0, int(os.getenv("ML_SCORE_ANALOG_DISABLE_IN_FLIGHT", "1"))
)
ML_INFERENCE_N_JOBS = _env_n_jobs("ML_INFERENCE_N_JOBS", 1)
PREDICTION_LOG_CONNECT_TIMEOUT_SEC = max(
    0.01, float(os.getenv("PREDICTION_LOG_CONNECT_TIMEOUT_SEC", "0.5"))
)
PREDICTION_LOG_BUSY_TIMEOUT_MS = max(
    0, int(os.getenv("PREDICTION_LOG_BUSY_TIMEOUT_MS", "500"))
)
PREDICTION_LOG_LOCK_WARN_INTERVAL_SEC = max(
    1.0, float(os.getenv("PREDICTION_LOG_LOCK_WARN_INTERVAL_SEC", "60"))
)
PREDICTION_LOG_SQLITE_SYNC = (os.getenv("PREDICTION_LOG_SQLITE_SYNC", "FULL") or "FULL").strip().upper()
if PREDICTION_LOG_SQLITE_SYNC not in {"OFF", "NORMAL", "FULL", "EXTRA"}:
    PREDICTION_LOG_SQLITE_SYNC = "FULL"
PREDICTION_LOG_WAL_AUTOCHECKPOINT = max(
    100, int(os.getenv("PREDICTION_LOG_WAL_AUTOCHECKPOINT", "1000"))
)
PREDICTION_LOG_QUEUE_MAX_SIZE = max(
    64, int(os.getenv("PREDICTION_LOG_QUEUE_MAX_SIZE", "4096"))
)
PREDICTION_LOG_ALERT_QUEUE_DEPTH = max(
    1, int(os.getenv("PREDICTION_LOG_ALERT_QUEUE_DEPTH", "512"))
)
PREDICTION_LOG_ALERT_WRITE_FAIL_TOTAL = max(
    0, int(os.getenv("PREDICTION_LOG_ALERT_WRITE_FAIL_TOTAL", "0"))
)
PREDICTION_LOG_ALERT_DROPPED_TOTAL = max(
    0, int(os.getenv("PREDICTION_LOG_ALERT_DROPPED_TOTAL", "0"))
)
ML_RELOAD_MIN_INTERVAL_SEC = max(
    0.0, float(os.getenv("ML_RELOAD_MIN_INTERVAL_SEC", "1.5"))
)
ML_ANALOG_ENABLED = _env_bool("ML_ANALOG_ENABLED", True)
ML_ANALOG_DB = Path(os.getenv("ML_ANALOG_DB", str(PREDICTION_LOG_DB)))
ML_ANALOG_K = max(3, int(os.getenv("ML_ANALOG_K", "20")))
ML_ANALOG_MAX_ROWS = max(500, int(os.getenv("ML_ANALOG_MAX_ROWS", "250000")))
ML_ANALOG_MAX_CANDIDATES = max(50, int(os.getenv("ML_ANALOG_MAX_CANDIDATES", "1500")))
ML_ANALOG_MIN_POOL = max(10, int(os.getenv("ML_ANALOG_MIN_POOL", "30")))
ML_ANALOG_MIN_N = max(3, int(os.getenv("ML_ANALOG_MIN_N", "10")))
ML_ANALOG_MIN_EFFECTIVE_N = max(2.0, float(os.getenv("ML_ANALOG_MIN_EFFECTIVE_N", "6")))
ML_ANALOG_MIN_FEATURES = max(1, int(os.getenv("ML_ANALOG_MIN_FEATURES", "3")))
ML_ANALOG_MIN_FEATURE_OVERLAP = max(
    1, int(os.getenv("ML_ANALOG_MIN_FEATURE_OVERLAP", "3"))
)
ML_ANALOG_MIN_FEATURE_SUPPORT = max(
    3, int(os.getenv("ML_ANALOG_MIN_FEATURE_SUPPORT", "20"))
)
ML_ANALOG_PREFILTER_ENABLED = _env_bool("ML_ANALOG_PREFILTER_ENABLED", True)
ML_ANALOG_PREFILTER_MAX_ROWS = max(
    100, int(os.getenv("ML_ANALOG_PREFILTER_MAX_ROWS", "600"))
)
ML_ANALOG_PREFILTER_FEATURE_LIMIT = max(
    1, int(os.getenv("ML_ANALOG_PREFILTER_FEATURE_LIMIT", "4"))
)
ML_ANALOG_MAX_MEAN_DISTANCE = max(
    0.1, float(os.getenv("ML_ANALOG_MAX_MEAN_DISTANCE", "2.5"))
)
ML_ANALOG_MAX_CI_WIDTH = max(0.05, float(os.getenv("ML_ANALOG_MAX_CI_WIDTH", "0.6")))
ML_ANALOG_RECENCY_TAU_DAYS = max(
    0.1, float(os.getenv("ML_ANALOG_RECENCY_TAU_DAYS", "14"))
)
ML_ANALOG_RECENCY_FLOOR = max(
    0.0, min(1.0, float(os.getenv("ML_ANALOG_RECENCY_FLOOR", "0.25")))
)
ML_ANALOG_SIM_WEIGHT_MODE = (
    os.getenv("ML_ANALOG_SIM_WEIGHT_MODE", "inv1p") or "inv1p"
).strip().lower()
if ML_ANALOG_SIM_WEIGHT_MODE not in {"inv1p", "invdist"}:
    ML_ANALOG_SIM_WEIGHT_MODE = "inv1p"
ML_ANALOG_PRIOR_STRENGTH = max(
    0.0, float(os.getenv("ML_ANALOG_PRIOR_STRENGTH", "8"))
)
ML_ANALOG_DISAGREEMENT_FLAG = max(
    0.0, min(1.0, float(os.getenv("ML_ANALOG_DISAGREEMENT_FLAG", "0.25")))
)
ML_ANALOG_DISAGREEMENT_GUARD_MODE = (
    os.getenv("ML_ANALOG_DISAGREEMENT_GUARD_MODE", "shadow") or "shadow"
).strip().lower()
if ML_ANALOG_DISAGREEMENT_GUARD_MODE not in {"off", "shadow", "active"}:
    ML_ANALOG_DISAGREEMENT_GUARD_MODE = "shadow"
ML_ANALOG_DISAGREEMENT_GUARD_HORIZONS = {
    int(h.strip())
    for h in os.getenv("ML_ANALOG_DISAGREEMENT_GUARD_HORIZONS", "15,30,60").split(",")
    if h.strip().isdigit() and int(h.strip()) in {5, 15, 30, 60}
}
if not ML_ANALOG_DISAGREEMENT_GUARD_HORIZONS:
    ML_ANALOG_DISAGREEMENT_GUARD_HORIZONS = {15, 30, 60}
ML_ANALOG_PROMOTION_GATE_PATH = Path(
    os.getenv(
        "ML_ANALOG_PROMOTION_GATE_PATH",
        str(ROOT / "logs" / "reports" / "analog_promotion_gate_latest.json"),
    )
)
ML_ANALOG_BLEND_MODE = (os.getenv("ML_ANALOG_BLEND_MODE", "shadow") or "shadow").strip().lower()
if ML_ANALOG_BLEND_MODE not in {"off", "shadow", "active"}:
    ML_ANALOG_BLEND_MODE = "shadow"
ML_ANALOG_BLEND_PARTIAL_MODE = (
    os.getenv("ML_ANALOG_BLEND_PARTIAL_MODE", "off") or "off"
).strip().lower()
if ML_ANALOG_BLEND_PARTIAL_MODE not in {"off", "horizon", "target"}:
    ML_ANALOG_BLEND_PARTIAL_MODE = "off"
ML_ANALOG_BLEND_WEIGHT_BASE = max(
    0.0, min(1.0, float(os.getenv("ML_ANALOG_BLEND_WEIGHT_BASE", "0.30")))
)
ML_ANALOG_BLEND_WEIGHT_MAX = max(
    0.0, min(1.0, float(os.getenv("ML_ANALOG_BLEND_WEIGHT_MAX", "0.60")))
)
if ML_ANALOG_BLEND_WEIGHT_MAX < ML_ANALOG_BLEND_WEIGHT_BASE:
    ML_ANALOG_BLEND_WEIGHT_MAX = ML_ANALOG_BLEND_WEIGHT_BASE
ML_ANALOG_BLEND_N_EFF_REF = max(
    1.0, float(os.getenv("ML_ANALOG_BLEND_N_EFF_REF", "20"))
)
ML_ANALOG_BLEND_MAX_SHIFT_REJECT = max(
    0.0, min(1.0, float(os.getenv("ML_ANALOG_BLEND_MAX_SHIFT_REJECT", "1.0")))
)
ML_ANALOG_BLEND_MAX_SHIFT_BREAK = max(
    0.0, min(1.0, float(os.getenv("ML_ANALOG_BLEND_MAX_SHIFT_BREAK", "1.0")))
)
ML_ANALOG_BLEND_MAX_SHIFT_REJECT_BY_HORIZON = _env_horizon_float_map(
    "ML_ANALOG_BLEND_MAX_SHIFT_REJECT_BY_HORIZON"
)
ML_ANALOG_BLEND_MAX_SHIFT_BREAK_BY_HORIZON = _env_horizon_float_map(
    "ML_ANALOG_BLEND_MAX_SHIFT_BREAK_BY_HORIZON"
)
ML_ANALOG_FEATURE_WEIGHTS = {
    "distance_bps": max(0.0, float(os.getenv("ML_ANALOG_W_DISTANCE_BPS", "1.0"))),
    "distance_atr_ratio": max(
        0.0, float(os.getenv("ML_ANALOG_W_DISTANCE_ATR_RATIO", "1.2"))
    ),
    "vwap_side": max(0.0, float(os.getenv("ML_ANALOG_W_VWAP_SIDE", "0.5"))),
    "ema_stack": max(0.0, float(os.getenv("ML_ANALOG_W_EMA_STACK", "0.5"))),
    "rv_30": max(0.0, float(os.getenv("ML_ANALOG_W_RV_30", "0.8"))),
    "or_size_atr": max(0.0, float(os.getenv("ML_ANALOG_W_OR_SIZE_ATR", "0.7"))),
    "overnight_gap_atr": max(
        0.0, float(os.getenv("ML_ANALOG_W_OVERNIGHT_GAP_ATR", "0.6"))
    ),
}

allowed_origins = [
    origin.strip()
    for origin in os.getenv(
        "ML_CORS_ORIGINS", "http://127.0.0.1:3000,http://localhost:3000"
    ).split(",")
    if origin.strip()
]
if "*" in allowed_origins:
    print("ML server warning: ignoring wildcard '*' in ML_CORS_ORIGINS; use explicit origins.")
    allowed_origins = [origin for origin in allowed_origins if origin != "*"]
if not allowed_origins:
    print("ML server warning: ML_CORS_ORIGINS is empty; no browser origins are allowed.")


def _threshold_from_map(
    threshold_map: dict[str, dict[int, float]],
    target: str,
    horizon: int,
    *,
    context: str,
) -> float:
    target_map = threshold_map.get(target, {})
    if horizon in target_map:
        return float(target_map[horizon])
    key = (target, int(horizon), context)
    if key not in _missing_threshold_warnings:
        log.warning(
            "Missing %s threshold for %sm horizon in %s; using 0.5 fallback",
            target,
            horizon,
            context,
        )
        _missing_threshold_warnings.add(key)
    return 0.5


class ModelRegistry:
    def __init__(self):
        self.manifest = None
        self.manifest_path: str | None = None
        self.manifest_signature: tuple[int, int] | None = None
        self.models = {"reject": {}, "break": {}}
        self.thresholds = {"reject": {}, "break": {}}
        self._lock = threading.RLock()

    def resolve_manifest_path(self) -> Path:
        if RF_MANIFEST_PATH:
            return Path(RF_MANIFEST_PATH)
        active_path = MODEL_DIR / RF_ACTIVE_MANIFEST
        if active_path.exists():
            return active_path
        candidate_path = MODEL_DIR / RF_CANDIDATE_MANIFEST
        if candidate_path.exists():
            return candidate_path
        if candidate_path.name != LEGACY_CANDIDATE_MANIFEST:
            legacy_path = MODEL_DIR / LEGACY_CANDIDATE_MANIFEST
            if legacy_path.exists():
                return legacy_path
        return candidate_path

    @staticmethod
    def _file_signature(path: Path) -> tuple[int, int]:
        stat = path.stat()
        return (int(stat.st_mtime_ns), int(stat.st_size))

    @staticmethod
    def _set_inference_n_jobs(node: object, n_jobs: int, seen: set[int] | None = None) -> None:
        """Recursively force estimator n_jobs for low-latency single-event inference."""
        if node is None:
            return
        if seen is None:
            seen = set()
        node_id = id(node)
        if node_id in seen:
            return
        seen.add(node_id)

        if hasattr(node, "n_jobs"):
            try:
                setattr(node, "n_jobs", n_jobs)
            except Exception:
                pass

        if isinstance(node, dict):
            for value in node.values():
                ModelRegistry._set_inference_n_jobs(value, n_jobs, seen)
            return
        if isinstance(node, (list, tuple, set)):
            for value in node:
                ModelRegistry._set_inference_n_jobs(value, n_jobs, seen)
            return

        for attr_name in ("base_model", "estimator", "model", "classifier", "regressor"):
            child = getattr(node, attr_name, None)
            if child is not None:
                ModelRegistry._set_inference_n_jobs(child, n_jobs, seen)

        named_steps = getattr(node, "named_steps", None)
        if hasattr(named_steps, "items"):
            for _, child in named_steps.items():
                ModelRegistry._set_inference_n_jobs(child, n_jobs, seen)

        steps = getattr(node, "steps", None)
        if isinstance(steps, list):
            for item in steps:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    ModelRegistry._set_inference_n_jobs(item[1], n_jobs, seen)

        transformers = getattr(node, "transformers", None)
        if isinstance(transformers, list):
            for item in transformers:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    ModelRegistry._set_inference_n_jobs(item[1], n_jobs, seen)

        transformers_fitted = getattr(node, "transformers_", None)
        if isinstance(transformers_fitted, list):
            for item in transformers_fitted:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    ModelRegistry._set_inference_n_jobs(item[1], n_jobs, seen)

        estimators = getattr(node, "estimators_", None)
        if isinstance(estimators, (list, tuple)):
            for child in estimators:
                ModelRegistry._set_inference_n_jobs(child, n_jobs, seen)

    def is_manifest_unchanged(self) -> bool:
        manifest_path = self.resolve_manifest_path()
        if not manifest_path.exists():
            return False
        signature = self._file_signature(manifest_path)
        manifest_path_str = str(manifest_path)
        with self._lock:
            return bool(
                self.manifest is not None
                and self.manifest_path == manifest_path_str
                and self.manifest_signature == signature
            )

    def load(self, *, force: bool = False) -> bool:
        manifest_path = self.resolve_manifest_path()
        if not manifest_path.exists():
            raise FileNotFoundError(f"Missing manifest at {manifest_path}")
        signature = self._file_signature(manifest_path)
        manifest_path_str = str(manifest_path)
        if not force:
            with self._lock:
                if (
                    self.manifest_path == manifest_path_str
                    and self.manifest_signature == signature
                    and self.manifest is not None
                ):
                    return False
        with manifest_path.open("r", encoding="utf-8") as handle:
            manifest = json.load(handle)
        models = {"reject": {}, "break": {}}
        thresholds = {"reject": {}, "break": {}}

        # Load thresholds from manifest first
        manifest_thresholds = manifest.get("thresholds", {})
        for target in ("reject", "break"):
            for horizon_str, threshold in manifest_thresholds.get(target, {}).items():
                thresholds[target][int(horizon_str)] = float(threshold)

        for target, horizons in manifest.get("models", {}).items():
            for horizon, filename in horizons.items():
                path = MODEL_DIR / filename
                if not path.exists():
                    continue
                payload = joblib.load(path)
                if isinstance(payload, dict):
                    ModelRegistry._set_inference_n_jobs(payload.get("pipeline"), ML_INFERENCE_N_JOBS)
                    ModelRegistry._set_inference_n_jobs(payload.get("calibrator"), ML_INFERENCE_N_JOBS)
                else:
                    ModelRegistry._set_inference_n_jobs(payload, ML_INFERENCE_N_JOBS)
                models[target][int(horizon)] = payload

                # Fall back to pickle-embedded threshold if manifest didn't have it
                h_int = int(horizon)
                if h_int not in thresholds.get(target, {}):
                    pkl_thresh = payload.get("optimal_threshold")
                    if pkl_thresh is not None:
                        thresholds.setdefault(target, {})[h_int] = float(pkl_thresh)
                if h_int not in thresholds.get(target, {}):
                    log.warning(
                        "Missing decision threshold for %s %sm model after manifest and pickle load; "
                        "runtime will fall back to 0.5 until artifacts are fixed.",
                        target,
                        h_int,
                    )

        # Atomically swap in the newly built registry payload so /score never
        # sees a transient empty model map during reload.
        with self._lock:
            self.manifest = manifest
            self.manifest_path = manifest_path_str
            self.manifest_signature = signature
            self.models = models
            self.thresholds = thresholds
        return True

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "manifest": self.manifest,
                "manifest_path": self.manifest_path,
                "manifest_signature": self.manifest_signature,
                "models": {
                    "reject": dict(self.models.get("reject", {})),
                    "break": dict(self.models.get("break", {})),
                },
                "thresholds": {
                    "reject": dict(self.thresholds.get("reject", {})),
                    "break": dict(self.thresholds.get("break", {})),
                },
            }

    def get_threshold(self, target: str, horizon: int) -> float:
        """Get optimal decision threshold for a target/horizon pair.
        Falls back to 0.5 if not available."""
        with self._lock:
            return _threshold_from_map(self.thresholds, target, horizon, context="registry")

    def available(self):
        snapshot = self.snapshot()
        models = snapshot.get("models", {})
        if not isinstance(models, dict):
            models = {}
        return {target: sorted(horizons.keys()) for target, horizons in models.items()}


def _analog_level_family(level_type: object) -> str:
    text = str(level_type or "")
    if text.startswith("R"):
        return "resistance"
    if text.startswith("S"):
        return "support"
    if text == "GAMMA":
        return "gamma"
    return "pivot"


def _analog_tod_bucket(ts_event_ms: int | None) -> str:
    if not ts_event_ms:
        return "unknown"
    dt = datetime.fromtimestamp(ts_event_ms / 1000, tz=ANALOG_TZ)
    hour = dt.hour
    if hour < 10:
        return "open"
    if hour < 14:
        return "mid"
    if hour < 16:
        return "power"
    return "overnight"


def _analog_regime_bucket(regime_type: int | None) -> str:
    if regime_type in (1, 2, 4):
        return "expansion"
    if regime_type == 3:
        return "compression"
    return "neutral"


def _sign3(value: float | None) -> float | None:
    if value is None:
        return None
    if value > 0:
        return 1.0
    if value < 0:
        return -1.0
    return 0.0


def _weighted_interval(p: float, n_eff: float, z: float = 1.96) -> tuple[float, float, float]:
    if n_eff <= 0:
        return (0.0, 1.0, 1.0)
    p = max(0.0, min(1.0, float(p)))
    se = math.sqrt(max(0.0, p * (1.0 - p)) / max(n_eff, 1e-9))
    lo = max(0.0, p - z * se)
    hi = min(1.0, p + z * se)
    return lo, hi, max(0.0, hi - lo)


class AnalogEngine:
    """Nearest-neighbor analogs for shadow diagnostics.

    This engine never changes live decisions; it only surfaces context and
    disagreement metrics alongside model probabilities.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.error: str | None = None
        self.loaded_at_ms: int | None = None
        self.rows_by_horizon: dict[int, list[dict[str, object]]] = {}
        self.enabled = bool(ML_ANALOG_ENABLED)
        self._lock = threading.RLock()

    def refresh(self) -> None:
        loaded_at_ms = int(time.time() * 1000)
        error: str | None = None
        rows_by_horizon: dict[int, list[dict[str, object]]] = {}
        enabled = bool(self.enabled)
        if not enabled:
            with self._lock:
                self.error = None
                self.rows_by_horizon = {}
                self.loaded_at_ms = loaded_at_ms
            return

        conn = None
        try:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            sql = """
                SELECT
                    te.event_id,
                    te.symbol,
                    te.ts_event,
                    te.level_type,
                    te.regime_type,
                    te.gamma_mode,
                    te.distance_bps,
                    te.atr,
                    te.touch_price,
                    te.ema_state,
                    te.vwap_dist_bps,
                    te.rv_30,
                    te.or_size_atr,
                    te.overnight_gap_atr,
                    el.horizon_min,
                    el.reject,
                    el.break
                FROM touch_events te
                JOIN event_labels el ON el.event_id = te.event_id
                WHERE te.ts_event IS NOT NULL
                  AND el.horizon_min IN (5, 15, 30, 60)
                ORDER BY te.ts_event DESC
                LIMIT ?
            """
            rows = conn.execute(sql, (int(ML_ANALOG_MAX_ROWS),)).fetchall()
            out: dict[int, list[dict[str, object]]] = {5: [], 15: [], 30: [], 60: []}
            for row in rows:
                horizon = int(row["horizon_min"])
                ts_event = int(row["ts_event"])
                atr = _to_float(row["atr"])
                touch_price = _to_float(row["touch_price"])
                distance_bps = _to_float(row["distance_bps"])
                atr_bps = None
                if atr is not None and atr > 0 and touch_price is not None and touch_price > 0:
                    atr_bps = atr / touch_price * 1e4
                distance_atr_ratio = None
                if (
                    atr_bps is not None
                    and atr_bps > 0
                    and distance_bps is not None
                ):
                    distance_atr_ratio = distance_bps / atr_bps
                out[horizon].append(
                    {
                        "event_id": row["event_id"],
                        "symbol": row["symbol"],
                        "ts_event": ts_event,
                        "level_family": _analog_level_family(row["level_type"]),
                        "tod_bucket": _analog_tod_bucket(ts_event),
                        "regime_bucket": _analog_regime_bucket(_to_int(row["regime_type"])),
                        "gamma_mode": _to_int(row["gamma_mode"]),
                        "distance_bps": distance_bps,
                        "distance_atr_ratio": _to_float(distance_atr_ratio),
                        "vwap_side": _sign3(_to_float(row["vwap_dist_bps"])),
                        "ema_stack": _sign3(_to_float(row["ema_state"])),
                        "rv_30": _to_float(row["rv_30"]),
                        "or_size_atr": _to_float(row["or_size_atr"]),
                        "overnight_gap_atr": _to_float(row["overnight_gap_atr"]),
                        "reject": _to_float(row["reject"]),
                        "break": _to_float(row["break"]),
                    }
                )
            rows_by_horizon = out
        except Exception as exc:  # pragma: no cover - defensive
            error = str(exc)
            rows_by_horizon = {}
        finally:
            if conn is not None:
                conn.close()
        with self._lock:
            self.error = error
            self.rows_by_horizon = rows_by_horizon
            self.loaded_at_ms = loaded_at_ms

    def health(self) -> dict[str, object]:
        with self._lock:
            rows_by_horizon = self.rows_by_horizon
            error = self.error
            loaded_at_ms = self.loaded_at_ms
            enabled = self.enabled
        return {
            "enabled": enabled,
            "db": str(self.db_path),
            "loaded_at_ms": loaded_at_ms,
            "error": error,
            "rows": {str(h): len(rows) for h, rows in rows_by_horizon.items()},
            "k": ML_ANALOG_K,
            "max_candidates": ML_ANALOG_MAX_CANDIDATES,
            "min_pool": ML_ANALOG_MIN_POOL,
            "min_n": ML_ANALOG_MIN_N,
            "min_effective_n": ML_ANALOG_MIN_EFFECTIVE_N,
            "sim_weight_mode": ML_ANALOG_SIM_WEIGHT_MODE,
            "recency_floor": ML_ANALOG_RECENCY_FLOOR,
            "prior_strength": ML_ANALOG_PRIOR_STRENGTH,
        }

    @staticmethod
    def _candidate_stages(
        base_rows: list[dict[str, object]],
        *,
        level_family: str,
        tod_bucket: str,
        regime_bucket: str,
        gamma_mode: int | None,
    ) -> tuple[str, list[dict[str, object]]]:
        def _gamma_ok(row_gamma: int | None) -> bool:
            return gamma_mode is None or row_gamma is None or row_gamma == gamma_mode

        stages = [
            (
                "strict",
                lambda row: (
                    row.get("level_family") == level_family
                    and row.get("tod_bucket") == tod_bucket
                    and row.get("regime_bucket") == regime_bucket
                    and _gamma_ok(_to_int(row.get("gamma_mode")))
                ),
            ),
            (
                "no_gamma",
                lambda row: (
                    row.get("level_family") == level_family
                    and row.get("tod_bucket") == tod_bucket
                    and row.get("regime_bucket") == regime_bucket
                ),
            ),
            (
                "no_tod",
                lambda row: (
                    row.get("level_family") == level_family
                    and row.get("regime_bucket") == regime_bucket
                ),
            ),
            ("level_only", lambda row: row.get("level_family") == level_family),
            ("symbol_only", lambda _row: True),
        ]

        fallback = list(base_rows)
        fallback_name = "symbol_only"
        for stage_name, predicate in stages:
            subset = [row for row in base_rows if predicate(row)]
            if len(subset) > len(fallback):
                fallback = subset
                fallback_name = stage_name
            if len(subset) >= ML_ANALOG_MIN_POOL:
                return stage_name, subset
        return fallback_name, fallback

    @staticmethod
    def _distance_features(
        query_features: dict[str, float | None],
        candidates: list[dict[str, object]],
    ) -> tuple[list[str], dict[str, tuple[float, float]]]:
        selected: list[str] = []
        stats: dict[str, tuple[float, float]] = {}
        for name, weight in ML_ANALOG_FEATURE_WEIGHTS.items():
            if weight <= 0:
                continue
            q_value = query_features.get(name)
            if q_value is None:
                continue
            values = [_to_float(row.get(name)) for row in candidates]
            values = [value for value in values if value is not None]
            if len(values) < ML_ANALOG_MIN_FEATURE_SUPPORT:
                continue
            mean = float(sum(values) / len(values))
            variance = float(sum((value - mean) ** 2 for value in values) / len(values))
            std = max(math.sqrt(variance), 1e-6)
            selected.append(name)
            stats[name] = (mean, std)
        return selected, stats

    @staticmethod
    def _prefilter_candidates(
        rows: list[dict[str, object]],
        *,
        query_features: dict[str, float | None],
        feature_names: list[str],
        feature_stats: dict[str, tuple[float, float]],
    ) -> list[dict[str, object]]:
        if (
            not ML_ANALOG_PREFILTER_ENABLED
            or len(rows) <= ML_ANALOG_PREFILTER_MAX_ROWS
            or ML_ANALOG_PREFILTER_MAX_ROWS <= 0
        ):
            return rows

        ranked_features = sorted(
            [
                name
                for name in feature_names
                if query_features.get(name) is not None
            ],
            key=lambda name: ML_ANALOG_FEATURE_WEIGHTS.get(name, 0.0),
            reverse=True,
        )
        prefilter_features = ranked_features[:ML_ANALOG_PREFILTER_FEATURE_LIMIT]
        if not prefilter_features:
            return rows

        coarse_ranked: list[tuple[float, dict[str, object]]] = []
        for row in rows:
            score = 0.0
            overlap = 0
            total_w = 0.0
            for name in prefilter_features:
                q_value = query_features.get(name)
                c_value = _to_float(row.get(name))
                if q_value is None or c_value is None:
                    continue
                mean, std = feature_stats.get(name, (0.0, 1.0))
                qz = (q_value - mean) / std
                cz = (c_value - mean) / std
                weight = ML_ANALOG_FEATURE_WEIGHTS.get(name, 1.0)
                score += weight * abs(qz - cz)
                total_w += weight
                overlap += 1
            if overlap == 0 or total_w <= 0:
                continue
            coarse_ranked.append((score / total_w, row))

        if not coarse_ranked:
            return rows

        keep_count = min(ML_ANALOG_PREFILTER_MAX_ROWS, len(coarse_ranked))
        top_rows = heapq.nsmallest(keep_count, coarse_ranked, key=lambda item: item[0])
        return [item[1] for item in top_rows]

    def _score_horizon(
        self,
        *,
        rows: list[dict[str, object]],
        query_event: dict[str, object],
        query_features: dict[str, float | None],
        model_reject_prob: float | None,
        model_break_prob: float | None,
    ) -> dict[str, object]:
        event_ts = _to_int(query_event.get("ts_event")) or int(time.time() * 1000)
        symbol = str(query_event.get("symbol") or "").strip()
        level_family = _analog_level_family(query_event.get("level_type"))
        tod_bucket = str(query_event.get("tod_bucket") or _analog_tod_bucket(event_ts))
        regime_bucket = str(query_event.get("regime_bucket") or "neutral")
        gamma_mode = _to_int(query_event.get("gamma_mode"))

        base_rows: list[dict[str, object]] = []
        for row in rows:
            ts_event = _to_int(row.get("ts_event"))
            if ts_event is None or ts_event >= event_ts:
                continue
            if symbol and str(row.get("symbol") or "") != symbol:
                continue
            base_rows.append(row)
            if len(base_rows) >= ML_ANALOG_MAX_CANDIDATES:
                break

        if not base_rows:
            return {
                "status": "no_history",
                "stage": "none",
                "n": 0,
                "n_eff": 0.0,
                "reject_prob": None,
                "break_prob": None,
                "reasons": ["no_history"],
            }

        stage_name, stage_rows = self._candidate_stages(
            base_rows,
            level_family=level_family,
            tod_bucket=tod_bucket,
            regime_bucket=regime_bucket,
            gamma_mode=gamma_mode,
        )
        feature_names, feature_stats = self._distance_features(query_features, stage_rows)
        if len(feature_names) < ML_ANALOG_MIN_FEATURES:
            return {
                "status": "insufficient_features",
                "stage": stage_name,
                "n": len(stage_rows),
                "n_eff": 0.0,
                "reject_prob": None,
                "break_prob": None,
                "reasons": ["insufficient_feature_support"],
            }

        candidate_rows = self._prefilter_candidates(
            stage_rows,
            query_features=query_features,
            feature_names=feature_names,
            feature_stats=feature_stats,
        )

        ranked: list[dict[str, object]] = []
        for row in candidate_rows:
            overlap = 0
            weighted_sq = 0.0
            total_w = 0.0
            for name in feature_names:
                q_value = query_features.get(name)
                c_value = _to_float(row.get(name))
                if q_value is None or c_value is None:
                    continue
                mean, std = feature_stats[name]
                qz = (q_value - mean) / std
                cz = (c_value - mean) / std
                weight = ML_ANALOG_FEATURE_WEIGHTS.get(name, 1.0)
                weighted_sq += weight * (qz - cz) ** 2
                total_w += weight
                overlap += 1
            if overlap < ML_ANALOG_MIN_FEATURE_OVERLAP or total_w <= 0:
                continue
            distance = math.sqrt(weighted_sq / total_w)
            ranked.append({"row": row, "distance": distance})

        if not ranked:
            return {
                "status": "insufficient_overlap",
                "stage": stage_name,
                "n": 0,
                "n_eff": 0.0,
                "reject_prob": None,
                "break_prob": None,
                "reasons": ["insufficient_feature_overlap"],
            }

        ranked.sort(key=lambda item: float(item["distance"]))
        top_k = ranked[: min(ML_ANALOG_K, len(ranked))]
        mean_distance = float(
            sum(float(item["distance"]) for item in top_k) / max(len(top_k), 1)
        )

        weighted_reject = 0.0
        weighted_break = 0.0
        weight_sum = 0.0
        weight_sq_sum = 0.0
        stage_reject_values = [
            _to_float(row.get("reject"))
            for row in stage_rows
            if _to_float(row.get("reject")) is not None
        ]
        stage_break_values = [
            _to_float(row.get("break"))
            for row in stage_rows
            if _to_float(row.get("break")) is not None
        ]
        reject_prior = (
            float(sum(stage_reject_values) / len(stage_reject_values))
            if stage_reject_values
            else 0.5
        )
        break_prior = (
            float(sum(stage_break_values) / len(stage_break_values))
            if stage_break_values
            else 0.5
        )
        for item in top_k:
            row = item["row"]
            ts_event = _to_int(row.get("ts_event")) or event_ts
            age_days = max(0.0, (event_ts - ts_event) / 86_400_000.0)
            distance = float(item["distance"])
            if ML_ANALOG_SIM_WEIGHT_MODE == "invdist":
                sim_weight = 1.0 / (distance + 1e-6)
            else:
                sim_weight = 1.0 / (1.0 + distance)
            recency_weight = max(
                ML_ANALOG_RECENCY_FLOOR,
                math.exp(-age_days / ML_ANALOG_RECENCY_TAU_DAYS),
            )
            weight = sim_weight * recency_weight
            reject_value = _to_float(row.get("reject")) or 0.0
            break_value = _to_float(row.get("break")) or 0.0
            weighted_reject += weight * reject_value
            weighted_break += weight * break_value
            weight_sum += weight
            weight_sq_sum += weight ** 2

        if weight_sum <= 0:
            return {
                "status": "insufficient_weight",
                "stage": stage_name,
                "n": 0,
                "n_eff": 0.0,
                "reject_prob": None,
                "break_prob": None,
                "reasons": ["invalid_weight_sum"],
            }

        reject_prob_raw = weighted_reject / weight_sum
        break_prob_raw = weighted_break / weight_sum
        n = len(top_k)
        n_eff = (weight_sum**2) / max(weight_sq_sum, 1e-9)
        reject_prob_est = float(reject_prob_raw)
        break_prob_est = float(break_prob_raw)
        if ML_ANALOG_PRIOR_STRENGTH > 0:
            denom = max(n_eff + ML_ANALOG_PRIOR_STRENGTH, 1e-9)
            reject_prob_est = float(
                (n_eff * reject_prob_est + ML_ANALOG_PRIOR_STRENGTH * reject_prior)
                / denom
            )
            break_prob_est = float(
                (n_eff * break_prob_est + ML_ANALOG_PRIOR_STRENGTH * break_prior)
                / denom
            )
        reject_ci_lo, reject_ci_hi, reject_ci_w = _weighted_interval(
            reject_prob_est, n_eff
        )
        break_ci_lo, break_ci_hi, break_ci_w = _weighted_interval(
            break_prob_est, n_eff
        )

        reasons: list[str] = []
        if n < ML_ANALOG_MIN_N:
            reasons.append("min_n")
        if n_eff < ML_ANALOG_MIN_EFFECTIVE_N:
            reasons.append("min_effective_n")
        if mean_distance > ML_ANALOG_MAX_MEAN_DISTANCE:
            reasons.append("mean_distance")
        if max(reject_ci_w, break_ci_w) > ML_ANALOG_MAX_CI_WIDTH:
            reasons.append("ci_width")

        passed = len(reasons) == 0
        reject_prob = reject_prob_est if passed else None
        break_prob = break_prob_est if passed else None

        disagreement = None
        if reject_prob is not None and model_reject_prob is not None:
            disagreement = abs(float(model_reject_prob) - float(reject_prob))
        if break_prob is not None and model_break_prob is not None:
            disagreement_break = abs(float(model_break_prob) - float(break_prob))
            disagreement = max(disagreement or 0.0, disagreement_break)

        return {
            "status": "ok" if passed else "insufficient_quality",
            "stage": stage_name,
            "features": feature_names,
            "n": n,
            "candidate_pool_n": len(candidate_rows),
            "n_eff": float(n_eff),
            "mean_distance": mean_distance,
            "reject_prob": reject_prob,
            "break_prob": break_prob,
            "reject_prob_raw": float(reject_prob_raw),
            "break_prob_raw": float(break_prob_raw),
            "reject_prob_shrunk": float(reject_prob_est),
            "break_prob_shrunk": float(break_prob_est),
            "reject_prior": float(reject_prior),
            "break_prior": float(break_prior),
            "reject_ci": [float(reject_ci_lo), float(reject_ci_hi)],
            "break_ci": [float(break_ci_lo), float(break_ci_hi)],
            "reject_ci_width": float(reject_ci_w),
            "break_ci_width": float(break_ci_w),
            "disagreement": disagreement,
            "reasons": reasons,
        }

    def score_event(
        self,
        *,
        event: dict[str, object],
        features: dict[str, object],
        horizons: list[int],
        trade_regime: str,
        scores: dict[str, float | None],
        best_horizon: int | None,
    ) -> dict[str, object]:
        with self._lock:
            enabled = self.enabled
            error = self.error
            loaded_at_ms = self.loaded_at_ms
            rows_by_horizon = self.rows_by_horizon
        summary: dict[str, object] = {
            "enabled": enabled,
            "error": error,
            "loaded_at_ms": loaded_at_ms,
            "horizons": {},
            "best": None,
        }
        if not enabled:
            return summary
        if error:
            summary["status"] = "degraded"
            return summary

        query_event = {
            "symbol": event.get("symbol"),
            "ts_event": event.get("ts_event"),
            "level_type": event.get("level_type"),
            "gamma_mode": event.get("gamma_mode"),
            "tod_bucket": features.get("tod_bucket"),
            "regime_bucket": trade_regime or _analog_regime_bucket(_to_int(event.get("regime_type"))),
        }
        vwap_dist_bps = _to_float(features.get("vwap_dist_bps_calc"))
        if vwap_dist_bps is None:
            vwap_dist_bps = _to_float(event.get("vwap_dist_bps"))
        ema_state = _to_float(features.get("ema_state_calc"))
        if ema_state is None:
            ema_state = _to_float(event.get("ema_state"))
        query_features = {
            "distance_bps": _to_float(event.get("distance_bps")),
            "distance_atr_ratio": _to_float(features.get("distance_atr_ratio")),
            "vwap_side": _sign3(vwap_dist_bps),
            "ema_stack": _sign3(ema_state),
            "rv_30": _to_float(event.get("rv_30")),
            "or_size_atr": _to_float(event.get("or_size_atr")),
            "overnight_gap_atr": _to_float(event.get("overnight_gap_atr")),
        }

        horizons_out: dict[str, object] = {}
        disagreement_max = None
        for horizon in sorted(set(horizons)):
            model_reject = _to_float(scores.get(f"prob_reject_{horizon}m"))
            model_break = _to_float(scores.get(f"prob_break_{horizon}m"))
            result = self._score_horizon(
                rows=rows_by_horizon.get(horizon, []),
                query_event=query_event,
                query_features=query_features,
                model_reject_prob=model_reject,
                model_break_prob=model_break,
            )
            horizons_out[str(horizon)] = result
            disagreement = _to_float(result.get("disagreement"))
            if disagreement is not None:
                disagreement_max = (
                    disagreement
                    if disagreement_max is None
                    else max(disagreement_max, disagreement)
                )

        summary["horizons"] = horizons_out
        summary["disagreement_max"] = disagreement_max
        if best_horizon is not None:
            summary["best"] = horizons_out.get(str(best_horizon))
            summary["best_horizon"] = best_horizon
        return summary


registry = ModelRegistry()
analog_engine = AnalogEngine(ML_ANALOG_DB)
_startup_error: Optional[str] = None
_RELOAD_LOCK = threading.Lock()
_RELOAD_STATE_LOCK = threading.Lock()
_reload_state: dict[str, object] = {
    "in_progress": False,
    "min_interval_sec": ML_RELOAD_MIN_INTERVAL_SEC,
    "last_started_at_ms": None,
    "last_completed_at_ms": None,
    "last_duration_ms": None,
    "last_status": "never",
    "last_error": None,
    "success_count": 0,
    "noop_count": 0,
    "failure_count": 0,
    "busy_reject_count": 0,
    "cooldown_reject_count": 0,
}
_SCORE_GATE = threading.BoundedSemaphore(ML_SCORE_MAX_IN_FLIGHT)
_SCORE_STATE_LOCK = threading.Lock()
_SCORE_LOAD_SHED_LOCAL = threading.local()
_score_state: dict[str, object] = {
    "max_in_flight": ML_SCORE_MAX_IN_FLIGHT,
    "in_flight": 0,
    "max_observed_in_flight": 0,
    "last_started_at_ms": None,
    "last_completed_at_ms": None,
    "last_duration_ms": None,
    "last_status": "never",
    "last_error": None,
    "success_count": 0,
    "failure_count": 0,
    "busy_reject_count": 0,
}
_PREDICTION_LOG_QUEUE: queue.Queue[tuple[dict, dict]] = queue.Queue(
    maxsize=PREDICTION_LOG_QUEUE_MAX_SIZE
)
_PREDICTION_LOG_WRITER_STOP = threading.Event()
_PREDICTION_LOG_WRITER_THREAD: threading.Thread | None = None
_PREDICTION_LOG_STATE_LOCK = threading.Lock()
_PREDICTION_LOG_CONTENTION_WARN_LOCK = threading.Lock()
_PREDICTION_LOG_CONTENTION_WARN_LAST_AT = 0.0
_PREDICTION_LOG_CONTENTION_WARN_SUPPRESSED = 0
_prediction_log_state: dict[str, object] = {
    "queue_max_size": PREDICTION_LOG_QUEUE_MAX_SIZE,
    "queue_high_watermark": 0,
    "queued_total": 0,
    "dropped_total": 0,
    "written_total": 0,
    "write_skip_total": 0,
    "write_fail_total": 0,
    "last_enqueued_at_ms": None,
    "last_written_at_ms": None,
    "last_error": None,
    "writer_started_at_ms": None,
    "writer_stopped_at_ms": None,
}


def _update_reload_state(**fields: object) -> None:
    with _RELOAD_STATE_LOCK:
        _reload_state.update(fields)


def _reload_state_snapshot() -> dict[str, object]:
    with _RELOAD_STATE_LOCK:
        return dict(_reload_state)


def _update_score_state(**fields: object) -> None:
    with _SCORE_STATE_LOCK:
        _score_state.update(fields)


def _score_state_snapshot() -> dict[str, object]:
    with _SCORE_STATE_LOCK:
        return dict(_score_state)


def _update_prediction_log_state(**fields: object) -> None:
    with _PREDICTION_LOG_STATE_LOCK:
        _prediction_log_state.update(fields)


def _prediction_log_state_snapshot() -> dict[str, object]:
    with _PREDICTION_LOG_STATE_LOCK:
        snapshot = dict(_prediction_log_state)
    snapshot["queue_depth"] = _PREDICTION_LOG_QUEUE.qsize()
    writer = _PREDICTION_LOG_WRITER_THREAD
    snapshot["writer_alive"] = bool(writer and writer.is_alive())
    return snapshot


def _warn_prediction_log_contention(exc: Exception) -> None:
    """Emit throttled warning logs for SQLite lock contention."""
    global _PREDICTION_LOG_CONTENTION_WARN_LAST_AT
    global _PREDICTION_LOG_CONTENTION_WARN_SUPPRESSED

    now = time.time()
    with _PREDICTION_LOG_CONTENTION_WARN_LOCK:
        elapsed = now - _PREDICTION_LOG_CONTENTION_WARN_LAST_AT
        if elapsed < PREDICTION_LOG_LOCK_WARN_INTERVAL_SEC:
            _PREDICTION_LOG_CONTENTION_WARN_SUPPRESSED += 1
            return
        suppressed = _PREDICTION_LOG_CONTENTION_WARN_SUPPRESSED
        _PREDICTION_LOG_CONTENTION_WARN_SUPPRESSED = 0
        _PREDICTION_LOG_CONTENTION_WARN_LAST_AT = now

    if suppressed > 0:
        log.warning(
            "Prediction log skipped due SQLite contention: %s (suppressed %d similar events)",
            exc,
            suppressed,
        )
    else:
        log.warning("Prediction log skipped due SQLite contention: %s", exc)


def _try_begin_score_request() -> bool:
    if not _SCORE_GATE.acquire(blocking=False):
        state_before = _score_state_snapshot()
        _update_score_state(
            busy_reject_count=int(state_before.get("busy_reject_count", 0)) + 1,
            last_status="busy",
            last_error="concurrency limit reached",
        )
        return False

    with _SCORE_STATE_LOCK:
        in_flight = int(_score_state.get("in_flight", 0)) + 1
        max_observed = max(in_flight, int(_score_state.get("max_observed_in_flight", 0)))
        _score_state.update(
            {
                "in_flight": in_flight,
                "max_observed_in_flight": max_observed,
                "last_started_at_ms": int(time.time() * 1000),
                "last_status": "running",
                "last_error": None,
            }
        )
    return True


def _finish_score_request(*, ok: bool, duration_ms: float, error: str | None = None) -> None:
    complete_ms = int(time.time() * 1000)
    with _SCORE_STATE_LOCK:
        in_flight = max(0, int(_score_state.get("in_flight", 0)) - 1)
        _score_state["in_flight"] = in_flight
        _score_state["last_completed_at_ms"] = complete_ms
        _score_state["last_duration_ms"] = round(duration_ms, 3)
        if ok:
            _score_state["last_status"] = "ok"
            _score_state["last_error"] = None
            _score_state["success_count"] = int(_score_state.get("success_count", 0)) + 1
        else:
            _score_state["last_status"] = "error"
            _score_state["last_error"] = error
            _score_state["failure_count"] = int(_score_state.get("failure_count", 0)) + 1
    _SCORE_GATE.release()


@asynccontextmanager
async def lifespan(_app):
    global _startup_error
    _start_prediction_log_writer()
    try:
        registry.load()
        analog_engine.refresh()
        _startup_error = None
    except Exception as exc:
        _startup_error = str(exc)
        print(f"ML server startup warning: {exc}")
    try:
        yield
    finally:
        _stop_prediction_log_writer()


app = FastAPI(title="PivotQuant ML Server", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _is_model_stale() -> bool:
    """Check if the model artifacts are older than STALE_MODEL_HOURS."""
    snapshot = registry.snapshot()
    manifest = snapshot.get("manifest")
    if not isinstance(manifest, dict):
        return False
    trained_end_ts = manifest.get("trained_end_ts")
    if trained_end_ts is None:
        return False
    try:
        trained_end_ts = float(trained_end_ts)
    except (TypeError, ValueError):
        return False
    if trained_end_ts <= 0:
        return False
    age_hours = (time.time() * 1000 - trained_end_ts) / (3600 * 1000)
    return age_hours > STALE_MODEL_HOURS


def _read_analog_promotion_gate() -> dict[str, object]:
    path = ML_ANALOG_PROMOTION_GATE_PATH
    if not path.exists():
        return {"status": "unknown", "path": str(path), "reason": "missing"}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive
        return {"status": "unknown", "path": str(path), "reason": f"parse_error:{exc}"}
    if not isinstance(payload, dict):
        return {"status": "unknown", "path": str(path), "reason": "invalid_payload"}
    horizon_results = payload.get("horizon_results")
    if not isinstance(horizon_results, dict):
        horizon_results = {}
    return {
        "status": str(payload.get("status") or "unknown"),
        "path": str(path),
        "report_date": payload.get("report_date"),
        "required_horizons": payload.get("required_horizons"),
        "evaluated_horizons": payload.get("evaluated_horizons"),
        "passed_horizons": payload.get("passed_horizons"),
        "horizon_results": horizon_results,
        "reasons": payload.get("reasons"),
        "generated_at_ms": payload.get("generated_at_ms"),
    }


def _compute_analog_blend_weight(
    *,
    n_eff: float | None,
    ci_width: float | None,
) -> float:
    if n_eff is None or n_eff <= 0:
        return 0.0
    eff_scale = min(1.0, float(n_eff) / ML_ANALOG_BLEND_N_EFF_REF)
    if ci_width is None:
        ci_scale = 1.0
    elif ML_ANALOG_MAX_CI_WIDTH <= 0:
        ci_scale = 1.0
    else:
        ci_scale = max(0.0, 1.0 - float(ci_width) / ML_ANALOG_MAX_CI_WIDTH)
    blend_scale = eff_scale * ci_scale
    weight = ML_ANALOG_BLEND_WEIGHT_BASE + (
        ML_ANALOG_BLEND_WEIGHT_MAX - ML_ANALOG_BLEND_WEIGHT_BASE
    ) * blend_scale
    return max(0.0, min(ML_ANALOG_BLEND_WEIGHT_MAX, weight))


def _apply_blend_shift_cap(
    *,
    model_prob: float | None,
    blended_prob: float | None,
    horizon: int,
    target: str,
) -> tuple[float | None, bool, float | None]:
    if model_prob is None or blended_prob is None:
        return blended_prob, False, None
    if target == "reject":
        cap = ML_ANALOG_BLEND_MAX_SHIFT_REJECT_BY_HORIZON.get(
            horizon, ML_ANALOG_BLEND_MAX_SHIFT_REJECT
        )
    else:
        cap = ML_ANALOG_BLEND_MAX_SHIFT_BREAK_BY_HORIZON.get(
            horizon, ML_ANALOG_BLEND_MAX_SHIFT_BREAK
        )
    cap = max(0.0, min(1.0, float(cap)))
    shift = float(blended_prob) - float(model_prob)
    if abs(shift) <= cap:
        return blended_prob, False, shift
    clipped = float(model_prob) + math.copysign(cap, shift)
    clipped = max(0.0, min(1.0, clipped))
    return clipped, True, clipped - float(model_prob)


def _get_prediction_log_conn() -> sqlite3.Connection:
    conn = getattr(_PREDICTION_LOG_LOCAL, "conn", None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except sqlite3.Error:
            try:
                conn.close()
            except sqlite3.Error:
                pass
            _PREDICTION_LOG_LOCAL.conn = None

    conn = sqlite3.connect(
        str(PREDICTION_LOG_DB),
        timeout=PREDICTION_LOG_CONNECT_TIMEOUT_SEC,
    )
    conn.execute(f"PRAGMA busy_timeout={int(PREDICTION_LOG_BUSY_TIMEOUT_MS)};")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(f"PRAGMA synchronous={PREDICTION_LOG_SQLITE_SYNC};")
    conn.execute(f"PRAGMA wal_autocheckpoint={PREDICTION_LOG_WAL_AUTOCHECKPOINT};")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")
    _PREDICTION_LOG_LOCAL.conn = conn
    return conn


def _close_prediction_log_conn() -> None:
    conn = getattr(_PREDICTION_LOG_LOCAL, "conn", None)
    if conn is None:
        return
    try:
        conn.close()
    except sqlite3.Error:
        pass
    _PREDICTION_LOG_LOCAL.conn = None


atexit.register(_close_prediction_log_conn)


def _ensure_prediction_log_schema(conn: sqlite3.Connection) -> None:
    global _PREDICTION_LOG_SCHEMA_READY
    if _PREDICTION_LOG_SCHEMA_READY:
        return

    with _PREDICTION_LOG_SCHEMA_LOCK:
        if _PREDICTION_LOG_SCHEMA_READY:
            return
        conn.execute(
            """CREATE TABLE IF NOT EXISTS prediction_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                ts_prediction INTEGER NOT NULL,
                model_version TEXT, feature_version TEXT,
                best_horizon INTEGER, abstain INTEGER NOT NULL DEFAULT 0,
                signal_5m TEXT, signal_15m TEXT, signal_30m TEXT, signal_60m TEXT,
                prob_reject_5m REAL, prob_reject_15m REAL, prob_reject_30m REAL, prob_reject_60m REAL,
                prob_break_5m REAL, prob_break_15m REAL, prob_break_30m REAL, prob_break_60m REAL,
                threshold_reject_5m REAL, threshold_reject_15m REAL, threshold_reject_30m REAL, threshold_reject_60m REAL,
                threshold_break_5m REAL, threshold_break_15m REAL, threshold_break_30m REAL, threshold_break_60m REAL,
                regime_policy_mode TEXT,
                trade_regime TEXT,
                selected_policy TEXT,
                regime_policy_json TEXT,
                analog_best_reject_prob REAL,
                analog_best_break_prob REAL,
                analog_best_n REAL,
                analog_best_ci_width REAL,
                analog_best_disagreement REAL,
                analog_json TEXT,
                quality_flags TEXT,
                is_preview INTEGER NOT NULL DEFAULT 0,
                UNIQUE(event_id, model_version)
            );"""
        )
        pred_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(prediction_log)").fetchall()
        }
        if "is_preview" not in pred_cols:
            conn.execute(
                "ALTER TABLE prediction_log "
                "ADD COLUMN is_preview INTEGER NOT NULL DEFAULT 0"
            )
            pred_cols.add("is_preview")
        compat_cols = {
            "signal_30m": "TEXT",
            "prob_reject_30m": "REAL",
            "prob_break_30m": "REAL",
            "threshold_reject_30m": "REAL",
            "threshold_break_30m": "REAL",
            "regime_policy_mode": "TEXT",
            "trade_regime": "TEXT",
            "selected_policy": "TEXT",
            "regime_policy_json": "TEXT",
            "analog_best_reject_prob": "REAL",
            "analog_best_break_prob": "REAL",
            "analog_best_n": "REAL",
            "analog_best_ci_width": "REAL",
            "analog_best_disagreement": "REAL",
            "analog_json": "TEXT",
        }
        for col_name, col_type in compat_cols.items():
            if col_name not in pred_cols:
                conn.execute(f"ALTER TABLE prediction_log ADD COLUMN {col_name} {col_type}")
        conn.commit()
        _PREDICTION_LOG_SCHEMA_READY = True


def _write_prediction_record(event: dict, result: dict) -> tuple[str, str | None]:
    """Write one prediction record to SQLite.

    Returns ("ok" | "skip" | "error", error_message).
    """
    event_id = event.get("event_id")
    if not event_id:
        return "skip", "missing event_id"

    conn = None
    try:
        conn = _get_prediction_log_conn()
        _ensure_prediction_log_schema(conn)

        scores = result.get("scores", {})
        signals = result.get("signals", {})
        thresholds = result.get("thresholds", {})
        regime_policy = result.get("regime_policy", {}) or {}
        regime_policy_mode = regime_policy.get("mode")
        trade_regime = regime_policy.get("trade_regime")
        selected_policy = regime_policy.get("selected_policy")
        analogs = result.get("analogs", {}) or {}
        analog_best = analogs.get("best", {}) if isinstance(analogs, dict) else {}
        analog_best_ci_width = None
        if isinstance(analog_best, dict):
            reject_w = _to_float(analog_best.get("reject_ci_width"))
            break_w = _to_float(analog_best.get("break_ci_width"))
            if reject_w is not None and break_w is not None:
                analog_best_ci_width = max(reject_w, break_w)
            else:
                analog_best_ci_width = reject_w if reject_w is not None else break_w
        is_preview = 1 if event.get("preview") else 0

        # Preserve original signal/probability/threshold payload for each
        # (event_id, model_version) row and only refresh runtime metadata on
        # conflict. This keeps first-prediction signal history stable while
        # allowing later rescoring metadata (policy/analogs/quality flags) to
        # be updated.
        conn.execute(
            """INSERT INTO prediction_log (
                event_id, ts_prediction, model_version, feature_version,
                best_horizon, abstain,
                signal_5m, signal_15m, signal_30m, signal_60m,
                prob_reject_5m, prob_reject_15m, prob_reject_30m, prob_reject_60m,
                prob_break_5m, prob_break_15m, prob_break_30m, prob_break_60m,
                threshold_reject_5m, threshold_reject_15m, threshold_reject_30m, threshold_reject_60m,
                threshold_break_5m, threshold_break_15m, threshold_break_30m, threshold_break_60m,
                regime_policy_mode, trade_regime, selected_policy, regime_policy_json,
                analog_best_reject_prob, analog_best_break_prob, analog_best_n,
                analog_best_ci_width, analog_best_disagreement, analog_json,
                quality_flags, is_preview
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(event_id, model_version) DO UPDATE SET
                regime_policy_mode = excluded.regime_policy_mode,
                trade_regime = excluded.trade_regime,
                selected_policy = excluded.selected_policy,
                regime_policy_json = excluded.regime_policy_json,
                analog_best_reject_prob = excluded.analog_best_reject_prob,
                analog_best_break_prob = excluded.analog_best_break_prob,
                analog_best_n = excluded.analog_best_n,
                analog_best_ci_width = excluded.analog_best_ci_width,
                analog_best_disagreement = excluded.analog_best_disagreement,
                analog_json = excluded.analog_json,
                quality_flags = excluded.quality_flags,
                is_preview = excluded.is_preview""",
            (
                event_id,
                int(time.time() * 1000),
                result.get("model_version"),
                result.get("feature_version"),
                result.get("best_horizon"),
                1 if result.get("abstain") else 0,
                signals.get("signal_5m"),
                signals.get("signal_15m"),
                signals.get("signal_30m"),
                signals.get("signal_60m"),
                scores.get("prob_reject_5m"),
                scores.get("prob_reject_15m"),
                scores.get("prob_reject_30m"),
                scores.get("prob_reject_60m"),
                scores.get("prob_break_5m"),
                scores.get("prob_break_15m"),
                scores.get("prob_break_30m"),
                scores.get("prob_break_60m"),
                thresholds.get("threshold_reject_5m"),
                thresholds.get("threshold_reject_15m"),
                thresholds.get("threshold_reject_30m"),
                thresholds.get("threshold_reject_60m"),
                thresholds.get("threshold_break_5m"),
                thresholds.get("threshold_break_15m"),
                thresholds.get("threshold_break_30m"),
                thresholds.get("threshold_break_60m"),
                regime_policy_mode,
                trade_regime,
                selected_policy,
                json.dumps(regime_policy, separators=(",", ":")) if regime_policy else None,
                _to_float(analog_best.get("reject_prob") if isinstance(analog_best, dict) else None),
                _to_float(analog_best.get("break_prob") if isinstance(analog_best, dict) else None),
                _to_float(analog_best.get("n") if isinstance(analog_best, dict) else None),
                analog_best_ci_width,
                _to_float(analog_best.get("disagreement") if isinstance(analog_best, dict) else None),
                json.dumps(analogs, separators=(",", ":")) if analogs else None,
                json.dumps(result.get("quality_flags", [])),
                is_preview,
            ),
        )
        conn.commit()
        return "ok", None
    except Exception as exc:
        if isinstance(exc, sqlite3.OperationalError):
            lowered = str(exc).lower()
            if "database is locked" in lowered or "database is busy" in lowered:
                _warn_prediction_log_contention(exc)
                return "skip", str(exc)
        if conn is not None and getattr(_PREDICTION_LOG_LOCAL, "conn", None) is conn:
            try:
                conn.close()
            except sqlite3.Error:
                pass
            _PREDICTION_LOG_LOCAL.conn = None
        log.warning("Prediction log write failed: %s", exc)
        return "error", f"{type(exc).__name__}: {exc}"


def _prediction_log_writer_loop() -> None:
    try:
        while True:
            if _PREDICTION_LOG_WRITER_STOP.is_set() and _PREDICTION_LOG_QUEUE.empty():
                return
            try:
                event, result = _PREDICTION_LOG_QUEUE.get(timeout=0.2)
            except queue.Empty:
                continue
            status, error = _write_prediction_record(event, result)
            now_ms = int(time.time() * 1000)
            state_before = _prediction_log_state_snapshot()
            if status == "ok":
                _update_prediction_log_state(
                    written_total=int(state_before.get("written_total", 0)) + 1,
                    last_written_at_ms=now_ms,
                    last_error=None,
                )
            elif status == "skip":
                _update_prediction_log_state(
                    write_skip_total=int(state_before.get("write_skip_total", 0)) + 1,
                    last_written_at_ms=now_ms,
                    last_error=error,
                )
            else:
                _update_prediction_log_state(
                    write_fail_total=int(state_before.get("write_fail_total", 0)) + 1,
                    last_written_at_ms=now_ms,
                    last_error=error,
                )
            _PREDICTION_LOG_QUEUE.task_done()
    finally:
        _close_prediction_log_conn()


def _start_prediction_log_writer() -> None:
    global _PREDICTION_LOG_WRITER_THREAD
    writer = _PREDICTION_LOG_WRITER_THREAD
    if writer is not None and writer.is_alive():
        return
    _PREDICTION_LOG_WRITER_STOP.clear()
    writer = threading.Thread(
        target=_prediction_log_writer_loop,
        name="prediction_log_writer",
        daemon=True,
    )
    _PREDICTION_LOG_WRITER_THREAD = writer
    _update_prediction_log_state(
        writer_started_at_ms=int(time.time() * 1000),
        writer_stopped_at_ms=None,
        last_error=None,
    )
    writer.start()


def _stop_prediction_log_writer(timeout_sec: float = 5.0) -> None:
    global _PREDICTION_LOG_WRITER_THREAD
    _PREDICTION_LOG_WRITER_STOP.set()
    deadline = time.monotonic() + max(0.1, timeout_sec)
    while time.monotonic() < deadline:
        if getattr(_PREDICTION_LOG_QUEUE, "unfinished_tasks", 0) <= 0:
            break
        time.sleep(0.05)
    writer = _PREDICTION_LOG_WRITER_THREAD
    if writer is not None:
        remaining = max(0.1, deadline - time.monotonic())
        writer.join(timeout=remaining)
    remaining_tasks = int(getattr(_PREDICTION_LOG_QUEUE, "unfinished_tasks", 0))
    _PREDICTION_LOG_WRITER_THREAD = None
    updates = {"writer_stopped_at_ms": int(time.time() * 1000)}
    if remaining_tasks > 0:
        updates["last_error"] = f"prediction log flush timeout (remaining={remaining_tasks})"
    _update_prediction_log_state(**updates)


atexit.register(_stop_prediction_log_writer)


def _enqueue_prediction(event: dict, result: dict) -> None:
    event_id = event.get("event_id")
    if not event_id:
        return
    try:
        _PREDICTION_LOG_QUEUE.put_nowait((event, result))
    except queue.Full:
        state_before = _prediction_log_state_snapshot()
        _update_prediction_log_state(
            dropped_total=int(state_before.get("dropped_total", 0)) + 1,
            last_error="prediction log queue full",
        )
        return

    queue_depth = _PREDICTION_LOG_QUEUE.qsize()
    state_before = _prediction_log_state_snapshot()
    _update_prediction_log_state(
        queued_total=int(state_before.get("queued_total", 0)) + 1,
        queue_high_watermark=max(
            int(state_before.get("queue_high_watermark", 0)),
            queue_depth,
        ),
        last_enqueued_at_ms=int(time.time() * 1000),
    )


def _log_prediction(event: dict, result: dict) -> None:
    """Compatibility path: write prediction record synchronously."""
    _write_prediction_record(event, result)


@app.get("/health")
async def health():
    registry_snapshot = registry.snapshot()
    manifest = registry_snapshot.get("manifest")
    if not isinstance(manifest, dict):
        manifest = None
    manifest_path = registry_snapshot.get("manifest_path")
    if manifest_path is not None:
        manifest_path = str(manifest_path)
    models_payload = registry_snapshot.get("models")
    if not isinstance(models_payload, dict):
        models_payload = {}
    models = {
        target: sorted(horizons.keys())
        for target, horizons in models_payload.items()
        if isinstance(horizons, dict)
    }
    has_models = any(horizons for horizons in models.values())
    stale = _is_model_stale()
    if not has_models or _startup_error is not None:
        status = "degraded"
    elif stale:
        status = "stale"
    else:
        status = "ok"
    prediction_log_state = _prediction_log_state_snapshot()
    queue_depth = int(prediction_log_state.get("queue_depth", 0) or 0)
    dropped_total = int(prediction_log_state.get("dropped_total", 0) or 0)
    write_fail_total = int(prediction_log_state.get("write_fail_total", 0) or 0)
    prediction_log_alerts = {
        "queue_depth": queue_depth,
        "queue_depth_limit": PREDICTION_LOG_ALERT_QUEUE_DEPTH,
        "queue_depth_exceeded": queue_depth > PREDICTION_LOG_ALERT_QUEUE_DEPTH,
        "dropped_total": dropped_total,
        "dropped_total_limit": PREDICTION_LOG_ALERT_DROPPED_TOTAL,
        "dropped_total_exceeded": dropped_total > PREDICTION_LOG_ALERT_DROPPED_TOTAL,
        "write_fail_total": write_fail_total,
        "write_fail_total_limit": PREDICTION_LOG_ALERT_WRITE_FAIL_TOTAL,
        "write_fail_total_exceeded": write_fail_total > PREDICTION_LOG_ALERT_WRITE_FAIL_TOTAL,
    }
    prediction_log_alerts["ok"] = not any(
        bool(prediction_log_alerts[key])
        for key in (
            "queue_depth_exceeded",
            "dropped_total_exceeded",
            "write_fail_total_exceeded",
        )
    )
    or_breakout_rules = _effective_reject_or_breakout_filter_rules()
    or_breakout_block_values = sorted({v for values in or_breakout_rules.values() for v in values})

    result = {
        "status": status,
        "feature_version": FEATURE_VERSION,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "models": models,
        "reload": _reload_state_snapshot(),
        "score": _score_state_snapshot(),
        "inference_n_jobs": ML_INFERENCE_N_JOBS,
        "prediction_log": prediction_log_state,
        "prediction_log_alerts": prediction_log_alerts,
        "regime_policy_mode": ML_REGIME_POLICY_MODE,
        "regime_threshold_max_delta": ML_REGIME_THRESHOLD_MAX_DELTA,
        "atr_zone_bounds": {
            "ultra_max": ML_ATR_ZONE_ULTRA_MAX,
            "near_max": ML_ATR_ZONE_NEAR_MAX,
            "mid_max": ML_ATR_ZONE_MID_MAX,
        },
        "feature_drift": {
            "min_features": ML_FEATURE_DRIFT_MIN_FEATURES,
            "ignore_columns": sorted(ML_FEATURE_DRIFT_IGNORE_COLUMNS),
        },
        "regime_guardrails": {
            "expansion_near": {
                "mode": ML_REGIME_GUARD_EXPANSION_NEAR_MODE,
                "strategy": ML_REGIME_GUARD_EXPANSION_NEAR_STRATEGY,
                "reject_delta": ML_REGIME_GUARD_EXPANSION_NEAR_REJECT_DELTA,
                "break_delta": ML_REGIME_GUARD_EXPANSION_NEAR_BREAK_DELTA,
            },
            "reject_or_breakout": {
                "mode": ML_REJECT_OR_BREAKOUT_FILTER_MODE,
                "horizons": sorted(or_breakout_rules),
                "block_values": or_breakout_block_values,
                "rules": _serialize_reject_or_breakout_filter_rules(or_breakout_rules),
            }
        },
        "analogs": {
            **analog_engine.health(),
            "promotion_gate": _read_analog_promotion_gate(),
            "min_features": ML_ANALOG_MIN_FEATURES,
            "min_feature_overlap": ML_ANALOG_MIN_FEATURE_OVERLAP,
            "min_feature_support": ML_ANALOG_MIN_FEATURE_SUPPORT,
            "prefilter_enabled": ML_ANALOG_PREFILTER_ENABLED,
            "prefilter_max_rows": ML_ANALOG_PREFILTER_MAX_ROWS,
            "prefilter_feature_limit": ML_ANALOG_PREFILTER_FEATURE_LIMIT,
            "max_mean_distance": ML_ANALOG_MAX_MEAN_DISTANCE,
            "max_ci_width": ML_ANALOG_MAX_CI_WIDTH,
            "recency_tau_days": ML_ANALOG_RECENCY_TAU_DAYS,
            "recency_floor": ML_ANALOG_RECENCY_FLOOR,
            "sim_weight_mode": ML_ANALOG_SIM_WEIGHT_MODE,
            "prior_strength": ML_ANALOG_PRIOR_STRENGTH,
            "disagreement_flag": ML_ANALOG_DISAGREEMENT_FLAG,
            "disagreement_guard_mode": ML_ANALOG_DISAGREEMENT_GUARD_MODE,
            "disagreement_guard_horizons": sorted(ML_ANALOG_DISAGREEMENT_GUARD_HORIZONS),
            "blend_mode": ML_ANALOG_BLEND_MODE,
            "blend_partial_mode": ML_ANALOG_BLEND_PARTIAL_MODE,
            "blend_weight_base": ML_ANALOG_BLEND_WEIGHT_BASE,
            "blend_weight_max": ML_ANALOG_BLEND_WEIGHT_MAX,
            "blend_n_eff_ref": ML_ANALOG_BLEND_N_EFF_REF,
            "blend_max_shift_reject": ML_ANALOG_BLEND_MAX_SHIFT_REJECT,
            "blend_max_shift_break": ML_ANALOG_BLEND_MAX_SHIFT_BREAK,
            "blend_max_shift_reject_by_horizon": ML_ANALOG_BLEND_MAX_SHIFT_REJECT_BY_HORIZON,
            "blend_max_shift_break_by_horizon": ML_ANALOG_BLEND_MAX_SHIFT_BREAK_BY_HORIZON,
            "feature_weights": ML_ANALOG_FEATURE_WEIGHTS,
        },
    }
    if _startup_error is not None:
        result["startup_error"] = _startup_error
    if stale and manifest is not None:
        trained_end_ts = manifest.get("trained_end_ts", 0)
        age_hours = (time.time() * 1000 - trained_end_ts) / (3600 * 1000)
        result["stale_hours"] = round(age_hours, 1)
    return result


@app.post("/reload")
async def reload_models(force: bool = False):
    """Hot-reload model artifacts from disk without restarting the server."""
    global _startup_error
    now_ms = int(time.time() * 1000)

    state_before = _reload_state_snapshot()
    last_started_ms = state_before.get("last_started_at_ms")
    if isinstance(last_started_ms, (int, float)) and ML_RELOAD_MIN_INTERVAL_SEC > 0:
        min_interval_ms = int(ML_RELOAD_MIN_INTERVAL_SEC * 1000)
        if now_ms - int(last_started_ms) < min_interval_ms:
            _update_reload_state(
                cooldown_reject_count=int(state_before.get("cooldown_reject_count", 0)) + 1,
            )
            return JSONResponse(
                status_code=429,
                content={
                    "status": "cooldown",
                    "message": "Reload requested too frequently.",
                    "reload": _reload_state_snapshot(),
                },
            )

    if not _RELOAD_LOCK.acquire(blocking=False):
        busy_before = _reload_state_snapshot()
        _update_reload_state(
            busy_reject_count=int(busy_before.get("busy_reject_count", 0)) + 1,
        )
        return JSONResponse(
            status_code=409,
            content={
                "status": "busy",
                "message": "Reload already in progress.",
                "reload": _reload_state_snapshot(),
            },
        )

    started_at = time.perf_counter()
    _update_reload_state(
        in_progress=True,
        last_started_at_ms=now_ms,
        last_status="running",
        last_error=None,
    )
    try:
        if not force and registry.is_manifest_unchanged():
            changed = False
        else:
            changed = await asyncio.to_thread(registry.load, force=force)
        if changed:
            await asyncio.to_thread(analog_engine.refresh)
        _startup_error = None
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        complete_ms = int(time.time() * 1000)
        ok_before = _reload_state_snapshot()
        status = "ok" if changed else "noop"
        _update_reload_state(
            in_progress=False,
            last_completed_at_ms=complete_ms,
            last_duration_ms=round(duration_ms, 3),
            last_status=status,
            last_error=None,
            success_count=int(ok_before.get("success_count", 0)) + 1,
            noop_count=int(ok_before.get("noop_count", 0)) + (0 if changed else 1),
        )
        registry_snapshot = registry.snapshot()
        manifest = registry_snapshot.get("manifest")
        if not isinstance(manifest, dict):
            manifest = None
        models_payload = registry_snapshot.get("models")
        if not isinstance(models_payload, dict):
            models_payload = {}
        models = {
            target: sorted(horizons.keys())
            for target, horizons in models_payload.items()
            if isinstance(horizons, dict)
        }
        return {
            "status": status,
            "changed": changed,
            "models": models,
            "manifest": manifest,
            "analogs": analog_engine.health(),
            "reload": _reload_state_snapshot(),
        }
    except Exception as exc:
        _startup_error = str(exc)
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        complete_ms = int(time.time() * 1000)
        err_before = _reload_state_snapshot()
        _update_reload_state(
            in_progress=False,
            last_completed_at_ms=complete_ms,
            last_duration_ms=round(duration_ms, 3),
            last_status="error",
            last_error=str(exc),
            failure_count=int(err_before.get("failure_count", 0)) + 1,
        )
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": str(exc),
                "reload": _reload_state_snapshot(),
            },
        )
    finally:
        _RELOAD_LOCK.release()


def _check_feature_drift(features: dict, payload: dict) -> list[str]:
    """Check if live feature values fall outside training-set p1/p99 bounds.

    Returns list of feature names that are out-of-range. An empty list
    means no drift detected. Only checks numeric features that have
    bounds stored in the model pickle.
    """
    bounds = payload.get("feature_bounds", {})
    if not bounds:
        return []
    drifted = []
    for col, limits in bounds.items():
        if col in ML_FEATURE_DRIFT_IGNORE_COLUMNS:
            continue
        value = features.get(col)
        if value is None or not isinstance(value, (int, float)):
            continue
        p1 = limits.get("p1")
        p99 = limits.get("p99")
        if p1 is not None and value < p1:
            drifted.append(col)
        elif p99 is not None and value > p99:
            drifted.append(col)
    return drifted


def _to_int(value) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _to_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _to_bool(value) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        norm = value.strip().lower()
        if norm in {"1", "true", "yes", "on"}:
            return True
        if norm in {"0", "false", "no", "off"}:
            return False
    return None


def _effective_reject_or_breakout_filter_rules() -> dict[int, set[int]]:
    rules: dict[int, set[int]] = {}
    if isinstance(ML_REJECT_OR_BREAKOUT_FILTER_RULES, dict) and ML_REJECT_OR_BREAKOUT_FILTER_RULES:
        for horizon_raw, values_raw in ML_REJECT_OR_BREAKOUT_FILTER_RULES.items():
            try:
                horizon = int(horizon_raw)
            except Exception:
                continue
            if horizon not in {5, 15, 30, 60}:
                continue
            values: set[int] = set()
            if isinstance(values_raw, (set, list, tuple)):
                iterable = values_raw
            else:
                iterable = [values_raw]
            for value_raw in iterable:
                try:
                    value = int(value_raw)
                except Exception:
                    continue
                if value in {-1, 0, 1}:
                    values.add(value)
            if values:
                rules[horizon] = values
        if rules:
            return rules
    fallback_values = {
        int(value)
        for value in ML_REJECT_OR_BREAKOUT_FILTER_BLOCK_VALUES
        if int(value) in {-1, 0, 1}
    }
    if not fallback_values:
        fallback_values = {-1}
    fallback_horizons = {
        int(horizon)
        for horizon in ML_REJECT_OR_BREAKOUT_FILTER_HORIZONS
        if int(horizon) in {5, 15, 30, 60}
    }
    if not fallback_horizons:
        fallback_horizons = {15}
    for horizon in sorted(fallback_horizons):
        rules[horizon] = set(fallback_values)
    return rules


def _serialize_reject_or_breakout_filter_rules(rules: dict[int, set[int]]) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {}
    for horizon in sorted(rules):
        values = sorted(int(v) for v in rules[horizon] if int(v) in {-1, 0, 1})
        if values:
            out[str(horizon)] = values
    return out


def _clamp_threshold(value: float) -> float:
    return max(0.01, min(0.99, float(value)))


def _compute_trade_regime(event: dict, features: dict) -> dict:
    """Compute a stable execution regime.

    Buckets:
      - compression: mean-reversion-friendly tape
      - expansion: trend/breakout-friendly tape
      - neutral: uncertain or mixed inputs (falls back to baseline policy)
    """
    regime_type = _to_int(event.get("regime_type"))
    if regime_type is None:
        regime_type = _to_int(features.get("regime_type"))
    rv_regime = _to_int(event.get("rv_regime"))
    if rv_regime is None:
        rv_regime = _to_int(features.get("rv_regime"))
    or_size_atr = _to_float(event.get("or_size_atr"))
    if or_size_atr is None:
        or_size_atr = _to_float(features.get("or_size_atr"))
    or_breakout = _to_int(event.get("or_breakout"))
    if or_breakout is None:
        or_breakout = _to_int(features.get("or_breakout"))
    overnight_gap_atr = _to_float(event.get("overnight_gap_atr"))
    if overnight_gap_atr is None:
        overnight_gap_atr = _to_float(features.get("overnight_gap_atr"))
    gamma_mode = _to_int(event.get("gamma_mode"))
    if gamma_mode is None:
        gamma_mode = _to_int(features.get("gamma_mode"))

    expansion_votes = 0
    compression_votes = 0
    drivers: list[str] = []

    if regime_type in (1, 2, 4):
        expansion_votes += 1
        drivers.append(f"regime_type={regime_type}")
    elif regime_type == 3:
        compression_votes += 1
        drivers.append("regime_type=range")

    if rv_regime == 3:
        expansion_votes += 1
        drivers.append("rv_regime=high")
    elif rv_regime == 1:
        compression_votes += 1
        drivers.append("rv_regime=low")

    if or_breakout in (-1, 1):
        expansion_votes += 1
        drivers.append(f"or_breakout={or_breakout}")

    if or_size_atr is not None:
        if or_size_atr >= 0.7:
            expansion_votes += 1
            drivers.append("or_size_atr>=0.7")
        elif or_size_atr <= 0.35:
            compression_votes += 1
            drivers.append("or_size_atr<=0.35")

    if overnight_gap_atr is not None and abs(overnight_gap_atr) >= 0.5:
        expansion_votes += 1
        drivers.append("|gap_atr|>=0.5")

    # Gamma is an enhancer only, never a required driver.
    if gamma_mode == -1:
        expansion_votes += 1
        drivers.append("gamma_mode=neg")
    elif gamma_mode == 1:
        compression_votes += 1
        drivers.append("gamma_mode=pos")

    if expansion_votes >= max(2, compression_votes + 1):
        bucket = "expansion"
    elif compression_votes >= max(2, expansion_votes + 1):
        bucket = "compression"
    else:
        bucket = "neutral"

    return {
        "bucket": bucket,
        "expansion_votes": expansion_votes,
        "compression_votes": compression_votes,
        "drivers": drivers,
    }


def _adjust_threshold(base: float, delta: float) -> float:
    safe_delta = max(-ML_REGIME_THRESHOLD_MAX_DELTA, min(ML_REGIME_THRESHOLD_MAX_DELTA, float(delta)))
    return _clamp_threshold(base + safe_delta)


def _compute_atr_distance_ratio(event: dict, features: dict) -> float | None:
    dist_ratio = _to_float(features.get("distance_atr_ratio"))
    if dist_ratio is not None:
        return abs(dist_ratio)

    atr_bps = _to_float(features.get("atr_bps"))
    if atr_bps is None:
        atr = _to_float(event.get("atr"))
        touch_price = _to_float(event.get("touch_price"))
        if atr is not None and atr > 0 and touch_price is not None and touch_price > 0:
            atr_bps = atr / touch_price * 1e4
    distance_bps = _to_float(event.get("distance_bps"))
    if distance_bps is None:
        distance_bps = _to_float(features.get("distance_bps"))
    if atr_bps is None or atr_bps <= 0 or distance_bps is None:
        return None
    return abs(distance_bps / atr_bps)


def _classify_atr_zone(distance_ratio: float | None) -> str:
    if distance_ratio is None:
        return "unknown"
    value = abs(float(distance_ratio))
    if value < ML_ATR_ZONE_ULTRA_MAX:
        return "ultra"
    if value < ML_ATR_ZONE_NEAR_MAX:
        return "near"
    if value < ML_ATR_ZONE_MID_MAX:
        return "mid"
    return "far"


def _build_regime_thresholds(
    all_horizons: list[int],
    trade_regime: str,
    baseline_thresholds: dict[str, dict[int, float]],
) -> dict[str, dict[int, float]]:
    thresholds = {"reject": {}, "break": {}}
    for horizon in all_horizons:
        base_reject = _clamp_threshold(
            _threshold_from_map(baseline_thresholds, "reject", horizon, context="baseline_thresholds")
        )
        base_break = _clamp_threshold(
            _threshold_from_map(baseline_thresholds, "break", horizon, context="baseline_thresholds")
        )
        if trade_regime == "compression":
            thresholds["reject"][horizon] = _adjust_threshold(
                base_reject, ML_REGIME_COMPRESSION_REJECT_DELTA
            )
            thresholds["break"][horizon] = _adjust_threshold(
                base_break, ML_REGIME_COMPRESSION_BREAK_DELTA
            )
        elif trade_regime == "expansion":
            thresholds["reject"][horizon] = _adjust_threshold(
                base_reject, ML_REGIME_EXPANSION_REJECT_DELTA
            )
            thresholds["break"][horizon] = _adjust_threshold(
                base_break, ML_REGIME_EXPANSION_BREAK_DELTA
            )
        else:
            thresholds["reject"][horizon] = _clamp_threshold(base_reject)
            thresholds["break"][horizon] = _clamp_threshold(base_break)
    return thresholds


def _atr_zone_threshold_deltas(trade_regime: str, atr_zone: str) -> tuple[float, float]:
    if trade_regime == "compression":
        if atr_zone == "ultra":
            return (
                ML_ATR_COMPRESSION_ULTRA_REJECT_DELTA,
                ML_ATR_COMPRESSION_ULTRA_BREAK_DELTA,
            )
        if atr_zone == "near":
            return (
                ML_ATR_COMPRESSION_NEAR_REJECT_DELTA,
                ML_ATR_COMPRESSION_NEAR_BREAK_DELTA,
            )
    elif trade_regime == "expansion":
        if atr_zone == "ultra":
            return (
                ML_ATR_EXPANSION_ULTRA_REJECT_DELTA,
                ML_ATR_EXPANSION_ULTRA_BREAK_DELTA,
            )
        if atr_zone == "near":
            return (
                ML_ATR_EXPANSION_NEAR_REJECT_DELTA,
                ML_ATR_EXPANSION_NEAR_BREAK_DELTA,
            )
    return 0.0, 0.0


def _apply_atr_zone_overlay(
    threshold_map: dict[str, dict[int, float]],
    all_horizons: list[int],
    trade_regime: str,
    atr_zone: str,
) -> tuple[dict[str, dict[int, float]], dict[str, float | str | bool]]:
    reject_delta, break_delta = _atr_zone_threshold_deltas(trade_regime, atr_zone)
    applied = (reject_delta != 0.0 or break_delta != 0.0) and atr_zone in {"ultra", "near"}
    if not applied:
        return threshold_map, {
            "applied": False,
            "atr_zone": atr_zone,
            "reject_delta": 0.0,
            "break_delta": 0.0,
        }

    overlaid = {
        "reject": {},
        "break": {},
    }
    for horizon in all_horizons:
        overlaid["reject"][horizon] = _adjust_threshold(
            _threshold_from_map(threshold_map, "reject", horizon, context="atr_zone_overlay"),
            reject_delta,
        )
        overlaid["break"][horizon] = _adjust_threshold(
            _threshold_from_map(threshold_map, "break", horizon, context="atr_zone_overlay"),
            break_delta,
        )
    return overlaid, {
        "applied": True,
        "atr_zone": atr_zone,
        "reject_delta": float(reject_delta),
        "break_delta": float(break_delta),
    }


def _apply_expansion_near_guardrail(
    threshold_map: dict[str, dict[int, float]],
    all_horizons: list[int],
    trade_regime: str,
    atr_zone: str,
) -> tuple[dict[str, dict[int, float]], dict[str, str | bool | float | None]]:
    triggered = (
        ML_REGIME_POLICY_MODE in {"shadow", "active"}
        and trade_regime == "expansion"
        and atr_zone == "near"
        and ML_REGIME_GUARD_EXPANSION_NEAR_MODE in {"shadow", "active"}
    )
    meta: dict[str, str | bool | float | None] = {
        "target": "expansion_near",
        "mode": ML_REGIME_GUARD_EXPANSION_NEAR_MODE,
        "strategy": ML_REGIME_GUARD_EXPANSION_NEAR_STRATEGY,
        "triggered": triggered,
        "applied": False,
        "reject_delta": None,
        "break_delta": None,
    }
    if not triggered:
        return threshold_map, meta

    guarded = {
        "reject": {},
        "break": {},
    }
    if ML_REGIME_GUARD_EXPANSION_NEAR_STRATEGY == "no_trade":
        for horizon in all_horizons:
            guarded["reject"][horizon] = 0.99
            guarded["break"][horizon] = 0.99
        return guarded, meta

    reject_delta = ML_REGIME_GUARD_EXPANSION_NEAR_REJECT_DELTA
    break_delta = ML_REGIME_GUARD_EXPANSION_NEAR_BREAK_DELTA
    meta["reject_delta"] = float(reject_delta)
    meta["break_delta"] = float(break_delta)
    for horizon in all_horizons:
        guarded["reject"][horizon] = _adjust_threshold(
            _threshold_from_map(threshold_map, "reject", horizon, context="expansion_near_guardrail"),
            reject_delta,
        )
        guarded["break"][horizon] = _adjust_threshold(
            _threshold_from_map(threshold_map, "break", horizon, context="expansion_near_guardrail"),
            break_delta,
        )
    return guarded, meta


def _classify_signals(
    all_horizons: list[int],
    scores: dict[str, float | None],
    threshold_map: dict[str, dict[int, float]],
) -> dict[str, str]:
    signals: dict[str, str] = {}
    for horizon in all_horizons:
        pr = scores.get(f"prob_reject_{horizon}m")
        pb = scores.get(f"prob_break_{horizon}m")
        if pr is None and pb is None:
            continue

        reject_thresh = _threshold_from_map(threshold_map, "reject", horizon, context="signal_classification")
        break_thresh = _threshold_from_map(threshold_map, "break", horizon, context="signal_classification")
        pr = pr if pr is not None else 0.0
        pb = pb if pb is not None else 0.0

        if pb >= break_thresh:
            signals[f"signal_{horizon}m"] = "break"
        elif pr >= reject_thresh:
            signals[f"signal_{horizon}m"] = "reject"
        else:
            signals[f"signal_{horizon}m"] = "no_edge"
    return signals


def _pick_best_horizon(
    scored_horizons: list[int],
    scores: dict[str, float | None],
    signals: dict[str, str],
    threshold_map: dict[str, dict[int, float]],
) -> tuple[int | None, bool]:
    best_horizon = None
    best_score = None
    best_signal_horizon = None
    best_signal_score = None
    for horizon in scored_horizons:
        pr = scores.get(f"prob_reject_{horizon}m")
        pb = scores.get(f"prob_break_{horizon}m")
        if pr is None and pb is None:
            continue
        pr = pr if pr is not None else 0.0
        pb = pb if pb is not None else 0.0

        signal = signals.get(f"signal_{horizon}m")
        reject_thresh = _threshold_from_map(
            threshold_map, "reject", horizon, context="shadow_signal_classification"
        )
        break_thresh = _threshold_from_map(
            threshold_map, "break", horizon, context="shadow_signal_classification"
        )

        if signal == "reject":
            signal_edge = pr - reject_thresh
        elif signal == "break":
            signal_edge = pb - break_thresh
        else:
            signal_edge = None

        # Fallback ranking used only when all horizons are no_edge.
        edge = max(pr - reject_thresh, pb - break_thresh)

        if best_score is None or edge > best_score:
            best_score = edge
            best_horizon = horizon

        if signal_edge is not None and (
            best_signal_score is None or signal_edge > best_signal_score
        ):
            best_signal_score = signal_edge
            best_signal_horizon = horizon

    has_signal = best_signal_horizon is not None
    if has_signal:
        return best_signal_horizon, False
    return best_horizon, True


def _score_event(event: dict):
    load_shed_analogs = bool(getattr(_SCORE_LOAD_SHED_LOCAL, "disable_analogs", False))
    missing = collect_missing(event)
    features = build_feature_row(event)
    registry_snapshot = registry.snapshot()
    manifest = registry_snapshot.get("manifest")
    if not isinstance(manifest, dict):
        manifest = None
    snapshot_models = registry_snapshot.get("models")
    if not isinstance(snapshot_models, dict):
        snapshot_models = {"reject": {}, "break": {}}
    snapshot_thresholds = registry_snapshot.get("thresholds")
    if not isinstance(snapshot_thresholds, dict):
        snapshot_thresholds = {"reject": {}, "break": {}}

    models_reject = snapshot_models.get("reject")
    if not isinstance(models_reject, dict):
        models_reject = {}
    models_break = snapshot_models.get("break")
    if not isinstance(models_break, dict):
        models_break = {}
    thresholds_reject = snapshot_thresholds.get("reject")
    if not isinstance(thresholds_reject, dict):
        thresholds_reject = {}
    thresholds_break = snapshot_thresholds.get("break")
    if not isinstance(thresholds_break, dict):
        thresholds_break = {}

    scores = {}
    quality_flags = []
    if missing:
        quality_flags.append("MISSING_FEATURES")
    if event.get("gamma_flip") is None:
        quality_flags.append("MISSING_GAMMA")
    if event.get("data_quality") is not None and event.get("data_quality") < 0.5:
        quality_flags.append("LOW_DATA_QUALITY")
    if _is_model_stale():
        quality_flags.append("STALE_MODEL")

    stats = manifest.get("stats", {}) if manifest else {}
    calibration = manifest.get("calibration", {}) if manifest else {}

    def extract_prob(model, df):
        probs = model.predict_proba(df)
        if probs.shape[1] == 2:
            return float(probs[:, 1][0])
        classes = getattr(model, "classes_", None)
        if classes is None and hasattr(model, "base_model"):
            classes = getattr(model.base_model, "classes_", None)
        if classes is not None and len(classes) == 1:
            return 1.0 if int(classes[0]) == 1 else 0.0
        return None

    thresholds_used = {}
    model_map = {"reject": models_reject, "break": models_break}
    threshold_map_live = {"reject": thresholds_reject, "break": thresholds_break}

    def _snapshot_threshold(target: str, horizon: int) -> float:
        return float(_threshold_from_map(threshold_map_live, target, horizon, context="snapshot"))

    for target in ("reject", "break"):
        for horizon, payload in model_map.get(target, {}).items():
            feature_cols = payload.get("feature_columns", [])
            if not feature_cols:
                continue
            row = {col: features.get(col) for col in feature_cols}
            df = pd.DataFrame([row])
            model = payload.get("calibrator") or payload.get("pipeline")
            if model is None:
                continue
            prob = extract_prob(model, df)
            scores[f"prob_{target}_{horizon}m"] = prob

            # Store the threshold used for this model
            threshold = _snapshot_threshold(target, horizon)
            thresholds_used[f"threshold_{target}_{horizon}m"] = threshold

            horizon_stats = stats.get(str(horizon), {}).get(target, {})
            for metric in ("mfe_bps", "mae_bps"):
                if f"{metric}_{target}" in horizon_stats:
                    scores[f"{metric}_{target}_{horizon}m"] = horizon_stats.get(f"{metric}_{target}")

            # ── Feature drift detection (#7) ──
            drifted = _check_feature_drift(features, payload)
            if drifted:
                scores[f"drifted_features_{target}_{horizon}m"] = drifted
            if drifted and len(drifted) >= ML_FEATURE_DRIFT_MIN_FEATURES:
                flag = f"FEATURE_DRIFT_{target}_{horizon}m"
                if flag not in quality_flags:
                    quality_flags.append(flag)

            # ── Uncalibrated model flag (#8) ──
            calib_method = payload.get("calibration", "none")
            if calib_method == "none":
                flag = f"UNCALIBRATED_{target}_{horizon}m"
                if flag not in quality_flags:
                    quality_flags.append(flag)

    # ── Signal classification per horizon ──
    # Uses optimal thresholds instead of hardcoded 0.5
    all_horizons = sorted(
        set(models_reject.keys())
        .union(models_break.keys())
    )
    scored_horizons = [h for h in all_horizons if h not in ML_SHADOW_HORIZONS]
    if not scored_horizons:
        scored_horizons = list(all_horizons)

    threshold_baseline = {
        "reject": {h: _clamp_threshold(_snapshot_threshold("reject", h)) for h in all_horizons},
        "break": {h: _clamp_threshold(_snapshot_threshold("break", h)) for h in all_horizons},
    }
    regime_state = _compute_trade_regime(event=event, features=features)
    atr_distance_ratio = _compute_atr_distance_ratio(event=event, features=features)
    atr_zone = _classify_atr_zone(atr_distance_ratio)
    threshold_regime_base = _build_regime_thresholds(
        all_horizons=all_horizons,
        trade_regime=regime_state["bucket"],
        baseline_thresholds=threshold_baseline,
    )
    threshold_regime, atr_overlay_meta = _apply_atr_zone_overlay(
        threshold_map=threshold_regime_base,
        all_horizons=all_horizons,
        trade_regime=regime_state["bucket"],
        atr_zone=atr_zone,
    )
    if load_shed_analogs:
        analog_gate = {
            "status": "load_shed",
            "path": str(ML_ANALOG_PROMOTION_GATE_PATH),
            "report_date": None,
            "passed_horizons": [],
            "horizon_results": {},
            "reasons": ["score_load_shed"],
        }
        analog_summary = {
            "enabled": bool(ML_ANALOG_ENABLED),
            "status": "load_shed",
            "error": None,
            "loaded_at_ms": None,
            "horizons": {},
            "best": None,
            "best_horizon": None,
        }
    else:
        analog_gate = _read_analog_promotion_gate()
        analog_summary = analog_engine.score_event(
            event=event,
            features=features,
            horizons=all_horizons,
            trade_regime=regime_state["bucket"],
            scores=scores,
            best_horizon=None,
        )
    gate_pass = str(analog_gate.get("status") or "").strip().lower() == "pass"
    all_horizons_set = set(all_horizons)
    gate_passed_horizons = {
        horizon
        for horizon in (
            _to_int(raw)
            for raw in (analog_gate.get("passed_horizons") if isinstance(analog_gate.get("passed_horizons"), list) else [])
        )
        if horizon in all_horizons_set
    }
    gate_horizon_results = analog_gate.get("horizon_results")
    if not isinstance(gate_horizon_results, dict):
        gate_horizon_results = {}
    blend_info: dict[str, object] = {
        "mode": ML_ANALOG_BLEND_MODE,
        "load_shed": bool(load_shed_analogs),
        "partial_mode": ML_ANALOG_BLEND_PARTIAL_MODE,
        "gate_status": analog_gate.get("status"),
        "gate_path": analog_gate.get("path"),
        "gate_report_date": analog_gate.get("report_date"),
        "gate_reasons": analog_gate.get("reasons"),
        "allow_active_blend": gate_pass,
        "allow_partial_blend": bool(
            ML_ANALOG_BLEND_PARTIAL_MODE != "off" and not gate_pass
        ),
        "gate_passed_horizons": sorted(gate_passed_horizons),
        "applied_horizons": [],
        "applied_targets": [],
        "horizons": {},
        "weight_base": ML_ANALOG_BLEND_WEIGHT_BASE,
        "weight_max": ML_ANALOG_BLEND_WEIGHT_MAX,
        "n_eff_ref": ML_ANALOG_BLEND_N_EFF_REF,
        "max_shift_reject": ML_ANALOG_BLEND_MAX_SHIFT_REJECT,
        "max_shift_break": ML_ANALOG_BLEND_MAX_SHIFT_BREAK,
        "max_shift_reject_by_horizon": ML_ANALOG_BLEND_MAX_SHIFT_REJECT_BY_HORIZON,
        "max_shift_break_by_horizon": ML_ANALOG_BLEND_MAX_SHIFT_BREAK_BY_HORIZON,
    }
    disagreement_guard_info: dict[str, object] = {
        "mode": ML_ANALOG_DISAGREEMENT_GUARD_MODE,
        "load_shed": bool(load_shed_analogs),
        "threshold": ML_ANALOG_DISAGREEMENT_FLAG,
        "horizons": sorted(ML_ANALOG_DISAGREEMENT_GUARD_HORIZONS),
        "triggered_horizons": [],
        "applied_horizons": [],
        "signal_diffs": {},
    }
    for horizon in all_horizons:
        analog_h = (
            analog_summary.get("horizons", {}).get(str(horizon), {})
            if isinstance(analog_summary, dict)
            else {}
        )
        model_reject = _to_float(scores.get(f"prob_reject_{horizon}m"))
        model_break = _to_float(scores.get(f"prob_break_{horizon}m"))
        analog_reject = _to_float(analog_h.get("reject_prob"))
        analog_break = _to_float(analog_h.get("break_prob"))
        n_eff = _to_float(analog_h.get("n_eff"))
        ci_width = max(
            _to_float(analog_h.get("reject_ci_width")) or 0.0,
            _to_float(analog_h.get("break_ci_width")) or 0.0,
        )
        weight = 0.0
        blend_reject = model_reject
        blend_break = model_break
        if (
            str(analog_h.get("status") or "").strip().lower() == "ok"
            and model_reject is not None
            and model_break is not None
            and analog_reject is not None
            and analog_break is not None
        ):
            weight = _compute_analog_blend_weight(n_eff=n_eff, ci_width=ci_width)
            blend_reject = (1.0 - weight) * model_reject + weight * analog_reject
            blend_break = (1.0 - weight) * model_break + weight * analog_break
        blend_reject, blend_reject_capped, blend_reject_shift = _apply_blend_shift_cap(
            model_prob=model_reject,
            blended_prob=blend_reject,
            horizon=horizon,
            target="reject",
        )
        blend_break, blend_break_capped, blend_break_shift = _apply_blend_shift_cap(
            model_prob=model_break,
            blended_prob=blend_break,
            horizon=horizon,
            target="break",
        )

        scores[f"analog_reject_{horizon}m"] = analog_reject
        scores[f"analog_break_{horizon}m"] = analog_break
        scores[f"analog_n_{horizon}m"] = _to_float(analog_h.get("n"))
        scores[f"analog_n_eff_{horizon}m"] = n_eff
        scores[f"analog_ci_width_{horizon}m"] = ci_width
        disagreement = _to_float(analog_h.get("disagreement"))
        scores[f"analog_disagreement_{horizon}m"] = disagreement
        disagreement_flagged = (
            disagreement is not None and disagreement >= ML_ANALOG_DISAGREEMENT_FLAG
        )
        guard_triggered = (
            disagreement_flagged
            and ML_ANALOG_DISAGREEMENT_GUARD_MODE != "off"
            and horizon in ML_ANALOG_DISAGREEMENT_GUARD_HORIZONS
        )
        scores[f"analog_disagreement_guard_{horizon}m"] = bool(guard_triggered)
        if guard_triggered:
            cast_triggered = disagreement_guard_info.get("triggered_horizons")
            if isinstance(cast_triggered, list):
                cast_triggered.append(horizon)
        if disagreement_flagged:
            flag = f"ANALOG_DISAGREE_{horizon}m"
            if flag not in quality_flags:
                quality_flags.append(flag)
        if blend_reject_capped or blend_break_capped:
            capped_flag = f"ANALOG_BLEND_SHIFT_CAPPED_{horizon}m"
            if capped_flag not in quality_flags:
                quality_flags.append(capped_flag)

        scores[f"blend_prob_reject_{horizon}m"] = blend_reject
        scores[f"blend_prob_break_{horizon}m"] = blend_break
        horizon_gate_payload = gate_horizon_results.get(str(horizon))
        if not isinstance(horizon_gate_payload, dict):
            horizon_gate_payload = {}
        horizon_pass_flag = _to_bool(horizon_gate_payload.get("pass"))
        if horizon_pass_flag is None:
            horizon_pass_flag = horizon in gate_passed_horizons
        reject_target_gate = _to_bool(horizon_gate_payload.get("reject_pass"))
        break_target_gate = _to_bool(horizon_gate_payload.get("break_pass"))
        if ML_ANALOG_BLEND_PARTIAL_MODE == "target":
            reject_blend_allowed = bool(
                gate_pass
                or reject_target_gate
                or (reject_target_gate is None and (horizon_pass_flag or horizon in gate_passed_horizons))
            )
            break_blend_allowed = bool(
                gate_pass
                or break_target_gate
                or (break_target_gate is None and (horizon_pass_flag or horizon in gate_passed_horizons))
            )
        elif ML_ANALOG_BLEND_PARTIAL_MODE == "horizon":
            horizon_blend_allowed = bool(gate_pass or horizon_pass_flag or horizon in gate_passed_horizons)
            reject_blend_allowed = horizon_blend_allowed
            break_blend_allowed = horizon_blend_allowed
        else:
            reject_blend_allowed = bool(gate_pass)
            break_blend_allowed = bool(gate_pass)

        blend_info["horizons"][str(horizon)] = {
            "weight": weight,
            "applied": False,
            "applied_reject": False,
            "applied_break": False,
            "gate_pass": bool(horizon_pass_flag),
            "gate_reject_pass": reject_target_gate,
            "gate_break_pass": break_target_gate,
            "allow_reject": bool(reject_blend_allowed),
            "allow_break": bool(break_blend_allowed),
            "model_reject": model_reject,
            "model_break": model_break,
            "analog_reject": analog_reject,
            "analog_break": analog_break,
            "blended_reject": blend_reject,
            "blended_break": blend_break,
            "blended_reject_shift": blend_reject_shift,
            "blended_break_shift": blend_break_shift,
            "blended_reject_capped": bool(blend_reject_capped),
            "blended_break_capped": bool(blend_break_capped),
            "disagreement_guard_triggered": bool(guard_triggered),
        }

        if ML_ANALOG_BLEND_MODE == "active" and weight > 0:
            applied_any = False
            if (
                reject_blend_allowed
                and blend_reject is not None
            ):
                scores[f"prob_reject_{horizon}m"] = blend_reject
                blend_info["horizons"][str(horizon)]["applied_reject"] = True
                applied_any = True
                cast_targets = blend_info.get("applied_targets")
                if isinstance(cast_targets, list):
                    cast_targets.append(f"{horizon}m:reject")
            if (
                break_blend_allowed
                and blend_break is not None
            ):
                scores[f"prob_break_{horizon}m"] = blend_break
                blend_info["horizons"][str(horizon)]["applied_break"] = True
                applied_any = True
                cast_targets = blend_info.get("applied_targets")
                if isinstance(cast_targets, list):
                    cast_targets.append(f"{horizon}m:break")
            if applied_any:
                blend_info["horizons"][str(horizon)]["applied"] = True
                cast_applied = blend_info.get("applied_horizons")
                if isinstance(cast_applied, list) and horizon not in cast_applied:
                    cast_applied.append(horizon)

        analog_horizons = analog_summary.get("horizons") if isinstance(analog_summary, dict) else None
        if isinstance(analog_horizons, dict):
            analog_horizon_payload = analog_horizons.get(str(horizon))
            if isinstance(analog_horizon_payload, dict):
                horizon_blend_payload = blend_info["horizons"].get(str(horizon), {})
                analog_horizon_payload["blend_weight"] = float(weight)
                analog_horizon_payload["blend_prob_reject"] = blend_reject
                analog_horizon_payload["blend_prob_break"] = blend_break
                analog_horizon_payload["blend_shift_reject"] = blend_reject_shift
                analog_horizon_payload["blend_shift_break"] = blend_break_shift
                analog_horizon_payload["blend_capped_reject"] = bool(blend_reject_capped)
                analog_horizon_payload["blend_capped_break"] = bool(blend_break_capped)
                analog_horizon_payload["blend_allow_reject"] = bool(
                    horizon_blend_payload.get("allow_reject")
                )
                analog_horizon_payload["blend_allow_break"] = bool(
                    horizon_blend_payload.get("allow_break")
                )
                analog_horizon_payload["blend_applied_reject"] = bool(
                    horizon_blend_payload.get("applied_reject")
                )
                analog_horizon_payload["blend_applied_break"] = bool(
                    horizon_blend_payload.get("applied_break")
                )
                analog_horizon_payload["blend_applied"] = bool(
                    horizon_blend_payload.get("applied")
                )
                analog_horizon_payload["disagreement_guard_triggered"] = bool(guard_triggered)

    applied_horizons = blend_info.get("applied_horizons")
    if (
        ML_ANALOG_BLEND_MODE == "active"
        and isinstance(applied_horizons, list)
        and not applied_horizons
        and not gate_pass
    ):
        quality_flags.append("ANALOG_BLEND_BLOCKED_GATE")
    if (
        ML_ANALOG_BLEND_MODE == "active"
        and isinstance(applied_horizons, list)
        and applied_horizons
    ):
        quality_flags.append("ANALOG_BLEND_ACTIVE")
        if not gate_pass and ML_ANALOG_BLEND_PARTIAL_MODE != "off":
            quality_flags.append("ANALOG_BLEND_PARTIAL_GATE")
    horizon_blend_payload = blend_info.get("horizons")
    if (
        ML_ANALOG_BLEND_MODE == "active"
        and isinstance(horizon_blend_payload, dict)
        and any(
            isinstance(payload, dict)
            and payload.get("applied_reject") != payload.get("applied_break")
            for payload in horizon_blend_payload.values()
        )
    ):
        quality_flags.append("ANALOG_BLEND_TARGET_SPLIT")
    if (
        isinstance(horizon_blend_payload, dict)
        and any(
            isinstance(payload, dict)
            and (
                bool(payload.get("blended_reject_capped"))
                or bool(payload.get("blended_break_capped"))
            )
            for payload in horizon_blend_payload.values()
        )
    ):
        quality_flags.append("ANALOG_BLEND_SHIFT_CAPPED")
    baseline_signals = _classify_signals(
        all_horizons=all_horizons,
        scores=scores,
        threshold_map=threshold_baseline,
    )
    regime_signals = _classify_signals(
        all_horizons=all_horizons,
        scores=scores,
        threshold_map=threshold_regime,
    )
    threshold_guardrail, guardrail_meta = _apply_expansion_near_guardrail(
        threshold_map=threshold_regime,
        all_horizons=all_horizons,
        trade_regime=regime_state["bucket"],
        atr_zone=atr_zone,
    )
    guardrail_signals = _classify_signals(
        all_horizons=all_horizons,
        scores=scores,
        threshold_map=threshold_guardrail,
    )
    # no_trade must be an absolute block, not a probabilistic threshold proxy.
    if bool(guardrail_meta.get("triggered")) and ML_REGIME_GUARD_EXPANSION_NEAR_STRATEGY == "no_trade":
        for horizon in all_horizons:
            key = f"signal_{horizon}m"
            if key in guardrail_signals:
                guardrail_signals[key] = "no_edge"

    selected_policy = "baseline"
    selected_threshold_map = threshold_baseline
    selected_signals = baseline_signals
    if ML_REGIME_POLICY_MODE == "active" and regime_state["bucket"] in {"compression", "expansion"}:
        selected_policy = "regime_active"
        selected_threshold_map = threshold_regime
        selected_signals = regime_signals

    if (
        bool(guardrail_meta.get("triggered"))
        and ML_REGIME_GUARD_EXPANSION_NEAR_MODE == "active"
        and ML_REGIME_POLICY_MODE == "active"
    ):
        selected_policy = f"guardrail_{ML_REGIME_GUARD_EXPANSION_NEAR_STRATEGY}"
        selected_threshold_map = threshold_guardrail
        selected_signals = guardrail_signals
        guardrail_meta["applied"] = True

    pre_disagreement_guard_signals = dict(selected_signals)
    triggered_horizons = disagreement_guard_info.get("triggered_horizons")
    if isinstance(triggered_horizons, list):
        available_horizons = set(all_horizons)
        normalized_triggered = {
            horizon
            for horizon in triggered_horizons
            if isinstance(horizon, int) and horizon in available_horizons
        }
        for horizon in sorted(normalized_triggered):
            key = f"signal_{horizon}m"
            before = pre_disagreement_guard_signals.get(key)
            after = before
            applied = False
            if (
                ML_ANALOG_DISAGREEMENT_GUARD_MODE == "active"
                and before is not None
                and before != "no_edge"
            ):
                selected_signals[key] = "no_edge"
                after = "no_edge"
                applied = True
                cast_applied = disagreement_guard_info.get("applied_horizons")
                if isinstance(cast_applied, list):
                    cast_applied.append(horizon)
            signal_diffs_payload = disagreement_guard_info.get("signal_diffs")
            if isinstance(signal_diffs_payload, dict):
                signal_diffs_payload[key] = {
                    "before": before,
                    "after": after,
                    "applied": applied,
                }
        signal_diffs_payload = disagreement_guard_info.get("signal_diffs")
        if isinstance(signal_diffs_payload, dict) and signal_diffs_payload:
            if (
                ML_ANALOG_DISAGREEMENT_GUARD_MODE == "shadow"
                and any(
                    payload.get("before") not in {None, "no_edge"}
                    for payload in signal_diffs_payload.values()
                    if isinstance(payload, dict)
                )
            ):
                quality_flags.append("ANALOG_DISAGREEMENT_GUARD_DIVERGENCE")
            if (
                ML_ANALOG_DISAGREEMENT_GUARD_MODE == "active"
                and any(
                    bool(payload.get("applied"))
                    for payload in signal_diffs_payload.values()
                    if isinstance(payload, dict)
                )
            ):
                quality_flags.append("ANALOG_DISAGREEMENT_GUARD_ACTIVE")
                selected_policy = f"{selected_policy}_analog_disagreement_guard"

    event_or_breakout = _to_int(event.get("or_breakout"))
    if event_or_breakout is None:
        event_or_breakout = _to_int(features.get("or_breakout"))
    or_breakout_rules = _effective_reject_or_breakout_filter_rules()
    blocked_values = sorted({v for values in or_breakout_rules.values() for v in values})
    or_breakout_filter_info = {
        "mode": ML_REJECT_OR_BREAKOUT_FILTER_MODE,
        "event_or_breakout": event_or_breakout,
        "horizons": sorted(or_breakout_rules),
        "block_values": blocked_values,
        "rules": _serialize_reject_or_breakout_filter_rules(or_breakout_rules),
        "candidate_count": 0,
        "applied_count": 0,
        "signal_diffs": {},
    }
    if (
        ML_REJECT_OR_BREAKOUT_FILTER_MODE in {"shadow", "active"}
        and event_or_breakout is not None
        and bool(or_breakout_rules)
    ):
        available_horizons = set(all_horizons)
        configured_horizons = {
            horizon
            for horizon in or_breakout_rules
            if horizon in available_horizons
        }
        for horizon in sorted(configured_horizons):
            horizon_block_values = or_breakout_rules.get(horizon, set())
            if event_or_breakout not in horizon_block_values:
                continue
            key = f"signal_{horizon}m"
            before = selected_signals.get(key)
            if before != "reject":
                continue
            after = before
            applied = False
            or_breakout_filter_info["candidate_count"] += 1
            if ML_REJECT_OR_BREAKOUT_FILTER_MODE == "active":
                selected_signals[key] = "no_edge"
                after = "no_edge"
                applied = True
                or_breakout_filter_info["applied_count"] += 1
            or_breakout_filter_info["signal_diffs"][key] = {
                "before": before,
                "after": after,
                "applied": applied,
                "or_breakout": event_or_breakout,
                "blocked_values": sorted(horizon_block_values),
            }
        candidate_count = int(or_breakout_filter_info.get("candidate_count", 0) or 0)
        if candidate_count > 0:
            if ML_REJECT_OR_BREAKOUT_FILTER_MODE == "shadow":
                quality_flags.append("OR_BREAKOUT_REJECT_FILTER_DIVERGENCE")
            elif ML_REJECT_OR_BREAKOUT_FILTER_MODE == "active":
                quality_flags.append("OR_BREAKOUT_REJECT_FILTER_ACTIVE")
                selected_policy = f"{selected_policy}_or_breakout_filter"

    signal_diffs = {
        f"signal_{h}m": {
            "baseline": baseline_signals.get(f"signal_{h}m"),
            "regime": regime_signals.get(f"signal_{h}m"),
            "guardrail": guardrail_signals.get(f"signal_{h}m"),
            "selected": selected_signals.get(f"signal_{h}m"),
        }
        for h in all_horizons
        if (
            baseline_signals.get(f"signal_{h}m") is not None
            or regime_signals.get(f"signal_{h}m") is not None
            or guardrail_signals.get(f"signal_{h}m") is not None
        )
    }
    if ML_REGIME_POLICY_MODE == "shadow":
        changed = [
            key
            for key, payload in signal_diffs.items()
            if payload.get("baseline") != payload.get("regime")
        ]
        if changed:
            quality_flags.append("REGIME_POLICY_DIVERGENCE")
    if bool(guardrail_meta.get("triggered")) and ML_REGIME_GUARD_EXPANSION_NEAR_MODE == "shadow":
        changed_guardrail = [
            key
            for key, payload in signal_diffs.items()
            if payload.get("selected") != payload.get("guardrail")
        ]
        if changed_guardrail:
            quality_flags.append("REGIME_GUARDRAIL_DIVERGENCE")

    signals = selected_signals

    # ── Expected MFE/MAE: signal-conditional ──
    # Reject and break are independent binary classifiers (not mutually exclusive),
    # so we use the classified signal to select the appropriate conditional stats
    # rather than a weighted mix that incorrectly assumes a shared probability space.
    for horizon in all_horizons:
        signal = signals.get(f"signal_{horizon}m")
        if signal is None:
            continue
        stats_h = stats.get(str(horizon), {})
        stats_reject = stats_h.get("reject", {})
        stats_break = stats_h.get("break", {})

        if signal == "break":
            mfe = stats_break.get("mfe_bps_break") or 0.0
            mae = stats_break.get("mae_bps_break") or 0.0
        elif signal == "reject":
            mfe = stats_reject.get("mfe_bps_reject") or 0.0
            mae = stats_reject.get("mae_bps_reject") or 0.0
        else:
            # no_edge: use reject-specific "other" bucket when available.
            # Fallback to legacy unscoped keys for older manifests.
            mfe = stats_reject.get("mfe_bps_reject_other")
            if mfe is None:
                mfe = stats_reject.get("mfe_bps_other")
            mae = stats_reject.get("mae_bps_reject_other")
            if mae is None:
                mae = stats_reject.get("mae_bps_other")
            mfe = mfe or 0.0
            mae = mae or 0.0

        scores[f"exp_mfe_bps_{horizon}m"] = float(mfe)
        scores[f"exp_mae_bps_{horizon}m"] = float(mae)

    # ── Best horizon selection (threshold-aware) ──
    best_horizon, abstain = _pick_best_horizon(
        scored_horizons=scored_horizons,
        scores=scores,
        signals=signals,
        threshold_map=selected_threshold_map,
    )

    baseline_best_horizon, baseline_abstain = _pick_best_horizon(
        scored_horizons=scored_horizons,
        scores=scores,
        signals=baseline_signals,
        threshold_map=threshold_baseline,
    )
    regime_best_horizon, regime_abstain = _pick_best_horizon(
        scored_horizons=scored_horizons,
        scores=scores,
        signals=regime_signals,
        threshold_map=threshold_regime,
    )
    guardrail_best_horizon, guardrail_abstain = _pick_best_horizon(
        scored_horizons=scored_horizons,
        scores=scores,
        signals=guardrail_signals,
        threshold_map=threshold_guardrail,
    )
    if isinstance(analog_summary, dict):
        analog_summary["best_horizon"] = best_horizon
        analog_summary["disagreement_guard"] = disagreement_guard_info
        horizons_payload = analog_summary.get("horizons")
        if isinstance(horizons_payload, dict) and best_horizon is not None:
            analog_summary["best"] = horizons_payload.get(str(best_horizon))

    if ML_ANALOG_BLEND_MODE == "shadow":
        diverged = False
        for horizon in all_horizons:
            model_reject = _to_float(scores.get(f"prob_reject_{horizon}m"))
            shadow_reject = _to_float(scores.get(f"blend_prob_reject_{horizon}m"))
            model_break = _to_float(scores.get(f"prob_break_{horizon}m"))
            shadow_break = _to_float(scores.get(f"blend_prob_break_{horizon}m"))
            if (
                model_reject is not None
                and shadow_reject is not None
                and abs(model_reject - shadow_reject) > 1e-9
            ) or (
                model_break is not None
                and shadow_break is not None
                and abs(model_break - shadow_break) > 1e-9
            ):
                diverged = True
                break
        if diverged:
            quality_flags.append("ANALOG_BLEND_DIVERGENCE")
    if load_shed_analogs:
        quality_flags.append("ANALOG_LOAD_SHED")

    for horizon in all_horizons:
        thresholds_used[f"threshold_reject_{horizon}m"] = _threshold_from_map(
            selected_threshold_map, "reject", horizon, context="response_payload"
        )
        thresholds_used[f"threshold_break_{horizon}m"] = _threshold_from_map(
            selected_threshold_map, "break", horizon, context="response_payload"
        )

    return {
        "status": "degraded" if missing else "ok",
        "scores": scores,
        "signals": signals,
        "thresholds": thresholds_used,
        "abstain": abstain,
        "best_horizon": best_horizon,
        "model_version": manifest.get("version") if manifest else None,
        "feature_version": FEATURE_VERSION,
        "trained_end_ts": manifest.get("trained_end_ts") if manifest else None,
        "calibration": calibration,
        "quality_flags": quality_flags,
        "analogs": analog_summary,
        "analog_blend": blend_info,
        "analog_disagreement_guard": disagreement_guard_info,
        "regime_policy": {
            "mode": ML_REGIME_POLICY_MODE,
            "selected_policy": selected_policy,
            "trade_regime": regime_state["bucket"],
            "drivers": regime_state["drivers"],
            "atr_zone": atr_zone,
            "atr_distance_ratio": atr_distance_ratio,
            "atr_overlay": atr_overlay_meta,
            "votes": {
                "expansion": regime_state["expansion_votes"],
                "compression": regime_state["compression_votes"],
            },
            "baseline": {
                "signals": baseline_signals,
                "best_horizon": baseline_best_horizon,
                "abstain": baseline_abstain,
            },
            "regime": {
                "signals": regime_signals,
                "best_horizon": regime_best_horizon,
                "abstain": regime_abstain,
            },
            "guardrail": {
                "signals": guardrail_signals,
                "best_horizon": guardrail_best_horizon,
                "abstain": guardrail_abstain,
                **guardrail_meta,
            },
            "or_breakout_reject_filter": or_breakout_filter_info,
            "signal_diffs": signal_diffs,
        },
    }


def _validate_score_payload(payload: object) -> tuple[str, dict | list[dict]]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object.")

    if "event" in payload:
        event = payload["event"]
        if not isinstance(event, dict):
            raise HTTPException(status_code=400, detail="'event' must be a JSON object.")
        return "single", event

    if "events" in payload:
        events = payload["events"]
        if not isinstance(events, list):
            raise HTTPException(status_code=400, detail="'events' must be a JSON array.")
        if len(events) > SCORE_MAX_BATCH_EVENTS:
            raise HTTPException(
                status_code=413,
                detail=f"Too many events in batch ({len(events)}). Max allowed: {SCORE_MAX_BATCH_EVENTS}.",
            )
        if any(not isinstance(ev, dict) for ev in events):
            raise HTTPException(status_code=400, detail="Each item in 'events' must be a JSON object.")
        return "batch", events

    raise HTTPException(status_code=400, detail="Payload must include 'event' or 'events'.")


def _parse_content_length_header(raw_value: str | None) -> int | None:
    if raw_value is None:
        return None
    header = raw_value.strip()
    if not header:
        return None
    try:
        parsed = int(header)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid Content-Length header.") from exc
    if parsed < 0:
        raise HTTPException(status_code=400, detail="Invalid Content-Length header.")
    return parsed


def _enforce_score_body_size(content_length: int | None, body_size: int) -> None:
    limit = SCORE_MAX_BODY_BYTES
    if content_length is not None and content_length > limit:
        raise HTTPException(
            status_code=413,
            detail=f"Payload too large ({content_length} bytes). Max allowed: {limit}.",
        )
    if body_size > limit:
        raise HTTPException(
            status_code=413,
            detail=f"Payload too large ({body_size} bytes). Max allowed: {limit}.",
        )


def _parse_score_json_body(raw_body: bytes) -> object:
    try:
        return json.loads(raw_body)
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload: body must be UTF-8.") from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {exc.msg}") from exc


async def _read_score_payload(request: Request) -> object:
    content_length = _parse_content_length_header(request.headers.get("content-length"))
    _enforce_score_body_size(content_length, 0)
    raw_body = await request.body()
    _enforce_score_body_size(content_length, len(raw_body))
    return _parse_score_json_body(raw_body)


@app.post("/score")
async def score(request: Request):
    registry_snapshot = registry.snapshot()
    manifest = registry_snapshot.get("manifest")
    if not isinstance(manifest, dict):
        manifest = None
    models_payload = registry_snapshot.get("models")
    if not isinstance(models_payload, dict):
        models_payload = {}
    models = {
        target: sorted(horizons.keys())
        for target, horizons in models_payload.items()
        if isinstance(horizons, dict)
    }
    has_models = any(horizons for horizons in models.values())
    if manifest is None or not has_models:
        raise HTTPException(status_code=503, detail="Models not loaded. Train artifacts first.")

    if not _try_begin_score_request():
        return JSONResponse(
            status_code=429,
            content={
                "status": "busy",
                "message": "Score concurrency limit reached.",
                "score": _score_state_snapshot(),
            },
        )

    started_at = time.perf_counter()
    request_ok = False
    request_error: str | None = None
    score_state = _score_state_snapshot()
    current_in_flight = int(score_state.get("in_flight", 0))
    disable_analogs = (
        ML_SCORE_ANALOG_DISABLE_IN_FLIGHT > 0
        and current_in_flight >= ML_SCORE_ANALOG_DISABLE_IN_FLIGHT
    )
    try:
        payload = await _read_score_payload(request)
        mode, normalized = _validate_score_payload(payload)

        if mode == "single":
            event = normalized
            result = await asyncio.to_thread(_score_single_event_with_log, event, disable_analogs)
            request_ok = True
            return JSONResponse(result)

        if mode == "batch":
            events = normalized
            results = await asyncio.to_thread(_score_events_batch, events, disable_analogs)
            request_ok = True
            return JSONResponse({"results": results})

        request_error = "HTTPException 400: Unsupported score payload mode."
        raise HTTPException(status_code=400, detail="Unsupported score payload mode.")
    except HTTPException as exc:
        request_error = f"HTTPException {exc.status_code}: {exc.detail}"
        raise
    except Exception as exc:
        request_error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        _finish_score_request(ok=request_ok, duration_ms=duration_ms, error=request_error)


def _score_events_batch(events: list[dict], disable_analogs: bool = False) -> list[dict]:
    results: list[dict] = []
    try:
        _SCORE_LOAD_SHED_LOCAL.disable_analogs = bool(disable_analogs)
        for ev in events:
            res = _score_event(ev)
            _enqueue_prediction(ev, res)
            results.append(res)
    finally:
        _SCORE_LOAD_SHED_LOCAL.disable_analogs = False
    return results


def _score_single_event_with_log(event: dict, disable_analogs: bool = False) -> dict:
    try:
        _SCORE_LOAD_SHED_LOCAL.disable_analogs = bool(disable_analogs)
        result = _score_event(event)
        _enqueue_prediction(event, result)
        return result
    finally:
        _SCORE_LOAD_SHED_LOCAL.disable_analogs = False


def run():
    import uvicorn

    uvicorn.run("server.ml_server:app", host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    run()
