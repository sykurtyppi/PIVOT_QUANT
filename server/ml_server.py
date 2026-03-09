import json
import logging
import math
import os
import sqlite3
import sys
import time
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
SCORE_MAX_BATCH_EVENTS = max(1, int(os.getenv("ML_SCORE_MAX_BATCH_EVENTS", "256")))
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
ML_ANALOG_PROMOTION_GATE_PATH = Path(
    os.getenv(
        "ML_ANALOG_PROMOTION_GATE_PATH",
        str(ROOT / "logs" / "reports" / "analog_promotion_gate_latest.json"),
    )
)
ML_ANALOG_BLEND_MODE = (os.getenv("ML_ANALOG_BLEND_MODE", "shadow") or "shadow").strip().lower()
if ML_ANALOG_BLEND_MODE not in {"off", "shadow", "active"}:
    ML_ANALOG_BLEND_MODE = "shadow"
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
ML_ANALOG_FEATURE_WEIGHTS = {
    "distance_bps": max(0.0, float(os.getenv("ML_ANALOG_W_DISTANCE_BPS", "1.0"))),
    "distance_atr_ratio": max(
        0.0, float(os.getenv("ML_ANALOG_W_DISTANCE_ATR_RATIO", "1.2"))
    ),
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


class ModelRegistry:
    def __init__(self):
        self.manifest = None
        self.manifest_path: str | None = None
        self.models = {"reject": {}, "break": {}}
        self.thresholds = {"reject": {}, "break": {}}

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

    def load(self):
        manifest_path = self.resolve_manifest_path()
        if not manifest_path.exists():
            raise FileNotFoundError(f"Missing manifest at {manifest_path}")
        with manifest_path.open("r", encoding="utf-8") as handle:
            self.manifest = json.load(handle)
        self.manifest_path = str(manifest_path)

        self.models = {"reject": {}, "break": {}}
        self.thresholds = {"reject": {}, "break": {}}

        # Load thresholds from manifest first
        manifest_thresholds = self.manifest.get("thresholds", {})
        for target in ("reject", "break"):
            for horizon_str, threshold in manifest_thresholds.get(target, {}).items():
                self.thresholds[target][int(horizon_str)] = float(threshold)

        for target, horizons in self.manifest.get("models", {}).items():
            for horizon, filename in horizons.items():
                path = MODEL_DIR / filename
                if not path.exists():
                    continue
                payload = joblib.load(path)
                self.models[target][int(horizon)] = payload

                # Fall back to pickle-embedded threshold if manifest didn't have it
                h_int = int(horizon)
                if h_int not in self.thresholds.get(target, {}):
                    pkl_thresh = payload.get("optimal_threshold")
                    if pkl_thresh is not None:
                        self.thresholds.setdefault(target, {})[h_int] = float(pkl_thresh)

    def get_threshold(self, target: str, horizon: int) -> float:
        """Get optimal decision threshold for a target/horizon pair.
        Falls back to 0.5 if not available."""
        return self.thresholds.get(target, {}).get(horizon, 0.5)

    def available(self):
        return {
            target: sorted(horizons.keys()) for target, horizons in self.models.items()
        }


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

    def refresh(self) -> None:
        self.error = None
        self.rows_by_horizon = {}
        self.loaded_at_ms = int(time.time() * 1000)
        if not self.enabled:
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
                        "rv_30": _to_float(row["rv_30"]),
                        "or_size_atr": _to_float(row["or_size_atr"]),
                        "overnight_gap_atr": _to_float(row["overnight_gap_atr"]),
                        "reject": _to_float(row["reject"]),
                        "break": _to_float(row["break"]),
                    }
                )
            self.rows_by_horizon = out
        except Exception as exc:  # pragma: no cover - defensive
            self.error = str(exc)
            self.rows_by_horizon = {}
        finally:
            if conn is not None:
                conn.close()

    def health(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "db": str(self.db_path),
            "loaded_at_ms": self.loaded_at_ms,
            "error": self.error,
            "rows": {str(h): len(rows) for h, rows in self.rows_by_horizon.items()},
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

        ranked: list[dict[str, object]] = []
        for row in stage_rows:
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
        summary: dict[str, object] = {
            "enabled": self.enabled,
            "error": self.error,
            "loaded_at_ms": self.loaded_at_ms,
            "horizons": {},
            "best": None,
        }
        if not self.enabled:
            return summary
        if self.error:
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
        query_features = {
            "distance_bps": _to_float(event.get("distance_bps")),
            "distance_atr_ratio": _to_float(features.get("distance_atr_ratio")),
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
                rows=self.rows_by_horizon.get(horizon, []),
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


@asynccontextmanager
async def lifespan(_app):
    global _startup_error
    try:
        registry.load()
        analog_engine.refresh()
        _startup_error = None
    except Exception as exc:
        _startup_error = str(exc)
        print(f"ML server startup warning: {exc}")
    yield


app = FastAPI(title="PivotQuant ML Server", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _is_model_stale() -> bool:
    """Check if the model artifacts are older than STALE_MODEL_HOURS."""
    if not registry.manifest:
        return False
    trained_end_ts = registry.manifest.get("trained_end_ts")
    if not trained_end_ts:
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
    return {
        "status": str(payload.get("status") or "unknown"),
        "path": str(path),
        "report_date": payload.get("report_date"),
        "required_horizons": payload.get("required_horizons"),
        "evaluated_horizons": payload.get("evaluated_horizons"),
        "passed_horizons": payload.get("passed_horizons"),
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
    weight = ML_ANALOG_BLEND_WEIGHT_BASE * eff_scale * ci_scale
    return max(0.0, min(ML_ANALOG_BLEND_WEIGHT_MAX, weight))


def _log_prediction(event: dict, result: dict) -> None:
    """Append a prediction record to the prediction_log table.

    Best-effort: failures are logged but never propagate to the caller.
    This lets us reconcile live predictions against actual outcomes later.
    The prediction_log table is created by migrate_db.py (migration v3);
    CREATE IF NOT EXISTS is kept here as a safety net for standalone use.
    """
    event_id = event.get("event_id")
    if not event_id:
        return

    conn = None
    try:
        conn = sqlite3.connect(str(PREDICTION_LOG_DB))
        conn.execute("PRAGMA journal_mode=WAL;")
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

        conn.execute(
            """INSERT OR IGNORE INTO prediction_log (
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
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
    except Exception as exc:
        log.warning("Prediction log write failed: %s", exc)
    finally:
        if conn is not None:
            conn.close()


@app.get("/health")
def health():
    has_models = any(horizons for horizons in registry.available().values())
    stale = _is_model_stale()
    if not has_models or _startup_error is not None:
        status = "degraded"
    elif stale:
        status = "stale"
    else:
        status = "ok"
    result = {
        "status": status,
        "feature_version": FEATURE_VERSION,
        "manifest": registry.manifest,
        "manifest_path": registry.manifest_path,
        "models": registry.available(),
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
            }
        },
        "analogs": {
            **analog_engine.health(),
            "promotion_gate": _read_analog_promotion_gate(),
            "min_features": ML_ANALOG_MIN_FEATURES,
            "min_feature_overlap": ML_ANALOG_MIN_FEATURE_OVERLAP,
            "min_feature_support": ML_ANALOG_MIN_FEATURE_SUPPORT,
            "max_mean_distance": ML_ANALOG_MAX_MEAN_DISTANCE,
            "max_ci_width": ML_ANALOG_MAX_CI_WIDTH,
            "recency_tau_days": ML_ANALOG_RECENCY_TAU_DAYS,
            "recency_floor": ML_ANALOG_RECENCY_FLOOR,
            "sim_weight_mode": ML_ANALOG_SIM_WEIGHT_MODE,
            "prior_strength": ML_ANALOG_PRIOR_STRENGTH,
            "disagreement_flag": ML_ANALOG_DISAGREEMENT_FLAG,
            "blend_mode": ML_ANALOG_BLEND_MODE,
            "blend_weight_base": ML_ANALOG_BLEND_WEIGHT_BASE,
            "blend_weight_max": ML_ANALOG_BLEND_WEIGHT_MAX,
            "blend_n_eff_ref": ML_ANALOG_BLEND_N_EFF_REF,
            "feature_weights": ML_ANALOG_FEATURE_WEIGHTS,
        },
    }
    if _startup_error is not None:
        result["startup_error"] = _startup_error
    if stale:
        trained_end_ts = registry.manifest.get("trained_end_ts", 0)
        age_hours = (time.time() * 1000 - trained_end_ts) / (3600 * 1000)
        result["stale_hours"] = round(age_hours, 1)
    return result


@app.post("/reload")
def reload_models():
    """Hot-reload model artifacts from disk without restarting the server."""
    global _startup_error
    try:
        registry.load()
        analog_engine.refresh()
        _startup_error = None
        return {
            "status": "ok",
            "models": registry.available(),
            "manifest": registry.manifest,
            "analogs": analog_engine.health(),
        }
    except Exception as exc:
        _startup_error = str(exc)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(exc)},
        )


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


def _build_regime_thresholds(all_horizons: list[int], trade_regime: str) -> dict[str, dict[int, float]]:
    thresholds = {"reject": {}, "break": {}}
    for horizon in all_horizons:
        base_reject = registry.get_threshold("reject", horizon)
        base_break = registry.get_threshold("break", horizon)
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
            threshold_map["reject"].get(horizon, 0.5),
            reject_delta,
        )
        overlaid["break"][horizon] = _adjust_threshold(
            threshold_map["break"].get(horizon, 0.5),
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
            threshold_map["reject"].get(horizon, 0.5),
            reject_delta,
        )
        guarded["break"][horizon] = _adjust_threshold(
            threshold_map["break"].get(horizon, 0.5),
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

        reject_thresh = threshold_map["reject"].get(horizon, 0.5)
        break_thresh = threshold_map["break"].get(horizon, 0.5)
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
    for horizon in scored_horizons:
        pr = scores.get(f"prob_reject_{horizon}m")
        pb = scores.get(f"prob_break_{horizon}m")
        if pr is None and pb is None:
            continue
        pr = pr if pr is not None else 0.0
        pb = pb if pb is not None else 0.0

        signal = signals.get(f"signal_{horizon}m")
        reject_thresh = threshold_map["reject"].get(horizon, 0.5)
        break_thresh = threshold_map["break"].get(horizon, 0.5)

        if signal == "reject":
            edge = (pr - reject_thresh) + 1.0
        elif signal == "break":
            edge = -(pb - break_thresh) - 1.0
        else:
            edge = pr - pb

        if best_score is None or edge > best_score:
            best_score = edge
            best_horizon = horizon

    has_signal = any(
        signals.get(f"signal_{h}m") in ("reject", "break")
        for h in scored_horizons
    )
    return best_horizon, not has_signal


def _score_event(event: dict):
    missing = collect_missing(event)
    features = build_feature_row(event)

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

    stats = registry.manifest.get("stats", {}) if registry.manifest else {}
    calibration = registry.manifest.get("calibration", {}) if registry.manifest else {}

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

    for target in ("reject", "break"):
        for horizon, payload in registry.models.get(target, {}).items():
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
            threshold = registry.get_threshold(target, horizon)
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
        set(registry.models.get("reject", {}).keys())
        .union(registry.models.get("break", {}).keys())
    )
    scored_horizons = [h for h in all_horizons if h not in ML_SHADOW_HORIZONS]
    if not scored_horizons:
        scored_horizons = list(all_horizons)

    threshold_baseline = {
        "reject": {h: _clamp_threshold(registry.get_threshold("reject", h)) for h in all_horizons},
        "break": {h: _clamp_threshold(registry.get_threshold("break", h)) for h in all_horizons},
    }
    regime_state = _compute_trade_regime(event=event, features=features)
    atr_distance_ratio = _compute_atr_distance_ratio(event=event, features=features)
    atr_zone = _classify_atr_zone(atr_distance_ratio)
    threshold_regime_base = _build_regime_thresholds(
        all_horizons=all_horizons,
        trade_regime=regime_state["bucket"],
    )
    threshold_regime, atr_overlay_meta = _apply_atr_zone_overlay(
        threshold_map=threshold_regime_base,
        all_horizons=all_horizons,
        trade_regime=regime_state["bucket"],
        atr_zone=atr_zone,
    )
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
    blend_info: dict[str, object] = {
        "mode": ML_ANALOG_BLEND_MODE,
        "gate_status": analog_gate.get("status"),
        "gate_path": analog_gate.get("path"),
        "gate_report_date": analog_gate.get("report_date"),
        "gate_reasons": analog_gate.get("reasons"),
        "allow_active_blend": gate_pass,
        "applied_horizons": [],
        "horizons": {},
        "weight_base": ML_ANALOG_BLEND_WEIGHT_BASE,
        "weight_max": ML_ANALOG_BLEND_WEIGHT_MAX,
        "n_eff_ref": ML_ANALOG_BLEND_N_EFF_REF,
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

        scores[f"analog_reject_{horizon}m"] = analog_reject
        scores[f"analog_break_{horizon}m"] = analog_break
        scores[f"analog_n_{horizon}m"] = _to_float(analog_h.get("n"))
        scores[f"analog_n_eff_{horizon}m"] = n_eff
        scores[f"analog_ci_width_{horizon}m"] = ci_width
        disagreement = _to_float(analog_h.get("disagreement"))
        scores[f"analog_disagreement_{horizon}m"] = disagreement
        if disagreement is not None and disagreement >= ML_ANALOG_DISAGREEMENT_FLAG:
            flag = f"ANALOG_DISAGREE_{horizon}m"
            if flag not in quality_flags:
                quality_flags.append(flag)

        scores[f"blend_prob_reject_{horizon}m"] = blend_reject
        scores[f"blend_prob_break_{horizon}m"] = blend_break
        blend_info["horizons"][str(horizon)] = {
            "weight": weight,
            "applied": False,
            "model_reject": model_reject,
            "model_break": model_break,
            "analog_reject": analog_reject,
            "analog_break": analog_break,
            "blended_reject": blend_reject,
            "blended_break": blend_break,
        }

        if (
            ML_ANALOG_BLEND_MODE == "active"
            and gate_pass
            and weight > 0
            and blend_reject is not None
            and blend_break is not None
        ):
            scores[f"prob_reject_{horizon}m"] = blend_reject
            scores[f"prob_break_{horizon}m"] = blend_break
            blend_info["horizons"][str(horizon)]["applied"] = True
            cast_applied = blend_info.get("applied_horizons")
            if isinstance(cast_applied, list):
                cast_applied.append(horizon)

    if ML_ANALOG_BLEND_MODE == "active" and not gate_pass:
        quality_flags.append("ANALOG_BLEND_BLOCKED_GATE")
    applied_horizons = blend_info.get("applied_horizons")
    if (
        ML_ANALOG_BLEND_MODE == "active"
        and isinstance(applied_horizons, list)
        and applied_horizons
    ):
        quality_flags.append("ANALOG_BLEND_ACTIVE")
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
            # no_edge: use "other" (non-reject) stats as baseline
            mfe = stats_reject.get("mfe_bps_other") or 0.0
            mae = stats_reject.get("mae_bps_other") or 0.0

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

    for horizon in all_horizons:
        thresholds_used[f"threshold_reject_{horizon}m"] = selected_threshold_map["reject"].get(horizon, 0.5)
        thresholds_used[f"threshold_break_{horizon}m"] = selected_threshold_map["break"].get(horizon, 0.5)

    return {
        "status": "degraded" if missing else "ok",
        "scores": scores,
        "signals": signals,
        "thresholds": thresholds_used,
        "abstain": abstain,
        "best_horizon": best_horizon,
        "model_version": registry.manifest.get("version") if registry.manifest else None,
        "feature_version": FEATURE_VERSION,
        "trained_end_ts": registry.manifest.get("trained_end_ts") if registry.manifest else None,
        "calibration": calibration,
        "quality_flags": quality_flags,
        "analogs": analog_summary,
        "analog_blend": blend_info,
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


@app.post("/score")
async def score(request: Request):
    has_models = any(horizons for horizons in registry.available().values())
    if registry.manifest is None or not has_models:
        raise HTTPException(status_code=503, detail="Models not loaded. Train artifacts first.")
    try:
        payload = await request.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {exc.msg}") from exc

    mode, normalized = _validate_score_payload(payload)

    if mode == "single":
        event = normalized
        result = _score_event(event)
        _log_prediction(event, result)
        return JSONResponse(result)

    if mode == "batch":
        events = normalized
        results = []
        for ev in events:
            res = _score_event(ev)
            _log_prediction(ev, res)
            results.append(res)
        return JSONResponse({"results": results})

    raise HTTPException(status_code=400, detail="Unsupported score payload mode.")


def run():
    import uvicorn

    uvicorn.run("server.ml_server:app", host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    run()
