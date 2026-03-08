import json
import logging
import os
import sqlite3
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
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


registry = ModelRegistry()
_startup_error: Optional[str] = None


@asynccontextmanager
async def lifespan(_app):
    global _startup_error
    try:
        registry.load()
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
                quality_flags, is_preview
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
        _startup_error = None
        return {
            "status": "ok",
            "models": registry.available(),
            "manifest": registry.manifest,
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
