import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import pandas as pd
import joblib

from ml.features import build_feature_row, collect_missing, FEATURE_VERSION


MODEL_DIR = Path(os.getenv("RF_MODEL_DIR", "data/models"))
HOST = os.getenv("ML_SERVER_BIND", "127.0.0.1")
PORT = int(os.getenv("ML_SERVER_PORT", "5003"))
STALE_MODEL_HOURS = int(os.getenv("STALE_MODEL_HOURS", "48"))

app = FastAPI(title="PivotQuant ML Server", version="1.0.0")

allowed_origins = [
    origin.strip()
    for origin in os.getenv(
        "ML_CORS_ORIGINS", "http://127.0.0.1:3000,http://localhost:3000"
    ).split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ModelRegistry:
    def __init__(self):
        self.manifest = None
        self.models = {"reject": {}, "break": {}}
        self.thresholds = {"reject": {}, "break": {}}

    def load(self):
        manifest_path = MODEL_DIR / "manifest_latest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Missing manifest at {manifest_path}")
        with manifest_path.open("r", encoding="utf-8") as handle:
            self.manifest = json.load(handle)

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
_startup_error: str | None = None


@app.on_event("startup")
def startup_event():
    global _startup_error
    try:
        registry.load()
        _startup_error = None
    except Exception as exc:
        _startup_error = str(exc)
        print(f"ML server startup warning: {exc}")


def _is_model_stale() -> bool:
    """Check if the model artifacts are older than STALE_MODEL_HOURS."""
    if not registry.manifest:
        return False
    trained_end_ts = registry.manifest.get("trained_end_ts")
    if not trained_end_ts:
        return False
    age_hours = (time.time() * 1000 - trained_end_ts) / (3600 * 1000)
    return age_hours > STALE_MODEL_HOURS


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
        "models": registry.available(),
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
        return {"status": "error", "message": str(exc)}


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
                flag = f"FEATURE_DRIFT_{target}_{horizon}m"
                if flag not in quality_flags:
                    quality_flags.append(flag)
                scores[f"drifted_features_{target}_{horizon}m"] = drifted

            # ── Uncalibrated model flag (#8) ──
            calib_method = payload.get("calibration", "none")
            if calib_method == "none":
                flag = f"UNCALIBRATED_{target}_{horizon}m"
                if flag not in quality_flags:
                    quality_flags.append(flag)

    # ── Signal classification per horizon ──
    # Uses optimal thresholds instead of hardcoded 0.5
    signals = {}
    all_horizons = sorted(
        set(registry.models.get("reject", {}).keys())
        .union(registry.models.get("break", {}).keys())
    )

    for horizon in all_horizons:
        pr = scores.get(f"prob_reject_{horizon}m")
        pb = scores.get(f"prob_break_{horizon}m")
        if pr is None and pb is None:
            continue

        reject_thresh = registry.get_threshold("reject", horizon)
        break_thresh = registry.get_threshold("break", horizon)

        pr = pr if pr is not None else 0.0
        pb = pb if pb is not None else 0.0

        # Determine signal: break wins if both fire (it's the more dangerous outcome)
        if pb >= break_thresh:
            signals[f"signal_{horizon}m"] = "break"
        elif pr >= reject_thresh:
            signals[f"signal_{horizon}m"] = "reject"
        else:
            signals[f"signal_{horizon}m"] = "no_edge"

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
    # Prefer horizons that have a directional signal, ranked by excess
    # confidence above their threshold. Fall back to raw edge if no signal.
    best_horizon = None
    best_score = None
    for horizon in all_horizons:
        pr = scores.get(f"prob_reject_{horizon}m")
        pb = scores.get(f"prob_break_{horizon}m")
        if pr is None and pb is None:
            continue
        pr = pr if pr is not None else 0.0
        pb = pb if pb is not None else 0.0

        signal = signals.get(f"signal_{horizon}m")
        reject_thresh = registry.get_threshold("reject", horizon)
        break_thresh = registry.get_threshold("break", horizon)

        if signal == "reject":
            # Excess confidence above reject threshold
            edge = (pr - reject_thresh) + 1.0  # +1.0 to rank above no_edge
        elif signal == "break":
            # Break signal — negative edge (warns against the trade)
            edge = -(pb - break_thresh) - 1.0
        else:
            # No signal — use raw reject-break spread as tiebreaker
            edge = pr - pb

        if best_score is None or edge > best_score:
            best_score = edge
            best_horizon = horizon

    # ── Abstain flag: true when no horizon has a directional signal ──
    has_signal = any(v in ("reject", "break") for v in signals.values())

    return {
        "status": "degraded" if missing else "ok",
        "scores": scores,
        "signals": signals,
        "thresholds": thresholds_used,
        "abstain": not has_signal,
        "best_horizon": best_horizon,
        "model_version": registry.manifest.get("version") if registry.manifest else None,
        "feature_version": FEATURE_VERSION,
        "trained_end_ts": registry.manifest.get("trained_end_ts") if registry.manifest else None,
        "calibration": calibration,
        "quality_flags": quality_flags,
    }


@app.post("/score")
async def score(request: Request):
    if registry.manifest is None or not registry.available():
        raise HTTPException(status_code=503, detail="Models not loaded. Train artifacts first.")
    payload = await request.json()
    if "event" in payload:
        return JSONResponse(_score_event(payload["event"]))
    if "events" in payload:
        return JSONResponse({"results": [_score_event(ev) for ev in payload["events"]]})
    raise HTTPException(status_code=400, detail="Payload must include 'event' or 'events'.")


def run():
    import uvicorn

    uvicorn.run("server.ml_server:app", host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    run()
