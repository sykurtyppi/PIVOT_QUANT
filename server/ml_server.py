import json
import os
import sys
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

    def load(self):
        manifest_path = MODEL_DIR / "manifest_latest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Missing manifest at {manifest_path}")
        with manifest_path.open("r", encoding="utf-8") as handle:
            self.manifest = json.load(handle)

        self.models = {"reject": {}, "break": {}}
        for target, horizons in self.manifest.get("models", {}).items():
            for horizon, filename in horizons.items():
                path = MODEL_DIR / filename
                if not path.exists():
                    continue
                payload = joblib.load(path)
                self.models[target][int(horizon)] = payload

    def available(self):
        return {
            target: sorted(horizons.keys()) for target, horizons in self.models.items()
        }


registry = ModelRegistry()


@app.on_event("startup")
def startup_event():
    try:
        registry.load()
    except Exception as exc:
        print(f"ML server startup warning: {exc}")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "feature_version": FEATURE_VERSION,
        "manifest": registry.manifest,
        "models": registry.available(),
    }


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

            horizon_stats = stats.get(str(horizon), {}).get(target, {})
            for metric in ("mfe_bps", "mae_bps"):
                if f"{metric}_{target}" in horizon_stats:
                    scores[f"{metric}_{target}_{horizon}m"] = horizon_stats.get(f"{metric}_{target}")

    # Expected MFE/MAE using simple weighted mix if both outputs available.
    for horizon in sorted(
        set(registry.models.get("reject", {}).keys()).union(registry.models.get("break", {}).keys())
    ):
        pr = scores.get(f"prob_reject_{horizon}m")
        pb = scores.get(f"prob_break_{horizon}m")
        if pr is None and pb is None:
            continue
        pr = pr if pr is not None else 0.0
        pb = pb if pb is not None else 0.0
        po = max(0.0, 1.0 - pr - pb)
        stats_reject = stats.get(str(horizon), {}).get("reject", {})
        stats_break = stats.get(str(horizon), {}).get("break", {})
        mfe_break = stats_break.get("mfe_bps_break") or 0.0
        mae_break = stats_break.get("mae_bps_break") or 0.0
        mfe = (
            pr * (stats_reject.get("mfe_bps_reject") or 0)
            + pb * mfe_break
            + po * (stats_reject.get("mfe_bps_other") or 0)
        )
        mae = (
            pr * (stats_reject.get("mae_bps_reject") or 0)
            + pb * mae_break
            + po * (stats_reject.get("mae_bps_other") or 0)
        )
        scores[f"exp_mfe_bps_{horizon}m"] = float(mfe)
        scores[f"exp_mae_bps_{horizon}m"] = float(mae)

    best_horizon = None
    best_score = None
    for horizon in sorted(
        set(registry.models.get("reject", {}).keys()).union(registry.models.get("break", {}).keys())
    ):
        pr = scores.get(f"prob_reject_{horizon}m")
        pb = scores.get(f"prob_break_{horizon}m")
        if pr is None and pb is None:
            continue
        pr = pr if pr is not None else 0.0
        pb = pb if pb is not None else 0.0
        edge = pr - pb
        if best_score is None or edge > best_score:
            best_score = edge
            best_horizon = horizon

    return {
        "status": "degraded" if missing else "ok",
        "scores": scores,
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
