#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ml.calibration import ProbabilityCalibrator
from ml.features import FEATURE_VERSION, build_feature_row, drop_features
from ml.thresholds import select_threshold, utility_bps_for_target

DEFAULT_DUCKDB = os.getenv("DUCKDB_PATH", "data/pivot_training.duckdb")
DEFAULT_VIEW = os.getenv("DUCKDB_VIEW", "training_events_v1")
DEFAULT_OUT_DIR = os.getenv("RF_MODEL_DIR", "data/models")
DEFAULT_METADATA_DIR = os.getenv("RF_METADATA_DIR", "metadata_runtime")
DEFAULT_CANDIDATE_MANIFEST = (
    os.getenv("RF_CANDIDATE_MANIFEST", "manifest_runtime_latest.json").strip()
    or "manifest_runtime_latest.json"
)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return int(default)
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return int(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _gamma_mode(name: str, default: str = "90dte") -> str:
    value = (os.getenv(name, default) or default).strip().lower()
    if value == "quarterly":
        raise ValueError(f"{name}=quarterly is no longer supported; use 90dte")
    if value not in {"0dte", "front", "monthly", "all", "90dte", "aggregate_90dte"}:
        raise ValueError(f"Unsupported {name}={value!r}")
    return value


def _gamma_context_metadata() -> dict[str, object]:
    return {
        "context_expiry_mode": _gamma_mode("GAMMA_CONTEXT_EXPIRY_MODE", "90dte"),
        "context_dte_window_days": _env_int("GAMMA_CONTEXT_DTE_DAYS", 120),
        "history_expiry_mode": _gamma_mode("GAMMA_HISTORY_EXPIRY_MODE", "90dte"),
        "history_dte_window_days": _env_int("GAMMA_HISTORY_LIVE_DTE_DAYS", 120),
    }


def _coerce_precision_floor(raw_value: str) -> float:
    value = float(raw_value)
    if value < 0.0 or value > 1.0:
        raise ValueError(f"precision floor must be within [0,1], got {raw_value!r}")
    return float(value)


def _coerce_min_signals(raw_value: str) -> int:
    value = int(raw_value)
    if value < 1:
        raise ValueError(f"min_signals must be >= 1, got {raw_value!r}")
    return int(value)


def _parse_threshold_overrides(
    raw_value: str,
    *,
    value_cast,
    option_name: str,
) -> dict[tuple[str, int | None], float | int]:
    """Parse target/horizon override map.

    Format: "break:15=8,break:30=8,break:60=6,reject:*=10"
    Keys accept ":" "/" "_" "-" separators and optional "m" suffix.
    """
    parsed: dict[tuple[str, int | None], float | int] = {}
    if raw_value is None:
        return parsed

    for token in str(raw_value).split(","):
        item = token.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(
                f"{option_name}: invalid entry {item!r}; expected '<target>:<horizon>=<value>'"
            )
        key_raw, value_raw = item.split("=", 1)
        key = key_raw.strip().lower()
        value_text = value_raw.strip()
        if not value_text:
            raise ValueError(f"{option_name}: missing value in entry {item!r}")

        match = re.match(r"^(reject|break)\s*[:/_-]\s*([0-9]+m?|all|\*)$", key)
        if not match:
            raise ValueError(
                f"{option_name}: invalid key {key_raw!r}; expected reject|break + horizon (e.g. break:15)"
            )

        target = str(match.group(1)).lower()
        horizon_token = str(match.group(2)).lower()
        horizon: int | None
        if horizon_token in {"all", "*"}:
            horizon = None
        else:
            horizon = int(horizon_token.rstrip("m"))

        try:
            value = value_cast(value_text)
        except Exception as exc:
            raise ValueError(f"{option_name}: invalid value {value_text!r} in entry {item!r}: {exc}") from exc

        parsed[(target, horizon)] = value
    return parsed


def _resolve_threshold_override(
    *,
    target: str,
    horizon: int,
    base_value: float | int,
    overrides: dict[tuple[str, int | None], float | int],
) -> float | int:
    target_key = str(target).strip().lower()
    direct_key = (target_key, int(horizon))
    if direct_key in overrides:
        return overrides[direct_key]
    wildcard_key = (target_key, None)
    if wildcard_key in overrides:
        return overrides[wildcard_key]
    return base_value


def require(module_name: str, hint: str):
    try:
        return __import__(module_name)
    except Exception:
        print(f"{module_name} not installed. Install with: {hint}", file=sys.stderr)
        sys.exit(1)


def choose_calibration(method, calib_size):
    if method != "auto":
        return method
    if calib_size >= 500:
        return "isotonic"
    return "sigmoid"


def build_pipeline(numeric_cols, categorical_cols, args):
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )
    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_cols),
            ("cat", categorical_transformer, categorical_cols),
        ]
    )

    rf = RandomForestClassifier(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth if args.max_depth > 0 else None,
        min_samples_leaf=args.min_samples_leaf,
        class_weight="balanced",
        n_jobs=-1,
        random_state=args.random_state,
    )

    return Pipeline(steps=[("prep", preprocessor), ("rf", rf)])


def load_dataframe(db_path: str, view: str, horizon: int):
    duckdb = require("duckdb", "python3 -m pip install duckdb")
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(
            f"SELECT * FROM {view} WHERE horizon_min = ? ORDER BY ts_event",
            [horizon],
        ).df()
    finally:
        con.close()
    return df


def ensure_event_date(df):
    pd = require("pandas", "python3 -m pip install pandas")
    if "event_date_et" in df.columns:
        return df
    if "ts_event" not in df.columns:
        raise ValueError("Missing ts_event in training view")
    df["event_date_et"] = pd.to_datetime(df["ts_event"], unit="ms", utc=True).dt.tz_convert(
        "America/New_York"
    ).dt.date
    return df


def build_feature_dataframe(df):
    pd = require("pandas", "python3 -m pip install pandas")
    rows = [build_feature_row(row) for row in df.to_dict("records")]
    return pd.DataFrame(rows, index=df.index)


def _parse_version_number(label: str) -> int | None:
    if not label:
        return None
    raw = str(label).strip().lower()
    if raw.startswith("v"):
        raw = raw[1:]
    if not raw.isdigit():
        return None
    return int(raw)


def _resolve_metadata_dir(out_dir: Path, raw_metadata_dir: str) -> Path:
    candidate = Path(raw_metadata_dir)
    if not candidate.is_absolute():
        candidate = out_dir / candidate
    return candidate


def next_version(out_dir: Path, metadata_dir: Path) -> str:
    version_numbers: list[int] = []

    for path in metadata_dir.glob("metadata_v*.json"):
        parsed = _parse_version_number(path.stem.replace("metadata_", ""))
        if parsed is not None:
            version_numbers.append(parsed)

    # Backward compatibility: include legacy metadata files in model root.
    for path in out_dir.glob("metadata_v*.json"):
        parsed = _parse_version_number(path.stem.replace("metadata_", ""))
        if parsed is not None:
            version_numbers.append(parsed)

    # Fallback: infer from existing model artifacts if metadata files were cleaned.
    for path in out_dir.glob("rf_*_v*.pkl"):
        stem = path.stem
        if "_v" not in stem:
            continue
        suffix = stem.rsplit("_v", 1)[-1]
        parsed = _parse_version_number(suffix)
        if parsed is not None:
            version_numbers.append(parsed)

    if not version_numbers:
        return "v001"

    return f"v{max(version_numbers) + 1:03d}"


def _temp_path(path: Path) -> Path:
    return path.with_name(
        f".{path.name}.tmp-{os.getpid()}-{int(time.time() * 1000)}"
    )


def atomic_write_json(path: Path, payload: dict) -> None:
    tmp_path = _temp_path(path)
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def atomic_joblib_dump(joblib_module, payload: dict, path: Path) -> None:
    tmp_path = _temp_path(path)
    try:
        joblib_module.dump(payload, tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def atomic_copy_file(src: Path, dst: Path) -> None:
    tmp_path = _temp_path(dst)
    try:
        shutil.copy2(src, tmp_path)
        os.replace(tmp_path, dst)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _merge_tune_date_range(current: dict[str, str] | None, event_dates) -> dict[str, str] | None:
    values: list[str] = []
    for value in list(event_dates):
        if value is None:
            continue
        iso_value = value.isoformat() if hasattr(value, "isoformat") else str(value)
        iso_value = str(iso_value).strip()
        if iso_value:
            values.append(iso_value)
    if not values:
        return current

    next_min = min(values)
    next_max = max(values)
    if not current:
        return {
            "min_event_date_et": next_min,
            "max_event_date_et": next_max,
        }
    return {
        "min_event_date_et": min(str(current.get("min_event_date_et") or next_min), next_min),
        "max_event_date_et": max(str(current.get("max_event_date_et") or next_max), next_max),
    }


def _regime_bucket(regime_type_value) -> str:
    regime_type = None
    try:
        if regime_type_value is not None:
            regime_type = int(regime_type_value)
    except (TypeError, ValueError):
        regime_type = None
    if regime_type in (1, 2, 4):
        return "expansion"
    if regime_type == 3:
        return "compression"
    return "neutral"


def _compute_target_stats(sub_df, target: str) -> dict:
    stats = {}
    if sub_df.empty or target not in sub_df.columns:
        return stats

    pos = sub_df[sub_df[target] == 1]
    neg = sub_df[sub_df[target] == 0]
    stats["sample_size"] = int(sub_df.shape[0])
    stats[f"{target}_count"] = int(pos.shape[0])
    stats[f"{target}_other_count"] = int(neg.shape[0])
    stats[f"{target}_rate"] = float(pos.shape[0] / max(1, sub_df.shape[0]))
    for metric in ["mfe_bps", "mae_bps"]:
        pos_metric = float(pos[metric].mean()) if (not pos.empty and metric in pos.columns) else None
        neg_metric = float(neg[metric].mean()) if (not neg.empty and metric in neg.columns) else None
        stats[f"{metric}_{target}"] = pos_metric
        # Target-scoped "other" bucket avoids cross-target overwrites when
        # reject/break stats are merged into a single payload.
        stats[f"{metric}_{target}_other"] = neg_metric
    return stats


def compute_horizon_stats(df, target, horizon):
    stats = {}
    sub = df[df["horizon_min"] == horizon]
    if sub.empty:
        return stats

    stats.update(_compute_target_stats(sub, target))
    if target not in sub.columns:
        return stats

    if "regime_type" in sub.columns:
        with_regime = sub.copy()
        with_regime["_regime_bucket"] = with_regime["regime_type"].map(_regime_bucket)
        by_regime: dict[str, dict] = {}
        for bucket in ("compression", "expansion", "neutral"):
            bucket_df = with_regime[with_regime["_regime_bucket"] == bucket]
            bucket_stats = _compute_target_stats(bucket_df, target)
            if not bucket_stats:
                continue
            bucket_stats["sample_share"] = float(bucket_stats["sample_size"] / max(1, stats["sample_size"]))
            by_regime[bucket] = bucket_stats
        if by_regime:
            stats["by_regime"] = by_regime
    return stats


def split_calibration_slices(X_calib, y_calib, *, fit_fraction: float, min_fit_events: int, min_tune_events: int):
    n = len(X_calib)
    if n == 0:
        return X_calib, y_calib, X_calib, y_calib, False

    fit_n = int(round(n * fit_fraction))
    fit_n = max(int(min_fit_events), fit_n)
    fit_n = min(fit_n, max(0, n - int(min_tune_events)))

    if fit_n <= 0 or fit_n >= n:
        return X_calib, y_calib, X_calib, y_calib, False

    X_fit = X_calib.iloc[:fit_n]
    y_fit = y_calib.iloc[:fit_n]
    X_tune = X_calib.iloc[fit_n:]
    y_tune = y_calib.iloc[fit_n:]
    return X_fit, y_fit, X_tune, y_tune, True


def apply_threshold_risk_guards(
    *,
    objective: str,
    threshold: float,
    threshold_meta: dict,
    no_trade_threshold: float,
    min_utility_score: float,
    disable_on_nonpositive_utility: bool,
    disable_on_fallback: bool,
) -> tuple[float, dict]:
    guarded_threshold = min(1.0, max(0.0, float(no_trade_threshold)))
    reasons: list[str] = []

    if objective == "utility_bps":
        selected_score = threshold_meta.get("score")
        score_value = float(selected_score) if selected_score is not None else None
        if disable_on_fallback and bool(threshold_meta.get("fallback")):
            reasons.append("fallback_threshold")
        if (
            disable_on_nonpositive_utility
            and score_value is not None
            and score_value <= float(min_utility_score)
        ):
            reasons.append(
                f"non_positive_utility({score_value:.6f}<={float(min_utility_score):.6f})"
            )

    threshold_meta["guard_applied"] = bool(reasons)
    threshold_meta["guard_reason"] = ";".join(reasons)
    threshold_meta["guard_no_trade_threshold"] = float(guarded_threshold)

    if reasons:
        threshold_meta["fallback"] = True
        return guarded_threshold, threshold_meta
    return float(threshold), threshold_meta


def compute_threshold_diagnostics(
    *,
    y_true,
    y_prob,
    utility_per_signal,
    threshold: float,
) -> dict:
    """Compute utility diagnostics for the chosen threshold on the tune slice."""
    np = require("numpy", "python3 -m pip install numpy")

    y_true_arr = np.asarray(y_true, dtype=int)
    y_prob_arr = np.asarray(y_prob, dtype=float)
    utility_arr = np.asarray(utility_per_signal, dtype=float)

    if y_true_arr.size == 0 or y_prob_arr.size == 0 or utility_arr.size == 0:
        return {}
    if y_true_arr.size != y_prob_arr.size or y_true_arr.size != utility_arr.size:
        return {}

    pred_mask = y_prob_arr >= float(threshold)
    tp_mask = pred_mask & (y_true_arr == 1)
    fp_mask = pred_mask & (y_true_arr == 0)
    pos_mask = y_true_arr == 1
    neg_mask = y_true_arr == 0

    def _mean_or_none(mask) -> float | None:
        count = int(np.sum(mask))
        if count <= 0:
            return None
        return float(np.mean(utility_arr[mask]))

    def _sum(mask) -> float:
        if int(np.sum(mask)) <= 0:
            return 0.0
        return float(np.sum(utility_arr[mask]))

    def _corr_or_none(x_arr, y_arr) -> float | None:
        if x_arr is None or y_arr is None:
            return None
        if len(x_arr) < 4 or len(y_arr) < 4:
            return None
        try:
            corr = float(np.corrcoef(x_arr, y_arr)[0, 1])
        except Exception:
            return None
        if not np.isfinite(corr):
            return None
        return corr

    diagnostics = {
        "selected_threshold_for_utility_diagnostics": float(threshold),
        "selected_tp_count": int(np.sum(tp_mask)),
        "selected_fp_count": int(np.sum(fp_mask)),
        "selected_utility_sum": _sum(pred_mask),
        "selected_utility_avg": _mean_or_none(pred_mask),
        "selected_tp_utility_sum": _sum(tp_mask),
        "selected_tp_utility_avg": _mean_or_none(tp_mask),
        "selected_fp_utility_sum": _sum(fp_mask),
        "selected_fp_utility_avg": _mean_or_none(fp_mask),
        "tune_utility_all_mean": _mean_or_none(np.ones_like(y_true_arr, dtype=bool)),
        "tune_utility_pos_mean": _mean_or_none(pos_mask),
        "tune_utility_neg_mean": _mean_or_none(neg_mask),
        "tune_prob_utility_corr_pos": _corr_or_none(y_prob_arr[pos_mask], utility_arr[pos_mask]),
        "tune_prob_utility_corr_all": _corr_or_none(y_prob_arr, utility_arr),
    }
    return diagnostics


def main() -> None:
    default_trade_cost_bps = _env_float(
        "RF_THRESHOLD_TRADE_COST_BPS",
        _env_float("ML_COST_SPREAD_BPS", 0.8)
        + _env_float("ML_COST_SLIPPAGE_BPS", 0.4)
        + _env_float("ML_COST_COMMISSION_BPS", 0.1),
    )

    parser = argparse.ArgumentParser(description="Train RF artifacts for inference server.")
    parser.add_argument("--db", default=DEFAULT_DUCKDB)
    parser.add_argument("--view", default=DEFAULT_VIEW)
    parser.add_argument("--horizons", default="5,15,30,60")
    parser.add_argument("--targets", default="reject,break")
    parser.add_argument("--calibration", choices=["auto", "isotonic", "sigmoid", "none"], default="auto")
    parser.add_argument("--calib-days", type=int, default=_env_int("RF_CALIB_DAYS", 10))
    parser.add_argument("--min-events", type=int, default=200)
    parser.add_argument("--n-estimators", type=int, default=300)
    parser.add_argument("--max-depth", type=int, default=12)
    parser.add_argument("--min-samples-leaf", type=int, default=5)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument(
        "--threshold-objective",
        choices=["f1", "utility_bps"],
        default=os.getenv("RF_THRESHOLD_OBJECTIVE", "utility_bps"),
        help="Threshold objective: utility_bps (default, cost-aware) or f1.",
    )
    parser.add_argument(
        "--threshold-precision-floor",
        type=float,
        default=_env_float("RF_THRESHOLD_PRECISION_FLOOR", 0.40),
        help="Minimum precision required for candidate threshold selection.",
    )
    parser.add_argument(
        "--threshold-min-signals",
        type=int,
        default=int(_env_float("RF_THRESHOLD_MIN_SIGNALS", 10)),
        help="Minimum predicted positives required for threshold candidates.",
    )
    parser.add_argument(
        "--threshold-precision-floor-overrides",
        default=os.getenv("RF_THRESHOLD_PRECISION_FLOOR_OVERRIDES", ""),
        help=(
            "Optional per target/horizon precision floors. "
            "Format: 'break:15=0.35,break:60=0.30,reject:*=0.40'"
        ),
    )
    parser.add_argument(
        "--threshold-min-signals-overrides",
        default=os.getenv("RF_THRESHOLD_MIN_SIGNALS_OVERRIDES", "break:15=8,break:30=8,break:60=6"),
        help=(
            "Optional per target/horizon minimum signal counts. "
            "Format: 'break:15=8,break:30=8,break:60=6,reject:*=10'"
        ),
    )
    parser.add_argument(
        "--threshold-trade-cost-bps",
        type=float,
        default=default_trade_cost_bps,
        help="Per-signal cost (bps) used with utility_bps objective.",
    )
    parser.add_argument(
        "--threshold-stability-band",
        type=float,
        default=_env_float("RF_THRESHOLD_STABILITY_BAND", 0.0),
        help="Average score over +/- band around threshold to avoid knife-edge picks.",
    )
    parser.add_argument(
        "--threshold-min-utility-score",
        type=float,
        default=_env_float("RF_THRESHOLD_MIN_UTILITY_SCORE", 0.0),
        help=(
            "Minimum utility_bps score required for a live threshold. "
            "When score <= this value and guard is enabled, threshold is set to no-trade."
        ),
    )
    parser.add_argument(
        "--threshold-no-trade-threshold",
        type=float,
        default=_env_float("RF_THRESHOLD_NO_TRADE_THRESHOLD", 1.0),
        help="Threshold used when risk guard disables a model/target horizon (typically 1.0).",
    )
    parser.add_argument(
        "--threshold-disable-on-nonpositive-utility",
        dest="threshold_disable_on_nonpositive_utility",
        action="store_true",
        default=_env_bool("RF_THRESHOLD_DISABLE_ON_NONPOSITIVE_UTILITY", True),
        help="Disable thresholds whose selected utility_bps score is <= threshold-min-utility-score.",
    )
    parser.add_argument(
        "--no-threshold-disable-on-nonpositive-utility",
        dest="threshold_disable_on_nonpositive_utility",
        action="store_false",
        help="Allow non-positive selected utility_bps thresholds.",
    )
    parser.add_argument(
        "--threshold-disable-on-fallback",
        dest="threshold_disable_on_fallback",
        action="store_true",
        default=_env_bool("RF_THRESHOLD_DISABLE_ON_FALLBACK", True),
        help="Disable fallback thresholds (e.g. 0.5) by setting them to no-trade threshold.",
    )
    parser.add_argument(
        "--no-threshold-disable-on-fallback",
        dest="threshold_disable_on_fallback",
        action="store_false",
        help="Allow fallback thresholds to remain live.",
    )
    parser.add_argument(
        "--calib-fit-fraction",
        type=float,
        default=_env_float("RF_CALIB_FIT_FRACTION", 0.6),
        help="Fraction of calibration window used to fit calibrator; remainder used for threshold tuning.",
    )
    parser.add_argument(
        "--calib-min-fit-events",
        type=int,
        default=int(_env_float("RF_CALIB_MIN_FIT_EVENTS", 20)),
        help="Minimum events reserved for calibration fitting slice.",
    )
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--metadata-dir",
        default=DEFAULT_METADATA_DIR,
        help="Directory for runtime metadata manifests (absolute or relative to --out-dir)",
    )
    parser.add_argument("--version", default=None)
    parser.add_argument(
        "--candidate-manifest",
        default=DEFAULT_CANDIDATE_MANIFEST,
        help="Runtime candidate manifest filename written into --out-dir",
    )
    parser.add_argument(
        "--allow-partial-manifest",
        action="store_true",
        default=False,
        help="Allow publishing when some target/horizon models are missing",
    )
    args = parser.parse_args()

    try:
        threshold_precision_floor_overrides = _parse_threshold_overrides(
            args.threshold_precision_floor_overrides,
            value_cast=_coerce_precision_floor,
            option_name="--threshold-precision-floor-overrides",
        )
        threshold_min_signals_overrides = _parse_threshold_overrides(
            args.threshold_min_signals_overrides,
            value_cast=_coerce_min_signals,
            option_name="--threshold-min-signals-overrides",
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from exc

    pd = require("pandas", "python3 -m pip install pandas")
    joblib = require("joblib", "python3 -m pip install joblib")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir = _resolve_metadata_dir(out_dir, args.metadata_dir)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    version = args.version or next_version(out_dir, metadata_dir)

    horizons = [int(h.strip()) for h in args.horizons.split(",") if h.strip()]
    targets = [t.strip() for t in args.targets.split(",") if t.strip()]

    manifest = {
        "version": version,
        "feature_version": FEATURE_VERSION,
        "models": {},
        "calibration": {},
        "thresholds_meta": {},
        "stats": {},
        "gamma_context": _gamma_context_metadata(),
        "trained_end_ts": None,
        "tune_date_range": None,
    }
    trained_end_ts_max = None
    latest_aliases: list[tuple[Path, Path]] = []

    for horizon in horizons:
        df = load_dataframe(args.db, args.view, horizon)
        if df.empty:
            print(f"No rows for horizon {horizon}m. Skipping.")
            continue

        df = ensure_event_date(df)
        df = df.sort_values("ts_event")
        if not df.empty:
            horizon_end_ts = int(df["ts_event"].max())
            if trained_end_ts_max is None or horizon_end_ts > trained_end_ts_max:
                trained_end_ts_max = horizon_end_ts

        label_cols = {
            "event_id",
            "ts_event",
            "created_at",
            "event_ts_utc",
            "event_ts_et",
            "event_date_et",
            "confluence_types",
            "horizon_min",
            "return_bps",
            "mfe_bps",
            "mae_bps",
            "reject",
            "break",
            "resolution_min",
            "or_high",
            "or_low",
        }
        feature_drops = drop_features()
        all_drops = label_cols | feature_drops

        feature_df = build_feature_dataframe(df)
        feature_df = feature_df.drop(columns=[c for c in all_drops if c in feature_df.columns], errors="ignore")
        feature_df = feature_df.loc[:, feature_df.notna().any()]

        dates = sorted({d for d in df["event_date_et"].tolist() if d is not None})
        calib_dates = set(dates[-args.calib_days :]) if args.calib_days and dates else set()

        for target in targets:
            if target not in df.columns:
                continue
            sub = df[df[target].notna()].copy()
            if sub.empty:
                continue

            threshold_precision_floor = float(
                _resolve_threshold_override(
                    target=target,
                    horizon=horizon,
                    base_value=float(args.threshold_precision_floor),
                    overrides=threshold_precision_floor_overrides,
                )
            )
            threshold_min_signals = int(
                _resolve_threshold_override(
                    target=target,
                    horizon=horizon,
                    base_value=int(args.threshold_min_signals),
                    overrides=threshold_min_signals_overrides,
                )
            )

            y = sub[target].astype(int)
            X = feature_df.loc[sub.index]
            calib_mask_sub = sub["event_date_et"].isin(calib_dates)

            if len(X) < args.min_events:
                print(f"Not enough events for {target} {horizon}m.")
                continue

            X_train = X.loc[~calib_mask_sub]
            y_train = y.loc[~calib_mask_sub]
            # Some features can be present overall but become fully missing in
            # the non-calibration training split. Drop them to avoid imputer warnings.
            all_null_train_cols = [col for col in X_train.columns if not X_train[col].notna().any()]
            if all_null_train_cols:
                X = X.drop(columns=all_null_train_cols)
                X_train = X_train.drop(columns=all_null_train_cols)
                print(
                    f"Dropping all-null training features for {target} {horizon}m: "
                    + ", ".join(sorted(all_null_train_cols))
                )

            if X.shape[1] == 0:
                print(f"No usable features for {target} {horizon}m after null filtering.")
                continue

            categorical_cols = [
                c for c in X.columns if not pd.api.types.is_numeric_dtype(X[c])
            ]
            numeric_cols = [c for c in X.columns if pd.api.types.is_numeric_dtype(X[c])]
            pipeline = build_pipeline(numeric_cols, categorical_cols, args)
            pipeline.fit(X_train, y_train)

            calibrator = None
            calib_method = None
            calibration_shared_slice = False
            X_calib_fit = None
            y_calib_fit = None
            X_calib_tune = None
            y_calib_tune = None
            if args.calibration != "none":
                X_calib = X.loc[calib_mask_sub]
                y_calib = y.loc[calib_mask_sub]
                (
                    X_calib_fit,
                    y_calib_fit,
                    X_calib_tune,
                    y_calib_tune,
                    split_used,
                ) = split_calibration_slices(
                    X_calib,
                    y_calib,
                    fit_fraction=float(args.calib_fit_fraction),
                    min_fit_events=int(args.calib_min_fit_events),
                    min_tune_events=int(threshold_min_signals),
                )
                calibration_shared_slice = not split_used
                if len(X_calib_fit) >= 20 and len(set(y_calib_fit)) == 2:
                    calib_method = choose_calibration(args.calibration, len(X_calib_fit))
                    calibrator = ProbabilityCalibrator(pipeline, calib_method).fit(X_calib_fit, y_calib_fit)

            # Compute optimal decision threshold on calibration set only.
            # Never fall back to the training set — that causes optimistic bias.
            optimal_threshold = 0.5
            threshold_stability_band = float(args.threshold_stability_band)
            if args.threshold_objective == "utility_bps" and threshold_stability_band <= 0.0:
                threshold_stability_band = 0.02
            threshold_meta = {
                "objective": args.threshold_objective,
                "score": None,
                "precision": None,
                "recall": None,
                "signals": 0,
                "evaluated_candidates": 0,
                "fallback": True,
                "precision_floor": float(threshold_precision_floor),
                "min_signals": int(threshold_min_signals),
                "precision_floor_base": float(args.threshold_precision_floor),
                "min_signals_base": int(args.threshold_min_signals),
                "precision_floor_override_applied": bool(
                    abs(float(threshold_precision_floor) - float(args.threshold_precision_floor)) > 1e-12
                ),
                "min_signals_override_applied": bool(
                    int(threshold_min_signals) != int(args.threshold_min_signals)
                ),
                "trade_cost_bps": float(args.threshold_trade_cost_bps),
                "stability_band": float(threshold_stability_band),
                "top_candidates": [],
                "calibration_shared_slice": bool(calibration_shared_slice),
                "calibration_fit_size": int(len(X_calib_fit)) if X_calib_fit is not None else 0,
                "threshold_tune_size": int(len(X_calib_tune)) if X_calib_tune is not None else 0,
                "search_enabled": True,
                "search_skip_reason": "",
                "min_utility_score": float(args.threshold_min_utility_score),
                "disable_on_nonpositive_utility": bool(args.threshold_disable_on_nonpositive_utility),
                "disable_on_fallback": bool(args.threshold_disable_on_fallback),
            }
            model_obj = calibrator if calibrator is not None else pipeline
            X_calib_set = X_calib_tune if X_calib_tune is not None else X.loc[calib_mask_sub]
            y_calib_for_thresh = y_calib_tune if y_calib_tune is not None else y.loc[X_calib_set.index]
            manifest["tune_date_range"] = _merge_tune_date_range(
                manifest.get("tune_date_range"),
                sub.loc[X_calib_set.index, "event_date_et"].tolist(),
            )
            y_prob_calib = None
            utility_values_for_diag = None
            if calibrator is not None and calibration_shared_slice:
                threshold_meta["search_enabled"] = False
                threshold_meta["search_skip_reason"] = "shared_calibration_slice"
            elif not hasattr(model_obj, "predict_proba"):
                threshold_meta["search_enabled"] = False
                threshold_meta["search_skip_reason"] = "model_missing_predict_proba"
            elif len(X_calib_set) < 20:
                threshold_meta["search_enabled"] = False
                threshold_meta["search_skip_reason"] = "insufficient_tuning_rows"

            if threshold_meta["search_enabled"]:
                try:
                    probs_calib = model_obj.predict_proba(X_calib_set)
                    if probs_calib.shape[1] == 2 and len(set(y_calib_for_thresh)) == 2:
                        y_prob_calib = probs_calib[:, 1]
                        utility_values = None
                        if args.threshold_objective == "utility_bps":
                            utility_values = utility_bps_for_target(
                                sub.loc[X_calib_set.index, "return_bps"],
                                sub.loc[X_calib_set.index, "touch_side"],
                                target,
                                trade_cost_bps=float(args.threshold_trade_cost_bps),
                            )
                            utility_values_for_diag = utility_values
                        selection = select_threshold(
                            y_calib_for_thresh.to_numpy(),
                            y_prob_calib,
                            objective=args.threshold_objective,
                            precision_floor=float(threshold_precision_floor),
                            min_signals=int(threshold_min_signals),
                            default_threshold=0.5,
                            utility_per_signal=utility_values,
                            stability_band=float(threshold_stability_band),
                            preferred_min_score=(
                                float(args.threshold_min_utility_score)
                                if args.threshold_objective == "utility_bps"
                                else None
                            ),
                        )
                        optimal_threshold = float(selection.threshold)
                        threshold_meta.update(
                            {
                                "score": float(selection.score),
                                "precision": float(selection.precision),
                                "recall": float(selection.recall),
                                "signals": int(selection.signals),
                                "evaluated_candidates": int(selection.evaluated_candidates),
                                "fallback": bool(selection.fallback),
                                "stability_score": float(
                                    selection.stability_score
                                    if selection.stability_score is not None
                                    else selection.score
                                ),
                                "top_candidates": selection.top_candidates,
                            }
                        )
                    else:
                        threshold_meta["search_enabled"] = False
                        threshold_meta["search_skip_reason"] = "invalid_probability_shape_or_labels"
                except Exception:
                    optimal_threshold = 0.5
                    threshold_meta["search_enabled"] = False
                    threshold_meta["search_skip_reason"] = "threshold_selection_exception"

            if (
                args.threshold_objective == "utility_bps"
                and y_prob_calib is not None
                and utility_values_for_diag is not None
            ):
                threshold_meta.update(
                    compute_threshold_diagnostics(
                        y_true=y_calib_for_thresh.to_numpy(),
                        y_prob=y_prob_calib,
                        utility_per_signal=utility_values_for_diag,
                        threshold=float(optimal_threshold),
                    )
                )

            optimal_threshold, threshold_meta = apply_threshold_risk_guards(
                objective=args.threshold_objective,
                threshold=optimal_threshold,
                threshold_meta=threshold_meta,
                no_trade_threshold=float(args.threshold_no_trade_threshold),
                min_utility_score=float(args.threshold_min_utility_score),
                disable_on_nonpositive_utility=bool(args.threshold_disable_on_nonpositive_utility),
                disable_on_fallback=bool(args.threshold_disable_on_fallback),
            )

            # Compute per-feature quantile bounds for drift detection at inference.
            # Uses the full training set (not calib) since we want the broadest
            # representative range. p1/p99 gives room for natural variance while
            # catching genuine distribution shifts.
            feature_bounds = {}
            for col in numeric_cols:
                series = X_train[col].dropna()
                if len(series) >= 10:
                    feature_bounds[col] = {
                        "p1": float(series.quantile(0.01)),
                        "p99": float(series.quantile(0.99)),
                        "median": float(series.median()),
                    }

            model_name = f"rf_{target}_{horizon}m_{version}.pkl"
            model_path = out_dir / model_name
            atomic_joblib_dump(
                joblib,
                {
                    "pipeline": pipeline,
                    "calibrator": calibrator,
                    "calibration": calib_method or "none",
                    "optimal_threshold": optimal_threshold,
                    "threshold_meta": threshold_meta,
                    "feature_columns": list(X.columns),
                    "numeric_columns": numeric_cols,
                    "categorical_columns": categorical_cols,
                    "feature_bounds": feature_bounds,
                },
                model_path,
            )

            manifest["models"].setdefault(target, {})[str(horizon)] = model_name
            manifest["calibration"].setdefault(target, {})[str(horizon)] = calib_method or "none"
            manifest["thresholds"] = manifest.get("thresholds", {})
            manifest["thresholds"].setdefault(target, {})[str(horizon)] = optimal_threshold
            manifest["thresholds_meta"].setdefault(target, {})[str(horizon)] = threshold_meta
            manifest["stats"].setdefault(str(horizon), {})[target] = compute_horizon_stats(df, target, horizon)

            latest_name = f"latest_{target}_{horizon}m.pkl"
            latest_path = out_dir / latest_name
            latest_aliases.append((model_path, latest_path))

    manifest["trained_end_ts"] = trained_end_ts_max

    expected_pairs = {(target, str(horizon)) for target in targets for horizon in horizons}
    actual_pairs = set()
    for target, horizon_map in manifest["models"].items():
        for horizon_key in horizon_map.keys():
            actual_pairs.add((target, str(horizon_key)))
    missing_pairs = sorted(expected_pairs - actual_pairs)

    if not actual_pairs:
        print(
            "No model artifacts were produced. "
            f"Aborting publish to preserve existing {args.candidate_manifest}.",
            file=sys.stderr,
        )
        sys.exit(1)

    if missing_pairs and not args.allow_partial_manifest:
        missing_fmt = ", ".join(f"{t}:{h}m" for t, h in missing_pairs)
        print(
            f"Partial model set produced; aborting publish to preserve existing {args.candidate_manifest}. "
            f"Missing: {missing_fmt}. "
            "Use --allow-partial-manifest to override.",
            file=sys.stderr,
        )
        sys.exit(1)

    metadata_path = metadata_dir / f"metadata_{version}.json"
    atomic_write_json(metadata_path, manifest)

    for source_path, alias_path in latest_aliases:
        atomic_copy_file(source_path, alias_path)

    latest_manifest = out_dir / args.candidate_manifest
    # Publish latest manifest last so readers never observe a half-written
    # pointer to artifacts.
    atomic_write_json(latest_manifest, manifest)

    print(f"Saved manifest to {metadata_path}")
    print(f"Saved latest manifest to {latest_manifest}")


if __name__ == "__main__":
    main()
