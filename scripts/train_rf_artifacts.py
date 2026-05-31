#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ml.calibration import ProbabilityCalibrator
from ml.features import FEATURE_VERSION, build_feature_row, drop_features
from ml.thresholds import NO_SIGNAL_THRESHOLD, select_threshold, utility_bps_for_target
from ml.walk_forward_oos import run_walk_forward_oos
# Single source of truth shared with scripts/refit_calibration.py so the two
# threshold-selecting paths cannot drift on per-(target, horizon) overrides.
from ml.threshold_overrides import (
    parse_threshold_overrides as _parse_threshold_overrides,
    resolve_threshold_override as _resolve_threshold_override,
)

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


def load_dataframe(db_path: str, view: str, horizon: int, symbol: str | None = None):
    """Load labeled events for a horizon, scoped to ``symbol`` when provided.

    CRITICAL-3 fix: the training view is not symbol-scoped (export_parquet.py /
    build_duckdb_view.py select all symbols), so foreign-symbol rows leak in.
    Filtering here at the dataframe boundary is the authoritative guard.
    """
    duckdb = require("duckdb", "python3 -m pip install duckdb")
    con = duckdb.connect(db_path, read_only=True)
    try:
        sym = (symbol or "").strip()
        if sym:
            df = con.execute(
                f"SELECT * FROM {view} WHERE horizon_min = ? AND symbol = ? ORDER BY ts_event",
                [horizon, sym],
            ).df()
        else:
            df = con.execute(
                f"SELECT * FROM {view} WHERE horizon_min = ? ORDER BY ts_event",
                [horizon],
            ).df()
    finally:
        con.close()
    return df


def apply_quality_filters(
    df,
    *,
    horizon: int,
    symbol: str | None,
    filter_unresolved: bool,
    drop_low_coverage_unresolved: bool,
    unresolved_min_coverage: float,
    coverage_column: str,
    verbose: bool = False,
):
    """Apply data-quality filters to a horizon's training frame.

    Pure (no I/O beyond optional prints); returns ``(filtered_df, info)``.

    Policy (post adversarial-audit 2026-05-31):
      * CRITICAL-3: drop foreign-symbol rows (belt-and-suspenders; load_dataframe
        already scopes by symbol, but the training view is not symbol-scoped).
      * CRITICAL-1 REVERTED: unresolved (resolution_min IS NULL) rows are KEPT as
        observed reject=0/break=0 negatives by default. ``filter_unresolved=True``
        is EXPERIMENTAL / NOT-FOR-PROMOTION (recreates the train/serve mismatch).
      * Item-3 partial coverage: when keeping unresolved rows, drop only those
        below ``unresolved_min_coverage`` IF ``coverage_column`` is present;
        otherwise keep them and record the count (never silent).
    """
    def _log(msg: str) -> None:
        if verbose:
            print(msg)

    n_foreign = 0
    sym = (symbol or "").strip()
    if sym and "symbol" in df.columns:
        foreign = df["symbol"].astype(str) != str(sym)
        n_foreign = int(foreign.sum())
        if n_foreign > 0:
            df = df[~foreign].copy()
            _log(
                f"[data-quality] horizon={horizon}m: dropped {n_foreign} foreign-symbol "
                f"rows (expected only {sym}); check export_parquet/build_duckdb_view"
            )

    has_res = "resolution_min" in df.columns
    n_unresolved = int(df["resolution_min"].isna().sum()) if has_res else 0
    n_low_cov_dropped = 0
    cov_col = str(coverage_column or "")
    cov_available = bool(cov_col) and cov_col in df.columns
    experimental_filter_applied = False

    if filter_unresolved and has_res:
        n_before = len(df)
        df = df[df["resolution_min"].notna()].copy()
        n_dropped = n_before - len(df)
        experimental_filter_applied = True
        if n_dropped > 0:
            _log(
                f"[data-quality][EXPERIMENTAL] horizon={horizon}m: dropped {n_dropped} "
                f"unresolved events ({100*n_dropped/max(1,n_before):.1f}% of {n_before}). "
                f"NOT-FOR-PROMOTION: creates train/serve prior mismatch."
            )
    elif has_res:
        if drop_low_coverage_unresolved and cov_available:
            unresolved_mask = df["resolution_min"].isna()
            low_cov = unresolved_mask & (
                df[cov_col].fillna(0.0) < float(unresolved_min_coverage)
            )
            n_low_cov_dropped = int(low_cov.sum())
            if n_low_cov_dropped > 0:
                df = df[~low_cov].copy()
                _log(
                    f"[data-quality] horizon={horizon}m: dropped {n_low_cov_dropped} "
                    f"unresolved rows below {unresolved_min_coverage:.0%} bar coverage"
                )
        elif drop_low_coverage_unresolved and not cov_available:
            _log(
                f"[data-quality] horizon={horizon}m: --drop-low-coverage-unresolved set but "
                f"coverage column '{cov_col}' absent from view; leaving {n_unresolved} "
                f"unresolved rows in as negatives (see build_labels.py follow-up)."
            )
        else:
            _log(
                f"[data-quality] horizon={horizon}m: kept {n_unresolved} unresolved "
                f"(chop) rows as negatives; coverage_column_present={cov_available}"
            )

    info = {
        "foreign_symbol_rows_dropped": int(n_foreign),
        "experimental_filter_unresolved_applied": bool(experimental_filter_applied),
        "unresolved_handling": {
            "unresolved_rows_kept": int(max(0, n_unresolved - n_low_cov_dropped))
            if not experimental_filter_applied
            else 0,
            "unresolved_rows_total": int(n_unresolved),
            "low_coverage_dropped": int(n_low_cov_dropped),
            "coverage_column_present": bool(cov_available),
            "coverage_floor": float(unresolved_min_coverage),
            "experimental_filter_unresolved_applied": bool(experimental_filter_applied),
        },
    }
    return df, info


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


def purge_training_overlap(df, train_mask, calib_mask, *, embargo_minutes: float):
    """Drop training rows whose label window overlaps the calibration slice."""
    pd = require("pandas", "python3 -m pip install pandas")

    train_mask_series = pd.Series(train_mask, index=df.index).astype(bool)
    calib_mask_series = pd.Series(calib_mask, index=df.index).astype(bool)
    train_before = int(train_mask_series.sum())
    diag = {
        "enabled": bool(float(embargo_minutes) > 0.0),
        "embargo_minutes": float(embargo_minutes),
        "calibration_start_ts": None,
        "train_rows_before_purge": train_before,
        "train_rows_after_purge": train_before,
        "train_rows_purged": 0,
        "earliest_purged_ts": None,
        "latest_purged_ts": None,
        "skip_reason": "",
    }

    if float(embargo_minutes) <= 0.0:
        diag["skip_reason"] = "disabled"
        return train_mask_series, diag
    if "ts_event" not in df.columns:
        diag["skip_reason"] = "missing_ts_event"
        return train_mask_series, diag
    if train_before == 0:
        diag["skip_reason"] = "no_training_rows"
        return train_mask_series, diag
    if not bool(calib_mask_series.any()):
        diag["skip_reason"] = "no_calibration_rows"
        return train_mask_series, diag

    ts_values = pd.to_numeric(df["ts_event"], errors="coerce")
    calib_start_ts = ts_values.loc[calib_mask_series].min()
    if pd.isna(calib_start_ts):
        diag["skip_reason"] = "invalid_calibration_start_ts"
        return train_mask_series, diag

    calib_start_ts = int(calib_start_ts)
    embargo_ms = int(round(float(embargo_minutes) * 60_000.0))
    train_ts = ts_values.loc[train_mask_series]
    overlap = (train_ts + embargo_ms) > calib_start_ts
    overlap = overlap.fillna(False)
    purged_index = overlap[overlap].index

    purged_train_mask = train_mask_series.copy()
    if len(purged_index) > 0:
        purged_train_mask.loc[purged_index] = False
        purged_ts = train_ts.loc[purged_index].dropna()
        if not purged_ts.empty:
            diag["earliest_purged_ts"] = int(purged_ts.min())
            diag["latest_purged_ts"] = int(purged_ts.max())

    train_after = int(purged_train_mask.sum())
    diag["calibration_start_ts"] = calib_start_ts
    diag["train_rows_after_purge"] = train_after
    diag["train_rows_purged"] = int(train_before - train_after)
    return purged_train_mask, diag


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
    guarded_threshold = max(0.0, float(no_trade_threshold))
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

    # P1-3 diagnostics (read-only flags).  These do NOT change the gate
    # decision.  They surface "would this horizon survive a zero-sum / zero-
    # mean utility floor?" so operators can spot horizons that ship live with
    # negative aggregate or per-signal edge under the current configured
    # floor (which may be negative, e.g. RF_THRESHOLD_MIN_UTILITY_SCORE=-20).
    # See ml/thresholds.py::compute_utility_gate_diagnostics for semantics.
    try:
        from ml.thresholds import compute_utility_gate_diagnostics as _diag
        threshold_meta.update(_diag(threshold_meta))
    except Exception:  # pragma: no cover — diagnostics must never break training
        pass

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
            "Aggregate utility_bps SCORE floor required for a live threshold. "
            "When threshold_meta.score <= this value and the disable flag is on, "
            "the threshold is set to no-trade.  NOTE: this is a SUM (selected_utility_sum) "
            "comparison, not a per-signal-mean comparison; a negative aggregate and "
            "negative per-signal mean can still pass if the sum is above a negative "
            "configured floor.  See the "
            "diagnostic fields utility_avg_is_negative / would_disable_under_zero_mean "
            "in threshold_meta for the per-signal-mean view."
        ),
    )
    parser.add_argument(
        "--threshold-no-trade-threshold",
        type=float,
        default=_env_float("RF_THRESHOLD_NO_TRADE_THRESHOLD", NO_SIGNAL_THRESHOLD),
        help="Threshold used when risk guard disables a model/target horizon (typically >1.0).",
    )
    parser.add_argument(
        "--threshold-disable-on-nonpositive-utility",
        dest="threshold_disable_on_nonpositive_utility",
        action="store_true",
        default=_env_bool("RF_THRESHOLD_DISABLE_ON_NONPOSITIVE_UTILITY", True),
        help=(
            "Disable thresholds whose selected utility_bps SCORE is "
            "<= --threshold-min-utility-score.  The flag name is historical: "
            "the implementation is 'disable when score <= configured floor', "
            "and the floor can be negative (e.g. -20), so this flag does NOT "
            "guarantee 'no nonpositive score ships live'.  Use the "
            "utility_score_is_negative / utility_avg_is_negative diagnostic "
            "fields in threshold_meta to detect that case explicitly."
        ),
    )
    parser.add_argument(
        "--no-threshold-disable-on-nonpositive-utility",
        dest="threshold_disable_on_nonpositive_utility",
        action="store_false",
        help=(
            "Allow utility thresholds with score below the configured "
            "--threshold-min-utility-score floor.  Same caveat as the "
            "enabling flag: 'nonpositive' is historical wording, the actual "
            "comparison is against the configured floor."
        ),
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
    parser.add_argument(
        "--train-embargo-minutes",
        default=os.getenv("RF_TRAIN_EMBARGO_MINUTES", ""),
        help=(
            "Minutes of embargo applied to training rows whose label window overlaps "
            "the calibration slice. Empty/unset defaults to max(horizons) (in minutes). "
            "Set to '0' to disable purging."
        ),
    )
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--metadata-dir",
        default=DEFAULT_METADATA_DIR,
        help="Directory for runtime metadata manifests (absolute or relative to --out-dir)",
    )
    # ── Walk-forward OOS validation harness ──────────────────────────────────
    # Emits per-signal utility observations on FUTURE test windows (source=
    # walk_forward_fold) so run_retrain_evidence_pack can populate the OOS
    # axis. Default OFF: it is ~K× slower (re-fits per fold) and is only needed
    # on the gated promotion-validation retrain, not routine training. The
    # gated path enables it via RF_WALK_FORWARD_OOS=true. It is PURELY ADDITIVE
    # to the manifest — it never changes the shipped model or threshold.
    parser.add_argument(
        "--walk-forward-oos",
        dest="walk_forward_oos",
        action="store_true",
        default=_env_bool("RF_WALK_FORWARD_OOS", False),
        help="Run the walk-forward OOS harness and emit oos_score_observations (additive).",
    )
    parser.add_argument(
        "--no-walk-forward-oos",
        dest="walk_forward_oos",
        action="store_false",
        help="Disable the walk-forward OOS harness (default).",
    )
    parser.add_argument(
        "--oos-folds",
        type=int,
        default=int(_env_float("RF_OOS_FOLDS", 5)),
        help="Number of expanding walk-forward folds (default 5).",
    )
    parser.add_argument(
        "--oos-test-window",
        type=int,
        default=int(_env_float("RF_OOS_TEST_WINDOW", 1000)),
        help="Rows per fold test window (default 1000).",
    )
    parser.add_argument(
        "--oos-min-train",
        type=int,
        default=int(_env_float("RF_OOS_MIN_TRAIN", 4000)),
        help="Minimum pipeline-fit rows required before fold 0's test window.",
    )
    parser.add_argument(
        "--oos-calib-window",
        type=int,
        default=int(_env_float("RF_OOS_CALIB_WINDOW", 2000)),
        help="Rows reserved (per fold) for calibrator-fit + threshold-tune, pre-test.",
    )
    # ── Data-quality filters ─────────────────────────────────────────────────
    # CRITICAL-3 fix: scope training to a single symbol. The export/view path
    # (export_parquet.py + build_duckdb_view.py) does NOT filter by symbol, so
    # foreign-symbol rows (e.g. SPX index touches collected Nov-2025..Feb-2026,
    # ema9 ~6600 vs SPY ~666) leak into training_events_v1. Symbol scoping is
    # the correct fix; the previous ema9>1000 heuristic was a brittle band-aid.
    parser.add_argument(
        "--symbol",
        dest="symbol",
        default=os.getenv("RF_TRAIN_SYMBOL", "SPY"),
        help=(
            "Restrict training to rows for this symbol (default SPY). The training "
            "view is not symbol-scoped, so this is the authoritative cross-symbol "
            "contamination guard. Set empty string to disable (not recommended)."
        ),
    )
    # CRITICAL-1 REVERTED: unresolved (resolution_min IS NULL) events are
    # FULLY-OBSERVED reject=0/break=0 chops (the adversarial audit proved 0 of
    # 14,030 h=5 / 8,807 h=15 are right-censored). They are real negatives the
    # live model faces, NOT label noise. Dropping them inverted full-population
    # utility to -8,785 bps and broke calibration (ECE 0.28-0.54). Default is now
    # OFF. The flag remains for experiments ONLY and any model trained with it
    # MUST NOT be promoted (it fails full-population OOS by construction).
    parser.add_argument(
        "--filter-unresolved",
        dest="filter_unresolved",
        action="store_true",
        default=_env_bool("RF_FILTER_UNRESOLVED_EVENTS", False),
        help=(
            "[EXPERIMENTAL / NOT-FOR-PROMOTION] Drop events where resolution_min "
            "IS NULL. These are observed reject=0 chops, not unknown outcomes; "
            "removing them creates a train/serve prior mismatch that degrades the "
            "live model. Default OFF. See adversarial audit 2026-05-31."
        ),
    )
    parser.add_argument(
        "--no-filter-unresolved",
        dest="filter_unresolved",
        action="store_false",
        help="Keep unresolved (timeout/chop) events as negatives (default behavior).",
    )
    # Item-3: partial-coverage unresolved rows. An unresolved row whose horizon
    # window has <coverage bar data could be hiding a reject/break in the gap, so
    # it is a less-certain negative than a fully-covered chop. We do NOT silently
    # treat these as clean negatives. Exclusion requires a per-event coverage
    # signal (column named by --coverage-column) which the training view does not
    # yet carry; until build_labels.py persists it, this filter is a documented
    # no-op and the count of unresolved rows is recorded in the manifest instead.
    parser.add_argument(
        "--drop-low-coverage-unresolved",
        dest="drop_low_coverage_unresolved",
        action="store_true",
        default=_env_bool("RF_DROP_LOW_COVERAGE_UNRESOLVED", False),
        help=(
            "Drop unresolved rows whose bar coverage < --unresolved-min-coverage. "
            "Requires the coverage column (--coverage-column) in the view; no-op "
            "with a recorded manifest flag when the column is absent."
        ),
    )
    parser.add_argument(
        "--unresolved-min-coverage",
        type=float,
        default=_env_float("RF_UNRESOLVED_MIN_COVERAGE", 0.8),
        help="Minimum horizon bar coverage for an unresolved row to count as a clean negative (default 0.8).",
    )
    parser.add_argument(
        "--coverage-column",
        default=os.getenv("RF_COVERAGE_COLUMN", "bar_coverage"),
        help="Name of the per-event bar-coverage column in the training view, if present.",
    )
    # ────────────────────────────────────────────────────────────────────────

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

    raw_embargo = (args.train_embargo_minutes or "").strip()
    if raw_embargo == "":
        train_embargo_minutes = float(max(horizons)) if horizons else 0.0
    else:
        try:
            train_embargo_minutes = float(raw_embargo)
        except (TypeError, ValueError):
            train_embargo_minutes = float(max(horizons)) if horizons else 0.0
    train_embargo_minutes = max(0.0, train_embargo_minutes)

    manifest = {
        "version": version,
        "feature_version": FEATURE_VERSION,
        "models": {},
        "calibration": {},
        "thresholds_meta": {},
        "stats": {},
        "gamma_context": _gamma_context_metadata(),
        "trained_end_ts": None,
        "train_embargo_minutes": float(train_embargo_minutes),
        # CRITICAL-1 reverted: default False (unresolved chops kept as negatives).
        # True here means an experimental, NOT-FOR-PROMOTION run.
        "filter_unresolved_events": bool(args.filter_unresolved),
        # CRITICAL-3 fix: symbol scoping replaces the ema9>1000 heuristic.
        "training_symbol": str(args.symbol or ""),
        "drop_low_coverage_unresolved": bool(args.drop_low_coverage_unresolved),
        "unresolved_min_coverage": float(args.unresolved_min_coverage),
    }
    trained_end_ts_max = None
    latest_aliases: list[tuple[Path, Path]] = []

    for horizon in horizons:
        df = load_dataframe(args.db, args.view, horizon, symbol=args.symbol)
        if df.empty:
            print(f"No rows for horizon {horizon}m. Skipping.")
            continue

        df = ensure_event_date(df)
        df = df.sort_values("ts_event")

        # ── Temporal scope: capture raw end-ts BEFORE quality filters ─────
        # trained_end_ts represents the furthest-forward data we considered,
        # not the furthest-forward event that survived filtering.  Unresolved
        # events at the tail have the most-recent timestamps and would shrink
        # trained_end_ts if measured post-filter, causing governance to
        # incorrectly conclude the model covers less time than it actually does.
        if not df.empty and "ts_event" in df.columns:
            raw_end_ts = int(df["ts_event"].max())
            if trained_end_ts_max is None or raw_end_ts > trained_end_ts_max:
                trained_end_ts_max = raw_end_ts

        # ── Data-quality filters (pure, testable) ─────────────────────────
        df, dq_info = apply_quality_filters(
            df,
            horizon=horizon,
            symbol=args.symbol,
            filter_unresolved=bool(args.filter_unresolved),
            drop_low_coverage_unresolved=bool(args.drop_low_coverage_unresolved),
            unresolved_min_coverage=float(args.unresolved_min_coverage),
            coverage_column=str(args.coverage_column or ""),
            verbose=True,
        )
        manifest.setdefault("unresolved_handling", {})[str(horizon)] = dq_info["unresolved_handling"]
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

            train_mask_sub = ~calib_mask_sub
            train_mask_sub, train_purge_diag = purge_training_overlap(
                sub,
                train_mask_sub,
                calib_mask_sub,
                embargo_minutes=float(train_embargo_minutes),
            )
            if train_purge_diag.get("train_rows_purged", 0) > 0:
                print(
                    f"Train-fold purge {target} {horizon}m: "
                    f"{train_purge_diag['train_rows_purged']} rows removed "
                    f"(embargo={train_purge_diag['embargo_minutes']}m)."
                )

            X_train = X.loc[train_mask_sub]
            y_train = y.loc[train_mask_sub]
            if X_train.empty:
                print(
                    f"All training rows purged for {target} {horizon}m after embargo. Skipping."
                )
                continue
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
                "train_purge": train_purge_diag,
            }
            model_obj = calibrator if calibrator is not None else pipeline
            X_calib_set = X_calib_tune if X_calib_tune is not None else X.loc[calib_mask_sub]
            y_calib_for_thresh = y_calib_tune if y_calib_tune is not None else y.loc[X_calib_set.index]
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
                            enforce_min_score=(
                                args.threshold_objective == "utility_bps"
                                and bool(args.threshold_disable_on_nonpositive_utility)
                            ),
                            enforce_no_fallback=(
                                args.threshold_objective == "utility_bps"
                                and bool(args.threshold_disable_on_fallback)
                            ),
                            no_signal_threshold=NO_SIGNAL_THRESHOLD,
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
                        # B3-feed: per-signal utility observations at the SELECTED
                        # threshold on the threshold-tune slice. Downstream consumers
                        # (run_retrain_evidence_pack.classify_candidate_readiness)
                        # use this for statistical validation. The source field is
                        # mandatory so readers cannot mistake this for clean OOS
                        # evidence — it is in-sample on the same slice that picked
                        # the threshold.
                        if selection.score_observations is not None:
                            threshold_meta["score_observations"] = list(
                                selection.score_observations
                            )
                            threshold_meta["score_observations_source"] = (
                                "threshold_tune_slice"
                            )
                            threshold_meta["signals_on_tune_slice"] = int(
                                len(selection.score_observations)
                            )
                        else:
                            threshold_meta["score_observations"] = None
                            threshold_meta["score_observations_source"] = (
                                "threshold_tune_slice"
                            )
                            threshold_meta["signals_on_tune_slice"] = 0
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

            # If post-hoc guards substituted the threshold to the no-signal
            # sentinel, the on-disk threshold no longer fires. Drop captured
            # observations so the manifest's score_observations always refers
            # to the threshold that ships.
            if bool(threshold_meta.get("guard_applied")):
                threshold_meta["score_observations"] = None
                threshold_meta["signals_on_tune_slice"] = 0

            # ── Walk-forward OOS observations (additive; never changes the
            # shipped model/threshold). Re-fits per fold on pre-test data only;
            # the harness enforces the anti-leak invariant internally. ──
            if bool(getattr(args, "walk_forward_oos", False)):
                def _wf_model():
                    return build_pipeline(numeric_cols, categorical_cols, args)

                def _wf_calibrate(_model, _Xcf, _ycf):
                    _method = choose_calibration(args.calibration, len(_Xcf))
                    return ProbabilityCalibrator(_model, _method).fit(_Xcf, _ycf)

                def _wf_select_threshold(_y, _p, _util):
                    return select_threshold(
                        _y,
                        _p,
                        objective=args.threshold_objective,
                        precision_floor=float(threshold_precision_floor),
                        min_signals=int(threshold_min_signals),
                        default_threshold=0.5,
                        utility_per_signal=_util,
                        stability_band=float(threshold_stability_band),
                        preferred_min_score=(
                            float(args.threshold_min_utility_score)
                            if args.threshold_objective == "utility_bps"
                            else None
                        ),
                        enforce_min_score=(
                            args.threshold_objective == "utility_bps"
                            and bool(args.threshold_disable_on_nonpositive_utility)
                        ),
                        enforce_no_fallback=(
                            args.threshold_objective == "utility_bps"
                            and bool(args.threshold_disable_on_fallback)
                        ),
                        no_signal_threshold=NO_SIGNAL_THRESHOLD,
                    )

                def _wf_utility(_r, _s, _t):
                    return utility_bps_for_target(
                        _r, _s, _t, trade_cost_bps=float(args.threshold_trade_cost_bps)
                    )

                try:
                    wf = run_walk_forward_oos(
                        X=X,
                        y=y.to_numpy(),
                        return_bps=sub["return_bps"].to_numpy(),
                        touch_side=sub["touch_side"].to_numpy(),
                        ts=sub["ts_event"].to_numpy(),
                        target=target,
                        model_factory=_wf_model,
                        calibrate_fn=(_wf_calibrate if args.calibration != "none" else None),
                        select_threshold_fn=_wf_select_threshold,
                        utility_fn=_wf_utility,
                        n_folds=int(args.oos_folds),
                        test_window=int(args.oos_test_window),
                        min_train=int(args.oos_min_train),
                        calib_window=int(args.oos_calib_window),
                        fit_fraction=float(args.calib_fit_fraction),
                        min_signals=int(threshold_min_signals),
                        trade_cost_bps=float(args.threshold_trade_cost_bps),
                    )
                    threshold_meta["oos_score_observations"] = wf["oos_score_observations"]
                    threshold_meta["oos_score_observations_source"] = (
                        wf["source"] if wf["feasible"] else None
                    )
                    threshold_meta["signals_on_oos_slice"] = wf["signals_on_oos_slice"]
                    threshold_meta["oos_slice_bounds"] = wf["oos_slice_bounds"]
                    threshold_meta["oos_feasible"] = bool(wf["feasible"])
                    threshold_meta["oos_skip_reason"] = wf["skip_reason"]
                    print(
                        f"[walk-forward-oos] {target}@{horizon}m feasible={wf['feasible']} "
                        f"oos_signals={wf['signals_on_oos_slice']} skip={wf['skip_reason']}"
                    )
                except Exception as exc:  # noqa: BLE001 — additive; never break training
                    threshold_meta["oos_score_observations"] = None
                    threshold_meta["oos_score_observations_source"] = None
                    threshold_meta["signals_on_oos_slice"] = 0
                    threshold_meta["oos_feasible"] = False
                    threshold_meta["oos_skip_reason"] = f"exception:{type(exc).__name__}:{exc}"
                    print(f"[walk-forward-oos] {target}@{horizon}m ERROR: {exc}")

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
