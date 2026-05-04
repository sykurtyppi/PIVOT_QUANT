"""Fixed realized-volatility regime benchmark diagnostics.

This is a descriptive benchmark layer only. It does not train models, tune
thresholds, tune hyperparameters, mutate T9 data, or make edge claims.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_YEAR_DATASETS = [
    {
        "year": "2023",
        "dataset_path": "reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.csv",
        "metadata_path": "reports/model_ready_dataset_smoke/spy_2023-01-03_2023-12-29.metadata.json",
    },
    {
        "year": "2024",
        "dataset_path": "reports/model_ready_dataset_smoke/spy_2024-01-02_2024-12-31.csv",
        "metadata_path": "reports/model_ready_dataset_smoke/spy_2024-01-02_2024-12-31.metadata.json",
    },
    {
        "year": "2025",
        "dataset_path": "reports/model_ready_dataset_smoke/spy_2025-01-02_2025-12-31.csv",
        "metadata_path": "reports/model_ready_dataset_smoke/spy_2025-01-02_2025-12-31.metadata.json",
    },
]
SMALL_SAMPLE_THRESHOLD = 30


def discover_year_datasets(
    years: list[str],
    *,
    datasets_dir: Path | str = "reports/model_ready_dataset_smoke",
    symbol: str = "spy",
) -> list[dict[str, str]]:
    """Build a year_datasets list by discovering dataset paths from a directory.

    Looks for files matching ``{symbol}_{year}-*.parquet`` first, then
    ``{symbol}_{year}-*.csv`` as fallback. Returns one dict per requested year.
    When no file matches, the dict has ``status='missing'`` so the validation
    pipeline can surface a clear warning.

    Examples
    --------
    >>> discover_year_datasets(["2020", "2021", "2022"])
    [{'year': '2020', 'dataset_path': '.../spy_2020-...parquet', ...}, ...]
    """
    base = Path(datasets_dir).expanduser().resolve()
    out: list[dict[str, str]] = []
    for year in years:
        prefix = f"{symbol.lower()}_{year}-"
        parquet_candidates = sorted(base.glob(f"{prefix}*.parquet"))
        csv_candidates = sorted(base.glob(f"{prefix}*.csv"))
        candidates = parquet_candidates or csv_candidates
        if not candidates:
            out.append(
                {
                    "year": str(year),
                    "status": "missing",
                    "dataset_path": str(base / f"{prefix}*.{{parquet,csv}}"),
                    "metadata_path": None,
                    "reason": (
                        f"no dataset file matching {prefix}*.parquet or"
                        f" {prefix}*.csv found in {base};"
                        " run scripts/run_model_ready_dataset_oneyear_smoke.py first"
                    ),
                }
            )
            continue
        # Prefer the file covering the largest date range (longest stem).
        chosen = max(candidates, key=lambda p: len(p.stem))
        meta = chosen.with_suffix("").with_name(chosen.stem + ".metadata.json")
        out.append(
            {
                "year": str(year),
                "dataset_path": str(chosen),
                "metadata_path": str(meta) if meta.exists() else None,
            }
        )
    return out


@dataclass(frozen=True)
class MLRegimeBenchmarkResult:
    report: dict[str, Any]

    def json_report(self) -> str:
        return json.dumps(self.report, indent=2, default=str) + "\n"


def run_ml_regime_benchmark(
    *,
    symbol: str = "SPY",
    year_datasets: list[dict[str, str]] | None = None,
    small_sample_threshold: int = SMALL_SAMPLE_THRESHOLD,
) -> MLRegimeBenchmarkResult:
    yearly_inputs = year_datasets or DEFAULT_YEAR_DATASETS
    year_frames: list[dict[str, Any]] = []
    for item in yearly_inputs:
        dataset_path = Path(item["dataset_path"]).expanduser().resolve()
        metadata_path = Path(item.get("metadata_path", "")).expanduser().resolve() if item.get("metadata_path") else None
        if not dataset_path.exists():
            year_frames.append(
                {
                    "year": str(item["year"]),
                    "status": "missing",
                    "dataset_path": str(dataset_path),
                    "metadata_path": str(metadata_path) if metadata_path else None,
                    "reason": "dataset file missing",
                }
            )
            continue
        year_frames.append(
            {
                "year": str(item["year"]),
                "status": "ok",
                "dataset_path": str(dataset_path),
                "metadata_path": str(metadata_path) if metadata_path and metadata_path.exists() else None,
                "metadata": _read_json(metadata_path) if metadata_path and metadata_path.exists() else {},
                "frame": _read_dataset(dataset_path),
            }
        )
    return build_regime_benchmark_report(
        symbol=symbol,
        year_frames=year_frames,
        small_sample_threshold=small_sample_threshold,
    )


def build_regime_benchmark_report(
    *,
    symbol: str,
    year_frames: list[dict[str, Any]],
    small_sample_threshold: int = SMALL_SAMPLE_THRESHOLD,
) -> MLRegimeBenchmarkResult:
    warnings = [
        "regime benchmark diagnostics are inspection-only; no model training, threshold tuning, hyperparameter tuning, strategy change, or edge claim is performed",
    ]
    year_reports: list[dict[str, Any]] = []
    successful_frames: list[pd.DataFrame] = []
    for item in year_frames:
        if item.get("status") != "ok":
            year_reports.append({key: value for key, value in item.items() if key != "frame"})
            warnings.append(f"{item['year']} regime benchmark skipped: {item.get('reason', item.get('status'))}")
            continue
        frame = _prepare_frame(item["frame"])
        year_report = _year_benchmark(
            year=str(item["year"]),
            frame=frame,
            metadata=item.get("metadata") or {},
            dataset_path=item.get("dataset_path"),
            metadata_path=item.get("metadata_path"),
            small_sample_threshold=small_sample_threshold,
        )
        year_reports.append(year_report)
        if year_report["status"] == "ok":
            successful_frames.append(frame.assign(_benchmark_year=str(item["year"])))
        else:
            warnings.extend(year_report.get("warnings", []))

    overall_report = _overall_benchmark(successful_frames, small_sample_threshold=small_sample_threshold)
    stability = _stability_summary(year_reports, overall_report)
    report = {
        "name": "ml_regime_benchmark",
        "status": "warn",
        "symbol": symbol.upper(),
        "read_only": True,
        "training_performed": False,
        "hyperparameter_tuning_performed": False,
        "threshold_optimization_performed": False,
        "performance_claim": False,
        "small_sample_threshold": int(small_sample_threshold),
        "year_count": int(len(year_reports)),
        "successful_year_count": int(sum(1 for year in year_reports if year.get("status") == "ok")),
        "year_reports": year_reports,
        "overall": overall_report,
        "stability_summary": stability,
        "warnings": _dedupe(warnings),
        "explicit_warning": "no edge claim",
    }
    return MLRegimeBenchmarkResult(report=report)


def write_ml_regime_benchmark_report(report: dict[str, Any], *, reports_dir: Path | None = None) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "ml_diagnostics"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    years = "-".join(str(year.get("year")) for year in report.get("year_reports", []))
    stem = f"{report['symbol'].lower()}_{years}_{report['name']}".replace("/", "-")
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _year_benchmark(
    *,
    year: str,
    frame: pd.DataFrame,
    metadata: dict[str, Any],
    dataset_path: str | None,
    metadata_path: str | None,
    small_sample_threshold: int,
) -> dict[str, Any]:
    required = ["realized_vol_60d", "forward_return_5d"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        return {
            "year": year,
            "status": "missing_regime_feature",
            "dataset_path": dataset_path,
            "metadata_path": metadata_path,
            "missing_columns": missing,
            "warnings": [f"missing required benchmark column(s): {', '.join(missing)}"],
        }
    working = frame.dropna(subset=required).copy()
    if working.empty:
        return {
            "year": year,
            "status": "empty",
            "dataset_path": dataset_path,
            "metadata_path": metadata_path,
            "warnings": ["no rows available after requiring realized_vol_60d and forward_return_5d"],
        }
    median_vol = pd.to_numeric(working["realized_vol_60d"], errors="coerce").median()
    groups = _benchmark_groups(working, median_vol=median_vol, small_sample_threshold=small_sample_threshold)
    baseline = groups["all_rows"]
    comparisons = {
        name: _compare_to_baseline(group, baseline)
        for name, group in groups.items()
        if name != "all_rows"
    }
    warnings = [
        warning
        for group in groups.values()
        for warning in group.get("warnings", [])
    ]
    return {
        "year": year,
        "status": "ok",
        "analysis_start_date": metadata.get("analysis_start_date", _date_min(working)),
        "analysis_end_date": metadata.get("analysis_end_date", _date_max(working)),
        "actual_start_date": _date_min(working),
        "actual_end_date": _date_max(working),
        "dataset_path": dataset_path,
        "metadata_path": metadata_path,
        "realized_vol_60d_split": {
            "method": "per_year_median",
            "median": _safe_float(median_vol),
        },
        "benchmarks": groups,
        "comparisons_to_all_rows": comparisons,
        "warnings": _dedupe(warnings),
    }


def _overall_benchmark(frames: list[pd.DataFrame], *, small_sample_threshold: int) -> dict[str, Any]:
    if not frames:
        return {
            "status": "missing",
            "reason": "no successful yearly frames",
        }
    combined = pd.concat(frames, ignore_index=True)
    required = ["realized_vol_60d", "forward_return_5d"]
    working = combined.dropna(subset=required).copy()
    if working.empty:
        return {
            "status": "empty",
            "reason": "no combined rows after requiring realized_vol_60d and forward_return_5d",
        }
    median_vol = pd.to_numeric(working["realized_vol_60d"], errors="coerce").median()
    groups = _benchmark_groups(working, median_vol=median_vol, small_sample_threshold=small_sample_threshold)
    baseline = groups["all_rows"]
    return {
        "status": "ok",
        "actual_start_date": _date_min(working),
        "actual_end_date": _date_max(working),
        "realized_vol_60d_split": {
            "method": "combined_median",
            "median": _safe_float(median_vol),
        },
        "benchmarks": groups,
        "comparisons_to_all_rows": {
            name: _compare_to_baseline(group, baseline)
            for name, group in groups.items()
            if name != "all_rows"
        },
    }


def _benchmark_groups(frame: pd.DataFrame, *, median_vol: float, small_sample_threshold: int) -> dict[str, dict[str, Any]]:
    high = frame[pd.to_numeric(frame["realized_vol_60d"], errors="coerce") >= median_vol].copy()
    low = frame[pd.to_numeric(frame["realized_vol_60d"], errors="coerce") < median_vol].copy()
    return {
        "all_rows": _benchmark_stats("all_rows", frame, small_sample_threshold=small_sample_threshold),
        "realized_vol_60d_high": _benchmark_stats("realized_vol_60d_high", high, small_sample_threshold=small_sample_threshold),
        "realized_vol_60d_low": _benchmark_stats("realized_vol_60d_low", low, small_sample_threshold=small_sample_threshold),
    }


def _benchmark_stats(name: str, frame: pd.DataFrame, *, small_sample_threshold: int) -> dict[str, Any]:
    forward_5d = pd.to_numeric(frame.get("forward_return_5d", pd.Series(dtype="float64")), errors="coerce").dropna()
    warnings: list[str] = []
    if len(forward_5d) < small_sample_threshold:
        warnings.append(f"{name} sample size {len(forward_5d)} below {small_sample_threshold}")
    return {
        "name": name,
        "rows": int(len(forward_5d)),
        "positive_rate": _safe_float((forward_5d > 0).mean()) if not forward_5d.empty else None,
        "mean_forward_return_5d": _safe_float(forward_5d.mean()) if not forward_5d.empty else None,
        "median_forward_return_5d": _safe_float(forward_5d.median()) if not forward_5d.empty else None,
        "target_comparison": _target_comparison(frame),
        "warnings": warnings,
    }


def _target_comparison(frame: pd.DataFrame) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for column in ["forward_return_1d", "forward_return_5d", "forward_return_21d"]:
        if column not in frame.columns:
            output[f"{column}_positive"] = {"available": False}
            continue
        values = pd.to_numeric(frame[column], errors="coerce").dropna()
        output[f"{column}_positive"] = {
            "available": True,
            "rows": int(len(values)),
            "positive_rate": _safe_float((values > 0).mean()) if not values.empty else None,
            "mean": _safe_float(values.mean()) if not values.empty else None,
            "median": _safe_float(values.median()) if not values.empty else None,
        }
    if "forward_volatility_21d" in frame.columns:
        values = pd.to_numeric(frame["forward_volatility_21d"], errors="coerce").dropna()
        output["forward_volatility_21d"] = {
            "available": True,
            "rows": int(len(values)),
            "mean": _safe_float(values.mean()) if not values.empty else None,
            "median": _safe_float(values.median()) if not values.empty else None,
        }
    else:
        output["forward_volatility_21d"] = {"available": False}
    return output


def _compare_to_baseline(group: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "positive_rate_delta": _none_safe_subtract(group.get("positive_rate"), baseline.get("positive_rate")),
        "mean_forward_return_5d_delta": _none_safe_subtract(group.get("mean_forward_return_5d"), baseline.get("mean_forward_return_5d")),
        "median_forward_return_5d_delta": _none_safe_subtract(group.get("median_forward_return_5d"), baseline.get("median_forward_return_5d")),
        "rows_delta": int(group.get("rows", 0) - baseline.get("rows", 0)),
    }


def _stability_summary(year_reports: list[dict[str, Any]], overall: dict[str, Any]) -> dict[str, Any]:
    successful = [year for year in year_reports if year.get("status") == "ok"]
    high_deltas = [
        year.get("comparisons_to_all_rows", {}).get("realized_vol_60d_high", {}).get("positive_rate_delta")
        for year in successful
    ]
    low_deltas = [
        year.get("comparisons_to_all_rows", {}).get("realized_vol_60d_low", {}).get("positive_rate_delta")
        for year in successful
    ]
    high_signs = [_sign(delta) for delta in high_deltas if delta is not None]
    low_signs = [_sign(delta) for delta in low_deltas if delta is not None]
    high_persists = bool(high_signs and len(set(high_signs)) == 1 and high_signs[0] == "positive")
    low_persists = bool(low_signs and len(set(low_signs)) == 1 and low_signs[0] == "negative")
    overall_high = overall.get("comparisons_to_all_rows", {}).get("realized_vol_60d_high", {}) if overall.get("status") == "ok" else {}
    overall_low = overall.get("comparisons_to_all_rows", {}).get("realized_vol_60d_low", {}) if overall.get("status") == "ok" else {}
    high_large = _is_interesting(overall_high)
    low_large = _is_interesting(overall_low)
    return {
        "successful_year_count": int(len(successful)),
        "high_vol_improves_positive_rate_each_year": high_persists,
        "low_vol_underperforms_positive_rate_each_year": low_persists,
        "directionally_stable": bool(high_persists or low_persists),
        "interesting_effect_size": bool(high_large or low_large),
        "high_vol_positive_rate_deltas": high_deltas,
        "low_vol_positive_rate_deltas": low_deltas,
        "overall_high_vol_comparison": overall_high,
        "overall_low_vol_comparison": overall_low,
        "interpretation": _interpretation(high_persists, low_persists, high_large, low_large, len(successful)),
        "note": "benchmark stability is descriptive only and is not an edge claim",
    }


def _is_interesting(comparison: dict[str, Any]) -> bool:
    positive_delta = comparison.get("positive_rate_delta")
    mean_delta = comparison.get("mean_forward_return_5d_delta")
    return bool(
        (positive_delta is not None and abs(float(positive_delta)) >= 0.05)
        or (mean_delta is not None and abs(float(mean_delta)) >= 0.002)
    )


def _interpretation(high_persists: bool, low_persists: bool, high_large: bool, low_large: bool, year_count: int) -> str:
    if year_count < 2:
        return "needs more bounded years before assessing persistence"
    if (high_persists or low_persists) and (high_large or low_large):
        return "realized_vol_60d conditioning persisted directionally and had a descriptive effect size worth further validation"
    if high_persists or low_persists:
        return "realized_vol_60d conditioning persisted directionally, but effect size was modest"
    return "realized_vol_60d conditioning did not persist directionally across years"


def _prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    if "entry_date" in output.columns:
        output["entry_date"] = pd.to_datetime(output["entry_date"], errors="coerce")
        output = output.dropna(subset=["entry_date"]).sort_values("entry_date").reset_index(drop=True)
    return output


def _read_dataset(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _none_safe_subtract(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return float(left) - float(right)


def _sign(value: float | None, *, epsilon: float = 1e-9) -> str:
    if value is None or pd.isna(value) or abs(float(value)) <= epsilon:
        return "flat"
    return "positive" if float(value) > 0 else "negative"


def _date_min(frame: pd.DataFrame) -> str | None:
    if frame.empty or "entry_date" not in frame.columns:
        return None
    dates = pd.to_datetime(frame["entry_date"], errors="coerce").dropna()
    return dates.min().date().isoformat() if not dates.empty else None


def _date_max(frame: pd.DataFrame) -> str | None:
    if frame.empty or "entry_date" not in frame.columns:
        return None
    dates = pd.to_datetime(frame["entry_date"], errors="coerce").dropna()
    return dates.max().date().isoformat() if not dates.empty else None


def _safe_float(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def _dedupe(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output
