#!/usr/bin/env python3
"""Retrain dry-run evidence pack (mechanics only).

Invokes ``scripts/train_rf_artifacts.py`` against the real training DuckDB
into an isolated ``--out-dir`` and emits a structured JSON report
covering:

- provenance (git sha, python, RF/ML env snapshot, active manifest path
  + version + sha256),
- the full candidate manifest (verbatim),
- a per-(target, horizon) summary (threshold, objective, score, signals,
  fallback, no-signal substitution, train_purge diagnostic,
  calibration/threshold-tune sizes),
- a runtime-safety dry-run that re-applies
  ``ModelRegistry._apply_runtime_threshold_safety`` against the candidate
  thresholds and reports which would be neutralized at serve time,
- summary counters.

No assertions, no adversarial folds, no governance. Policy gates land
in B2; this pack only proves the safer training path runs end to end
and produces a parseable artifact the runtime would accept.

Safety mechanics:
- Refuses to run if ``--out-dir`` resolves under the live model dir or
  vice versa.
- Refuses to run if the active manifest resolves inside ``--out-dir``.
- Reads the active manifest read-only for provenance; never writes
  outside ``--out-dir`` and the report destination.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import importlib.util
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

SCHEMA_VERSION = 1

DEFAULT_ACTIVE_MANIFEST_NAME = (
    os.getenv("RF_ACTIVE_MANIFEST", "manifest_active.json").strip()
    or "manifest_active.json"
)
DEFAULT_CANDIDATE_MANIFEST_NAME = (
    os.getenv("RF_CANDIDATE_MANIFEST", "manifest_runtime_latest.json").strip()
    or "manifest_runtime_latest.json"
)
DEFAULT_LIVE_MODEL_DIR = os.getenv("RF_MODEL_DIR", "data/models")

# Env keys --pass-through is not allowed to override. These either control
# where artifacts land (RF_MODEL_DIR / RF_METADATA_DIR -- the isolation
# guarantees) or where the candidate manifest is read from after training
# (RF_CANDIDATE_MANIFEST -- the pack already exposes its own
# --candidate-manifest flag). Allowing silent overrides via --pass-through
# would defeat the safety contract. RF_METADATA_DIR mirrors the
# --metadata-dir CLI protection: the train script accepts it as absolute,
# so the env equivalent is the same write-redirect risk.
PROTECTED_ENV_KEYS = frozenset(
    {"RF_MODEL_DIR", "RF_METADATA_DIR", "RF_CANDIDATE_MANIFEST"}
)

# CLI flags --train-arg is not allowed to forward. argparse takes the last
# occurrence, so a trailing --train-arg --out-dir=data/models would silently
# override the controlled --out-dir we put earlier in the command and
# redirect writes back to the live model tree. --metadata-dir is protected
# for the same reason -- the train script lets it be an absolute path and
# would otherwise write metadata_v*.json outside the isolated tree.
PROTECTED_TRAIN_ARGS = frozenset({"--out-dir", "--candidate-manifest", "--metadata-dir"})

RF_ENV_KEYS = (
    "RF_TRAIN_EMBARGO_MINUTES",
    "RF_THRESHOLD_OBJECTIVE",
    "RF_THRESHOLD_PRECISION_FLOOR",
    "RF_THRESHOLD_MIN_SIGNALS",
    "RF_THRESHOLD_MIN_UTILITY_SCORE",
    "RF_THRESHOLD_NO_TRADE_THRESHOLD",
    "RF_THRESHOLD_STABILITY_BAND",
    "RF_THRESHOLD_DISABLE_ON_NONPOSITIVE_UTILITY",
    "RF_THRESHOLD_DISABLE_ON_FALLBACK",
    "RF_THRESHOLD_TRADE_COST_BPS",
    "RF_THRESHOLD_PRECISION_FLOOR_OVERRIDES",
    "RF_THRESHOLD_MIN_SIGNALS_OVERRIDES",
    "RF_CALIB_FIT_FRACTION",
    "RF_CALIB_MIN_FIT_EVENTS",
    "RF_CALIB_DAYS",
    "ML_COST_SPREAD_BPS",
    "ML_COST_SLIPPAGE_BPS",
    "ML_COST_COMMISSION_BPS",
    "RF_MODEL_DIR",
    "RF_ACTIVE_MANIFEST",
    "RF_CANDIDATE_MANIFEST",
    "DUCKDB_PATH",
    "DUCKDB_VIEW",
)


def _resolve(path_str: str) -> Path:
    return Path(path_str).expanduser().resolve()


def _is_within(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def validate_out_dir(
    out_dir: Path,
    live_model_dir: Path,
    active_manifest_path: Path,
) -> None:
    """Fail-closed if ``out_dir`` overlaps live artifact paths."""
    if out_dir == live_model_dir:
        raise SystemExit(
            f"--out-dir resolves to the live model dir ({live_model_dir}); refusing to run."
        )
    if _is_within(out_dir, live_model_dir):
        raise SystemExit(
            f"--out-dir ({out_dir}) is inside the live model dir ({live_model_dir}); refusing to run."
        )
    if _is_within(live_model_dir, out_dir):
        raise SystemExit(
            f"Live model dir ({live_model_dir}) would be inside --out-dir ({out_dir}); refusing to run."
        )
    if _is_within(active_manifest_path, out_dir):
        raise SystemExit(
            f"Active manifest ({active_manifest_path}) resolves inside --out-dir ({out_dir}); refusing to run."
        )


def _git_head() -> tuple[str, bool]:
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(ROOT),
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        sha = "<unknown>"
    try:
        porcelain = subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=str(ROOT),
            stderr=subprocess.DEVNULL,
        ).decode()
        dirty = bool(porcelain.strip())
    except Exception:
        dirty = False
    return sha, dirty


def _python_version_tuple(executable: str) -> tuple[int, int, int] | None:
    """Return (major, minor, micro) for an interpreter, or None on failure."""
    try:
        out = subprocess.check_output(
            [executable, "-c", "import sys; print('%d.%d.%d' % sys.version_info[:3])"],
            stderr=subprocess.DEVNULL,
            timeout=10,
        ).decode().strip()
        parts = [int(p) for p in out.split(".")[:3]]
        while len(parts) < 3:
            parts.append(0)
        return (parts[0], parts[1], parts[2])
    except Exception:
        return None


def resolve_training_python() -> tuple[str, str, str]:
    """Pick the interpreter used to spawn ``train_rf_artifacts.py``.

    Resolution order:
      1. ``PYTHON_BIN`` env var, if set and executable.
      2. ``<ROOT>/.venv313/bin/python``, if present.
      3. ``<ROOT>/.venv/bin/python``, if present.
      4. ``sys.executable``, if its version is >= 3.10.
      5. Otherwise SystemExit with the missing-prerequisite message.

    Returns ``(executable, version, source)`` where ``version`` is
    ``"MAJOR.MINOR.MICRO"`` and ``source`` is the label of the rule that
    matched (e.g. ``".venv/bin/python"`` or ``"PYTHON_BIN"``).

    The training script (``scripts/train_rf_artifacts.py``) uses Python 3.10+
    type syntax (``int | None``) at module level, so Python 3.9 cannot import
    it and must be rejected here rather than failing opaquely inside the
    subprocess.
    """
    candidates: list[tuple[str, str]] = []

    env_bin = os.environ.get("PYTHON_BIN", "").strip()
    if env_bin:
        candidates.append(("PYTHON_BIN", env_bin))

    candidates.append((".venv313/bin/python", str(ROOT / ".venv313" / "bin" / "python")))
    candidates.append((".venv/bin/python", str(ROOT / ".venv" / "bin" / "python")))
    candidates.append(("sys.executable", sys.executable))

    seen: set[str] = set()
    tried: list[str] = []
    for label, path in candidates:
        if not path or path in seen:
            continue
        seen.add(path)
        # PYTHON_BIN / venv paths must exist on disk to be usable; sys.executable
        # always exists by definition but may still be the wrong version.
        if label != "sys.executable" and not Path(path).is_file():
            tried.append(f"{label}={path} (not present)")
            continue
        version = _python_version_tuple(path)
        if version is None:
            tried.append(f"{label}={path} (probe failed)")
            continue
        if version < (3, 10):
            tried.append(f"{label}={path} (version {version[0]}.{version[1]}.{version[2]} < 3.10)")
            continue
        return path, "%d.%d.%d" % version, label

    raise SystemExit(
        "Could not resolve a Python >= 3.10 to run train_rf_artifacts.py. "
        "Tried in order: " + "; ".join(tried) + ". "
        "Set PYTHON_BIN, create .venv/ with a 3.10+ interpreter, or run the "
        "evidence pack under a 3.10+ python."
    )


def build_provenance(active_manifest_path: Path) -> dict:
    sha, dirty = _git_head()
    rf_env = {key: os.environ.get(key, "") for key in RF_ENV_KEYS}
    active_version = None
    active_signature = None
    active_path_resolved = str(active_manifest_path)
    if active_manifest_path.exists():
        try:
            raw = active_manifest_path.read_bytes()
            active_signature = hashlib.sha256(raw).hexdigest()
            parsed = json.loads(raw.decode())
            if isinstance(parsed, dict):
                active_version = parsed.get("version")
        except Exception:
            # Provenance is best-effort; B1 reports what it sees and moves on.
            pass
    return {
        "git_sha": sha,
        "git_dirty": dirty,
        "python_executable": sys.executable,
        "python_version": sys.version.split()[0],
        "evidence_pack_python_executable": sys.executable,
        "evidence_pack_python_version": sys.version.split()[0],
        "rf_env_snapshot": rf_env,
        "active_manifest_path": active_path_resolved,
        "active_manifest_exists": active_manifest_path.exists(),
        "active_manifest_version": active_version,
        "active_manifest_signature": active_signature,
    }


def _load_ml_server_module() -> tuple[object | None, str | None]:
    """Best-effort load of server/ml_server.py for the runtime-safety dry-run.

    Imported via importlib so callers (tests, CI without fastapi) can still
    use the rest of the pack. Returns (module, None) on success or
    (None, reason) when unavailable.
    """
    try:
        spec = importlib.util.spec_from_file_location(
            "ml_server_for_evidence_pack",
            ROOT / "server" / "ml_server.py",
        )
        if spec is None or spec.loader is None:
            return None, "spec_unavailable"
        module = importlib.util.module_from_spec(spec)
        sys.modules.setdefault(spec.name, module)
        spec.loader.exec_module(module)
        return module, None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _no_signal_sentinel() -> float:
    try:
        from ml.thresholds import NO_SIGNAL_THRESHOLD

        return float(NO_SIGNAL_THRESHOLD)
    except Exception:
        return float("inf")


def thresholds_from_manifest(manifest: dict) -> dict:
    """Convert manifest['thresholds'] to the {target: {int(horizon): float}} runtime form."""
    raw = manifest.get("thresholds", {})
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[int, float]] = {}
    for target, horizon_map in raw.items():
        if not isinstance(horizon_map, dict):
            continue
        target_out: dict[int, float] = {}
        for horizon_key, value in horizon_map.items():
            try:
                horizon = int(str(horizon_key).rstrip("m"))
                target_out[horizon] = float(value)
            except (TypeError, ValueError):
                continue
        if target_out:
            out[target] = target_out
    return out


def _reason_for_neutralization(meta: dict) -> str:
    parts: list[str] = []
    if bool(meta.get("fallback")):
        parts.append("fallback")
    score = meta.get("score")
    if score is None:
        parts.append("none_score")
    else:
        try:
            score_f = float(score)
            if not math.isfinite(score_f):
                parts.append("nonfinite_score")
            elif score_f <= 0.0:
                parts.append("nonpositive_score")
        except (TypeError, ValueError):
            parts.append("uncoercible_score")
    if meta.get("objective") != "utility_bps":
        parts.append("non_utility_objective")
    return ",".join(parts) if parts else "unknown"


def runtime_safety_dry_run(
    thresholds: dict[str, dict[int, float]],
    manifest: dict,
) -> dict:
    module, skip_reason = _load_ml_server_module()
    if module is None:
        return {
            "skipped": True,
            "skip_reason": skip_reason,
            "would_neutralize_count": 0,
            "would_neutralize": [],
        }

    try:
        sentinel = float(getattr(module, "NO_SIGNAL_THRESHOLD"))
        registry_cls = getattr(module, "ModelRegistry")
        apply_safety = getattr(registry_cls, "_apply_runtime_threshold_safety")
    except Exception as exc:
        return {
            "skipped": True,
            "skip_reason": f"symbol_missing: {type(exc).__name__}: {exc}",
            "would_neutralize_count": 0,
            "would_neutralize": [],
        }

    original = copy.deepcopy(thresholds)
    candidate = copy.deepcopy(thresholds)
    try:
        apply_safety(candidate, manifest)
    except Exception as exc:
        return {
            "skipped": True,
            "skip_reason": f"apply_failed: {type(exc).__name__}: {exc}",
            "would_neutralize_count": 0,
            "would_neutralize": [],
        }

    neutralized: list[dict] = []
    for target, horizon_map in candidate.items():
        for horizon, value in horizon_map.items():
            original_value = float(original.get(target, {}).get(int(horizon), 0.0))
            new_value = float(value)
            if new_value >= sentinel and original_value < sentinel:
                meta = (
                    manifest.get("thresholds_meta", {})
                    .get(target, {})
                    .get(str(int(horizon)), {})
                    if isinstance(manifest.get("thresholds_meta"), dict)
                    else {}
                )
                neutralized.append(
                    {
                        "target": target,
                        "horizon": int(horizon),
                        "original_threshold": original_value,
                        "reason": _reason_for_neutralization(meta if isinstance(meta, dict) else {}),
                        "score": (meta or {}).get("score") if isinstance(meta, dict) else None,
                    }
                )

    return {
        "skipped": False,
        "no_signal_sentinel": sentinel,
        "would_neutralize_count": len(neutralized),
        "would_neutralize": neutralized,
    }


def _row_for_horizon(
    target: str,
    horizon: int,
    threshold_value: float,
    meta: dict,
    sentinel: float,
) -> dict:
    return {
        "target": target,
        "horizon": int(horizon),
        "threshold": float(threshold_value),
        "objective": meta.get("objective"),
        "score": meta.get("score"),
        "signals": meta.get("signals"),
        "fallback": bool(meta.get("fallback")),
        "no_signal_substituted": bool(float(threshold_value) >= sentinel),
        "train_purge": meta.get("train_purge"),
        "calibration_fit_size": meta.get("calibration_fit_size"),
        "threshold_tune_size": meta.get("threshold_tune_size"),
        # B3: per-signal utility observations, if the training script
        # captured them. Today's manifests do not; the validator reports
        # ``insufficient_data`` for such horizons.
        "score_observations": meta.get("score_observations"),
    }


def per_horizon_summary(manifest: dict, sentinel: float) -> list[dict]:
    thresholds_meta = manifest.get("thresholds_meta", {})
    thresholds = manifest.get("thresholds", {})
    if not isinstance(thresholds_meta, dict):
        return []
    if not isinstance(thresholds, dict):
        thresholds = {}

    rows: list[dict] = []
    for target, horizon_meta_map in thresholds_meta.items():
        if not isinstance(horizon_meta_map, dict):
            continue
        target_thresholds = thresholds.get(target, {}) if isinstance(thresholds.get(target), dict) else {}
        for horizon_key, meta in horizon_meta_map.items():
            if not isinstance(meta, dict):
                continue
            try:
                horizon = int(str(horizon_key).rstrip("m"))
            except (TypeError, ValueError):
                continue
            threshold_value = target_thresholds.get(str(horizon), target_thresholds.get(horizon))
            if threshold_value is None:
                threshold_value = sentinel
            try:
                threshold_value = float(threshold_value)
            except (TypeError, ValueError):
                threshold_value = float(sentinel)
            rows.append(_row_for_horizon(target, horizon, threshold_value, meta, sentinel))
    return rows


def _sum_train_purge(rows: list[dict]) -> int:
    total = 0
    for row in rows:
        purge = row.get("train_purge")
        if not isinstance(purge, dict):
            continue
        try:
            total += int(purge.get("train_rows_purged", 0) or 0)
        except (TypeError, ValueError):
            continue
    return total


def summary_block(rows: list[dict]) -> dict:
    return {
        "models_attempted": len(rows),
        "models_with_no_signal": sum(1 for row in rows if row.get("no_signal_substituted")),
        "models_with_fallback_threshold": sum(1 for row in rows if row.get("fallback")),
        "models_with_negative_utility": sum(
            1
            for row in rows
            if row.get("objective") == "utility_bps"
            and isinstance(row.get("score"), (int, float))
            and float(row["score"]) <= 0.0
        ),
        "total_train_rows_purged": _sum_train_purge(rows),
    }


def invoke_training(
    out_dir: Path,
    pass_through_env: dict[str, str],
    extra_args: list[str],
    *,
    candidate_manifest_name: str,
) -> dict:
    env = os.environ.copy()
    env.update(pass_through_env)
    # Re-assert isolation AFTER pass_through_env so a misconfigured caller
    # cannot accidentally redirect artifact writes back to the live model
    # dir. parse_pass_through already rejects PROTECTED_ENV_KEYS, but this
    # is defense-in-depth: the dry-run contract is non-publishing.
    # RF_METADATA_DIR is pinned to the relative "metadata_runtime" so it
    # resolves inside out_dir via _resolve_metadata_dir in the train script.
    env["RF_MODEL_DIR"] = str(out_dir)
    env["RF_METADATA_DIR"] = "metadata_runtime"
    env["RF_CANDIDATE_MANIFEST"] = candidate_manifest_name
    training_python, training_python_version, training_python_source = resolve_training_python()
    cmd = [
        training_python,
        str(ROOT / "scripts" / "train_rf_artifacts.py"),
        "--out-dir",
        str(out_dir),
        "--metadata-dir",
        "metadata_runtime",
        "--candidate-manifest",
        candidate_manifest_name,
        *extra_args,
    ]
    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            cwd=str(ROOT),
            check=False,
        )
        exit_code = result.returncode
        stdout_tail = (result.stdout or "")[-4000:]
        stderr_tail = (result.stderr or "")[-4000:]
        error = None
    except Exception as exc:
        exit_code = -1
        stdout_tail = ""
        stderr_tail = ""
        error = f"{type(exc).__name__}: {exc}"

    return {
        "cmd": cmd,
        "exit_code": int(exit_code),
        "duration_seconds": round(time.time() - start, 3),
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
        "error": error,
        "training_python_executable": training_python,
        "training_python_version": training_python_version,
        "training_python_resolution_source": training_python_source,
    }


def parse_train_args(values: list[str]) -> list[str]:
    """Validate forwarded train args; reject anything that could override
    the safety-critical flags we set in invoke_training()."""
    for item in values or []:
        for protected in PROTECTED_TRAIN_ARGS:
            if item == protected or item.startswith(protected + "="):
                raise SystemExit(
                    f"--train-arg {item!r} is not allowed; {protected} is controlled by "
                    "this script to preserve the dry-run isolation contract."
                )
    return list(values or [])


def parse_pass_through(values: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in values or []:
        if "=" not in item:
            raise SystemExit(f"--pass-through expects KEY=VALUE, got {item!r}")
        key, value = item.split("=", 1)
        key = key.strip()
        if key in PROTECTED_ENV_KEYS:
            raise SystemExit(
                f"--pass-through {key} is not allowed; it is controlled by this script "
                "to preserve the dry-run isolation contract."
            )
        parsed[key] = value
    return parsed


def _purge_diagnostic_state(per_horizon: list[dict]) -> tuple[str, list[str]]:
    """Aggregate per-horizon ``train_purge`` blocks into one of:

    ``valid_noop``  — purge enabled, ran, found nothing to drop (current data layout).
    ``valid_purged`` — purge enabled, ran, dropped >0 train rows.
    ``disabled``    — any horizon explicitly disabled (operator opt-out).
    ``invalid``     — missing/inconsistent diagnostic; treated as not-ready.

    Returns ``(state, reasons)`` where ``reasons`` is empty when state is
    ``valid_noop`` or ``valid_purged``.

    Aggregation precedence (most severe wins): invalid > disabled > valid_purged > valid_noop.
    """
    if not per_horizon:
        return "invalid", ["purge_no_horizons"]

    any_purged = False
    any_disabled = False
    any_invalid = False
    reasons: list[str] = []

    for row in per_horizon:
        tp = row.get("train_purge")
        target = row.get("target")
        horizon = row.get("horizon")
        tag = f"{target}@{horizon}"
        if not isinstance(tp, dict):
            any_invalid = True
            reasons.append(f"purge_missing_diagnostic:{tag}")
            continue
        enabled = bool(tp.get("enabled"))
        skip_reason = (tp.get("skip_reason") or "").strip()
        before = tp.get("train_rows_before_purge")
        after = tp.get("train_rows_after_purge")
        purged = tp.get("train_rows_purged")
        calib_start = tp.get("calibration_start_ts")

        if not enabled or skip_reason == "disabled":
            any_disabled = True
            continue

        # Enabled path: validate the diagnostic block is internally consistent.
        if skip_reason:
            any_invalid = True
            reasons.append(f"purge_unexpected_skip_reason:{tag}:{skip_reason}")
            continue
        if calib_start is None:
            any_invalid = True
            reasons.append(f"purge_missing_calibration_start_ts:{tag}")
            continue
        if not isinstance(before, int) or before <= 0:
            any_invalid = True
            reasons.append(f"purge_no_train_rows_before:{tag}")
            continue
        if not isinstance(after, int) or after < 0:
            any_invalid = True
            reasons.append(f"purge_invalid_train_rows_after:{tag}")
            continue
        if not isinstance(purged, int) or purged < 0:
            any_invalid = True
            reasons.append(f"purge_invalid_train_rows_purged:{tag}")
            continue
        if purged > 0:
            any_purged = True

    if any_invalid:
        return "invalid", reasons
    if any_disabled:
        return "disabled", ["purge_disabled"]
    if any_purged:
        return "valid_purged", []
    return "valid_noop", []


def _horizon_viability(
    row: dict,
    neutralize_set: set[tuple[str, int]],
) -> tuple[bool, list[str]]:
    """Return ``(is_viable, blocked_reasons)`` for a per-horizon row.

    A horizon is viable only when ALL of:
      - ``objective == "utility_bps"``
      - ``score > 0`` (numeric)
      - ``fallback == False``
      - ``no_signal_substituted == False``
      - the runtime-safety dry-run would not neutralize this (target, horizon)
    """
    reasons: list[str] = []
    if row.get("objective") != "utility_bps":
        reasons.append("wrong_objective")
    # Score must be a finite, positive number. The previous predicate
    # ``not isinstance(score, (int, float)) or float(score) <= 0.0`` is
    # fail-open against NaN because ``NaN <= 0.0`` is False per IEEE 754.
    # Runtime safety (PR #12) catches NaN scores when fastapi is available,
    # but ``runtime_safety_dry_run.skipped == True`` is a supported case
    # (dev envs without fastapi), and readiness must close the same hole
    # at this layer rather than relying on a downstream gate that may not
    # have run. Mirrors the predicate in
    # ``server.ml_server.ModelRegistry._apply_runtime_threshold_safety``.
    score = row.get("score")
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        reasons.append("nonpositive_utility")
    else:
        try:
            score_f = float(score)
        except (TypeError, ValueError):
            reasons.append("nonpositive_utility")
        else:
            if not math.isfinite(score_f):
                reasons.append("nonfinite_utility")
            elif score_f <= 0.0:
                reasons.append("nonpositive_utility")
    if bool(row.get("fallback")):
        reasons.append("fallback")
    if bool(row.get("no_signal_substituted")):
        reasons.append("no_signal_substituted")
    key = (row.get("target"), int(row.get("horizon"))) if row.get("horizon") is not None else None
    if key is not None and key in neutralize_set:
        reasons.append("runtime_safety_neutralize")
    return (len(reasons) == 0), reasons


def _validate_horizon_statistically(
    row: dict,
    *,
    n_bootstrap: int = 5000,
    n_permutations: int = 2000,
    alpha: float = 0.05,
    rng_seed: int = 42,
    min_signals: int = 30,
) -> dict:
    """One-sample statistical validation of per-signal utility observations.

    The validator is **only** meaningful when the candidate manifest exposes a
    raw per-signal utility distribution (``meta["score_observations"]``) for the
    horizon. When that field is absent or too small, this function reports
    ``status="insufficient_data"`` — not a pass. Aggregate scores alone cannot
    be tested for significance; fabricating one would defeat the purpose of B2.

    When per-signal observations are available, two complementary tests run:

    - Bootstrap CI on the mean utility (``n_bootstrap`` resamples). Pass
      criterion: ``ci_low > 0`` at ``alpha``.
    - One-sample sign-flip permutation against H0 ``mean == 0`` vs
      H1 ``mean > 0``. Pass criterion: ``p_value < alpha``.

    The horizon passes iff BOTH criteria hold. This is intentionally strict —
    promotion gates should err on the side of withholding readiness.

    Returns a dict with: method, sample_size, observed_score, ci_low, ci_high,
    p_value, passed, status, warnings.
    """
    import numpy as np  # noqa: PLC0415 — keep numpy out of cold import paths
    target = row.get("target")
    horizon = row.get("horizon")
    observations = row.get("score_observations")
    observed_score = row.get("score")

    result: dict = {
        "target": target,
        "horizon": horizon,
        "method": None,
        "sample_size": 0,
        "observed_score": observed_score,
        "observed_mean": None,
        "ci_low": None,
        "ci_high": None,
        "p_value": None,
        "passed": False,
        "status": "insufficient_data",
        "warnings": [],
    }

    if observations is None:
        result["warnings"].append("no_score_observations_in_manifest")
        return result
    if not isinstance(observations, (list, tuple)):
        result["warnings"].append("score_observations_not_list")
        return result

    # Filter to finite numeric values; ignore NaN/inf/non-numeric entries
    # rather than failing — but track how many were dropped.
    raw_n = len(observations)
    cleaned: list[float] = []
    for x in observations:
        if isinstance(x, bool) or not isinstance(x, (int, float)):
            continue
        try:
            xf = float(x)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(xf):
            continue
        cleaned.append(xf)
    n = len(cleaned)
    result["sample_size"] = int(n)
    dropped = raw_n - n
    if dropped > 0:
        result["warnings"].append(f"dropped_{dropped}_non_finite_observations")

    if n < min_signals:
        result["warnings"].append(f"sample_size_{n}_below_min_{min_signals}")
        result["status"] = "insufficient_data"
        return result

    arr = np.asarray(cleaned, dtype=float)
    rng = np.random.default_rng(rng_seed)

    # --- Bootstrap CI on mean -------------------------------------------
    idx = rng.integers(0, n, size=(n_bootstrap, n))
    boot_means = arr[idx].mean(axis=1)
    ci_low = float(np.percentile(boot_means, 100 * alpha / 2))
    ci_high = float(np.percentile(boot_means, 100 * (1 - alpha / 2)))

    # --- One-sample sign-flip permutation against H0: mean = 0 ------------
    observed_mean = float(arr.mean())
    signs = rng.choice([-1.0, 1.0], size=(n_permutations, n))
    perm_means = (signs * arr).mean(axis=1)
    # One-sided: P(perm_mean >= observed_mean) under null. Add-one smoothing
    # so a perfect-pass run does not report p == 0.
    p_value = float((perm_means >= observed_mean).sum() + 1) / float(n_permutations + 1)

    passed = bool(ci_low > 0.0 and p_value < alpha)

    result["method"] = "bootstrap_ci + one_sample_sign_flip_permutation"
    result["observed_mean"] = observed_mean
    result["ci_low"] = ci_low
    result["ci_high"] = ci_high
    result["p_value"] = p_value
    result["passed"] = passed
    result["status"] = "passed" if passed else "failed"
    return result


def run_statistical_validation(
    per_horizon: list[dict],
    *,
    viable_set: set[tuple[str, int]],
    rng_seed: int = 42,
) -> dict[str, dict]:
    """Validate only mechanically-viable horizons.

    ``viable_set`` is the set of ``(target, horizon)`` tuples that already
    passed mechanical viability in ``classify_candidate_readiness``. Anything
    not in that set is silently skipped — statistical validation never
    unblocks a horizon the safety chain has rejected.
    """
    out: dict[str, dict] = {}
    for row in per_horizon:
        target = row.get("target")
        horizon = row.get("horizon")
        if horizon is None:
            continue
        try:
            key_tuple = (target, int(horizon))
        except (TypeError, ValueError):
            continue
        if key_tuple not in viable_set:
            continue
        key = f"{target}@{int(horizon)}m"
        out[key] = _validate_horizon_statistically(row, rng_seed=rng_seed)
    return out


def _aggregate_statistical_validation(per_horizon_results: dict) -> tuple[bool, bool | None]:
    """Roll per-horizon statistical results up to (present, passed).

    ``present`` is True when at least one mechanically-viable horizon ran a
    real test (status ``passed`` or ``failed``); False when every viable
    horizon was ``insufficient_data`` (or there were none).

    ``passed`` is True only when every viable horizon's test ran AND every
    one passed. False when any ran and failed. None when coverage is
    incomplete (some insufficient_data) or no test ran.
    """
    if not per_horizon_results:
        return False, None
    statuses = [r.get("status") for r in per_horizon_results.values()]
    ran = [s for s in statuses if s in ("passed", "failed")]
    if not ran:
        return False, None
    present = True
    if any(s == "failed" for s in ran):
        return present, False
    # No failures among those that ran. Pass only if every viable horizon
    # actually ran (no insufficient_data anywhere).
    if all(s == "passed" for s in statuses):
        return present, True
    return present, None


def _compute_promotion_disposition(
    *,
    state: str,
    has_viable: bool,
    statistical_validation_present: bool,
    statistical_validation_passed: object,
) -> tuple[bool, str]:
    """Map (state, statistical-validation) into the promotion axis.

    Promotion is a *second* axis on top of readiness. Readiness ("how did
    training + safety look?") is necessary but not sufficient for promotion;
    statistical validation is an independent gate. Keeping the fields
    separate prevents future automation from reading ``partial_ready=true``
    or ``degraded_candidate=true`` as ``promotion_ready=true``.

    Disposition order (most-blocking first):

    - ``blocked_not_ready``                    — ``state == "not_ready"``.
    - ``ready_full_family``                    — ``state == "full_family_ready"``
                                                 AND statistical validation is
                                                 present AND passed.
    - ``hold_pending_statistical_validation``  — has at least one viable horizon
                                                 AND statistical validation is
                                                 missing or did not pass. This is
                                                 where the current candidate sits
                                                 until B3 lands.
    - ``hold_partial_degraded``                — partial/degraded with statistical
                                                 validation present-and-passed
                                                 but not the full family. Reserved
                                                 for the B3+B4 regime; not
                                                 reachable from this PR's
                                                 hard-coded stat-validation
                                                 stub.
    """
    stat_promotable = bool(
        statistical_validation_present and statistical_validation_passed is True
    )
    if state == "not_ready":
        return False, "blocked_not_ready"
    if state == "full_family_ready" and stat_promotable:
        return True, "ready_full_family"
    if has_viable and not stat_promotable:
        return False, "hold_pending_statistical_validation"
    return False, "hold_partial_degraded"


def classify_candidate_readiness(report: dict) -> dict:
    """Classify a candidate from its evidence report (policy-only, no mutation).

    See module docstring; states are ``full_family_ready``, ``partial_ready``,
    ``degraded_candidate``, ``not_ready``. ``partial_ready`` and
    ``degraded_candidate`` can both be true at once — the ``state`` field
    picks the most specific applicable label.

    Statistical validation (B3, ``_validate_horizon_statistically`` /
    ``run_statistical_validation``) runs on the mechanically-viable horizons
    here. It depends on the manifest exposing per-signal utility observations
    under ``thresholds_meta[target][horizon]["score_observations"]``; current
    training runs do not yet write that field, so today's candidates report
    ``status="insufficient_data"`` per viable horizon. The aggregate
    ``statistical_validation_present`` / ``statistical_validation_passed``
    booleans reflect that honestly, and ``promotion_disposition`` stays at
    ``hold_pending_statistical_validation`` until a future training-side PR
    captures observations. Statistical validation never unblocks a horizon
    the safety chain rejected.
    """
    per_horizon = report.get("per_horizon") or []
    safety = report.get("runtime_safety_dry_run") or {}
    training = report.get("training") or {}
    candidate_manifest = report.get("candidate_manifest")

    reasons: list[str] = []
    fatal: list[str] = []

    # --- Training / manifest preconditions ---------------------------------
    training_skipped = bool(training.get("skipped"))
    training_exit_code = training.get("exit_code")
    if not training_skipped and training_exit_code not in (0, None):
        fatal.append("training_failed")
    if candidate_manifest is None or (
        isinstance(candidate_manifest, dict) and "_parse_error" in candidate_manifest
    ):
        fatal.append("no_candidate_manifest")

    # --- Runtime-safety agreement -----------------------------------------
    would_neutralize = safety.get("would_neutralize") or []
    would_neutralize_count = int(safety.get("would_neutralize_count") or 0)
    runtime_safety_skipped = bool(safety.get("skipped"))
    runtime_safety_agreement = (
        not runtime_safety_skipped and would_neutralize_count == 0
    )
    if would_neutralize_count > 0:
        fatal.append("runtime_safety_disagreement")
    elif runtime_safety_skipped:
        # Skipped dry-run isn't a hard fail (dev environments without
        # fastapi), but it prevents the strongest readiness claim.
        reasons.append("runtime_safety_skipped")

    neutralize_set: set[tuple[str, int]] = set()
    for entry in would_neutralize:
        if not isinstance(entry, dict):
            continue
        try:
            neutralize_set.add((entry.get("target"), int(entry.get("horizon"))))
        except (TypeError, ValueError):
            continue

    # --- Horizon viability -------------------------------------------------
    viable_horizons: list[dict] = []
    blocked_horizons: list[dict] = []
    for row in per_horizon:
        is_viable, blocked_reasons = _horizon_viability(row, neutralize_set)
        if is_viable:
            viable_horizons.append({
                "target": row.get("target"),
                "horizon": row.get("horizon"),
                "score": row.get("score"),
            })
        else:
            blocked_horizons.append({
                "target": row.get("target"),
                "horizon": row.get("horizon"),
                "reasons": blocked_reasons,
            })

    if not viable_horizons:
        fatal.append("no_viable_horizons")

    # --- Purge diagnostic --------------------------------------------------
    purge_state, purge_reasons = _purge_diagnostic_state(per_horizon)
    if purge_state == "invalid":
        fatal.append("purge_diagnostic_invalid")
        reasons.extend(purge_reasons)
    elif purge_state == "disabled":
        reasons.append("purge_disabled")

    # --- Statistical validation (B3) ---------------------------------------
    # Run validation ONLY on mechanically-viable horizons. The framework here
    # is real; whether it produces ``passed``/``failed`` or
    # ``insufficient_data`` depends on what the candidate manifest stored
    # for each horizon (``score_observations``). Today's manifests do not
    # capture per-signal observations, so today's report is honest about
    # being unable to test significance — and the promotion disposition
    # reflects that.
    statistical_validation_required = True
    viable_set_for_validation: set[tuple[str, int]] = {
        (v.get("target"), int(v.get("horizon")))
        for v in viable_horizons
        if v.get("horizon") is not None
    }
    statistical_validation_per_horizon = run_statistical_validation(
        per_horizon, viable_set=viable_set_for_validation
    )
    statistical_validation_present, statistical_validation_passed = (
        _aggregate_statistical_validation(statistical_validation_per_horizon)
    )
    if viable_horizons and not statistical_validation_present:
        reasons.append("statistical_validation_missing")
    if statistical_validation_present and statistical_validation_passed is False:
        reasons.append("statistical_validation_failed")
    # Surface per-horizon insufficient_data so the reason list is actionable.
    for key, res in statistical_validation_per_horizon.items():
        if res.get("status") == "insufficient_data":
            reasons.append(f"statistical_validation_insufficient_data:{key}")

    # --- State resolution --------------------------------------------------
    if fatal:
        state = "not_ready"
        full_family_ready = False
        partial_ready = False
        degraded_candidate = False
        not_ready = True
    elif (
        len(viable_horizons) == len(per_horizon)
        and runtime_safety_agreement
        and purge_state in ("valid_noop", "valid_purged")
    ):
        # All attempted horizons viable; clean across all gates.
        state = "full_family_ready"
        full_family_ready = True
        partial_ready = False
        degraded_candidate = False
        not_ready = False
    else:
        # Mixed: at least one viable, at least one blocked or a soft issue.
        full_family_ready = False
        not_ready = False
        partial_ready = True
        # Promote to degraded_candidate when the failure signal is strong:
        # half or more horizons blocked, OR purge was operator-disabled.
        majority_blocked = len(blocked_horizons) * 2 >= len(per_horizon) if per_horizon else False
        degraded_candidate = bool(majority_blocked or purge_state == "disabled")
        state = "degraded_candidate" if degraded_candidate else "partial_ready"
        if majority_blocked:
            reasons.append("majority_horizons_blocked")

    # --- Promotion disposition ---------------------------------------------
    promotion_ready, promotion_disposition = _compute_promotion_disposition(
        state=state,
        has_viable=bool(viable_horizons),
        statistical_validation_present=statistical_validation_present,
        statistical_validation_passed=statistical_validation_passed,
    )

    # Dedupe reasons while preserving order.
    seen_r: set[str] = set()
    reasons_ordered: list[str] = []
    for item in fatal + reasons:
        if item not in seen_r:
            seen_r.add(item)
            reasons_ordered.append(item)

    return {
        "state": state,
        "full_family_ready": bool(full_family_ready),
        "partial_ready": bool(partial_ready),
        "degraded_candidate": bool(degraded_candidate),
        "not_ready": bool(not_ready),
        "promotion_ready": bool(promotion_ready),
        "promotion_disposition": promotion_disposition,
        "viable_horizons": viable_horizons,
        "blocked_horizons": blocked_horizons,
        "runtime_safety_agreement": bool(runtime_safety_agreement),
        "purge_diagnostic_state": purge_state,
        "statistical_validation_required": statistical_validation_required,
        "statistical_validation_present": statistical_validation_present,
        "statistical_validation_passed": statistical_validation_passed,
        "statistical_validation": statistical_validation_per_horizon,
        "reasons": reasons_ordered,
    }


def build_report(
    *,
    run_id: str,
    out_dir: Path,
    active_manifest_path: Path,
    training_block: dict,
    candidate_manifest_path: Path,
    candidate_manifest: dict | None,
) -> dict:
    sentinel = _no_signal_sentinel()
    if isinstance(candidate_manifest, dict) and "_parse_error" not in candidate_manifest:
        per_horizon = per_horizon_summary(candidate_manifest, sentinel)
        thresholds = thresholds_from_manifest(candidate_manifest)
        safety = runtime_safety_dry_run(thresholds, candidate_manifest)
        summary = summary_block(per_horizon)
    else:
        per_horizon = []
        safety = {
            "skipped": True,
            "skip_reason": "no_candidate_manifest",
            "would_neutralize_count": 0,
            "would_neutralize": [],
        }
        summary = summary_block(per_horizon)

    report = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "out_dir": str(out_dir),
        "provenance": build_provenance(active_manifest_path),
        "training": training_block,
        "candidate_manifest_path": str(candidate_manifest_path),
        "candidate_manifest": candidate_manifest,
        "per_horizon": per_horizon,
        "runtime_safety_dry_run": safety,
        "summary": summary,
    }
    report["candidate_readiness"] = classify_candidate_readiness(report)
    return report


def _load_candidate_manifest(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        return {"_parse_error": f"{type(exc).__name__}: {exc}"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Retrain dry-run evidence pack (mechanics only).",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Isolated artifact dir; must not overlap data/models.",
    )
    parser.add_argument(
        "--report",
        default="",
        help="Report JSON destination; defaults to evidence/retrain_<run_id>.json.",
    )
    parser.add_argument(
        "--candidate-manifest",
        default=DEFAULT_CANDIDATE_MANIFEST_NAME,
        help="Filename produced by training inside --out-dir.",
    )
    parser.add_argument(
        "--active-manifest",
        default=str(Path(DEFAULT_LIVE_MODEL_DIR) / DEFAULT_ACTIVE_MANIFEST_NAME),
        help="Path to live active manifest (read-only).",
    )
    parser.add_argument(
        "--live-model-dir",
        default=DEFAULT_LIVE_MODEL_DIR,
        help="Live model dir protected from --out-dir collision.",
    )
    parser.add_argument(
        "--pass-through",
        action="append",
        default=[],
        help="KEY=VALUE env var forwarded to train_rf_artifacts (repeatable).",
    )
    parser.add_argument(
        "--train-arg",
        action="append",
        default=[],
        help="Extra CLI arg forwarded to train_rf_artifacts (repeatable).",
    )
    parser.add_argument(
        "--skip-training",
        action="store_true",
        help="Do not invoke training; expect candidate manifest already in --out-dir.",
    )
    args = parser.parse_args(argv)

    out_dir = _resolve(args.out_dir)
    live_model_dir = _resolve(args.live_model_dir)
    active_manifest_path = _resolve(args.active_manifest)
    validate_out_dir(out_dir, live_model_dir, active_manifest_path)

    out_dir.mkdir(parents=True, exist_ok=True)
    pass_through_env = parse_pass_through(args.pass_through)
    forwarded_train_args = parse_train_args(args.train_arg)

    run_id = (
        dt.datetime.now(dt.timezone.utc)
        .isoformat(timespec="seconds")
        .replace(":", "")
        .replace("+0000", "Z")
    )
    if args.report:
        report_path = _resolve(args.report)
    else:
        report_path = ROOT / "evidence" / f"retrain_{run_id}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    if args.skip_training:
        training_block = {
            "skipped": True,
            "skip_reason": "skip_training=true",
            "cmd": None,
            "exit_code": None,
            "duration_seconds": 0.0,
            "stdout_tail": "",
            "stderr_tail": "",
            "error": None,
        }
    else:
        training_block = invoke_training(
            out_dir,
            pass_through_env,
            forwarded_train_args,
            candidate_manifest_name=args.candidate_manifest,
        )

    candidate_manifest_path = out_dir / args.candidate_manifest
    candidate_manifest = _load_candidate_manifest(candidate_manifest_path)

    report = build_report(
        run_id=run_id,
        out_dir=out_dir,
        active_manifest_path=active_manifest_path,
        training_block=training_block,
        candidate_manifest_path=candidate_manifest_path,
        candidate_manifest=candidate_manifest,
    )
    report_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"Evidence pack report written to {report_path}")
    _print_readiness_summary(report.get("candidate_readiness") or {})
    return 0


def _print_readiness_summary(readiness: dict) -> None:
    """One-screen recap of the candidate_readiness block."""
    if not readiness:
        return
    viable = readiness.get("viable_horizons") or []
    blocked = readiness.get("blocked_horizons") or []
    print(f"[candidate readiness] state={readiness.get('state')}")
    print(
        f"  promotion_ready:         {readiness.get('promotion_ready')}  "
        f"(disposition={readiness.get('promotion_disposition')})"
    )
    if viable:
        bits = []
        for v in viable:
            score = v.get("score")
            score_s = f"{float(score):.3f}" if isinstance(score, (int, float)) else "n/a"
            bits.append(f"{v.get('target')}@{v.get('horizon')}m (score {score_s})")
        print(f"  viable:                  {', '.join(bits)}")
    else:
        print("  viable:                  none")
    print(f"  blocked horizons:        {len(blocked)}")
    print(f"  runtime_safety_agreement: {readiness.get('runtime_safety_agreement')}")
    print(f"  purge_diagnostic_state:   {readiness.get('purge_diagnostic_state')}")
    sv_required = readiness.get("statistical_validation_required")
    sv_present = readiness.get("statistical_validation_present")
    sv_passed = readiness.get("statistical_validation_passed")
    sv_per_horizon = readiness.get("statistical_validation") or {}
    print(
        f"  statistical_validation:   required={sv_required} "
        f"present={sv_present} passed={sv_passed}"
    )
    for key, res in sv_per_horizon.items():
        status = res.get("status")
        n = res.get("sample_size")
        ci_low = res.get("ci_low")
        ci_high = res.get("ci_high")
        p_value = res.get("p_value")
        if status in ("passed", "failed"):
            print(
                f"    {key:<14} {status:<11} n={n}  "
                f"ci=[{ci_low:.4f},{ci_high:.4f}]  p={p_value:.4f}"
            )
        else:
            warnings = ",".join(res.get("warnings") or [])
            print(f"    {key:<14} {status:<11} n={n}  warnings={warnings}")


if __name__ == "__main__":
    raise SystemExit(main())
