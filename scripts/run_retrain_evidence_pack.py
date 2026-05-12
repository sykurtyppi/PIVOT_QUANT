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

    return {
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
