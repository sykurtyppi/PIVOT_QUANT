"""Shared Python interpreter resolver.

Single source of truth for "which Python should this codebase use." Mirrors
the shell-side resolver in ``scripts/_pybin.sh``. Used by any Python script
that needs to:

  - assert at import time that it is running under Python >= 3.10
    (see ``assert_python_310``), OR
  - spawn another Python script as a subprocess and need that subprocess
    to use the same supported interpreter (see ``resolve_python``).

Resolution precedence (matches scripts/run_retrain_evidence_pack.resolve_training_python
and scripts/_pybin.sh):

  1. ``PYTHON_BIN`` env var, if set and executable
  2. ``<ROOT>/.venv313/bin/python``, if present
  3. ``<ROOT>/.venv/bin/python``, if present
  4. ``sys.executable``, if its version is >= 3.10
  5. Otherwise SystemExit with the list of candidates tried

Why this exists: Apple's CommandLineTools ``python3`` on macOS is 3.9.6,
which cannot import modules using PEP 604 ``int | None`` annotations
(``scripts/train_rf_artifacts.py`` is one such module). Subprocesses that
inherit ``sys.executable`` from a 3.9 parent would fail at import. This
module centralizes the resolver so we don't ship a half-fix where one
spawn path uses the resolver and the next sibling path goes unguarded.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def python_version_tuple(executable: str) -> tuple[int, int, int] | None:
    """Return ``(major, minor, micro)`` for an interpreter, or None on failure."""
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


def resolve_python(min_version: tuple[int, int] = (3, 10)) -> tuple[str, str, str]:
    """Resolve a Python interpreter satisfying ``min_version`` (default 3.10).

    Returns ``(executable_path, version_string, source_label)`` where
    ``source_label`` is one of ``"PYTHON_BIN"``, ``".venv313/bin/python"``,
    ``".venv/bin/python"``, or ``"sys.executable"``.

    Raises ``SystemExit`` with the candidates tried and why each was
    rejected — never returns a sub-min interpreter.
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
        # PYTHON_BIN / venv paths must exist on disk to be usable;
        # sys.executable always exists but may be the wrong version.
        if label != "sys.executable" and not Path(path).is_file():
            tried.append(f"{label}={path} (not present)")
            continue
        version = python_version_tuple(path)
        if version is None:
            tried.append(f"{label}={path} (probe failed)")
            continue
        if version < min_version:
            tried.append(
                f"{label}={path} (version "
                f"{version[0]}.{version[1]}.{version[2]} "
                f"< {min_version[0]}.{min_version[1]})"
            )
            continue
        return path, "%d.%d.%d" % version, label

    raise SystemExit(
        f"Could not resolve a Python >= {min_version[0]}.{min_version[1]}. "
        "Tried in order: " + "; ".join(tried) + ". "
        "Set PYTHON_BIN, create .venv/ with a 3.10+ interpreter, or run under "
        "a 3.10+ python directly."
    )


def assert_python_310() -> None:
    """Module-level guard: abort if the CURRENT interpreter is < 3.10.

    Use at the top of any script that uses PEP 604 union syntax (``int | None``)
    or other 3.10+ features at module load. Provides a clear error message
    pointing at the venv hint instead of an opaque ``TypeError`` from the
    annotation evaluation.
    """
    if sys.version_info < (3, 10):
        running = ".".join(str(x) for x in sys.version_info[:3])
        raise SystemExit(
            f"This script requires Python >= 3.10 (running {running} at "
            f"{sys.executable}). Use .venv/bin/python or set PYTHON_BIN."
        )
