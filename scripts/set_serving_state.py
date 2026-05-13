#!/usr/bin/env python3
"""Manual CLI for flipping ``data/models/serving_state.json``.

This is the *only* writer of serving state in D1. The server reads the
file but never writes it. There is no audit auto-wiring, no auto-clear,
and no scheduled re-evaluation — operators flip the file themselves
when they decide live serving should pause or resume.

Why a separate CLI:
    - Atomic write (matches manifest_active.json discipline).
    - Refuses to silently overwrite a dormant state with a different
      dormant state — ``--force`` is required to chain dormancies.
    - Records ``set_by`` (user@host) and ``manifest_version_when_set``
      so the recorded reason is always tied to a known config snapshot.
    - Loud warning when a dormant state is set without ``--expires-at``;
      operators should commit to a review date even if the schema does
      not strictly require it.

Future D3 will let audit scripts opt in via ``--write-serving-state-on-fail``,
which will shell out to this CLI rather than implement its own writer.
"""

from __future__ import annotations

import argparse
import datetime as dt
import getpass
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.serving_state import (
    DORMANT_STATES,
    SCHEMA_VERSION,
    SERVING_STATE_FILENAME,
    STATE_ACTIVE,
    VALID_STATES,
    validate_state_payload,
)

DEFAULT_MODEL_DIR = ROOT / "data" / "models"
DEFAULT_ACTIVE_MANIFEST_NAME = (
    os.getenv("RF_ACTIVE_MANIFEST", "manifest_active.json").strip()
    or "manifest_active.json"
)


def _temp_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.tmp-{os.getpid()}-{int(time.time() * 1000)}")


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
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


def _parse_expires_at(raw: str | None) -> int | None:
    if raw is None or raw.strip() == "":
        return None
    raw = raw.strip()
    # Epoch milliseconds (integer-ish, large).
    if raw.isdigit():
        value = int(raw)
        # Treat values < 1e10 as seconds, otherwise milliseconds. 1e10 s is
        # 2286-11-20, so anything plausibly today and in seconds is < 1e10.
        if value < 10_000_000_000:
            return value * 1000
        return value
    # ISO-8601. Allow trailing ``Z`` shorthand.
    iso = raw.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(iso)
    except ValueError as exc:
        raise SystemExit(f"--expires-at: cannot parse {raw!r} ({exc})")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return int(parsed.timestamp() * 1000)


def _resolve_manifest_version(model_dir: Path) -> str | None:
    path = model_dir / DEFAULT_ACTIVE_MANIFEST_NAME
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(data, dict):
        v = data.get("version")
        if isinstance(v, str) and v:
            return v
    return None


def _set_by_default() -> str:
    try:
        user = getpass.getuser()
    except Exception:
        user = "unknown_user"
    try:
        host = socket.gethostname()
    except Exception:
        host = "unknown_host"
    return f"{user}@{host}"


def _existing_payload(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def build_payload(
    *,
    state: str,
    reason: str,
    triggering_audit: str | None,
    expires_at_ms: int | None,
    set_by: str,
    manifest_version_when_set: str | None,
    now_ms: int,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "state": state,
        "since_ts": int(now_ms),
        "reason": reason,
        "triggering_audit": triggering_audit,
        "set_by": set_by,
        "manifest_version_when_set": manifest_version_when_set,
        "expires_at": int(expires_at_ms) if expires_at_ms is not None else None,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Manually set serving state by writing serving_state.json. "
            "ml_server reads the file; this CLI is the only writer."
        ),
    )
    parser.add_argument(
        "--state",
        required=True,
        choices=sorted(VALID_STATES),
        help="Target serving state. ``active`` resumes serving.",
    )
    parser.add_argument(
        "--reason",
        required=True,
        help="Free-text reason recorded in serving_state.json.",
    )
    parser.add_argument(
        "--triggering-audit",
        default=None,
        help=(
            "Optional path or identifier of an audit report that motivated the "
            "state change (e.g. evidence/provider_normalized_regime/<run>.json)."
        ),
    )
    parser.add_argument(
        "--expires-at",
        default=None,
        help=(
            "Optional auto-review marker. Accepts ISO-8601 (2026-05-15T12:00:00Z) "
            "or epoch seconds/milliseconds. Strongly encouraged for dormant states. "
            "Not enforced by the server in D1 (no auto-clear yet)."
        ),
    )
    parser.add_argument(
        "--model-dir",
        default=str(DEFAULT_MODEL_DIR),
        help="Directory containing serving_state.json and the active manifest.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Required to overwrite an existing non-active state with a "
            "different non-active state. Active->dormant and any->active "
            "transitions never require --force."
        ),
    )
    parser.add_argument(
        "--now-ms",
        type=int,
        default=None,
        help="Override the recorded ``since_ts`` (testing/replay only).",
    )
    parser.add_argument(
        "--set-by",
        default=None,
        help="Override the recorded ``set_by`` (testing/replay only).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress informational output (only emit errors and the final path).",
    )
    args = parser.parse_args(argv)

    model_dir = Path(args.model_dir).expanduser().resolve()
    state_path = model_dir / SERVING_STATE_FILENAME

    if not model_dir.is_dir():
        raise SystemExit(f"--model-dir {model_dir} is not a directory")

    target_state = args.state
    expires_at_ms = _parse_expires_at(args.expires_at)

    # Refuse to overwrite a dormant state with a *different* dormant state
    # unless --force. Allow:
    #   any -> active
    #   active -> dormant_*
    #   dormant_X -> dormant_X (same state, refresh reason/expiry)
    # Refuse:
    #   dormant_X -> dormant_Y without --force
    existing = _existing_payload(state_path)
    if existing is not None:
        current_state = existing.get("state")
        if (
            isinstance(current_state, str)
            and current_state in DORMANT_STATES
            and target_state in DORMANT_STATES
            and current_state != target_state
            and not args.force
        ):
            raise SystemExit(
                f"Refusing to overwrite existing dormant state "
                f"{current_state!r} with {target_state!r}; pass --force to "
                "confirm. (active->dormant and any->active never require --force.)"
            )

    set_by = args.set_by if args.set_by is not None else _set_by_default()
    manifest_version = _resolve_manifest_version(model_dir)
    now_ms = (
        int(args.now_ms) if args.now_ms is not None else int(time.time() * 1000)
    )

    payload = build_payload(
        state=target_state,
        reason=str(args.reason),
        triggering_audit=args.triggering_audit,
        expires_at_ms=expires_at_ms,
        set_by=str(set_by),
        manifest_version_when_set=manifest_version,
        now_ms=now_ms,
    )
    ok, validation_reason = validate_state_payload(payload)
    if not ok:
        # Should not happen given argparse + our own builder, but fail
        # loudly rather than write a corrupt control-plane file.
        raise SystemExit(
            f"Refusing to write: built payload failed validation ({validation_reason})"
        )

    if target_state != STATE_ACTIVE and expires_at_ms is None and not args.quiet:
        print(
            "WARNING: setting a dormant state without --expires-at. Operators "
            "are strongly encouraged to commit to a review date.",
            file=sys.stderr,
        )

    atomic_write_json(state_path, payload)
    if not args.quiet:
        print(
            f"serving_state -> {target_state} "
            f"(reason={payload['reason']!r}, "
            f"manifest_version_when_set={manifest_version}, "
            f"expires_at={payload['expires_at']})"
        )
    print(str(state_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
