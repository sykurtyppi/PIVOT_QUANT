"""Serving-state registry — the *third axis* on top of readiness and promotion.

Background:
    Readiness asks "is this candidate artifact mechanically/statistically
    acceptable?" Promotion asks "is this candidate allowed to become active?"
    Neither answers "should the currently-active, already-promoted model
    be answering live predictions right now?" That third axis is *serving
    state*.

D1 contract (the registry below):
    A single file-backed flag, ``data/models/serving_state.json``, decides
    whether ``/score`` should answer normally or short-circuit with a
    structured dormant response. Operators flip the file via
    ``scripts/set_serving_state.py``; ``ml_server`` only reads it. No
    audit auto-wiring, no auto-clear, no expiry enforcement here — D3/D4
    work.

D2 contract (``ServingStateObservability`` + audit events):
    Process-local counters expose how often the dormant short-circuit
    has fired since process start and since the current state was set;
    ``/health`` surfaces these so ops can see "we paused 4 minutes ago,
    blocked 1,254 requests so far." A sampled ``predict_blocked_dormant``
    audit event fires (rate-and-time-gated) so the audit log records
    dormant blocks without spamming under sustained dormancy. The CLI
    emits ``serving_state_changed`` on every successful write. Both
    event types live in the shared research-protocol audit log
    (``reports/research_protocol/audit_log.jsonl``) — see
    ``services/research_protocol/audit_logger.py``.

Failure model:
    - Missing file              -> default ``active`` with a marker so
                                   /health can distinguish "no flag set"
                                   from "flag says active".
    - Unreadable / invalid JSON -> ``dormant_data_quality`` with a
                                   ``serving_state_invalid`` reason. An
                                   unparseable control-plane file must
                                   NOT silently allow serving.
    - Invalid state value       -> same conservative dormant outcome.

The loader caches by (mtime_ns, size) so /reload is cheap when nothing
changed, and never mutates the manifest, thresholds, or model artifacts.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

SERVING_STATE_FILENAME = "serving_state.json"

# Recognized states. Anything else is invalid and treated as
# ``dormant_data_quality`` by the loader so a typo'd manual write does
# not silently leave serving live.
STATE_ACTIVE = "active"
STATE_DORMANT_AUDIT_FAIL = "dormant_audit_fail"
STATE_DORMANT_MANUAL_PAUSE = "dormant_manual_pause"
STATE_DORMANT_DATA_QUALITY = "dormant_data_quality"

VALID_STATES: frozenset[str] = frozenset(
    {
        STATE_ACTIVE,
        STATE_DORMANT_AUDIT_FAIL,
        STATE_DORMANT_MANUAL_PAUSE,
        STATE_DORMANT_DATA_QUALITY,
    }
)

DORMANT_STATES: frozenset[str] = frozenset(VALID_STATES) - {STATE_ACTIVE}

SCHEMA_VERSION = 1


def _file_signature(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return (int(stat.st_mtime_ns), int(stat.st_size))


def validate_state_payload(payload: object) -> tuple[bool, str | None]:
    """Return (is_valid, error_reason) for an *on-disk* serving-state record.

    Strict for a reason: this file is the operator control plane — an
    incomplete record like ``{"state": "dormant_manual_pause"}`` should
    NOT silently pause serving without recording who paused it, when, or
    why. The loader runs every parsed file through this function and
    falls back to ``dormant_data_quality`` on rejection. The CLI runs the
    built payload through this function before writing so a buggy CLI
    cannot ship a half-valid file.

    Required keys (presence + type-checked):
      - ``schema_version`` == 1 (int; reject missing / non-int / future versions)
      - ``state`` in VALID_STATES (str)
      - ``since_ts`` numeric, non-negative (int or float; reject bool)
      - ``reason`` non-empty string

    Optional keys (type-checked when present, including when explicitly null):
      - ``expires_at``                  — null OR numeric non-negative
      - ``triggering_audit``            — null OR string
      - ``set_by``                      — null OR string
      - ``manifest_version_when_set``   — null OR string

    The synthetic ``default_missing_file`` snapshot built by the loader
    when no file exists does NOT go through this validator — it is
    returned directly and labeled ``source="default_missing_file"`` so
    /health can distinguish it from a literally-active record.
    """
    if not isinstance(payload, dict):
        return False, "payload_not_object"

    # schema_version: required, must be the exact supported int (1).
    if "schema_version" not in payload:
        return False, "schema_version_missing"
    sv = payload.get("schema_version")
    if isinstance(sv, bool) or not isinstance(sv, int):
        return False, "schema_version_invalid_type"
    if sv != SCHEMA_VERSION:
        return False, "schema_version_unsupported"

    # state: required, must be one of the recognized values.
    if "state" not in payload:
        return False, "state_missing"
    state = payload.get("state")
    if not isinstance(state, str) or state not in VALID_STATES:
        return False, "state_invalid"

    # since_ts: required, numeric non-negative. Excludes bool.
    if "since_ts" not in payload:
        return False, "since_ts_missing"
    ts = payload.get("since_ts")
    if isinstance(ts, bool) or not isinstance(ts, (int, float)):
        return False, "since_ts_invalid_type"
    if ts < 0:
        return False, "since_ts_negative"

    # reason: required, non-empty string.
    if "reason" not in payload:
        return False, "reason_missing"
    reason = payload.get("reason")
    if not isinstance(reason, str):
        return False, "reason_invalid_type"
    if not reason.strip():
        return False, "reason_empty"

    # expires_at: optional, null-or-numeric-non-negative when present.
    if "expires_at" in payload:
        expires = payload.get("expires_at")
        if expires is not None:
            if isinstance(expires, bool) or not isinstance(expires, (int, float)):
                return False, "expires_at_invalid_type"
            if expires < 0:
                return False, "expires_at_negative"

    # Optional string-or-null fields. Reject any other type (numbers, bools,
    # lists, dicts) to catch accidental schema drift.
    for key in ("triggering_audit", "set_by", "manifest_version_when_set"):
        if key in payload:
            value = payload.get(key)
            if value is not None and not isinstance(value, str):
                return False, f"{key}_invalid_type"

    return True, None


class ServingStateRegistry:
    """Thread-safe loader for ``data/models/serving_state.json``.

    Designed to be a long-lived module-level singleton in ``ml_server``.
    Holds the last successfully-parsed payload plus a load-time error
    string (if the file existed but was unparseable). The reader API is:

      - ``snapshot()``      -> a dict suitable for /health
      - ``is_active()``     -> bool: predict path may answer normally
      - ``blocked_payload(manifest_version=...)``
                           -> a dict to merge into the dormant response

    The registry never writes to ``serving_state.json``. Use the CLI
    (``scripts/set_serving_state.py``) for that.
    """

    def __init__(self, state_path: Path):
        self._state_path = Path(state_path)
        self._lock = threading.RLock()
        # ``_payload`` holds the most recently honored state. When the file
        # is missing, this is the synthetic "default active" record. When
        # the file is unparseable, this is the synthetic "dormant_data_quality"
        # record carrying the parse error.
        self._payload: dict[str, Any] = self._default_active_payload()
        self._signature: tuple[int, int] | None = None
        self._load_error: str | None = None
        self._source: str = "default_missing_file"

    @property
    def state_path(self) -> Path:
        return self._state_path

    @staticmethod
    def _default_active_payload() -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "state": STATE_ACTIVE,
            "since_ts": None,
            "reason": "serving_state_missing_default_active",
            "triggering_audit": None,
            "set_by": None,
            "manifest_version_when_set": None,
            "expires_at": None,
        }

    @staticmethod
    def _invalid_file_payload(error: str) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "state": STATE_DORMANT_DATA_QUALITY,
            "since_ts": int(time.time() * 1000),
            "reason": "serving_state_invalid",
            "triggering_audit": None,
            "set_by": None,
            "manifest_version_when_set": None,
            "expires_at": None,
            "invalid_file_error": error,
        }

    def is_signature_unchanged(self) -> bool:
        sig = _file_signature(self._state_path)
        with self._lock:
            return sig == self._signature

    def load(self, *, force: bool = False) -> bool:
        """Reload from disk. Returns True iff the cached payload changed.

        Never raises. A missing file becomes "default active"; an
        unparseable / schema-invalid file becomes "dormant_data_quality".
        """
        sig = _file_signature(self._state_path)
        with self._lock:
            if not force and sig == self._signature:
                return False

        if sig is None:
            new_payload = self._default_active_payload()
            new_error = None
            new_source = "default_missing_file"
        else:
            try:
                raw = self._state_path.read_text(encoding="utf-8")
                parsed = json.loads(raw)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                log.warning(
                    "serving_state.json unreadable at %s -> %s; treating as dormant_data_quality",
                    self._state_path,
                    error,
                )
                new_payload = self._invalid_file_payload(error)
                new_error = error
                new_source = "invalid_file"
            else:
                ok, reason = validate_state_payload(parsed)
                if not ok:
                    error = f"schema_invalid: {reason}"
                    log.warning(
                        "serving_state.json schema-invalid at %s (%s); treating as dormant_data_quality",
                        self._state_path,
                        reason,
                    )
                    new_payload = self._invalid_file_payload(error)
                    new_error = error
                    new_source = "invalid_file"
                else:
                    new_payload = dict(parsed)
                    new_error = None
                    new_source = "file"

        with self._lock:
            changed = (new_payload != self._payload) or (new_source != self._source)
            self._payload = new_payload
            self._signature = sig
            self._load_error = new_error
            self._source = new_source
        if changed:
            log.info(
                "serving_state loaded: source=%s state=%s reason=%s",
                new_source,
                new_payload.get("state"),
                new_payload.get("reason"),
            )
        return changed

    def snapshot(self) -> dict[str, Any]:
        """Return a /health-suitable dict. Always non-None."""
        with self._lock:
            payload = dict(self._payload)
            payload["source"] = self._source
            payload["load_error"] = self._load_error
            payload["state_path"] = str(self._state_path)
        return payload

    def is_active(self) -> bool:
        with self._lock:
            return self._payload.get("state") == STATE_ACTIVE

    def state(self) -> str:
        with self._lock:
            value = self._payload.get("state")
        return str(value) if isinstance(value, str) else STATE_DORMANT_DATA_QUALITY

    def blocked_payload(self, manifest_version: str | None) -> dict[str, Any]:
        """Build the dormant-response merge dict the predict path returns.

        Intentionally omits probability/threshold; a dormant serving path
        must not leak prediction-internal state. Callers merge this into
        their per-event response so the API contract still returns a
        dict-of-known-fields.
        """
        with self._lock:
            payload = dict(self._payload)
        return {
            "signal": None,
            "blocked_reason": "serving_dormant",
            "serving_state": payload.get("state"),
            "serving_state_reason": payload.get("reason"),
            "serving_state_since_ts": payload.get("since_ts"),
            "serving_state_expires_at": payload.get("expires_at"),
            "serving_state_triggering_audit": payload.get("triggering_audit"),
            "manifest_version": manifest_version,
            "manifest_version_when_set": payload.get("manifest_version_when_set"),
        }


# --------------------------------------------------------------------------
# D2: process-local observability counters + sampler for predict_blocked_dormant
# --------------------------------------------------------------------------
#
# Process-local means: reset on every server restart, never persisted. The
# audit log is the durable record; these counters are the live read-out for
# /health and the rate-gate that decides when to emit the next audit line.


DEFAULT_DORMANT_LOG_SAMPLE_N = 100
DEFAULT_DORMANT_LOG_MIN_INTERVAL_SEC = 60.0


class ServingStateObservability:
    """Counters + sampler for the dormant short-circuit, decoupled from the
    state-loading registry.

    Counters:
      - ``transitions_count_in_process``         lifetime; bumped each time
                                                 a ``load()`` actually flips
                                                 the cached state.
      - ``dormant_requests_count_in_process``    lifetime; bumped on every
                                                 dormant-short-circuited
                                                 ``/score`` request.
      - ``dormant_requests_count_since_state_set``  resets on transition.
      - ``last_blocked_at_ms``                   last short-circuit timestamp.
      - ``last_loaded_at_ms``                    last ``load()`` timestamp,
                                                 transition or no-op.

    Sampler (``record_dormant_block``):
      Returns ``(count_since_last_emit, should_emit)``. ``should_emit`` is
      True for the first dormant request after a state transition (so we
      always capture the start of every dormant episode), and afterwards
      only when BOTH the rate gate (``>= sample_n`` requests since the
      last emit) AND the time gate (``>= min_interval_ms`` since the last
      emit) are satisfied. Defaults are tuned for "no more than one
      ``predict_blocked_dormant`` audit line per state per minute on a
      busy server."

    Thread safety: a single ``RLock`` guards every counter mutation. The
    lock is short (no I/O inside) so it is safe to take on every dormant
    request even at hundreds of req/sec.
    """

    def __init__(
        self,
        *,
        sample_n: int = DEFAULT_DORMANT_LOG_SAMPLE_N,
        min_interval_sec: float = DEFAULT_DORMANT_LOG_MIN_INTERVAL_SEC,
    ):
        self._sample_n = max(1, int(sample_n))
        self._min_interval_ms = int(float(min_interval_sec) * 1000.0)
        self._lock = threading.RLock()
        self._transitions = 0
        self._dormant_total = 0
        self._dormant_since_state_set = 0
        self._last_blocked_at_ms: int | None = None
        self._last_loaded_at_ms: int | None = None
        # Sampler state, scoped to the *current* state — reset on transition.
        self._count_since_last_emit = 0
        self._last_emit_at_ms: int | None = None

    @property
    def sample_n(self) -> int:
        return self._sample_n

    @property
    def min_interval_ms(self) -> int:
        return self._min_interval_ms

    def record_state_transition(self, now_ms: int) -> None:
        """Called from the load() path when the cached state actually flipped."""
        with self._lock:
            self._transitions += 1
            self._dormant_since_state_set = 0
            self._count_since_last_emit = 0
            self._last_emit_at_ms = None
            self._last_loaded_at_ms = int(now_ms)

    def record_load_noop(self, now_ms: int) -> None:
        """Called from the load() path when load() ran but state did not flip."""
        with self._lock:
            self._last_loaded_at_ms = int(now_ms)

    def record_dormant_block(self, now_ms: int) -> tuple[int, bool]:
        """Bump counters for one dormant short-circuit. Returns
        ``(count_since_last_emit, should_emit)``. Caller is responsible for
        actually emitting the audit event when ``should_emit`` is True.
        """
        now_ms = int(now_ms)
        with self._lock:
            self._dormant_total += 1
            self._dormant_since_state_set += 1
            self._count_since_last_emit += 1
            self._last_blocked_at_ms = now_ms

            count = self._count_since_last_emit
            last_emit = self._last_emit_at_ms
            if last_emit is None:
                # First dormant block in this state — always capture.
                should_emit = True
            else:
                rate_ok = count >= self._sample_n
                time_ok = (now_ms - last_emit) >= self._min_interval_ms
                should_emit = rate_ok and time_ok

            if should_emit:
                self._last_emit_at_ms = now_ms
                # Reset the sampler counter so the next emit needs another
                # ``sample_n`` requests on top of the time gate.
                self._count_since_last_emit = 0

            return count, should_emit

    def counters_snapshot(self) -> dict[str, Any]:
        """Return a /health-suitable dict of all counters.

        Field names are stable; downstream automation reads these. The
        values reset on process restart — documented as ``_in_process``
        in the field names so it cannot be confused with a durable count.
        """
        with self._lock:
            return {
                "transitions_count_in_process": int(self._transitions),
                "dormant_requests_count_in_process": int(self._dormant_total),
                "dormant_requests_count_since_state_set": int(
                    self._dormant_since_state_set
                ),
                "last_blocked_at_ms": self._last_blocked_at_ms,
                "last_loaded_at_ms": self._last_loaded_at_ms,
                "dormant_log_sample_n": self._sample_n,
                "dormant_log_min_interval_ms": self._min_interval_ms,
            }
