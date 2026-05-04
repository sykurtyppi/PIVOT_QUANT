"""Pre-registration enforcement (RESEARCH_PROTOCOL §1, §7.2).

A registration is a JSON document at
``reports/research_protocol/registrations/{candidate_id}.json`` whose
``registration_hash`` field equals the SHA256 of its canonical-JSON
serialization with that hash field removed. Any drift between the
recorded hash and the recomputed hash raises
:class:`RegistrationHashMismatchError`.

This module is read-only over registration files. There is no
``register_candidate`` here; writing a new registration is a separate
process tracked at the program level (§8.3, future PR).
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.research_protocol._paths import registrations_dir
from services.research_protocol.errors import (
    RegistrationHashMismatchError,
    RegistrationInvalidError,
    RegistrationMissingError,
)

CANDIDATE_ID_PATTERN = re.compile(r"^[a-z][a-z0-9]*(-[a-z0-9]+)*$")
HASH_FIELD = "registration_hash"

REQUIRED_TOP_LEVEL_KEYS: tuple[str, ...] = (
    "candidate_id",
    HASH_FIELD,
    "registration_timestamp",
    "git_commit_sha",
    "hypothesis",
    "features",
    "thresholds",
    "transformations",
    "forbidden_changes",
    "falsification",
    "datasets",
    "horizon_days",
    "random_seed",
    "stages_required",
)
REQUIRED_HYPOTHESIS_KEYS: tuple[str, ...] = (
    "mechanism",
    "predicted_direction",
    "why_might_fail",
    "citations",
)
ALLOWED_PREDICTED_DIRECTIONS: frozenset[str] = frozenset({"long", "short", "hedge"})


@dataclass(frozen=True)
class Registration:
    """In-memory view of a validated registration document."""

    candidate_id: str
    registration_hash: str
    body: dict[str, Any]

    @property
    def horizon_days(self) -> int:
        return int(self.body["horizon_days"])

    @property
    def stages_required(self) -> list[int]:
        return list(self.body["stages_required"])

    @property
    def random_seed(self) -> int:
        return int(self.body["random_seed"])


def canonical_json(payload: dict[str, Any]) -> str:
    """Sorted-key, no-whitespace JSON used for hashing.

    The canonical form is independent of insertion order and whitespace
    so a researcher cannot change the hash by reformatting.
    """
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )


def compute_registration_hash(payload: dict[str, Any]) -> str:
    """SHA256 over canonical JSON of payload with HASH_FIELD removed."""
    body = {k: v for k, v in payload.items() if k != HASH_FIELD}
    return hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()


def _assert_schema(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise RegistrationInvalidError(
            f"registration payload must be a dict; got {type(payload).__name__}"
        )
    missing = [k for k in REQUIRED_TOP_LEVEL_KEYS if k not in payload]
    if missing:
        raise RegistrationInvalidError(
            f"registration missing required top-level keys: {missing}"
        )
    cid = payload["candidate_id"]
    if not isinstance(cid, str) or not CANDIDATE_ID_PATTERN.match(cid):
        raise RegistrationInvalidError(
            f"candidate_id must match {CANDIDATE_ID_PATTERN.pattern};"
            f" got {cid!r}"
        )
    if not isinstance(payload["hypothesis"], dict):
        raise RegistrationInvalidError(
            f"hypothesis must be a dict; got {type(payload['hypothesis']).__name__}"
        )
    h_missing = [
        k for k in REQUIRED_HYPOTHESIS_KEYS if k not in payload["hypothesis"]
    ]
    if h_missing:
        raise RegistrationInvalidError(
            f"hypothesis missing required keys: {h_missing}"
        )
    direction = payload["hypothesis"]["predicted_direction"]
    if direction not in ALLOWED_PREDICTED_DIRECTIONS:
        raise RegistrationInvalidError(
            f"hypothesis.predicted_direction must be one of"
            f" {sorted(ALLOWED_PREDICTED_DIRECTIONS)}; got {direction!r}"
        )
    horizon = payload.get("horizon_days")
    if not isinstance(horizon, int) or isinstance(horizon, bool) or horizon < 1:
        raise RegistrationInvalidError(
            f"horizon_days must be a positive int; got {horizon!r}"
        )
    seed = payload.get("random_seed")
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise RegistrationInvalidError(
            f"random_seed must be an int; got {seed!r}"
        )
    stages = payload.get("stages_required")
    if (
        not isinstance(stages, list)
        or not stages
        or not all(isinstance(s, int) and 1 <= s <= 6 for s in stages)
    ):
        raise RegistrationInvalidError(
            f"stages_required must be a non-empty list of ints in [1,6];"
            f" got {stages!r}. Stage 0 (dataset/infrastructure smoke) is"
            " implicit in every registration and must not be listed here;"
            " runnable ladder stages begin at 1."
        )
    if not isinstance(payload.get("features"), list) or not payload["features"]:
        raise RegistrationInvalidError(
            "features must be a non-empty list"
        )
    if (
        not isinstance(payload.get("thresholds"), list)
        or not payload["thresholds"]
    ):
        raise RegistrationInvalidError(
            "thresholds must be a non-empty list"
        )
    if not isinstance(payload.get("transformations"), dict):
        raise RegistrationInvalidError(
            "transformations must be a dict with 'allowed' and"
            " 'forbidden_unless_listed' keys;"
            f" got {type(payload.get('transformations')).__name__}"
        )
    if not isinstance(payload.get("forbidden_changes"), list):
        raise RegistrationInvalidError(
            "forbidden_changes must be a list of strings describing"
            " what is locked after registration;"
            f" got {type(payload.get('forbidden_changes')).__name__}"
        )
    if not isinstance(payload.get("falsification"), dict):
        raise RegistrationInvalidError("falsification must be a dict")
    if not isinstance(payload.get("datasets"), dict):
        raise RegistrationInvalidError("datasets must be a dict")
    if not isinstance(payload[HASH_FIELD], str) or len(payload[HASH_FIELD]) != 64:
        raise RegistrationInvalidError(
            f"{HASH_FIELD} must be a 64-char hex SHA256 string;"
            f" got {payload[HASH_FIELD]!r}"
        )


def assert_registration_valid(payload: dict[str, Any]) -> None:
    """Validate schema then verify the registration_hash field.

    Raises :class:`RegistrationInvalidError` on schema failure or
    :class:`RegistrationHashMismatchError` if the recorded hash does
    not match the recomputed canonical hash.
    """
    _assert_schema(payload)
    expected = compute_registration_hash(payload)
    actual = payload[HASH_FIELD]
    if actual != expected:
        raise RegistrationHashMismatchError(
            f"registration_hash mismatch for candidate_id="
            f"{payload['candidate_id']!r}: expected={expected}, got={actual}."
            " The registration JSON was edited after signing; treat as a"
            " new candidate and re-register."
        )


def registration_path(candidate_id: str) -> Path:
    return registrations_dir() / f"{candidate_id}.json"


def assert_registration_exists(candidate_id: str) -> Path:
    """Return the registration path or raise :class:`RegistrationMissingError`."""
    path = registration_path(candidate_id)
    if not path.exists():
        raise RegistrationMissingError(
            f"no registration at {path}. Validation runs may not proceed"
            " without a pre-registration; create the JSON, hash it, and re-run."
        )
    return path


def load_registration(candidate_id: str) -> Registration:
    """Load and fully validate the registration. Read-only."""
    path = assert_registration_exists(candidate_id)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RegistrationInvalidError(
            f"registration at {path} is not valid JSON: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise RegistrationInvalidError(
            f"registration at {path} must contain a JSON object at the top level"
        )
    if payload.get("candidate_id") != candidate_id:
        raise RegistrationInvalidError(
            f"registration at {path} has candidate_id="
            f"{payload.get('candidate_id')!r} which does not match the"
            f" requested {candidate_id!r}"
        )
    assert_registration_valid(payload)
    return Registration(
        candidate_id=payload["candidate_id"],
        registration_hash=payload[HASH_FIELD],
        body=payload,
    )
