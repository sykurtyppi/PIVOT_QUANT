#!/usr/bin/env python3
"""Create a validated research-protocol registration from a TOML body file.

Closes the manual-JSON-editing gap in the enforcement workflow
(RESEARCH_PROTOCOL.md §1, §7.2). The researcher supplies a TOML file
describing the registration body; this script auto-fills the three
fields that must not be hand-written:

  - ``registration_timestamp`` — UTC ISO8601 (Z-suffixed) at run time,
  - ``git_commit_sha``         — output of ``git rev-parse HEAD``,
  - ``registration_hash``      — :func:`compute_registration_hash`
                                 over the canonical-JSON of the
                                 payload with the hash field removed.

The result is validated via :func:`assert_registration_valid` and
written atomically to
``reports/research_protocol/registrations/<candidate_id>.json``.

Refuses to overwrite an existing registration unless ``--allow-overwrite``
is passed (overwriting breaks the audit chain — the trial budget,
ladder state, and replication evidence all key on
``registration_hash``, so a re-registration silently invalidates them).

Usage::

    python scripts/register_candidate.py --input candidate.toml

Example TOML body::

    candidate_id = "fomc-iv-crush-001"
    horizon_days = 5
    random_seed = 42
    stages_required = [1, 2, 3, 4, 5, 6]
    hypothesis_family = "iv_crush"
    forbidden_changes = ["any threshold change", "any feature change"]

    [hypothesis]
    mechanism = "post-FOMC dealer hedging compresses near-dated IV"
    predicted_direction = "short"
    why_might_fail = "regime-conditional liquidity"
    citations = ["paper:fomc-iv-2024"]

    [[features]]
    name = "iv_change_5d"
    input_columns = ["iv"]

    [[thresholds]]
    name = "iv_drop_threshold"
    kind = "fixed"
    value = -0.05

    [transformations]
    allowed = ["log"]
    forbidden_unless_listed = ["any"]

    [falsification]
    stage_3 = "cross_period_validated=false"

    [datasets]
    symbol = "SPY"
    validation_dataset_pattern = "spy_2025_validation.parquet"
    holdout_dataset_pattern = "spy_2018_2020_holdout.parquet"
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.research_protocol.errors import (  # noqa: E402
    RegistrationHashMismatchError,
    RegistrationInvalidError,
)
from services.research_protocol.registration import (  # noqa: E402
    HASH_FIELD,
    assert_registration_valid,
    compute_registration_hash,
)

# Fields that the helper computes; they may not appear in the input file.
_AUTO_FILLED_FIELDS: frozenset[str] = frozenset({
    HASH_FIELD,
    "registration_timestamp",
    "git_commit_sha",
})

EXIT_OK = 0
EXIT_OVERWRITE_REFUSED = 1
EXIT_USER_ERROR = 2

# Valid TOML registration template printed by --print-toml-template.
# Structurally complete and accepted by assert_registration_valid after
# the user replaces the candidate_id placeholder (which deliberately
# contains uppercase characters so the protocol's CANDIDATE_ID_PATTERN
# rejects it until edited).
_TOML_TEMPLATE = """\
# PivotQuant research-protocol registration template.
#
# Edit the fields below before running:
#   1. Replace candidate_id with a kebab-case identifier matching
#      ^[a-z][a-z0-9]*(-[a-z0-9]+)*$
#   2. Adjust the [hypothesis] block to describe a concrete economic or
#      microstructure mechanism.
#   3. Adjust features, thresholds, transformations, falsification, and
#      datasets to fit your hypothesis.
#   4. Run:
#        python scripts/register_candidate.py --input <this-file>.toml
#
# Do NOT add registration_hash, registration_timestamp, or
# git_commit_sha — those are auto-filled by the CLI.

candidate_id = "REPLACE-WITH-CANDIDATE-ID"
horizon_days = 5
random_seed = 42
stages_required = [1, 2, 3, 4, 5, 6]
forbidden_changes = [
    "any threshold change",
    "any feature change",
    "any change to the forward-return horizon",
]

[hypothesis]
mechanism = "Describe the economic or microstructure mechanism the signal exploits."
predicted_direction = "long"
why_might_fail = "Describe the regimes or conditions under which the mechanism breaks."
citations = ["paper:replace-with-citation"]

[[features]]
name = "feature_a"
input_columns = ["close"]

[[thresholds]]
name = "threshold_a"
kind = "fixed"
value = 0.5

[transformations]
allowed = ["log", "z_score_train"]
forbidden_unless_listed = ["any non-monotonic transformation"]

[falsification]
stage_3 = "cross_period_validated=false"

[datasets]
symbol = "SPY"
validation_dataset_pattern = "spy_2025_validation.parquet"
holdout_dataset_pattern = "spy_2018_2020_holdout.parquet"
"""


# --------------------------------------------------------------------- #
# Auto-fill helpers
# --------------------------------------------------------------------- #


def detect_git_commit_sha(*, cwd: Path | None = None) -> str:
    """Return the current git HEAD SHA.

    Raises :class:`RuntimeError` if git is unavailable, the directory
    is not a git repo, or the returned value is not a 40-char hex
    string. The script defaults to ``cwd=ROOT`` so that the SHA reflects
    the PivotQuant repo root regardless of where the user invoked from.
    """
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git rev-parse HEAD failed (returncode={result.returncode});"
            f" stderr={result.stderr.strip()!r}"
        )
    sha = result.stdout.strip()
    if len(sha) != 40 or any(c not in "0123456789abcdef" for c in sha.lower()):
        raise RuntimeError(
            f"git rev-parse HEAD returned non-hex SHA: {sha!r}"
        )
    return sha


def utc_now_iso() -> str:
    """ISO8601 UTC timestamp matching existing fixtures (Z suffix)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------- #
# TOML loader + payload builder
# --------------------------------------------------------------------- #


def load_toml(path: Path) -> dict[str, Any]:
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def build_registration(
    body: dict[str, Any],
    *,
    git_commit_sha: str,
    registration_timestamp: str,
) -> dict[str, Any]:
    """Compose the canonical payload from a TOML body.

    Rejects any pre-supplied auto-filled field with a clear message —
    the entire point of this helper is to be the single source of those
    three values.
    """
    if not isinstance(body, dict):
        raise ValueError(
            f"TOML body must be a table at the top level; got"
            f" {type(body).__name__}"
        )
    overlap = sorted(_AUTO_FILLED_FIELDS & set(body.keys()))
    if overlap:
        raise ValueError(
            f"input must not contain auto-filled fields {overlap};"
            " these are computed by register_candidate.py."
            " Remove them from the TOML and rerun."
        )
    payload: dict[str, Any] = dict(body)
    payload["registration_timestamp"] = registration_timestamp
    payload["git_commit_sha"] = git_commit_sha
    payload[HASH_FIELD] = compute_registration_hash(payload)
    return payload


# --------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------- #


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a validated research-protocol registration from a"
            " TOML body file. Auto-fills registration_timestamp,"
            " git_commit_sha, and registration_hash; validates against"
            " services.research_protocol.registration; writes to"
            " reports/research_protocol/registrations/<candidate_id>.json."
        ),
    )
    parser.add_argument(
        "--input",
        dest="input_path",
        type=Path,
        default=None,
        help=(
            "Path to a TOML file describing the registration body."
            " Must NOT contain registration_timestamp, git_commit_sha,"
            " or registration_hash (those are auto-filled). Required"
            " unless --print-toml-template is given."
        ),
    )
    parser.add_argument(
        "--print-toml-template",
        dest="print_toml_template",
        action="store_true",
        help=(
            "Print a valid TOML registration template to stdout and exit."
            " Does not require --input, performs no file writes, and"
            " does not run validation or hashing."
        ),
    )
    parser.add_argument(
        "--reports-dir",
        dest="reports_dir",
        type=Path,
        default=None,
        help=(
            "Override the output directory for the registration JSON."
            " Default: reports/research_protocol/registrations/ relative"
            " to the protocol root (honors PIVOTQUANT_RESEARCH_PROTOCOL_ROOT"
            " when set)."
        ),
    )
    parser.add_argument(
        "--git-commit-sha",
        dest="git_commit_sha",
        default=None,
        help=(
            "Override the git commit SHA. Default: result of"
            " 'git rev-parse HEAD' inside the PivotQuant root."
        ),
    )
    parser.add_argument(
        "--registration-timestamp",
        dest="registration_timestamp",
        default=None,
        help=(
            "Override the registration timestamp (ISO8601 UTC)."
            " Default: now in UTC, formatted as YYYY-MM-DDTHH:MM:SSZ."
        ),
    )
    parser.add_argument(
        "--allow-overwrite",
        dest="allow_overwrite",
        action="store_true",
        help=(
            "Allow overwriting an existing registration file. Disabled"
            " by default because overwriting breaks the audit chain"
            " (trial-budget, ladder state, and replication evidence all"
            " key on registration_hash)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help=(
            "Validate and print the payload to stdout without writing"
            " anything to disk."
        ),
    )
    return parser.parse_args(argv)


def _resolve_output_dir(reports_dir: Path | None) -> Path:
    if reports_dir is not None:
        return reports_dir.expanduser().resolve()
    # Lazy import to keep tests clean when overriding via --reports-dir.
    from services.research_protocol._paths import registrations_dir
    return registrations_dir()


def _atomic_write_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def _print_next_command(payload: dict[str, Any]) -> None:
    cid = payload["candidate_id"]
    stages = payload.get("stages_required") or []
    first_stage = stages[0] if stages else 1
    print()
    print("Next command (typical Stage 1 entry):")
    if first_stage == 1:
        print(f"  .venv/bin/python scripts/record_stage1_sanity.py \\")
        print(f"      --candidate-id {cid} \\")
        print(f"      --dataset-identifier <DATASET> \\")
        print(f"      --enforce-protocol \\")
        print(f"      --passed   # or --failed --reason <REASON>")
    else:
        print(
            f"  .venv/bin/python scripts/run_ml_regime_validation.py \\"
        )
        print(f"      --candidate-id {cid} \\")
        print(f"      --enforce-protocol")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    # --print-toml-template short-circuit: print and exit cleanly with
    # zero side effects (no validation, no hashing, no filesystem writes).
    if args.print_toml_template:
        print(_TOML_TEMPLATE)
        return EXIT_OK

    if args.input_path is None:
        print(
            "[register_candidate] --input is required (or pass"
            " --print-toml-template to emit a valid TOML skeleton).",
            file=sys.stderr,
        )
        return EXIT_USER_ERROR

    if not args.input_path.exists():
        print(
            f"[register_candidate] input file not found: {args.input_path}",
            file=sys.stderr,
        )
        return EXIT_USER_ERROR

    try:
        body = load_toml(args.input_path)
    except tomllib.TOMLDecodeError as exc:
        print(
            f"[register_candidate] {args.input_path} is not valid TOML: {exc}",
            file=sys.stderr,
        )
        return EXIT_USER_ERROR

    try:
        git_sha = (
            args.git_commit_sha
            if args.git_commit_sha
            else detect_git_commit_sha(cwd=ROOT)
        )
    except RuntimeError as exc:
        print(
            f"[register_candidate] could not detect git commit SHA: {exc}."
            " Pass --git-commit-sha explicitly to override.",
            file=sys.stderr,
        )
        return EXIT_USER_ERROR

    timestamp = args.registration_timestamp or utc_now_iso()

    try:
        payload = build_registration(
            body,
            git_commit_sha=git_sha,
            registration_timestamp=timestamp,
        )
    except ValueError as exc:
        print(f"[register_candidate] {exc}", file=sys.stderr)
        return EXIT_USER_ERROR

    try:
        assert_registration_valid(payload)
    except RegistrationInvalidError as exc:
        print(
            f"[register_candidate] schema validation failed: {exc}",
            file=sys.stderr,
        )
        return EXIT_USER_ERROR
    except RegistrationHashMismatchError as exc:
        # Should not be reachable: we just computed the hash.
        print(
            f"[register_candidate] internal hash mismatch (bug): {exc}",
            file=sys.stderr,
        )
        return EXIT_USER_ERROR

    if args.dry_run:
        print("[register_candidate] DRY RUN — not writing to disk")
        print(json.dumps(payload, indent=2, sort_keys=True))
        return EXIT_OK

    out_dir = _resolve_output_dir(args.reports_dir)
    out_path = out_dir / f"{payload['candidate_id']}.json"

    if out_path.exists() and not args.allow_overwrite:
        print(
            f"[register_candidate] refusing to overwrite existing"
            f" registration: {out_path}\n"
            "Overwriting breaks the audit chain (kill list, ladder, and"
            " trial budget all key on registration_hash). Pass"
            " --allow-overwrite if that's really what you intend.",
            file=sys.stderr,
        )
        return EXIT_OVERWRITE_REFUSED

    _atomic_write_json(payload, out_path)

    print(f"candidate_id:        {payload['candidate_id']}")
    print(f"registration_hash:   {payload[HASH_FIELD]}")
    print(f"registration_path:   {out_path}")
    _print_next_command(payload)
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
