"""CLI integration for protocol enforcement.

Standardizes how research scripts wire :func:`assert_protocol_compliant`
into their entry points. Two helpers:

  - :func:`add_protocol_arguments` adds ``--candidate-id``,
    ``--protocol-stage``, and the mutually-exclusive
    ``--enforce-protocol`` / ``--no-enforce-protocol`` flags to an
    existing :class:`argparse.ArgumentParser`.

  - :func:`enforce_protocol_from_args` reads those flags and either:

      * if enforcement is on, calls
        :func:`protocol_guard.assert_protocol_compliant` and returns the
        loaded :class:`Registration`. Missing ``candidate_id`` /
        ``protocol_stage`` raise :class:`ProtocolCLIError`. A supplied
        stage that disagrees with the script's
        ``expected_stage`` raises.

      * if enforcement is off, prints a diagnostic-only warning to
        stderr and returns ``None``. The caller proceeds with the
        legacy unguarded code path.

The helpers do not run any analysis themselves. Scripts call
:func:`enforce_protocol_from_args` at the top of ``main()`` before
opening any dataset; if it returns a :class:`Registration` the script
can also reference its hash in downstream artifacts.

Stage mapping documented in
``docs/RESEARCH_PROTOCOL_ENFORCEMENT.md`` §9 — scripts should pass the
stage they correspond to as ``expected_stage`` so a mismatch (e.g., a
user running the cross-period script with ``--protocol-stage=2``) is
rejected at parse time.
"""

from __future__ import annotations

import argparse
import sys
from typing import TextIO

from services.research_protocol.errors import ProtocolCLIError
from services.research_protocol.protocol_guard import assert_protocol_compliant
from services.research_protocol.registration import Registration

PROTOCOL_DISABLED_WARNING: str = (
    "[protocol] WARNING: Protocol enforcement disabled; output is"
    " diagnostic only.\n"
)


def add_protocol_arguments(
    parser: argparse.ArgumentParser,
    *,
    expected_stage: int | None = None,
) -> None:
    """Add the standard protocol CLI flags to ``parser``.

    Adds:
      - ``--candidate-id <str>``
      - ``--protocol-stage <int>`` (defaults to ``expected_stage`` if
        provided so users running this script under enforcement do not
        have to repeat the stage they already implied by choosing this
        script)
      - ``--enforce-protocol`` (sets ``enforce_protocol=True``)
      - ``--no-enforce-protocol`` (sets ``enforce_protocol=False``)

    The ``enforce_protocol`` attribute defaults to False to preserve
    legacy non-enforcing behavior; users opt in explicitly.
    """
    group = parser.add_argument_group("research protocol enforcement")
    group.add_argument(
        "--candidate-id",
        dest="candidate_id",
        default=None,
        help=(
            "Pre-registered candidate ID. Required when --enforce-protocol"
            " is set."
        ),
    )
    group.add_argument(
        "--protocol-stage",
        dest="protocol_stage",
        type=int,
        default=expected_stage,
        help=(
            "Validation-ladder stage this run targets (0..6)."
            + (
                f" Defaults to {expected_stage} for this script."
                if expected_stage is not None
                else " Required when --enforce-protocol is set."
            )
        ),
    )
    enforce = group.add_mutually_exclusive_group()
    enforce.add_argument(
        "--enforce-protocol",
        dest="enforce_protocol",
        action="store_true",
        default=False,
        help=(
            "Enforce the research protocol: requires --candidate-id and"
            " --protocol-stage; calls assert_protocol_compliant and"
            " refuses to proceed on any violation."
        ),
    )
    enforce.add_argument(
        "--no-enforce-protocol",
        dest="enforce_protocol",
        action="store_false",
        default=False,
        help=(
            "Explicitly disable protocol enforcement (default). The"
            " script prints a diagnostic-only warning and proceeds."
        ),
    )


def enforce_protocol_from_args(
    args: argparse.Namespace,
    *,
    expected_stage: int | None = None,
    stream: TextIO | None = None,
) -> Registration | None:
    """Apply protocol enforcement based on parsed CLI args.

    When ``args.enforce_protocol`` is True:
      - ``args.candidate_id`` must be a non-empty string.
      - ``args.protocol_stage`` must be an int in [0, 6].
      - If ``expected_stage`` is provided, it must equal
        ``args.protocol_stage``.
      - :func:`assert_protocol_compliant` is called; the returned
        :class:`Registration` is returned to the caller.

    When ``args.enforce_protocol`` is False (the default):
      - A diagnostic-only warning is written to ``stream`` (default
        ``sys.stderr``).
      - Returns ``None``. The caller proceeds with legacy behavior.

    Raises:
      :class:`ProtocolCLIError` for missing or mismatched flags under
      enforcement.
      :class:`ProtocolViolationError` (any subclass) propagated from
      :func:`assert_protocol_compliant` when enforcement fires.
    """
    out = stream if stream is not None else sys.stderr
    enforce = bool(getattr(args, "enforce_protocol", False))
    if not enforce:
        out.write(PROTOCOL_DISABLED_WARNING)
        return None

    candidate_id = getattr(args, "candidate_id", None)
    if not isinstance(candidate_id, str) or not candidate_id:
        raise ProtocolCLIError(
            "--enforce-protocol requires --candidate-id <id>"
        )

    stage = getattr(args, "protocol_stage", None)
    if stage is None:
        raise ProtocolCLIError(
            "--enforce-protocol requires --protocol-stage <0..6>"
        )
    if not isinstance(stage, int) or isinstance(stage, bool) or not (0 <= stage <= 6):
        raise ProtocolCLIError(
            f"--protocol-stage must be an int in [0, 6]; got {stage!r}"
        )
    if expected_stage is not None and stage != expected_stage:
        raise ProtocolCLIError(
            f"this script runs at protocol stage {expected_stage};"
            f" got --protocol-stage={stage}. Either use the matching"
            " stage flag or run a script that targets a different stage."
        )

    return assert_protocol_compliant(
        candidate_id, requested_stage=stage,
    )
