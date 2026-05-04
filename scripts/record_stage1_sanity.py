#!/usr/bin/env python3
"""Record a Stage 1 (in-sample sanity) result for a registered candidate.

Stage 1 is the implementation sanity check: confirm the signal produces
output, the feature pipeline runs without errors, and in-sample mechanics
look reasonable. It does NOT require statistical guards (those start at
stage 2). The researcher decides pass/fail and supplies a reason.

Usage — pass::

    .venv/bin/python scripts/record_stage1_sanity.py \\
        --candidate-id sanity-check-001 \\
        --dataset-identifier "SPY_2024_in_sample" \\
        --passed

Usage — fail::

    .venv/bin/python scripts/record_stage1_sanity.py \\
        --candidate-id sanity-check-001 \\
        --dataset-identifier "SPY_2024_in_sample" \\
        --failed \\
        --reason "Feature pipeline produced NaN for 30% of rows"

The script enforces the full protocol gate before writing anything:
registration must be valid, trial budget must not be exhausted, the
candidate must not be on the kill list, and stage 1 must be the next
allowable stage. A passing stage 1 record is required before the
stage 2 OOS validation script will allow execution.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(ROOT / ".env", override=False)
except ImportError:
    pass

PROTOCOL_STAGE = 1  # in-sample sanity (RESEARCH_PROTOCOL §3, stage 1)
DEFAULT_REPORT_DIR = ROOT / "reports" / "research_protocol" / "stage1"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Record a Stage 1 (in-sample sanity) result for a registered"
            " candidate. Enforces the full protocol gate before writing."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--candidate-id",
        dest="candidate_id",
        required=True,
        help="Pre-registered candidate ID (must exist in registrations/).",
    )
    parser.add_argument(
        "--dataset-identifier",
        dest="dataset_identifier",
        required=True,
        help=(
            "Human-readable identifier for the dataset used (e.g."
            " 'SPY_2024_in_sample'). Recorded in the stage artifact and"
            " the validation-ladder state."
        ),
    )
    parser.add_argument(
        "--report-path",
        dest="report_path",
        default=None,
        help=(
            "Output path for the stage 1 report JSON. Defaults to"
            " reports/research_protocol/stage1/{candidate_id}_stage1_sanity.json"
        ),
    )

    verdict = parser.add_mutually_exclusive_group(required=True)
    verdict.add_argument(
        "--passed",
        dest="passed",
        action="store_true",
        help="Record stage 1 as passed.",
    )
    verdict.add_argument(
        "--failed",
        dest="passed",
        action="store_false",
        help="Record stage 1 as failed (permanently blocks later stages for this candidate).",
    )

    parser.add_argument(
        "--reason",
        dest="reason",
        default=None,
        help="Optional free-text explanation of the pass/fail decision.",
    )
    return parser.parse_args(argv)


def _resolve_report_path(candidate_id: str, report_path: str | None) -> Path:
    if report_path:
        p = Path(report_path)
        return p if p.is_absolute() else ROOT / p
    return DEFAULT_REPORT_DIR / f"{candidate_id}_stage1_sanity.json"


def main(argv: list[str] | None = None) -> int:
    from services.research_protocol.errors import ProtocolViolationError
    from services.research_protocol.protocol_guard import assert_protocol_compliant
    from services.research_protocol.validation_ladder import record_stage_result

    args = parse_args(argv)

    # Enforce the full protocol gate before any file I/O.
    try:
        registration = assert_protocol_compliant(
            args.candidate_id,
            requested_stage=PROTOCOL_STAGE,
        )
    except ProtocolViolationError as exc:
        print(f"[protocol] BLOCKED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    report_path = _resolve_report_path(args.candidate_id, args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now(tz=timezone.utc).isoformat()

    report = {
        "candidate_id": args.candidate_id,
        "registration_hash": registration.registration_hash,
        "protocol_stage": PROTOCOL_STAGE,
        "stage_name": "stage_1_in_sample_sanity",
        "passed": args.passed,
        "dataset_identifier": args.dataset_identifier,
        "reason": args.reason,
        "run_timestamp": run_timestamp,
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    metadata = {
        "run_timestamp": run_timestamp,
        "dataset_identifier": args.dataset_identifier,
    }
    if args.reason:
        metadata["reason"] = args.reason

    try:
        record_stage_result(
            candidate_id=args.candidate_id,
            stage=PROTOCOL_STAGE,
            passed=args.passed,
            report_path=str(report_path),
            metadata=metadata,
            registration_hash=registration.registration_hash,
        )
    except ProtocolViolationError as exc:
        print(
            f"[protocol] ERROR recording stage result: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1

    verdict_str = "PASS" if args.passed else "FAIL"
    print(f"candidate_id : {args.candidate_id}")
    print(f"stage        : {PROTOCOL_STAGE} (stage_1_in_sample_sanity)")
    print(f"verdict      : {verdict_str}")
    print(f"report_path  : {report_path}")
    if args.reason:
        print(f"reason       : {args.reason}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
