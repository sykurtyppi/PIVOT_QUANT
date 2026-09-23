#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import shlex
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "logs" / "reports" / "burn_in"
DEFAULT_RUNTIME_DB = ROOT / "data" / "pivot_events.sqlite"

_OPF_MODULE = None


def _load_operational_preflight():
    """Load scripts/operational_preflight.py as a module (single source of truth).

    The scoring-freshness / backlog logic lives there and is unit-tested there; the
    scorecard reuses it rather than reimplementing so the daily artifact and the
    preflight gate can never disagree.
    """
    global _OPF_MODULE
    if _OPF_MODULE is not None:
        return _OPF_MODULE
    path = Path(__file__).resolve().parent / "operational_preflight.py"
    spec = importlib.util.spec_from_file_location("operational_preflight", path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError(f"Could not load operational_preflight from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("operational_preflight", module)
    spec.loader.exec_module(module)
    _OPF_MODULE = module
    return module


@dataclass
class ReleaseCheckSummary:
    command: str
    exit_code: int
    overall_result: str
    validation_result: str
    integrity_result: str
    ui_smoke_result: str
    output: str


@dataclass
class ScoringFreshnessSummary:
    db_path: str
    overall_result: str
    lines: list[str] = field(default_factory=list)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a daily burn-in scorecard markdown artifact.")
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Scorecard trading date in YYYY-MM-DD format (default: today).",
    )
    parser.add_argument(
        "--day-number",
        type=int,
        default=None,
        help="Optional burn-in day number (for example 1..10).",
    )
    parser.add_argument(
        "--total-days",
        type=int,
        default=10,
        help="Planned burn-in trading days (default: 10).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where scorecards are written (default: logs/reports/burn_in).",
    )
    parser.add_argument(
        "--run-release-check",
        action="store_true",
        help="Run release-check and prefill release results into the scorecard.",
    )
    parser.add_argument(
        "--release-check-cmd",
        default="npm run release:check",
        help="Command to run when --run-release-check is set.",
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_RUNTIME_DB),
        help="Runtime SQLite DB for the scoring-freshness section (default: data/pivot_events.sqlite).",
    )
    parser.add_argument(
        "--skip-freshness",
        action="store_true",
        help="Do not run the scoring-freshness / backlog check.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing scorecard file for the date.",
    )
    return parser.parse_args()


def _read_result_line(output: str) -> str:
    result_value = "UNKNOWN"
    for raw in output.splitlines():
        line = raw.strip()
        if line.startswith("[RESULT] "):
            result_value = line[len("[RESULT] ") :].strip()
    return result_value


def _section_lines(output: str, section_name: str) -> list[str]:
    target = f"=== {section_name.upper()} ==="
    current = ""
    collected: list[str] = []
    for raw in output.splitlines():
        line = raw.rstrip("\n")
        if line.startswith("=== ") and line.endswith(" ==="):
            current = line.strip()
            continue
        if current == target:
            collected.append(line.strip())
    return collected


def _parse_validation_result(lines: list[str]) -> str:
    if any("[FAIL]" in line for line in lines):
        return "FAIL"
    if any("[PASS] Local validation passed" in line for line in lines):
        return "PASS"
    if any("[WARN]" in line for line in lines):
        return "WARN"
    return "UNKNOWN"


def _parse_integrity_result(lines: list[str]) -> str:
    if any("[FAIL]" in line for line in lines):
        return "FAIL"
    if any("[WARN]" in line for line in lines):
        return "WARN"
    if any("[PASS] Integrity verification passed" in line for line in lines):
        return "PASS"
    return "UNKNOWN"


def _parse_ui_smoke_result(lines: list[str]) -> str:
    if any("[FAIL]" in line for line in lines):
        return "FAIL"
    if any(line.startswith("[SKIP]") for line in lines):
        return "SKIP"
    if any("[WARN]" in line for line in lines):
        return "WARN"
    if any("[PASS] UI smoke passed" in line for line in lines):
        return "PASS"
    return "UNKNOWN"


def _run_release_check(command: str) -> ReleaseCheckSummary:
    cmd_parts = shlex.split(command)
    proc = subprocess.run(
        cmd_parts,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    output = output.strip()
    validation_lines = _section_lines(output, "validation")
    integrity_lines = _section_lines(output, "integrity")
    ui_lines = _section_lines(output, "ui smoke")
    return ReleaseCheckSummary(
        command=command,
        exit_code=proc.returncode,
        overall_result=_read_result_line(output),
        validation_result=_parse_validation_result(validation_lines),
        integrity_result=_parse_integrity_result(integrity_lines),
        ui_smoke_result=_parse_ui_smoke_result(ui_lines),
        output=output,
    )


def _worst_status(statuses: list[str]) -> str:
    """Collapse per-check statuses into one scorecard-level result.

    Precedence: FAIL dominates, then WARN, then PASS. SKIP means the check could not
    run (missing DB/deps) and is surfaced only when nothing else ran, so a genuine
    PASS is never masked by an unrelated skip.
    """
    if not statuses:
        return "UNKNOWN"
    if "FAIL" in statuses:
        return "FAIL"
    if "WARN" in statuses:
        return "WARN"
    if "PASS" in statuses:
        return "PASS"
    if "SKIP" in statuses:
        return "SKIP"
    return "UNKNOWN"


def _collect_scoring_freshness(db_path: Path) -> ScoringFreshnessSummary:
    """Run the shared scoring-freshness / backlog check against the runtime DB."""
    opf = _load_operational_preflight()
    results = opf.check_scoring_freshness(db_path)
    statuses = [result.status for result in results]
    lines = [f"[{result.status}] {result.message}" for result in results]
    return ScoringFreshnessSummary(
        db_path=str(db_path),
        overall_result=_worst_status(statuses),
        lines=lines,
    )


def _template_body(
    *,
    report_date: str,
    day_number: int | None,
    total_days: int,
    release_summary: ReleaseCheckSummary | None,
    scoring_summary: ScoringFreshnessSummary | None,
) -> str:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    day_label = f"{day_number}/{total_days}" if day_number is not None else f"--/{total_days}"

    release_result = release_summary.overall_result if release_summary else "NOT_RUN"
    validation_result = release_summary.validation_result if release_summary else "NOT_RUN"
    integrity_result = release_summary.integrity_result if release_summary else "NOT_RUN"
    ui_result = release_summary.ui_smoke_result if release_summary else "NOT_RUN"
    release_cmd = release_summary.command if release_summary else "npm run release:check"
    release_exit = str(release_summary.exit_code) if release_summary else "--"

    lines = [
        f"# Burn-In Scorecard — {report_date}",
        "",
        f"- Burn-in day: **{day_label}**",
        f"- Generated at (UTC): `{generated_at}`",
        f"- Operator: `TODO`",
        "",
        "## Release Readiness",
        f"- Command: `{release_cmd}`",
        f"- Release result: **{release_result}**",
        f"- Validation result: **{validation_result}**",
        f"- Integrity result: **{integrity_result}**",
        f"- UI smoke result: **{ui_result}**",
        f"- Release command exit code: `{release_exit}`",
        "",
    ]

    scoring_result = scoring_summary.overall_result if scoring_summary else "NOT_RUN"
    scoring_db = scoring_summary.db_path if scoring_summary else str(DEFAULT_RUNTIME_DB)
    lines.extend(
        [
            "## Scoring Freshness",
            f"- Runtime DB: `{scoring_db}`",
            f"- Scoring freshness result: **{scoring_result}**",
        ]
    )
    if scoring_summary and scoring_summary.lines:
        lines.extend(f"- `{line}`" for line in scoring_summary.lines)
    elif not scoring_summary:
        lines.append("- Detail: `NOT_RUN (freshness check skipped)`")
    lines.append("")

    lines.extend(
        [
            "## Governance Queue / Review Activity",
            "- `queue_total_count`: `TODO`",
            "- `queue_page_count`: `TODO`",
            "- `pending_review_count`: `TODO`",
            "- `needs_refresh_count`: `TODO`",
            "- `reviews_written_today`: `TODO`",
            "",
            "## Metadata / Lineage Checks",
            "- `metadata_mode=explicit` count: `TODO`",
            "- `metadata_mode=legacy_inferred` count: `TODO`",
            "- `requires_model_context_backfill=true` count: `TODO`",
            "- `source_duckdb_matches=false` count: `TODO`",
            "- `cost_model_matches=false` count: `TODO`",
            "",
            "## Research / Replay Workflow Checks",
            "- Research workspace load + slice query success: `TODO`",
            "- Walk-forward date sequence appears calendar-correct: `TODO`",
            "- Walk-forward zero-row windows present when expected: `TODO`",
            "- Replay handoff from Research works: `TODO`",
            "- Replay handoff from Governance works: `TODO`",
            "",
            "## Operator Friction Notes",
            "- Top pain points:",
            "  1. `TODO`",
            "  2. `TODO`",
            "  3. `TODO`",
            "- Action items:",
            "  1. `TODO`",
            "",
        ]
    )

    if release_summary and release_summary.output:
        lines.extend(
            [
                "## Release Output (Captured)",
                "```text",
                release_summary.output,
                "```",
                "",
            ]
        )

    return "\n".join(lines)


def main() -> int:
    args = _parse_args()

    # Validate date format early for deterministic file naming.
    try:
        parsed_date = date.fromisoformat(args.date)
    except ValueError as exc:
        raise SystemExit(f"Invalid --date '{args.date}': {exc}") from exc

    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"{parsed_date.isoformat()}_burn_in.md"
    if report_path.exists() and not args.overwrite:
        raise SystemExit(
            f"Scorecard already exists: {report_path}\n"
            "Use --overwrite to replace it."
        )

    release_summary = _run_release_check(args.release_check_cmd) if args.run_release_check else None
    scoring_summary = (
        None if args.skip_freshness else _collect_scoring_freshness(Path(args.db).expanduser())
    )
    body = _template_body(
        report_date=parsed_date.isoformat(),
        day_number=args.day_number,
        total_days=args.total_days,
        release_summary=release_summary,
        scoring_summary=scoring_summary,
    )
    report_path.write_text(body + "\n", encoding="utf-8")

    print(f"Created burn-in scorecard: {report_path}")
    if release_summary is not None:
        print(
            "Captured release-check: "
            f"result={release_summary.overall_result}, "
            f"validation={release_summary.validation_result}, "
            f"integrity={release_summary.integrity_result}, "
            f"ui_smoke={release_summary.ui_smoke_result}, "
            f"exit_code={release_summary.exit_code}"
        )
    if scoring_summary is not None:
        print(f"Scoring freshness: result={scoring_summary.overall_result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
