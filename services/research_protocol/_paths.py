"""Filesystem paths for research-protocol artifacts.

The protocol root defaults to ``reports/research_protocol`` under the
current working directory. Tests override the root via the
``PIVOTQUANT_RESEARCH_PROTOCOL_ROOT`` environment variable.
"""

from __future__ import annotations

import os
from pathlib import Path

ENV_PROTOCOL_ROOT = "PIVOTQUANT_RESEARCH_PROTOCOL_ROOT"
DEFAULT_PROTOCOL_ROOT = "reports/research_protocol"


def protocol_root() -> Path:
    return Path(
        os.environ.get(ENV_PROTOCOL_ROOT, DEFAULT_PROTOCOL_ROOT)
    ).expanduser().resolve()


def registrations_dir() -> Path:
    return protocol_root() / "registrations"


def kill_list_path() -> Path:
    return protocol_root() / "kill_list.json"


def validation_ladder_state_path() -> Path:
    return protocol_root() / "validation_ladder_state.json"


def replication_state_path() -> Path:
    return protocol_root() / "replication_evidence.json"


def audit_log_path() -> Path:
    return protocol_root() / "audit_log.jsonl"


def trial_budget_state_path() -> Path:
    return protocol_root() / "trial_budget_state.json"
