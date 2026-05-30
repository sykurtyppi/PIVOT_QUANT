#!/usr/bin/env python3
"""Shared per-(target, horizon) threshold-override parsing/resolution.

Single source of truth for the ``--threshold-*-overrides`` CLI maps so that the
two threshold-selecting entry points cannot drift apart:

  - ``scripts/train_rf_artifacts.py`` (canonical retrain path)
  - ``scripts/refit_calibration.py`` (calibration-only refit / re-tune path)

Before this module existed, ``train_rf_artifacts.py`` carried per-target/horizon
``min_signals`` overrides (e.g. ``break:15=8,break:30=8,break:60=6``) while
``refit_calibration.py`` used a single flat ``--threshold-min-signals``. When
refit re-tuned thresholds it demanded more predicted positives than train would
on the same sparse break heads, flipping otherwise-valid thresholds to fallback
(no-trade) and silently shipping a strictly worse threshold. Centralizing the
parser/resolver here removes that divergence at the root.
"""

from __future__ import annotations

import re

__all__ = ["parse_threshold_overrides", "resolve_threshold_override"]


def parse_threshold_overrides(
    raw_value: str,
    *,
    value_cast,
    option_name: str,
) -> dict[tuple[str, int | None], float | int]:
    """Parse target/horizon override map.

    Format: "break:15=8,break:30=8,break:60=6,reject:*=10"
    Keys accept ":" "/" "_" "-" separators and optional "m" suffix.
    """
    parsed: dict[tuple[str, int | None], float | int] = {}
    if raw_value is None:
        return parsed

    for token in str(raw_value).split(","):
        item = token.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(
                f"{option_name}: invalid entry {item!r}; expected '<target>:<horizon>=<value>'"
            )
        key_raw, value_raw = item.split("=", 1)
        key = key_raw.strip().lower()
        value_text = value_raw.strip()
        if not value_text:
            raise ValueError(f"{option_name}: missing value in entry {item!r}")

        match = re.match(r"^(reject|break)\s*[:/_-]\s*([0-9]+m?|all|\*)$", key)
        if not match:
            raise ValueError(
                f"{option_name}: invalid key {key_raw!r}; expected reject|break + horizon (e.g. break:15)"
            )

        target = str(match.group(1)).lower()
        horizon_token = str(match.group(2)).lower()
        horizon: int | None
        if horizon_token in {"all", "*"}:
            horizon = None
        else:
            horizon = int(horizon_token.rstrip("m"))

        try:
            value = value_cast(value_text)
        except Exception as exc:
            raise ValueError(f"{option_name}: invalid value {value_text!r} in entry {item!r}: {exc}") from exc

        parsed[(target, horizon)] = value
    return parsed


def resolve_threshold_override(
    *,
    target: str,
    horizon: int,
    base_value: float | int,
    overrides: dict[tuple[str, int | None], float | int],
) -> float | int:
    target_key = str(target).strip().lower()
    direct_key = (target_key, int(horizon))
    if direct_key in overrides:
        return overrides[direct_key]
    wildcard_key = (target_key, None)
    if wildcard_key in overrides:
        return overrides[wildcard_key]
    return base_value
