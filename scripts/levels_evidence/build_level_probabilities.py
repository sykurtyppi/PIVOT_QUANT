#!/usr/bin/env python3
"""Step 5 producer: reshape the level-behavior evidence into a per-LEVEL contract
the /levels page can render directly (prereg §8).

For each displayed realized-vol band level (+1σ/+2σ upper, −1σ/−2σ lower) emits,
per outcome (touch, close-beyond):
  - the unconditional base rate (rate, Wilson CI, n),
  - the gap-bucketed descriptive rates (up/flat/down) with n and the §5
    sufficiency flag, and
  - the §7/§10 out-of-sample verdict (best predictor; whether gap beats
    unconditional and by how much), so the page shows a gap-conditioned number
    only where it is earned, abstains when a bucket is thin, and otherwise falls
    back to the unconditional base rate.

Single source of truth: recomputes from the committed snapshot via the existing
build_daily_event_table + regime_calibration modules — no parallel estimator, no
lookahead. Deterministic; offline (Mac Mini). Writes
research/levels_evidence/level_probabilities.json (served to /levels).
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    m = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(m)
    return m


_b = _load("levels_evidence_build", "scripts/levels_evidence/build_daily_event_table.py")
_rc = _load("regime_calibration", "scripts/levels_evidence/regime_calibration.py")

# displayed band level -> (touch outcome, close-beyond outcome, k, gap side that favors it, label)
LEVELS = [
    ("u1", "touch_u1", "close_above_u1", 1, "up",   "+1σ (upper)"),
    ("u2", "touch_u2", "close_above_u2", 2, "up",   "+2σ (upper)"),
    ("l1", "touch_l1", "close_below_l1", 1, "down", "−1σ (lower)"),
    ("l2", "touch_l2", "close_below_l2", 2, "down", "−2σ (lower)"),
]


def _uncond(agg_metrics: dict, metric: str) -> dict:
    m = agg_metrics[metric]
    return {"rate": m["rate"], "ci_low": m["ci_low"], "ci_high": m["ci_high"], "n": m["n"]}


def _by_gap(desc: dict, outcome: str) -> dict:
    return {r["bucket"]: {"rate": r["rate"], "ci_low": r["ci_low"], "ci_high": r["ci_high"],
                          "n": r["n"], "sufficient": r["sufficient"]}
            for r in desc[outcome]["gap"]}


def _outcome_block(agg_metrics: dict, desc: dict, calib: dict, outcome: str) -> dict:
    c = calib[outcome]
    return {
        "outcome": outcome,
        "unconditional": _uncond(agg_metrics, outcome),
        "by_gap": _by_gap(desc, outcome),
        "calibration": {
            "best_predictor": c["best_predictor"],
            "gap_beats_unconditional": c["gap_beats_unconditional"],
            "gap_rel_improvement": c["gap_rel_improvement"],
            "brier_gap_at_open": c["brier"]["gap_at_open"],
            "brier_unconditional": c["brier"]["unconditional"],
            "scored_sessions": c["scored_sessions"],
        },
    }


def build() -> dict:
    bars, sha = _b.fetch_or_load_snapshot()
    agg = _b.aggregate(_b.build_events(bars))
    agg_metrics = {m["metric"]: m for m in agg["metrics"]}
    aug = _rc.build_augmented(bars)
    desc = _rc.descriptive(aug)
    calib = {name: _rc.calibration(aug, name, k) for name, k, _ in _rc.OUTCOMES}

    levels = {}
    for lid, touch_o, close_o, _k, gap_side, label in LEVELS:
        levels[lid] = {
            "label": label,
            "gap_side": gap_side,     # the gap bucket that favors this level's touch
            "touch": _outcome_block(agg_metrics, desc, calib, touch_o),
            "close": _outcome_block(agg_metrics, desc, calib, close_o),
        }
    return {
        "symbol": _b.SYMBOL,
        "generated_from": "research/levels_evidence/prereg_level_behavior.md §8",
        "data_snapshot_sha256_16": sha,
        "gap_cut": _rc.GAP_CUT,                # ±0.3% gap threshold (must match the page)
        "min_bucket": _rc.MIN_BUCKET,          # per-gap-bucket gate (prereg §5)
        "unconditional_min_n": 100,            # prereg §5 unconditional gate
        "n_sessions_unconditional": agg["n_sessions"],
        "n_events_conditioned": len(aug),
        "labelling": ("Historical base rates, point-in-time — not a forecast. "
                      "Gap-conditioned only where it beats unconditional out-of-sample; "
                      "abstains when the bucket is thin."),
        "levels": levels,
    }


def main() -> int:
    report = build()
    out = _b.OUT_DIR / "level_probabilities.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"{report['symbol']} snapshot {report['data_snapshot_sha256_16']} wrote "
          f"{out.relative_to(ROOT)} ({len(report['levels'])} levels)")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
