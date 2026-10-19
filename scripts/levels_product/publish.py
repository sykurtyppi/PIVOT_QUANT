#!/usr/bin/env python3
"""Phase-1: format + deliver the MORNING post (level map + track-record scoreboard).

Reads the two artifacts the daily job just wrote (levels_snapshot_<sym>_<date>.json
and track_record_<sym>.json) and renders a Discord/newsletter message, then posts
it via notify.py (dry-runs to stdout if no webhook configured).

The message leads with the level map (the descriptive draw) and closes with the
rolling track record + the honest "most levels are coin-flips, confluence levels
aren't" framing. Read-only; no DB access.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
from datetime import datetime, timezone

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(REPO / "scripts" / "levels_product"))

import notify  # noqa: E402
from level_snapshot import (  # noqa: E402
    _atomic_latest_link,
    _create_immutable_json,
    load_snapshot,
)

ART = REPO / "evidence" / "levels_product"


def _latest(symbol):
    snapshot_path = ART / f"levels_snapshot_{symbol}_latest.json"
    if not snapshot_path.exists():
        raise SystemExit(f"no {snapshot_path.name} — run morning_level_map.py first")
    tr = ART / f"track_record_{symbol}.json"
    if not tr.exists():
        raise SystemExit(f"no {tr.name} — run build_track_record.py first")
    try:
        return load_snapshot(snapshot_path), json.loads(tr.read_text())
    except (ValueError, json.JSONDecodeError, OSError) as exc:
        raise SystemExit(f"could not read product artifacts ({type(exc).__name__}: {exc}) "
                         f"— regenerate via the daily job") from exc


def render(mp, tr) -> str:
    """Clean, no-emoji, plain-text post for a human reader (single column so it
    degrades gracefully in proportional-font mail clients). Keeps the exporter's
    discipline (plain text, price+label) but uses real section headers and puts
    the track record — the product's proof — front and centre.
    """
    sym = mp["symbol"]
    spot = mp["reference_price"]
    res = [r for r in mp["levels"] if r["side"] == "resistance"]  # already price-desc
    sup = [r for r in mp["levels"] if r["side"] == "support"]

    def level_line(r):
        return f"  {r['label']:<3} {r['price']:>8.2f}   {r['distance_from_reference_bps']:>+5.0f} bps"

    L = [f"{sym} LEVELS — {mp['session_date']} session   (spot {spot:.2f})", ""]
    L.append("RESISTANCE")
    L += [level_line(r) for r in res]
    L += ["", "SUPPORT"]
    L += [level_line(r) for r in sup]

    rates_publishable = (
        mp.get("evidence", {}).get("hold_rates_publishable") is True
        and tr.get("hold_rates_publishable") is True
    )
    if rates_publishable:
        tparts, n_alerted = [], None
        for h in (15, 30, 60):
            rec = tr["horizons"].get(f"h{h}", {})
            sb = rec.get("rolling_scoreboard_alerted_levels") or rec.get("rolling_scoreboard_alerted")
            if sb and sb.get("actual_hold_rate") is not None:
                tparts.append(f"{h}m {int(round(sb['actual_hold_rate'] * 100))}% held")
                n_alerted = sb.get("n_alerted_touches", sb.get("n"))
        if tparts:
            L += ["", "TRACK RECORD — last 30 trading days, confluence levels",
                  "  " + "    ".join(tparts) + (f"   (n={n_alerted})" if n_alerted else "")]
        base = mp.get("evidence", {}).get("unconditional_hold_rate_by_horizon", {})
        if base:
            bstr = " / ".join(f"{int(round(v['hold_rate'] * 100))}%"
                              for _, v in sorted(base.items(), key=lambda x: int(x[0])))
            L.append(f"  Base rate, any level (15m/30m/60m): {bstr}")
    else:
        coverage = mp.get("evidence", {}).get("label_coverage") or tr.get("label_coverage") or {}
        incomplete = []
        for h in (15, 30, 60):
            item = coverage.get(str(h), coverage.get(h, {}))
            if not item.get("complete"):
                missing = item.get("missing_count")
                incomplete.append(f"{h}m missing {missing}" if missing else f"{h}m unavailable")
        detail = ", ".join(incomplete) if incomplete else "coverage metadata unavailable"
        L += ["", f"HOLD RATES UNAVAILABLE — incomplete labels: {detail}"]

    L += ["",
          "Most S/R levels are roughly coin-flips; confluence levels hold meaningfully more.",
          "Educational, not financial advice."]
    return "\n".join(L)


def _canonical_bytes(value) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def build_briefing_artifact(mp, tr) -> dict:
    """Freeze the exact pre-market message and the evidence it was built from."""
    track_record_sha256 = hashlib.sha256(_canonical_bytes(tr)).hexdigest()
    core = {
        "contract": "pivotquant.morning_briefing",
        "contract_version": 1,
        "symbol": mp["symbol"],
        "session_date": mp["session_date"],
        "prepared_at_utc": mp["generated_at_utc"],
        "snapshot_id": mp["snapshot_id"],
        "track_record_as_of_utc": tr.get("as_of_utc"),
        "track_record_sha256": track_record_sha256,
        "track_record": tr,
        "body": render(mp, tr),
    }
    digest = hashlib.sha256(_canonical_bytes(core)).hexdigest()[:24]
    return {**core, "briefing_id": f"brf_{digest}"}


def _load_briefing(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read morning briefing {path}: {exc}") from exc


def write_briefing_artifact(artifact: dict, out_dir: Path) -> tuple[Path, Path]:
    """Create one immutable pre-market briefing plus a monotonic latest pointer."""
    immutable = out_dir / (
        f"morning_briefing_{artifact['symbol']}_{artifact['session_date']}.json"
    )
    latest = out_dir / f"morning_briefing_{artifact['symbol']}_latest.json"
    if not _create_immutable_json(immutable, artifact):
        existing = _load_briefing(immutable)
        if existing != artifact:
            raise ValueError(
                f"immutable briefing already exists with different contents: {immutable}"
            )

    lock_path = out_dir / f".{latest.name}.lock"
    with lock_path.open("a+b") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            if latest.exists() or latest.is_symlink():
                current = _load_briefing(latest)
                if current["session_date"] > artifact["session_date"]:
                    return immutable, latest
                if (
                    current["session_date"] == artifact["session_date"]
                    and current != artifact
                ):
                    raise ValueError(
                        "latest briefing conflicts with immutable session artifact"
                    )
            _atomic_latest_link(latest, immutable)
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    return immutable, latest


def _resolve_channel(requested: str) -> str:
    """auto → email if SMTP configured, else webhook if set, else dry-run."""
    if requested != "auto":
        return requested
    notify._load_env_file()
    if (os.getenv("ML_REPORT_SMTP_HOST") or "").strip() and (os.getenv("ML_REPORT_EMAIL_TO") or "").strip():
        return "email"
    if (os.getenv(notify.WEBHOOK_ENV) or "").strip():
        return "webhook"
    return "dryrun"


def _delivery_receipt_path(artifact: dict, channel: str, out_dir: Path) -> Path:
    return out_dir / f"morning_delivery_{artifact['briefing_id']}_{channel}.json"


def deliver_briefing(
    artifact: dict,
    *,
    channel: str,
    out_dir: Path,
    dry_run: bool = False,
) -> tuple[bool, str]:
    """Deliver once per briefing/channel and persist confirmed success separately."""
    if dry_run or channel == "dryrun":
        print(artifact["body"])
        return False, "dry-run"
    if channel not in {"email", "webhook"}:
        raise ValueError(f"unsupported delivery channel: {channel}")

    out_dir.mkdir(parents=True, exist_ok=True)
    receipt_path = _delivery_receipt_path(artifact, channel, out_dir)
    lock_path = out_dir / f".{receipt_path.name}.lock"
    with lock_path.open("a+b") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            if receipt_path.exists():
                receipt = _load_briefing(receipt_path)
                if (
                    receipt.get("briefing_id") != artifact["briefing_id"]
                    or receipt.get("channel") != channel
                ):
                    raise ValueError(f"delivery receipt identity conflict: {receipt_path}")
                return True, "already-delivered"

            if channel == "email":
                subject = (
                    f"{artifact['symbol']} Levels — "
                    f"{artifact['session_date']} session"
                )
                sent = notify.email_post(subject, artifact["body"], dry_run=False)
            else:
                sent = notify.post(artifact["body"])
            if not sent:
                return False, "delivery-failed"

            receipt = {
                "contract": "pivotquant.morning_delivery_receipt",
                "contract_version": 1,
                "briefing_id": artifact["briefing_id"],
                "snapshot_id": artifact["snapshot_id"],
                "channel": channel,
                "delivered_at_utc": datetime.now(timezone.utc).isoformat().replace(
                    "+00:00", "Z"
                ),
            }
            if not _create_immutable_json(receipt_path, receipt):
                raise ValueError(f"delivery receipt appeared during locked send: {receipt_path}")
            return True, "sent"
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--channel", default=os.getenv("LEVELS_CHANNEL", "auto"),
                    choices=["auto", "email", "webhook", "dryrun"])
    ap.add_argument("--dry-run", action="store_true", help="build but do not actually send")
    args = ap.parse_args()
    mp, tr = _latest(args.symbol)
    briefing = build_briefing_artifact(mp, tr)
    immutable, _ = write_briefing_artifact(briefing, ART)
    channel = _resolve_channel(args.channel)
    ok, status = deliver_briefing(
        briefing, channel=channel, out_dir=ART, dry_run=args.dry_run
    )
    print(f"\n[channel={channel} status={status}]")
    if status == "delivery-failed":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
