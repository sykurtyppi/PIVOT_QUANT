from __future__ import annotations


def normalize_trade_regime(bucket: str | None) -> str:
    value = str(bucket or "").strip().lower()
    if value in {"compression", "expansion", "neutral"}:
        return value
    return "neutral"


def favored_side_for_trade_regime(bucket: str | None) -> str:
    regime = normalize_trade_regime(bucket)
    if regime == "compression":
        return "reject"
    if regime == "expansion":
        return "break"
    return "abstain"


def favored_bucket_for_target(target: str | None) -> str | None:
    side = str(target or "").strip().lower()
    if side == "reject":
        return "compression"
    if side == "break":
        return "expansion"
    return None
