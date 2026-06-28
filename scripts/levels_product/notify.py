#!/usr/bin/env python3
"""Tiny webhook notifier for the levels product (Discord-compatible).

Posts {"content": <markdown>} to LEVELS_PRODUCT_WEBHOOK_URL (a Discord webhook
or any generic JSON-POST endpoint). If the env var is unset, it prints to stdout
instead — so every publisher works in dry-run with zero config. stdlib only.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

WEBHOOK_ENV = "LEVELS_PRODUCT_WEBHOOK_URL"


def post(content: str, *, username: str = "PivotQuant Levels", timeout: float = 8.0) -> bool:
    """Best-effort deliver. Returns True if delivered, False on dry-run OR failure.

    NEVER raises: a webhook error must not crash the daily pipeline or the
    intraday poller (a crash mid-loop would skip the post-loop state advance and
    cause a double-alert storm on the next poll). Delivery failures are logged
    and swallowed — alerts are at-most-once, which for a free notification is the
    right trade vs. duplicate spam.
    """
    url = (os.getenv(WEBHOOK_ENV) or "").strip()
    if not url:
        print("─" * 60)
        print(f"[DRY RUN — set {WEBHOOK_ENV} to deliver]\n")
        print(content)
        print("─" * 60)
        return False
    body = json.dumps({"content": content, "username": username}).encode()
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (operator-set webhook)
            return 200 <= resp.status < 300
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        # do not echo the URL (it is a bearer credential)
        print(f"[notify] webhook delivery failed ({type(exc).__name__}); skipped")
        return False
