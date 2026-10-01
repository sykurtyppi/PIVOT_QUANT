"""Tests for the levels-product webhook notifier's payload routing.

The one behavior that matters: pick the right payload shape for the webhook
host (Slack vs Discord/generic) and rewrite **bold** to Slack *bold* for Slack.
No network — urlopen is stubbed so we can read the body that would be sent.
"""
from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "levels_product" / "notify.py"

spec = importlib.util.spec_from_file_location("levels_product_notify", SCRIPT)
notify = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(notify)


class SlackTextTest(unittest.TestCase):
    def test_discord_bold_becomes_slack_bold(self):
        self.assertEqual(notify._slack_text("**SPY 768** near **+1σ**"),
                         "*SPY 768* near *+1σ*")
        self.assertEqual(notify._slack_text("no bold here"), "no bold here")


class PostRoutingTest(unittest.TestCase):
    def setUp(self):
        self._orig_urlopen = notify.urllib.request.urlopen
        self.captured = {}

        def fake_urlopen(req, timeout=0):
            self.captured["url"] = req.full_url
            self.captured["body"] = json.loads(req.data.decode())

            class _Resp:
                status = 200

                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, *a):
                    return False

            return _Resp()

        notify.urllib.request.urlopen = fake_urlopen

    def tearDown(self):
        notify.urllib.request.urlopen = self._orig_urlopen

    def test_slack_host_gets_text_with_mrkdwn(self):
        import os
        os.environ[notify.WEBHOOK_ENV] = "https://hooks.slack.com/services/XXX"
        try:
            self.assertTrue(notify.post("**bold** here"))
        finally:
            os.environ.pop(notify.WEBHOOK_ENV, None)
        self.assertEqual(self.captured["body"], {"text": "*bold* here"})

    def test_discord_host_gets_content_markdown(self):
        import os
        os.environ[notify.WEBHOOK_ENV] = "https://discord.com/api/webhooks/XXX"
        try:
            self.assertTrue(notify.post("**bold** here"))
        finally:
            os.environ.pop(notify.WEBHOOK_ENV, None)
        self.assertEqual(self.captured["body"].get("content"), "**bold** here")

    def test_unset_env_is_dry_run_returns_false(self):
        import os
        os.environ.pop(notify.WEBHOOK_ENV, None)
        self.assertFalse(notify.post("anything"))
        self.assertEqual(self.captured, {})  # no delivery attempted


if __name__ == "__main__":
    unittest.main()
