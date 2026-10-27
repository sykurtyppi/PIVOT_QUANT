#!/usr/bin/env python3
"""Regression tests for daily-report transport configuration and timeouts."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
SENDER_PATH = REPO_ROOT / "scripts" / "send_daily_report.py"


def load_sender():
    spec = importlib.util.spec_from_file_location("pq_report_delivery_transport_test", SENDER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {SENDER_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TestReportDeliveryTransport(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sender = load_sender()

    def test_email_rejects_placeholder_credentials_before_smtp(self) -> None:
        env = {
            "ML_REPORT_SMTP_HOST": "smtp.example.test",
            "ML_REPORT_SMTP_PORT": "587",
            "ML_REPORT_SMTP_USER": "<smtp-user>",
            "ML_REPORT_SMTP_PASS": "<smtp-password>",
            "ML_REPORT_EMAIL_FROM": "<sender-email>",
        }
        with patch.dict(os.environ, env, clear=False), patch.object(
            self.sender.smtplib, "SMTP"
        ) as smtp:
            ok, detail = self.sender.send_email(
                recipients=["<recipient-email>"],
                subject="subject",
                body_text="body",
                report_text="# report",
                report_path=Path("report.md"),
                log_tail_text=None,
                dry_run=False,
            )

        self.assertFalse(ok)
        self.assertIn("placeholder", detail.lower())
        smtp.assert_not_called()

    def test_imessage_rejects_placeholder_recipient_before_osascript(self) -> None:
        with patch.object(self.sender.subprocess, "run") as run:
            ok, detail = self.sender.send_imessage(
                recipients=["<imessage-recipient>"],
                message_text="report ready",
                dry_run=False,
            )

        self.assertFalse(ok)
        self.assertIn("placeholder", detail.lower())
        run.assert_not_called()

    def test_imessage_timeout_is_reported_as_delivery_failure(self) -> None:
        timeout = subprocess.TimeoutExpired(cmd=["osascript"], timeout=30)
        with patch.object(self.sender.subprocess, "run", side_effect=timeout) as run:
            ok, detail = self.sender.send_imessage(
                recipients=["operator@example.test"],
                message_text="report ready",
                dry_run=False,
            )

        self.assertFalse(ok)
        self.assertIn("timed out", detail.lower())
        self.assertEqual(run.call_args.kwargs.get("timeout"), 30)


if __name__ == "__main__":
    unittest.main()
