"""Canonical deterministic-levels snapshot contract and consumer parity tests."""
from __future__ import annotations

import importlib.util
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
LEVELS_DIR = ROOT / "scripts" / "levels_product"
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))
if str(LEVELS_DIR) not in sys.path:
    sys.path.insert(0, str(LEVELS_DIR))

from backfill_events import calculate_pivots  # noqa: E402


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


snapshot_contract = load_module(
    "levels_product_snapshot_contract",
    LEVELS_DIR / "level_snapshot.py",
)
publish = load_module(
    "levels_product_publish_snapshot_contract",
    LEVELS_DIR / "publish.py",
)
intraday = load_module(
    "levels_product_intraday_snapshot_contract",
    LEVELS_DIR / "intraday_alert.py",
)


class SnapshotContractTest(unittest.TestCase):
    SOURCE_OHLC = {"high": 680.0, "low": 670.0, "close": 676.0}

    def _build(self, generated_at_utc: str = "2026-10-02T11:00:00Z"):
        return snapshot_contract.build_snapshot(
            symbol="SPY",
            source_session_date=date(2026, 10, 1),
            source_ohlc=self.SOURCE_OHLC,
            generated_at_utc=generated_at_utc,
            raw_levels=calculate_pivots(**self.SOURCE_OHLC),
            evidence={
                "hold_rates_publishable": True,
                "label_coverage": {15: {"complete": True}},
                "unconditional_hold_rate_by_horizon": {15: {"hold_rate": 0.6, "n": 100}},
            },
        )

    def test_builds_versioned_self_describing_snapshot(self):
        snapshot = self._build()

        self.assertEqual(snapshot["contract"], "pivotquant.levels_snapshot")
        self.assertEqual(snapshot["contract_version"], 1)
        self.assertEqual(snapshot["symbol"], "SPY")
        self.assertEqual(snapshot["session_date"], "2026-10-02")
        self.assertEqual(snapshot["source"]["session_date"], "2026-10-01")
        self.assertEqual(snapshot["source"]["as_of_utc"], "2026-10-01T20:00:00Z")
        self.assertEqual(snapshot["source"]["freshness"], "current_prior_session")
        self.assertEqual(snapshot["calculation"]["family"], "classic_floor_pivots")
        self.assertEqual(
            snapshot["calculation"]["version"],
            "backfill_events.calculate_pivots:v1",
        )
        self.assertRegex(snapshot["snapshot_id"], r"^lvs_[0-9a-f]{24}$")

        by_label = {level["label"]: level for level in snapshot["levels"]}
        self.assertEqual(by_label["R1"]["side"], "resistance")
        self.assertEqual(by_label["S1"]["side"], "support")
        self.assertRegex(by_label["R1"]["level_id"], r"^lvl_[0-9a-f]{24}$")
        self.assertEqual(by_label["R1"]["validity"]["session_date"], "2026-10-02")
        self.assertEqual(by_label["R1"]["validity"]["expires_at_utc"], "2026-10-02T20:00:00Z")
        self.assertEqual(by_label["R1"]["confluence_components"], [])

    def test_ids_are_stable_across_reruns_but_validation_detects_tampering(self):
        first = self._build("2026-10-02T11:00:00Z")
        rerun = self._build("2026-10-02T12:00:00Z")

        self.assertEqual(first["snapshot_id"], rerun["snapshot_id"])
        self.assertEqual(
            [level["level_id"] for level in first["levels"]],
            [level["level_id"] for level in rerun["levels"]],
        )
        snapshot_contract.validate_snapshot(first)

        first["levels"][0]["price"] += 0.01
        with self.assertRaisesRegex(ValueError, "id"):
            snapshot_contract.validate_snapshot(first)

    def test_validation_rejects_malformed_freshness_timestamps(self):
        snapshot = self._build()
        snapshot["generated_at_utc"] = "not-a-timestamp"

        with self.assertRaisesRegex(ValueError, "generated_at_utc"):
            snapshot_contract.validate_snapshot(snapshot)

        snapshot = self._build()
        snapshot["source"]["as_of_utc"] = "not-a-timestamp"

        with self.assertRaisesRegex(ValueError, "source.as_of_utc"):
            snapshot_contract.validate_snapshot(snapshot)

    def test_validation_rejects_self_consistent_wrong_level_semantics(self):
        snapshot = self._build()
        snapshot["levels"][0]["side"] = "support"
        level = snapshot["levels"][0]
        snapshot["levels"][0]["level_id"] = snapshot_contract._digest(
            "lvl",
            snapshot_contract._level_identity_core(
                symbol=snapshot["symbol"],
                session_date=snapshot["session_date"],
                label=level["label"],
                price=level["price"],
            ),
        )
        snapshot["snapshot_id"] = snapshot_contract._digest(
            "lvs", snapshot_contract._identity_core(snapshot)
        )

        with self.assertRaisesRegex(ValueError, "side"):
            snapshot_contract.validate_snapshot(snapshot)

    def test_builder_rejects_wrong_or_incomplete_pivot_calculation(self):
        wrong = calculate_pivots(**self.SOURCE_OHLC)
        wrong["PP"] = 999.0
        with self.assertRaisesRegex(ValueError, "pivot calculation"):
            snapshot_contract.build_snapshot(
                symbol="SPY", source_session_date=date(2026, 10, 1),
                source_ohlc=self.SOURCE_OHLC,
                generated_at_utc="2026-10-02T11:00:00Z",
                raw_levels=wrong, evidence={},
            )

        incomplete = calculate_pivots(**self.SOURCE_OHLC)
        incomplete.pop("M4")
        with self.assertRaisesRegex(ValueError, "pivot labels"):
            snapshot_contract.build_snapshot(
                symbol="SPY", source_session_date=date(2026, 10, 1),
                source_ohlc=self.SOURCE_OHLC,
                generated_at_utc="2026-10-02T11:00:00Z",
                raw_levels=incomplete, evidence={},
            )

    def test_validation_rejects_self_consistent_wrong_pivot_price(self):
        snapshot = self._build()
        level = next(item for item in snapshot["levels"] if item["label"] == "PP")
        level["price"] = 999.0
        level["side"] = "resistance"
        level["distance_from_reference_bps"] = round(
            (level["price"] - snapshot["reference_price"])
            / snapshot["reference_price"] * 10_000, 1,
        )
        level["level_id"] = snapshot_contract._digest(
            "lvl", snapshot_contract._level_identity_core(
                symbol=snapshot["symbol"], session_date=snapshot["session_date"],
                label=level["label"], price=level["price"],
            ),
        )
        snapshot["snapshot_id"] = snapshot_contract._digest(
            "lvs", snapshot_contract._identity_core(snapshot)
        )
        with self.assertRaisesRegex(ValueError, "pivot calculation"):
            snapshot_contract.validate_snapshot(snapshot)

    def test_validation_rejects_reference_price_drift_from_source_close(self):
        snapshot = self._build()
        snapshot["reference_price"] = 1.0
        for level in snapshot["levels"]:
            level["side"] = "resistance"
            level["distance_from_reference_bps"] = round(
                (level["price"] - snapshot["reference_price"])
                / snapshot["reference_price"] * 10_000,
                1,
            )
        snapshot["snapshot_id"] = snapshot_contract._digest(
            "lvs", snapshot_contract._identity_core(snapshot)
        )

        with self.assertRaisesRegex(ValueError, "reference_price"):
            snapshot_contract.validate_snapshot(snapshot)

    def test_builder_rejects_non_trading_source_and_invalid_generation_time(self):
        levels = calculate_pivots(**self.SOURCE_OHLC)
        with self.assertRaisesRegex(ValueError, "trading session"):
            snapshot_contract.build_snapshot(
                symbol="SPY", source_session_date=date(2026, 7, 3),
                source_ohlc=self.SOURCE_OHLC,
                generated_at_utc="2026-07-06T11:00:00Z",
                raw_levels=levels, evidence={},
            )

        for generated_at in ("2020-01-01T00:00:00Z", "2026-10-02T20:00:01Z"):
            with self.subTest(generated_at=generated_at):
                with self.assertRaisesRegex(ValueError, "generation time"):
                    snapshot_contract.build_snapshot(
                        symbol="SPY", source_session_date=date(2026, 10, 1),
                        source_ohlc=self.SOURCE_OHLC,
                        generated_at_utc=generated_at, raw_levels=levels, evidence={},
                    )

    def test_builder_rejects_path_unsafe_symbol(self):
        with self.assertRaisesRegex(ValueError, "symbol"):
            snapshot_contract.build_snapshot(
                symbol="../../../ESCAPE", source_session_date=date(2026, 10, 1),
                source_ohlc=self.SOURCE_OHLC,
                generated_at_utc="2026-10-02T11:00:00Z",
                raw_levels=calculate_pivots(**self.SOURCE_OHLC), evidence={},
            )

    def test_weekend_and_holiday_roll_to_next_trading_session(self):
        snapshot = snapshot_contract.build_snapshot(
            symbol="SPY",
            source_session_date=date(2026, 7, 2),
            source_ohlc={"high": 680.0, "low": 670.0, "close": 676.0},
            generated_at_utc="2026-07-02T21:00:00Z",
            raw_levels=calculate_pivots(high=680.0, low=670.0, close=676.0),
            evidence={},
        )

        self.assertEqual(snapshot["session_date"], "2026-07-06")
        self.assertEqual(snapshot["levels"][0]["validity"]["expires_at_utc"], "2026-07-06T20:00:00Z")

    def test_write_publishes_immutable_and_latest_identical_payloads(self):
        snapshot = self._build()
        with tempfile.TemporaryDirectory() as tmp:
            immutable, latest = snapshot_contract.write_snapshot(snapshot, Path(tmp))

            self.assertEqual(immutable.name, "levels_snapshot_SPY_2026-10-02.json")
            self.assertEqual(latest.name, "levels_snapshot_SPY_latest.json")
            self.assertEqual(json.loads(immutable.read_text()), snapshot)
            self.assertEqual(json.loads(latest.read_text()), snapshot)

    def test_write_rejects_conflicting_immutable_snapshot(self):
        first = self._build()
        conflicting = snapshot_contract.build_snapshot(
            symbol="SPY", source_session_date=date(2026, 10, 1),
            source_ohlc={"high": 690.0, "low": 670.0, "close": 676.0},
            generated_at_utc="2026-10-02T11:00:00Z",
            raw_levels=calculate_pivots(high=690.0, low=670.0, close=676.0),
            evidence={},
        )
        with tempfile.TemporaryDirectory() as tmp:
            immutable, _ = snapshot_contract.write_snapshot(first, Path(tmp))
            before = immutable.read_bytes()
            with self.assertRaisesRegex(ValueError, "immutable"):
                snapshot_contract.write_snapshot(conflicting, Path(tmp))
            self.assertEqual(immutable.read_bytes(), before)

    def test_write_does_not_regress_latest_to_older_session(self):
        newer = snapshot_contract.build_snapshot(
            symbol="SPY", source_session_date=date(2026, 10, 2),
            source_ohlc=self.SOURCE_OHLC,
            generated_at_utc="2026-10-05T11:00:00Z",
            raw_levels=calculate_pivots(**self.SOURCE_OHLC), evidence={},
        )
        older = self._build()
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            snapshot_contract.write_snapshot(newer, out_dir)
            snapshot_contract.write_snapshot(older, out_dir)
            latest = snapshot_contract.load_snapshot(
                out_dir / "levels_snapshot_SPY_latest.json"
            )
            self.assertEqual(latest["session_date"], newer["session_date"])

    def test_concurrent_writers_keep_latest_on_newest_session(self):
        newer = snapshot_contract.build_snapshot(
            symbol="SPY", source_session_date=date(2026, 10, 2),
            source_ohlc=self.SOURCE_OHLC,
            generated_at_utc="2026-10-05T11:00:00Z",
            raw_levels=calculate_pivots(**self.SOURCE_OHLC), evidence={},
        )
        older = self._build()
        older_checked = threading.Event()
        release_older = threading.Event()
        real_publish = snapshot_contract._atomic_latest_link
        failures = []

        def pause_older(latest, immutable):
            if immutable.name.endswith("2026-10-02.json"):
                older_checked.set()
                if not release_older.wait(timeout=5):
                    raise TimeoutError("newer writer did not run")
            real_publish(latest, immutable)

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
                snapshot_contract, "_atomic_latest_link", side_effect=pause_older):
            out_dir = Path(tmp)

            def write(payload):
                try:
                    snapshot_contract.write_snapshot(payload, out_dir)
                except Exception as exc:  # pragma: no cover - asserted below
                    failures.append(exc)

            old_thread = threading.Thread(target=write, args=(older,))
            new_thread = threading.Thread(target=write, args=(newer,))
            old_thread.start()
            self.assertTrue(older_checked.wait(timeout=5))
            new_thread.start()
            new_thread.join(timeout=0.2)
            release_older.set()
            old_thread.join(timeout=5)
            new_thread.join(timeout=5)

            self.assertFalse(old_thread.is_alive())
            self.assertFalse(new_thread.is_alive())
            self.assertEqual(failures, [])
            latest = snapshot_contract.load_snapshot(
                out_dir / "levels_snapshot_SPY_latest.json"
            )
            self.assertEqual(latest["session_date"], newer["session_date"])


class BriefingArtifactTest(unittest.TestCase):
    def setUp(self):
        # Delivery uses the real wall clock in production; pin fixture time explicitly.
        self.clock = self.enterContext(mock.patch.object(publish, "datetime", wraps=datetime))
        self.clock.now.return_value = datetime(2026, 10, 2, 11, 2, tzinfo=timezone.utc)
        self.snapshot = snapshot_contract.build_snapshot(
            symbol="SPY",
            source_session_date=date(2026, 10, 1),
            source_ohlc={"high": 680.0, "low": 670.0, "close": 676.0},
            generated_at_utc="2026-10-02T11:00:00Z",
            raw_levels=calculate_pivots(high=680.0, low=670.0, close=676.0),
            evidence={"hold_rates_publishable": False, "label_coverage": {}},
        )
        self.track_record = {
            "symbol": "SPY",
            "as_of_utc": "2026-10-02T10:59:00Z",
            "report_session_date": "2026-10-01",
            "hold_rates_publishable": False,
            "label_coverage": {},
            "horizons": {},
        }

    def test_publish_cli_imports_from_repo_root(self):
        result = subprocess.run(
            [sys.executable, str(LEVELS_DIR / "publish.py"), "--help"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("usage: publish.py", result.stdout)
        self.assertIn("--channel", result.stdout)

    def test_builds_content_addressed_briefing_bound_to_snapshot(self):
        artifact = publish.build_briefing_artifact(self.snapshot, self.track_record)

        self.assertEqual(artifact["contract"], "pivotquant.morning_briefing")
        self.assertEqual(artifact["contract_version"], 1)
        self.assertEqual(artifact["snapshot_id"], self.snapshot["snapshot_id"])
        self.assertEqual(artifact["prepared_at_utc"], self.snapshot["generated_at_utc"])
        self.assertEqual(
            artifact["snapshot_generated_at_utc"], self.snapshot["generated_at_utc"]
        )
        self.assertEqual(artifact["track_record"], self.track_record)
        self.assertRegex(artifact["briefing_id"], r"^brf_[0-9a-f]{24}$")
        self.assertIn("SPY LEVELS — 2026-10-02 session", artifact["body"])

    def test_rejects_post_open_or_future_evidence_capture(self):
        with self.assertRaisesRegex(ValueError, "later than briefing capture"):
            publish.build_briefing_artifact(
                self.snapshot,
                {**self.track_record, "as_of_utc": "2026-10-02T12:01:00Z"},
                captured_at_utc="2026-10-02T12:00:00Z",
            )
        with self.assertRaisesRegex(ValueError, "no later than session open"):
            publish.build_briefing_artifact(
                self.snapshot,
                self.track_record,
                captured_at_utc="2026-10-02T14:00:00Z",
            )

    def test_rejects_cross_symbol_or_stale_track_record(self):
        with self.assertRaisesRegex(ValueError, "symbol"):
            publish.build_briefing_artifact(
                self.snapshot, {**self.track_record, "symbol": "QQQ"}
            )
        with self.assertRaisesRegex(ValueError, "session"):
            publish.build_briefing_artifact(
                self.snapshot,
                {**self.track_record, "report_session_date": "2025-01-02"},
            )
        with self.assertRaisesRegex(ValueError, "older than snapshot source"):
            publish.build_briefing_artifact(
                self.snapshot,
                {**self.track_record, "as_of_utc": "2026-10-01T12:00:00Z"},
            )

    def test_retry_reuses_frozen_briefing_even_if_track_record_file_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            first, immutable = publish.load_or_build_briefing(
                self.snapshot,
                self.track_record,
                out_dir,
                captured_at_utc="2026-10-02T11:01:00Z",
            )
            changed = {**self.track_record, "as_of_utc": "2026-10-02T11:05:00Z"}
            retry, retry_path = publish.load_or_build_briefing(
                self.snapshot,
                changed,
                out_dir,
                captured_at_utc="2026-10-02T11:06:00Z",
            )

            self.assertEqual(retry, first)
            self.assertEqual(retry_path, immutable)
            self.assertEqual(retry["track_record"], self.track_record)

    def test_tampered_frozen_briefing_is_rejected_before_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            artifact, immutable = publish.load_or_build_briefing(
                self.snapshot,
                self.track_record,
                out_dir,
                captured_at_utc="2026-10-02T11:01:00Z",
            )
            immutable.write_text(json.dumps({**artifact, "body": "TAMPERED"}))

            with self.assertRaisesRegex(ValueError, "briefing identity digest mismatch"):
                publish.load_or_build_briefing(
                    self.snapshot,
                    self.track_record,
                    out_dir,
                    captured_at_utc="2026-10-02T11:02:00Z",
                )

    def test_rehashed_semantically_tampered_briefing_is_rejected_on_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            artifact, immutable = publish.load_or_build_briefing(
                self.snapshot,
                self.track_record,
                out_dir,
                captured_at_utc="2026-10-02T11:01:00Z",
            )
            artifact["track_record"]["symbol"] = "QQQ"
            artifact["track_record_sha256"] = hashlib.sha256(
                publish._canonical_bytes(artifact["track_record"])
            ).hexdigest()
            core = {key: value for key, value in artifact.items() if key != "briefing_id"}
            artifact["briefing_id"] = (
                "brf_" + hashlib.sha256(publish._canonical_bytes(core)).hexdigest()[:24]
            )
            immutable.write_text(json.dumps(artifact), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "symbol"):
                publish.load_or_build_briefing(
                    self.snapshot,
                    self.track_record,
                    out_dir,
                    captured_at_utc="2026-10-02T11:02:00Z",
                )

    def test_first_delivery_after_open_is_rejected_without_receipt(self):
        artifact = publish.build_briefing_artifact(self.snapshot, self.track_record)
        self.clock.now.return_value = datetime(2026, 10, 2, 13, 30, 1, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            publish.notify, "post", return_value=True
        ) as send:
            with self.assertRaisesRegex(ValueError, "delivery window"):
                publish.deliver_briefing(artifact, out_dir=Path(tmp), channel="webhook", dry_run=False)
            send.assert_not_called()
            self.assertFalse(list(Path(tmp).glob("morning_delivery_*.json")))

    def test_delivery_before_capture_is_rejected(self):
        artifact = publish.build_briefing_artifact(self.snapshot, self.track_record)
        self.clock.now.return_value = datetime(2026, 10, 2, 10, 59, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(publish.notify, "post") as send:
            with self.assertRaisesRegex(ValueError, "before briefing capture"):
                publish.deliver_briefing(artifact, out_dir=Path(tmp), channel="webhook", dry_run=False)
            send.assert_not_called()

    def test_delivery_at_open_remains_allowed(self):
        artifact = publish.build_briefing_artifact(self.snapshot, self.track_record)
        self.clock.now.return_value = datetime(2026, 10, 2, 13, 30, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            publish.notify, "post", return_value=True
        ) as send:
            result = publish.deliver_briefing(artifact, out_dir=Path(tmp), channel="webhook", dry_run=False)
            self.assertEqual(result, (True, "sent"))
            send.assert_called_once()

    def test_already_sent_retry_after_open_still_returns_existing_receipt(self):
        artifact = publish.build_briefing_artifact(self.snapshot, self.track_record)
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            publish.notify, "post", return_value=True
        ) as send:
            first = publish.deliver_briefing(artifact, out_dir=Path(tmp), channel="webhook", dry_run=False)
            self.clock.now.return_value = datetime(2026, 10, 5, 14, 0, tzinfo=timezone.utc)
            retry = publish.deliver_briefing(artifact, out_dir=Path(tmp), channel="webhook", dry_run=False)
            self.assertEqual(first, (True, "sent"))
            self.assertEqual(retry, (True, "already-delivered"))
            send.assert_called_once()

    def test_write_freezes_first_briefing_and_rejects_changed_rerun(self):
        first = publish.build_briefing_artifact(self.snapshot, self.track_record)
        changed_track_record = dict(self.track_record)
        changed_track_record["hold_rates_publishable"] = True
        changed_track_record["horizons"] = {
            "h15": {"rolling_scoreboard_alerted": {"actual_hold_rate": 0.75, "n": 20}}
        }
        changed = publish.build_briefing_artifact(self.snapshot, changed_track_record)

        with tempfile.TemporaryDirectory() as tmp:
            immutable, latest = publish.write_briefing_artifact(first, Path(tmp))
            before = immutable.read_bytes()

            self.assertEqual(immutable.name, "morning_briefing_SPY_2026-10-02.json")
            self.assertEqual(json.loads(latest.read_text()), first)
            with self.assertRaisesRegex(ValueError, "immutable briefing"):
                publish.write_briefing_artifact(changed, Path(tmp))
            self.assertEqual(immutable.read_bytes(), before)

    def test_write_rejects_symlink_immutable_path(self):
        """F2: a pre-existing symlink at the immutable path is rejected, not
        followed, when the writer finds an artifact already present."""
        artifact = publish.build_briefing_artifact(self.snapshot, self.track_record)
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            decoy = out_dir / "decoy.json"
            decoy.write_text("{}", encoding="utf-8")
            immutable = out_dir / "morning_briefing_SPY_2026-10-02.json"
            immutable.symlink_to(decoy.name)

            with self.assertRaisesRegex(ValueError, "symlink"):
                publish.write_briefing_artifact(artifact, out_dir)

    def test_load_or_build_rejects_symlink_immutable_path(self):
        """F2: the reuse path refuses a symlinked immutable rather than reading
        through it to an attacker-chosen target."""
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            decoy = out_dir / "decoy.json"
            decoy.write_text("{}", encoding="utf-8")
            immutable = out_dir / "morning_briefing_SPY_2026-10-02.json"
            immutable.symlink_to(decoy.name)

            with self.assertRaisesRegex(ValueError, "symlink"):
                publish.load_or_build_briefing(
                    self.snapshot,
                    self.track_record,
                    out_dir,
                    captured_at_utc="2026-10-02T11:01:00Z",
                )

    @unittest.skipUnless(hasattr(os, "mkfifo"), "requires os.mkfifo")
    def test_write_rejects_non_regular_immutable_path(self):
        """F2: a non-regular file (FIFO) at the immutable path is rejected."""
        artifact = publish.build_briefing_artifact(self.snapshot, self.track_record)
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            immutable = out_dir / "morning_briefing_SPY_2026-10-02.json"
            os.mkfifo(immutable)

            with self.assertRaisesRegex(ValueError, "regular file"):
                publish.write_briefing_artifact(artifact, out_dir)

    @staticmethod
    def _rehash(artifact):
        core = {key: value for key, value in artifact.items() if key != "briefing_id"}
        return {**core, "briefing_id": "brf_" + hashlib.sha256(
            publish._canonical_bytes(core)
        ).hexdigest()[:24]}

    def _legacy(self, **changes):
        artifact = publish.build_briefing_artifact(self.snapshot, self.track_record)
        artifact.pop("track_record")
        artifact.pop("snapshot_generated_at_utc")
        artifact.update(session_date="2026-10-01", prepared_at_utc="2026-10-01T11:00:00Z",
                        track_record_as_of_utc="2026-10-01T10:59:00Z")
        artifact.update(changes)
        return self._rehash(artifact)

    @staticmethod
    def _seed_latest(out_dir, artifact):
        immutable = out_dir / (
            f"morning_briefing_{artifact['symbol']}_{artifact['session_date']}.json"
        )
        immutable.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
        latest = out_dir / "morning_briefing_SPY_latest.json"
        latest.symlink_to(immutable.name)
        return immutable, latest

    def test_older_legacy_pointer_advances_without_rewriting_legacy(self):
        modern = publish.build_briefing_artifact(self.snapshot, self.track_record)
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            legacy_path, latest = self._seed_latest(out_dir, self._legacy())
            before = legacy_path.read_bytes()
            immutable, returned_latest = publish.write_briefing_artifact(modern, out_dir)
            self.assertEqual(returned_latest, latest)
            self.assertEqual(latest.resolve(), immutable.resolve())
            self.assertEqual(publish._load_briefing(latest), modern)
            self.assertEqual(legacy_path.read_bytes(), before)

    def test_existing_immutable_retry_reconciles_legacy_pointer_without_redelivery(self):
        modern = publish.build_briefing_artifact(self.snapshot, self.track_record)
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            publish.notify, "post", return_value=True
        ) as post:
            out_dir = Path(tmp)
            legacy_path, latest = self._seed_latest(out_dir, self._legacy())
            immutable = out_dir / "morning_briefing_SPY_2026-10-02.json"
            immutable.write_text(json.dumps(modern) + "\n", encoding="utf-8")
            before = immutable.read_bytes(), legacy_path.read_bytes()
            self.assertEqual(publish.deliver_briefing(
                modern, channel="webhook", out_dir=out_dir
            ), (True, "sent"))
            retry, retry_path = publish.load_or_build_briefing(
                self.snapshot, {**self.track_record, "symbol": "CHANGED"}, out_dir,
                captured_at_utc="2026-10-02T15:00:00Z",
            )
            self.assertEqual(retry, modern)
            self.assertEqual(retry_path, immutable)
            self.assertEqual(latest.resolve(), immutable.resolve())
            self.assertEqual((immutable.read_bytes(), legacy_path.read_bytes()), before)
            self.assertEqual(publish.deliver_briefing(
                retry, channel="webhook", out_dir=out_dir
            ), (True, "already-delivered"))
            post.assert_called_once()

    def test_tampered_legacy_identity_cannot_be_superseded(self):
        legacy = {**self._legacy(), "body": "tampered without rehash"}
        modern = publish.build_briefing_artifact(self.snapshot, self.track_record)
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            legacy_path, latest = self._seed_latest(out_dir, legacy)
            with self.assertRaisesRegex(ValueError, "identity digest mismatch"):
                publish.write_briefing_artifact(modern, out_dir)
            self.assertEqual(latest.resolve(), legacy_path.resolve())

    def test_only_exact_recognizable_legacy_schema_can_be_superseded(self):
        variants = [
            {"track_record": None}, {"snapshot_generated_at_utc": "2026-10-01T11:00:00Z"},
            {"unknown": True}, {"contract": "not-a-briefing"}, {"contract_version": 2},
            {"symbol": "QQQ"}, {"session_date": "2026-1-01"},
            {"prepared_at_utc": "bad"}, {"track_record_as_of_utc": None},
            {"snapshot_id": "bad"}, {"track_record_sha256": "bad"}, {"body": None},
        ]
        modern = publish.build_briefing_artifact(self.snapshot, self.track_record)
        for changes in variants:
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp)
                legacy_path, latest = self._seed_latest(out_dir, self._legacy(**changes))
                with self.assertRaises(ValueError):
                    publish.write_briefing_artifact(modern, out_dir)
                self.assertEqual(latest.resolve(), legacy_path.resolve())

    def test_same_or_future_legacy_pointer_fails_closed(self):
        modern = publish.build_briefing_artifact(self.snapshot, self.track_record)
        for session in ("2026-10-02", "2026-10-05"):
            with self.subTest(session=session), tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp)
                legacy_path, latest = self._seed_latest(
                    out_dir, self._legacy(session_date=session)
                )
                with self.assertRaises(ValueError):
                    publish.write_briefing_artifact(modern, out_dir)
                self.assertEqual(latest.resolve(), legacy_path.resolve())

    def test_legacy_is_never_reusable_or_deliverable(self):
        legacy = self._legacy(session_date="2026-10-02")
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            publish.notify, "post"
        ) as post:
            out_dir = Path(tmp)
            legacy_path, latest = self._seed_latest(out_dir, legacy)
            with self.assertRaises(ValueError):
                publish._load_briefing(latest)
            with self.assertRaises(ValueError):
                publish.load_or_build_briefing(
                    self.snapshot, self.track_record, out_dir,
                    captured_at_utc="2026-10-02T11:01:00Z",
                )
            for dry_run in (True, False):
                with self.assertRaises(ValueError):
                    publish.deliver_briefing(
                        legacy, channel="webhook", out_dir=out_dir, dry_run=dry_run
                    )
            post.assert_not_called()
            self.assertEqual(latest.resolve(), legacy_path.resolve())
            self.assertEqual(list(out_dir.glob("morning_delivery_*.json")), [])

    def test_invalid_new_briefing_is_rejected_before_any_artifact_write(self):
        modern = publish.build_briefing_artifact(self.snapshot, self.track_record)
        variants = [{**modern, "body": "tampered"}]
        for field in ("track_record", "snapshot_generated_at_utc", "contract"):
            partial = dict(modern)
            partial.pop(field)
            variants.append(self._rehash(partial))
        variants.append(self._rehash({**modern, "track_record": None}))
        variants.append(self._rehash({**modern, "track_record_sha256": "0" * 64}))
        for artifact in variants:
            with self.subTest(keys=artifact.keys()), tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp)
                with self.assertRaises(ValueError):
                    publish.write_briefing_artifact(artifact, out_dir)
                self.assertEqual(list(out_dir.iterdir()), [])

    def test_partial_modern_latest_cannot_use_legacy_transition(self):
        modern = publish.build_briefing_artifact(self.snapshot, self.track_record)
        partial = self._legacy(track_record=self.track_record)
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            old_path, latest = self._seed_latest(out_dir, partial)
            with self.assertRaises(ValueError):
                publish.write_briefing_artifact(modern, out_dir)
            self.assertEqual(latest.resolve(), old_path.resolve())

    def test_modern_briefing_latest_remains_monotonic_on_retry(self):
        older = publish.build_briefing_artifact(self.snapshot, self.track_record)
        newer_snapshot = snapshot_contract.build_snapshot(
            symbol="SPY", source_session_date=date(2026, 10, 2),
            source_ohlc=SnapshotContractTest.SOURCE_OHLC,
            generated_at_utc="2026-10-05T11:00:00Z",
            raw_levels=calculate_pivots(**SnapshotContractTest.SOURCE_OHLC), evidence={},
        )
        newer = publish.build_briefing_artifact(newer_snapshot, {
            **self.track_record, "report_session_date": "2026-10-02",
            "as_of_utc": "2026-10-05T10:59:00Z",
        })
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            publish.write_briefing_artifact(older, out_dir)
            immutable, latest = publish.write_briefing_artifact(newer, out_dir)
            publish.load_or_build_briefing(
                self.snapshot, self.track_record, out_dir,
                captured_at_utc="2026-10-02T11:01:00Z",
            )
            self.assertEqual(latest.resolve(), immutable.resolve())
            self.assertEqual(publish._load_briefing(latest), newer)

    def test_writer_preserves_snapshot_supported_symbol_format(self):
        for symbol in ("1ABC", "ABC_DEF", "A" * 32):
            with self.subTest(symbol=symbol), tempfile.TemporaryDirectory() as tmp:
                snapshot = snapshot_contract.build_snapshot(
                    symbol=symbol, source_session_date=date(2026, 10, 1),
                    source_ohlc=SnapshotContractTest.SOURCE_OHLC,
                    generated_at_utc="2026-10-02T11:00:00Z",
                    raw_levels=calculate_pivots(**SnapshotContractTest.SOURCE_OHLC), evidence={},
                )
                modern = publish.build_briefing_artifact(
                    snapshot, {**self.track_record, "symbol": symbol}
                )
                immutable, latest = publish.write_briefing_artifact(modern, Path(tmp))
                self.assertEqual(latest.resolve(), immutable.resolve())
                self.assertEqual(publish._load_briefing(latest), modern)

    def test_failed_delivery_has_no_receipt_and_returns_failure(self):
        artifact = publish.build_briefing_artifact(self.snapshot, self.track_record)
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            publish.notify, "post", return_value=False
        ) as post:
            ok, status = publish.deliver_briefing(
                artifact, channel="webhook", out_dir=Path(tmp)
            )

            self.assertFalse(ok)
            self.assertEqual(status, "delivery-failed")
            self.assertEqual(post.call_count, 1)
            self.assertEqual(list(Path(tmp).glob("morning_delivery_*.json")), [])

    def test_dry_run_has_no_receipt_and_does_not_send(self):
        artifact = publish.build_briefing_artifact(self.snapshot, self.track_record)
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            publish.notify, "post"
        ) as post:
            ok, status = publish.deliver_briefing(
                artifact, channel="webhook", out_dir=Path(tmp), dry_run=True
            )

            self.assertFalse(ok)
            self.assertEqual(status, "dry-run")
            post.assert_not_called()
            self.assertEqual(list(Path(tmp).glob("morning_delivery_*.json")), [])

    def test_concurrent_delivery_sends_once_and_reuses_receipt(self):
        artifact = publish.build_briefing_artifact(self.snapshot, self.track_record)
        outcomes = []

        def slow_success(_body):
            time.sleep(0.05)
            return True

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            publish.notify, "post", side_effect=slow_success
        ) as post:
            out_dir = Path(tmp)

            def deliver():
                outcomes.append(
                    publish.deliver_briefing(
                        artifact, channel="webhook", out_dir=out_dir
                    )
                )

            threads = [threading.Thread(target=deliver) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)

            self.assertEqual(post.call_count, 1)
            self.assertEqual(sorted(status for _, status in outcomes), [
                "already-delivered", "sent"
            ])
            receipts = list(out_dir.glob("morning_delivery_*.json"))
            self.assertEqual(len(receipts), 1)
            receipt = json.loads(receipts[0].read_text())
            self.assertEqual(receipt["briefing_id"], artifact["briefing_id"])
            self.assertEqual(receipt["channel"], "webhook")


class SnapshotConsumerParityTest(unittest.TestCase):
    def setUp(self):
        self.snapshot = snapshot_contract.build_snapshot(
            symbol="SPY",
            source_session_date=date(2026, 10, 1),
            source_ohlc={"high": 680.0, "low": 670.0, "close": 676.0},
            generated_at_utc="2026-10-02T11:00:00Z",
            raw_levels=calculate_pivots(high=680.0, low=670.0, close=676.0),
            evidence={
                "hold_rates_publishable": False,
                "label_coverage": {30: {"complete": False, "missing_count": 7}},
                "unconditional_hold_rate_by_horizon": {},
            },
        )

    def test_publication_renders_contract_levels_without_legacy_field_names(self):
        track_record = {
            "hold_rates_publishable": False,
            "label_coverage": {"30": {"complete": False, "missing_count": 7}},
            "horizons": {},
        }

        body = publish.render(self.snapshot, track_record)

        self.assertIn("SPY LEVELS — 2026-10-02 session", body)
        self.assertIn("R1", body)
        self.assertIn("680.67", body)
        self.assertIn("HOLD RATES UNAVAILABLE", body)

    def test_intraday_alert_binds_exact_snapshot_and_level_identity(self):
        pp = next(level for level in self.snapshot["levels"] if level["label"] == "PP")
        row = SimpleNamespace(
            ts_event=1_759_407_000_000,  # 2025 value is irrelevant; identity matching is explicit here
            event_id="event-1",
            level_type="PP",
            level_price=pp["price"],
            touch_side=1,
            confluence_count=2,
        )

        bound = intraday.bind_snapshot_identity(row, self.snapshot)
        message = intraday.fmt_alert(bound, {"_publishable": False, "_coverage": {}})

        self.assertEqual(bound.snapshot_id, self.snapshot["snapshot_id"])
        self.assertEqual(bound.level_id, pp["level_id"])
        self.assertIn(self.snapshot["snapshot_id"], message)
        self.assertIn(pp["level_id"], message)

    def test_intraday_binding_fails_closed_on_level_drift(self):
        row = SimpleNamespace(
            ts_event=1_759_407_000_000,
            event_id="event-1",
            level_type="PP",
            level_price=999.0,
            touch_side=1,
            confluence_count=2,
        )

        with self.assertRaisesRegex(ValueError, "does not match snapshot"):
            intraday.bind_snapshot_identity(row, self.snapshot)


if __name__ == "__main__":
    unittest.main()
