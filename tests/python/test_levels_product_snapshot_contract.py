"""Canonical deterministic-levels snapshot contract and consumer parity tests."""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from datetime import date
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
        self.snapshot = snapshot_contract.build_snapshot(
            symbol="SPY",
            source_session_date=date(2026, 10, 1),
            source_ohlc={"high": 680.0, "low": 670.0, "close": 676.0},
            generated_at_utc="2026-10-02T11:00:00Z",
            raw_levels=calculate_pivots(high=680.0, low=670.0, close=676.0),
            evidence={"hold_rates_publishable": False, "label_coverage": {}},
        )
        self.track_record = {
            "as_of_utc": "2026-10-02T10:59:00Z",
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
        self.assertEqual(artifact["track_record"], self.track_record)
        self.assertRegex(artifact["briefing_id"], r"^brf_[0-9a-f]{24}$")
        self.assertIn("SPY LEVELS — 2026-10-02 session", artifact["body"])

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
