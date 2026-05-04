"""Tests for services.research_protocol.audit_logger (RESEARCH_PROTOCOL §7)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from services.research_protocol._paths import ENV_PROTOCOL_ROOT, audit_log_path
from services.research_protocol.audit_logger import (
    AUDIT_LOG_VERSION,
    DECISIONS,
    EVENT_TYPES,
    AuditEvent,
    DatasetFingerprint,
    build_run_fingerprint,
    emit_audit_event,
    hash_dataframe_schema_or_csv,
    hash_file,
    hash_signal_definition,
    load_audit_events,
    safe_emit_audit_event,
)
from services.research_protocol.errors import AuditLogTamperingError


class _ProtocolRootBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp_ctx = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp_ctx.name).resolve()
        self._prev_env = os.environ.get(ENV_PROTOCOL_ROOT)
        os.environ[ENV_PROTOCOL_ROOT] = str(self.tmp)

    def tearDown(self) -> None:
        if self._prev_env is None:
            os.environ.pop(ENV_PROTOCOL_ROOT, None)
        else:
            os.environ[ENV_PROTOCOL_ROOT] = self._prev_env
        self._tmp_ctx.cleanup()


# --------------------------------------------------------------------- #
# Emit + load
# --------------------------------------------------------------------- #


class TestEmitAuditEvent(_ProtocolRootBase):
    def test_emit_creates_jsonl_line(self):
        event = emit_audit_event(
            event_type="protocol_pass",
            decision="pass",
            candidate_id="cand-1",
            protocol_stage=2,
            registration_hash="a" * 64,
        )
        self.assertIsInstance(event, AuditEvent)
        self.assertEqual(event.event_type, "protocol_pass")
        self.assertEqual(event.decision, "pass")
        path = audit_log_path()
        self.assertTrue(path.exists())
        lines = path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["candidate_id"], "cand-1")
        self.assertEqual(record["protocol_stage"], 2)
        self.assertEqual(record["audit_log_version"], AUDIT_LOG_VERSION)

    def test_pass_event_logged_with_required_fields(self):
        emit_audit_event(
            event_type="protocol_pass", decision="pass",
            candidate_id="cand-1", registration_hash="a" * 64,
        )
        events = load_audit_events()
        self.assertEqual(len(events), 1)
        e = events[0]
        self.assertEqual(e.event_type, "protocol_pass")
        self.assertEqual(e.decision, "pass")
        self.assertEqual(e.candidate_id, "cand-1")
        self.assertIsNotNone(e.event_id)
        self.assertIsNotNone(e.timestamp_utc)

    def test_block_event_logged(self):
        emit_audit_event(
            event_type="kill_list_block", decision="block",
            candidate_id="dead-cand",
            reason="cross-period falsified",
            registration_hash="b" * 64,
        )
        events = load_audit_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].decision, "block")
        self.assertEqual(events[0].reason, "cross-period falsified")

    def test_event_id_unique_across_emits(self):
        ids = set()
        for _ in range(20):
            ev = emit_audit_event(
                event_type="protocol_pass", decision="pass",
                candidate_id="cand-1", registration_hash="a" * 64,
            )
            ids.add(ev.event_id)
        self.assertEqual(len(ids), 20)

    def test_timestamp_is_utc_iso8601(self):
        ev = emit_audit_event(
            event_type="protocol_pass", decision="pass",
            candidate_id="cand-1", registration_hash="a" * 64,
        )
        parsed = datetime.fromisoformat(ev.timestamp_utc)
        self.assertEqual(parsed.utcoffset(), timezone.utc.utcoffset(None))

    def test_invalid_event_type_rejected(self):
        with self.assertRaises(AuditLogTamperingError):
            emit_audit_event(
                event_type="not_a_real_event",
                decision="pass",
                candidate_id="cand-1",
            )

    def test_invalid_decision_rejected(self):
        with self.assertRaises(AuditLogTamperingError):
            emit_audit_event(
                event_type="protocol_pass",
                decision="approve",
                candidate_id="cand-1",
            )

    def test_invalid_protocol_stage_rejected(self):
        for bad in (-1, 7, 100, "2", True):
            with self.subTest(bad=bad):
                with self.assertRaises(AuditLogTamperingError):
                    emit_audit_event(
                        event_type="protocol_pass",
                        decision="pass",
                        candidate_id="cand-1",
                        protocol_stage=bad,  # type: ignore[arg-type]
                    )

    def test_missing_candidate_id_rejected_for_most_events(self):
        with self.assertRaises(AuditLogTamperingError):
            emit_audit_event(
                event_type="protocol_pass",
                decision="pass",
                candidate_id=None,
            )

    def test_registration_rejected_event_allows_no_candidate_id(self):
        # registration_rejected events may fire before a candidate is loaded.
        ev = emit_audit_event(
            event_type="registration_rejected",
            decision="block",
            candidate_id=None,
            reason="schema invalid",
        )
        self.assertEqual(ev.event_type, "registration_rejected")

    def test_metadata_round_trip(self):
        emit_audit_event(
            event_type="stage_result_recorded",
            decision="record",
            candidate_id="cand-1",
            protocol_stage=2,
            metadata={"status": "pass", "n_eff": 50, "nested": {"k": "v"}},
        )
        events = load_audit_events()
        self.assertEqual(events[0].metadata["status"], "pass")
        self.assertEqual(events[0].metadata["n_eff"], 50)
        self.assertEqual(events[0].metadata["nested"], {"k": "v"})


class TestSafeEmit(_ProtocolRootBase):
    def test_safe_emit_returns_event_on_success(self):
        ev = safe_emit_audit_event(
            event_type="protocol_pass", decision="pass",
            candidate_id="cand-1", registration_hash="a" * 64,
        )
        self.assertIsNotNone(ev)
        self.assertEqual(ev.event_type, "protocol_pass")

    def test_safe_emit_propagates_input_validation_errors(self):
        # AuditLogTamperingError on bad input must propagate.
        with self.assertRaises(AuditLogTamperingError):
            safe_emit_audit_event(
                event_type="bogus", decision="pass",
                candidate_id="cand-1",
            )


class TestLoadAuditEvents(_ProtocolRootBase):
    def test_no_log_returns_empty_list(self):
        self.assertEqual(load_audit_events(), [])

    def test_filters_by_candidate_id(self):
        for cand in ("cand-1", "cand-2"):
            emit_audit_event(
                event_type="protocol_pass", decision="pass",
                candidate_id=cand, registration_hash="a" * 64,
            )
        first = load_audit_events(candidate_id="cand-1")
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0].candidate_id, "cand-1")

    def test_filters_by_event_type(self):
        emit_audit_event(
            event_type="protocol_pass", decision="pass",
            candidate_id="cand-1", registration_hash="a" * 64,
        )
        emit_audit_event(
            event_type="kill_list_block", decision="block",
            candidate_id="cand-1", registration_hash="a" * 64,
            reason="r",
        )
        passes = load_audit_events(event_type="protocol_pass")
        blocks = load_audit_events(event_type="kill_list_block")
        self.assertEqual(len(passes), 1)
        self.assertEqual(len(blocks), 1)


class TestTamperingDetection(_ProtocolRootBase):
    def _write_lines(self, lines: list[str]) -> None:
        path = audit_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def test_invalid_json_line_rejected(self):
        self._write_lines(["{not json"])
        with self.assertRaises(AuditLogTamperingError):
            load_audit_events()

    def test_top_level_array_rejected(self):
        self._write_lines(["[1, 2, 3]"])
        with self.assertRaises(AuditLogTamperingError):
            load_audit_events()

    def test_missing_required_keys_rejected(self):
        self._write_lines([
            json.dumps({
                "event_id": "x",
                "timestamp_utc": "2026-05-04T19:00:00+00:00",
                # missing event_type, decision, audit_log_version
            }),
        ])
        with self.assertRaises(AuditLogTamperingError):
            load_audit_events()

    def test_unknown_event_type_rejected(self):
        self._write_lines([
            json.dumps({
                "event_id": "x",
                "timestamp_utc": "2026-05-04T19:00:00+00:00",
                "event_type": "bogus_event",
                "decision": "pass",
                "audit_log_version": AUDIT_LOG_VERSION,
            }),
        ])
        with self.assertRaises(AuditLogTamperingError):
            load_audit_events()

    def test_unknown_decision_rejected(self):
        self._write_lines([
            json.dumps({
                "event_id": "x",
                "timestamp_utc": "2026-05-04T19:00:00+00:00",
                "event_type": "protocol_pass",
                "decision": "bogus",
                "audit_log_version": AUDIT_LOG_VERSION,
            }),
        ])
        with self.assertRaises(AuditLogTamperingError):
            load_audit_events()

    def test_non_utc_timestamp_rejected(self):
        self._write_lines([
            json.dumps({
                "event_id": "x",
                "timestamp_utc": "2026-05-04T19:00:00-05:00",  # not UTC
                "event_type": "protocol_pass",
                "decision": "pass",
                "audit_log_version": AUDIT_LOG_VERSION,
            }),
        ])
        with self.assertRaises(AuditLogTamperingError):
            load_audit_events()

    def test_naive_timestamp_rejected(self):
        self._write_lines([
            json.dumps({
                "event_id": "x",
                "timestamp_utc": "2026-05-04T19:00:00",       # no offset
                "event_type": "protocol_pass",
                "decision": "pass",
                "audit_log_version": AUDIT_LOG_VERSION,
            }),
        ])
        with self.assertRaises(AuditLogTamperingError):
            load_audit_events()

    def test_unparseable_timestamp_rejected(self):
        self._write_lines([
            json.dumps({
                "event_id": "x",
                "timestamp_utc": "yesterday",
                "event_type": "protocol_pass",
                "decision": "pass",
                "audit_log_version": AUDIT_LOG_VERSION,
            }),
        ])
        with self.assertRaises(AuditLogTamperingError):
            load_audit_events()

    def test_wrong_audit_log_version_rejected(self):
        self._write_lines([
            json.dumps({
                "event_id": "x",
                "timestamp_utc": "2026-05-04T19:00:00+00:00",
                "event_type": "protocol_pass",
                "decision": "pass",
                "audit_log_version": 999,
            }),
        ])
        with self.assertRaises(AuditLogTamperingError):
            load_audit_events()


class TestAppendOnlyAPI(unittest.TestCase):
    """The audit logger module surface must not expose any removal."""

    def test_no_removal_or_clear_functions(self):
        from services.research_protocol import audit_logger as mod
        for name in (
            "remove",
            "remove_event",
            "delete",
            "delete_event",
            "clear",
            "clear_log",
            "reset",
            "reset_log",
            "truncate",
            "rotate",
            "purge",
        ):
            self.assertFalse(
                hasattr(mod, name),
                msg=f"audit_logger must not expose {name}() — log is append-only",
            )

    def test_package_exports_have_no_removal_names(self):
        import services.research_protocol as pkg
        for name in pkg.__all__:
            lower = name.lower()
            for forbidden in ("remove", "delete", "clear", "reset", "purge"):
                self.assertNotIn(
                    forbidden, lower,
                    msg=f"package __all__ exposes a {forbidden}-like name: {name}",
                )


# --------------------------------------------------------------------- #
# Fingerprint helpers
# --------------------------------------------------------------------- #


class TestHashFile(_ProtocolRootBase):
    def test_identical_content_same_hash(self):
        a = self.tmp / "a.txt"
        b = self.tmp / "b.txt"
        a.write_bytes(b"hello world")
        b.write_bytes(b"hello world")
        self.assertEqual(hash_file(a), hash_file(b))

    def test_different_content_different_hash(self):
        a = self.tmp / "a.txt"
        b = self.tmp / "b.txt"
        a.write_bytes(b"hello world")
        b.write_bytes(b"hello WORLD")
        self.assertNotEqual(hash_file(a), hash_file(b))

    def test_missing_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            hash_file(self.tmp / "nope.txt")


class TestHashDataframeSchemaOrCsv(_ProtocolRootBase):
    def _write_csv(self, name: str, df: pd.DataFrame) -> Path:
        p = self.tmp / name
        df.to_csv(p, index=False)
        return p

    def test_csv_fingerprint_basic(self):
        df = pd.DataFrame({
            "entry_date": ["2025-01-02", "2025-01-03"],
            "win_rate": [0.5, 0.7],
        })
        p = self._write_csv("ds.csv", df)
        fp = hash_dataframe_schema_or_csv(p)
        self.assertIsInstance(fp, DatasetFingerprint)
        self.assertEqual(fp.row_count, 2)
        self.assertEqual(fp.columns, ("entry_date", "win_rate"))
        self.assertEqual(fp.min_date, "2025-01-02")
        self.assertEqual(fp.max_date, "2025-01-03")
        self.assertEqual(len(fp.sha256), 64)

    def test_identical_data_same_hashes(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        a = self._write_csv("a.csv", df)
        b = self._write_csv("b.csv", df)
        fa = hash_dataframe_schema_or_csv(a)
        fb = hash_dataframe_schema_or_csv(b)
        self.assertEqual(fa.sha256, fb.sha256)
        self.assertEqual(fa.column_set_hash, fb.column_set_hash)

    def test_different_row_changes_sha(self):
        a = self._write_csv("a.csv", pd.DataFrame({"x": [1, 2, 3]}))
        b = self._write_csv("b.csv", pd.DataFrame({"x": [1, 2, 4]}))
        self.assertNotEqual(
            hash_dataframe_schema_or_csv(a).sha256,
            hash_dataframe_schema_or_csv(b).sha256,
        )

    def test_added_column_changes_column_set_hash(self):
        a = self._write_csv("a.csv", pd.DataFrame({"x": [1, 2]}))
        b = self._write_csv("b.csv", pd.DataFrame({"x": [1, 2], "y": [3, 4]}))
        self.assertNotEqual(
            hash_dataframe_schema_or_csv(a).column_set_hash,
            hash_dataframe_schema_or_csv(b).column_set_hash,
        )

    def test_unknown_extension_returns_sha_only(self):
        p = self.tmp / "blob.bin"
        p.write_bytes(b"\x00\x01\x02")
        fp = hash_dataframe_schema_or_csv(p)
        self.assertIsNone(fp.row_count)
        self.assertIsNone(fp.column_set_hash)
        self.assertEqual(len(fp.sha256), 64)


def _signal_body(*, threshold_value: float = 0.7) -> dict:
    return {
        "hypothesis": {
            "mechanism": "test mechanism",
            "predicted_direction": "long",
            "why_might_fail": "regime",
            "citations": [],
        },
        "features": [{"name": "f1", "input_columns": ["close"]}],
        "thresholds": [{"name": "t1", "kind": "fixed", "value": threshold_value}],
        "transformations": {"allowed": [], "forbidden_unless_listed": []},
        "forbidden_changes": ["any"],
        "falsification": {"stage_3": "x"},
        "horizon_days": 5,
    }


class TestHashSignalDefinition(unittest.TestCase):
    def test_identical_body_identical_hash(self):
        a = _signal_body()
        b = _signal_body()
        self.assertEqual(hash_signal_definition(a), hash_signal_definition(b))

    def test_threshold_change_changes_hash(self):
        a = _signal_body(threshold_value=0.7)
        b = _signal_body(threshold_value=0.8)
        self.assertNotEqual(
            hash_signal_definition(a), hash_signal_definition(b)
        )

    def test_irrelevant_metadata_does_not_change_signal_hash(self):
        a = _signal_body()
        b = _signal_body()
        # add a non-signal-defining key — should NOT affect signal hash
        b["registration_timestamp"] = "2026-05-04T19:00:00Z"
        b["git_commit_sha"] = "ffffffff" * 5
        self.assertEqual(hash_signal_definition(a), hash_signal_definition(b))

    def test_horizon_change_changes_hash(self):
        a = _signal_body()
        b = _signal_body()
        b["horizon_days"] = 21
        self.assertNotEqual(hash_signal_definition(a), hash_signal_definition(b))

    def test_non_dict_input_rejected(self):
        with self.assertRaises(AuditLogTamperingError):
            hash_signal_definition("not a dict")  # type: ignore[arg-type]


class TestBuildRunFingerprint(_ProtocolRootBase):
    def _write_csv(self, name: str, df: pd.DataFrame) -> Path:
        p = self.tmp / name
        df.to_csv(p, index=False)
        return p

    def test_fingerprint_is_stable_for_same_inputs(self):
        body = _signal_body()
        ds = self._write_csv("ds.csv", pd.DataFrame({"x": [1, 2, 3]}))
        fp1 = build_run_fingerprint(
            registration_body=body, registration_hash="r" * 64,
            datasets=[ds], code_version="commit-abc",
        )
        fp2 = build_run_fingerprint(
            registration_body=body, registration_hash="r" * 64,
            datasets=[ds], code_version="commit-abc",
        )
        # The signal + dataset hashes are deterministic.
        self.assertEqual(
            fp1["signal_definition_hash"], fp2["signal_definition_hash"]
        )
        self.assertEqual(
            fp1["datasets"][0]["sha256"], fp2["datasets"][0]["sha256"]
        )
        self.assertEqual(fp1["registration_hash"], fp2["registration_hash"])
        self.assertEqual(fp1["code_version"], fp2["code_version"])

    def test_fingerprint_changes_with_dataset_content(self):
        body = _signal_body()
        ds_a = self._write_csv("a.csv", pd.DataFrame({"x": [1, 2, 3]}))
        ds_b = self._write_csv("b.csv", pd.DataFrame({"x": [1, 2, 4]}))
        fp_a = build_run_fingerprint(
            registration_body=body, registration_hash="r" * 64,
            datasets=[ds_a], code_version="c",
        )
        fp_b = build_run_fingerprint(
            registration_body=body, registration_hash="r" * 64,
            datasets=[ds_b], code_version="c",
        )
        self.assertNotEqual(
            fp_a["datasets"][0]["sha256"],
            fp_b["datasets"][0]["sha256"],
        )

    def test_fingerprint_changes_with_signal_definition(self):
        body_a = _signal_body(threshold_value=0.7)
        body_b = _signal_body(threshold_value=0.8)
        fp_a = build_run_fingerprint(
            registration_body=body_a, registration_hash="r" * 64,
            datasets=[], code_version="c",
        )
        fp_b = build_run_fingerprint(
            registration_body=body_b, registration_hash="r" * 64,
            datasets=[], code_version="c",
        )
        self.assertNotEqual(
            fp_a["signal_definition_hash"],
            fp_b["signal_definition_hash"],
        )

    def test_fingerprint_includes_required_top_level_keys(self):
        fp = build_run_fingerprint(
            registration_body=_signal_body(),
            registration_hash="r" * 64,
            datasets=[],
            code_version="c",
        )
        for key in (
            "registration_hash",
            "signal_definition_hash",
            "datasets",
            "code_version",
            "fingerprinted_at",
        ):
            self.assertIn(key, fp)


if __name__ == "__main__":
    unittest.main()
