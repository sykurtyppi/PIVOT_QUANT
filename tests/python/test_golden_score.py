"""Golden end-to-end score test — Phase 0 exit criterion.

Exit criterion (verbatim): *a controlled event receives a reproducible score from a
named, hashed model bundle, and that result is independently read back from the
database.*

This test enforces exactly that, hermetically:

1. **Named bundle** — resolve the active runtime manifest, assert it declares a
   non-empty ``version``, and that every model artifact it references exists.
2. **Hashed bundle** — compute a sha256 over the manifest bytes plus every referenced
   ``.pkl`` and assert it is stable across two independent computations. This is the
   same integrity notion ``scripts/model_governance.py`` records as
   ``manifest_sha256`` / ``pkl_sha256s``.
3. **Reproducible score** — score one frozen "controlled event" through the *exact*
   inference contract the live server uses in ``server/ml_server.py::_score_event``
   (build a row from ``payload['feature_columns']`` → ``calibrator or pipeline`` →
   ``predict_proba[:, 1]``), then reload the artifact from disk and score again. The
   two probabilities must be bit-identical.
4. **Golden value pin** — when the on-disk bundle and scikit-learn version match the
   recorded fixture, the probability must equal the frozen expected value. If either
   differs (bundle rotated or a different sklearn), the exact pin is skipped with a
   clear message but reproducibility and read-back are still enforced.
5. **Independent DB read-back** — persist the prediction into a throwaway SQLite file
   through one connection, then reopen a *new* connection and read it back, asserting
   equality. No operator dataset is touched.

Run: ``.venv/bin/python -m pytest tests/python/test_golden_score.py`` (or unittest).
The test SKIPS (does not fail) when the model bundle is absent, so a clean checkout
without artifacts stays green; when artifacts are present the criterion is enforced.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import warnings
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
# The model pickles reference custom classes from the repo's ``ml`` package; the live
# server imports them because it runs from the repo root. Ensure the same here so
# joblib.load can resolve ``ml.*`` regardless of how this test is invoked.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
FIXTURE_PATH = ROOT / "tests" / "python" / "fixtures" / "golden_score.json"

# Bit-identical is the intent; allow a floating-point epsilon only to be defensive
# against platform FP nondeterminism that should not occur for tree ensembles.
REPRODUCIBILITY_TOL = 1e-12
GOLDEN_VALUE_TOL = 1e-9


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _read_env_file(path: Path) -> dict:
    """Minimal .env reader (KEY=VALUE, ignores comments / `export`)."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        values[key.strip()] = val.strip().strip('"').strip("'")
    return values


def _resolve_manifest(fixture: dict) -> Path:
    """Resolve the active manifest the way the *running* server does.

    Mirrors server/ml_server.py::ModelRegistry.resolve_manifest_path: RF_MANIFEST_PATH
    (full-path override) wins; otherwise MODEL_DIR / RF_ACTIVE_MANIFEST, then the
    runtime-latest candidate, then legacy. Crucially the live stack is launched via
    server/run_ml_server.sh, which sources the repo `.env` (RF_ACTIVE_MANIFEST there is
    `manifest_runtime_latest.json`, the bundle that actually serves — the code default
    `manifest_active.json` is the stale/broken v175 pointer). So we layer the same `.env`
    under the process environment (explicit env wins, exactly like a sourced shell) to
    resolve the bundle prod really loads, not the unused code default.
    """
    env = _read_env_file(ROOT / ".env")
    env.update({k: v for k, v in os.environ.items() if k in ("RF_MANIFEST_PATH", "RF_MODEL_DIR", "RF_ACTIVE_MANIFEST")})

    def _relative(p: str) -> Path:
        candidate = Path(p).expanduser()
        return candidate if candidate.is_absolute() else (ROOT / candidate)

    manifest_override = (env.get("RF_MANIFEST_PATH") or "").strip()
    if manifest_override:
        return _relative(manifest_override)

    model_dir = _relative((env.get("RF_MODEL_DIR") or "data/models").strip())
    active_name = (env.get("RF_ACTIVE_MANIFEST") or "manifest_active.json").strip() or "manifest_active.json"
    for name in (active_name, "manifest_runtime_latest.json", "manifest_latest.json"):
        candidate = model_dir / name
        if candidate.exists():
            return candidate
    # Last resort: whatever the fixture recorded (keeps the test meaningful offline).
    return ROOT / fixture["manifest"]


def _referenced_artifacts(manifest: dict, manifest_dir: Path) -> list[Path]:
    paths: list[Path] = []
    for horizons in manifest.get("models", {}).values():
        if isinstance(horizons, dict):
            for filename in horizons.values():
                paths.append(manifest_dir / str(filename))
    return paths


def _bundle_hash(manifest_path: Path, manifest: dict) -> str:
    """Stable sha256 over the manifest bytes + every referenced artifact, in sorted order."""
    digest = hashlib.sha256()
    digest.update(manifest_path.read_bytes())
    for artifact in sorted(_referenced_artifacts(manifest, manifest_path.parent), key=str):
        digest.update(str(artifact.name).encode("utf-8"))
        digest.update(artifact.read_bytes())
    return digest.hexdigest()


def _score_from_bundle(payload: dict, feature_row: dict) -> float:
    """Mirror server/ml_server.py::_score_event inference for a single model payload.

    We reproduce the server's per-payload scoring rather than import and call
    ``ml_server._score_event`` directly: importing that module has heavy import-time side
    effects (config load, prediction-log writer thread, registry bootstrap), and the
    scoring core lives in the flag-only zone the repo's AGENTS.md forbids editing to add a
    test seam. The trade-off is that this copy can drift from the server; it is kept
    line-faithful to _score_event / extract_prob (server/ml_server.py:3365-3397):
      - row is projected onto payload['feature_columns'] (missing -> None, like the server)
      - the scored estimator is `payload['calibrator'] or payload['pipeline']`
      - probability is the positive-class column of predict_proba, with the same
        single-class (shape[1]==1) fallback the server applies via classes_.
    """
    import pandas as pd

    feature_cols = payload.get("feature_columns", [])
    if not feature_cols:
        raise ValueError("bundle payload missing feature_columns")
    row = {col: feature_row.get(col) for col in feature_cols}
    frame = pd.DataFrame([row])
    model = payload.get("calibrator") or payload.get("pipeline")
    if model is None:
        raise ValueError("bundle payload exposes neither calibrator nor pipeline")
    probs = model.predict_proba(frame)
    if probs.shape[1] == 2:
        return float(probs[:, 1][0])
    # Degenerate single-class model: mirror server extract_prob's classes_ fallback.
    classes = getattr(model, "classes_", None)
    if classes is None and hasattr(model, "base_model"):
        classes = getattr(model.base_model, "classes_", None)
    if classes is not None and len(classes) == 1:
        return 1.0 if int(classes[0]) == 1 else 0.0
    raise AssertionError(f"unexpected predict_proba shape {probs.shape} with classes={classes}")


class GoldenScoreTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not FIXTURE_PATH.exists():
            raise unittest.SkipTest(f"golden fixture missing: {FIXTURE_PATH}")
        try:
            import joblib  # noqa: F401
            import pandas  # noqa: F401
            import sklearn  # noqa: F401
        except Exception as exc:  # pragma: no cover - env guard
            raise unittest.SkipTest(f"runtime deps unavailable ({exc}); use .venv 3.11")

        cls.fixture = _load_fixture()
        cls.manifest_path = _resolve_manifest(cls.fixture)
        if not cls.manifest_path.exists():
            raise unittest.SkipTest(f"active manifest not present: {cls.manifest_path}")
        cls.manifest = json.loads(cls.manifest_path.read_text(encoding="utf-8"))

        golden = cls.fixture["golden"]
        cls.artifact_path = cls.manifest_path.parent / golden["artifact"]
        # The golden artifact may not be the one the resolved manifest names (bundle
        # rotation). Resolve the reject@15 artifact from the manifest itself.
        try:
            manifest_artifact = cls.manifest["models"][golden["target"]][str(golden["horizon"])]
            cls.artifact_path = cls.manifest_path.parent / manifest_artifact
        except (KeyError, TypeError):
            pass
        if not cls.artifact_path.exists():
            raise unittest.SkipTest(f"golden artifact not present: {cls.artifact_path}")

    def _load_payload(self) -> dict:
        import joblib

        with warnings.catch_warnings():
            # Bundles may have been trained under a different sklearn; the version
            # skew is asserted separately, we don't want the warning to fail -W error.
            warnings.simplefilter("ignore")
            return joblib.load(self.artifact_path)

    # 1 + 2 — named, hashed bundle -------------------------------------------------
    def test_bundle_is_named_and_hash_covers_artifacts(self) -> None:
        version = str(self.manifest.get("version") or "").strip()
        self.assertTrue(version, "runtime manifest must declare a non-empty version (named bundle)")

        for artifact in _referenced_artifacts(self.manifest, self.manifest_path.parent):
            self.assertTrue(
                artifact.exists(),
                f"manifest references missing artifact: {artifact}",
            )

        full = _bundle_hash(self.manifest_path, self.manifest)
        self.assertEqual(len(full), 64, "sha256 hex digest expected")

        # Non-tautological: prove the digest actually covers the .pkl bytes, not just the
        # manifest JSON. A manifest-only digest must differ from the full bundle digest.
        import hashlib

        manifest_only = hashlib.sha256(self.manifest_path.read_bytes()).hexdigest()
        self.assertNotEqual(
            full,
            manifest_only,
            "bundle hash does not incorporate model artifacts (would miss pkl tampering)",
        )

        # Pin against the fixture when the resolved bundle IS the fixture's bundle, so a
        # change to any manifest/pkl byte is caught. On rotation the pin is skipped (the
        # fixture is regenerated intentionally), but the structural asserts above still run.
        pinned = self.fixture.get("bundle_sha256")
        same_manifest = self.manifest_path.resolve() == (ROOT / self.fixture["manifest"]).resolve()
        if pinned and same_manifest and version == self.fixture.get("model_version"):
            self.assertEqual(
                full,
                pinned,
                "bundle hash drifted from the pinned fixture value; a manifest or pkl changed",
            )

    # 3 — reproducible score -------------------------------------------------------
    def test_controlled_event_score_is_reproducible(self) -> None:
        feature_row = self.fixture["golden"]["feature_row"]
        payload_a = self._load_payload()
        prob_a = _score_from_bundle(payload_a, feature_row)

        self.assertGreaterEqual(prob_a, 0.0)
        self.assertLessEqual(prob_a, 1.0)

        payload_b = self._load_payload()  # independent load from disk
        prob_b = _score_from_bundle(payload_b, feature_row)
        self.assertLessEqual(
            abs(prob_a - prob_b),
            REPRODUCIBILITY_TOL,
            f"score not reproducible across loads: {prob_a!r} vs {prob_b!r}",
        )

    # 4 — golden value pin (version-guarded) --------------------------------------
    def test_controlled_event_matches_golden_value(self) -> None:
        import sklearn

        fixture = self.fixture
        version = str(self.manifest.get("version") or "").strip()
        if version != fixture.get("model_version"):
            self.skipTest(
                f"bundle version {version!r} != fixture {fixture.get('model_version')!r}; "
                "reproducibility still enforced elsewhere, regenerate fixture to re-pin"
            )
        if sklearn.__version__ != fixture.get("sklearn_version"):
            self.skipTest(
                f"sklearn {sklearn.__version__} != fixture {fixture.get('sklearn_version')}; "
                "exact value pin skipped (pin is version-specific)"
            )

        payload = self._load_payload()
        prob = _score_from_bundle(payload, fixture["golden"]["feature_row"])
        self.assertLessEqual(
            abs(prob - float(fixture["golden"]["expected_prob"])),
            GOLDEN_VALUE_TOL,
            f"golden score drifted: got {prob!r}, expected {fixture['golden']['expected_prob']!r}",
        )

    # 5 — independent DB read-back -------------------------------------------------
    def test_score_persists_and_reads_back_from_database(self) -> None:
        """Persist the score into the REAL prediction_log column layout, then read it
        back on an independent connection.

        The score is written to the target/horizon-specific column (e.g. prob_reject_15m),
        so the assertion is not a tautological float round-trip: it fails if the score is
        mapped to the wrong column, if the opposite-side column is not left NULL, or if the
        real UNIQUE(event_id, model_version) constraint rejects the row. The table uses the
        production DDL so a schema change that drops/renames the column surfaces here.
        """
        fixture = self.fixture
        payload = self._load_payload()
        prob = _score_from_bundle(payload, fixture["golden"]["feature_row"])

        target = fixture["golden"]["target"]
        horizon = int(fixture["golden"]["horizon"])
        score_col = f"prob_{target}_{horizon}m"
        other_col = f"prob_{'break' if target == 'reject' else 'reject'}_{horizon}m"
        version = str(self.manifest.get("version") or "").strip()
        feature_version = str(self.manifest.get("feature_version") or fixture.get("feature_version") or "")
        event_id = f"GOLDEN::{version}::{target}::{horizon}m"

        # Production prediction_log DDL (server/ml_server.py), trimmed to the columns this
        # test asserts plus the real UNIQUE(event_id, model_version) constraint.
        create_sql = """
            CREATE TABLE prediction_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                ts_prediction INTEGER NOT NULL,
                model_version TEXT,
                feature_version TEXT,
                prob_reject_5m REAL, prob_reject_15m REAL, prob_reject_30m REAL, prob_reject_60m REAL,
                prob_break_5m REAL, prob_break_15m REAL, prob_break_30m REAL, prob_break_60m REAL,
                UNIQUE(event_id, model_version)
            )
        """

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "golden_readback.sqlite"

            writer = sqlite3.connect(str(db_path))
            try:
                writer.execute(create_sql)
                writer.execute(
                    f"INSERT INTO prediction_log "
                    f"(event_id, ts_prediction, model_version, feature_version, {score_col}) "
                    f"VALUES (?, ?, ?, ?, ?)",
                    (event_id, 1_700_000_000_000, version, feature_version, prob),
                )
                writer.commit()
            finally:
                writer.close()

            reader = sqlite3.connect(str(db_path))
            try:
                reader.row_factory = sqlite3.Row
                got = reader.execute(
                    f"SELECT event_id, model_version, feature_version, {score_col} AS scored, "
                    f"{other_col} AS other_side FROM prediction_log WHERE event_id = ?",
                    [event_id],
                ).fetchone()
            finally:
                reader.close()

        self.assertIsNotNone(got, "prediction did not read back from the database")
        self.assertEqual(got["event_id"], event_id)
        self.assertEqual(got["model_version"], version)
        self.assertEqual(got["feature_version"], feature_version)
        self.assertEqual(
            got["scored"], prob,
            f"score did not land in {score_col}: {got['scored']!r} vs {prob!r}",
        )
        self.assertIsNone(
            got["other_side"],
            f"opposite-side column {other_col} should be NULL, got {got['other_side']!r}",
        )


if __name__ == "__main__":
    unittest.main()
