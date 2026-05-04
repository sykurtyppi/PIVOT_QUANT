# Known Pre-Existing Test Failures

This file documents test failures that existed before the historical validation
and candidate signal work (PR23–PR42, Audit-Fix PR1–PR4). They are unrelated to
that work and should not be counted as regressions introduced by it.

**Full suite command:**
```bash
python -m unittest discover -s tests/unit/test_services
```

**Expected outcome:** `FAILED (failures=1, errors=1)` — both failures listed below.

**Focused ML-only suite (clean):**
```bash
python -m unittest discover -s tests/unit/test_services -p "test_ml_*.py"
```

**Expected outcome:** `OK` — 142 tests, zero failures.

---

## Failure 1 — `test_market_data_client` (ImportError: `pytest` not installed)

**Classification:** ERROR (module load failure)

**Reproducer:**
```bash
python -m unittest tests.unit.test_services.test_market_data_client -v
```

**Observed error:**
```
ERROR: test_market_data_client (unittest.loader._FailedTest.test_market_data_client)
----------------------------------------------------------------------
ImportError: Failed to import test module: test_market_data_client
Traceback (most recent call last):
  File ".../unittest/loader.py", line 137, in loadTestsFromName
    module = __import__(module_name)
  File ".../tests/unit/test_services/test_market_data_client.py", line 17, in <module>
    import pytest
ModuleNotFoundError: No module named 'pytest'
```

**Root cause:**
`test_market_data_client.py` uses `pytest` fixtures and assertions, but the
project's test runner is `unittest` and `pytest` is not installed in the `.venv`.
The test file fails at import time — no test logic is executed.

**Why unrelated to historical validation / candidate signal work:**
The `test_market_data_client.py` file tests `MarketDataClient`, a live data
fetching class with no connection to the historical T9 pipeline, the model-ready
dataset export, the candidate signal, or the readiness checklist. None of the
PRs in the PR23–PR42 / Audit-Fix sequence touch this file or its module.

**Recommended fix:**
Either install `pytest` as a dev dependency (`pip install pytest`) or rewrite
`test_market_data_client.py` to use `unittest` assertions only, removing the
`import pytest` line and any `pytest`-specific constructs. This is a one-line
fix for the import; the broader question of whether `pytest` should be a
project dependency is a separate decision.

---

## Failure 2 — `test_snapshot_pairing_progress_summary` (percentage vs. fraction)

**Classification:** FAIL (assertion error — unit mismatch)

**Reproducer:**
```bash
python -m unittest \
  tests.unit.test_services.test_institutional_ml_db_diagnostics.TestInstitutionalDiagnostics.test_snapshot_pairing_progress_summary \
  -v
```

**Observed error:**
```
FAIL: test_snapshot_pairing_progress_summary
----------------------------------------------------------------------
AssertionError: 25.0 != 0.25 within 7 places (24.75 difference)
```

**Location:**
- Test: `tests/unit/test_services/test_institutional_ml_db_diagnostics.py`, line 525
- Production: `services/institutional_ml_db.py`, line 3023

**Root cause:**
The production code computes `pairable_event_pct` as a **percentage** (0–100):
```python
pairable_event_pct = float(pairable_events / total_events) * 100.0
```
The test asserts the value as a **fraction** (0–1):
```python
self.assertAlmostEqual(status["pairable_event_pct"], 0.25)
```
With 1 pairable event out of 4 total, the production code returns `25.0`; the
test expects `0.25`. One of the two is wrong — the field name ends in `_pct`
which implies percentage (0–100), suggesting the test expectation is incorrect.

**Why unrelated to historical validation / candidate signal work:**
This failure is inside `TestInstitutionalDiagnostics`, which tests
`InstitutionalMLDatabase` — the earnings-event snapshot pairing subsystem.
That subsystem is entirely separate from the historical T9 pipeline, the
model-ready dataset export, the boundary purge, the candidate signal diagnostics,
and the readiness checklist. None of the PRs in the PR23–PR42 / Audit-Fix
sequence touch `institutional_ml_db.py` or its tests.

**Recommended fix:**
Decide the intended unit for `pairable_event_pct`. If percentage (0–100) is
correct (consistent with the `_pct` suffix convention), fix the test:
```python
self.assertAlmostEqual(status["pairable_event_pct"], 25.0)
```
If fraction (0–1) is desired, rename the field to `pairable_event_rate` and
update the production code:
```python
pairable_event_pct = float(pairable_events / total_events)  # fraction
```
Either fix is a single-line change. The chosen unit should be applied
consistently to the analogous `0.0` defaults on lines 2960 and 2976 of
`institutional_ml_db.py`.

---

## Scope statement

The **142 tests** in `test_ml_*.py` cover the entire historical validation and
candidate signal pipeline and are fully clean. The two failures above will remain
in the full suite count until separately addressed. Any new failure in
`test_ml_*.py` is a regression introduced by recent work and must be
investigated immediately.
