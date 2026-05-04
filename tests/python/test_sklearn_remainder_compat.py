"""Unit tests for _patch_sklearn_remainder_compat in server/ml_server.py.

Covers:
  - shim is registered when _RemainderColsList is absent
  - shim is list (semantically correct)
  - shim is idempotent when attribute already exists (no clobber)
  - helper is importable from ml_server without side-effects
"""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Load only the helper function, without importing the full ml_server module
# (which requires fastapi, uvicorn, etc. and tries to connect to services).
# We extract it via source inspection and exec in an isolated namespace.
# ---------------------------------------------------------------------------

def _load_patch_fn():
    """Return the _patch_sklearn_remainder_compat function from ml_server source."""
    src_path = REPO_ROOT / "server" / "ml_server.py"
    source = src_path.read_text(encoding="utf-8")

    # Extract just the function definition by finding its boundaries.
    start_marker = "def _patch_sklearn_remainder_compat"
    end_marker = "\nclass ModelRegistry:"
    start = source.index(start_marker)
    end = source.index(end_marker, start)
    fn_source = source[start:end].rstrip()

    ns: dict = {}
    exec(compile(fn_source, str(src_path), "exec"), ns)  # noqa: S102
    return ns["_patch_sklearn_remainder_compat"]


_patch_sklearn_remainder_compat = _load_patch_fn()


class TestPatchSklearnRemainderCompatMissing(unittest.TestCase):
    """Shim is registered when the attribute does not exist."""

    def setUp(self):
        import sklearn.compose._column_transformer as ct
        self._ct = ct
        self._original = ct.__dict__.get("_RemainderColsList", _SENTINEL := object())
        self._sentinel = _SENTINEL
        # Remove the attribute if present so we can test the "absent" branch.
        if hasattr(ct, "_RemainderColsList"):
            delattr(ct, "_RemainderColsList")

    def tearDown(self):
        # Restore original state.
        if self._original is self._sentinel:
            if hasattr(self._ct, "_RemainderColsList"):
                delattr(self._ct, "_RemainderColsList")
        else:
            self._ct._RemainderColsList = self._original  # type: ignore[attr-defined]

    def test_attribute_absent_before_patch(self):
        self.assertFalse(hasattr(self._ct, "_RemainderColsList"))

    def test_patch_registers_attribute(self):
        _patch_sklearn_remainder_compat()
        self.assertTrue(hasattr(self._ct, "_RemainderColsList"))

    def test_registered_value_is_list_subclass(self):
        """Must be a subclass of list (not bare list) so __dict__ exists for pickle BUILD."""
        _patch_sklearn_remainder_compat()
        cls = self._ct._RemainderColsList  # type: ignore[attr-defined]
        self.assertTrue(issubclass(cls, list))
        # Must NOT be bare list — instances need __dict__ for pickle state restore.
        self.assertIsNot(cls, list)

    def test_registered_value_is_usable_as_list(self):
        """Pickle machinery calls _RemainderColsList() to reconstruct instances."""
        _patch_sklearn_remainder_compat()
        instance = self._ct._RemainderColsList([1, 2, 3])  # type: ignore[attr-defined]
        self.assertIsInstance(instance, list)
        self.assertEqual(instance, [1, 2, 3])

    def test_instance_has_dict_for_pickle_build(self):
        """BUILD step sets __dict__; must not raise 'list has no __dict__'."""
        _patch_sklearn_remainder_compat()
        instance = self._ct._RemainderColsList()  # type: ignore[attr-defined]
        # Simulate what pickle BUILD does: update __dict__
        instance.__dict__.update(
            {"data": [], "future_dtype": "str", "warning_was_emitted": False, "warning_enabled": True}
        )
        self.assertEqual(instance.data, [])  # type: ignore[attr-defined]
        self.assertEqual(instance.future_dtype, "str")  # type: ignore[attr-defined]


class TestPatchSklearnRemainderCompatPresent(unittest.TestCase):
    """Shim is idempotent when _RemainderColsList already exists."""

    def setUp(self):
        import sklearn.compose._column_transformer as ct
        self._ct = ct
        # Record whether it exists and its value so we can restore.
        self._had_attr = hasattr(ct, "_RemainderColsList")
        if self._had_attr:
            self._original_value = ct._RemainderColsList  # type: ignore[attr-defined]
        else:
            # Install a sentinel non-list class to verify it is NOT overwritten.
            class _Sentinel(list):
                pass
            ct._RemainderColsList = _Sentinel  # type: ignore[attr-defined]
            self._original_value = _Sentinel

    def tearDown(self):
        if self._had_attr:
            self._ct._RemainderColsList = self._original_value  # type: ignore[attr-defined]
        else:
            if hasattr(self._ct, "_RemainderColsList"):
                delattr(self._ct, "_RemainderColsList")

    def test_existing_attribute_not_overwritten(self):
        existing = self._ct._RemainderColsList  # type: ignore[attr-defined]
        _patch_sklearn_remainder_compat()
        self.assertIs(self._ct._RemainderColsList, existing)  # type: ignore[attr-defined]

    def test_idempotent_on_repeated_calls(self):
        _patch_sklearn_remainder_compat()
        _patch_sklearn_remainder_compat()
        _patch_sklearn_remainder_compat()
        # Should still be whatever was registered after the first call.
        self.assertTrue(hasattr(self._ct, "_RemainderColsList"))


if __name__ == "__main__":
    unittest.main()
