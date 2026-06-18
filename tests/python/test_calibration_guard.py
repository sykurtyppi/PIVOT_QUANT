"""Guard tests for ml.calibration.ProbabilityCalibrator against a single-class
base model.

A single-class base model returns a 1-column predict_proba; the old code did
``predict_proba(...)[:, 1]`` unconditionally, raising an opaque IndexError
mid-training (and, with no try/except around the train loop, aborting the whole
run). The calibrator must instead raise a clear, diagnosable ValueError so the
caller can skip the degenerate horizon.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ml.calibration import ProbabilityCalibrator  # noqa: E402


class _SingleColModel:
    """Mimics a single-class fitted estimator: predict_proba has ONE column."""

    def predict_proba(self, X):
        n = np.asarray(X).shape[0]
        return np.full((n, 1), 0.5)


class _BinaryModel:
    """Proper binary estimator: predict_proba has two columns summing to 1."""

    def predict_proba(self, X):
        x = np.asarray(X, dtype=float).reshape(-1)
        p = 1.0 / (1.0 + np.exp(-x))
        return np.column_stack([1.0 - p, p])


class TestCalibrationSingleClassGuard(unittest.TestCase):
    def _xy(self, n=60):
        X = np.linspace(-2.0, 2.0, n).reshape(-1, 1)
        y = (X.reshape(-1) > 0).astype(int)
        return X, y

    def test_fit_single_class_base_raises_clear_valueerror_not_indexerror(self):
        X, y = self._xy(40)
        with self.assertRaises(ValueError) as ctx:
            ProbabilityCalibrator(_SingleColModel(), "isotonic").fit(X, y)
        # The message must be diagnosable (names the binary-base requirement),
        # and it must be a ValueError — NOT the opaque IndexError the old code
        # produced. assertRaises(ValueError) already excludes IndexError.
        self.assertIn("binary base model", str(ctx.exception))

    def test_fit_single_class_base_sigmoid_also_guards(self):
        X, y = self._xy(40)
        with self.assertRaises(ValueError):
            ProbabilityCalibrator(_SingleColModel(), "sigmoid").fit(X, y)

    def test_binary_base_fits_and_calibrates_normally(self):
        X, y = self._xy(60)
        cal = ProbabilityCalibrator(_BinaryModel(), "isotonic").fit(X, y)
        proba = cal.predict_proba(X)
        self.assertEqual(proba.shape, (60, 2))
        np.testing.assert_allclose(proba.sum(axis=1), 1.0, atol=1e-9)

    def test_predict_proba_guards_if_base_degrades_to_single_col(self):
        X, y = self._xy(40)
        cal = ProbabilityCalibrator(_BinaryModel(), "isotonic").fit(X, y)
        cal.base_model = _SingleColModel()  # base now returns one column
        with self.assertRaises(ValueError):
            cal.predict_proba(X)


if __name__ == "__main__":
    unittest.main()
