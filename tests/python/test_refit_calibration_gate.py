"""Gate tests for refit_calibration's decision-threshold preservation.

The calibration refit may update calibration mappings on the LIVE active manifest,
but it must NOT change decision thresholds unless --retune-thresholds is explicitly
set. Otherwise recalibration could silently overwrite the authoritative
manifest-first threshold with the artifact's value — resurrecting a threshold that
a runtime guard or governance had neutralized (e.g. the no-signal sentinel) on the
live-served model.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import refit_calibration as rc  # noqa: E402

SENTINEL = float(np.nextafter(1.0, 2.0))


class TestRefitThresholdGate(unittest.TestCase):
    def _manifest(self, thr):
        return {"thresholds": {"reject": {"15": thr}}}

    def test_retune_off_preserves_manifest_threshold(self):
        # artifact disagrees (0.83) but retune is OFF -> keep the manifest's 0.62
        v = rc.resolve_manifest_threshold_to_write(
            self._manifest(0.62), "reject", 15,
            artifact_threshold=0.83, retune_thresholds=False,
        )
        self.assertEqual(v, 0.62)

    def test_retune_off_preserves_sentinel_no_resurrection(self):
        # manifest threshold neutralized to the no-signal sentinel; artifact still
        # carries a tradeable threshold. retune OFF must keep the sentinel so the
        # disabled horizon is NOT resurrected on the live model.
        v = rc.resolve_manifest_threshold_to_write(
            self._manifest(SENTINEL), "reject", 15,
            artifact_threshold=0.70, retune_thresholds=False,
        )
        self.assertEqual(v, SENTINEL)

    def test_retune_on_uses_new_threshold(self):
        # explicit opt-in: the (re)selected threshold is written
        v = rc.resolve_manifest_threshold_to_write(
            self._manifest(0.62), "reject", 15,
            artifact_threshold=0.83, retune_thresholds=True,
        )
        self.assertEqual(v, 0.83)

    def test_new_pair_falls_back_to_artifact(self):
        # a pair the manifest does not yet carry -> use the artifact threshold
        v = rc.resolve_manifest_threshold_to_write(
            {"thresholds": {}}, "reject", 15,
            artifact_threshold=0.55, retune_thresholds=False,
        )
        self.assertEqual(v, 0.55)


if __name__ == "__main__":
    unittest.main()
