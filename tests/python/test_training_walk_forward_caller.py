from __future__ import annotations

import ast
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TRAINER_PATH = REPO_ROOT / "scripts" / "train_rf_artifacts.py"


class ProductionWalkForwardCallerTests(unittest.TestCase):
    def test_training_caller_passes_current_label_horizon(self) -> None:
        tree = ast.parse(TRAINER_PATH.read_text(encoding="utf-8"))
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "run_walk_forward_oos"
        ]
        self.assertEqual(len(calls), 1)
        keyword = next(
            (kw for kw in calls[0].keywords if kw.arg == "label_horizon_min"),
            None,
        )
        self.assertIsNotNone(keyword)
        assert keyword is not None
        self.assertEqual(ast.unparse(keyword.value), "int(horizon)")


if __name__ == "__main__":
    unittest.main()
