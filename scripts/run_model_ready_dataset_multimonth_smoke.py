#!/usr/bin/env python3
"""Run the bounded multi-month model-ready dataset export smoke."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_model_ready_dataset_smoke import main as dataset_smoke_main


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.argv.extend(
            [
                "--symbol",
                "SPY",
                "--analysis-start-date",
                "2024-01-02",
                "--analysis-end-date",
                "2024-03-29",
                "--feature-lookback-days",
                "120",
                "--label-lookahead-days",
                "45",
                "--max-files",
                "20",
                "--max-days",
                "280",
                "--daily-source",
                "yahoo",
                "--horizons",
                "1d,5d,21d",
            ]
        )
    raise SystemExit(dataset_smoke_main())
