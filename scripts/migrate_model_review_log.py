from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from server.model_review_store import migrate_jsonl_to_sqlite, verify_hash_chain


def _default_review_log_path() -> Path:
    return ROOT / "data" / "model_lab" / "review_log.jsonl"


def _default_review_store_path() -> Path:
    return ROOT / "data" / "model_lab" / "review_store.sqlite"


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate legacy model review JSONL history into the SQLite review store.")
    parser.add_argument("--log-path", default=str(_default_review_log_path()))
    parser.add_argument("--store-path", default=str(_default_review_store_path()))
    args = parser.parse_args()

    log_path = Path(args.log_path).expanduser()
    store_path = Path(args.store_path).expanduser()

    result = migrate_jsonl_to_sqlite(log_path, store_path)
    verification = verify_hash_chain(store_path)
    print(
        json.dumps(
            {
                "migration": result,
                "verification": verification,
                "log_path": str(log_path),
                "store_path": str(store_path),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
