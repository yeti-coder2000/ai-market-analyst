from __future__ import annotations

"""Focused, read-only validation of the Master Research v3 contract set."""

import argparse
import json
from pathlib import Path

from app.services.research.v3.contracts import (
    load_json,
    validate_contract_directory,
)
from app.services.research.v3.schemas import (
    to_pyarrow_schema,
    validate_schema_registry,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--contract-root",
        type=Path,
        default=(
            Path(__file__).resolve().parents[1]
            / "research"
            / "contracts"
            / "v3"
        ),
    )
    parser.add_argument("--require-pyarrow", action="store_true")
    args = parser.parse_args()

    contracts = validate_contract_directory(args.contract_root)
    schema_contract = load_json(args.contract_root / "SCHEMA_REGISTRY_V3.json.gz")
    validate_schema_registry(schema_contract)
    arrow_count = 0
    if args.require_pyarrow:
        for declaration in schema_contract["payload"]["datasets"]:
            to_pyarrow_schema(declaration)
            arrow_count += 1
    summary = {
        "state": "PASS",
        "contract_count": len(contracts),
        "schema_count": len(schema_contract["payload"]["datasets"]),
        "arrow_schema_count": arrow_count,
        "master_contract_hash": contracts["MASTER_PROTOCOL_V3"][
            "contract_hash"
        ],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
