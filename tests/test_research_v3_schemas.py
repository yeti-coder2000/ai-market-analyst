from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path

from app.services.research.v3.contracts import load_json
from app.services.research.v3.schemas import (
    REQUIRED_DATASETS,
    SchemaValidationError,
    schema_fingerprint,
    to_pyarrow_schema,
    validate_schema_declaration,
    validate_schema_registry,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_ROOT = ROOT / "research" / "contracts" / "v3"


class ResearchV3SchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = load_json(CONTRACT_ROOT / "SCHEMA_REGISTRY_V3.json.gz")

    def test_full_registry_validates(self) -> None:
        validate_schema_registry(self.registry)
        names = {row["schema_name"] for row in self.registry["payload"]["datasets"]}
        self.assertEqual(names, set(REQUIRED_DATASETS))

    def test_schema_fingerprint_mutation_is_detected(self) -> None:
        declaration = copy.deepcopy(self.registry["payload"]["datasets"][0])
        validate_schema_declaration(declaration)
        declaration["fields"][0]["semantics"] += " mutated"
        self.assertNotEqual(declaration["schema_fingerprint"], schema_fingerprint(declaration))
        with self.assertRaises(SchemaValidationError):
            validate_schema_declaration(declaration)

    def test_primary_and_sort_keys_reference_real_fields(self) -> None:
        for declaration in self.registry["payload"]["datasets"]:
            validate_schema_declaration(declaration)

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "pyarrow not installed")
    def test_arrow_materialization_is_deterministic(self) -> None:
        declaration = next(
            row for row in self.registry["payload"]["datasets"]
            if row["schema_name"] == "market_bar_v3"
        )
        first = to_pyarrow_schema(declaration)
        second = to_pyarrow_schema(declaration)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
