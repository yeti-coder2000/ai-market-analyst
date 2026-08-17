from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from app.services.research.v3.contracts import (
    CANONICAL_SYMBOLS,
    ContractValidationError,
    canonical_json_bytes,
    compute_contract_hash,
    load_json,
    validate_contract,
    validate_contract_directory,
    validate_dependency_graph,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_ROOT = ROOT / "research" / "contracts" / "v3"


class ResearchV3ContractTests(unittest.TestCase):
    def test_canonical_json_is_key_order_and_unicode_stable(self) -> None:
        left = {"β": [2, 1], "a": {"z": True, "x": 0.0}}
        right = {"a": {"x": -0.0, "z": True}, "β": [2, 1]}
        self.assertEqual(canonical_json_bytes(left), canonical_json_bytes(right))

    def test_contract_hash_mutation_is_detected(self) -> None:
        contract = load_json(CONTRACT_ROOT / "BT1_CONTRACT_V3.json")
        validate_contract(contract)
        mutated = copy.deepcopy(contract)
        mutated["payload"]["primary_gate_location"] = "READY_ONLY"
        self.assertNotEqual(mutated["contract_hash"], compute_contract_hash(mutated))
        with self.assertRaises(ContractValidationError):
            validate_contract(mutated)

    def test_contract_directory_and_dependency_graph_validate(self) -> None:
        contracts = validate_contract_directory(CONTRACT_ROOT)
        self.assertIn("MASTER_PROTOCOL_V3", contracts)
        self.assertEqual(
            tuple(contracts["MASTER_PROTOCOL_V3"]["payload"]["canonical_symbols"]),
            CANONICAL_SYMBOLS,
        )

    def test_dependency_cycle_is_rejected(self) -> None:
        contracts = validate_contract_directory(CONTRACT_ROOT)
        altered = copy.deepcopy(contracts)
        decision = altered["V3_DECISION_LOG_FREEZE"]
        master = altered["MASTER_PROTOCOL_V3"]
        decision["dependencies"] = [{
            "contract_id": "MASTER_PROTOCOL_V3",
            "contract_hash": master["contract_hash"],
        }]
        decision["contract_hash"] = compute_contract_hash(decision)
        # Cycle detection precedes dependency-hash identity checks so topology
        # defects are reported directly rather than hidden by stale pins.
        with self.assertRaisesRegex(ContractValidationError, "cycle"):
            validate_dependency_graph(altered)

    def test_approved_semantics_are_exact(self) -> None:
        bt1 = load_json(CONTRACT_ROOT / "BT1_CONTRACT_V3.json")["payload"]
        self.assertEqual(bt1["threshold_semantics"], "UPPER_PERMISSION_CAP_NOT_ENTRY_TRIGGER")
        self.assertEqual(bt1["primary_gate_location"], "READY_AND_FILL")
        self.assertEqual(bt1["pre_fill_bar_rule"], "BAR_CLOSE_UTC < FILL_AT_UTC")
        self.assertEqual(len(bt1["candidate_policies"]), 10)

        bt2 = load_json(CONTRACT_ROOT / "BT2_CONTRACT_V3.json")["payload"]
        self.assertEqual(bt2["actions"], {"ALLOW": 1.0, "DOWNWEIGHT": 0.5, "BLOCK": 0.0})
        self.assertFalse(bt2["missing_policy"]["no_data_may_cause_block"])
        self.assertEqual(bt2["no_incremental_value_output"], "BT2_KEEP_ALLOW_ALL")

        bt3 = load_json(CONTRACT_ROOT / "BT3_CONTRACT_V3.json")["payload"]
        self.assertEqual(len(bt3["selectable_policies"]), 7)
        self.assertEqual(bt3["baseline_policy"], "REAL_STRUCTURAL_TARGET_FULL")
        self.assertEqual(bt3["no_incremental_value_output"], "BT3_KEEP_REAL_TARGET_BASELINE")

    def test_session_defaults_are_not_misrepresented_as_audited(self) -> None:
        session = load_json(CONTRACT_ROOT / "SESSION_CONTRACT_V3.json")
        self.assertIn("PENDING_PARITY_AUDIT", session["status"])
        templates = {row["template_id"]: row for row in session["payload"]["approved_seed_templates"]}
        self.assertEqual(templates["FX_LONDON_EVENT_OPEN"]["open_local_time"], "08:00")
        self.assertEqual(templates["FX_TOKYO_EVENT_OPEN"]["open_local_time"], "09:00")
        self.assertEqual(templates["FX_NY_CASH_EVENT_OPEN"]["open_local_time"], "09:30")
        self.assertEqual(templates["UKOIL_ICE_LONDON_PRIMARY"]["resolution_status"], "AUDIT_REQUIRED_EXACT_ICE_SESSION_AND_SOURCE_COVERAGE")
        self.assertFalse(templates["UKOIL_NY_RISK_SECONDARY"]["official_exchange_open_claimed"])


    def test_evidence_workspace_and_set_hashes_are_pinned(self) -> None:
        evidence = load_json(
            CONTRACT_ROOT / "IMMUTABLE_EVIDENCE_MANIFEST_V3.json"
        )
        workspace = load_json(CONTRACT_ROOT / "WORKSPACE_CONTRACT_V3.json")
        contract_set = load_json(CONTRACT_ROOT / "CONTRACT_SET_V3.json")
        self.assertEqual(
            evidence["manifest_hash"], compute_contract_hash(evidence)
        )
        self.assertEqual(
            workspace["contract_hash"], compute_contract_hash(workspace)
        )
        self.assertEqual(
            contract_set["set_hash"], compute_contract_hash(contract_set)
        )
        self.assertEqual(
            workspace["immutable_evidence_manifest"]["manifest_hash"],
            evidence["manifest_hash"],
        )
        self.assertEqual(
            contract_set["workspace_contract_hash"],
            workspace["contract_hash"],
        )
        self.assertEqual(
            contract_set["immutable_evidence_manifest_hash"],
            evidence["manifest_hash"],
        )

    def test_phase_file_manifest_matches_tree(self) -> None:
        manifest_path = CONTRACT_ROOT / "PHASE1_FILE_MANIFEST_V3.json"
        manifest = load_json(manifest_path)
        expected = {row["path"]: row for row in manifest["files"]}
        actual = {}
        for path in sorted(ROOT.rglob("*")):
            if (
                not path.is_file()
                or "__pycache__" in path.parts
                or path == manifest_path
            ):
                continue
            rel = path.relative_to(ROOT).as_posix()
            actual[rel] = {
                "path": rel,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "bytes": path.stat().st_size,
            }
        self.assertEqual(expected, actual)

    def test_provisional_cost_blocks_production(self) -> None:
        cost = load_json(CONTRACT_ROOT / "COST_CONTRACT_V3.json")["payload"]
        self.assertFalse(cost["production_permitted_with_provisional_or_unavailable"])
        self.assertTrue(cost["historical_research_permitted_with_provisional"])
        self.assertFalse(cost["historical_support_permitted_with_unavailable"])


if __name__ == "__main__":
    unittest.main()
