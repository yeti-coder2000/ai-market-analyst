from __future__ import annotations

"""Canonical, deterministic validation for Master Research v3 contracts."""

import gzip
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

CANONICAL_SYMBOLS: tuple[str, ...] = (
    "XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD",
    "BTCUSD", "ETHUSD", "GER40", "NAS100", "SPX500", "UKOIL",
)

_HASH_FIELDS = frozenset({"contract_hash", "set_hash", "approval_hash", "manifest_hash"})


class ContractValidationError(ValueError):
    """Raised when a v3 contract or dependency graph violates its freeze rules."""


def _canonicalize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _canonicalize(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ContractValidationError("non-finite floats are forbidden")
        return 0.0 if value == 0.0 else value
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise ContractValidationError(
        f"unsupported canonical JSON value: {type(value).__name__}"
    )


def canonical_json_bytes(value: Any) -> bytes:
    """Return UTF-8 canonical JSON with stable key order and one final newline."""
    normalized = _canonicalize(value)
    return (
        json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def compute_contract_hash(document: Mapping[str, Any]) -> str:
    """Hash a contract while excluding only its declared top-level hash field."""
    payload = {
        str(key): value
        for key, value in document.items()
        if str(key) not in _HASH_FIELDS
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    try:
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8") as stream:
                value = json.load(stream)
        else:
            value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractValidationError(f"cannot load {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ContractValidationError(f"{path} must contain a JSON object")
    return value


def validate_exact_symbols(symbols: Sequence[str]) -> None:
    if tuple(symbols) != CANONICAL_SYMBOLS:
        raise ContractValidationError(
            "canonical symbol sequence must contain the exact ordered 13-asset universe"
        )


def validate_contract(document: Mapping[str, Any]) -> None:
    required = {
        "contract_id", "version", "status", "immutable_evidence_code_sha",
        "dependencies", "source_documents", "payload", "contract_hash",
    }
    missing = sorted(required.difference(document))
    if missing:
        raise ContractValidationError(f"missing contract fields: {missing}")
    if document["version"] != "3.0.0":
        raise ContractValidationError("phase-1 contracts must use version 3.0.0")
    contract_hash = str(document["contract_hash"])
    if len(contract_hash) != 64 or contract_hash != compute_contract_hash(document):
        raise ContractValidationError(
            f"contract hash mismatch for {document.get('contract_id')}"
        )
    dependencies = document["dependencies"]
    if not isinstance(dependencies, list):
        raise ContractValidationError("dependencies must be a list")
    dependency_ids = [str(item.get("contract_id")) for item in dependencies]
    if dependency_ids != sorted(dependency_ids):
        raise ContractValidationError("dependencies must be sorted by contract_id")
    if len(dependency_ids) != len(set(dependency_ids)):
        raise ContractValidationError("dependencies must be unique")
    for item in dependencies:
        if set(item) != {"contract_id", "contract_hash"}:
            raise ContractValidationError("dependency entries require id and hash only")
        if len(str(item["contract_hash"])) != 64:
            raise ContractValidationError("dependency hash must be SHA-256 hex")


def validate_dependency_graph(contracts: Mapping[str, Mapping[str, Any]]) -> None:
    """Validate dependency existence/hash identity and reject cycles."""
    for contract_id, document in contracts.items():
        if contract_id != document.get("contract_id"):
            raise ContractValidationError("contract mapping key/id mismatch")
        validate_contract(document)
    for contract_id, document in contracts.items():
        for dependency in document["dependencies"]:
            dependency_id = dependency["contract_id"]
            if dependency_id not in contracts:
                raise ContractValidationError(
                    f"{contract_id} references missing dependency {dependency_id}"
                )

    state: dict[str, int] = {}

    def visit(node: str, stack: list[str]) -> None:
        marker = state.get(node, 0)
        if marker == 1:
            cycle = " -> ".join([*stack, node])
            raise ContractValidationError(f"contract dependency cycle: {cycle}")
        if marker == 2:
            return
        state[node] = 1
        for dependency in contracts[node]["dependencies"]:
            visit(str(dependency["contract_id"]), [*stack, node])
        state[node] = 2

    for node in sorted(contracts):
        visit(node, [])

    # Check pinned dependency identities only after cycle detection. This keeps
    # topology errors diagnosable even when a malformed graph also has stale
    # dependency hashes.
    for contract_id, document in contracts.items():
        for dependency in document["dependencies"]:
            dependency_id = dependency["contract_id"]
            if dependency["contract_hash"] != contracts[dependency_id]["contract_hash"]:
                raise ContractValidationError(
                    f"{contract_id} dependency hash mismatch for {dependency_id}"
                )


def validate_contract_directory(root: Path) -> dict[str, dict[str, Any]]:
    """Load and validate the canonical phase-1 contract directory."""
    if not root.is_dir():
        raise ContractValidationError(f"contract root does not exist: {root}")
    documents: dict[str, dict[str, Any]] = {}
    paths = sorted([*root.glob("*.json"), *root.glob("*.json.gz")])
    for path in paths:
        document = load_json(path)
        contract_id = document.get("contract_id")
        if contract_id is None:
            continue  # approval, workspace, and set manifests use separate envelopes
        if contract_id in {"WORKSPACE_SAFETY_V3", "DEPENDENCY_DAG_V3"}:
            continue
        contract_id = str(contract_id)
        if contract_id in documents:
            raise ContractValidationError(f"duplicate contract ID: {contract_id}")
        documents[contract_id] = document
    validate_dependency_graph(documents)
    master = documents.get("MASTER_PROTOCOL_V3")
    if master is None:
        raise ContractValidationError("MASTER_PROTOCOL_V3 is required")
    validate_exact_symbols(master["payload"]["canonical_symbols"])
    return documents
