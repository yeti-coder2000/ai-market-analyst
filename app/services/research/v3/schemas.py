from __future__ import annotations

"""Logical Arrow-schema declarations and deterministic fingerprints for v3."""

import hashlib
from collections.abc import Mapping, Sequence
from typing import Any

from .contracts import ContractValidationError, canonical_json_bytes

REQUIRED_DATASETS: frozenset[str] = frozenset({
    "symbol_registry_v3", "source_partition_v3", "market_bar_v3",
    "market_bar_quality_issue_v3", "session_contract_v3", "session_instance_v3",
    "reference_profile_v3", "event_v3", "event_outcome_v3",
    "event_threshold_reach_v3", "baseline_entry_candidate_v3",
    "entry_path_threshold_v3", "context_snapshot_v3", "context_feature_v3",
    "bt2_permission_v3", "exit_path_level_v3", "exit_policy_outcome_v3",
    "cost_model_v3", "cost_application_v3", "fold_assignment_v3",
    "stage_decision_v3", "asset_registry_v3", "audit_run_v3", "audit_check_v3",
})

LOGICAL_TYPES: frozenset[str] = frozenset({
    "string", "enum", "timestamp", "date", "int32", "int64", "float64",
    "bool", "decimal", "array[string]", "object",
})


class SchemaValidationError(ContractValidationError):
    """Raised when a logical v3 schema declaration is inconsistent."""


def schema_fingerprint(declaration: Mapping[str, Any]) -> str:
    payload = {
        str(key): value
        for key, value in declaration.items()
        if str(key) != "schema_fingerprint"
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def validate_schema_declaration(declaration: Mapping[str, Any]) -> None:
    required = {
        "schema_name", "schema_version", "primary_key", "sort_key", "partition",
        "fields", "causal_cutoff_semantics", "schema_fingerprint",
    }
    missing = sorted(required.difference(declaration))
    if missing:
        raise SchemaValidationError(f"schema fields missing: {missing}")
    if declaration["schema_version"] != "3.0.0":
        raise SchemaValidationError("schema version must be 3.0.0")
    fields = declaration["fields"]
    if not isinstance(fields, list) or not fields:
        raise SchemaValidationError("schema fields must be a non-empty list")
    names: list[str] = []
    for field in fields:
        if set(field) != {
            "name", "logical_type", "nullable", "requiredness_token", "semantics"
        }:
            raise SchemaValidationError("schema field envelope is not canonical")
        name = str(field["name"])
        if not name or name in names:
            raise SchemaValidationError(f"duplicate/empty field: {name}")
        names.append(name)
        if field["logical_type"] not in LOGICAL_TYPES:
            raise SchemaValidationError(
                f"unknown logical type {field['logical_type']} in {declaration['schema_name']}"
            )
        if not isinstance(field["nullable"], bool):
            raise SchemaValidationError("nullable must be boolean")
        if not str(field["semantics"]).strip():
            raise SchemaValidationError("field semantics must be explicit")
    for key_name in ("primary_key", "sort_key"):
        values = declaration[key_name]
        if not isinstance(values, list) or not values:
            raise SchemaValidationError(f"{key_name} must be non-empty")
        unknown = sorted(set(values).difference(names))
        if unknown:
            raise SchemaValidationError(
                f"{declaration['schema_name']} {key_name} references unknown fields: {unknown}"
            )
    unique_key = declaration.get("unique_key", [])
    unknown_unique = sorted(set(unique_key).difference(names))
    if unknown_unique:
        raise SchemaValidationError(
            f"{declaration['schema_name']} unique_key references unknown fields: {unknown_unique}"
        )
    if schema_fingerprint(declaration) != declaration["schema_fingerprint"]:
        raise SchemaValidationError(
            f"schema fingerprint mismatch: {declaration['schema_name']}"
        )


def validate_schema_registry(registry_contract: Mapping[str, Any]) -> None:
    datasets = registry_contract["payload"]["datasets"]
    by_name: dict[str, Mapping[str, Any]] = {}
    for declaration in datasets:
        validate_schema_declaration(declaration)
        name = str(declaration["schema_name"])
        if name in by_name:
            raise SchemaValidationError(f"duplicate schema: {name}")
        by_name[name] = declaration
    missing = sorted(REQUIRED_DATASETS.difference(by_name))
    extra = sorted(set(by_name).difference(REQUIRED_DATASETS))
    if missing or extra:
        raise SchemaValidationError(
            f"schema registry mismatch; missing={missing}, extra={extra}"
        )
    asset_registry = by_name["asset_registry_v3"]
    if asset_registry["primary_key"] != ["symbol"]:
        raise SchemaValidationError("asset_registry_v3 primary key must be symbol")


def to_pyarrow_schema(declaration: Mapping[str, Any]) -> Any:
    """Build a PyArrow schema lazily inside the research environment.

    Arbitrary JSON objects/arrays are persisted as canonical JSON UTF-8 unless a
    future frozen schema explicitly normalizes them into child tables.
    """
    try:
        import pyarrow as pa
    except ImportError as exc:  # pragma: no cover - exercised in research env
        raise SchemaValidationError(
            "pyarrow is required to materialize an Arrow schema"
        ) from exc
    type_map = {
        "string": pa.string(), "enum": pa.string(),
        "timestamp": pa.timestamp("us", tz="UTC"), "date": pa.date32(),
        "int32": pa.int32(), "int64": pa.int64(), "float64": pa.float64(),
        "bool": pa.bool_(), "decimal": pa.decimal128(38, 12),
        "array[string]": pa.string(), "object": pa.string(),
    }
    return pa.schema([
        pa.field(
            str(field["name"]),
            type_map[str(field["logical_type"])],
            nullable=bool(field["nullable"]),
            metadata={b"logical_type": str(field["logical_type"]).encode("utf-8")},
        )
        for field in declaration["fields"]
    ])
