"""Research-only Master Causal Research Store v3 contracts and schemas.

This package is append-only research infrastructure. It must never be imported
by live signal, Battle Gate, or Telegram delivery paths.
"""

from .contracts import (
    CANONICAL_SYMBOLS,
    ContractValidationError,
    canonical_json_bytes,
    compute_contract_hash,
    validate_contract_directory,
    validate_dependency_graph,
)
from .schemas import SchemaValidationError, schema_fingerprint, validate_schema_registry
from .workspace import (
    WorkspaceSafetyError,
    assert_safe_output_root,
    deterministic_tree_hash,
    exact_write_probe,
    git_snapshot,
)

__all__ = [
    "CANONICAL_SYMBOLS",
    "ContractValidationError",
    "SchemaValidationError",
    "canonical_json_bytes",
    "compute_contract_hash",
    "schema_fingerprint",
    "validate_contract_directory",
    "validate_dependency_graph",
    "validate_schema_registry",
    "WorkspaceSafetyError",
    "assert_safe_output_root",
    "deterministic_tree_hash",
    "exact_write_probe",
    "git_snapshot",
]
