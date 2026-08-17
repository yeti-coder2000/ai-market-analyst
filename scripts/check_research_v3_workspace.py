from __future__ import annotations

"""Explicit-probe G1 checker for the local Master Research v3 workspace."""

import argparse
import importlib.metadata
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from app.services.research.v3.contracts import (
    compute_contract_hash,
    load_json,
)
from app.services.research.v3.workspace import (
    WorkspaceSafetyError,
    assert_safe_output_root,
    deterministic_tree_hash,
    exact_write_probe,
    git_snapshot,
    resolved_path,
    sha256_file,
)


def _version(package: str) -> str | None:
    try:
        return importlib.metadata.version(package)
    except importlib.metadata.PackageNotFoundError:
        return None


def _plain_git_snapshot(repo: Path) -> dict[str, Any]:
    def run(*args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    return {
        "repository_path": str(resolved_path(repo)),
        "head_sha": run("rev-parse", "HEAD"),
        "branch": run("branch", "--show-current"),
        "status_short": run("status", "--short").splitlines(),
    }


def main() -> int:
    default_root = (
        Path(__file__).resolve().parents[1]
        / "research"
        / "contracts"
        / "v3"
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-root", type=Path, default=default_root)
    parser.add_argument("--evidence-root", type=Path)
    parser.add_argument("--allow-write-probe", action="store_true")
    parser.add_argument("--hash-protected", action="store_true")
    parser.add_argument("--minimum-free-gib", type=float, default=0.0)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    workspace = load_json(
        args.contract_root / "WORKSPACE_CONTRACT_V3.json"
    )
    if workspace.get("contract_hash") != compute_contract_hash(workspace):
        raise WorkspaceSafetyError("workspace contract hash mismatch")
    evidence = load_json(
        args.contract_root / "IMMUTABLE_EVIDENCE_MANIFEST_V3.json"
    )
    expected_manifest_hash = workspace["immutable_evidence_manifest"][
        "manifest_hash"
    ]
    if evidence.get("manifest_hash") != expected_manifest_hash:
        raise WorkspaceSafetyError("immutable evidence manifest pin mismatch")

    paths = {
        key: Path(value)
        for key, value in workspace["expected_local_paths"].items()
    }
    protected = [
        paths["production_checkout"],
        paths["validation_worktree"],
        paths["frozen_universe_v2"],
        paths["native_m5_cache"],
    ]
    output = assert_safe_output_root(paths["v3_output_root"], protected)

    checks: list[dict[str, Any]] = []
    for key in (
        "production_checkout",
        "validation_worktree",
        "v3_worktree",
        "canonical_root",
        "native_m5_cache",
        "frozen_universe_v2",
    ):
        checks.append(
            {
                "check": f"PATH_EXISTS:{key}",
                "passed": paths[key].exists(),
                "path": str(paths[key]),
            }
        )

    repo_contract = workspace["repository"]
    worktree_snapshot = git_snapshot(
        paths["v3_worktree"],
        expected_branch=repo_contract["research_branch"],
        approved_base_sha=repo_contract["approved_base_sha"],
    )
    try:
        production_snapshot = _plain_git_snapshot(paths["production_checkout"])
    except (OSError, subprocess.CalledProcessError) as exc:
        raise WorkspaceSafetyError(
            f"cannot inspect production checkout: {exc}"
        ) from exc
    checks.append(
        {
            "check": "V3_WORKTREE_CLEAN",
            "passed": worktree_snapshot["clean"],
        }
    )

    runtime_versions = {
        "python": sys.version.split()[0],
        "numpy": _version("numpy"),
        "pandas": _version("pandas"),
        "pyarrow": _version("pyarrow"),
        "pydantic": _version("pydantic"),
    }
    for package in ("numpy", "pandas", "pyarrow"):
        checks.append(
            {
                "check": f"RUNTIME_PRESENT:{package}",
                "passed": runtime_versions[package] is not None,
            }
        )

    disk_basis = (
        output.parent if output.parent.exists() else paths["canonical_root"]
    )
    disk = shutil.disk_usage(disk_basis)
    free_gib = disk.free / (1024**3)
    checks.append(
        {
            "check": "FREE_DISK",
            "passed": free_gib >= args.minimum_free_gib,
            "free_gib": free_gib,
            "minimum_free_gib": args.minimum_free_gib,
        }
    )

    evidence_results: list[dict[str, Any]] = []
    for item in evidence["required_local_files"]:
        candidate = (
            args.evidence_root / item["filename"]
            if args.evidence_root
            else None
        )
        passed = bool(
            candidate
            and candidate.is_file()
            and sha256_file(candidate) == item["sha256"]
        )
        evidence_results.append(
            {
                "filename": item["filename"],
                "path": str(candidate) if candidate else None,
                "expected_sha256": item["sha256"],
                "passed": passed,
            }
        )
        checks.append(
            {
                "check": f"IMMUTABLE_EVIDENCE:{item['filename']}",
                "passed": passed,
            }
        )

    protected_hashes: list[dict[str, Any]] = []
    if args.hash_protected:
        for path in protected:
            protected_hashes.append(deterministic_tree_hash(path))
        for item in evidence_results:
            if item["passed"]:
                protected_hashes.append(
                    deterministic_tree_hash(Path(item["path"]))
                )
    checks.append(
        {
            "check": "PROTECTED_TREE_HASHES",
            "passed": bool(args.hash_protected),
            "count": len(protected_hashes),
        }
    )

    probe = None
    if args.allow_write_probe:
        probe = exact_write_probe(output, protected)
    checks.append(
        {"check": "WRITE_PROBE", "passed": probe is not None}
    )

    passed = all(bool(item["passed"]) for item in checks)
    report = {
        "gate_id": "G1_WORKSPACE_SAFE",
        "state": "PASS" if passed else "NOT_YET_PASS",
        "workspace_contract_hash": workspace["contract_hash"],
        "immutable_evidence_manifest_hash": evidence["manifest_hash"],
        "target_output_root": str(output),
        "worktree": worktree_snapshot,
        "production_checkout": production_snapshot,
        "runtime_versions": runtime_versions,
        "checks": checks,
        "evidence": evidence_results,
        "protected_tree_hashes": protected_hashes,
        "write_probe": probe,
    }
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        report_path = resolved_path(args.report)
        if not (
            report_path == output or output in report_path.parents
        ):
            raise WorkspaceSafetyError(
                "G1 report path must be inside v3 output root"
            )
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0 if passed else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except WorkspaceSafetyError as exc:
        print(
            json.dumps(
                {
                    "gate_id": "G1_WORKSPACE_SAFE",
                    "state": "FAILED",
                    "error": str(exc),
                },
                indent=2,
            ),
            file=sys.stderr,
        )
        raise SystemExit(2)
