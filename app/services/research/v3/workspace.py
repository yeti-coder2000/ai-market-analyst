from __future__ import annotations

"""Fail-closed workspace-safety primitives for the v3 research worktree."""

import hashlib
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Iterable


class WorkspaceSafetyError(RuntimeError):
    """Raised when a filesystem or repository safety invariant is violated."""


def resolved_path(path: Path) -> Path:
    return Path(os.path.realpath(os.path.abspath(os.fspath(path))))


def path_is_within(candidate: Path, parent: Path) -> bool:
    child = resolved_path(candidate)
    root = resolved_path(parent)
    return child == root or root in child.parents


def assert_safe_output_root(output_root: Path, protected_paths: Iterable[Path]) -> Path:
    """Reject an output root equal to, inside, or containing protected paths."""
    output = resolved_path(output_root)
    for raw in protected_paths:
        protected = resolved_path(raw)
        if (
            output == protected
            or path_is_within(output, protected)
            or path_is_within(protected, output)
        ):
            raise WorkspaceSafetyError(
                "unsafe output/protected path overlap: "
                f"output={output} protected={protected}"
            )
    return output


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def deterministic_tree_hash(root: Path) -> dict[str, Any]:
    """Content-hash a tree without following symlinks or Git/runtime internals."""
    root = resolved_path(root)
    if not root.exists():
        raise WorkspaceSafetyError(f"protected path does not exist: {root}")
    if root.is_file():
        return {"root": str(root), "entry_count": 1, "sha256": sha256_file(root)}

    ignored = {".git", ".venv", "__pycache__", ".pytest_cache", ".mypy_cache"}
    entries: list[tuple[str, str, int]] = []
    for current, dirnames, filenames in os.walk(
        root, topdown=True, followlinks=False
    ):
        current_path = Path(current)
        kept_dirs: list[str] = []
        for name in sorted(dirnames):
            path = current_path / name
            if name in ignored:
                continue
            rel = path.relative_to(root).as_posix()
            if path.is_symlink():
                target = os.readlink(path)
                digest = hashlib.sha256(
                    ("SYMLINK\0" + target).encode("utf-8")
                ).hexdigest()
                entries.append((rel, digest, 0))
            else:
                kept_dirs.append(name)
        dirnames[:] = kept_dirs
        for name in sorted(filenames):
            if name in ignored:
                continue
            path = current_path / name
            rel = path.relative_to(root).as_posix()
            if path.is_symlink():
                target = os.readlink(path)
                digest = hashlib.sha256(
                    ("SYMLINK\0" + target).encode("utf-8")
                ).hexdigest()
                size = 0
            elif path.is_file():
                digest = sha256_file(path)
                size = path.stat().st_size
            else:
                continue
            entries.append((rel, digest, size))

    aggregate = hashlib.sha256()
    for rel, digest, size in sorted(entries):
        aggregate.update(rel.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(digest.encode("ascii"))
        aggregate.update(b"\0")
        aggregate.update(str(size).encode("ascii"))
        aggregate.update(b"\n")
    return {
        "root": str(root),
        "entry_count": len(entries),
        "sha256": aggregate.hexdigest(),
    }


def exact_write_probe(
    output_root: Path, protected_paths: Iterable[Path]
) -> dict[str, Any]:
    """Create, verify, and delete exactly one probe inside the safe output root."""
    output = assert_safe_output_root(output_root, protected_paths)
    output.mkdir(parents=True, exist_ok=True)
    payload = b"MASTER_RESEARCH_V3_WORKSPACE_PROBE\n"
    probe_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=".workspace_probe_v3_",
            suffix=".tmp",
            dir=output,
            delete=False,
        ) as stream:
            probe_path = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if probe_path.read_bytes() != payload:
            raise WorkspaceSafetyError("workspace write probe round-trip mismatch")
        return {
            "root": str(output),
            "probe": probe_path.name,
            "bytes": len(payload),
            "passed": True,
        }
    finally:
        if probe_path is not None and probe_path.exists():
            probe_path.unlink()


def _run_git(
    repo: Path, *args: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", "-C", os.fspath(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise WorkspaceSafetyError(
            f"git {' '.join(args)} failed in {repo}: {result.stderr.strip()}"
        )
    return result


def git_snapshot(
    repo: Path, *, expected_branch: str, approved_base_sha: str
) -> dict[str, Any]:
    repo = resolved_path(repo)
    top = resolved_path(
        Path(_run_git(repo, "rev-parse", "--show-toplevel").stdout.strip())
    )
    if top != repo:
        raise WorkspaceSafetyError(
            f"repository path mismatch: requested={repo} top={top}"
        )
    head = _run_git(repo, "rev-parse", "HEAD").stdout.strip()
    branch = _run_git(repo, "branch", "--show-current").stdout.strip()
    status = _run_git(repo, "status", "--short").stdout.splitlines()
    ancestor = (
        _run_git(
            repo,
            "merge-base",
            "--is-ancestor",
            approved_base_sha,
            head,
            check=False,
        ).returncode
        == 0
    )
    if branch != expected_branch:
        raise WorkspaceSafetyError(f"unexpected research branch: {branch!r}")
    if not ancestor:
        raise WorkspaceSafetyError(
            "approved base SHA is not an ancestor of worktree HEAD"
        )
    return {
        "repository_path": str(repo),
        "head_sha": head,
        "branch": branch,
        "status_short": status,
        "clean": not status,
        "approved_base_is_ancestor": ancestor,
    }
