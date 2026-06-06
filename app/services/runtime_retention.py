from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.settings import settings


# =============================================================================
# RUNTIME RETENTION v1.0
# =============================================================================
# Purpose:
# - Keep Render persistent disk healthy.
# - Trim large NDJSON runtime files without external dependencies.
# - Rotate/truncate app.log when it becomes too large.
# - Remove old briefing/report artifacts.
# - Safe to run manually:
#
#     python -m app.services.runtime_retention
#
# Design:
# - Does not delete stats/signal_outcomes.json or quality/statistics files.
# - Does not touch tpo_latest.json or calendar cache.
# - Uses environment variables for limits.
# - Fails soft per-file: one bad file should not stop other cleanup tasks.
# =============================================================================


RUNTIME_RETENTION_VERSION = "runtime-retention-v1.0-safe-ndjson-trim"

RUNTIME_DIR = settings.runtime_dir
LOGS_DIR = settings.logs_dir
REPORTS_DIR = RUNTIME_DIR / "reports"
BRIEFINGS_DIR = REPORTS_DIR / "briefings"
TPO_REPORTS_DIR = REPORTS_DIR / "tpo"

RADAR_JOURNAL_PATH = RUNTIME_DIR / "radar_journal.ndjson"
RADAR_SNAPSHOT_PATH = RUNTIME_DIR / "radar_snapshot_v2.ndjson"
APP_LOG_PATH = LOGS_DIR / "app.log"

MB = 1024 * 1024

DEFAULT_JOURNAL_MAX_MB = 220
DEFAULT_JOURNAL_KEEP_LINES = 50_000
DEFAULT_SNAPSHOT_MAX_MB = 180
DEFAULT_SNAPSHOT_KEEP_LINES = 30_000
DEFAULT_APP_LOG_MAX_MB = 25
DEFAULT_REPORT_RETENTION_DAYS = 14
DEFAULT_MIN_FREE_MB = 150


@dataclass
class RetentionAction:
    name: str
    path: str
    status: str
    before_bytes: int | None = None
    after_bytes: int | None = None
    removed_files: int = 0
    error: str | None = None
    note: str | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def file_size(path: Path) -> int | None:
    try:
        return path.stat().st_size if path.exists() else None
    except OSError:
        return None


def disk_usage(path: Path) -> dict[str, Any]:
    target = path if path.exists() else path.parent
    usage = shutil.disk_usage(target)
    used_pct = round((usage.used / usage.total) * 100, 2) if usage.total else None
    return {
        "path": str(target),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "used_pct": used_pct,
        "free_mb": round(usage.free / MB, 2),
    }


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def tail_bytes_for_last_lines(path: Path, keep_lines: int, chunk_size: int = 1024 * 1024) -> bytes:
    """
    Read only enough data from the end of a file to preserve the last keep_lines.
    Returns bytes ending with the same final bytes as source file.
    """
    if keep_lines <= 0:
        return b""

    size = path.stat().st_size
    if size <= 0:
        return b""

    chunks: list[bytes] = []
    newline_count = 0

    with path.open("rb") as f:
        pos = size
        while pos > 0 and newline_count <= keep_lines:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            chunk = f.read(read_size)
            chunks.append(chunk)
            newline_count += chunk.count(b"\n")

    data = b"".join(reversed(chunks))

    if newline_count <= keep_lines:
        return data

    # Split from the right and preserve exactly last keep_lines lines.
    # This handles final newline correctly enough for NDJSON retention.
    lines = data.splitlines(keepends=True)
    return b"".join(lines[-keep_lines:])


def trim_ndjson_file(
    *,
    path: Path,
    name: str,
    max_bytes: int,
    keep_lines: int,
    dry_run: bool = False,
) -> RetentionAction:
    before = file_size(path)

    if before is None:
        return RetentionAction(
            name=name,
            path=str(path),
            status="missing",
            before_bytes=None,
            after_bytes=None,
        )

    if before <= max_bytes:
        return RetentionAction(
            name=name,
            path=str(path),
            status="skipped",
            before_bytes=before,
            after_bytes=before,
            note="below max_bytes",
        )

    if dry_run:
        return RetentionAction(
            name=name,
            path=str(path),
            status="would_trim",
            before_bytes=before,
            after_bytes=None,
            note=f"keep_last_lines={keep_lines}",
        )

    try:
        ensure_parent(path)
        retained = tail_bytes_for_last_lines(path, keep_lines=keep_lines)

        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=str(path.parent),
        )
        tmp_path = Path(tmp_name)

        try:
            with os.fdopen(fd, "wb") as f:
                f.write(retained)
                if retained and not retained.endswith(b"\n"):
                    f.write(b"\n")
                f.flush()
                os.fsync(f.fileno())

            tmp_path.replace(path)
        finally:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

        after = file_size(path)
        return RetentionAction(
            name=name,
            path=str(path),
            status="trimmed",
            before_bytes=before,
            after_bytes=after,
            note=f"kept_last_lines={keep_lines}",
        )

    except Exception as exc:  # noqa: BLE001
        return RetentionAction(
            name=name,
            path=str(path),
            status="error",
            before_bytes=before,
            after_bytes=file_size(path),
            error=repr(exc),
        )


def truncate_file_if_large(
    *,
    path: Path,
    name: str,
    max_bytes: int,
    dry_run: bool = False,
) -> RetentionAction:
    before = file_size(path)

    if before is None:
        return RetentionAction(
            name=name,
            path=str(path),
            status="missing",
        )

    if before <= max_bytes:
        return RetentionAction(
            name=name,
            path=str(path),
            status="skipped",
            before_bytes=before,
            after_bytes=before,
            note="below max_bytes",
        )

    if dry_run:
        return RetentionAction(
            name=name,
            path=str(path),
            status="would_truncate",
            before_bytes=before,
            after_bytes=None,
        )

    try:
        path.write_text("", encoding="utf-8")
        return RetentionAction(
            name=name,
            path=str(path),
            status="truncated",
            before_bytes=before,
            after_bytes=file_size(path),
        )
    except Exception as exc:  # noqa: BLE001
        return RetentionAction(
            name=name,
            path=str(path),
            status="error",
            before_bytes=before,
            after_bytes=file_size(path),
            error=repr(exc),
        )


def delete_old_files(
    *,
    root: Path,
    name: str,
    retention_days: int,
    patterns: tuple[str, ...],
    dry_run: bool = False,
) -> RetentionAction:
    if not root.exists():
        return RetentionAction(
            name=name,
            path=str(root),
            status="missing",
        )

    now_ts = datetime.now(timezone.utc).timestamp()
    cutoff_seconds = retention_days * 24 * 60 * 60
    removed = 0
    before_bytes = 0
    after_bytes = 0

    try:
        candidates: list[Path] = []
        for pattern in patterns:
            candidates.extend(root.rglob(pattern))

        unique_candidates = sorted(set(candidates))

        for item in unique_candidates:
            if not item.is_file():
                continue

            try:
                stat = item.stat()
            except OSError:
                continue

            before_bytes += stat.st_size
            age_seconds = now_ts - stat.st_mtime

            if age_seconds < cutoff_seconds:
                after_bytes += stat.st_size
                continue

            if not dry_run:
                try:
                    item.unlink()
                except OSError:
                    after_bytes += stat.st_size
                    continue

            removed += 1

        status = "deleted" if removed else "skipped"
        if dry_run and removed:
            status = "would_delete"

        return RetentionAction(
            name=name,
            path=str(root),
            status=status,
            before_bytes=before_bytes,
            after_bytes=after_bytes,
            removed_files=removed,
            note=f"retention_days={retention_days}",
        )

    except Exception as exc:  # noqa: BLE001
        return RetentionAction(
            name=name,
            path=str(root),
            status="error",
            before_bytes=before_bytes,
            after_bytes=after_bytes,
            removed_files=removed,
            error=repr(exc),
        )


def run_runtime_retention(*, dry_run: bool | None = None) -> dict[str, Any]:
    if dry_run is None:
        dry_run = env_bool("RUNTIME_RETENTION_DRY_RUN", False)

    journal_max_mb = env_int("RETENTION_JOURNAL_MAX_MB", DEFAULT_JOURNAL_MAX_MB)
    journal_keep_lines = env_int("RETENTION_JOURNAL_KEEP_LINES", DEFAULT_JOURNAL_KEEP_LINES)
    snapshot_max_mb = env_int("RETENTION_SNAPSHOT_MAX_MB", DEFAULT_SNAPSHOT_MAX_MB)
    snapshot_keep_lines = env_int("RETENTION_SNAPSHOT_KEEP_LINES", DEFAULT_SNAPSHOT_KEEP_LINES)
    app_log_max_mb = env_int("RETENTION_APP_LOG_MAX_MB", DEFAULT_APP_LOG_MAX_MB)
    report_retention_days = env_int("RETENTION_REPORT_DAYS", DEFAULT_REPORT_RETENTION_DAYS)

    before_disk = disk_usage(RUNTIME_DIR)

    actions = [
        trim_ndjson_file(
            path=RADAR_JOURNAL_PATH,
            name="radar_journal",
            max_bytes=journal_max_mb * MB,
            keep_lines=journal_keep_lines,
            dry_run=dry_run,
        ),
        trim_ndjson_file(
            path=RADAR_SNAPSHOT_PATH,
            name="radar_snapshot_v2",
            max_bytes=snapshot_max_mb * MB,
            keep_lines=snapshot_keep_lines,
            dry_run=dry_run,
        ),
        truncate_file_if_large(
            path=APP_LOG_PATH,
            name="app_log",
            max_bytes=app_log_max_mb * MB,
            dry_run=dry_run,
        ),
        delete_old_files(
            root=BRIEFINGS_DIR,
            name="briefing_artifacts",
            retention_days=report_retention_days,
            patterns=("*.json", "*.txt", "*.md"),
            dry_run=dry_run,
        ),
        delete_old_files(
            root=TPO_REPORTS_DIR,
            name="tpo_report_artifacts",
            retention_days=report_retention_days,
            patterns=("*.json", "*.txt", "*.md"),
            dry_run=dry_run,
        ),
    ]

    after_disk = disk_usage(RUNTIME_DIR)

    return {
        "status": "ok",
        "version": RUNTIME_RETENTION_VERSION,
        "dry_run": dry_run,
        "generated_at_utc": utc_now(),
        "runtime_dir": str(RUNTIME_DIR),
        "before_disk": before_disk,
        "after_disk": after_disk,
        "config": {
            "journal_max_mb": journal_max_mb,
            "journal_keep_lines": journal_keep_lines,
            "snapshot_max_mb": snapshot_max_mb,
            "snapshot_keep_lines": snapshot_keep_lines,
            "app_log_max_mb": app_log_max_mb,
            "report_retention_days": report_retention_days,
        },
        "actions": [asdict(action) for action in actions],
    }


def main() -> None:
    result = run_runtime_retention()
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()