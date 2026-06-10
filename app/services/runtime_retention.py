from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO

from app.core.settings import settings


# =============================================================================
# RUNTIME RETENTION v1.1
# =============================================================================
# Purpose:
# - Keep Render persistent disk healthy.
# - Trim large NDJSON runtime files without external dependencies.
# - Use /tmp for retained-tail staging so /var/data does not need extra space.
# - Never replace a non-empty source file with an empty retained file.
# - Never replace original unless the temporary retained file was fully written
#   and verified.
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
#
# Important:
# - If the temporary file is staged on another filesystem (/tmp) atomic rename
#   into /var/data is not possible. In that case this module verifies the staged
#   file first, then truncates and rewrites the original. This is intentional:
#   it allows emergency trimming when /var/data is almost full.
# =============================================================================


RUNTIME_RETENTION_VERSION = "runtime-retention-v1.1-safe-tmp-trim-no-empty-replace"

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
DEFAULT_TMP_DIR = "/tmp/runtime_retention"
DEFAULT_COPY_CHUNK_BYTES = 1024 * 1024


@dataclass
class RetentionAction:
    name: str
    path: str
    status: str
    before_bytes: int | None = None
    after_bytes: int | None = None
    before_lines: int | None = None
    after_lines: int | None = None
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


def env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip()


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


def ensure_tmp_dir(tmp_dir: Path) -> None:
    tmp_dir.mkdir(parents=True, exist_ok=True)


def fsync_parent(path: Path) -> None:
    """
    Best-effort directory fsync after replace/write.
    Some platforms/filesystems may not support directory fsync; ignore failures.
    """
    try:
        fd = os.open(str(path.parent), os.O_RDONLY)
    except OSError:
        return

    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def count_lines(path: Path) -> int:
    """
    Count physical lines with bounded memory.
    Good enough for NDJSON retention where each record is one line.
    """
    total = 0
    with path.open("rb") as f:
        for _line in f:
            total += 1
    return total


def copy_stream(src: BinaryIO, dst: BinaryIO, *, chunk_size: int = DEFAULT_COPY_CHUNK_BYTES) -> int:
    copied = 0
    while True:
        chunk = src.read(chunk_size)
        if not chunk:
            break
        dst.write(chunk)
        copied += len(chunk)
    return copied


def copy_last_lines_to_tmp(
    *,
    source_path: Path,
    tmp_dir: Path,
    keep_lines: int,
) -> tuple[Path, int, int]:
    """
    Build a retained-tail file in tmp_dir without holding the source in memory.

    Returns:
        (tmp_path, source_line_count, retained_line_count)
    """
    if keep_lines <= 0:
        raise ValueError("keep_lines must be > 0 for safe NDJSON trim")

    ensure_tmp_dir(tmp_dir)

    source_lines = count_lines(source_path)
    skip_lines = max(0, source_lines - keep_lines)

    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{source_path.name}.",
        suffix=".retained.tmp",
        dir=str(tmp_dir),
    )
    tmp_path = Path(tmp_name)

    retained_lines = 0
    try:
        with source_path.open("rb") as src, os.fdopen(fd, "wb") as dst:
            for idx, line in enumerate(src):
                if idx < skip_lines:
                    continue
                dst.write(line)
                retained_lines += 1

            dst.flush()
            os.fsync(dst.fileno())

    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise

    actual_retained_lines = count_lines(tmp_path) if tmp_path.exists() else 0
    return tmp_path, source_lines, actual_retained_lines


def staged_file_looks_safe(
    *,
    source_path: Path,
    tmp_path: Path,
    source_size: int,
    source_lines: int,
    retained_lines: int,
    keep_lines: int,
) -> tuple[bool, str]:
    tmp_size = file_size(tmp_path)

    if tmp_size is None:
        return False, "tmp_missing"

    if source_size > 0 and source_lines > 0 and tmp_size <= 0:
        return False, "tmp_empty_for_non_empty_source"

    if source_lines > 0 and retained_lines <= 0:
        return False, "tmp_has_zero_lines_for_non_empty_source"

    if retained_lines > keep_lines:
        return False, f"tmp_retained_too_many_lines={retained_lines}>{keep_lines}"

    if source_lines <= keep_lines and tmp_size < source_size:
        return False, "tmp_smaller_than_source_when_no_lines_should_be_skipped"

    if source_path.exists() and source_size > 0:
        try:
            with tmp_path.open("rb") as f:
                for line in f:
                    stripped = line.lstrip()
                    if not stripped:
                        continue
                    if not (stripped.startswith(b"{") or stripped.startswith(b"[")):
                        return False, "tmp_first_non_empty_line_is_not_json_like"
                    break
        except OSError as exc:
            return False, f"tmp_read_error={exc!r}"

    return True, "ok"


def overwrite_original_from_staged_tmp(
    *,
    source_path: Path,
    tmp_path: Path,
    before_bytes: int,
    min_free_bytes: int,
) -> None:
    """
    Replace source_path with tmp_path content.

    If tmp and source are on the same filesystem, use atomic os.replace.
    If not, verify enough effective free space, then rewrite original from tmp.
    The caller must verify tmp_path before calling this function.
    """
    ensure_parent(source_path)

    tmp_size = tmp_path.stat().st_size

    try:
        source_parent_device = source_path.parent.stat().st_dev
        tmp_device = tmp_path.stat().st_dev
    except OSError:
        source_parent_device = None
        tmp_device = None

    if source_parent_device is not None and tmp_device == source_parent_device:
        os.replace(str(tmp_path), str(source_path))
        fsync_parent(source_path)
        return

    usage = shutil.disk_usage(source_path.parent)
    effective_free_after_truncate = usage.free + max(before_bytes, 0)

    if effective_free_after_truncate < tmp_size + min_free_bytes:
        raise OSError(
            "not_enough_effective_space_for_cross_device_overwrite: "
            f"effective_free_after_truncate={effective_free_after_truncate}, "
            f"tmp_size={tmp_size}, min_free_bytes={min_free_bytes}"
        )

    with tmp_path.open("rb") as src, source_path.open("wb") as dst:
        copy_stream(src, dst)
        dst.flush()
        os.fsync(dst.fileno())

    fsync_parent(source_path)


def trim_ndjson_file(
    *,
    path: Path,
    name: str,
    max_bytes: int,
    keep_lines: int,
    dry_run: bool = False,
    tmp_dir: Path | None = None,
    min_free_bytes: int = DEFAULT_MIN_FREE_MB * MB,
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

    if before <= 0:
        return RetentionAction(
            name=name,
            path=str(path),
            status="skipped",
            before_bytes=before,
            after_bytes=before,
            before_lines=0,
            after_lines=0,
            note="empty file",
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

    if keep_lines <= 0:
        return RetentionAction(
            name=name,
            path=str(path),
            status="error",
            before_bytes=before,
            after_bytes=before,
            error="keep_lines must be > 0",
        )

    if dry_run:
        return RetentionAction(
            name=name,
            path=str(path),
            status="would_trim",
            before_bytes=before,
            after_bytes=None,
            note=f"keep_last_lines={keep_lines}; tmp_dir={tmp_dir or DEFAULT_TMP_DIR}",
        )

    tmp_dir = tmp_dir or Path(DEFAULT_TMP_DIR)
    tmp_path: Path | None = None

    try:
        tmp_path, source_lines, retained_lines = copy_last_lines_to_tmp(
            source_path=path,
            tmp_dir=tmp_dir,
            keep_lines=keep_lines,
        )

        safe, reason = staged_file_looks_safe(
            source_path=path,
            tmp_path=tmp_path,
            source_size=before,
            source_lines=source_lines,
            retained_lines=retained_lines,
            keep_lines=keep_lines,
        )

        if not safe:
            return RetentionAction(
                name=name,
                path=str(path),
                status="error",
                before_bytes=before,
                after_bytes=file_size(path),
                before_lines=source_lines,
                after_lines=retained_lines,
                error=f"staged_tmp_not_safe: {reason}",
            )

        overwrite_original_from_staged_tmp(
            source_path=path,
            tmp_path=tmp_path,
            before_bytes=before,
            min_free_bytes=min_free_bytes,
        )

        after = file_size(path)
        after_lines = count_lines(path) if path.exists() else None

        return RetentionAction(
            name=name,
            path=str(path),
            status="trimmed",
            before_bytes=before,
            after_bytes=after,
            before_lines=source_lines,
            after_lines=after_lines,
            note=f"kept_last_lines={keep_lines}; tmp_dir={tmp_dir}",
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

    finally:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


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
    min_free_mb = env_int("RETENTION_MIN_FREE_MB", DEFAULT_MIN_FREE_MB)
    tmp_dir = Path(env_str("RETENTION_TMP_DIR", DEFAULT_TMP_DIR))

    before_disk = disk_usage(RUNTIME_DIR)

    actions = [
        trim_ndjson_file(
            path=RADAR_JOURNAL_PATH,
            name="radar_journal",
            max_bytes=journal_max_mb * MB,
            keep_lines=journal_keep_lines,
            dry_run=dry_run,
            tmp_dir=tmp_dir,
            min_free_bytes=min_free_mb * MB,
        ),
        trim_ndjson_file(
            path=RADAR_SNAPSHOT_PATH,
            name="radar_snapshot_v2",
            max_bytes=snapshot_max_mb * MB,
            keep_lines=snapshot_keep_lines,
            dry_run=dry_run,
            tmp_dir=tmp_dir,
            min_free_bytes=min_free_mb * MB,
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
    has_errors = any(action.status == "error" for action in actions)

    return {
        "status": "error" if has_errors else "ok",
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
            "min_free_mb": min_free_mb,
            "tmp_dir": str(tmp_dir),
        },
        "actions": [asdict(action) for action in actions],
    }


def main() -> None:
    result = run_runtime_retention()
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
