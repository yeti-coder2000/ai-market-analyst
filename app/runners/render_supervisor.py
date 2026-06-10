from __future__ import annotations

"""
Render supervisor for AI Market Analyst.

Runs both long-lived workers inside one Render service so they share the same
Persistent Disk mounted at /var/data:

1. Market analysis worker:
   python -m app.runners.multi_group_worker

2. Daily reporting worker:
   python -m app.runners.daily_reporting_worker

Why this exists:
Render Persistent Disks are service-local. A separate Render service cannot read
the main worker's /var/data/runtime files. Therefore, while the system uses
file-based runtime state, both workers must run inside the same service instance.

v1.1:
- Adds safe automatic runtime retention.
- Retention runs inside the supervisor process on a timer.
- Retention errors are logged but never stop market/reporting workers.
"""

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


SUPERVISOR_VERSION = "render-supervisor-v1.1-auto-runtime-retention"

DEFAULT_SUPERVISOR_POLL_SEC = 10
DEFAULT_RETENTION_INTERVAL_SEC = 6 * 60 * 60  # 6 hours
DEFAULT_RETENTION_INITIAL_DELAY_SEC = 5 * 60  # wait 5 minutes after boot

# Safe production defaults. Existing Render env vars override these values.
RETENTION_ENV_DEFAULTS = {
    "RUNTIME_RETENTION_DRY_RUN": "false",
    "RETENTION_JOURNAL_MAX_MB": "120",
    "RETENTION_JOURNAL_KEEP_LINES": "20000",
    "RETENTION_SNAPSHOT_MAX_MB": "120",
    "RETENTION_SNAPSHOT_KEEP_LINES": "10000",
    "RETENTION_APP_LOG_MAX_MB": "25",
    "RETENTION_REPORT_DAYS": "14",
    "RETENTION_MIN_FREE_MB": "150",
    "RETENTION_TMP_DIR": "/tmp/runtime_retention",
}


@dataclass
class ManagedProcess:
    name: str
    command: list[str]
    process: subprocess.Popen | None = None
    restart_count: int = 0
    last_started_at: str | None = None


@dataclass
class RetentionScheduler:
    enabled: bool
    interval_sec: int
    initial_delay_sec: int
    next_run_monotonic: float
    run_count: int = 0
    last_run_at_utc: str | None = None
    last_status: str | None = None


shutdown_requested = False


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(message: str) -> None:
    print(
        f"{now_utc()} | supervisor | {message}",
        flush=True,
    )


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def start_process(item: ManagedProcess) -> None:
    log(f"starting {item.name}: {' '.join(item.command)}")
    item.process = subprocess.Popen(
        item.command,
        stdout=None,
        stderr=None,
        stdin=subprocess.DEVNULL,
        env=os.environ.copy(),
    )
    item.restart_count += 1
    item.last_started_at = now_utc()
    log(f"started {item.name} pid={item.process.pid} restart_count={item.restart_count}")


def stop_process(item: ManagedProcess, *, timeout_sec: int = 30) -> None:
    if item.process is None:
        return

    proc = item.process
    if proc.poll() is not None:
        log(f"{item.name} already stopped returncode={proc.returncode}")
        return

    log(f"stopping {item.name} pid={proc.pid}")
    try:
        proc.terminate()
        proc.wait(timeout=timeout_sec)
        log(f"stopped {item.name} returncode={proc.returncode}")
    except subprocess.TimeoutExpired:
        log(f"{item.name} did not stop in {timeout_sec}s; killing pid={proc.pid}")
        proc.kill()
        proc.wait(timeout=10)
        log(f"killed {item.name} returncode={proc.returncode}")


def handle_signal(signum: int, frame: object) -> None:
    global shutdown_requested
    del frame
    shutdown_requested = True
    log(f"shutdown requested by signal={signum}")


def build_processes() -> list[ManagedProcess]:
    python_bin = sys.executable or "python"

    run_reporting = env_bool("ENABLE_DAILY_REPORTING_WORKER", True)
    run_market_worker = env_bool("ENABLE_MARKET_WORKER", True)

    processes: list[ManagedProcess] = []

    if run_market_worker:
        processes.append(
            ManagedProcess(
                name="multi_group_worker",
                command=[python_bin, "-m", "app.runners.multi_group_worker"],
            )
        )

    if run_reporting:
        processes.append(
            ManagedProcess(
                name="daily_reporting_worker",
                command=[python_bin, "-m", "app.runners.daily_reporting_worker"],
            )
        )

    return processes


def build_retention_scheduler() -> RetentionScheduler:
    enabled = env_bool("ENABLE_RUNTIME_RETENTION", True)
    interval_sec = max(
        300,
        env_int("RUNTIME_RETENTION_INTERVAL_SEC", DEFAULT_RETENTION_INTERVAL_SEC),
    )
    initial_delay_sec = max(
        0,
        env_int("RUNTIME_RETENTION_INITIAL_DELAY_SEC", DEFAULT_RETENTION_INITIAL_DELAY_SEC),
    )

    return RetentionScheduler(
        enabled=enabled,
        interval_sec=interval_sec,
        initial_delay_sec=initial_delay_sec,
        next_run_monotonic=time.monotonic() + initial_delay_sec,
    )


def _apply_retention_env_defaults() -> None:
    for key, value in RETENTION_ENV_DEFAULTS.items():
        os.environ.setdefault(key, value)


def _summarize_retention_actions(actions: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for action in actions:
        name = action.get("name", "unknown")
        status = action.get("status", "unknown")
        before_bytes = action.get("before_bytes")
        after_bytes = action.get("after_bytes")
        removed_files = action.get("removed_files", 0)

        detail = f"{name}:{status}"
        if isinstance(before_bytes, int) and isinstance(after_bytes, int):
            detail += f"({before_bytes}->{after_bytes}B)"
        if removed_files:
            detail += f" removed={removed_files}"
        parts.append(detail)
    return ", ".join(parts) if parts else "no_actions"


def run_runtime_retention_once() -> dict[str, Any] | None:
    """
    Run runtime retention safely.

    This function must never raise into the supervisor main loop. If retention
    fails, market/reporting child processes must continue running.
    """
    _apply_retention_env_defaults()

    try:
        from app.services.runtime_retention import (  # noqa: PLC0415
            RUNTIME_RETENTION_VERSION,
            run_runtime_retention,
        )

        started_monotonic = time.monotonic()
        result = run_runtime_retention(dry_run=False)
        elapsed_sec = round(time.monotonic() - started_monotonic, 3)

        actions = result.get("actions") if isinstance(result, dict) else []
        if not isinstance(actions, list):
            actions = []

        before_disk = result.get("before_disk", {}) if isinstance(result, dict) else {}
        after_disk = result.get("after_disk", {}) if isinstance(result, dict) else {}

        status = result.get("status", "unknown") if isinstance(result, dict) else "unknown"
        dry_run = result.get("dry_run", None) if isinstance(result, dict) else None

        log(
            "runtime retention completed "
            f"version={RUNTIME_RETENTION_VERSION} "
            f"status={status} dry_run={dry_run} elapsed_sec={elapsed_sec} "
            f"free_mb_before={before_disk.get('free_mb')} "
            f"free_mb_after={after_disk.get('free_mb')} "
            f"actions={_summarize_retention_actions(actions)}"
        )

        errors = [a for a in actions if isinstance(a, dict) and a.get("status") == "error"]
        if errors:
            log(f"runtime retention warnings errors={errors}")

        return result if isinstance(result, dict) else None

    except Exception as exc:  # noqa: BLE001
        log(f"runtime retention failed error={repr(exc)}")
        return None


def maybe_run_runtime_retention(scheduler: RetentionScheduler) -> None:
    if not scheduler.enabled:
        return

    if shutdown_requested:
        return

    now_monotonic = time.monotonic()
    if now_monotonic < scheduler.next_run_monotonic:
        return

    scheduler.run_count += 1
    scheduler.last_run_at_utc = now_utc()

    log(
        "runtime retention starting "
        f"run_count={scheduler.run_count} "
        f"interval_sec={scheduler.interval_sec}"
    )

    result = run_runtime_retention_once()
    scheduler.last_status = (
        str(result.get("status")) if isinstance(result, dict) and result.get("status") else "error"
    )

    # Schedule the next run even if this run failed. Retention must never spam
    # retries or block the supervisor loop.
    scheduler.next_run_monotonic = time.monotonic() + scheduler.interval_sec


def main() -> int:
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    restart_failed = env_bool("SUPERVISOR_RESTART_FAILED_WORKERS", True)
    sleep_sec = max(1, env_int("SUPERVISOR_POLL_SEC", DEFAULT_SUPERVISOR_POLL_SEC))

    processes = build_processes()
    retention_scheduler = build_retention_scheduler()

    log(
        "started "
        f"version={SUPERVISOR_VERSION} "
        f"processes={[p.name for p in processes]} "
        f"restart_failed={restart_failed} "
        f"retention_enabled={retention_scheduler.enabled} "
        f"retention_interval_sec={retention_scheduler.interval_sec} "
        f"retention_initial_delay_sec={retention_scheduler.initial_delay_sec}"
    )

    if not processes:
        log("no processes enabled; exiting")
        return 1

    for item in processes:
        start_process(item)

    try:
        while not shutdown_requested:
            maybe_run_runtime_retention(retention_scheduler)

            for item in processes:
                proc = item.process
                if proc is None:
                    if restart_failed:
                        start_process(item)
                    continue

                returncode = proc.poll()
                if returncode is None:
                    continue

                log(f"{item.name} exited returncode={returncode}")

                if not restart_failed:
                    log("restart disabled; requesting supervisor shutdown")
                    return 1

                if shutdown_requested:
                    break

                time.sleep(3)
                start_process(item)

            for _ in range(sleep_sec):
                if shutdown_requested:
                    break
                time.sleep(1)

    finally:
        log("stopping child processes")
        for item in reversed(processes):
            stop_process(item)
        log("stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())