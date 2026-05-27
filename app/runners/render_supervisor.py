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
"""

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence


SUPERVISOR_VERSION = "render-supervisor-v1.0-shared-disk-two-workers"


@dataclass
class ManagedProcess:
    name: str
    command: list[str]
    process: subprocess.Popen | None = None
    restart_count: int = 0
    last_started_at: str | None = None


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


def main() -> int:
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    restart_failed = env_bool("SUPERVISOR_RESTART_FAILED_WORKERS", True)
    sleep_sec = int(os.getenv("SUPERVISOR_POLL_SEC", "10"))

    processes = build_processes()

    log(
        "started "
        f"version={SUPERVISOR_VERSION} "
        f"processes={[p.name for p in processes]} "
        f"restart_failed={restart_failed}"
    )

    if not processes:
        log("no processes enabled; exiting")
        return 1

    for item in processes:
        start_process(item)

    try:
        while not shutdown_requested:
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

            for _ in range(max(1, sleep_sec)):
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