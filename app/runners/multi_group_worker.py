from __future__ import annotations

"""
Multi-group worker for AI Market Analyst.

Runs:
1. core
2. waits FX_MAJOR_DELAY_AFTER_CORE_SEC
3. fx_major
4. sleeps until RUN_INTERVAL_SEC window completes

Telegram trade alerts are NOT generated here.
They remain inside stateful_batch_runner hard gate:
READY + EXECUTABLE + RR 2-10 + complete geometry.
"""

import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from app.core.instrument_batches import get_batch_symbols, list_available_batches
from app.core.logger import bind_logger, get_logger, log_exception, setup_logging
from app.core.settings import settings
from app.runners.stateful_batch_runner import run_batch_cycle
from app.services.heartbeat import HeartbeatService
from app.services.telegram_notifier import TelegramNotifier


logger = get_logger(__name__, component="multi_group_worker")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_int(value: str | None, default: int) -> int:
    try:
        return int(value) if value not in (None, "") else default
    except ValueError:
        return default


def _to_bool(value: str | None, default: bool = False) -> bool:
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_groups() -> list[str]:
    raw = os.getenv("MULTI_BATCH_GROUPS") or os.getenv("BATCH_GROUPS") or "core,fx_major"
    groups = [x.strip().lower() for x in raw.split(",") if x.strip()]

    available = set(list_available_batches())
    unknown = [g for g in groups if g not in available]

    if unknown:
        raise ValueError(
            f"Unknown batch groups: {unknown}. Available: {sorted(available)}"
        )

    if not groups:
        raise ValueError("MULTI_BATCH_GROUPS is empty.")

    return groups


class GracefulShutdown:
    def __init__(self) -> None:
        self.stop_requested = False
        self.last_signal: int | None = None

    def install(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        del frame
        self.stop_requested = True
        self.last_signal = signum
        logger.warning("Shutdown signal received: signum=%s", signum)


def sleep_with_shutdown(seconds: int | float, shutdown: GracefulShutdown) -> None:
    remaining = max(0, int(seconds))

    while remaining > 0 and not shutdown.stop_requested:
        time.sleep(1)
        remaining -= 1


def _boot_message(groups: list[str], interval_sec: int, delay_sec: int) -> str:
    return (
        f"<b>{getattr(settings, 'app_name', 'AI Market Analyst')}</b>\n"
        "Multi-group worker booted.\n"
        f"Env: {getattr(settings, 'app_env', 'unknown')}\n"
        f"Groups: {', '.join(groups)}\n"
        f"Interval: {interval_sec}s\n"
        f"Delay after first group: {delay_sec}s\n"
        f"Paper mode: {'ON' if getattr(settings, 'paper_mode', True) else 'OFF'}"
    )


def _stop_message() -> str:
    return (
        f"<b>{getattr(settings, 'app_name', 'AI Market Analyst')}</b>\n"
        "Multi-group worker stopped."
    )


def run_group_once(group: str) -> dict[str, Any]:
    started = utc_now_iso()
    t0 = time.monotonic()

    group_logger = bind_logger(
        logger,
        component="multi_group_worker",
        cycle_id="-",
        symbol=group,
    )

    try:
        symbols = get_batch_symbols(group)
        group_logger.info("Starting group=%s symbols=%s", group, symbols)

        result = run_batch_cycle(batch_group=group)

        instruments = result.get("instruments", []) if isinstance(result, dict) else []
        errors = result.get("errors", []) if isinstance(result, dict) else []

        summary = {
            "group": group,
            "cycle_id": result.get("cycle_id") if isinstance(result, dict) else None,
            "status": result.get("status", "unknown") if isinstance(result, dict) else "unknown",
            "instrument_count": len(instruments) if isinstance(instruments, list) else 0,
            "error_count": len(errors) if isinstance(errors, list) else 0,
            "symbols": [
                x.get("symbol")
                for x in instruments
                if isinstance(x, dict)
            ],
            "started_at_utc": started,
            "finished_at_utc": utc_now_iso(),
            "elapsed_sec": round(time.monotonic() - t0, 3),
            "error_message": None,
        }

        group_logger.info("Group finished summary=%s", summary)
        return summary

    except Exception as exc:
        log_exception(
            group_logger,
            f"Group failed: {group}: {exc}",
            component="multi_group_worker",
            cycle_id="-",
            symbol=group,
        )

        return {
            "group": group,
            "cycle_id": None,
            "status": "failed",
            "instrument_count": 0,
            "error_count": 1,
            "symbols": [],
            "started_at_utc": started,
            "finished_at_utc": utc_now_iso(),
            "elapsed_sec": round(time.monotonic() - t0, 3),
            "error_message": str(exc),
        }


def run_sequence(
    *,
    groups: list[str],
    delay_after_first_sec: int,
    shutdown: GracefulShutdown,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for index, group in enumerate(groups):
        if shutdown.stop_requested:
            break

        if index == 1 and delay_after_first_sec > 0:
            logger.info(
                "Waiting before secondary group. delay_sec=%s next_group=%s",
                delay_after_first_sec,
                group,
            )
            sleep_with_shutdown(delay_after_first_sec, shutdown)

        if shutdown.stop_requested:
            break

        results.append(run_group_once(group))

    return results


def main() -> int:
    setup_logging()

    worker_logger = bind_logger(
        logger,
        component="multi_group_worker",
        cycle_id="-",
        symbol="-",
    )
    worker_logger.info("Starting multi-group worker...")

    shutdown = GracefulShutdown()
    shutdown.install()

    groups = _parse_groups()
    interval_sec = _to_int(
        os.getenv("RUN_INTERVAL_SEC"),
        int(getattr(settings, "run_interval_sec", 900)),
    )
    startup_grace_sec = _to_int(
        os.getenv("STARTUP_GRACE_SEC"),
        int(getattr(settings, "startup_grace_sec", 10)),
    )
    delay_after_first_sec = _to_int(
        os.getenv("FX_MAJOR_DELAY_AFTER_CORE_SEC")
        or os.getenv("SECONDARY_GROUP_DELAY_SEC"),
        300,
    )
    enable_telegram = _to_bool(
        os.getenv("ENABLE_TELEGRAM"),
        bool(getattr(settings, "enable_telegram", False)),
    )
    run_once = _to_bool(os.getenv("RUN_ONCE"), False)

    heartbeat = HeartbeatService()
    notifier = TelegramNotifier()

    worker_logger.info(
        "Config groups=%s interval_sec=%s delay_after_first_sec=%s run_once=%s",
        groups,
        interval_sec,
        delay_after_first_sec,
        run_once,
    )

    try:
        heartbeat.mark_boot()
    except Exception as exc:
        log_exception(
            worker_logger,
            f"Heartbeat boot failed: {exc}",
            component="multi_group_worker",
            cycle_id="-",
            symbol="-",
        )

    if enable_telegram and getattr(notifier, "is_active", False):
        try:
            notifier.send_admin_message(
                _boot_message(groups, interval_sec, delay_after_first_sec)
            )
        except Exception as exc:
            log_exception(
                worker_logger,
                f"Boot Telegram failed: {exc}",
                component="multi_group_worker",
                cycle_id="-",
                symbol="-",
            )

    if startup_grace_sec > 0:
        worker_logger.info("Startup grace sleep: %ss", startup_grace_sec)
        sleep_with_shutdown(startup_grace_sec, shutdown)

    while not shutdown.stop_requested:
        sequence_id = utc_now_iso()
        t0 = time.monotonic()

        sequence_logger = bind_logger(worker_logger, cycle_id=sequence_id, symbol="-")
        sequence_logger.info("Sequence started.")

        try:
            heartbeat.mark_cycle_started(sequence_id)
        except Exception as exc:
            log_exception(
                sequence_logger,
                f"Heartbeat cycle start failed: {exc}",
                component="multi_group_worker",
                cycle_id=sequence_id,
                symbol="-",
            )

        results = run_sequence(
            groups=groups,
            delay_after_first_sec=delay_after_first_sec,
            shutdown=shutdown,
        )

        failed = any(x.get("status") == "failed" for x in results)

        try:
            if failed:
                heartbeat.mark_cycle_failure(sequence_id, "One or more groups failed")
            else:
                heartbeat.mark_cycle_success(sequence_id)
        except Exception as exc:
            log_exception(
                sequence_logger,
                f"Heartbeat finish failed: {exc}",
                component="multi_group_worker",
                cycle_id=sequence_id,
                symbol="-",
            )

        elapsed = time.monotonic() - t0
        sleep_next = max(0, interval_sec - int(elapsed))

        sequence_logger.info(
            "Sequence finished results=%s elapsed=%.2fs sleep_next=%ss",
            results,
            elapsed,
            sleep_next,
        )

        if run_once:
            worker_logger.info("RUN_ONCE=true, exiting after one sequence.")
            break

        sleep_with_shutdown(sleep_next, shutdown)

    try:
        heartbeat.mark_stopped()
    except Exception as exc:
        log_exception(
            worker_logger,
            f"Heartbeat stop failed: {exc}",
            component="multi_group_worker",
            cycle_id="-",
            symbol="-",
        )

    if enable_telegram and getattr(notifier, "is_active", False):
        try:
            notifier.send_admin_message(_stop_message())
        except Exception as exc:
            log_exception(
                worker_logger,
                f"Stop Telegram failed: {exc}",
                component="multi_group_worker",
                cycle_id="-",
                symbol="-",
            )

    worker_logger.info("Multi-group worker stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())