from __future__ import annotations

"""
Multi-group worker for AI Market Analyst.

Runs configured batch groups sequentially:
1. runs TPO context exporter
2. verifies TPO offline store
3. core
4. waits GROUP_DELAY_SEC / SECONDARY_GROUP_DELAY_SEC
5. fx_major
6. waits again
7. indices
8. sleeps until RUN_INTERVAL_SEC window completes

Important architecture rule:
- TPO / auction context is calculated by app.services.tpo_context_exporter.
- Live/stateful workers do NOT calculate TPO.
- Live/stateful workers only read the offline TPO store:
  /var/data/runtime/tpo/tpo_latest.json

Telegram trade alerts are NOT generated here.
They remain inside stateful_batch_runner hard gate:
READY + EXECUTABLE + RR 2-10 + complete geometry.

Telegram admin messages are controlled separately by:
ENABLE_TELEGRAM_ADMIN_MESSAGES=true/false

Default:
- trade Telegram alerts stay enabled through normal alert pipeline
- admin boot/stop messages are disabled by default to avoid Telegram spam
- TPO exporter is required before live batches
"""

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
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


def _runtime_dir() -> Path:
    raw = getattr(settings, "runtime_dir", None)

    if raw:
        return Path(raw)

    return Path("/var/data/runtime")


def _tpo_store_path() -> Path:
    raw = (
        os.getenv("TPO_CONTEXT_STORE_PATH")
        or os.getenv("TPO_LATEST_PATH")
        or os.getenv("TPO_STORE_PATH")
    )

    if raw:
        return Path(raw)

    return _runtime_dir() / "tpo" / "tpo_latest.json"


def _read_tpo_store_summary(path: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "store_path": str(path),
        "exists": path.exists(),
        "size_bytes": None,
        "exporter_version": None,
        "updated_at_utc": None,
        "symbols": None,
        "errors": None,
        "read_error": None,
    }

    if not path.exists():
        return summary

    try:
        summary["size_bytes"] = path.stat().st_size
    except Exception as exc:
        summary["read_error"] = f"stat_failed: {exc}"
        return summary

    try:
        data = json.loads(path.read_text())
    except Exception as exc:
        summary["read_error"] = f"json_read_failed: {exc}"
        return summary

    if isinstance(data, dict):
        summary["exporter_version"] = data.get("exporter_version")
        summary["updated_at_utc"] = data.get("updated_at_utc")
        summary["errors"] = data.get("errors")

        symbols = data.get("symbols")
        if isinstance(symbols, dict):
            summary["symbols"] = len(symbols)
        else:
            summary["symbols"] = symbols

        contexts = data.get("contexts")
        if summary["symbols"] is None and isinstance(contexts, dict):
            summary["symbols"] = len(contexts)

    return summary


def _tail_text(value: str | None, max_chars: int = 4000) -> str:
    if not value:
        return ""
    return value[-max_chars:]


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
        f"Delay between groups: {delay_sec}s\n"
        f"TPO export before groups: ON\n"
        f"Paper mode: {'ON' if getattr(settings, 'paper_mode', True) else 'OFF'}"
    )


def _stop_message() -> str:
    return (
        f"<b>{getattr(settings, 'app_name', 'AI Market Analyst')}</b>\n"
        "Multi-group worker stopped."
    )


def run_tpo_exporter_once(
    *,
    sequence_id: str,
    timeout_sec: int,
) -> dict[str, Any]:
    started = utc_now_iso()
    t0 = time.monotonic()
    store_path = _tpo_store_path()

    tpo_logger = bind_logger(
        logger,
        component="multi_group_worker",
        cycle_id=sequence_id,
        symbol="TPO",
    )

    summary: dict[str, Any] = {
        "status": "unknown",
        "started_at_utc": started,
        "finished_at_utc": None,
        "elapsed_sec": None,
        "returncode": None,
        "timeout_sec": timeout_sec,
        "store_path": str(store_path),
        "store_summary": None,
        "stdout_tail": "",
        "stderr_tail": "",
        "error_message": None,
    }

    tpo_logger.info(
        "TPO export started. module=app.services.tpo_context_exporter store_path=%s timeout_sec=%s",
        store_path,
        timeout_sec,
    )

    try:
        completed = subprocess.run(
            [sys.executable, "-m", "app.services.tpo_context_exporter"],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )

        summary["returncode"] = completed.returncode
        summary["stdout_tail"] = _tail_text(completed.stdout)
        summary["stderr_tail"] = _tail_text(completed.stderr)

        store_summary = _read_tpo_store_summary(store_path)
        summary["store_summary"] = store_summary

        if completed.returncode != 0:
            summary["status"] = "failed"
            summary["error_message"] = (
                f"tpo_context_exporter_returncode_{completed.returncode}"
            )
            tpo_logger.error(
                "TPO export failed. summary=%s",
                summary,
            )
            return summary

        if not store_summary.get("exists"):
            summary["status"] = "failed"
            summary["error_message"] = "tpo_store_missing_after_export"
            tpo_logger.error(
                "TPO export finished but store is missing. summary=%s",
                summary,
            )
            return summary

        if not store_summary.get("size_bytes"):
            summary["status"] = "failed"
            summary["error_message"] = "tpo_store_empty_after_export"
            tpo_logger.error(
                "TPO export finished but store is empty. summary=%s",
                summary,
            )
            return summary

        if store_summary.get("read_error"):
            summary["status"] = "failed"
            summary["error_message"] = str(store_summary.get("read_error"))
            tpo_logger.error(
                "TPO export finished but store summary failed. summary=%s",
                summary,
            )
            return summary

        summary["status"] = "ok"
        tpo_logger.info(
            "TPO export finished successfully. summary=%s",
            summary,
        )
        return summary

    except subprocess.TimeoutExpired as exc:
        summary["status"] = "failed"
        summary["error_message"] = f"tpo_export_timeout_after_{timeout_sec}s"
        summary["stdout_tail"] = _tail_text(
            exc.stdout.decode() if isinstance(exc.stdout, bytes) else exc.stdout
        )
        summary["stderr_tail"] = _tail_text(
            exc.stderr.decode() if isinstance(exc.stderr, bytes) else exc.stderr
        )

        tpo_logger.error(
            "TPO export timeout. summary=%s",
            summary,
        )
        return summary

    except Exception as exc:
        summary["status"] = "failed"
        summary["error_message"] = str(exc)

        log_exception(
            tpo_logger,
            f"TPO export crashed: {exc}",
            component="multi_group_worker",
            cycle_id=sequence_id,
            symbol="TPO",
        )
        return summary

    finally:
        summary["finished_at_utc"] = utc_now_iso()
        summary["elapsed_sec"] = round(time.monotonic() - t0, 3)


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
    delay_between_groups_sec: int,
    shutdown: GracefulShutdown,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for index, group in enumerate(groups):
        if shutdown.stop_requested:
            break

        if index > 0 and delay_between_groups_sec > 0:
            logger.info(
                "Waiting before next group. delay_sec=%s next_group=%s",
                delay_between_groups_sec,
                group,
            )
            sleep_with_shutdown(delay_between_groups_sec, shutdown)

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
    delay_between_groups_sec = _to_int(
        os.getenv("GROUP_DELAY_SEC")
        or os.getenv("SECONDARY_GROUP_DELAY_SEC")
        or os.getenv("FX_MAJOR_DELAY_AFTER_CORE_SEC"),
        300,
    )

    enable_tpo_export_before_groups = _to_bool(
        os.getenv("ENABLE_TPO_EXPORT_BEFORE_GROUPS"),
        True,
    )
    tpo_export_required = _to_bool(
        os.getenv("TPO_EXPORT_REQUIRED"),
        True,
    )
    tpo_export_timeout_sec = _to_int(
        os.getenv("TPO_EXPORT_TIMEOUT_SEC"),
        300,
    )

    # General Telegram flag.
    # Keep this enabled if trade alerts must work.
    enable_telegram = _to_bool(
        os.getenv("ENABLE_TELEGRAM"),
        bool(getattr(settings, "enable_telegram", False)),
    )

    # Admin Telegram flag.
    # This controls only boot/stop service messages.
    # It does NOT affect ENTRY_READY / trade alerts from stateful_batch_runner.
    enable_telegram_admin_messages = _to_bool(
        os.getenv("ENABLE_TELEGRAM_ADMIN_MESSAGES"),
        bool(getattr(settings, "enable_telegram_admin_messages", False)),
    )

    run_once = _to_bool(os.getenv("RUN_ONCE"), False)

    heartbeat = HeartbeatService()
    notifier = TelegramNotifier()

    worker_logger.info(
        "Config groups=%s interval_sec=%s delay_between_groups_sec=%s run_once=%s enable_telegram=%s enable_telegram_admin_messages=%s enable_tpo_export_before_groups=%s tpo_export_required=%s tpo_export_timeout_sec=%s tpo_store_path=%s",
        groups,
        interval_sec,
        delay_between_groups_sec,
        run_once,
        enable_telegram,
        enable_telegram_admin_messages,
        enable_tpo_export_before_groups,
        tpo_export_required,
        tpo_export_timeout_sec,
        _tpo_store_path(),
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

    if (
        enable_telegram
        and enable_telegram_admin_messages
        and getattr(notifier, "is_active", False)
    ):
        try:
            notifier.send_admin_message(
                _boot_message(groups, interval_sec, delay_between_groups_sec)
            )
        except Exception as exc:
            log_exception(
                worker_logger,
                f"Boot Telegram failed: {exc}",
                component="multi_group_worker",
                cycle_id="-",
                symbol="-",
            )
    else:
        worker_logger.info(
            "Telegram admin boot message skipped. enable_telegram=%s enable_admin_messages=%s notifier_active=%s",
            enable_telegram,
            enable_telegram_admin_messages,
            getattr(notifier, "is_active", False),
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

        tpo_export_summary: dict[str, Any] | None = None
        tpo_failed = False
        skip_groups = False

        if enable_tpo_export_before_groups and not shutdown.stop_requested:
            tpo_export_summary = run_tpo_exporter_once(
                sequence_id=sequence_id,
                timeout_sec=tpo_export_timeout_sec,
            )
            tpo_failed = tpo_export_summary.get("status") != "ok"

            if tpo_failed:
                sequence_logger.error(
                    "TPO export failed before groups. tpo_export_required=%s summary=%s",
                    tpo_export_required,
                    tpo_export_summary,
                )

                if tpo_export_required:
                    skip_groups = True
                    sequence_logger.error(
                        "Skipping live groups because TPO export is required and failed."
                    )
            else:
                sequence_logger.info(
                    "TPO export OK before groups. summary=%s",
                    tpo_export_summary,
                )
        elif not enable_tpo_export_before_groups:
            sequence_logger.warning(
                "TPO export before groups is disabled. Live worker will rely on existing store."
            )

        if shutdown.stop_requested:
            break

        if skip_groups:
            results: list[dict[str, Any]] = []
        else:
            results = run_sequence(
                groups=groups,
                delay_between_groups_sec=delay_between_groups_sec,
                shutdown=shutdown,
            )

        group_failed = any(x.get("status") == "failed" for x in results)
        failed = group_failed or (tpo_failed and tpo_export_required)

        try:
            if failed:
                heartbeat.mark_cycle_failure(
                    sequence_id,
                    "TPO export failed or one or more groups failed",
                )
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
            "Sequence finished tpo_export=%s results=%s elapsed=%.2fs sleep_next=%ss",
            tpo_export_summary,
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

    if (
        enable_telegram
        and enable_telegram_admin_messages
        and getattr(notifier, "is_active", False)
    ):
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
    else:
        worker_logger.info(
            "Telegram admin stop message skipped. enable_telegram=%s enable_admin_messages=%s notifier_active=%s",
            enable_telegram,
            enable_telegram_admin_messages,
            getattr(notifier, "is_active", False),
        )

    worker_logger.info("Multi-group worker stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())