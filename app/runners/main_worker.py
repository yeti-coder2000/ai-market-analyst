from __future__ import annotations

import json
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from app.core.logger import bind_logger, get_logger, log_exception, setup_logging
from app.core.settings import settings
from app.services.alert_deduper import AlertDeduper
from app.services.heartbeat import HeartbeatService
from app.services.telegram_notifier import TelegramNotifier


logger = get_logger(__name__, component="main_worker")


# =============================================================================
# TELEGRAM HARD GATE
# =============================================================================

MIN_TELEGRAM_RR = 2.0
MAX_TELEGRAM_RR = 10.0


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value is not None and value != "":
            return value
    return None


def is_trade_alert_allowed(payload: dict[str, Any]) -> tuple[bool, str]:
    """
    Final Telegram gate for the cloud worker.

    Main rule:
    Telegram is a battle siren, not a radar feed.

    Allowed only:
    - READY
    - EXECUTABLE
    - LONG / SHORT
    - RR between 2.0 and 10.0
    - entry / stop / target present

    Everything else remains in journal/statistics/open_signals.
    """
    if not isinstance(payload, dict):
        return False, "payload_is_not_dict"

    signal_class = str(
        _first_present(payload, "signal_class", "stage", "current_stage") or ""
    ).upper()
    execution_status = str(payload.get("execution_status") or "").upper()
    direction = str(payload.get("direction") or "").upper()
    scenario = str(_first_present(payload, "scenario", "scenario_type") or "").upper()
    alert_type = str(payload.get("alert_type") or "").upper()

    rr = _float_or_none(_first_present(payload, "risk_reward_ratio", "rr"))
    entry = _first_present(payload, "entry_reference_price", "entry")
    stop = _first_present(
        payload,
        "invalidation_reference_price",
        "stop_loss",
        "stop",
    )
    target = _first_present(
        payload,
        "target_reference_price",
        "take_profit",
        "target",
    )

    if scenario in {"NO_ACTION", "MARKET_CLOSED"}:
        return False, f"blocked_non_trade_scenario:{scenario}"

    if alert_type in {"WATCH_NEW", "WATCH_UPGRADED", "INVALIDATED"}:
        return False, f"blocked_non_entry_alert_type:{alert_type}"

    if signal_class != "READY":
        return False, f"blocked_non_ready_signal_class:{signal_class or '-'}"

    if execution_status != "EXECUTABLE":
        return False, f"blocked_non_executable_status:{execution_status or '-'}"

    if direction not in {"LONG", "SHORT"}:
        return False, f"blocked_invalid_direction:{direction or '-'}"

    if rr is None:
        return False, "blocked_missing_rr"

    if rr < MIN_TELEGRAM_RR:
        return False, f"blocked_rr_too_low:{rr:.2f}"

    if rr > MAX_TELEGRAM_RR:
        return False, f"blocked_rr_too_high:{rr:.2f}"

    if entry is None or stop is None or target is None:
        return False, "blocked_missing_trade_geometry"

    return True, "allowed_ready_executable"


class GracefulShutdown:
    """
    Handles SIGINT / SIGTERM for clean worker shutdown.
    """

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
        logger.warning(
            f"Shutdown signal received: signum={signum}",
            extra={"component": "main_worker", "cycle_id": "-", "symbol": "-"},
        )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def get_setting(name: str, default: Any) -> Any:
    """
    Safe accessor for backward compatibility.
    """
    return getattr(settings, name, default)


def is_weekend_utc() -> bool:
    """
    Monday=0, Sunday=6.
    Weekend skip remains UTC-based for deterministic cloud behavior.
    """
    return utc_now().weekday() >= 5


def sleep_with_shutdown(total_seconds: int, shutdown: GracefulShutdown) -> None:
    """
    Sleep in 1-second chunks so worker can stop gracefully.
    """
    remaining = max(0, int(total_seconds))
    while remaining > 0 and not shutdown.stop_requested:
        time.sleep(1)
        remaining -= 1


def ensure_cycle_result_shape(cycle_result: dict[str, Any], cycle_id: str) -> dict[str, Any]:
    """
    Normalize cycle_result into stable worker contract.
    """
    if not isinstance(cycle_result, dict):
        raise RuntimeError("Analytics cycle returned non-dict result.")

    normalized = dict(cycle_result)

    normalized["cycle_id"] = str(normalized.get("cycle_id") or cycle_id)
    normalized["started_at"] = normalized.get("started_at") or cycle_id
    normalized["finished_at"] = normalized.get("finished_at") or utc_now_iso()
    normalized["status"] = str(normalized.get("status") or "ok")

    instruments = normalized.get("instruments")
    if not isinstance(instruments, list):
        instruments = []
    normalized["instruments"] = instruments

    errors = normalized.get("errors")
    if not isinstance(errors, list):
        errors = []
    normalized["errors"] = errors

    return normalized


def extract_alert_candidates(cycle_result: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Extract alert candidates from normalized cycle result.

    Important production decision:
    - We DO NOT build legacy WATCH_NEW payloads from summary fields anymore.
    - Legacy fallback was the source of Telegram noise: EDGE_FORMING / WATCH / NOT_EXECUTABLE.
    - Only explicit alert_payload from stateful_batch_runner is accepted.
    - Even explicit payloads are still hard-gated in process_alerts().
    """
    cycle_id = str(cycle_result.get("cycle_id", "-"))
    instruments = cycle_result.get("instruments", [])

    if not isinstance(instruments, list):
        return []

    alerts: list[dict[str, Any]] = []
    paper_mode = bool(get_setting("paper_mode", True))

    for item in instruments:
        if not isinstance(item, dict):
            continue

        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue

        raw_alert_payload = item.get("alert_payload")
        if not isinstance(raw_alert_payload, dict):
            continue

        if not bool(raw_alert_payload.get("should_alert", False)):
            continue

        payload = dict(raw_alert_payload)
        payload.setdefault("cycle_id", cycle_id)
        payload.setdefault("symbol", symbol)
        payload.setdefault("paper_mode", paper_mode)
        alerts.append(payload)

    return alerts


def write_last_cycle_snapshot(cycle_result: dict[str, Any]) -> None:
    """
    Writes last cycle snapshot into runner_state_path for quick inspection.
    This does not replace radar_journal; it complements it.
    """
    runner_state_path = get_setting("runner_state_path", None)
    if runner_state_path is None:
        logger.warning(
            "settings.runner_state_path is not configured. Snapshot write skipped.",
            extra={
                "component": "main_worker",
                "cycle_id": cycle_result.get("cycle_id", "-"),
                "symbol": "-",
            },
        )
        return

    tmp_path = runner_state_path.with_suffix(".tmp")
    tmp_path.write_text(
        json.dumps(cycle_result, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    tmp_path.replace(runner_state_path)


def run_analytics_cycle(cycle_id: str) -> dict[str, Any]:
    """
    Adapter around existing stateful_batch_runner.
    """
    cycle_logger = bind_logger(logger, cycle_id=cycle_id)

    try:
        from app.runners import stateful_batch_runner as runner_module  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"Failed to import stateful_batch_runner: {exc}") from exc

    if hasattr(runner_module, "run_batch_cycle"):
        cycle_logger.info("Using runner_module.run_batch_cycle()")
        result = runner_module.run_batch_cycle()  # type: ignore[attr-defined]
        normalized = ensure_cycle_result_shape(result, cycle_id)
        return normalized

    if hasattr(runner_module, "main"):
        cycle_logger.warning(
            "Using runner_module.main() compatibility mode. "
            "Production contract should be run_batch_cycle()."
        )
        runner_module.main()  # type: ignore[attr-defined]
        return {
            "cycle_id": cycle_id,
            "status": "ok",
            "started_at": cycle_id,
            "finished_at": utc_now_iso(),
            "instruments": [],
            "errors": [],
            "compatibility_mode": True,
        }

    raise RuntimeError(
        "stateful_batch_runner has neither run_batch_cycle() nor main(). "
        "Expose one of these entrypoints."
    )


def process_alerts(
    cycle_result: dict[str, Any],
    notifier: TelegramNotifier,
    deduper: AlertDeduper,
) -> list[dict[str, Any]]:
    """
    Extract, hard-gate, dedupe and send alerts.

    This is a secondary safety layer. The stateful runner should already enforce
    Telegram rules, but main_worker must never be able to leak WATCH/EDGE messages.
    """
    cycle_id = str(cycle_result.get("cycle_id", "-"))
    cycle_logger = bind_logger(logger, cycle_id=cycle_id)

    alerts = extract_alert_candidates(cycle_result)
    if not alerts:
        cycle_logger.info("No explicit alert_payload candidates found.")
        return []

    processed: list[dict[str, Any]] = []

    for payload in alerts:
        symbol = str(payload.get("symbol", "-"))
        symbol_logger = bind_logger(cycle_logger, symbol=symbol)

        try:
            symbol_logger.info(
                "Alert candidate payload=%s",
                json.dumps(payload, ensure_ascii=False, default=str),
            )
        except Exception:
            symbol_logger.info("Alert candidate payload could not be JSON-serialized.")

        allowed, gate_reason = is_trade_alert_allowed(payload)

        alert_result: dict[str, Any] = {
            "symbol": symbol,
            "alert_type": payload.get("alert_type"),
            "scenario_type": payload.get("scenario_type") or payload.get("scenario"),
            "signal_class": payload.get("signal_class") or payload.get("stage"),
            "execution_status": payload.get("execution_status"),
            "risk_reward_ratio": payload.get("risk_reward_ratio") or payload.get("rr"),
            "telegram_gate_allowed": allowed,
            "telegram_gate_reason": gate_reason,
            "dedupe_decision": None,
            "sent": False,
            "send_error": None,
        }

        if not allowed:
            symbol_logger.info(
                "Main worker Telegram hard-blocked | symbol=%s | alert=%s | class=%s | execution=%s | rr=%s | reason=%s",
                payload.get("symbol"),
                payload.get("alert_type"),
                payload.get("signal_class") or payload.get("stage"),
                payload.get("execution_status"),
                payload.get("risk_reward_ratio") or payload.get("rr"),
                gate_reason,
            )
            processed.append(alert_result)
            continue

        should_send, dedupe_reason = deduper.should_send(payload)
        alert_result["dedupe_decision"] = dedupe_reason

        if not should_send:
            symbol_logger.info(f"Alert suppressed by deduper. reason={dedupe_reason}")
            processed.append(alert_result)
            continue

        try:
            sent = bool(notifier.send_alert(payload))
            alert_result["sent"] = sent

            if sent:
                deduper.mark_sent(payload, reason=dedupe_reason)
                symbol_logger.info(f"Alert sent successfully. reason={dedupe_reason}")
            else:
                symbol_logger.warning(f"Alert send returned False. reason={dedupe_reason}")

        except Exception as exc:
            alert_result["sent"] = False
            alert_result["send_error"] = str(exc)
            log_exception(
                symbol_logger,
                f"Alert send exception: {exc}",
                component="main_worker",
                cycle_id=cycle_id,
                symbol=symbol,
            )

        processed.append(alert_result)

    return processed


def worker_boot_message() -> str:
    app_name = get_setting("app_name", "AI Market Analyst")
    app_env = get_setting("app_env", "unknown")
    run_interval_sec = int(get_setting("run_interval_sec", 900))
    paper_mode = bool(get_setting("paper_mode", True))

    return (
        f"<b>{app_name}</b>\n"
        f"Worker booted.\n"
        f"Env: {app_env}\n"
        f"Interval: {run_interval_sec}s\n"
        f"Paper mode: {'ON' if paper_mode else 'OFF'}"
    )


def worker_stop_message() -> str:
    app_name = get_setting("app_name", "AI Market Analyst")
    return (
        f"<b>{app_name}</b>\n"
        "Worker stopped gracefully."
    )


def worker_failfast_message(cycle_id: str, error_message: str) -> str:
    app_name = get_setting("app_name", "AI Market Analyst")
    return (
        f"<b>{app_name}</b>\n"
        f"FAIL_FAST triggered.\n"
        f"Cycle: {cycle_id}\n"
        f"Error: {error_message}"
    )


def main() -> int:
    setup_logging()

    worker_logger = bind_logger(logger, component="main_worker", cycle_id="-", symbol="-")
    worker_logger.info("Starting main worker...")

    shutdown = GracefulShutdown()
    shutdown.install()

    heartbeat = HeartbeatService()
    deduper = AlertDeduper()
    notifier = TelegramNotifier()

    startup_grace_sec = int(get_setting("startup_grace_sec", 0))
    run_interval_sec = int(get_setting("run_interval_sec", 900))
    enable_weekend_skip = bool(get_setting("enable_weekend_skip", True))
    enable_telegram = bool(get_setting("enable_telegram", False))
    fail_fast = bool(get_setting("fail_fast", False))

    # Critical production behavior:
    # stateful_batch_runner owns trade-alert dispatch.
    # main_worker alert forwarding is OFF by default to prevent duplicate/noisy legacy alerts.
    # If enabled later, process_alerts() still has a hard gate.
    main_worker_alert_forwarding_enabled = bool(
        get_setting("main_worker_alert_forwarding_enabled", False)
    )

    heartbeat.mark_boot()

    if getattr(notifier, "is_active", False):
        try:
            notifier.send_admin_message(worker_boot_message())
        except Exception as exc:
            log_exception(
                worker_logger,
                f"Failed to send worker boot message: {exc}",
                component="main_worker",
                cycle_id="-",
                symbol="-",
            )

    if startup_grace_sec > 0:
        worker_logger.info(f"Startup grace sleep: {startup_grace_sec}s")
        sleep_with_shutdown(startup_grace_sec, shutdown)

    while not shutdown.stop_requested:
        cycle_started_at = utc_now()
        cycle_id = cycle_started_at.isoformat()
        cycle_logger = bind_logger(worker_logger, cycle_id=cycle_id)

        cycle_logger.info("Cycle started.")

        if enable_weekend_skip and is_weekend_utc():
            cycle_logger.info("Weekend skip active. Worker sleeping until next interval.")
            try:
                heartbeat.mark_idle()
            except Exception as exc:
                log_exception(
                    cycle_logger,
                    f"Heartbeat mark_idle failed: {exc}",
                    component="main_worker",
                    cycle_id=cycle_id,
                    symbol="-",
                )
            sleep_with_shutdown(run_interval_sec, shutdown)
            continue

        try:
            heartbeat.mark_cycle_started(cycle_id)
        except Exception as exc:
            log_exception(
                cycle_logger,
                f"Heartbeat mark_cycle_started failed: {exc}",
                component="main_worker",
                cycle_id=cycle_id,
                symbol="-",
            )

        try:
            cycle_result = run_analytics_cycle(cycle_id)
            cycle_result = ensure_cycle_result_shape(cycle_result, cycle_id)

            write_last_cycle_snapshot(cycle_result)

            alert_results: list[dict[str, Any]] = []
            if enable_telegram and main_worker_alert_forwarding_enabled:
                alert_results = process_alerts(
                    cycle_result=cycle_result,
                    notifier=notifier,
                    deduper=deduper,
                )
            else:
                cycle_logger.info(
                    "Main worker alert forwarding disabled. "
                    "Trade alerts are handled by stateful_batch_runner hard gate."
                )

            cycle_result["alert_results"] = alert_results
            cycle_result["worker_meta"] = {
                "cycle_id": cycle_id,
                "finished_at": utc_now_iso(),
                "weekend_skip_enabled": enable_weekend_skip,
                "telegram_enabled": enable_telegram,
                "main_worker_alert_forwarding_enabled": main_worker_alert_forwarding_enabled,
            }

            write_last_cycle_snapshot(cycle_result)

            try:
                heartbeat.mark_cycle_success(cycle_id)
            except Exception as exc:
                log_exception(
                    cycle_logger,
                    f"Heartbeat mark_cycle_success failed: {exc}",
                    component="main_worker",
                    cycle_id=cycle_id,
                    symbol="-",
                )

            instrument_count = len(cycle_result.get("instruments", []))
            cycle_logger.info(
                f"Cycle completed successfully. "
                f"instruments={instrument_count} alerts={len(alert_results)}"
            )

        except Exception as exc:
            error_message = str(exc)

            try:
                heartbeat.mark_cycle_failure(cycle_id, error_message)
            except Exception as hb_exc:
                log_exception(
                    cycle_logger,
                    f"Heartbeat mark_cycle_failure failed: {hb_exc}",
                    component="main_worker",
                    cycle_id=cycle_id,
                    symbol="-",
                )

            log_exception(
                cycle_logger,
                f"Cycle failed: {error_message}",
                component="main_worker",
                cycle_id=cycle_id,
                symbol="-",
            )

            failure_snapshot = {
                "cycle_id": cycle_id,
                "status": "failed",
                "started_at": cycle_id,
                "finished_at": utc_now_iso(),
                "instruments": [],
                "errors": [error_message],
            }

            try:
                write_last_cycle_snapshot(failure_snapshot)
            except Exception as snapshot_exc:
                log_exception(
                    cycle_logger,
                    f"Failed to write failure snapshot: {snapshot_exc}",
                    component="main_worker",
                    cycle_id=cycle_id,
                    symbol="-",
                )

            if fail_fast:
                worker_logger.error("FAIL_FAST=true, stopping worker.")
                if getattr(notifier, "is_active", False):
                    try:
                        notifier.send_admin_message(worker_failfast_message(cycle_id, error_message))
                    except Exception as notify_exc:
                        log_exception(
                            worker_logger,
                            f"Failed to send FAIL_FAST admin message: {notify_exc}",
                            component="main_worker",
                            cycle_id=cycle_id,
                            symbol="-",
                        )
                break

        elapsed_sec = int((utc_now() - cycle_started_at).total_seconds())
        sleep_sec = max(0, run_interval_sec - elapsed_sec)

        cycle_logger.info(
            f"Cycle finished. elapsed={elapsed_sec}s sleep_next={sleep_sec}s"
        )

        if sleep_sec > 0:
            sleep_with_shutdown(sleep_sec, shutdown)

    try:
        heartbeat.mark_stopped()
    except Exception as exc:
        log_exception(
            worker_logger,
            f"Heartbeat mark_stopped failed: {exc}",
            component="main_worker",
            cycle_id="-",
            symbol="-",
        )

    if getattr(notifier, "is_active", False):
        try:
            notifier.send_admin_message(worker_stop_message())
        except Exception as exc:
            log_exception(
                worker_logger,
                f"Failed to send worker stop message: {exc}",
                component="main_worker",
                cycle_id="-",
                symbol="-",
            )

    worker_logger.info("Main worker stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())