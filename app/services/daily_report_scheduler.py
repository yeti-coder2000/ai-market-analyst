from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.core.settings import settings


# =============================================================================
# DAILY REPORT SCHEDULER v1
# =============================================================================
# Purpose:
# - Run Telegram Daily Report automatically inside the SAME Render Background Worker.
# - Use the same /var/data persistent disk as the live worker.
# - Avoid Render Cron Job because Render Cron Jobs cannot access persistent disks.
# - Keep this process lightweight: sleep loop + subprocess calls once per day.
#
# Intended Render start command:
#
#   bash -lc 'python -m app.services.daily_report_scheduler & exec python -m app.runners.multi_group_worker'
#
# Default schedule:
# - 21:30 Europe/Kyiv
#
# Env vars:
# - ENABLE_DAILY_REPORT_SCHEDULER=true/false
# - DAILY_REPORT_TIME=21:30
# - DAILY_REPORT_TIMEZONE=Europe/Kyiv
# - DAILY_REPORT_DATE_ARG=today
# - DAILY_REPORT_DRY_RUN=false
# - DAILY_REPORT_DISABLE_NOTIFICATION=false
# - DAILY_REPORT_RUN_ON_START=false
# - DAILY_REPORT_SLEEP_SEC=60
# - DAILY_REPORT_COMMAND_TIMEOUT_SEC=300
# =============================================================================


STATE_DIR = settings.runtime_dir / "state"
STATE_PATH = STATE_DIR / "daily_report_scheduler_state.json"
LOCK_PATH = STATE_DIR / "daily_report_scheduler.lock"

DEFAULT_TIMEZONE = "Europe/Kyiv"
DEFAULT_REPORT_TIME = "21:30"
DEFAULT_DATE_ARG = "today"


@dataclass(frozen=True)
class SchedulerConfig:
    enabled: bool
    timezone_name: str
    report_time: dtime
    date_arg: str
    dry_run: bool
    disable_notification: bool
    run_on_start: bool
    sleep_sec: int
    command_timeout_sec: int


# =============================================================================
# HELPERS
# =============================================================================


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(message: str) -> None:
    print(
        f"{datetime.now(timezone.utc).isoformat()} | "
        f"component=daily_report_scheduler | {message}",
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
        value = int(str(raw).strip())
    except ValueError:
        return default

    return value


def get_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        log(f"Unknown timezone '{name}', falling back to UTC.")
        return ZoneInfo("UTC")


def parse_report_time(value: str) -> dtime:
    text = str(value or DEFAULT_REPORT_TIME).strip()

    try:
        hh, mm = text.split(":", 1)
        hour = int(hh)
        minute = int(mm)

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError

        return dtime(hour=hour, minute=minute)
    except Exception:
        log(f"Invalid DAILY_REPORT_TIME='{value}', using {DEFAULT_REPORT_TIME}.")
        return dtime(hour=21, minute=30)


def load_config() -> SchedulerConfig:
    return SchedulerConfig(
        enabled=env_bool("ENABLE_DAILY_REPORT_SCHEDULER", True),
        timezone_name=os.getenv("DAILY_REPORT_TIMEZONE", DEFAULT_TIMEZONE).strip()
        or DEFAULT_TIMEZONE,
        report_time=parse_report_time(os.getenv("DAILY_REPORT_TIME", DEFAULT_REPORT_TIME)),
        date_arg=os.getenv("DAILY_REPORT_DATE_ARG", DEFAULT_DATE_ARG).strip()
        or DEFAULT_DATE_ARG,
        dry_run=env_bool("DAILY_REPORT_DRY_RUN", False),
        disable_notification=env_bool("DAILY_REPORT_DISABLE_NOTIFICATION", False),
        run_on_start=env_bool("DAILY_REPORT_RUN_ON_START", False),
        sleep_sec=max(10, env_int("DAILY_REPORT_SLEEP_SEC", 60)),
        command_timeout_sec=max(60, env_int("DAILY_REPORT_COMMAND_TIMEOUT_SEC", 300)),
    )


def ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def load_state() -> dict[str, Any]:
    ensure_state_dir()

    if not STATE_PATH.exists():
        return {}

    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

    return data if isinstance(data, dict) else {}


def save_state(state: dict[str, Any]) -> None:
    ensure_state_dir()

    tmp_path = STATE_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp_path.replace(STATE_PATH)


def acquire_lock() -> bool:
    ensure_state_dir()

    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "created_at_utc": utc_now(),
                    },
                    ensure_ascii=False,
                )
            )
        return True
    except FileExistsError:
        return False


def release_lock() -> None:
    try:
        LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def local_date_text(now_local: datetime) -> str:
    return now_local.date().isoformat()


def should_run_now(
    *,
    now_local: datetime,
    cfg: SchedulerConfig,
    state: dict[str, Any],
) -> bool:
    today = local_date_text(now_local)
    last_success_date = str(state.get("last_success_local_date") or "")

    if last_success_date == today:
        return False

    target = now_local.replace(
        hour=cfg.report_time.hour,
        minute=cfg.report_time.minute,
        second=0,
        microsecond=0,
    )

    return now_local >= target


def seconds_until_next_check(
    *,
    now_local: datetime,
    cfg: SchedulerConfig,
    state: dict[str, Any],
) -> int:
    today = local_date_text(now_local)
    last_success_date = str(state.get("last_success_local_date") or "")

    target_today = now_local.replace(
        hour=cfg.report_time.hour,
        minute=cfg.report_time.minute,
        second=0,
        microsecond=0,
    )

    if last_success_date == today or now_local >= target_today:
        target = target_today + timedelta(days=1)
    else:
        target = target_today

    delta_sec = int((target - now_local).total_seconds())

    if delta_sec <= 0:
        return cfg.sleep_sec

    return min(cfg.sleep_sec, max(10, delta_sec))


# =============================================================================
# PIPELINE RUNNER
# =============================================================================


def run_command(args: list[str], *, timeout_sec: int) -> dict[str, Any]:
    log(f"Running command: {' '.join(args)}")

    started = time.time()

    try:
        proc = subprocess.run(
            args,
            text=True,
            capture_output=True,
            timeout=timeout_sec,
            check=False,
        )

        elapsed = round(time.time() - started, 2)

        if proc.stdout:
            print(proc.stdout.strip(), flush=True)

        if proc.stderr:
            print(proc.stderr.strip(), flush=True)

        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
            "stdout_tail": proc.stdout[-2000:] if proc.stdout else "",
            "stderr_tail": proc.stderr[-2000:] if proc.stderr else "",
            "args": args,
        }

    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "error": f"timeout after {timeout_sec}s",
            "args": args,
            "stdout_tail": (exc.stdout or "")[-2000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-2000:] if isinstance(exc.stderr, str) else "",
        }

    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "args": args,
        }


def build_sender_command(cfg: SchedulerConfig) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "app.services.telegram_daily_report_sender",
        "--date",
        cfg.date_arg,
    ]

    if cfg.dry_run:
        cmd.append("--dry-run")

    if cfg.disable_notification:
        cmd.append("--disable-notification")

    return cmd


def run_daily_report_pipeline(cfg: SchedulerConfig) -> dict[str, Any]:
    commands = [
        [sys.executable, "-m", "app.services.signal_outcome_tracker"],
        [sys.executable, "-m", "app.services.signal_quality_tiers"],
        build_sender_command(cfg),
    ]

    results: list[dict[str, Any]] = []

    for cmd in commands:
        result = run_command(cmd, timeout_sec=cfg.command_timeout_sec)
        results.append(result)

        if not result.get("ok"):
            return {
                "ok": False,
                "failed_command": cmd,
                "results": results,
            }

    return {
        "ok": True,
        "results": results,
    }


def run_once(cfg: SchedulerConfig, *, reason: str) -> dict[str, Any]:
    tz = get_timezone(cfg.timezone_name)
    now_local = datetime.now(timezone.utc).astimezone(tz)
    today = local_date_text(now_local)

    log(f"Daily report run started. reason={reason} local_date={today}")

    if not acquire_lock():
        log("Another daily report run is already active. Skipping.")
        return {
            "ok": False,
            "skipped": True,
            "reason": "lock_exists",
        }

    try:
        state = load_state()
        state["last_attempt_at_utc"] = utc_now()
        state["last_attempt_local_date"] = today
        state["last_attempt_reason"] = reason
        save_state(state)

        result = run_daily_report_pipeline(cfg)

        state = load_state()

        if result.get("ok"):
            state["last_success_at_utc"] = utc_now()
            state["last_success_local_date"] = today
            state["last_success_reason"] = reason
            state["last_success_result"] = result
            log(f"Daily report run finished successfully. local_date={today}")
        else:
            state["last_failure_at_utc"] = utc_now()
            state["last_failure_local_date"] = today
            state["last_failure_reason"] = reason
            state["last_failure_result"] = result
            log(f"Daily report run failed. local_date={today}")

        save_state(state)

        return result

    finally:
        release_lock()


# =============================================================================
# MAIN LOOP
# =============================================================================


def main() -> None:
    cfg = load_config()

    log(
        "Scheduler boot. "
        f"enabled={cfg.enabled} "
        f"timezone={cfg.timezone_name} "
        f"report_time={cfg.report_time.strftime('%H:%M')} "
        f"date_arg={cfg.date_arg} "
        f"dry_run={cfg.dry_run}"
    )

    if not cfg.enabled:
        log("Scheduler disabled by ENABLE_DAILY_REPORT_SCHEDULER=false.")
        return

    tz = get_timezone(cfg.timezone_name)

    if cfg.run_on_start:
        run_once(cfg, reason="startup")

    while True:
        state = load_state()
        now_local = datetime.now(timezone.utc).astimezone(tz)

        if should_run_now(now_local=now_local, cfg=cfg, state=state):
            run_once(cfg, reason="scheduled")

            # Reload after run and avoid tight loop.
            time.sleep(cfg.sleep_sec)
            continue

        sleep_for = seconds_until_next_check(
            now_local=now_local,
            cfg=cfg,
            state=state,
        )

        log(
            "Scheduler sleeping. "
            f"local_now={now_local.isoformat()} "
            f"sleep_sec={sleep_for}"
        )

        time.sleep(sleep_for)


if __name__ == "__main__":
    main()