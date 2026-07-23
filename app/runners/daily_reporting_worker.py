from __future__ import annotations

"""
Scheduled Telegram reporting worker for AI Market Analyst.

Runs independently from the trading/signal worker.

Production schedule:
- positioning_japan_open: 09:00 Asia/Tokyo (silent operational baseline)
- morning_combined: 08:05 Europe/Berlin (Frankfurt open)
- london_1h:       09:00 Europe/London (London open +1h)
- daily_close:      16:15 America/New_York (completed NY close bar)

The legacy NY report remains callable manually and can be re-enabled by the
new explicit ENABLE_LEGACY_NY_REPORT flag. The old ENABLE_NY_REPORT setting is
intentionally ignored so a stale Render environment cannot silently restore
the retired production schedule.

Weekend policy:
- By default, Saturday/Sunday full London/NY reporting is skipped.
- Optional guarded crypto-only/health report can be enabled with:
    ENABLE_WEEKEND_CRYPTO_ONLY_REPORT=true
    REPORT_TYPE_WEEKEND_CRYPTO=crypto_health
    REPORT_TIME_WEEKEND_CRYPTO=11:05

Each report is sent once per local date. State is stored in:
  runtime/reporting/daily_reporting_state.json

Recommended Render command:
  python -m app.runners.daily_reporting_worker
"""

import json
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    from app.core.settings import settings
except Exception:  # pragma: no cover
    settings = None  # type: ignore[assignment]

from app.services.telegram_daily_reporter import send_daily_report


WORKER_VERSION = "daily-reporting-worker-v1.4-three-checkpoints"
DEFAULT_TIMEZONE = "Europe/Kyiv"
DEFAULT_JAPAN_TIMEZONE = "Asia/Tokyo"
DEFAULT_FRANKFURT_TIMEZONE = "Europe/Berlin"
DEFAULT_LONDON_TIMEZONE = "Europe/London"
DEFAULT_NY_TIMEZONE = "America/New_York"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def _runtime_dir() -> Path:
    raw = os.getenv("RUNTIME_DIR")
    if raw:
        return Path(raw).expanduser().resolve()

    if settings is not None:
        value = getattr(settings, "runtime_dir", None)
        if value:
            return Path(value).expanduser().resolve()

    render_runtime = Path("/var/data/runtime")
    if render_runtime.exists():
        return render_runtime

    return Path("runtime").resolve()


def _state_path() -> Path:
    raw = os.getenv("DAILY_REPORTING_STATE_PATH")
    if raw:
        return Path(raw).expanduser().resolve()
    return _runtime_dir() / "reporting" / "daily_reporting_state.json"


def _load_state() -> dict[str, Any]:
    p = _state_path()
    try:
        if p.exists():
            loaded = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                sent = loaded.get("sent")
                if not isinstance(sent, dict):
                    loaded["sent"] = {}
                loaded["version"] = WORKER_VERSION
                return loaded
    except Exception:
        pass
    return {"version": WORKER_VERSION, "sent": {}}


def _save_state(state: dict[str, Any]) -> None:
    p = _state_path()
    p.parent.mkdir(parents=True, exist_ok=True)

    state["version"] = WORKER_VERSION
    state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()

    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _parse_hhmm(raw: str, default: str) -> dtime:
    value = (raw or default).strip()
    try:
        h, m = [int(x) for x in value.split(":", 1)]
        return dtime(hour=h, minute=m)
    except Exception:
        h, m = [int(x) for x in default.split(":", 1)]
        return dtime(hour=h, minute=m)


@dataclass(frozen=True)
class ScheduledReport:
    report_type: str
    hhmm: str
    refresh_tpo: bool = False
    weekend_only: bool = False
    schedule_timezone: str | None = None
    delivery_mode: str = "briefing"
    max_lateness_minutes: int | None = None


def _is_weekend(now_local: datetime) -> bool:
    return now_local.weekday() >= 5


def _weekday_schedule() -> list[ScheduledReport]:
    reports = [
        ScheduledReport(
            "positioning_japan_open",
            os.getenv("REPORT_TIME_POSITIONING_JAPAN_OPEN", "09:00"),
            schedule_timezone=os.getenv(
                "REPORT_POSITIONING_JAPAN_TIMEZONE",
                DEFAULT_JAPAN_TIMEZONE,
            ),
            delivery_mode="positioning_checkpoint",
            max_lateness_minutes=_env_int(
                "REPORT_POSITIONING_JAPAN_MAX_LATENESS_MINUTES",
                30,
            ),
        ),
        ScheduledReport(
            "morning_combined",
            os.getenv("REPORT_TIME_MORNING_COMBINED", "08:05"),
            refresh_tpo=_env_bool("REPORT_REFRESH_TPO_MORNING_COMBINED", False),
            schedule_timezone=os.getenv(
                "REPORT_MORNING_TIMEZONE",
                DEFAULT_FRANKFURT_TIMEZONE,
            ),
        ),
        ScheduledReport(
            "london_1h",
            os.getenv("REPORT_TIME_LONDON_1H", "09:00"),
            refresh_tpo=_env_bool("REPORT_REFRESH_TPO_LONDON_1H", True),
            schedule_timezone=os.getenv(
                "REPORT_LONDON_1H_TIMEZONE",
                DEFAULT_LONDON_TIMEZONE,
            ),
        ),
    ]

    if _env_bool("ENABLE_DAILY_CLOSE_REPORT", True):
        reports.append(
            ScheduledReport(
                "daily_close",
                os.getenv("REPORT_TIME_DAILY_CLOSE", "16:15"),
                refresh_tpo=_env_bool("REPORT_REFRESH_TPO_DAILY_CLOSE", True),
                schedule_timezone=os.getenv(
                    "REPORT_DAILY_CLOSE_TIMEZONE",
                    DEFAULT_NY_TIMEZONE,
                ),
            )
        )

    # Reversible compatibility switch. London Focus v1 defaults this to off;
    # the NY renderer and all US session/macro code remain available.
    if _env_bool("ENABLE_LEGACY_NY_REPORT", False):
        reports.append(
            ScheduledReport(
                "ny_1h",
                os.getenv("REPORT_TIME_NY_1H", "17:35"),
                refresh_tpo=_env_bool("REPORT_REFRESH_TPO_NY_1H", False),
                schedule_timezone=os.getenv("REPORT_TIMEZONE", DEFAULT_TIMEZONE),
            )
        )

    return reports


def _weekend_schedule() -> list[ScheduledReport]:
    if not _env_bool("ENABLE_WEEKEND_CRYPTO_ONLY_REPORT", False):
        return []

    return [
        ScheduledReport(
            os.getenv("REPORT_TYPE_WEEKEND_CRYPTO", "crypto_health"),
            os.getenv("REPORT_TIME_WEEKEND_CRYPTO", "11:05"),
            refresh_tpo=_env_bool("REPORT_REFRESH_TPO_WEEKEND_CRYPTO", False),
            weekend_only=True,
            schedule_timezone=os.getenv("REPORT_TIMEZONE", DEFAULT_TIMEZONE),
        )
    ]


def _schedule(now_local: datetime | None = None) -> list[ScheduledReport]:
    timezone_name = os.getenv("REPORT_TIMEZONE", DEFAULT_TIMEZONE)
    current = now_local or datetime.now(timezone.utc).astimezone(ZoneInfo(timezone_name))

    if _is_weekend(current):
        return _weekend_schedule()

    return _weekday_schedule()


class Shutdown:
    def __init__(self) -> None:
        self.requested = False

    def install(self) -> None:
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, frame: Any) -> None:
        del signum, frame
        self.requested = True


def _state_key(now_local: datetime, scheduled: ScheduledReport) -> str:
    scheduled_now = _scheduled_local_time(now_local, scheduled)
    day_key = scheduled_now.date().isoformat()
    return f"{day_key}:{scheduled.report_type}"


def _scheduled_local_time(now_local: datetime, scheduled: ScheduledReport) -> datetime:
    timezone_name = scheduled.schedule_timezone or os.getenv("REPORT_TIMEZONE", DEFAULT_TIMEZONE)
    try:
        zone = ZoneInfo(timezone_name)
    except Exception:
        zone = ZoneInfo(DEFAULT_TIMEZONE)

    if now_local.tzinfo is None:
        return now_local.replace(tzinfo=zone)
    return now_local.astimezone(zone)


def _should_send(now_local: datetime, scheduled: ScheduledReport, state: dict[str, Any]) -> bool:
    now_local = _scheduled_local_time(now_local, scheduled)
    target = _parse_hhmm(scheduled.hhmm, scheduled.hhmm)
    target_dt = now_local.replace(hour=target.hour, minute=target.minute, second=0, microsecond=0)

    if now_local < target_dt:
        return False
    if (
        scheduled.max_lateness_minutes is not None
        and (now_local - target_dt).total_seconds() > scheduled.max_lateness_minutes * 60
    ):
        return False

    sent = state.get("sent")
    if not isinstance(sent, dict):
        return True

    return _state_key(now_local, scheduled) not in sent


def _run_positioning_checkpoint(
    *,
    scheduled: ScheduledReport,
    report_date: str,
) -> dict[str, Any]:
    from app.services.telegram_daily_reporter import _refresh_positioning_runtime

    runtime_dir = os.getenv("POSITIONING_RUNTIME_DIR") or os.getenv("RUNTIME_DIR")
    result = _refresh_positioning_runtime(
        runtime_dir=runtime_dir,
        report_date=report_date,
        report_type=scheduled.report_type,
    )
    payload = result.get("result") if isinstance(result.get("result"), dict) else {}
    operational_status = str(
        payload.get("operational_positioning_status") or ""
    ).upper()
    baseline_captured = operational_status == "JAPAN_BASELINE_CAPTURED"
    return {
        "version": WORKER_VERSION,
        "status": (
            "ok"
            if result.get("returncode") == 0 and baseline_captured
            else "retryable_no_baseline"
        ),
        "report_type": scheduled.report_type,
        "report_date": report_date,
        "delivery_mode": scheduled.delivery_mode,
        "telegram_sent": False,
        "operational_positioning_status": operational_status or None,
        "positioning_refresh": result,
        "error_message": result.get("error_message"),
    }


def _mark_sent(
    now_local: datetime,
    scheduled: ScheduledReport,
    result: dict[str, Any],
    state: dict[str, Any],
) -> None:
    now_local = _scheduled_local_time(now_local, scheduled)
    sent = state.setdefault("sent", {})
    if not isinstance(sent, dict):
        state["sent"] = sent = {}

    sent[_state_key(now_local, scheduled)] = {
        "report_type": scheduled.report_type,
        "scheduled_time": scheduled.hhmm,
        "schedule_timezone": scheduled.schedule_timezone,
        "sent_at_local": now_local.isoformat(),
        "weekend_only": scheduled.weekend_only,
        "result": result,
    }
    _save_state(state)


def run_due_reports_once(
    *,
    dry_run: bool = False,
    timezone_name: str | None = None,
) -> list[dict[str, Any]]:
    tz_name = timezone_name or os.getenv("REPORT_TIMEZONE", DEFAULT_TIMEZONE)
    now_local = datetime.now(timezone.utc).astimezone(ZoneInfo(tz_name))
    state = _load_state()
    results: list[dict[str, Any]] = []

    for scheduled in _schedule(now_local):
        if not _should_send(now_local, scheduled, state):
            continue

        scheduled_now = _scheduled_local_time(now_local, scheduled)
        report_date = scheduled_now.date().isoformat()

        try:
            if scheduled.delivery_mode == "positioning_checkpoint":
                result = _run_positioning_checkpoint(
                    scheduled=scheduled,
                    report_date=report_date,
                )
            else:
                result = send_daily_report(
                    report_type=scheduled.report_type,
                    report_date=report_date,
                    timezone_name=tz_name,
                    dry_run=dry_run,
                    refresh=True,
                    include_tpo_refresh=scheduled.refresh_tpo,
                ).to_dict()
        except Exception as exc:
            result = {
                "version": WORKER_VERSION,
                "status": "error",
                "report_type": scheduled.report_type,
                "report_date": report_date,
                "telegram_sent": False,
                "dry_run": dry_run,
                "error_message": f"{type(exc).__name__}: {exc}",
            }

        results.append(result)

        if not dry_run and result.get("status") == "ok":
            _mark_sent(now_local, scheduled, result, state)

    return results


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run scheduled AI Market Analyst Telegram reports.")
    parser.add_argument("--run-once", action="store_true", default=_env_bool("RUN_ONCE", False))
    parser.add_argument("--dry-run", action="store_true", default=_env_bool("REPORT_DRY_RUN", False))
    parser.add_argument("--type", default=os.getenv("REPORT_TYPE"))
    parser.add_argument("--date", default=os.getenv("REPORT_DATE"))
    parser.add_argument("--timezone", default=os.getenv("REPORT_TIMEZONE", DEFAULT_TIMEZONE))
    parser.add_argument("--sleep-sec", type=int, default=_env_int("DAILY_REPORTING_POLL_SEC", 60))

    args = parser.parse_args()

    if args.type:
        result = send_daily_report(
            report_type=args.type,
            report_date=args.date,
            timezone_name=args.timezone,
            dry_run=args.dry_run,
            refresh=True,
            include_tpo_refresh=_env_bool("REPORT_REFRESH_TPO", False),
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2), flush=True)
        return 0 if result.status == "ok" else 1

    shutdown = Shutdown()
    shutdown.install()

    startup_now_local = datetime.now(timezone.utc).astimezone(ZoneInfo(args.timezone))
    print(
        json.dumps(
            {
                "version": WORKER_VERSION,
                "status": "started",
                "timezone": args.timezone,
                "schedule": [s.__dict__ for s in _schedule(startup_now_local)],
                "state_path": str(_state_path()),
                "dry_run": args.dry_run,
                "run_once": args.run_once,
                "weekend_mode": _is_weekend(startup_now_local),
                "weekend_crypto_only_enabled": _env_bool("ENABLE_WEEKEND_CRYPTO_ONLY_REPORT", False),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )

    while not shutdown.requested:
        results = run_due_reports_once(dry_run=args.dry_run, timezone_name=args.timezone)
        if results:
            print(json.dumps({"sent_reports": results}, ensure_ascii=False, indent=2), flush=True)

        if args.run_once:
            break

        for _ in range(max(1, args.sleep_sec)):
            if shutdown.requested:
                break
            time.sleep(1)

    print(json.dumps({"version": WORKER_VERSION, "status": "stopped"}, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
