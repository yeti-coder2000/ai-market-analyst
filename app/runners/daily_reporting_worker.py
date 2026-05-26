from __future__ import annotations

"""
Scheduled Telegram reporting worker for AI Market Analyst.

Runs independently from the trading/signal worker.

Default schedule in Europe/Kyiv:
- holiday_warning: 07:15
- morning:         08:00
- london_1h:       11:05
- ny_1h:           17:35

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


WORKER_VERSION = "daily-reporting-worker-v1.0"
DEFAULT_TIMEZONE = "Europe/Kyiv"


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
        return int(raw)
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
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"version": WORKER_VERSION, "sent": {}}


def _save_state(state: dict[str, Any]) -> None:
    p = _state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


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


def _schedule() -> list[ScheduledReport]:
    return [
        ScheduledReport(
            "holiday_warning",
            os.getenv("REPORT_TIME_HOLIDAY_WARNING", "07:15"),
            refresh_tpo=False,
        ),
        ScheduledReport(
            "morning",
            os.getenv("REPORT_TIME_MORNING", "08:00"),
            refresh_tpo=False,
        ),
        ScheduledReport(
            "london_1h",
            os.getenv("REPORT_TIME_LONDON_1H", "11:05"),
            refresh_tpo=False,
        ),
        ScheduledReport(
            "ny_1h",
            os.getenv("REPORT_TIME_NY_1H", "17:35"),
            refresh_tpo=False,
        ),
    ]


class Shutdown:
    def __init__(self) -> None:
        self.requested = False

    def install(self) -> None:
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, frame: Any) -> None:
        del signum, frame
        self.requested = True


def _should_send(now_local: datetime, scheduled: ScheduledReport, state: dict[str, Any]) -> bool:
    target = _parse_hhmm(scheduled.hhmm, scheduled.hhmm)
    target_dt = now_local.replace(hour=target.hour, minute=target.minute, second=0, microsecond=0)

    if now_local < target_dt:
        return False

    day_key = now_local.date().isoformat()
    sent = state.get("sent")
    if not isinstance(sent, dict):
        return True

    key = f"{day_key}:{scheduled.report_type}"
    return key not in sent


def _mark_sent(now_local: datetime, scheduled: ScheduledReport, result: dict[str, Any], state: dict[str, Any]) -> None:
    sent = state.setdefault("sent", {})
    if not isinstance(sent, dict):
        state["sent"] = sent = {}

    day_key = now_local.date().isoformat()
    key = f"{day_key}:{scheduled.report_type}"

    sent[key] = {
        "report_type": scheduled.report_type,
        "scheduled_time": scheduled.hhmm,
        "sent_at_local": now_local.isoformat(),
        "result": result,
    }
    state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    _save_state(state)


def run_due_reports_once(*, dry_run: bool = False) -> list[dict[str, Any]]:
    timezone_name = os.getenv("REPORT_TIMEZONE", DEFAULT_TIMEZONE)
    now_local = datetime.now(timezone.utc).astimezone(ZoneInfo(timezone_name))
    state = _load_state()
    results: list[dict[str, Any]] = []

    for scheduled in _schedule():
        if not _should_send(now_local, scheduled, state):
            continue

        result = send_daily_report(
            report_type=scheduled.report_type,
            report_date=now_local.date().isoformat(),
            timezone_name=timezone_name,
            dry_run=dry_run,
            refresh=True,
            include_tpo_refresh=scheduled.refresh_tpo,
        ).to_dict()

        results.append(result)

        if result.get("status") == "ok":
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
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        return 0 if result.status == "ok" else 1

    shutdown = Shutdown()
    shutdown.install()

    print(
        json.dumps(
            {
                "version": WORKER_VERSION,
                "status": "started",
                "timezone": args.timezone,
                "schedule": [s.__dict__ for s in _schedule()],
                "state_path": str(_state_path()),
                "dry_run": args.dry_run,
                "run_once": args.run_once,
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    while not shutdown.requested:
        results = run_due_reports_once(dry_run=args.dry_run)
        if results:
            print(json.dumps({"sent_reports": results}, ensure_ascii=False, indent=2))

        if args.run_once:
            break

        for _ in range(max(1, args.sleep_sec)):
            if shutdown.requested:
                break
            time.sleep(1)

    print(json.dumps({"version": WORKER_VERSION, "status": "stopped"}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())