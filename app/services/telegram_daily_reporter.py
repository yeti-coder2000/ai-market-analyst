from __future__ import annotations

"""
Telegram daily/session report sender for AI Market Analyst.

This module sends read-only intelligence reports. It does NOT use the trade alert
Battle Gate and does NOT create signals.

Report types:
- morning
- holiday_warning
- london_1h
- ny_1h
"""

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.services.daily_market_briefing import (
    build_briefing_report,
    render_briefing_text,
    write_briefing_artifacts,
)
from app.services.telegram_notifier import TelegramNotifier


REPORTER_VERSION = "telegram-daily-reporter-v1.0"


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


@dataclass
class ReporterResult:
    status: str
    report_type: str
    report_date: str | None
    telegram_sent: bool
    dry_run: bool
    message_length: int
    artifact_json: str | None
    artifact_text: str | None
    refresh_results: list[dict[str, Any]]
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": REPORTER_VERSION,
            "status": self.status,
            "report_type": self.report_type,
            "report_date": self.report_date,
            "telegram_sent": self.telegram_sent,
            "dry_run": self.dry_run,
            "message_length": self.message_length,
            "artifact_json": self.artifact_json,
            "artifact_text": self.artifact_text,
            "refresh_results": self.refresh_results,
            "error_message": self.error_message,
        }


def _run_module(module: str, *, timeout_sec: int) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            [sys.executable, "-m", module],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
        return {
            "module": module,
            "returncode": completed.returncode,
            "ok": completed.returncode == 0,
            "stdout_tail": (completed.stdout or "")[-3000:],
            "stderr_tail": (completed.stderr or "")[-3000:],
            "error_message": None,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "module": module,
            "returncode": None,
            "ok": False,
            "stdout_tail": (exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or ""))[-3000:],
            "stderr_tail": (exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or ""))[-3000:],
            "error_message": f"timeout_after_{timeout_sec}s",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "module": module,
            "returncode": None,
            "ok": False,
            "stdout_tail": "",
            "stderr_tail": "",
            "error_message": str(exc),
        }


def refresh_runtime_artifacts(*, include_tpo: bool | None = None) -> list[dict[str, Any]]:
    """
    Refresh stats before sending reports.

    TPO is usually refreshed by multi_group_worker every cycle, so default is false
    to avoid unnecessary provider pressure. Enable via:
      REPORT_REFRESH_TPO=true
    """
    timeout_sec = _env_int("REPORT_REFRESH_TIMEOUT_SEC", 180)

    if include_tpo is None:
        include_tpo = _env_bool("REPORT_REFRESH_TPO", False)

    modules: list[str] = []

    if include_tpo:
        modules.append("app.services.tpo_context_exporter")

    if _env_bool("REPORT_REFRESH_OUTCOMES", True):
        modules.append("app.services.signal_outcome_tracker")

    if _env_bool("REPORT_REFRESH_STATISTICS", True):
        modules.append("app.services.lightweight_statistics_exporter")

    return [_run_module(module, timeout_sec=timeout_sec) for module in modules]


def send_daily_report(
    *,
    report_type: str = "morning",
    report_date: str | None = None,
    timezone_name: str | None = None,
    dry_run: bool = False,
    refresh: bool = True,
    include_tpo_refresh: bool | None = None,
) -> ReporterResult:
    refresh_results: list[dict[str, Any]] = []

    if refresh:
        refresh_results = refresh_runtime_artifacts(include_tpo=include_tpo_refresh)

    report = build_briefing_report(
        report_type=report_type,
        report_date=report_date,
        timezone_name=timezone_name,
    )
    message = render_briefing_text(report)
    json_path, txt_path = write_briefing_artifacts(report)

    sent = False

    if not dry_run:
        notifier = TelegramNotifier()
        sent = notifier.send_text(message)

    return ReporterResult(
        status="ok" if dry_run or sent else "telegram_send_failed",
        report_type=report.report_type,
        report_date=report.report_date,
        telegram_sent=sent,
        dry_run=dry_run,
        message_length=len(message),
        artifact_json=str(json_path),
        artifact_text=str(txt_path),
        refresh_results=refresh_results,
        error_message=None if dry_run or sent else "telegram_send_failed",
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Send AI Market Analyst daily/session Telegram report.")
    parser.add_argument("--type", default=os.getenv("REPORT_TYPE", "morning"))
    parser.add_argument("--date", default=os.getenv("REPORT_DATE"))
    parser.add_argument("--timezone", default=os.getenv("REPORT_TIMEZONE"))
    parser.add_argument("--dry-run", action="store_true", default=_env_bool("REPORT_DRY_RUN", False))
    parser.add_argument("--no-refresh", action="store_true")
    parser.add_argument("--refresh-tpo", action="store_true", default=_env_bool("REPORT_REFRESH_TPO", False))
    parser.add_argument("--print-message", action="store_true")

    args = parser.parse_args()

    result = send_daily_report(
        report_type=args.type,
        report_date=args.date,
        timezone_name=args.timezone,
        dry_run=args.dry_run,
        refresh=not args.no_refresh,
        include_tpo_refresh=args.refresh_tpo,
    )

    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))

    if args.print_message and result.artifact_text:
        path = Path(result.artifact_text)
        if path.exists():
            print(path.read_text(encoding="utf-8"))

    return 0 if result.status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())