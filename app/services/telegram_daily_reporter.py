from __future__ import annotations

"""
Telegram daily/session report sender for AI Market Analyst.

This module sends read-only intelligence reports. It does NOT use the trade alert
Battle Gate and does NOT create signals.

Report types:
- morning
- holiday_warning
- london_1h
- london_close
- ny_1h
"""

import json
import copy
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from app.services.daily_market_briefing import (
    build_briefing_report,
    render_briefing_text,
    write_briefing_artifacts,
)
from app.services.telegram_notifier import TelegramNotifier


REPORTER_VERSION = "telegram-daily-reporter-v1.4-london-operational-positioning"


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


def _resolve_report_date(report_date: str | None, timezone_name: str | None) -> str:
    explicit = str(report_date or "").strip()
    if explicit:
        return explicit

    timezone_value = str(
        timezone_name
        or os.getenv("REPORT_TIMEZONE")
        or "Europe/Kyiv"
    ).strip()
    try:
        return datetime.now(ZoneInfo(timezone_value)).date().isoformat()
    except Exception:
        return datetime.now(ZoneInfo("Europe/Kyiv")).date().isoformat()


def _refresh_positioning_runtime(
    *,
    runtime_dir: str | None,
    report_date: str,
    report_type: str,
) -> dict[str, Any]:
    try:
        from app.services.positioning.positioning_pipeline import (
            POSITIONING_PIPELINE_VERSION,
            refresh_positioning_runtime,
        )

        result = refresh_positioning_runtime(
            runtime_dir=runtime_dir,
            report_date=report_date,
            report_type=report_type,
        )
        return {
            "module": "app.services.positioning.positioning_pipeline",
            "version": POSITIONING_PIPELINE_VERSION,
            "returncode": 0 if result.get("status") != "ERROR" else 1,
            "ok": bool(result.get("ok")),
            "result": result,
            "error_message": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "module": "app.services.positioning.positioning_pipeline",
            "returncode": None,
            "ok": False,
            "result": None,
            "error_message": f"{type(exc).__name__}: {exc}",
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }


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
    telegram_main_parts: int = 0
    telegram_main_part_lengths: list[int] | None = None
    positioning_delivery: dict[str, Any] | None = None
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
            "telegram_main_parts": self.telegram_main_parts,
            "telegram_main_part_lengths": self.telegram_main_part_lengths or [],
            "artifact_json": self.artifact_json,
            "artifact_text": self.artifact_text,
            "refresh_results": self.refresh_results,
            "positioning_delivery": self.positioning_delivery,
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




TELEGRAM_MAIN_MESSAGE_SAFE_LIMIT = 3900


def _split_main_telegram_message(text: str, max_chars: int | None = None) -> list[str]:
    """
    Split main daily/session Telegram briefing below Telegram hard limit.

    Telegram sendMessage hard limit is 4096 chars. We use 3900 by default.
    Prefer paragraph/line boundaries and hard-split only as final fallback.
    """
    limit = max_chars or _env_int("REPORT_MAIN_TELEGRAM_SPLIT_LIMIT", TELEGRAM_MAIN_MESSAGE_SAFE_LIMIT)
    raw = str(text or "").strip()

    if not raw:
        return []

    if len(raw) <= limit:
        return [raw]

    chunks: list[str] = []
    current = ""

    for block in raw.split("\n\n"):
        block = block.strip()
        if not block:
            continue

        candidate = block if not current else current + "\n\n" + block

        if len(candidate) <= limit:
            current = candidate
            continue

        if current:
            chunks.append(current)
            current = ""

        if len(block) <= limit:
            current = block
            continue

        for line_chunk in _split_main_long_block(block, limit=limit):
            if len(line_chunk) <= limit:
                chunks.append(line_chunk)
            else:
                chunks.extend(_hard_split_text(line_chunk, limit=limit))

    if current:
        chunks.append(current)

    return [chunk for chunk in chunks if chunk.strip()]


def _split_main_long_block(block: str, limit: int) -> list[str]:
    chunks: list[str] = []
    current = ""

    for line in str(block or "").splitlines():
        line = line.rstrip()

        if len(line) > limit:
            if current:
                chunks.append(current)
                current = ""
            chunks.extend(_hard_split_text(line, limit=limit))
            continue

        candidate = line if not current else current + "\n" + line

        if len(candidate) <= limit:
            current = candidate
        else:
            if current:
                chunks.append(current)
            current = line

    if current:
        chunks.append(current)

    return chunks


def _hard_split_text(text: str, limit: int) -> list[str]:
    raw = str(text or "")
    if len(raw) <= limit:
        return [raw]

    out: list[str] = []
    start = 0
    while start < len(raw):
        out.append(raw[start:start + limit])
        start += limit
    return out


def _send_main_telegram_message(
    notifier: TelegramNotifier,
    message: str,
    *,
    split_limit: int | None = None,
) -> tuple[bool, list[int]]:
    """
    Send main market briefing in safe chunks.

    Returns:
    - overall send status
    - list of sent/attempted chunk lengths
    """
    chunks = _split_main_telegram_message(message, max_chars=split_limit)

    if not chunks:
        return False, []

    total = len(chunks)
    lengths: list[int] = []

    for idx, chunk in enumerate(chunks, start=1):
        if total > 1:
            chunk_text = f"{chunk}\n\n<i>Main briefing part {idx}/{total}</i>"
        else:
            chunk_text = chunk

        lengths.append(len(chunk_text))

        if not notifier.send_text(chunk_text):
            return False, lengths

    return True, lengths



def _env_csv_set(name: str, default: str) -> set[str]:
    raw = os.getenv(name, default)
    return {
        part.strip()
        for part in str(raw or "").split(",")
        if part.strip()
    }


def _positioning_delivery_enabled(report_type: str, explicit: bool | None = None) -> bool:
    if explicit is not None:
        return bool(explicit)

    if not _env_bool("REPORT_SEND_POSITIONING_TELEGRAM", True):
        return False

    normalized = str(report_type or "").strip().lower()
    if normalized in {"london_close", "london_close_briefing"}:
        return _env_bool("REPORT_SEND_LONDON_CLOSE_POSITIONING", True)

    allowed = _env_csv_set(
        "REPORT_POSITIONING_TYPES",
        "morning,morning_combined,london_close,crypto_health",
    )
    return normalized in allowed


def _briefing_report_for_main_telegram(report: Any) -> Any:
    """
    Keep full Positioning/COT out of the main Telegram market briefing.

    The full Positioning Intelligence / COT / Daily Participation Proxy report
    is sent separately as a second Telegram message. This keeps the main market
    briefing below Telegram limits and avoids duplicated long context.
    """
    if not _env_bool("REPORT_TELEGRAM_EXCLUDE_POSITIONING_SECTIONS", True):
        return report

    sections = getattr(report, "sections", None)
    if not isinstance(sections, list):
        return report

    try:
        cloned = copy.copy(report)
        cloned.sections = [
            section
            for section in sections
            if not _is_positioning_briefing_section(section)
        ]
        return cloned
    except Exception:
        # Fail-open: if copying fails, do not break reporting.
        return report


def _is_positioning_briefing_section(section: Any) -> bool:
    title = ""
    for attr in ("title", "heading", "name"):
        value = getattr(section, attr, None)
        if value:
            title = str(value)
            break

    text = title.lower()
    return (
        "positioning context" in text
        or "positioning diagnostics" in text
        or "cot" in text
        or "daily participation proxy" in text
    )


def _dry_positioning_sender(text: str) -> bool:
    del text
    return True


def _positioning_close_skip_reason(
    report_type: str,
    *,
    runtime_dir: str | None,
) -> str | None:
    if str(report_type or "").strip().lower() not in {"london_close", "london_close_briefing"}:
        return None

    try:
        from app.services.positioning.positioning_service import get_latest_positioning_context

        snapshot = get_latest_positioning_context(runtime_dir) or {}
        operational = snapshot.get("operational_positioning")
        if not isinstance(operational, dict):
            return "operational_positioning_missing"
        status = str(operational.get("status") or "UNKNOWN").upper()
        symbols = operational.get("symbols") if isinstance(operational.get("symbols"), dict) else {}
        if status in {"DELTA_READY", "PARTIAL"} and symbols:
            return None
        return f"no_london_close_delta:{status.lower()}"
    except Exception as exc:  # noqa: BLE001
        return f"operational_positioning_read_error:{type(exc).__name__}"


def _send_positioning_second_message(
    *,
    notifier: TelegramNotifier | None,
    report_type: str,
    dry_run: bool,
    runtime_dir: str | None = None,
    send_positioning_report: bool | None = None,
    positioning_max_items: int | None = None,
    positioning_split_limit: int | None = None,
) -> dict[str, Any]:
    """
    Send Positioning Intelligence as separate Telegram message(s).

    Fail-open by design: any positioning error is returned in metadata and must
    not break the main market briefing delivery.
    """
    enabled = _positioning_delivery_enabled(report_type, explicit=send_positioning_report)

    if not enabled:
        return {
            "ok": True,
            "enabled": False,
            "prepared": 0,
            "sent": 0,
            "errors": [],
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }

    close_skip_reason = _positioning_close_skip_reason(
        report_type,
        runtime_dir=runtime_dir,
    )
    if close_skip_reason:
        return {
            "ok": True,
            "enabled": True,
            "prepared": 0,
            "sent": 0,
            "skipped": True,
            "skipped_reason": close_skip_reason,
            "dry_run": dry_run,
            "report_type": report_type,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }

    try:
        from app.services.positioning.positioning_telegram_delivery import (
            TELEGRAM_MESSAGE_SAFE_LIMIT,
            build_delivery_summary,
            send_positioning_telegram_parts,
        )

        max_items = positioning_max_items
        if max_items is None:
            max_items = _env_int("REPORT_POSITIONING_MAX_ITEMS", 12)

        split_limit = positioning_split_limit
        if split_limit is None:
            split_limit = _env_int("REPORT_POSITIONING_SPLIT_LIMIT", TELEGRAM_MESSAGE_SAFE_LIMIT)

        sender = _dry_positioning_sender if dry_run else (notifier.send_text if notifier else _dry_positioning_sender)

        result = send_positioning_telegram_parts(
            sender=sender,
            runtime_dir=runtime_dir,
            max_items=max_items,
            split_limit=split_limit,
            enabled=True,
            prepare=True,
            fail_open=True,
        )
        summary = build_delivery_summary(result)
        summary["dry_run"] = dry_run
        summary["report_type"] = report_type
        return summary

    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "enabled": enabled,
            "prepared": 0,
            "sent": 0,
            "errors": [f"{type(exc).__name__}: {exc}"],
            "dry_run": dry_run,
            "report_type": report_type,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }



def send_daily_report(
    *,
    report_type: str = "morning",
    report_date: str | None = None,
    timezone_name: str | None = None,
    dry_run: bool = False,
    refresh: bool = True,
    include_tpo_refresh: bool | None = None,
    send_positioning_report: bool | None = None,
    positioning_max_items: int | None = None,
    positioning_split_limit: int | None = None,
) -> ReporterResult:
    refresh_results: list[dict[str, Any]] = []
    runtime_dir = os.getenv("POSITIONING_RUNTIME_DIR") or os.getenv("RUNTIME_DIR")
    resolved_report_date = _resolve_report_date(report_date, timezone_name)

    if refresh:
        refresh_results = refresh_runtime_artifacts(include_tpo=include_tpo_refresh)

        if (
            _env_bool("REPORT_REFRESH_POSITIONING", True)
            and _positioning_delivery_enabled(report_type, explicit=send_positioning_report)
        ):
            refresh_results.append(
                _refresh_positioning_runtime(
                    runtime_dir=runtime_dir,
                    report_date=resolved_report_date,
                    report_type=report_type,
                )
            )

    report = build_briefing_report(
        report_type=report_type,
        report_date=resolved_report_date,
        timezone_name=timezone_name,
    )

    # Save full artifacts first. Artifacts may include Positioning/COT sections.
    json_path, txt_path = write_briefing_artifacts(report)

    # Telegram main message excludes full Positioning/COT; it is sent separately.
    telegram_report = _briefing_report_for_main_telegram(report)
    message = render_briefing_text(telegram_report)

    sent = False
    notifier: TelegramNotifier | None = None
    positioning_delivery: dict[str, Any] | None = None
    main_chunks = _split_main_telegram_message(message)
    main_part_lengths = [len(chunk) for chunk in main_chunks]

    if not dry_run:
        notifier = TelegramNotifier()
        sent, main_part_lengths = _send_main_telegram_message(notifier, message)

    if dry_run or sent:
        positioning_delivery = _send_positioning_second_message(
            notifier=notifier,
            report_type=report.report_type,
            dry_run=dry_run,
            runtime_dir=runtime_dir,
            send_positioning_report=send_positioning_report,
            positioning_max_items=positioning_max_items,
            positioning_split_limit=positioning_split_limit,
        )
    else:
        positioning_delivery = {
            "ok": False,
            "enabled": _positioning_delivery_enabled(report.report_type, explicit=send_positioning_report),
            "prepared": 0,
            "sent": 0,
            "errors": ["main_telegram_send_failed"],
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        }

    return ReporterResult(
        status="ok" if dry_run or sent else "telegram_send_failed",
        report_type=report.report_type,
        report_date=report.report_date,
        telegram_sent=sent,
        dry_run=dry_run,
        message_length=len(message),
        telegram_main_parts=len(main_part_lengths),
        telegram_main_part_lengths=main_part_lengths,
        artifact_json=str(json_path),
        artifact_text=str(txt_path),
        refresh_results=refresh_results,
        positioning_delivery=positioning_delivery,
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
    parser.add_argument("--no-positioning-report", action="store_true", default=False)
    parser.add_argument("--positioning-max-items", type=int, default=_env_int("REPORT_POSITIONING_MAX_ITEMS", 12))
    parser.add_argument("--positioning-split-limit", type=int, default=_env_int("REPORT_POSITIONING_SPLIT_LIMIT", 3900))
    parser.add_argument("--print-message", action="store_true")

    args = parser.parse_args()

    result = send_daily_report(
        report_type=args.type,
        report_date=args.date,
        timezone_name=args.timezone,
        dry_run=args.dry_run,
        refresh=not args.no_refresh,
        include_tpo_refresh=args.refresh_tpo,
        send_positioning_report=not args.no_positioning_report,
        positioning_max_items=args.positioning_max_items,
        positioning_split_limit=args.positioning_split_limit,
    )

    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))

    if args.print_message and result.artifact_text:
        path = Path(result.artifact_text)
        if path.exists():
            print(path.read_text(encoding="utf-8"))

    return 0 if result.status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())