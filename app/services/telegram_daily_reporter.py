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
import hashlib
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from app.services.daily_market_briefing import (
    build_briefing_report,
    render_briefing_text,
    write_briefing_artifacts,
)
from app.services.telegram_notifier import TelegramNotifier


REPORTER_VERSION = "telegram-daily-reporter-v1.7-idempotent-multipart"


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


def _previous_trading_date(value: str) -> str:
    current = date.fromisoformat(value)
    previous = current - timedelta(days=1)
    while previous.weekday() >= 5:
        previous -= timedelta(days=1)
    return previous.isoformat()


def _positioning_reference_date(report_type: str, report_date: str) -> str:
    """Return the historical snapshot date; never relabel a live refresh with it."""
    normalized = str(report_type or "").strip().lower()
    if normalized in {"morning", "morning_briefing", "morning_combined"}:
        return _previous_trading_date(report_date)
    return report_date


def _daily_close_tpo_path(runtime_dir: str | None) -> Path:
    root = Path(runtime_dir or os.getenv("RUNTIME_DIR") or "runtime")
    return root / "tpo" / "tpo_london_ny_close_latest.json"


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
    telegram_delivery: dict[str, Any] | None = None
    positioning_delivery: dict[str, Any] | None = None
    error_message: str | None = None
    pending_delivery: dict[str, Any] | None = None

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
            "telegram_delivery": self.telegram_delivery,
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


def refresh_runtime_artifacts(
    *,
    include_tpo: bool | None = None,
    report_type: str | None = None,
) -> list[dict[str, Any]]:
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

    previous_scope = os.environ.get("TPO_PROFILE_SCOPE")
    previous_output = os.environ.get("TPO_EXPORT_OUTPUT")
    if str(report_type or "").strip().lower() in {"daily_close", "ny_close"}:
        os.environ["TPO_PROFILE_SCOPE"] = "LONDON_NY_COMBINED"
        os.environ["TPO_EXPORT_OUTPUT"] = str(
            _daily_close_tpo_path(os.getenv("RUNTIME_DIR"))
        )
    try:
        return [_run_module(module, timeout_sec=timeout_sec) for module in modules]
    finally:
        if previous_scope is None:
            os.environ.pop("TPO_PROFILE_SCOPE", None)
        else:
            os.environ["TPO_PROFILE_SCOPE"] = previous_scope
        if previous_output is None:
            os.environ.pop("TPO_EXPORT_OUTPUT", None)
        else:
            os.environ["TPO_EXPORT_OUTPUT"] = previous_output




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


def _main_telegram_parts(
    message: str,
    *,
    split_limit: int | None = None,
) -> list[str]:
    chunks = _split_main_telegram_message(message, max_chars=split_limit)
    total = len(chunks)
    if total <= 1:
        return chunks
    return [
        f"{chunk}\n\n<i>Main briefing part {idx}/{total}</i>"
        for idx, chunk in enumerate(chunks, start=1)
    ]


def _delivery_id(parts: list[str]) -> str:
    payload = "\x00".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _retry_after_utc(retry_after_seconds: Any) -> str | None:
    try:
        seconds = max(0, int(retry_after_seconds))
    except (TypeError, ValueError):
        return None
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _public_delivery_summary(delivery: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in delivery.items()
        if key not in {"parts", "refresh_results"}
    }


def _send_main_telegram_message(
    notifier: TelegramNotifier,
    message: str,
    *,
    split_limit: int | None = None,
    resume: dict[str, Any] | None = None,
    progress_context: dict[str, Any] | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """
    Send or resume the main market briefing in safe chunks.

    Exact prepared part payloads and cumulative completion are exposed to the
    worker callback before part 1 and after every attempt. This lets the
    persistent worker state resume at the first unsent part without repeating
    already delivered parts.
    """
    resume_parts = resume.get("parts") if isinstance(resume, dict) else None
    if isinstance(resume_parts, list) and resume_parts and all(
        isinstance(part, str) and part.strip() for part in resume_parts
    ):
        parts = list(resume_parts)
    else:
        parts = _main_telegram_parts(message, split_limit=split_limit)

    total = len(parts)
    if not parts:
        return {
            "version": "telegram-main-delivery-v1",
            "ok": False,
            "delivery_id": None,
            "total_parts": 0,
            "completed_parts": 0,
            "sent_parts_this_attempt": 0,
            "failed_part": None,
            "api_error": {
                "description": "empty_telegram_message",
                "retry_after": None,
            },
            "retry_after": None,
            "retry_after_utc": None,
            "part_lengths": [],
            "parts": [],
        }

    try:
        completed = int((resume or {}).get("completed_parts") or 0)
    except (TypeError, ValueError):
        completed = 0
    completed = max(0, min(completed, total))
    delivery_id = _delivery_id(parts)
    sent_this_attempt = 0

    def publish(
        *,
        ok: bool,
        failed_part: int | None,
        api_error: dict[str, Any] | None,
    ) -> dict[str, Any]:
        retry_after = api_error.get("retry_after") if isinstance(api_error, dict) else None
        delivery = {
            "version": "telegram-main-delivery-v1",
            "ok": ok,
            "delivery_id": delivery_id,
            "total_parts": total,
            "completed_parts": completed,
            "sent_parts_this_attempt": sent_this_attempt,
            "failed_part": failed_part,
            "api_error": api_error,
            "retry_after": retry_after,
            "retry_after_utc": _retry_after_utc(retry_after),
            "part_lengths": [len(part) for part in parts],
            "parts": parts,
        }
        if progress_context:
            delivery.update(progress_context)
        if progress_callback is not None:
            progress_callback(dict(delivery))
        return delivery

    publish(ok=False, failed_part=None, api_error=None)

    for idx in range(completed, total):
        part_number = idx + 1
        if not notifier.send_text(parts[idx]):
            api_error = getattr(notifier, "last_send_result", None)
            if not isinstance(api_error, dict):
                api_error = {
                    "ok": False,
                    "description": "telegram_send_failed_without_api_details",
                    "retry_after": None,
                }
            return publish(
                ok=False,
                failed_part=part_number,
                api_error=api_error,
            )

        completed = part_number
        sent_this_attempt += 1
        publish(ok=(completed == total), failed_part=None, api_error=None)

    return publish(ok=True, failed_part=None, api_error=None)



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
    if normalized in {"london_1h", "london_open_1h"}:
        return _env_bool("REPORT_SEND_LONDON_1H_POSITIONING", True)
    if normalized in {"london_close", "london_close_briefing"}:
        return _env_bool("REPORT_SEND_LONDON_CLOSE_POSITIONING", True)
    if normalized in {"daily_close", "ny_close"}:
        return _env_bool("REPORT_SEND_DAILY_CLOSE_POSITIONING", False)

    allowed = _env_csv_set(
        "REPORT_POSITIONING_TYPES",
        "morning,morning_combined,london_1h,crypto_health",
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
    normalized = str(report_type or "").strip().lower()
    if normalized not in {
        "london_1h",
        "london_open_1h",
        "london_close",
        "london_close_briefing",
    }:
        return None

    try:
        from app.services.positioning.positioning_service import get_latest_positioning_context

        snapshot = get_latest_positioning_context(runtime_dir) or {}
        operational = snapshot.get("operational_positioning")
        if not isinstance(operational, dict):
            return "operational_positioning_missing"
        status = str(operational.get("status") or "UNKNOWN").upper()
        symbols = operational.get("symbols") if isinstance(operational.get("symbols"), dict) else {}
        if status in {
            "LONDON_1H_DELTA_READY",
            "DELTA_READY",
            "PARTIAL",
        } and symbols:
            return None
        checkpoint = (
            "london_1h"
            if normalized in {"london_1h", "london_open_1h"}
            else "london_close"
        )
        return f"no_{checkpoint}_delta:{status.lower()}"
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
    delivery_resume: dict[str, Any] | None = None,
    delivery_progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> ReporterResult:
    refresh_results: list[dict[str, Any]] = []
    runtime_dir = os.getenv("POSITIONING_RUNTIME_DIR") or os.getenv("RUNTIME_DIR")
    resolved_report_date = _resolve_report_date(report_date, timezone_name)

    resume_parts = delivery_resume.get("parts") if isinstance(delivery_resume, dict) else None
    resume_matches = (
        not dry_run
        and isinstance(delivery_resume, dict)
        and str(delivery_resume.get("stage") or "main") == "main"
        and str(delivery_resume.get("report_type") or "") == str(report_type)
        and str(delivery_resume.get("report_date") or "") == resolved_report_date
        and isinstance(resume_parts, list)
        and bool(resume_parts)
    )
    if resume_matches:
        notifier = TelegramNotifier()
        progress_context = {
            "stage": "main",
            "report_type": str(report_type),
            "report_date": resolved_report_date,
            "message_length": int(delivery_resume.get("message_length") or 0),
            "artifact_json": delivery_resume.get("artifact_json"),
            "artifact_text": delivery_resume.get("artifact_text"),
            "refresh_results": delivery_resume.get("refresh_results") or [],
        }
        main_delivery = _send_main_telegram_message(
            notifier,
            "",
            resume=delivery_resume,
            progress_context=progress_context,
            progress_callback=delivery_progress_callback,
        )
        sent = bool(main_delivery.get("ok"))
        if sent:
            positioning_delivery = _send_positioning_second_message(
                notifier=notifier,
                report_type=str(report_type),
                dry_run=False,
                runtime_dir=runtime_dir,
                send_positioning_report=send_positioning_report,
                positioning_max_items=positioning_max_items,
                positioning_split_limit=positioning_split_limit,
            )
        else:
            positioning_delivery = {
                "ok": False,
                "enabled": _positioning_delivery_enabled(
                    str(report_type),
                    explicit=send_positioning_report,
                ),
                "prepared": 0,
                "sent": 0,
                "errors": ["main_telegram_send_failed"],
                "battle_gate_impact": "none",
                "telegram_signal_impact": "none",
            }

        return ReporterResult(
            status="ok" if sent else "telegram_send_failed",
            report_type=str(report_type),
            report_date=resolved_report_date,
            telegram_sent=sent,
            dry_run=False,
            message_length=int(delivery_resume.get("message_length") or 0),
            telegram_main_parts=int(main_delivery.get("total_parts") or 0),
            telegram_main_part_lengths=list(main_delivery.get("part_lengths") or []),
            telegram_delivery=_public_delivery_summary(main_delivery),
            artifact_json=(
                str(delivery_resume.get("artifact_json"))
                if delivery_resume.get("artifact_json")
                else None
            ),
            artifact_text=(
                str(delivery_resume.get("artifact_text"))
                if delivery_resume.get("artifact_text")
                else None
            ),
            refresh_results=list(delivery_resume.get("refresh_results") or []),
            positioning_delivery=positioning_delivery,
            error_message=None if sent else "telegram_send_failed",
            pending_delivery=None if sent else main_delivery,
        )

    if refresh:
        refresh_results = refresh_runtime_artifacts(
            include_tpo=include_tpo_refresh,
            report_type=report_type,
        )

        normalized_type = str(report_type or "").strip().lower()
        refresh_positioning = _env_bool("REPORT_REFRESH_POSITIONING", True) and (
            _positioning_delivery_enabled(
                report_type,
                explicit=send_positioning_report,
            )
            or normalized_type in {"daily_close", "ny_close"}
        )
        if refresh_positioning:
            refresh_results.append(
                _refresh_positioning_runtime(
                    runtime_dir=runtime_dir,
                    report_date=resolved_report_date,
                    report_type=report_type,
                )
            )

    previous_tpo_store = os.environ.get("TPO_CONTEXT_STORE_PATH")
    if str(report_type or "").strip().lower() in {"daily_close", "ny_close"}:
        os.environ["TPO_CONTEXT_STORE_PATH"] = str(
            _daily_close_tpo_path(runtime_dir)
        )
    try:
        report = build_briefing_report(
            report_type=report_type,
            report_date=resolved_report_date,
            timezone_name=timezone_name,
        )
    finally:
        if previous_tpo_store is None:
            os.environ.pop("TPO_CONTEXT_STORE_PATH", None)
        else:
            os.environ["TPO_CONTEXT_STORE_PATH"] = previous_tpo_store

    # Save full artifacts first. Artifacts may include Positioning/COT sections.
    json_path, txt_path = write_briefing_artifacts(report)

    # Telegram main message excludes full Positioning/COT; it is sent separately.
    telegram_report = _briefing_report_for_main_telegram(report)
    message = render_briefing_text(telegram_report)

    sent = False
    notifier: TelegramNotifier | None = None
    positioning_delivery: dict[str, Any] | None = None
    main_parts = _main_telegram_parts(message)
    main_delivery: dict[str, Any] = {
        "version": "telegram-main-delivery-v1",
        "ok": True,
        "dry_run": True,
        "delivery_id": _delivery_id(main_parts) if main_parts else None,
        "total_parts": len(main_parts),
        "completed_parts": len(main_parts),
        "sent_parts_this_attempt": 0,
        "failed_part": None,
        "api_error": None,
        "retry_after": None,
        "retry_after_utc": None,
        "part_lengths": [len(part) for part in main_parts],
        "parts": main_parts,
    }

    if not dry_run:
        notifier = TelegramNotifier()
        progress_context = {
            "stage": "main",
            "report_type": report.report_type,
            "report_date": report.report_date,
            "message_length": len(message),
            "artifact_json": str(json_path),
            "artifact_text": str(txt_path),
            "refresh_results": refresh_results,
        }
        main_delivery = _send_main_telegram_message(
            notifier,
            message,
            progress_context=progress_context,
            progress_callback=delivery_progress_callback,
        )
        sent = bool(main_delivery.get("ok"))

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
        telegram_main_parts=int(main_delivery.get("total_parts") or 0),
        telegram_main_part_lengths=list(main_delivery.get("part_lengths") or []),
        telegram_delivery=_public_delivery_summary(main_delivery),
        artifact_json=str(json_path),
        artifact_text=str(txt_path),
        refresh_results=refresh_results,
        positioning_delivery=positioning_delivery,
        error_message=None if dry_run or sent else "telegram_send_failed",
        pending_delivery=None if dry_run or sent else main_delivery,
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
        send_positioning_report=False if args.no_positioning_report else None,
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
