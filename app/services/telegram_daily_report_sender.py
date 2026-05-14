from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib import error, request

from app.core.settings import settings
from app.services.daily_signal_report import (
    DEFAULT_TIMEZONE,
    QUALITY_TIERS_PATH,
    REPORTS_DIR,
    SIGNAL_OUTCOMES_PATH,
    build_report,
    get_timezone,
    load_quality_tiers,
    load_signal_outcomes,
    parse_report_date,
    render_telegram_report,
    render_text_report,
    save_report_files,
)


# =============================================================================
# TELEGRAM DAILY REPORT SENDER v1
# =============================================================================
# Purpose:
# - Build fresh Telegram-ready Daily Signal Report.
# - Send it to Telegram using TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID.
# - Keep this separate from live worker.
# - No pandas.
# - No external dependencies.
#
# Safe manual run:
#
#   python -m app.services.telegram_daily_report_sender --dry-run
#   python -m app.services.telegram_daily_report_sender
#   python -m app.services.telegram_daily_report_sender --date today
#
# Notes:
# - This service does NOT run signal_outcome_tracker or signal_quality_tiers.
#   Run them before sender if you want the freshest report:
#
#   python -m app.services.signal_outcome_tracker
#   python -m app.services.signal_quality_tiers
#   python -m app.services.telegram_daily_report_sender
# =============================================================================


TELEGRAM_API_BASE = "https://api.telegram.org"
TELEGRAM_MAX_MESSAGE_CHARS = 3900


# =============================================================================
# BASIC HELPERS
# =============================================================================


def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default

    text = str(value).strip()
    return text if text else default


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)

    if raw is None or str(raw).strip() == "":
        return default

    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def get_telegram_bot_token(cli_value: str | None = None) -> str:
    if cli_value and cli_value.strip():
        return cli_value.strip()

    env_value = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if env_value:
        return env_value

    return safe_str(getattr(settings, "telegram_bot_token", ""), "")


def get_telegram_chat_id(cli_value: str | None = None) -> str:
    if cli_value and cli_value.strip():
        return cli_value.strip()

    env_value = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if env_value:
        return env_value

    return safe_str(getattr(settings, "telegram_chat_id", ""), "")


def split_telegram_message(text: str, *, max_chars: int = TELEGRAM_MAX_MESSAGE_CHARS) -> list[str]:
    """
    Telegram sendMessage limit is 4096 chars.
    We use 3900 for safety.

    Split by lines when possible. If one line is too long, split hard.
    """
    if len(text) <= max_chars:
        return [text]

    chunks: list[str] = []
    current_lines: list[str] = []
    current_len = 0

    for line in text.splitlines():
        line_len = len(line) + 1

        if line_len > max_chars:
            if current_lines:
                chunks.append("\n".join(current_lines).strip())
                current_lines = []
                current_len = 0

            start = 0
            while start < len(line):
                chunks.append(line[start:start + max_chars])
                start += max_chars

            continue

        if current_len + line_len > max_chars:
            chunks.append("\n".join(current_lines).strip())
            current_lines = [line]
            current_len = line_len
        else:
            current_lines.append(line)
            current_len += line_len

    if current_lines:
        chunks.append("\n".join(current_lines).strip())

    return [chunk for chunk in chunks if chunk.strip()]


# =============================================================================
# TELEGRAM API
# =============================================================================


def send_telegram_message(
    *,
    bot_token: str,
    chat_id: str,
    text: str,
    disable_notification: bool = False,
    timeout_sec: int = 20,
) -> dict[str, Any]:
    if not bot_token:
        return {
            "ok": False,
            "error": "missing TELEGRAM_BOT_TOKEN",
        }

    if not chat_id:
        return {
            "ok": False,
            "error": "missing TELEGRAM_CHAT_ID",
        }

    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "disable_notification": disable_notification,
    }

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    req = request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8", errors="replace")

        try:
            result = json.loads(raw)
        except json.JSONDecodeError:
            result = {
                "ok": False,
                "error": "invalid Telegram JSON response",
                "raw": raw,
            }

        return result if isinstance(result, dict) else {"ok": False, "raw": result}

    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "error": f"Telegram HTTPError {exc.code}",
            "raw": raw,
        }

    except error.URLError as exc:
        return {
            "ok": False,
            "error": f"Telegram URLError: {exc.reason}",
        }

    except Exception as exc:
        return {
            "ok": False,
            "error": f"Telegram send exception: {type(exc).__name__}: {exc}",
        }


def send_telegram_text(
    *,
    bot_token: str,
    chat_id: str,
    text: str,
    disable_notification: bool = False,
    timeout_sec: int = 20,
) -> dict[str, Any]:
    chunks = split_telegram_message(text)

    results: list[dict[str, Any]] = []

    for idx, chunk in enumerate(chunks, start=1):
        if len(chunks) > 1:
            chunk_text = f"{chunk}\n\nЧастина {idx}/{len(chunks)}"
        else:
            chunk_text = chunk

        result = send_telegram_message(
            bot_token=bot_token,
            chat_id=chat_id,
            text=chunk_text,
            disable_notification=disable_notification,
            timeout_sec=timeout_sec,
        )

        results.append(result)

        if not result.get("ok"):
            return {
                "ok": False,
                "sent_parts": idx - 1,
                "total_parts": len(chunks),
                "failed_part": idx,
                "results": results,
            }

    return {
        "ok": True,
        "sent_parts": len(chunks),
        "total_parts": len(chunks),
        "results": results,
    }


# =============================================================================
# REPORT BUILDING
# =============================================================================


def build_daily_telegram_report_text(
    *,
    source_path: Path,
    quality_path: Path,
    out_dir: Path,
    report_date_raw: str | None,
    timezone_name: str,
    save: bool = True,
) -> dict[str, Any]:
    tz = get_timezone(timezone_name)
    report_date = parse_report_date(report_date_raw, tz=tz)

    payload = load_signal_outcomes(source_path)
    quality_payload = load_quality_tiers(quality_path)

    report = build_report(
        payload=payload,
        quality_payload=quality_payload,
        report_date=report_date,
        tz=tz,
        source_path=source_path,
        quality_path=quality_path,
    )

    full_text = render_text_report(report)
    telegram_text = render_telegram_report(report)

    paths: dict[str, str] = {}

    if save:
        paths = save_report_files(
            report=report,
            text=full_text,
            out_dir=out_dir,
            telegram_text=telegram_text,
        )
        report["report_files"] = paths

    return {
        "ok": True,
        "report": report,
        "telegram_text": telegram_text,
        "paths": paths,
    }


# =============================================================================
# CLI
# =============================================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build and send Telegram Daily Signal Report."
    )

    parser.add_argument(
        "--source",
        type=str,
        default=str(SIGNAL_OUTCOMES_PATH),
        help="Path to signal_outcomes.json.",
    )

    parser.add_argument(
        "--quality-source",
        type=str,
        default=str(QUALITY_TIERS_PATH),
        help="Path to quality_tiers.json.",
    )

    parser.add_argument(
        "--out-dir",
        type=str,
        default=str(REPORTS_DIR),
        help="Directory for report files.",
    )

    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help=(
            "Optional local report date in YYYY-MM-DD. "
            "Use 'today' for current date in selected timezone. "
            "If omitted, report includes all tracked alerts."
        ),
    )

    parser.add_argument(
        "--timezone",
        type=str,
        default=DEFAULT_TIMEZONE,
        help="Timezone for --date filtering. Default: Europe/Kyiv.",
    )

    parser.add_argument(
        "--bot-token",
        type=str,
        default=None,
        help="Telegram bot token. Defaults to TELEGRAM_BOT_TOKEN/settings.",
    )

    parser.add_argument(
        "--chat-id",
        type=str,
        default=None,
        help="Telegram chat id. Defaults to TELEGRAM_CHAT_ID/settings.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print the Telegram report, do not send it.",
    )

    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Do not write report files.",
    )

    parser.add_argument(
        "--disable-notification",
        action="store_true",
        help="Send Telegram message silently.",
    )

    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=20,
        help="Telegram HTTP timeout in seconds.",
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON result.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    source_path = Path(args.source)
    quality_path = Path(args.quality_source)
    out_dir = Path(args.out_dir)

    build_result = build_daily_telegram_report_text(
        source_path=source_path,
        quality_path=quality_path,
        out_dir=out_dir,
        report_date_raw=args.date,
        timezone_name=args.timezone,
        save=not args.no_save,
    )

    telegram_text = str(build_result["telegram_text"])

    if args.dry_run:
        result = {
            "ok": True,
            "dry_run": True,
            "sent": False,
            "message_chars": len(telegram_text),
            "message_parts": len(split_telegram_message(telegram_text)),
            "paths": build_result.get("paths", {}),
        }

        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(telegram_text)
            print("")
            print("DRY RUN")
            print("-" * 72)
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))

        return

    bot_token = get_telegram_bot_token(args.bot_token)
    chat_id = get_telegram_chat_id(args.chat_id)

    send_result = send_telegram_text(
        bot_token=bot_token,
        chat_id=chat_id,
        text=telegram_text,
        disable_notification=args.disable_notification,
        timeout_sec=args.timeout_sec,
    )

    result = {
        "ok": bool(send_result.get("ok")),
        "dry_run": False,
        "sent": bool(send_result.get("ok")),
        "message_chars": len(telegram_text),
        "message_parts": len(split_telegram_message(telegram_text)),
        "paths": build_result.get("paths", {}),
        "telegram": send_result,
    }

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return

    if result["ok"]:
        print("Telegram Daily Report sent.")
    else:
        print("Telegram Daily Report send failed.")

    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()