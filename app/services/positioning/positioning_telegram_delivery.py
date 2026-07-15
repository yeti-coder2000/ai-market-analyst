from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .positioning_store import get_positioning_dir
from .positioning_telegram_report import (
    TELEGRAM_MESSAGE_SAFE_LIMIT,
    build_and_write_positioning_telegram_report,
    write_positioning_telegram_report_parts,
)


POSITIONING_TELEGRAM_DELIVERY_VERSION = "positioning-telegram-delivery-v0.1"


TelegramSender = Callable[[str], Any]


@dataclass(slots=True)
class PositioningTelegramPart:
    index: int
    total: int
    path: Path
    text: str
    chars: int


@dataclass(slots=True)
class PositioningTelegramDeliveryResult:
    ok: bool
    enabled: bool
    prepared: int
    sent: int
    errors: list[str]
    parts: list[PositioningTelegramPart]


def prepare_positioning_telegram_parts(
    runtime_dir: str | None = None,
    max_items: int = 12,
    split_limit: int = TELEGRAM_MESSAGE_SAFE_LIMIT,
    parts_dir: str | None = None,
) -> list[PositioningTelegramPart]:
    """
    Generate Positioning Intelligence report and split it into Telegram-ready parts.

    Does not send Telegram.
    """

    _path, text = build_and_write_positioning_telegram_report(
        runtime_dir=runtime_dir,
        max_items=max_items,
    )

    part_paths = write_positioning_telegram_report_parts(
        text,
        runtime_dir=runtime_dir,
        output_dir=parts_dir,
        limit=split_limit,
    )

    return _read_part_paths(part_paths)


def get_existing_positioning_telegram_parts(
    runtime_dir: str | None = None,
    parts_dir: str | None = None,
) -> list[PositioningTelegramPart]:
    """
    Read existing positioning_telegram_part_* files from runtime.

    Does not regenerate report.
    """

    directory = Path(parts_dir) if parts_dir else get_positioning_dir(runtime_dir)
    paths = sorted(directory.glob("positioning_telegram_part_*_of_*.txt"))
    return _read_part_paths(paths)


def send_positioning_telegram_parts(
    sender: TelegramSender,
    runtime_dir: str | None = None,
    max_items: int = 12,
    split_limit: int = TELEGRAM_MESSAGE_SAFE_LIMIT,
    parts_dir: str | None = None,
    enabled: bool = True,
    prepare: bool = True,
    fail_open: bool = True,
) -> PositioningTelegramDeliveryResult:
    """
    Send Positioning Intelligence report parts through a caller-provided Telegram sender.

    Safety:
    - sender is injected from the existing Telegram system;
    - this module does not know Telegram tokens/chats;
    - if fail_open=True, exceptions are returned in result instead of raised;
    - never modifies Battle Gate or signal logic.
    """

    errors: list[str] = []
    parts: list[PositioningTelegramPart] = []

    if not enabled:
        return PositioningTelegramDeliveryResult(
            ok=True,
            enabled=False,
            prepared=0,
            sent=0,
            errors=[],
            parts=[],
        )

    try:
        if prepare:
            parts = prepare_positioning_telegram_parts(
                runtime_dir=runtime_dir,
                max_items=max_items,
                split_limit=split_limit,
                parts_dir=parts_dir,
            )
        else:
            parts = get_existing_positioning_telegram_parts(
                runtime_dir=runtime_dir,
                parts_dir=parts_dir,
            )
    except Exception as exc:
        message = f"prepare_failed:{type(exc).__name__}:{exc}"
        if not fail_open:
            raise
        return PositioningTelegramDeliveryResult(
            ok=False,
            enabled=True,
            prepared=0,
            sent=0,
            errors=[message],
            parts=[],
        )

    sent = 0

    for part in parts:
        try:
            sender(part.text)
            sent += 1
        except Exception as exc:
            message = f"send_failed:part_{part.index}_of_{part.total}:{type(exc).__name__}:{exc}"
            errors.append(message)
            if not fail_open:
                raise

    return PositioningTelegramDeliveryResult(
        ok=(not errors and sent == len(parts)),
        enabled=True,
        prepared=len(parts),
        sent=sent,
        errors=errors,
        parts=parts,
    )


def build_delivery_summary(result: PositioningTelegramDeliveryResult) -> dict[str, Any]:
    return {
        "version": POSITIONING_TELEGRAM_DELIVERY_VERSION,
        "ok": result.ok,
        "enabled": result.enabled,
        "prepared": result.prepared,
        "sent": result.sent,
        "errors": result.errors,
        "parts": [
            {
                "index": part.index,
                "total": part.total,
                "path": str(part.path),
                "chars": part.chars,
            }
            for part in result.parts
        ],
        "battle_gate_impact": "none",
        "telegram_signal_impact": "none",
    }


def _read_part_paths(paths: list[Path]) -> list[PositioningTelegramPart]:
    out: list[PositioningTelegramPart] = []
    total = len(paths)

    for idx, path in enumerate(paths, start=1):
        text = path.read_text(encoding="utf-8").strip()
        parsed_idx, parsed_total = _parse_part_numbers(path.name)
        out.append(
            PositioningTelegramPart(
                index=parsed_idx or idx,
                total=parsed_total or total,
                path=path,
                text=text,
                chars=len(text),
            )
        )

    return out


def _parse_part_numbers(filename: str) -> tuple[int | None, int | None]:
    # positioning_telegram_part_01_of_04.txt
    stem = Path(filename).stem
    parts = stem.split("_")
    try:
        part_idx = parts.index("part")
        current = int(parts[part_idx + 1])
        if parts[part_idx + 2] != "of":
            return None, None
        total = int(parts[part_idx + 3])
        return current, total
    except Exception:
        return None, None


def _dry_run_sender(text: str) -> None:
    print(f"DRY_SEND chars={len(text)} first_line={text.splitlines()[0] if text else ''}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare or dry-send Positioning Intelligence Telegram parts."
    )
    parser.add_argument("--runtime-dir", default=None, help="Runtime dir.")
    parser.add_argument("--max-items", type=int, default=12, help="Max assets to render.")
    parser.add_argument("--split-limit", type=int, default=TELEGRAM_MESSAGE_SAFE_LIMIT)
    parser.add_argument("--parts-dir", default=None, help="Directory for part files.")
    parser.add_argument(
        "--existing",
        action="store_true",
        help="Use existing part files instead of regenerating.",
    )
    parser.add_argument(
        "--dry-send",
        action="store_true",
        help="Dry-run send using stdout sender.",
    )
    parser.add_argument(
        "--disabled",
        action="store_true",
        help="Simulate disabled delivery.",
    )

    args = parser.parse_args()

    if args.dry_send:
        result = send_positioning_telegram_parts(
            sender=_dry_run_sender,
            runtime_dir=args.runtime_dir,
            max_items=args.max_items,
            split_limit=args.split_limit,
            parts_dir=args.parts_dir,
            enabled=not args.disabled,
            prepare=not args.existing,
            fail_open=True,
        )
        summary = build_delivery_summary(result)
        print(f"ok={summary['ok']}")
        print(f"enabled={summary['enabled']}")
        print(f"prepared={summary['prepared']}")
        print(f"sent={summary['sent']}")
        print(f"errors={len(summary['errors'])}")
        return

    if args.existing:
        parts = get_existing_positioning_telegram_parts(
            runtime_dir=args.runtime_dir,
            parts_dir=args.parts_dir,
        )
    else:
        parts = prepare_positioning_telegram_parts(
            runtime_dir=args.runtime_dir,
            max_items=args.max_items,
            split_limit=args.split_limit,
            parts_dir=args.parts_dir,
        )

    print(f"prepared={len(parts)}")
    for part in parts:
        print(f"part_{part.index}_of_{part.total} chars={part.chars} path={part.path}")


if __name__ == "__main__":
    main()
