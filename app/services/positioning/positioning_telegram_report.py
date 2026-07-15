from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

from .positioning_service import get_latest_positioning_context
from .positioning_store import get_positioning_dir


POSITIONING_TELEGRAM_REPORT_VERSION = "positioning-telegram-report-v0.1-separate-message"

DEFAULT_POSITIONING_TELEGRAM_REPORT_FILENAME = "positioning_telegram_latest.txt"
DEFAULT_POSITIONING_TELEGRAM_PART_PREFIX = "positioning_telegram_part"

TELEGRAM_MESSAGE_HARD_LIMIT = 4096
TELEGRAM_MESSAGE_SAFE_LIMIT = 3900


def render_positioning_telegram_message(
    snapshot: dict[str, Any] | None = None,
    runtime_dir: str | None = None,
    max_items: int = 12,
) -> str:
    """
    Render separate Telegram-ready Positioning Intelligence / COT-style message.

    Architecture rule:
    - this is a separate Telegram message;
    - do not append full COT/Positioning briefing into the main market briefing;
    - research/context only;
    - no Battle Gate impact;
    - no Telegram signal permission impact.
    """

    source_snapshot = snapshot if snapshot is not None else get_latest_positioning_context(runtime_dir)
    if not isinstance(source_snapshot, dict):
        source_snapshot = {}

    status = str(source_snapshot.get("status") or "UNKNOWN")
    date_value = str(source_snapshot.get("date") or "unknown")
    generated_at = str(source_snapshot.get("generated_at") or "")
    items = source_snapshot.get("items") or []
    if not isinstance(items, list):
        items = []

    lines: list[str] = []

    lines.append("<b>📊 Positioning Intelligence Briefing</b>")
    lines.append("")
    lines.append(f"Date: <b>{_h(date_value)}</b>")
    lines.append(f"Status: <b>{_h(status)}</b>")
    if generated_at:
        lines.append(f"Generated: {_h(generated_at)}")
    lines.append(f"Version: {_h(POSITIONING_TELEGRAM_REPORT_VERSION)}")
    lines.append("")
    lines.append("<b>Mode</b>")
    lines.append("Research-only / context-only.")
    lines.append("Battle Gate: <b>none</b>")
    lines.append("Telegram signal impact: <b>none</b>")
    lines.append("Delivery: <b>separate Telegram message</b>")
    lines.append("")

    if not items:
        lines.append("<b>Summary</b>")
        lines.append("Positioning data unavailable or empty.")
        lines.append("Main market briefing must continue without this layer.")
        lines.append("")
        lines.append(_safety_footer())
        return "\n".join(lines).strip()

    tag_counts = Counter()
    quality_counts = Counter()
    safety = {
        "allow_true": 0,
        "block_true": 0,
        "bg_not_none": 0,
        "telegram_not_none": 0,
    }

    lines.append("<b>Summary</b>")
    lines.append(f"Assets covered: <b>{len(items)}</b>")

    for item in items:
        interp = item.get("positioning_interpretation") or {}
        quality = item.get("data_quality") or {}
        auction_usage = item.get("auction_usage") or {}

        primary_tag = str(interp.get("primary_tag") or "DATA_UNAVAILABLE")
        data_quality = str(interp.get("data_quality") or quality.get("status") or "UNKNOWN")

        tag_counts[primary_tag] += 1
        quality_counts[data_quality] += 1

        if bool(item.get("positioning_can_allow_signal")):
            safety["allow_true"] += 1
        if bool(item.get("positioning_can_block_signal")):
            safety["block_true"] += 1
        if str(auction_usage.get("battle_gate_impact") or "none").lower() != "none":
            safety["bg_not_none"] += 1
        if str(auction_usage.get("telegram_signal_impact") or "none").lower() != "none":
            safety["telegram_not_none"] += 1

    lines.append(f"Tags: {_h(_format_counter(tag_counts))}")
    lines.append(f"Data quality: {_h(_format_counter(quality_counts))}")
    lines.append(
        "Safety: "
        f"allow=True {safety['allow_true']} | "
        f"block=True {safety['block_true']} | "
        f"BG!=none {safety['bg_not_none']} | "
        f"TG impact!=none {safety['telegram_not_none']}"
    )
    lines.append("")

    lines.append("<b>Assets</b>")

    for item in items[:max_items]:
        lines.extend(_render_asset_block(item))
        lines.append("")

    remaining = len(items) - max_items
    if remaining > 0:
        lines.append(f"...ще {remaining} assets не показано через max_items={max_items}.")
        lines.append("")

    lines.append(_safety_footer())

    return "\n".join(lines).strip()



def split_telegram_message(
    text: str,
    limit: int = TELEGRAM_MESSAGE_SAFE_LIMIT,
    title: str = "<b>📊 Positioning Intelligence Briefing</b>",
) -> list[str]:
    """
    Split Telegram HTML message into safe parts.

    Rules:
    - default safe limit is 3900 chars, below Telegram hard limit 4096;
    - split prefers paragraph boundaries;
    - then line boundaries;
    - final fallback hard-splits very long lines;
    - every part gets a title + Part X/Y marker;
    - does not send Telegram, only prepares text chunks.
    """
    raw = str(text or "").strip()
    if not raw:
        return []

    if limit <= 500:
        raise ValueError("Telegram split limit is too small; use > 500 chars.")

    if len(raw) <= limit:
        return [raw]

    body = raw
    if body.startswith(title):
        body = body[len(title):].lstrip()

    # Reserve space for title + Part X/Y header.
    content_limit = max(500, limit - 140)

    chunks = _split_text_blocks(body, limit=content_limit)
    total = len(chunks)

    parts: list[str] = []
    for idx, chunk in enumerate(chunks, start=1):
        header = f"{title}\n<i>Part {idx}/{total}</i>\n\n"
        part = header + chunk.strip()

        if len(part) <= limit:
            parts.append(part)
            continue

        # Extremely defensive fallback if header pushed it over the limit.
        sub_limit = max(500, limit - len(header) - 5)
        sub_chunks = _split_text_blocks(chunk, limit=sub_limit)
        for sub in sub_chunks:
            parts.append(header + sub.strip())

    # If defensive fallback changed count, normalize headers.
    if len(parts) != total:
        normalized: list[str] = []
        total = len(parts)
        for idx, part in enumerate(parts, start=1):
            body_part = _strip_positioning_part_header(part, title=title)
            normalized.append(f"{title}\n<i>Part {idx}/{total}</i>\n\n{body_part.strip()}")
        parts = normalized

    return parts


def write_positioning_telegram_report_parts(
    text: str,
    runtime_dir: str | None = None,
    output_dir: str | None = None,
    limit: int = TELEGRAM_MESSAGE_SAFE_LIMIT,
) -> list[Path]:
    """
    Write split Telegram parts to runtime/positioning.

    Existing positioning_telegram_part_* files are removed first to prevent
    stale parts from previous longer reports.
    """
    directory = Path(output_dir) if output_dir else get_positioning_dir(runtime_dir)
    directory.mkdir(parents=True, exist_ok=True)

    for old in directory.glob(f"{DEFAULT_POSITIONING_TELEGRAM_PART_PREFIX}_*_of_*.txt"):
        try:
            old.unlink()
        except FileNotFoundError:
            pass

    parts = split_telegram_message(text, limit=limit)
    total = len(parts)

    paths: list[Path] = []
    for idx, part in enumerate(parts, start=1):
        path = directory / f"{DEFAULT_POSITIONING_TELEGRAM_PART_PREFIX}_{idx:02d}_of_{total:02d}.txt"
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(part + "\n", encoding="utf-8")
        tmp.replace(path)
        paths.append(path)

    return paths


def _split_text_blocks(text: str, limit: int) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []

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

        line_chunks = _split_long_block_by_lines(block, limit=limit)
        if line_chunks:
            chunks.extend(line_chunks[:-1])
            current = line_chunks[-1]

    if current:
        chunks.append(current)

    return chunks


def _split_long_block_by_lines(block: str, limit: int) -> list[str]:
    chunks: list[str] = []
    current = ""

    for line in block.splitlines():
        line = line.rstrip()

        if len(line) > limit:
            if current:
                chunks.append(current)
                current = ""
            chunks.extend(_hard_split_line(line, limit=limit))
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


def _hard_split_line(line: str, limit: int) -> list[str]:
    text = str(line or "")
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    start = 0
    while start < len(text):
        chunks.append(text[start:start + limit])
        start += limit
    return chunks


def _strip_positioning_part_header(text: str, title: str) -> str:
    raw = str(text or "").strip()
    if raw.startswith(title):
        raw = raw[len(title):].lstrip()

    lines = raw.splitlines()
    if lines and lines[0].strip().startswith("<i>Part "):
        lines = lines[1:]
    return "\n".join(lines).lstrip()



def write_positioning_telegram_report(
    text: str,
    runtime_dir: str | None = None,
    output_path: str | None = None,
) -> Path:
    path = Path(output_path) if output_path else get_positioning_dir(runtime_dir) / DEFAULT_POSITIONING_TELEGRAM_REPORT_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text + "\n", encoding="utf-8")
    tmp.replace(path)
    return path


def build_and_write_positioning_telegram_report(
    runtime_dir: str | None = None,
    output_path: str | None = None,
    max_items: int = 12,
) -> tuple[Path, str]:
    snapshot = get_latest_positioning_context(runtime_dir)
    text = render_positioning_telegram_message(
        snapshot=snapshot,
        runtime_dir=runtime_dir,
        max_items=max_items,
    )
    path = write_positioning_telegram_report(
        text,
        runtime_dir=runtime_dir,
        output_path=output_path,
    )
    return path, text


def _render_asset_block(item: dict[str, Any]) -> list[str]:
    symbol = str(item.get("symbol") or "UNKNOWN")
    market = item.get("daily_market_data") or {}
    interp = item.get("positioning_interpretation") or {}
    quality = item.get("data_quality") or {}
    auction_usage = item.get("auction_usage") or {}
    raw_source = item.get("raw_source") or {}
    proxy = item.get("market_proxy") or {}

    price_change = market.get("price_change_pct")
    oi_change = market.get("open_interest_change_pct")
    volume_change = market.get("volume_change_pct_vs_20d")

    primary_tag = str(interp.get("primary_tag") or "DATA_UNAVAILABLE")
    confidence = interp.get("confidence")
    data_quality = str(interp.get("data_quality") or quality.get("status") or "UNKNOWN")
    flags = quality.get("flags") or interp.get("flags") or raw_source.get("flags") or []

    interpretation = str(interp.get("interpretation") or "")
    tpo_note = str(interp.get("tpo_note") or auction_usage.get("recommended_usage") or "")
    source = str(proxy.get("source") or raw_source.get("source") or "unknown_source")
    source_ts = str(proxy.get("source_timestamp") or raw_source.get("source_timestamp") or "")
    notes = str(raw_source.get("notes") or "")

    out: list[str] = []
    out.append(f"<b>{_h(symbol)}</b>")
    out.append(
        "Daily proxy: "
        f"Price {_arrow(price_change)} {_fmt_pct(price_change)} / "
        f"OI {_arrow(oi_change)} {_fmt_pct(oi_change)} / "
        f"Volume {_arrow(volume_change)} {_fmt_pct(volume_change)}"
    )
    out.append(
        f"Tag: <b>{_h(primary_tag)}</b>"
        f" / conf {_fmt_confidence(confidence)}"
        f" / quality {_h(data_quality)}"
    )

    if flags:
        out.append(f"Flags: {_h(', '.join(str(x) for x in flags))}")

    out.append(f"Source: {_h(source)}")
    if source_ts:
        out.append(f"Source time: {_h(source_ts)}")

    if interpretation:
        out.append(f"Read: {_h(interpretation)}")

    if tpo_note:
        out.append(f"TPO usage: {_h(tpo_note)}")

    if notes:
        out.append(f"Notes: {_h(_shorten(notes, 280))}")

    return out


def _safety_footer() -> str:
    return (
        "<b>Safety rule</b>\n"
        "Positioning / COT / Daily Participation Proxy is context only. "
        "It cannot allow a signal, cannot block a signal, and cannot modify Battle Gate."
    )


def _format_counter(counter: Counter[str], limit: int = 8) -> str:
    if not counter:
        return "none"

    parts = [f"{key}={value}" for key, value in counter.most_common(limit)]
    remaining = sum(counter.values()) - sum(value for _, value in counter.most_common(limit))
    if remaining > 0:
        parts.append(f"other={remaining}")
    return ", ".join(parts)


def _fmt_pct(value: Any) -> str:
    number = _to_float(value)
    if number is None:
        return "n/a"
    return f"{number:.2f}%"


def _fmt_confidence(value: Any) -> str:
    number = _to_float(value)
    if number is None:
        return "n/a"
    return f"{number:.2f}"


def _arrow(value: Any) -> str:
    number = _to_float(value)
    if number is None:
        return "·"
    if number > 0:
        return "↑"
    if number < 0:
        return "↓"
    return "→"


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _shorten(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _h(value: Any) -> str:
    return escape(str(value), quote=False)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render separate Telegram-ready Positioning Intelligence message."
    )
    parser.add_argument("--runtime-dir", default=None, help="Runtime dir.")
    parser.add_argument("--output", default=None, help="Output TXT path.")
    parser.add_argument("--max-items", type=int, default=12, help="Max assets to render.")
    parser.add_argument("--stdout", action="store_true", help="Print report text to stdout.")
    parser.add_argument("--no-save", action="store_true", help="Do not save report text.")
    parser.add_argument(
        "--split-limit",
        type=int,
        default=TELEGRAM_MESSAGE_SAFE_LIMIT,
        help="Safe Telegram split limit. Default: 3900.",
    )
    parser.add_argument(
        "--parts-dir",
        default=None,
        help="Directory for split Telegram part files. Default: runtime/positioning.",
    )

    args = parser.parse_args()

    snapshot = get_latest_positioning_context(args.runtime_dir)
    text = render_positioning_telegram_message(
        snapshot=snapshot,
        runtime_dir=args.runtime_dir,
        max_items=args.max_items,
    )

    parts = split_telegram_message(text, limit=args.split_limit)

    if not args.no_save:
        path = write_positioning_telegram_report(
            text,
            runtime_dir=args.runtime_dir,
            output_path=args.output,
        )
        print(f"positioning_telegram_report={path}")

        part_paths = write_positioning_telegram_report_parts(
            text,
            runtime_dir=args.runtime_dir,
            output_dir=args.parts_dir,
            limit=args.split_limit,
        )
        print(f"split_parts={len(part_paths)}")
        for idx, part_path in enumerate(part_paths, start=1):
            part_text = part_path.read_text(encoding="utf-8")
            print(f"part_{idx}={part_path} chars={len(part_text.strip())}")
    else:
        print(f"split_parts={len(parts)}")
        for idx, part in enumerate(parts, start=1):
            print(f"part_{idx}_chars={len(part)}")

    print(f"chars={len(text)}")
    print(f"lines={len(text.splitlines())}")
    print(f"generated_at={_utc_now_iso()}")

    if args.stdout:
        print()
        print(text)


if __name__ == "__main__":
    main()
