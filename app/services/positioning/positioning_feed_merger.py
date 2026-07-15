from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

from .positioning_store import (
    get_positioning_dir,
    read_json_file,
    write_json_atomic,
)


POSITIONING_FEED_MERGER_VERSION = "positioning-feed-merger-v0.1"

DEFAULT_MERGED_FEED_FILENAME = "merged_daily_positioning_feed.json"


def merge_positioning_feeds(
    feed_paths: list[str | Path],
    target_date: str | None = None,
    dedupe_by_symbol: bool = True,
    source_priority: list[str] | None = None,
) -> dict[str, Any]:
    """
    Merge multiple Positioning Intelligence feed JSON files into one feed.

    v0.1 rules:
    - merge feed items only;
    - no signal generation;
    - no Battle Gate impact;
    - latest/higher-priority item wins when duplicate symbol appears;
    - safe for CSV/manual feed + crypto collector feed + future collectors.
    """

    merged_items: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    warnings: list[str] = []

    for raw_path in feed_paths:
        path = Path(raw_path)
        if not path.exists():
            warnings.append(f"missing_feed:{path}")
            continue

        try:
            feed = read_json_file(path)
        except Exception as exc:
            warnings.append(f"bad_feed:{path}:{type(exc).__name__}")
            continue

        items = feed.get("items") or []
        if not isinstance(items, list):
            warnings.append(f"bad_items:{path}")
            continue

        sources.append(
            {
                "path": str(path),
                "version": feed.get("version"),
                "date": feed.get("date"),
                "items": len(items),
                "collector": feed.get("collector"),
            }
        )

        for item in items:
            if not isinstance(item, dict):
                continue
            copied = dict(item)
            copied.setdefault("merged_from", str(path))
            merged_items.append(copied)

    if dedupe_by_symbol:
        merged_items = _dedupe_items_by_symbol(
            merged_items,
            source_priority=source_priority or [],
        )

    feed_date = target_date or _infer_date_from_sources(sources) or date.today().isoformat()

    return {
        "version": POSITIONING_FEED_MERGER_VERSION,
        "date": feed_date,
        "items": merged_items,
        "merge_meta": {
            "dedupe_by_symbol": dedupe_by_symbol,
            "source_priority": source_priority or [],
            "sources": sources,
            "warnings": warnings,
            "battle_gate_impact": "none",
            "telegram_signal_impact": "none",
        },
    }


def write_merged_feed(
    payload: dict[str, Any],
    runtime_dir: str | None = None,
    output_path: str | None = None,
) -> Path:
    path = Path(output_path) if output_path else get_positioning_dir(runtime_dir) / DEFAULT_MERGED_FEED_FILENAME
    write_json_atomic(path, payload)
    return path


def _dedupe_items_by_symbol(
    items: list[dict[str, Any]],
    source_priority: list[str],
) -> list[dict[str, Any]]:
    """
    Deduplicate by symbol.

    Higher score wins:
    - explicit source_priority match wins;
    - otherwise later feed item wins.
    """

    priority = {str(token): idx for idx, token in enumerate(source_priority)}
    selected: dict[str, tuple[int, int, dict[str, Any]]] = {}

    for order, item in enumerate(items):
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue

        source_text = " ".join(
            str(item.get(key) or "")
            for key in ("source", "merged_from", "notes")
        )

        priority_rank = _priority_rank(source_text, priority)
        # Higher comparable score wins. Lower priority_rank is better if matched.
        # Convert to score: unmatched = 0, best priority = high.
        if priority_rank is None:
            score = 0
        else:
            score = 10_000 - priority_rank

        current = selected.get(symbol)
        if current is None:
            selected[symbol] = (score, order, item)
            continue

        current_score, current_order, _ = current
        if (score, order) >= (current_score, current_order):
            selected[symbol] = (score, order, item)

    return [entry[2] for _, entry in sorted(selected.items(), key=lambda kv: kv[1][1])]


def _priority_rank(source_text: str, priority: dict[str, int]) -> int | None:
    if not priority:
        return None

    for token, rank in priority.items():
        if token and token in source_text:
            return rank

    return None


def _infer_date_from_sources(sources: list[dict[str, Any]]) -> str | None:
    for source in reversed(sources):
        value = source.get("date")
        if value:
            return str(value)
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge Positioning Intelligence feed files.")
    parser.add_argument("--runtime-dir", default=None, help="Runtime dir.")
    parser.add_argument("--date", default=None, help="Merged feed date YYYY-MM-DD.")
    parser.add_argument("--output", default=None, help="Output merged feed path.")
    parser.add_argument(
        "--feed",
        action="append",
        default=[],
        help="Input feed JSON path. Can be repeated.",
    )
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Do not deduplicate by symbol.",
    )
    parser.add_argument(
        "--priority",
        action="append",
        default=[],
        help="Source priority token. Earlier priority wins. Can be repeated.",
    )
    parser.add_argument(
        "--build-context",
        action="store_true",
        help="After writing merged feed, build daily positioning context snapshot.",
    )

    args = parser.parse_args()

    if not args.feed:
        raise SystemExit("ERROR: provide at least one --feed path")

    payload = merge_positioning_feeds(
        feed_paths=args.feed,
        target_date=args.date,
        dedupe_by_symbol=not args.no_dedupe,
        source_priority=args.priority,
    )

    output_path = write_merged_feed(
        payload,
        runtime_dir=args.runtime_dir,
        output_path=args.output,
    )

    print(f"merged_feed={output_path}")
    print(f"date={payload.get('date')}")
    print(f"items={len(payload.get('items') or [])}")
    print(f"warnings={len(payload.get('merge_meta', {}).get('warnings') or [])}")

    if args.build_context:
        from .positioning_service import build_daily_positioning_context

        snapshot = build_daily_positioning_context(
            runtime_dir=args.runtime_dir,
            feed_path=str(output_path),
            persist=True,
        )
        print(f"context_status={snapshot.get('status')}")
        print(f"context_items={len(snapshot.get('items') or [])}")


if __name__ == "__main__":
    main()
