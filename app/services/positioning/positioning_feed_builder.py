from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path
from typing import Any

from .positioning_store import (
    get_manual_feed_path,
    get_positioning_dir,
    write_json_atomic,
)


POSITIONING_FEED_BUILDER_VERSION = "positioning-feed-builder-v0.1-csv"


DEFAULT_INPUT_CSV = "daily_positioning_input.csv"


CSV_FIELDS = [
    "symbol",
    "price_change_pct",
    "volume_change_pct_vs_20d",
    "open_interest_change_pct",
    "price",
    "volume",
    "open_interest",
    "source",
    "source_timestamp",
    "notes",
    "flags",
]


def build_manual_feed_from_csv(
    csv_path: str | Path,
    target_date: str | None = None,
) -> dict[str, Any]:
    """
    Build manual_daily_positioning_feed.json payload from a simple CSV.

    v0.1 purpose:
    - operator-friendly daily data input;
    - no trading logic;
    - no Battle Gate impact;
    - feed only for Positioning Intelligence research/context layer.
    """

    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV input not found: {path}")

    items: list[dict[str, Any]] = []

    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)

        for row_idx, row in enumerate(reader, start=2):
            item = _row_to_feed_item(row, row_idx=row_idx)
            if item is None:
                continue
            items.append(item)

    return {
        "version": POSITIONING_FEED_BUILDER_VERSION,
        "date": target_date or date.today().isoformat(),
        "items": items,
    }


def write_manual_feed(
    payload: dict[str, Any],
    runtime_dir: str | None = None,
    output_path: str | None = None,
) -> Path:
    path = Path(output_path) if output_path else get_manual_feed_path(runtime_dir=runtime_dir)
    write_json_atomic(path, payload)
    return path


def ensure_sample_csv(
    runtime_dir: str | None = None,
    csv_path: str | None = None,
    overwrite: bool = False,
) -> Path:
    path = Path(csv_path) if csv_path else get_positioning_dir(runtime_dir) / DEFAULT_INPUT_CSV
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and not overwrite:
        return path

    rows = [
        {
            "symbol": "XAUUSD",
            "price_change_pct": "1.18",
            "volume_change_pct_vs_20d": "22.4",
            "open_interest_change_pct": "3.72",
            "price": "",
            "volume": "",
            "open_interest": "",
            "source": "manual_cme_gc_proxy",
            "source_timestamp": "",
            "notes": "sample row",
            "flags": "",
        },
        {
            "symbol": "NAS100",
            "price_change_pct": "0.85",
            "volume_change_pct_vs_20d": "5.1",
            "open_interest_change_pct": "-1.40",
            "price": "",
            "volume": "",
            "open_interest": "",
            "source": "manual_cme_nq_proxy",
            "source_timestamp": "",
            "notes": "sample row",
            "flags": "",
        },
        {
            "symbol": "BTCUSD",
            "price_change_pct": "-2.40",
            "volume_change_pct_vs_20d": "31.0",
            "open_interest_change_pct": "-2.10",
            "price": "",
            "volume": "",
            "open_interest": "",
            "source": "manual_cme_btc_proxy",
            "source_timestamp": "",
            "notes": "sample row",
            "flags": "CRYPTO_EXCHANGE_OI_NOISY",
        },
    ]

    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    return path


def _row_to_feed_item(row: dict[str, Any], row_idx: int) -> dict[str, Any] | None:
    symbol = _clean_str(row.get("symbol")).upper()
    if not symbol:
        return None

    item: dict[str, Any] = {
        "symbol": symbol,
        "price_change_pct": _to_float_or_none(row.get("price_change_pct")),
        "volume_change_pct_vs_20d": _to_float_or_none(row.get("volume_change_pct_vs_20d")),
        "open_interest_change_pct": _to_float_or_none(row.get("open_interest_change_pct")),
        "price": _to_float_or_none(row.get("price")),
        "volume": _to_float_or_none(row.get("volume")),
        "open_interest": _to_float_or_none(row.get("open_interest")),
        "source": _clean_str(row.get("source")) or "manual_csv_proxy",
        "source_timestamp": _clean_str(row.get("source_timestamp")) or None,
        "notes": _clean_str(row.get("notes")) or None,
        "flags": _parse_flags(row.get("flags")),
    }

    # Remove optional numeric nulls to keep feed compact, but keep key metrics visible.
    for key in ("price", "volume", "open_interest"):
        if item.get(key) is None:
            item.pop(key, None)

    item["feed_builder_row"] = row_idx
    return item


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_float_or_none(value: Any) -> float | None:
    text = _clean_str(value)
    if not text:
        return None
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_flags(value: Any) -> list[str]:
    text = _clean_str(value)
    if not text:
        return []

    raw_parts: list[str] = []
    for chunk in text.replace(",", "|").split("|"):
        cleaned = chunk.strip().upper()
        if cleaned:
            raw_parts.append(cleaned)

    seen: set[str] = set()
    out: list[str] = []
    for part in raw_parts:
        if part not in seen:
            seen.add(part)
            out.append(part)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build Positioning Intelligence manual feed from CSV."
    )
    parser.add_argument("--csv", dest="csv_path", default=None, help="Input CSV path.")
    parser.add_argument("--date", dest="target_date", default=None, help="Feed date YYYY-MM-DD.")
    parser.add_argument("--runtime-dir", dest="runtime_dir", default=None, help="Runtime dir.")
    parser.add_argument("--output", dest="output_path", default=None, help="Output JSON feed path.")
    parser.add_argument(
        "--init-sample",
        action="store_true",
        help="Create sample CSV if missing and exit unless --build-context is also used.",
    )
    parser.add_argument(
        "--overwrite-sample",
        action="store_true",
        help="Overwrite sample CSV when using --init-sample.",
    )
    parser.add_argument(
        "--build-context",
        action="store_true",
        help="After writing feed, build daily positioning context snapshot.",
    )

    args = parser.parse_args()

    csv_path = args.csv_path
    if args.init_sample:
        sample_path = ensure_sample_csv(
            runtime_dir=args.runtime_dir,
            csv_path=csv_path,
            overwrite=args.overwrite_sample,
        )
        print(f"sample_csv={sample_path}")
        csv_path = str(sample_path)

        if not args.build_context:
            return

    if not csv_path:
        csv_path = str(get_positioning_dir(args.runtime_dir) / DEFAULT_INPUT_CSV)

    payload = build_manual_feed_from_csv(csv_path=csv_path, target_date=args.target_date)
    output_path = write_manual_feed(
        payload,
        runtime_dir=args.runtime_dir,
        output_path=args.output_path,
    )

    print(f"feed={output_path}")
    print(f"date={payload.get('date')}")
    print(f"items={len(payload.get('items') or [])}")

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
