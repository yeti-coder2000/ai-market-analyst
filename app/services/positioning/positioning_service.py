from __future__ import annotations

from typing import Any

from .positioning_models import (
    POSITIONING_LAYER_VERSION,
    PositioningContextItem,
    PositioningFeedItem,
    PositioningSnapshot,
    utc_now_iso,
)
from .positioning_store import (
    get_manual_feed_path,
    load_latest_snapshot,
    read_json_file,
    save_snapshot,
    save_source_health,
)
from .positioning_tagger import interpret_positioning_item


def build_daily_positioning_context(
    runtime_dir: str | None = None,
    feed_path: str | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    """
    Build daily positioning context from manual feed.

    v0.1 behavior:
    - read manual JSON feed;
    - normalize items;
    - generate research-only tags;
    - save latest/history/source_health;
    - return snapshot dict.

    This function must not affect Battle Gate or signal permission.
    """

    path = get_manual_feed_path(runtime_dir=runtime_dir, feed_path=feed_path)
    warnings: list[str] = []
    source_health: dict[str, Any] = {
        "status": "UNKNOWN",
        "input_path": str(path),
        "feed_exists": path.exists(),
        "feed_date": None,
        "item_count": 0,
        "errors": [],
    }

    if not path.exists():
        source_health["status"] = "MISSING_FEED"
        source_health["errors"].append("manual_daily_positioning_feed.json not found")
        snapshot = PositioningSnapshot(
            version=POSITIONING_LAYER_VERSION,
            generated_at=utc_now_iso(),
            date="unknown",
            status="DATA_UNAVAILABLE",
            items=[],
            source_health=source_health,
            warnings=["Positioning manual feed not found. Context unavailable."],
        )
        if persist:
            save_source_health(source_health, runtime_dir)
            save_snapshot(snapshot, runtime_dir)
        return snapshot.to_dict()

    try:
        feed = read_json_file(path)
    except Exception as exc:
        source_health["status"] = "BAD_FEED"
        source_health["errors"].append(f"{type(exc).__name__}: {exc}")
        snapshot = PositioningSnapshot(
            version=POSITIONING_LAYER_VERSION,
            generated_at=utc_now_iso(),
            date="unknown",
            status="DATA_UNAVAILABLE",
            items=[],
            source_health=source_health,
            warnings=["Positioning feed could not be read."],
        )
        if persist:
            save_source_health(source_health, runtime_dir)
            save_snapshot(snapshot, runtime_dir)
        return snapshot.to_dict()

    date = str(feed.get("date") or "unknown")
    raw_items = feed.get("items") or []

    if not isinstance(raw_items, list):
        source_health["status"] = "BAD_FEED"
        source_health["errors"].append("feed.items must be a list")
        raw_items = []

    context_items: list[PositioningContextItem] = []

    for idx, raw in enumerate(raw_items):
        if not isinstance(raw, dict):
            warnings.append(f"Skipped item #{idx}: expected object")
            continue

        item = PositioningFeedItem.from_dict(raw)
        if not item.symbol:
            warnings.append(f"Skipped item #{idx}: missing symbol")
            continue

        interpretation = interpret_positioning_item(item)

        context_items.append(
            PositioningContextItem(
                date=date,
                symbol=item.symbol,
                market_proxy={
                    "proxy_type": "daily_participation_proxy",
                    "source": item.source,
                    "source_timestamp": item.source_timestamp,
                },
                daily_market_data={
                    "price": item.price,
                    "price_change_pct": item.price_change_pct,
                    "volume": item.volume,
                    "volume_change_pct_vs_20d": item.volume_change_pct_vs_20d,
                    "open_interest": item.open_interest,
                    "open_interest_change_pct": item.open_interest_change_pct,
                },
                positioning_interpretation=interpretation,
                auction_usage={
                    "tpo_impact": "context_only",
                    "battle_gate_impact": "none",
                    "telegram_signal_impact": "none",
                    "recommended_usage": interpretation.tpo_note,
                },
                data_quality={
                    "status": interpretation.data_quality,
                    "source_lag": "previous_session_or_manual_input",
                    "flags": interpretation.flags,
                },
                raw_source=item.to_dict(),
            )
        )

    source_health["status"] = "OK" if context_items else "NO_VALID_ITEMS"
    source_health["feed_date"] = date
    source_health["item_count"] = len(context_items)

    snapshot = PositioningSnapshot(
        version=POSITIONING_LAYER_VERSION,
        generated_at=utc_now_iso(),
        date=date,
        status="OK" if context_items else "DATA_UNAVAILABLE",
        items=context_items,
        source_health=source_health,
        warnings=warnings,
    )

    if persist:
        save_source_health(source_health, runtime_dir)
        save_snapshot(snapshot, runtime_dir)

    return snapshot.to_dict()


def get_latest_positioning_context(runtime_dir: str | None = None) -> dict[str, Any] | None:
    return load_latest_snapshot(runtime_dir)


def get_positioning_item_for_symbol(
    symbol: str,
    snapshot: dict[str, Any] | None = None,
    runtime_dir: str | None = None,
) -> dict[str, Any] | None:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return None

    source = snapshot if snapshot is not None else get_latest_positioning_context(runtime_dir)
    if not source:
        return None

    for item in source.get("items", []):
        if str(item.get("symbol", "")).upper() == normalized:
            return item
    return None
