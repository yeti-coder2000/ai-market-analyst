from __future__ import annotations

from datetime import date as calendar_date
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
    fallback_date: str | None = None,
) -> dict[str, Any]:
    """
    Build daily Positioning Intelligence context from a normalized feed.

    The feed may be manual, collector-generated, or merged. This function is
    research/context only and must not affect Battle Gate or signal permission.
    """

    path = get_manual_feed_path(runtime_dir=runtime_dir, feed_path=feed_path)
    resolved_fallback_date = str(fallback_date or calendar_date.today().isoformat())
    warnings: list[str] = []
    source_health: dict[str, Any] = {
        "status": "NO_DATA",
        "input_path": str(path),
        "feed_exists": path.exists(),
        "feed_date": None,
        "item_count": 0,
        "errors": [],
        "pipeline": {},
        "merge": {},
    }

    if not path.exists():
        source_health["status"] = "NO_DATA"
        snapshot = PositioningSnapshot(
            version=POSITIONING_LAYER_VERSION,
            generated_at=utc_now_iso(),
            date=resolved_fallback_date,
            status="NO_DATA",
            items=[],
            source_health=source_health,
            warnings=["Positioning feed not found. Context unavailable."],
        )
        if persist:
            save_source_health(source_health, runtime_dir)
            save_snapshot(snapshot, runtime_dir)
        return snapshot.to_dict()

    try:
        feed = read_json_file(path)
    except Exception as exc:
        source_health["status"] = "ERROR"
        source_health["errors"].append(f"{type(exc).__name__}: {exc}")
        snapshot = PositioningSnapshot(
            version=POSITIONING_LAYER_VERSION,
            generated_at=utc_now_iso(),
            date=resolved_fallback_date,
            status="ERROR",
            items=[],
            source_health=source_health,
            warnings=["Positioning feed could not be read."],
        )
        if persist:
            save_source_health(source_health, runtime_dir)
            save_snapshot(snapshot, runtime_dir)
        return snapshot.to_dict()

    date_value = str(feed.get("date") or resolved_fallback_date)
    raw_items = feed.get("items") or []
    pipeline_meta = feed.get("pipeline_meta") if isinstance(feed.get("pipeline_meta"), dict) else {}
    merge_meta = feed.get("merge_meta") if isinstance(feed.get("merge_meta"), dict) else {}

    source_health["pipeline"] = pipeline_meta
    source_health["merge"] = merge_meta

    for value in pipeline_meta.get("warnings") or []:
        warnings.append(str(value))
    for value in merge_meta.get("warnings") or []:
        warnings.append(str(value))

    if not isinstance(raw_items, list):
        source_health["status"] = "ERROR"
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
                date=date_value,
                symbol=item.symbol,
                market_proxy={
                    "proxy_type": "daily_participation_proxy",
                    "source": item.source,
                    "source_timestamp": item.source_timestamp,
                    "operational_window": item.operational_window,
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

    status_hint = str(pipeline_meta.get("status_hint") or "").strip().upper()
    if context_items:
        snapshot_status = status_hint if status_hint in {"OK", "PARTIAL", "STALE"} else "OK"
    else:
        snapshot_status = "ERROR" if status_hint == "ERROR" or source_health["errors"] else "NO_DATA"

    source_health["status"] = snapshot_status
    source_health["feed_date"] = date_value
    source_health["item_count"] = len(context_items)

    snapshot = PositioningSnapshot(
        version=POSITIONING_LAYER_VERSION,
        generated_at=utc_now_iso(),
        date=date_value,
        status=snapshot_status,
        items=context_items,
        source_health=source_health,
        warnings=_dedupe_strings(warnings),
    )

    if persist:
        save_source_health(source_health, runtime_dir)
        save_snapshot(snapshot, runtime_dir)

    return snapshot.to_dict()


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        cleaned = str(value or "").strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out

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
