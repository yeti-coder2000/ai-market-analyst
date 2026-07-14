from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterable

from .positioning_service import (
    get_latest_positioning_context,
    get_positioning_item_for_symbol,
)


POSITIONING_RECORD_ENRICHER_VERSION = "positioning-record-enricher-v0.1-research-only"

RESEARCH_ONLY = "RESEARCH_ONLY"
NO_BATTLE_GATE_IMPACT = "none"
NO_TELEGRAM_SIGNAL_IMPACT = "none"


DEFAULT_SYMBOL_KEYS = (
    "symbol",
    "asset",
    "instrument",
    "ticker",
    "market",
    "market_symbol",
)


DEFAULT_DIRECTION_KEYS = (
    "direction",
    "side",
    "signal_direction",
    "setup_direction",
    "scenario",
    "signal_type",
    "trade_type",
    "model",
)


def enrich_record_with_positioning(
    record: dict[str, Any],
    snapshot: dict[str, Any] | None = None,
    runtime_dir: str | None = None,
    symbol_keys: Iterable[str] = DEFAULT_SYMBOL_KEYS,
    direction_keys: Iterable[str] = DEFAULT_DIRECTION_KEYS,
    mutate: bool = False,
) -> dict[str, Any]:
    """
    Attach Positioning Intelligence metadata to one signal/stat/candidate record.

    v0.1 safety rules:
    - does NOT modify Battle Gate decision;
    - does NOT allow signals;
    - does NOT block signals;
    - only adds metadata for research/statistics/outcome tracking.
    """

    out = record if mutate else deepcopy(record)

    symbol = _extract_symbol(out, symbol_keys)
    direction = _infer_direction(out, direction_keys)

    if not symbol:
        return _attach_unavailable(
            out,
            reason="missing_symbol",
            direction=direction,
        )

    source_snapshot = snapshot if snapshot is not None else get_latest_positioning_context(runtime_dir)
    item = get_positioning_item_for_symbol(symbol, snapshot=source_snapshot)

    if not item:
        return _attach_unavailable(
            out,
            reason="positioning_item_not_found",
            direction=direction,
            symbol=symbol,
        )

    interp = item.get("positioning_interpretation", {}) or {}
    market = item.get("daily_market_data", {}) or {}
    data_quality = item.get("data_quality", {}) or {}
    auction_usage = item.get("auction_usage", {}) or {}

    primary_tag = str(interp.get("primary_tag") or "DATA_UNAVAILABLE")
    secondary_tags = list(interp.get("secondary_tags") or [])
    confidence = interp.get("confidence")
    quality_status = interp.get("data_quality") or data_quality.get("status") or "UNKNOWN"

    alignment = classify_positioning_alignment(primary_tag=primary_tag, direction=direction)

    out.update(
        {
            "positioning_enricher_version": POSITIONING_RECORD_ENRICHER_VERSION,
            "positioning_mode": RESEARCH_ONLY,
            "positioning_symbol": symbol,
            "positioning_primary_tag": primary_tag,
            "positioning_secondary_tags": secondary_tags,
            "positioning_confidence": confidence,
            "positioning_data_quality": quality_status,
            "positioning_alignment": alignment,
            "positioning_direction_inferred": direction,
            "positioning_price_change_pct": market.get("price_change_pct"),
            "positioning_volume_change_pct_vs_20d": market.get("volume_change_pct_vs_20d"),
            "positioning_open_interest_change_pct": market.get("open_interest_change_pct"),
            "positioning_battle_gate_impact": auction_usage.get("battle_gate_impact") or NO_BATTLE_GATE_IMPACT,
            "positioning_telegram_signal_impact": auction_usage.get("telegram_signal_impact") or NO_TELEGRAM_SIGNAL_IMPACT,
            "positioning_tpo_impact": auction_usage.get("tpo_impact") or "context_only",
            "positioning_context_note": interp.get("tpo_note") or auction_usage.get("recommended_usage"),
            "positioning_interpretation": interp.get("interpretation"),
            "positioning_source_lag": data_quality.get("source_lag"),
            "positioning_flags": data_quality.get("flags") or interp.get("flags") or [],
        }
    )

    # Hard safety rail: even if source data accidentally says otherwise, v0.1 cannot alter permissions.
    out["positioning_battle_gate_impact"] = NO_BATTLE_GATE_IMPACT
    out["positioning_telegram_signal_impact"] = NO_TELEGRAM_SIGNAL_IMPACT
    out["positioning_can_allow_signal"] = False
    out["positioning_can_block_signal"] = False

    return out


def enrich_records_with_positioning(
    records: list[dict[str, Any]],
    snapshot: dict[str, Any] | None = None,
    runtime_dir: str | None = None,
    mutate: bool = False,
) -> list[dict[str, Any]]:
    source_snapshot = snapshot if snapshot is not None else get_latest_positioning_context(runtime_dir)
    return [
        enrich_record_with_positioning(
            record,
            snapshot=source_snapshot,
            runtime_dir=runtime_dir,
            mutate=mutate,
        )
        for record in records
    ]


def classify_positioning_alignment(primary_tag: str, direction: str) -> str:
    tag = str(primary_tag or "").upper()
    side = str(direction or "UNKNOWN").upper()

    if tag in {"DATA_UNAVAILABLE", "DATA_STALE"}:
        return "POSITIONING_UNAVAILABLE"

    if tag in {"POSITIONING_NEUTRAL", "LOW_CONVICTION_MOVE"}:
        return "POSITIONING_NEUTRAL_OR_WEAK"

    if side == "LONG":
        if tag == "FRESH_LONG_PARTICIPATION":
            return "SUPPORTS_LONG_CONTINUATION_CONTEXT"
        if tag == "SHORT_COVERING_RISK":
            return "CAUTION_WEAK_LONG_CONTINUATION"
        if tag == "FRESH_SHORT_PARTICIPATION":
            return "CONFLICTS_WITH_LONG_CONTEXT"
        if tag == "LONG_LIQUIDATION_RISK":
            return "CAUTION_AFTER_LIQUIDATION_IMPULSE"
        return "CONTEXT_ONLY"

    if side == "SHORT":
        if tag == "FRESH_SHORT_PARTICIPATION":
            return "SUPPORTS_SHORT_CONTINUATION_CONTEXT"
        if tag == "LONG_LIQUIDATION_RISK":
            return "CAUTION_WEAK_SHORT_CONTINUATION"
        if tag == "FRESH_LONG_PARTICIPATION":
            return "CONFLICTS_WITH_SHORT_CONTEXT"
        if tag == "SHORT_COVERING_RISK":
            return "CAUTION_AFTER_COVERING_IMPULSE"
        return "CONTEXT_ONLY"

    return "CONTEXT_ONLY_DIRECTION_UNKNOWN"


def _attach_unavailable(
    record: dict[str, Any],
    reason: str,
    direction: str,
    symbol: str | None = None,
) -> dict[str, Any]:
    record.update(
        {
            "positioning_enricher_version": POSITIONING_RECORD_ENRICHER_VERSION,
            "positioning_mode": RESEARCH_ONLY,
            "positioning_symbol": symbol,
            "positioning_primary_tag": "DATA_UNAVAILABLE",
            "positioning_secondary_tags": [],
            "positioning_confidence": None,
            "positioning_data_quality": "BAD",
            "positioning_alignment": "POSITIONING_UNAVAILABLE",
            "positioning_direction_inferred": direction,
            "positioning_unavailable_reason": reason,
            "positioning_battle_gate_impact": NO_BATTLE_GATE_IMPACT,
            "positioning_telegram_signal_impact": NO_TELEGRAM_SIGNAL_IMPACT,
            "positioning_tpo_impact": "context_only",
            "positioning_can_allow_signal": False,
            "positioning_can_block_signal": False,
        }
    )
    return record


def _extract_symbol(record: dict[str, Any], keys: Iterable[str]) -> str | None:
    for key in keys:
        value = record.get(key)
        if value:
            return str(value).strip().upper()
    return None


def _infer_direction(record: dict[str, Any], keys: Iterable[str]) -> str:
    blob_parts: list[str] = []

    for key in keys:
        value = record.get(key)
        if value:
            blob_parts.append(str(value))

    blob = " ".join(blob_parts).upper()

    if any(token in blob for token in ("LONG", "BUY", "BULL", "BULLISH", "_LONG")):
        return "LONG"

    if any(token in blob for token in ("SHORT", "SELL", "BEAR", "BEARISH", "_SHORT")):
        return "SHORT"

    return "UNKNOWN"
