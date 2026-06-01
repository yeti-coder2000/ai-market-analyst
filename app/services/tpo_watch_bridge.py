from __future__ import annotations

"""
TPO Watch Bridge for AI Market Analyst.

Purpose:
- Convert precomputed TPO/open-behavior context into a live watch-state.
- This module does NOT generate trade entries.
- It only answers: "Is there a valid auction context that should be watched
  for a 5m-15m LTF model?"

Pipeline idea:
HTF context
→ TPO open context / open behavior
→ TPO Watch Bridge
→ LTF_MODEL_PENDING
→ LTF_MODEL_CONFIRMED
→ execution plan
→ Battle Gate
→ Telegram

Important:
- POC/nPOC/VAH/VAL are interest zones, not entry triggers.
- OPEN_TEST_DRIVE + HTF NEUTRAL is allowed as a transition candidate.
- A watch-state is not a battle signal.
"""

from dataclasses import dataclass, field, asdict
from typing import Any


TPO_WATCH_BRIDGE_VERSION = "tpo-watch-bridge-v1.0-open-behavior-to-ltf-pending"


BLOCK_MARKET_STATUSES = {
    "MARKET_CLOSED",
    "MARKET_CLOSED_AND_STALE",
    "STALE_DATA",
    "DATA_STALE",
    "CLOSED",
}

BLOCK_PERMISSIONS = {
    "BLOCK",
    "MARKET_CLOSED",
    "STALE_DATA",
    "DATA_STALE",
}

DOWNGRADE_MODIFIERS = {
    "DOWNGRADE",
    "BLOCK",
}


@dataclass
class TPOWatchResult:
    version: str = TPO_WATCH_BRIDGE_VERSION

    tpo_watch_state: str = "NO_WATCH"
    ltf_model_state: str = "NO_MODEL"

    tpo_watch_active: bool = False
    tpo_watch_setup: str | None = None
    tpo_watch_reason: str | None = None

    open_context: str | None = None
    open_behavior: str | None = None
    open_behavior_confidence: float | None = None

    market_status: str | None = None
    tpo_signal_permission: str | None = None
    tpo_telegram_modifier: str | None = None

    entry_model_hint: str | None = None
    stop_model_hint: str | None = None
    battle_bias_hint: str | None = None

    primary_interest_zone: dict[str, Any] | None = None
    interest_zone_type: str | None = None
    interest_zone_price: float | None = None
    interest_zone_role: str | None = None

    direction: str | None = None
    htf_bias: str | None = None
    allowed_htf_neutral_transition: bool = False
    htf_alignment_state: str = "UNKNOWN"

    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _s(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip().upper()


def _raw_s(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _f(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _extract_zone(*sources: Any) -> dict[str, Any] | None:
    for source in sources:
        if not isinstance(source, dict):
            continue

        for key in ("primary_interest_zone", "interest_zone", "zone"):
            zone = source.get(key)
            if isinstance(zone, dict) and zone:
                return dict(zone)

    return None


def _zone_type(zone: dict[str, Any] | None) -> str | None:
    if not isinstance(zone, dict):
        return None
    value = _first_non_empty(zone.get("zone_type"), zone.get("type"))
    return _raw_s(value) if value is not None else None


def _zone_price(zone: dict[str, Any] | None) -> float | None:
    if not isinstance(zone, dict):
        return None
    return _f(_first_non_empty(zone.get("price"), zone.get("level")))


def _zone_role(zone: dict[str, Any] | None) -> str | None:
    if not isinstance(zone, dict):
        return None
    value = _first_non_empty(zone.get("role"), zone.get("zone_role"))
    return _raw_s(value) if value is not None else None


def _normalize_tpo_record(
    *,
    symbol_payload: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    filters: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """
    Accept either:
    - full symbol record from tpo_latest.json: {"context": {...}, "filters": {...}}
    - separate context/filters dicts
    - already flattened payload-like dict

    Returns:
    - record
    - context
    - filters
    """
    record = symbol_payload if isinstance(symbol_payload, dict) else {}

    if context is None:
        candidate = record.get("context")
        context = candidate if isinstance(candidate, dict) else {}

    if filters is None:
        candidate = record.get("filters")
        filters = candidate if isinstance(candidate, dict) else {}

    return record, context or {}, filters or {}


def evaluate_tpo_watch_bridge(
    *,
    symbol: str | None = None,
    direction: str | None = None,
    htf_bias: str | None = None,
    symbol_payload: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    filters: dict[str, Any] | None = None,
    signal_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Convert TPO open behavior into live watch-state.

    This function does NOT create ENTRY_READY.
    It creates LTF_MODEL_PENDING when auction context is worth watching.
    """
    del symbol
    signal_payload = signal_payload if isinstance(signal_payload, dict) else {}
    record, ctx, flt = _normalize_tpo_record(
        symbol_payload=symbol_payload,
        context=context,
        filters=filters,
    )

    result = TPOWatchResult()

    result.direction = _s(
        _first_non_empty(
            direction,
            signal_payload.get("direction"),
            record.get("direction"),
            ctx.get("direction"),
            flt.get("direction"),
        )
    ) or None

    result.htf_bias = _s(
        _first_non_empty(
            htf_bias,
            signal_payload.get("htf_bias"),
            record.get("htf_bias"),
            ctx.get("htf_bias"),
            flt.get("htf_bias"),
        ),
        "NEUTRAL",
    )

    result.market_status = _s(
        _first_non_empty(
            signal_payload.get("market_status"),
            record.get("market_status"),
            ctx.get("market_status"),
            flt.get("market_status"),
        ),
        "OPEN",
    )

    result.tpo_signal_permission = _s(
        _first_non_empty(
            signal_payload.get("tpo_signal_permission"),
            record.get("tpo_signal_permission"),
            ctx.get("tpo_signal_permission"),
            flt.get("tpo_signal_permission"),
            flt.get("permission"),
        ),
        "OPEN_FOR_EVALUATION",
    )

    result.tpo_telegram_modifier = _s(
        _first_non_empty(
            signal_payload.get("tpo_telegram_modifier"),
            record.get("tpo_telegram_modifier"),
            ctx.get("tpo_telegram_modifier"),
            flt.get("telegram_modifier"),
            flt.get("modifier"),
        ),
        "NEUTRAL",
    )

    result.open_context = _s(
        _first_non_empty(
            signal_payload.get("open_context"),
            record.get("open_context"),
            ctx.get("open_context"),
            ctx.get("open_relation"),
            flt.get("open_context"),
            flt.get("open_relation"),
        )
    ) or None

    result.open_behavior = _s(
        _first_non_empty(
            signal_payload.get("open_behavior"),
            record.get("open_behavior"),
            ctx.get("open_behavior"),
            flt.get("open_behavior"),
        )
    ) or "UNCONFIRMED"

    result.open_behavior_confidence = _f(
        _first_non_empty(
            signal_payload.get("open_behavior_confidence"),
            record.get("open_behavior_confidence"),
            ctx.get("open_behavior_confidence"),
            flt.get("open_behavior_confidence"),
        )
    )

    result.entry_model_hint = _s(
        _first_non_empty(
            signal_payload.get("entry_model_hint"),
            record.get("entry_model_hint"),
            ctx.get("entry_model_hint"),
            flt.get("entry_model_hint"),
        )
    ) or None

    result.stop_model_hint = _s(
        _first_non_empty(
            signal_payload.get("stop_model_hint"),
            record.get("stop_model_hint"),
            ctx.get("stop_model_hint"),
            flt.get("stop_model_hint"),
        )
    ) or None

    result.battle_bias_hint = _s(
        _first_non_empty(
            signal_payload.get("battle_bias_hint"),
            record.get("battle_bias_hint"),
            ctx.get("battle_bias_hint"),
            flt.get("battle_bias_hint"),
        )
    ) or None

    zone = _extract_zone(signal_payload, record, ctx, flt)
    result.primary_interest_zone = zone
    result.interest_zone_type = _zone_type(zone)
    result.interest_zone_price = _zone_price(zone)
    result.interest_zone_role = _zone_role(zone)

    if result.market_status in BLOCK_MARKET_STATUSES:
        result.tpo_watch_state = "BLOCKED"
        result.ltf_model_state = "NO_MODEL"
        result.tpo_watch_active = False
        result.tpo_watch_setup = result.open_behavior
        result.tpo_watch_reason = f"Market/TPO data status blocks watch: {result.market_status}."
        result.blockers.append(f"market_status_{result.market_status.lower()}")
        result.reasons.append(result.tpo_watch_reason)
        return result.to_dict()

    if result.tpo_signal_permission in BLOCK_PERMISSIONS:
        result.tpo_watch_state = "BLOCKED"
        result.ltf_model_state = "NO_MODEL"
        result.tpo_watch_active = False
        result.tpo_watch_setup = result.open_behavior
        result.tpo_watch_reason = f"TPO permission blocks watch: {result.tpo_signal_permission}."
        result.blockers.append(f"tpo_permission_{result.tpo_signal_permission.lower()}")
        result.reasons.append(result.tpo_watch_reason)
        return result.to_dict()

    if result.tpo_telegram_modifier in DOWNGRADE_MODIFIERS:
        result.tpo_watch_state = "OBSERVE_ONLY"
        result.ltf_model_state = "NO_MODEL"
        result.tpo_watch_active = False
        result.tpo_watch_setup = result.open_behavior
        result.tpo_watch_reason = f"TPO modifier is {result.tpo_telegram_modifier}; no battle watch."
        result.blockers.append(f"tpo_modifier_{result.tpo_telegram_modifier.lower()}")
        result.reasons.append(result.tpo_watch_reason)
        return result.to_dict()

    direction_value = result.direction or ""
    htf_bias_value = result.htf_bias or "NEUTRAL"

    if direction_value and htf_bias_value == "NEUTRAL":
        result.htf_alignment_state = "NEUTRAL_TRANSITION_CANDIDATE"
    elif direction_value and direction_value == htf_bias_value and htf_bias_value != "NEUTRAL":
        result.htf_alignment_state = "HTF_ALIGNED"
    elif direction_value and htf_bias_value and direction_value != htf_bias_value:
        result.htf_alignment_state = "HTF_CONFLICT"
    else:
        result.htf_alignment_state = "UNKNOWN"

    if result.open_behavior == "OPEN_TEST_DRIVE":
        result.tpo_watch_setup = "OPEN_TEST_DRIVE"

        if result.htf_alignment_state == "HTF_CONFLICT":
            result.tpo_watch_state = "RESEARCH_ONLY"
            result.ltf_model_state = "NO_MODEL"
            result.tpo_watch_active = False
            result.tpo_watch_reason = (
                "OPEN_TEST_DRIVE detected, but direction conflicts with HTF bias; "
                "keep research-only unless later policy explicitly allows it."
            )
            result.blockers.append("htf_conflict")
            result.reasons.append(result.tpo_watch_reason)
            return result.to_dict()

        if result.htf_alignment_state == "NEUTRAL_TRANSITION_CANDIDATE":
            result.allowed_htf_neutral_transition = True
            result.warnings.append("htf_neutral_transition_candidate")

        result.tpo_watch_state = "LTF_MODEL_PENDING"
        result.ltf_model_state = "PENDING"
        result.tpo_watch_active = True
        result.tpo_watch_reason = (
            "OPEN_TEST_DRIVE context is active; wait for 5m-15m LTF model "
            "before any ENTRY_READY signal."
        )
        result.reasons.append(result.tpo_watch_reason)

        if result.primary_interest_zone:
            result.reasons.append("Primary interest zone is available; zone is not an entry trigger.")

        return result.to_dict()

    if result.open_behavior == "OPEN_DRIVE":
        result.tpo_watch_setup = "OPEN_DRIVE"

        if result.htf_alignment_state != "HTF_ALIGNED":
            result.tpo_watch_state = "RESEARCH_ONLY"
            result.ltf_model_state = "NO_MODEL"
            result.tpo_watch_active = False
            result.tpo_watch_reason = "OPEN_DRIVE requires HTF alignment for battle watch."
            result.blockers.append("open_drive_without_htf_alignment")
            result.reasons.append(result.tpo_watch_reason)
            return result.to_dict()

        result.tpo_watch_state = "LTF_MODEL_PENDING"
        result.ltf_model_state = "PENDING"
        result.tpo_watch_active = True
        result.tpo_watch_reason = "OPEN_DRIVE context is active; wait for pullback/continuation LTF model."
        result.reasons.append(result.tpo_watch_reason)
        return result.to_dict()

    if result.open_behavior == "OPEN_REJECTION_REVERSE":
        result.tpo_watch_setup = "OPEN_REJECTION_REVERSE"
        result.tpo_watch_state = "LTF_MODEL_PENDING"
        result.ltf_model_state = "PENDING"
        result.tpo_watch_active = True
        result.tpo_watch_reason = (
            "OPEN_REJECTION_REVERSE context is active; cautious watch only, "
            "requires very clean reclaim/BOS/retest."
        )
        result.warnings.append("orr_requires_caution")
        result.reasons.append(result.tpo_watch_reason)
        return result.to_dict()

    if result.open_behavior == "OPEN_AUCTION":
        result.tpo_watch_setup = "OPEN_AUCTION"
        result.tpo_watch_state = "OBSERVE_ROTATION"
        result.ltf_model_state = "NO_MODEL"
        result.tpo_watch_active = False
        result.tpo_watch_reason = "OPEN_AUCTION is observe/rotation context, not directional battle watch."
        result.reasons.append(result.tpo_watch_reason)
        return result.to_dict()

    result.tpo_watch_setup = result.open_behavior
    result.tpo_watch_state = "NO_WATCH"
    result.ltf_model_state = "NO_MODEL"
    result.tpo_watch_active = False
    result.tpo_watch_reason = f"Open behavior is not actionable: {result.open_behavior}."
    result.reasons.append(result.tpo_watch_reason)
    return result.to_dict()


def enrich_payload_with_tpo_watch(
    payload: dict[str, Any],
    *,
    symbol_payload: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Add TPO watch fields to any signal/journal payload.
    Does not mutate the original payload.
    """
    enriched = dict(payload)

    watch = evaluate_tpo_watch_bridge(
        symbol=enriched.get("symbol"),
        direction=enriched.get("direction"),
        htf_bias=enriched.get("htf_bias"),
        symbol_payload=symbol_payload,
        context=context,
        filters=filters,
        signal_payload=enriched,
    )

    metadata = enriched.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    for key, value in watch.items():
        enriched[key] = value
        metadata[key] = value

    enriched["metadata"] = metadata
    return enriched