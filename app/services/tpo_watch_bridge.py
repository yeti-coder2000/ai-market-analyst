from __future__ import annotations

"""
TPO Watch Bridge for AI Market Analyst.

v1.2 purpose:
- Consume the richer auction-state fields produced by
  tpo-open-behavior-classifier-v1.4-auction-state-transitions.
- Keep legacy open_behavior/open_context behavior backward-compatible.
- Prevent broad legacy OPEN_TEST_DRIVE from activating an LTF watch when the
  detailed auction state says the setup is only initial/unconfirmed or has
  accepted back inside value.
- Treat POC/nPOC/VAH/VAL as interest zones only; this bridge never creates
  ENTRY_READY.

Pipeline idea:
HTF context
→ TPO open context / auction-state fields
→ TPO Watch Bridge
→ LTF_MODEL_PENDING / OBSERVE_ONLY / RESEARCH_ONLY / BLOCKED
→ LTF_MODEL_CONFIRMED
→ execution plan
→ Battle Gate
→ Telegram

Important:
- OPEN_TEST_DRIVE + HTF NEUTRAL is allowed as a transition candidate.
- OPEN_TEST_DRIVE_CANDIDATE is a watch context only when value rejection is
  present and current behavior is not contradicted by acceptance back into value.
- OPEN_AUCTION remains observe/rotation unless later structure proves a real
  directional break and LTF model.
- A watch-state is not a battle signal.
"""

from dataclasses import asdict, dataclass, field
import re
from typing import Any


TPO_WATCH_BRIDGE_VERSION = "tpo-watch-bridge-v1.4-session-normalization-brain"


BLOCK_MARKET_STATUSES = {
    "MARKET_CLOSED",
    "MARKET_CLOSED_AND_STALE",
    "STALE_DATA",
    "DATA_STALE",
    "CLOSED",
}

BLOCK_PERMISSIONS = {
    "BLOCK",
    "BLOCKED",
    "MARKET_CLOSED",
    "STALE_DATA",
    "DATA_STALE",
    "SUPPRESS",
    "NO_TRADE",
    "NOT_ALLOWED",
}

BLOCK_TELEGRAM_MODIFIERS = {
    "BLOCK",
    "SUPPRESS",
}

DOWNGRADE_TELEGRAM_MODIFIERS = {
    "DOWNGRADE",
}

VALUE_REJECTION_STATES = {
    "VALUE_REJECTED_UP",
    "VALUE_REJECTED_DOWN",
}

VALUE_ACCEPTED_BACK_STATES = {
    "ACCEPTED_INSIDE_VALUE",
    "ACCEPTED_BACK_INSIDE_VALUE",
    "FAILED_OUTSIDE_VALUE",
}

OTD_DETAILED_STATES = {
    "OPEN_TEST_DRIVE_CANDIDATE",
    "OPEN_TEST_DRIVE_CONFIRMED",
}

OPEN_AUCTION_DETAILED_STATES = {
    "OPEN_AUCTION_IN_RANGE",
    "OPEN_AUCTION_OUT_OF_RANGE",
}

OPEN_DRIVE_DETAILED_STATES = {
    "OPEN_DRIVE_CANDIDATE",
    "OPEN_DRIVE_CONFIRMED",
}

VALUE_ACCEPTED_OUTSIDE_STATES = {
    "ACCEPTED_OUTSIDE_VALUE",
    "ACCEPTED_OUTSIDE_PRIOR_VALUE",
    "ACCEPTED_OUTSIDE_RANGE",
    "ACCEPTED_OUTSIDE_PRIOR_RANGE",
    "ACCEPTED_ABOVE_VALUE",
    "ACCEPTED_BELOW_VALUE",
    "ACCEPTED_BREAKOUT",
    "ACCEPTED_EXTENSION",
    "IB_ACCEPTED_EXTENSION",
}

VALUE_REJECTED_BACK_STATES = {
    "REJECTED_BACK_INTO_PRIOR_VALUE",
    "REJECTED_BACK_INTO_PRIOR_RANGE",
    "FAILED_ACCEPTANCE_INTO_PRIOR_VALUE",
    "FAILED_ACCEPTANCE_INTO_PRIOR_RANGE",
    "ACCEPTED_BACK_INSIDE_VALUE",
    "ACCEPTED_INSIDE_VALUE",
    "VALUE_ACCEPTED_INSIDE",
    "FAILED_OUTSIDE_VALUE",
}

DIRECTIONAL_BREAK_TRANSITIONS = {
    "OPEN_AUCTION_TO_DIRECTIONAL_BREAK",
    "OPEN_AUCTION_TO_OPEN_DRIVE",
    "ROTATION_TO_DIRECTIONAL_BREAK",
    "ROTATION_TO_ACCEPTED_BREAKOUT",
    "OAOR_TO_ACCEPTED_BREAKOUT",
    "OAIR_TO_ACCEPTED_BREAKOUT",
}

FAILED_ACCEPTANCE_TRANSITIONS = {
    "OAOR_TO_FAILED_ACCEPTANCE",
    "OUTSIDE_RANGE_TO_BACK_TO_VALUE",
    "OUTSIDE_VALUE_TO_BACK_TO_VALUE",
    "OPEN_AUCTION_TO_FAILED_ACCEPTANCE",
}

_QUOTED_ENUM_VALUE_RE = re.compile(r":\s*['\"]([^'\"]+)['\"]")


@dataclass
class TPOWatchResult:
    version: str = TPO_WATCH_BRIDGE_VERSION

    tpo_watch_state: str = "NO_WATCH"
    ltf_model_state: str = "NO_MODEL"

    tpo_watch_active: bool = False
    tpo_watch_setup: str | None = None
    tpo_watch_reason: str | None = None

    # Legacy broad fields kept for downstream compatibility.
    open_context: str | None = None
    open_behavior: str | None = None
    open_behavior_confidence: float | None = None

    # v1.2 richer auction-state fields from classifier v1.4.
    open_behavior_version: str | None = None
    open_location: str | None = None
    initial_open_behavior: str | None = None
    current_open_behavior: str | None = None
    behavior_transition: str | None = None
    value_acceptance_state: str | None = None
    value_test_occurred: bool | None = None
    value_test_level: str | None = None
    value_rejection_confirmed: bool | None = None
    day_type_candidate: str | None = None
    auction_state_confidence: float | None = None
    auction_state_reason: str | None = None

    # v1.4 session-normalization fields from classifier v1.5.
    session_normalization_version: str | None = None
    session_scope: str | None = None
    primary_session: str | None = None
    prior_value_scope: str | None = None
    prior_range_scope: str | None = None
    open_event: str | None = None
    open_event_type: str | None = None
    reference_profile_id: str | None = None
    active_participation_center: str | None = None
    profile_reliability_score: int | None = None
    profile_reliability_state: str | None = None
    session_status: str | None = None
    holiday_mode: str | None = None
    weekend_flag: bool | None = None
    synthetic_open: bool | None = None
    synthetic_open_confirmed: bool | None = None
    true_otd_allowed: bool | None = None

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
    raw_direction: str | None = None
    raw_htf_bias: str | None = None

    allowed_htf_neutral_transition: bool = False
    htf_alignment_state: str = "UNKNOWN"

    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _unwrap_enum_value(value: Any) -> Any:
    """Normalize Python Enum-like values before string conversion."""
    if value is None:
        return None

    enum_value = getattr(value, "value", None)
    if enum_value is not None and not isinstance(value, (str, bytes, int, float, bool)):
        return enum_value

    return value


def _s(value: Any, default: str = "") -> str:
    """Uppercase normalizer with Enum/string-enum protection."""
    if value is None:
        return default

    value = _unwrap_enum_value(value)

    # Avoid converting whole dicts/lists into unusable uppercase strings.
    if isinstance(value, (dict, list, tuple, set)):
        return default

    text = str(value).strip()

    if not text:
        return default

    quoted_match = _QUOTED_ENUM_VALUE_RE.search(text)
    if text.startswith("<") and quoted_match:
        text = quoted_match.group(1).strip()
    elif "." in text:
        tail = text.rsplit(".", 1)[-1].strip()
        if tail:
            text = tail

    return text.upper()


def _raw_s(value: Any, default: str = "") -> str:
    if value is None:
        return default

    value = _unwrap_enum_value(value)
    if isinstance(value, (dict, list, tuple, set)):
        return default

    text = str(value).strip()

    quoted_match = _QUOTED_ENUM_VALUE_RE.search(text)
    if text.startswith("<") and quoted_match:
        text = quoted_match.group(1).strip()

    return text or default


def _direction_s(value: Any, default: str = "") -> str:
    """Normalize direction-like values."""
    text = _s(value, default)

    if not text:
        return default

    if text in {"LONG", "BUY", "BULL", "BULLISH", "UP"}:
        return "LONG"

    if text in {"SHORT", "SELL", "BEAR", "BEARISH", "DOWN"}:
        return "SHORT"

    if text in {"NEUTRAL", "NONE", "FLAT", "NO_BIAS", "UNKNOWN"}:
        return "NEUTRAL"

    if "LONG" in text and "SHORT" not in text:
        return "LONG"

    if "SHORT" in text and "LONG" not in text:
        return "SHORT"

    return text


def _direction_from_scenario(*values: Any) -> str | None:
    for value in values:
        direction = _direction_s(value)
        if direction in {"LONG", "SHORT", "NEUTRAL"}:
            return direction
    return None


def _bool(value: Any, default: bool | None = None) -> bool | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off", "none", "null", ""}:
        return False
    return default


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
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    """
    Accept either:
    - full symbol record from tpo_latest.json: {"context": {...}, "filters": {...}, "open_behavior": {...}}
    - separate context/filters dicts
    - already flattened payload-like dict

    Returns:
    - record
    - context
    - filters
    - open_behavior record
    """
    record = symbol_payload if isinstance(symbol_payload, dict) else {}

    if context is None:
        candidate = record.get("context")
        context = candidate if isinstance(candidate, dict) else {}

    if filters is None:
        candidate = record.get("filters")
        filters = candidate if isinstance(candidate, dict) else {}

    behavior = record.get("open_behavior")
    behavior_record = behavior if isinstance(behavior, dict) else {}

    return record, context or {}, filters or {}, behavior_record


def _open_behavior_value(*values: Any) -> str | None:
    """
    Extract a broad open_behavior value from strings or nested behavior dicts.
    """
    for value in values:
        if isinstance(value, dict):
            candidate = _first_non_empty(
                value.get("open_behavior"),
                value.get("current_open_behavior"),
                value.get("initial_open_behavior"),
            )
            text = _s(candidate)
        else:
            text = _s(value)

        if text:
            return text

    return None


def _activate_ltf_pending(
    result: TPOWatchResult,
    *,
    reason: str,
) -> dict[str, Any]:
    result.tpo_watch_state = "LTF_MODEL_PENDING"
    result.ltf_model_state = "PENDING"
    result.tpo_watch_active = True
    result.tpo_watch_reason = reason
    result.reasons.append(reason)

    if result.primary_interest_zone:
        result.reasons.append("Primary interest zone is available; zone is not an entry trigger.")

    return result.to_dict()


def _set_blocked(
    result: TPOWatchResult,
    *,
    state: str,
    reason: str,
    blocker: str | None = None,
) -> dict[str, Any]:
    result.tpo_watch_state = state
    result.ltf_model_state = "NO_MODEL"
    result.tpo_watch_active = False
    result.tpo_watch_reason = reason
    if blocker:
        result.blockers.append(blocker)
    result.reasons.append(reason)
    return result.to_dict()


def _base_watch_setup(result: TPOWatchResult) -> str | None:
    return result.current_open_behavior or result.open_behavior or result.initial_open_behavior


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
    It creates LTF_MODEL_PENDING only when auction context is worth watching.
    """
    del symbol

    signal_payload = signal_payload if isinstance(signal_payload, dict) else {}
    record, ctx, flt, ob = _normalize_tpo_record(
        symbol_payload=symbol_payload,
        context=context,
        filters=filters,
    )

    result = TPOWatchResult()

    raw_direction = _first_non_empty(
        direction,
        signal_payload.get("direction"),
        record.get("direction"),
        ctx.get("direction"),
        flt.get("direction"),
        ob.get("direction"),
    )

    result.raw_direction = _raw_s(raw_direction) if raw_direction is not None else None
    result.direction = _direction_s(raw_direction) or _direction_from_scenario(
        signal_payload.get("scenario"),
        signal_payload.get("scenario_type"),
        record.get("scenario"),
        record.get("scenario_type"),
        ctx.get("scenario"),
        ctx.get("scenario_type"),
        flt.get("scenario"),
        flt.get("scenario_type"),
        ob.get("scenario"),
        ob.get("scenario_type"),
    )

    raw_htf_bias = _first_non_empty(
        htf_bias,
        signal_payload.get("htf_bias"),
        record.get("htf_bias"),
        ctx.get("htf_bias"),
        flt.get("htf_bias"),
        ob.get("htf_bias"),
    )

    result.raw_htf_bias = _raw_s(raw_htf_bias) if raw_htf_bias is not None else None
    result.htf_bias = _direction_s(raw_htf_bias, "NEUTRAL") or "NEUTRAL"

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
            ob.get("open_context"),
        )
    ) or None

    result.open_behavior = _open_behavior_value(
        signal_payload.get("open_behavior"),
        record.get("open_behavior"),
        ctx.get("open_behavior"),
        flt.get("open_behavior"),
        ob.get("open_behavior"),
    ) or "UNCONFIRMED"

    result.open_behavior_confidence = _f(
        _first_non_empty(
            signal_payload.get("open_behavior_confidence"),
            record.get("open_behavior_confidence"),
            ctx.get("open_behavior_confidence"),
            flt.get("open_behavior_confidence"),
            ob.get("open_behavior_confidence"),
        )
    )

    # v1.2 auction-state fields. Prefer explicit signal override, then context/filters,
    # then nested open_behavior dict.
    result.open_behavior_version = _raw_s(_first_non_empty(ob.get("version"), ctx.get("open_behavior_version"))) or None
    result.open_location = _s(
        _first_non_empty(
            signal_payload.get("open_location"),
            record.get("open_location"),
            ctx.get("open_location"),
            flt.get("open_location"),
            ob.get("open_location"),
        )
    ) or None
    result.initial_open_behavior = _s(
        _first_non_empty(
            signal_payload.get("initial_open_behavior"),
            record.get("initial_open_behavior"),
            ctx.get("initial_open_behavior"),
            flt.get("initial_open_behavior"),
            ob.get("initial_open_behavior"),
        )
    ) or None
    result.current_open_behavior = _s(
        _first_non_empty(
            signal_payload.get("current_open_behavior"),
            record.get("current_open_behavior"),
            ctx.get("current_open_behavior"),
            flt.get("current_open_behavior"),
            ob.get("current_open_behavior"),
        )
    ) or None
    result.behavior_transition = _s(
        _first_non_empty(
            signal_payload.get("behavior_transition"),
            record.get("behavior_transition"),
            ctx.get("behavior_transition"),
            flt.get("behavior_transition"),
            ob.get("behavior_transition"),
        )
    ) or None
    result.value_acceptance_state = _s(
        _first_non_empty(
            signal_payload.get("value_acceptance_state"),
            record.get("value_acceptance_state"),
            ctx.get("value_acceptance_state"),
            flt.get("value_acceptance_state"),
            ob.get("value_acceptance_state"),
        )
    ) or None
    result.value_test_occurred = _bool(
        _first_non_empty(
            signal_payload.get("value_test_occurred"),
            record.get("value_test_occurred"),
            ctx.get("value_test_occurred"),
            flt.get("value_test_occurred"),
            ob.get("value_test_occurred"),
        ),
        None,
    )
    result.value_test_level = _s(
        _first_non_empty(
            signal_payload.get("value_test_level"),
            record.get("value_test_level"),
            ctx.get("value_test_level"),
            flt.get("value_test_level"),
            ob.get("value_test_level"),
        )
    ) or None
    result.value_rejection_confirmed = _bool(
        _first_non_empty(
            signal_payload.get("value_rejection_confirmed"),
            record.get("value_rejection_confirmed"),
            ctx.get("value_rejection_confirmed"),
            flt.get("value_rejection_confirmed"),
            ob.get("value_rejection_confirmed"),
        ),
        None,
    )
    result.day_type_candidate = _s(
        _first_non_empty(
            signal_payload.get("day_type_candidate"),
            record.get("day_type_candidate"),
            ctx.get("day_type_candidate"),
            flt.get("day_type_candidate"),
            ob.get("day_type_candidate"),
        )
    ) or None
    result.auction_state_confidence = _f(
        _first_non_empty(
            signal_payload.get("auction_state_confidence"),
            record.get("auction_state_confidence"),
            ctx.get("auction_state_confidence"),
            flt.get("auction_state_confidence"),
            ob.get("auction_state_confidence"),
        )
    )
    result.auction_state_reason = _raw_s(
        _first_non_empty(
            signal_payload.get("auction_state_reason"),
            record.get("auction_state_reason"),
            ctx.get("auction_state_reason"),
            flt.get("auction_state_reason"),
            ob.get("auction_state_reason"),
        )
    ) or None

    result.session_normalization_version = _raw_s(_first_non_empty(
        signal_payload.get("session_normalization_version"),
        record.get("session_normalization_version"),
        ctx.get("session_normalization_version"),
        flt.get("session_normalization_version"),
        ob.get("session_normalization_version"),
    )) or None
    result.session_scope = _s(_first_non_empty(
        signal_payload.get("session_scope"), record.get("session_scope"), ctx.get("session_scope"), flt.get("session_scope"), ob.get("session_scope")
    )) or None
    result.primary_session = _s(_first_non_empty(
        signal_payload.get("primary_session"), record.get("primary_session"), ctx.get("primary_session"), flt.get("primary_session"), ob.get("primary_session")
    )) or None
    result.prior_value_scope = _s(_first_non_empty(
        signal_payload.get("prior_value_scope"), record.get("prior_value_scope"), ctx.get("prior_value_scope"), flt.get("prior_value_scope"), ob.get("prior_value_scope")
    )) or None
    result.prior_range_scope = _s(_first_non_empty(
        signal_payload.get("prior_range_scope"), record.get("prior_range_scope"), ctx.get("prior_range_scope"), flt.get("prior_range_scope"), ob.get("prior_range_scope")
    )) or None
    result.open_event = _s(_first_non_empty(
        signal_payload.get("open_event"), record.get("open_event"), ctx.get("open_event"), flt.get("open_event"), ob.get("open_event")
    )) or None
    result.open_event_type = _s(_first_non_empty(
        signal_payload.get("open_event_type"), record.get("open_event_type"), ctx.get("open_event_type"), flt.get("open_event_type"), ob.get("open_event_type")
    )) or None
    result.reference_profile_id = _raw_s(_first_non_empty(
        signal_payload.get("reference_profile_id"), record.get("reference_profile_id"), ctx.get("reference_profile_id"), flt.get("reference_profile_id"), ob.get("reference_profile_id")
    )) or None
    result.active_participation_center = _s(_first_non_empty(
        signal_payload.get("active_participation_center"), record.get("active_participation_center"), ctx.get("active_participation_center"), flt.get("active_participation_center"), ob.get("active_participation_center")
    )) or None
    result.profile_reliability_score = _f(_first_non_empty(
        signal_payload.get("profile_reliability_score"), record.get("profile_reliability_score"), ctx.get("profile_reliability_score"), flt.get("profile_reliability_score"), ob.get("profile_reliability_score")
    ))
    result.profile_reliability_state = _s(_first_non_empty(
        signal_payload.get("profile_reliability_state"), record.get("profile_reliability_state"), ctx.get("profile_reliability_state"), flt.get("profile_reliability_state"), ob.get("profile_reliability_state")
    )) or None
    result.session_status = _s(_first_non_empty(
        signal_payload.get("session_status"), record.get("session_status"), ctx.get("session_status"), flt.get("session_status"), ob.get("session_status")
    )) or None
    result.holiday_mode = _s(_first_non_empty(
        signal_payload.get("holiday_mode"), record.get("holiday_mode"), ctx.get("holiday_mode"), flt.get("holiday_mode"), ob.get("holiday_mode")
    )) or None
    result.weekend_flag = _bool(_first_non_empty(
        signal_payload.get("weekend_flag"), record.get("weekend_flag"), ctx.get("weekend_flag"), flt.get("weekend_flag"), ob.get("weekend_flag")
    ), None)
    result.synthetic_open = _bool(_first_non_empty(
        signal_payload.get("synthetic_open"), record.get("synthetic_open"), ctx.get("synthetic_open"), flt.get("synthetic_open"), ob.get("synthetic_open")
    ), None)
    result.synthetic_open_confirmed = _bool(_first_non_empty(
        signal_payload.get("synthetic_open_confirmed"), record.get("synthetic_open_confirmed"), ctx.get("synthetic_open_confirmed"), flt.get("synthetic_open_confirmed"), ob.get("synthetic_open_confirmed")
    ), None)
    result.true_otd_allowed = _bool(_first_non_empty(
        signal_payload.get("true_otd_allowed"), record.get("true_otd_allowed"), ctx.get("true_otd_allowed"), flt.get("true_otd_allowed"), ob.get("true_otd_allowed")
    ), None)

    result.entry_model_hint = _s(
        _first_non_empty(
            signal_payload.get("entry_model_hint"),
            record.get("entry_model_hint"),
            ctx.get("entry_model_hint"),
            flt.get("entry_model_hint"),
            ob.get("entry_model_hint"),
        )
    ) or None

    result.stop_model_hint = _s(
        _first_non_empty(
            signal_payload.get("stop_model_hint"),
            record.get("stop_model_hint"),
            ctx.get("stop_model_hint"),
            flt.get("stop_model_hint"),
            ob.get("stop_model_hint"),
        )
    ) or None

    result.battle_bias_hint = _s(
        _first_non_empty(
            signal_payload.get("battle_bias_hint"),
            record.get("battle_bias_hint"),
            ctx.get("battle_bias_hint"),
            flt.get("battle_bias_hint"),
            ob.get("battle_bias_hint"),
        )
    ) or None

    zone = _extract_zone(signal_payload, record, ctx, flt, ob)
    result.primary_interest_zone = zone
    result.interest_zone_type = _zone_type(zone)
    result.interest_zone_price = _zone_price(zone)
    result.interest_zone_role = _zone_role(zone)

    result.tpo_watch_setup = _base_watch_setup(result)

    if result.open_behavior_version:
        result.reasons.append(f"Auction-state source: {result.open_behavior_version}.")

    if result.market_status in BLOCK_MARKET_STATUSES:
        return _set_blocked(
            result,
            state="BLOCKED",
            reason=f"Market/TPO data status blocks watch: {result.market_status}.",
            blocker=f"market_status_{result.market_status.lower()}",
        )

    if result.tpo_signal_permission in BLOCK_PERMISSIONS:
        return _set_blocked(
            result,
            state="BLOCKED",
            reason=f"TPO permission blocks watch: {result.tpo_signal_permission}.",
            blocker=f"tpo_permission_{result.tpo_signal_permission.lower()}",
        )

    if result.tpo_telegram_modifier in BLOCK_TELEGRAM_MODIFIERS:
        return _set_blocked(
            result,
            state="OBSERVE_ONLY",
            reason=f"TPO modifier is {result.tpo_telegram_modifier}; no battle watch.",
            blocker=f"tpo_modifier_{result.tpo_telegram_modifier.lower()}",
        )

    if result.tpo_telegram_modifier in DOWNGRADE_TELEGRAM_MODIFIERS:
        result.warnings.append("tpo_modifier_downgrade_watch_allowed_for_ltf_validation")

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
        result.warnings.append("direction_or_htf_bias_unknown_ltf_detector_must_confirm")

    current_behavior = result.current_open_behavior or ""
    initial_behavior = result.initial_open_behavior or ""
    value_state = result.value_acceptance_state or "UNKNOWN"
    value_rejected = bool(
        result.value_rejection_confirmed is True
        or value_state in VALUE_REJECTION_STATES
    )
    value_accepted_back = value_state in VALUE_ACCEPTED_BACK_STATES

    # v1.4 session-normalization reliability gate.
    reliability_state = _s(result.profile_reliability_state)
    if reliability_state in {"MARKET_CLOSED", "SUPPRESS", "PROFILE_UNRELIABLE"}:
        return _set_blocked(
            result,
            state="BLOCKED",
            reason=(
                f"Session normalization blocks auction watch: profile_reliability_state={reliability_state}; "
                "do not activate LTF/Battle from unreliable session scope."
            ),
            blocker=f"session_reliability_{reliability_state.lower()}",
        )

    if reliability_state == "RESEARCH_ONLY":
        return _set_blocked(
            result,
            state="RESEARCH_ONLY",
            reason=(
                "Session normalization marks this profile as RESEARCH_ONLY; "
                "levels may be useful for journal/statistics, but not for actionable watch."
            ),
            blocker="session_reliability_research_only",
        )

    if reliability_state == "CAUTION":
        result.warnings.append("session_reliability_caution")

    # v1.2 detailed auction-state gates first.
    if current_behavior in OPEN_AUCTION_DETAILED_STATES or result.open_behavior == "OPEN_AUCTION":
        # Dalton-style open auction is not a signal by itself. It becomes a
        # directional watch only after the auction chooses a branch:
        # accepted breakout/IB extension, or failed acceptance back into value/range.
        result.tpo_watch_setup = current_behavior or "OPEN_AUCTION"

        accepted_outside = bool(
            value_state in VALUE_ACCEPTED_OUTSIDE_STATES
            or value_state.startswith("ACCEPTED_OUTSIDE")
            or result.behavior_transition in DIRECTIONAL_BREAK_TRANSITIONS
        )
        rejected_back = bool(
            value_state in VALUE_REJECTED_BACK_STATES
            or value_state.startswith("REJECTED_BACK")
            or result.behavior_transition in FAILED_ACCEPTANCE_TRANSITIONS
        )

        if current_behavior == "OPEN_AUCTION_IN_RANGE":
            if accepted_outside:
                result.tpo_watch_setup = "OPEN_AUCTION_ACCEPTED_BREAKOUT"
                result.entry_model_hint = result.entry_model_hint or "ACCEPTED_BREAKOUT_RETEST"
                result.stop_model_hint = result.stop_model_hint or "BEHIND_RETEST_OR_BACK_INSIDE_VALUE"
                result.battle_bias_hint = result.battle_bias_hint or "DIRECTIONAL_AFTER_ACCEPTANCE"
                result.warnings.append("open_auction_in_range_requires_accepted_breakout_retest")
                return _activate_ltf_pending(
                    result,
                    reason=(
                        "OPEN_AUCTION_IN_RANGE has shifted into accepted breakout/extension; "
                        "activate LTF watch only for retest/hold, not for the first poke."
                    ),
                )

            return _set_blocked(
                result,
                state="OBSERVE_ROTATION",
                reason=(
                    "OPEN_AUCTION_IN_RANGE is rotational until value/range break is accepted; "
                    "POC/VAH/VAL are interest zones, not entry triggers."
                ),
            )

        if current_behavior == "OPEN_AUCTION_OUT_OF_RANGE":
            if accepted_outside:
                result.tpo_watch_setup = "OPEN_AUCTION_OUT_OF_RANGE_ACCEPTED_BREAKOUT"
                result.entry_model_hint = result.entry_model_hint or "ACCEPTED_BREAKOUT_RETEST"
                result.stop_model_hint = result.stop_model_hint or "BEHIND_RETEST_OR_BACK_INSIDE_OLD_RANGE"
                result.battle_bias_hint = result.battle_bias_hint or "DIRECTIONAL_OUT_OF_BALANCE"
                result.warnings.append("oaor_breakout_branch_requires_retest_hold")
                return _activate_ltf_pending(
                    result,
                    reason=(
                        "OPEN_AUCTION_OUT_OF_RANGE chose accepted-outside branch; "
                        "wait for LTF breakout/retest/hold before Battle."
                    ),
                )

            if rejected_back:
                result.tpo_watch_setup = "OPEN_AUCTION_OUT_OF_RANGE_FAILED_ACCEPTANCE"
                result.entry_model_hint = result.entry_model_hint or "FAILED_ACCEPTANCE_BACK_TO_VALUE"
                result.stop_model_hint = result.stop_model_hint or "BEHIND_FAILED_PROBE_EXTREME"
                result.battle_bias_hint = result.battle_bias_hint or "BACK_TO_VALUE_AFTER_FAILED_ACCEPTANCE"
                result.warnings.append("oaor_failed_acceptance_branch_requires_inside_acceptance")
                return _activate_ltf_pending(
                    result,
                    reason=(
                        "OPEN_AUCTION_OUT_OF_RANGE rejected outside range/value and is back inside accepted area; "
                        "watch for failed-acceptance/back-to-value LTF model."
                    ),
                )

            return _set_blocked(
                result,
                state="OBSERVE_ONLY",
                reason=(
                    "OPEN_AUCTION_OUT_OF_RANGE has not selected acceptance-outside or rejection-back branch yet; "
                    "observe until the auction proves a branch."
                ),
            )

        return _set_blocked(
            result,
            state="OBSERVE_ONLY",
            reason="Broad OPEN_AUCTION is observe-only until detailed acceptance/rejection branch is known.",
        )

    if current_behavior == "OPEN_REJECTION_REVERSE" or result.open_behavior == "OPEN_REJECTION_REVERSE":
        result.tpo_watch_setup = "OPEN_REJECTION_REVERSE"
        result.warnings.append("orr_requires_caution")

        if result.htf_alignment_state == "HTF_CONFLICT":
            return _set_blocked(
                result,
                state="RESEARCH_ONLY",
                reason=(
                    "OPEN_REJECTION_REVERSE is active, but normalized direction conflicts with HTF bias; "
                    "keep research-only unless later structure confirms reversal quality."
                ),
                blocker="orr_htf_conflict",
            )

        return _activate_ltf_pending(
            result,
            reason=(
                "OPEN_REJECTION_REVERSE context is active; cautious watch only, "
                "requires very clean reclaim/BOS/retest."
            ),
        )

    if current_behavior in OPEN_DRIVE_DETAILED_STATES or result.open_behavior == "OPEN_DRIVE":
        result.tpo_watch_setup = current_behavior or "OPEN_DRIVE"

        if current_behavior == "OPEN_DRIVE_CANDIDATE":
            return _set_blocked(
                result,
                state="OBSERVE_ONLY",
                reason="OPEN_DRIVE_CANDIDATE is not confirmed yet; wait for continuation/pullback structure.",
                blocker="open_drive_candidate_not_confirmed",
            )

        if result.htf_alignment_state != "HTF_ALIGNED":
            return _set_blocked(
                result,
                state="RESEARCH_ONLY",
                reason="OPEN_DRIVE requires HTF alignment for battle watch.",
                blocker="open_drive_without_htf_alignment",
            )

        return _activate_ltf_pending(
            result,
            reason="OPEN_DRIVE context is active; wait for pullback/continuation LTF model.",
        )

    if current_behavior in OTD_DETAILED_STATES or result.open_behavior == "OPEN_TEST_DRIVE":
        result.tpo_watch_setup = current_behavior or "OPEN_TEST_DRIVE"

        if result.true_otd_allowed is False:
            return _set_blocked(
                result,
                state="RESEARCH_ONLY",
                reason=(
                    "Session normalization does not allow TRUE_OPEN_TEST_DRIVE for this context; "
                    "likely in-range rejection/continuation-through-session-change, not a full OTD license."
                ),
                blocker="true_otd_not_allowed_by_session_scope",
            )

        if result.synthetic_open is True and result.synthetic_open_confirmed is False:
            return _set_blocked(
                result,
                state="OBSERVE_ONLY",
                reason=(
                    "Synthetic open is required/active but not confirmed; "
                    "do not promote continuation-through-session-change into OTD watch."
                ),
                blocker="synthetic_open_not_confirmed",
            )

        if value_accepted_back:
            return _set_blocked(
                result,
                state="RESEARCH_ONLY",
                reason=(
                    "Legacy OPEN_TEST_DRIVE is contradicted by value acceptance back inside/failed outside; "
                    "do not activate OTD watch."
                ),
                blocker="otd_contradicted_by_value_acceptance",
            )

        if current_behavior == "OPEN_TEST_DRIVE_CANDIDATE" and result.value_test_occurred is not True:
            return _set_blocked(
                result,
                state="OBSERVE_ONLY",
                reason="OPEN_TEST_DRIVE_CANDIDATE needs a confirmed test of value/reference before LTF watch can activate.",
                blocker="otd_candidate_reference_test_pending",
            )

        if current_behavior == "OPEN_TEST_DRIVE_CANDIDATE" and not value_rejected:
            return _set_blocked(
                result,
                state="OBSERVE_ONLY",
                reason="OPEN_TEST_DRIVE_CANDIDATE needs value/reference rejection before LTF watch can activate.",
                blocker="otd_candidate_value_rejection_pending",
            )

        if (not current_behavior or current_behavior == "UNCONFIRMED") and result.open_behavior == "OPEN_TEST_DRIVE" and result.value_test_occurred is False:
            return _set_blocked(
                result,
                state="OBSERVE_ONLY",
                reason="Legacy OPEN_TEST_DRIVE lacks confirmed reference/value test; keep observe-only.",
                blocker="legacy_otd_missing_reference_test",
            )

        if initial_behavior == "OPEN_TEST_DRIVE_CANDIDATE" and current_behavior in {"", "UNCONFIRMED"}:
            return _set_blocked(
                result,
                state="OBSERVE_ONLY",
                reason="Initial OTD candidate is not current/confirmed; keep observe-only.",
                blocker="initial_otd_not_current",
            )

        if result.htf_alignment_state == "HTF_CONFLICT":
            return _set_blocked(
                result,
                state="RESEARCH_ONLY",
                reason=(
                    "OPEN_TEST_DRIVE detected, but normalized direction conflicts with HTF bias; "
                    "keep research-only unless later policy explicitly allows counter-HTF OTD."
                ),
                blocker="htf_conflict",
            )

        if result.htf_alignment_state == "NEUTRAL_TRANSITION_CANDIDATE":
            result.allowed_htf_neutral_transition = True
            result.warnings.append("htf_neutral_transition_candidate")

        return _activate_ltf_pending(
            result,
            reason=(
                "OPEN_TEST_DRIVE auction-state is active; wait for 5m-15m LTF model "
                "before any ENTRY_READY signal."
            ),
        )

    if initial_behavior == "OPEN_TEST_DRIVE_CANDIDATE" and current_behavior in {"", "UNCONFIRMED"}:
        result.tpo_watch_setup = initial_behavior
        return _set_blocked(
            result,
            state="OBSERVE_ONLY",
            reason="Initial OTD candidate exists, but current auction behavior is unconfirmed.",
            blocker="initial_otd_current_unconfirmed",
        )

    result.tpo_watch_setup = _base_watch_setup(result)
    return _set_blocked(
        result,
        state="NO_WATCH",
        reason=f"Open behavior is not actionable: {result.tpo_watch_setup or result.open_behavior}.",
    )


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
