from __future__ import annotations

"""
TPO open behavior classifier for AI Market Analyst.

v1.4 purpose:
- Add an auction-state layer without breaking existing downstream keys.
- Keep broad legacy fields open_context/open_behavior stable.
- Add detailed fields for:
    open_location,
    initial_open_behavior,
    current_open_behavior,
    behavior_transition,
    value_acceptance_state,
    value_test_occurred,
    value_rejection_confirmed,
    day_type_candidate.

Core rule fixed in v1.4:
Open Test Drive is NOT a generic "test level + extension" pattern.
For this system, OTD requires:
    outside/edge-of-value open
    + value edge test
    + failure to accept back into value
    + rejection away from value
    + directional continuation context.

This module is intentionally pure and dependency-light.
It does not fetch market data, does not write files, and does not send Telegram.
It only converts already-computed TPO/session context into a normalized decision layer:

Open Location
→ Initial Activity
→ Value Interaction
→ Open Behavior / Behavior Transition
→ Day Type Candidate
→ Interest Zone
→ Entry/Stop readiness hints

Primary public function:
    classify_tpo_open_behavior(context, filters=None, htf_bias=None)

Expected input sources:
- item["context"] from runtime/tpo/tpo_latest.json
- item["filters"] from runtime/tpo/tpo_latest.json
- optional htf_bias from scenario/context layer

The classifier is deliberately conservative:
- TPO does not create entries.
- POC/nPOC are interest zones, not entry triggers.
- If behavior is unclear, return UNCONFIRMED instead of pretending precision.
"""

from dataclasses import asdict, dataclass, field
import math
from typing import Any, Literal

try:
    from app.services.session_normalization import (
        SESSION_NORMALIZATION_VERSION,
        resolve_session_context,
    )
except Exception:  # pragma: no cover
    SESSION_NORMALIZATION_VERSION = "session-normalization-unavailable"
    resolve_session_context = None  # type: ignore[assignment]


TPO_OPEN_BEHAVIOR_CLASSIFIER_VERSION = (
    "tpo-open-behavior-classifier-v1.5-session-normalization-brain"
)


# Legacy broad context. Keep stable for downstream consumers.
OpenContext = Literal[
    "OPEN_INSIDE_VA",
    "OPEN_IN_RANGE",
    "OPEN_OUT_OF_RANGE",
    "UNKNOWN",
]

# Detailed location. Additive field; safe for telemetry/reports.
OpenLocation = Literal[
    "OPEN_INSIDE_VALUE",
    "OPEN_ABOVE_VALUE_INSIDE_RANGE",
    "OPEN_BELOW_VALUE_INSIDE_RANGE",
    "OPEN_ABOVE_RANGE",
    "OPEN_BELOW_RANGE",
    "UNKNOWN",
]

# Legacy broad behavior. Keep stable for downstream consumers.
OpenBehavior = Literal[
    "OPEN_DRIVE",
    "OPEN_TEST_DRIVE",
    "OPEN_REJECTION_REVERSE",
    "OPEN_AUCTION",
    "UNCONFIRMED",
]

# Detailed behavior. Additive field; carries the real auction-state nuance.
DetailedOpenBehavior = Literal[
    "OPEN_AUCTION_IN_RANGE",
    "OPEN_AUCTION_OUT_OF_RANGE",
    "OPEN_DRIVE_CANDIDATE",
    "OPEN_DRIVE_CONFIRMED",
    "OPEN_TEST_DRIVE_CANDIDATE",
    "OPEN_TEST_DRIVE_CONFIRMED",
    "OPEN_REJECTION_REVERSE",
    "UNCONFIRMED",
]

Direction = Literal["UP", "DOWN", "BALANCED", "NONE", "UNKNOWN"]

ValueAcceptanceState = Literal[
    "VALUE_NOT_TESTED",
    "VALUE_TEST_PENDING",
    "VALUE_REJECTED_UP",
    "VALUE_REJECTED_DOWN",
    "ACCEPTED_INSIDE_VALUE",
    "ACCEPTED_ABOVE_VALUE",
    "ACCEPTED_BELOW_VALUE",
    "ACCEPTED_BACK_INSIDE_VALUE",
    "FAILED_OUTSIDE_VALUE",
    "UNKNOWN",
]

DayTypeCandidate = Literal[
    "NORMAL_DAY_CANDIDATE",
    "NORMAL_VARIATION_DAY_CANDIDATE",
    "NEUTRAL_DAY_CANDIDATE",
    "TREND_DAY_CANDIDATE",
    "DOUBLE_DISTRIBUTION_DAY_CANDIDATE",
    "NON_TREND_DAY_CANDIDATE",
    "UNKNOWN",
]

ZoneRole = Literal[
    "MAGNET",
    "REACTION_ZONE",
    "INVALIDATION_ZONE",
    "TARGET_ZONE",
    "REFERENCE_ZONE",
    "UNKNOWN",
]

ZoneReaction = Literal[
    "REJECTED",
    "ACCEPTED",
    "SWEPT",
    "UNCONFIRMED",
    "NONE",
]


@dataclass(frozen=True)
class InterestZone:
    zone_type: str
    price: float | None = None
    distance: float | None = None
    role: ZoneRole = "REFERENCE_ZONE"
    reaction: ZoneReaction = "UNCONFIRMED"
    reason: str = ""


@dataclass(frozen=True)
class FirstHourActivity:
    ib_high: float | None = None
    ib_low: float | None = None
    ib_range: float | None = None
    ib_extension_up_pct: float | None = None
    ib_extension_down_pct: float | None = None
    ib_direction: Direction = "UNKNOWN"
    ib_extension_direction: Direction = "NONE"
    open_direction: Direction = "UNKNOWN"
    accepted_outside_range: bool = False
    accepted_back_inside_value: bool = False
    accepted_back_inside_range: bool = False
    failed_auction: bool = False
    tested_level: str = "NONE"
    test_result: ZoneReaction = "UNCONFIRMED"

    # v1.4 additive diagnostics.
    open_location: OpenLocation = "UNKNOWN"
    value_test_occurred: bool = False
    value_test_level: str = "NONE"
    value_acceptance_state: ValueAcceptanceState = "UNKNOWN"
    value_rejection_confirmed: bool = False
    returned_to_value: bool = False
    returned_to_range: bool = False
    accepted_outside_value: bool = False
    failed_outside_value: bool = False
    initial_open_behavior: DetailedOpenBehavior = "UNCONFIRMED"
    current_open_behavior: DetailedOpenBehavior = "UNCONFIRMED"
    behavior_transition: str | None = None
    day_type_candidate: DayTypeCandidate = "UNKNOWN"


@dataclass(frozen=True)
class OpenBehaviorResult:
    version: str
    open_context: OpenContext
    open_behavior: OpenBehavior
    open_behavior_confidence: float
    first_hour_activity: FirstHourActivity
    interest_zones: list[InterestZone] = field(default_factory=list)
    primary_interest_zone: InterestZone | None = None
    entry_model_hint: str = "NO_ENTRY_MODEL"
    stop_model_hint: str = "NO_STOP_MODEL"
    battle_bias_hint: str = "RESEARCH_ONLY"
    reason: str = ""
    warnings: list[str] = field(default_factory=list)

    # v1.4 additive top-level fields mirrored for easy downstream use.
    open_location: OpenLocation = "UNKNOWN"
    initial_open_behavior: DetailedOpenBehavior = "UNCONFIRMED"
    current_open_behavior: DetailedOpenBehavior = "UNCONFIRMED"
    behavior_transition: str | None = None
    value_acceptance_state: ValueAcceptanceState = "UNKNOWN"
    value_test_occurred: bool = False
    value_test_level: str = "NONE"
    value_rejection_confirmed: bool = False
    day_type_candidate: DayTypeCandidate = "UNKNOWN"
    auction_state_confidence: float = 0.0
    auction_state_reason: str = ""

    # v1.5 session-normalization fields.
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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _as_upper(value: Any, default: str = "") -> str:
    text = str(value or "").strip().upper()
    return text or default


def _first_non_empty(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return default


def _safe_abs_distance(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return abs(a - b)


def _open_price(ctx: dict[str, Any]) -> float | None:
    return _as_float(
        _first_non_empty(
            ctx.get("current_open"),
            ctx.get("open_price"),
            ctx.get("session_open"),
            ctx.get("open"),
        )
    )


def _current_price(ctx: dict[str, Any]) -> float | None:
    return _as_float(
        _first_non_empty(
            ctx.get("current_price"),
            ctx.get("last_price"),
            ctx.get("close"),
        )
    )


def _observed_session_low(ctx: dict[str, Any]) -> float | None:
    candidates = [
        _as_float(ctx.get("session_low")),
        _as_float(ctx.get("current_session_low")),
        _as_float(ctx.get("day_low")),
        _as_float(ctx.get("low")),
        _as_float(ctx.get("ib_low")),
        _current_price(ctx),
    ]
    values = [v for v in candidates if v is not None]
    return min(values) if values else None


def _observed_session_high(ctx: dict[str, Any]) -> float | None:
    candidates = [
        _as_float(ctx.get("session_high")),
        _as_float(ctx.get("current_session_high")),
        _as_float(ctx.get("day_high")),
        _as_float(ctx.get("high")),
        _as_float(ctx.get("ib_high")),
        _current_price(ctx),
    ]
    values = [v for v in candidates if v is not None]
    return max(values) if values else None


def _previous_vah(ctx: dict[str, Any]) -> float | None:
    return _as_float(_first_non_empty(ctx.get("previous_vah"), ctx.get("prior_vah"), ctx.get("vah")))


def _previous_val(ctx: dict[str, Any]) -> float | None:
    return _as_float(_first_non_empty(ctx.get("previous_val"), ctx.get("prior_val"), ctx.get("val")))


def _previous_poc(ctx: dict[str, Any]) -> float | None:
    return _as_float(_first_non_empty(ctx.get("previous_poc"), ctx.get("prior_poc"), ctx.get("poc")))


def _previous_high(ctx: dict[str, Any]) -> float | None:
    return _as_float(_first_non_empty(ctx.get("previous_high"), ctx.get("prior_high")))


def _previous_low(ctx: dict[str, Any]) -> float | None:
    return _as_float(_first_non_empty(ctx.get("previous_low"), ctx.get("prior_low")))


def _value_bounds(ctx: dict[str, Any]) -> tuple[float | None, float | None]:
    val = _previous_val(ctx)
    vah = _previous_vah(ctx)
    if val is None or vah is None:
        return None, None
    return min(val, vah), max(val, vah)


def _range_bounds(ctx: dict[str, Any]) -> tuple[float | None, float | None]:
    prev_low = _previous_low(ctx)
    prev_high = _previous_high(ctx)
    if prev_low is None or prev_high is None:
        return None, None
    return min(prev_low, prev_high), max(prev_low, prev_high)


def _distance_threshold(ctx: dict[str, Any]) -> float:
    """
    Generic cross-asset threshold for near-zone checks.

    This is not an execution ladder. It only marks an area as worth attention.
    """
    price = abs(_current_price(ctx) or _open_price(ctx) or 1.0)
    tick_size = _as_float(ctx.get("tick_size")) or 0.0

    pct_threshold = price * 0.001  # 0.10%
    tick_threshold = tick_size * 3 if tick_size > 0 else 0.0

    return max(pct_threshold, tick_threshold)


def infer_open_location(ctx: dict[str, Any]) -> OpenLocation:
    """
    Detailed open-location classifier.

    Uses numeric levels when available. Falls back to open_relation strings only when
    numeric context is incomplete.
    """
    open_px = _open_price(ctx)
    value_low, value_high = _value_bounds(ctx)
    range_low, range_high = _range_bounds(ctx)

    if open_px is not None and value_low is not None and value_high is not None:
        if value_low <= open_px <= value_high:
            return "OPEN_INSIDE_VALUE"

        if open_px > value_high:
            if range_high is not None and open_px > range_high:
                return "OPEN_ABOVE_RANGE"
            return "OPEN_ABOVE_VALUE_INSIDE_RANGE"

        if open_px < value_low:
            if range_low is not None and open_px < range_low:
                return "OPEN_BELOW_RANGE"
            return "OPEN_BELOW_VALUE_INSIDE_RANGE"

    relation = _as_upper(ctx.get("open_relation"), "UNKNOWN")
    if relation in {"INSIDE_VA", "OPEN_INSIDE_VA", "INSIDE_VALUE", "INSIDE_VALUE_AREA"}:
        return "OPEN_INSIDE_VALUE"
    if relation in {"ABOVE_RANGE", "OPEN_ABOVE_RANGE"}:
        return "OPEN_ABOVE_RANGE"
    if relation in {"BELOW_RANGE", "OPEN_BELOW_RANGE"}:
        return "OPEN_BELOW_RANGE"
    if relation in {"ABOVE_VA", "OPEN_ABOVE_VALUE", "OPEN_ABOVE_VA"}:
        return "OPEN_ABOVE_VALUE_INSIDE_RANGE"
    if relation in {"BELOW_VA", "OPEN_BELOW_VALUE", "OPEN_BELOW_VA"}:
        return "OPEN_BELOW_VALUE_INSIDE_RANGE"

    return "UNKNOWN"


def normalize_open_context(open_relation: Any, ctx: dict[str, Any] | None = None) -> OpenContext:
    """
    Legacy broad open context.

    Kept for existing downstream consumers, but enriched by numeric open_location
    when ctx is available.
    """
    if isinstance(ctx, dict) and ctx:
        location = infer_open_location(ctx)
        if location == "OPEN_INSIDE_VALUE":
            return "OPEN_INSIDE_VA"
        if location in {"OPEN_ABOVE_RANGE", "OPEN_BELOW_RANGE"}:
            return "OPEN_OUT_OF_RANGE"
        if location in {"OPEN_ABOVE_VALUE_INSIDE_RANGE", "OPEN_BELOW_VALUE_INSIDE_RANGE"}:
            return "OPEN_IN_RANGE"

    relation = _as_upper(open_relation, "UNKNOWN")

    if relation in {"INSIDE_VA", "OPEN_INSIDE_VA", "INSIDE_VALUE", "INSIDE_VALUE_AREA"}:
        return "OPEN_INSIDE_VA"

    if relation in {
        "RANGE",
        "OPEN_IN_RANGE",
        "IN_RANGE",
        "INSIDE_RANGE",
        "ABOVE_VA",
        "BELOW_VA",
        "OPEN_ABOVE_VALUE",
        "OPEN_BELOW_VALUE",
    }:
        return "OPEN_IN_RANGE"

    if relation in {"OUT_OF_RANGE", "OPEN_OUT_OF_RANGE", "ABOVE_RANGE", "BELOW_RANGE"}:
        return "OPEN_OUT_OF_RANGE"

    return "UNKNOWN"


def _direction_from_open_location(location: OpenLocation) -> Direction:
    if location in {"OPEN_ABOVE_VALUE_INSIDE_RANGE", "OPEN_ABOVE_RANGE"}:
        return "UP"
    if location in {"OPEN_BELOW_VALUE_INSIDE_RANGE", "OPEN_BELOW_RANGE"}:
        return "DOWN"
    if location == "OPEN_INSIDE_VALUE":
        return "BALANCED"
    return "UNKNOWN"


def _infer_open_direction(ctx: dict[str, Any]) -> Direction:
    return _direction_from_open_location(infer_open_location(ctx))


def _infer_ib_direction(ctx: dict[str, Any]) -> Direction:
    up_pct = _as_float(ctx.get("ib_extension_up_pct"))
    down_pct = _as_float(ctx.get("ib_extension_down_pct"))

    if up_pct is None and down_pct is None:
        return "UNKNOWN"

    up = up_pct or 0.0
    down = down_pct or 0.0

    if up < 0.5 and down < 0.5:
        return "BALANCED"

    if up >= down * 1.5 and up >= 0.5:
        return "UP"

    if down >= up * 1.5 and down >= 0.5:
        return "DOWN"

    return "BALANCED"


def _infer_ib_extension_direction(ctx: dict[str, Any]) -> Direction:
    up_pct = _as_float(ctx.get("ib_extension_up_pct")) or 0.0
    down_pct = _as_float(ctx.get("ib_extension_down_pct")) or 0.0

    up_active = up_pct >= 0.5
    down_active = down_pct >= 0.5

    if up_active and down_active:
        return "BALANCED"

    if up_active:
        return "UP"

    if down_active:
        return "DOWN"

    return "NONE"


def _price_inside_value(ctx: dict[str, Any], price: float | None = None) -> bool:
    px = price if price is not None else _current_price(ctx)
    value_low, value_high = _value_bounds(ctx)

    if px is None or value_low is None or value_high is None:
        return False

    return value_low <= px <= value_high


def _price_inside_range(ctx: dict[str, Any], price: float | None = None) -> bool:
    px = price if price is not None else _current_price(ctx)
    range_low, range_high = _range_bounds(ctx)

    if px is None or range_low is None or range_high is None:
        return False

    return range_low <= px <= range_high


def _accepted_outside_range(ctx: dict[str, Any], open_direction: Direction) -> bool:
    px = _current_price(ctx)
    range_low, range_high = _range_bounds(ctx)

    if px is None or range_low is None or range_high is None:
        return False

    if open_direction == "UP":
        return px > range_high

    if open_direction == "DOWN":
        return px < range_low

    return False


def _accepted_outside_value(ctx: dict[str, Any], open_direction: Direction) -> bool:
    px = _current_price(ctx)
    value_low, value_high = _value_bounds(ctx)

    if px is None or value_low is None or value_high is None:
        return False

    if open_direction == "UP":
        return px > value_high

    if open_direction == "DOWN":
        return px < value_low

    return False


def _value_test_occurred(ctx: dict[str, Any], location: OpenLocation) -> tuple[bool, str]:
    """
    Detect whether price tested the prior value edge.

    For bullish OTD: open above value → low tests VAH.
    For bearish OTD: open below value → high tests VAL.
    """
    threshold = _distance_threshold(ctx)
    value_low, value_high = _value_bounds(ctx)
    if value_low is None or value_high is None:
        return False, "NONE"

    observed_low = _observed_session_low(ctx)
    observed_high = _observed_session_high(ctx)
    current_px = _current_price(ctx)

    if location in {"OPEN_ABOVE_VALUE_INSIDE_RANGE", "OPEN_ABOVE_RANGE"}:
        tested = bool(
            (observed_low is not None and observed_low <= value_high + threshold)
            or (current_px is not None and current_px <= value_high + threshold)
        )
        return tested, "VAH" if tested else "NONE"

    if location in {"OPEN_BELOW_VALUE_INSIDE_RANGE", "OPEN_BELOW_RANGE"}:
        tested = bool(
            (observed_high is not None and observed_high >= value_low - threshold)
            or (current_px is not None and current_px >= value_low - threshold)
        )
        return tested, "VAL" if tested else "NONE"

    # Inside value: touches are rotations, not OTD value tests.
    return False, "NONE"


def _value_acceptance_state(
    ctx: dict[str, Any],
    *,
    location: OpenLocation,
    open_direction: Direction,
    value_test_occurred: bool,
) -> ValueAcceptanceState:
    px = _current_price(ctx)
    value_low, value_high = _value_bounds(ctx)

    if px is None or value_low is None or value_high is None:
        return "UNKNOWN"

    inside_value = value_low <= px <= value_high

    if location == "OPEN_INSIDE_VALUE":
        return "ACCEPTED_INSIDE_VALUE" if inside_value else "VALUE_TEST_PENDING"

    if inside_value:
        return "ACCEPTED_BACK_INSIDE_VALUE"

    if open_direction == "UP":
        if px > value_high:
            return "VALUE_REJECTED_UP" if value_test_occurred else "ACCEPTED_ABOVE_VALUE"
        return "FAILED_OUTSIDE_VALUE"

    if open_direction == "DOWN":
        if px < value_low:
            return "VALUE_REJECTED_DOWN" if value_test_occurred else "ACCEPTED_BELOW_VALUE"
        return "FAILED_OUTSIDE_VALUE"

    return "UNKNOWN"


def _value_rejection_confirmed(state: ValueAcceptanceState) -> bool:
    return state in {"VALUE_REJECTED_UP", "VALUE_REJECTED_DOWN"}


def _failed_outside_value(state: ValueAcceptanceState) -> bool:
    return state in {"ACCEPTED_BACK_INSIDE_VALUE", "FAILED_OUTSIDE_VALUE"}


def _build_interest_zones(ctx: dict[str, Any]) -> list[InterestZone]:
    current_price = _current_price(ctx)
    threshold = _distance_threshold(ctx)

    candidates: list[InterestZone] = []

    raw_levels = [
        ("POC", _previous_poc(ctx), "REFERENCE_ZONE", "previous POC"),
        ("VAH", _previous_vah(ctx), "REACTION_ZONE", "previous value area high"),
        ("VAL", _previous_val(ctx), "REACTION_ZONE", "previous value area low"),
        ("PREVIOUS_HIGH", _previous_high(ctx), "REFERENCE_ZONE", "previous session high"),
        ("PREVIOUS_LOW", _previous_low(ctx), "REFERENCE_ZONE", "previous session low"),
    ]

    for zone_type, price, role, reason in raw_levels:
        distance = _safe_abs_distance(current_price, price)
        if price is None:
            continue
        candidates.append(
            InterestZone(
                zone_type=zone_type,
                price=price,
                distance=distance,
                role=role,  # type: ignore[arg-type]
                reaction="UNCONFIRMED",
                reason=reason,
            )
        )

    nearest_npoc = _as_float(ctx.get("nearest_npoc"))
    nearest_npoc_distance = _as_float(ctx.get("nearest_npoc_distance"))
    if nearest_npoc is not None:
        candidates.append(
            InterestZone(
                zone_type="NPOC",
                price=nearest_npoc,
                distance=nearest_npoc_distance
                if nearest_npoc_distance is not None
                else _safe_abs_distance(current_price, nearest_npoc),
                role="MAGNET",
                reaction="UNCONFIRMED",
                reason="nearest naked POC; interest zone only, not entry trigger",
            )
        )

    candidates.sort(
        key=lambda z: (
            float("inf") if z.distance is None else z.distance,
            z.zone_type,
        )
    )

    result: list[InterestZone] = []
    for z in candidates:
        near = z.distance is not None and z.distance <= threshold
        role: ZoneRole = z.role
        reason = z.reason

        if near and z.zone_type in {"VAH", "VAL", "POC"}:
            role = "REACTION_ZONE"
            reason += "; price is near this level"

        if near and z.zone_type == "NPOC":
            role = "MAGNET"
            reason += "; price is near nPOC magnet"

        result.append(
            InterestZone(
                zone_type=z.zone_type,
                price=z.price,
                distance=z.distance,
                role=role,
                reaction=z.reaction,
                reason=reason,
            )
        )

    return result[:8]


def _infer_tested_level(interest_zones: list[InterestZone], ctx: dict[str, Any]) -> str:
    threshold = _distance_threshold(ctx)
    for zone in interest_zones:
        if zone.distance is not None and zone.distance <= threshold:
            return zone.zone_type
    return "NONE"


def _infer_test_result(
    *,
    tested_level: str,
    value_acceptance_state: ValueAcceptanceState,
) -> ZoneReaction:
    if tested_level == "NONE":
        return "NONE"

    if value_acceptance_state in {"VALUE_REJECTED_UP", "VALUE_REJECTED_DOWN"}:
        return "REJECTED"

    if value_acceptance_state in {"ACCEPTED_BACK_INSIDE_VALUE", "ACCEPTED_INSIDE_VALUE"}:
        return "ACCEPTED"

    return "UNCONFIRMED"


def _infer_day_type_candidate(
    ctx: dict[str, Any],
    *,
    auction_bias: str,
    ib_extension_direction: Direction,
    current_behavior: DetailedOpenBehavior,
) -> DayTypeCandidate:
    up_pct = _as_float(ctx.get("ib_extension_up_pct")) or 0.0
    down_pct = _as_float(ctx.get("ib_extension_down_pct")) or 0.0

    # Prefer explicit exporter hints if they exist in the future.
    explicit = _as_upper(
        _first_non_empty(ctx.get("day_type"), ctx.get("day_type_candidate"), ctx.get("profile_day_type")),
        "",
    )
    explicit_map: dict[str, DayTypeCandidate] = {
        "NORMAL_DAY": "NORMAL_DAY_CANDIDATE",
        "NORMAL_VARIATION_DAY": "NORMAL_VARIATION_DAY_CANDIDATE",
        "NEUTRAL_DAY": "NEUTRAL_DAY_CANDIDATE",
        "TREND_DAY": "TREND_DAY_CANDIDATE",
        "DOUBLE_DISTRIBUTION_DAY": "DOUBLE_DISTRIBUTION_DAY_CANDIDATE",
        "NON_TREND_DAY": "NON_TREND_DAY_CANDIDATE",
    }
    if explicit in explicit_map:
        return explicit_map[explicit]

    if bool(ctx.get("double_distribution_detected")) or bool(ctx.get("single_prints_present")):
        return "DOUBLE_DISTRIBUTION_DAY_CANDIDATE"

    if up_pct >= 0.5 and down_pct >= 0.5:
        return "NEUTRAL_DAY_CANDIDATE"

    if current_behavior in {"OPEN_DRIVE_CONFIRMED", "OPEN_TEST_DRIVE_CONFIRMED"}:
        if max(up_pct, down_pct) >= 1.25 or auction_bias == "DIRECTIONAL_IMBALANCE":
            return "TREND_DAY_CANDIDATE"
        return "NORMAL_VARIATION_DAY_CANDIDATE"

    if ib_extension_direction in {"UP", "DOWN"}:
        if max(up_pct, down_pct) >= 1.0:
            return "NORMAL_VARIATION_DAY_CANDIDATE"
        return "NORMAL_DAY_CANDIDATE"

    if auction_bias == "BALANCE":
        return "NON_TREND_DAY_CANDIDATE"

    if ib_extension_direction == "NONE":
        return "NORMAL_DAY_CANDIDATE"

    return "UNKNOWN"


def _initial_open_behavior(
    *,
    location: OpenLocation,
    open_direction: Direction,
    ib_extension_direction: Direction,
    value_test_occurred: bool,
) -> DetailedOpenBehavior:
    if value_test_occurred and open_direction in {"UP", "DOWN"}:
        return "OPEN_TEST_DRIVE_CANDIDATE"

    if location in {"OPEN_ABOVE_RANGE", "OPEN_BELOW_RANGE"}:
        if ib_extension_direction == open_direction and open_direction in {"UP", "DOWN"}:
            return "OPEN_DRIVE_CANDIDATE"
        return "OPEN_AUCTION_OUT_OF_RANGE"

    if location in {"OPEN_ABOVE_VALUE_INSIDE_RANGE", "OPEN_BELOW_VALUE_INSIDE_RANGE"}:
        if ib_extension_direction == open_direction and open_direction in {"UP", "DOWN"}:
            return "OPEN_DRIVE_CANDIDATE"
        return "OPEN_AUCTION_IN_RANGE"

    if location == "OPEN_INSIDE_VALUE":
        return "OPEN_AUCTION_IN_RANGE"

    return "UNCONFIRMED"


def _behavior_transition(initial: DetailedOpenBehavior, current: DetailedOpenBehavior) -> str | None:
    if not initial or not current or initial == current:
        return None
    if initial == "UNCONFIRMED" or current == "UNCONFIRMED":
        return None
    return f"{initial}_TO_{current}"


def _classify_current_behavior(
    *,
    location: OpenLocation,
    open_context: OpenContext,
    open_direction: Direction,
    ib_extension_direction: Direction,
    auction_bias: str,
    value_test_occurred: bool,
    value_acceptance_state: ValueAcceptanceState,
    accepted_outside_range: bool,
    accepted_outside_value: bool,
    true_otd_allowed: bool = True,
) -> tuple[OpenBehavior, DetailedOpenBehavior, float, str, str, str, str]:
    """
    Return broad behavior, detailed behavior, confidence, entry_hint,
    stop_hint, battle_hint, reason.
    """
    if value_acceptance_state == "ACCEPTED_BACK_INSIDE_VALUE":
        return (
            "OPEN_REJECTION_REVERSE",
            "OPEN_REJECTION_REVERSE",
            0.78,
            "SWEEP_RECLAIM_BOS_RETEST",
            "BEHIND_SWEEP_EXTREME",
            "RESEARCH_COUNTERTREND_UNLESS_LTF_CONFIRMED",
            "Outside-value attempt accepted back inside prior value; rejection-reverse conditions detected.",
        )

    if value_acceptance_state == "FAILED_OUTSIDE_VALUE":
        return (
            "OPEN_REJECTION_REVERSE",
            "OPEN_REJECTION_REVERSE",
            0.64,
            "WAIT_FOR_ACCEPTANCE_BACK_INSIDE_VALUE",
            "BEHIND_FAILED_OUTSIDE_EXTREME",
            "RESEARCH_ONLY",
            "Outside-value attempt is failing, but value acceptance needs confirmation.",
        )

    if open_direction in {"UP", "DOWN"} and value_test_occurred and not true_otd_allowed:
        detailed = "OPEN_AUCTION_IN_RANGE" if open_context in {"OPEN_IN_RANGE", "OPEN_INSIDE_VA"} else "OPEN_AUCTION_OUT_OF_RANGE"
        return (
            "OPEN_AUCTION",
            detailed,
            0.52,
            "WAIT_FOR_TRUE_SESSION_ACCEPTANCE",
            "NO_STOP_MODEL",
            "RESEARCH_ONLY",
            "Value/reference test exists, but session normalization does not allow TRUE_OPEN_TEST_DRIVE; keep as observe/research until correct session acceptance is proven.",
        )

    if open_direction in {"UP", "DOWN"} and value_test_occurred:
        if _value_rejection_confirmed(value_acceptance_state) and ib_extension_direction == open_direction:
            return (
                "OPEN_TEST_DRIVE",
                "OPEN_TEST_DRIVE_CONFIRMED",
                0.76,
                "FAILED_ACCEPTANCE_RETEST",
                "BEYOND_FAILED_ACCEPTANCE_ZONE",
                "ALLOW_IF_HTF_ALIGNED_AND_LTF_CONFIRMED",
                "Outside/edge-of-value open tested prior value edge, failed to accept back into value, and continued away from value.",
            )
        return (
            "OPEN_TEST_DRIVE",
            "OPEN_TEST_DRIVE_CANDIDATE",
            0.58,
            "WAIT_FOR_VALUE_REJECTION_AND_LTF_CONFIRMATION",
            "BEYOND_VALUE_EDGE_OR_TEST_EXTREME",
            "RESEARCH_UNTIL_ACCEPTANCE_CONFIRMED",
            "Value edge test occurred, but rejection/continuation is not fully confirmed.",
        )

    if (
        open_direction in {"UP", "DOWN"}
        and ib_extension_direction == open_direction
        and (accepted_outside_range or accepted_outside_value)
        and auction_bias in {"DIRECTIONAL_IMBALANCE", "RANGE_EXTENSION", "UNKNOWN"}
    ):
        return (
            "OPEN_DRIVE",
            "OPEN_DRIVE_CONFIRMED",
            0.80,
            "PULLBACK_CONTINUATION",
            "BEHIND_PULLBACK_STRUCTURE_OR_IB_EDGE",
            "BOOST_IF_HTF_ALIGNED_AND_EXECUTABLE",
            "Open is holding outside value/range with directional IB extension and no meaningful value test first.",
        )

    if open_context == "OPEN_OUT_OF_RANGE":
        return (
            "OPEN_AUCTION",
            "OPEN_AUCTION_OUT_OF_RANGE",
            0.56,
            "WAIT_FOR_ACCEPTANCE_OR_REJECTION",
            "NO_STOP_MODEL",
            "RESEARCH_ONLY",
            "Out-of-range context is building auction/balance instead of clean initiative continuation.",
        )

    if open_context in {"OPEN_IN_RANGE", "OPEN_INSIDE_VA"}:
        return (
            "OPEN_AUCTION",
            "OPEN_AUCTION_IN_RANGE",
            0.62 if open_context == "OPEN_IN_RANGE" else 0.72,
            "NO_DIRECTIONAL_ENTRY_MODEL",
            "NO_STOP_MODEL",
            "RESEARCH_ONLY",
            "Open is inside prior value/range context without strict OTD or clean drive confirmation.",
        )

    return (
        "UNCONFIRMED",
        "UNCONFIRMED",
        0.35,
        "NO_ENTRY_MODEL",
        "NO_STOP_MODEL",
        "RESEARCH_ONLY",
        "Open behavior is not clear enough.",
    )


def _build_first_hour_activity(ctx: dict[str, Any], *, auction_bias: str) -> FirstHourActivity:
    location = infer_open_location(ctx)
    open_direction = _infer_open_direction(ctx)
    ib_direction = _infer_ib_direction(ctx)
    ib_extension_direction = _infer_ib_extension_direction(ctx)

    accepted_back_inside_value = _price_inside_value(ctx)
    accepted_back_inside_range = _price_inside_range(ctx)
    accepted_outside_range = _accepted_outside_range(ctx, open_direction)
    accepted_outside_value = _accepted_outside_value(ctx, open_direction)

    value_test_occurred, value_test_level = _value_test_occurred(ctx, location)
    value_acceptance_state = _value_acceptance_state(
        ctx,
        location=location,
        open_direction=open_direction,
        value_test_occurred=value_test_occurred,
    )
    value_rejection_confirmed = _value_rejection_confirmed(value_acceptance_state)
    failed_outside_value = _failed_outside_value(value_acceptance_state)

    interest_zones = _build_interest_zones(ctx)
    tested_level = value_test_level if value_test_occurred else _infer_tested_level(interest_zones, ctx)
    test_result = _infer_test_result(
        tested_level=tested_level,
        value_acceptance_state=value_acceptance_state,
    )

    failed_auction = bool(open_direction in {"UP", "DOWN"} and failed_outside_value)

    open_context = normalize_open_context(ctx.get("open_relation"), ctx)
    broad_behavior, current_behavior, _confidence, _entry, _stop, _battle, _reason = _classify_current_behavior(
        location=location,
        open_context=open_context,
        open_direction=open_direction,
        ib_extension_direction=ib_extension_direction,
        auction_bias=auction_bias,
        value_test_occurred=value_test_occurred,
        value_acceptance_state=value_acceptance_state,
        accepted_outside_range=accepted_outside_range,
        accepted_outside_value=accepted_outside_value,
        true_otd_allowed=True,
    )
    del broad_behavior

    initial_behavior = _initial_open_behavior(
        location=location,
        open_direction=open_direction,
        ib_extension_direction=ib_extension_direction,
        value_test_occurred=value_test_occurred,
    )
    transition = _behavior_transition(initial_behavior, current_behavior)
    day_type = _infer_day_type_candidate(
        ctx,
        auction_bias=auction_bias,
        ib_extension_direction=ib_extension_direction,
        current_behavior=current_behavior,
    )

    return FirstHourActivity(
        ib_high=_as_float(ctx.get("ib_high")),
        ib_low=_as_float(ctx.get("ib_low")),
        ib_range=_as_float(ctx.get("ib_range")),
        ib_extension_up_pct=_as_float(ctx.get("ib_extension_up_pct")),
        ib_extension_down_pct=_as_float(ctx.get("ib_extension_down_pct")),
        ib_direction=ib_direction,
        ib_extension_direction=ib_extension_direction,
        open_direction=open_direction,
        accepted_outside_range=accepted_outside_range,
        accepted_back_inside_value=accepted_back_inside_value,
        accepted_back_inside_range=accepted_back_inside_range,
        failed_auction=failed_auction,
        tested_level=tested_level,
        test_result=test_result,
        open_location=location,
        value_test_occurred=value_test_occurred,
        value_test_level=value_test_level,
        value_acceptance_state=value_acceptance_state,
        value_rejection_confirmed=value_rejection_confirmed,
        returned_to_value=accepted_back_inside_value,
        returned_to_range=accepted_back_inside_range,
        accepted_outside_value=accepted_outside_value,
        failed_outside_value=failed_outside_value,
        initial_open_behavior=initial_behavior,
        current_open_behavior=current_behavior,
        behavior_transition=transition,
        day_type_candidate=day_type,
    )


def classify_tpo_open_behavior(
    context: dict[str, Any] | None,
    filters: dict[str, Any] | None = None,
    *,
    htf_bias: str | None = None,
) -> dict[str, Any]:
    """
    Classify TPO open behavior from precomputed TPO context.

    Returns a JSON-safe dict suitable for attaching to tpo_latest.json,
    journal payloads, statistics, and Telegram reports.
    """
    ctx = context if isinstance(context, dict) else {}
    flt = filters if isinstance(filters, dict) else {}

    warnings: list[str] = []

    symbol = _first_non_empty(
        ctx.get("symbol"),
        ctx.get("instrument"),
        flt.get("symbol"),
        flt.get("instrument"),
        default=None,
    )

    auction_bias = _as_upper(ctx.get("auction_bias") or flt.get("auction_bias"), "UNKNOWN")
    market_status = _as_upper(ctx.get("market_status") or flt.get("market_status"), "UNKNOWN")
    tpo_permission = _as_upper(flt.get("tpo_signal_permission") or ctx.get("tpo_signal_permission"), "UNKNOWN")
    modifier = _as_upper(
        flt.get("telegram_modifier")
        or flt.get("tpo_telegram_modifier")
        or ctx.get("tpo_telegram_modifier"),
        "NEUTRAL",
    )
    htf = _as_upper(htf_bias, "UNKNOWN")

    open_relation = ctx.get("open_relation") or flt.get("open_relation")
    open_context = normalize_open_context(open_relation, ctx)
    first_hour = _build_first_hour_activity(ctx, auction_bias=auction_bias)

    session_context: dict[str, Any]
    if resolve_session_context is not None:
        ctx_for_session = dict(ctx)
        flt_for_session = dict(flt)
        ctx_for_session.setdefault("open_location", first_hour.open_location)
        ctx_for_session.setdefault("value_test_occurred", first_hour.value_test_occurred)
        session_context = resolve_session_context(symbol=symbol, context=ctx_for_session, filters=flt_for_session)
    else:
        session_context = {
            "version": SESSION_NORMALIZATION_VERSION,
            "profile_reliability_score": 70,
            "profile_reliability_state": "CAUTION",
            "true_otd_allowed": True,
            "warnings": ["session_normalization_unavailable"],
            "blockers": [],
            "reasons": [],
        }

    true_otd_allowed = bool(session_context.get("true_otd_allowed", True))
    warnings.extend(str(w) for w in (session_context.get("warnings") or []) if w)

    interest_zones = _build_interest_zones(ctx)
    primary_zone = interest_zones[0] if interest_zones else None

    context_available = bool(
        flt.get("auction_context_available")
        or ctx.get("auction_context_available")
        or ctx
    )
    is_stale = bool(flt.get("is_stale") or ctx.get("is_stale") or ctx.get("market_data_is_stale"))

    def _blocked_result(
        *,
        confidence: float,
        reason: str,
        warning: str,
        battle_hint: str = "BLOCK",
    ) -> dict[str, Any]:
        result = OpenBehaviorResult(
            version=TPO_OPEN_BEHAVIOR_CLASSIFIER_VERSION,
            open_context=open_context if context_available else "UNKNOWN",
            open_behavior="UNCONFIRMED",
            open_behavior_confidence=round(confidence, 4),
            first_hour_activity=first_hour,
            interest_zones=interest_zones,
            primary_interest_zone=primary_zone,
            battle_bias_hint=battle_hint,
            reason=reason,
            warnings=[warning],
            open_location=first_hour.open_location,
            initial_open_behavior=first_hour.initial_open_behavior,
            current_open_behavior="UNCONFIRMED",
            behavior_transition=first_hour.behavior_transition,
            value_acceptance_state=first_hour.value_acceptance_state,
            value_test_occurred=first_hour.value_test_occurred,
            value_test_level=first_hour.value_test_level,
            value_rejection_confirmed=first_hour.value_rejection_confirmed,
            day_type_candidate=first_hour.day_type_candidate,
            auction_state_confidence=round(confidence, 4),
            auction_state_reason=reason,
            session_normalization_version=session_context.get("version"),
            session_scope=session_context.get("session_scope"),
            primary_session=session_context.get("primary_session"),
            prior_value_scope=session_context.get("prior_value_scope"),
            prior_range_scope=session_context.get("prior_range_scope"),
            open_event=session_context.get("open_event"),
            open_event_type=session_context.get("open_event_type"),
            reference_profile_id=session_context.get("reference_profile_id"),
            active_participation_center=session_context.get("active_participation_center"),
            profile_reliability_score=session_context.get("profile_reliability_score"),
            profile_reliability_state=session_context.get("profile_reliability_state"),
            session_status=session_context.get("session_status"),
            holiday_mode=session_context.get("holiday_mode"),
            weekend_flag=session_context.get("weekend_flag"),
            synthetic_open=session_context.get("synthetic_open"),
            synthetic_open_confirmed=session_context.get("synthetic_open_confirmed"),
            true_otd_allowed=session_context.get("true_otd_allowed"),
        )
        return result.to_dict()

    if not context_available:
        return _blocked_result(
            confidence=0.0,
            reason="No auction context available.",
            warning="auction_context_missing",
        )

    if market_status not in {"OPEN", "UNKNOWN"}:
        return _blocked_result(
            confidence=0.1,
            reason=f"Market is not open enough for behavior classification: market_status={market_status}.",
            warning=f"market_status_{market_status.lower()}",
        )

    if is_stale or tpo_permission in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"}:
        return _blocked_result(
            confidence=0.15,
            reason="TPO context is stale or degraded.",
            warning="tpo_stale_or_degraded",
        )

    behavior, current_behavior, confidence, entry_hint, stop_hint, battle_hint, reason = _classify_current_behavior(
        location=first_hour.open_location,
        open_context=open_context,
        open_direction=first_hour.open_direction,
        ib_extension_direction=first_hour.ib_extension_direction,
        auction_bias=auction_bias,
        value_test_occurred=first_hour.value_test_occurred,
        value_acceptance_state=first_hour.value_acceptance_state,
        accepted_outside_range=first_hour.accepted_outside_range,
        accepted_outside_value=first_hour.accepted_outside_value,
        true_otd_allowed=true_otd_allowed,
    )

    if htf in {"LONG", "SHORT"} and battle_hint.startswith("BOOST"):
        reason += f" HTF bias is available: {htf}."

    if primary_zone and primary_zone.zone_type == "NPOC":
        warnings.append("npoc_is_interest_zone_not_entry_trigger")

    if modifier == "DOWNGRADE":
        warnings.append("telegram_modifier_downgrade")

    if first_hour.current_open_behavior == "OPEN_AUCTION_OUT_OF_RANGE":
        warnings.append("outside_range_auction_requires_later_acceptance_or_rejection")

    if first_hour.current_open_behavior == "OPEN_TEST_DRIVE_CANDIDATE":
        warnings.append("otd_candidate_requires_value_rejection_and_ltf_confirmation")

    if behavior == "OPEN_TEST_DRIVE" and first_hour.current_open_behavior != "OPEN_TEST_DRIVE_CONFIRMED":
        warnings.append("open_test_drive_not_actionable_without_ltf_confirmation")

    result = OpenBehaviorResult(
        version=TPO_OPEN_BEHAVIOR_CLASSIFIER_VERSION,
        open_context=open_context,
        open_behavior=behavior,
        open_behavior_confidence=round(confidence, 4),
        first_hour_activity=first_hour,
        interest_zones=interest_zones,
        primary_interest_zone=primary_zone,
        entry_model_hint=entry_hint,
        stop_model_hint=stop_hint,
        battle_bias_hint=battle_hint,
        reason=reason,
        warnings=warnings,
        open_location=first_hour.open_location,
        initial_open_behavior=first_hour.initial_open_behavior,
        current_open_behavior=current_behavior,
        behavior_transition=first_hour.behavior_transition,
        value_acceptance_state=first_hour.value_acceptance_state,
        value_test_occurred=first_hour.value_test_occurred,
        value_test_level=first_hour.value_test_level,
        value_rejection_confirmed=first_hour.value_rejection_confirmed,
        day_type_candidate=first_hour.day_type_candidate,
        auction_state_confidence=round(confidence, 4),
        auction_state_reason=reason,
        session_normalization_version=session_context.get("version"),
        session_scope=session_context.get("session_scope"),
        primary_session=session_context.get("primary_session"),
        prior_value_scope=session_context.get("prior_value_scope"),
        prior_range_scope=session_context.get("prior_range_scope"),
        open_event=session_context.get("open_event"),
        open_event_type=session_context.get("open_event_type"),
        reference_profile_id=session_context.get("reference_profile_id"),
        active_participation_center=session_context.get("active_participation_center"),
        profile_reliability_score=session_context.get("profile_reliability_score"),
        profile_reliability_state=session_context.get("profile_reliability_state"),
        session_status=session_context.get("session_status"),
        holiday_mode=session_context.get("holiday_mode"),
        weekend_flag=session_context.get("weekend_flag"),
        synthetic_open=session_context.get("synthetic_open"),
        synthetic_open_confirmed=session_context.get("synthetic_open_confirmed"),
        true_otd_allowed=session_context.get("true_otd_allowed"),
    )
    return result.to_dict()


def attach_open_behavior_to_tpo_item(item: dict[str, Any], *, htf_bias: str | None = None) -> dict[str, Any]:
    """
    Return a shallow-copied TPO store item with open_behavior fields attached.

    This helper is convenient for tpo_context_exporter integration.
    """
    if not isinstance(item, dict):
        return {}

    output = dict(item)
    ctx = output.get("context") if isinstance(output.get("context"), dict) else {}
    flt = output.get("filters") if isinstance(output.get("filters"), dict) else {}

    # Make session normalization symbol-aware even when symbol lives at item root.
    ctx = dict(ctx)
    flt = dict(flt)

    item_symbol = _first_non_empty(
        output.get("symbol"),
        output.get("instrument"),
        output.get("ticker"),
        ctx.get("symbol"),
        ctx.get("instrument"),
        flt.get("symbol"),
        flt.get("instrument"),
        default=None,
    )
    if item_symbol:
        ctx.setdefault("symbol", item_symbol)
        flt.setdefault("symbol", item_symbol)

    behavior = classify_tpo_open_behavior(ctx, flt, htf_bias=htf_bias)

    output["open_behavior"] = behavior

    # Mirror the most important fields into context/filters for easier downstream use.

    mirror_keys = [
        "version",
        "open_context",
        "open_location",
        "open_behavior",
        "open_behavior_confidence",
        "initial_open_behavior",
        "current_open_behavior",
        "behavior_transition",
        "value_acceptance_state",
        "value_test_occurred",
        "value_test_level",
        "value_rejection_confirmed",
        "day_type_candidate",
        "auction_state_confidence",
        "auction_state_reason",
        "first_hour_activity",
        "primary_interest_zone",
        "interest_zones",
        "entry_model_hint",
        "stop_model_hint",
        "battle_bias_hint",
        "session_normalization_version",
        "session_scope",
        "primary_session",
        "prior_value_scope",
        "prior_range_scope",
        "open_event",
        "open_event_type",
        "reference_profile_id",
        "active_participation_center",
        "profile_reliability_score",
        "profile_reliability_state",
        "session_status",
        "holiday_mode",
        "weekend_flag",
        "synthetic_open",
        "synthetic_open_confirmed",
        "true_otd_allowed",
    ]

    for key in mirror_keys:
        if key in behavior:
            ctx[key] = behavior.get(key)

    ctx["open_behavior_reason"] = behavior.get("reason")
    ctx["open_behavior_warnings"] = behavior.get("warnings")

    # Filters get compact, high-value routing keys only.
    for key in [
        "open_context",
        "open_location",
        "open_behavior",
        "open_behavior_confidence",
        "initial_open_behavior",
        "current_open_behavior",
        "behavior_transition",
        "value_acceptance_state",
        "value_test_occurred",
        "value_test_level",
        "value_rejection_confirmed",
        "day_type_candidate",
        "entry_model_hint",
        "stop_model_hint",
        "battle_bias_hint",
        "session_normalization_version",
        "session_scope",
        "primary_session",
        "prior_value_scope",
        "prior_range_scope",
        "open_event",
        "open_event_type",
        "reference_profile_id",
        "active_participation_center",
        "profile_reliability_score",
        "profile_reliability_state",
        "session_status",
        "holiday_mode",
        "weekend_flag",
        "synthetic_open",
        "synthetic_open_confirmed",
        "true_otd_allowed",
    ]:
        if key in behavior:
            flt[key] = behavior.get(key)

    output["context"] = ctx
    output["filters"] = flt
    return output
