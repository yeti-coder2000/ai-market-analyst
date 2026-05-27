from __future__ import annotations

"""
TPO open behavior classifier for AI Market Analyst.

This module is intentionally pure and dependency-light.
It does not fetch market data, does not write files, and does not send Telegram.
It only converts already-computed TPO/session context into a normalized decision layer:

HTF context
→ open context
→ first-hour activity
→ open behavior
→ interest zone
→ entry/stop readiness hints

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
from typing import Any, Literal


OpenContext = Literal[
    "OPEN_INSIDE_VA",
    "OPEN_IN_RANGE",
    "OPEN_OUT_OF_RANGE",
    "UNKNOWN",
]

OpenBehavior = Literal[
    "OPEN_DRIVE",
    "OPEN_TEST_DRIVE",
    "OPEN_REJECTION_REVERSE",
    "OPEN_AUCTION",
    "UNCONFIRMED",
]

Direction = Literal["UP", "DOWN", "BALANCED", "NONE", "UNKNOWN"]

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


@dataclass(frozen=True)
class OpenBehaviorResult:
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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_upper(value: Any, default: str = "") -> str:
    text = str(value or "").strip().upper()
    return text or default


def _safe_abs_distance(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return abs(a - b)


def normalize_open_context(open_relation: Any) -> OpenContext:
    relation = _as_upper(open_relation, "UNKNOWN")

    if relation in {"INSIDE_VA", "OPEN_INSIDE_VA", "INSIDE_VALUE", "INSIDE_VALUE_AREA"}:
        return "OPEN_INSIDE_VA"

    if relation in {"RANGE", "OPEN_IN_RANGE", "IN_RANGE", "INSIDE_RANGE"}:
        return "OPEN_IN_RANGE"

    if relation in {"OUT_OF_RANGE", "OPEN_OUT_OF_RANGE", "ABOVE_RANGE", "BELOW_RANGE"}:
        return "OPEN_OUT_OF_RANGE"

    return "UNKNOWN"


def _infer_open_direction(ctx: dict[str, Any]) -> Direction:
    current_open = _as_float(ctx.get("current_open"))
    prev_high = _as_float(ctx.get("previous_high"))
    prev_low = _as_float(ctx.get("previous_low"))

    if current_open is None or prev_high is None or prev_low is None:
        return "UNKNOWN"

    if current_open > prev_high:
        return "UP"

    if current_open < prev_low:
        return "DOWN"

    return "BALANCED"


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
    px = price if price is not None else _as_float(ctx.get("current_price"))
    val = _as_float(ctx.get("previous_val"))
    vah = _as_float(ctx.get("previous_vah"))

    if px is None or val is None or vah is None:
        return False

    low = min(val, vah)
    high = max(val, vah)
    return low <= px <= high


def _price_inside_range(ctx: dict[str, Any], price: float | None = None) -> bool:
    px = price if price is not None else _as_float(ctx.get("current_price"))
    prev_low = _as_float(ctx.get("previous_low"))
    prev_high = _as_float(ctx.get("previous_high"))

    if px is None or prev_low is None or prev_high is None:
        return False

    low = min(prev_low, prev_high)
    high = max(prev_low, prev_high)
    return low <= px <= high


def _accepted_outside_range(ctx: dict[str, Any], open_direction: Direction) -> bool:
    px = _as_float(ctx.get("current_price"))
    prev_high = _as_float(ctx.get("previous_high"))
    prev_low = _as_float(ctx.get("previous_low"))

    if px is None or prev_high is None or prev_low is None:
        return False

    if open_direction == "UP":
        return px > prev_high

    if open_direction == "DOWN":
        return px < prev_low

    return False


def _distance_threshold(ctx: dict[str, Any]) -> float:
    """
    Generic cross-asset threshold for near-zone checks.

    This is not an execution ladder. It only marks an area as worth attention.
    """
    price = _as_float(ctx.get("current_price")) or 1.0
    tick_size = _as_float(ctx.get("tick_size")) or 0.0

    pct_threshold = abs(price) * 0.001  # 0.10%
    tick_threshold = tick_size * 3 if tick_size > 0 else 0.0

    return max(pct_threshold, tick_threshold)


def _build_interest_zones(ctx: dict[str, Any]) -> list[InterestZone]:
    current_price = _as_float(ctx.get("current_price"))
    threshold = _distance_threshold(ctx)

    candidates: list[InterestZone] = []

    raw_levels = [
        ("POC", ctx.get("previous_poc"), "REFERENCE_ZONE", "previous POC"),
        ("VAH", ctx.get("previous_vah"), "REACTION_ZONE", "previous value area high"),
        ("VAL", ctx.get("previous_val"), "REACTION_ZONE", "previous value area low"),
        ("PREVIOUS_HIGH", ctx.get("previous_high"), "REFERENCE_ZONE", "previous session high"),
        ("PREVIOUS_LOW", ctx.get("previous_low"), "REFERENCE_ZONE", "previous session low"),
    ]

    for zone_type, raw_price, role, reason in raw_levels:
        price = _as_float(raw_price)
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
    open_context: OpenContext,
    ib_direction: Direction,
    accepted_back_inside_value: bool,
    accepted_back_inside_range: bool,
) -> ZoneReaction:
    if tested_level == "NONE":
        return "NONE"

    if open_context == "OPEN_OUT_OF_RANGE" and (accepted_back_inside_value or accepted_back_inside_range):
        return "REJECTED"

    if ib_direction in {"UP", "DOWN"}:
        return "REJECTED"

    return "UNCONFIRMED"


def _build_first_hour_activity(ctx: dict[str, Any]) -> FirstHourActivity:
    open_direction = _infer_open_direction(ctx)
    ib_direction = _infer_ib_direction(ctx)
    ib_extension_direction = _infer_ib_extension_direction(ctx)
    accepted_back_inside_value = _price_inside_value(ctx)
    accepted_back_inside_range = _price_inside_range(ctx)
    accepted_outside_range = _accepted_outside_range(ctx, open_direction)

    interest_zones = _build_interest_zones(ctx)
    tested_level = _infer_tested_level(interest_zones, ctx)
    test_result = _infer_test_result(
        tested_level=tested_level,
        open_context=normalize_open_context(ctx.get("open_relation")),
        ib_direction=ib_direction,
        accepted_back_inside_value=accepted_back_inside_value,
        accepted_back_inside_range=accepted_back_inside_range,
    )

    failed_auction = bool(
        open_direction in {"UP", "DOWN"}
        and (accepted_back_inside_value or accepted_back_inside_range)
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

    open_relation = ctx.get("open_relation") or flt.get("open_relation")
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

    open_context = normalize_open_context(open_relation)
    first_hour = _build_first_hour_activity(ctx)
    interest_zones = _build_interest_zones(ctx)
    primary_zone = interest_zones[0] if interest_zones else None

    context_available = bool(
        flt.get("auction_context_available")
        or ctx.get("auction_context_available")
        or ctx
    )
    is_stale = bool(flt.get("is_stale") or ctx.get("is_stale") or ctx.get("market_data_is_stale"))

    if not context_available:
        result = OpenBehaviorResult(
            open_context="UNKNOWN",
            open_behavior="UNCONFIRMED",
            open_behavior_confidence=0.0,
            first_hour_activity=first_hour,
            interest_zones=interest_zones,
            primary_interest_zone=primary_zone,
            battle_bias_hint="BLOCK",
            reason="No auction context available.",
            warnings=["auction_context_missing"],
        )
        return result.to_dict()

    if market_status not in {"OPEN", "UNKNOWN"}:
        result = OpenBehaviorResult(
            open_context=open_context,
            open_behavior="UNCONFIRMED",
            open_behavior_confidence=0.1,
            first_hour_activity=first_hour,
            interest_zones=interest_zones,
            primary_interest_zone=primary_zone,
            battle_bias_hint="BLOCK",
            reason=f"Market is not open enough for behavior classification: market_status={market_status}.",
            warnings=[f"market_status_{market_status.lower()}"],
        )
        return result.to_dict()

    if is_stale or tpo_permission in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"}:
        result = OpenBehaviorResult(
            open_context=open_context,
            open_behavior="UNCONFIRMED",
            open_behavior_confidence=0.15,
            first_hour_activity=first_hour,
            interest_zones=interest_zones,
            primary_interest_zone=primary_zone,
            battle_bias_hint="BLOCK",
            reason="TPO context is stale or degraded.",
            warnings=["tpo_stale_or_degraded"],
        )
        return result.to_dict()

    behavior: OpenBehavior = "UNCONFIRMED"
    confidence = 0.35
    entry_hint = "NO_ENTRY_MODEL"
    stop_hint = "NO_STOP_MODEL"
    battle_hint = "RESEARCH_ONLY"
    reason = "Open behavior is not clear enough."

    if open_context == "OPEN_OUT_OF_RANGE":
        if first_hour.failed_auction or first_hour.accepted_back_inside_value or first_hour.accepted_back_inside_range:
            behavior = "OPEN_REJECTION_REVERSE"
            confidence = 0.75
            entry_hint = "SWEEP_RECLAIM_BOS_RETEST"
            stop_hint = "BEHIND_SWEEP_EXTREME"
            battle_hint = "RESEARCH_COUNTERTREND_UNLESS_LTF_CONFIRMED"
            reason = "Out-of-range open failed to hold outside prior value/range; rejection-reverse conditions detected."
        elif (
            auction_bias == "DIRECTIONAL_IMBALANCE"
            and first_hour.accepted_outside_range
            and first_hour.ib_extension_direction in {"UP", "DOWN"}
            and first_hour.ib_extension_direction == first_hour.open_direction
        ):
            behavior = "OPEN_DRIVE"
            confidence = 0.82
            entry_hint = "PULLBACK_CONTINUATION"
            stop_hint = "BEHIND_PULLBACK_STRUCTURE_OR_IB_EDGE"
            battle_hint = "BOOST_IF_HTF_ALIGNED_AND_EXECUTABLE"
            reason = "Out-of-range open is holding outside prior range with directional IB extension."
        else:
            behavior = "UNCONFIRMED"
            confidence = 0.45
            entry_hint = "WAIT_FOR_ACCEPTANCE_OR_REJECTION"
            stop_hint = "NO_STOP_MODEL"
            battle_hint = "RESEARCH_ONLY"
            reason = "Out-of-range context exists, but acceptance/rejection is not confirmed yet."

    elif open_context == "OPEN_IN_RANGE":
        if (
            first_hour.tested_level in {"VAH", "VAL", "POC", "NPOC"}
            and first_hour.ib_extension_direction in {"UP", "DOWN"}
            and first_hour.test_result in {"REJECTED", "UNCONFIRMED"}
        ):
            behavior = "OPEN_TEST_DRIVE"
            confidence = 0.68
            entry_hint = "FAILED_ACCEPTANCE_RETEST"
            stop_hint = "BEYOND_FAILED_ACCEPTANCE_ZONE"
            battle_hint = "ALLOW_IF_HTF_ALIGNED_AND_LTF_CONFIRMED"
            reason = f"Open-in-range context with test near {first_hour.tested_level} and directional IB extension."
        elif auction_bias == "RANGE_EXTENSION" and first_hour.ib_extension_direction in {"UP", "DOWN"}:
            behavior = "OPEN_TEST_DRIVE"
            confidence = 0.60
            entry_hint = "PULLBACK_OR_FAILED_ACCEPTANCE_RETEST"
            stop_hint = "BEYOND_TEST_ZONE_OR_PULLBACK_STRUCTURE"
            battle_hint = "ALLOW_IF_HTF_ALIGNED_AND_LTF_CONFIRMED"
            reason = "Range open developed directional range extension; possible open-test-drive behavior."
        else:
            behavior = "OPEN_AUCTION"
            confidence = 0.55
            entry_hint = "NO_DIRECTIONAL_ENTRY_MODEL"
            stop_hint = "NO_STOP_MODEL"
            battle_hint = "RESEARCH_ONLY"
            reason = "Open-in-range context without clean directional drive; auction behavior preferred."

    elif open_context == "OPEN_INSIDE_VA":
        if auction_bias == "BALANCE" or modifier == "DOWNGRADE":
            behavior = "OPEN_AUCTION"
            confidence = 0.78
            entry_hint = "ROTATION_ONLY_IF_LTF_CONFIRMED"
            stop_hint = "BEYOND_VALUE_EDGE_OR_STRUCTURE"
            battle_hint = "DOWNGRADE_NO_DIRECTIONAL_BATTLE"
            reason = "Open inside value area with balance/downgrade context; directional edge reduced."
        elif first_hour.ib_extension_direction in {"UP", "DOWN"}:
            behavior = "OPEN_TEST_DRIVE"
            confidence = 0.55
            entry_hint = "FAILED_ACCEPTANCE_RETEST"
            stop_hint = "BEYOND_VALUE_EDGE"
            battle_hint = "RESEARCH_UNTIL_ACCEPTANCE_CONFIRMED"
            reason = "Inside-VA open has directional IB extension, but acceptance must be confirmed."
        else:
            behavior = "OPEN_AUCTION"
            confidence = 0.70
            entry_hint = "NO_DIRECTIONAL_ENTRY_MODEL"
            stop_hint = "NO_STOP_MODEL"
            battle_hint = "RESEARCH_ONLY"
            reason = "Open inside value area with no confirmed directional behavior."

    if htf in {"LONG", "SHORT"} and battle_hint.startswith("BOOST"):
        reason += f" HTF bias is available: {htf}."

    if primary_zone and primary_zone.zone_type == "NPOC":
        warnings.append("npoc_is_interest_zone_not_entry_trigger")

    if modifier == "DOWNGRADE":
        warnings.append("telegram_modifier_downgrade")

    result = OpenBehaviorResult(
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

    behavior = classify_tpo_open_behavior(ctx, flt, htf_bias=htf_bias)

    output["open_behavior"] = behavior

    # Mirror the most important fields into context/filters for easier downstream use.
    ctx = dict(ctx)
    flt = dict(flt)

    ctx["open_context"] = behavior.get("open_context")
    ctx["open_behavior"] = behavior.get("open_behavior")
    ctx["open_behavior_confidence"] = behavior.get("open_behavior_confidence")
    ctx["first_hour_activity"] = behavior.get("first_hour_activity")
    ctx["primary_interest_zone"] = behavior.get("primary_interest_zone")
    ctx["interest_zones"] = behavior.get("interest_zones")
    ctx["entry_model_hint"] = behavior.get("entry_model_hint")
    ctx["stop_model_hint"] = behavior.get("stop_model_hint")
    ctx["battle_bias_hint"] = behavior.get("battle_bias_hint")
    ctx["open_behavior_reason"] = behavior.get("reason")

    flt["open_context"] = behavior.get("open_context")
    flt["open_behavior"] = behavior.get("open_behavior")
    flt["open_behavior_confidence"] = behavior.get("open_behavior_confidence")
    flt["entry_model_hint"] = behavior.get("entry_model_hint")
    flt["stop_model_hint"] = behavior.get("stop_model_hint")
    flt["battle_bias_hint"] = behavior.get("battle_bias_hint")

    output["context"] = ctx
    output["filters"] = flt
    return output