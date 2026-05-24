from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class BattlePermission(str, Enum):
    BATTLE_READY = "BATTLE_READY"
    RESEARCH_ONLY = "RESEARCH_ONLY"
    BLOCKED_BY_MARKET_CLOSED = "BLOCKED_BY_MARKET_CLOSED"
    BLOCKED_BY_STALE_DATA = "BLOCKED_BY_STALE_DATA"
    BLOCKED_BY_AUCTION = "BLOCKED_BY_AUCTION"
    BLOCKED_BY_HTF = "BLOCKED_BY_HTF"
    BLOCKED_BY_EXECUTION = "BLOCKED_BY_EXECUTION"
    BLOCKED_BY_RR = "BLOCKED_BY_RR"
    BLOCKED_BY_STOP_QUALITY = "BLOCKED_BY_STOP_QUALITY"
    BLOCKED_BY_QUALITY = "BLOCKED_BY_QUALITY"
    BLOCKED_BY_CONTEXT = "BLOCKED_BY_CONTEXT"
    NOT_READY = "NOT_READY"


class TelegramDeliveryMode(str, Enum):
    BATTLE_ALERT = "BATTLE_ALERT"
    RESEARCH_ALERT = "RESEARCH_ALERT"
    SUPPRESS = "SUPPRESS"


@dataclass
class BattlePermissionResult:
    battle_permission: str
    telegram_delivery_mode: str
    battle_ready: bool
    auction_context_score: int
    reasons: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    modifiers: list[str] = field(default_factory=list)

    market_is_open: bool | None = None
    market_status: str | None = None
    tpo_signal_permission: str | None = None
    tpo_telegram_modifier: str | None = None
    open_relation: str | None = None
    auction_bias: str | None = None

    direction: str | None = None
    htf_bias: str | None = None
    signal_alignment: str | None = None
    execution_status: str | None = None
    practical_rr: float | None = None
    stop_quality: str | None = None
    quality_tier: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _deep_get(data: dict[str, Any], *paths: str) -> Any:
    """
    Reads the first non-empty value from dotted paths.

    Example:
    _deep_get(payload, "metadata.auction_context.market_status", "market_status")
    """
    for path in paths:
        current: Any = data

        for part in path.split("."):
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(part)

        if current not in (None, "", [], {}):
            return current

    return None


def _as_upper(value: Any) -> str | None:
    if value in (None, "", [], {}):
        return None
    return str(value).strip().upper()


def _as_float(value: Any) -> float | None:
    if value in (None, "", [], {}):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value

    if value is None:
        return None

    if isinstance(value, str):
        normalized = value.strip().lower()

        if normalized in {"true", "yes", "1"}:
            return True

        if normalized in {"false", "no", "0"}:
            return False

    return None


def _normalize_direction(value: Any) -> str | None:
    normalized = _as_upper(value)

    if normalized in {"LONG", "BUY", "BULL", "BULLISH", "UP"}:
        return "LONG"

    if normalized in {"SHORT", "SELL", "BEAR", "BEARISH", "DOWN"}:
        return "SHORT"

    if normalized in {"NEUTRAL", "NONE", "NO_TRADE"}:
        return "NEUTRAL"

    return normalized


def _normalize_open_relation(value: Any) -> str | None:
    normalized = _as_upper(value)

    if normalized in {"OPEN_INSIDE_VA", "INSIDE_VALUE", "INSIDE_VALUE_AREA"}:
        return "INSIDE_VA"

    if normalized in {"OPEN_IN_RANGE", "IN_RANGE"}:
        return "RANGE"

    if normalized in {"OPEN_OUT_OF_RANGE", "OUTSIDE_RANGE", "OUTSIDE_PREVIOUS_RANGE"}:
        return "OUT_OF_RANGE"

    return normalized


def _direction_matches_htf(direction: str | None, htf_bias: str | None) -> bool:
    if not direction or not htf_bias:
        return False

    direction = _normalize_direction(direction)
    htf_bias = _normalize_direction(htf_bias)

    return direction in {"LONG", "SHORT"} and direction == htf_bias


def _extract_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Some journal events are shaped like:
    {"payload": {"payload": signal}}

    Telegram payloads are usually already flat enough.
    This keeps the gate tolerant.
    """
    nested = payload.get("payload")

    if isinstance(nested, dict):
        nested_2 = nested.get("payload")
        if isinstance(nested_2, dict):
            return nested_2
        return nested

    return payload


def extract_battle_inputs(raw_payload: dict[str, Any]) -> dict[str, Any]:
    payload = _extract_payload(raw_payload)

    market_is_open = _as_bool(
        _deep_get(
            payload,
            "metadata.auction_context.market_is_open",
            "metadata.auction_filters.market_is_open",
            "auction_context.market_is_open",
            "auction_filters.market_is_open",
            "market_is_open",
        )
    )

    market_status = _as_upper(
        _deep_get(
            payload,
            "metadata.auction_context.market_status",
            "metadata.auction_filters.market_status",
            "auction_context.market_status",
            "auction_filters.market_status",
            "market_status",
        )
    )

    tpo_signal_permission = _as_upper(
        _deep_get(
            payload,
            "metadata.tpo_signal_permission",
            "metadata.auction_filters.tpo_signal_permission",
            "auction_filters.tpo_signal_permission",
            "tpo_signal_permission",
        )
    )

    tpo_telegram_modifier = _as_upper(
        _deep_get(
            payload,
            "metadata.tpo_telegram_modifier",
            "metadata.auction_filters.telegram_modifier",
            "auction_filters.telegram_modifier",
            "telegram_modifier",
            "tpo_telegram_modifier",
        )
    )

    open_relation = _normalize_open_relation(
        _deep_get(
            payload,
            "metadata.tpo_open_relation",
            "metadata.auction_context.open_relation",
            "metadata.auction_filters.open_relation",
            "auction_context.open_relation",
            "auction_filters.open_relation",
            "open_relation",
        )
    )

    auction_bias = _as_upper(
        _deep_get(
            payload,
            "metadata.tpo_auction_bias",
            "metadata.auction_context.auction_bias",
            "metadata.auction_filters.auction_bias",
            "auction_context.auction_bias",
            "auction_filters.auction_bias",
            "auction_bias",
        )
    )

    direction = _normalize_direction(
        _deep_get(
            payload,
            "direction",
            "trade_direction",
            "metadata.direction",
        )
    )

    htf_bias = _normalize_direction(
        _deep_get(
            payload,
            "htf_bias",
            "metadata.htf_bias",
            "context.htf_bias",
        )
    )

    signal_alignment = _as_upper(
        _deep_get(
            payload,
            "signal_alignment",
            "alignment",
            "metadata.signal_alignment",
            "metadata.alignment",
        )
    )

    execution_status = _as_upper(
        _deep_get(
            payload,
            "execution_status",
            "metadata.execution_status",
            "execution.status",
        )
    )

    practical_rr = _as_float(
        _deep_get(
            payload,
            "practical_rr",
            "rr",
            "risk_reward",
            "metadata.practical_rr",
            "metadata.rr",
            "execution.practical_rr",
        )
    )

    stop_quality = _as_upper(
        _deep_get(
            payload,
            "stop_quality",
            "metadata.stop_quality",
            "execution.stop_quality",
        )
    )

    quality_tier = _as_upper(
        _deep_get(
            payload,
            "quality_tier",
            "quality_level",
            "metadata.quality_tier",
            "metadata.quality_level",
        )
    )

    status = _as_upper(
        _deep_get(
            payload,
            "status",
            "alert_type",
            "signal_class",
        )
    )

    market_state = _as_upper(
        _deep_get(
            payload,
            "market_state",
            "metadata.market_state",
            "context.market_state",
        )
    )

    scenario = _as_upper(
        _deep_get(
            payload,
            "scenario",
            "metadata.scenario",
        )
    )

    nearest_npoc_distance = _as_float(
        _deep_get(
            payload,
            "metadata.auction_context.nearest_npoc_distance",
            "auction_context.nearest_npoc_distance",
            "nearest_npoc_distance",
        )
    )

    ib_extension_up_pct = _as_float(
        _deep_get(
            payload,
            "metadata.auction_context.ib_extension_up_pct",
            "auction_context.ib_extension_up_pct",
            "ib_extension_up_pct",
        )
    )

    ib_extension_down_pct = _as_float(
        _deep_get(
            payload,
            "metadata.auction_context.ib_extension_down_pct",
            "auction_context.ib_extension_down_pct",
            "ib_extension_down_pct",
        )
    )

    accepted_back_inside_value = _as_bool(
        _deep_get(
            payload,
            "metadata.auction_context.accepted_back_inside_value",
            "auction_context.accepted_back_inside_value",
            "accepted_back_inside_value",
        )
    )

    return {
        "payload": payload,
        "market_is_open": market_is_open,
        "market_status": market_status,
        "tpo_signal_permission": tpo_signal_permission,
        "tpo_telegram_modifier": tpo_telegram_modifier,
        "open_relation": open_relation,
        "auction_bias": auction_bias,
        "direction": direction,
        "htf_bias": htf_bias,
        "signal_alignment": signal_alignment,
        "execution_status": execution_status,
        "practical_rr": practical_rr,
        "stop_quality": stop_quality,
        "quality_tier": quality_tier,
        "status": status,
        "market_state": market_state,
        "scenario": scenario,
        "nearest_npoc_distance": nearest_npoc_distance,
        "ib_extension_up_pct": ib_extension_up_pct,
        "ib_extension_down_pct": ib_extension_down_pct,
        "accepted_back_inside_value": accepted_back_inside_value,
    }


def calculate_auction_context_score(inputs: dict[str, Any]) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

    open_relation = inputs.get("open_relation")
    direction = inputs.get("direction")
    htf_bias = inputs.get("htf_bias")
    nearest_npoc_distance = inputs.get("nearest_npoc_distance")
    ib_extension_up_pct = inputs.get("ib_extension_up_pct")
    ib_extension_down_pct = inputs.get("ib_extension_down_pct")
    accepted_back_inside_value = inputs.get("accepted_back_inside_value")

    if open_relation == "OUT_OF_RANGE":
        score += 2
        reasons.append("open_relation OUT_OF_RANGE: +2")

    elif open_relation == "RANGE":
        score += 1
        reasons.append("open_relation RANGE: +1")

    elif open_relation == "INSIDE_VA":
        score -= 2
        reasons.append("open_relation INSIDE_VA: -2")

    if _direction_matches_htf(direction, htf_bias):
        score += 2
        reasons.append("direction aligned with HTF: +2")
    else:
        reasons.append("direction not aligned with HTF: +0")

    if nearest_npoc_distance is not None:
        score += 1
        reasons.append("nearest nPOC available as interest zone: +1")

    direction_norm = _normalize_direction(direction)

    if direction_norm == "LONG" and ib_extension_up_pct is not None and ib_extension_up_pct >= 0.5:
        score += 1
        reasons.append("IB upside extension >= 0.5 in LONG direction: +1")

    if direction_norm == "SHORT" and ib_extension_down_pct is not None and ib_extension_down_pct >= 0.5:
        score += 1
        reasons.append("IB downside extension >= 0.5 in SHORT direction: +1")

    if accepted_back_inside_value is True:
        score -= 2
        reasons.append("accepted back inside value: -2")

    return score, reasons


def evaluate_battle_permission(raw_payload: dict[str, Any]) -> BattlePermissionResult:
    inputs = extract_battle_inputs(raw_payload)
    auction_score, score_reasons = calculate_auction_context_score(inputs)

    reasons: list[str] = list(score_reasons)
    blockers: list[str] = []
    modifiers: list[str] = []

    market_is_open = inputs.get("market_is_open")
    market_status = inputs.get("market_status")
    tpo_signal_permission = inputs.get("tpo_signal_permission")
    tpo_telegram_modifier = inputs.get("tpo_telegram_modifier")
    open_relation = inputs.get("open_relation")
    auction_bias = inputs.get("auction_bias")
    direction = inputs.get("direction")
    htf_bias = inputs.get("htf_bias")
    signal_alignment = inputs.get("signal_alignment")
    execution_status = inputs.get("execution_status")
    practical_rr = inputs.get("practical_rr")
    stop_quality = inputs.get("stop_quality")
    quality_tier = inputs.get("quality_tier")
    status = inputs.get("status")
    market_state = inputs.get("market_state")
    scenario = inputs.get("scenario")

    # 1. Absolute market / data blockers.
    if market_is_open is False or market_status in {"MARKET_CLOSED", "MARKET_CLOSED_AND_STALE"}:
        blockers.append("market_closed")
        return BattlePermissionResult(
            battle_permission=BattlePermission.BLOCKED_BY_MARKET_CLOSED.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + ["market is closed; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    if market_status == "STALE_DATA" or tpo_signal_permission == "STALE_DATA":
        blockers.append("stale_data")
        return BattlePermissionResult(
            battle_permission=BattlePermission.BLOCKED_BY_STALE_DATA.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + ["market data is stale; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    # 2. TPO / auction research blockers.
    if tpo_signal_permission in {"MARKET_CLOSED", "RESEARCH_ONLY", "BLOCKED_BY_CONTEXT", "BLOCKED_BY_AUCTION"}:
        blockers.append(f"tpo_permission_{tpo_signal_permission.lower()}")
        return BattlePermissionResult(
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + [f"TPO permission is {tpo_signal_permission}; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    if tpo_telegram_modifier == "DOWNGRADE":
        blockers.append("tpo_downgrade")
        return BattlePermissionResult(
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + ["TPO telegram modifier is DOWNGRADE; research only"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    # 3. Technical readiness.
    if status not in {"READY", "ENTRY_READY", "EXECUTABLE"}:
        blockers.append("not_ready_status")
        return BattlePermissionResult(
            battle_permission=BattlePermission.NOT_READY.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + [f"status={status}; not a battle-ready signal"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    if execution_status != "EXECUTABLE":
        blockers.append("execution_not_executable")
        return BattlePermissionResult(
            battle_permission=BattlePermission.BLOCKED_BY_EXECUTION.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + [f"execution_status={execution_status}; not executable"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    # 4. HTF alignment.
    if not _direction_matches_htf(direction, htf_bias):
        blockers.append("direction_not_aligned_with_htf")
        return BattlePermissionResult(
            battle_permission=BattlePermission.BLOCKED_BY_HTF.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + [f"direction={direction} not aligned with htf_bias={htf_bias}"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    if signal_alignment == "COUNTER_TREND":
        blockers.append("counter_trend")
        return BattlePermissionResult(
            battle_permission=BattlePermission.BLOCKED_BY_HTF.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + ["signal_alignment=COUNTER_TREND; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    # 5. RR / stop / quality.
    if practical_rr is None or practical_rr < 2.0:
        blockers.append("practical_rr_below_2")
        return BattlePermissionResult(
            battle_permission=BattlePermission.BLOCKED_BY_RR.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + [f"practical_rr={practical_rr}; minimum is 2.0"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    if stop_quality == "TIGHT_STOP":
        blockers.append("tight_stop")
        return BattlePermissionResult(
            battle_permission=BattlePermission.BLOCKED_BY_STOP_QUALITY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + ["stop_quality=TIGHT_STOP; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    if quality_tier in {"DANGER", "BLOCK", "FAIL"}:
        blockers.append("quality_tier_blocked")
        return BattlePermissionResult(
            battle_permission=BattlePermission.BLOCKED_BY_QUALITY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + [f"quality_tier={quality_tier}; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    if quality_tier == "CAUTION" and market_state == "TRANSITION" and scenario in {"SWEEP_RETURN_LONG", "SWEEP_RETURN_SHORT"}:
        blockers.append("caution_transition_sweep_return")
        return BattlePermissionResult(
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + ["CAUTION + TRANSITION + SWEEP_RETURN; research only"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    # 6. Auction score final gate.
    if auction_score < 3:
        blockers.append("auction_context_score_below_3")
        return BattlePermissionResult(
            battle_permission=BattlePermission.BLOCKED_BY_AUCTION.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            auction_context_score=auction_score,
            reasons=reasons + [f"auction_context_score={auction_score}; minimum is 3"],
            blockers=blockers,
            modifiers=modifiers,
            market_is_open=market_is_open,
            market_status=market_status,
            tpo_signal_permission=tpo_signal_permission,
            tpo_telegram_modifier=tpo_telegram_modifier,
            open_relation=open_relation,
            auction_bias=auction_bias,
            direction=direction,
            htf_bias=htf_bias,
            signal_alignment=signal_alignment,
            execution_status=execution_status,
            practical_rr=practical_rr,
            stop_quality=stop_quality,
            quality_tier=quality_tier,
        )

    # 7. Battle ready.
    if tpo_telegram_modifier == "BOOST":
        modifiers.append("tpo_boost")

    return BattlePermissionResult(
        battle_permission=BattlePermission.BATTLE_READY.value,
        telegram_delivery_mode=TelegramDeliveryMode.BATTLE_ALERT.value,
        battle_ready=True,
        auction_context_score=auction_score,
        reasons=reasons + ["all battle permission checks passed"],
        blockers=blockers,
        modifiers=modifiers,
        market_is_open=market_is_open,
        market_status=market_status,
        tpo_signal_permission=tpo_signal_permission,
        tpo_telegram_modifier=tpo_telegram_modifier,
        open_relation=open_relation,
        auction_bias=auction_bias,
        direction=direction,
        htf_bias=htf_bias,
        signal_alignment=signal_alignment,
        execution_status=execution_status,
        practical_rr=practical_rr,
        stop_quality=stop_quality,
        quality_tier=quality_tier,
    )


def apply_battle_permission(raw_payload: dict[str, Any]) -> dict[str, Any]:
    """
    Returns a copy of payload enriched with final battle permission fields.
    Does not mutate the input payload.
    """
    payload = dict(raw_payload)
    result = evaluate_battle_permission(payload).to_dict()

    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    metadata["battle_permission"] = result["battle_permission"]
    metadata["telegram_delivery_mode"] = result["telegram_delivery_mode"]
    metadata["battle_ready"] = result["battle_ready"]
    metadata["auction_context_score"] = result["auction_context_score"]
    metadata["battle_permission_reasons"] = result["reasons"]
    metadata["battle_permission_blockers"] = result["blockers"]
    metadata["battle_permission_modifiers"] = result["modifiers"]

    payload["metadata"] = metadata
    payload["battle_permission"] = result["battle_permission"]
    payload["telegram_delivery_mode"] = result["telegram_delivery_mode"]
    payload["battle_ready"] = result["battle_ready"]
    payload["auction_context_score"] = result["auction_context_score"]

    return payload