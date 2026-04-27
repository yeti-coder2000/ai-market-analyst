ffrom __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


# =============================================================================
# EXECUTION CONTRACT
# =============================================================================

@dataclass
class ExecutionPlan:
    status: str  # EXECUTABLE | INCOMPLETE | NOT_EXECUTABLE
    model: str

    entry_reference_price: Optional[float] = None
    invalidation_reference_price: Optional[float] = None
    target_reference_price: Optional[float] = None

    risk_reward_ratio: Optional[float] = None
    stop_distance: Optional[float] = None
    target_distance: Optional[float] = None

    execution_timeframe: Optional[str] = None
    trigger_reason: Optional[str] = None


# =============================================================================
# EXECUTION QUALITY GUARDS
# =============================================================================

MIN_RR = 1.5
MAX_RR = 10.0
MIN_STOP_DISTANCE_PCT = 0.0002  # 0.02% of price

MIN_STOP_DISTANCE_FX = 0.0003
MIN_STOP_DISTANCE_LOW_PRICE = 0.05
MIN_STOP_DISTANCE_XAU = 1.0
MIN_STOP_DISTANCE_CRYPTO = 25.0


def build_execution_plan(
    context: Any,
    scenario_type: Any,
    direction: Any,
    evidence: Any,
) -> ExecutionPlan:
    """
    Conservative execution layer.

    It may return INCOMPLETE often, but it must never mark broken geometry,
    microscopic stops, or fantasy RR as EXECUTABLE.
    """
    try:
        scenario_name = str(getattr(scenario_type, "value", scenario_type) or "").upper()

        if "SWEEP_RETURN" in scenario_name:
            return _build_sweep_return(context, direction, evidence)

        if "TREND_CONTINUATION" in scenario_name:
            return _build_trend_continuation(context, direction, evidence)

        return ExecutionPlan(
            status="NOT_EXECUTABLE",
            model="NONE",
            trigger_reason="unsupported_scenario",
        )

    except Exception as exc:  # noqa: BLE001
        return ExecutionPlan(
            status="INCOMPLETE",
            model="NONE",
            trigger_reason=f"execution_exception:{type(exc).__name__}",
        )


# ============================================================================
# SWEEP RETURN
# ============================================================================

def _build_sweep_return(context: Any, direction: Any, evidence: Any) -> ExecutionPlan:
    del evidence

    price = _get_price(context)
    if price is None:
        return ExecutionPlan(
            status="INCOMPLETE",
            model="LIMIT_ON_RETEST",
            trigger_reason="missing_price",
        )

    atr = _extract_atr(context)
    sweep = getattr(context, "sweep", None)
    profile = getattr(context, "profile", None)

    if sweep is None:
        return ExecutionPlan(
            status="INCOMPLETE",
            model="LIMIT_ON_RETEST",
            trigger_reason="missing_sweep_context",
        )

    direction_value = _normalize_direction(direction)

    sweep_reference = _first_float(
        getattr(sweep, "reference_price", None),
        getattr(sweep, "swept_price", None),
        getattr(sweep, "level_price", None),
    )

    daily_profile = getattr(profile, "daily", None) if profile is not None else None
    weekly_profile = getattr(profile, "weekly", None) if profile is not None else None

    daily_val = _first_float(getattr(daily_profile, "val", None))
    daily_vah = _first_float(getattr(daily_profile, "vah", None))
    daily_poc = _first_float(getattr(daily_profile, "poc", None))

    weekly_val = _first_float(getattr(weekly_profile, "val", None))
    weekly_vah = _first_float(getattr(weekly_profile, "vah", None))
    weekly_poc = _first_float(getattr(weekly_profile, "poc", None))

    if direction_value == "SHORT":
        entry = _first_float(price, daily_poc, weekly_poc)
        invalidation_anchor = _first_float(
            getattr(sweep, "high", None),
            daily_vah,
            weekly_vah,
            sweep_reference,
        )
        target = _first_float(
            daily_val,
            weekly_val,
            price - _fallback_target_distance(price, atr),
        )

        if entry is None or invalidation_anchor is None or target is None:
            return _incomplete_plan(
                model="LIMIT_ON_RETEST",
                entry=entry,
                invalidation=invalidation_anchor,
                target=target,
                reason="sweep_return_short_structure_incomplete",
            )

        invalidation = invalidation_anchor + _buffer(price, atr)

        return _finalize_plan(
            entry=entry,
            invalidation=invalidation,
            target=target,
            model="LIMIT_ON_RETEST",
            direction=direction_value,
            trigger_reason="sweep_return_short",
            price=price,
        )

    if direction_value == "LONG":
        entry = _first_float(price, daily_poc, weekly_poc)
        invalidation_anchor = _first_float(
            getattr(sweep, "low", None),
            sweep_reference,
            daily_val,
            weekly_val,
        )
        target = _first_float(
            daily_vah,
            weekly_vah,
            price + _fallback_target_distance(price, atr),
        )

        if entry is None or invalidation_anchor is None or target is None:
            return _incomplete_plan(
                model="LIMIT_ON_RETEST",
                entry=entry,
                invalidation=invalidation_anchor,
                target=target,
                reason="sweep_return_long_structure_incomplete",
            )

        invalidation = invalidation_anchor - _buffer(price, atr)

        return _finalize_plan(
            entry=entry,
            invalidation=invalidation,
            target=target,
            model="LIMIT_ON_RETEST",
            direction=direction_value,
            trigger_reason="sweep_return_long",
            price=price,
        )

    return ExecutionPlan(
        status="INCOMPLETE",
        model="LIMIT_ON_RETEST",
        trigger_reason="sweep_return_neutral_direction",
    )


# ============================================================================
# TREND CONTINUATION
# ============================================================================

def _build_trend_continuation(context: Any, direction: Any, evidence: Any) -> ExecutionPlan:
    del evidence

    price = _get_price(context)
    if price is None:
        return ExecutionPlan(
            status="INCOMPLETE",
            model="LIMIT_ON_RETEST",
            trigger_reason="missing_price",
        )

    atr = _extract_atr(context)
    direction_value = _normalize_direction(direction)

    pullback = getattr(context, "pullback", None)
    structure_15m = getattr(context, "structure_15m", None)
    profile = getattr(context, "profile", None)

    daily_profile = getattr(profile, "daily", None) if profile is not None else None
    daily_val = _first_float(getattr(daily_profile, "val", None))
    daily_vah = _first_float(getattr(daily_profile, "vah", None))
    daily_poc = _first_float(getattr(daily_profile, "poc", None))

    pullback_low = _first_float(
        getattr(pullback, "low", None),
        getattr(pullback, "reference_low", None),
        getattr(structure_15m, "last_hl_price", None),
    )

    pullback_high = _first_float(
        getattr(pullback, "high", None),
        getattr(pullback, "reference_high", None),
        getattr(structure_15m, "last_lh_price", None),
    )

    if direction_value == "LONG":
        entry = _first_float(price, daily_poc)
        invalidation_anchor = _first_float(pullback_low, daily_val)
        target = _first_float(
            daily_vah,
            price + _fallback_target_distance(price, atr),
        )

        if entry is None or invalidation_anchor is None or target is None:
            return _incomplete_plan(
                model="LIMIT_ON_RETEST",
                entry=entry,
                invalidation=invalidation_anchor,
                target=target,
                reason="trend_continuation_long_structure_incomplete",
            )

        invalidation = invalidation_anchor - _buffer(price, atr)

        return _finalize_plan(
            entry=entry,
            invalidation=invalidation,
            target=target,
            model="LIMIT_ON_RETEST",
            direction=direction_value,
            trigger_reason="trend_continuation_long",
            price=price,
        )

    if direction_value == "SHORT":
        entry = _first_float(price, daily_poc)
        invalidation_anchor = _first_float(pullback_high, daily_vah)
        target = _first_float(
            daily_val,
            price - _fallback_target_distance(price, atr),
        )

        if entry is None or invalidation_anchor is None or target is None:
            return _incomplete_plan(
                model="LIMIT_ON_RETEST",
                entry=entry,
                invalidation=invalidation_anchor,
                target=target,
                reason="trend_continuation_short_structure_incomplete",
            )

        invalidation = invalidation_anchor + _buffer(price, atr)

        return _finalize_plan(
            entry=entry,
            invalidation=invalidation,
            target=target,
            model="LIMIT_ON_RETEST",
            direction=direction_value,
            trigger_reason="trend_continuation_short",
            price=price,
        )

    return ExecutionPlan(
        status="INCOMPLETE",
        model="LIMIT_ON_RETEST",
        trigger_reason="trend_continuation_neutral_direction",
    )


# ============================================================================
# FINALIZATION
# ============================================================================

def _finalize_plan(
    *,
    entry: float,
    invalidation: float,
    target: float,
    model: str,
    direction: str,
    trigger_reason: str,
    price: float | None,
) -> ExecutionPlan:
    if entry is None or invalidation is None or target is None:
        return _incomplete_plan(
            model=model,
            entry=entry,
            invalidation=invalidation,
            target=target,
            reason=f"{trigger_reason}_missing_price_points",
        )

    entry = float(entry)
    invalidation = float(invalidation)
    target = float(target)
    direction = str(direction or "").upper()

    if direction == "SHORT":
        if not (invalidation > entry > target):
            return _incomplete_plan(
                model=model,
                entry=entry,
                invalidation=invalidation,
                target=target,
                reason=f"{trigger_reason}_invalid_geometry",
            )

    elif direction == "LONG":
        if not (target > entry > invalidation):
            return _incomplete_plan(
                model=model,
                entry=entry,
                invalidation=invalidation,
                target=target,
                reason=f"{trigger_reason}_invalid_geometry",
            )

    else:
        return _incomplete_plan(
            model=model,
            entry=entry,
            invalidation=invalidation,
            target=target,
            reason=f"{trigger_reason}_invalid_direction",
        )

    stop_distance = abs(entry - invalidation)
    target_distance = abs(target - entry)

    if stop_distance <= 0 or target_distance <= 0:
        return _incomplete_plan(
            model=model,
            entry=entry,
            invalidation=invalidation,
            target=target,
            reason=f"{trigger_reason}_zero_distance",
            stop_distance=stop_distance,
            target_distance=target_distance,
        )

    min_stop_distance = _min_stop_distance(price or entry)

    if stop_distance < min_stop_distance:
        return _incomplete_plan(
            model=model,
            entry=entry,
            invalidation=invalidation,
            target=target,
            reason=f"{trigger_reason}_invalid_geometry_small_stop",
            stop_distance=stop_distance,
            target_distance=target_distance,
        )

    rr = target_distance / stop_distance

    if rr < MIN_RR:
        return _incomplete_plan(
            model=model,
            entry=entry,
            invalidation=invalidation,
            target=target,
            reason=f"{trigger_reason}_rr_too_low",
            rr=rr,
            stop_distance=stop_distance,
            target_distance=target_distance,
        )

    if rr > MAX_RR:
        return _incomplete_plan(
            model=model,
            entry=entry,
            invalidation=invalidation,
            target=target,
            reason=f"{trigger_reason}_rr_too_high",
            rr=rr,
            stop_distance=stop_distance,
            target_distance=target_distance,
        )

    return ExecutionPlan(
        status="EXECUTABLE",
        model=model,
        entry_reference_price=_round_price(entry),
        invalidation_reference_price=_round_price(invalidation),
        target_reference_price=_round_price(target),
        risk_reward_ratio=round(rr, 2),
        stop_distance=_round_distance(stop_distance, price or entry),
        target_distance=_round_distance(target_distance, price or entry),
        execution_timeframe="15m",
        trigger_reason=trigger_reason,
    )


def _incomplete_plan(
    *,
    model: str,
    entry: float | None = None,
    invalidation: float | None = None,
    target: float | None = None,
    reason: str,
    rr: float | None = None,
    stop_distance: float | None = None,
    target_distance: float | None = None,
) -> ExecutionPlan:
    return ExecutionPlan(
        status="INCOMPLETE",
        model=model,
        entry_reference_price=_round_price(entry),
        invalidation_reference_price=_round_price(invalidation),
        target_reference_price=_round_price(target),
        risk_reward_ratio=round(rr, 2) if rr is not None else None,
        stop_distance=_round_price(stop_distance),
        target_distance=_round_price(target_distance),
        execution_timeframe="15m",
        trigger_reason=reason,
    )


# ============================================================================
# HELPERS
# ============================================================================

def _normalize_direction(direction: Any) -> str:
    return str(getattr(direction, "value", direction) or "").upper()


def _extract_atr(context: Any) -> float | None:
    return _first_float(
        getattr(context, "atr", None),
        getattr(getattr(context, "volatility", None), "atr", None),
        getattr(getattr(context, "stats", None), "atr", None),
    )


def _buffer(price: float | None, atr: float | None) -> float:
    if atr is not None and atr > 0:
        return atr * 0.10

    if price is None:
        return 0.0005

    if price > 10000:
        return 25.0
    if price > 1000:
        return 2.5
    if price > 50:
        return 0.5
    return 0.0005


def _fallback_target_distance(price: float, atr: float | None) -> float:
    if atr is not None and atr > 0:
        return atr * 2.0

    if price > 10000:
        return 800.0
    if price > 1000:
        return 20.0
    if price > 50:
        return 2.0
    return 0.004


def _min_stop_distance(price: float | None) -> float:
    if price is None:
        return MIN_STOP_DISTANCE_FX

    price = abs(float(price))
    pct_floor = price * MIN_STOP_DISTANCE_PCT

    if price > 10000:
        return max(MIN_STOP_DISTANCE_CRYPTO, pct_floor)

    if price > 1000:
        return max(MIN_STOP_DISTANCE_XAU, pct_floor)

    if price > 50:
        return max(MIN_STOP_DISTANCE_LOW_PRICE, pct_floor)

    return max(MIN_STOP_DISTANCE_FX, pct_floor)


def _get_price(context: Any) -> float | None:
    return _first_float(
        getattr(context, "current_price", None),
        getattr(context, "price", None),
    )


def _first_float(*values: Any) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _round_price(value: float | None) -> float | None:
    if value is None:
        return None

    value = float(value)

    if abs(value) >= 1000:
        return round(value, 2)
    if abs(value) >= 10:
        return round(value, 2)
    return round(value, 5)


def _round_distance(value: float | None, price: float | None) -> float | None:
    if value is None:
        return None

    value = float(value)

    if price is None:
        return _round_price(value)

    price = abs(float(price))

    if price >= 1000:
        return round(value, 2)
    if price >= 10:
        return round(value, 2)
    return round(value, 5)