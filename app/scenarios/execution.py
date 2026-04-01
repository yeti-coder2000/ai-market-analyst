from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


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


def build_execution_plan(context: Any, scenario_type: Any, direction: Any, evidence: Any) -> ExecutionPlan:
    try:
        scenario_name = getattr(scenario_type, "value", scenario_type) or ""

        if "SWEEP_RETURN" in str(scenario_name):
            return _build_sweep_return(context, direction, evidence)

        if "TREND_CONTINUATION" in str(scenario_name):
            return _build_trend_continuation(context, direction, evidence)

        return ExecutionPlan(status="NOT_EXECUTABLE", model="NONE")

    except Exception:
        return ExecutionPlan(status="INCOMPLETE", model="NONE")


# ============================================================================
# SWEEP RETURN
# ============================================================================

def _build_sweep_return(context: Any, direction: Any, evidence: Any) -> ExecutionPlan:
    price = _get_price(context)
    if price is None:
        return ExecutionPlan(status="INCOMPLETE", model="LIMIT_ON_RETEST")

    atr = _extract_atr(context)
    sweep = getattr(context, "sweep", None)
    profile = getattr(context, "profile", None)

    if sweep is None:
        return ExecutionPlan(status="INCOMPLETE", model="LIMIT_ON_RETEST")

    direction_value = getattr(direction, "value", direction)

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
        entry = _first_float(
            price,
            daily_poc,
            weekly_poc,
        )

        sweep_direction = getattr(sweep, "direction", None)

        if direction_value == "SHORT":
            invalidation_anchor = _first_float(
                getattr(sweep, "high", None),
                daily_vah,
                weekly_vah,
                sweep_reference,
            )
        else:
            invalidation_anchor = _first_float(
                getattr(sweep, "low", None),
                daily_val,
                weekly_val,
                sweep_reference,
            )

        target = _first_float(
            daily_val,
            weekly_val,
            price - _fallback_target_distance(price, atr, direction_value),
        )

        if entry is None or invalidation_anchor is None or target is None:
            return ExecutionPlan(
                status="INCOMPLETE",
                model="LIMIT_ON_RETEST",
                entry_reference_price=_round_price(entry),
                invalidation_reference_price=_round_price(invalidation_anchor),
                target_reference_price=_round_price(target),
                execution_timeframe="15m",
                trigger_reason="sweep_return_structure_incomplete",
            )

        invalidation = invalidation_anchor + _buffer(price, atr)

        return _finalize_plan(
            entry=entry,
            invalidation=invalidation,
            target=target,
            model="LIMIT_ON_RETEST",
            direction=direction_value,
            trigger_reason="sweep_return_short",
        )

    if direction_value == "LONG":
        entry = _first_float(
            price,
            daily_poc,
            weekly_poc,
        )

        invalidation_anchor = _first_float(
            sweep_reference,
            daily_val,
            weekly_val,
        )

        target = _first_float(
            daily_vah,
            weekly_vah,
            price + _fallback_target_distance(price, atr, direction_value),
        )

        if entry is None or invalidation_anchor is None or target is None:
            return ExecutionPlan(
                status="INCOMPLETE",
                model="LIMIT_ON_RETEST",
                entry_reference_price=_round_price(entry),
                invalidation_reference_price=_round_price(invalidation_anchor),
                target_reference_price=_round_price(target),
                execution_timeframe="15m",
                trigger_reason="sweep_return_structure_incomplete",
            )

        invalidation = invalidation_anchor - _buffer(price, atr)

        return _finalize_plan(
            entry=entry,
            invalidation=invalidation,
            target=target,
            model="LIMIT_ON_RETEST",
            direction=direction_value,
            trigger_reason="sweep_return_long",
        )

    return ExecutionPlan(status="INCOMPLETE", model="LIMIT_ON_RETEST")


# ============================================================================
# TREND CONTINUATION
# ============================================================================

def _build_trend_continuation(context: Any, direction: Any, evidence: Any) -> ExecutionPlan:
    price = _get_price(context)
    if price is None:
        return ExecutionPlan(status="INCOMPLETE", model="LIMIT_ON_RETEST")

    atr = _extract_atr(context)
    direction_value = getattr(direction, "value", direction)

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
            price + _fallback_target_distance(price, atr, direction_value),
        )

        if entry is None or invalidation_anchor is None or target is None:
            return ExecutionPlan(
                status="INCOMPLETE",
                model="LIMIT_ON_RETEST",
                entry_reference_price=_round_price(entry),
                invalidation_reference_price=_round_price(invalidation_anchor),
                target_reference_price=_round_price(target),
                execution_timeframe="15m",
                trigger_reason="trend_continuation_structure_incomplete",
            )

        invalidation = invalidation_anchor - _buffer(price, atr)

        return _finalize_plan(
            entry=entry,
            invalidation=invalidation,
            target=target,
            model="LIMIT_ON_RETEST",
            direction=direction_value,
            trigger_reason="trend_continuation_long",
        )

    if direction_value == "SHORT":
        entry = _first_float(price, daily_poc)
        invalidation_anchor = _first_float(pullback_high, daily_vah)
        target = _first_float(
            daily_val,
            price - _fallback_target_distance(price, atr, direction_value),
        )

        if entry is None or invalidation_anchor is None or target is None:
            return ExecutionPlan(
                status="INCOMPLETE",
                model="LIMIT_ON_RETEST",
                entry_reference_price=_round_price(entry),
                invalidation_reference_price=_round_price(invalidation_anchor),
                target_reference_price=_round_price(target),
                execution_timeframe="15m",
                trigger_reason="trend_continuation_structure_incomplete",
            )

        invalidation = invalidation_anchor + _buffer(price, atr)

        return _finalize_plan(
            entry=entry,
            invalidation=invalidation,
            target=target,
            model="LIMIT_ON_RETEST",
            direction=direction_value,
            trigger_reason="trend_continuation_short",
        )

    return ExecutionPlan(status="INCOMPLETE", model="LIMIT_ON_RETEST")


# ============================================================================
# FINALIZATION
# ============================================================================

def _finalize_plan(
    entry: float,
    invalidation: float,
    target: float,
    model: str,
    direction: str,
    trigger_reason: str,
) -> ExecutionPlan:
    if None in (entry, invalidation, target):
        return ExecutionPlan(status="INCOMPLETE", model=model)

    if direction == "SHORT":
        if not (invalidation > entry > target):
            return ExecutionPlan(
                status="INCOMPLETE",
                model=model,
                entry_reference_price=_round_price(entry),
                invalidation_reference_price=_round_price(invalidation),
                target_reference_price=_round_price(target),
                execution_timeframe="15m",
                trigger_reason=f"{trigger_reason}_invalid_geometry",
            )

    elif direction == "LONG":
        if not (target > entry > invalidation):
            return ExecutionPlan(
                status="INCOMPLETE",
                model=model,
                entry_reference_price=_round_price(entry),
                invalidation_reference_price=_round_price(invalidation),
                target_reference_price=_round_price(target),
                execution_timeframe="15m",
                trigger_reason=f"{trigger_reason}_invalid_geometry",
            )

    stop_distance = abs(entry - invalidation)
    target_distance = abs(target - entry)

    if stop_distance <= 0 or target_distance <= 0:
        return ExecutionPlan(status="INCOMPLETE", model=model)

    rr = target_distance / stop_distance

    return ExecutionPlan(
        status="EXECUTABLE",
        model=model,
        entry_reference_price=_round_price(entry),
        invalidation_reference_price=_round_price(invalidation),
        target_reference_price=_round_price(target),
        risk_reward_ratio=round(rr, 2),
        stop_distance=_round_price(stop_distance),
        target_distance=_round_price(target_distance),
        execution_timeframe="15m",
        trigger_reason=trigger_reason,
    )


# ============================================================================
# HELPERS
# ============================================================================

def _extract_atr(context: Any) -> float | None:
    atr = _first_float(
        getattr(context, "atr", None),
        getattr(getattr(context, "volatility", None), "atr", None),
        getattr(getattr(context, "stats", None), "atr", None),
    )
    return atr


def _buffer(price: float | None, atr: float | None) -> float:
    if atr is not None and atr > 0:
        return atr * 0.10

    if price is None:
        return 0.0005

    if price > 10000:
        return 25.0      # BTC-style fallback
    if price > 1000:
        return 2.5       # XAU-style fallback
    if price > 50:
        return 0.5
    return 0.0005        # FX fallback


def _fallback_target_distance(price: float, atr: float | None, direction: str) -> float:
    if atr is not None and atr > 0:
        return atr * 2.0

    if price > 10000:
        return 250.0
    if price > 1000:
        return 15.0
    if price > 50:
        return 2.0
    return 0.005


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