from dataclasses import dataclass
from typing import Optional


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


# ------------------------------------------------------------


def build_execution_plan(context, scenario_type, direction, evidence) -> ExecutionPlan:
    try:
        if "SWEEP_RETURN" in scenario_type.value:
            return _build_sweep_return(context, direction, evidence)

        if "TREND_CONTINUATION" in scenario_type.value:
            return _build_trend_continuation(context, direction, evidence)

        return ExecutionPlan(status="NOT_EXECUTABLE", model="NONE")

    except Exception:
        return ExecutionPlan(status="INCOMPLETE", model="NONE")


# ------------------------------------------------------------


def _build_sweep_return(context, direction, evidence) -> ExecutionPlan:
    price = _get_price(context)
    atr = getattr(context, "atr", None)

    sweep = getattr(context, "sweep", None)

    if not sweep:
        return ExecutionPlan(status="INCOMPLETE", model="NONE")

    high = getattr(sweep, "high", None)
    low = getattr(sweep, "low", None)

    if direction.value == "SHORT":
        if high is None or low is None:
            return ExecutionPlan(status="INCOMPLETE", model="LIMIT_ON_RETEST")

        entry = price
        invalidation = high + _buffer(atr)
        target = low

    else:
        if high is None or low is None:
            return ExecutionPlan(status="INCOMPLETE", model="LIMIT_ON_RETEST")

        entry = price
        invalidation = low - _buffer(atr)
        target = high

    return _finalize_plan(entry, invalidation, target, "LIMIT_ON_RETEST")


# ------------------------------------------------------------


def _build_trend_continuation(context, direction, evidence) -> ExecutionPlan:
    price = _get_price(context)
    atr = getattr(context, "atr", None)

    swing_high = getattr(context, "swing_high", None)
    swing_low = getattr(context, "swing_low", None)

    if direction.value == "LONG":
        if swing_low is None:
            return ExecutionPlan(status="INCOMPLETE", model="LIMIT_ON_RETEST")

        entry = price
        invalidation = swing_low - _buffer(atr)
        target = price + (price - invalidation) * 2

    else:
        if swing_high is None:
            return ExecutionPlan(status="INCOMPLETE", model="LIMIT_ON_RETEST")

        entry = price
        invalidation = swing_high + _buffer(atr)
        target = price - (invalidation - price) * 2

    return _finalize_plan(entry, invalidation, target, "LIMIT_ON_RETEST")


# ------------------------------------------------------------


def _finalize_plan(entry, invalidation, target, model) -> ExecutionPlan:
    if None in (entry, invalidation, target):
        return ExecutionPlan(status="INCOMPLETE", model=model)

    stop_distance = abs(entry - invalidation)
    target_distance = abs(target - entry)

    if stop_distance == 0:
        return ExecutionPlan(status="INCOMPLETE", model=model)

    rr = target_distance / stop_distance

    return ExecutionPlan(
        status="EXECUTABLE",
        model=model,
        entry_reference_price=round(entry, 5),
        invalidation_reference_price=round(invalidation, 5),
        target_reference_price=round(target, 5),
        risk_reward_ratio=round(rr, 2),
        stop_distance=round(stop_distance, 5),
        target_distance=round(target_distance, 5),
        execution_timeframe="15m",
        trigger_reason="structure_based_entry",
    )


# ------------------------------------------------------------


def _buffer(atr):
    if atr:
        return atr * 0.1
    return 0.0005


def _get_price(context):
    return float(
        getattr(context, "current_price", None)
        or getattr(context, "price", 0)
    )