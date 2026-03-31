from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from schema import Direction, SetupStatus


@dataclass
class DistanceInfo:
    trigger_price: Optional[float]
    distance_points: Optional[float]
    distance_atr: Optional[float]
    comment: str


def _safe_div(a: float, b: float) -> Optional[float]:
    if b is None or b == 0:
        return None
    return a / b


def estimate_distance_to_trigger(context, result) -> DistanceInfo:
    """
    Оцінює, наскільки ринок близько до активації сетапу.

    Логіка MVP:
    - якщо entry_plan вже є → рахуємо дистанцію до entry zone
    - якщо Setup A у WATCH → як проксі використовуємо останній 15m pivot у напрямку continuation
    - якщо Setup B у WATCH → як проксі використовуємо Weekly VAL/VAH для потенційного sweep
    - якщо нічого адекватного немає → повертаємо коментар без дистанції
    """

    current_price = float(context.current_price)
    atr = float(context.atr_15m) if context.atr_15m else 0.0

    # 1) Якщо вже є entry plan — це найкраща оцінка
    if result.entry_plan:
        entry_min = float(result.entry_plan.entry_min)
        entry_max = float(result.entry_plan.entry_max)

        if current_price < entry_min:
            trigger_price = entry_min
            distance_points = entry_min - current_price
        elif current_price > entry_max:
            trigger_price = entry_max
            distance_points = current_price - entry_max
        else:
            trigger_price = current_price
            distance_points = 0.0

        return DistanceInfo(
            trigger_price=trigger_price,
            distance_points=distance_points,
            distance_atr=_safe_div(distance_points, atr),
            comment="Є готовий entry plan",
        )

    # 2) Setup A — continuation logic
    if result.setup_type.value == "IMPULSE_PULLBACK_CONTINUATION":
        if result.status == SetupStatus.WATCH:
            if result.direction == Direction.SHORT and context.structure_15m.last_pivot_low:
                trigger_price = float(context.structure_15m.last_pivot_low.price)
                distance_points = abs(current_price - trigger_price)
                return DistanceInfo(
                    trigger_price=trigger_price,
                    distance_points=distance_points,
                    distance_atr=_safe_div(distance_points, atr),
                    comment="Проксі-тригер: пробій / підтвердження нижче 15m swing low",
                )

            if result.direction == Direction.LONG and context.structure_15m.last_pivot_high:
                trigger_price = float(context.structure_15m.last_pivot_high.price)
                distance_points = abs(current_price - trigger_price)
                return DistanceInfo(
                    trigger_price=trigger_price,
                    distance_points=distance_points,
                    distance_atr=_safe_div(distance_points, atr),
                    comment="Проксі-тригер: пробій / підтвердження вище 15m swing high",
                )

        if result.status == SetupStatus.IMPULSE_FOUND:
            return DistanceInfo(
                trigger_price=None,
                distance_points=None,
                distance_atr=None,
                comment="Імпульс уже є, чекаємо корекцію",
            )

        if result.status == SetupStatus.PULLBACK_IN_PROGRESS:
            return DistanceInfo(
                trigger_price=None,
                distance_points=None,
                distance_atr=None,
                comment="Корекція в роботі, чекаємо BOS / reclaim",
            )

    # 3) Setup B — sweep logic
    if result.setup_type.value == "SWEEP_RETURN_TO_VALUE":
        if result.status == SetupStatus.WATCH:
            if context.htf_bias.bias == Direction.SHORT:
                trigger_price = float(context.profile.weekly.vah)
                distance_points = abs(trigger_price - current_price)
                return DistanceInfo(
                    trigger_price=trigger_price,
                    distance_points=distance_points,
                    distance_atr=_safe_div(distance_points, atr),
                    comment="Проксі-тригер: potential sweep вище Weekly VAH",
                )

            if context.htf_bias.bias == Direction.LONG:
                trigger_price = float(context.profile.weekly.val)
                distance_points = abs(current_price - trigger_price)
                return DistanceInfo(
                    trigger_price=trigger_price,
                    distance_points=distance_points,
                    distance_atr=_safe_div(distance_points, atr),
                    comment="Проксі-тригер: potential sweep нижче Weekly VAL",
                )

        if result.status == SetupStatus.SWEEP_DETECTED:
            return DistanceInfo(
                trigger_price=None,
                distance_points=None,
                distance_atr=None,
                comment="Sweep уже є, чекаємо return to value",
            )

        if result.status == SetupStatus.RETURNING_TO_VALUE:
            return DistanceInfo(
                trigger_price=None,
                distance_points=None,
                distance_atr=None,
                comment="Повернення у value вже в роботі",
            )

    return DistanceInfo(
        trigger_price=None,
        distance_points=None,
        distance_atr=None,
        comment="Немає надійної оцінки дистанції до тригера",
    )