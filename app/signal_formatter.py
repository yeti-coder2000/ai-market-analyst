from __future__ import annotations

from typing import List

from app.context.schema import (
    Direction,
    MarketContext,
    SetupStatus,
    SetupAResult,
    SetupBResult,
)


def _price_fmt(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


def build_context_summary(context: MarketContext) -> List[str]:
    summary: List[str] = []

    # HTF bias
    if context.htf_bias.bias == Direction.LONG:
        summary.append("Старший контекст лонговий")
    elif context.htf_bias.bias == Direction.SHORT:
        summary.append("Старший контекст шортовий")
    else:
        summary.append("Старший контекст нейтральний")

    # market state
    if context.market_state.value == "TREND":
        summary.append("Ринок у трендовому стані")
    elif context.market_state.value == "BALANCE":
        summary.append("Ринок у балансі")
    else:
        summary.append("Ринок у перехідному стані")

    # acceptance
    if context.acceptance.accepted_below:
        summary.append("Є acceptance нижче Weekly VAL")
    if context.acceptance.accepted_above:
        summary.append("Є acceptance вище Weekly VAH")
    if context.acceptance.no_acceptance_above:
        summary.append("Є відмова від вищих цін")
    if context.acceptance.no_acceptance_below:
        summary.append("Є відмова від нижчих цін")

    # structure
    if context.structure_4h.hh_hl_structure:
        summary.append("4H структура підтримує continuation вгору")
    if context.structure_4h.ll_lh_structure:
        summary.append("4H структура підтримує continuation вниз")

    # levels relation
    if context.current_price < context.profile.weekly.val:
        summary.append("Ціна нижче Weekly Value")
    elif context.current_price > context.profile.weekly.vah:
        summary.append("Ціна вище Weekly Value")
    else:
        summary.append("Ціна всередині Weekly Value")

    return summary


def build_arguments_for(context: MarketContext, result: SetupAResult | SetupBResult) -> List[str]:
    args: List[str] = []

    if context.htf_bias.bias == result.direction and result.direction != Direction.NEUTRAL:
        args.append("Напрям сетапу збігається зі старшим контекстом")

    if context.market_state.value == "TREND" and result.setup_type.value == "IMPULSE_PULLBACK_CONTINUATION":
        args.append("Стан ринку підходить для continuation-сценарію")

    if context.market_state.value in {"BALANCE", "TRANSITION"} and result.setup_type.value == "SWEEP_RETURN_TO_VALUE":
        args.append("Стан ринку підходить для sweep/return сценарію")

    if context.impulse.detected:
        args.append(
            f"Є імпульс у напрямку {context.impulse.direction.value.lower()} "
            f"(ATR x {context.impulse.range_atr_multiple:.2f})"
        )

    if context.pullback.detected and context.pullback.held_structure:
        args.append("Корекція не зламала структуру")

    if context.sweep.detected and context.sweep.returned_to_value:
        args.append("Є sweep і повернення у value")

    if context.acceptance.accepted_below and result.direction == Direction.SHORT:
        args.append("Нижчі ціни прийняті ринком")
    if context.acceptance.accepted_above and result.direction == Direction.LONG:
        args.append("Вищі ціни прийняті ринком")

    if result.entry_plan:
        args.append(
            f"Є сформований план входу: {_price_fmt(result.entry_plan.entry_min)} - "
            f"{_price_fmt(result.entry_plan.entry_max)}"
        )

    return args


def build_arguments_against(context: MarketContext, result: SetupAResult | SetupBResult) -> List[str]:
    args: List[str] = []

    if context.htf_bias.bias == Direction.NEUTRAL:
        args.append("Старший контекст нейтральний")

    if result.status == SetupStatus.WATCH:
        args.append("Сетап ще не дозрів до входу")

    if result.status == SetupStatus.IDLE:
        args.append("Ринковий стан не дозволяє цей тип сетапу")

    if not context.impulse.detected and result.setup_type.value == "IMPULSE_PULLBACK_CONTINUATION":
        args.append("Немає підтвердженого імпульсу за правилами плейбука")

    if context.impulse.detected and context.impulse.body_ratio < 0.55:
        args.append("Імпульс слабкий по якості тіл свічок")

    if context.pullback.detected and not context.pullback.held_structure:
        args.append("Корекція ризикує зламати структуру")

    if result.entry_plan is None:
        args.append("Немає готового плану входу")

    if context.current_price < context.profile.weekly.val and result.direction == Direction.LONG:
        args.append("Лонг проти acceptance нижче value")
    if context.current_price > context.profile.weekly.vah and result.direction == Direction.SHORT:
        args.append("Шорт проти acceptance вище value")

    return args


def build_invalidation_reasons(context: MarketContext, result: SetupAResult | SetupBResult) -> List[str]:
    reasons: List[str] = []

    if result.direction == Direction.SHORT:
        reasons.append("Ідея зламається при поверненні та acceptance вище ключового опору")
        reasons.append(f"Небезпечно, якщо ціна повернеться вище Weekly VAL {_price_fmt(context.profile.weekly.val)}")
    elif result.direction == Direction.LONG:
        reasons.append("Ідея зламається при acceptance нижче ключової підтримки")
        reasons.append(f"Небезпечно, якщо ціна піде нижче Weekly VAL {_price_fmt(context.profile.weekly.val)}")
    else:
        reasons.append("Немає активної ідеї — інвалідація не застосовується")

    if result.entry_plan:
        reasons.append(f"Базова інвалідація — стоп за {_price_fmt(result.entry_plan.stop_price)}")

    return reasons


def build_next_step(context: MarketContext, result: SetupAResult | SetupBResult) -> str:
    if result.status == SetupStatus.READY:
        return "Сетап готовий. Можна оцінювати вхід по плану."

    if result.status == SetupStatus.WATCH:
        if result.setup_type.value == "IMPULSE_PULLBACK_CONTINUATION":
            return "Чекаємо підтверджений імпульс і continuation-підтвердження."
        return "Чекаємо sweep та повернення у value."

    if result.status == SetupStatus.IMPULSE_FOUND:
        return "Імпульс уже є. Чекаємо корекцію, яка не зламає структуру."

    if result.status == SetupStatus.PULLBACK_IN_PROGRESS:
        return "Корекція в роботі. Чекаємо BOS / reclaim / retest для continuation."

    if result.status == SetupStatus.SWEEP_DETECTED:
        return "Sweep уже є. Чекаємо повернення у value і підтвердження."

    if result.status == SetupStatus.RETURNING_TO_VALUE:
        return "Ціна повертається у value. Чекаємо підтвердження структури."

    if result.status == SetupStatus.INVALIDATED:
        return "Сценарій зламаний. Не працюємо цю ідею."

    return "Поки що спостерігаємо, без активних дій."


def enrich_setup_result(
    context: MarketContext,
    result: SetupAResult | SetupBResult,
) -> SetupAResult | SetupBResult:
    result.context_summary = build_context_summary(context)
    result.diagnostics.arguments_for = build_arguments_for(context, result)
    result.diagnostics.arguments_against = build_arguments_against(context, result)
    result.diagnostics.invalidation_reasons = build_invalidation_reasons(context, result)
    result.context_summary.append(f"Наступний крок: {build_next_step(context, result)}")
    return result