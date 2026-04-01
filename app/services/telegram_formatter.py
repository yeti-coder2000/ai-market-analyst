from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class FormattedTelegramMessage:
    message_type: str
    title: str
    body: str
    signal_id: Optional[str] = None
    symbol: Optional[str] = None
    stage: Optional[str] = None

    def render(self) -> str:
        return f"{self.title}\n\n{self.body}".strip()


def _safe_str(value: Any, default: str = "-") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _format_price(value: Any) -> str:
    if value is None:
        return "не задано"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "не задано"

    if abs(num) >= 1000:
        return f"{num:,.2f}".replace(",", " ")
    if abs(num) >= 10:
        return f"{num:.2f}"
    return f"{num:.5f}"


def _format_pct(value: Any) -> str:
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return "0.00%"


def _format_rr(value: Any) -> str:
    try:
        rr = float(value)
    except (TypeError, ValueError):
        return "не задано"
    return f"1:{rr:.2f}"


def _extract_stage(signal_payload: dict) -> str:
    stage = _safe_str(signal_payload.get("stage") or signal_payload.get("signal_class"), "")
    execution_status = _safe_str(signal_payload.get("execution_status"), "")
    status = _safe_str(signal_payload.get("status"), "")

    if stage == "READY" and execution_status != "EXECUTABLE":
        return "WATCH"

    if status == "RESOLVED":
        return "RESOLVED"

    return stage or "SCENARIO_FORMING"


def _extract_confidence(signal_payload: dict) -> float:
    confidence = _safe_float(signal_payload.get("confidence"), None)
    if confidence is not None:
        return float(confidence)

    probability = _safe_float(signal_payload.get("probability"), 0.0)
    if probability is None:
        return 0.0
    return float(probability)


def humanize_scenario_name(scenario: str) -> str:
    mapping = {
        "TREND_CONTINUATION_SHORT": "Trend Continuation Short",
        "TREND_CONTINUATION_LONG": "Trend Continuation Long",
        "SWEEP_RETURN_LONG": "Sweep Return Long",
        "SWEEP_RETURN_SHORT": "Sweep Return Short",
        "BALANCE_ROTATION": "Balance Rotation",
        "TRANSITION_EXPANSION": "Transition Expansion",
        "NO_ACTION": "No Action",
        "MARKET_CLOSED": "Market Closed",
    }
    return mapping.get(
        _safe_str(scenario, ""),
        _safe_str(scenario, "Unknown Scenario").replace("_", " ").title(),
    )


def humanize_stage(stage: str) -> str:
    mapping = {
        "NO_ACTION": "⚪ NO ACTION",
        "BIAS_ONLY": "🔵 BIAS",
        "SCENARIO_FORMING": "🟡 FORMING",
        "WATCH": "🟠 WATCH",
        "READY": "🔴 READY",
        "ACTIVE": "🚨 ACTIVE",
        "RESOLVED": "✅ RESOLVED",
    }
    return mapping.get(_safe_str(stage, ""), f"ℹ️ {_safe_str(stage, 'UNKNOWN')}")


def humanize_direction(direction: str) -> str:
    mapping = {
        "LONG": "LONG",
        "SHORT": "SHORT",
        "NEUTRAL": "NEUTRAL",
    }
    return mapping.get(_safe_str(direction, "NEUTRAL"), _safe_str(direction, "NEUTRAL"))


def humanize_market_state(state: str) -> str:
    mapping = {
        "TREND": "Trend",
        "TRANSITION": "Transition",
        "BALANCE": "Balance",
        "UNKNOWN": "Unknown",
    }
    return mapping.get(_safe_str(state, "UNKNOWN"), _safe_str(state, "Unknown"))


def humanize_bias(bias: str) -> str:
    mapping = {
        "LONG": "Bullish",
        "SHORT": "Bearish",
        "NEUTRAL": "Neutral",
    }
    return mapping.get(_safe_str(bias, "NEUTRAL"), _safe_str(bias, "Neutral"))


def humanize_execution_model(model: str) -> str:
    mapping = {
        "LIMIT_ON_RETEST": "Limit on Retest",
        "STOP_ON_CONFIRMATION": "Stop on Confirmation",
        "MARKET": "Market",
        "ZONE_RETEST": "Zone Retest",
        "NONE": "None",
    }
    return mapping.get(_safe_str(model, "NONE"), _safe_str(model, "None").replace("_", " ").title())


def build_reason_text(signal_payload: dict) -> str:
    scenario = _safe_str(signal_payload.get("scenario"), "")
    rationale = _safe_str(signal_payload.get("rationale"), "")
    reason = _safe_str(signal_payload.get("reason"), "")
    market_state = _safe_str(signal_payload.get("market_state"), "")
    htf_bias = _safe_str(signal_payload.get("htf_bias"), "")

    if rationale not in {"-", ""}:
        return rationale

    if scenario == "TREND_CONTINUATION_SHORT":
        return "Є ведмежий контекст. Ринок у фазі continuation, але без підтвердженої структури входу."

    if scenario == "TREND_CONTINUATION_LONG":
        return "Є бичачий контекст. Ринок у фазі continuation, але без підтвердженої структури входу."

    if scenario == "SWEEP_RETURN_LONG":
        return "Ліквідність знизу зібрана. Є повернення у value і готовність до long-сценарію."

    if scenario == "SWEEP_RETURN_SHORT":
        return "Ліквідність зверху зібрана. Є повернення у value і готовність до short-сценарію."

    if reason not in {"-", ""}:
        return reason

    return (
        f"Сформовано сценарій у контексті {humanize_market_state(market_state)} "
        f"при HTF bias: {humanize_bias(htf_bias)}."
    )


def build_missing_conditions_text(signal_payload: dict) -> str:
    missing = signal_payload.get("missing_conditions") or []
    if not missing:
        return "нічого критичного не бракує"

    human_map = {
        "impulse": "імпульсу",
        "pullback": "пулбеку",
        "entry_trigger": "тригера входу",
        "return_to_value": "повернення у value",
        "sweep": "sweep-події",
        "structure_confirmation": "підтвердження структури",
        "htf_alignment": "узгодження з HTF",
        "market_state": "відповідного market state",
        "execution_plan": "готового execution plan",
        "execution_plan_incomplete": "повного execution plan",
    }

    parts = [human_map.get(str(item), str(item)) for item in missing]
    return "\n".join(f"- {part}" for part in parts)


def build_action_text(signal_payload: dict) -> str:
    stage = _extract_stage(signal_payload)
    direction = humanize_direction(signal_payload.get("direction"))
    scenario = _safe_str(signal_payload.get("scenario"), "")
    execution_status = _safe_str(signal_payload.get("execution_status"), "")
    execution_model = humanize_execution_model(signal_payload.get("execution_model"))
    trigger_reason = _safe_str(signal_payload.get("trigger_reason"), "")

    if stage == "SCENARIO_FORMING":
        return "Спостерігати. Без входу. Чекати підтвердження структури."

    if stage == "WATCH":
        if execution_status == "INCOMPLETE":
            return "Сценарій є, але execution ще не зібраний повністю. Без входу."
        if scenario.startswith("TREND_CONTINUATION"):
            return f"Шукати continuation у бік {direction} після імпульсу і structure hold."
        if scenario.startswith("SWEEP_RETURN"):
            return f"Шукати підтвердження входу в бік {direction} після рейду і повернення у value."
        return "Тримати фокус на підтвердженні сценарію."

    if stage == "READY":
        if trigger_reason not in {"", "-"}:
            return (
                f"Сценарій готовий. Execution model: {execution_model}. "
                f"Тригер: {trigger_reason}."
            )
        return f"Сценарій готовий. Execution model: {execution_model}. У фокусі вхід у бік {direction}."

    if stage == "ACTIVE":
        return f"Ідея активна. Супровід у бік {direction}."

    if stage == "RESOLVED":
        return "Сигнал завершений. Потрібен розбір результату."

    return "Спостерігати за розвитком структури."


def build_levels_text(signal_payload: dict) -> str:
    invalidation = signal_payload.get("invalidation_reference_price")
    target = signal_payload.get("target_reference_price")
    entry = signal_payload.get("entry_reference_price")
    rr = signal_payload.get("risk_reward_ratio")
    execution_model = signal_payload.get("execution_model")
    execution_status = signal_payload.get("execution_status")
    execution_timeframe = signal_payload.get("execution_timeframe")

    lines = [
        f"Execution: {humanize_execution_model(_safe_str(execution_model, 'NONE'))}",
        f"Execution status: {_safe_str(execution_status, 'NOT_EXECUTABLE')}",
        f"Entry: {_format_price(entry)}",
        f"Invalidation: {_format_price(invalidation)}",
        f"Target: {_format_price(target)}",
        f"RR: {_format_rr(rr)}",
    ]

    if execution_timeframe not in {"", "-"}:
        lines.append(f"TF execution: {_safe_str(execution_timeframe)}")

    return "\n".join(lines)


def format_signal_message(signal_payload: dict) -> FormattedTelegramMessage:
    stage = _extract_stage(signal_payload)
    symbol = _safe_str(signal_payload.get("symbol"), "-")
    direction = humanize_direction(signal_payload.get("direction"))
    scenario = humanize_scenario_name(signal_payload.get("scenario"))
    confidence = _extract_confidence(signal_payload)
    market_state = humanize_market_state(signal_payload.get("market_state"))
    htf_bias = humanize_bias(signal_payload.get("htf_bias"))
    signal_id = signal_payload.get("signal_id")
    execution_status = _safe_str(signal_payload.get("execution_status"), "NOT_EXECUTABLE")

    title = f"{humanize_stage(stage)} | {symbol} | {direction}"

    body_parts = [
        f"Сценарій: {scenario}",
        (
            f"Ринок: {market_state} | HTF: {htf_bias} | "
            f"Сила: {_format_pct(confidence * 100 if confidence <= 1 else confidence)}"
        ),
        "",
        "Картина:",
        build_reason_text(signal_payload),
        "",
        "Що бракує:" if signal_payload.get("missing_conditions") else "Фокус:",
        (
            build_missing_conditions_text(signal_payload)
            if signal_payload.get("missing_conditions")
            else build_action_text(signal_payload)
        ),
        "",
        "План:",
        build_action_text(signal_payload),
        "",
        build_levels_text(signal_payload),
    ]

    if stage == "WATCH" and execution_status != "EXECUTABLE":
        body_parts.extend([
            "",
            "Коментар:",
            "READY ще не підтверджено, бо execution-план неповний.",
        ])

    if signal_id:
        body_parts.extend(["", f"ID: {signal_id}"])

    return FormattedTelegramMessage(
        message_type="signal_alert",
        title=title,
        body="\n".join(body_parts).strip(),
        signal_id=signal_id,
        symbol=symbol,
        stage=stage,
    )


def format_resolution_message(signal_payload: dict, resolution_payload: dict) -> FormattedTelegramMessage:
    symbol = _safe_str(signal_payload.get("symbol"), "-")
    scenario = humanize_scenario_name(signal_payload.get("scenario"))
    final_status = _safe_str(
        resolution_payload.get("final_status") or resolution_payload.get("resolution"),
        "UNKNOWN",
    )
    mfe_pct = resolution_payload.get("mfe_pct")
    mae_pct = resolution_payload.get("mae_pct")
    ttv = resolution_payload.get("time_to_validation_min")

    title = f"✅ RESOLVED | {symbol} | {final_status}"

    body_parts = [
        f"Сценарій: {scenario}",
        f"Причина: {_safe_str(resolution_payload.get('resolution_reason') or resolution_payload.get('resolution_note'), '-')}",
        "",
        f"MFE: {_format_pct(mfe_pct)}",
        f"MAE: {_format_pct(mae_pct)}",
        f"Час до підтвердження: {ttv if ttv is not None else '-'} хв",
    ]

    return FormattedTelegramMessage(
        message_type="signal_resolution",
        title=title,
        body="\n".join(body_parts).strip(),
        signal_id=signal_payload.get("signal_id"),
        symbol=symbol,
        stage="RESOLVED",
    )


def format_cycle_summary_message(summary_payload: dict) -> FormattedTelegramMessage:
    title = "📊 CYCLE SUMMARY"

    system_metrics = summary_payload.get("system_metrics", {}) or {}
    signal_metrics = summary_payload.get("signal_metrics", {}) or {}

    body_parts = [
        f"Cycles total: {system_metrics.get('cycles_total', 0)}",
        f"Cycles OK: {system_metrics.get('cycles_ok', 0)}",
        f"Cycle errors: {system_metrics.get('cycles_with_errors', 0)}",
        f"Signals total: {signal_metrics.get('signals_total', 0)}",
        f"Executable: {signal_metrics.get('executable_signals', 0)}",
        f"Validated: {signal_metrics.get('validated_total', 0)}",
        f"Invalidated: {signal_metrics.get('invalidated_total', 0)}",
        f"Expired: {signal_metrics.get('expired_total', 0)}",
        f"Avg confidence: {_format_pct(signal_metrics.get('avg_confidence', 0.0) * 100 if signal_metrics.get('avg_confidence', 0.0) <= 1 else signal_metrics.get('avg_confidence', 0.0))}",
        f"Avg RR: {_format_rr(signal_metrics.get('avg_rr'))}",
        f"Avg MFE: {_format_pct(signal_metrics.get('avg_mfe_pct', 0.0))}",
        f"Avg MAE: {_format_pct(signal_metrics.get('avg_mae_pct', 0.0))}",
    ]

    return FormattedTelegramMessage(
        message_type="cycle_summary",
        title=title,
        body="\n".join(body_parts).strip(),
        signal_id=None,
        symbol=None,
        stage=None,
    )