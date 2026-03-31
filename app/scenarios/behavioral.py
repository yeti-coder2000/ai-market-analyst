from __future__ import annotations

from typing import Any


def safe_attr(obj: Any, name: str) -> Any:
    if obj is None:
        return None

    if isinstance(obj, dict):
        return obj.get(name)

    return getattr(obj, name, None)


def enum_value(value: Any) -> Any:
    return getattr(value, "value", value)


def extract_context_price(context: Any) -> Any:
    if isinstance(context, dict):
        return context.get("price") or context.get("current_price")
    return safe_attr(context, "current_price") or safe_attr(context, "price")


def extract_context_market_state(context: Any) -> Any:
    if isinstance(context, dict):
        value = context.get("market_state")
        return enum_value(value)

    value = safe_attr(context, "market_state")
    return enum_value(value)


def extract_context_htf_bias(context: Any) -> Any:
    if isinstance(context, dict):
        htf_bias = context.get("htf_bias")
        if isinstance(htf_bias, dict):
            return enum_value(htf_bias.get("bias"))
        return enum_value(htf_bias)

    htf_bias = safe_attr(context, "htf_bias")
    if htf_bias is None:
        return None

    bias = safe_attr(htf_bias, "bias")
    return enum_value(bias)


def classify_market_phase(context: Any) -> str:
    """
    Legacy-compatible phase classification.

    This stays available as fallback when Scenario Engine is absent
    or failed, but it lives in scenarios/ instead of runner.
    """
    impulse = safe_attr(context, "impulse")
    pullback = safe_attr(context, "pullback")
    sweep = safe_attr(context, "sweep")

    impulse_detected = bool(safe_attr(impulse, "detected"))
    pullback_detected = bool(safe_attr(pullback, "detected"))
    sweep_detected = bool(safe_attr(sweep, "detected"))
    returned_to_value = bool(safe_attr(sweep, "returned_to_value"))

    if sweep_detected and not returned_to_value:
        return "SWEEP_PHASE"
    if sweep_detected and returned_to_value:
        return "RETURN_TO_VALUE_PHASE"
    if impulse_detected and not pullback_detected:
        return "IMPULSE_PHASE"
    if pullback_detected:
        return "PULLBACK_PHASE"
    return "NO_STRUCTURE"


def infer_missing_conditions(setups: list[Any]) -> list[str]:
    missing: list[str] = []

    for setup in setups:
        diagnostics = safe_attr(setup, "diagnostics")
        failed = safe_attr(diagnostics, "failed_conditions") or []

        for item in failed:
            name = safe_attr(item, "name")
            if name and str(name) not in missing:
                missing.append(str(name))

    return missing


def infer_alignment_score(context: Any, setups: list[Any]) -> float:
    """
    Legacy/fallback alignment score.

    Scenario Engine has its own scoring logic.
    This function remains useful for fallback mode and debugging.
    """
    score = 0.0

    htf_bias = extract_context_htf_bias(context)
    market_state = extract_context_market_state(context)
    phase = classify_market_phase(context)

    if htf_bias and htf_bias != "NEUTRAL":
        score += 0.25

    if market_state in {"TREND", "TRANSITION", "BALANCE"}:
        score += 0.15

    if phase != "NO_STRUCTURE":
        score += 0.20

    impulse = safe_attr(context, "impulse")
    pullback = safe_attr(context, "pullback")
    sweep = safe_attr(context, "sweep")

    if bool(safe_attr(impulse, "detected")):
        score += 0.10

    if bool(safe_attr(pullback, "detected")):
        score += 0.10

    if bool(safe_attr(sweep, "detected")):
        score += 0.10

    if bool(safe_attr(sweep, "returned_to_value")):
        score += 0.05

    for setup in setups:
        status = enum_value(safe_attr(setup, "status"))
        if status == "WATCH":
            score += 0.05
        elif status == "READY":
            score += 0.10

    return round(min(score, 1.0), 2)


def infer_next_expected_event(context: Any, setups: list[Any] | None = None) -> str | None:
    """
    Fallback next-event predictor for legacy journaling / no-scenario mode.
    """
    del setups  # reserved for future refinement

    htf_bias = extract_context_htf_bias(context)
    phase = classify_market_phase(context)

    if phase == "NO_STRUCTURE":
        if htf_bias == "SHORT":
            return "liquidity_sweep_high_or_bearish_impulse"
        if htf_bias == "LONG":
            return "liquidity_sweep_low_or_bullish_impulse"
        return "structure_confirmation"

    if phase == "SWEEP_PHASE":
        return "return_to_value_confirmation"

    if phase == "RETURN_TO_VALUE_PHASE":
        return "entry_trigger"

    if phase == "IMPULSE_PHASE":
        return "pullback_confirmation"

    if phase == "PULLBACK_PHASE":
        return "continuation_trigger"

    return None


def infer_behavioral_scenario(context: Any, setups: list[Any]) -> str:
    """
    Legacy scenario labeler used as fallback if Scenario Engine is absent.

    Scenario Engine should be primary source of truth.
    """
    htf_bias = extract_context_htf_bias(context)
    market_state = extract_context_market_state(context)
    phase = classify_market_phase(context)

    if market_state == "TRANSITION" and htf_bias == "SHORT" and phase == "NO_STRUCTURE":
        return "PRE_SWEEP_RETURN_SHORT"

    if market_state == "TRANSITION" and htf_bias == "LONG" and phase == "NO_STRUCTURE":
        return "PRE_SWEEP_RETURN_LONG"

    if market_state == "TREND" and htf_bias == "SHORT":
        return "PRE_CONTINUATION_SHORT"

    if market_state == "TREND" and htf_bias == "LONG":
        return "PRE_CONTINUATION_LONG"

    if market_state == "BALANCE" and phase == "NO_STRUCTURE":
        return "BALANCE_ROTATION_CANDIDATE"

    if phase == "SWEEP_PHASE":
        return "SWEEP_IN_PROGRESS"

    if phase == "RETURN_TO_VALUE_PHASE":
        return "RETURN_TO_VALUE_IN_PROGRESS"

    if phase == "IMPULSE_PHASE":
        return "IMPULSE_IN_PROGRESS"

    if phase == "PULLBACK_PHASE":
        return "PULLBACK_IN_PROGRESS"

    if infer_missing_conditions(setups):
        return "UNCONFIRMED_STRUCTURE"

    return "UNCLASSIFIED"