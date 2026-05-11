from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class SignalQualityDecision(str, Enum):
    PASS = "PASS"
    BLOCK = "BLOCK"


@dataclass(frozen=True)
class SignalQualityResult:
    decision: SignalQualityDecision
    is_telegram_allowed: bool
    reason: str
    score: int


# Internal probability scale is normalized to 0.0 - 1.0.
# Accepts both 0.8 and 80 as 80%.
MIN_READY_PROBABILITY = 0.60

# Minimum RR for Telegram trade alerts.
MIN_EXECUTABLE_RR = 2.0


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_present(*values: Any) -> Any:
    """
    Return first value that is not None and not an empty string.

    Important:
    - 0 and 0.0 are valid values and must not be skipped.
    """
    for value in values:
        if value is None:
            continue
        if value == "":
            continue
        return value
    return None


def _normalize_probability(value: Any) -> float | None:
    """
    Normalize probability/confidence to 0.0 - 1.0.

    Supported inputs:
    - 0.8  -> 0.8
    - 80   -> 0.8
    - 100  -> 1.0
    - 0    -> 0.0

    Values above 1.0 are treated as percentages.
    """
    probability = _safe_float(value)

    if probability is None:
        return None

    if probability < 0:
        return None

    if probability > 1.0:
        probability = probability / 100.0

    if probability > 1.0:
        return None

    return probability


def _format_probability(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.0f}%"


def evaluate_signal_quality(payload: dict[str, Any]) -> SignalQualityResult:
    if not isinstance(payload, dict):
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason="blocked: payload is not a dict",
            score=0,
        )

    signal_class = str(
        _first_present(
            payload.get("signal_class"),
            payload.get("stage"),
            payload.get("alert"),
        )
        or ""
    ).upper()

    execution_status = str(payload.get("execution_status") or "").upper()
    direction = str(payload.get("direction") or "").upper()

    probability = _normalize_probability(
        _first_present(
            payload.get("probability"),
            payload.get("confidence"),
            payload.get("scenario_probability"),
        )
    )

    entry = _safe_float(
        _first_present(
            payload.get("entry"),
            payload.get("entry_reference_price"),
        )
    )

    stop_loss = _safe_float(
        _first_present(
            payload.get("stop_loss"),
            payload.get("sl"),
            payload.get("stop"),
            payload.get("invalidation_reference_price"),
        )
    )

    take_profit = _safe_float(
        _first_present(
            payload.get("take_profit"),
            payload.get("tp"),
            payload.get("target"),
            payload.get("target_reference_price"),
        )
    )

    rr = _safe_float(
        _first_present(
            payload.get("rr"),
            payload.get("risk_reward_ratio"),
            payload.get("risk_reward"),
        )
    )

    if signal_class != "READY":
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason=f"blocked: signal_class is {signal_class or '-'}, expected READY",
            score=20,
        )

    if execution_status != "EXECUTABLE":
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason=f"blocked: execution_status is {execution_status or '-'}, expected EXECUTABLE",
            score=25,
        )

    if direction not in {"LONG", "SHORT"}:
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason=f"blocked: invalid direction {direction or '-'}",
            score=30,
        )

    if probability is None:
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason="blocked: missing probability/confidence",
            score=40,
        )

    if probability < MIN_READY_PROBABILITY:
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason=(
                f"blocked: probability {_format_probability(probability)} "
                f"below {_format_probability(MIN_READY_PROBABILITY)}"
            ),
            score=40,
        )

    if entry is None or stop_loss is None or take_profit is None:
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason="blocked: missing entry / stop_loss / take_profit",
            score=45,
        )

    if rr is None:
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason="blocked: missing RR",
            score=50,
        )

    if rr < MIN_EXECUTABLE_RR:
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason=f"blocked: RR {rr:.2f} below {MIN_EXECUTABLE_RR:.2f}",
            score=60,
        )

    if direction == "LONG" and not (stop_loss < entry < take_profit):
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason="blocked: invalid LONG geometry",
            score=65,
        )

    if direction == "SHORT" and not (take_profit < entry < stop_loss):
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason="blocked: invalid SHORT geometry",
            score=65,
        )

    return SignalQualityResult(
        decision=SignalQualityDecision.PASS,
        is_telegram_allowed=True,
        reason=(
            "passed: READY + EXECUTABLE + valid geometry + "
            f"RR {rr:.2f} + probability {_format_probability(probability)}"
        ),
        score=100,
    )


def enrich_payload_with_quality(payload: dict[str, Any]) -> dict[str, Any]:
    result = evaluate_signal_quality(payload)

    enriched = dict(payload)
    enriched["signal_quality_decision"] = result.decision.value
    enriched["signal_quality_score"] = result.score
    enriched["signal_quality_reason"] = result.reason
    enriched["telegram_allowed"] = result.is_telegram_allowed

    return enriched