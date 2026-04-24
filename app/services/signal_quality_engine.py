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


MIN_READY_PROBABILITY = 60
MIN_EXECUTABLE_RR = 2.0


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def evaluate_signal_quality(payload: dict[str, Any]) -> SignalQualityResult:
    if not isinstance(payload, dict):
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason="payload is not a dict",
            score=0,
        )

    signal_class = str(payload.get("signal_class") or payload.get("alert") or "").upper()
    execution_status = str(payload.get("execution_status") or "").upper()
    direction = str(payload.get("direction") or "").upper()

    probability = _safe_float(payload.get("probability"))
    entry = _safe_float(payload.get("entry") or payload.get("entry_reference_price"))
    stop_loss = _safe_float(
        payload.get("stop_loss")
        or payload.get("sl")
        or payload.get("invalidation_reference_price")
    )
    take_profit = _safe_float(
        payload.get("take_profit")
        or payload.get("tp")
        or payload.get("target_reference_price")
    )
    rr = _safe_float(payload.get("rr") or payload.get("risk_reward_ratio") or payload.get("risk_reward"))

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

    if probability is None or probability < MIN_READY_PROBABILITY:
        return SignalQualityResult(
            decision=SignalQualityDecision.BLOCK,
            is_telegram_allowed=False,
            reason=f"blocked: probability {probability} below {MIN_READY_PROBABILITY}",
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
        reason="passed: READY + EXECUTABLE + valid geometry + RR filter",
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