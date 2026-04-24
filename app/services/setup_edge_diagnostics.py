from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SetupEdgeDiagnostic:
    setup_name: str | None
    status: str | None
    direction: str | None
    confidence: float
    passed: list[str]
    failed: list[str]
    edge_blocker: str
    edge_stage: str


def _safe_attr(obj: Any, name: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _value(value: Any) -> Any:
    return getattr(value, "value", value)


def _condition_names(items: list[Any]) -> list[str]:
    result: list[str] = []
    for item in items or []:
        name = _safe_attr(item, "name")
        if name is not None:
            result.append(str(name))
    return result


def diagnose_setup_edge(setup: Any) -> SetupEdgeDiagnostic:
    setup_type = _value(_safe_attr(setup, "setup_type"))
    status = _value(_safe_attr(setup, "status"))
    direction = _value(_safe_attr(setup, "direction"))
    confidence = float(_safe_attr(setup, "confidence") or 0.0)

    diagnostics = _safe_attr(setup, "diagnostics")
    passed = _condition_names(_safe_attr(diagnostics, "passed_conditions") or [])
    failed = _condition_names(_safe_attr(diagnostics, "failed_conditions") or [])

    if "market_state" in failed:
        blocker = "MARKET_STATE"
        stage = "NO_CONTEXT_EDGE"
    elif "htf_alignment" in failed:
        blocker = "HTF_ALIGNMENT"
        stage = "NO_DIRECTION_EDGE"
    elif "impulse" in failed:
        blocker = "IMPULSE"
        stage = "WAITING_FOR_IMPULSE"
    elif "pullback" in failed:
        blocker = "PULLBACK"
        stage = "WAITING_FOR_PULLBACK"
    elif "sweep" in failed:
        blocker = "SWEEP"
        stage = "WAITING_FOR_SWEEP"
    elif "return_to_value" in failed:
        blocker = "RETURN_TO_VALUE"
        stage = "WAITING_FOR_RETURN"
    elif status in {"READY", "ACTIVE", "TRIGGERED"}:
        blocker = "NONE"
        stage = "EDGE_ACTIVE"
    else:
        blocker = "UNKNOWN"
        stage = "NO_EDGE"

    return SetupEdgeDiagnostic(
        setup_name=setup_type,
        status=status,
        direction=direction,
        confidence=confidence,
        passed=passed,
        failed=failed,
        edge_blocker=blocker,
        edge_stage=stage,
    )


def diagnose_setups_edge(setups: list[Any]) -> list[dict[str, Any]]:
    return [
        diagnose_setup_edge(setup).__dict__
        for setup in setups or []
    ]