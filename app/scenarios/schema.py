from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.context.schema import (
    Direction,
    Instrument,
    MarketState,
    SetupStatus,
    SetupType,
)


class ScenarioType(str, Enum):
    TREND_CONTINUATION_LONG = "TREND_CONTINUATION_LONG"
    TREND_CONTINUATION_SHORT = "TREND_CONTINUATION_SHORT"
    SWEEP_RETURN_LONG = "SWEEP_RETURN_LONG"
    SWEEP_RETURN_SHORT = "SWEEP_RETURN_SHORT"
    BALANCE_ROTATION = "BALANCE_ROTATION"
    TRANSITION_EXPANSION = "TRANSITION_EXPANSION"
    NO_ACTION = "NO_ACTION"


class ScenarioPhase(str, Enum):
    PRECONDITION = "PRECONDITION"
    TRIGGER_ZONE = "TRIGGER_ZONE"
    CONFIRMED = "CONFIRMED"
    LATE = "LATE"
    INVALID = "INVALID"


class ScenarioDecision(str, Enum):
    NO_TRADE = "NO_TRADE"
    WATCH = "WATCH"
    ARMED = "ARMED"
    TRADEABLE = "TRADEABLE"
    INVALID = "INVALID"


class ScenarioEvidence(BaseModel):
    """
    Explainable evidence payload.

    This is the machine-readable trace of why the engine produced
    the final scenario result.
    """

    model_config = ConfigDict(extra="allow")

    market_state: str | None = None
    htf_bias: str | None = None

    impulse_detected: bool = False
    impulse_direction: str | None = None

    pullback_detected: bool = False
    pullback_direction: str | None = None
    pullback_held_structure: bool = False

    sweep_detected: bool = False
    sweep_direction: str | None = None
    return_to_value: bool = False

    acceptance_above: bool = False
    acceptance_below: bool = False
    no_acceptance_above: bool = False
    no_acceptance_below: bool = False

    setup_a_status: str | None = None
    setup_a_direction: str | None = None
    setup_a_confidence: float | None = None

    setup_b_status: str | None = None
    setup_b_direction: str | None = None
    setup_b_confidence: float | None = None

    passed_conditions: list[str] = Field(default_factory=list)
    failed_conditions: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class ScenarioResult(BaseModel):
    """
    Primary output of Scenario Engine.

    It is intentionally compatible with the existing final signal flow:
    - instrument
    - direction
    - status
    - setup_type
    - rationale
    - confidence

    And also extends it with scenario-specific metadata:
    - scenario_type
    - phase
    - decision
    - dominant_setup
    - next_expected_event
    - missing_conditions
    - alignment_score
    """

    model_config = ConfigDict(extra="allow")

    instrument: Instrument = Instrument.UNKNOWN
    price: float | None = None

    scenario_type: ScenarioType = ScenarioType.NO_ACTION
    phase: ScenarioPhase = ScenarioPhase.PRECONDITION
    decision: ScenarioDecision = ScenarioDecision.NO_TRADE

    market_state: MarketState = MarketState.TRANSITION
    direction: Direction = Direction.NEUTRAL

    # compatibility with existing final signal selection / journaling
    status: SetupStatus = SetupStatus.NO_SETUP
    setup_type: SetupType = SetupType.NONE

    dominant_setup: str | None = None
    setup_name: str | None = None

    rationale: str | None = None
    confidence: float = 0.0

    next_expected_event: str | None = None
    invalidation_reason: str | None = None
    missing_conditions: list[str] = Field(default_factory=list)
    alignment_score: float = 0.0

    tags: list[str] = Field(default_factory=list)
    evidence: ScenarioEvidence = Field(default_factory=ScenarioEvidence)
    metadata: dict[str, Any] = Field(default_factory=dict)