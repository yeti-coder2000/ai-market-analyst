"""
Core dataclasses used by the analytics engine.

These types represent the contracts between:
context_builder
setup engines
scenario engine
market radar
reporting
"""

from dataclasses import dataclass, field
from typing import List, Optional

from .enums import Direction, MarketState, SetupStatus, DataQuality


# -------------------------------------------------------------------
# Market Context
# -------------------------------------------------------------------


@dataclass
class MarketContext:
    """
    Aggregated market context produced by context_builder.

    Contains structural and profile information used by setups
    and scenario engines.
    """

    instrument: str
    price: float

    market_state: MarketState
    htf_bias: Direction

    weekly_val: Optional[float] = None
    weekly_poc: Optional[float] = None
    weekly_vah: Optional[float] = None

    impulse: bool = False
    pullback: bool = False
    sweep: bool = False

    data_quality: DataQuality = DataQuality.OK


# -------------------------------------------------------------------
# Setup Result
# -------------------------------------------------------------------


@dataclass
class SetupResult:
    """
    Output of a setup module (setup_a / setup_b).

    Each setup evaluates context and produces a potential signal candidate.
    """

    setup_name: str

    status: SetupStatus
    direction: Direction

    score: float = 0.0
    grade: Optional[str] = None

    trigger_price: Optional[float] = None

    context_notes: List[str] = field(default_factory=list)


# -------------------------------------------------------------------
# Scenario Result
# -------------------------------------------------------------------


@dataclass
class ScenarioResult:
    """
    Final scenario selected by scenario_engine.

    This is the object consumed by:
    market_radar
    reporting layer
    journaling
    """

    instrument: str

    setup_name: str
    status: SetupStatus
    direction: Direction

    score: float

    entry_trigger: Optional[float]

    context_bias: Direction
    market_state: MarketState

    context_notes: List[str] = field(default_factory=list)

    # distance metrics
    distance_points: Optional[float] = None
    distance_atr: Optional[float] = None

    comment: Optional[str] = None