from __future__ import annotations

from datetime import datetime, UTC
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ============================================================================
# ENUMS
# ============================================================================


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


TradeDirection = Direction


class MarketState(str, Enum):
    BALANCE = "BALANCE"
    TREND = "TREND"
    TRANSITION = "TRANSITION"


class Timeframe(str, Enum):
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"
    W1 = "1w"

    @classmethod
    def from_value(cls, value: str) -> "Timeframe":
        normalized = str(value).strip().lower()
        mapping = {
            "5m": cls.M5,
            "15m": cls.M15,
            "30m": cls.M30,
            "1h": cls.H1,
            "4h": cls.H4,
            "1d": cls.D1,
            "1w": cls.W1,
        }
        if normalized not in mapping:
            raise ValueError(f"Unsupported timeframe: {value}")
        return mapping[normalized]


class Instrument(str, Enum):
    XAUUSD = "XAUUSD"
    EURUSD = "EURUSD"
    GBPUSD = "GBPUSD"
    BTCUSD = "BTCUSD"
    ETHUSD = "ETHUSD"
    UNKNOWN = "UNKNOWN"


class LevelType(str, Enum):
    # basic
    VAL = "VAL"
    VAH = "VAH"
    POC = "POC"

    HIGH = "HIGH"
    LOW = "LOW"
    OPEN = "OPEN"
    CLOSE = "CLOSE"

    # contextual profile levels
    WEEKLY_VAL = "WEEKLY_VAL"
    WEEKLY_VAH = "WEEKLY_VAH"
    WEEKLY_POC = "WEEKLY_POC"

    DAILY_VAL = "DAILY_VAL"
    DAILY_VAH = "DAILY_VAH"
    DAILY_POC = "DAILY_POC"

    MONTHLY_VAL = "MONTHLY_VAL"
    MONTHLY_VAH = "MONTHLY_VAH"
    MONTHLY_POC = "MONTHLY_POC"

    # swing / pivot structure
    SWING_HIGH = "SWING_HIGH"
    SWING_LOW = "SWING_LOW"
    PIVOT_HIGH = "PIVOT_HIGH"
    PIVOT_LOW = "PIVOT_LOW"

    # misc
    CUSTOM = "CUSTOM"
    OTHER = "OTHER"


class SetupStatus(str, Enum):
    NO_SETUP = "NO_SETUP"
    IDLE = "IDLE"
    EDGE_FORMING = "EDGE_FORMING"
    WATCH = "WATCH"
    READY = "READY"
    ACTIVE = "ACTIVE"
    INVALID = "INVALID"


class SetupType(str, Enum):
    IMPULSE_PULLBACK_CONTINUATION = "IMPULSE_PULLBACK_CONTINUATION"
    SWEEP_RETURN_TO_VALUE = "SWEEP_RETURN_TO_VALUE"
    NONE = "NONE"


class SetupGrade(str, Enum):
    A = "A"
    B = "B"
    C = "C"


# ============================================================================
# NORMALIZERS
# ============================================================================


def utc_now() -> datetime:
    return datetime.now(UTC)


def normalize_instrument(value: Any) -> Instrument:
    if isinstance(value, Instrument):
        return value

    if isinstance(value, Enum):
        raw = value.value
    else:
        raw = value

    if raw is None:
        return Instrument.UNKNOWN

    raw_str = str(raw).upper().strip()

    try:
        return Instrument(raw_str)
    except ValueError:
        return Instrument.UNKNOWN


def normalize_timeframe(value: Any) -> str:
    if isinstance(value, Timeframe):
        return value.value
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


# ============================================================================
# BASIC MARKET PROFILE / LEVEL MODELS
# ============================================================================


class PriceLevel(BaseModel):
    model_config = ConfigDict(extra="allow")

    price: float
    level_type: LevelType = LevelType.OTHER
    label: str | None = None


class ValueAreaLevels(BaseModel):
    model_config = ConfigDict(extra="allow")

    poc: float
    vah: float
    val: float


class MarketProfileSnapshot(BaseModel):
    model_config = ConfigDict(extra="allow")

    instrument: Instrument = Instrument.UNKNOWN
    timestamp: datetime = Field(default_factory=utc_now)
    monthly: ValueAreaLevels = Field(
        default_factory=lambda: ValueAreaLevels(poc=0.0, vah=0.0, val=0.0)
    )
    weekly: ValueAreaLevels = Field(
        default_factory=lambda: ValueAreaLevels(poc=0.0, vah=0.0, val=0.0)
    )
    daily: ValueAreaLevels = Field(
        default_factory=lambda: ValueAreaLevels(poc=0.0, vah=0.0, val=0.0)
    )

    @field_validator("instrument", mode="before")
    @classmethod
    def _normalize_instrument(cls, value: Any) -> Instrument:
        return normalize_instrument(value)


# ============================================================================
# CONTEXT SUBMODELS
# ============================================================================


class HTFBiasContext(BaseModel):
    model_config = ConfigDict(extra="allow")

    bias: Direction = Direction.NEUTRAL
    note: str | None = None


class AcceptanceState(BaseModel):
    model_config = ConfigDict(extra="allow")

    accepted_above: bool = False
    accepted_below: bool = False
    no_acceptance_above: bool = False
    no_acceptance_below: bool = False


class StructureState(BaseModel):
    """
    Legacy-compatible structure model expected by builder/formatter/logger.
    """

    model_config = ConfigDict(extra="allow")

    bos_up: bool = False
    bos_down: bool = False
    hh_hl_structure: bool = False
    ll_lh_structure: bool = False


class ImpulseDebugInfo(BaseModel):
    model_config = ConfigDict(extra="allow")

    min_atr_multiple_required: float | None = None
    min_body_ratio_required: float | None = None
    max_internal_pullback_allowed: float | None = None
    checks_passed: list[str] = Field(default_factory=list)
    checks_failed: list[str] = Field(default_factory=list)


class ImpulseMetrics(BaseModel):
    model_config = ConfigDict(extra="allow")

    detected: bool = False
    direction: Direction = Direction.NEUTRAL
    range_points: float = 0.0
    range_atr_multiple: float = 0.0
    body_ratio: float = 0.0
    internal_pullback_pct: float = 0.0
    broke_local_balance: bool = False
    debug: ImpulseDebugInfo | None = None


class PullbackMetrics(BaseModel):
    model_config = ConfigDict(extra="allow")

    detected: bool = False
    direction: Direction = Direction.NEUTRAL
    depth_pct_of_impulse: float = 0.0
    held_structure: bool = False


class SweepMetrics(BaseModel):
    model_config = ConfigDict(extra="allow")

    detected: bool = False
    direction: Direction = Direction.NEUTRAL
    returned_to_value: bool = False
    swept_level: LevelType | None = None
    reference_price: float | None = None


class LiquidityContext(BaseModel):
    model_config = ConfigDict(extra="allow")

    sweep: SweepMetrics = Field(default_factory=SweepMetrics)


class SwingPoint(BaseModel):
    model_config = ConfigDict(extra="allow")

    timeframe: Timeframe | str
    price: float
    timestamp: datetime | None = None
    kind: str | None = None
    direction: Direction = Direction.NEUTRAL
    label: str | None = None
    strength: float = Field(default=0.0, ge=0.0, le=1.0)
    is_major: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timeframe", mode="before")
    @classmethod
    def _normalize_timeframe(cls, value: Any) -> str:
        return normalize_timeframe(value)


# ============================================================================
# MAIN MARKET CONTEXT
# ============================================================================


class MarketContext(BaseModel):
    """
    Compatibility-first market context.

    Old modules expect nested fields like:
    - context.htf_bias.bias
    - context.profile.weekly.val
    - context.acceptance.accepted_above
    - context.structure_4h.hh_hl_structure
    - context.impulse.detected
    - context.pullback.held_structure
    - context.sweep.returned_to_value
    """

    model_config = ConfigDict(extra="allow")

    instrument: Instrument = Instrument.UNKNOWN
    timestamp: datetime = Field(default_factory=utc_now)
    current_price: float = 0.0

    market_state: MarketState = MarketState.TRANSITION
    htf_bias: HTFBiasContext = Field(default_factory=HTFBiasContext)

    profile: MarketProfileSnapshot = Field(default_factory=MarketProfileSnapshot)
    acceptance: AcceptanceState = Field(default_factory=AcceptanceState)

    structure_4h: StructureState = Field(default_factory=StructureState)
    structure_15m: StructureState = Field(default_factory=StructureState)

    impulse: ImpulseMetrics = Field(default_factory=ImpulseMetrics)
    pullback: PullbackMetrics = Field(default_factory=PullbackMetrics)
    sweep: SweepMetrics = Field(default_factory=SweepMetrics)
    liquidity: LiquidityContext = Field(default_factory=LiquidityContext)

    notes: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("instrument", mode="before")
    @classmethod
    def _normalize_instrument(cls, value: Any) -> Instrument:
        return normalize_instrument(value)

    @property
    def price(self) -> float:
        return self.current_price


# ============================================================================
# SETUP / SIGNAL SUPPORT MODELS
# ============================================================================


class EntryPlan(BaseModel):
    model_config = ConfigDict(extra="allow")

    entry_min: float | None = None
    entry_max: float | None = None
    stop: float | None = None
    target: float | None = None


class ConditionResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    message: str


class SetupDiagnostics(BaseModel):
    model_config = ConfigDict(extra="allow")

    passed_conditions: list[ConditionResult] = Field(default_factory=list)
    failed_conditions: list[ConditionResult] = Field(default_factory=list)


class SetupConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    min_confidence: float = 0.0
    enabled: bool = True


class BaseSetupResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    setup_type: SetupType = SetupType.NONE
    status: SetupStatus = SetupStatus.NO_SETUP
    direction: Direction = Direction.NEUTRAL
    grade: SetupGrade | None = None
    entry_plan: EntryPlan | None = None
    diagnostics: SetupDiagnostics = Field(default_factory=SetupDiagnostics)
    rationale: str | None = None
    confidence: float = 0.0


class SetupAResult(BaseSetupResult):
    model_config = ConfigDict(extra="allow")

    setup_type: SetupType = SetupType.IMPULSE_PULLBACK_CONTINUATION


class SetupBResult(BaseSetupResult):
    model_config = ConfigDict(extra="allow")

    setup_type: SetupType = SetupType.SWEEP_RETURN_TO_VALUE


class SetupAInput(BaseModel):
    model_config = ConfigDict(extra="allow")

    context: MarketContext
    config: SetupConfig = Field(default_factory=SetupConfig)


class SetupBInput(BaseModel):
    model_config = ConfigDict(extra="allow")

    context: MarketContext
    config: SetupConfig = Field(default_factory=SetupConfig)


# ============================================================================
# RULE EVALUATORS
# ============================================================================


class SetupARule(BaseModel):
    """
    Legacy-compatible evaluator for continuation setup.

    EDGE_FORMING is a pre-edge state:
    - it is NOT an executable trade signal
    - it is NOT supposed to reach Telegram
    - it is useful for journal/statistics/diagnostics
    """

    model_config = ConfigDict(extra="allow")

    config: SetupConfig = Field(default_factory=SetupConfig)

    def evaluate(self, payload: SetupAInput) -> SetupAResult:
        ctx = payload.context
        result = SetupAResult()

        effective_direction = (
            ctx.impulse.direction
            if ctx.impulse.direction != Direction.NEUTRAL
            else ctx.htf_bias.bias
        )

        aligned_with_htf = (
            ctx.htf_bias.bias != Direction.NEUTRAL
            and effective_direction == ctx.htf_bias.bias
        )
        valid_market_state = ctx.market_state == MarketState.TREND
        valid_impulse = ctx.impulse.detected
        valid_pullback = ctx.pullback.detected and ctx.pullback.held_structure

        if valid_market_state:
            result.diagnostics.passed_conditions.append(
                ConditionResult(
                    name="market_state",
                    message="Market state supports continuation scenario",
                )
            )
        else:
            result.diagnostics.failed_conditions.append(
                ConditionResult(
                    name="market_state",
                    message="Market state does not support continuation scenario",
                )
            )

        if valid_impulse:
            result.diagnostics.passed_conditions.append(
                ConditionResult(
                    name="impulse",
                    message="Impulse detected",
                )
            )
        else:
            result.diagnostics.failed_conditions.append(
                ConditionResult(
                    name="impulse",
                    message="No confirmed impulse detected",
                )
            )

        if valid_pullback:
            result.diagnostics.passed_conditions.append(
                ConditionResult(
                    name="pullback",
                    message="Pullback held structure",
                )
            )
        else:
            result.diagnostics.failed_conditions.append(
                ConditionResult(
                    name="pullback",
                    message="No valid pullback / structure hold",
                )
            )

        if aligned_with_htf:
            result.diagnostics.passed_conditions.append(
                ConditionResult(
                    name="htf_alignment",
                    message="Setup direction aligns with HTF bias",
                )
            )
        else:
            result.diagnostics.failed_conditions.append(
                ConditionResult(
                    name="htf_alignment",
                    message="Setup direction does not align with HTF bias",
                )
            )

        result.direction = effective_direction

        # ------------------------------------------------------------
        # READY: full continuation setup confirmed.
        # ------------------------------------------------------------
        if valid_market_state and valid_impulse and valid_pullback and aligned_with_htf:
            result.status = SetupStatus.READY
            result.grade = SetupGrade.A
            result.confidence = 0.8
            result.rationale = (
                "Trend continuation setup is aligned across context, impulse and pullback."
            )
            result.entry_plan = EntryPlan(
                entry_min=ctx.current_price,
                entry_max=ctx.current_price,
            )
            return result

        # ------------------------------------------------------------
        # WATCH: impulse already exists, but pullback confirmation is incomplete.
        # ------------------------------------------------------------
        if valid_market_state and valid_impulse and aligned_with_htf:
            result.status = SetupStatus.WATCH
            result.grade = SetupGrade.B
            result.confidence = 0.55
            result.rationale = (
                "Impulse exists in trend context and aligns with HTF bias; "
                "waiting for pullback / structure hold confirmation."
            )
            return result

        # ------------------------------------------------------------
        # EDGE_FORMING: trend context + HTF alignment exist, but impulse is not confirmed.
        # This is reconnaissance, not a trade signal.
        # ------------------------------------------------------------
        if valid_market_state and aligned_with_htf and not valid_impulse:
            result.status = SetupStatus.EDGE_FORMING
            result.grade = SetupGrade.C
            result.confidence = 0.25
            result.rationale = (
                "EDGE_FORMING: trend context and HTF alignment are present; "
                "waiting for confirmed impulse."
            )
            return result

        result.status = SetupStatus.IDLE
        result.grade = None
        result.confidence = 0.0
        result.rationale = "Continuation setup is not active in current context."
        return result


class SetupBRule(BaseModel):
    """
    Legacy-compatible evaluator for sweep -> return to value setup.

    EDGE_FORMING is used when market context supports sweep/return behavior,
    but the sweep itself has not appeared yet.
    """

    model_config = ConfigDict(extra="allow")

    config: SetupConfig = Field(default_factory=SetupConfig)

    def evaluate(self, payload: SetupBInput) -> SetupBResult:
        ctx = payload.context
        result = SetupBResult()

        valid_state = ctx.market_state in {MarketState.BALANCE, MarketState.TRANSITION}
        has_sweep = ctx.sweep.detected
        returned = ctx.sweep.returned_to_value

        if valid_state:
            result.diagnostics.passed_conditions.append(
                ConditionResult(
                    name="market_state",
                    message="Market state supports sweep / return scenario",
                )
            )
        else:
            result.diagnostics.failed_conditions.append(
                ConditionResult(
                    name="market_state",
                    message="Market state does not support sweep / return scenario",
                )
            )

        if has_sweep:
            result.diagnostics.passed_conditions.append(
                ConditionResult(
                    name="sweep",
                    message="Liquidity sweep detected",
                )
            )
        else:
            result.diagnostics.failed_conditions.append(
                ConditionResult(
                    name="sweep",
                    message="No sweep detected",
                )
            )

        if returned:
            result.diagnostics.passed_conditions.append(
                ConditionResult(
                    name="return_to_value",
                    message="Price returned to value after sweep",
                )
            )
        else:
            result.diagnostics.failed_conditions.append(
                ConditionResult(
                    name="return_to_value",
                    message="No return to value confirmed",
                )
            )

        result.direction = ctx.sweep.direction

        # ------------------------------------------------------------
        # READY: sweep and return-to-value setup is fully confirmed.
        # ------------------------------------------------------------
        if valid_state and has_sweep and returned:
            result.status = SetupStatus.READY
            result.grade = SetupGrade.A
            result.confidence = 0.8
            result.rationale = "Sweep and return-to-value setup is fully confirmed."
            result.entry_plan = EntryPlan(
                entry_min=ctx.current_price,
                entry_max=ctx.current_price,
            )
            return result

        # ------------------------------------------------------------
        # WATCH: sweep exists, but return-to-value is incomplete.
        # ------------------------------------------------------------
        if valid_state and has_sweep:
            result.status = SetupStatus.WATCH
            result.grade = SetupGrade.B
            result.confidence = 0.6
            result.rationale = (
                "Sweep detected; waiting for clearer return-to-value confirmation."
            )
            return result

        # ------------------------------------------------------------
        # EDGE_FORMING: market state supports sweep behavior, but no sweep yet.
        # This is useful for diagnostics and statistics only.
        # ------------------------------------------------------------
        if valid_state and not has_sweep:
            result.status = SetupStatus.EDGE_FORMING
            result.grade = SetupGrade.C
            result.confidence = 0.2
            result.rationale = (
                "EDGE_FORMING: balance/transition context supports sweep-return scenario; "
                "waiting for liquidity sweep."
            )
            return result

        result.status = SetupStatus.IDLE
        result.grade = None
        result.confidence = 0.0
        result.rationale = "Sweep / return-to-value setup is not active in current context."
        return result


# ============================================================================
# OPTIONAL FINAL SIGNAL
# ============================================================================


class FinalSignal(BaseModel):
    model_config = ConfigDict(extra="allow")

    instrument: Instrument = Instrument.UNKNOWN
    price: float | None = None
    market_state: MarketState = MarketState.TRANSITION
    direction: Direction = Direction.NEUTRAL
    status: SetupStatus = SetupStatus.NO_SETUP
    setup_type: SetupType = SetupType.NONE
    rationale: str | None = None
    confidence: float = 0.0

    @field_validator("instrument", mode="before")
    @classmethod
    def _normalize_instrument(cls, value: Any) -> Instrument:
        return normalize_instrument(value)


def build_final_signal_from_setup(
    context: MarketContext,
    result: BaseSetupResult,
) -> FinalSignal:
    return FinalSignal(
        instrument=context.instrument,
        price=context.current_price,
        market_state=context.market_state,
        direction=result.direction,
        status=result.status,
        setup_type=result.setup_type,
        rationale=result.rationale,
        confidence=result.confidence,
    )