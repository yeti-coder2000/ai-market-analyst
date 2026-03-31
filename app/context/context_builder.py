from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from app.context.detectors import (
    build_structure_state,
    compute_atr,
    detect_impulse,
    detect_local_balance,
    detect_pullback_after_impulse,
    detect_sweep,
)
from app.context.schema import (
    AcceptanceState,
    Direction,
    HTFBiasContext,
    ImpulseMetrics,
    Instrument,
    LevelType,
    LiquidityContext,
    MarketContext,
    MarketProfileSnapshot,
    MarketState,
    PriceLevel,
    PullbackMetrics,
    StructureState,
    SweepMetrics,
    Timeframe,
    ValueAreaLevels,
)


# ============================================================
# INPUT CONTAINER
# ============================================================

@dataclass(slots=True)
class ContextBuilderInput:
    instrument: Instrument
    df_1d: pd.DataFrame
    df_4h: pd.DataFrame
    df_30m: pd.DataFrame
    df_15m: pd.DataFrame


# ============================================================
# BASIC HELPERS
# ============================================================

def validate_df(df: pd.DataFrame, name: str) -> None:
    required = {"open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{name} missing required columns: {sorted(missing)}")

    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(f"{name} index must be DatetimeIndex")

    if df.empty:
        raise ValueError(f"{name} is empty")


def get_last_timestamp(df: pd.DataFrame) -> datetime:
    ts = df.index[-1]
    return ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:  # noqa: BLE001
        return default


# ============================================================
# VALUE AREA PROXY
# ============================================================

def build_value_area_from_df(df: pd.DataFrame) -> ValueAreaLevels:
    """
    MVP proxy for value area.

    This is not real TPO / volume profile yet.
    """
    poc = safe_float(df["close"].median())
    vah = safe_float(df["high"].quantile(0.70))
    val = safe_float(df["low"].quantile(0.30))

    vah = max(vah, poc)
    val = min(val, poc)

    return ValueAreaLevels(
        poc=poc,
        vah=vah,
        val=val,
    )


def build_profile_snapshot(
    instrument: Instrument,
    df_1d: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_30m: pd.DataFrame,
) -> MarketProfileSnapshot:
    monthly_window = df_1d.tail(22)
    weekly_window = df_4h.tail(30)
    daily_window = df_30m.tail(48)

    monthly = build_value_area_from_df(monthly_window)
    weekly = build_value_area_from_df(weekly_window)
    daily = build_value_area_from_df(daily_window)

    return MarketProfileSnapshot(
        instrument=instrument,
        timestamp=get_last_timestamp(df_30m),
        monthly=monthly,
        weekly=weekly,
        daily=daily,
    )


# ============================================================
# ACCEPTANCE LOGIC
# ============================================================

def infer_acceptance_state(
    df_15m: pd.DataFrame,
    weekly_vah: float,
    weekly_val: float,
    acceptance_bars_threshold: int = 2,
) -> AcceptanceState:
    closes = df_15m["close"].tail(max(acceptance_bars_threshold, 3))

    bars_above_vah = int((closes > weekly_vah).sum())
    bars_below_val = int((closes < weekly_val).sum())

    accepted_above = bars_above_vah >= acceptance_bars_threshold
    accepted_below = bars_below_val >= acceptance_bars_threshold

    no_acceptance_above = (
        float(closes.max()) > weekly_vah
        and not accepted_above
        and float(closes.iloc[-1]) < weekly_vah
    )

    no_acceptance_below = (
        float(closes.min()) < weekly_val
        and not accepted_below
        and float(closes.iloc[-1]) > weekly_val
    )

    return AcceptanceState(
        above_level=weekly_vah,
        below_level=weekly_val,
        accepted_above=accepted_above,
        accepted_below=accepted_below,
        no_acceptance_above=no_acceptance_above,
        no_acceptance_below=no_acceptance_below,
        bars_above=bars_above_vah,
        bars_below=bars_below_val,
    )


# ============================================================
# LIQUIDITY CONTEXT
# ============================================================

def infer_liquidity_context(
    current_price: float,
    profile: MarketProfileSnapshot,
    structure_15m: StructureState,
    structure_4h: StructureState,
) -> LiquidityContext:
    liquidity_above: list[PriceLevel] = []
    liquidity_below: list[PriceLevel] = []

    liquidity_above.append(
        PriceLevel(
            level_type=LevelType.WEEKLY_VAH,
            price=profile.weekly.vah,
            label="Weekly VAH",
            timeframe=Timeframe.H4,
        )
    )
    liquidity_above.append(
        PriceLevel(
            level_type=LevelType.MONTHLY_VAH,
            price=profile.monthly.vah,
            label="Monthly VAH",
            timeframe=Timeframe.D1,
        )
    )
    liquidity_above.append(
        PriceLevel(
            level_type=LevelType.DAILY_VAH,
            price=profile.daily.vah,
            label="Daily VAH",
            timeframe=Timeframe.M30,
        )
    )

    liquidity_below.append(
        PriceLevel(
            level_type=LevelType.WEEKLY_VAL,
            price=profile.weekly.val,
            label="Weekly VAL",
            timeframe=Timeframe.H4,
        )
    )
    liquidity_below.append(
        PriceLevel(
            level_type=LevelType.MONTHLY_VAL,
            price=profile.monthly.val,
            label="Monthly VAL",
            timeframe=Timeframe.D1,
        )
    )
    liquidity_below.append(
        PriceLevel(
            level_type=LevelType.DAILY_VAL,
            price=profile.daily.val,
            label="Daily VAL",
            timeframe=Timeframe.M30,
        )
    )

    liquidity_above.append(
        PriceLevel(
            level_type=LevelType.WEEKLY_POC,
            price=profile.weekly.poc,
            label="Weekly POC",
            timeframe=Timeframe.H4,
        )
    )
    liquidity_below.append(
        PriceLevel(
            level_type=LevelType.DAILY_POC,
            price=profile.daily.poc,
            label="Daily POC",
            timeframe=Timeframe.M30,
        )
    )

    if structure_15m.last_pivot_high:
        liquidity_above.append(
            PriceLevel(
                level_type=LevelType.SWING_HIGH,
                price=structure_15m.last_pivot_high.price,
                label="15m Swing High",
                timeframe=Timeframe.M15,
            )
        )

    if structure_15m.last_pivot_low:
        liquidity_below.append(
            PriceLevel(
                level_type=LevelType.SWING_LOW,
                price=structure_15m.last_pivot_low.price,
                label="15m Swing Low",
                timeframe=Timeframe.M15,
            )
        )

    if structure_4h.last_pivot_high:
        liquidity_above.append(
            PriceLevel(
                level_type=LevelType.SWING_HIGH,
                price=structure_4h.last_pivot_high.price,
                label="4H Swing High",
                timeframe=Timeframe.H4,
            )
        )

    if structure_4h.last_pivot_low:
        liquidity_below.append(
            PriceLevel(
                level_type=LevelType.SWING_LOW,
                price=structure_4h.last_pivot_low.price,
                label="4H Swing Low",
                timeframe=Timeframe.H4,
            )
        )

    liquidity_above = sorted(liquidity_above, key=lambda x: x.price)
    liquidity_below = sorted(liquidity_below, key=lambda x: x.price, reverse=True)

    just_swept_high = False
    just_swept_low = False

    if structure_15m.last_pivot_high and current_price > structure_15m.last_pivot_high.price:
        just_swept_high = True

    if structure_15m.last_pivot_low and current_price < structure_15m.last_pivot_low.price:
        just_swept_low = True

    return LiquidityContext(
        nearest_liquidity_above=liquidity_above,
        nearest_liquidity_below=liquidity_below,
        just_swept_high=just_swept_high,
        just_swept_low=just_swept_low,
    )


# ============================================================
# HTF BIAS
# ============================================================

def infer_htf_bias(
    df_1d: pd.DataFrame,
    df_4h: pd.DataFrame,
    profile: MarketProfileSnapshot,
    structure_4h: StructureState,
) -> HTFBiasContext:
    notes_1d: list[str] = []
    notes_4h: list[str] = []

    current_close_1d = safe_float(df_1d["close"].iloc[-1])
    current_close_4h = safe_float(df_4h["close"].iloc[-1])

    bullish = False
    bearish = False

    if current_close_1d > profile.monthly.poc:
        notes_1d.append("1D close above Monthly POC")
        bullish = True
    elif current_close_1d < profile.monthly.poc:
        notes_1d.append("1D close below Monthly POC")
        bearish = True

    if structure_4h.hh_hl_structure:
        notes_4h.append("4H HH/HL structure intact")
        bullish = True

    if structure_4h.ll_lh_structure:
        notes_4h.append("4H LL/LH structure intact")
        bearish = True

    if current_close_4h > profile.weekly.val:
        notes_4h.append("4H holding above Weekly VAL")

    if current_close_4h < profile.weekly.vah:
        notes_4h.append("4H below Weekly VAH")

    if bullish and not bearish:
        return HTFBiasContext(
            bias=Direction.LONG,
            reason_1d=notes_1d,
            reason_4h=notes_4h,
            conflict=False,
        )

    if bearish and not bullish:
        return HTFBiasContext(
            bias=Direction.SHORT,
            reason_1d=notes_1d,
            reason_4h=notes_4h,
            conflict=False,
        )

    return HTFBiasContext(
        bias=Direction.NEUTRAL,
        reason_1d=notes_1d,
        reason_4h=notes_4h,
        conflict=True,
    )


# ============================================================
# MARKET STATE
# ============================================================

def infer_market_state(
    df_30m: pd.DataFrame,
    df_15m: pd.DataFrame,
    profile: MarketProfileSnapshot,
    structure_15m: StructureState,
) -> MarketState:
    del df_30m  # reserved for future logic

    current_price = safe_float(df_15m["close"].iloc[-1])

    local_balance = detect_local_balance(
        df_15m,
        lookback_bars=12,
        max_range_atr_multiple=2.0,
        atr_period=14,
    )

    inside_weekly_value = profile.weekly.val <= current_price <= profile.weekly.vah
    weekly_range = max(profile.weekly.vah - profile.weekly.val, 0.01)
    close_to_weekly_poc = abs(current_price - profile.weekly.poc) <= (weekly_range * 0.15)

    if local_balance and inside_weekly_value and close_to_weekly_poc:
        return MarketState.BALANCE

    if (structure_15m.bos_up or structure_15m.bos_down) and not local_balance:
        return MarketState.TREND

    if current_price > profile.weekly.vah or current_price < profile.weekly.val:
        return MarketState.TRANSITION

    return MarketState.TRANSITION


def infer_direction_of_interest(
    htf_bias: HTFBiasContext,
    market_state: MarketState,
) -> Direction:
    if market_state in {MarketState.TREND, MarketState.TRANSITION}:
        return htf_bias.bias

    if market_state == MarketState.BALANCE:
        return Direction.NEUTRAL

    return Direction.NEUTRAL


# ============================================================
# MAIN BUILDER
# ============================================================

class ContextBuilder:
    def __init__(
        self,
        pivot_lookback: int = 3,
        acceptance_bars_threshold: int = 2,
        impulse_window_bars: int = 6,
        impulse_min_atr_multiple: float = 1.2,
        impulse_body_ratio: float = 0.6,
        impulse_max_internal_pullback_pct: float = 0.35,
        pullback_min_pct: float = 0.25,
        pullback_max_pct: float = 0.62,
        sweep_min_points: float = 0.0,
        max_sweep_return_bars: int = 3,
    ) -> None:
        self.pivot_lookback = pivot_lookback
        self.acceptance_bars_threshold = acceptance_bars_threshold
        self.impulse_window_bars = impulse_window_bars
        self.impulse_min_atr_multiple = impulse_min_atr_multiple
        self.impulse_body_ratio = impulse_body_ratio
        self.impulse_max_internal_pullback_pct = impulse_max_internal_pullback_pct
        self.pullback_min_pct = pullback_min_pct
        self.pullback_max_pct = pullback_max_pct
        self.sweep_min_points = sweep_min_points
        self.max_sweep_return_bars = max_sweep_return_bars

    def build(self, payload: ContextBuilderInput) -> MarketContext:
        validate_df(payload.df_1d, "df_1d")
        validate_df(payload.df_4h, "df_4h")
        validate_df(payload.df_30m, "df_30m")
        validate_df(payload.df_15m, "df_15m")

        df_1d = payload.df_1d.copy()
        df_4h = payload.df_4h.copy()
        df_30m = payload.df_30m.copy()
        df_15m = payload.df_15m.copy()

        current_price = safe_float(df_15m["close"].iloc[-1])
        timestamp = get_last_timestamp(df_15m)

        profile = build_profile_snapshot(
            instrument=payload.instrument,
            df_1d=df_1d,
            df_4h=df_4h,
            df_30m=df_30m,
        )

        structure_15m = build_structure_state(
            df=df_15m,
            timeframe=Timeframe.M15,
            pivot_lookback=self.pivot_lookback,
            min_break_distance=0.0,
        )

        structure_4h = build_structure_state(
            df=df_4h,
            timeframe=Timeframe.H4,
            pivot_lookback=self.pivot_lookback,
            min_break_distance=0.0,
        )

        acceptance = infer_acceptance_state(
            df_15m=df_15m,
            weekly_vah=profile.weekly.vah,
            weekly_val=profile.weekly.val,
            acceptance_bars_threshold=self.acceptance_bars_threshold,
        )

        htf_bias = infer_htf_bias(
            df_1d=df_1d,
            df_4h=df_4h,
            profile=profile,
            structure_4h=structure_4h,
        )

        market_state = infer_market_state(
            df_30m=df_30m,
            df_15m=df_15m,
            profile=profile,
            structure_15m=structure_15m,
        )

        direction_interest = infer_direction_of_interest(
            htf_bias=htf_bias,
            market_state=market_state,
        )

        atr_15m_series = compute_atr(df_15m, period=14)
        atr_15m = safe_float(atr_15m_series.iloc[-1], 0.0)

        impulse = ImpulseMetrics()
        pullback = PullbackMetrics()

        if direction_interest in {Direction.LONG, Direction.SHORT}:
            impulse = detect_impulse(
                df=df_15m,
                direction=direction_interest,
                impulse_window_bars=self.impulse_window_bars,
                impulse_min_atr_multiple=self.impulse_min_atr_multiple,
                impulse_body_ratio=self.impulse_body_ratio,
                impulse_max_internal_pullback_pct=self.impulse_max_internal_pullback_pct,
                atr_period=14,
                require_breakout_from_balance=False,
            )

            pullback = detect_pullback_after_impulse(
                df=df_15m,
                impulse=impulse,
                direction=direction_interest,
                pullback_min_pct=self.pullback_min_pct,
                pullback_max_pct=self.pullback_max_pct,
                max_lookahead_bars=10,
            )

        sweep = SweepMetrics()

        if market_state in {MarketState.BALANCE, MarketState.TRANSITION}:
            short_sweep = detect_sweep(
                df=df_15m,
                reference_price=profile.weekly.vah,
                direction=Direction.SHORT,
                sweep_min_points=self.sweep_min_points,
                max_sweep_return_bars=self.max_sweep_return_bars,
            )

            long_sweep = detect_sweep(
                df=df_15m,
                reference_price=profile.weekly.val,
                direction=Direction.LONG,
                sweep_min_points=self.sweep_min_points,
                max_sweep_return_bars=self.max_sweep_return_bars,
            )

            if short_sweep.detected:
                sweep = short_sweep
            elif long_sweep.detected:
                sweep = long_sweep

        liquidity = infer_liquidity_context(
            current_price=current_price,
            profile=profile,
            structure_15m=structure_15m,
            structure_4h=structure_4h,
        )

        notes: list[str] = []
        notes.extend(htf_bias.reason_1d)
        notes.extend(htf_bias.reason_4h)

        if market_state == MarketState.BALANCE:
            notes.append("Market classified as BALANCE")
        elif market_state == MarketState.TREND:
            notes.append("Market classified as TREND")
        else:
            notes.append("Market classified as TRANSITION")

        if impulse.detected:
            notes.append(
                f"Impulse detected: {impulse.direction.value} / ATR x {impulse.range_atr_multiple:.2f}"
            )

        if pullback.detected:
            notes.append(
                f"Pullback detected: {pullback.depth_pct_of_impulse:.2%} of impulse"
            )

        if sweep.detected:
            notes.append(
                f"Sweep detected: {sweep.direction.value} at {sweep.swept_level_price:.2f}"
            )

        return MarketContext(
            instrument=payload.instrument,
            timestamp=timestamp,
            current_price=current_price,
            atr_15m=atr_15m,
            market_state=market_state,
            htf_bias=htf_bias,
            profile=profile,
            structure_15m=structure_15m,
            structure_4h=structure_4h,
            acceptance=acceptance,
            impulse=impulse,
            pullback=pullback,
            sweep=sweep,
            liquidity=liquidity,
            notes=notes,
        )