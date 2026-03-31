from __future__ import annotations

from datetime import datetime
from typing import List, Tuple

import numpy as np
import pandas as pd

from app.context.schema import (
    Direction,
    Timeframe,
    SwingPoint,
    StructureState,
    ImpulseMetrics,
    PullbackMetrics,
    SweepMetrics,
    LevelType,
    ImpulseDebugInfo,
)


# ============================================================
# HELPERS
# ============================================================

def ensure_columns(df: pd.DataFrame, required: List[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    ensure_columns(df, ["high", "low", "close"])

    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return tr.rolling(period, min_periods=period).mean()


def candle_body_ratio(df: pd.DataFrame) -> pd.Series:
    ensure_columns(df, ["open", "high", "low", "close"])

    total_range = (df["high"] - df["low"]).replace(0, np.nan)
    body = (df["close"] - df["open"]).abs()
    return (body / total_range).fillna(0.0)


def get_timestamp(df: pd.DataFrame, idx: int) -> datetime:
    ts = df.index[idx]
    if isinstance(ts, pd.Timestamp):
        return ts.to_pydatetime()
    if isinstance(ts, datetime):
        return ts
    raise ValueError("DataFrame index must be datetime-like")


# ============================================================
# PIVOTS
# ============================================================

def is_pivot_high(df: pd.DataFrame, idx: int, lookback: int = 3) -> bool:
    if idx < lookback or idx >= len(df) - lookback:
        return False

    current_high = df["high"].iloc[idx]
    left = df["high"].iloc[idx - lookback: idx]
    right = df["high"].iloc[idx + 1: idx + 1 + lookback]

    return current_high > left.max() and current_high > right.max()


def is_pivot_low(df: pd.DataFrame, idx: int, lookback: int = 3) -> bool:
    if idx < lookback or idx >= len(df) - lookback:
        return False

    current_low = df["low"].iloc[idx]
    left = df["low"].iloc[idx - lookback: idx]
    right = df["low"].iloc[idx + 1: idx + 1 + lookback]

    return current_low < left.min() and current_low < right.min()


def extract_pivots(
    df: pd.DataFrame,
    timeframe: Timeframe,
    lookback: int = 3,
) -> Tuple[List[SwingPoint], List[SwingPoint]]:
    ensure_columns(df, ["high", "low"])

    highs: List[SwingPoint] = []
    lows: List[SwingPoint] = []

    for i in range(len(df)):
        if is_pivot_high(df, i, lookback):
            highs.append(
                SwingPoint(
                    timestamp=get_timestamp(df, i),
                    price=float(df["high"].iloc[i]),
                    kind="HIGH",
                    timeframe=timeframe,
                )
            )

        if is_pivot_low(df, i, lookback):
            lows.append(
                SwingPoint(
                    timestamp=get_timestamp(df, i),
                    price=float(df["low"].iloc[i]),
                    kind="LOW",
                    timeframe=timeframe,
                )
            )

    return highs, lows


# ============================================================
# STRUCTURE / BOS
# ============================================================

def build_structure_state(
    df: pd.DataFrame,
    timeframe: Timeframe,
    pivot_lookback: int = 3,
    min_break_distance: float = 0.0,
) -> StructureState:
    ensure_columns(df, ["high", "low", "close"])

    pivot_highs, pivot_lows = extract_pivots(df, timeframe, pivot_lookback)

    last_high = pivot_highs[-1] if len(pivot_highs) >= 1 else None
    prev_high = pivot_highs[-2] if len(pivot_highs) >= 2 else None

    last_low = pivot_lows[-1] if len(pivot_lows) >= 1 else None
    prev_low = pivot_lows[-2] if len(pivot_lows) >= 2 else None

    current_close = float(df["close"].iloc[-1])

    bos_up = False
    bos_down = False

    if last_high is not None and current_close > (last_high.price + min_break_distance):
        bos_up = True

    if last_low is not None and current_close < (last_low.price - min_break_distance):
        bos_down = True

    hh_hl = False
    ll_lh = False

    if prev_high and last_high and prev_low and last_low:
        hh_hl = last_high.price > prev_high.price and last_low.price >= prev_low.price
        ll_lh = last_high.price <= prev_high.price and last_low.price < prev_low.price

    return StructureState(
        last_pivot_high=last_high,
        prev_pivot_high=prev_high,
        last_pivot_low=last_low,
        prev_pivot_low=prev_low,
        bos_up=bos_up,
        bos_down=bos_down,
        hh_hl_structure=hh_hl,
        ll_lh_structure=ll_lh,
    )


# ============================================================
# LOCAL BALANCE
# ============================================================

def detect_local_balance(
    df: pd.DataFrame,
    lookback_bars: int = 12,
    max_range_atr_multiple: float = 2.0,
    atr_period: int = 14,
) -> bool:
    ensure_columns(df, ["high", "low", "close"])

    if len(df) < max(lookback_bars, atr_period + 2):
        return False

    atr = compute_atr(df, atr_period)
    recent = df.iloc[-lookback_bars:]
    recent_range = float(recent["high"].max() - recent["low"].min())
    recent_atr = float(atr.iloc[-1]) if not np.isnan(atr.iloc[-1]) else 0.0

    if recent_atr <= 0:
        return False

    return recent_range <= (recent_atr * max_range_atr_multiple)


# ============================================================
# IMPULSE DETECTOR
# ============================================================

def detect_impulse(
    df: pd.DataFrame,
    direction: Direction,
    impulse_window_bars: int = 6,
    impulse_min_atr_multiple: float = 1.2,
    impulse_body_ratio: float = 0.6,
    impulse_max_internal_pullback_pct: float = 0.35,
    atr_period: int = 14,
    require_breakout_from_balance: bool = False,
) -> ImpulseMetrics:
    ensure_columns(df, ["open", "high", "low", "close"])

    if len(df) < max(atr_period + 2, impulse_window_bars + 2):
        return ImpulseMetrics(
            debug=ImpulseDebugInfo(
                min_atr_multiple_required=impulse_min_atr_multiple,
                min_body_ratio_required=impulse_body_ratio,
                max_internal_pullback_allowed=impulse_max_internal_pullback_pct,
                require_breakout_from_balance=require_breakout_from_balance,
                checks_failed=["Недостатньо барів для оцінки імпульсу"],
            )
        )

    atr = compute_atr(df, atr_period)
    body_ratio_series = candle_body_ratio(df)

    window = df.iloc[-impulse_window_bars:]
    window_body_ratio = body_ratio_series.iloc[-impulse_window_bars:]

    current_atr = atr.iloc[-1]
    if pd.isna(current_atr) or current_atr <= 0:
        return ImpulseMetrics(
            debug=ImpulseDebugInfo(
                min_atr_multiple_required=impulse_min_atr_multiple,
                min_body_ratio_required=impulse_body_ratio,
                max_internal_pullback_allowed=impulse_max_internal_pullback_pct,
                require_breakout_from_balance=require_breakout_from_balance,
                checks_failed=["ATR недоступний або дорівнює 0"],
            )
        )

    start_idx = len(df) - impulse_window_bars
    end_idx = len(df) - 1

    if direction == Direction.LONG:
        start_price = float(window["low"].min())
        end_price = float(window["high"].max())
        impulse_range = end_price - start_price

        internal_high = window["high"].cummax()
        internal_pullbacks = (internal_high - window["low"]) / max(impulse_range, 1e-9)
        internal_pullback_pct = float(internal_pullbacks.max())

    elif direction == Direction.SHORT:
        start_price = float(window["high"].max())
        end_price = float(window["low"].min())
        impulse_range = start_price - end_price

        internal_low = window["low"].cummin()
        internal_pullbacks = (window["high"] - internal_low) / max(impulse_range, 1e-9)
        internal_pullback_pct = float(internal_pullbacks.max())

    else:
        return ImpulseMetrics(
            debug=ImpulseDebugInfo(
                min_atr_multiple_required=impulse_min_atr_multiple,
                min_body_ratio_required=impulse_body_ratio,
                max_internal_pullback_allowed=impulse_max_internal_pullback_pct,
                require_breakout_from_balance=require_breakout_from_balance,
                checks_failed=["Напрям імпульсу не заданий"],
            )
        )

    avg_body_ratio = float(window_body_ratio.mean())
    range_atr_multiple = impulse_range / float(current_atr) if current_atr > 0 else 0.0

    broke_local_balance = True
    if require_breakout_from_balance:
        balance_source = df.iloc[:-impulse_window_bars]
        if len(balance_source) < max(12, atr_period + 2):
            broke_local_balance = False
        else:
            was_balanced_before = detect_local_balance(
                balance_source,
                lookback_bars=12,
                atr_period=atr_period,
            )
            broke_local_balance = was_balanced_before

    checks_passed: list[str] = []
    checks_failed: list[str] = []

    if impulse_range > 0:
        checks_passed.append("Діапазон імпульсу > 0")
    else:
        checks_failed.append("Діапазон імпульсу <= 0")

    if range_atr_multiple >= impulse_min_atr_multiple:
        checks_passed.append(
            f"ATR multiple OK ({range_atr_multiple:.2f} >= {impulse_min_atr_multiple:.2f})"
        )
    else:
        checks_failed.append(
            f"ATR multiple слабкий ({range_atr_multiple:.2f} < {impulse_min_atr_multiple:.2f})"
        )

    if avg_body_ratio >= impulse_body_ratio:
        checks_passed.append(
            f"Body ratio OK ({avg_body_ratio:.2f} >= {impulse_body_ratio:.2f})"
        )
    else:
        checks_failed.append(
            f"Body ratio слабкий ({avg_body_ratio:.2f} < {impulse_body_ratio:.2f})"
        )

    if internal_pullback_pct <= impulse_max_internal_pullback_pct:
        checks_passed.append(
            f"Internal pullback OK ({internal_pullback_pct:.2f} <= {impulse_max_internal_pullback_pct:.2f})"
        )
    else:
        checks_failed.append(
            f"Internal pullback завеликий ({internal_pullback_pct:.2f} > {impulse_max_internal_pullback_pct:.2f})"
        )

    if require_breakout_from_balance:
        if broke_local_balance:
            checks_passed.append("Є вихід із локального балансу")
        else:
            checks_failed.append("Немає виходу з локального балансу")
    else:
        checks_passed.append("Breakout from balance не вимагається")

    detected = (
        impulse_range > 0
        and range_atr_multiple >= impulse_min_atr_multiple
        and avg_body_ratio >= impulse_body_ratio
        and internal_pullback_pct <= impulse_max_internal_pullback_pct
        and broke_local_balance
    )

    return ImpulseMetrics(
        detected=detected,
        direction=direction if detected else Direction.NEUTRAL,
        start_time=get_timestamp(df, start_idx),
        end_time=get_timestamp(df, end_idx),
        start_price=start_price,
        end_price=end_price,
        range_points=float(impulse_range),
        range_atr_multiple=float(range_atr_multiple),
        body_ratio=float(avg_body_ratio),
        internal_pullback_pct=float(internal_pullback_pct),
        broke_local_balance=bool(broke_local_balance),
        from_key_level=False,
        debug=ImpulseDebugInfo(
            min_atr_multiple_required=impulse_min_atr_multiple,
            actual_atr_multiple=float(range_atr_multiple),
            min_body_ratio_required=impulse_body_ratio,
            actual_body_ratio=float(avg_body_ratio),
            max_internal_pullback_allowed=impulse_max_internal_pullback_pct,
            actual_internal_pullback=float(internal_pullback_pct),
            require_breakout_from_balance=require_breakout_from_balance,
            broke_local_balance=bool(broke_local_balance),
            checks_passed=checks_passed,
            checks_failed=checks_failed,
        ),
    )


# ============================================================
# PULLBACK DETECTOR
# ============================================================

def detect_pullback_after_impulse(
    df: pd.DataFrame,
    impulse: ImpulseMetrics,
    direction: Direction,
    pullback_min_pct: float = 0.25,
    pullback_max_pct: float = 0.62,
    max_lookahead_bars: int = 10,
) -> PullbackMetrics:
    ensure_columns(df, ["high", "low", "close"])

    if not impulse.detected or impulse.direction != direction:
        return PullbackMetrics()

    if impulse.end_time is None or impulse.start_price is None or impulse.end_price is None:
        return PullbackMetrics()

    try:
        impulse_end_loc = df.index.get_loc(pd.Timestamp(impulse.end_time))
    except KeyError:
        return PullbackMetrics()

    after = df.iloc[impulse_end_loc + 1: impulse_end_loc + 1 + max_lookahead_bars]
    if after.empty:
        return PullbackMetrics()

    impulse_size = abs(impulse.end_price - impulse.start_price)
    if impulse_size <= 0:
        return PullbackMetrics()

    if direction == Direction.LONG:
        pullback_low = float(after["low"].min())
        depth = impulse.end_price - pullback_low
        depth_pct = depth / impulse_size

        held_structure = pullback_low > impulse.start_price
        invalidated_impulse = pullback_low <= impulse.start_price

        end_idx = after["low"].idxmin()

    elif direction == Direction.SHORT:
        pullback_high = float(after["high"].max())
        depth = pullback_high - impulse.end_price
        depth_pct = depth / impulse_size

        held_structure = pullback_high < impulse.start_price
        invalidated_impulse = pullback_high >= impulse.start_price

        end_idx = after["high"].idxmax()

    else:
        return PullbackMetrics()

    detected = pullback_min_pct <= depth_pct <= pullback_max_pct
    end_pos = df.index.get_loc(end_idx)

    return PullbackMetrics(
        detected=detected,
        direction=direction if detected else Direction.NEUTRAL,
        start_time=impulse.end_time,
        end_time=get_timestamp(df, end_pos),
        depth_pct_of_impulse=float(depth_pct),
        held_structure=bool(held_structure),
        touched_micro_poc=False,
        touched_retest_level=False,
        touched_fib_zone=bool(0.38 <= depth_pct <= 0.62),
        invalidated_impulse=bool(invalidated_impulse),
    )


# ============================================================
# SWEEP DETECTOR
# ============================================================

def detect_sweep(
    df: pd.DataFrame,
    reference_price: float,
    direction: Direction,
    sweep_min_points: float,
    max_sweep_return_bars: int = 3,
) -> SweepMetrics:
    """
    direction:
        LONG  -> шукаємо SWEEP_LOW і повернення вгору
        SHORT -> шукаємо SWEEP_HIGH і повернення вниз
    """
    ensure_columns(df, ["high", "low", "close"])

    if len(df) < max_sweep_return_bars + 2:
        return SweepMetrics()

    recent = df.iloc[-(max_sweep_return_bars + 1):]

    if direction == Direction.SHORT:
        max_high = float(recent["high"].max())
        last_close = float(df["close"].iloc[-1])

        swept = max_high >= (reference_price + sweep_min_points)
        returned = last_close < reference_price
        detected = swept and returned

        return SweepMetrics(
            detected=detected,
            direction=Direction.SHORT if detected else Direction.NEUTRAL,
            swept_level_price=float(reference_price),
            swept_level_type=LevelType.CUSTOM,
            swept_time=get_timestamp(df, len(df) - 1),
            excess_distance=float(max(0.0, max_high - reference_price)),
            returned_within_bars=max_sweep_return_bars if detected else None,
            returned_to_value=bool(returned),
            no_acceptance_beyond_sweep=bool(returned),
        )

    if direction == Direction.LONG:
        min_low = float(recent["low"].min())
        last_close = float(df["close"].iloc[-1])

        swept = min_low <= (reference_price - sweep_min_points)
        returned = last_close > reference_price
        detected = swept and returned

        return SweepMetrics(
            detected=detected,
            direction=Direction.LONG if detected else Direction.NEUTRAL,
            swept_level_price=float(reference_price),
            swept_level_type=LevelType.CUSTOM,
            swept_time=get_timestamp(df, len(df) - 1),
            excess_distance=float(max(0.0, reference_price - min_low)),
            returned_within_bars=max_sweep_return_bars if detected else None,
            returned_to_value=bool(returned),
            no_acceptance_beyond_sweep=bool(returned),
        )

    return SweepMetrics()