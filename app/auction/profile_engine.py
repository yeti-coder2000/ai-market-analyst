from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

import pandas as pd

from app.core.session_config import (
    SessionConfig,
    compute_session_open_for_timestamp,
    get_session_config,
)


class OpenRelation(str, Enum):
    INSIDE_VA = "INSIDE_VA"
    RANGE = "RANGE"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    UNKNOWN = "UNKNOWN"


class AuctionBias(str, Enum):
    BALANCE = "BALANCE"
    RANGE_EXTENSION = "RANGE_EXTENSION"
    DIRECTIONAL_IMBALANCE = "DIRECTIONAL_IMBALANCE"
    UNKNOWN = "UNKNOWN"


class NpocStatus(str, Enum):
    UNTOUCHED = "UNTOUCHED"
    TOUCHED = "TOUCHED"
    UNKNOWN = "UNKNOWN"


@dataclass
class ProfileLevels:
    session_id: str
    start_ts: str
    end_ts: str

    high: float
    low: float
    open: float
    close: float

    poc: float
    vah: float
    val: float

    total_volume: float = 0.0
    value_area_pct: float = 0.70

    ib_high: Optional[float] = None
    ib_low: Optional[float] = None
    ib_range: Optional[float] = None

    session_anchor: Optional[str] = None
    session_timezone: Optional[str] = None
    session_open_local: Optional[str] = None
    session_open_utc: Optional[str] = None
    session_open_kyiv: Optional[str] = None
    next_session_open_utc: Optional[str] = None
    primary_logic: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class NakedPOC:
    session_id: str
    poc: float
    created_at: str
    status: str = NpocStatus.UNTOUCHED.value
    touched_at: Optional[str] = None
    distance_to_price: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AuctionContext:
    symbol: str
    timeframe: str
    current_price: Optional[float]

    current_session_id: Optional[str]
    previous_session_id: Optional[str]

    previous_poc: Optional[float]
    previous_vah: Optional[float]
    previous_val: Optional[float]
    previous_high: Optional[float]
    previous_low: Optional[float]

    current_open: Optional[float]
    open_relation: str
    auction_bias: str

    nearest_npoc: Optional[float]
    nearest_npoc_distance: Optional[float]
    naked_pocs: list[dict[str, Any]] = field(default_factory=list)

    ib_high: Optional[float] = None
    ib_low: Optional[float] = None
    ib_range: Optional[float] = None
    ib_extension_up: Optional[float] = None
    ib_extension_down: Optional[float] = None
    ib_extension_up_pct: Optional[float] = None
    ib_extension_down_pct: Optional[float] = None

    session_anchor: Optional[str] = None
    session_timezone: Optional[str] = None
    session_open_local: Optional[str] = None
    session_open_utc: Optional[str] = None
    session_open_kyiv: Optional[str] = None
    next_session_open_utc: Optional[str] = None
    is_primary_session_active: Optional[bool] = None
    primary_logic: Optional[str] = None
    secondary_anchors: list[str] = field(default_factory=list)
    exchange_open_reference: Optional[str] = None

    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    try:
        v = float(value)
    except (TypeError, ValueError):
        return None

    if pd.isna(v):
        return None

    return v


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower().strip() for c in df.columns]

    rename_map = {
        "datetime": "timestamp",
        "date": "timestamp",
        "time": "timestamp",
        "vol": "volume",
    }

    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    required = {"timestamp", "open", "high", "low", "close"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Profile engine missing required columns: {sorted(missing)}")

    if "volume" not in df.columns:
        df["volume"] = 1.0

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    return df


def _timestamp_to_python_datetime_utc(ts: Any) -> datetime:
    value = pd.Timestamp(ts)

    if value.tzinfo is None:
        value = value.tz_localize("UTC")
    else:
        value = value.tz_convert("UTC")

    return value.to_pydatetime()


def _session_id_from_timestamp(ts: pd.Timestamp) -> str:
    """
    Legacy UTC calendar-day session id.

    Kept for backward compatibility, but production TPO should use
    assign_symbol_sessions(), which respects instrument-specific session opens.
    """
    return ts.date().isoformat()


def assign_daily_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Legacy daily UTC sessions.

    Kept so older callers do not break. New auction-aware logic should call
    assign_symbol_sessions().
    """
    df = _normalize_columns(df)
    df["session_id"] = df["timestamp"].apply(_session_id_from_timestamp)
    return df


def assign_symbol_sessions(
    df: pd.DataFrame,
    *,
    symbol: str,
    session_config: SessionConfig | None = None,
) -> pd.DataFrame:
    """
    Assigns session_id using instrument-specific session opens.

    Examples:
    - GER40: 09:00 Europe/Berlin
    - NAS100/SPX500: 09:30 America/New_York
    - FX majors: 17:00 America/New_York rollover
    - XAUUSD: 18:00 America/New_York metals/Globex session
    - BTCUSD/ETHUSD: 00:00 UTC
    """
    df = _normalize_columns(df)
    config = session_config or get_session_config(symbol)

    session_rows: list[dict[str, Any]] = []

    for ts in df["timestamp"]:
        ts_utc = _timestamp_to_python_datetime_utc(ts)
        meta = compute_session_open_for_timestamp(ts_utc, config)
        session_rows.append(meta)

    meta_df = pd.DataFrame(session_rows)

    df["session_id"] = meta_df["current_session_id"].astype(str)
    df["session_anchor"] = meta_df["session_anchor"]
    df["session_timezone"] = meta_df["session_timezone"]
    df["session_open_local"] = meta_df["session_open_local"]
    df["session_open_utc"] = meta_df["session_open_utc"]
    df["session_open_kyiv"] = meta_df["session_open_kyiv"]
    df["next_session_open_utc"] = meta_df["next_session_open_utc"]
    df["is_primary_session_active"] = meta_df["is_primary_session_active"]
    df["primary_logic"] = meta_df["primary_logic"]
    df["secondary_anchors"] = meta_df["secondary_anchors"]
    df["exchange_open_reference"] = meta_df["exchange_open_reference"]

    return df


def _first_value(session_df: pd.DataFrame, column: str) -> Any:
    if session_df.empty or column not in session_df.columns:
        return None
    return session_df[column].iloc[0]


def _price_bin(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        raise ValueError("tick_size must be > 0")
    return round(round(price / tick_size) * tick_size, 10)


def build_volume_profile(
    session_df: pd.DataFrame,
    tick_size: float,
) -> dict[float, float]:
    """
    Approximate volume profile.

    If real bid/ask or TPO data is unavailable, this distributes each candle volume
    across price bins between low and high. This is not a replacement for true
    Market Profile, but it is good enough for auction context filtering.
    """
    profile: dict[float, float] = {}

    for row in session_df.itertuples(index=False):
        high = _safe_float(getattr(row, "high", None))
        low = _safe_float(getattr(row, "low", None))
        volume = _safe_float(getattr(row, "volume", None)) or 1.0

        if high is None or low is None:
            continue

        if high < low:
            high, low = low, high

        low_bin = _price_bin(low, tick_size)
        high_bin = _price_bin(high, tick_size)

        if high_bin == low_bin:
            profile[low_bin] = profile.get(low_bin, 0.0) + volume
            continue

        steps = int(round((high_bin - low_bin) / tick_size)) + 1
        steps = max(1, min(steps, 10_000))

        vol_per_bin = volume / steps

        for i in range(steps):
            price = round(low_bin + i * tick_size, 10)
            profile[price] = profile.get(price, 0.0) + vol_per_bin

    return profile


def compute_poc(profile: dict[float, float]) -> Optional[float]:
    if not profile:
        return None

    return max(profile.items(), key=lambda kv: kv[1])[0]


def compute_value_area(
    profile: dict[float, float],
    value_area_pct: float = 0.70,
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Returns POC, VAH, VAL.

    Algorithm:
    - Start from POC.
    - Expand one price level up/down by choosing side with more volume.
    - Continue until target value-area volume is reached.
    """
    if not profile:
        return None, None, None

    prices = sorted(profile.keys())
    total_volume = sum(profile.values())

    if total_volume <= 0:
        return None, None, None

    poc = compute_poc(profile)

    if poc is None:
        return None, None, None

    idx = prices.index(poc)
    included = {idx}
    included_volume = profile[poc]
    target_volume = total_volume * value_area_pct

    low_idx = idx
    high_idx = idx

    while included_volume < target_volume and (low_idx > 0 or high_idx < len(prices) - 1):
        next_low_idx = low_idx - 1 if low_idx > 0 else None
        next_high_idx = high_idx + 1 if high_idx < len(prices) - 1 else None

        low_volume = profile[prices[next_low_idx]] if next_low_idx is not None else -1
        high_volume = profile[prices[next_high_idx]] if next_high_idx is not None else -1

        if high_volume >= low_volume and next_high_idx is not None:
            high_idx = next_high_idx
            included.add(high_idx)
            included_volume += profile[prices[high_idx]]
        elif next_low_idx is not None:
            low_idx = next_low_idx
            included.add(low_idx)
            included_volume += profile[prices[low_idx]]
        else:
            break

    val = prices[min(included)]
    vah = prices[max(included)]

    return poc, vah, val


def compute_initial_balance(
    session_df: pd.DataFrame,
    minutes: int = 60,
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    if session_df.empty:
        return None, None, None

    start_ts = session_df["timestamp"].iloc[0]
    end_ts = start_ts + pd.Timedelta(minutes=minutes)

    ib_df = session_df[session_df["timestamp"] < end_ts]

    if ib_df.empty:
        return None, None, None

    ib_high = _safe_float(ib_df["high"].max())
    ib_low = _safe_float(ib_df["low"].min())

    if ib_high is None or ib_low is None:
        return None, None, None

    return ib_high, ib_low, abs(ib_high - ib_low)


def build_session_profile(
    session_df: pd.DataFrame,
    tick_size: float,
    value_area_pct: float = 0.70,
    ib_minutes: int = 60,
) -> Optional[ProfileLevels]:
    if session_df.empty:
        return None

    session_df = _normalize_columns(session_df)

    if "session_id" not in session_df.columns:
        session_df["session_id"] = session_df["timestamp"].apply(_session_id_from_timestamp)

    session_id = str(session_df["session_id"].iloc[0])

    profile = build_volume_profile(session_df, tick_size=tick_size)
    poc, vah, val = compute_value_area(profile, value_area_pct=value_area_pct)

    if poc is None or vah is None or val is None:
        return None

    high = _safe_float(session_df["high"].max())
    low = _safe_float(session_df["low"].min())
    open_price = _safe_float(session_df["open"].iloc[0])
    close_price = _safe_float(session_df["close"].iloc[-1])

    if high is None or low is None or open_price is None or close_price is None:
        return None

    ib_high, ib_low, ib_range = compute_initial_balance(session_df, minutes=ib_minutes)

    return ProfileLevels(
        session_id=session_id,
        start_ts=session_df["timestamp"].iloc[0].isoformat(),
        end_ts=session_df["timestamp"].iloc[-1].isoformat(),
        high=high,
        low=low,
        open=open_price,
        close=close_price,
        poc=poc,
        vah=vah,
        val=val,
        total_volume=float(session_df["volume"].sum()),
        value_area_pct=value_area_pct,
        ib_high=ib_high,
        ib_low=ib_low,
        ib_range=ib_range,
        session_anchor=_first_value(session_df, "session_anchor"),
        session_timezone=_first_value(session_df, "session_timezone"),
        session_open_local=_first_value(session_df, "session_open_local"),
        session_open_utc=_first_value(session_df, "session_open_utc"),
        session_open_kyiv=_first_value(session_df, "session_open_kyiv"),
        next_session_open_utc=_first_value(session_df, "next_session_open_utc"),
        primary_logic=_first_value(session_df, "primary_logic"),
    )


def build_profiles_by_session(
    df: pd.DataFrame,
    tick_size: float,
    value_area_pct: float = 0.70,
    ib_minutes: int = 60,
    *,
    symbol: str | None = None,
    session_config: SessionConfig | None = None,
) -> list[ProfileLevels]:
    if symbol:
        df = assign_symbol_sessions(df, symbol=symbol, session_config=session_config)
    else:
        df = assign_daily_sessions(df)

    profiles: list[ProfileLevels] = []

    for _, session_df in df.groupby("session_id", sort=True):
        profile = build_session_profile(
            session_df,
            tick_size=tick_size,
            value_area_pct=value_area_pct,
            ib_minutes=ib_minutes,
        )

        if profile is not None:
            profiles.append(profile)

    return profiles


def classify_open_relation(
    current_open: Optional[float],
    previous_profile: Optional[ProfileLevels],
) -> str:
    if current_open is None or previous_profile is None:
        return OpenRelation.UNKNOWN.value

    if previous_profile.val <= current_open <= previous_profile.vah:
        return OpenRelation.INSIDE_VA.value

    if previous_profile.low <= current_open <= previous_profile.high:
        return OpenRelation.RANGE.value

    return OpenRelation.OUT_OF_RANGE.value


def classify_auction_bias(open_relation: str) -> str:
    if open_relation == OpenRelation.INSIDE_VA.value:
        return AuctionBias.BALANCE.value

    if open_relation == OpenRelation.RANGE.value:
        return AuctionBias.RANGE_EXTENSION.value

    if open_relation == OpenRelation.OUT_OF_RANGE.value:
        return AuctionBias.DIRECTIONAL_IMBALANCE.value

    return AuctionBias.UNKNOWN.value


def find_naked_pocs(
    profiles: list[ProfileLevels],
    current_price: Optional[float] = None,
) -> list[NakedPOC]:
    naked: list[NakedPOC] = []

    for i, profile in enumerate(profiles[:-1]):
        poc = profile.poc
        touched = False
        touched_at: Optional[str] = None

        for future_profile in profiles[i + 1:]:
            if future_profile.low <= poc <= future_profile.high:
                touched = True
                touched_at = future_profile.session_id
                break

        if not touched:
            distance = None

            if current_price is not None:
                distance = round(abs(current_price - poc), 10)

            naked.append(
                NakedPOC(
                    session_id=profile.session_id,
                    poc=poc,
                    created_at=profile.end_ts,
                    status=NpocStatus.UNTOUCHED.value,
                    touched_at=touched_at,
                    distance_to_price=distance,
                )
            )

    naked.sort(key=lambda x: x.distance_to_price if x.distance_to_price is not None else float("inf"))

    return naked


def compute_ib_extension(
    current_session_df: pd.DataFrame,
    ib_high: Optional[float],
    ib_low: Optional[float],
    ib_range: Optional[float],
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if current_session_df.empty or ib_high is None or ib_low is None or not ib_range:
        return None, None, None, None

    high = _safe_float(current_session_df["high"].max())
    low = _safe_float(current_session_df["low"].min())

    if high is None or low is None:
        return None, None, None, None

    ext_up = max(0.0, high - ib_high)
    ext_down = max(0.0, ib_low - low)

    ext_up_pct = round(ext_up / ib_range, 4) if ib_range > 0 else None
    ext_down_pct = round(ext_down / ib_range, 4) if ib_range > 0 else None

    return ext_up, ext_down, ext_up_pct, ext_down_pct


def _empty_context(
    *,
    symbol: str,
    timeframe: str,
    note: str,
    session_config: SessionConfig | None = None,
) -> AuctionContext:
    config = session_config or get_session_config(symbol)

    return AuctionContext(
        symbol=symbol,
        timeframe=timeframe,
        current_price=None,
        current_session_id=None,
        previous_session_id=None,
        previous_poc=None,
        previous_vah=None,
        previous_val=None,
        previous_high=None,
        previous_low=None,
        current_open=None,
        open_relation=OpenRelation.UNKNOWN.value,
        auction_bias=AuctionBias.UNKNOWN.value,
        nearest_npoc=None,
        nearest_npoc_distance=None,
        session_anchor=config.session_anchor,
        session_timezone=config.timezone,
        primary_logic=config.primary_logic,
        secondary_anchors=list(config.secondary_anchors),
        exchange_open_reference=config.exchange_open_reference,
        notes=[note],
    )


def build_auction_context(
    df: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str = "15m",
    tick_size: float,
    value_area_pct: float = 0.70,
    ib_minutes: int = 60,
    session_config: SessionConfig | None = None,
) -> AuctionContext:
    """
    Main read-only auction context builder.

    Input:
    - dataframe with timestamp/open/high/low/close/volume
    - preferably enough history for several sessions

    Output:
    - AuctionContext ready to be added to journal/snapshot as passive diagnostics.

    Important:
    - Session boundaries are instrument-specific.
    - Do not use one universal midnight session for every symbol.
    """
    config = session_config or get_session_config(symbol)

    df = assign_symbol_sessions(df, symbol=symbol, session_config=config)

    if df.empty:
        return _empty_context(
            symbol=symbol,
            timeframe=timeframe,
            note="empty dataframe",
            session_config=config,
        )

    profiles = build_profiles_by_session(
        df,
        tick_size=tick_size,
        value_area_pct=value_area_pct,
        ib_minutes=ib_minutes,
        symbol=symbol,
        session_config=config,
    )

    if not profiles:
        return _empty_context(
            symbol=symbol,
            timeframe=timeframe,
            note="no valid session profiles",
            session_config=config,
        )

    current_session_id = str(df["session_id"].iloc[-1])
    current_session_df = df[df["session_id"] == current_session_id]

    current_open = _safe_float(current_session_df["open"].iloc[0]) if not current_session_df.empty else None
    current_price = _safe_float(df["close"].iloc[-1])

    previous_profile: Optional[ProfileLevels] = None

    completed_profiles = [p for p in profiles if p.session_id != current_session_id]

    if completed_profiles:
        previous_profile = completed_profiles[-1]

    open_relation = classify_open_relation(current_open, previous_profile)
    auction_bias = classify_auction_bias(open_relation)

    naked_pocs = find_naked_pocs(completed_profiles, current_price=current_price)
    nearest_npoc = naked_pocs[0] if naked_pocs else None

    current_profile = None

    for profile in profiles:
        if profile.session_id == current_session_id:
            current_profile = profile
            break

    ib_high = current_profile.ib_high if current_profile else None
    ib_low = current_profile.ib_low if current_profile else None
    ib_range = current_profile.ib_range if current_profile else None

    ext_up, ext_down, ext_up_pct, ext_down_pct = compute_ib_extension(
        current_session_df,
        ib_high,
        ib_low,
        ib_range,
    )

    notes: list[str] = []

    if open_relation == OpenRelation.INSIDE_VA.value:
        notes.append("Open inside previous value area: balance/open-auction risk is elevated.")

    if open_relation == OpenRelation.OUT_OF_RANGE.value:
        notes.append("Open outside previous range: directional imbalance potential is elevated.")

    if nearest_npoc is not None:
        notes.append("Nearest nPOC is available as interest zone, not as standalone entry.")

    session_anchor = _first_value(current_session_df, "session_anchor") or config.session_anchor
    session_timezone = _first_value(current_session_df, "session_timezone") or config.timezone
    session_open_local = _first_value(current_session_df, "session_open_local")
    session_open_utc = _first_value(current_session_df, "session_open_utc")
    session_open_kyiv = _first_value(current_session_df, "session_open_kyiv")
    next_session_open_utc = _first_value(current_session_df, "next_session_open_utc")
    is_primary_session_active = _first_value(current_session_df, "is_primary_session_active")

    return AuctionContext(
        symbol=symbol,
        timeframe=timeframe,
        current_price=current_price,
        current_session_id=current_session_id,
        previous_session_id=previous_profile.session_id if previous_profile else None,
        previous_poc=previous_profile.poc if previous_profile else None,
        previous_vah=previous_profile.vah if previous_profile else None,
        previous_val=previous_profile.val if previous_profile else None,
        previous_high=previous_profile.high if previous_profile else None,
        previous_low=previous_profile.low if previous_profile else None,
        current_open=current_open,
        open_relation=open_relation,
        auction_bias=auction_bias,
        nearest_npoc=nearest_npoc.poc if nearest_npoc else None,
        nearest_npoc_distance=nearest_npoc.distance_to_price if nearest_npoc else None,
        naked_pocs=[x.to_dict() for x in naked_pocs[:10]],
        ib_high=ib_high,
        ib_low=ib_low,
        ib_range=ib_range,
        ib_extension_up=ext_up,
        ib_extension_down=ext_down,
        ib_extension_up_pct=ext_up_pct,
        ib_extension_down_pct=ext_down_pct,
        session_anchor=session_anchor,
        session_timezone=session_timezone,
        session_open_local=session_open_local,
        session_open_utc=session_open_utc,
        session_open_kyiv=session_open_kyiv,
        next_session_open_utc=next_session_open_utc,
        is_primary_session_active=bool(is_primary_session_active)
        if is_primary_session_active is not None
        else None,
        primary_logic=config.primary_logic,
        secondary_anchors=list(config.secondary_anchors),
        exchange_open_reference=config.exchange_open_reference,
        notes=notes,
    )


def auction_context_to_signal_filters(context: AuctionContext) -> dict[str, Any]:
    """
    Converts auction context into passive signal-quality hints.

    This function does not block trades by itself. It only produces labels
    that can later be consumed by Telegram/quality tiers.
    """
    filters: dict[str, Any] = {
        "auction_context_available": True,
        "open_relation": context.open_relation,
        "auction_bias": context.auction_bias,
        "telegram_modifier": "NEUTRAL",
        "confidence_modifier": 0.0,
        "reasons": [],
        "session_anchor": context.session_anchor,
        "session_timezone": context.session_timezone,
        "session_open_utc": context.session_open_utc,
        "session_open_kyiv": context.session_open_kyiv,
        "primary_logic": context.primary_logic,
    }

    if context.open_relation == OpenRelation.INSIDE_VA.value:
        filters["telegram_modifier"] = "DOWNGRADE"
        filters["confidence_modifier"] = -0.15
        filters["reasons"].append("Open inside previous VA: directional edge reduced.")

    elif context.open_relation == OpenRelation.RANGE.value:
        filters["telegram_modifier"] = "NEUTRAL"
        filters["confidence_modifier"] = 0.0
        filters["reasons"].append("Open inside previous range but outside VA: normal auction context.")

    elif context.open_relation == OpenRelation.OUT_OF_RANGE.value:
        filters["telegram_modifier"] = "BOOST"
        filters["confidence_modifier"] = 0.10
        filters["reasons"].append("Open outside previous range: directional imbalance potential.")

    if context.nearest_npoc is not None:
        filters["reasons"].append("nPOC nearby: interest zone only; require LTF confirmation.")

    if context.ib_extension_up_pct is not None and context.ib_extension_up_pct >= 0.5:
        filters["reasons"].append("IB upside extension >= 0.5 IB.")

    if context.ib_extension_down_pct is not None and context.ib_extension_down_pct >= 0.5:
        filters["reasons"].append("IB downside extension >= 0.5 IB.")

    return filters