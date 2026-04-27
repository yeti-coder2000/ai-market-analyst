from __future__ import annotations

from enum import Enum


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


class MarketState(str, Enum):
    TREND = "TREND"
    BALANCE = "BALANCE"
    TRANSITION = "TRANSITION"


class SetupStatus(str, Enum):
    ACTIVE = "ACTIVE"
    WAITING = "WAITING"
    INVALID = "INVALID"
    NONE = "NONE"


class DataQuality(str, Enum):
    OK = "OK"
    WEAK = "WEAK"
    BAD = "BAD"


class Instrument(str, Enum):
    # core
    XAUUSD = "XAUUSD"
    EURUSD = "EURUSD"
    GBPUSD = "GBPUSD"
    BTCUSD = "BTCUSD"
    ETHUSD = "ETHUSD"

    # fx_major expansion
    USDJPY = "USDJPY"
    USDCHF = "USDCHF"
    USDCAD = "USDCAD"
    AUDUSD = "AUDUSD"

    # optional future fx reserve
    NZDUSD = "NZDUSD"
    EURJPY = "EURJPY"
    GBPJPY = "GBPJPY"
    AUDJPY = "AUDJPY"

    # indices / commodities reserve
    # NOTE:
    # These are kept in enum for future multi-provider support.
    # Do not route them to TwelveData until we have a correct provider mapping.
    UKOIL = "UKOIL"
    GER40 = "GER40"
    NAS100 = "NAS100"
    SPX500 = "SPX500"

    # optional / reserve
    DXY = "DXY"


class Timeframe(str, Enum):
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"