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
    XAUUSD = "XAUUSD"
    EURUSD = "EURUSD"
    GBPUSD = "GBPUSD"
    BTCUSD = "BTCUSD"
    ETHUSD = "ETHUSD"


class Timeframe(str, Enum):
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"