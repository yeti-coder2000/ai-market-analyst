from __future__ import annotations

from datetime import UTC, datetime


def is_trading_day(dt: datetime | None = None) -> bool:
    ref = dt or datetime.now(UTC)
    return ref.weekday() < 5


def get_market_session_status(dt: datetime | None = None) -> str:
    ref = dt or datetime.now(UTC)

    if ref.weekday() >= 5:
        return "WEEKEND_CLOSED"

    return "TRADING_DAY"