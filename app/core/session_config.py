from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo


KYIV_TZ = "Europe/Kyiv"


@dataclass(frozen=True)
class SessionConfig:
    symbol: str
    session_anchor: str
    timezone: str
    open_time: str
    primary_logic: str
    secondary_anchors: tuple[str, ...] = ()
    exchange_open_reference: str | None = None


SESSION_CONFIG: dict[str, SessionConfig] = {
    "GER40": SessionConfig(
        symbol="GER40",
        session_anchor="XETRA_CASH_OPEN",
        timezone="Europe/Berlin",
        open_time="09:00",
        primary_logic="TPO_DAY_OPEN",
    ),
    "NAS100": SessionConfig(
        symbol="NAS100",
        session_anchor="US_CASH_OPEN",
        timezone="America/New_York",
        open_time="09:30",
        primary_logic="TPO_DAY_OPEN",
    ),
    "SPX500": SessionConfig(
        symbol="SPX500",
        session_anchor="US_CASH_OPEN",
        timezone="America/New_York",
        open_time="09:30",
        primary_logic="TPO_DAY_OPEN",
    ),
    "UKOIL": SessionConfig(
        symbol="UKOIL",
        session_anchor="NY_RISK_SESSION_OPEN",
        timezone="America/New_York",
        open_time="09:30",
        primary_logic="INTRADAY_TPO_ANCHOR",
        exchange_open_reference="ICE_BRENT_ELECTRONIC",
    ),
    "XAUUSD": SessionConfig(
        symbol="XAUUSD",
        session_anchor="GLOBEX_METALS_OPEN",
        timezone="America/New_York",
        open_time="18:00",
        primary_logic="DAILY_PROFILE_OPEN",
        secondary_anchors=("LONDON_OPEN", "NY_CASH_OPEN"),
    ),
    "EURUSD": SessionConfig(
        symbol="EURUSD",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("LONDON_OPEN", "NY_CASH_OPEN"),
    ),
    "GBPUSD": SessionConfig(
        symbol="GBPUSD",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("LONDON_OPEN", "NY_CASH_OPEN"),
    ),
    "USDJPY": SessionConfig(
        symbol="USDJPY",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("TOKYO_OPEN", "LONDON_OPEN", "NY_CASH_OPEN"),
    ),
    "USDCHF": SessionConfig(
        symbol="USDCHF",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("LONDON_OPEN", "NY_CASH_OPEN"),
    ),
    "USDCAD": SessionConfig(
        symbol="USDCAD",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("NY_CASH_OPEN",),
    ),
    "AUDUSD": SessionConfig(
        symbol="AUDUSD",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("ASIA_OPEN", "LONDON_OPEN", "NY_CASH_OPEN"),
    ),
    "BTCUSD": SessionConfig(
        symbol="BTCUSD",
        session_anchor="UTC_DAILY_RESET",
        timezone="UTC",
        open_time="00:00",
        primary_logic="CRYPTO_DAILY_PROFILE_OPEN",
        secondary_anchors=("NY_CASH_OPEN",),
    ),
    "ETHUSD": SessionConfig(
        symbol="ETHUSD",
        session_anchor="UTC_DAILY_RESET",
        timezone="UTC",
        open_time="00:00",
        primary_logic="CRYPTO_DAILY_PROFILE_OPEN",
        secondary_anchors=("NY_CASH_OPEN",),
    ),
}


def get_session_config(symbol: str) -> SessionConfig:
    key = str(symbol).upper().strip()
    if key not in SESSION_CONFIG:
        return SessionConfig(
            symbol=key,
            session_anchor="UTC_DAILY_RESET",
            timezone="UTC",
            open_time="00:00",
            primary_logic="DEFAULT_UTC_DAILY_PROFILE_OPEN",
        )
    return SESSION_CONFIG[key]


def _parse_hhmm(value: str) -> time:
    hour_str, minute_str = value.split(":", 1)
    return time(hour=int(hour_str), minute=int(minute_str))


def compute_session_open_for_timestamp(
    timestamp_utc: datetime,
    config: SessionConfig,
) -> dict[str, str | bool]:
    if timestamp_utc.tzinfo is None:
        timestamp_utc = timestamp_utc.replace(tzinfo=ZoneInfo("UTC"))

    timestamp_utc = timestamp_utc.astimezone(ZoneInfo("UTC"))
    session_tz = ZoneInfo(config.timezone)
    kyiv_tz = ZoneInfo(KYIV_TZ)

    local_dt = timestamp_utc.astimezone(session_tz)
    open_local_time = _parse_hhmm(config.open_time)

    candidate_local = datetime.combine(
        local_dt.date(),
        open_local_time,
        tzinfo=session_tz,
    )

    if local_dt < candidate_local:
        candidate_local = candidate_local - timedelta(days=1)

    session_open_utc = candidate_local.astimezone(ZoneInfo("UTC"))
    session_open_kyiv = candidate_local.astimezone(kyiv_tz)

    next_session_open_utc = (
        candidate_local + timedelta(days=1)
    ).astimezone(ZoneInfo("UTC"))

    return {
        "session_anchor": config.session_anchor,
        "session_timezone": config.timezone,
        "session_open_local": candidate_local.isoformat(),
        "session_open_utc": session_open_utc.isoformat(),
        "session_open_kyiv": session_open_kyiv.isoformat(),
        "current_session_id": f"{candidate_local.date().isoformat()}_{config.session_anchor}",
        "next_session_open_utc": next_session_open_utc.isoformat(),
        "is_primary_session_active": timestamp_utc >= session_open_utc,
        "primary_logic": config.primary_logic,
        "secondary_anchors": list(config.secondary_anchors),
        "exchange_open_reference": config.exchange_open_reference,
    }