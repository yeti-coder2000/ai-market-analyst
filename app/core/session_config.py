from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any
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

    asset_class: str = "UNKNOWN"

    # True only for crypto in our current universe.
    trades_weekends: bool = False

    # If True, battle logic is allowed only inside primary session hours.
    # Used for cash/session-anchored instruments.
    enforce_primary_session_hours: bool = False
    close_time: str | None = None

    # Sunday reopen for 24/5 markets.
    # FX: 17:00 NY
    # Metals: 18:00 NY
    sunday_reopen_time: str | None = None
    sunday_reopen_timezone: str | None = None

    # Data freshness guard.
    # If latest bar is older than this threshold and market is expected open,
    # signal permission should be downgraded/blocked.
    stale_bar_threshold_minutes: int = 90


SESSION_CONFIG: dict[str, SessionConfig] = {
    "GER40": SessionConfig(
        symbol="GER40",
        session_anchor="XETRA_CASH_OPEN",
        timezone="Europe/Berlin",
        open_time="09:00",
        close_time="17:30",
        primary_logic="TPO_DAY_OPEN",
        asset_class="INDEX_CASH",
        trades_weekends=False,
        enforce_primary_session_hours=True,
        stale_bar_threshold_minutes=90,
    ),
    "NAS100": SessionConfig(
        symbol="NAS100",
        session_anchor="US_CASH_OPEN",
        timezone="America/New_York",
        open_time="09:30",
        close_time="16:00",
        primary_logic="TPO_DAY_OPEN",
        asset_class="US_INDEX_CASH",
        trades_weekends=False,
        enforce_primary_session_hours=True,
        stale_bar_threshold_minutes=90,
    ),
    "SPX500": SessionConfig(
        symbol="SPX500",
        session_anchor="US_CASH_OPEN",
        timezone="America/New_York",
        open_time="09:30",
        close_time="16:00",
        primary_logic="TPO_DAY_OPEN",
        asset_class="US_INDEX_CASH",
        trades_weekends=False,
        enforce_primary_session_hours=True,
        stale_bar_threshold_minutes=90,
    ),
    "UKOIL": SessionConfig(
        symbol="UKOIL",
        session_anchor="NY_RISK_SESSION_OPEN",
        timezone="America/New_York",
        open_time="09:30",
        close_time="17:00",
        primary_logic="INTRADAY_TPO_ANCHOR",
        exchange_open_reference="ICE_BRENT_ELECTRONIC",
        asset_class="ENERGY",
        trades_weekends=False,
        enforce_primary_session_hours=True,
        stale_bar_threshold_minutes=120,
    ),
    "XAUUSD": SessionConfig(
        symbol="XAUUSD",
        session_anchor="GLOBEX_METALS_OPEN",
        timezone="America/New_York",
        open_time="18:00",
        primary_logic="DAILY_PROFILE_OPEN",
        secondary_anchors=("LONDON_OPEN", "NY_CASH_OPEN"),
        asset_class="METALS",
        trades_weekends=False,
        enforce_primary_session_hours=False,
        sunday_reopen_time="18:00",
        sunday_reopen_timezone="America/New_York",
        stale_bar_threshold_minutes=120,
    ),
    "EURUSD": SessionConfig(
        symbol="EURUSD",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("LONDON_OPEN", "NY_CASH_OPEN"),
        asset_class="FX",
        trades_weekends=False,
        enforce_primary_session_hours=False,
        sunday_reopen_time="17:00",
        sunday_reopen_timezone="America/New_York",
        stale_bar_threshold_minutes=120,
    ),
    "GBPUSD": SessionConfig(
        symbol="GBPUSD",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("LONDON_OPEN", "NY_CASH_OPEN"),
        asset_class="FX",
        trades_weekends=False,
        enforce_primary_session_hours=False,
        sunday_reopen_time="17:00",
        sunday_reopen_timezone="America/New_York",
        stale_bar_threshold_minutes=120,
    ),
    "USDJPY": SessionConfig(
        symbol="USDJPY",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("TOKYO_OPEN", "LONDON_OPEN", "NY_CASH_OPEN"),
        asset_class="FX",
        trades_weekends=False,
        enforce_primary_session_hours=False,
        sunday_reopen_time="17:00",
        sunday_reopen_timezone="America/New_York",
        stale_bar_threshold_minutes=120,
    ),
    "USDCHF": SessionConfig(
        symbol="USDCHF",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("LONDON_OPEN", "NY_CASH_OPEN"),
        asset_class="FX",
        trades_weekends=False,
        enforce_primary_session_hours=False,
        sunday_reopen_time="17:00",
        sunday_reopen_timezone="America/New_York",
        stale_bar_threshold_minutes=120,
    ),
    "USDCAD": SessionConfig(
        symbol="USDCAD",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("NY_CASH_OPEN",),
        asset_class="FX",
        trades_weekends=False,
        enforce_primary_session_hours=False,
        sunday_reopen_time="17:00",
        sunday_reopen_timezone="America/New_York",
        stale_bar_threshold_minutes=120,
    ),
    "AUDUSD": SessionConfig(
        symbol="AUDUSD",
        session_anchor="FX_ROLLOVER",
        timezone="America/New_York",
        open_time="17:00",
        primary_logic="FX_DAILY_PROFILE_OPEN",
        secondary_anchors=("ASIA_OPEN", "LONDON_OPEN", "NY_CASH_OPEN"),
        asset_class="FX",
        trades_weekends=False,
        enforce_primary_session_hours=False,
        sunday_reopen_time="17:00",
        sunday_reopen_timezone="America/New_York",
        stale_bar_threshold_minutes=120,
    ),
    "BTCUSD": SessionConfig(
        symbol="BTCUSD",
        session_anchor="UTC_DAILY_RESET",
        timezone="UTC",
        open_time="00:00",
        primary_logic="CRYPTO_DAILY_PROFILE_OPEN",
        secondary_anchors=("NY_CASH_OPEN",),
        asset_class="CRYPTO",
        trades_weekends=True,
        enforce_primary_session_hours=False,
        stale_bar_threshold_minutes=90,
    ),
    "ETHUSD": SessionConfig(
        symbol="ETHUSD",
        session_anchor="UTC_DAILY_RESET",
        timezone="UTC",
        open_time="00:00",
        primary_logic="CRYPTO_DAILY_PROFILE_OPEN",
        secondary_anchors=("NY_CASH_OPEN",),
        asset_class="CRYPTO",
        trades_weekends=True,
        enforce_primary_session_hours=False,
        stale_bar_threshold_minutes=90,
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
            asset_class="UNKNOWN",
            trades_weekends=False,
            enforce_primary_session_hours=False,
            stale_bar_threshold_minutes=120,
        )

    return SESSION_CONFIG[key]


def _parse_hhmm(value: str) -> time:
    hour_str, minute_str = value.split(":", 1)
    return time(hour=int(hour_str), minute=int(minute_str))


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def compute_session_open_for_timestamp(
    timestamp_utc: datetime,
    config: SessionConfig,
) -> dict[str, Any]:
    timestamp_utc = _ensure_utc(timestamp_utc)

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

    session_open_utc = candidate_local.astimezone(timezone.utc)
    session_open_kyiv = candidate_local.astimezone(kyiv_tz)

    next_session_open_utc = (
        candidate_local + timedelta(days=1)
    ).astimezone(timezone.utc)

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


def _is_weekend_closed(now_utc: datetime, config: SessionConfig) -> tuple[bool, str | None]:
    if config.trades_weekends:
        return False, None

    check_tz_name = config.sunday_reopen_timezone or config.timezone
    local_now = now_utc.astimezone(ZoneInfo(check_tz_name))
    weekday = local_now.weekday()  # Monday=0, Sunday=6

    if weekday == 5:
        return True, "weekend_closed_saturday"

    if weekday == 6:
        # FX/metals can reopen Sunday evening. Cash indices stay closed.
        if not config.sunday_reopen_time:
            return True, "weekend_closed_sunday"

        reopen_time = _parse_hhmm(config.sunday_reopen_time)
        reopen_local = datetime.combine(
            local_now.date(),
            reopen_time,
            tzinfo=ZoneInfo(check_tz_name),
        )

        if local_now < reopen_local:
            return True, "weekend_closed_before_sunday_reopen"

    return False, None


def _is_outside_primary_session_hours(
    now_utc: datetime,
    config: SessionConfig,
) -> tuple[bool, str | None]:
    if not config.enforce_primary_session_hours:
        return False, None

    if not config.close_time:
        return False, None

    session_tz = ZoneInfo(config.timezone)
    local_now = now_utc.astimezone(session_tz)

    open_local = datetime.combine(
        local_now.date(),
        _parse_hhmm(config.open_time),
        tzinfo=session_tz,
    )
    close_local = datetime.combine(
        local_now.date(),
        _parse_hhmm(config.close_time),
        tzinfo=session_tz,
    )

    if local_now < open_local:
        return True, "before_primary_session_open"

    if local_now > close_local:
        return True, "after_primary_session_close"

    return False, None


def evaluate_market_state(
    *,
    now_utc: datetime,
    last_bar_ts_utc: datetime | None,
    config: SessionConfig,
) -> dict[str, Any]:
    now_utc = _ensure_utc(now_utc)

    closed, closed_reason = _is_weekend_closed(now_utc, config)

    if not closed:
        outside_hours, hours_reason = _is_outside_primary_session_hours(now_utc, config)
        if outside_hours:
            closed = True
            closed_reason = hours_reason

    data_age_minutes: float | None = None
    data_is_stale = False

    if last_bar_ts_utc is not None:
        last_bar_ts_utc = _ensure_utc(last_bar_ts_utc)
        data_age_minutes = round((now_utc - last_bar_ts_utc).total_seconds() / 60.0, 2)
        data_is_stale = data_age_minutes > config.stale_bar_threshold_minutes
    else:
        data_is_stale = True

    if closed and data_is_stale:
        market_status = "MARKET_CLOSED_AND_STALE"
    elif closed:
        market_status = "MARKET_CLOSED"
    elif data_is_stale:
        market_status = "STALE_DATA"
    else:
        market_status = "OPEN"

    return {
        "asset_class": config.asset_class,
        "market_is_open": not closed,
        "market_status": market_status,
        "market_closed_reason": closed_reason,
        "market_data_is_stale": data_is_stale,
        "market_data_age_minutes": data_age_minutes,
        "last_bar_timestamp_utc": last_bar_ts_utc.isoformat() if last_bar_ts_utc else None,
        "stale_bar_threshold_minutes": config.stale_bar_threshold_minutes,
    }