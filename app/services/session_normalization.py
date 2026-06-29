from __future__ import annotations

"""
Session Normalization Brain Update v1 for AI Market Analyst.

Purpose:
- Provide a lightweight session truth layer without rewriting tpo_store.
- Separate primary profile scope, synthetic open, prior value scope and reliability.
- Prevent broad OTD/readiness permissions from using the wrong session universe.

This module is intentionally dependency-light and safe for production rollout.
It does not fetch data, does not write files, and does not send Telegram.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


SESSION_NORMALIZATION_VERSION = "session-normalization-brain-v1.0"


@dataclass(frozen=True)
class SessionProfileConfig:
    asset_class: str
    primary_session: str
    session_scope: str
    prior_value_scope: str
    prior_range_scope: str
    open_event: str
    open_event_type: str
    active_participation_center: str
    synthetic_open_required: bool = False
    weekend_sensitive: bool = False
    holiday_sensitive: bool = True
    notes: str = ""


@dataclass(frozen=True)
class SessionNormalizationResult:
    version: str = SESSION_NORMALIZATION_VERSION
    symbol: str | None = None
    asset_class: str = "unknown"
    primary_session: str = "UNKNOWN"
    session_scope: str = "UNKNOWN"
    prior_value_scope: str = "UNKNOWN"
    prior_range_scope: str = "UNKNOWN"
    open_event: str = "UNKNOWN"
    open_event_type: str = "UNKNOWN"
    reference_profile_id: str | None = None
    active_participation_center: str = "UNKNOWN"

    profile_reliability_score: int = 70
    profile_reliability_state: str = "CAUTION"
    session_status: str = "UNKNOWN"
    holiday_mode: str = "NONE"
    weekend_flag: bool = False
    synthetic_open: bool = False
    synthetic_open_confirmed: bool = False

    true_otd_allowed: bool = True
    warnings: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _s(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, (dict, list, tuple, set)):
        return default
    text = str(value).strip()
    return text or default


def _u(value: Any, default: str = "") -> str:
    return _s(value, default).upper()


def _b(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "open"}:
        return True
    if text in {"0", "false", "no", "n", "off", "closed"}:
        return False
    return default


def _first_non_empty(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, tuple, set, dict)) and not value:
            continue
        return value
    return default


def _canonical_symbol(symbol: Any) -> str:
    raw = _u(symbol)
    aliases = {
        "^NDX": "NAS100",
        "NDX": "NAS100",
        "NQ": "NAS100",
        "NASDAQ100": "NAS100",
        "^GSPC": "SPX500",
        "SPX": "SPX500",
        "ES": "SPX500",
        "US500": "SPX500",
        "^GDAXI": "GER40",
        "DAX": "GER40",
        "DE40": "GER40",
        "XAU": "XAUUSD",
        "GOLD": "XAUUSD",
        "BTC": "BTCUSD",
        "BTCUSDT": "BTCUSD",
        "ETH": "ETHUSD",
        "ETHUSDT": "ETHUSD",
        "BRENT": "UKOIL",
        "BZ=F": "UKOIL",
        "UKOILSPOT": "UKOIL",
    }
    return aliases.get(raw, raw)


CONFIGS: dict[str, SessionProfileConfig] = {
    "NAS100": SessionProfileConfig(
        asset_class="us_index",
        primary_session="us_cash_rth",
        session_scope="NY_RTH",
        prior_value_scope="PRIOR_RTH_VALUE",
        prior_range_scope="PRIOR_RTH_RANGE",
        open_event="NY_CASH_OPEN",
        open_event_type="EXCHANGE_CASH_OPEN",
        active_participation_center="NEW_YORK",
        notes="RTH primary; overnight/globex context only.",
    ),
    "SPX500": SessionProfileConfig(
        asset_class="us_index",
        primary_session="us_cash_rth",
        session_scope="NY_RTH",
        prior_value_scope="PRIOR_RTH_VALUE",
        prior_range_scope="PRIOR_RTH_RANGE",
        open_event="NY_CASH_OPEN",
        open_event_type="EXCHANGE_CASH_OPEN",
        active_participation_center="NEW_YORK",
        notes="RTH primary; overnight/globex context only.",
    ),
    "GER40": SessionProfileConfig(
        asset_class="eu_index",
        primary_session="dax_cash_core",
        session_scope="XETRA_CASH",
        prior_value_scope="PRIOR_XETRA_CASH_VALUE",
        prior_range_scope="PRIOR_XETRA_CASH_RANGE",
        open_event="XETRA_CASH_OPEN",
        open_event_type="EXCHANGE_CASH_OPEN",
        active_participation_center="EUROPE",
        notes="Xetra/cash core primary; Eurex/extended context only.",
    ),
    "XAUUSD": SessionProfileConfig(
        asset_class="precious_metal_spot",
        primary_session="spot_24h_roll",
        session_scope="XAU_24H_WITH_SYNTHETIC_SESSIONS",
        prior_value_scope="PRIOR_24H_ROLL_VALUE",
        prior_range_scope="PRIOR_24H_ROLL_RANGE",
        open_event="LONDON_OR_NY_SYNTHETIC_OPEN",
        open_event_type="SYNTHETIC_LIQUIDITY_OPEN",
        active_participation_center="LONDON_NY",
        synthetic_open_required=True,
        notes="24h background; London/NY synthetic opens for intraday auction-state.",
    ),
    "EURUSD": SessionProfileConfig(
        asset_class="fx_major",
        primary_session="fx_24h_roll",
        session_scope="FX_24H_WITH_LONDON_NY_SYNTHETIC",
        prior_value_scope="PRIOR_24H_ROLL_VALUE",
        prior_range_scope="PRIOR_24H_ROLL_RANGE",
        open_event="LONDON_OR_NY_SYNTHETIC_OPEN",
        open_event_type="SYNTHETIC_LIQUIDITY_OPEN",
        active_participation_center="LONDON_NY",
        synthetic_open_required=True,
    ),
    "GBPUSD": SessionProfileConfig(
        asset_class="fx_major",
        primary_session="fx_24h_roll",
        session_scope="FX_24H_WITH_LONDON_NY_SYNTHETIC",
        prior_value_scope="PRIOR_24H_ROLL_VALUE",
        prior_range_scope="PRIOR_24H_ROLL_RANGE",
        open_event="LONDON_OR_NY_SYNTHETIC_OPEN",
        open_event_type="SYNTHETIC_LIQUIDITY_OPEN",
        active_participation_center="LONDON_NY",
        synthetic_open_required=True,
    ),
    "USDCHF": SessionProfileConfig(
        asset_class="fx_major",
        primary_session="fx_24h_roll",
        session_scope="FX_24H_WITH_LONDON_NY_SYNTHETIC",
        prior_value_scope="PRIOR_24H_ROLL_VALUE",
        prior_range_scope="PRIOR_24H_ROLL_RANGE",
        open_event="LONDON_OR_NY_SYNTHETIC_OPEN",
        open_event_type="SYNTHETIC_LIQUIDITY_OPEN",
        active_participation_center="LONDON_NY",
        synthetic_open_required=True,
    ),
    "USDJPY": SessionProfileConfig(
        asset_class="fx_major_asia",
        primary_session="fx_24h_roll",
        session_scope="FX_24H_WITH_TOKYO_LONDON_NY_SYNTHETIC",
        prior_value_scope="PRIOR_24H_ROLL_VALUE",
        prior_range_scope="PRIOR_24H_ROLL_RANGE",
        open_event="TOKYO_OR_LONDON_OR_NY_SYNTHETIC_OPEN",
        open_event_type="SYNTHETIC_LIQUIDITY_OPEN",
        active_participation_center="TOKYO_LONDON_NY",
        synthetic_open_required=True,
    ),
    "USDCAD": SessionProfileConfig(
        asset_class="fx_major_commodity",
        primary_session="fx_24h_roll",
        session_scope="FX_24H_WITH_NY_OIL_CONTEXT",
        prior_value_scope="PRIOR_24H_ROLL_VALUE",
        prior_range_scope="PRIOR_24H_ROLL_RANGE",
        open_event="NY_SYNTHETIC_OPEN",
        open_event_type="SYNTHETIC_LIQUIDITY_OPEN",
        active_participation_center="NEW_YORK",
        synthetic_open_required=True,
    ),
    "AUDUSD": SessionProfileConfig(
        asset_class="fx_major_asia",
        primary_session="fx_24h_roll",
        session_scope="FX_24H_WITH_ASIA_LONDON_SYNTHETIC",
        prior_value_scope="PRIOR_24H_ROLL_VALUE",
        prior_range_scope="PRIOR_24H_ROLL_RANGE",
        open_event="ASIA_OR_LONDON_SYNTHETIC_OPEN",
        open_event_type="SYNTHETIC_LIQUIDITY_OPEN",
        active_participation_center="ASIA_LONDON",
        synthetic_open_required=True,
    ),
    "BTCUSD": SessionProfileConfig(
        asset_class="crypto",
        primary_session="crypto_utc_day",
        session_scope="UTC_CRYPTO_DAY_WITH_SYNTHETIC_SESSIONS",
        prior_value_scope="PRIOR_UTC_DAY_VALUE",
        prior_range_scope="PRIOR_UTC_DAY_RANGE",
        open_event="UTC_DAY_OR_LONDON_NY_SYNTHETIC_OPEN",
        open_event_type="CONTINUOUS_SYNTHETIC_OPEN",
        active_participation_center="GLOBAL",
        synthetic_open_required=False,
        weekend_sensitive=True,
        holiday_sensitive=False,
    ),
    "ETHUSD": SessionProfileConfig(
        asset_class="crypto",
        primary_session="crypto_utc_day",
        session_scope="UTC_CRYPTO_DAY_WITH_SYNTHETIC_SESSIONS",
        prior_value_scope="PRIOR_UTC_DAY_VALUE",
        prior_range_scope="PRIOR_UTC_DAY_RANGE",
        open_event="UTC_DAY_OR_LONDON_NY_SYNTHETIC_OPEN",
        open_event_type="CONTINUOUS_SYNTHETIC_OPEN",
        active_participation_center="GLOBAL",
        synthetic_open_required=False,
        weekend_sensitive=True,
        holiday_sensitive=False,
    ),
    "UKOIL": SessionProfileConfig(
        asset_class="energy",
        primary_session="ice_brent_day",
        session_scope="ICE_BRENT_DAY_WITH_LONDON_CONTEXT",
        prior_value_scope="PRIOR_ICE_FULL_SESSION_VALUE",
        prior_range_scope="PRIOR_ICE_FULL_SESSION_RANGE",
        open_event="LONDON_MORNING_SYNTHETIC_OPEN",
        open_event_type="SYNTHETIC_LIQUIDITY_OPEN",
        active_participation_center="LONDON",
        synthetic_open_required=True,
    ),
}


def _config_for(symbol: str) -> SessionProfileConfig:
    return CONFIGS.get(
        symbol,
        SessionProfileConfig(
            asset_class="unknown",
            primary_session="unknown",
            session_scope="UNKNOWN",
            prior_value_scope="UNKNOWN",
            prior_range_scope="UNKNOWN",
            open_event="UNKNOWN",
            open_event_type="UNKNOWN",
            active_participation_center="UNKNOWN",
            synthetic_open_required=False,
            notes="Unknown symbol; conservative fallback.",
        ),
    )


def _is_weekend(now: datetime | None = None) -> bool:
    dt = now or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.weekday() >= 5


def _extract_symbol(symbol: Any, context: dict[str, Any], filters: dict[str, Any], record: dict[str, Any]) -> str:
    return _canonical_symbol(
        _first_non_empty(
            symbol,
            context.get("symbol"),
            context.get("instrument"),
            filters.get("symbol"),
            filters.get("instrument"),
            record.get("symbol"),
            record.get("instrument"),
            default="",
        )
    )


def _session_status_from_inputs(context: dict[str, Any], filters: dict[str, Any], record: dict[str, Any]) -> str:
    status = _u(
        _first_non_empty(
            context.get("session_status"),
            context.get("market_status"),
            filters.get("session_status"),
            filters.get("market_status"),
            record.get("session_status"),
            record.get("market_status"),
            default="UNKNOWN",
        )
    )
    if status in {"OPEN", "NORMAL", "NORMAL_SESSION"}:
        return "NORMAL_SESSION"
    if status in {"MARKET_CLOSED", "CLOSED", "HOLIDAY", "US_HOLIDAY"}:
        return "MARKET_CLOSED"
    if status in {"HALF_DAY", "EARLY_CLOSE"}:
        return "HALF_DAY"
    if status in {"STALE_DATA", "DATA_STALE", "PROVIDER_ERROR", "NO_DATA"}:
        return "PROFILE_UNRELIABLE"
    return status or "UNKNOWN"


def _holiday_mode(context: dict[str, Any], filters: dict[str, Any], record: dict[str, Any]) -> str:
    mode = _u(
        _first_non_empty(
            context.get("holiday_mode"),
            context.get("holiday_status"),
            filters.get("holiday_mode"),
            filters.get("holiday_status"),
            record.get("holiday_mode"),
            record.get("holiday_status"),
            default="NONE",
        )
    )
    return mode or "NONE"


def _synthetic_open_confirmed(context: dict[str, Any], filters: dict[str, Any], record: dict[str, Any]) -> bool:
    explicit = _first_non_empty(
        context.get("synthetic_open_confirmed"),
        context.get("synthetic_session_boundary"),
        context.get("new_session_boundary"),
        filters.get("synthetic_open_confirmed"),
        filters.get("synthetic_session_boundary"),
        filters.get("new_session_boundary"),
        record.get("synthetic_open_confirmed"),
        record.get("synthetic_session_boundary"),
        record.get("new_session_boundary"),
        default=None,
    )
    if explicit is not None:
        return _b(explicit, False)

    label = _u(
        _first_non_empty(
            context.get("session_label"),
            context.get("session"),
            filters.get("session_label"),
            filters.get("session"),
            record.get("session_label"),
            record.get("session"),
            default="",
        )
    )
    return any(token in label for token in {"LONDON", "NY", "NEW_YORK", "TOKYO", "ASIA", "UTC"})


def _score_and_state(
    *,
    cfg: SessionProfileConfig,
    session_status: str,
    holiday_mode: str,
    weekend_flag: bool,
    synthetic_confirmed: bool,
) -> tuple[int, str, list[str], list[str], list[str]]:
    score = 88
    warnings: list[str] = []
    blockers: list[str] = []
    reasons: list[str] = []

    if session_status == "MARKET_CLOSED":
        return 0, "MARKET_CLOSED", warnings, ["market_closed_session_scope"], ["primary session is closed"]

    if session_status == "PROFILE_UNRELIABLE":
        return 45, "PROFILE_UNRELIABLE", warnings, ["profile_unreliable"], ["provider/session profile is unreliable"]

    if session_status == "HALF_DAY":
        score = min(score, 62)
        warnings.append("half_day_profile_downgrade")
        reasons.append("half-day can distort IB/day-type interpretation")

    if holiday_mode not in {"", "NONE", "NORMAL"} and cfg.holiday_sensitive:
        score = min(score, 64)
        warnings.append(f"holiday_mode_{holiday_mode.lower()}")
        reasons.append("holiday mode lowers session reliability")

    if cfg.weekend_sensitive and weekend_flag:
        score = min(score, 72)
        warnings.append("crypto_weekend_liquidity_downgrade")
        reasons.append("weekend liquidity can distort crypto auction-state")

    if cfg.synthetic_open_required and not synthetic_confirmed:
        score = min(score, 68)
        warnings.append("synthetic_open_unconfirmed")
        reasons.append("synthetic open is required for this asset class but was not explicitly confirmed")

    if score >= 85:
        state = "NORMAL"
    elif score >= 70:
        state = "CAUTION"
    elif score >= 55:
        state = "RESEARCH_ONLY"
    else:
        state = "SUPPRESS"

    return score, state, warnings, blockers, reasons


def resolve_session_context(
    symbol: Any = None,
    *,
    context: dict[str, Any] | None = None,
    filters: dict[str, Any] | None = None,
    record: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    ctx = context if isinstance(context, dict) else {}
    flt = filters if isinstance(filters, dict) else {}
    rec = record if isinstance(record, dict) else {}

    canonical = _extract_symbol(symbol, ctx, flt, rec)
    cfg = _config_for(canonical)

    session_status = _session_status_from_inputs(ctx, flt, rec)
    holiday_mode = _holiday_mode(ctx, flt, rec)
    weekend_flag = _is_weekend(now)
    synthetic_confirmed = _synthetic_open_confirmed(ctx, flt, rec)
    synthetic_open = bool(cfg.synthetic_open_required or cfg.open_event_type.startswith("SYNTHETIC") or "SYNTHETIC" in cfg.open_event_type)

    score, state, warnings, blockers, reasons = _score_and_state(
        cfg=cfg,
        session_status=session_status,
        holiday_mode=holiday_mode,
        weekend_flag=weekend_flag,
        synthetic_confirmed=synthetic_confirmed,
    )

    open_location = _u(
        _first_non_empty(
            ctx.get("open_location"),
            flt.get("open_location"),
            rec.get("open_location"),
            default="UNKNOWN",
        )
    )
    value_test = _b(
        _first_non_empty(
            ctx.get("value_test_occurred"),
            flt.get("value_test_occurred"),
            rec.get("value_test_occurred"),
            default=False,
        ),
        False,
    )

    outside_or_edge_value = open_location in {
        "OPEN_ABOVE_VALUE_INSIDE_RANGE",
        "OPEN_BELOW_VALUE_INSIDE_RANGE",
        "OPEN_ABOVE_RANGE",
        "OPEN_BELOW_RANGE",
    }

    true_otd_allowed = bool(
        score >= 70
        and outside_or_edge_value
        and value_test
        and session_status not in {"MARKET_CLOSED", "PROFILE_UNRELIABLE", "HALF_DAY"}
    )

    if not outside_or_edge_value:
        warnings.append("true_otd_not_allowed_open_not_outside_value")
    if score < 70:
        warnings.append("true_otd_not_allowed_profile_reliability_below_70")

    return SessionNormalizationResult(
        symbol=canonical or None,
        asset_class=cfg.asset_class,
        primary_session=cfg.primary_session,
        session_scope=cfg.session_scope,
        prior_value_scope=cfg.prior_value_scope,
        prior_range_scope=cfg.prior_range_scope,
        open_event=cfg.open_event,
        open_event_type=cfg.open_event_type,
        reference_profile_id=_s(
            _first_non_empty(
                ctx.get("reference_profile_id"),
                flt.get("reference_profile_id"),
                rec.get("reference_profile_id"),
                default=f"{canonical}:{cfg.prior_value_scope}" if canonical else None,
            )
        ) or None,
        active_participation_center=cfg.active_participation_center,
        profile_reliability_score=score,
        profile_reliability_state=state,
        session_status=session_status,
        holiday_mode=holiday_mode,
        weekend_flag=weekend_flag,
        synthetic_open=synthetic_open,
        synthetic_open_confirmed=synthetic_confirmed,
        true_otd_allowed=true_otd_allowed,
        warnings=sorted(set(warnings)),
        blockers=sorted(set(blockers)),
        reasons=sorted(set(reasons)),
    ).to_dict()
