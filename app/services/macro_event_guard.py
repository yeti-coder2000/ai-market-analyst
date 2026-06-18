from __future__ import annotations

"""
Macro event guard for AI Market Analyst.

Purpose
-------
This module is the execution-facing macro/news safety layer.
It does NOT generate signals and does NOT replace Battle Permission.
It classifies whether a symbol may be promoted to Battle/Telegram READY
around high-impact macro events.

Human-facing strings may be Ukrainian where they are meant for Telegram/debug.
Internal statuses/enums remain English and stable.

Typical use from runner / Battle Gate:

    from app.services.macro_event_guard import evaluate_macro_guard

    decision = evaluate_macro_guard(
        symbol="NAS100",
        report_date="2026-06-17",
        timezone_name="Europe/Kyiv",
        context={
            "acceptance_confirmed": False,
            "retest_confirmed": False,
            "ltf_confirmed": False,
            "has_real_target": False,
            "stop_quality": "OK",
            "practical_rr": 2.4,
        },
    )

    if decision.block_battle:
        # force RESEARCH / WATCH, do not send BATTLE_ALERT
        ...

Version history
---------------
v1.0-symbol-aware-fomc-post-news-lock
- Symbol-aware macro mapping.
- FOMC day / FOMC press-conference lock.
- Pre-news lock, hard post-news lock, post-news acceptance-required state.
- Conservative mode when the macro calendar is unavailable.
"""

import argparse
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

try:
    from app.services.daily_market_briefing import load_high_impact_calendar
except Exception:  # pragma: no cover - useful for isolated import diagnostics
    load_high_impact_calendar = None  # type: ignore[assignment]


MACRO_EVENT_GUARD_VERSION = "macro-event-guard-v1.0-symbol-aware-fomc-post-news-lock"
DEFAULT_TIMEZONE = "Europe/Kyiv"

# Stable decision states.
MACRO_CLEAR = "MACRO_CLEAR"
MACRO_UNKNOWN_CONSERVATIVE = "MACRO_UNKNOWN_CONSERVATIVE"
PRE_NEWS_LOCK = "PRE_NEWS_LOCK"
POST_NEWS_HARD_LOCK = "POST_NEWS_HARD_LOCK"
POST_NEWS_ACCEPTANCE_REQUIRED = "POST_NEWS_ACCEPTANCE_REQUIRED"
FOMC_DAY_LOCK = "FOMC_DAY_LOCK"
FOMC_PRESSER_LOCK = "FOMC_PRESSER_LOCK"
OIL_POST_NEWS_ACCEPTANCE_REQUIRED = "OIL_POST_NEWS_ACCEPTANCE_REQUIRED"

# Stable blockers.
BLOCKER_MACRO_UNKNOWN = "macro_unknown_conservative"
BLOCKER_PRE_NEWS = "pre_news_lock"
BLOCKER_POST_NEWS_HARD = "post_news_hard_lock"
BLOCKER_POST_NEWS_ACCEPTANCE_REQUIRED = "post_news_acceptance_required"
BLOCKER_FOMC_DAY = "fomc_day_lock"
BLOCKER_FOMC_PRESSER = "fomc_presser_lock"
BLOCKER_OIL_POST_NEWS = "oil_post_news_acceptance_required"

# Stable requirements used by runner/Battle Gate.
REQ_EXTERNAL_CALENDAR_CHECK = "external_calendar_check"
REQ_ACCEPTANCE_CONFIRMED = "acceptance_confirmed"
REQ_RETEST_CONFIRMED = "retest_confirmed"
REQ_LTF_CONFIRMED = "ltf_confirmed"
REQ_REAL_TARGET = "real_target"
REQ_STOP_OK = "stop_ok"
REQ_PRACTICAL_RR_OK = "practical_rr_ok"
REQ_MACRO_CLEARANCE = "macro_clearance"
REQ_PRESS_CONFERENCE_COMPLETE = "press_conference_complete"

# Conservative defaults. All can be overridden by env if needed.
DEFAULT_PRE_NEWS_LOCK_MIN = int(os.getenv("MACRO_GUARD_PRE_NEWS_LOCK_MIN", "30"))
DEFAULT_POST_NEWS_HARD_LOCK_MIN = int(os.getenv("MACRO_GUARD_POST_NEWS_HARD_LOCK_MIN", "15"))
DEFAULT_POST_NEWS_ACCEPTANCE_WINDOW_MIN = int(os.getenv("MACRO_GUARD_POST_NEWS_ACCEPTANCE_WINDOW_MIN", "90"))

FOMC_PRE_NEWS_LOCK_MIN = int(os.getenv("MACRO_GUARD_FOMC_PRE_NEWS_LOCK_MIN", "90"))
FOMC_PRESSER_AFTER_LOCK_MIN = int(os.getenv("MACRO_GUARD_FOMC_PRESSER_AFTER_LOCK_MIN", "90"))
FOMC_DAY_LOOKAHEAD_MIN = int(os.getenv("MACRO_GUARD_FOMC_DAY_LOOKAHEAD_MIN", "720"))
FOMC_DAY_POST_WINDOW_MIN = int(os.getenv("MACRO_GUARD_FOMC_DAY_POST_WINDOW_MIN", "360"))

OIL_PRE_NEWS_LOCK_MIN = int(os.getenv("MACRO_GUARD_OIL_PRE_NEWS_LOCK_MIN", "30"))
OIL_POST_NEWS_ACCEPTANCE_WINDOW_MIN = int(os.getenv("MACRO_GUARD_OIL_POST_NEWS_ACCEPTANCE_WINDOW_MIN", "90"))

MIN_PRACTICAL_RR = float(os.getenv("MACRO_GUARD_MIN_PRACTICAL_RR", "2.0"))

ALL_TRADING_SYMBOLS: tuple[str, ...] = (
    "GER40",
    "NAS100",
    "SPX500",
    "UKOIL",
    "XAUUSD",
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "USDCAD",
    "AUDUSD",
    "BTCUSD",
    "ETHUSD",
)

NY_FOCUS_SYMBOLS: tuple[str, ...] = (
    "NAS100",
    "SPX500",
    "UKOIL",
    "XAUUSD",
    "USDCAD",
    "BTCUSD",
    "ETHUSD",
)

AFFECTED_SYMBOLS_BY_CURRENCY: dict[str, tuple[str, ...]] = {
    "USD": ("XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NAS100", "SPX500", "BTCUSD", "ETHUSD"),
    "EUR": ("EURUSD", "GER40", "XAUUSD"),
    "GBP": ("GBPUSD",),
    "JPY": ("USDJPY", "XAUUSD"),
    "CHF": ("USDCHF", "XAUUSD"),
    "CAD": ("USDCAD", "UKOIL"),
    "AUD": ("AUDUSD", "XAUUSD"),
    "CNY": ("XAUUSD", "AUDUSD", "NAS100", "SPX500", "BTCUSD", "ETHUSD"),
}


@dataclass(frozen=True)
class MacroGuardDecision:
    version: str
    symbol: str
    status: str
    allowed_for_battle: bool
    block_battle: bool
    research_only: bool
    suppress: bool
    reason_code: str
    blockers: list[str] = field(default_factory=list)
    requirements: list[str] = field(default_factory=list)
    missing_requirements: list[str] = field(default_factory=list)
    satisfied_requirements: list[str] = field(default_factory=list)
    macro_risk_status: str = MACRO_UNKNOWN_CONSERVATIVE
    calendar_status: str = "UNKNOWN"
    calendar_source: str = "unknown"
    fallback_chain: list[str] = field(default_factory=list)
    event_title: str | None = None
    event_time_local: str | None = None
    event_currency: str | None = None
    event_impact: str | None = None
    event_source: str | None = None
    minutes_since_event: float | None = None
    minutes_until_event: float | None = None
    affected_symbols: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    raw_event: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------


def _upper(value: Any, default: str = "") -> str:
    text = str(value if value is not None else default).strip()
    return text.upper() if text else default


def _boolish(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "ok", "confirmed", "valid", "allow", "allowed"}:
        return True
    if text in {"0", "false", "no", "n", "off", "none", "null", "blocked", "invalid"}:
        return False
    return default


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _tz(timezone_name: str | None = None) -> ZoneInfo:
    return ZoneInfo(timezone_name or os.getenv("REPORT_TIMEZONE") or DEFAULT_TIMEZONE)


def _parse_as_of(value: str | datetime | None, timezone_name: str) -> datetime:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_tz(timezone_name))
        return dt.astimezone(_tz(timezone_name))

    raw = str(value or "").strip()
    if not raw:
        return datetime.now(timezone.utc).astimezone(_tz(timezone_name))

    text = raw.replace("Z", "+00:00")
    if len(text) == 10:
        d = date.fromisoformat(text)
        return datetime.combine(d, time(0, 0), tzinfo=_tz(timezone_name))

    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_tz(timezone_name))
    return dt.astimezone(_tz(timezone_name))


def _parse_report_date(value: str | date | None, timezone_name: str, as_of: str | datetime | None = None) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    raw = str(value or "").strip()
    if raw:
        return date.fromisoformat(raw)
    return _parse_as_of(as_of, timezone_name).date()


def _event_title(event: dict[str, Any]) -> str:
    return str(event.get("title") or event.get("event") or event.get("name") or "").strip()


def _event_title_upper(event: dict[str, Any]) -> str:
    return _event_title(event).upper()


def _event_local_datetime(event: dict[str, Any], timezone_name: str) -> datetime | None:
    event_date = str(event.get("date") or "").strip()
    event_time = str(event.get("time") or "").strip()
    event_tz = str(event.get("timezone") or timezone_name).strip() or timezone_name

    if not event_date or not event_time:
        return None

    try:
        hour, minute = [int(x) for x in event_time.split(":", 1)]
        provider_dt = datetime.combine(
            date.fromisoformat(event_date),
            time(hour=hour, minute=minute),
            tzinfo=ZoneInfo(event_tz),
        )
        return provider_dt.astimezone(_tz(timezone_name))
    except Exception:
        return None


def _event_time_local_text(event: dict[str, Any], timezone_name: str) -> str | None:
    dt = _event_local_datetime(event, timezone_name)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d %H:%M %Z")


def _minutes_since_event(event: dict[str, Any], *, as_of_local: datetime, timezone_name: str) -> float | None:
    dt = _event_local_datetime(event, timezone_name)
    if dt is None:
        return None
    return round((as_of_local - dt).total_seconds() / 60.0, 1)


# -----------------------------------------------------------------------------
# Event classification and symbol mapping
# -----------------------------------------------------------------------------


def _is_fomc_press_conference(event: dict[str, Any]) -> bool:
    title = _event_title_upper(event)
    return "FOMC" in title and ("PRESS CONFERENCE" in title or "PRESSER" in title)


def _is_fomc_event(event: dict[str, Any]) -> bool:
    title = _event_title_upper(event)
    return (
        "FOMC" in title
        or "FEDERAL RESERVE" in title
        or "FED " in f"{title} "
        or "RATE DECISION" in title
        or "INTEREST RATE" in title
        or "ECONOMIC PROJECTIONS" in title
        or "DOT PLOT" in title
        or "PRESS CONFERENCE" in title
    )


def _is_oil_event(event: dict[str, Any]) -> bool:
    title = _event_title_upper(event)
    return "CRUDE" in title or "OIL" in title or "OPEC" in title


def _is_major_usd_event(event: dict[str, Any]) -> bool:
    title = _event_title_upper(event)
    currency = _upper(event.get("currency"), "")
    if currency != "USD":
        return False
    major_words = (
        "CPI",
        "PCE",
        "NFP",
        "NON FARM",
        "NON-FARM",
        "PAYROLL",
        "UNEMPLOYMENT",
        "RETAIL SALES",
        "GDP",
        "ISM",
        "PMI",
        "JOLTS",
        "JOBLESS CLAIMS",
        "DURABLE GOODS",
        "FOMC",
        "FED",
        "RATE DECISION",
    )
    return any(word in title for word in major_words)


def _dedupe_symbols(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        sym = str(value or "").strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        result.append(sym)
    return result


def affected_symbols_for_event(event: dict[str, Any]) -> list[str]:
    """Return project symbols affected by an event, using provider symbols plus guard fallbacks."""
    explicit = event.get("symbols")
    symbols: list[str] = []
    if isinstance(explicit, list):
        symbols.extend(str(x).strip().upper() for x in explicit if str(x).strip())

    currency = _upper(event.get("currency"), "")
    symbols.extend(AFFECTED_SYMBOLS_BY_CURRENCY.get(currency, ()))

    if _is_fomc_event(event):
        symbols.extend(ALL_TRADING_SYMBOLS)
        symbols.extend(NY_FOCUS_SYMBOLS)

    if _is_oil_event(event):
        symbols.extend(("UKOIL", "USDCAD"))

    return _dedupe_symbols(symbols)


def _event_affects_symbol(event: dict[str, Any], symbol: str) -> bool:
    symbol = symbol.strip().upper()
    return symbol in affected_symbols_for_event(event)


def _event_type_priority(event: dict[str, Any]) -> int:
    if _is_fomc_press_conference(event):
        return 0
    if _is_fomc_event(event):
        return 1
    if _is_major_usd_event(event):
        return 2
    if _is_oil_event(event):
        return 3
    return 4


# -----------------------------------------------------------------------------
# Context requirement checks
# -----------------------------------------------------------------------------


def _practical_rr_ok(context: dict[str, Any]) -> bool:
    if _boolish(context.get("practical_rr_ok"), default=False):
        return True
    try:
        return float(context.get("practical_rr")) >= MIN_PRACTICAL_RR
    except (TypeError, ValueError):
        return False


def _stop_ok(context: dict[str, Any]) -> bool:
    if _boolish(context.get("stop_ok"), default=False):
        return True
    stop_quality = _upper(context.get("stop_quality"), "")
    if stop_quality and stop_quality not in {"TIGHT_STOP", "NO_STOP", "INVALID", "UNKNOWN"}:
        return True
    return False


def _ltf_confirmed(context: dict[str, Any]) -> bool:
    if _boolish(context.get("ltf_confirmed"), default=False):
        return True
    if _boolish(context.get("ltf_model_confirmed"), default=False):
        return True
    status = _upper(context.get("ltf_model_status") or context.get("ltf_status"), "")
    return status in {"CONFIRMED", "CONFIRMED_EXECUTABLE", "EXECUTABLE"}


def _real_target(context: dict[str, Any]) -> bool:
    if _boolish(context.get("real_target"), default=False):
        return True
    if _boolish(context.get("has_real_target"), default=False):
        return True
    return bool(context.get("target") or context.get("tp") or context.get("take_profit") or context.get("primary_target"))


def _acceptance_confirmed(context: dict[str, Any]) -> bool:
    return bool(
        _boolish(context.get("acceptance_confirmed"), default=False)
        or _boolish(context.get("post_news_acceptance_confirmed"), default=False)
        or _boolish(context.get("has_acceptance"), default=False)
    )


def _retest_confirmed(context: dict[str, Any]) -> bool:
    return bool(
        _boolish(context.get("retest_confirmed"), default=False)
        or _boolish(context.get("post_news_retest_confirmed"), default=False)
        or _boolish(context.get("has_retest"), default=False)
    )


def _macro_clearance(context: dict[str, Any]) -> bool:
    return bool(
        _boolish(context.get("macro_clearance"), default=False)
        or _boolish(context.get("external_calendar_checked"), default=False)
        or _boolish(context.get("macro_checked"), default=False)
    )


def _press_conference_complete(context: dict[str, Any]) -> bool:
    return bool(
        _boolish(context.get("press_conference_complete"), default=False)
        or _boolish(context.get("fomc_press_conference_complete"), default=False)
    )


def _requirement_satisfied(requirement: str, context: dict[str, Any]) -> bool:
    if requirement == REQ_EXTERNAL_CALENDAR_CHECK:
        return _macro_clearance(context)
    if requirement == REQ_ACCEPTANCE_CONFIRMED:
        return _acceptance_confirmed(context)
    if requirement == REQ_RETEST_CONFIRMED:
        return _retest_confirmed(context)
    if requirement == REQ_LTF_CONFIRMED:
        return _ltf_confirmed(context)
    if requirement == REQ_REAL_TARGET:
        return _real_target(context)
    if requirement == REQ_STOP_OK:
        return _stop_ok(context)
    if requirement == REQ_PRACTICAL_RR_OK:
        return _practical_rr_ok(context)
    if requirement == REQ_MACRO_CLEARANCE:
        return _macro_clearance(context)
    if requirement == REQ_PRESS_CONFERENCE_COMPLETE:
        return _press_conference_complete(context)
    return False


def _split_requirements(requirements: list[str], context: dict[str, Any]) -> tuple[list[str], list[str]]:
    satisfied: list[str] = []
    missing: list[str] = []
    for req in requirements:
        if _requirement_satisfied(req, context):
            satisfied.append(req)
        else:
            missing.append(req)
    return satisfied, missing


def _post_news_requirements() -> list[str]:
    return [
        REQ_ACCEPTANCE_CONFIRMED,
        REQ_RETEST_CONFIRMED,
        REQ_LTF_CONFIRMED,
        REQ_REAL_TARGET,
        REQ_STOP_OK,
        REQ_PRACTICAL_RR_OK,
    ]


# -----------------------------------------------------------------------------
# Decision construction
# -----------------------------------------------------------------------------


def _decision(
    *,
    symbol: str,
    status: str,
    reason_code: str,
    macro_risk_status: str,
    calendar_status: str,
    calendar_source: str,
    fallback_chain: list[str] | None = None,
    event: dict[str, Any] | None = None,
    as_of_local: datetime | None = None,
    timezone_name: str = DEFAULT_TIMEZONE,
    blockers: list[str] | None = None,
    requirements: list[str] | None = None,
    context: dict[str, Any] | None = None,
    notes: list[str] | None = None,
    force_block: bool = False,
    suppress: bool = False,
) -> MacroGuardDecision:
    ctx = _as_dict(context)
    reqs = list(requirements or [])
    satisfied, missing = _split_requirements(reqs, ctx)

    hard_block_statuses = {
        MACRO_UNKNOWN_CONSERVATIVE,
        PRE_NEWS_LOCK,
        POST_NEWS_HARD_LOCK,
        FOMC_DAY_LOCK,
        FOMC_PRESSER_LOCK,
    }

    if force_block or status in hard_block_statuses:
        allowed = False
    elif reqs:
        allowed = not missing
    else:
        allowed = status == MACRO_CLEAR

    event_title = _event_title(event) if event else None
    minutes_since: float | None = None
    minutes_until: float | None = None
    if event and as_of_local is not None:
        minutes_since = _minutes_since_event(event, as_of_local=as_of_local, timezone_name=timezone_name)
        if minutes_since is not None and minutes_since < 0:
            minutes_until = round(abs(minutes_since), 1)

    affected = affected_symbols_for_event(event) if event else []

    return MacroGuardDecision(
        version=MACRO_EVENT_GUARD_VERSION,
        symbol=symbol.strip().upper(),
        status=status,
        allowed_for_battle=allowed,
        block_battle=not allowed,
        research_only=not allowed and not suppress,
        suppress=suppress,
        reason_code=reason_code,
        blockers=list(blockers or ([] if allowed else [reason_code])),
        requirements=reqs,
        missing_requirements=missing,
        satisfied_requirements=satisfied,
        macro_risk_status=macro_risk_status,
        calendar_status=calendar_status,
        calendar_source=calendar_source,
        fallback_chain=list(fallback_chain or []),
        event_title=event_title,
        event_time_local=_event_time_local_text(event, timezone_name) if event else None,
        event_currency=str(event.get("currency") or "") if event else None,
        event_impact=str(event.get("impact") or "") if event else None,
        event_source=str(event.get("source") or "") if event else None,
        minutes_since_event=minutes_since,
        minutes_until_event=minutes_until,
        affected_symbols=affected,
        notes=list(notes or []),
        raw_event=dict(event) if event else None,
    )


def _clear_decision(symbol: str, *, calendar_status: str, calendar_source: str, macro_risk_status: str, fallback_chain: list[str]) -> MacroGuardDecision:
    return _decision(
        symbol=symbol,
        status=MACRO_CLEAR,
        reason_code="macro_clear",
        macro_risk_status=macro_risk_status,
        calendar_status=calendar_status,
        calendar_source=calendar_source,
        fallback_chain=fallback_chain,
        notes=["Macro guard clear for this symbol."],
    )


def _unknown_decision(symbol: str, *, calendar_status: str, calendar_source: str, macro_risk_status: str, fallback_chain: list[str], message: str | None = None) -> MacroGuardDecision:
    notes = [
        "Macro calendar unavailable. Provider unavailable is not treated as no-news.",
        "NO BATTLE without external calendar check.",
    ]
    if message:
        notes.append(str(message))
    return _decision(
        symbol=symbol,
        status=MACRO_UNKNOWN_CONSERVATIVE,
        reason_code=BLOCKER_MACRO_UNKNOWN,
        blockers=[BLOCKER_MACRO_UNKNOWN],
        requirements=[REQ_EXTERNAL_CALENDAR_CHECK, REQ_LTF_CONFIRMED, REQ_REAL_TARGET, REQ_STOP_OK, REQ_PRACTICAL_RR_OK],
        macro_risk_status=macro_risk_status or MACRO_UNKNOWN_CONSERVATIVE,
        calendar_status=calendar_status,
        calendar_source=calendar_source,
        fallback_chain=fallback_chain,
        notes=notes,
        force_block=True,
    )


def _status_for_event(event: dict[str, Any], minutes: float | None) -> tuple[str, str, list[str], list[str], list[str], bool]:
    """Return status, reason_code, blockers, requirements, notes, force_block."""
    if minutes is None:
        return (
            POST_NEWS_ACCEPTANCE_REQUIRED,
            BLOCKER_POST_NEWS_ACCEPTANCE_REQUIRED,
            [BLOCKER_POST_NEWS_ACCEPTANCE_REQUIRED],
            _post_news_requirements(),
            ["High-impact event has no parseable time; conservative acceptance/retest required."],
            False,
        )

    if _is_fomc_press_conference(event):
        if -FOMC_PRE_NEWS_LOCK_MIN <= minutes <= FOMC_PRESSER_AFTER_LOCK_MIN:
            return (
                FOMC_PRESSER_LOCK,
                BLOCKER_FOMC_PRESSER,
                [BLOCKER_FOMC_PRESSER],
                [REQ_PRESS_CONFERENCE_COMPLETE] + _post_news_requirements(),
                ["FOMC press conference lock. NO BATTLE until press conference is complete; afterwards acceptance/retest is required."],
                True,
            )
        if minutes > FOMC_PRESSER_AFTER_LOCK_MIN and minutes <= FOMC_DAY_POST_WINDOW_MIN:
            return (
                POST_NEWS_ACCEPTANCE_REQUIRED,
                BLOCKER_POST_NEWS_ACCEPTANCE_REQUIRED,
                [BLOCKER_POST_NEWS_ACCEPTANCE_REQUIRED],
                _post_news_requirements(),
                ["Post-FOMC press window. Battle only after acceptance + retest + LTF confirmation + real target."],
                False,
            )

    if _is_fomc_event(event):
        if -FOMC_PRE_NEWS_LOCK_MIN <= minutes < 0:
            return (
                PRE_NEWS_LOCK,
                BLOCKER_PRE_NEWS,
                [BLOCKER_PRE_NEWS, BLOCKER_FOMC_DAY],
                [REQ_MACRO_CLEARANCE],
                ["FOMC pre-news lock. Do not promote pre-release structure to Battle."],
                True,
            )
        if 0 <= minutes <= DEFAULT_POST_NEWS_HARD_LOCK_MIN:
            return (
                POST_NEWS_HARD_LOCK,
                BLOCKER_POST_NEWS_HARD,
                [BLOCKER_POST_NEWS_HARD, BLOCKER_FOMC_DAY],
                _post_news_requirements(),
                ["FOMC just released. First impulse is unsafe; hard post-news lock active."],
                True,
            )
        if DEFAULT_POST_NEWS_HARD_LOCK_MIN < minutes <= FOMC_DAY_POST_WINDOW_MIN:
            return (
                POST_NEWS_ACCEPTANCE_REQUIRED,
                BLOCKER_POST_NEWS_ACCEPTANCE_REQUIRED,
                [BLOCKER_POST_NEWS_ACCEPTANCE_REQUIRED, BLOCKER_FOMC_DAY],
                _post_news_requirements(),
                ["FOMC post-news regime. No chase; require acceptance/retest and full execution quality."],
                False,
            )
        if -FOMC_DAY_LOOKAHEAD_MIN <= minutes <= FOMC_DAY_POST_WINDOW_MIN:
            return (
                FOMC_DAY_LOCK,
                BLOCKER_FOMC_DAY,
                [BLOCKER_FOMC_DAY],
                [REQ_MACRO_CLEARANCE] + _post_news_requirements(),
                ["FOMC day lock. Battle requires explicit macro clearance and execution confirmation."],
                True,
            )

    if _is_oil_event(event):
        if -OIL_PRE_NEWS_LOCK_MIN <= minutes < 0:
            return (
                PRE_NEWS_LOCK,
                BLOCKER_PRE_NEWS,
                [BLOCKER_PRE_NEWS],
                [REQ_MACRO_CLEARANCE],
                ["Oil inventory/event pre-news lock for UKOIL/USDCAD."],
                True,
            )
        if 0 <= minutes <= DEFAULT_POST_NEWS_HARD_LOCK_MIN:
            return (
                POST_NEWS_HARD_LOCK,
                BLOCKER_POST_NEWS_HARD,
                [BLOCKER_POST_NEWS_HARD],
                _post_news_requirements(),
                ["Oil event just released. Do not trade first impulse."],
                True,
            )
        if DEFAULT_POST_NEWS_HARD_LOCK_MIN < minutes <= OIL_POST_NEWS_ACCEPTANCE_WINDOW_MIN:
            return (
                OIL_POST_NEWS_ACCEPTANCE_REQUIRED,
                BLOCKER_OIL_POST_NEWS,
                [BLOCKER_OIL_POST_NEWS],
                _post_news_requirements(),
                ["Oil post-news regime. UKOIL/USDCAD require acceptance/retest."],
                False,
            )

    # Generic high-impact event.
    if -DEFAULT_PRE_NEWS_LOCK_MIN <= minutes < 0:
        return (
            PRE_NEWS_LOCK,
            BLOCKER_PRE_NEWS,
            [BLOCKER_PRE_NEWS],
            [REQ_MACRO_CLEARANCE],
            ["High-impact pre-news lock. Do not promote pre-release structure to Battle."],
            True,
        )
    if 0 <= minutes <= DEFAULT_POST_NEWS_HARD_LOCK_MIN:
        return (
            POST_NEWS_HARD_LOCK,
            BLOCKER_POST_NEWS_HARD,
            [BLOCKER_POST_NEWS_HARD],
            _post_news_requirements(),
            ["High-impact event just released. First impulse hard lock."],
            True,
        )
    if DEFAULT_POST_NEWS_HARD_LOCK_MIN < minutes <= DEFAULT_POST_NEWS_ACCEPTANCE_WINDOW_MIN:
        return (
            POST_NEWS_ACCEPTANCE_REQUIRED,
            BLOCKER_POST_NEWS_ACCEPTANCE_REQUIRED,
            [BLOCKER_POST_NEWS_ACCEPTANCE_REQUIRED],
            _post_news_requirements(),
            ["Post-news acceptance required. No chase; wait for retest and LTF confirmation."],
            False,
        )

    return (
        MACRO_CLEAR,
        "macro_clear",
        [],
        [],
        ["Relevant event is outside active macro guard windows."],
        False,
    )


def _candidate_events_for_symbol(events: list[dict[str, Any]], symbol: str, target_date: date) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for event in events:
        if str(event.get("date") or target_date.isoformat()) != target_date.isoformat():
            continue
        if _event_affects_symbol(event, symbol):
            result.append(event)
    return result


def _event_selection_key(event: dict[str, Any], *, as_of_local: datetime, timezone_name: str) -> tuple[int, int, float, str]:
    minutes = _minutes_since_event(event, as_of_local=as_of_local, timezone_name=timezone_name)
    if minutes is None:
        abs_minutes = 99999.0
        active = 9
    else:
        abs_minutes = abs(minutes)
        # Active locks/regimes come before merely same-day events.
        status, *_rest = _status_for_event(event, minutes)
        active = 0 if status != MACRO_CLEAR else 5
    return (active, _event_type_priority(event), abs_minutes, _event_title_upper(event))


def _select_primary_event(events: list[dict[str, Any]], *, as_of_local: datetime, timezone_name: str) -> dict[str, Any] | None:
    if not events:
        return None
    return sorted(events, key=lambda e: _event_selection_key(e, as_of_local=as_of_local, timezone_name=timezone_name))[0]


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------


def evaluate_macro_guard(
    symbol: str,
    *,
    report_date: str | date | None = None,
    timezone_name: str = DEFAULT_TIMEZONE,
    as_of: str | datetime | None = None,
    context: dict[str, Any] | None = None,
    events: list[dict[str, Any]] | None = None,
) -> MacroGuardDecision:
    """
    Evaluate whether a symbol can be promoted to Battle/Telegram READY.

    This function is intentionally conservative:
    - provider unavailable != no news;
    - FOMC blocks Battle by default;
    - first impulse after high-impact news is not tradable;
    - post-news setup requires acceptance + retest + LTF + target + stop + RR.
    """
    sym = str(symbol or "").strip().upper()
    tz_name = timezone_name or DEFAULT_TIMEZONE
    as_of_local = _parse_as_of(as_of, tz_name)
    target_date = _parse_report_date(report_date, tz_name, as_of_local)
    ctx = _as_dict(context)

    calendar_status = "MANUAL_EVENTS"
    calendar_source = "provided_events"
    macro_risk_status = "OK"
    fallback_chain: list[str] = []
    message: str | None = None

    if events is None:
        if load_high_impact_calendar is None:
            return _unknown_decision(
                sym,
                calendar_status="UNAVAILABLE",
                calendar_source="macro_event_guard_import_error",
                macro_risk_status=MACRO_UNKNOWN_CONSERVATIVE,
                fallback_chain=[],
                message="Could not import daily_market_briefing.load_high_impact_calendar.",
            )
        calendar = load_high_impact_calendar(target_date)
        events = list(getattr(calendar, "events", []) or [])
        calendar_status = str(getattr(calendar, "status", "UNKNOWN") or "UNKNOWN")
        calendar_source = str(getattr(calendar, "source", "unknown") or "unknown")
        macro_risk_status = str(getattr(calendar, "macro_risk_status", calendar_status) or calendar_status)
        fallback_chain = list(getattr(calendar, "fallback_chain", []) or [])
        message = getattr(calendar, "message", None)

    # Unknown calendar is a Battle blocker unless caller gives explicit external clearance.
    if not events and macro_risk_status == MACRO_UNKNOWN_CONSERVATIVE:
        return _unknown_decision(
            sym,
            calendar_status=calendar_status,
            calendar_source=calendar_source,
            macro_risk_status=macro_risk_status,
            fallback_chain=fallback_chain,
            message=message,
        )

    symbol_events = _candidate_events_for_symbol(events, sym, target_date)
    if not symbol_events:
        # If the calendar is fallback/last-good but no event affects the symbol, we still allow.
        return _clear_decision(
            sym,
            calendar_status=calendar_status,
            calendar_source=calendar_source,
            macro_risk_status=macro_risk_status,
            fallback_chain=fallback_chain,
        )

    primary = _select_primary_event(symbol_events, as_of_local=as_of_local, timezone_name=tz_name)
    if primary is None:
        return _clear_decision(
            sym,
            calendar_status=calendar_status,
            calendar_source=calendar_source,
            macro_risk_status=macro_risk_status,
            fallback_chain=fallback_chain,
        )

    minutes = _minutes_since_event(primary, as_of_local=as_of_local, timezone_name=tz_name)
    status, reason_code, blockers, requirements, notes, force_block = _status_for_event(primary, minutes)

    if status == MACRO_CLEAR:
        return _clear_decision(
            sym,
            calendar_status=calendar_status,
            calendar_source=calendar_source,
            macro_risk_status=macro_risk_status,
            fallback_chain=fallback_chain,
        )

    return _decision(
        symbol=sym,
        status=status,
        reason_code=reason_code,
        blockers=blockers,
        requirements=requirements,
        macro_risk_status=macro_risk_status,
        calendar_status=calendar_status,
        calendar_source=calendar_source,
        fallback_chain=fallback_chain,
        event=primary,
        as_of_local=as_of_local,
        timezone_name=tz_name,
        context=ctx,
        notes=notes,
        force_block=force_block,
    )


def evaluate_macro_guard_many(
    symbols: Iterable[str],
    *,
    report_date: str | date | None = None,
    timezone_name: str = DEFAULT_TIMEZONE,
    as_of: str | datetime | None = None,
    context_by_symbol: dict[str, dict[str, Any]] | None = None,
    events: list[dict[str, Any]] | None = None,
) -> dict[str, MacroGuardDecision]:
    contexts = context_by_symbol or {}
    return {
        str(sym).strip().upper(): evaluate_macro_guard(
            str(sym).strip().upper(),
            report_date=report_date,
            timezone_name=timezone_name,
            as_of=as_of,
            context=contexts.get(str(sym).strip().upper(), {}),
            events=events,
        )
        for sym in symbols
        if str(sym).strip()
    }


def apply_macro_guard_to_signal(
    signal: dict[str, Any],
    *,
    symbol: str | None = None,
    report_date: str | date | None = None,
    timezone_name: str = DEFAULT_TIMEZONE,
    as_of: str | datetime | None = None,
) -> dict[str, Any]:
    """
    Return a copy of signal with macro guard fields attached.

    This helper does not send/suppress Telegram by itself. Runner/Battle Gate should
    use the returned fields to downgrade READY -> RESEARCH/OBSERVE when needed.
    """
    payload = dict(signal or {})
    sym = str(symbol or payload.get("symbol") or payload.get("instrument") or "").strip().upper()
    decision = evaluate_macro_guard(
        sym,
        report_date=report_date,
        timezone_name=timezone_name,
        as_of=as_of,
        context=payload,
    )

    payload["macro_guard_version"] = decision.version
    payload["macro_guard_status"] = decision.status
    payload["macro_guard_allowed_for_battle"] = decision.allowed_for_battle
    payload["macro_guard_block_battle"] = decision.block_battle
    payload["macro_guard_reason_code"] = decision.reason_code
    payload["macro_guard_blockers"] = list(decision.blockers)
    payload["macro_guard_requirements"] = list(decision.requirements)
    payload["macro_guard_missing_requirements"] = list(decision.missing_requirements)
    payload["macro_guard_event_title"] = decision.event_title
    payload["macro_guard_event_time_local"] = decision.event_time_local
    payload["macro_guard_minutes_since_event"] = decision.minutes_since_event
    payload["macro_guard_calendar_source"] = decision.calendar_source
    payload["macro_guard_macro_risk_status"] = decision.macro_risk_status

    return payload


# -----------------------------------------------------------------------------
# CLI smoke tool
# -----------------------------------------------------------------------------


def _main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate AI Market Analyst macro event guard.")
    parser.add_argument("--symbol", default="NAS100")
    parser.add_argument("--date", default=None)
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols. Overrides --symbol if provided.")
    args = parser.parse_args()

    if args.symbols:
        decisions = evaluate_macro_guard_many(
            [x.strip() for x in str(args.symbols).split(",") if x.strip()],
            report_date=args.date,
            timezone_name=args.timezone,
            as_of=args.as_of,
        )
        if args.as_json:
            print(json.dumps({k: v.to_dict() for k, v in decisions.items()}, ensure_ascii=False, indent=2, default=str))
        else:
            for sym, d in decisions.items():
                print(f"{sym}: {d.status} | allowed={d.allowed_for_battle} | reason={d.reason_code} | event={d.event_title}")
        return 0

    decision = evaluate_macro_guard(
        args.symbol,
        report_date=args.date,
        timezone_name=args.timezone,
        as_of=args.as_of,
    )
    if args.as_json:
        print(json.dumps(decision.to_dict(), ensure_ascii=False, indent=2, default=str))
    else:
        print(
            f"{decision.symbol}: {decision.status} | allowed={decision.allowed_for_battle} | "
            f"reason={decision.reason_code} | event={decision.event_title} | "
            f"missing={','.join(decision.missing_requirements) or '-'}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
