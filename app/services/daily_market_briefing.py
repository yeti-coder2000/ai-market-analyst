from __future__ import annotations

"""
Daily market briefing service for AI Market Analyst.

Read-only reporting layer:
- market holidays / closed markets
- high-impact macro events
- yesterday performance recap
- TPO / auction snapshot
- provider/data issues

Human-facing Telegram output is Ukrainian.
Internal status/enums/JSON fields remain English and stable.
"""

import html
import json
import os
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    from app.core.settings import settings
except Exception:  # pragma: no cover
    settings = None  # type: ignore[assignment]


BRIEFING_VERSION = "daily-market-briefing-v1.25-macro-affected-assets-post-news-impact"
DEFAULT_TIMEZONE = "Europe/Kyiv"

TPO_LATEST_RELATIVE = Path("tpo") / "tpo_latest.json"
DAILY_SUMMARY_RELATIVE = Path("stats") / "daily_summary.json"
SIGNAL_OUTCOMES_RELATIVE = Path("stats") / "signal_outcomes.json"
HIGH_IMPACT_EVENTS_RELATIVE = Path("calendar") / "high_impact_events.json"
ECONOMIC_CALENDAR_CACHE_RELATIVE = Path("calendar")
TRADING_ECONOMICS_CALENDAR_CACHE_RELATIVE = Path("calendar")
FMP_ECONOMIC_CALENDAR_CACHE_RELATIVE = Path("calendar")
EODHD_ECONOMIC_EVENTS_CACHE_RELATIVE = Path("calendar")
FAIRECONOMY_CALENDAR_CACHE_RELATIVE = Path("calendar")
MANUAL_HIGH_IMPACT_EVENTS_RELATIVE = Path("macro") / "manual_high_impact_events.json"
LAST_GOOD_HIGH_IMPACT_EVENTS_RELATIVE = Path("calendar") / "last_good_high_impact_events.json"

MACRO_OK = "OK"
MACRO_EMPTY = "EMPTY"
MACRO_FALLBACK = "FALLBACK"
MACRO_HIGH_FROM_BACKUP_CALENDAR = "HIGH_FROM_BACKUP_CALENDAR"
MACRO_WATCH_FROM_BACKUP_CALENDAR = "MACRO_WATCH_FROM_BACKUP_CALENDAR"
MACRO_LAST_GOOD_CACHE = "LAST_GOOD_CACHE"
MACRO_UNKNOWN_CONSERVATIVE = "MACRO_UNKNOWN_CONSERVATIVE"

# Static holiday/session overlay for cases where online macro providers fail.
# This prevents known US market holidays from being rendered as generic
# MACRO_UNKNOWN_CONSERVATIVE / provider-unavailable red risk blocks.
# Keep this list explicit and conservative; it is a reporting overlay, not a
# trade-permission bypass.
US_MARKET_HOLIDAY_OVERLAY: dict[str, dict[str, Any]] = {
    "2026-06-19": {
        "code": "US_JUNETEENTH",
        "name": "Juneteenth / US market holiday",
        "ny_equity_status": "MARKET_CLOSED",
        "liquidity_mode": "HOLIDAY_LIQUIDITY_CAUTION",
        "session_note": "London/morning assets are not globally blocked; NY cash/risk assets are not normal Battle focus.",
    },
}

FINAL_TP = "TP_HIT"
FINAL_SL = "SL_HIT"
MISSED = "MISSED_TARGET_BEFORE_ENTRY"
EXPIRED = "EXPIRED"
INVALID = "INVALID"


# =============================================================================
# SESSION-SCOPED REPORTING RULES
# =============================================================================

MORNING_SESSION_SYMBOLS: tuple[str, ...] = (
    "GER40",
    "XAUUSD",
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "AUDUSD",
    "BTCUSD",
    "ETHUSD",
)

LONDON_SESSION_SYMBOLS: tuple[str, ...] = MORNING_SESSION_SYMBOLS

NY_SESSION_SYMBOLS: tuple[str, ...] = (
    "NAS100",
    "SPX500",
    "UKOIL",
    "XAUUSD",
    "USDCAD",
    "BTCUSD",
    "ETHUSD",
)

GLOBAL_SYMBOL_ORDER: tuple[str, ...] = (
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

REPORT_SCOPE_LABELS_UK: dict[str, str] = {
    "morning": "London / ранкова сесія. NY cash/risk-активи винесені у NY post-open звіт.",
    "morning_briefing": "London / ранкова сесія. NY cash/risk-активи винесені у NY post-open звіт.",
    "morning_combined": "London + ранковий брифінг. NY cash/risk-активи винесені у NY post-open звіт.",
    "london": "London +1h. NY cash/risk-активи тут не показуються як активний фокус.",
    "london_1h": "London +1h. NY cash/risk-активи тут не показуються як активний фокус.",
    "ny": "New York post-open / NY active focus. London-only активи тут не показуються як активний фокус.",
    "ny_1h": "New York post-open / NY active focus. London-only активи тут не показуються як активний фокус.",
    "new_york": "New York post-open / NY active focus. London-only активи тут не показуються як активний фокус.",
    "holiday_warning": "Глобальне попередження про свята / закриті ринки. Можуть показуватись усі релевантні інструменти.",
    "pre_market": "Глобальне попередження про свята / закриті ринки. Можуть показуватись усі релевантні інструменти.",
}

BUILTIN_HIGH_IMPACT_EVENTS: list[dict[str, Any]] = [
    {
        "date": "2026-05-26",
        "time": "10:00",
        "timezone": "America/New_York",
        "currency": "USD",
        "impact": "HIGH",
        "title": "CB Consumer Confidence",
        "symbols": ["XAUUSD", "EURUSD", "GBPUSD", "NAS100", "SPX500", "BTCUSD", "ETHUSD"],
        "note": "US consumer confidence can move USD, gold, US indices and risk sentiment.",
        "source": "builtin",
    },
    {
        "date": "2026-05-28",
        "time": "08:30",
        "timezone": "America/New_York",
        "currency": "USD",
        "impact": "HIGH",
        "title": "Core PCE / GDP / Durable Goods cluster",
        "symbols": ["XAUUSD", "EURUSD", "GBPUSD", "NAS100", "SPX500", "BTCUSD", "ETHUSD"],
        "note": "Inflation/growth cluster. Avoid treating pre-release structure as stable direction.",
        "source": "builtin",
    },
]

CURRENCY_BY_COUNTRY: dict[str, str] = {
    "UNITED STATES": "USD",
    "UNITED STATES OF AMERICA": "USD",
    "US": "USD",
    "USA": "USD",
    "EURO AREA": "EUR",
    "EUROZONE": "EUR",
    "EUROPEAN UNION": "EUR",
    "GERMANY": "EUR",
    "FRANCE": "EUR",
    "ITALY": "EUR",
    "SPAIN": "EUR",
    "UNITED KINGDOM": "GBP",
    "UK": "GBP",
    "JAPAN": "JPY",
    "SWITZERLAND": "CHF",
    "CANADA": "CAD",
    "AUSTRALIA": "AUD",
    "NEW ZEALAND": "NZD",
    "CHINA": "CNY",
    # ISO-3166 alpha-2 country codes used by EODHD economic-events API.
    "GB": "GBP",
    "JP": "JPY",
    "CH": "CHF",
    "CA": "CAD",
    "AU": "AUD",
    "CN": "CNY",
    "DE": "EUR",
    "FR": "EUR",
    "IT": "EUR",
    "ES": "EUR",
}

AFFECTED_SYMBOLS_BY_CURRENCY: dict[str, list[str]] = {
    "USD": ["XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NAS100", "SPX500", "BTCUSD", "ETHUSD"],
    "EUR": ["EURUSD", "GER40", "XAUUSD"],
    "GBP": ["GBPUSD"],
    "JPY": ["USDJPY", "XAUUSD"],
    "CHF": ["USDCHF", "XAUUSD"],
    "CAD": ["USDCAD", "UKOIL"],
    "AUD": ["AUDUSD", "XAUUSD"],
    "CNY": ["XAUUSD", "AUDUSD", "NAS100", "SPX500", "BTCUSD", "ETHUSD"],
}


RELEVANT_RISK_CURRENCIES: set[str] = {"USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "CNY"}
UNKNOWN_CURRENCY_VALUES: set[str] = {"", "-", "UNKNOWN", "NONE", "NULL", "N/A"}

HIGH_IMPACT_KEYWORDS: tuple[str, ...] = (
    "NFP",
    "NON FARM",
    "NON-FARM",
    "CPI",
    "PCE",
    "CORE PCE",
    "FOMC",
    "FED",
    "INTEREST RATE",
    "RATE DECISION",
    "GDP",
    "ISM",
    "PMI",
    "JOLTS",
    "UNEMPLOYMENT",
    "PAYROLL",
    "RETAIL SALES",
    "DURABLE GOODS",
    "JOBLESS CLAIMS",
    "INFLATION",
    "BOE",
    "ECB",
    "BOJ",
    "SNB",
    "BOC",
    "RBA",
    "OPEC",
    "CRUDE OIL INVENTORIES",
)


CRITICAL_MEDIUM_EVENT_KEYWORDS: tuple[str, ...] = (
    # ForexFactory/Faireconomy often marks market-moving flash PMIs and
    # housing/oil data as Medium even though they are session-relevant for our
    # NY/London instruments. Keep these as calendar risk/watch events instead
    # of returning a false EMPTY calendar.
    "FLASH MANUFACTURING PMI",
    "FLASH SERVICES PMI",
    "MANUFACTURING PMI",
    "SERVICES PMI",
    "COMPOSITE PMI",
    "ISM",
    "GDP",
    "PCE",
    "CORE PCE",
    "CPI",
    "RETAIL SALES",
    "DURABLE GOODS",
    "JOBLESS CLAIMS",
    "UNEMPLOYMENT",
    "PAYROLL",
    "JOLTS",
    "CONSUMER CONFIDENCE",
    "NEW HOME SALES",
    "EXISTING HOME SALES",
    "CRUDE OIL INVENTORIES",
    "OIL INVENTORIES",
    "EIA",
    "OPEC",
    "FOMC",
    "FED",
    "BOC",
    "ECB",
    "BOE",
    "BOJ",
    "SNB",
    "RBA",
)


def _is_critical_medium_macro_event(impact: str, title: str, currency: str = "") -> bool:
    normalized_impact = str(impact or "").strip().upper()
    if normalized_impact not in {"MEDIUM", "ORANGE", "2", "2.0"}:
        return False

    title_upper = str(title or "").upper()
    if any(keyword in title_upper for keyword in CRITICAL_MEDIUM_EVENT_KEYWORDS):
        return True

    # Central-bank speeches are not all equal, but Governor/President/Chair
    # speeches can move FX/index/gold intraday. Keep them as watch risk.
    cur = str(currency or "").upper()
    if cur in {"USD", "EUR", "GBP", "JPY", "CAD", "CHF", "AUD"}:
        if any(word in title_upper for word in ("SPEAKS", "TESTIFIES", "PRESS CONFERENCE")):
            if any(bank in title_upper for bank in ("FED", "FOMC", "ECB", "BOE", "BOJ", "BOC", "SNB", "RBA", "CHAIR", "PRESIDENT", "GOV")):
                return True

    return False


@dataclass
class CalendarLoadResult:
    status: str
    source: str
    events: list[dict[str, Any]] = field(default_factory=list)
    message: str | None = None
    cache_path: str | None = None
    provider_error: str | None = None
    macro_risk_status: str = MACRO_UNKNOWN_CONSERVATIVE
    fallback_chain: list[str] = field(default_factory=list)
    data_freshness: str | None = None
    last_good_cache_path: str | None = None

    @property
    def ok(self) -> bool:
        return self.status in {MACRO_OK, MACRO_EMPTY}



# =============================================================================
# UKRAINIAN HUMAN-FACING LABELS
# =============================================================================

MARKET_STATUS_UK: dict[str, str] = {
    "OPEN": "відкритий",
    "STALE_DATA": "застарілі дані",
    "MARKET_CLOSED": "ринок закритий",
    "CLOSED": "закрито",
    "NO_DATA": "немає даних",
    "PROVIDER_ERROR": "помилка провайдера",
}

PERMISSION_UK: dict[str, str] = {
    "OPEN_FOR_EVALUATION": "можна оцінювати",
    "STALE_DATA": "застарілі дані",
    "MARKET_CLOSED": "ринок закритий",
    "NO_DATA": "немає даних",
    "PROVIDER_ERROR": "помилка провайдера",
    "RESEARCH_ONLY": "тільки research",
    "BLOCKED_BY_CONTEXT": "заблоковано контекстом",
    "BLOCKED_BY_AUCTION": "заблоковано аукціоном",
    "ALLOW_BOOST": "дозволено з підсиленням",
    "ALLOW_NEUTRAL": "дозволено нейтрально",
    "NEUTRAL": "нейтрально",
}

MODIFIER_UK: dict[str, str] = {
    "BOOST": "посилений контекст",
    "NEUTRAL": "нейтрально",
    "DOWNGRADE": "знижений пріоритет",
    "BLOCK": "блок",
}

OPEN_RELATION_UK: dict[str, str] = {
    "INSIDE_VA": "всередині VA",
    "OPEN_INSIDE_VA": "всередині VA",
    "RANGE": "у межах діапазону",
    "OPEN_IN_RANGE": "у межах діапазону",
    "OUT_OF_RANGE": "поза діапазоном",
    "OPEN_OUT_OF_RANGE": "поза діапазоном",
    "UNKNOWN": "невідомо",
}

AUCTION_BIAS_UK: dict[str, str] = {
    "BALANCE": "баланс",
    "RANGE_EXTENSION": "розширення діапазону",
    "DIRECTIONAL_IMBALANCE": "направлений дисбаланс",
    "OPEN_AUCTION": "відкритий аукціон",
    "OPEN_DRIVE": "open drive",
    "OPEN_TEST_DRIVE": "open test drive",
    "OPEN_REJECTION_REVERSE": "open rejection reverse",
    "UNKNOWN": "невідомо",
}

OPEN_CONTEXT_UK: dict[str, str] = {
    "OPEN_INSIDE_VA": "всередині VA",
    "OPEN_IN_RANGE": "у межах діапазону",
    "OPEN_OUT_OF_RANGE": "поза діапазоном",
    "UNKNOWN": "невідомо",
}

OPEN_BEHAVIOR_UK: dict[str, str] = {
    "OPEN_DRIVE": "open drive",
    "OPEN_TEST_DRIVE": "open test drive",
    "OPEN_REJECTION_REVERSE": "rejection reverse",
    "OPEN_AUCTION": "open auction",
    "UNCONFIRMED": "не підтверджено",
}

ENTRY_MODEL_HINT_UK: dict[str, str] = {
    "NO_ENTRY_MODEL": "немає моделі входу",
    "NO_DIRECTIONAL_ENTRY_MODEL": "немає directional-моделі",
    "ROTATION_ONLY_IF_LTF_CONFIRMED": "ротація тільки після LTF-підтвердження",
    "PULLBACK_CONTINUATION": "pullback continuation",
    "PULLBACK_OR_FAILED_ACCEPTANCE_RETEST": "pullback або failed acceptance retest",
    "FAILED_ACCEPTANCE_RETEST": "failed acceptance retest",
    "SWEEP_RECLAIM_BOS_RETEST": "sweep → reclaim → BOS → retest",
    "WAIT_FOR_ACCEPTANCE_OR_REJECTION": "чекати acceptance або rejection",
}

STOP_MODEL_HINT_UK: dict[str, str] = {
    "NO_STOP_MODEL": "немає моделі стопа",
    "BEHIND_SWEEP_EXTREME": "за sweep extreme",
    "BEHIND_PULLBACK_STRUCTURE_OR_IB_EDGE": "за pullback-структурою або IB edge",
    "BEYOND_FAILED_ACCEPTANCE_ZONE": "за зоною failed acceptance",
    "BEYOND_TEST_ZONE_OR_PULLBACK_STRUCTURE": "за test zone або pullback-структурою",
    "BEYOND_VALUE_EDGE_OR_STRUCTURE": "за value edge або структурою",
    "BEYOND_VALUE_EDGE": "за value edge",
}

BATTLE_BIAS_HINT_UK: dict[str, str] = {
    "BOOST_IF_HTF_ALIGNED_AND_EXECUTABLE": "BOOST якщо HTF aligned і EXECUTABLE",
    "ALLOW_IF_HTF_ALIGNED_AND_LTF_CONFIRMED": "дозвіл тільки при HTF alignment + LTF confirmation",
    "RESEARCH_COUNTERTREND_UNLESS_LTF_CONFIRMED": "countertrend research до LTF-підтвердження",
    "RESEARCH_UNTIL_ACCEPTANCE_CONFIRMED": "research до підтвердження acceptance",
    "DOWNGRADE_NO_DIRECTIONAL_BATTLE": "DOWNGRADE: без directional battle",
    "RESEARCH_ONLY": "тільки research",
    "BLOCK": "блок",
}

ZONE_TYPE_UK: dict[str, str] = {
    "POC": "POC",
    "NPOC": "nPOC",
    "VAH": "VAH",
    "VAL": "VAL",
    "PREVIOUS_HIGH": "попередній high",
    "PREVIOUS_LOW": "попередній low",
}

ZONE_ROLE_UK: dict[str, str] = {
    "MAGNET": "магніт / зона інтересу",
    "REACTION_ZONE": "зона реакції",
    "INVALIDATION_ZONE": "зона invalidation",
    "TARGET_ZONE": "цільова зона",
    "REFERENCE_ZONE": "орієнтир",
    "UNKNOWN": "невідомо",
}

ZONE_REACTION_UK: dict[str, str] = {
    "REJECTED": "відхилено",
    "ACCEPTED": "прийнято",
    "SWEPT": "знято ліквідність",
    "UNCONFIRMED": "не підтверджено",
    "NONE": "немає",
}

OUTCOME_UK: dict[str, str] = {
    "TP_HIT": "TP",
    "SL_HIT": "SL",
    "MISSED_TARGET_BEFORE_ENTRY": "пропущено до входу",
    "EXPIRED": "прострочено",
    "INVALID": "невалідно",
    "PENDING_ENTRY": "очікує входу",
    "ENTRY_TRIGGERED": "вхід активовано",
}

COMMON_NOTE_UK: dict[str, str] = {
    "US consumer confidence can move USD, gold, US indices and risk sentiment.": (
        "Споживча довіра США може рухати USD, золото, індекси США та risk sentiment."
    ),
    "Inflation/growth cluster. Avoid treating pre-release structure as stable direction.": (
        "Кластер інфляції / зростання. До релізу не вважати структуру стабільним напрямком."
    ),
}


@dataclass
class BriefingSection:
    title: str
    lines: list[str] = field(default_factory=list)


@dataclass
class BriefingReport:
    report_type: str
    report_date: str
    timezone: str
    generated_at_utc: str
    generated_at_local: str
    version: str = BRIEFING_VERSION
    sections: list[BriefingSection] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


def _runtime_dir() -> Path:
    env_runtime = os.getenv("RUNTIME_DIR")
    if env_runtime:
        return Path(env_runtime).expanduser().resolve()

    if settings is not None:
        value = getattr(settings, "runtime_dir", None)
        if value:
            return Path(value).expanduser().resolve()

    render_runtime = Path("/var/data/runtime")
    if render_runtime.exists():
        return render_runtime

    return Path("runtime").resolve()


def _safe_read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default



def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _runtime_writes_disabled() -> bool:
    return _env_bool("BRIEFING_DISABLE_RUNTIME_WRITES", False)


def _macro_cache_writes_enabled() -> bool:
    if _runtime_writes_disabled():
        return False
    return _env_bool("ENABLE_MACRO_CACHE_WRITES", True)

def _safe_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _get_path(env_name: str, default_relative: Path) -> Path:
    raw = os.getenv(env_name)
    if raw:
        return Path(raw).expanduser().resolve()
    return _runtime_dir() / default_relative


def _tpo_path() -> Path:
    return _get_path("TPO_CONTEXT_STORE_PATH", TPO_LATEST_RELATIVE)


def _daily_summary_path() -> Path:
    return _get_path("DAILY_SUMMARY_PATH", DAILY_SUMMARY_RELATIVE)


def _signal_outcomes_path() -> Path:
    return _get_path("SIGNAL_OUTCOMES_PATH", SIGNAL_OUTCOMES_RELATIVE)


def _high_impact_events_path() -> Path:
    return _get_path("HIGH_IMPACT_EVENTS_PATH", HIGH_IMPACT_EVENTS_RELATIVE)


def _economic_calendar_cache_path(target_date: date) -> Path:
    raw = os.getenv("ECONOMIC_CALENDAR_CACHE_PATH")
    if raw:
        base = Path(raw).expanduser().resolve()
        if base.suffix:
            return base
        return base / f"economic_calendar_{target_date.isoformat()}.json"
    return _runtime_dir() / ECONOMIC_CALENDAR_CACHE_RELATIVE / f"economic_calendar_{target_date.isoformat()}.json"


def _trading_economics_calendar_cache_path(target_date: date) -> Path:
    raw = os.getenv("TRADING_ECONOMICS_CALENDAR_CACHE_PATH")
    if raw:
        base = Path(raw).expanduser().resolve()
        if base.suffix:
            return base
        return base / f"tradingeconomics_calendar_{target_date.isoformat()}.json"
    return _runtime_dir() / TRADING_ECONOMICS_CALENDAR_CACHE_RELATIVE / f"tradingeconomics_calendar_{target_date.isoformat()}.json"


def _fmp_economic_calendar_cache_path(target_date: date) -> Path:
    raw = os.getenv("FMP_ECONOMIC_CALENDAR_CACHE_PATH")
    if raw:
        base = Path(raw).expanduser().resolve()
        if base.suffix:
            return base
        return base / f"fmp_economic_calendar_{target_date.isoformat()}.json"
    return _runtime_dir() / FMP_ECONOMIC_CALENDAR_CACHE_RELATIVE / f"fmp_economic_calendar_{target_date.isoformat()}.json"


def _eodhd_economic_events_cache_path(target_date: date) -> Path:
    raw = os.getenv("EODHD_ECONOMIC_EVENTS_CACHE_PATH") or os.getenv("EODHD_ECONOMIC_CALENDAR_CACHE_PATH")
    if raw:
        base = Path(raw).expanduser().resolve()
        if base.suffix:
            return base
        return base / f"eodhd_economic_events_{target_date.isoformat()}.json"
    return _runtime_dir() / EODHD_ECONOMIC_EVENTS_CACHE_RELATIVE / f"eodhd_economic_events_{target_date.isoformat()}.json"


def _faireconomy_calendar_cache_path(target_date: date) -> Path:
    raw = os.getenv("FAIRECONOMY_CALENDAR_CACHE_PATH") or os.getenv("FOREXFACTORY_CALENDAR_CACHE_PATH")
    if raw:
        base = Path(raw).expanduser().resolve()
        if base.suffix:
            return base
        return base / f"faireconomy_forexfactory_calendar_{target_date.isoformat()}.json"
    return _runtime_dir() / FAIRECONOMY_CALENDAR_CACHE_RELATIVE / f"faireconomy_forexfactory_calendar_{target_date.isoformat()}.json"


def _manual_high_impact_events_path() -> Path:
    return _get_path("MANUAL_HIGH_IMPACT_EVENTS_PATH", MANUAL_HIGH_IMPACT_EVENTS_RELATIVE)


def _last_good_high_impact_events_path() -> Path:
    return _get_path("LAST_GOOD_HIGH_IMPACT_EVENTS_PATH", LAST_GOOD_HIGH_IMPACT_EVENTS_RELATIVE)


def _parse_as_of_datetime(value: str | None) -> datetime | None:
    """
    Parse deterministic report clock override.

    Accepted examples:
    - 2026-06-17T21:29:00+03:00
    - 2026-06-17 21:29:00+03:00
    - 2026-06-17T21:29:00   # interpreted in BRIEFING_AS_OF_TIMEZONE / REPORT_TIMEZONE
    - 2026-06-17            # interpreted as 00:00 local report time
    """
    raw = str(value or "").strip()
    if not raw:
        return None

    text = raw.replace("Z", "+00:00")
    try:
        if len(text) == 10:
            local_date = date.fromisoformat(text)
            tz_name = os.getenv("BRIEFING_AS_OF_TIMEZONE") or os.getenv("REPORT_TIMEZONE") or DEFAULT_TIMEZONE
            return datetime.combine(local_date, time(0, 0), tzinfo=ZoneInfo(tz_name)).astimezone(timezone.utc)

        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            tz_name = os.getenv("BRIEFING_AS_OF_TIMEZONE") or os.getenv("REPORT_TIMEZONE") or DEFAULT_TIMEZONE
            dt = dt.replace(tzinfo=ZoneInfo(tz_name))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _now_utc() -> datetime:
    override = (
        os.getenv("BRIEFING_AS_OF_UTC")
        or os.getenv("BRIEFING_AS_OF")
        or os.getenv("REPORT_AS_OF")
    )
    parsed = _parse_as_of_datetime(override)
    if parsed is not None:
        return parsed
    return datetime.now(timezone.utc)


def _tz(name: str | None = None) -> ZoneInfo:
    return ZoneInfo(name or os.getenv("REPORT_TIMEZONE") or DEFAULT_TIMEZONE)


def _today_local(timezone_name: str) -> date:
    return _now_utc().astimezone(_tz(timezone_name)).date()


def _parse_date(value: str | None, timezone_name: str) -> date:
    if not value:
        return _today_local(timezone_name)
    return date.fromisoformat(str(value).strip())


def _fmt_num(value: Any, ndigits: int = 2, default: str = "-") -> str:
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)

    if number.is_integer():
        return str(int(number))

    return f"{number:.{ndigits}f}"


def _fmt_pct(value: Any, default: str = "-") -> str:
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)

    if abs(number) <= 1:
        return f"{number * 100:.1f}%"

    return f"{number:.1f}%"


def _esc(value: Any) -> str:
    if value is None:
        return "-"
    return html.escape(str(value), quote=False)


def _raw(value: Any, default: str = "-") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _upper(value: Any, default: str = "-") -> str:
    text = _raw(value, default)
    return text.upper() if text != default else default


def _label(value: Any, mapping: dict[str, str], default: str = "-") -> str:
    raw = _upper(value, default)
    if raw == default:
        return default
    translated = mapping.get(raw)
    if translated:
        return f"{translated} ({raw})"
    return raw


def _label_plain(value: Any, mapping: dict[str, str], default: str = "-") -> str:
    raw = _upper(value, default)
    if raw == default:
        return default
    return mapping.get(raw, raw)


def _status_label(value: Any) -> str:
    return _label(value, MARKET_STATUS_UK)


def _permission_label(value: Any) -> str:
    return _label(value, PERMISSION_UK)


def _modifier_label(value: Any) -> str:
    return _label(value, MODIFIER_UK)


def _open_relation_label(value: Any) -> str:
    return _label(value, OPEN_RELATION_UK)


def _auction_bias_label(value: Any) -> str:
    return _label(value, AUCTION_BIAS_UK)


def _open_context_label(value: Any) -> str:
    return _label(value, OPEN_CONTEXT_UK)


def _open_behavior_label(value: Any) -> str:
    return _label(value, OPEN_BEHAVIOR_UK)


def _entry_hint_label(value: Any) -> str:
    return _label_plain(value, ENTRY_MODEL_HINT_UK)


def _stop_hint_label(value: Any) -> str:
    return _label_plain(value, STOP_MODEL_HINT_UK)


def _battle_hint_label(value: Any) -> str:
    return _label_plain(value, BATTLE_BIAS_HINT_UK)


def _zone_type_label(value: Any) -> str:
    return _label_plain(value, ZONE_TYPE_UK)


def _zone_role_label(value: Any) -> str:
    return _label_plain(value, ZONE_ROLE_UK)


def _zone_reaction_label(value: Any) -> str:
    return _label_plain(value, ZONE_REACTION_UK)


def _confidence_pct(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if number <= 1:
        return f"{number * 100:.0f}%"
    return f"{number:.0f}%"


def _primary_zone_text(zone: Any) -> str:
    if not isinstance(zone, dict) or not zone:
        return "-"

    zone_type = _zone_type_label(zone.get("zone_type"))
    role = _zone_role_label(zone.get("role"))
    reaction = _zone_reaction_label(zone.get("reaction"))
    price = _fmt_num(zone.get("price"), 5)
    distance = _fmt_num(zone.get("distance"), 5)

    parts = [zone_type]
    if price != "-":
        parts.append(f"ціна={price}")
    if distance != "-":
        parts.append(f"відстань={distance}")
    if role != "-":
        parts.append(f"роль={role}")
    if reaction not in {"-", "не підтверджено"}:
        parts.append(f"реакція={reaction}")
    return ", ".join(parts)


def _translate_note(note: Any) -> str:
    text = _raw(note, "")
    if not text:
        return ""
    return COMMON_NOTE_UK.get(text, text)


def _translate_outcomes_dict(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "{}"

    parts: list[str] = []
    for key, count in value.items():
        label = _label_plain(key, OUTCOME_UK, str(key))
        parts.append(f"{label}: {count}")
    return "{" + ", ".join(parts) + "}"


def _local_dt_from_event(event: dict[str, Any], report_timezone: str) -> str:
    event_date = str(event.get("date") or "").strip()
    event_time = str(event.get("time") or "").strip()
    event_tz = str(event.get("timezone") or report_timezone).strip() or report_timezone

    if not event_date:
        return "-"

    if not event_time:
        return event_date

    try:
        hour, minute = [int(x) for x in event_time.split(":", 1)]
        local_event = datetime.combine(
            date.fromisoformat(event_date),
            time(hour=hour, minute=minute),
            tzinfo=ZoneInfo(event_tz),
        )
        report_event = local_event.astimezone(_tz(report_timezone))
        return report_event.strftime("%H:%M %Z")
    except Exception:
        return f"{event_date} {event_time} {event_tz}"


def _event_local_datetime(event: dict[str, Any], report_timezone: str) -> datetime | None:
    event_date = str(event.get("date") or "").strip()
    event_time = str(event.get("time") or "").strip()
    event_tz = str(event.get("timezone") or report_timezone).strip() or report_timezone

    if not event_date or not event_time:
        return None

    try:
        hour, minute = [int(x) for x in event_time.split(":", 1)]
        provider_dt = datetime.combine(
            date.fromisoformat(event_date),
            time(hour=hour, minute=minute),
            tzinfo=ZoneInfo(event_tz),
        )
        return provider_dt.astimezone(_tz(report_timezone))
    except Exception:
        return None


def _time_aware_event_note(event: dict[str, Any], report_timezone: str) -> str:
    """Return operational pre/post-news guidance for Telegram risk block."""
    event_dt = _event_local_datetime(event, report_timezone)
    if event_dt is None:
        return "До high-impact news не вважати ранню структуру стабільною."

    now_local = _now_utc().astimezone(_tz(report_timezone))
    minutes = (now_local - event_dt).total_seconds() / 60.0

    if minutes < -15:
        return "До релізу: не піднімати research у battle; перша структура може бути фальшивою."

    if -15 <= minutes < 0:
        return "Реліз близько. Не відкривати новий battle без уже сформованої моделі та захисту ризику."

    if 0 <= minutes <= 90:
        return "Реліз уже був. Режим: post-news acceptance / failed move; не наздоганяти перший імпульс."

    return "Новина вже дала post-news режим; якщо volatility regime ще активний — не наздоганяти, чекати ретест / acceptance."


def _extract_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("events", "items", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
    return []


def load_tpo_store() -> dict[str, Any]:
    return _safe_read_json(_tpo_path(), {})


def load_daily_summary() -> dict[str, Any]:
    return _safe_read_json(_daily_summary_path(), {})


def load_signal_outcomes() -> dict[str, Any]:
    return _safe_read_json(_signal_outcomes_path(), {})


def _normalize_impact(value: Any, title: str = "") -> str:
    raw = str(value or "").strip().upper()
    if raw in {"HIGH", "RED", "IMPORTANT", "3", "3.0"}:
        return "HIGH"
    if raw in {"MEDIUM", "ORANGE", "2", "2.0"}:
        return "MEDIUM"
    if raw in {"LOW", "YELLOW", "1", "1.0"}:
        return "LOW"

    title_upper = str(title or "").upper()
    if any(keyword in title_upper for keyword in HIGH_IMPACT_KEYWORDS):
        return "HIGH"

    return raw or "UNKNOWN"


def _currency_from_event(country: Any, currency: Any = None) -> str:
    raw_currency = str(currency or "").strip().upper()
    if raw_currency:
        return raw_currency

    country_key = str(country or "").strip().upper()
    if country_key in CURRENCY_BY_COUNTRY:
        return CURRENCY_BY_COUNTRY[country_key]

    for key, value in CURRENCY_BY_COUNTRY.items():
        if key and key in country_key:
            return value

    return "-"


REGIONAL_FED_EVENT_KEYWORDS: tuple[str, ...] = (
    "PHILADELPHIA FED",
    "PHILLY FED",
    "NEW YORK FED",
    "NY FED",
    "EMPIRE STATE",
    "DALLAS FED",
    "RICHMOND FED",
    "CHICAGO FED",
    "KANSAS CITY FED",
    "KC FED",
    "CLEVELAND FED",
    "ATLANTA FED",
    "ST LOUIS FED",
    "ST. LOUIS FED",
    "MINNEAPOLIS FED",
    "BEIGE BOOK",
)


def _is_regional_fed_indicator_title(title: Any) -> bool:
    """Return True for regional Fed indicators that must not trigger FOMC locks."""
    title_upper = str(title or "").upper()
    if not title_upper:
        return False
    return any(keyword in title_upper for keyword in REGIONAL_FED_EVENT_KEYWORDS)


def _is_fomc_rate_decision_title(title: Any, currency: Any = None) -> bool:
    """Strict USD/FOMC rate-decision detector. Regional Fed data is excluded."""
    title_upper = str(title or "").upper()
    cur = str(currency or "").strip().upper()

    if not title_upper or _is_regional_fed_indicator_title(title_upper):
        return False

    if "FEDERAL FUNDS RATE" in title_upper or "FED FUNDS" in title_upper:
        return True

    explicit_fed_context = (
        "FOMC" in title_upper
        or "FEDERAL RESERVE" in title_upper
        or "FED INTEREST RATE" in title_upper
        or "FED RATE" in title_upper
        or "US INTEREST RATE" in title_upper
        or "U.S. INTEREST RATE" in title_upper
    )

    rate_decision = "RATE DECISION" in title_upper or "INTEREST RATE DECISION" in title_upper
    return bool(rate_decision and (explicit_fed_context or cur == "USD"))


def _is_fomc_press_conference_title(title: Any, currency: Any = None) -> bool:
    """Strict FOMC/Fed Chair press-conference detector. Regional Fed data is excluded."""
    title_upper = str(title or "").upper()
    cur = str(currency or "").strip().upper()

    if not title_upper or _is_regional_fed_indicator_title(title_upper):
        return False

    has_press = "PRESS CONFERENCE" in title_upper or "PRESSER" in title_upper
    if not has_press:
        return False

    return bool(
        "FOMC" in title_upper
        or "FEDERAL RESERVE" in title_upper
        or "FED CHAIR" in title_upper
        or "POWELL" in title_upper
        or ("FED" in title_upper and cur == "USD")
    )


def _is_strict_fomc_title(title: Any, currency: Any = None) -> bool:
    """
    Detect true FOMC/Fed Chair macro events without confusing regional Fed indicators.

    Examples that must NOT match:
    - Philadelphia Fed Manufacturing Index
    - Dallas/Richmond/Chicago/Kansas City Fed surveys
    - Empire State Manufacturing

    Examples that must match:
    - FOMC Rate Decision / Statement / Economic Projections
    - FOMC Press Conference
    - Federal Funds Rate / Fed Interest Rate Decision
    - Fed Chair Powell speech/testimony/press conference
    """
    title_upper = str(title or "").upper()
    cur = str(currency or "").strip().upper()

    if not title_upper or _is_regional_fed_indicator_title(title_upper):
        return False

    if "FOMC" in title_upper or "FEDERAL OPEN MARKET COMMITTEE" in title_upper:
        return True

    if _is_fomc_rate_decision_title(title_upper, cur):
        return True

    if _is_fomc_press_conference_title(title_upper, cur):
        return True

    if ("ECONOMIC PROJECTIONS" in title_upper or "DOT PLOT" in title_upper) and (
        cur == "USD" or "FED" in title_upper or "FEDERAL RESERVE" in title_upper
    ):
        return True

    if "FEDERAL RESERVE STATEMENT" in title_upper:
        return True

    if "POWELL" in title_upper and any(token in title_upper for token in ("SPEAK", "SPEAKS", "SPEECH", "TESTIMONY", "TESTIFIES", "PRESS", "CONFERENCE")):
        return True

    if "FED CHAIR" in title_upper and any(token in title_upper for token in ("SPEAK", "SPEAKS", "SPEECH", "TESTIMONY", "TESTIFIES", "PRESS", "CONFERENCE")):
        return True

    return False


def _affected_symbols(currency: str, event_title: str = "") -> list[str]:
    cur = str(currency or "").strip().upper()
    symbols = list(AFFECTED_SYMBOLS_BY_CURRENCY.get(cur, []))

    title = str(event_title or "").upper()
    if "OIL" in title or "CRUDE" in title or "OPEC" in title:
        for sym in ("UKOIL", "USDCAD"):
            if sym not in symbols:
                symbols.append(sym)

    # True FOMC/Fed Chair events reprice broad USD liquidity and risk assets.
    # Regional Fed indicators, such as Philadelphia Fed Manufacturing Index,
    # remain USD high-impact data but must not trigger FOMC-specific broad locks.
    if _is_strict_fomc_title(title, cur):
        for sym in ("UKOIL",):
            if sym not in symbols:
                symbols.append(sym)

    return symbols


def _parse_finnhub_time(value: Any, target_date: date) -> tuple[str, str]:
    """
    Finnhub economicCalendar records may expose time as an ISO string, date string,
    datetime string, or unix timestamp depending on plan/endpoint version.
    Return (date, HH:MM) in UTC unless the API clearly provides date-only data.
    """
    if value is None:
        return target_date.isoformat(), ""

    if isinstance(value, (int, float)):
        try:
            number = float(value)
            if number > 10_000_000_000:
                number = number / 1000.0
            dt = datetime.fromtimestamp(number, tz=timezone.utc)
            return dt.date().isoformat(), dt.strftime("%H:%M")
        except Exception:
            return target_date.isoformat(), ""

    text = str(value).strip()
    if not text:
        return target_date.isoformat(), ""

    try:
        d = date.fromisoformat(text[:10])
        if "T" not in text and len(text) <= 10:
            return d.isoformat(), ""
    except Exception:
        pass

    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.date().isoformat(), dt.strftime("%H:%M")
    except Exception:
        pass

    try:
        dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.date().isoformat(), dt.strftime("%H:%M")
    except Exception:
        return target_date.isoformat(), ""


def _normalize_finnhub_event(raw: dict[str, Any], target_date: date) -> dict[str, Any] | None:
    title = str(raw.get("event") or raw.get("title") or raw.get("name") or "").strip()
    if not title:
        return None

    event_date, event_time = _parse_finnhub_time(
        raw.get("time") or raw.get("datetime") or raw.get("date"),
        target_date,
    )

    country = raw.get("country") or raw.get("region")
    currency = _currency_from_event(country, raw.get("currency"))
    impact = _normalize_impact(raw.get("impact") or raw.get("importance"), title)

    return {
        "date": event_date,
        "time": event_time,
        "timezone": "UTC",
        "currency": currency,
        "impact": impact,
        "title": title,
        "country": country or "-",
        "symbols": _affected_symbols(currency, title),
        "note": _event_trading_note(currency, title, impact),
        "source": "finnhub",
        "actual": raw.get("actual"),
        "forecast": raw.get("estimate") if raw.get("estimate") is not None else raw.get("forecast"),
        "previous": raw.get("prev") if raw.get("prev") is not None else raw.get("previous"),
        "raw": raw,
    }


def _event_trading_note(currency: str, title: str, impact: str) -> str:
    cur = str(currency or "").upper()
    title_upper = str(title or "").upper()

    if impact != "HIGH":
        return "Подія не high-impact; використовувати як фон, не як торговий тригер."

    if "OIL" in title_upper or "CRUDE" in title_upper or "OPEC" in title_upper:
        return "Oil-related event. Для UKOIL не торгувати перший імпульс без acceptance / retest."
    if _is_strict_fomc_title(title_upper, cur):
        return "FOMC / Fed high-impact macro. NO BATTLE до завершення пресконференції; після — тільки acceptance + retest."
    if cur == "USD":
        return "USD high-impact macro. До релізу не піднімати research у battle; після релізу чекати acceptance."
    if cur == "EUR":
        return "EUR / Europe high-impact macro. Впливає на EURUSD та GER40; чекати підтвердження після релізу."
    if cur == "GBP":
        return "GBP high-impact macro. Для GBPUSD чекати post-news acceptance / rejection."
    if cur == "JPY":
        return "JPY high-impact macro. Для USDJPY не торгувати ранній імпульс без LTF confirmation."
    if cur == "CAD":
        return "CAD high-impact macro. Для USDCAD / UKOIL врахувати post-news volatility."
    if "OIL" in title_upper or "CRUDE" in title_upper or "OPEC" in title_upper:
        return "Oil-related event. Для UKOIL не торгувати перший імпульс без acceptance."

    return "High-impact macro. До релізу не вважати структуру стабільним напрямком."


# =============================================================================
# TRADING ECONOMICS BACKUP CALENDAR
# =============================================================================

TRADING_ECONOMICS_DEFAULT_COUNTRIES: tuple[str, ...] = (
    "united states",
    "euro area",
    "united kingdom",
    "japan",
    "switzerland",
    "canada",
    "australia",
    "china",
    "germany",
)


def _trading_economics_enabled() -> bool:
    return str(os.getenv("ENABLE_TRADING_ECONOMICS_CALENDAR", "true")).strip().lower() in {"1", "true", "yes", "on"}


def _trading_economics_credentials() -> str:
    raw = (
        os.getenv("TRADING_ECONOMICS_API_KEY")
        or os.getenv("TRADING_ECONOMICS_CREDENTIALS")
        or os.getenv("TRADING_ECONOMICS_CLIENT")
        or "guest:guest"
    )
    return str(raw).strip()


def _trading_economics_countries() -> list[str]:
    raw = os.getenv("TRADING_ECONOMICS_COUNTRIES")
    if raw:
        parts = [x.strip().lower() for x in raw.split(",") if x.strip()]
        if parts:
            return parts
    return list(TRADING_ECONOMICS_DEFAULT_COUNTRIES)


def _trading_economics_country_slug(countries: list[str]) -> str:
    # API supports comma-separated country slugs in the path. Encode spaces as %20, keep commas.
    return ",".join(urllib.parse.quote(country.strip().lower(), safe="") for country in countries if country.strip())


def _trading_economics_currency(country: Any, currency: Any = None) -> str:
    raw = str(currency or "").strip().upper()
    if raw in {"USD", "$", "US$", "U.S. DOLLAR"}:
        return "USD"
    if raw in {"EUR", "€", "EURO"}:
        return "EUR"
    if raw in {"GBP", "£", "POUND"}:
        return "GBP"
    if raw in {"JPY", "¥", "YEN"}:
        return "JPY"
    if raw in {"CHF"}:
        return "CHF"
    if raw in {"CAD", "C$"}:
        return "CAD"
    if raw in {"AUD", "A$"}:
        return "AUD"
    if raw in {"CNY", "CN¥", "RMB", "YUAN"}:
        return "CNY"
    return _currency_from_event(country, raw if raw not in {"", "-", "N/A"} else None)


def _parse_trading_economics_datetime(value: Any, target_date: date, report_timezone: str) -> tuple[str, str, str] | None:
    """Return (local_date, HH:MM, timezone_name) from Trading Economics UTC Date field."""
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception:
        try:
            dt = datetime.strptime(text[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            try:
                dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                return None

    # Trading Economics docs describe Date as UTC. Treat naive datetimes as UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local_dt = dt.astimezone(_tz(report_timezone))
    if local_dt.date() != target_date:
        return None
    return local_dt.date().isoformat(), local_dt.strftime("%H:%M"), report_timezone


def _normalize_trading_economics_event(raw: dict[str, Any], target_date: date, report_timezone: str) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    title = str(raw.get("Event") or raw.get("event") or raw.get("Category") or raw.get("category") or "").strip()
    if not title:
        return None

    parsed_dt = _parse_trading_economics_datetime(raw.get("Date") or raw.get("date"), target_date, report_timezone)
    if parsed_dt is None:
        return None
    event_date, event_time, event_tz = parsed_dt

    country = raw.get("Country") or raw.get("country")
    currency = _trading_economics_currency(country, raw.get("Currency") or raw.get("currency"))

    importance_raw = raw.get("Importance") if raw.get("Importance") is not None else raw.get("importance")
    impact = _normalize_impact(importance_raw, title)
    if impact not in {"HIGH", "RED", "IMPORTANT"}:
        return None

    symbols = _affected_symbols(currency, title)
    if not symbols and str(currency).upper() in RELEVANT_RISK_CURRENCIES:
        symbols = _affected_symbols(str(currency).upper(), title)

    return {
        "date": event_date,
        "time": event_time,
        "timezone": event_tz,
        "currency": currency,
        "impact": "HIGH",
        "title": title,
        "country": country or "-",
        "category": raw.get("Category") or raw.get("category"),
        "symbols": symbols,
        "note": _event_trading_note(currency, title, "HIGH"),
        "source": "tradingeconomics",
        "actual": raw.get("Actual") if raw.get("Actual") is not None else raw.get("actual"),
        "forecast": raw.get("Forecast") if raw.get("Forecast") is not None else raw.get("forecast"),
        "previous": raw.get("Previous") if raw.get("Previous") is not None else raw.get("previous"),
        "te_forecast": raw.get("TEForecast") if raw.get("TEForecast") is not None else raw.get("te_forecast"),
        "calendar_id": raw.get("CalendarId") or raw.get("CalendarID") or raw.get("calendar_id"),
        "ticker": raw.get("Ticker") or raw.get("ticker"),
        "symbol": raw.get("Symbol") or raw.get("symbol"),
        "raw": raw,
    }




# =============================================================================
# FAIRECONOMY / FOREX FACTORY WEEKLY XML CALENDAR FALLBACK
# =============================================================================

FAIRECONOMY_DEFAULT_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
FAIRECONOMY_SOURCE = "faireconomy_forexfactory_xml"


def _faireconomy_calendar_enabled() -> bool:
    return _env_bool("ENABLE_FAIRECONOMY_FOREXFACTORY_CALENDAR", True)


def _faireconomy_calendar_endpoint() -> str:
    return (
        os.getenv("FAIRECONOMY_CALENDAR_URL")
        or os.getenv("FOREXFACTORY_CALENDAR_URL")
        or FAIRECONOMY_DEFAULT_URL
    )


def _faireconomy_cache_ttl_seconds() -> int:
    try:
        return max(300, int(os.getenv("FAIRECONOMY_CALENDAR_CACHE_TTL_SECONDS", "21600")))
    except Exception:
        return 21600


def _faireconomy_assumed_timezone() -> str:
    # The nfs.faireconomy.media weekly XML feed is commonly consumed as GMT/UTC by
    # trading systems. Keep this explicit and configurable so we never silently
    # treat feed times as Kyiv/local time.
    return os.getenv("FAIRECONOMY_CALENDAR_TIMEZONE", "UTC") or "UTC"


def _parse_faireconomy_datetime(
    date_text: Any,
    time_text: Any,
    target_date: date,
    report_timezone: str,
) -> tuple[str, str, str] | None:
    raw_date = str(date_text or "").strip()
    raw_time = str(time_text or "").strip()
    if not raw_date:
        return None

    parsed_date: date | None = None
    for fmt in ("%m-%d-%Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            parsed_date = datetime.strptime(raw_date, fmt).date()
            break
        except Exception:
            continue
    if parsed_date is None:
        return None

    # Parse normal intraday timestamps such as "12:30pm". For all-day/tentative
    # entries, keep the calendar item at 00:00 UTC so it is visible in the risk
    # block, but mark raw_time in the normalized payload.
    local_source_tz = ZoneInfo(_faireconomy_assumed_timezone())
    event_time = time(0, 0)
    if raw_time and raw_time.upper() not in {"ALL DAY", "TENTATIVE", "", "-"}:
        normalized = raw_time.replace(" ", "").lower()
        parsed_time: time | None = None
        for fmt in ("%I:%M%p", "%I%p", "%H:%M"):
            try:
                parsed_time = datetime.strptime(normalized, fmt).time()
                break
            except Exception:
                continue
        if parsed_time is not None:
            event_time = parsed_time

    source_dt = datetime.combine(parsed_date, event_time, tzinfo=local_source_tz)
    report_dt = source_dt.astimezone(ZoneInfo(report_timezone))

    if report_dt.date() != target_date:
        return None
    return report_dt.date().isoformat(), report_dt.strftime("%H:%M"), report_timezone


def _normalize_faireconomy_xml_event(raw: dict[str, Any], target_date: date, report_timezone: str) -> dict[str, Any] | None:
    title = str(raw.get("title") or raw.get("event") or "").strip()
    if not title:
        return None

    currency = str(raw.get("country") or raw.get("currency") or "").strip().upper()
    if currency not in RELEVANT_RISK_CURRENCIES:
        return None

    source_impact = _normalize_impact(raw.get("impact"), title)
    critical_medium = _is_critical_medium_macro_event(source_impact, title, currency)
    if source_impact not in {"HIGH", "RED", "IMPORTANT"} and not critical_medium:
        return None

    # Keep critical Medium events in the same risk pipeline to avoid false
    # EMPTY calendars, but preserve the original feed impact for transparency.
    impact = "HIGH" if critical_medium else "HIGH"
    risk_tier = "MACRO_WATCH_CRITICAL_MEDIUM" if critical_medium else "HIGH_IMPACT"

    parsed_dt = _parse_faireconomy_datetime(raw.get("date"), raw.get("time"), target_date, report_timezone)
    if parsed_dt is None:
        return None
    event_date, event_time, event_tz = parsed_dt

    symbols = _affected_symbols(currency, title)
    return {
        "date": event_date,
        "time": event_time,
        "timezone": event_tz,
        "currency": currency,
        "impact": impact,
        "source_impact": source_impact,
        "original_impact": source_impact,
        "risk_tier": risk_tier,
        "critical_medium": critical_medium,
        "title": title,
        "country": currency,
        "symbols": symbols,
        "note": _event_trading_note(currency, title, impact),
        "source": FAIRECONOMY_SOURCE,
        "source_reliability": "unofficial_weekly_xml_feed",
        "raw_time": str(raw.get("time") or "").strip(),
        "actual": raw.get("actual"),
        "forecast": raw.get("forecast"),
        "previous": raw.get("previous"),
        "raw": raw,
    }


def _parse_faireconomy_xml_payload(body: bytes) -> list[dict[str, Any]]:
    root = ET.fromstring(body)
    events: list[dict[str, Any]] = []
    for ev in root.findall(".//event"):
        item: dict[str, Any] = {}
        for child in list(ev):
            item[child.tag.lower()] = (child.text or "").strip()
        if item:
            events.append(item)
    return events


def _fetch_faireconomy_forexfactory_calendar(target_date: date) -> CalendarLoadResult:
    if not _faireconomy_calendar_enabled():
        return CalendarLoadResult(
            status="DISABLED",
            source=f"{FAIRECONOMY_SOURCE}_disabled",
            message="Faireconomy / Forex Factory XML calendar disabled.",
        )

    report_tz = os.getenv("REPORT_TIMEZONE", DEFAULT_TIMEZONE) or DEFAULT_TIMEZONE
    cache_path = _faireconomy_calendar_cache_path(target_date)
    ttl_seconds = _faireconomy_cache_ttl_seconds()

    cached = _safe_read_json(cache_path, {})
    if isinstance(cached, dict):
        fetched_at = str(cached.get("fetched_at_utc") or "")
        try:
            dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00")) if fetched_at else None
            if dt and dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt and (_now_utc() - dt.astimezone(timezone.utc)).total_seconds() <= ttl_seconds:
                normalized_events: list[dict[str, Any]] = []
                for raw_event in _extract_list(cached.get("raw_events", [])):
                    event = _normalize_faireconomy_xml_event(raw_event, target_date, report_tz)
                    if event:
                        normalized_events.append(event)
                normalized_events = _dedupe_calendar_events(normalized_events)
                return CalendarLoadResult(
                    status=MACRO_OK if normalized_events else MACRO_EMPTY,
                    source=f"{FAIRECONOMY_SOURCE}_cache",
                    events=normalized_events,
                    message="Using cached Faireconomy / Forex Factory weekly XML calendar.",
                    cache_path=str(cache_path),
                    macro_risk_status=MACRO_OK if normalized_events else MACRO_EMPTY,
                    data_freshness="faireconomy_cache",
                )
        except Exception:
            pass

    endpoint = _faireconomy_calendar_endpoint()
    try:
        request = urllib.request.Request(
            endpoint,
            headers={
                "User-Agent": os.getenv(
                    "FAIRECONOMY_CALENDAR_USER_AGENT",
                    "Mozilla/5.0 AI-Market-Analyst macro-calendar",
                )
            },
        )
        with urllib.request.urlopen(request, timeout=float(os.getenv("FAIRECONOMY_CALENDAR_TIMEOUT_SEC", "20"))) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        return CalendarLoadResult(
            status=MACRO_UNKNOWN_CONSERVATIVE,
            source=FAIRECONOMY_SOURCE,
            events=[],
            message=f"Faireconomy / Forex Factory XML calendar HTTP error: {exc.code}.",
            cache_path=str(cache_path),
            provider_error=str(exc),
            macro_risk_status=MACRO_UNKNOWN_CONSERVATIVE,
        )
    except Exception as exc:
        return CalendarLoadResult(
            status=MACRO_UNKNOWN_CONSERVATIVE,
            source=FAIRECONOMY_SOURCE,
            events=[],
            message=f"Faireconomy / Forex Factory XML calendar unavailable: {exc}.",
            cache_path=str(cache_path),
            provider_error=str(exc),
            macro_risk_status=MACRO_UNKNOWN_CONSERVATIVE,
        )

    try:
        raw_events = _parse_faireconomy_xml_payload(body)
    except Exception as exc:
        return CalendarLoadResult(
            status=MACRO_UNKNOWN_CONSERVATIVE,
            source=FAIRECONOMY_SOURCE,
            events=[],
            message=f"Faireconomy / Forex Factory XML calendar parse error: {exc}.",
            cache_path=str(cache_path),
            provider_error=str(exc),
            macro_risk_status=MACRO_UNKNOWN_CONSERVATIVE,
        )

    normalized_events: list[dict[str, Any]] = []
    for raw_event in raw_events:
        event = _normalize_faireconomy_xml_event(raw_event, target_date, report_tz)
        if event:
            normalized_events.append(event)
    normalized_events = _dedupe_calendar_events(normalized_events)

    payload = {
        "source": FAIRECONOMY_SOURCE,
        "status": MACRO_OK if normalized_events else MACRO_EMPTY,
        "fetched_at_utc": _now_utc().isoformat(),
        "target_date": target_date.isoformat(),
        "endpoint": endpoint,
        "assumed_source_timezone": _faireconomy_assumed_timezone(),
        "raw_event_count": len(raw_events),
        "events": normalized_events,
        "raw_events": raw_events,
    }
    try:
        _safe_write_json(cache_path, payload)
    except Exception:
        pass

    return CalendarLoadResult(
        status=MACRO_OK if normalized_events else MACRO_EMPTY,
        source=FAIRECONOMY_SOURCE,
        events=normalized_events,
        message="Loaded Faireconomy / Forex Factory weekly XML calendar, including critical Medium macro watch events.",
        cache_path=str(cache_path),
        macro_risk_status=MACRO_OK if normalized_events else MACRO_EMPTY,
        data_freshness="faireconomy_fresh",
    )


# =============================================================================
# EODHD ECONOMIC EVENTS PRIMARY CALENDAR
# =============================================================================

EODHD_DEFAULT_COUNTRIES: tuple[str, ...] = (
    "US",
    "GB",
    "JP",
    "CH",
    "CA",
    "AU",
    "CN",
    "DE",
)


def _eodhd_economic_events_enabled() -> bool:
    return str(os.getenv("ENABLE_EODHD_ECONOMIC_EVENTS", os.getenv("ENABLE_EODHD_ECONOMIC_CALENDAR", "true"))).strip().lower() in {"1", "true", "yes", "on"}


def _eodhd_economic_events_api_token() -> str:
    for name in (
        "EODHD_API_TOKEN",
        "EODHD_API_KEY",
        "EODHD_ECONOMIC_EVENTS_API_TOKEN",
        "EODHD_ECONOMIC_CALENDAR_API_TOKEN",
        "EODHISTORICALDATA_API_TOKEN",
    ):
        value = str(os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _eodhd_economic_events_endpoint() -> str:
    return str(
        os.getenv("EODHD_ECONOMIC_EVENTS_ENDPOINT")
        or os.getenv("EODHD_ECONOMIC_CALENDAR_ENDPOINT")
        or "https://eodhd.com/api/economic-events"
    ).strip()


def _eodhd_economic_events_countries() -> list[str]:
    raw = os.getenv("EODHD_ECONOMIC_EVENTS_COUNTRIES") or os.getenv("EODHD_ECONOMIC_CALENDAR_COUNTRIES")
    if raw:
        countries = [x.strip().upper() for x in raw.split(",") if x.strip()]
        if countries:
            return countries
    # Empty list means one broad API call without a country filter.
    return []


def _parse_eodhd_datetime(value: Any, target_date: date, default_timezone: str) -> tuple[str, str, str] | None:
    """Parse EODHD economic-events date into internal calendar contract."""
    event_tz = str(default_timezone or "UTC").strip() or "UTC"
    if value is None:
        return target_date.isoformat(), "", event_tz

    text = str(value).strip()
    if not text:
        return target_date.isoformat(), "", event_tz

    try:
        d = date.fromisoformat(text[:10])
        if len(text) <= 10 or ("T" not in text and ":" not in text):
            return d.isoformat(), "", event_tz
    except Exception:
        pass

    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_tz(event_tz))
        else:
            event_tz = str(dt.tzinfo)
        return dt.date().isoformat(), dt.strftime("%H:%M"), event_tz
    except Exception:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=_tz(event_tz))
            return dt.date().isoformat(), dt.strftime("%H:%M"), event_tz
        except Exception:
            continue

    return None


def _normalize_eodhd_event_title(raw: dict[str, Any]) -> str:
    base = str(raw.get("type") or raw.get("event") or raw.get("title") or raw.get("name") or "").strip()
    comparison = str(raw.get("comparison") or "").strip()
    period = str(raw.get("period") or "").strip()

    title = base
    details: list[str] = []
    if comparison:
        details.append(comparison.upper())
    if period:
        details.append(period)
    if details and base:
        title = f"{base} ({', '.join(details)})"
    return title.strip()


def _normalize_eodhd_economic_event(raw: dict[str, Any], target_date: date, report_timezone: str) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    title = _normalize_eodhd_event_title(raw)
    if not title:
        return None

    country = raw.get("country") or raw.get("Country")
    currency = _currency_from_event(country, raw.get("currency") or raw.get("Currency"))
    if str(currency or "-").upper() not in RELEVANT_RISK_CURRENCIES:
        return None

    parsed_dt = _parse_eodhd_datetime(
        raw.get("date") or raw.get("Date") or raw.get("datetime") or raw.get("time"),
        target_date,
        os.getenv("EODHD_ECONOMIC_EVENTS_TIMEZONE") or os.getenv("EODHD_ECONOMIC_CALENDAR_TIMEZONE") or "UTC",
    )
    if parsed_dt is None:
        return None
    event_date, event_time, event_tz = parsed_dt
    if event_date != target_date.isoformat():
        return None

    # EODHD public docs do not expose a direct impact field. Keep only titles
    # matching our high-impact macro keyword set.
    impact = _normalize_impact(raw.get("impact") or raw.get("importance"), title)
    if impact not in {"HIGH", "RED", "IMPORTANT"}:
        return None

    symbols = _affected_symbols(currency, title)
    if not symbols and str(currency).upper() in RELEVANT_RISK_CURRENCIES:
        symbols = _affected_symbols(str(currency).upper(), title)

    return {
        "date": event_date,
        "time": event_time,
        "timezone": event_tz,
        "currency": currency,
        "impact": "HIGH",
        "title": title,
        "country": country or "-",
        "category": raw.get("type") or raw.get("category") or raw.get("Category"),
        "symbols": symbols,
        "note": _event_trading_note(currency, title, "HIGH"),
        "source": "eodhd_economic_events",
        "actual": raw.get("actual") if raw.get("actual") is not None else raw.get("Actual"),
        "forecast": raw.get("estimate") if raw.get("estimate") is not None else raw.get("forecast"),
        "previous": raw.get("previous") if raw.get("previous") is not None else raw.get("Previous"),
        "comparison": raw.get("comparison"),
        "period": raw.get("period"),
        "change": raw.get("change"),
        "change_percentage": raw.get("change_percentage") if raw.get("change_percentage") is not None else raw.get("changePercentage"),
        "raw": raw,
    }


def _fetch_eodhd_economic_events_calendar(target_date: date) -> CalendarLoadResult:
    """Fetch high-impact macro events from EODHD Economic Events Data API."""
    if not _eodhd_economic_events_enabled():
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="eodhd_economic_events_disabled",
            message="EODHD economic events calendar disabled by ENABLE_EODHD_ECONOMIC_EVENTS.",
        )

    api_token = _eodhd_economic_events_api_token()
    if not api_token:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="eodhd_economic_events",
            message="EODHD_API_TOKEN / EODHD_API_KEY is missing.",
        )

    cache_path = _eodhd_economic_events_cache_path(target_date)
    cache_ttl_sec = int(os.getenv("EODHD_ECONOMIC_EVENTS_CACHE_TTL_SEC", os.getenv("ECONOMIC_CALENDAR_CACHE_TTL_SEC", "21600")))
    force_refresh = str(os.getenv("EODHD_ECONOMIC_EVENTS_FORCE_REFRESH", os.getenv("EODHD_ECONOMIC_CALENDAR_FORCE_REFRESH", "false"))).strip().lower() in {"1", "true", "yes", "on"}

    if cache_path.exists() and not force_refresh:
        try:
            age_sec = (_now_utc().timestamp() - cache_path.stat().st_mtime)
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if age_sec <= cache_ttl_sec and isinstance(cached, dict):
                events = _extract_list(cached.get("events", []))
                status = MACRO_OK if events else MACRO_EMPTY
                return CalendarLoadResult(
                    status=status,
                    source=str(cached.get("source") or "eodhd_economic_events_cache"),
                    events=events,
                    message=f"Loaded EODHD economic events calendar from cache. age_sec={round(age_sec, 1)}",
                    cache_path=str(cache_path),
                )
        except Exception:
            pass

    endpoint = _eodhd_economic_events_endpoint()
    countries = _eodhd_economic_events_countries()
    timeout = float(os.getenv("EODHD_ECONOMIC_EVENTS_TIMEOUT_SEC", os.getenv("EODHD_ECONOMIC_CALENDAR_TIMEOUT_SEC", "12")))
    limit = str(os.getenv("EODHD_ECONOMIC_EVENTS_LIMIT", "1000"))
    report_tz = os.getenv("REPORT_TIMEZONE") or DEFAULT_TIMEZONE

    def _request(country: str | None = None) -> list[Any]:
        params_payload: dict[str, Any] = {
            "api_token": api_token,
            "from": target_date.isoformat(),
            "to": target_date.isoformat(),
            "limit": limit,
            "fmt": "json",
        }
        if country:
            params_payload["country"] = country
        params = urllib.parse.urlencode(params_payload)
        separator = "&" if "?" in endpoint else "?"
        url = f"{endpoint}{separator}{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "AI-Market-Analyst/1.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
        payload = json.loads(body)
        return _extract_list(payload)

    raw_events: list[Any] = []
    try:
        if countries:
            for country in countries:
                raw_events.extend(_request(country))
        else:
            raw_events.extend(_request(None))
    except urllib.error.HTTPError as exc:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="eodhd_economic_events",
            message=f"EODHD economic events HTTP error: {exc.code}.",
            provider_error=str(exc),
            cache_path=str(cache_path),
        )
    except Exception as exc:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="eodhd_economic_events",
            message="EODHD economic events calendar unavailable.",
            provider_error=f"{type(exc).__name__}: {exc}",
            cache_path=str(cache_path),
        )

    normalized: list[dict[str, Any]] = []
    for raw_event in raw_events:
        event = _normalize_eodhd_economic_event(raw_event, target_date, report_tz)
        if event:
            normalized.append(event)

    normalized = _dedupe_calendar_events(normalized)
    status = MACRO_OK if normalized else MACRO_EMPTY
    message = "Loaded from EODHD economic events API." if normalized else "EODHD economic events returned no high-impact events for this date."
    payload_to_cache = {
        "source": "eodhd_economic_events",
        "status": status,
        "date": target_date.isoformat(),
        "updated_at_utc": _now_utc().isoformat(),
        "events": normalized,
        "raw_count": len(raw_events),
        "country_filter": countries,
    }
    try:
        _safe_write_json(cache_path, payload_to_cache)
    except Exception:
        pass

    return CalendarLoadResult(
        status=status,
        source="eodhd_economic_events",
        events=normalized,
        message=message,
        cache_path=str(cache_path),
        macro_risk_status=status,
    )


def _fmp_economic_calendar_enabled() -> bool:
    return str(os.getenv("ENABLE_FMP_ECONOMIC_CALENDAR", "true")).strip().lower() in {"1", "true", "yes", "on"}


def _fmp_economic_calendar_api_key() -> str:
    for name in (
        "FMP_API_KEY",
        "FMP_ECONOMIC_CALENDAR_API_KEY",
        "FINANCIAL_MODELING_PREP_API_KEY",
    ):
        value = str(os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _fmp_economic_calendar_endpoint() -> str:
    return str(
        os.getenv("FMP_ECONOMIC_CALENDAR_ENDPOINT")
        or "https://financialmodelingprep.com/stable/economic-calendar"
    ).strip()


def _parse_fmp_datetime(value: Any, target_date: date, default_timezone: str) -> tuple[str, str, str] | None:
    """Parse FMP economic-calendar date/time into the internal calendar contract."""
    event_tz = str(default_timezone or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    if value is None:
        return target_date.isoformat(), "", event_tz

    text = str(value).strip()
    if not text:
        return target_date.isoformat(), "", event_tz

    try:
        d = date.fromisoformat(text[:10])
        if len(text) <= 10 or ("T" not in text and ":" not in text):
            return d.isoformat(), "", event_tz
    except Exception:
        pass

    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_tz(event_tz))
        else:
            event_tz = str(dt.tzinfo)
        return dt.date().isoformat(), dt.strftime("%H:%M"), event_tz
    except Exception:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=_tz(event_tz))
            return dt.date().isoformat(), dt.strftime("%H:%M"), event_tz
        except Exception:
            continue

    return None


def _normalize_fmp_calendar_event(raw: dict[str, Any], target_date: date, report_timezone: str) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    title = str(
        raw.get("event")
        or raw.get("Event")
        or raw.get("title")
        or raw.get("name")
        or raw.get("type")
        or ""
    ).strip()
    if not title:
        return None

    country = raw.get("country") or raw.get("Country")
    currency = _currency_from_event(country, raw.get("currency") or raw.get("Currency"))
    parsed_dt = _parse_fmp_datetime(
        raw.get("date") or raw.get("Date") or raw.get("datetime") or raw.get("time"),
        target_date,
        os.getenv("FMP_ECONOMIC_CALENDAR_TIMEZONE") or report_timezone or DEFAULT_TIMEZONE,
    )
    if parsed_dt is None:
        return None
    event_date, event_time, event_tz = parsed_dt
    if event_date != target_date.isoformat():
        return None

    # Some FMP calendar payloads include impact; some only include event/country/value fields.
    # When impact is absent, keep only known market-moving macro titles via HIGH_IMPACT_KEYWORDS.
    impact = _normalize_impact(
        raw.get("impact")
        or raw.get("Impact")
        or raw.get("importance")
        or raw.get("volatility"),
        title,
    )
    if impact not in {"HIGH", "RED", "IMPORTANT"}:
        return None

    symbols = _affected_symbols(currency, title)
    if not symbols and str(currency).upper() in RELEVANT_RISK_CURRENCIES:
        symbols = _affected_symbols(str(currency).upper(), title)

    return {
        "date": event_date,
        "time": event_time,
        "timezone": event_tz,
        "currency": currency,
        "impact": "HIGH",
        "title": title,
        "country": country or "-",
        "category": raw.get("category") or raw.get("Category") or raw.get("type"),
        "symbols": symbols,
        "note": _event_trading_note(currency, title, "HIGH"),
        "source": "fmp_economic_calendar",
        "actual": raw.get("actual") if raw.get("actual") is not None else raw.get("Actual"),
        "forecast": (
            raw.get("estimate") if raw.get("estimate") is not None else
            raw.get("forecast") if raw.get("forecast") is not None else
            raw.get("Forecast")
        ),
        "previous": raw.get("previous") if raw.get("previous") is not None else raw.get("Previous"),
        "change": raw.get("change") if raw.get("change") is not None else raw.get("Change"),
        "change_percentage": raw.get("changePercentage") or raw.get("change_percentage"),
        "raw": raw,
    }


def _fetch_fmp_economic_calendar(target_date: date) -> CalendarLoadResult:
    """Fetch high-impact macro events from Financial Modeling Prep economic calendar."""
    if not _fmp_economic_calendar_enabled():
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="fmp_economic_calendar_disabled",
            message="FMP economic calendar disabled by ENABLE_FMP_ECONOMIC_CALENDAR.",
        )

    api_key = _fmp_economic_calendar_api_key()
    if not api_key:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="fmp_economic_calendar",
            message="FMP_API_KEY / FMP_ECONOMIC_CALENDAR_API_KEY is missing.",
        )

    cache_path = _fmp_economic_calendar_cache_path(target_date)
    cache_ttl_sec = int(os.getenv("FMP_ECONOMIC_CALENDAR_CACHE_TTL_SEC", os.getenv("ECONOMIC_CALENDAR_CACHE_TTL_SEC", "21600")))
    force_refresh = str(os.getenv("FMP_ECONOMIC_CALENDAR_FORCE_REFRESH", "false")).strip().lower() in {"1", "true", "yes", "on"}

    if cache_path.exists() and not force_refresh:
        try:
            age_sec = (_now_utc().timestamp() - cache_path.stat().st_mtime)
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if age_sec <= cache_ttl_sec and isinstance(cached, dict):
                events = _extract_list(cached.get("events", []))
                status = MACRO_OK if events else MACRO_EMPTY
                return CalendarLoadResult(
                    status=status,
                    source=str(cached.get("source") or "fmp_economic_calendar_cache"),
                    events=events,
                    message=f"Loaded FMP economic calendar from cache. age_sec={round(age_sec, 1)}",
                    cache_path=str(cache_path),
                )
        except Exception:
            pass

    endpoint = _fmp_economic_calendar_endpoint()
    params = urllib.parse.urlencode(
        {
            "from": target_date.isoformat(),
            "to": target_date.isoformat(),
            "apikey": api_key,
        }
    )
    separator = "&" if "?" in endpoint else "?"
    url = f"{endpoint}{separator}{params}"

    try:
        timeout = float(os.getenv("FMP_ECONOMIC_CALENDAR_TIMEOUT_SEC", "12"))
        req = urllib.request.Request(url, headers={"User-Agent": "AI-Market-Analyst/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
        payload = json.loads(body)
    except urllib.error.HTTPError as exc:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="fmp_economic_calendar",
            message=f"FMP economic calendar HTTP error: {exc.code}.",
            provider_error=str(exc),
            cache_path=str(cache_path),
        )
    except Exception as exc:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="fmp_economic_calendar",
            message="FMP economic calendar unavailable.",
            provider_error=f"{type(exc).__name__}: {exc}",
            cache_path=str(cache_path),
        )

    raw_events = _extract_list(payload)
    report_tz = os.getenv("REPORT_TIMEZONE") or DEFAULT_TIMEZONE
    normalized: list[dict[str, Any]] = []
    for raw_event in raw_events:
        event = _normalize_fmp_calendar_event(raw_event, target_date, report_tz)
        if event:
            normalized.append(event)

    normalized = _dedupe_calendar_events(normalized)
    status = MACRO_OK if normalized else MACRO_EMPTY
    message = "Loaded from FMP economic calendar API." if normalized else "FMP economic calendar returned no high-impact events for this date."
    payload_to_cache = {
        "source": "fmp_economic_calendar",
        "status": status,
        "date": target_date.isoformat(),
        "updated_at_utc": _now_utc().isoformat(),
        "events": normalized,
        "raw_count": len(raw_events),
    }
    try:
        _safe_write_json(cache_path, payload_to_cache)
    except Exception:
        pass

    return CalendarLoadResult(
        status=status,
        source="fmp_economic_calendar",
        events=normalized,
        message=message,
        cache_path=str(cache_path),
        macro_risk_status=status,
    )


def _fetch_trading_economics_calendar(target_date: date) -> CalendarLoadResult:
    """Fetch high-impact macro events from Trading Economics as automatic backup."""
    if not _trading_economics_enabled():
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="tradingeconomics_disabled",
            message="Trading Economics backup calendar disabled by ENABLE_TRADING_ECONOMICS_CALENDAR.",
        )

    credentials = _trading_economics_credentials()
    if not credentials:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="tradingeconomics",
            message="TRADING_ECONOMICS_API_KEY/CREDENTIALS is missing.",
        )

    cache_path = _trading_economics_calendar_cache_path(target_date)
    cache_ttl_sec = int(os.getenv("TRADING_ECONOMICS_CALENDAR_CACHE_TTL_SEC", os.getenv("ECONOMIC_CALENDAR_CACHE_TTL_SEC", "21600")))
    force_refresh = str(os.getenv("TRADING_ECONOMICS_FORCE_REFRESH", "false")).strip().lower() in {"1", "true", "yes", "on"}

    if cache_path.exists() and not force_refresh:
        try:
            age_sec = (_now_utc().timestamp() - cache_path.stat().st_mtime)
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if age_sec <= cache_ttl_sec and isinstance(cached, dict):
                events = _extract_list(cached.get("events", []))
                status = MACRO_OK if events else MACRO_EMPTY
                return CalendarLoadResult(
                    status=status,
                    source=str(cached.get("source") or "tradingeconomics_cache"),
                    events=events,
                    message=f"Loaded Trading Economics from cache. age_sec={round(age_sec, 1)}",
                    cache_path=str(cache_path),
                )
        except Exception:
            pass

    countries = _trading_economics_countries()
    country_path = _trading_economics_country_slug(countries)
    if not country_path:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="tradingeconomics",
            message="Trading Economics countries list is empty.",
            cache_path=str(cache_path),
        )

    report_tz = os.getenv("REPORT_TIMEZONE") or DEFAULT_TIMEZONE
    params = urllib.parse.urlencode(
        {
            "c": credentials,
            "importance": str(os.getenv("TRADING_ECONOMICS_IMPORTANCE", "3")),
            "f": "json",
        }
    )
    url = f"https://api.tradingeconomics.com/calendar/country/{country_path}/{target_date.isoformat()}/{target_date.isoformat()}?{params}"

    try:
        timeout = float(os.getenv("TRADING_ECONOMICS_TIMEOUT_SEC", "12"))
        req = urllib.request.Request(url, headers={"User-Agent": "AI-Market-Analyst/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
        payload = json.loads(body)
    except urllib.error.HTTPError as exc:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="tradingeconomics",
            message=f"Trading Economics HTTP error: {exc.code}.",
            provider_error=str(exc),
            cache_path=str(cache_path),
        )
    except Exception as exc:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="tradingeconomics",
            message="Trading Economics calendar unavailable.",
            provider_error=f"{type(exc).__name__}: {exc}",
            cache_path=str(cache_path),
        )

    raw_events = _extract_list(payload)
    normalized: list[dict[str, Any]] = []
    for raw_event in raw_events:
        event = _normalize_trading_economics_event(raw_event, target_date, report_tz)
        if event:
            normalized.append(event)

    normalized = _dedupe_calendar_events(normalized)
    status = MACRO_OK if normalized else MACRO_EMPTY

    cache_payload = {
        "source": "tradingeconomics",
        "status": status,
        "date": target_date.isoformat(),
        "updated_at_utc": _now_utc().isoformat(),
        "countries": countries,
        "events": normalized,
        "raw_count": len(raw_events),
    }
    if _macro_cache_writes_enabled():
        try:
            _safe_write_json(cache_path, cache_payload)
        except Exception:
            pass

    return CalendarLoadResult(
        status=status,
        source="tradingeconomics",
        events=normalized,
        message="Loaded from Trading Economics API." if normalized else "Trading Economics returned no high-impact events for this date.",
        cache_path=str(cache_path),
    )


def _fetch_finnhub_calendar(target_date: date) -> CalendarLoadResult:
    enabled = str(os.getenv("ENABLE_ECONOMIC_CALENDAR", "true")).strip().lower() in {"1", "true", "yes", "on"}
    provider = str(os.getenv("ECONOMIC_CALENDAR_PROVIDER", "finnhub")).strip().lower()
    if not enabled:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="disabled",
            message="Economic calendar disabled by ENABLE_ECONOMIC_CALENDAR.",
        )
    if provider not in {"finnhub", "auto"}:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source=provider or "unknown",
            message=f"Economic calendar provider is not Finnhub: {provider}.",
        )

    token = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_TOKEN")
    if not token:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="finnhub",
            message="FINNHUB_API_KEY is missing.",
        )

    cache_path = _economic_calendar_cache_path(target_date)
    cache_ttl_sec = int(os.getenv("ECONOMIC_CALENDAR_CACHE_TTL_SEC", "21600"))
    force_refresh = str(os.getenv("ECONOMIC_CALENDAR_FORCE_REFRESH", "false")).strip().lower() in {"1", "true", "yes", "on"}

    if cache_path.exists() and not force_refresh:
        try:
            age_sec = (_now_utc().timestamp() - cache_path.stat().st_mtime)
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if age_sec <= cache_ttl_sec and isinstance(cached, dict):
                events = _extract_list(cached.get("events", []))
                status = "OK" if events else "EMPTY"
                return CalendarLoadResult(
                    status=status,
                    source=str(cached.get("source") or "finnhub_cache"),
                    events=events,
                    message=f"Loaded from cache. age_sec={round(age_sec, 1)}",
                    cache_path=str(cache_path),
                )
        except Exception:
            pass

    params = urllib.parse.urlencode(
        {
            "from": target_date.isoformat(),
            "to": target_date.isoformat(),
            "token": token,
        }
    )
    url = f"https://finnhub.io/api/v1/calendar/economic?{params}"

    try:
        timeout = float(os.getenv("FINNHUB_TIMEOUT_SEC", "12"))
        req = urllib.request.Request(url, headers={"User-Agent": "AI-Market-Analyst/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
        payload = json.loads(body)
    except urllib.error.HTTPError as exc:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="finnhub",
            message=f"Finnhub HTTP error: {exc.code}.",
            provider_error=str(exc),
            cache_path=str(cache_path),
        )
    except Exception as exc:
        return CalendarLoadResult(
            status="UNAVAILABLE",
            source="finnhub",
            message="Finnhub economic calendar unavailable.",
            provider_error=f"{type(exc).__name__}: {exc}",
            cache_path=str(cache_path),
        )

    raw_events = payload.get("economicCalendar") if isinstance(payload, dict) else None
    if not isinstance(raw_events, list):
        raw_events = _extract_list(payload)

    normalized: list[dict[str, Any]] = []
    for raw_event in raw_events:
        if not isinstance(raw_event, dict):
            continue
        event = _normalize_finnhub_event(raw_event, target_date)
        if not event:
            continue
        if event.get("date") != target_date.isoformat():
            continue
        normalized.append(event)

    normalized.sort(key=_calendar_display_sort_key)

    status = "OK" if normalized else "EMPTY"
    cache_payload = {
        "source": "finnhub",
        "status": status,
        "date": target_date.isoformat(),
        "updated_at_utc": _now_utc().isoformat(),
        "events": normalized,
        "raw_count": len(raw_events),
    }
    if _macro_cache_writes_enabled():
        try:
            _safe_write_json(cache_path, cache_payload)
        except Exception:
            pass

    return CalendarLoadResult(
        status=status,
        source="finnhub",
        events=normalized,
        message="Loaded from Finnhub API." if normalized else "Finnhub returned no events for this date.",
        cache_path=str(cache_path),
    )


def _normalize_calendar_event_payload(event: dict[str, Any], target_date: date, *, source: str) -> dict[str, Any] | None:
    """Normalize manual/static/cache calendar entries into the internal event contract."""
    if not isinstance(event, dict):
        return None

    title = str(event.get("title") or event.get("event") or event.get("name") or "").strip()
    if not title:
        return None

    event_date = str(event.get("date") or target_date.isoformat()).strip()
    if event_date != target_date.isoformat():
        return None

    event_time = str(event.get("time") or "").strip()
    event_tz = str(event.get("timezone") or event.get("tz") or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    currency = _currency_from_event(event.get("country"), event.get("currency"))
    impact = _normalize_impact(event.get("impact") or event.get("importance"), title)
    if impact and impact not in {"HIGH", "RED", "IMPORTANT"}:
        return None

    symbols = event.get("symbols")
    if not isinstance(symbols, list):
        symbols = _affected_symbols(currency, title)

    normalized = dict(event)
    normalized.update(
        {
            "date": event_date,
            "time": event_time,
            "timezone": event_tz,
            "currency": currency,
            "impact": "HIGH" if impact in {"RED", "IMPORTANT"} else (impact or "HIGH"),
            "title": title,
            "symbols": [str(x).strip().upper() for x in symbols if str(x).strip()],
            "note": event.get("note") or _event_trading_note(currency, title, "HIGH"),
            "source": str(event.get("source") or source),
        }
    )
    return normalized



def _event_title(event: dict[str, Any]) -> str:
    return str(event.get("title") or event.get("event") or event.get("name") or "").strip()


def _event_title_upper(event: dict[str, Any]) -> str:
    return _event_title(event).upper()


def _is_fomc_event(event: dict[str, Any]) -> bool:
    return _is_strict_fomc_title(_event_title(event), event.get("currency"))


def _is_fomc_press_conference(event: dict[str, Any]) -> bool:
    return _is_fomc_press_conference_title(_event_title(event), event.get("currency"))


def _is_fomc_statement(event: dict[str, Any]) -> bool:
    title = _event_title_upper(event)
    return _is_fomc_event(event) and "STATEMENT" in title


def _is_fomc_projections(event: dict[str, Any]) -> bool:
    title = _event_title_upper(event)
    return _is_fomc_event(event) and ("PROJECTION" in title or "DOT PLOT" in title)


def _is_rate_decision_event(event: dict[str, Any]) -> bool:
    return _is_fomc_rate_decision_title(_event_title(event), event.get("currency"))


def _calendar_event_type_priority(event: dict[str, Any]) -> int:
    """
    Stable ordering inside same timestamp.

    FOMC clusters must read like the actual decision flow:
    Rate Decision -> Statement -> Projections -> Press Conference.
    """
    if _is_fomc_event(event):
        if _is_rate_decision_event(event):
            return 10
        if _is_fomc_statement(event):
            return 20
        if _is_fomc_projections(event):
            return 30
        if _is_fomc_press_conference(event):
            return 40
        return 35

    title = _event_title_upper(event)
    if "CPI" in title or "PCE" in title or "NFP" in title or "NON FARM" in title or "NON-FARM" in title:
        return 50
    if "RETAIL SALES" in title:
        return 60
    if "CRUDE" in title or "OIL" in title or "OPEC" in title:
        return 70
    return 100


def _calendar_display_sort_key(event: dict[str, Any]) -> tuple[str, str, int, str, str]:
    return (
        str(event.get("date") or ""),
        str(event.get("time") or "99:99"),
        _calendar_event_type_priority(event),
        str(event.get("currency") or ""),
        _event_title_upper(event),
    )


def _select_events_for_high_impact_section(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Keep the Telegram risk block concise, but never hide parts of an FOMC cluster.
    """
    selected = sorted(events, key=_calendar_display_sort_key)
    if not selected:
        return []

    try:
        max_events = int(os.getenv("BRIEFING_HIGH_IMPACT_MAX_EVENTS", "8"))
    except Exception:
        max_events = 8

    if any(_is_fomc_event(e) for e in selected):
        max_events = max(max_events, 12)

    return selected[:max_events]


def _macro_regime_for_event(event: dict[str, Any]) -> str:
    minutes = event.get("minutes_since_event")
    try:
        minutes_float = float(minutes)
    except (TypeError, ValueError):
        minutes_float = None

    if _is_fomc_press_conference(event):
        return "FOMC_PRESSER_LOCK"

    if _is_fomc_event(event):
        if minutes_float is not None and minutes_float >= 0:
            return "FOMC_POST_NEWS_LOCK"
        return "FOMC_DAY_LOCK"

    title = _event_title_upper(event)
    if "CRUDE" in title or "OIL" in title or "OPEC" in title:
        return "OIL_POST_NEWS_ACCEPTANCE_REQUIRED"

    return "POST_NEWS_ACCEPTANCE_REQUIRED"


def _macro_event_selection_priority(event: dict[str, Any]) -> tuple[int, float, int, str]:
    """
    Pick the event that should drive the NY post-news regime.

    Priority:
    - FOMC press conference active/upcoming/recent
    - FOMC decision/statement/projections
    - other USD high-impact data
    - crude/oil unless no broader macro is closer
    """
    title = _event_title_upper(event)
    minutes_raw = event.get("minutes_since_event")
    try:
        minutes = float(minutes_raw)
    except (TypeError, ValueError):
        minutes = 9999.0

    if _is_fomc_press_conference(event):
        group = 0
    elif _is_fomc_event(event):
        group = 1
    elif "CPI" in title or "PCE" in title or "NFP" in title or "NON FARM" in title or "NON-FARM" in title:
        group = 2
    elif "RETAIL SALES" in title:
        group = 3
    elif "CRUDE" in title or "OIL" in title or "OPEC" in title:
        group = 5
    else:
        group = 4

    return (group, abs(minutes), _calendar_event_type_priority(event), title)


def _select_primary_macro_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not events:
        return None
    return sorted(events, key=_macro_event_selection_priority)[0]


def _has_fomc_cluster(events: list[dict[str, Any]]) -> bool:
    return any(_is_fomc_event(event) for event in events)


def _has_fomc_press_conference(events: list[dict[str, Any]]) -> bool:
    return any(_is_fomc_press_conference(event) for event in events)


def _has_source_high_calendar_event(events: list[dict[str, Any]]) -> bool:
    for event in events:
        source_impact = _normalize_impact(
            event.get("source_impact") or event.get("original_impact") or event.get("impact"),
            str(event.get("title") or event.get("event") or ""),
        )
        if source_impact in {"HIGH", "RED", "IMPORTANT"}:
            return True
    return False


def _has_critical_medium_calendar_event(events: list[dict[str, Any]]) -> bool:
    return any(bool(event.get("critical_medium")) or str(event.get("risk_tier") or "").upper() == "MACRO_WATCH_CRITICAL_MEDIUM" for event in events)


def _calendar_macro_status_for_events(events: list[dict[str, Any]], *, backup: bool = True) -> str:
    if not events:
        return MACRO_EMPTY
    if _has_source_high_calendar_event(events):
        return MACRO_HIGH_FROM_BACKUP_CALENDAR if backup else MACRO_OK
    if _has_critical_medium_calendar_event(events):
        return MACRO_WATCH_FROM_BACKUP_CALENDAR
    return MACRO_OK if not backup else MACRO_HIGH_FROM_BACKUP_CALENDAR


def _calendar_event_impact_display(event: dict[str, Any]) -> str:
    source_impact = _normalize_impact(
        event.get("source_impact") or event.get("original_impact") or event.get("impact"),
        str(event.get("title") or event.get("event") or ""),
    )
    risk_tier = str(event.get("risk_tier") or "").upper()
    if risk_tier == "MACRO_WATCH_CRITICAL_MEDIUM" or bool(event.get("critical_medium")):
        return f"{source_impact}→SYSTEM WATCH"
    return source_impact or str(event.get("impact") or "HIGH").upper()


def _calendar_event_source_impact_note(event: dict[str, Any]) -> str | None:
    source_impact = _normalize_impact(
        event.get("source_impact") or event.get("original_impact") or event.get("impact"),
        str(event.get("title") or event.get("event") or ""),
    )
    risk_tier = str(event.get("risk_tier") or "").upper()
    if risk_tier == "MACRO_WATCH_CRITICAL_MEDIUM" or bool(event.get("critical_medium")):
        return f"Source impact: {source_impact}; system risk tier: MACRO_WATCH_CRITICAL_MEDIUM."
    return None


def _macro_risk_status_text(calendar: CalendarLoadResult) -> str:
    if calendar.macro_risk_status == MACRO_HIGH_FROM_BACKUP_CALENDAR:
        return MACRO_HIGH_FROM_BACKUP_CALENDAR
    if calendar.macro_risk_status == MACRO_WATCH_FROM_BACKUP_CALENDAR:
        return MACRO_WATCH_FROM_BACKUP_CALENDAR
    if calendar.status == MACRO_FALLBACK and calendar.events:
        return _calendar_macro_status_for_events(calendar.events, backup=True)
    return str(calendar.macro_risk_status or calendar.status or MACRO_UNKNOWN_CONSERVATIVE)


def _dedupe_calendar_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str, str]] = set()
    selected: list[dict[str, Any]] = []

    for event in events:
        key = (
            str(event.get("date") or ""),
            str(event.get("time") or ""),
            str(event.get("timezone") or ""),
            str(event.get("currency") or ""),
            str(event.get("title") or event.get("event") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        selected.append(event)

    selected.sort(key=_calendar_display_sort_key)
    return selected


def _load_manual_calendar_events(target_date: date) -> list[dict[str, Any]]:
    """Load operator-maintained macro events from /var/data/runtime/macro/manual_high_impact_events.json."""
    payload = _safe_read_json(_manual_high_impact_events_path(), [])
    events: list[dict[str, Any]] = []
    for event in _extract_list(payload):
        normalized = _normalize_calendar_event_payload(event, target_date, source="manual_macro_json")
        if normalized:
            events.append(normalized)
    return _dedupe_calendar_events(events)


def _load_static_calendar_events(target_date: date) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    # Existing static file/env fallback remains supported.
    events.extend(_extract_list(_safe_read_json(_high_impact_events_path(), [])))

    inline = os.getenv("HIGH_IMPACT_EVENTS_JSON")
    if inline:
        try:
            events.extend(_extract_list(json.loads(inline)))
        except Exception:
            pass

    events.extend(BUILTIN_HIGH_IMPACT_EVENTS)

    normalized_events: list[dict[str, Any]] = []
    for event in events:
        normalized = _normalize_calendar_event_payload(event, target_date, source="static_fallback")
        if normalized:
            normalized_events.append(normalized)

    return _dedupe_calendar_events(normalized_events)


def _write_last_good_calendar_cache(target_date: date, events: list[dict[str, Any]], *, source: str) -> None:
    if not events:
        return
    if not _macro_cache_writes_enabled():
        return

    payload = {
        "source": source,
        "status": MACRO_OK,
        "date": target_date.isoformat(),
        "updated_at_utc": _now_utc().isoformat(),
        "events": events,
    }
    try:
        _safe_write_json(_last_good_high_impact_events_path(), payload)
    except Exception:
        pass


def _load_last_good_calendar_events(target_date: date) -> tuple[list[dict[str, Any]], str | None]:
    path = _last_good_high_impact_events_path()
    payload = _safe_read_json(path, {})
    if not isinstance(payload, dict):
        return [], None

    max_age_hours = float(os.getenv("LAST_GOOD_MACRO_CACHE_MAX_AGE_HOURS", "36"))
    updated_at = str(payload.get("updated_at_utc") or "")
    if updated_at:
        try:
            dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_hours = (_now_utc() - dt.astimezone(timezone.utc)).total_seconds() / 3600.0
            if age_hours > max_age_hours:
                return [], f"last_good_cache_stale:{age_hours:.1f}h"
        except Exception:
            return [], "last_good_cache_bad_timestamp"

    events: list[dict[str, Any]] = []
    for event in _extract_list(payload.get("events", [])):
        normalized = _normalize_calendar_event_payload(event, target_date, source="last_good_cache")
        if normalized:
            normalized["source"] = "last_good_cache"
            events.append(normalized)

    return _dedupe_calendar_events(events), str(path)


def load_high_impact_calendar(target_date: date) -> CalendarLoadResult:
    """
    High-impact macro calendar cascade.

    Order v1.24:
    1) manual operator JSON override, if populated;
    2) Faireconomy / Forex Factory weekly XML calendar primary free fallback;
    3) EODHD Economic Events Data API provider, if plan allows it;
    4) Trading Economics automatic backup provider;
    5) Finnhub legacy backup provider;
    6) static fallback file/env/builtin;
    7) last-good cache;
    8) MACRO_UNKNOWN_CONSERVATIVE.

    Provider unavailable is never treated as "no news".
    """
    fallback_chain: list[str] = []

    manual_events = _load_manual_calendar_events(target_date)
    fallback_chain.append(f"primary:manual_macro_json:{'OK' if manual_events else 'EMPTY'}")
    if manual_events:
        _write_last_good_calendar_cache(target_date, manual_events, source="manual_macro_json")
        return CalendarLoadResult(
            status=MACRO_FALLBACK,
            source="manual_macro_json",
            events=manual_events,
            message="Using manual macro JSON override.",
            cache_path=str(_manual_high_impact_events_path()),
            macro_risk_status=_calendar_macro_status_for_events(manual_events, backup=True),
            fallback_chain=fallback_chain,
            data_freshness="manual_override",
            last_good_cache_path=str(_last_good_high_impact_events_path()),
        )

    ff_result = _fetch_faireconomy_forexfactory_calendar(target_date)
    fallback_chain.append(f"primary:{ff_result.source}:{ff_result.status}")
    if ff_result.status in {MACRO_OK, MACRO_EMPTY}:
        high_events = [
            e for e in ff_result.events
            if _normalize_impact(e.get("impact"), str(e.get("title") or "")) in {"HIGH", "RED", "IMPORTANT"}
        ]
        high_events = _dedupe_calendar_events(high_events)
        if high_events:
            _write_last_good_calendar_cache(target_date, high_events, source=ff_result.source)
        return CalendarLoadResult(
            status=MACRO_OK if high_events else MACRO_EMPTY,
            source=ff_result.source,
            events=high_events,
            message=ff_result.message,
            cache_path=ff_result.cache_path,
            provider_error=ff_result.provider_error,
            macro_risk_status=_calendar_macro_status_for_events(high_events, backup=True),
            fallback_chain=fallback_chain,
            data_freshness=ff_result.data_freshness or "faireconomy_forexfactory_xml",
            last_good_cache_path=str(_last_good_high_impact_events_path()),
        )

    result = _fetch_eodhd_economic_events_calendar(target_date)
    fallback_chain.append(f"backup:{result.source}:{result.status}")
    if result.status in {MACRO_OK, MACRO_EMPTY}:
        high_events = [
            e for e in result.events
            if _normalize_impact(e.get("impact"), str(e.get("title") or "")) in {"HIGH", "RED", "IMPORTANT"}
        ]
        high_events = _dedupe_calendar_events(high_events)
        if high_events:
            _write_last_good_calendar_cache(target_date, high_events, source=result.source)
        return CalendarLoadResult(
            status=MACRO_OK if high_events else MACRO_EMPTY,
            source=result.source,
            events=high_events,
            message=(ff_result.message or "Faireconomy provider unavailable") + " Using EODHD backup calendar.",
            cache_path=result.cache_path,
            provider_error=ff_result.provider_error or result.provider_error,
            macro_risk_status=MACRO_OK if high_events else MACRO_EMPTY,
            fallback_chain=fallback_chain,
            data_freshness="eodhd_backup",
            last_good_cache_path=str(_last_good_high_impact_events_path()),
        )

    te_result = _fetch_trading_economics_calendar(target_date)
    fallback_chain.append(f"backup:{te_result.source}:{te_result.status}")
    if te_result.status == MACRO_OK and te_result.events:
        high_events = [
            e for e in te_result.events
            if _normalize_impact(e.get("impact"), str(e.get("title") or "")) in {"HIGH", "RED", "IMPORTANT"}
        ]
        high_events = _dedupe_calendar_events(high_events)
        if high_events:
            _write_last_good_calendar_cache(target_date, high_events, source=te_result.source)
            return CalendarLoadResult(
                status=MACRO_FALLBACK,
                source=te_result.source,
                events=high_events,
                message=f"{result.message or 'EODHD provider unavailable'} Using Trading Economics backup calendar.",
                cache_path=te_result.cache_path,
                provider_error=result.provider_error or te_result.provider_error,
                macro_risk_status=MACRO_HIGH_FROM_BACKUP_CALENDAR,
                fallback_chain=fallback_chain,
                data_freshness="tradingeconomics_backup",
                last_good_cache_path=str(_last_good_high_impact_events_path()),
            )

    finnhub_result = _fetch_finnhub_calendar(target_date)
    fallback_chain.append(f"legacy:{finnhub_result.source}:{finnhub_result.status}")
    if finnhub_result.status in {MACRO_OK, MACRO_EMPTY}:
        high_events = [
            e for e in finnhub_result.events
            if _normalize_impact(e.get("impact"), str(e.get("title") or "")) in {"HIGH", "RED", "IMPORTANT"}
        ]
        high_events = _dedupe_calendar_events(high_events)
        if high_events:
            _write_last_good_calendar_cache(target_date, high_events, source=finnhub_result.source)
        return CalendarLoadResult(
            status=MACRO_OK if high_events else MACRO_EMPTY,
            source=finnhub_result.source,
            events=high_events,
            message=f"{result.message or 'EODHD provider unavailable'} Using Finnhub legacy calendar.",
            cache_path=finnhub_result.cache_path,
            provider_error=result.provider_error or finnhub_result.provider_error,
            macro_risk_status=MACRO_OK if high_events else MACRO_EMPTY,
            fallback_chain=fallback_chain,
            data_freshness="finnhub_legacy",
            last_good_cache_path=str(_last_good_high_impact_events_path()),
        )

    static_events = _load_static_calendar_events(target_date)
    fallback_chain.append(f"backup:static_fallback:{'OK' if static_events else 'EMPTY'}")
    if static_events:
        _write_last_good_calendar_cache(target_date, static_events, source="static_fallback")
        return CalendarLoadResult(
            status=MACRO_FALLBACK,
            source="static_fallback",
            events=static_events,
            message=f"{result.message or 'Primary provider unavailable'} Using static fallback.",
            cache_path=result.cache_path,
            provider_error=result.provider_error,
            macro_risk_status=MACRO_HIGH_FROM_BACKUP_CALENDAR,
            fallback_chain=fallback_chain,
            data_freshness="static_fallback",
            last_good_cache_path=str(_last_good_high_impact_events_path()),
        )

    last_good_events, last_good_status = _load_last_good_calendar_events(target_date)
    fallback_chain.append(f"backup:last_good_cache:{'OK' if last_good_events else (last_good_status or 'EMPTY')}")
    if last_good_events:
        return CalendarLoadResult(
            status=MACRO_LAST_GOOD_CACHE,
            source="last_good_cache",
            events=last_good_events,
            message=f"{result.message or 'Primary provider unavailable'} Using last-good macro cache.",
            cache_path=str(_last_good_high_impact_events_path()),
            provider_error=result.provider_error,
            macro_risk_status=MACRO_LAST_GOOD_CACHE,
            fallback_chain=fallback_chain,
            data_freshness="stale_or_cached",
            last_good_cache_path=str(_last_good_high_impact_events_path()),
        )

    return CalendarLoadResult(
        status=MACRO_UNKNOWN_CONSERVATIVE,
        source=result.source or "macro_calendar_cascade",
        events=[],
        message=result.message or "Macro calendar unavailable; no fallback data.",
        cache_path=result.cache_path,
        provider_error=result.provider_error,
        macro_risk_status=MACRO_UNKNOWN_CONSERVATIVE,
        fallback_chain=fallback_chain,
        data_freshness="unknown",
        last_good_cache_path=str(_last_good_high_impact_events_path()),
    )


def load_high_impact_events(target_date: date) -> list[dict[str, Any]]:
    return load_high_impact_calendar(target_date).events


def _signals_from_outcomes(outcomes: dict[str, Any]) -> list[dict[str, Any]]:
    value = outcomes.get("signals")
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    value = outcomes.get("records")
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    return []


def _first_timestamp(record: dict[str, Any]) -> str | None:
    for key in (
        "created_at_utc",
        "created_at",
        "ts_utc",
        "cycle_id",
        "updated_at_utc",
        "resolved_at_utc",
        "completed_at_utc",
    ):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _record_local_date(record: dict[str, Any], timezone_name: str) -> date | None:
    raw = _first_timestamp(record)
    if not raw:
        return None

    text = raw.replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(_tz(timezone_name)).date()


def _production_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for r in records:
        if bool(r.get("exclude_from_metrics")):
            continue
        if str(r.get("tracking_scope") or "").upper() == "SYNTHETIC_TEST":
            continue
        signal_id = str(r.get("signal_id") or "")
        if signal_id.startswith("TEST_"):
            continue
        result.append(r)
    return result


def _dedup_signal_outcome_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Deduplicate signal outcome records before report aggregation.

    signal_outcomes.json can contain repeated research/counterfactual rows for
    the same logical observation. Counting them raw inflates missed/expired
    statistics, especially RESEARCH_COUNTERFACTUAL buckets.

    Dedup key intentionally keeps tracking_scope and outcome_status:
    - TELEGRAM_ALERT and RESEARCH_COUNTERFACTUAL remain separate;
    - TP_HIT / SL_HIT / MISSED / EXPIRED states remain explicit;
    - repeated snapshots of the same signal state collapse to one observation.
    """
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for record in records:
        if not isinstance(record, dict):
            continue

        signal_id = str(record.get("signal_id") or "").strip()
        tracking_scope = str(record.get("tracking_scope") or "UNKNOWN").strip().upper()
        outcome_status = str(
            record.get("outcome_status") or record.get("status") or "UNKNOWN"
        ).strip().upper()

        if signal_id:
            key = (signal_id, tracking_scope, outcome_status)
        else:
            # Conservative fallback for legacy rows without signal_id.
            key = (
                str(record.get("created_at_utc") or record.get("closed_at_utc") or ""),
                tracking_scope,
                outcome_status,
            )

        if key in seen:
            continue

        seen.add(key)
        result.append(record)

    return result


def _safe_avg(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)



def _positioning_enrich_records_for_reporting(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Attach Positioning Intelligence metadata to reporting/statistics records.

    v0.1 safety:
    - in-memory reporting enrichment only;
    - does not write back to signal_outcomes.json;
    - does not allow signals;
    - does not block signals;
    - does not modify Battle Gate decisions.
    """
    if not records:
        return records

    try:
        from app.services.positioning.positioning_record_enricher import enrich_records_with_positioning

        return enrich_records_with_positioning(records, mutate=False)
    except Exception as exc:
        # Fail-open: reporting must not break if positioning context is unavailable.
        out: list[dict[str, Any]] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            copy = dict(record)
            copy.setdefault("positioning_primary_tag", "DATA_UNAVAILABLE")
            copy.setdefault("positioning_mode", "RESEARCH_ONLY")
            copy.setdefault("positioning_battle_gate_impact", "none")
            copy.setdefault("positioning_telegram_signal_impact", "none")
            copy.setdefault("positioning_can_allow_signal", False)
            copy.setdefault("positioning_can_block_signal", False)
            copy.setdefault("positioning_enrichment_error", f"{type(exc).__name__}: {exc}")
            out.append(copy)
        return out


def _metric_from_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    tp = sl = missed = expired = invalid = pending = 0
    result_values: list[float] = []
    rr_values: list[float] = []
    practical_rr_values: list[float] = []

    for r in records:
        status = str(r.get("outcome_status") or r.get("status") or "").upper()

        if status == FINAL_TP:
            tp += 1
        elif status == FINAL_SL:
            sl += 1
        elif status == MISSED:
            missed += 1
        elif status == EXPIRED:
            expired += 1
        elif status == INVALID:
            invalid += 1
        else:
            pending += 1

        try:
            result_values.append(float(r.get("result_R")))
        except (TypeError, ValueError):
            if status == FINAL_TP:
                try:
                    result_values.append(float(r.get("practical_rr") or r.get("risk_reward_ratio") or 0.0))
                except (TypeError, ValueError):
                    pass
            elif status == FINAL_SL:
                result_values.append(-1.0)
            elif status == MISSED:
                result_values.append(0.0)

        for key, bucket in (("risk_reward_ratio", rr_values), ("practical_rr", practical_rr_values)):
            try:
                bucket.append(float(r.get(key)))
            except (TypeError, ValueError):
                pass

    closed = tp + sl
    winrate = round(tp / closed, 4) if closed else None

    return {
        "total": len(records),
        "tp": tp,
        "sl": sl,
        "missed": missed,
        "expired": expired,
        "invalid": invalid,
        "pending": pending,
        "closed_tp_sl": closed,
        "winrate": winrate,
        "avg_result_R": _safe_avg(result_values),
        "avg_rr": _safe_avg(rr_values),
        "avg_practical_rr": _safe_avg(practical_rr_values),
    }


def _record_is_battle_alert(record: dict[str, Any]) -> bool:
    tracking_scope = str(record.get("tracking_scope") or "").upper()
    delivery_mode = str(record.get("telegram_delivery_mode") or "").upper()
    if tracking_scope == "TELEGRAM_ALERT":
        return True
    if delivery_mode == "BATTLE_ALERT":
        return True
    if bool(record.get("sent_to_telegram")) and delivery_mode not in {"RESEARCH_ALERT", "SUPPRESS"}:
        return True
    return False


def _record_is_research_counterfactual(record: dict[str, Any]) -> bool:
    tracking_scope = str(record.get("tracking_scope") or "").upper()
    delivery_mode = str(record.get("telegram_delivery_mode") or "").upper()
    v2_risk_mode = str(record.get("battle_gate_v2_risk_mode") or "").upper()
    if tracking_scope == "RESEARCH_COUNTERFACTUAL":
        return True
    if delivery_mode == "RESEARCH_ALERT":
        return True
    if v2_risk_mode == "RESEARCH_COUNTERFACTUAL":
        return True
    return False


def _yesterday_grouped_metrics(timezone_name: str, target_date: date) -> tuple[dict[str, Any], str]:
    outcomes = load_signal_outcomes()
    records = _dedup_signal_outcome_records(_production_records(_signals_from_outcomes(outcomes)))
    records = _positioning_enrich_records_for_reporting(records)

    yesterday = target_date - timedelta(days=1)
    dated = [r for r in records if _record_local_date(r, timezone_name) == yesterday]

    if dated:
        battle = [r for r in dated if _record_is_battle_alert(r)]
        research = [r for r in dated if _record_is_research_counterfactual(r)]
        other = [r for r in dated if r not in battle and r not in research]
        return {
            "battle": _metric_from_records(battle),
            "research": _metric_from_records(research),
            "other": _metric_from_records(other),
            "all": _metric_from_records(dated),
        }, "signal_outcomes_by_yesterday_date"

    summary = load_daily_summary()
    battle_metrics = summary.get("battle_metrics") if isinstance(summary, dict) else {}
    by_scope = battle_metrics.get("by_tracking_scope_all_records") if isinstance(battle_metrics, dict) else {}

    if isinstance(by_scope, dict) and by_scope:
        def _metric_from_summary_bucket(key: str) -> dict[str, Any]:
            b = by_scope.get(key) if isinstance(by_scope.get(key), dict) else {}
            return {
                "total": b.get("total_signals", 0),
                "tp": b.get("tp_hit", 0),
                "sl": b.get("sl_hit", 0),
                "missed": b.get("missed_before_entry", 0),
                "expired": b.get("expired", 0),
                "invalid": b.get("invalid", 0),
                "pending": b.get("pending_or_active", 0),
                "closed_tp_sl": b.get("closed_tp_sl", 0),
                "winrate": b.get("winrate_tp_sl"),
                "avg_result_R": b.get("avg_result_R"),
                "avg_rr": b.get("avg_rr"),
                "avg_practical_rr": b.get("avg_practical_rr"),
            }

        return {
            "battle": _metric_from_summary_bucket("TELEGRAM_ALERT"),
            "research": _metric_from_summary_bucket("RESEARCH_COUNTERFACTUAL"),
            "other": _metric_from_records([]),
            "all": _metric_from_records([]),
        }, "daily_summary_tracking_scope_fallback"

    return {
        "battle": _metric_from_records([]),
        "research": _metric_from_records([]),
        "other": _metric_from_records([]),
        "all": _metric_from_records([]),
    }, "no_stats_available"


def _yesterday_metric(timezone_name: str, target_date: date) -> tuple[dict[str, Any], str]:
    grouped, source = _yesterday_grouped_metrics(timezone_name, target_date)
    return grouped.get("all", _metric_from_records([])), source

def _overall_grouped_metrics() -> tuple[dict[str, Any], str]:
    """
    Build cumulative production metrics from signal_outcomes.json.

    This intentionally separates real Telegram/Battle alerts from research /
    counterfactual records. Mixing them makes winrate and expectancy unusable:
    research records are useful for diagnostics, but they must not dilute the
    production Battle/Telegram quality line.
    """
    outcomes = load_signal_outcomes()
    records = _dedup_signal_outcome_records(_production_records(_signals_from_outcomes(outcomes)))
    records = _positioning_enrich_records_for_reporting(records)

    if not records:
        return {
            "battle": _metric_from_records([]),
            "research": _metric_from_records([]),
            "other": _metric_from_records([]),
            "all": _metric_from_records([]),
            "tpo_otd": _metric_from_records([]),
            "tpo_otd_long": _metric_from_records([]),
            "tpo_otd_short": _metric_from_records([]),
        }, "signal_outcomes_cumulative_empty"

    battle = [r for r in records if _record_is_battle_alert(r)]
    research = [r for r in records if _record_is_research_counterfactual(r)]
    other = [r for r in records if r not in battle and r not in research]

    def _scenario_name(record: dict[str, Any]) -> str:
        return str(record.get("scenario") or record.get("scenario_type") or "").upper()

    tpo_otd = [r for r in records if _scenario_name(r).startswith("TPO_OPEN_TEST_DRIVE")]
    tpo_otd_long = [r for r in tpo_otd if _scenario_name(r).endswith("LONG")]
    tpo_otd_short = [r for r in tpo_otd if _scenario_name(r).endswith("SHORT")]

    return {
        "battle": _metric_from_records(battle),
        "research": _metric_from_records(research),
        "other": _metric_from_records(other),
        "all": _metric_from_records(records),
        "tpo_otd": _metric_from_records(tpo_otd),
        "tpo_otd_long": _metric_from_records(tpo_otd_long),
        "tpo_otd_short": _metric_from_records(tpo_otd_short),
    }, "signal_outcomes_cumulative"


def _metric_closed_sample(metric: dict[str, Any]) -> int:
    return int(metric.get("closed_tp_sl") or 0)


def _metric_total_sample(metric: dict[str, Any]) -> int:
    return int(metric.get("total") or 0)


def _format_cumulative_metric_line(label: str, metric: dict[str, Any]) -> str:
    total = _metric_total_sample(metric)
    closed = _metric_closed_sample(metric)
    tp = int(metric.get("tp") or 0)
    sl = int(metric.get("sl") or 0)
    missed = int(metric.get("missed") or 0)
    expired = int(metric.get("expired") or 0)
    pending = int(metric.get("pending") or 0)

    return (
        f"{label}: n={total} | TP/SL: {tp}/{sl} | "
        f"WR: {_fmt_pct(metric.get('winrate'))} | avgR: {_fmt_num(metric.get('avg_result_R'), 4)}R | "
        f"closed={closed}, missed/expired/pending={missed}/{expired}/{pending}"
    )


def _build_overall_stats_section() -> BriefingSection:
    grouped, source = _overall_grouped_metrics()
    section = BriefingSection("📈 Загальна статистика")

    battle = grouped.get("battle", {}) if isinstance(grouped, dict) else {}
    research = grouped.get("research", {}) if isinstance(grouped, dict) else {}
    all_metric = grouped.get("all", {}) if isinstance(grouped, dict) else {}
    tpo_otd = grouped.get("tpo_otd", {}) if isinstance(grouped, dict) else {}
    tpo_otd_short = grouped.get("tpo_otd_short", {}) if isinstance(grouped, dict) else {}
    tpo_otd_long = grouped.get("tpo_otd_long", {}) if isinstance(grouped, dict) else {}

    if _metric_total_sample(all_metric) <= 0:
        section.lines.append("Немає cumulative signal_outcomes для загальної статистики.")
        section.lines.append(f"Джерело: {source}")
        return section

    section.lines.append(_format_cumulative_metric_line("Battle / Telegram", battle))
    section.lines.append(_format_cumulative_metric_line("Research / counterfactual", research))

    if _metric_total_sample(tpo_otd) > 0:
        section.lines.append(_format_cumulative_metric_line("TPO OTD total", tpo_otd))

    if _metric_total_sample(tpo_otd_short) > 0:
        section.lines.append(_format_cumulative_metric_line("TPO OTD short", tpo_otd_short))

    if _metric_total_sample(tpo_otd_long) > 0:
        section.lines.append(_format_cumulative_metric_line("TPO OTD long", tpo_otd_long))

    if _metric_total_sample(research) and (research.get("winrate") or 0) < (battle.get("winrate") or 0):
        section.lines.append("Висновок: research/counterfactual не змішувати з бойовою статистикою.")
    else:
        section.lines.append("Висновок: оцінювати Battle і Research окремо; загальний winrate без розділення може брехати.")

    section.lines.append(f"Джерело: {source}")
    return section


def _normalize_report_type(report_type: str) -> str:
    return str(report_type or "morning").strip().lower()


def _briefing_minutes_from_hhmm(value: Any) -> int | None:
    text = str(value or "").strip()
    if "T" in text:
        text = text.split("T", 1)[1]
    text = text[:5]
    try:
        hh, mm = text.split(":", 1)
        return int(hh) * 60 + int(mm)
    except Exception:
        return None


def _ny_report_delay_minutes(generated_at_local: Any) -> int | None:
    actual = _briefing_minutes_from_hhmm(generated_at_local)
    scheduled = _briefing_minutes_from_hhmm(os.getenv("REPORT_TIME_NY_1H", "17:35"))
    if actual is None or scheduled is None:
        return None
    delta = actual - scheduled
    if delta < 0:
        return None
    return delta


def _section_header_for_type(report_type: str, generated_at_local: Any = None) -> str:
    r = _normalize_report_type(report_type)
    if r in {"morning", "morning_briefing", "morning_combined"}:
        return "🌅 Ранковий брифінг"
    if r in {"london_1h", "london"}:
        return "🇬🇧 Звіт London +1 година"
    if r in {"ny_1h", "ny", "new_york"}:
        delay_minutes = _ny_report_delay_minutes(generated_at_local)
        if delay_minutes is not None and delay_minutes > 60:
            return "🇺🇸 Звіт NY delayed / mid-session"
        return "🇺🇸 Звіт NY +1 година"
    if r in {"holiday_warning", "pre_market"}:
        return "🗓 Попередження про свята / ризики ринку"
    return "📡 Ринкова аналітична доповідь"


def _symbol_scope_for_report(report_type: str) -> tuple[str, ...]:
    r = _normalize_report_type(report_type)

    if r in {"morning", "morning_briefing", "morning_combined"}:
        return MORNING_SESSION_SYMBOLS

    if r in {"london", "london_1h"}:
        return LONDON_SESSION_SYMBOLS

    if r in {"ny", "ny_1h", "new_york"}:
        return NY_SESSION_SYMBOLS

    return GLOBAL_SYMBOL_ORDER


def _scope_label_for_report(report_type: str) -> str:
    return REPORT_SCOPE_LABELS_UK.get(
        _normalize_report_type(report_type),
        "Фокус конкретного звіту.",
    )


def _sort_symbols_by_scope(symbols: list[str], report_type: str) -> list[str]:
    scope = list(_symbol_scope_for_report(report_type))
    rank = {sym: idx for idx, sym in enumerate(scope)}
    return sorted(symbols, key=lambda x: (rank.get(x, 10_000), x))


def _filter_symbols_for_report(symbols: list[str], report_type: str) -> list[str]:
    scope = set(_symbol_scope_for_report(report_type))
    filtered = [str(s).strip().upper() for s in symbols if str(s).strip().upper() in scope]
    return _sort_symbols_by_scope(filtered, report_type)


def _sort_symbols_global(symbols: list[str]) -> list[str]:
    rank = {sym: idx for idx, sym in enumerate(GLOBAL_SYMBOL_ORDER)}
    clean: list[str] = []
    seen: set[str] = set()
    for sym in symbols or []:
        s = str(sym or "").strip().upper()
        if not s or s in seen:
            continue
        seen.add(s)
        clean.append(s)
    return sorted(clean, key=lambda x: (rank.get(x, 10_000), x))


def _outside_focus_label_for_report(report_type: str) -> str:
    r = _normalize_report_type(report_type)
    if r in {"ny", "ny_1h", "new_york"}:
        return "поза активним NY-фокусом"
    if r in {"morning", "morning_briefing", "morning_combined", "london", "london_1h"}:
        return "поза активним London/ранковим фокусом"
    return "поза активним фокусом звіту"


def _macro_affected_symbols_text(symbols: list[str], report_type: str) -> str:
    """
    Risk block must show the full macro-affected universe, not only the
    session-scoped active focus. Session scope is a reporting/priority filter,
    not a macro-causality filter. Example: EUR PMI in a NY report may actively
    affect XAUUSD in focus, while EURUSD/GER40 remain relevant but outside NY focus.
    """
    full = _sort_symbols_global(symbols or [])
    if not full:
        return ""

    scope = set(_symbol_scope_for_report(report_type))
    outside = [sym for sym in full if sym not in scope]

    text = f" | активи: {', '.join(full[:12])}"
    if outside:
        text += f" | {_outside_focus_label_for_report(report_type)}: {', '.join(outside[:8])}"
    return text


def _market_line(
    *,
    sym: str,
    market_status: Any,
    permission: Any,
    modifier: Any,
    reason: Any = None,
    holiday: Any = None,
    fallback: bool = False,
    provider_error: bool = False,
) -> str:
    parts = [
        f"{sym}: {_status_label(market_status)}",
        f"дозвіл={_permission_label(permission)}",
        f"пріоритет={_modifier_label(modifier)}",
    ]
    if reason:
        parts.append(f"причина={_raw(reason)}")
    if holiday:
        parts.append(f"свято={_raw(holiday)}")
    if fallback or provider_error:
        parts.append("provider fallback / резервний режим")
    return " / ".join(parts)


def _build_market_status_section(tpo: dict[str, Any], report_type: str) -> BriefingSection:
    symbols = tpo.get("symbols") if isinstance(tpo, dict) else {}
    symbols = symbols if isinstance(symbols, dict) else {}
    section = BriefingSection("🗓 Ринки / свята / стан даних")

    if not symbols:
        section.lines.append("Немає доступних символів у TPO store.")
        return section

    scope = set(_symbol_scope_for_report(report_type))
    section.lines.append(f"Фокус: {_scope_label_for_report(report_type)}")

    closed_holidays: list[str] = []
    closed_regular: list[str] = []
    stale_or_degraded: list[str] = []
    open_symbols: list[str] = []

    for sym in _sort_symbols_by_scope([str(x).upper() for x in symbols.keys()], report_type):
        if sym not in scope:
            continue
        item = symbols.get(sym)
        if not isinstance(item, dict):
            continue
        ctx = item.get("context") if isinstance(item.get("context"), dict) else {}
        filters = item.get("filters") if isinstance(item.get("filters"), dict) else {}
        market_status = str(ctx.get("market_status") or "-")
        reason = ctx.get("market_closed_reason")
        holiday = ctx.get("market_holiday_name")
        permission = filters.get("tpo_signal_permission")
        modifier = filters.get("telegram_modifier") or filters.get("tpo_telegram_modifier")
        fallback = bool(item.get("fallback_preserved_previous_context") or filters.get("fallback_preserved_previous_context"))
        provider_error = bool(ctx.get("provider_error") or filters.get("provider_error"))

        text = _market_line(
            sym=sym,
            market_status=market_status,
            permission=permission,
            modifier=modifier,
            reason=reason,
            holiday=holiday,
            fallback=fallback,
            provider_error=provider_error,
        )

        if reason == "US_HOLIDAY" or holiday:
            closed_holidays.append(text)
        elif market_status.startswith("MARKET_CLOSED"):
            closed_regular.append(text)
        elif market_status == "STALE_DATA" or fallback or provider_error:
            stale_or_degraded.append(text)
        elif market_status == "OPEN":
            open_symbols.append(text)

    if closed_holidays:
        section.lines.append("🗓 Закрито через свято:")
        section.lines.extend([f"• {x}" for x in closed_holidays[:10]])
    if closed_regular:
        section.lines.append("🔒 Сесія закрита:")
        section.lines.extend([f"• {x}" for x in closed_regular[:10]])
    if stale_or_degraded:
        section.lines.append("⚠️ Застарілі або деградовані дані:")
        section.lines.extend([f"• {x}" for x in stale_or_degraded[:10]])
    if open_symbols:
        section.lines.append("✅ Відкрито для оцінки:")
        section.lines.extend([f"• {x}" for x in open_symbols[:10]])
    if len(section.lines) == 1:
        section.lines.append("Критичних проблем зі станом ринку не виявлено.")

    return section



def _calendar_currency(event: dict[str, Any]) -> str:
    return str(event.get("currency") or "-").strip().upper()


def _calendar_symbols(event: dict[str, Any]) -> list[str]:
    value = event.get("symbols")
    if not isinstance(value, list):
        return []
    return [str(x).strip().upper() for x in value if str(x).strip()]


def _is_unmapped_high_impact_event(event: dict[str, Any]) -> bool:
    """
    Some providers return globally tagged HIGH events with no currency and no symbol mapping
    (for example currency='-' / symbols=[]). Keep them out of the main red-risk block
    unless explicitly enabled, because they are not actionable for our trading universe.
    """
    currency = _calendar_currency(event)
    symbols = _calendar_symbols(event)
    if symbols:
        return False
    return currency in UNKNOWN_CURRENCY_VALUES


def _show_unmapped_high_impact_events() -> bool:
    return str(os.getenv("ECONOMIC_CALENDAR_SHOW_UNMAPPED_HIGH_IMPACT", "false")).strip().lower() in {"1", "true", "yes", "on"}


def _filter_actionable_high_impact_events(events: list[dict[str, Any]], report_type: str) -> tuple[list[dict[str, Any]], int]:
    """Return events suitable for the Telegram risk block and number of hidden unmapped events."""
    actionable: list[dict[str, Any]] = []
    hidden_unmapped = 0
    show_unmapped = _show_unmapped_high_impact_events()

    for event in events:
        if _is_unmapped_high_impact_event(event) and not show_unmapped:
            hidden_unmapped += 1
            continue

        # If provider gave a recognized currency but no symbols, try a last-resort mapping here.
        # This keeps the risk block useful even when provider data is incomplete.
        currency = _calendar_currency(event)
        if not _calendar_symbols(event) and currency in RELEVANT_RISK_CURRENCIES:
            enriched = dict(event)
            enriched["symbols"] = _affected_symbols(currency, str(event.get("title") or event.get("event") or ""))
            event = enriched

        actionable.append(event)

    return actionable, hidden_unmapped

def _macro_calendar_unknown_conservative(calendar: CalendarLoadResult) -> bool:
    return calendar.status == MACRO_UNKNOWN_CONSERVATIVE or calendar.macro_risk_status == MACRO_UNKNOWN_CONSERVATIVE


def _us_market_holiday_overlay(target_date: date) -> dict[str, Any] | None:
    """Return static US holiday/session overlay for known market holidays.

    This is intentionally separate from the macro calendar. If providers are
    down on a known US holiday, the report should say "holiday liquidity risk"
    rather than generic "macro unknown".
    """
    raw_enabled = str(os.getenv("ENABLE_US_HOLIDAY_RISK_OVERLAY", "true")).strip().lower()
    if raw_enabled not in {"1", "true", "yes", "on"}:
        return None

    override = os.getenv("FORCE_US_HOLIDAY_OVERLAY")
    if override:
        override = override.strip().lower()
        if override in {"0", "false", "no", "off"}:
            return None
        if override in {"1", "true", "yes", "on"}:
            return {
                "code": "US_HOLIDAY_FORCED",
                "name": os.getenv("US_HOLIDAY_OVERLAY_NAME", "US market holiday / forced overlay"),
                "ny_equity_status": "MARKET_CLOSED",
                "liquidity_mode": "HOLIDAY_LIQUIDITY_CAUTION",
                "session_note": "Forced holiday overlay: London/morning assets are not globally blocked; NY cash/risk assets are limited.",
            }

    profile = US_MARKET_HOLIDAY_OVERLAY.get(target_date.isoformat())
    return dict(profile) if isinstance(profile, dict) else None


def _is_us_holiday_overlay_active(target_date: date | None) -> bool:
    return bool(target_date and _us_market_holiday_overlay(target_date))


def _calendar_provider_diagnostic_lines(calendar: CalendarLoadResult) -> list[str]:
    lines: list[str] = []
    if calendar.message:
        lines.append(f"Calendar status: {calendar.message}")
    if calendar.provider_error:
        lines.append(f"Provider error: {calendar.provider_error}")
    if calendar.fallback_chain:
        lines.append(f"Fallback chain: {' → '.join(calendar.fallback_chain)}")
    if calendar.last_good_cache_path:
        lines.append(f"Last-good cache: {calendar.last_good_cache_path}")
    return lines


def _build_high_impact_section(target_date: date, timezone_name: str, report_type: str) -> BriefingSection:
    calendar = load_high_impact_calendar(target_date)
    raw_events = calendar.events
    events, hidden_unmapped = _filter_actionable_high_impact_events(raw_events, report_type)
    holiday_overlay = _us_market_holiday_overlay(target_date)
    section_title = "🟠 Ризик дня" if holiday_overlay and not events else "🔴 Ризик дня"
    section = BriefingSection(section_title)

    if hidden_unmapped:
        section.lines.append(f"Provider повернув {hidden_unmapped} unmapped HIGH подій; вони сховані з Telegram як неactionable.")

    if not events:
        if calendar.status == MACRO_EMPTY:
            section.lines.append("Actionable macro-watch подій за підключеним календарем не знайдено.")
            section.lines.append("Macro mode: CLEAR_BY_PROVIDER.")
            section.lines.append(f"Джерело: {calendar.source}.")
        elif raw_events:
            section.lines.append("Actionable macro-watch подій для нашого торгового фокусу не знайдено.")
            section.lines.append(f"Macro mode: {calendar.macro_risk_status}.")
            section.lines.append(f"Джерело: {calendar.source}.")
        else:
            if holiday_overlay:
                section.lines.append(f"US_HOLIDAY_REGIME: {holiday_overlay.get('code')} — {holiday_overlay.get('name')}.")
                section.lines.append("High-impact macro: немає підтверджених strong NY news у підключеному календарі; provider unavailable не перетворюємо на global no-trade.")
                section.lines.append("Режим: HOLIDAY_LIQUIDITY_CAUTION — London/morning assets можна оцінювати до NY тільки при clean setup.")
                section.lines.append("Session scope: NAS100/SPX500 = MARKET_CLOSED / US holiday; UKOIL = caution через змінений holiday liquidity schedule.")
                section.lines.append("Battle policy: без FOMC/CPI/NFP/Powell не блокувати ранкові активи глобально; потрібні LTF model + stop + real target + RR.")
                section.lines.extend(_calendar_provider_diagnostic_lines(calendar))
            else:
                section.lines.append("MACRO_RISK_STATUS: UNKNOWN.")
                section.lines.append("Календар high-impact news не завантажений / provider unavailable.")
                section.lines.append("Це не означає, що high-impact news немає.")
                section.lines.append("Режим: MACRO_UNKNOWN_CONSERVATIVE — не піднімати research у battle без зовнішньої перевірки.")
                section.lines.extend(_calendar_provider_diagnostic_lines(calendar))
        return section

    if calendar.status == MACRO_FALLBACK:
        section.lines.append(f"MACRO_RISK_STATUS: {_macro_risk_status_text(calendar)}.")
        section.lines.append(f"⚠️ Основний календар недоступний; використовується {calendar.source}.")
        section.lines.append("Це може бути неповний список подій. Режим: conservative.")
    elif calendar.status == MACRO_LAST_GOOD_CACHE:
        section.lines.append("⚠️ Використовується last-good macro cache.")
        section.lines.append("Кеш може бути застарілим; перед NY перевірити зовнішній календар.")
    elif calendar.status == MACRO_UNKNOWN_CONSERVATIVE:
        section.lines.append("⚠️ Macro calendar unknown; conservative mode.")

    if calendar.fallback_chain and calendar.status != MACRO_OK:
        section.lines.append(f"Fallback chain: {' → '.join(calendar.fallback_chain)}")

    if _has_fomc_cluster(events):
        section.lines.append("FOMC_DAY_LOCK: не піднімати Battle у день FOMC без macro-clearance.")
        if _has_fomc_press_conference(events):
            section.lines.append("FOMC_PRESSER_LOCK: NO BATTLE до завершення пресконференції; після — тільки 15m acceptance + retest + LTF confirmation + real target.")

    for e in _select_events_for_high_impact_section(events):
        local_time = _local_dt_from_event(e, timezone_name)
        currency = e.get("currency") or "-"
        impact = _calendar_event_impact_display(e)
        title = e.get("title") or e.get("event") or "Unnamed event"
        symbols = _sort_symbols_global(e.get("symbols") or [])
        source = e.get("source") or calendar.source

        provider_note = _translate_note(e.get("note"))
        operational_note = _time_aware_event_note(e, timezone_name)
        note = operational_note or provider_note

        symbol_text = _macro_affected_symbols_text(symbols, report_type)

        section.lines.append(f"• {local_time} — {currency} {impact}: {title}{symbol_text}")
        if note:
            section.lines.append(f"  {note}")
        if provider_note and provider_note != note:
            section.lines.append(f"  {provider_note}")
        source_impact_note = _calendar_event_source_impact_note(e)
        if source_impact_note:
            section.lines.append(f"  {source_impact_note}")
        if source:
            section.lines.append(f"  Джерело: {source}")

    return section


def _build_yesterday_section(target_date: date, timezone_name: str) -> BriefingSection:
    grouped, source = _yesterday_grouped_metrics(timezone_name, target_date)
    yday = (target_date - timedelta(days=1)).isoformat()
    section = BriefingSection(f"📊 Вчора — {yday}")

    battle = grouped.get("battle", {}) if isinstance(grouped, dict) else {}
    research = grouped.get("research", {}) if isinstance(grouped, dict) else {}
    all_metric = grouped.get("all", {}) if isinstance(grouped, dict) else {}

    battle_total = int(battle.get("total") or 0)
    research_total = int(research.get("total") or 0)

    if battle_total:
        section.lines.append(
            f"Battle alerts: {battle_total} | TP/SL: {battle.get('tp', 0)}/{battle.get('sl', 0)} | WR: {_fmt_pct(battle.get('winrate'))} | avgR: {_fmt_num(battle.get('avg_result_R'), 4)}R"
        )
    else:
        section.lines.append("Battle alerts: 0 | бойовий winrate не рахуємо")

    if research_total:
        section.lines.append(
            f"Research/counterfactual: {research_total} | TP/SL: {research.get('tp', 0)}/{research.get('sl', 0)} | WR: {_fmt_pct(research.get('winrate'))} | avgR: {_fmt_num(research.get('avg_result_R'), 4)}R"
        )
    else:
        section.lines.append("Research/counterfactual: 0")

    all_total = int(all_metric.get("total") or 0)
    if all_total and not battle_total and research_total:
        section.lines.append("Висновок: вчора працювали research-моделі; не трактувати це як бойовий winrate.")
    elif research_total and (research.get("tp", 0) or 0) == 0 and (research.get("sl", 0) or 0) > 0:
        section.lines.append("Висновок: research-моделі токсичні; не піднімати без v2 gate + LTF confirmation.")
    elif battle_total:
        section.lines.append("Висновок: оцінювати окремо battle-якість, не змішувати з research.")

    section.lines.append(f"Джерело: {source}")
    return section


def _build_statistics_section() -> BriefingSection:
    summary = load_daily_summary()
    section = BriefingSection("🛡 Бойова / production-статистика")
    if not isinstance(summary, dict) or not summary:
        section.lines.append("Немає daily_summary.json.")
        return section

    s = summary.get("summary") if isinstance(summary.get("summary"), dict) else {}
    battle_metrics = summary.get("battle_metrics") if isinstance(summary.get("battle_metrics"), dict) else {}

    section.lines.extend(
        [
            f"Exporter: {summary.get('exporter_version', '-')}",
            f"Production-записи: {s.get('production_records', s.get('total_signals', '-'))}",
            f"Синтетичні тести виключено: {s.get('synthetic_test_records', s.get('excluded_from_metrics', '-'))}",
            f"TP / SL / пропущено до входу: {s.get('tp_hit', '-')} / {s.get('sl_hit', '-')} / {s.get('missed_before_entry', '-')}",
            f"Winrate по TP/SL: {_fmt_pct(s.get('winrate_tp_sl'))}",
            f"Середній результат: {_fmt_num(s.get('avg_result_R'), 4)}R",
            f"Середній RR / практичний RR: {_fmt_num(s.get('avg_rr'), 2)} / {_fmt_num(s.get('avg_practical_rr'), 2)}",
        ]
    )

    by_permission = battle_metrics.get("by_battle_permission")
    if isinstance(by_permission, dict) and by_permission:
        section.lines.append("Групи Battle Permission:")
        for key, value in list(by_permission.items())[:8]:
            if not isinstance(value, dict):
                continue
            section.lines.append(
                f"• {key}: сигналів={value.get('total_signals', '-')} WR={_fmt_pct(value.get('winrate_tp_sl'))} avgR={_fmt_num(value.get('avg_result_R'), 4)}"
            )

    by_scope_all = battle_metrics.get("by_tracking_scope_all_records")
    if isinstance(by_scope_all, dict) and by_scope_all:
        section.lines.append("Усі записи за tracking scope:")
        for key, value in list(by_scope_all.items())[:8]:
            if not isinstance(value, dict):
                continue
            outcomes = _translate_outcomes_dict(value.get("by_outcome_status"))
            section.lines.append(f"• {key}: сигналів={value.get('total_signals', '-')}, результати={outcomes}")

    return section



def _compact_zone_text(zone: Any) -> str:
    if not isinstance(zone, dict) or not zone:
        return "-"
    zone_type = _zone_type_label(zone.get("zone_type"))
    if zone_type == "-":
        return "-"
    if str(zone.get("zone_type") or "").upper() == "NPOC":
        return f"{zone_type} interest zone, не entry"
    return zone_type



def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _boolish(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "open"}:
        return True
    if text in {"0", "false", "no", "off", "closed"}:
        return False
    return default


def _nested_dicts(item: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """
    Return all known nested context containers used by the project.

    The short Telegram market-state block must not accidentally read stale top-level
    fallback/control values while fresh TPO context says the market is OPEN.
    """
    metadata = _as_dict(item.get("metadata"))
    payload = _as_dict(item.get("payload"))
    payload_payload = _as_dict(payload.get("payload"))

    context = _as_dict(item.get("context"))
    filters = _as_dict(item.get("filters"))
    open_behavior = _as_dict(item.get("open_behavior"))

    auction_context = _as_dict(item.get("auction_context"))
    auction_filters = _as_dict(item.get("auction_filters"))

    metadata_context = _as_dict(metadata.get("context"))
    metadata_filters = _as_dict(metadata.get("filters"))
    metadata_auction_context = _as_dict(metadata.get("auction_context"))
    metadata_auction_filters = _as_dict(metadata.get("auction_filters"))

    payload_context = _as_dict(payload.get("context"))
    payload_filters = _as_dict(payload.get("filters"))
    payload_auction_context = _as_dict(payload.get("auction_context"))
    payload_auction_filters = _as_dict(payload.get("auction_filters"))

    nested_payload_context = _as_dict(payload_payload.get("context"))
    nested_payload_filters = _as_dict(payload_payload.get("filters"))
    nested_payload_auction_context = _as_dict(payload_payload.get("auction_context"))
    nested_payload_auction_filters = _as_dict(payload_payload.get("auction_filters"))

    return {
        "context": context,
        "filters": filters,
        "open_behavior": open_behavior,
        "auction_context": auction_context,
        "auction_filters": auction_filters,
        "metadata": metadata,
        "metadata_context": metadata_context,
        "metadata_filters": metadata_filters,
        "metadata_auction_context": metadata_auction_context,
        "metadata_auction_filters": metadata_auction_filters,
        "payload": payload,
        "payload_context": payload_context,
        "payload_filters": payload_filters,
        "payload_auction_context": payload_auction_context,
        "payload_auction_filters": payload_auction_filters,
        "payload_payload": payload_payload,
        "nested_payload_context": nested_payload_context,
        "nested_payload_filters": nested_payload_filters,
        "nested_payload_auction_context": nested_payload_auction_context,
        "nested_payload_auction_filters": nested_payload_auction_filters,
    }


def _first_from_sources(keys: tuple[str, ...], sources: list[dict[str, Any]], default: Any = None) -> Any:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in keys:
            value = source.get(key)
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return value
    return default


def _first_zone_from_sources(sources: list[dict[str, Any]]) -> dict[str, Any]:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in ("primary_interest_zone", "interest_zone", "zone"):
            value = source.get(key)
            if isinstance(value, dict) and value:
                return value
    return {}


def _context_says_open(data: dict[str, Any]) -> bool:
    return bool(
        data.get("market_is_open")
        or data.get("context_market_status") == "OPEN"
        or data.get("auction_market_status") == "OPEN"
        or data.get("market_status") == "OPEN"
    )


def _context_says_clean_data(data: dict[str, Any]) -> bool:
    return bool(
        not data.get("provider_error")
        and not data.get("market_data_is_stale")
        and not data.get("fallback")
    )


def _brief_symbol_context(item: dict[str, Any]) -> dict[str, Any]:
    d = _nested_dicts(item)

    # Fresh TPO/auction context has priority over stale top-level signal/fallback state.
    context_sources = [
        d["context"],
        d["auction_context"],
        d["metadata_auction_context"],
        d["payload_auction_context"],
        d["nested_payload_auction_context"],
        d["metadata_context"],
        d["payload_context"],
        d["nested_payload_context"],
        item,
        d["payload"],
        d["payload_payload"],
    ]
    filter_sources = [
        d["filters"],
        d["auction_filters"],
        d["metadata_auction_filters"],
        d["payload_auction_filters"],
        d["nested_payload_auction_filters"],
        d["metadata_filters"],
        d["payload_filters"],
        d["nested_payload_filters"],
        item,
        d["payload"],
        d["payload_payload"],
    ]
    behavior_sources = [
        d["open_behavior"],
        d["context"],
        d["auction_context"],
        d["metadata_auction_context"],
        d["payload_auction_context"],
        d["nested_payload_auction_context"],
        item,
        d["payload"],
        d["payload_payload"],
    ]

    context_market_status = _upper(_first_from_sources(("market_status",), context_sources), "UNKNOWN")
    auction_market_status = _upper(
        _first_from_sources(("market_status",), [d["auction_context"], d["metadata_auction_context"], d["payload_auction_context"], d["nested_payload_auction_context"]]),
        "UNKNOWN",
    )

    raw_permission = _first_from_sources(("tpo_signal_permission", "permission"), filter_sources)
    raw_market_status = _first_from_sources(("market_status",), context_sources)

    # Keep market_is_open and primary_session_active separate.
    # At night a market can be legitimately closed even if old first-hour context exists.
    # During the cash session, however, provider/yfinance can mark indices as
    # MARKET_CLOSED_AND_STALE after data lag; that should render as stale data,
    # not as a real closed market.
    market_is_open = _boolish(
        _first_from_sources(("market_is_open",), context_sources),
        default=False,
    )
    primary_session_active = _boolish(
        _first_from_sources(("is_primary_session_active",), context_sources),
        default=False,
    )
    market_data_is_stale = _boolish(
        _first_from_sources(("market_data_is_stale", "is_stale"), context_sources),
        default=False,
    )
    provider_error = _boolish(
        _first_from_sources(("provider_error",), context_sources + filter_sources),
        default=False,
    )

    fallback = bool(
        _first_from_sources(
            ("fallback_preserved_previous_context", "fallback"),
            [item, d["filters"], d["auction_filters"], d["context"], d["auction_context"], d["metadata"], d["payload"], d["payload_payload"]],
            default=False,
        )
    )

    market_status = _upper(raw_market_status, "UNKNOWN")
    permission = _upper(raw_permission, "UNKNOWN")

    # Guard against false MARKET_CLOSED in short briefing:
    # if fresh auction context says the market is open, do not let stale permission/top-level state
    # print MARKET_CLOSED in the NY +1h report.
    fresh_context_open = (
        market_is_open
        or context_market_status == "OPEN"
        or auction_market_status == "OPEN"
        or (
            market_status == "OPEN"
            and not market_data_is_stale
            and not provider_error
        )
    )

    if fresh_context_open:
        market_status = "OPEN"
        if permission == "MARKET_CLOSED":
            permission = "OPEN_FOR_EVALUATION"

    modifier = _upper(
        _first_from_sources(("telegram_modifier", "tpo_telegram_modifier", "modifier"), filter_sources + context_sources),
        "NEUTRAL",
    )
    open_context = _upper(
        _first_from_sources(("open_context", "open_relation"), behavior_sources + context_sources + filter_sources),
        "UNKNOWN",
    )
    raw_open_behavior = _upper(
        _first_from_sources(("open_behavior",), behavior_sources + context_sources + filter_sources),
        "UNKNOWN",
    )
    current_open_behavior = _upper(
        _first_from_sources(("current_open_behavior", "updated_open_behavior"), behavior_sources + context_sources + filter_sources),
        "",
    )
    initial_open_behavior = _upper(
        _first_from_sources(("initial_open_behavior",), behavior_sources + context_sources + filter_sources),
        "",
    )
    true_otd_allowed = _first_from_sources(("true_otd_allowed",), behavior_sources + context_sources + filter_sources)
    synthetic_open = _first_from_sources(("synthetic_open",), behavior_sources + context_sources + filter_sources)
    synthetic_open_confirmed = _first_from_sources(("synthetic_open_confirmed",), behavior_sources + context_sources + filter_sources)
    profile_reliability_state = _upper(
        _first_from_sources(("profile_reliability_state",), behavior_sources + context_sources + filter_sources),
        "",
    )
    open_behavior = _safe_briefing_open_behavior(
        raw_open_behavior,
        current_open_behavior=current_open_behavior,
        initial_open_behavior=initial_open_behavior,
        true_otd_allowed=true_otd_allowed,
    )
    entry_hint = _upper(
        _first_from_sources(("entry_model_hint",), behavior_sources + context_sources + filter_sources),
        "NO_ENTRY_MODEL",
    )
    battle_hint = _upper(
        _first_from_sources(("battle_bias_hint",), behavior_sources + context_sources + filter_sources),
        "RESEARCH_ONLY",
    )
    primary_zone = _first_zone_from_sources(behavior_sources + context_sources + filter_sources)

    warnings = _first_from_sources(("warnings",), behavior_sources + context_sources + filter_sources, default=[])
    if not isinstance(warnings, list):
        warnings = []

    return {
        "market_status": market_status,
        "context_market_status": context_market_status,
        "auction_market_status": auction_market_status,
        "market_is_open": market_is_open,
        "primary_session_active": primary_session_active,
        "market_data_is_stale": market_data_is_stale,
        "permission": permission,
        "modifier": modifier,
        "open_context": open_context,
        "open_behavior": open_behavior,
        "raw_open_behavior": raw_open_behavior,
        "initial_open_behavior": initial_open_behavior,
        "current_open_behavior": current_open_behavior,
        "true_otd_allowed": true_otd_allowed,
        "synthetic_open": synthetic_open,
        "synthetic_open_confirmed": synthetic_open_confirmed,
        "profile_reliability_state": profile_reliability_state,
        "entry_hint": entry_hint,
        "battle_hint": battle_hint,
        "primary_zone": primary_zone,
        "warnings": warnings,
        "provider_error": provider_error,
        "fallback": bool(fallback),
        "first_hour_activity": _first_from_sources(("first_hour_activity",), behavior_sources + context_sources + filter_sources, default={}),
        "entry_timing_status": _upper(_first_from_sources(("entry_timing_status",), behavior_sources + context_sources + filter_sources), ""),
        "already_moved_R": _first_from_sources(("already_moved_R", "already_moved_r"), behavior_sources + context_sources + filter_sources),
        "trigger_reason": _first_from_sources(("trigger_reason",), behavior_sources + context_sources + filter_sources),
        "tpo_signal_reason": _first_from_sources(("tpo_signal_reason", "signal_reason", "reason"), filter_sources + context_sources + behavior_sources),
        "status_source_guard": "fresh_context_open_overrides_closed" if fresh_context_open else "normal",
    }




def _briefing_falseish(value: Any) -> bool:
    if isinstance(value, bool):
        return value is False
    if value is None:
        return False
    return str(value).strip().lower() in {"false", "0", "no", "n", "off"}


def _briefing_current_behavior_to_display(value: Any) -> str | None:
    current = _upper(value, "")
    if not current:
        return None
    if current.startswith("OPEN_AUCTION"):
        return "OPEN_AUCTION"
    if current == "OPEN_TEST_DRIVE_CONFIRMED":
        return "OPEN_TEST_DRIVE"
    if current == "OPEN_TEST_DRIVE_CANDIDATE":
        return "OPEN_TEST_DRIVE_CANDIDATE"
    if current.startswith("OPEN_REJECTION_REVERSE"):
        return "OPEN_REJECTION_REVERSE"
    if current.startswith("OPEN_DRIVE"):
        return "OPEN_DRIVE"
    return current


def _safe_briefing_open_behavior(
    raw_open_behavior: Any,
    current_open_behavior: Any = None,
    initial_open_behavior: Any = None,
    true_otd_allowed: Any = None,
) -> str:
    """
    Resolve a human/reporting-safe open_behavior.

    Broad legacy open_behavior can remain OPEN_TEST_DRIVE for compatibility even
    when the current auction state is only a candidate or was downgraded by
    session normalization. Briefings should not print confident OTD in that case.
    """
    raw = _upper(raw_open_behavior, "")
    current_display = _briefing_current_behavior_to_display(current_open_behavior)
    initial_display = _briefing_current_behavior_to_display(initial_open_behavior)

    if raw == "OPEN_TEST_DRIVE":
        if _briefing_falseish(true_otd_allowed):
            return current_display or "OPEN_AUCTION"
        if current_display in {"OPEN_AUCTION", "OPEN_TEST_DRIVE_CANDIDATE"}:
            return current_display
        if current_display:
            return current_display

    if raw in {"", "UNKNOWN", "UNCONFIRMED"}:
        return current_display or initial_display or raw or "UNKNOWN"

    return raw or current_display or initial_display or "UNKNOWN"


def _calendar_status_for_context(target_date: date | None, timezone_name: str | None, report_type: str) -> CalendarLoadResult | None:
    if target_date is None or timezone_name is None:
        return None
    if _normalize_report_type(report_type) not in {"ny", "ny_1h", "new_york"}:
        return None
    return load_high_impact_calendar(target_date)


def _macro_unknown_conservative_for_report(target_date: date | None, timezone_name: str | None, report_type: str) -> bool:
    # Known US holiday/session regime is not the same thing as actionable macro unknown.
    # If providers are unavailable on a known US holiday, downstream report sections
    # must not render every focus symbol as MACRO_UNKNOWN / NO TRADE.
    if target_date is not None and _us_market_holiday_overlay(target_date):
        return False
    calendar = _calendar_status_for_context(target_date, timezone_name, report_type)
    return bool(calendar and _macro_calendar_unknown_conservative(calendar))


def _first_hour_activity_from_item(item: dict[str, Any]) -> dict[str, Any]:
    d = _nested_dicts(item)
    context_sources = [
        d["context"],
        d["auction_context"],
        d["metadata_auction_context"],
        d["payload_auction_context"],
        d["nested_payload_auction_context"],
        d["metadata_context"],
        d["payload_context"],
        d["nested_payload_context"],
        item,
        d["payload"],
        d["payload_payload"],
    ]
    activity = _first_from_sources(("first_hour_activity",), context_sources, default={})
    return activity if isinstance(activity, dict) else {}


def _activity_direction_label(activity: dict[str, Any]) -> str:
    direction = _post_news_direction_from_activity(activity)
    if direction == "DOWN":
        return "bearish"
    if direction == "UP":
        return "bullish"
    return "neutral"


def _directional_continuation_state(sym: str, data: dict[str, Any]) -> str | None:
    """Detect active directional continuation hidden behind OPEN_AUCTION/DOWNGRADE wording.

    This is reporting wording only. It does not grant Battle permission. It prevents
    strong breakdown/continuation days from being described as plain BALANCE_CHOP.
    """
    if data.get("provider_error") or data.get("fallback"):
        return None
    market_status = str(data.get("market_status") or "").upper()
    if market_status in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"}:
        return None

    activity = data.get("first_hour_activity") if isinstance(data.get("first_hour_activity"), dict) else {}
    direction = _post_news_direction_from_activity(activity)
    entry_hint = str(data.get("entry_hint") or "").upper()
    reason = str(data.get("trigger_reason") or data.get("tpo_signal_reason") or "").upper()
    open_behavior = str(data.get("open_behavior") or "").upper()
    modifier = str(data.get("modifier") or "").upper()

    directional_hint = any(token in entry_hint or token in reason for token in (
        "RETEST", "PULLBACK", "SWEEP", "RECLAIM", "BOS", "FAILED_ACCEPTANCE",
        "VALUE_REJECTION", "OPEN_TEST_DRIVE", "CONTINUATION",
    ))
    failed_or_accepted = any(_boolish(activity.get(k)) for k in (
        "failed_auction", "accepted_outside_range", "accepted_back_inside_value", "accepted_back_inside_range"
    ))
    downgraded_auction = open_behavior in {"OPEN_AUCTION", "UNCONFIRMED", "UNKNOWN"} and modifier == "DOWNGRADE"

    if direction == "DOWN" and (directional_hint or failed_or_accepted or downgraded_auction):
        return "BEARISH_CONTINUATION_WAIT_RETEST"
    if direction == "UP" and (directional_hint or failed_or_accepted or downgraded_auction):
        return "BULLISH_CONTINUATION_WAIT_RETEST"
    return None


def _auction_subtype(sym: str, data: dict[str, Any], *, post_news_active: bool = False, macro_unknown: bool = False) -> str:
    market_status = str(data.get("market_status") or "").upper()
    permission = str(data.get("permission") or "").upper()
    open_behavior = str(data.get("open_behavior") or "").upper()
    modifier = str(data.get("modifier") or "").upper()
    warnings = [str(x).upper() for x in data.get("warnings") or []]
    activity = data.get("first_hour_activity") if isinstance(data.get("first_hour_activity"), dict) else {}
    entry_timing_status = str(data.get("entry_timing_status") or "").upper()
    trigger_reason = str(data.get("trigger_reason") or data.get("tpo_signal_reason") or "").upper()

    if market_status in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"} or permission in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"} or data.get("provider_error") or data.get("fallback"):
        return "PROVIDER_STALE"
    if macro_unknown:
        return "MACRO_UNKNOWN"
    directional_state = _directional_continuation_state(sym, data)
    if directional_state:
        return directional_state
    if entry_timing_status in {"LATE_SIGNAL", "HARD_LATE_SIGNAL"}:
        return "FIRST_IMPULSE_GONE"
    if any("FIRST_IMPULSE" in x or "IMPULSE_ALREADY_GONE" in x for x in warnings) or "FIRST_IMPULSE" in trigger_reason:
        return "FIRST_IMPULSE_GONE"
    if open_behavior == "OPEN_TEST_DRIVE":
        if _boolish(activity.get("failed_auction")):
            return "OTD_FAILED_ACCEPTANCE_RETEST"
        if _boolish(activity.get("accepted_back_inside_value")) or _boolish(activity.get("accepted_back_inside_range")):
            return "OTD_RETEST_PENDING"

    if _boolish(activity.get("failed_auction")):
        return "FAILED_ACCEPTANCE"
    if _boolish(activity.get("accepted_back_inside_value")) or _boolish(activity.get("accepted_back_inside_range")):
        return "REJECTION_ROTATION"
    if post_news_active:
        return "FIRST_IMPULSE_GONE"
    if open_behavior == "OPEN_REJECTION_REVERSE":
        return "REJECTION_ROTATION"
    if open_behavior == "OPEN_AUCTION" and modifier == "DOWNGRADE":
        return "BALANCE_CHOP"
    if open_behavior == "OPEN_AUCTION":
        return "BALANCE_CHOP"
    return open_behavior or "UNCONFIRMED"


def _bias_without_trade(sym: str, data: dict[str, Any], *, post_news_active: bool = False, macro_unknown: bool = False) -> str:
    if data.get("provider_error") or data.get("fallback") or str(data.get("market_status") or "").upper() in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"}:
        return "provider stale / no trade"
    if macro_unknown:
        return "macro unknown / conservative mode"

    activity = data.get("first_hour_activity") if isinstance(data.get("first_hour_activity"), dict) else {}
    direction = _activity_direction_label(activity)
    open_behavior = str(data.get("open_behavior") or "").upper()

    directional_state = _directional_continuation_state(sym, data)
    if directional_state == "BEARISH_CONTINUATION_WAIT_RETEST":
        return "bearish continuation active / wait fresh retest"
    if directional_state == "BULLISH_CONTINUATION_WAIT_RETEST":
        return "bullish continuation active / wait fresh pullback"
    if direction == "bearish":
        return "bearish bias / wait retest"
    if direction == "bullish":
        return "bullish bias / wait pullback"
    if open_behavior == "OPEN_TEST_DRIVE":
        return "directional watch / LTF pending"
    if open_behavior == "OPEN_REJECTION_REVERSE":
        return "failed-move watch / reclaim-BOS-retest only"
    if open_behavior == "OPEN_AUCTION":
        return "neutral chop / rotations only"
    if post_news_active:
        return "post-news volatility / wait acceptance"
    return "neutral / wait clarity"

def _post_news_watch_qualifier(sym: str, behavior: str) -> str:
    base = "WATCH_AFTER_RETEST_ONLY: перший імпульс уже міг відпрацювати; чекати ретест + LTF model + stop + real target"
    if sym in {"NAS100", "SPX500"}:
        return "POST_NEWS_INDEX: first impulse already gone; не наздоганяти, чекати acceptance/retest"
    if sym in {"BTCUSD", "ETHUSD"}:
        return "POST_NEWS_CRYPTO: risk-impulse уже доставлений; новий battle тільки після ретесту + real target"
    if sym in {"XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD"}:
        return "POST_NEWS_USD: не наздоганяти USD-імпульс; чекати acceptance/retest"
    if sym == "UKOIL":
        return "POST_NEWS_RISK: не наздоганяти імпульс; чекати retest/acceptance"
    return base


def _brief_verdict(
    sym: str,
    data: dict[str, Any],
    *,
    post_news_active: bool = False,
    macro_unknown: bool = False,
    us_holiday_overlay: bool = False,
) -> tuple[str, str]:
    market_status = data["market_status"]
    permission = data["permission"]
    modifier = data["modifier"]
    open_behavior = data["open_behavior"]
    entry_hint = data["entry_hint"]
    battle_hint = data["battle_hint"]
    zone = _compact_zone_text(data.get("primary_zone"))

    context_open = _context_says_open(data)
    clean_data = _context_says_clean_data(data)
    primary_session_active = bool(data.get("primary_session_active"))
    market_data_is_stale = bool(data.get("market_data_is_stale"))
    provider_error = bool(data.get("provider_error"))
    subtype = _auction_subtype(sym, data, post_news_active=post_news_active, macro_unknown=macro_unknown)
    bias = _bias_without_trade(sym, data, post_news_active=post_news_active, macro_unknown=macro_unknown)

    activity = data.get("first_hour_activity") if isinstance(data.get("first_hour_activity"), dict) else {}
    open_context = str(data.get("open_context") or "").upper()
    has_operational_context = bool(
        activity
        or open_behavior not in {"", "-", "UNKNOWN"}
        or open_context not in {"", "-", "UNKNOWN"}
    )

    zone_text = f" | зона: {zone}" if zone != "-" else ""
    bias_text = f" | bias: {bias}"
    subtype_text = f"{subtype}"

    # US holiday overlay must win over stale/false-session wording for NY cash assets.
    # On Juneteenth/US market holidays NAS100/SPX500 are closed by session calendar,
    # not merely "stale while session active". UKOIL can have limited/changed holiday
    # liquidity, so render it as holiday caution instead of a normal NY-stale condition.
    if us_holiday_overlay:
        if sym in {"NAS100", "SPX500"}:
            detail = "US_HOLIDAY | MARKET_CLOSED | NY cash session closed; no Battle"
            if zone != "-":
                detail += f" | зона: {zone}"
            detail += f"{bias_text}"
            return "NO_TRADE", f"• {sym} — {detail}"
        if sym == "UKOIL" and (
            market_status.startswith("MARKET_CLOSED")
            or permission == "MARKET_CLOSED"
            or market_data_is_stale
            or permission in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"}
        ):
            detail = "HOLIDAY_LIQUIDITY_CAUTION | changed/limited NY holiday schedule"
            if subtype_text and subtype_text not in {"UNKNOWN", "-"}:
                detail += f" | {subtype_text}"
            if zone != "-":
                detail += f" | зона: {zone}"
            detail += f" | no Battle; only fresh data + retest/acceptance + real target{bias_text}"
            return "NO_TRADE", f"• {sym} — {detail}"

    # NO TRADE is reserved only for real blockers: stale/no data/provider error/fallback.
    if data.get("fallback") or market_status in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"} or permission in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"}:
        return "NO_TRADE", f"• {sym} — {subtype_text} | {market_status}{zone_text}{bias_text}"

    # In NY reports, macro-unknown is the blocker. Do not let stale top-level
    # MARKET_CLOSED labels hide valid post-open/auction context for indices/oil.
    if macro_unknown and has_operational_context:
        detail = f"{subtype_text}{zone_text}{bias_text} | no Battle without external calendar check"
        return "NO_TRADE", f"• {sym} — {detail}"

    # Session-aware false-closed guard.
    # Important: do not override a real night/weekend close. We only rewrite the
    # short market-state text when the TPO store says the primary session was active
    # and there is usable first-hour/open-behavior context. If data is stale, this
    # remains NO_TRADE, but it is rendered as STALE_NY_DATA instead of MARKET_CLOSED.
    if market_status.startswith("MARKET_CLOSED") or permission == "MARKET_CLOSED":
        if (
            market_data_is_stale
            and primary_session_active
            and has_operational_context
            and not provider_error
        ):
            detail = f"STALE_NY_DATA | session active, provider data stale"
            if subtype_text and subtype_text not in {"UNKNOWN", "-"}:
                detail += f" | {subtype_text}"
            if zone != "-":
                detail += f" | зона: {zone}"
            detail += f" | no Battle; wait fresh data + retest/acceptance{bias_text}"
            return "NO_TRADE", f"• {sym} — {detail}"

        if clean_data and (context_open or ((post_news_active or macro_unknown) and has_operational_context)):
            guarded_behavior = open_behavior if open_behavior not in {"UNKNOWN", "-"} else "UNCONFIRMED"
            return "OBSERVE", f"• {sym} — {guarded_behavior} | stale MARKET_CLOSED ignored; wait retest/acceptance{bias_text}"
        return "NO_TRADE", f"• {sym} — MARKET_CLOSED"

    if macro_unknown and open_behavior in {"OPEN_AUCTION", "UNCONFIRMED", "UNKNOWN"}:
        detail = f"{subtype_text}{zone_text}{bias_text} | no Battle without external calendar check"
        return "NO_TRADE", f"• {sym} — {detail}"

    if modifier == "DOWNGRADE" or permission in {"BLOCKED_BY_CONTEXT", "BLOCKED_BY_AUCTION"} or battle_hint in {"DOWNGRADE_NO_DIRECTIONAL_BATTLE", "BLOCK"}:
        reason = open_behavior if open_behavior not in {"UNKNOWN", "-"} else (permission if permission not in {"UNKNOWN", "-"} else "DOWNGRADE")
        detail = f"{subtype_text}"
        if zone != "-":
            detail += f" | зона: {zone}"
        detail += bias_text
        if post_news_active:
            detail += " | post-news: тільки після retest/acceptance, без chase"
        if subtype_text in {"BEARISH_CONTINUATION_WAIT_RETEST", "BULLISH_CONTINUATION_WAIT_RETEST"}:
            detail += " | no chase; потрібен fresh retest + LTF model + context-invalidation stop"
            return "WATCH", f"• {sym} — WAIT_FRESH_RETEST | {detail}"
        return "NO_TRADE", f"• {sym} — {reason} + DOWNGRADE | {detail}"

    # WATCH means there is a behavior candidate, but still no entry without 5m–15m confirmation.
    # After high-impact USD macro, WATCH must not read like immediate permission.
    if open_behavior in {"OPEN_DRIVE", "OPEN_TEST_DRIVE"}:
        if post_news_active or macro_unknown:
            qualifier = _post_news_watch_qualifier(sym, open_behavior) if post_news_active else "MACRO_UNKNOWN: тільки після зовнішньої перевірки + LTF model + stop + real target"
            detail = f"{subtype_text}{zone_text} | {qualifier}{bias_text}"
            return "WATCH", f"• {sym} — {open_behavior} | {detail}"

        detail = f"{subtype_text}{zone_text} | чекати LTF model{bias_text}"
        return "WATCH", f"• {sym} — {open_behavior} | {detail}"

    if open_behavior == "OPEN_REJECTION_REVERSE":
        detail = f"{subtype_text}{zone_text} | тільки research до чистої LTF-моделі{bias_text}"
        if post_news_active:
            detail = f"{subtype_text}{zone_text} | post-news failed move/rejection context; тільки після reclaim/BOS/retest, без chase{bias_text}"
        return "WATCH", f"• {sym} — OPEN_REJECTION_REVERSE | {detail}"

    # OPEN_AUCTION without DOWNGRADE is not a full no-trade state.
    # It means observe rotations only; no directional battle.
    if open_behavior == "OPEN_AUCTION":
        detail = f"{subtype_text}{zone_text} | тільки ротації{bias_text}"
        if post_news_active:
            detail = f"{subtype_text}{zone_text} | post-news auction/rotation; перший імпульс не наздоганяти{bias_text}"
        return "OBSERVE", f"• {sym} — OPEN_AUCTION | {detail}"

    if open_behavior in {"UNCONFIRMED", "UNKNOWN"}:
        if post_news_active:
            return "OBSERVE", f"• {sym} — POST_NEWS_UNCONFIRMED | {subtype_text} | first impulse already gone; чекати retest/acceptance, не наздоганяти{bias_text}"
        return "OBSERVE", f"• {sym} — {open_behavior} | {subtype_text} | чекати ясності{bias_text}"

    if entry_hint in {"NO_ENTRY_MODEL", "NO_DIRECTIONAL_ENTRY_MODEL"} or battle_hint in {"RESEARCH_ONLY"}:
        suffix = " | post-news: перший імпульс не наздоганяти" if post_news_active else ""
        return "OBSERVE", f"• {sym} — no directional model | {subtype_text} | спостерігати{suffix}{bias_text}"

    if post_news_active:
        return "WATCH", f"• {sym} — {open_behavior} | {subtype_text} | post-news: чекати LTF confirmation + retest/acceptance{bias_text}"

    return "WATCH", f"• {sym} — {open_behavior} | {subtype_text} | чекати LTF confirmation{bias_text}"


def _recent_high_impact_events(
    target_date: date,
    timezone_name: str,
    report_type: str,
    window_minutes: int = 240,
    lookahead_minutes: int = 45,
) -> list[dict[str, Any]]:
    if _normalize_report_type(report_type) not in {"ny", "ny_1h", "new_york"}:
        return []

    calendar = load_high_impact_calendar(target_date)
    events, _hidden = _filter_actionable_high_impact_events(calendar.events, report_type)
    now_local = _now_utc().astimezone(_tz(timezone_name))

    selected: list[dict[str, Any]] = []
    for event in events:
        event_dt = _event_local_datetime(event, timezone_name)
        if event_dt is None:
            continue
        minutes = (now_local - event_dt).total_seconds() / 60.0
        if -lookahead_minutes <= minutes <= window_minutes:
            enriched = dict(event)
            enriched["minutes_since_event"] = round(minutes, 1)
            if minutes < 0:
                enriched["minutes_until_event"] = round(abs(minutes), 1)
            enriched["macro_regime"] = _macro_regime_for_event(enriched)
            selected.append(enriched)

    selected.sort(key=_macro_event_selection_priority)
    return selected


def _first_hour_activity_text(activity: dict[str, Any]) -> str:
    if not isinstance(activity, dict) or not activity:
        return "first-hour activity: немає даних"

    parts: list[str] = []
    ib_dir = _upper(activity.get("ib_direction"), "")
    ib_ext = _upper(activity.get("ib_extension_direction"), "")
    open_dir = _upper(activity.get("open_direction"), "")
    tested = _upper(activity.get("tested_level"), "")
    test_result = _upper(activity.get("test_result"), "")

    if ib_dir:
        parts.append(f"IB={ib_dir}")
    if ib_ext and ib_ext not in {"NONE", "-"}:
        parts.append(f"extension={ib_ext}")
    if open_dir:
        parts.append(f"open={open_dir}")
    if tested and tested not in {"NONE", "-"}:
        if test_result and test_result not in {"NONE", "-"}:
            parts.append(f"test {tested}: {test_result}")
        else:
            parts.append(f"test {tested}")

    if _boolish(activity.get("failed_auction")):
        parts.append("failed auction")
    if _boolish(activity.get("accepted_back_inside_value")):
        parts.append("acceptance back inside value")
    if _boolish(activity.get("accepted_back_inside_range")):
        parts.append("acceptance back inside range")
    if _boolish(activity.get("accepted_outside_range")):
        parts.append("acceptance outside range")

    return ", ".join(parts) if parts else "first-hour activity: без явного висновку"


def _post_news_direction_from_activity(activity: dict[str, Any]) -> str:
    if not isinstance(activity, dict):
        return "UNKNOWN"

    for key in ("ib_extension_direction", "ib_direction", "open_direction", "direction"):
        value = _upper(activity.get(key), "")
        if value in {"UP", "DOWN", "LONG", "SHORT", "BULLISH", "BEARISH"}:
            if value in {"UP", "LONG", "BULLISH"}:
                return "UP"
            return "DOWN"
    return "UNKNOWN"


def _post_news_macro_read(sym: str, direction: str, behavior: str) -> str:
    direction = _upper(direction, "UNKNOWN")
    behavior = _upper(behavior, "UNKNOWN")

    if direction == "DOWN":
        if sym in {"NAS100", "SPX500", "BTCUSD", "ETHUSD"}:
            return "risk-off / downside impulse delivered; first impulse already gone"
        if sym in {"XAUUSD", "EURUSD", "GBPUSD", "AUDUSD"}:
            return "USD-strength downside impulse delivered; no chase"
        if sym == "UKOIL":
            return "risk/oil downside impulse; wait for retest"
        return "downside impulse delivered; wait for retest"

    if direction == "UP":
        if sym in {"USDJPY", "USDCHF", "USDCAD"}:
            return "USD-strength upside impulse delivered; no chase"
        if sym in {"NAS100", "SPX500"}:
            return "risk rebound impulse; wait for acceptance/retest"
        return "upside impulse delivered; wait for retest"

    if behavior in {"OPEN_TEST_DRIVE", "OPEN_DRIVE"}:
        return "directional context present, but post-news battle only after retest + LTF confirmation"
    if behavior == "OPEN_REJECTION_REVERSE":
        return "possible failed move/rejection; require reclaim/BOS/retest"
    if behavior == "OPEN_AUCTION":
        return "auction/rotation after news; do not chase"

    return "post-news volatility regime; wait for acceptance / failed move"


def _post_news_symbol_line(
    sym: str,
    item: dict[str, Any],
    *,
    macro_unknown: bool = False,
    holiday_overlay: bool = False,
) -> str:
    data = _brief_symbol_context(item)
    activity = data.get("first_hour_activity") if isinstance(data.get("first_hour_activity"), dict) else {}

    behavior = data.get("open_behavior") or "UNKNOWN"
    context = data.get("open_context") or "UNKNOWN"
    zone = _compact_zone_text(data.get("primary_zone"))
    direction = _post_news_direction_from_activity(activity)
    activity_text = _first_hour_activity_text(activity)
    macro_read = _post_news_macro_read(sym, direction, behavior)
    subtype = _auction_subtype(sym, data, post_news_active=not macro_unknown, macro_unknown=macro_unknown)
    bias = _bias_without_trade(sym, data, post_news_active=not macro_unknown, macro_unknown=macro_unknown)

    if holiday_overlay and sym in {"NAS100", "SPX500"}:
        zone_text = f" | зона: {zone}" if zone != "-" else ""
        return (
            f"• {sym} — US_HOLIDAY / MARKET_CLOSED{zone_text} | "
            "NY cash session closed; немає нормального NY cash impulse | no Battle"
        )

    if behavior == "OPEN_REJECTION_REVERSE":
        mode = "failed move/rejection: тільки після reclaim/BOS/retest"
    elif behavior == "OPEN_TEST_DRIVE":
        mode = "WATCH_AFTER_RETEST_ONLY: LTF model + stop + real target обовʼязкові"
    elif behavior == "OPEN_AUCTION":
        mode = "rotation/auction: без directional battle"
    elif behavior == "OPEN_DRIVE":
        mode = "drive після news: не chase, тільки pullback/acceptance"
    elif behavior in {"UNCONFIRMED", "UNKNOWN"}:
        mode = "UNCONFIRMED: чекати retest/acceptance"
    else:
        mode = "чекати acceptance / failed move"

    if macro_unknown:
        macro_read = "macro calendar unknown; conservative mode"
        mode = "no Battle without external calendar check + LTF confirmation"

    zone_text = f" | зона: {zone}" if zone != "-" else ""
    return f"• {sym} — {behavior} / {context}{zone_text} | subtype={subtype} | bias={bias} | {macro_read} | {activity_text} | {mode}"


def _build_post_news_reaction_section(
    tpo: dict[str, Any],
    target_date: date,
    timezone_name: str,
    report_type: str,
) -> BriefingSection | None:
    if _normalize_report_type(report_type) not in {"ny", "ny_1h", "new_york"}:
        return None

    recent_events = _recent_high_impact_events(target_date, timezone_name, report_type)
    calendar = load_high_impact_calendar(target_date)
    holiday_overlay = _us_market_holiday_overlay(target_date)
    macro_unknown = _macro_calendar_unknown_conservative(calendar) and not holiday_overlay

    symbols = tpo.get("symbols") if isinstance(tpo, dict) else {}
    symbols = symbols if isinstance(symbols, dict) else {}

    section = BriefingSection("🧭 NY post-open / post-news реакція")

    if recent_events:
        event = _select_primary_macro_event(recent_events) or recent_events[0]
        title = event.get("title") or event.get("event") or "high-impact event"
        minutes = event.get("minutes_since_event")
        local_time = _local_dt_from_event(event, timezone_name)
        macro_regime = event.get("macro_regime") or _macro_regime_for_event(event)

        event_impact_display = _calendar_event_impact_display(event)
        if isinstance(minutes, (int, float)) and float(minutes) < 0:
            section.lines.append(f"Подія: {local_time} — {_raw(event.get('currency'))} {event_impact_display}: {_raw(title)} | до релізу: {event.get('minutes_until_event')} хв")
        else:
            section.lines.append(f"Подія: {local_time} — {_raw(event.get('currency'))} {event_impact_display}: {_raw(title)} | минуло: {minutes} хв")

        section.lines.append(f"Macro regime: {macro_regime}.")
        if str(macro_regime) == "FOMC_PRESSER_LOCK":
            section.lines.append("Режим: NO BATTLE до завершення FOMC press conference; після — тільки 15m acceptance + retest + LTF confirmation + real target.")
        elif str(macro_regime).startswith("FOMC"):
            section.lines.append("Режим: FOMC day/post-news lock; перший імпульс не наздоганяти, чекати acceptance / failed move.")
        else:
            section.lines.append("Режим: оцінюємо acceptance / failed move; перший імпульс не наздоганяти.")

        if _is_fomc_event(event):
            focus_symbols = list(_symbol_scope_for_report(report_type))
        else:
            focus_symbols = _filter_symbols_for_report(event.get("symbols") or [], report_type)
    else:
        if holiday_overlay:
            section.lines.append(f"US holiday session: {holiday_overlay.get('code')} — немає нормального NY cash impulse.")
            section.lines.append("Режим: HOLIDAY_LIQUIDITY_CAUTION; NAS100/SPX500 closed/no Battle, UKOIL caution, FX/XAU/crypto тільки clean setup без chase.")
        elif macro_unknown:
            section.lines.append("Macro calendar unavailable: MACRO_UNKNOWN_CONSERVATIVE.")
            section.lines.append("Режим: NY post-open оцінюємо без macro-clearance; Battle тільки після зовнішньої перевірки + LTF confirmation.")
        else:
            section.lines.append("High-impact post-news подій у вікні не знайдено; оцінюємо NY post-open поведінку.")
        focus_symbols = list(_symbol_scope_for_report(report_type))

    if not focus_symbols:
        focus_symbols = list(_symbol_scope_for_report(report_type))

    printed = 0
    for sym in focus_symbols:
        item = symbols.get(sym)
        if not isinstance(item, dict):
            continue
        section.lines.append(_post_news_symbol_line(sym, item, macro_unknown=macro_unknown and not recent_events, holiday_overlay=bool(holiday_overlay)))
        printed += 1
        if printed >= 8:
            break

    if printed == 0:
        section.lines.append("Немає TPO snapshot для активів NY фокусу.")

    return section


def _build_intermarket_context_section(tpo: dict[str, Any], report_type: str) -> BriefingSection | None:
    symbols = tpo.get("symbols") if isinstance(tpo, dict) else {}
    symbols = symbols if isinstance(symbols, dict) else {}
    scope = list(_symbol_scope_for_report(report_type))

    data_by_symbol: dict[str, dict[str, Any]] = {}
    for sym in scope:
        item = symbols.get(sym)
        if isinstance(item, dict):
            data_by_symbol[sym] = _brief_symbol_context(item)

    if not data_by_symbol:
        return None

    usd_strength_votes = 0
    risk_off_votes = 0
    bearish_pressure: list[str] = []
    bullish_usd_pairs: list[str] = []

    for sym, data in data_by_symbol.items():
        bias = _bias_without_trade(sym, data)
        direction = _post_news_direction_from_activity(data.get("first_hour_activity") if isinstance(data.get("first_hour_activity"), dict) else {})
        if sym in {"EURUSD", "GBPUSD", "AUDUSD", "XAUUSD", "BTCUSD", "ETHUSD"} and ("bearish" in bias or direction == "DOWN"):
            bearish_pressure.append(sym)
        if sym in {"USDJPY", "USDCHF", "USDCAD"} and ("bullish" in bias or direction == "UP"):
            bullish_usd_pairs.append(sym)

    if len(bearish_pressure) >= 2:
        usd_strength_votes += 1
    if bullish_usd_pairs:
        usd_strength_votes += 1
    if any(sym in bearish_pressure for sym in {"BTCUSD", "ETHUSD"}) and any(sym in bearish_pressure for sym in {"XAUUSD", "EURUSD", "GBPUSD"}):
        risk_off_votes += 1

    section = BriefingSection("🧩 Intermarket context")
    if usd_strength_votes:
        section.lines.append(
            "USD bid / defensive tone: pressure підтверджується кількома інструментами; "
            "short-side ідеї по EUR/GBP/XAU/crypto не chase, тільки fresh retest."
        )
        if bearish_pressure:
            section.lines.append(f"Під тиском: {', '.join(bearish_pressure[:8])}.")
        if bullish_usd_pairs:
            section.lines.append(f"USD-пари підтримують імпульс: {', '.join(bullish_usd_pairs[:5])}.")
    elif risk_off_votes:
        section.lines.append("Risk-off tone: risk assets під тиском; потрібен fresh retest, не вхід у перший імпульс.")
    else:
        section.lines.append("Єдиного cross-asset імпульсу немає; оцінювати setups окремо через LTF confirmation.")

    section.lines.append("Intermarket context — це bias-фільтр, не entry trigger.")
    return section


def _build_tpo_snapshot_section(
    tpo: dict[str, Any],
    report_type: str,
    target_date: date | None = None,
    timezone_name: str | None = None,
) -> BriefingSection:
    section = BriefingSection("📌 Стан ринку")
    symbols = tpo.get("symbols") if isinstance(tpo, dict) else {}
    symbols = symbols if isinstance(symbols, dict) else {}

    if not symbols:
        section.lines.append("Немає доступних TPO-символів.")
        return section

    watch_symbols = list(_symbol_scope_for_report(report_type))
    post_news_active = False
    macro_unknown = False
    if target_date is not None and timezone_name:
        post_news_active = bool(_recent_high_impact_events(target_date, timezone_name, report_type))
        macro_unknown = _macro_unknown_conservative_for_report(target_date, timezone_name, report_type)

    no_trade: list[str] = []
    watch: list[str] = []
    observe: list[str] = []
    missing: list[str] = []

    for sym in watch_symbols:
        item = symbols.get(sym)
        if not isinstance(item, dict):
            missing.append(f"• {sym} — немає даних")
            continue

        bucket, line = _brief_verdict(
            sym,
            _brief_symbol_context(item),
            post_news_active=post_news_active,
            macro_unknown=macro_unknown,
            us_holiday_overlay=bool(target_date and _is_us_holiday_overlay_active(target_date)),
        )
        if bucket == "WATCH":
            watch.append(line)
        elif bucket == "OBSERVE":
            observe.append(line)
        else:
            no_trade.append(line)

    if no_trade:
        section.lines.append("NO TRADE:")
        section.lines.extend(no_trade[:8])

    if watch:
        section.lines.append("WATCH:")
        section.lines.extend(watch[:8])
    else:
        section.lines.append("WATCH: немає чистих кандидатів без LTF confirmation")

    if observe:
        section.lines.append("OBSERVE:")
        section.lines.extend(observe[:8])

    if missing:
        section.lines.append("DATA MISSING:")
        section.lines.extend(missing[:5])

    return section




def _fmt_count(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0



def _briefing_section_add_line(section: "BriefingSection", line: str) -> None:
    """
    Compatibility helper for BriefingSection.

    Different versions of daily_market_briefing.py may store section body
    in different attributes. Keep this adapter defensive so positioning
    context cannot break report rendering.
    """
    if hasattr(section, "lines") and isinstance(getattr(section, "lines"), list):
        section.lines.append(line)
        return

    if hasattr(section, "items") and isinstance(getattr(section, "items"), list):
        section.items.append(line)
        return

    if hasattr(section, "body") and isinstance(getattr(section, "body"), list):
        section.body.append(line)
        return

    if hasattr(section, "append"):
        section.append(line)
        return

    # Last-resort fallback. Most project versions use `.lines`, but keep
    # a fallback for older/newer shapes.
    try:
        setattr(section, "lines", [line])
    except Exception:
        pass


def _build_positioning_context_section() -> "BriefingSection":
    """
    Build optional Positioning Intelligence section.

    v0.1 rules:
    - context only;
    - no Battle Gate impact;
    - no Telegram signal permission impact;
    - fail-open: if positioning layer is unavailable, report still renders.
    """
    section = BriefingSection("📊 Positioning Context")

    try:
        from app.services.positioning.positioning_service import get_latest_positioning_context
        from app.services.positioning.positioning_briefing_renderer import render_positioning_block

        snapshot = get_latest_positioning_context()
        block = render_positioning_block(snapshot, max_items=7)
    except Exception as exc:
        block = f"📊 Positioning Context: дані недоступні. ({type(exc).__name__})"

    lines = str(block).splitlines()

    # Renderer already includes the same header. Since BriefingSection also has
    # a title, remove duplicate heading if present.
    if lines and lines[0].strip().startswith("📊 Positioning Context"):
        lines = lines[1:]

    if not lines:
        lines = ["дані недоступні."]

    for line in lines:
        _briefing_section_add_line(section, line)

    return section



def _build_positioning_diagnostics_section() -> "BriefingSection":
    """
    Build diagnostics for Positioning Intelligence metadata in reporting records.

    v0.1 safety:
    - diagnostics only;
    - no signal generation;
    - no Battle Gate impact;
    - no writes to signal_outcomes.json.
    """
    section = BriefingSection("📊 Positioning Diagnostics")

    try:
        from collections import Counter

        outcomes = load_signal_outcomes()
        records = _dedup_signal_outcome_records(_production_records(_signals_from_outcomes(outcomes)))
        records = _positioning_enrich_records_for_reporting(records)

        if not records:
            _briefing_section_add_line(
                section,
                "Поки немає production/research records для оцінки positioning tags."
            )
            _briefing_section_add_line(
                section,
                "Режим: research-only. Battle Gate не змінюється."
            )
            return section

        tag_counts = Counter(
            str(r.get("positioning_primary_tag") or "DATA_UNAVAILABLE")
            for r in records
            if isinstance(r, dict)
        )
        alignment_counts = Counter(
            str(r.get("positioning_alignment") or "UNKNOWN")
            for r in records
            if isinstance(r, dict)
        )
        quality_counts = Counter(
            str(r.get("positioning_data_quality") or "UNKNOWN")
            for r in records
            if isinstance(r, dict)
        )

        unsafe_allow = sum(1 for r in records if bool(r.get("positioning_can_allow_signal")))
        unsafe_block = sum(1 for r in records if bool(r.get("positioning_can_block_signal")))
        non_none_bg = sum(
            1
            for r in records
            if str(r.get("positioning_battle_gate_impact") or "none").lower() != "none"
        )

        _briefing_section_add_line(section, f"Records enriched: {len(records)}")
        _briefing_section_add_line(section, f"Tags: {_format_counter_inline(tag_counts)}")
        _briefing_section_add_line(section, f"Alignment: {_format_counter_inline(alignment_counts)}")
        _briefing_section_add_line(section, f"Data quality: {_format_counter_inline(quality_counts)}")
        _briefing_section_add_line(
            section,
            f"Safety: allow=True {unsafe_allow} | block=True {unsafe_block} | battle_gate_impact!=none {non_none_bg}"
        )
        _briefing_section_add_line(
            section,
            "Висновок: це лише діагностика контексту участі. Сигнали й Battle Gate не змінюються."
        )
        return section

    except Exception as exc:
        _briefing_section_add_line(
            section,
            f"Positioning diagnostics unavailable: {type(exc).__name__}"
        )
        _briefing_section_add_line(
            section,
            "Fail-open: reporting продовжено без positioning diagnostics."
        )
        return section


def _format_counter_inline(counter: "Counter[str]", limit: int = 6) -> str:
    if not counter:
        return "none"

    parts = []
    for key, value in counter.most_common(limit):
        parts.append(f"{key}={value}")

    remaining = sum(counter.values()) - sum(value for _, value in counter.most_common(limit))
    if remaining > 0:
        parts.append(f"other={remaining}")

    return ", ".join(parts)


def _build_suppressed_telegram_section() -> BriefingSection:
    section = BriefingSection("🧹 Відфільтровано з Telegram")
    summary = load_daily_summary()
    if not isinstance(summary, dict) or not summary:
        section.lines.append("Немає daily_summary.json / suppressed metrics ще не згенеровані.")
        return section

    metrics = summary.get("suppressed_telegram_metrics")
    if not isinstance(metrics, dict) or not metrics:
        section.lines.append("Suppressed metrics ще недоступні. Потрібен lightweight_statistics_exporter v2.4.")
        return section

    total = _fmt_count(metrics.get("total"))
    stats_only_total = _fmt_count(metrics.get("statistics_only_total"))
    tracked = metrics.get("tracked_reasons") if isinstance(metrics.get("tracked_reasons"), dict) else {}
    by_reason = metrics.get("by_reason") if isinstance(metrics.get("by_reason"), dict) else {}

    section.lines.append(f"Усього statistics-only / suppressed: {total} | statistics_only=True: {stats_only_total}")
    section.lines.append(
        "Причини: "
        f"invalidated_before_alert={_fmt_count(tracked.get('invalidated_before_alert'))}, "
        f"post_shock_rr_below_3={_fmt_count(tracked.get('post_shock_rr_below_3'))}, "
        f"weak_otd_long={_fmt_count(tracked.get('weak_otd_long'))}"
    )

    extra = [
        (str(k), _fmt_count(v))
        for k, v in by_reason.items()
        if str(k) not in {"invalidated_before_alert", "post_shock_rr_below_3", "tpo_otd_long_stats_downgrade", "weak_otd_long", "UNKNOWN", "None", ""}
    ]
    extra = sorted(extra, key=lambda kv: (-kv[1], kv[0]))[:4]
    if extra:
        section.lines.append("Інші suppress-причини: " + ", ".join(f"{k}={v}" for k, v in extra))

    by_symbol = metrics.get("by_symbol") if isinstance(metrics.get("by_symbol"), dict) else {}
    if by_symbol:
        top_symbols = sorted(((str(k), _fmt_count(v)) for k, v in by_symbol.items()), key=lambda kv: (-kv[1], kv[0]))[:5]
        section.lines.append("Топ символів: " + ", ".join(f"{k}={v}" for k, v in top_symbols))

    section.lines.append("Висновок: це не видалені сигнали, а контроль шуму — вони лишаються в telemetry/statistics, але не йдуть у Telegram.")
    return section


def _build_session_scope_section(tpo: dict[str, Any], report_type: str) -> BriefingSection:
    section = BriefingSection("🎯 Фокус звіту")
    normalized = _normalize_report_type(report_type)
    scope = list(_symbol_scope_for_report(normalized))
    section.lines.append(_scope_label_for_report(normalized))
    section.lines.append(f"У фокусі: {', '.join(scope)}")

    symbols = tpo.get("symbols") if isinstance(tpo, dict) else {}
    symbols = symbols if isinstance(symbols, dict) else {}
    outside_active: list[str] = []
    outside_degraded: list[str] = []

    for sym in _sort_symbols_by_scope([str(x).upper() for x in symbols.keys()], "global"):
        if sym in scope:
            continue
        item = symbols.get(sym)
        if not isinstance(item, dict):
            continue
        data = _brief_symbol_context(item)
        status = str(data.get("market_status") or "").upper()
        if status == "OPEN":
            outside_active.append(sym)
        elif status in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"}:
            outside_degraded.append(f"{sym}:{status}")

    if outside_active:
        section.lines.append(f"Поза фокусом, але активні: {', '.join(outside_active[:8])}")
    if outside_degraded:
        section.lines.append(f"Поза фокусом з data issue: {', '.join(outside_degraded[:5])}")

    if not outside_active and not outside_degraded:
        section.lines.append("Поза фокусом немає важливих active/data-warning символів.")

    return section

def _build_provider_section(tpo: dict[str, Any]) -> BriefingSection:
    section = BriefingSection("🧯 Дані")
    errors = tpo.get("errors") if isinstance(tpo, dict) else []
    fallbacks = tpo.get("fallbacks") if isinstance(tpo, dict) else []
    if not isinstance(errors, list):
        errors = []
    if not isinstance(fallbacks, list):
        fallbacks = []

    if not errors and not fallbacks:
        section.lines.append("Критичних проблем з даними немає.")
        return section

    seen: set[str] = set()

    for err in errors[:5]:
        if not isinstance(err, dict):
            continue
        sym = str(err.get("symbol") or "-")
        seen.add(sym)
        err_type = str(err.get("error_type") or "provider_error")
        section.lines.append(f"• {sym} — {err_type} → STALE_DATA / fallback / NO TRADE")

    for fb in fallbacks[:5]:
        if not isinstance(fb, dict):
            continue
        sym = str(fb.get("symbol") or "-")
        if sym in seen:
            continue
        section.lines.append(f"• {sym} — fallback → STALE_DATA / NO TRADE")

    return section


def _build_focus_section(report_type: str, target_date: date | None = None) -> BriefingSection:
    section = BriefingSection("🧠 Правило дня")
    rt = report_type.lower().strip()
    holiday_overlay = _us_market_holiday_overlay(target_date) if target_date is not None else None

    if rt in {"morning", "morning_briefing", "morning_combined", "holiday_warning", "pre_market"}:
        if holiday_overlay:
            section.lines.extend(
                [
                    "US holiday: це holiday-liquidity caution, не global no-trade для London/morning assets.",
                    "London/morning assets можна торгувати до NY тільки якщо є clean LTF setup + stop + real target + RR.",
                    "NAS100/SPX500 не є Battle focus: US cash market closed / holiday schedule.",
                    "POC/nPOC = зона інтересу, не кнопка входу.",
                ]
            )
        else:
            section.lines.extend(
                [
                    "POC/nPOC = зона інтересу, не кнопка входу.",
                    "Battle тільки після HTF alignment + LTF 5m–15m model + context-invalidation stop + RR.",
                    "До high-impact news не вважати ранню структуру стабільною.",
                ]
            )
    elif rt in {"london", "london_1h"}:
        section.lines.extend(
            [
                "London +1h: acceptance / rejection важливіші за прогноз.",
                "Inside VA = research. Directional battle тільки після LTF-моделі.",
            ]
        )
    elif rt in {"ny", "ny_1h", "new_york"}:
        if holiday_overlay:
            section.lines.extend(
                [
                    "NY holiday: немає нормального NY cash impulse; NAS100/SPX500 closed / no Battle.",
                    "FX/XAU/crypto тільки reduced-confidence clean setups; без chase і без weak RR.",
                    "UKOIL — caution через holiday liquidity schedule; потрібен retest/acceptance + real target.",
                    "POC/nPOC = зона інтересу, не entry trigger.",
                ]
            )
        else:
            section.lines.extend(
                [
                    "NY post-open: не наздоганяємо перший імпульс.",
                    "FOMC / high-impact news: NO BATTLE до завершення lock; після — тільки acceptance + retest.",
                    "Потрібні open behavior + LTF model + context-invalidation stop + Battle Gate.",
                    "POC/nPOC = зона інтересу, не entry trigger.",
                ]
            )
    else:
        section.lines.append("Battle Gate — фінальний дозвіл для Telegram.")
    return section



def _tpo_audit_sources(record: Any) -> list[dict[str, Any]]:
    if not isinstance(record, dict):
        return []

    context = record.get("context") if isinstance(record.get("context"), dict) else {}
    filters = record.get("filters") if isinstance(record.get("filters"), dict) else {}
    behavior = record.get("open_behavior") if isinstance(record.get("open_behavior"), dict) else {}
    auction_state = record.get("auction_state") if isinstance(record.get("auction_state"), dict) else {}

    # Prefer normalized/context-rich sources, but keep root as fallback.
    return [behavior, auction_state, context, filters, record]


def _tpo_audit_pick(record: Any, *keys: str) -> Any:
    for source in _tpo_audit_sources(record):
        for key in keys:
            value = source.get(key)
            if value not in (None, "", [], {}):
                return value
    return None


def _looks_like_tpo_symbol_record(key: Any, record: Any) -> bool:
    if not isinstance(record, dict):
        return False
    if str(key).lower() in {
        "updated_at_utc",
        "created_at_utc",
        "version",
        "schema_version",
        "metadata",
        "provider",
        "providers",
        "errors",
        "warnings",
    }:
        return False

    if isinstance(record.get("context"), dict) or isinstance(record.get("filters"), dict) or isinstance(record.get("open_behavior"), dict):
        return True

    return any(
        _tpo_audit_pick(record, field) is not None
        for field in (
            "previous_vah",
            "previous_val",
            "previous_poc",
            "current_open",
            "open_relation",
            "open_context",
            "open_behavior",
        )
    )


def _build_tpo_audit_snapshot(tpo: Any, report_type: str | None = None) -> dict[str, Any]:
    """
    Persist compact per-briefing TPO audit data.

    This snapshot makes historical VAH/VAL/POC/open-behavior reviews possible
    even after runtime/tpo/tpo_latest.json moves on to a later session.
    """
    if not isinstance(tpo, dict):
        return {
            "version": "tpo-audit-snapshot-v1",
            "report_type": report_type,
            "updated_at_utc": None,
            "symbols": {},
        }

    container = None
    for key in ("symbols", "items", "records", "data"):
        if isinstance(tpo.get(key), dict):
            container = tpo.get(key)
            break
    if container is None:
        container = tpo

    fields = (
        "symbol",
        "current_session_id",
        "previous_session_id",
        "current_price",
        "previous_poc",
        "previous_vah",
        "previous_val",
        "previous_high",
        "previous_low",
        "current_open",
        "open_relation",
        "open_context",
        "open_location",
        "open_behavior",
        "open_behavior_confidence",
        "initial_open_behavior",
        "current_open_behavior",
        "behavior_transition",
        "value_acceptance_state",
        "value_test_occurred",
        "value_test_level",
        "value_rejection_confirmed",
        "day_type_candidate",
        "auction_state_confidence",
        "session_normalization_version",
        "session_scope",
        "primary_session",
        "prior_value_scope",
        "prior_range_scope",
        "open_event",
        "open_event_type",
        "reference_profile_id",
        "active_participation_center",
        "profile_reliability_score",
        "profile_reliability_state",
        "session_status",
        "market_status",
        "synthetic_open",
        "synthetic_open_confirmed",
        "true_otd_allowed",
        "entry_model_hint",
        "stop_model_hint",
        "battle_bias_hint",
        "open_behavior_reason",
        "open_behavior_warnings",
        "warnings",
    )

    symbols: dict[str, Any] = {}
    for key, record in container.items():
        if not _looks_like_tpo_symbol_record(key, record):
            continue

        symbol = str(_tpo_audit_pick(record, "symbol") or key).upper()
        row = {field: _tpo_audit_pick(record, field) for field in fields}
        row["symbol"] = symbol
        symbols[symbol] = row

    return {
        "version": "tpo-audit-snapshot-v1",
        "report_type": report_type,
        "updated_at_utc": tpo.get("updated_at_utc"),
        "symbol_count": len(symbols),
        "symbols": symbols,
    }


def build_briefing_report(
    *,
    report_type: str = "morning",
    report_date: str | None = None,
    timezone_name: str | None = None,
) -> BriefingReport:
    tz_name = timezone_name or os.getenv("REPORT_TIMEZONE") or DEFAULT_TIMEZONE
    target_date = _parse_date(report_date, tz_name)
    now_utc = _now_utc()
    now_local = now_utc.astimezone(_tz(tz_name))

    tpo = load_tpo_store()
    daily_summary = load_daily_summary()
    normalized_type = _normalize_report_type(report_type)

    report = BriefingReport(
        report_type=normalized_type,
        report_date=target_date.isoformat(),
        timezone=tz_name,
        generated_at_utc=now_utc.isoformat(),
        generated_at_local=now_local.isoformat(),
        raw={
            "tpo_path": str(_tpo_path()),
            "daily_summary_path": str(_daily_summary_path()),
            "signal_outcomes_path": str(_signal_outcomes_path()),
            "high_impact_events_path": str(_high_impact_events_path()),
            "economic_calendar_cache_path": str(_economic_calendar_cache_path(target_date)),
            "trading_economics_calendar_cache_path": str(_trading_economics_calendar_cache_path(target_date)),
            "manual_high_impact_events_path": str(_manual_high_impact_events_path()),
            "last_good_high_impact_events_path": str(_last_good_high_impact_events_path()),
            "economic_calendar_provider": os.getenv("ECONOMIC_CALENDAR_PROVIDER", "faireconomy_forexfactory_xml"),
            "economic_calendar_enabled": os.getenv("ENABLE_ECONOMIC_CALENDAR", "true"),
            "tpo_updated_at_utc": tpo.get("updated_at_utc") if isinstance(tpo, dict) else None,
            "daily_summary_updated_at_utc": daily_summary.get("updated_at_utc") if isinstance(daily_summary, dict) else None,
            "tpo_audit_snapshot": _build_tpo_audit_snapshot(tpo, normalized_type),
        },
    )

    report.sections.append(_build_high_impact_section(target_date, tz_name, normalized_type))
    report.sections.append(_build_session_scope_section(tpo, normalized_type))

    post_news_section = _build_post_news_reaction_section(tpo, target_date, tz_name, normalized_type)
    if post_news_section is not None:
        report.sections.append(post_news_section)

    intermarket_section = _build_intermarket_context_section(tpo, normalized_type)
    if intermarket_section is not None:
        report.sections.append(intermarket_section)

    report.sections.append(_build_tpo_snapshot_section(tpo, normalized_type, target_date, tz_name))
    report.sections.append(_build_positioning_context_section())
    report.sections.append(_build_suppressed_telegram_section())
    report.sections.append(_build_positioning_diagnostics_section())

    if normalized_type in {"morning", "morning_briefing", "morning_combined", "holiday_warning", "pre_market"}:
        report.sections.append(_build_yesterday_section(target_date, tz_name))
        report.sections.append(_build_overall_stats_section())

    # Provider details remain available in JSON artifacts. Telegram stays operational and concise.
    provider_section = _build_provider_section(tpo)
    if provider_section.lines and provider_section.lines != ["Критичних проблем з даними немає."]:
        report.sections.append(provider_section)

    report.sections.append(_build_focus_section(normalized_type, target_date))
    return report


def render_briefing_text(report: BriefingReport) -> str:
    header = _section_header_for_type(report.report_type, report.generated_at_local)
    lines = [
        f"<b>{_esc(header)} — {_esc(report.report_date)}</b>",
        f"Згенеровано: {_esc(report.generated_at_local)}",
        f"Версія: {_esc(report.version)}",
        "",
    ]
    for section in report.sections:
        lines.append(f"<b>{_esc(section.title)}</b>")
        if section.lines:
            lines.extend(_esc(line) for line in section.lines)
        else:
            lines.append("-")
        lines.append("")
    return "\n".join(lines).strip()


def report_to_dict(report: BriefingReport) -> dict[str, Any]:
    return {
        "version": report.version,
        "report_type": report.report_type,
        "report_date": report.report_date,
        "timezone": report.timezone,
        "generated_at_utc": report.generated_at_utc,
        "generated_at_local": report.generated_at_local,
        "sections": [{"title": s.title, "lines": list(s.lines)} for s in report.sections],
        "raw": report.raw,
    }


def write_briefing_artifacts(report: BriefingReport, *, output_dir: Path | None = None) -> tuple[Path, Path]:
    out_dir = output_dir or (_runtime_dir() / "reports" / "briefings")
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{report.report_date}_{report.report_type}"
    json_path = out_dir / f"{stem}.json"
    txt_path = out_dir / f"{stem}.txt"
    _safe_write_json(json_path, report_to_dict(report))
    txt_path.write_text(render_briefing_text(report), encoding="utf-8")
    return json_path, txt_path


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Build AI Market Analyst daily/session briefing.")
    parser.add_argument("--type", default=os.getenv("REPORT_TYPE", "morning"))
    parser.add_argument("--date", default=os.getenv("REPORT_DATE"))
    parser.add_argument("--timezone", default=os.getenv("REPORT_TIMEZONE", DEFAULT_TIMEZONE))
    parser.add_argument(
        "--as-of",
        default=os.getenv("REPORT_AS_OF"),
        help="Deterministic report clock override, e.g. 2026-06-17T21:29:00+03:00. Useful for historical dry-runs.",
    )
    parser.add_argument("--print", dest="print_report", action="store_true")

    write_group = parser.add_mutually_exclusive_group()
    write_group.add_argument(
        "--write",
        dest="write",
        action="store_true",
        help="Write briefing JSON/text artifacts to the runtime reports directory. This is the default.",
    )
    write_group.add_argument(
        "--no-write",
        dest="write",
        action="store_false",
        help="Dry-run mode: build the report without writing runtime artifacts or macro caches.",
    )
    parser.set_defaults(write=True)

    args = parser.parse_args()

    previous_disable_runtime_writes = os.environ.get("BRIEFING_DISABLE_RUNTIME_WRITES")
    previous_as_of = os.environ.get("BRIEFING_AS_OF")
    previous_as_of_tz = os.environ.get("BRIEFING_AS_OF_TIMEZONE")

    if not args.write:
        os.environ["BRIEFING_DISABLE_RUNTIME_WRITES"] = "1"
    if args.as_of:
        os.environ["BRIEFING_AS_OF"] = str(args.as_of)
        os.environ["BRIEFING_AS_OF_TIMEZONE"] = str(args.timezone or DEFAULT_TIMEZONE)

    try:
        report = build_briefing_report(report_type=args.type, report_date=args.date, timezone_name=args.timezone)
    finally:
        if not args.write:
            if previous_disable_runtime_writes is None:
                os.environ.pop("BRIEFING_DISABLE_RUNTIME_WRITES", None)
            else:
                os.environ["BRIEFING_DISABLE_RUNTIME_WRITES"] = previous_disable_runtime_writes

        if args.as_of:
            if previous_as_of is None:
                os.environ.pop("BRIEFING_AS_OF", None)
            else:
                os.environ["BRIEFING_AS_OF"] = previous_as_of

            if previous_as_of_tz is None:
                os.environ.pop("BRIEFING_AS_OF_TIMEZONE", None)
            else:
                os.environ["BRIEFING_AS_OF_TIMEZONE"] = previous_as_of_tz

    if args.write:
        json_path, txt_path = write_briefing_artifacts(report)
        print(json.dumps({"json": str(json_path), "text": str(txt_path)}, ensure_ascii=False, indent=2))
    elif not args.print_report:
        print(
            json.dumps(
                {
                    "json": None,
                    "text": None,
                    "write": False,
                    "note": "--no-write used; no runtime artifacts or macro caches were written.",
                },
                ensure_ascii=False,
                indent=2,
            )
        )

    if args.print_report:
        print(render_briefing_text(report))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())