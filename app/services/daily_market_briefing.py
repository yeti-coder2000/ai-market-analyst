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
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    from app.core.settings import settings
except Exception:  # pragma: no cover
    settings = None  # type: ignore[assignment]


BRIEFING_VERSION = "daily-market-briefing-v1.7-finnhub-filtered-risk-calendar"
DEFAULT_TIMEZONE = "Europe/Kyiv"

TPO_LATEST_RELATIVE = Path("tpo") / "tpo_latest.json"
DAILY_SUMMARY_RELATIVE = Path("stats") / "daily_summary.json"
SIGNAL_OUTCOMES_RELATIVE = Path("stats") / "signal_outcomes.json"
HIGH_IMPACT_EVENTS_RELATIVE = Path("calendar") / "high_impact_events.json"
ECONOMIC_CALENDAR_CACHE_RELATIVE = Path("calendar")

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
    "morning": "London / ранкова сесія. NY cash/risk-активи винесені у звіт NY +1h.",
    "morning_briefing": "London / ранкова сесія. NY cash/risk-активи винесені у звіт NY +1h.",
    "morning_combined": "London + ранковий брифінг. NY cash/risk-активи винесені у звіт NY +1h.",
    "london": "London +1h. NY cash/risk-активи тут не показуються як активний фокус.",
    "london_1h": "London +1h. NY cash/risk-активи тут не показуються як активний фокус.",
    "ny": "New York +1h. London-only активи тут не показуються як активний фокус.",
    "ny_1h": "New York +1h. London-only активи тут не показуються як активний фокус.",
    "new_york": "New York +1h. London-only активи тут не показуються як активний фокус.",
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


@dataclass
class CalendarLoadResult:
    status: str
    source: str
    events: list[dict[str, Any]] = field(default_factory=list)
    message: str | None = None
    cache_path: str | None = None
    provider_error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status in {"OK", "EMPTY"}



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


def _now_utc() -> datetime:
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


def _affected_symbols(currency: str, event_title: str = "") -> list[str]:
    cur = str(currency or "").strip().upper()
    symbols = list(AFFECTED_SYMBOLS_BY_CURRENCY.get(cur, []))

    title = str(event_title or "").upper()
    if "OIL" in title or "CRUDE" in title or "OPEC" in title:
        for sym in ("UKOIL", "USDCAD"):
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

    normalized.sort(key=lambda x: (str(x.get("time") or "99:99"), str(x.get("currency") or ""), str(x.get("title") or "")))

    status = "OK" if normalized else "EMPTY"
    cache_payload = {
        "source": "finnhub",
        "status": status,
        "date": target_date.isoformat(),
        "updated_at_utc": _now_utc().isoformat(),
        "events": normalized,
        "raw_count": len(raw_events),
    }
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


def _load_static_calendar_events(target_date: date) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    events.extend(_extract_list(_safe_read_json(_high_impact_events_path(), [])))

    inline = os.getenv("HIGH_IMPACT_EVENTS_JSON")
    if inline:
        try:
            events.extend(_extract_list(json.loads(inline)))
        except Exception:
            pass

    events.extend(BUILTIN_HIGH_IMPACT_EVENTS)

    target_text = target_date.isoformat()
    seen: set[tuple[str, str, str, str]] = set()
    selected: list[dict[str, Any]] = []

    for event in events:
        if str(event.get("date") or "").strip() != target_text:
            continue

        impact = _normalize_impact(event.get("impact"), str(event.get("title") or ""))
        if impact and impact not in {"HIGH", "RED", "IMPORTANT"}:
            continue

        normalized = dict(event)
        normalized["impact"] = "HIGH" if impact in {"RED", "IMPORTANT"} else impact
        normalized.setdefault("source", "static_fallback")
        normalized.setdefault("symbols", _affected_symbols(str(normalized.get("currency") or ""), str(normalized.get("title") or "")))
        normalized.setdefault("note", _event_trading_note(str(normalized.get("currency") or ""), str(normalized.get("title") or ""), str(normalized.get("impact") or "")))

        key = (
            str(normalized.get("date") or ""),
            str(normalized.get("time") or ""),
            str(normalized.get("timezone") or ""),
            str(normalized.get("title") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        selected.append(normalized)

    selected.sort(key=lambda x: (str(x.get("time") or "99:99"), str(x.get("title") or "")))
    return selected


def load_high_impact_calendar(target_date: date) -> CalendarLoadResult:
    result = _fetch_finnhub_calendar(target_date)

    if result.status in {"OK", "EMPTY"}:
        high_events = [
            e for e in result.events
            if _normalize_impact(e.get("impact"), str(e.get("title") or "")) in {"HIGH", "RED", "IMPORTANT"}
        ]
        return CalendarLoadResult(
            status="OK" if high_events else "EMPTY",
            source=result.source,
            events=high_events,
            message=result.message,
            cache_path=result.cache_path,
            provider_error=result.provider_error,
        )

    static_events = _load_static_calendar_events(target_date)
    if static_events:
        return CalendarLoadResult(
            status="FALLBACK",
            source="static_fallback",
            events=static_events,
            message=f"{result.message or 'Provider unavailable'} Using static fallback.",
            cache_path=result.cache_path,
            provider_error=result.provider_error,
        )

    return result


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


def _safe_avg(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


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
    records = _production_records(_signals_from_outcomes(outcomes))

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


def _normalize_report_type(report_type: str) -> str:
    return str(report_type or "morning").strip().lower()


def _section_header_for_type(report_type: str) -> str:
    r = _normalize_report_type(report_type)
    if r in {"morning", "morning_briefing", "morning_combined"}:
        return "🌅 Ранковий брифінг"
    if r in {"london_1h", "london"}:
        return "🇬🇧 Звіт London +1 година"
    if r in {"ny_1h", "ny", "new_york"}:
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

def _build_high_impact_section(target_date: date, timezone_name: str, report_type: str) -> BriefingSection:
    calendar = load_high_impact_calendar(target_date)
    raw_events = calendar.events
    events, hidden_unmapped = _filter_actionable_high_impact_events(raw_events, report_type)
    section = BriefingSection("🔴 Ризик дня")

    if not events:
        if calendar.status == "EMPTY":
            section.lines.append("HIGH/RED подій за підключеним календарем не знайдено.")
            section.lines.append(f"Джерело: {calendar.source}.")
        elif raw_events and hidden_unmapped:
            section.lines.append("HIGH/RED подій для нашого торгового фокусу не знайдено.")
            section.lines.append(f"Приховано невизначених provider-подій без валюти/активів: {hidden_unmapped}.")
            section.lines.append(f"Джерело: {calendar.source}.")
        else:
            section.lines.append("Календар high-impact news не завантажений / provider unavailable.")
            section.lines.append("Це не означає, що high-impact news немає.")
            section.lines.append("Режим: обережність, перевірити зовнішній календар перед NY.")
            if calendar.message:
                section.lines.append(f"Статус: {calendar.message}")
        return section

    if calendar.status == "FALLBACK":
        section.lines.append("⚠️ Основний календар недоступний; використовується static fallback.")
        section.lines.append("Це може бути неповний список подій.")

    if hidden_unmapped:
        section.lines.append(f"ℹ️ Приховано provider-подій без валюти/активів: {hidden_unmapped}.")

    for e in events[:5]:
        local_time = _local_dt_from_event(e, timezone_name)
        currency = e.get("currency") or "-"
        impact = str(e.get("impact") or "HIGH").upper()
        title = e.get("title") or e.get("event") or "Unnamed event"
        symbols = _filter_symbols_for_report(e.get("symbols") or [], report_type)
        note = _translate_note(e.get("note"))
        source = e.get("source") or calendar.source

        symbol_text = ""
        if symbols:
            symbol_text = f" | активи: {', '.join(symbols[:8])}"

        section.lines.append(f"• {local_time} — {currency} {impact}: {title}{symbol_text}")
        if note:
            section.lines.append(f"  {note}")
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


def _brief_symbol_context(item: dict[str, Any]) -> dict[str, Any]:
    ctx = item.get("context") if isinstance(item.get("context"), dict) else {}
    filters = item.get("filters") if isinstance(item.get("filters"), dict) else {}
    ob = item.get("open_behavior") if isinstance(item.get("open_behavior"), dict) else {}

    market_status = _upper(ctx.get("market_status") or filters.get("market_status"), "UNKNOWN")
    permission = _upper(filters.get("tpo_signal_permission") or ctx.get("tpo_signal_permission"), "UNKNOWN")
    modifier = _upper(filters.get("telegram_modifier") or filters.get("tpo_telegram_modifier") or ctx.get("tpo_telegram_modifier"), "NEUTRAL")
    open_context = _upper(ctx.get("open_context") or filters.get("open_context") or ob.get("open_context"), "UNKNOWN")
    open_behavior = _upper(ctx.get("open_behavior") or filters.get("open_behavior") or ob.get("open_behavior"), "UNKNOWN")
    entry_hint = _upper(ctx.get("entry_model_hint") or filters.get("entry_model_hint") or ob.get("entry_model_hint"), "NO_ENTRY_MODEL")
    battle_hint = _upper(ctx.get("battle_bias_hint") or filters.get("battle_bias_hint") or ob.get("battle_bias_hint"), "RESEARCH_ONLY")
    primary_zone = ctx.get("primary_interest_zone") or ob.get("primary_interest_zone")
    warnings = ob.get("warnings") if isinstance(ob.get("warnings"), list) else []

    return {
        "market_status": market_status,
        "permission": permission,
        "modifier": modifier,
        "open_context": open_context,
        "open_behavior": open_behavior,
        "entry_hint": entry_hint,
        "battle_hint": battle_hint,
        "primary_zone": primary_zone,
        "warnings": warnings,
        "fallback": bool(item.get("fallback_preserved_previous_context") or filters.get("fallback_preserved_previous_context")),
    }


def _brief_verdict(sym: str, data: dict[str, Any]) -> tuple[str, str]:
    market_status = data["market_status"]
    permission = data["permission"]
    modifier = data["modifier"]
    open_behavior = data["open_behavior"]
    entry_hint = data["entry_hint"]
    battle_hint = data["battle_hint"]
    zone = _compact_zone_text(data.get("primary_zone"))

    # NO TRADE is reserved only for real blockers: stale/closed/no data/provider error/downshift/block.
    if data.get("fallback") or market_status in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"} or permission in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"}:
        return "NO_TRADE", f"• {sym} — {market_status}"

    if market_status.startswith("MARKET_CLOSED") or permission == "MARKET_CLOSED":
        return "NO_TRADE", f"• {sym} — MARKET_CLOSED"

    if modifier == "DOWNGRADE" or permission in {"BLOCKED_BY_CONTEXT", "BLOCKED_BY_AUCTION"} or battle_hint in {"DOWNGRADE_NO_DIRECTIONAL_BATTLE", "BLOCK"}:
        reason = open_behavior if open_behavior not in {"UNKNOWN", "-"} else (permission if permission not in {"UNKNOWN", "-"} else "DOWNGRADE")
        return "NO_TRADE", f"• {sym} — {reason} + DOWNGRADE"

    # WATCH means there is a behavior candidate, but still no entry without 5m–15m confirmation.
    if open_behavior in {"OPEN_DRIVE", "OPEN_TEST_DRIVE"}:
        detail = "чекати LTF model"
        if zone != "-":
            detail = f"зона: {zone} | чекати LTF model"
        return "WATCH", f"• {sym} — {open_behavior} | {detail}"

    if open_behavior == "OPEN_REJECTION_REVERSE":
        detail = "тільки research до чистої LTF-моделі"
        if zone != "-":
            detail = f"зона: {zone} | {detail}"
        return "WATCH", f"• {sym} — OPEN_REJECTION_REVERSE | {detail}"

    # OPEN_AUCTION without DOWNGRADE is not a full no-trade state.
    # It means observe rotations only; no directional battle.
    if open_behavior == "OPEN_AUCTION":
        detail = "тільки ротації"
        if zone != "-":
            detail = f"зона: {zone} | тільки ротації"
        return "OBSERVE", f"• {sym} — OPEN_AUCTION | {detail}"

    if open_behavior in {"UNCONFIRMED", "UNKNOWN"}:
        return "OBSERVE", f"• {sym} — {open_behavior} | чекати ясності"

    if entry_hint in {"NO_ENTRY_MODEL", "NO_DIRECTIONAL_ENTRY_MODEL"} or battle_hint in {"RESEARCH_ONLY"}:
        return "OBSERVE", f"• {sym} — no directional model | спостерігати"

    return "WATCH", f"• {sym} — {open_behavior} | чекати LTF confirmation"


def _build_tpo_snapshot_section(tpo: dict[str, Any], report_type: str) -> BriefingSection:
    section = BriefingSection("📌 Стан ринку")
    symbols = tpo.get("symbols") if isinstance(tpo, dict) else {}
    symbols = symbols if isinstance(symbols, dict) else {}

    if not symbols:
        section.lines.append("Немає доступних TPO-символів.")
        return section

    watch_symbols = list(_symbol_scope_for_report(report_type))
    no_trade: list[str] = []
    watch: list[str] = []
    observe: list[str] = []
    missing: list[str] = []

    for sym in watch_symbols:
        item = symbols.get(sym)
        if not isinstance(item, dict):
            missing.append(f"• {sym} — немає даних")
            continue

        bucket, line = _brief_verdict(sym, _brief_symbol_context(item))
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


def _build_focus_section(report_type: str) -> BriefingSection:
    section = BriefingSection("🧠 Правило дня")
    rt = report_type.lower().strip()

    if rt in {"morning", "morning_briefing", "morning_combined", "holiday_warning", "pre_market"}:
        section.lines.extend(
            [
                "POC/nPOC = зона інтересу, не кнопка входу.",
                "Battle тільки після HTF alignment + LTF 5m–15m model + stop + RR.",
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
        section.lines.extend(
            [
                "NY +1h: не наздоганяємо перший імпульс.",
                "Потрібні open behavior + LTF model + stop + Battle Gate.",
                "POC/nPOC = зона інтересу, не entry trigger.",
            ]
        )
    else:
        section.lines.append("Battle Gate — фінальний дозвіл для Telegram.")
    return section


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
            "economic_calendar_provider": os.getenv("ECONOMIC_CALENDAR_PROVIDER", "finnhub"),
            "economic_calendar_enabled": os.getenv("ENABLE_ECONOMIC_CALENDAR", "true"),
            "tpo_updated_at_utc": tpo.get("updated_at_utc") if isinstance(tpo, dict) else None,
            "daily_summary_updated_at_utc": daily_summary.get("updated_at_utc") if isinstance(daily_summary, dict) else None,
        },
    )

    report.sections.append(_build_high_impact_section(target_date, tz_name, normalized_type))
    report.sections.append(_build_tpo_snapshot_section(tpo, normalized_type))

    if normalized_type in {"morning", "morning_briefing", "morning_combined", "holiday_warning", "pre_market"}:
        report.sections.append(_build_yesterday_section(target_date, tz_name))

    # Provider details remain available in JSON artifacts. Telegram stays operational and concise.
    provider_section = _build_provider_section(tpo)
    if any(line.startswith("Помилок: ") and not line.endswith("0") for line in provider_section.lines) or any(
        line.startswith("Fallback-режимів: ") and not line.endswith("0") for line in provider_section.lines
    ):
        report.sections.append(provider_section)

    report.sections.append(_build_focus_section(normalized_type))
    return report


def render_briefing_text(report: BriefingReport) -> str:
    header = _section_header_for_type(report.report_type)
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
    parser.add_argument("--print", dest="print_report", action="store_true")
    parser.add_argument("--write", action="store_true", default=True)
    args = parser.parse_args()

    report = build_briefing_report(report_type=args.type, report_date=args.date, timezone_name=args.timezone)

    if args.write:
        json_path, txt_path = write_briefing_artifacts(report)
        print(json.dumps({"json": str(json_path), "text": str(txt_path)}, ensure_ascii=False, indent=2))

    if args.print_report:
        print(render_briefing_text(report))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
