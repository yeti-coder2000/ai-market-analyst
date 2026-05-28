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
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    from app.core.settings import settings
except Exception:  # pragma: no cover
    settings = None  # type: ignore[assignment]


BRIEFING_VERSION = "daily-market-briefing-v1.4-ukrainian-operational-brief"
DEFAULT_TIMEZONE = "Europe/Kyiv"

TPO_LATEST_RELATIVE = Path("tpo") / "tpo_latest.json"
DAILY_SUMMARY_RELATIVE = Path("stats") / "daily_summary.json"
SIGNAL_OUTCOMES_RELATIVE = Path("stats") / "signal_outcomes.json"
HIGH_IMPACT_EVENTS_RELATIVE = Path("calendar") / "high_impact_events.json"

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


def load_high_impact_events(target_date: date) -> list[dict[str, Any]]:
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

        impact = str(event.get("impact") or "").strip().upper()
        if impact and impact not in {"HIGH", "RED", "IMPORTANT"}:
            continue

        key = (
            str(event.get("date") or ""),
            str(event.get("time") or ""),
            str(event.get("timezone") or ""),
            str(event.get("title") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        selected.append(event)

    selected.sort(key=lambda x: (str(x.get("time") or "99:99"), str(x.get("title") or "")))
    return selected


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


def _build_high_impact_section(target_date: date, timezone_name: str, report_type: str) -> BriefingSection:
    events = load_high_impact_events(target_date)
    section = BriefingSection("🔴 Ризик дня")

    if not events:
        section.lines.append("HIGH/RED подій на сьогодні не налаштовано.")
        return section

    for e in events[:4]:
        local_time = _local_dt_from_event(e, timezone_name)
        currency = e.get("currency") or "-"
        impact = str(e.get("impact") or "HIGH").upper()
        title = e.get("title") or "Unnamed event"
        note = _translate_note(e.get("note"))
        section.lines.append(f"• {local_time} — {currency} {impact}: {title}")
        if note:
            section.lines.append(f"  {note}")
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

    if data.get("fallback") or market_status in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"} or permission in {"STALE_DATA", "NO_DATA", "PROVIDER_ERROR"}:
        return "NO_TRADE", f"• {sym} — {market_status}"

    if market_status.startswith("MARKET_CLOSED") or permission == "MARKET_CLOSED":
        return "NO_TRADE", f"• {sym} — MARKET_CLOSED"

    if modifier == "DOWNGRADE":
        reason = open_behavior if open_behavior not in {"UNKNOWN", "-"} else "DOWNGRADE"
        return "NO_TRADE", f"• {sym} — {reason} + DOWNGRADE"

    if open_behavior in {"OPEN_DRIVE", "OPEN_TEST_DRIVE"}:
        detail = "чекати LTF model"
        if zone != "-":
            detail = f"{zone} | чекати LTF model"
        return "WATCH", f"• {sym} — {open_behavior} | {detail}"

    if open_behavior == "OPEN_REJECTION_REVERSE":
        return "WATCH", f"• {sym} — OPEN_REJECTION_REVERSE | тільки research до чистої LTF-моделі"

    if open_behavior in {"OPEN_AUCTION", "UNCONFIRMED", "UNKNOWN"}:
        return "NO_TRADE", f"• {sym} — {open_behavior}"

    if entry_hint in {"NO_ENTRY_MODEL", "NO_DIRECTIONAL_ENTRY_MODEL"} or battle_hint in {"RESEARCH_ONLY", "DOWNGRADE_NO_DIRECTIONAL_BATTLE"}:
        return "NO_TRADE", f"• {sym} — no directional model"

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
    missing: list[str] = []

    for sym in watch_symbols:
        item = symbols.get(sym)
        if not isinstance(item, dict):
            missing.append(f"• {sym} — немає даних")
            continue

        bucket, line = _brief_verdict(sym, _brief_symbol_context(item))
        if bucket == "WATCH":
            watch.append(line)
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

    if missing:
        section.lines.append("DATA MISSING:")
        section.lines.extend(missing[:5])

    return section


def _build_provider_section(tpo: dict[str, Any]) -> BriefingSection:
    section = BriefingSection("🧯 Проблеми з даними / провайдерами")
    errors = tpo.get("errors") if isinstance(tpo, dict) else []
    fallbacks = tpo.get("fallbacks") if isinstance(tpo, dict) else []
    if not isinstance(errors, list):
        errors = []
    if not isinstance(fallbacks, list):
        fallbacks = []

    symbols = tpo.get("symbols") if isinstance(tpo, dict) and isinstance(tpo.get("symbols"), dict) else {}
    section.lines.append(f"TPO-символів: {len(symbols)}")
    section.lines.append(f"Помилок: {len(errors)}")
    section.lines.append(f"Fallback-режимів: {len(fallbacks)}")

    for err in errors[:5]:
        if isinstance(err, dict):
            section.lines.append(
                f"• помилка {err.get('symbol', '-')}: {err.get('error_type', '-')}: {err.get('error', '-')}"
            )
    for fb in fallbacks[:5]:
        if isinstance(fb, dict):
            section.lines.append(
                f"• fallback {fb.get('symbol', '-')}: {fb.get('reason', fb.get('error_type', '-'))}"
            )
    if len(section.lines) == 3 and not errors and not fallbacks:
        section.lines.append("У latest TPO export немає проблем з провайдерами.")
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