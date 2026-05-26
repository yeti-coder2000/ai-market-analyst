from __future__ import annotations

"""
Daily market briefing service for AI Market Analyst.

Read-only reporting layer:
- market holidays / closed markets
- high-impact macro events
- yesterday performance recap
- TPO / auction snapshot
- provider/data issues

Runtime inputs:
- runtime/tpo/tpo_latest.json
- runtime/stats/daily_summary.json
- runtime/stats/signal_outcomes.json
- runtime/calendar/high_impact_events.json (optional)
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


BRIEFING_VERSION = "daily-market-briefing-v1.0"
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
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
        kyiv_event = local_event.astimezone(_tz(report_timezone))
        return kyiv_event.strftime("%H:%M %Z")
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


def _yesterday_metric(timezone_name: str, target_date: date) -> tuple[dict[str, Any], str]:
    outcomes = load_signal_outcomes()
    records = _production_records(_signals_from_outcomes(outcomes))

    yesterday = target_date - timedelta(days=1)
    dated = [r for r in records if _record_local_date(r, timezone_name) == yesterday]

    if dated:
        return _metric_from_records(dated), "signal_outcomes_by_yesterday_date"

    summary = load_daily_summary()
    s = summary.get("summary") if isinstance(summary, dict) else {}
    if isinstance(s, dict) and s:
        metric = {
            "total": s.get("total_signals"),
            "tp": s.get("tp_hit"),
            "sl": s.get("sl_hit"),
            "missed": s.get("missed_before_entry"),
            "expired": s.get("expired"),
            "invalid": s.get("invalid"),
            "pending": s.get("pending_or_active"),
            "closed_tp_sl": s.get("closed_tp_sl"),
            "winrate": s.get("winrate_tp_sl"),
            "avg_result_R": s.get("avg_result_R"),
            "avg_rr": s.get("avg_rr"),
            "avg_practical_rr": s.get("avg_practical_rr"),
        }
        return metric, "daily_summary_latest_fallback"

    return _metric_from_records([]), "no_stats_available"


def _section_header_for_type(report_type: str) -> str:
    r = report_type.lower().strip()
    if r in {"morning", "morning_briefing"}:
        return "🌅 Morning Market Briefing"
    if r in {"london_1h", "london"}:
        return "🇬🇧 London +1h Market Report"
    if r in {"ny_1h", "ny", "new_york"}:
        return "🇺🇸 New York +1h Market Report"
    if r in {"holiday_warning", "pre_market"}:
        return "🗓 Market Holiday / Risk Warning"
    return "📡 Market Intelligence Report"


def _build_market_status_section(tpo: dict[str, Any]) -> BriefingSection:
    symbols = tpo.get("symbols") if isinstance(tpo, dict) else {}
    symbols = symbols if isinstance(symbols, dict) else {}
    section = BriefingSection("🗓 Markets / holidays / provider state")

    if not symbols:
        section.lines.append("No TPO store symbols available.")
        return section

    closed_holidays: list[str] = []
    closed_regular: list[str] = []
    stale_or_degraded: list[str] = []
    open_symbols: list[str] = []

    for sym, item in sorted(symbols.items()):
        if not isinstance(item, dict):
            continue
        ctx = item.get("context") if isinstance(item.get("context"), dict) else {}
        filters = item.get("filters") if isinstance(item.get("filters"), dict) else {}
        market_status = str(ctx.get("market_status") or "-")
        reason = ctx.get("market_closed_reason")
        holiday = ctx.get("market_holiday_name")
        permission = filters.get("tpo_signal_permission")
        modifier = filters.get("telegram_modifier")
        fallback = item.get("fallback_preserved_previous_context")
        provider_error = ctx.get("provider_error")

        text = f"{sym}: {market_status} / permission={permission or '-'} / modifier={modifier or '-'}"
        if reason:
            text += f" / reason={reason}"
        if holiday:
            text += f" / holiday={holiday}"
        if fallback or provider_error:
            text += " / provider_fallback"

        if reason == "US_HOLIDAY" or holiday:
            closed_holidays.append(text)
        elif market_status.startswith("MARKET_CLOSED"):
            closed_regular.append(text)
        elif market_status == "STALE_DATA" or fallback or provider_error:
            stale_or_degraded.append(text)
        elif market_status == "OPEN":
            open_symbols.append(text)

    if closed_holidays:
        section.lines.append("Holiday closed:")
        section.lines.extend([f"• {x}" for x in closed_holidays[:10]])
    if closed_regular:
        section.lines.append("Session closed:")
        section.lines.extend([f"• {x}" for x in closed_regular[:10]])
    if stale_or_degraded:
        section.lines.append("Stale / degraded:")
        section.lines.extend([f"• {x}" for x in stale_or_degraded[:10]])
    if open_symbols:
        section.lines.append("Open for evaluation:")
        section.lines.extend([f"• {x}" for x in open_symbols[:10]])
    if not section.lines:
        section.lines.append("No market status issues detected.")

    return section


def _build_high_impact_section(target_date: date, timezone_name: str) -> BriefingSection:
    events = load_high_impact_events(target_date)
    section = BriefingSection("🔴 High-impact news today")

    if not events:
        section.lines.append("No HIGH/RED events configured for today.")
        section.lines.append("Calendar source: runtime/calendar/high_impact_events.json or built-in fallback.")
        return section

    for e in events[:12]:
        local_time = _local_dt_from_event(e, timezone_name)
        currency = e.get("currency") or "-"
        impact = str(e.get("impact") or "HIGH").upper()
        title = e.get("title") or "Unnamed event"
        symbols = e.get("symbols")
        symbols_text = ", ".join(symbols) if isinstance(symbols, list) else str(symbols or "-")
        note = e.get("note")
        line = f"• {local_time} — {currency} {impact}: {title}"
        if symbols_text and symbols_text != "-":
            line += f" | watch: {symbols_text}"
        section.lines.append(line)
        if note:
            section.lines.append(f"  Note: {note}")
    return section


def _build_yesterday_section(target_date: date, timezone_name: str) -> BriefingSection:
    metric, source = _yesterday_metric(timezone_name, target_date)
    yday = (target_date - timedelta(days=1)).isoformat()
    section = BriefingSection(f"📊 Yesterday performance — {yday}")

    total = metric.get("total")
    if not total:
        section.lines.append(f"No yesterday-specific closed records found. source={source}")
        return section

    section.lines.extend(
        [
            f"Signals: {total}",
            f"TP/SL/Missed/Expired/Pending: {metric.get('tp', 0)} / {metric.get('sl', 0)} / {metric.get('missed', 0)} / {metric.get('expired', 0)} / {metric.get('pending', 0)}",
            f"Winrate TP/SL: {_fmt_pct(metric.get('winrate'))}",
            f"Avg result: {_fmt_num(metric.get('avg_result_R'), 4)}R",
            f"Avg RR / practical RR: {_fmt_num(metric.get('avg_rr'), 2)} / {_fmt_num(metric.get('avg_practical_rr'), 2)}",
            f"Source: {source}",
        ]
    )
    return section


def _build_statistics_section() -> BriefingSection:
    summary = load_daily_summary()
    section = BriefingSection("🛡 Battle / production statistics")
    if not isinstance(summary, dict) or not summary:
        section.lines.append("No daily_summary.json available.")
        return section

    s = summary.get("summary") if isinstance(summary.get("summary"), dict) else {}
    battle_metrics = summary.get("battle_metrics") if isinstance(summary.get("battle_metrics"), dict) else {}

    section.lines.extend(
        [
            f"Exporter: {summary.get('exporter_version', '-')}",
            f"Production records: {s.get('production_records', s.get('total_signals', '-'))}",
            f"Synthetic excluded: {s.get('synthetic_test_records', s.get('excluded_from_metrics', '-'))}",
            f"TP/SL/Missed: {s.get('tp_hit', '-')} / {s.get('sl_hit', '-')} / {s.get('missed_before_entry', '-')}",
            f"Winrate TP/SL: {_fmt_pct(s.get('winrate_tp_sl'))}",
            f"Avg result: {_fmt_num(s.get('avg_result_R'), 4)}R",
            f"Avg RR / practical RR: {_fmt_num(s.get('avg_rr'), 2)} / {_fmt_num(s.get('avg_practical_rr'), 2)}",
        ]
    )

    by_permission = battle_metrics.get("by_battle_permission")
    if isinstance(by_permission, dict) and by_permission:
        section.lines.append("Battle permission groups:")
        for key, value in list(by_permission.items())[:8]:
            if not isinstance(value, dict):
                continue
            section.lines.append(
                f"• {key}: signals={value.get('total_signals', '-')} WR={_fmt_pct(value.get('winrate_tp_sl'))} avgR={_fmt_num(value.get('avg_result_R'), 4)}"
            )

    by_scope_all = battle_metrics.get("by_tracking_scope_all_records")
    if isinstance(by_scope_all, dict) and by_scope_all:
        section.lines.append("Tracking scope all records:")
        for key, value in list(by_scope_all.items())[:8]:
            if not isinstance(value, dict):
                continue
            section.lines.append(f"• {key}: signals={value.get('total_signals', '-')}, outcomes={value.get('by_outcome_status', {})}")
    return section


def _build_tpo_snapshot_section(tpo: dict[str, Any], report_type: str) -> BriefingSection:
    section = BriefingSection("🧠 TPO / auction snapshot")
    symbols = tpo.get("symbols") if isinstance(tpo, dict) else {}
    symbols = symbols if isinstance(symbols, dict) else {}

    if not symbols:
        section.lines.append("No TPO symbols available.")
        return section

    rt = report_type.lower()
    if rt in {"ny", "ny_1h", "new_york"}:
        watch = ["NAS100", "SPX500", "UKOIL", "XAUUSD", "EURUSD", "GBPUSD", "BTCUSD", "ETHUSD"]
    elif rt in {"london", "london_1h"}:
        watch = ["GER40", "UKOIL", "XAUUSD", "EURUSD", "GBPUSD", "USDCHF", "BTCUSD", "ETHUSD"]
    else:
        watch = ["GER40", "NAS100", "SPX500", "UKOIL", "XAUUSD", "EURUSD", "GBPUSD", "BTCUSD", "ETHUSD"]

    for sym in watch:
        item = symbols.get(sym)
        if not isinstance(item, dict):
            section.lines.append(f"• {sym}: MISSING")
            continue
        ctx = item.get("context") if isinstance(item.get("context"), dict) else {}
        filters = item.get("filters") if isinstance(item.get("filters"), dict) else {}
        line = (
            f"• {sym}: {ctx.get('open_relation', '-')}/{ctx.get('auction_bias', '-')}; "
            f"market={ctx.get('market_status', '-')}; "
            f"permission={filters.get('tpo_signal_permission', '-')}; "
            f"modifier={filters.get('telegram_modifier', '-')}"
        )
        reason = ctx.get("market_closed_reason")
        holiday = ctx.get("market_holiday_name")
        if reason:
            line += f"; reason={reason}"
        if holiday:
            line += f"; holiday={holiday}"
        section.lines.append(line)
    return section


def _build_provider_section(tpo: dict[str, Any]) -> BriefingSection:
    section = BriefingSection("🧯 Provider / data issues")
    errors = tpo.get("errors") if isinstance(tpo, dict) else []
    fallbacks = tpo.get("fallbacks") if isinstance(tpo, dict) else []
    if not isinstance(errors, list):
        errors = []
    if not isinstance(fallbacks, list):
        fallbacks = []

    symbols = tpo.get("symbols") if isinstance(tpo, dict) and isinstance(tpo.get("symbols"), dict) else {}
    section.lines.append(f"TPO symbols: {len(symbols)}")
    section.lines.append(f"Errors: {len(errors)}")
    section.lines.append(f"Fallbacks: {len(fallbacks)}")

    for err in errors[:5]:
        if isinstance(err, dict):
            section.lines.append(f"• error {err.get('symbol', '-')}: {err.get('error_type', '-')}: {err.get('error', '-')}")
    for fb in fallbacks[:5]:
        if isinstance(fb, dict):
            section.lines.append(f"• fallback {fb.get('symbol', '-')}: {fb.get('reason', fb.get('error_type', '-'))}")
    if len(section.lines) == 3 and not errors and not fallbacks:
        section.lines.append("No provider issues in latest TPO export.")
    return section


def _build_focus_section(report_type: str) -> BriefingSection:
    section = BriefingSection("🎯 Operating focus")
    rt = report_type.lower().strip()
    if rt in {"morning", "morning_briefing", "holiday_warning", "pre_market"}:
        section.lines.extend(
            [
                "Start with permission, not prediction.",
                "Do not trade MARKET_CLOSED / STALE_DATA / DOWNGRADE as battle alerts.",
                "Prefer trend-aligned executable setups; counter-trend remains research unless extremely clean.",
            ]
        )
    elif rt in {"london", "london_1h"}:
        section.lines.extend(
            [
                "London +1h: check FX/XAU/GER40 acceptance and IB behavior.",
                "Inside VA = research/downgrade; OUT_OF_RANGE only matters if market is open and HTF-aligned.",
            ]
        )
    elif rt in {"ny", "ny_1h", "new_york"}:
        section.lines.extend(
            [
                "NY +1h: do not chase first impulse; require auction acceptance and Battle Gate permission.",
                "High-impact news windows can invalidate early structure. Wait for post-news confirmation.",
            ]
        )
    else:
        section.lines.append("Use Battle Gate output as final Telegram permission.")
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

    report = BriefingReport(
        report_type=report_type,
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

    report.sections.append(_build_market_status_section(tpo))
    report.sections.append(_build_high_impact_section(target_date, tz_name))

    if report_type.lower() in {"morning", "morning_briefing", "holiday_warning", "pre_market"}:
        report.sections.append(_build_yesterday_section(target_date, tz_name))

    report.sections.append(_build_statistics_section())
    report.sections.append(_build_tpo_snapshot_section(tpo, report_type))
    report.sections.append(_build_provider_section(tpo))
    report.sections.append(_build_focus_section(report_type))
    return report


def render_briefing_text(report: BriefingReport) -> str:
    header = _section_header_for_type(report.report_type)
    lines = [
        f"<b>{_esc(header)} — {_esc(report.report_date)}</b>",
        f"Generated: {_esc(report.generated_at_local)}",
        f"Version: {_esc(report.version)}",
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