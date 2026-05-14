from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.core.settings import settings


# =============================================================================
# DAILY SIGNAL REPORT v1.1
# =============================================================================
# Purpose:
# - Read signal_outcomes.json produced by app.services.signal_outcome_tracker.
# - Build lightweight operational report for console/dashboard.
# - Build short Telegram-ready daily report.
# - Optionally read quality_tiers.json for weak-category diagnostics.
# - No pandas.
# - No external API calls.
# - Safe to run manually:
#
#   python -m app.services.daily_signal_report
#
# Telegram mode:
#
#   python -m app.services.daily_signal_report --telegram
#   python -m app.services.daily_signal_report --telegram --date today
#   python -m app.services.daily_signal_report --telegram --date 2026-05-14
#
# JSON mode:
#
#   python -m app.services.daily_signal_report --json
#
# Notes:
# - Default mode reports ALL tracked Telegram alerts from signal_outcomes.json.
# - Use --date YYYY-MM-DD to filter by sent_at_utc converted to selected timezone.
# - --telegram renders a compact Ukrainian human-facing report.
# - Technical enum values remain English: TP_HIT, SL_HIT, CAUTION, TIGHT_STOP, etc.
# =============================================================================


SIGNAL_OUTCOMES_PATH = settings.runtime_dir / "stats" / "signal_outcomes.json"
QUALITY_TIERS_PATH = settings.runtime_dir / "stats" / "quality_tiers.json"
REPORTS_DIR = settings.runtime_dir / "stats" / "reports"

DEFAULT_TIMEZONE = os.getenv("DAILY_SIGNAL_REPORT_TZ", "Europe/Kyiv")

TP_STATUS = "TP_HIT"
SL_STATUS = "SL_HIT"

MISSED_STATUSES = {
    "MISSED_TARGET_BEFORE_ENTRY",
}

PENDING_STATUSES = {
    "",
    "PENDING_ENTRY",
    "ENTRY_TRIGGERED",
    "ACTIVE",
}

EXPIRED_STATUSES = {
    "EXPIRED",
    "EXPIRED_AFTER_ENTRY",
}

FINAL_STATUSES = {
    "TP_HIT",
    "SL_HIT",
    "EXPIRED",
    "EXPIRED_AFTER_ENTRY",
    "MISSED_TARGET_BEFORE_ENTRY",
    "INVALID",
}

QUALITY_TIER_SEVERITY = {
    "NO_DATA": 0,
    "A-GRADE": 1,
    "INSUFFICIENT_SAMPLE": 2,
    "OBSERVE": 3,
    "CAUTION": 4,
    "LOW_PRIORITY": 5,
}

QUALITY_TIER_MARKERS = {
    "A-GRADE": "🟢",
    "CAUTION": "🟠",
    "OBSERVE": "🔵",
    "LOW_PRIORITY": "🔴",
    "INSUFFICIENT_SAMPLE": "⚪",
    "NO_DATA": "⚫",
}


# =============================================================================
# BASIC HELPERS
# =============================================================================


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def parse_utc(value: Any) -> datetime | None:
    if value is None:
        return None

    try:
        text = str(value).strip()
        if not text:
            return None

        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))

        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)

        return dt
    except Exception:
        return None


def safe_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_text(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").strip()
    return text if text else default


def normalize_status(value: Any) -> str:
    return str(value or "PENDING_ENTRY").strip().upper()


def get_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def local_date_from_utc(value: Any, tz: ZoneInfo) -> date | None:
    dt = parse_utc(value)
    if dt is None:
        return None
    return dt.astimezone(tz).date()


def today_in_timezone(tz: ZoneInfo) -> date:
    return datetime.now(timezone.utc).astimezone(tz).date()


def format_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def format_r(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}R"


def format_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def quality_marker(tier: Any) -> str:
    tier_text = normalize_text(tier, "NO_DATA")
    return QUALITY_TIER_MARKERS.get(tier_text, "⚫")


def tier_severity(tier: Any) -> int:
    tier_text = normalize_text(tier, "NO_DATA")
    return QUALITY_TIER_SEVERITY.get(tier_text, 0)


# =============================================================================
# LOADING / FILTERING
# =============================================================================


def load_signal_outcomes(path: Path = SIGNAL_OUTCOMES_PATH) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": "missing",
            "updated_at_utc": utc_now(),
            "summary": {},
            "signals": [],
            "error": f"File does not exist: {path}",
        }

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {
            "schema_version": "invalid_json",
            "updated_at_utc": utc_now(),
            "summary": {},
            "signals": [],
            "error": f"Invalid JSON in {path}: {exc}",
        }

    if isinstance(data, list):
        return {
            "schema_version": "legacy_list",
            "updated_at_utc": utc_now(),
            "summary": {},
            "signals": [x for x in data if isinstance(x, dict)],
        }

    if not isinstance(data, dict):
        return {
            "schema_version": "invalid_payload",
            "updated_at_utc": utc_now(),
            "summary": {},
            "signals": [],
            "error": f"Unsupported payload type: {type(data).__name__}",
        }

    signals = data.get("signals")
    if not isinstance(signals, list):
        signals = []

    data["signals"] = [x for x in signals if isinstance(x, dict)]
    data.setdefault("summary", {})
    data.setdefault("updated_at_utc", utc_now())

    return data


def load_quality_tiers(path: Path = QUALITY_TIERS_PATH) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": "missing",
            "generated_at_utc": utc_now(),
            "dimensions": {},
            "signal_annotations": [],
            "error": f"File does not exist: {path}",
        }

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {
            "schema_version": "invalid_json",
            "generated_at_utc": utc_now(),
            "dimensions": {},
            "signal_annotations": [],
            "error": f"Invalid JSON in {path}: {exc}",
        }

    if not isinstance(data, dict):
        return {
            "schema_version": "invalid_payload",
            "generated_at_utc": utc_now(),
            "dimensions": {},
            "signal_annotations": [],
            "error": f"Unsupported payload type: {type(data).__name__}",
        }

    if not isinstance(data.get("dimensions"), dict):
        data["dimensions"] = {}

    if not isinstance(data.get("signal_annotations"), list):
        data["signal_annotations"] = []

    return data


def filter_signals_by_date(
    signals: list[dict[str, Any]],
    *,
    report_date: date | None,
    tz: ZoneInfo,
) -> list[dict[str, Any]]:
    if report_date is None:
        return signals

    filtered: list[dict[str, Any]] = []

    for signal in signals:
        sent_date = local_date_from_utc(signal.get("sent_at_utc"), tz)

        if sent_date == report_date:
            filtered.append(signal)

    return filtered


# =============================================================================
# METRICS
# =============================================================================


def count_by(signals: list[dict[str, Any]], key: str) -> dict[str, int]:
    result: dict[str, int] = {}

    for signal in signals:
        value = normalize_text(signal.get(key))
        result[value] = result.get(value, 0) + 1

    return dict(sorted(result.items(), key=lambda x: x[0]))


def status_counts(signals: list[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}

    for signal in signals:
        status = normalize_status(signal.get("outcome_status"))
        result[status] = result.get(status, 0) + 1

    return dict(sorted(result.items(), key=lambda x: x[0]))


def calc_winrate(signals: list[dict[str, Any]]) -> float | None:
    closed = [
        x for x in signals
        if normalize_status(x.get("outcome_status")) in {TP_STATUS, SL_STATUS}
    ]

    if not closed:
        return None

    wins = sum(1 for x in closed if normalize_status(x.get("outcome_status")) == TP_STATUS)
    return round(wins / len(closed), 4)


def calc_avg_result_r(signals: list[dict[str, Any]]) -> float | None:
    values: list[float] = []

    for signal in signals:
        result_r = safe_float(signal.get("result_R"))
        if result_r is not None:
            values.append(result_r)

    if not values:
        return None

    return round(sum(values) / len(values), 4)


def calc_avg_field(signals: list[dict[str, Any]], key: str) -> float | None:
    values: list[float] = []

    for signal in signals:
        value = safe_float(signal.get(key))
        if value is not None:
            values.append(value)

    if not values:
        return None

    return round(sum(values) / len(values), 4)


def count_status_group(signals: list[dict[str, Any]], statuses: set[str]) -> int:
    return sum(
        1 for x in signals
        if normalize_status(x.get("outcome_status")) in statuses
    )


def build_group_metrics(signals: list[dict[str, Any]], group_key: str) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = {}

    for signal in signals:
        key = normalize_text(signal.get(group_key))
        groups.setdefault(key, []).append(signal)

    result: dict[str, Any] = {}

    for key, items in sorted(groups.items(), key=lambda x: x[0]):
        result[key] = build_basic_metrics(items)

    return result


def build_basic_metrics(signals: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(signals)

    tp = count_status_group(signals, {TP_STATUS})
    sl = count_status_group(signals, {SL_STATUS})
    missed = count_status_group(signals, MISSED_STATUSES)
    expired = count_status_group(signals, EXPIRED_STATUSES)
    pending = count_status_group(signals, PENDING_STATUSES)
    invalid = count_status_group(signals, {"INVALID"})

    return {
        "total_alerts": total,
        "tp_hit": tp,
        "sl_hit": sl,
        "missed_target_before_entry": missed,
        "expired": expired,
        "pending_or_active": pending,
        "invalid": invalid,
        "winrate": calc_winrate(signals),
        "avg_result_R": calc_avg_result_r(signals),
        "avg_rr": calc_avg_field(signals, "risk_reward_ratio"),
        "avg_practical_rr": calc_avg_field(signals, "practical_rr"),
        "outcome_status": status_counts(signals),
    }


def build_warnings(report: dict[str, Any]) -> list[str]:
    warnings: list[str] = []

    metrics = report["metrics"]
    total = metrics["total_alerts"]
    tp = metrics["tp_hit"]
    sl = metrics["sl_hit"]
    pending = metrics["pending_or_active"]

    if total == 0:
        warnings.append("NO_SIGNALS: no signals in selected scope.")
        return warnings

    if total < 30:
        warnings.append(
            "LOW_SAMPLE_SIZE: sample is still small; use conclusions as early diagnostics, not final edge statistics."
        )

    if tp == 0 and sl > 0:
        warnings.append(
            "NO_WINNERS_YET: TP_HIT is 0 while SL_HIT exists. Do not optimize aggressively until sample grows."
        )

    if pending > 0:
        warnings.append(
            f"PENDING_ALERTS: {pending} signal(s) are still pending/active."
        )

    by_stop_quality = report.get("by_stop_quality", {})
    tight_stop = by_stop_quality.get("TIGHT_STOP")
    if isinstance(tight_stop, dict):
        tight_count = int(tight_stop.get("total_alerts") or 0)
        tight_avg_r = safe_float(tight_stop.get("avg_result_R"))
        tight_sl = int(tight_stop.get("sl_hit") or 0)

        if tight_count >= 3 and tight_avg_r is not None and tight_avg_r < 0:
            warnings.append(
                f"TIGHT_STOP_WEAKNESS: count={tight_count}, SL={tight_sl}, avg_result_R={tight_avg_r}. Candidate for CAUTION tier."
            )

    by_alignment = report.get("by_signal_alignment", {})
    neutral_htf = by_alignment.get("NEUTRAL_HTF")
    if isinstance(neutral_htf, dict):
        neutral_count = int(neutral_htf.get("total_alerts") or 0)
        neutral_avg_r = safe_float(neutral_htf.get("avg_result_R"))
        neutral_sl = int(neutral_htf.get("sl_hit") or 0)

        if neutral_count >= 3 and neutral_avg_r is not None and neutral_avg_r < 0:
            warnings.append(
                f"NEUTRAL_HTF_WEAKNESS: count={neutral_count}, SL={neutral_sl}, avg_result_R={neutral_avg_r}. Candidate for OBSERVE/CAUTION tier."
            )

    return warnings


# =============================================================================
# QUALITY TIER SUMMARY
# =============================================================================


def extract_quality_dimensions(quality_payload: dict[str, Any]) -> dict[str, Any]:
    dimensions = quality_payload.get("dimensions")
    if isinstance(dimensions, dict):
        return dimensions
    return {}


def extract_weak_quality_categories(
    quality_payload: dict[str, Any],
    *,
    preferred_dimensions: tuple[str, ...] = (
        "stop_quality",
        "signal_alignment",
        "scenario",
        "execution_model",
        "symbol",
    ),
    weak_tiers: set[str] | None = None,
    max_items: int = 8,
) -> list[dict[str, Any]]:
    if weak_tiers is None:
        weak_tiers = {"LOW_PRIORITY", "CAUTION", "OBSERVE"}

    dimensions = extract_quality_dimensions(quality_payload)
    categories: list[dict[str, Any]] = []

    for dimension_name in preferred_dimensions:
        dimension_data = dimensions.get(dimension_name)

        if not isinstance(dimension_data, dict):
            continue

        for key, item in dimension_data.items():
            if not isinstance(item, dict):
                continue

            quality = item.get("quality")
            metrics = item.get("metrics")

            if not isinstance(quality, dict) or not isinstance(metrics, dict):
                continue

            tier = normalize_text(quality.get("tier"), "NO_DATA")
            if tier not in weak_tiers:
                continue

            categories.append(
                {
                    "dimension": dimension_name,
                    "key": str(key),
                    "tier": tier,
                    "confidence": normalize_text(quality.get("confidence")),
                    "action": normalize_text(quality.get("action")),
                    "total_alerts": int(metrics.get("total_alerts") or 0),
                    "tp_hit": int(metrics.get("tp_hit") or 0),
                    "sl_hit": int(metrics.get("sl_hit") or 0),
                    "missed_target_before_entry": int(metrics.get("missed_target_before_entry") or 0),
                    "pending_or_active": int(metrics.get("pending_or_active") or 0),
                    "winrate": safe_float(metrics.get("winrate")),
                    "avg_result_R": safe_float(metrics.get("avg_result_R")),
                }
            )

    categories.sort(
        key=lambda x: (
            -tier_severity(x.get("tier")),
            -int(x.get("total_alerts") or 0),
            str(x.get("dimension") or ""),
            str(x.get("key") or ""),
        )
    )

    return categories[:max_items]


def extract_quality_tier_counts(quality_payload: dict[str, Any]) -> dict[str, int]:
    tier_counts = quality_payload.get("tier_counts")
    if isinstance(tier_counts, dict):
        return {
            str(k): int(v)
            for k, v in tier_counts.items()
            if isinstance(v, int) or str(v).isdigit()
        }

    dimensions = extract_quality_dimensions(quality_payload)
    result: dict[str, int] = {}

    for dimension_data in dimensions.values():
        if not isinstance(dimension_data, dict):
            continue

        for item in dimension_data.values():
            if not isinstance(item, dict):
                continue

            quality = item.get("quality")
            if not isinstance(quality, dict):
                continue

            tier = normalize_text(quality.get("tier"), "NO_DATA")
            result[tier] = result.get(tier, 0) + 1

    return dict(sorted(result.items(), key=lambda x: x[0]))


# =============================================================================
# REPORT BUILDER
# =============================================================================


def build_report(
    *,
    payload: dict[str, Any],
    quality_payload: dict[str, Any] | None,
    report_date: date | None,
    tz: ZoneInfo,
    source_path: Path,
    quality_path: Path,
) -> dict[str, Any]:
    all_signals = payload.get("signals")
    if not isinstance(all_signals, list):
        all_signals = []

    signals = filter_signals_by_date(
        all_signals,
        report_date=report_date,
        tz=tz,
    )

    scope = "all_tracked_alerts" if report_date is None else f"sent_date:{report_date.isoformat()}"

    if quality_payload is None:
        quality_payload = {
            "dimensions": {},
            "signal_annotations": [],
            "error": "quality payload not loaded",
        }

    report: dict[str, Any] = {
        "schema_version": "1.1",
        "generated_at_utc": utc_now(),
        "timezone": str(tz),
        "scope": scope,
        "source_path": str(source_path),
        "quality_path": str(quality_path),
        "source_updated_at_utc": payload.get("updated_at_utc"),
        "quality_generated_at_utc": quality_payload.get("generated_at_utc"),
        "source_error": payload.get("error"),
        "quality_error": quality_payload.get("error"),
        "metrics": build_basic_metrics(signals),
        "by_symbol": build_group_metrics(signals, "symbol"),
        "by_scenario": build_group_metrics(signals, "scenario"),
        "by_direction": build_group_metrics(signals, "direction"),
        "by_signal_alignment": build_group_metrics(signals, "signal_alignment"),
        "by_stop_quality": build_group_metrics(signals, "stop_quality"),
        "by_execution_model": build_group_metrics(signals, "execution_model"),
        "quality_tier_counts": extract_quality_tier_counts(quality_payload),
        "weak_quality_categories": extract_weak_quality_categories(quality_payload),
    }

    report["warnings"] = build_warnings(report)

    return report


# =============================================================================
# FULL TEXT RENDERING
# =============================================================================


def render_group_block(title: str, groups: dict[str, Any], *, max_items: int = 20) -> list[str]:
    lines: list[str] = []
    lines.append("")
    lines.append(title)

    if not groups:
        lines.append("  - n/a")
        return lines

    sorted_items = sorted(
        groups.items(),
        key=lambda item: (
            -int(item[1].get("total_alerts") or 0),
            item[0],
        ),
    )

    for key, metrics in sorted_items[:max_items]:
        total = int(metrics.get("total_alerts") or 0)
        tp = int(metrics.get("tp_hit") or 0)
        sl = int(metrics.get("sl_hit") or 0)
        missed = int(metrics.get("missed_target_before_entry") or 0)
        pending = int(metrics.get("pending_or_active") or 0)
        avg_r = safe_float(metrics.get("avg_result_R"))
        winrate = safe_float(metrics.get("winrate"))

        lines.append(
            f"  - {key}: total={total}, TP={tp}, SL={sl}, missed={missed}, "
            f"pending={pending}, winrate={format_pct(winrate)}, avgR={format_r(avg_r)}"
        )

    if len(sorted_items) > max_items:
        lines.append(f"  ... {len(sorted_items) - max_items} more")

    return lines


def render_text_report(report: dict[str, Any]) -> str:
    metrics = report["metrics"]

    total = int(metrics.get("total_alerts") or 0)
    tp = int(metrics.get("tp_hit") or 0)
    sl = int(metrics.get("sl_hit") or 0)
    missed = int(metrics.get("missed_target_before_entry") or 0)
    expired = int(metrics.get("expired") or 0)
    pending = int(metrics.get("pending_or_active") or 0)
    invalid = int(metrics.get("invalid") or 0)

    winrate = safe_float(metrics.get("winrate"))
    avg_result_r = safe_float(metrics.get("avg_result_R"))
    avg_rr = safe_float(metrics.get("avg_rr"))
    avg_practical_rr = safe_float(metrics.get("avg_practical_rr"))

    lines: list[str] = []

    lines.append("📊 Daily Signal Report v1.1")
    lines.append("=" * 72)
    lines.append(f"Generated UTC: {report.get('generated_at_utc')}")
    lines.append(f"Timezone:      {report.get('timezone')}")
    lines.append(f"Scope:         {report.get('scope')}")
    lines.append(f"Source:        {report.get('source_path')}")
    lines.append(f"Quality:       {report.get('quality_path')}")
    lines.append(f"Source update: {report.get('source_updated_at_utc')}")
    lines.append(f"Quality gen:   {report.get('quality_generated_at_utc')}")
    lines.append("")

    if report.get("source_error"):
        lines.append(f"⚠️ Source error: {report.get('source_error')}")
        lines.append("")

    if report.get("quality_error"):
        lines.append(f"⚠️ Quality error: {report.get('quality_error')}")
        lines.append("")

    lines.append("Summary")
    lines.append("-" * 72)
    lines.append(f"Total alerts:          {total}")
    lines.append(f"TP / SL:               {tp} / {sl}")
    lines.append(f"Missed before entry:   {missed}")
    lines.append(f"Expired:               {expired}")
    lines.append(f"Pending / active:      {pending}")
    lines.append(f"Invalid:               {invalid}")
    lines.append(f"Winrate TP/SL only:    {format_pct(winrate)}")
    lines.append(f"Average result:        {format_r(avg_result_r)}")
    lines.append(f"Average RR:            {format_float(avg_rr)}")
    lines.append(f"Average practical RR:  {format_float(avg_practical_rr)}")

    lines.extend(render_group_block("By symbol", report.get("by_symbol", {})))
    lines.extend(render_group_block("By scenario", report.get("by_scenario", {})))
    lines.extend(render_group_block("By signal alignment", report.get("by_signal_alignment", {})))
    lines.extend(render_group_block("By stop quality", report.get("by_stop_quality", {})))
    lines.extend(render_group_block("By execution model", report.get("by_execution_model", {})))

    tier_counts = report.get("quality_tier_counts") or {}
    lines.append("")
    lines.append("Quality tier counts")
    lines.append("-" * 72)
    if tier_counts:
        for tier, count in sorted(tier_counts.items(), key=lambda x: x[0]):
            lines.append(f"  - {tier}: {count}")
    else:
        lines.append("  - n/a")

    weak_categories = report.get("weak_quality_categories") or []
    lines.append("")
    lines.append("Weak quality categories")
    lines.append("-" * 72)
    if weak_categories:
        for item in weak_categories:
            tier = normalize_text(item.get("tier"), "NO_DATA")
            dimension = normalize_text(item.get("dimension"))
            key = normalize_text(item.get("key"))
            total_cat = int(item.get("total_alerts") or 0)
            sl_cat = int(item.get("sl_hit") or 0)
            missed_cat = int(item.get("missed_target_before_entry") or 0)
            avg_cat = safe_float(item.get("avg_result_R"))
            lines.append(
                f"  - {dimension}={key}: tier={tier}, total={total_cat}, "
                f"SL={sl_cat}, missed={missed_cat}, avgR={format_r(avg_cat)}"
            )
    else:
        lines.append("  - n/a")

    warnings = report.get("warnings") or []
    lines.append("")
    lines.append("Warnings / diagnostics")
    lines.append("-" * 72)

    if not warnings:
        lines.append("  - No warnings.")
    else:
        for warning in warnings:
            lines.append(f"  - {warning}")

    lines.append("")
    lines.append("Operator note")
    lines.append("-" * 72)
    lines.append(
        "Do not delete weak categories yet. Keep collecting statistics, then route Telegram output through quality tiers."
    )

    return "\n".join(lines)


# =============================================================================
# TELEGRAM TEXT RENDERING
# =============================================================================


def render_telegram_scope(report: dict[str, Any]) -> str:
    scope = normalize_text(report.get("scope"), "all_tracked_alerts")

    if scope == "all_tracked_alerts":
        return "усі відстежені сигнали"

    if scope.startswith("sent_date:"):
        return scope.replace("sent_date:", "дата ")

    return scope


def render_telegram_warning(warning: str) -> str:
    if warning.startswith("LOW_SAMPLE_SIZE"):
        return "мала вибірка — висновки тільки ранні"
    if warning.startswith("NO_WINNERS_YET"):
        return "поки немає TP_HIT при наявних SL_HIT"
    if warning.startswith("PENDING_ALERTS"):
        return "є активні/незавершені сигнали"
    if warning.startswith("TIGHT_STOP_WEAKNESS"):
        return "TIGHT_STOP показує слабкість"
    if warning.startswith("NEUTRAL_HTF_WEAKNESS"):
        return "NEUTRAL_HTF показує слабкість"
    if warning.startswith("NO_SIGNALS"):
        return "немає сигналів у вибраному періоді"

    return warning


def render_telegram_weak_categories(
    categories: list[dict[str, Any]],
    *,
    max_items: int = 5,
) -> list[str]:
    lines: list[str] = []

    if not categories:
        lines.append("- немає слабких категорій у поточному зрізі")
        return lines

    for item in categories[:max_items]:
        tier = normalize_text(item.get("tier"), "NO_DATA")
        marker = quality_marker(tier)
        dimension = normalize_text(item.get("dimension"))
        key = normalize_text(item.get("key"))
        total = int(item.get("total_alerts") or 0)
        sl = int(item.get("sl_hit") or 0)
        missed = int(item.get("missed_target_before_entry") or 0)
        avg_r = safe_float(item.get("avg_result_R"))

        lines.append(
            f"- {marker} {dimension}: {key} → {tier} "
            f"(n={total}, SL={sl}, missed={missed}, avg={format_r(avg_r)})"
        )

    if len(categories) > max_items:
        lines.append(f"- ...ще {len(categories) - max_items} категорій")

    return lines


def render_telegram_report(report: dict[str, Any]) -> str:
    metrics = report["metrics"]

    total = int(metrics.get("total_alerts") or 0)
    tp = int(metrics.get("tp_hit") or 0)
    sl = int(metrics.get("sl_hit") or 0)
    missed = int(metrics.get("missed_target_before_entry") or 0)
    expired = int(metrics.get("expired") or 0)
    pending = int(metrics.get("pending_or_active") or 0)
    invalid = int(metrics.get("invalid") or 0)

    winrate = safe_float(metrics.get("winrate"))
    avg_result_r = safe_float(metrics.get("avg_result_R"))
    avg_rr = safe_float(metrics.get("avg_rr"))
    avg_practical_rr = safe_float(metrics.get("avg_practical_rr"))

    tier_counts = report.get("quality_tier_counts") or {}
    weak_categories = report.get("weak_quality_categories") or {}
    warnings = report.get("warnings") or []

    lines: list[str] = []

    lines.append("📊 Денний звіт сигналів")
    lines.append("")
    lines.append(f"Період: {render_telegram_scope(report)}")
    lines.append(f"Оновлено: {report.get('generated_at_utc')}")
    lines.append("")
    lines.append("Підсумок:")
    lines.append(f"- Усього сигналів: {total}")
    lines.append(f"- TP / SL: {tp} / {sl}")
    lines.append(f"- Missed before entry: {missed}")
    lines.append(f"- Expired: {expired}")
    lines.append(f"- Pending / active: {pending}")
    lines.append(f"- Invalid: {invalid}")
    lines.append(f"- Winrate TP/SL: {format_pct(winrate)}")
    lines.append(f"- Avg result: {format_r(avg_result_r)}")
    lines.append(f"- Avg RR: {format_float(avg_rr)}")
    lines.append(f"- Avg practical RR: {format_float(avg_practical_rr)}")

    if tier_counts:
        lines.append("")
        lines.append("Якість категорій:")
        for tier, count in sorted(
            tier_counts.items(),
            key=lambda item: (-tier_severity(item[0]), item[0]),
        ):
            lines.append(f"- {quality_marker(tier)} {tier}: {count}")

    lines.append("")
    lines.append("Слабкі категорії:")
    lines.extend(render_telegram_weak_categories(weak_categories))

    if warnings:
        lines.append("")
        lines.append("Діагностика:")
        for warning in warnings[:5]:
            lines.append(f"- {render_telegram_warning(str(warning))}")

    lines.append("")
    lines.append("Висновок:")
    if total == 0:
        lines.append("Сигналів у вибраному періоді немає. Система продовжує збір даних.")
    elif tp == 0 and sl > 0:
        lines.append(
            "Сигнали поки не блокуємо, але слабкі категорії маркуємо через quality tiers. "
            "Потрібна більша вибірка перед жорсткою фільтрацією."
        )
    elif pending > 0:
        lines.append(
            "Є активні сигнали. Остаточний висновок по дню краще робити після їх закриття."
        )
    else:
        lines.append(
            "Продовжуємо збір статистики. Quality tiers використовуємо як попередження, не як сокиру."
        )

    return "\n".join(lines)


# =============================================================================
# SAVING
# =============================================================================


def build_report_file_prefix(report: dict[str, Any], *, telegram: bool = False) -> str:
    scope = str(report.get("scope") or "all").replace(":", "_").replace("/", "_")
    generated = str(report.get("generated_at_utc") or utc_now())
    stamp = generated[:19].replace(":", "-")

    prefix = "daily_signal_report_telegram" if telegram else "daily_signal_report"
    return f"{prefix}_{scope}_{stamp}"


def save_report_files(
    *,
    report: dict[str, Any],
    text: str,
    out_dir: Path = REPORTS_DIR,
    telegram_text: str | None = None,
) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)

    prefix = build_report_file_prefix(report)

    json_path = out_dir / f"{prefix}.json"
    txt_path = out_dir / f"{prefix}.txt"

    tmp_json = json_path.with_suffix(".json.tmp")
    tmp_txt = txt_path.with_suffix(".txt.tmp")

    tmp_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp_txt.write_text(text, encoding="utf-8")

    tmp_json.replace(json_path)
    tmp_txt.replace(txt_path)

    latest_json = out_dir / "daily_signal_report_latest.json"
    latest_txt = out_dir / "daily_signal_report_latest.txt"

    tmp_latest_json = latest_json.with_suffix(".json.tmp")
    tmp_latest_txt = latest_txt.with_suffix(".txt.tmp")

    tmp_latest_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp_latest_txt.write_text(text, encoding="utf-8")

    tmp_latest_json.replace(latest_json)
    tmp_latest_txt.replace(latest_txt)

    paths = {
        "json_path": str(json_path),
        "txt_path": str(txt_path),
        "latest_json_path": str(latest_json),
        "latest_txt_path": str(latest_txt),
    }

    if telegram_text is not None:
        telegram_prefix = build_report_file_prefix(report, telegram=True)

        telegram_txt_path = out_dir / f"{telegram_prefix}.txt"
        telegram_latest_txt_path = out_dir / "daily_signal_report_telegram_latest.txt"

        tmp_telegram_txt = telegram_txt_path.with_suffix(".txt.tmp")
        tmp_telegram_latest_txt = telegram_latest_txt_path.with_suffix(".txt.tmp")

        tmp_telegram_txt.write_text(telegram_text, encoding="utf-8")
        tmp_telegram_latest_txt.write_text(telegram_text, encoding="utf-8")

        tmp_telegram_txt.replace(telegram_txt_path)
        tmp_telegram_latest_txt.replace(telegram_latest_txt_path)

        paths["telegram_txt_path"] = str(telegram_txt_path)
        paths["telegram_latest_txt_path"] = str(telegram_latest_txt_path)

    return paths


# =============================================================================
# CLI
# =============================================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Daily Signal Report from signal_outcomes.json."
    )

    parser.add_argument(
        "--source",
        type=str,
        default=str(SIGNAL_OUTCOMES_PATH),
        help="Path to signal_outcomes.json.",
    )

    parser.add_argument(
        "--quality-source",
        type=str,
        default=str(QUALITY_TIERS_PATH),
        help="Path to quality_tiers.json.",
    )

    parser.add_argument(
        "--out-dir",
        type=str,
        default=str(REPORTS_DIR),
        help="Directory for report files.",
    )

    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help=(
            "Optional local report date in YYYY-MM-DD. "
            "Use 'today' for current date in selected timezone. "
            "If omitted, report includes all tracked alerts."
        ),
    )

    parser.add_argument(
        "--timezone",
        type=str,
        default=DEFAULT_TIMEZONE,
        help="Timezone for --date filtering. Default: Europe/Kyiv.",
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON report instead of text report.",
    )

    parser.add_argument(
        "--telegram",
        action="store_true",
        help="Print compact Ukrainian Telegram-ready report.",
    )

    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Do not write report files, only print to console.",
    )

    return parser.parse_args()


def parse_report_date(value: str | None, *, tz: ZoneInfo) -> date | None:
    if value is None:
        return None

    text = value.strip()
    if not text:
        return None

    if text.lower() == "today":
        return today_in_timezone(tz)

    return date.fromisoformat(text)


def main() -> None:
    args = parse_args()

    source_path = Path(args.source)
    quality_path = Path(args.quality_source)
    out_dir = Path(args.out_dir)
    tz = get_timezone(args.timezone)
    report_date = parse_report_date(args.date, tz=tz)

    payload = load_signal_outcomes(source_path)
    quality_payload = load_quality_tiers(quality_path)

    report = build_report(
        payload=payload,
        quality_payload=quality_payload,
        report_date=report_date,
        tz=tz,
        source_path=source_path,
        quality_path=quality_path,
    )

    text = render_text_report(report)
    telegram_text = render_telegram_report(report)

    if not args.no_save:
        paths = save_report_files(
            report=report,
            text=text,
            out_dir=out_dir,
            telegram_text=telegram_text,
        )
        report["report_files"] = paths

        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
            return

        if args.telegram:
            print(telegram_text)
            print("")
            print("Saved files")
            print("-" * 72)
            print(f"Telegram TXT:        {paths.get('telegram_txt_path')}")
            print(f"Telegram latest TXT: {paths.get('telegram_latest_txt_path')}")
            return

        print(text)
        print("")
        print("Saved files")
        print("-" * 72)
        print(f"JSON:                {paths['json_path']}")
        print(f"TXT:                 {paths['txt_path']}")
        print(f"Latest JSON:         {paths['latest_json_path']}")
        print(f"Latest TXT:          {paths['latest_txt_path']}")
        print(f"Telegram TXT:        {paths.get('telegram_txt_path')}")
        print(f"Telegram latest TXT: {paths.get('telegram_latest_txt_path')}")
        return

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    elif args.telegram:
        print(telegram_text)
    else:
        print(text)


if __name__ == "__main__":
    main()