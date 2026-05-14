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
# DAILY SIGNAL REPORT v1
# =============================================================================
# Purpose:
# - Read signal_outcomes.json produced by app.services.signal_outcome_tracker.
# - Build lightweight operational report for Telegram/console/dashboard usage.
# - No pandas.
# - No external API calls.
# - Safe to run manually:
#
#   python -m app.services.daily_signal_report
#
# Optional:
#   python -m app.services.daily_signal_report --date 2026-05-14
#   python -m app.services.daily_signal_report --timezone Europe/Kyiv
#   python -m app.services.daily_signal_report --json
#
# Notes:
# - Default mode reports ALL tracked Telegram alerts from signal_outcomes.json.
# - Use --date YYYY-MM-DD to filter by sent_at_utc converted to selected timezone.
# =============================================================================


SIGNAL_OUTCOMES_PATH = settings.runtime_dir / "stats" / "signal_outcomes.json"
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
        warnings.append("No signals in selected scope.")
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
# REPORT BUILDER
# =============================================================================


def build_report(
    *,
    payload: dict[str, Any],
    report_date: date | None,
    tz: ZoneInfo,
    source_path: Path,
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

    report: dict[str, Any] = {
        "schema_version": "1.0",
        "generated_at_utc": utc_now(),
        "timezone": str(tz),
        "scope": scope,
        "source_path": str(source_path),
        "source_updated_at_utc": payload.get("updated_at_utc"),
        "source_error": payload.get("error"),
        "metrics": build_basic_metrics(signals),
        "by_symbol": build_group_metrics(signals, "symbol"),
        "by_scenario": build_group_metrics(signals, "scenario"),
        "by_direction": build_group_metrics(signals, "direction"),
        "by_signal_alignment": build_group_metrics(signals, "signal_alignment"),
        "by_stop_quality": build_group_metrics(signals, "stop_quality"),
        "by_execution_model": build_group_metrics(signals, "execution_model"),
    }

    report["warnings"] = build_warnings(report)

    return report


# =============================================================================
# TEXT RENDERING
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

    lines.append("📊 Daily Signal Report v1")
    lines.append("=" * 72)
    lines.append(f"Generated UTC: {report.get('generated_at_utc')}")
    lines.append(f"Timezone:      {report.get('timezone')}")
    lines.append(f"Scope:         {report.get('scope')}")
    lines.append(f"Source:        {report.get('source_path')}")
    lines.append(f"Source update: {report.get('source_updated_at_utc')}")
    lines.append("")

    if report.get("source_error"):
        lines.append(f"⚠️ Source error: {report.get('source_error')}")
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
# SAVING
# =============================================================================


def build_report_file_prefix(report: dict[str, Any]) -> str:
    scope = str(report.get("scope") or "all").replace(":", "_").replace("/", "_")
    generated = str(report.get("generated_at_utc") or utc_now())
    stamp = generated[:19].replace(":", "-")
    return f"daily_signal_report_{scope}_{stamp}"


def save_report_files(
    *,
    report: dict[str, Any],
    text: str,
    out_dir: Path = REPORTS_DIR,
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

    return {
        "json_path": str(json_path),
        "txt_path": str(txt_path),
        "latest_json_path": str(latest_json),
        "latest_txt_path": str(latest_txt),
    }


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
        "--no-save",
        action="store_true",
        help="Do not write report files, only print to console.",
    )

    return parser.parse_args()


def parse_report_date(value: str | None) -> date | None:
    if value is None:
        return None

    text = value.strip()
    if not text:
        return None

    if text.lower() == "today":
        return date.today()

    return date.fromisoformat(text)


def main() -> None:
    args = parse_args()

    source_path = Path(args.source)
    out_dir = Path(args.out_dir)
    tz = get_timezone(args.timezone)
    report_date = parse_report_date(args.date)

    payload = load_signal_outcomes(source_path)

    report = build_report(
        payload=payload,
        report_date=report_date,
        tz=tz,
        source_path=source_path,
    )

    text = render_text_report(report)

    if not args.no_save:
        paths = save_report_files(
            report=report,
            text=text,
            out_dir=out_dir,
        )
        report["report_files"] = paths

        # Re-render is not needed for text readability, but JSON should include paths.
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
            return

        print(text)
        print("")
        print("Saved files")
        print("-" * 72)
        print(f"JSON:        {paths['json_path']}")
        print(f"TXT:         {paths['txt_path']}")
        print(f"Latest JSON: {paths['latest_json_path']}")
        print(f"Latest TXT:  {paths['latest_txt_path']}")
        return

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(text)


if __name__ == "__main__":
    main()