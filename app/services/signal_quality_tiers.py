from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.settings import settings


# =============================================================================
# SIGNAL QUALITY TIERS v1
# =============================================================================
# Purpose:
# - Read signal_outcomes.json produced by signal_outcome_tracker.
# - Calculate category-level quality tiers.
# - Prepare future Telegram noise-control layer.
# - Do NOT block/delete signals yet.
# - No pandas.
# - No external API calls.
#
# Safe to run manually:
#
#   python -m app.services.signal_quality_tiers
#
# Optional:
#
#   python -m app.services.signal_quality_tiers --json
#   python -m app.services.signal_quality_tiers --min-sample 30
#   python -m app.services.signal_quality_tiers --early-min-sample 3
#
# Output:
#   /var/data/runtime/stats/quality_tiers.json
#   /var/data/runtime/stats/quality_tiers.txt
#
# Strategy:
# - With low sample size, avoid final conclusions.
# - Still mark early negative categories as CAUTION for operator awareness.
# - Later this file can feed Telegram formatting/gating.
# =============================================================================


SIGNAL_OUTCOMES_PATH = settings.runtime_dir / "stats" / "signal_outcomes.json"
QUALITY_TIERS_JSON_PATH = settings.runtime_dir / "stats" / "quality_tiers.json"
QUALITY_TIERS_TXT_PATH = settings.runtime_dir / "stats" / "quality_tiers.txt"


TIER_A_GRADE = "A-GRADE"
TIER_CAUTION = "CAUTION"
TIER_OBSERVE = "OBSERVE"
TIER_LOW_PRIORITY = "LOW_PRIORITY"
TIER_INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE"
TIER_NO_DATA = "NO_DATA"


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


DEFAULT_MIN_SAMPLE = int(os.getenv("QUALITY_TIER_MIN_SAMPLE", "30"))
DEFAULT_EARLY_MIN_SAMPLE = int(os.getenv("QUALITY_TIER_EARLY_MIN_SAMPLE", "3"))
DEFAULT_MIN_CLOSED_SAMPLE = int(os.getenv("QUALITY_TIER_MIN_CLOSED_SAMPLE", "10"))

DEFAULT_A_GRADE_WINRATE = float(os.getenv("QUALITY_TIER_A_GRADE_WINRATE", "0.55"))
DEFAULT_A_GRADE_AVG_R = float(os.getenv("QUALITY_TIER_A_GRADE_AVG_R", "0.25"))

DEFAULT_LOW_PRIORITY_WINRATE = float(os.getenv("QUALITY_TIER_LOW_PRIORITY_WINRATE", "0.35"))
DEFAULT_LOW_PRIORITY_AVG_R = float(os.getenv("QUALITY_TIER_LOW_PRIORITY_AVG_R", "-0.25"))

DEFAULT_EARLY_CAUTION_AVG_R = float(os.getenv("QUALITY_TIER_EARLY_CAUTION_AVG_R", "-0.25"))
DEFAULT_EARLY_CAUTION_SL_RATE = float(os.getenv("QUALITY_TIER_EARLY_CAUTION_SL_RATE", "0.50"))


DIMENSIONS: dict[str, tuple[str, ...]] = {
    "symbol": ("symbol",),
    "scenario": ("scenario",),
    "direction": ("direction",),
    "signal_alignment": ("signal_alignment",),
    "stop_quality": ("stop_quality",),
    "execution_model": ("execution_model",),
    "scenario_alignment": ("scenario", "signal_alignment"),
    "scenario_stop_quality": ("scenario", "stop_quality"),
    "symbol_scenario": ("symbol", "scenario"),
}


TIER_SEVERITY = {
    TIER_NO_DATA: 0,
    TIER_A_GRADE: 1,
    TIER_INSUFFICIENT_SAMPLE: 2,
    TIER_OBSERVE: 3,
    TIER_CAUTION: 4,
    TIER_LOW_PRIORITY: 5,
}


# =============================================================================
# BASIC HELPERS
# =============================================================================


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


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
# LOADING
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


# =============================================================================
# METRICS
# =============================================================================


def count_status_group(signals: list[dict[str, Any]], statuses: set[str]) -> int:
    return sum(
        1 for x in signals
        if normalize_status(x.get("outcome_status")) in statuses
    )


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


def status_counts(signals: list[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}

    for signal in signals:
        status = normalize_status(signal.get("outcome_status"))
        result[status] = result.get(status, 0) + 1

    return dict(sorted(result.items(), key=lambda x: x[0]))


def build_metrics(signals: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(signals)

    tp = count_status_group(signals, {TP_STATUS})
    sl = count_status_group(signals, {SL_STATUS})
    missed = count_status_group(signals, MISSED_STATUSES)
    expired = count_status_group(signals, EXPIRED_STATUSES)
    pending = count_status_group(signals, PENDING_STATUSES)
    invalid = count_status_group(signals, {"INVALID"})

    closed = tp + sl
    resolved = tp + sl + missed + expired + invalid

    winrate = calc_winrate(signals)
    avg_result_r = calc_avg_result_r(signals)

    sl_rate = round(sl / closed, 4) if closed > 0 else None
    tp_rate = round(tp / closed, 4) if closed > 0 else None
    missed_rate = round(missed / total, 4) if total > 0 else None
    pending_rate = round(pending / total, 4) if total > 0 else None
    resolved_rate = round(resolved / total, 4) if total > 0 else None

    return {
        "total_alerts": total,
        "tp_hit": tp,
        "sl_hit": sl,
        "missed_target_before_entry": missed,
        "expired": expired,
        "pending_or_active": pending,
        "invalid": invalid,
        "closed_tp_sl": closed,
        "resolved": resolved,
        "winrate": winrate,
        "avg_result_R": avg_result_r,
        "avg_rr": calc_avg_field(signals, "risk_reward_ratio"),
        "avg_practical_rr": calc_avg_field(signals, "practical_rr"),
        "tp_rate": tp_rate,
        "sl_rate": sl_rate,
        "missed_rate": missed_rate,
        "pending_rate": pending_rate,
        "resolved_rate": resolved_rate,
        "outcome_status": status_counts(signals),
    }


# =============================================================================
# QUALITY CLASSIFICATION
# =============================================================================


def build_thresholds(
    *,
    min_sample: int,
    early_min_sample: int,
    min_closed_sample: int,
) -> dict[str, Any]:
    return {
        "min_sample": min_sample,
        "early_min_sample": early_min_sample,
        "min_closed_sample": min_closed_sample,
        "a_grade_winrate": DEFAULT_A_GRADE_WINRATE,
        "a_grade_avg_result_R": DEFAULT_A_GRADE_AVG_R,
        "low_priority_winrate": DEFAULT_LOW_PRIORITY_WINRATE,
        "low_priority_avg_result_R": DEFAULT_LOW_PRIORITY_AVG_R,
        "early_caution_avg_result_R": DEFAULT_EARLY_CAUTION_AVG_R,
        "early_caution_sl_rate": DEFAULT_EARLY_CAUTION_SL_RATE,
    }


def classify_quality(metrics: dict[str, Any], thresholds: dict[str, Any]) -> dict[str, Any]:
    total = int(metrics.get("total_alerts") or 0)
    closed = int(metrics.get("closed_tp_sl") or 0)
    sl = int(metrics.get("sl_hit") or 0)
    tp = int(metrics.get("tp_hit") or 0)
    pending = int(metrics.get("pending_or_active") or 0)

    winrate = safe_float(metrics.get("winrate"))
    avg_result_r = safe_float(metrics.get("avg_result_R"))
    sl_rate = safe_float(metrics.get("sl_rate"))
    missed_rate = safe_float(metrics.get("missed_rate"))

    min_sample = int(thresholds["min_sample"])
    early_min_sample = int(thresholds["early_min_sample"])
    min_closed_sample = int(thresholds["min_closed_sample"])

    reasons: list[str] = []
    flags: list[str] = []

    if total <= 0:
        return {
            "tier": TIER_NO_DATA,
            "confidence": "NO_DATA",
            "action": "do_not_use_for_filtering",
            "reasons": ["No signals in this category."],
            "flags": [],
        }

    if total < min_sample:
        flags.append("LOW_SAMPLE_SIZE")
        reasons.append(
            f"Sample below statistical threshold: total={total}, required={min_sample}."
        )

        # Early diagnostic protection:
        # We still mark obviously weak small-sample categories as CAUTION,
        # but this is NOT a final statistical verdict.
        if total >= early_min_sample:
            early_negative_avg = (
                avg_result_r is not None
                and avg_result_r <= float(thresholds["early_caution_avg_result_R"])
            )
            early_bad_sl_rate = (
                sl_rate is not None
                and sl_rate >= float(thresholds["early_caution_sl_rate"])
            )
            no_winners_with_losses = tp == 0 and sl > 0

            if early_negative_avg and (early_bad_sl_rate or no_winners_with_losses):
                reasons.append(
                    f"Early weakness detected: avg_result_R={avg_result_r}, "
                    f"SL={sl}, TP={tp}, sl_rate={sl_rate}."
                )
                flags.append("EARLY_NEGATIVE_DIAGNOSTIC")

                return {
                    "tier": TIER_CAUTION,
                    "confidence": "EARLY_DIAGNOSTIC",
                    "action": "mark_in_telegram_but_do_not_block",
                    "reasons": reasons,
                    "flags": flags,
                }

        return {
            "tier": TIER_INSUFFICIENT_SAMPLE,
            "confidence": "INSUFFICIENT_SAMPLE",
            "action": "collect_more_data",
            "reasons": reasons,
            "flags": flags,
        }

    if closed < min_closed_sample:
        flags.append("LOW_CLOSED_SAMPLE")
        reasons.append(
            f"Closed TP/SL sample below threshold: closed={closed}, required={min_closed_sample}."
        )

        if avg_result_r is not None and avg_result_r < 0:
            reasons.append(f"Average result is negative: avg_result_R={avg_result_r}.")
            return {
                "tier": TIER_CAUTION,
                "confidence": "PARTIAL_SAMPLE",
                "action": "mark_in_telegram_but_do_not_block",
                "reasons": reasons,
                "flags": flags,
            }

        return {
            "tier": TIER_OBSERVE,
            "confidence": "PARTIAL_SAMPLE",
            "action": "observe_until_more_closed_trades",
            "reasons": reasons,
            "flags": flags,
        }

    # Statistical rules after enough sample exists.
    if (
        winrate is not None
        and avg_result_r is not None
        and winrate >= float(thresholds["a_grade_winrate"])
        and avg_result_r >= float(thresholds["a_grade_avg_result_R"])
    ):
        reasons.append(
            f"Positive category: winrate={winrate}, avg_result_R={avg_result_r}."
        )
        return {
            "tier": TIER_A_GRADE,
            "confidence": "STATISTICAL",
            "action": "allow_full_priority",
            "reasons": reasons,
            "flags": flags,
        }

    if (
        winrate is not None
        and avg_result_r is not None
        and winrate <= float(thresholds["low_priority_winrate"])
        and avg_result_r <= float(thresholds["low_priority_avg_result_R"])
    ):
        reasons.append(
            f"Weak category: winrate={winrate}, avg_result_R={avg_result_r}."
        )
        return {
            "tier": TIER_LOW_PRIORITY,
            "confidence": "STATISTICAL",
            "action": "downgrade_or_suppress_non_critical_telegram_output",
            "reasons": reasons,
            "flags": flags,
        }

    if avg_result_r is not None and avg_result_r < 0:
        reasons.append(f"Negative average result: avg_result_R={avg_result_r}.")
        return {
            "tier": TIER_CAUTION,
            "confidence": "STATISTICAL",
            "action": "mark_in_telegram_but_do_not_block",
            "reasons": reasons,
            "flags": flags,
        }

    if winrate is not None and winrate < 0.45:
        reasons.append(f"Weak winrate: winrate={winrate}.")
        return {
            "tier": TIER_CAUTION,
            "confidence": "STATISTICAL",
            "action": "mark_in_telegram_but_do_not_block",
            "reasons": reasons,
            "flags": flags,
        }

    if missed_rate is not None and missed_rate >= 0.35:
        reasons.append(f"High missed-before-entry rate: missed_rate={missed_rate}.")
        return {
            "tier": TIER_OBSERVE,
            "confidence": "STATISTICAL",
            "action": "observe_entry_model_quality",
            "reasons": reasons,
            "flags": flags,
        }

    if pending > 0:
        flags.append("HAS_PENDING_SIGNALS")

    reasons.append("Mixed or neutral category. Keep collecting data.")
    return {
        "tier": TIER_OBSERVE,
        "confidence": "STATISTICAL",
        "action": "observe",
        "reasons": reasons,
        "flags": flags,
    }


# =============================================================================
# GROUPING
# =============================================================================


def group_key(signal: dict[str, Any], fields: tuple[str, ...]) -> str:
    values = [normalize_text(signal.get(field)) for field in fields]
    return " | ".join(values)


def group_signals(
    signals: list[dict[str, Any]],
    fields: tuple[str, ...],
) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}

    for signal in signals:
        key = group_key(signal, fields)
        groups.setdefault(key, []).append(signal)

    return groups


def build_dimension_tiers(
    *,
    signals: list[dict[str, Any]],
    dimension_name: str,
    fields: tuple[str, ...],
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    groups = group_signals(signals, fields)
    result: dict[str, Any] = {}

    for key, items in sorted(groups.items(), key=lambda x: x[0]):
        metrics = build_metrics(items)
        quality = classify_quality(metrics, thresholds)

        result[key] = {
            "dimension": dimension_name,
            "fields": list(fields),
            "key": key,
            "quality": quality,
            "metrics": metrics,
        }

    return result


def build_all_dimension_tiers(
    *,
    signals: list[dict[str, Any]],
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for dimension_name, fields in DIMENSIONS.items():
        result[dimension_name] = build_dimension_tiers(
            signals=signals,
            dimension_name=dimension_name,
            fields=fields,
            thresholds=thresholds,
        )

    return result


# =============================================================================
# SIGNAL-LEVEL ANNOTATIONS
# =============================================================================


def find_category_tier(
    *,
    dimensions: dict[str, Any],
    dimension_name: str,
    signal: dict[str, Any],
) -> dict[str, Any] | None:
    fields = DIMENSIONS.get(dimension_name)
    if not fields:
        return None

    key = group_key(signal, fields)
    dimension_data = dimensions.get(dimension_name)
    if not isinstance(dimension_data, dict):
        return None

    item = dimension_data.get(key)
    if not isinstance(item, dict):
        return None

    return item


def choose_worst_tier(items: list[dict[str, Any]]) -> str:
    if not items:
        return TIER_NO_DATA

    best = TIER_NO_DATA
    best_score = -1

    for item in items:
        quality = item.get("quality")
        if not isinstance(quality, dict):
            continue

        tier = str(quality.get("tier") or TIER_NO_DATA)
        score = TIER_SEVERITY.get(tier, 0)

        if score > best_score:
            best = tier
            best_score = score

    return best


def build_signal_annotations(
    *,
    signals: list[dict[str, Any]],
    dimensions: dict[str, Any],
) -> list[dict[str, Any]]:
    annotations: list[dict[str, Any]] = []

    relevant_dimensions = [
        "symbol",
        "scenario",
        "signal_alignment",
        "stop_quality",
        "execution_model",
        "scenario_alignment",
        "scenario_stop_quality",
    ]

    for signal in signals:
        category_items: list[dict[str, Any]] = []

        for dimension_name in relevant_dimensions:
            item = find_category_tier(
                dimensions=dimensions,
                dimension_name=dimension_name,
                signal=signal,
            )
            if item is not None:
                category_items.append(item)

        final_tier = choose_worst_tier(category_items)

        reasons: list[str] = []
        flags: list[str] = []

        for item in category_items:
            quality = item.get("quality")
            if not isinstance(quality, dict):
                continue

            tier = str(quality.get("tier") or TIER_NO_DATA)
            dimension = str(item.get("dimension") or "UNKNOWN")
            key = str(item.get("key") or "UNKNOWN")

            if tier == final_tier:
                reasons.append(f"{dimension}={key} => {tier}")

            item_flags = quality.get("flags")
            if isinstance(item_flags, list):
                for flag in item_flags:
                    if flag not in flags:
                        flags.append(str(flag))

        annotations.append(
            {
                "alert_id": signal.get("alert_id"),
                "signal_id": signal.get("signal_id"),
                "symbol": signal.get("symbol"),
                "scenario": signal.get("scenario"),
                "direction": signal.get("direction"),
                "outcome_status": signal.get("outcome_status"),
                "telegram_quality_tier": final_tier,
                "quality_flags": flags,
                "quality_reasons": reasons,
            }
        )

    return annotations


# =============================================================================
# REPORT BUILDER
# =============================================================================


def count_tiers(dimensions: dict[str, Any]) -> dict[str, int]:
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

            tier = str(quality.get("tier") or TIER_NO_DATA)
            result[tier] = result.get(tier, 0) + 1

    return dict(sorted(result.items(), key=lambda x: x[0]))


def build_report(
    *,
    payload: dict[str, Any],
    source_path: Path,
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    signals = payload.get("signals")
    if not isinstance(signals, list):
        signals = []

    clean_signals = [x for x in signals if isinstance(x, dict)]

    dimensions = build_all_dimension_tiers(
        signals=clean_signals,
        thresholds=thresholds,
    )

    signal_annotations = build_signal_annotations(
        signals=clean_signals,
        dimensions=dimensions,
    )

    return {
        "schema_version": "1.0",
        "generated_at_utc": utc_now(),
        "source_path": str(source_path),
        "source_updated_at_utc": payload.get("updated_at_utc"),
        "source_error": payload.get("error"),
        "thresholds": thresholds,
        "global_metrics": build_metrics(clean_signals),
        "tier_counts": count_tiers(dimensions),
        "dimensions": dimensions,
        "signal_annotations": signal_annotations,
        "operator_note": (
            "Quality tiers are diagnostic. Do not block/delete signals yet. "
            "Use tiers first for Telegram labels and operator awareness."
        ),
    }


# =============================================================================
# TEXT RENDERING
# =============================================================================


def render_dimension_block(
    *,
    title: str,
    dimension_data: dict[str, Any],
    max_items: int = 30,
) -> list[str]:
    lines: list[str] = []
    lines.append("")
    lines.append(title)

    if not dimension_data:
        lines.append("  - n/a")
        return lines

    sorted_items = sorted(
        dimension_data.values(),
        key=lambda item: (
            -TIER_SEVERITY.get(str(item.get("quality", {}).get("tier")), 0),
            -int(item.get("metrics", {}).get("total_alerts") or 0),
            str(item.get("key") or ""),
        ),
    )

    for item in sorted_items[:max_items]:
        key = str(item.get("key") or "UNKNOWN")
        quality = item.get("quality") if isinstance(item.get("quality"), dict) else {}
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}

        tier = str(quality.get("tier") or TIER_NO_DATA)
        confidence = str(quality.get("confidence") or "UNKNOWN")
        action = str(quality.get("action") or "UNKNOWN")

        total = int(metrics.get("total_alerts") or 0)
        tp = int(metrics.get("tp_hit") or 0)
        sl = int(metrics.get("sl_hit") or 0)
        missed = int(metrics.get("missed_target_before_entry") or 0)
        pending = int(metrics.get("pending_or_active") or 0)
        winrate = safe_float(metrics.get("winrate"))
        avg_r = safe_float(metrics.get("avg_result_R"))

        lines.append(
            f"  - {key}: tier={tier}, confidence={confidence}, "
            f"total={total}, TP={tp}, SL={sl}, missed={missed}, pending={pending}, "
            f"winrate={format_pct(winrate)}, avgR={format_r(avg_r)}, action={action}"
        )

    if len(sorted_items) > max_items:
        lines.append(f"  ... {len(sorted_items) - max_items} more")

    return lines


def render_annotations_block(annotations: list[dict[str, Any]], max_items: int = 30) -> list[str]:
    lines: list[str] = []
    lines.append("")
    lines.append("Signal annotations")

    if not annotations:
        lines.append("  - n/a")
        return lines

    sorted_items = sorted(
        annotations,
        key=lambda item: (
            -TIER_SEVERITY.get(str(item.get("telegram_quality_tier")), 0),
            str(item.get("symbol") or ""),
            str(item.get("scenario") or ""),
        ),
    )

    for item in sorted_items[:max_items]:
        symbol = normalize_text(item.get("symbol"))
        scenario = normalize_text(item.get("scenario"))
        direction = normalize_text(item.get("direction"))
        status = normalize_text(item.get("outcome_status"))
        tier = normalize_text(item.get("telegram_quality_tier"))

        reasons = item.get("quality_reasons")
        if isinstance(reasons, list) and reasons:
            reason_text = "; ".join(str(x) for x in reasons[:2])
        else:
            reason_text = "n/a"

        lines.append(
            f"  - {symbol} {direction} {scenario}: status={status}, tier={tier}, reason={reason_text}"
        )

    if len(sorted_items) > max_items:
        lines.append(f"  ... {len(sorted_items) - max_items} more")

    return lines


def render_text_report(report: dict[str, Any]) -> str:
    metrics = report.get("global_metrics")
    if not isinstance(metrics, dict):
        metrics = {}

    total = int(metrics.get("total_alerts") or 0)
    tp = int(metrics.get("tp_hit") or 0)
    sl = int(metrics.get("sl_hit") or 0)
    missed = int(metrics.get("missed_target_before_entry") or 0)
    pending = int(metrics.get("pending_or_active") or 0)

    winrate = safe_float(metrics.get("winrate"))
    avg_r = safe_float(metrics.get("avg_result_R"))
    avg_rr = safe_float(metrics.get("avg_rr"))
    avg_practical_rr = safe_float(metrics.get("avg_practical_rr"))

    lines: list[str] = []

    lines.append("🧠 Signal Quality Tiers v1")
    lines.append("=" * 72)
    lines.append(f"Generated UTC: {report.get('generated_at_utc')}")
    lines.append(f"Source:        {report.get('source_path')}")
    lines.append(f"Source update: {report.get('source_updated_at_utc')}")
    lines.append("")

    if report.get("source_error"):
        lines.append(f"⚠️ Source error: {report.get('source_error')}")
        lines.append("")

    lines.append("Global metrics")
    lines.append("-" * 72)
    lines.append(f"Total alerts:          {total}")
    lines.append(f"TP / SL:               {tp} / {sl}")
    lines.append(f"Missed before entry:   {missed}")
    lines.append(f"Pending / active:      {pending}")
    lines.append(f"Winrate TP/SL only:    {format_pct(winrate)}")
    lines.append(f"Average result:        {format_r(avg_r)}")
    lines.append(f"Average RR:            {format_float(avg_rr)}")
    lines.append(f"Average practical RR:  {format_float(avg_practical_rr)}")

    tier_counts = report.get("tier_counts")
    if not isinstance(tier_counts, dict):
        tier_counts = {}

    lines.append("")
    lines.append("Tier counts")
    lines.append("-" * 72)
    if tier_counts:
        for tier, count in tier_counts.items():
            lines.append(f"  - {tier}: {count}")
    else:
        lines.append("  - n/a")

    dimensions = report.get("dimensions")
    if not isinstance(dimensions, dict):
        dimensions = {}

    for dimension_name in [
        "stop_quality",
        "signal_alignment",
        "scenario",
        "symbol",
        "execution_model",
        "scenario_alignment",
        "scenario_stop_quality",
        "symbol_scenario",
    ]:
        dimension_data = dimensions.get(dimension_name)
        if isinstance(dimension_data, dict):
            lines.extend(
                render_dimension_block(
                    title=f"By {dimension_name}",
                    dimension_data=dimension_data,
                )
            )

    annotations = report.get("signal_annotations")
    if isinstance(annotations, list):
        lines.extend(render_annotations_block(annotations))

    lines.append("")
    lines.append("Operator note")
    lines.append("-" * 72)
    lines.append(str(report.get("operator_note") or ""))

    return "\n".join(lines)


# =============================================================================
# SAVING
# =============================================================================


def save_report_files(
    *,
    report: dict[str, Any],
    text: str,
    json_path: Path = QUALITY_TIERS_JSON_PATH,
    txt_path: Path = QUALITY_TIERS_TXT_PATH,
) -> dict[str, str]:
    ensure_parent_dir(json_path)
    ensure_parent_dir(txt_path)

    tmp_json = json_path.with_suffix(json_path.suffix + ".tmp")
    tmp_txt = txt_path.with_suffix(txt_path.suffix + ".tmp")

    tmp_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp_txt.write_text(text, encoding="utf-8")

    tmp_json.replace(json_path)
    tmp_txt.replace(txt_path)

    return {
        "json_path": str(json_path),
        "txt_path": str(txt_path),
    }


# =============================================================================
# CLI
# =============================================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build signal quality tiers from signal_outcomes.json."
    )

    parser.add_argument(
        "--source",
        type=str,
        default=str(SIGNAL_OUTCOMES_PATH),
        help="Path to signal_outcomes.json.",
    )

    parser.add_argument(
        "--out-json",
        type=str,
        default=str(QUALITY_TIERS_JSON_PATH),
        help="Output path for quality_tiers.json.",
    )

    parser.add_argument(
        "--out-txt",
        type=str,
        default=str(QUALITY_TIERS_TXT_PATH),
        help="Output path for quality_tiers.txt.",
    )

    parser.add_argument(
        "--min-sample",
        type=int,
        default=DEFAULT_MIN_SAMPLE,
        help="Statistical sample threshold before final tier decisions.",
    )

    parser.add_argument(
        "--early-min-sample",
        type=int,
        default=DEFAULT_EARLY_MIN_SAMPLE,
        help="Minimum sample for early caution diagnostics.",
    )

    parser.add_argument(
        "--min-closed-sample",
        type=int,
        default=DEFAULT_MIN_CLOSED_SAMPLE,
        help="Minimum TP/SL closed sample for stronger decisions.",
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of text.",
    )

    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Do not write files, only print.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    source_path = Path(args.source)
    out_json = Path(args.out_json)
    out_txt = Path(args.out_txt)

    thresholds = build_thresholds(
        min_sample=args.min_sample,
        early_min_sample=args.early_min_sample,
        min_closed_sample=args.min_closed_sample,
    )

    payload = load_signal_outcomes(source_path)

    report = build_report(
        payload=payload,
        source_path=source_path,
        thresholds=thresholds,
    )

    text = render_text_report(report)

    if not args.no_save:
        paths = save_report_files(
            report=report,
            text=text,
            json_path=out_json,
            txt_path=out_txt,
        )
        report["report_files"] = paths

        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
            return

        print(text)
        print("")
        print("Saved files")
        print("-" * 72)
        print(f"JSON: {paths['json_path']}")
        print(f"TXT:  {paths['txt_path']}")
        return

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(text)


if __name__ == "__main__":
    main()