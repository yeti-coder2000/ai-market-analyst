from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from app.core.settings import settings


@dataclass
class FormattedTelegramMessage:
    message_type: str
    title: str
    body: str
    signal_id: Optional[str] = None
    symbol: Optional[str] = None
    stage: Optional[str] = None

    def render(self) -> str:
        return f"{self.title}\n\n{self.body}".strip()


# =============================================================================
# QUALITY TIER CONFIG
# =============================================================================


QUALITY_TIERS_PATH = settings.runtime_dir / "stats" / "quality_tiers.json"

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

QUALITY_DIMENSION_FIELDS: dict[str, tuple[str, ...]] = {
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

QUALITY_DIMENSION_PRIORITY = [
    "scenario_stop_quality",
    "scenario_alignment",
    "symbol_scenario",
    "stop_quality",
    "signal_alignment",
    "scenario",
    "execution_model",
    "symbol",
]

_QUALITY_TIERS_CACHE: dict[str, Any] = {
    "mtime_ns": None,
    "payload": None,
}


# =============================================================================
# BASIC HELPERS
# =============================================================================


def _safe_str(value: Any, default: str = "-") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if value == "":
            continue
        return value
    return None


def _env_bool(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _format_price(value: Any) -> str:
    if value is None:
        return "не задано"

    try:
        num = float(value)
    except (TypeError, ValueError):
        return "не задано"

    if abs(num) >= 1000:
        return f"{num:,.2f}".replace(",", " ")

    if abs(num) >= 10:
        return f"{num:.2f}"

    return f"{num:.5f}"


def _format_pct(value: Any) -> str:
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return "0.00%"


def _format_rr(value: Any) -> str:
    try:
        rr = float(value)
    except (TypeError, ValueError):
        return "не задано"

    return f"1:{rr:.2f}"


def _extract_stage(signal_payload: dict) -> str:
    stage = _safe_str(
        signal_payload.get("stage") or signal_payload.get("signal_class"),
        "",
    )
    execution_status = _safe_str(signal_payload.get("execution_status"), "")
    status = _safe_str(signal_payload.get("status"), "")

    if stage == "READY" and execution_status != "EXECUTABLE":
        return "WATCH"

    if status == "RESOLVED":
        return "RESOLVED"

    return stage or "SCENARIO_FORMING"


def _extract_confidence(signal_payload: dict) -> float:
    confidence = _safe_float(signal_payload.get("confidence"), None)
    if confidence is not None:
        return float(confidence)

    probability = _safe_float(signal_payload.get("probability"), 0.0)
    if probability is None:
        return 0.0

    return float(probability)


# =============================================================================
# HUMANIZATION
# =============================================================================


def humanize_scenario_name(scenario: str) -> str:
    mapping = {
        "TREND_CONTINUATION_SHORT": "Trend Continuation Short",
        "TREND_CONTINUATION_LONG": "Trend Continuation Long",
        "SWEEP_RETURN_LONG": "Sweep Return Long",
        "SWEEP_RETURN_SHORT": "Sweep Return Short",
        "BALANCE_ROTATION": "Balance Rotation",
        "TRANSITION_EXPANSION": "Transition Expansion",
        "NO_ACTION": "No Action",
        "MARKET_CLOSED": "Market Closed",
    }

    return mapping.get(
        _safe_str(scenario, ""),
        _safe_str(scenario, "Unknown Scenario").replace("_", " ").title(),
    )


def humanize_stage(stage: str) -> str:
    mapping = {
        "NO_ACTION": "⚪ NO ACTION",
        "BIAS_ONLY": "🔵 BIAS",
        "SCENARIO_FORMING": "🟡 FORMING",
        "WATCH": "🟠 WATCH",
        "READY": "🔴 READY",
        "ACTIVE": "🚨 ACTIVE",
        "RESOLVED": "✅ RESOLVED",
    }

    return mapping.get(_safe_str(stage, ""), f"ℹ️ {_safe_str(stage, 'UNKNOWN')}")


def humanize_direction(direction: str) -> str:
    mapping = {
        "LONG": "LONG",
        "SHORT": "SHORT",
        "NEUTRAL": "NEUTRAL",
    }

    return mapping.get(
        _safe_str(direction, "NEUTRAL"),
        _safe_str(direction, "NEUTRAL"),
    )


def humanize_market_state(state: str) -> str:
    mapping = {
        "TREND": "Trend",
        "TRANSITION": "Transition",
        "BALANCE": "Balance",
        "UNKNOWN": "Unknown",
    }

    return mapping.get(
        _safe_str(state, "UNKNOWN"),
        _safe_str(state, "Unknown"),
    )


def humanize_bias(bias: str) -> str:
    mapping = {
        "LONG": "Bullish",
        "SHORT": "Bearish",
        "NEUTRAL": "Neutral",
    }

    return mapping.get(
        _safe_str(bias, "NEUTRAL"),
        _safe_str(bias, "Neutral"),
    )


def humanize_execution_model(model: str) -> str:
    mapping = {
        "LIMIT_ON_RETEST": "Limit on Retest",
        "STOP_ON_CONFIRMATION": "Stop on Confirmation",
        "MARKET": "Market",
        "ZONE_RETEST": "Zone Retest",
        "NONE": "None",
    }

    return mapping.get(
        _safe_str(model, "NONE"),
        _safe_str(model, "None").replace("_", " ").title(),
    )


def humanize_quality_confidence(confidence: Any) -> str:
    value = _safe_str(confidence, "UNKNOWN")

    mapping = {
        "EARLY_DIAGNOSTIC": "рання діагностика",
        "INSUFFICIENT_SAMPLE": "мала вибірка",
        "HISTORICAL_ANNOTATION": "оцінка з історичної статистики",
        "STATISTICAL": "статистично підтверджено",
        "PARTIAL_SAMPLE": "часткова вибірка",
        "NO_DATA": "немає даних",
        "UNKNOWN": "невідомо",
    }

    text = mapping.get(value, value.replace("_", " ").lower())
    return f"{text} ({value})"


def humanize_quality_action(action: Any) -> str:
    value = _safe_str(action, "UNKNOWN")

    mapping = {
        "mark_in_telegram_but_do_not_block": "позначити в Telegram, але не блокувати сигнал",
        "collect_more_data": "збирати більше статистики",
        "downgrade_or_suppress_non_critical_telegram_output": "понизити пріоритет або приховувати некритичну видачу",
        "allow_full_priority": "дати повний пріоритет",
        "use_operator_awareness": "використати як попередження для оператора",
        "observe": "спостерігати",
        "observe_until_more_closed_trades": "спостерігати до більшої кількості закритих угод",
        "observe_entry_model_quality": "перевіряти якість моделі входу",
        "do_not_use_for_filtering": "не використовувати для фільтрації",
        "UNKNOWN": "невідомо",
    }

    return mapping.get(value, value.replace("_", " ").lower())


def humanize_quality_flag(flag: Any) -> str:
    value = _safe_str(flag, "UNKNOWN")

    mapping = {
        "LOW_SAMPLE_SIZE": "мала вибірка",
        "EARLY_NEGATIVE_DIAGNOSTIC": "ранній негативний сигнал статистики",
        "LOW_CLOSED_SAMPLE": "мало закритих TP/SL результатів",
        "HAS_PENDING_SIGNALS": "є активні або незавершені сигнали",
        "UNKNOWN": "невідома позначка",
    }

    return f"{mapping.get(value, value.replace('_', ' ').lower())} ({value})"


def humanize_quality_dimension(dimension: Any) -> str:
    value = _safe_str(dimension, "UNKNOWN")

    mapping = {
        "symbol": "актив",
        "scenario": "сценарій",
        "direction": "напрям",
        "signal_alignment": "узгодження з HTF",
        "stop_quality": "якість стопа",
        "execution_model": "модель входу",
        "scenario_alignment": "сценарій + HTF",
        "scenario_stop_quality": "сценарій + якість стопа",
        "symbol_scenario": "актив + сценарій",
    }

    return mapping.get(value, value.replace("_", " "))


def humanize_quality_reason(reason: Any) -> str:
    text = _safe_str(reason, "")

    if not text:
        return "-"

    # Pattern: stop_quality=TIGHT_STOP => CAUTION
    if "=>" in text and "=" in text:
        try:
            left, tier = text.split("=>", 1)
            tier = tier.strip()

            dimension, value = left.split("=", 1)
            dimension = dimension.strip()
            value = value.strip()

            return f"{humanize_quality_dimension(dimension)}: {value} → {tier}"
        except ValueError:
            return text

    return text


# =============================================================================
# ALIGNMENT
# =============================================================================


def infer_signal_alignment(direction: Any, htf_bias: Any) -> str:
    direction_text = _safe_str(direction, "").upper()
    htf_text = _safe_str(htf_bias, "").upper()

    if direction_text not in {"LONG", "SHORT"}:
        return "NO_DIRECTION"

    if htf_text == "NEUTRAL" or not htf_text:
        return "NEUTRAL_HTF"

    if htf_text not in {"LONG", "SHORT"}:
        return "UNKNOWN_HTF"

    if direction_text == htf_text:
        return "TREND_ALIGNED"

    return "COUNTER_TREND"


def signal_alignment_marker(alignment: Any) -> str:
    mapping = {
        "TREND_ALIGNED": "🟢",
        "COUNTER_TREND": "🔴",
        "NEUTRAL_HTF": "⚪",
        "NO_DIRECTION": "⚫",
        "UNKNOWN_HTF": "⚫",
    }

    return mapping.get(
        _safe_str(alignment, "UNKNOWN_HTF").upper(),
        "⚫",
    )


def signal_alignment_label(alignment: Any) -> str:
    mapping = {
        "TREND_ALIGNED": "TREND-ALIGNED",
        "COUNTER_TREND": "COUNTER-TREND",
        "NEUTRAL_HTF": "NEUTRAL HTF",
        "NO_DIRECTION": "NO DIRECTION",
        "UNKNOWN_HTF": "UNKNOWN HTF",
    }

    return mapping.get(
        _safe_str(alignment, "UNKNOWN_HTF").upper(),
        "UNKNOWN HTF",
    )


def build_alignment_text(signal_payload: dict) -> str:
    alignment = _safe_str(signal_payload.get("signal_alignment"), "")

    if alignment in {"", "-"}:
        alignment = infer_signal_alignment(
            signal_payload.get("direction"),
            signal_payload.get("htf_bias"),
        )

    marker = _safe_str(
        signal_payload.get("signal_alignment_marker"),
        signal_alignment_marker(alignment),
    )
    label = _safe_str(
        signal_payload.get("signal_alignment_label"),
        signal_alignment_label(alignment),
    )

    return f"{marker} {label}"


# =============================================================================
# STOP QUALITY / EXECUTION WARNING
# =============================================================================


MIN_STOP_DISTANCE_BY_SYMBOL = {
    "XAUUSD": 8.0,
    "BTCUSD": 100.0,
    "ETHUSD": 3.0,
    "EURUSD": 0.00050,
    "GBPUSD": 0.00060,
    "AUDUSD": 0.00040,
    "USDCHF": 0.00050,
    "USDCAD": 0.00050,
    "USDJPY": 0.08,
    "GER40": 30.0,
    "NAS100": 50.0,
    "SPX500": 8.0,
    "UKOIL": 0.20,
}


def infer_stop_quality(signal_payload: dict) -> tuple[str, str | None]:
    symbol = _safe_str(signal_payload.get("symbol"), "").upper()
    entry = _safe_float(
        _first_present(
            signal_payload.get("entry_reference_price"),
            signal_payload.get("entry"),
        ),
        None,
    )
    stop = _safe_float(
        _first_present(
            signal_payload.get("invalidation_reference_price"),
            signal_payload.get("stop_loss"),
            signal_payload.get("stop"),
        ),
        None,
    )

    if entry is None or stop is None:
        return "UNKNOWN", None

    stop_distance = abs(entry - stop)
    min_stop = MIN_STOP_DISTANCE_BY_SYMBOL.get(symbol)

    if min_stop is None:
        return "UNKNOWN", None

    if stop_distance < min_stop:
        return (
            "TIGHT_STOP",
            (
                f"⚠️ TIGHT STOP / RR INFLATED: stop distance "
                f"{_format_price(stop_distance)} is below practical minimum "
                f"{_format_price(min_stop)} for {symbol}."
            ),
        )

    return "OK", None


def build_execution_warning_text(signal_payload: dict) -> str | None:
    explicit_quality = _safe_str(signal_payload.get("stop_quality"), "").upper()
    explicit_reason = _safe_str(signal_payload.get("stop_quality_reason"), "")

    if explicit_quality == "TIGHT_STOP" and explicit_reason not in {"", "-"}:
        return f"⚠️ TIGHT STOP / RR INFLATED: {explicit_reason}"

    quality, reason = infer_stop_quality(signal_payload)

    if quality == "TIGHT_STOP":
        return reason

    return None


# =============================================================================
# QUALITY TIER HELPERS
# =============================================================================


def _quality_tiers_enabled() -> bool:
    return _env_bool("ENABLE_TELEGRAM_QUALITY_TIERS", True)


def _quality_tiers_path() -> Path:
    raw = os.getenv("QUALITY_TIERS_PATH")
    if raw and raw.strip():
        return Path(raw.strip())

    return QUALITY_TIERS_PATH


def load_quality_tiers_payload() -> dict[str, Any] | None:
    """
    Load quality_tiers.json safely.

    Fail-open behavior:
    - missing file -> no quality block
    - invalid JSON -> no quality block
    - read error -> no quality block

    Telegram signal must never fail only because quality_tiers.json is missing.
    """
    if not _quality_tiers_enabled():
        return None

    path = _quality_tiers_path()

    try:
        if not path.exists():
            return None

        stat = path.stat()
        mtime_ns = stat.st_mtime_ns

        if (
            _QUALITY_TIERS_CACHE.get("mtime_ns") == mtime_ns
            and isinstance(_QUALITY_TIERS_CACHE.get("payload"), dict)
        ):
            return _QUALITY_TIERS_CACHE["payload"]

        payload = json.loads(path.read_text(encoding="utf-8"))

        if not isinstance(payload, dict):
            return None

        _QUALITY_TIERS_CACHE["mtime_ns"] = mtime_ns
        _QUALITY_TIERS_CACHE["payload"] = payload

        return payload

    except Exception:
        return None


def _quality_normalize_text(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").strip()
    return text if text else default


def _quality_field_value(signal_payload: dict, field: str) -> str:
    if field == "signal_alignment":
        alignment = _safe_str(signal_payload.get("signal_alignment"), "")
        if alignment in {"", "-"}:
            alignment = infer_signal_alignment(
                signal_payload.get("direction"),
                signal_payload.get("htf_bias"),
            )
        return _quality_normalize_text(alignment)

    if field == "stop_quality":
        explicit = _safe_str(signal_payload.get("stop_quality"), "")
        if explicit not in {"", "-"}:
            return _quality_normalize_text(explicit.upper())

        inferred, _ = infer_stop_quality(signal_payload)
        return _quality_normalize_text(inferred)

    if field == "execution_model":
        return _quality_normalize_text(
            signal_payload.get("execution_model"),
            "NONE",
        )

    return _quality_normalize_text(signal_payload.get(field))


def _quality_group_key(signal_payload: dict, fields: tuple[str, ...]) -> str:
    return " | ".join(
        _quality_field_value(signal_payload, field)
        for field in fields
    )


def _quality_severity(tier: Any) -> int:
    return QUALITY_TIER_SEVERITY.get(_safe_str(tier, "NO_DATA"), 0)


def _quality_marker(tier: str) -> str:
    return QUALITY_TIER_MARKERS.get(_safe_str(tier, "NO_DATA"), "⚫")


def _find_quality_annotation(
    *,
    payload: dict[str, Any],
    signal_payload: dict,
) -> dict[str, Any] | None:
    annotations = payload.get("signal_annotations")
    if not isinstance(annotations, list):
        return None

    signal_id = _safe_str(signal_payload.get("signal_id"), "")
    alert_id = _safe_str(signal_payload.get("alert_id"), "")

    for item in annotations:
        if not isinstance(item, dict):
            continue

        item_signal_id = _safe_str(item.get("signal_id"), "")
        item_alert_id = _safe_str(item.get("alert_id"), "")

        if alert_id not in {"", "-"} and item_alert_id == alert_id:
            return item

        if signal_id not in {"", "-"} and item_signal_id == signal_id:
            return item

    return None


def _build_quality_from_annotation(annotation: dict[str, Any]) -> dict[str, Any] | None:
    tier = _safe_str(annotation.get("telegram_quality_tier"), "")

    if tier in {"", "-"}:
        return None

    reasons = annotation.get("quality_reasons")
    flags = annotation.get("quality_flags")

    return {
        "tier": tier,
        "confidence": "HISTORICAL_ANNOTATION",
        "action": "use_operator_awareness",
        "reasons": reasons if isinstance(reasons, list) else [],
        "flags": flags if isinstance(flags, list) else [],
        "source": "signal_annotations",
    }


def _find_dimension_quality_candidates(
    *,
    payload: dict[str, Any],
    signal_payload: dict,
) -> list[dict[str, Any]]:
    dimensions = payload.get("dimensions")
    if not isinstance(dimensions, dict):
        return []

    candidates: list[dict[str, Any]] = []

    for dimension_name in QUALITY_DIMENSION_PRIORITY:
        fields = QUALITY_DIMENSION_FIELDS.get(dimension_name)
        if not fields:
            continue

        dimension_data = dimensions.get(dimension_name)
        if not isinstance(dimension_data, dict):
            continue

        key = _quality_group_key(signal_payload, fields)
        item = dimension_data.get(key)

        if not isinstance(item, dict):
            continue

        quality = item.get("quality")
        if not isinstance(quality, dict):
            continue

        tier = _safe_str(quality.get("tier"), "")
        if tier in {"", "-"}:
            continue

        candidates.append(
            {
                "dimension": dimension_name,
                "key": key,
                "tier": tier,
                "confidence": _safe_str(quality.get("confidence"), "UNKNOWN"),
                "action": _safe_str(quality.get("action"), "UNKNOWN"),
                "reasons": (
                    quality.get("reasons")
                    if isinstance(quality.get("reasons"), list)
                    else []
                ),
                "flags": (
                    quality.get("flags")
                    if isinstance(quality.get("flags"), list)
                    else []
                ),
                "metrics": (
                    item.get("metrics")
                    if isinstance(item.get("metrics"), dict)
                    else {}
                ),
                "source": "dimensions",
            }
        )

    return candidates


def resolve_signal_quality(signal_payload: dict) -> dict[str, Any] | None:
    payload = load_quality_tiers_payload()
    if payload is None:
        return None

    annotation = _find_quality_annotation(
        payload=payload,
        signal_payload=signal_payload,
    )

    if annotation is not None:
        quality = _build_quality_from_annotation(annotation)
        if quality is not None:
            return quality

    candidates = _find_dimension_quality_candidates(
        payload=payload,
        signal_payload=signal_payload,
    )

    if not candidates:
        return None

    worst = sorted(
        candidates,
        key=lambda item: (
            -_quality_severity(item.get("tier")),
            (
                QUALITY_DIMENSION_PRIORITY.index(item["dimension"])
                if item.get("dimension") in QUALITY_DIMENSION_PRIORITY
                else 999
            ),
        ),
    )[0]

    worst_tier = _safe_str(worst.get("tier"), "NO_DATA")

    reasons: list[str] = []
    flags: list[str] = []

    for item in candidates:
        tier = _safe_str(item.get("tier"), "NO_DATA")
        dimension = _safe_str(item.get("dimension"), "UNKNOWN")
        key = _safe_str(item.get("key"), "UNKNOWN")

        if tier == worst_tier:
            reasons.append(f"{dimension}={key} => {tier}")

        item_flags = item.get("flags")
        if isinstance(item_flags, list):
            for flag in item_flags:
                flag_text = str(flag)
                if flag_text not in flags:
                    flags.append(flag_text)

    return {
        "tier": worst_tier,
        "confidence": _safe_str(worst.get("confidence"), "UNKNOWN"),
        "action": _safe_str(worst.get("action"), "UNKNOWN"),
        "reasons": reasons,
        "flags": flags,
        "source": "dimension_fallback",
    }


def build_quality_tier_text(signal_payload: dict) -> str | None:
    quality = resolve_signal_quality(signal_payload)
    if quality is None:
        return None

    tier = _safe_str(quality.get("tier"), "NO_DATA")
    if tier == "NO_DATA":
        return None

    confidence = _safe_str(quality.get("confidence"), "UNKNOWN")
    action = _safe_str(quality.get("action"), "UNKNOWN")
    marker = _quality_marker(tier)

    lines = [
        f"{marker} Рівень якості: {tier}",
        f"Довіра до оцінки: {humanize_quality_confidence(confidence)}",
    ]

    if action not in {"", "-", "UNKNOWN"}:
        lines.append(f"Рекомендована дія: {humanize_quality_action(action)}")

    reasons = quality.get("reasons")
    if isinstance(reasons, list) and reasons:
        lines.append("Причина оцінки:")
        for reason in reasons[:3]:
            lines.append(f"- {humanize_quality_reason(reason)}")

    flags = quality.get("flags")
    if isinstance(flags, list) and flags:
        clean_flags = [
            humanize_quality_flag(x)
            for x in flags
            if str(x).strip()
        ]

        if clean_flags:
            lines.append(f"Позначки якості: {', '.join(clean_flags[:5])}")

    return "\n".join(lines)


# =============================================================================
# MESSAGE TEXT
# =============================================================================


TELEGRAM_FORMATTER_VERSION = "telegram-formatter-v1.1-tpo-ltf-ready-cleanup"

TPO_LTF_READY_TRIGGERS = {
    "ltf_model_confirmed_open_test_drive",
}


def _normalize_upper(value: Any) -> str:
    return _safe_str(value, "").upper()


def _is_tpo_ltf_ready(signal_payload: dict) -> bool:
    """
    True when the old scenario-engine narrative should be suppressed.

    TPO/LTF READY signals are already confirmed by the LTF detector, so Telegram
    must not show stale missing_conditions like sweep / return_to_value.
    """
    stage = _extract_stage(signal_payload)
    execution_status = _normalize_upper(signal_payload.get("execution_status"))
    trigger_reason = _safe_str(signal_payload.get("trigger_reason"), "")
    scenario = _normalize_upper(
        signal_payload.get("scenario") or signal_payload.get("scenario_type")
    )
    setup_type = _normalize_upper(
        signal_payload.get("setup_type") or signal_payload.get("setup_name")
    )
    ltf_outcome = _normalize_upper(signal_payload.get("ltf_model_outcome"))
    ltf_state = _normalize_upper(
        signal_payload.get("ltf_model_state_full") or signal_payload.get("ltf_model_state")
    )

    if stage != "READY" or execution_status != "EXECUTABLE":
        return False

    if trigger_reason in TPO_LTF_READY_TRIGGERS:
        return True

    if scenario.startswith("TPO_OPEN_TEST_DRIVE") and (
        ltf_outcome == "CONFIRMED_EXECUTABLE"
        or ltf_state in {"LTF_MODEL_CONFIRMED", "CONFIRMED"}
    ):
        return True

    if setup_type == "TPO_OPEN_TEST_DRIVE" and (
        ltf_outcome == "CONFIRMED_EXECUTABLE"
        or ltf_state in {"LTF_MODEL_CONFIRMED", "CONFIRMED"}
    ):
        return True

    return False


def _humanize_zone_type(value: Any) -> str:
    text = _safe_str(value, "")
    mapping = {
        "PREVIOUS_LOW": "previous low",
        "PREVIOUS_HIGH": "previous high",
        "VAL": "VAL",
        "VAH": "VAH",
        "POC": "POC",
        "NPOC": "nPOC",
    }
    return mapping.get(text.upper(), text.replace("_", " ").lower() if text else "реальна зона інтересу")


def _extract_target_zone_type(signal_payload: dict) -> str:
    direct = _safe_str(signal_payload.get("target_zone_type"), "")
    if direct not in {"", "-"}:
        return direct

    metadata = signal_payload.get("metadata")
    if isinstance(metadata, dict):
        diagnostics = metadata.get("diagnostics")
        if isinstance(diagnostics, dict):
            geometry = diagnostics.get("geometry")
            if isinstance(geometry, dict):
                value = _safe_str(geometry.get("target_zone_type"), "")
                if value not in {"", "-"}:
                    return value

        ltf_diagnostics = metadata.get("ltf_model_diagnostics")
        if isinstance(ltf_diagnostics, dict):
            geometry = ltf_diagnostics.get("geometry")
            if isinstance(geometry, dict):
                value = _safe_str(geometry.get("target_zone_type"), "")
                if value not in {"", "-"}:
                    return value

    return ""


def build_tpo_ltf_ready_reason_text(signal_payload: dict) -> str:
    direction = humanize_direction(signal_payload.get("direction"))
    timeframe = _safe_str(signal_payload.get("execution_timeframe"), "15m")
    selected_method = _safe_str(signal_payload.get("selected_method"), "")
    if selected_method in {"", "-"}:
        metadata = signal_payload.get("metadata")
        if isinstance(metadata, dict):
            diagnostics = metadata.get("diagnostics") or metadata.get("ltf_model_diagnostics")
            if isinstance(diagnostics, dict):
                displacement = diagnostics.get("displacement")
                if isinstance(displacement, dict):
                    selected_method = _safe_str(displacement.get("selected_method"), "")

    method_text = (
        f" через {selected_method}"
        if selected_method not in {"", "-"}
        else ""
    )

    target_source = _safe_str(signal_payload.get("target_source"), "")
    target_zone_type = _extract_target_zone_type(signal_payload)
    target_text = _humanize_zone_type(target_zone_type)

    if target_source == "interest_zone" or target_zone_type not in {"", "-"}:
        target_sentence = f"Ціль обрана з ринкової зони: {target_text}."
    else:
        target_sentence = "Ціль задана execution-планом; перевірити, що вона не synthetic."

    return (
        f"OPEN_TEST_DRIVE {direction} підтверджено на {timeframe} LTF-моделі{method_text}. "
        f"HTF bias узгоджений із напрямком. {target_sentence}"
    )


def build_tpo_ltf_ready_focus_text(signal_payload: dict) -> str:
    rr = _format_rr(signal_payload.get("risk_reward_ratio"))
    practical_rr = signal_payload.get("practical_rr")
    target_zone_type = _extract_target_zone_type(signal_payload)
    target_text = _humanize_zone_type(target_zone_type)

    parts = [
        "LTF-модель підтверджена; старі precondition-умови вже не актуальні.",
        f"У фокусі виконання за планом: stop, real target zone ({target_text}) і RR {rr}.",
    ]

    if practical_rr is not None and practical_rr != signal_payload.get("risk_reward_ratio"):
        parts.append(f"Практичний RR: {_format_rr(practical_rr)}.")

    return "\n".join(parts)


def build_tpo_ltf_ready_action_text(signal_payload: dict) -> str:
    execution_model = humanize_execution_model(signal_payload.get("execution_model"))
    return (
        f"Сценарій готовий. Execution model: {execution_model}. "
        "Працювати тільки за entry / invalidation / target. "
        "Після досягнення target не наздоганяти — нова структура має формувати новий сигнал."
    )


def build_reason_text(signal_payload: dict) -> str:
    if _is_tpo_ltf_ready(signal_payload):
        return build_tpo_ltf_ready_reason_text(signal_payload)

    scenario = _safe_str(signal_payload.get("scenario"), "")
    rationale = _safe_str(signal_payload.get("rationale"), "")
    reason = _safe_str(signal_payload.get("reason"), "")
    market_state = _safe_str(signal_payload.get("market_state"), "")
    htf_bias = _safe_str(signal_payload.get("htf_bias"), "")

    if rationale not in {"-", ""}:
        return rationale

    if scenario == "TREND_CONTINUATION_SHORT":
        return (
            "Є ведмежий контекст. Ринок у фазі continuation, "
            "але без підтвердженої структури входу."
        )

    if scenario == "TREND_CONTINUATION_LONG":
        return (
            "Є бичачий контекст. Ринок у фазі continuation, "
            "але без підтвердженої структури входу."
        )

    if scenario == "SWEEP_RETURN_LONG":
        return (
            "Ліквідність знизу зібрана. Є повернення у value "
            "і готовність до long-сценарію."
        )

    if scenario == "SWEEP_RETURN_SHORT":
        return (
            "Ліквідність зверху зібрана. Є повернення у value "
            "і готовність до short-сценарію."
        )

    if reason not in {"-", ""}:
        return reason

    return (
        f"Сформовано сценарій у контексті {humanize_market_state(market_state)} "
        f"при HTF bias: {humanize_bias(htf_bias)}."
    )


def build_missing_conditions_text(signal_payload: dict) -> str:
    if _is_tpo_ltf_ready(signal_payload):
        return build_tpo_ltf_ready_focus_text(signal_payload)

    missing = signal_payload.get("missing_conditions") or []

    if not missing:
        return "нічого критичного не бракує"

    human_map = {
        "impulse": "імпульсу",
        "pullback": "пулбеку",
        "entry_trigger": "тригера входу",
        "return_to_value": "повернення у value",
        "sweep": "sweep-події",
        "structure_confirmation": "підтвердження структури",
        "htf_alignment": "узгодження з HTF",
        "market_state": "відповідного market state",
        "execution_plan": "готового execution plan",
        "execution_plan_incomplete": "повного execution plan",
    }

    parts = [human_map.get(str(item), str(item)) for item in missing]
    return "\n".join(f"- {part}" for part in parts)


def build_action_text(signal_payload: dict) -> str:
    if _is_tpo_ltf_ready(signal_payload):
        return build_tpo_ltf_ready_action_text(signal_payload)

    stage = _extract_stage(signal_payload)
    direction = humanize_direction(signal_payload.get("direction"))
    scenario = _safe_str(signal_payload.get("scenario"), "")
    execution_status = _safe_str(signal_payload.get("execution_status"), "")
    execution_model = humanize_execution_model(signal_payload.get("execution_model"))
    trigger_reason = _safe_str(signal_payload.get("trigger_reason"), "")

    if stage == "SCENARIO_FORMING":
        return "Спостерігати. Без входу. Чекати підтвердження структури."

    if stage == "WATCH":
        if execution_status == "INCOMPLETE":
            return "Сценарій є, але execution ще не зібраний повністю. Без входу."

        if scenario.startswith("TREND_CONTINUATION"):
            return (
                f"Шукати continuation у бік {direction} "
                "після імпульсу і structure hold."
            )

        if scenario.startswith("SWEEP_RETURN"):
            return (
                f"Шукати підтвердження входу в бік {direction} "
                "після рейду і повернення у value."
            )

        return "Тримати фокус на підтвердженні сценарію."

    if stage == "READY":
        if trigger_reason not in {"", "-"}:
            return (
                f"Сценарій готовий. Execution model: {execution_model}. "
                f"Тригер: {trigger_reason}."
            )

        return (
            f"Сценарій готовий. Execution model: {execution_model}. "
            f"У фокусі вхід у бік {direction}."
        )

    if stage == "ACTIVE":
        return f"Ідея активна. Супровід у бік {direction}."

    if stage == "RESOLVED":
        return "Сигнал завершений. Потрібен розбір результату."

    return "Спостерігати за розвитком структури."


def build_levels_text(signal_payload: dict) -> str:
    invalidation = signal_payload.get("invalidation_reference_price")
    target = signal_payload.get("target_reference_price")
    entry = signal_payload.get("entry_reference_price")
    rr = signal_payload.get("risk_reward_ratio")
    practical_rr = signal_payload.get("practical_rr")
    execution_model = signal_payload.get("execution_model")
    execution_status = signal_payload.get("execution_status")
    execution_timeframe = _safe_str(signal_payload.get("execution_timeframe"), "")

    lines = [
        f"Модель входу: {humanize_execution_model(_safe_str(execution_model, 'NONE'))}",
        f"Статус виконання: {_safe_str(execution_status, 'NOT_EXECUTABLE')}",
        f"Вхід: {_format_price(entry)}",
        f"Інвалідація: {_format_price(invalidation)}",
        f"Ціль: {_format_price(target)}",
        f"RR: {_format_rr(rr)}",
    ]

    if practical_rr is not None and practical_rr != rr:
        lines.append(f"Практичний RR: {_format_rr(practical_rr)}")

    if execution_timeframe not in {"", "-"}:
        lines.append(f"ТФ виконання: {execution_timeframe}")

    return "\n".join(lines)


# =============================================================================
# FORMATTERS
# =============================================================================


def format_signal_message(signal_payload: dict) -> FormattedTelegramMessage:
    stage = _extract_stage(signal_payload)
    symbol = _safe_str(signal_payload.get("symbol"), "-")
    direction = humanize_direction(signal_payload.get("direction"))
    scenario = humanize_scenario_name(signal_payload.get("scenario"))
    confidence = _extract_confidence(signal_payload)
    market_state = humanize_market_state(signal_payload.get("market_state"))
    htf_bias = humanize_bias(signal_payload.get("htf_bias"))
    signal_id = signal_payload.get("signal_id")
    execution_status = _safe_str(
        signal_payload.get("execution_status"),
        "NOT_EXECUTABLE",
    )
    alignment_text = build_alignment_text(signal_payload)
    quality_tier_text = build_quality_tier_text(signal_payload)
    execution_warning = build_execution_warning_text(signal_payload)
    tpo_ltf_ready = _is_tpo_ltf_ready(signal_payload)
    focus_label = "Фокус:" if tpo_ltf_ready or not signal_payload.get("missing_conditions") else "Що бракує:"
    focus_text = (
        build_tpo_ltf_ready_focus_text(signal_payload)
        if tpo_ltf_ready
        else (
            build_missing_conditions_text(signal_payload)
            if signal_payload.get("missing_conditions")
            else build_action_text(signal_payload)
        )
    )

    title = f"{humanize_stage(stage)} | {symbol} | {direction}"

    body_parts = [
        alignment_text,
    ]

    if quality_tier_text:
        body_parts.append(quality_tier_text)

    body_parts.extend(
        [
            f"Сценарій: {scenario}",
            (
                f"Ринок: {market_state} | HTF: {htf_bias} | "
                f"Сила: {_format_pct(confidence * 100 if confidence <= 1 else confidence)}"
            ),
            "",
            "Картина:",
            build_reason_text(signal_payload),
            "",
            focus_label,
            focus_text,
            "",
            "План:",
            build_action_text(signal_payload),
            "",
            build_levels_text(signal_payload),
        ]
    )

    if execution_warning:
        body_parts.extend(["", "Попередження по виконанню:", execution_warning])

    if stage == "WATCH" and execution_status != "EXECUTABLE":
        body_parts.extend(
            [
                "",
                "Коментар:",
                "READY ще не підтверджено, бо execution-план неповний.",
            ]
        )

    if signal_id:
        body_parts.extend(["", f"ID: {signal_id}"])

    return FormattedTelegramMessage(
        message_type="signal_alert",
        title=title,
        body="\n".join(body_parts).strip(),
        signal_id=signal_id,
        symbol=symbol,
        stage=stage,
    )


def format_resolution_message(
    signal_payload: dict,
    resolution_payload: dict,
) -> FormattedTelegramMessage:
    symbol = _safe_str(signal_payload.get("symbol"), "-")
    scenario = humanize_scenario_name(signal_payload.get("scenario"))
    final_status = _safe_str(
        resolution_payload.get("final_status") or resolution_payload.get("resolution"),
        "UNKNOWN",
    )
    mfe_pct = resolution_payload.get("mfe_pct")
    mae_pct = resolution_payload.get("mae_pct")
    ttv = resolution_payload.get("time_to_validation_min")

    title = f"✅ RESOLVED | {symbol} | {final_status}"

    body_parts = [
        f"Сценарій: {scenario}",
        (
            "Причина: "
            f"{_safe_str(resolution_payload.get('resolution_reason') or resolution_payload.get('resolution_note'), '-')}"
        ),
        "",
        f"MFE: {_format_pct(mfe_pct)}",
        f"MAE: {_format_pct(mae_pct)}",
        f"Час до підтвердження: {ttv if ttv is not None else '-'} хв",
    ]

    return FormattedTelegramMessage(
        message_type="signal_resolution",
        title=title,
        body="\n".join(body_parts).strip(),
        signal_id=signal_payload.get("signal_id"),
        symbol=symbol,
        stage="RESOLVED",
    )


def format_cycle_summary_message(summary_payload: dict) -> FormattedTelegramMessage:
    title = "📊 CYCLE SUMMARY"

    system_metrics = summary_payload.get("system_metrics", {}) or {}
    signal_metrics = summary_payload.get("signal_metrics", {}) or {}

    avg_confidence = signal_metrics.get("avg_confidence", 0.0)
    avg_confidence_pct = (
        avg_confidence * 100
        if avg_confidence <= 1
        else avg_confidence
    )

    body_parts = [
        f"Cycles total: {system_metrics.get('cycles_total', 0)}",
        f"Cycles OK: {system_metrics.get('cycles_ok', 0)}",
        f"Cycle errors: {system_metrics.get('cycles_with_errors', 0)}",
        f"Signals total: {signal_metrics.get('signals_total', 0)}",
        f"Executable: {signal_metrics.get('executable_signals', 0)}",
        f"Validated: {signal_metrics.get('validated_total', 0)}",
        f"Invalidated: {signal_metrics.get('invalidated_total', 0)}",
        f"Expired: {signal_metrics.get('expired_total', 0)}",
        f"Avg confidence: {_format_pct(avg_confidence_pct)}",
        f"Avg RR: {_format_rr(signal_metrics.get('avg_rr'))}",
        f"Avg MFE: {_format_pct(signal_metrics.get('avg_mfe_pct', 0.0))}",
        f"Avg MAE: {_format_pct(signal_metrics.get('avg_mae_pct', 0.0))}",
    ]

    return FormattedTelegramMessage(
        message_type="cycle_summary",
        title=title,
        body="\n".join(body_parts).strip(),
        signal_id=None,
        symbol=None,
        stage=None,
    )