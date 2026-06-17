from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib import error, parse, request

from app.services.battle_permission import apply_battle_permission
from app.services.battle_permission_telemetry import record_battle_permission_event
from app.services.telegram_formatter import format_signal_message


logger = logging.getLogger(__name__)

TELEGRAM_NOTIFIER_VERSION = "telegram-notifier-v1.8-telemetry-runtime-version-forwarding"


# =============================================================================
# TELEGRAM NOTIFIER CONFIG / HELPERS
# =============================================================================

MIN_STOP_DISTANCE_BY_SYMBOL: dict[str, float] = {
    "XAUUSD": 15.0,
    "BTCUSD": 100.0,
    "ETHUSD": 8.0,
    "EURUSD": 0.0005,
    "GBPUSD": 0.0007,
    "AUDUSD": 0.0005,
    "USDJPY": 0.08,
    "USDCHF": 0.0005,
    "USDCAD": 0.0007,
    "GER40": 25.0,
    "NAS100": 35.0,
    "SPX500": 8.0,
    "UKOIL": 0.25,
}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if value == "":
            continue
        if value == []:
            continue
        if value == {}:
            continue
        return value
    return None


def _format_percent(value: Any) -> str:
    number = _safe_float_or_none(value)
    if number is None:
        return "-"

    if number <= 1:
        return f"{number * 100:.0f}%"

    return f"{number:.0f}%"


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3] + "..."


def _escape_html(text: Any) -> str:
    if text is None:
        return "-"
    s = str(text)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _as_upper(value: Any) -> str | None:
    if value in (None, "", [], {}):
        return None
    return str(value).strip().upper()


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value

    if value in (None, "", [], {}):
        return None

    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False

    normalized = str(value).strip().lower()
    if normalized in {"true", "yes", "y", "1", "on", "damaged"}:
        return True
    if normalized in {"false", "no", "n", "0", "off", "clean", "ok"}:
        return False

    return None


def _deep_get(data: dict[str, Any], path: str) -> Any:
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _is_present(value: Any) -> bool:
    return value not in (None, "", [], {})


def _iter_payload_contexts(payload: Dict[str, Any]) -> list[dict[str, Any]]:
    """
    Return payload dictionaries that may contain market/TPO/Battle fields.

    Historically some fields were stored at root, some in metadata, and some
    inside context.auction.context / context.auction.filters. Telemetry should
    not write null just because the field is nested one level deeper.
    """
    contexts: list[dict[str, Any]] = []
    seen: set[int] = set()

    def add(value: Any) -> None:
        if not isinstance(value, dict):
            return
        marker = id(value)
        if marker in seen:
            return
        seen.add(marker)
        contexts.append(value)

    add(payload)

    metadata = payload.get("metadata")
    add(metadata)

    for root in (payload, metadata if isinstance(metadata, dict) else {}):
        add(root.get("auction_filters"))
        add(root.get("auction_context"))
        add(root.get("tpo_context"))
        add(root.get("battle_context"))
        add(root.get("safety_context"))
        add(root.get("post_news_context"))

        auction = root.get("auction")
        add(auction)
        if isinstance(auction, dict):
            add(auction.get("filters"))
            add(auction.get("context"))

        context = root.get("context")
        add(context)
        if isinstance(context, dict):
            context_auction = context.get("auction")
            add(context_auction)
            if isinstance(context_auction, dict):
                add(context_auction.get("filters"))
                add(context_auction.get("context"))

    return contexts


def _payload_get(payload: Dict[str, Any], *paths: str) -> Any:
    for path in paths:
        for context in _iter_payload_contexts(payload):
            value = _deep_get(context, path)
            if _is_present(value):
                return value

    return None


def _as_text_list(value: Any) -> list[str]:
    if value in (None, "", [], {}):
        return []

    if isinstance(value, (list, tuple, set)):
        result: list[str] = []
        for item in value:
            if item in (None, "", [], {}):
                continue
            result.append(str(item).strip())
        return [x for x in result if x]

    if isinstance(value, dict):
        try:
            return [json.dumps(value, ensure_ascii=False, sort_keys=True)]
        except Exception:
            return [str(value)]

    return [str(value).strip()] if str(value).strip() else []


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        key = item.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(item.strip())
    return result


def _normalize_target_zone(value: Any) -> str:
    if value is None:
        return "-"

    if isinstance(value, (list, tuple)):
        cleaned = [str(x).strip() for x in value if str(x).strip()]
        return ", ".join(_escape_html(x) for x in cleaned) if cleaned else "-"

    if isinstance(value, dict):
        try:
            return _escape_html(json.dumps(value, ensure_ascii=False, sort_keys=True))
        except Exception:
            return _escape_html(str(value))

    text = str(value).strip()
    return _escape_html(text) if text else "-"


def _normalize_direction(value: Any) -> str:
    direction = str(value or "NEUTRAL").strip().upper()
    if direction in {"LONG", "SHORT", "NEUTRAL"}:
        return direction
    if direction in {"BUY", "BULL", "BULLISH", "UP"}:
        return "LONG"
    if direction in {"SELL", "BEAR", "BEARISH", "DOWN"}:
        return "SHORT"
    return "NEUTRAL"


def _normalize_htf_bias(value: Any) -> str:
    htf_bias = str(value or "NEUTRAL").strip().upper()
    if htf_bias in {"LONG", "SHORT", "NEUTRAL"}:
        return htf_bias
    if htf_bias in {"BUY", "BULL", "BULLISH", "UP"}:
        return "LONG"
    if htf_bias in {"SELL", "BEAR", "BEARISH", "DOWN"}:
        return "SHORT"
    return "NEUTRAL"


def _derive_signal_alignment(direction: Any, htf_bias: Any) -> tuple[str, str, str]:
    d = _normalize_direction(direction)
    h = _normalize_htf_bias(htf_bias)

    if d not in {"LONG", "SHORT"}:
        return "NO_DIRECTION", "⚫", "NO DIRECTION"

    if h == "NEUTRAL":
        return "NEUTRAL_HTF", "⚪", "NEUTRAL HTF"

    if h not in {"LONG", "SHORT"}:
        return "UNKNOWN_HTF", "⚫", "UNKNOWN HTF"

    if d == h:
        return "TREND_ALIGNED", "🟢", "TREND-ALIGNED"

    return "COUNTER_TREND", "🔴", "COUNTER-TREND"


def _derive_stop_quality(
    *,
    symbol: Any,
    entry: Any,
    stop: Any,
    target: Any,
    rr: Any,
) -> tuple[str, str, float | None, float | None]:
    """
    Returns:
    - stop_quality
    - stop_quality_reason
    - theoretical_rr
    - practical_rr
    """
    symbol_text = str(symbol or "").strip().upper()
    entry_f = _safe_float_or_none(entry)
    stop_f = _safe_float_or_none(stop)
    target_f = _safe_float_or_none(target)
    rr_f = _safe_float_or_none(rr)

    theoretical_rr = rr_f

    if entry_f is None or stop_f is None or target_f is None:
        return "UNKNOWN", "missing entry/stop/target", theoretical_rr, None

    stop_distance = abs(entry_f - stop_f)
    target_distance = abs(target_f - entry_f)

    if stop_distance <= 0:
        return "INVALID", "stop distance is zero or negative", theoretical_rr, None

    min_stop = MIN_STOP_DISTANCE_BY_SYMBOL.get(symbol_text)

    if min_stop is None:
        return (
            "OK",
            "no instrument-specific practical stop threshold",
            theoretical_rr,
            theoretical_rr,
        )

    if stop_distance < min_stop:
        practical_rr = round(target_distance / min_stop, 3) if min_stop > 0 else None
        return (
            "TIGHT_STOP",
            f"stop_distance {stop_distance:.5f} below practical_min_stop {min_stop:.5f}",
            theoretical_rr,
            practical_rr,
        )

    return (
        "OK",
        f"stop_distance {stop_distance:.5f} >= practical_min_stop {min_stop:.5f}",
        theoretical_rr,
        theoretical_rr,
    )


def _infer_alert_type(payload: Dict[str, Any]) -> str:
    """
    Infer Telegram alert type from payload state.

    This function is intentionally module-level because both the class method
    and standalone helper may use it.
    """
    explicit = str(payload.get("alert_type", "")).strip().upper()
    if explicit:
        return explicit

    signal_class = str(
        payload.get("signal_class")
        or payload.get("stage")
        or payload.get("current_stage")
        or ""
    ).strip().upper()

    execution_status = str(payload.get("execution_status") or "").strip().upper()

    if signal_class == "ACTIVE":
        return "TRIGGERED"

    if signal_class == "READY":
        return "ENTRY_READY"

    if execution_status == "EXECUTABLE":
        return "ENTRY_READY"

    if signal_class == "WATCH":
        return "WATCH_NEW"

    if signal_class == "RESOLVED":
        resolution = str(payload.get("resolution") or payload.get("resolution_reason") or "").upper()
        if resolution == "INVALIDATED":
            return "INVALIDATED"

    return ""


def _copy_metadata_aliases(normalized: Dict[str, Any]) -> None:
    """
    Keep the formatter tolerant to fields attached either at root or metadata.

    battle_permission.apply_battle_permission() writes most v1.5 fields at root,
    but older payloads or journal replays may only have them under metadata.
    """
    metadata = normalized.get("metadata")
    if not isinstance(metadata, dict):
        return

    aliases = (
        "battle_permission",
        "telegram_delivery_mode",
        "battle_ready",
        "auction_context_score",
        "risk_mode",
        "battle_risk_mode",
        "scenario_family",
        "news_risk_state",
        "news_provider_status",
        "local_structure_damaged",
        "target_quality",
        "caution_flags",
        "risk_flags",
        "safety_flags",
        "battle_permission_modifiers",
        "battle_permission_blockers",
        "battle_permission_reasons",
        "battle_gate_v2_risk_mode",
        "battle_gate_v2_decision",
        "battle_gate_v2_modifiers",
        "battle_gate_v2_blockers",
        "battle_gate_v2_reasons",
        "battle_permission_version",
        "runner_version",
        "stateful_runner_version",
        "telegram_notifier_version",
        "source_component_version",
    )

    for key in aliases:
        if normalized.get(key) in (None, "", [], {}) and metadata.get(key) not in (None, "", [], {}):
            normalized[key] = metadata.get(key)

    if normalized.get("risk_mode") in (None, "", [], {}):
        normalized["risk_mode"] = _first_present(
            metadata.get("battle_risk_mode"),
            metadata.get("battle_gate_v2_risk_mode"),
            normalized.get("battle_gate_v2_risk_mode"),
        )


def _copy_battle_telemetry_aliases(normalized: Dict[str, Any]) -> None:
    """
    Flatten safety/TPO fields before telemetry is written.

    record_battle_permission_event() receives one payload and historically reads
    mostly root-level keys. This helper makes sure fields that already exist in
    metadata, auction_filters, auction_context, context.auction.context, or
    context.auction.filters are also available at root.
    """

    def set_if_missing(key: str, *paths: str) -> None:
        if _is_present(normalized.get(key)):
            return
        value = _payload_get(normalized, *(paths or (key,)))
        if _is_present(value):
            normalized[key] = value

    set_if_missing("battle_permission")
    set_if_missing("telegram_delivery_mode")
    set_if_missing("battle_ready")
    set_if_missing("auction_context_score")
    set_if_missing("risk_mode", "risk_mode", "battle_risk_mode", "battle_gate_v2_risk_mode")
    set_if_missing("scenario_family")
    set_if_missing("news_risk_state")
    set_if_missing("news_provider_status")
    set_if_missing("local_structure_damaged")
    set_if_missing("target_quality")
    set_if_missing("caution_flags")
    set_if_missing("risk_flags")
    set_if_missing("safety_flags")
    set_if_missing("session_label")
    set_if_missing("battle_permission_modifiers")
    set_if_missing("battle_permission_blockers")
    set_if_missing("battle_permission_reasons")
    set_if_missing("battle_permission_version")
    set_if_missing("runner_version")
    set_if_missing("stateful_runner_version")
    set_if_missing("telegram_notifier_version")
    set_if_missing("source_component_version")
    set_if_missing("battle_gate_v2_decision")
    set_if_missing("battle_gate_v2_risk_mode")
    set_if_missing("battle_gate_v2_modifiers")
    set_if_missing("battle_gate_v2_blockers")
    set_if_missing("battle_gate_v2_reasons")

    set_if_missing("market_is_open")
    set_if_missing("market_status")
    set_if_missing("market_closed_reason")
    set_if_missing("market_holiday_name")
    set_if_missing("market_data_is_stale")
    set_if_missing("market_data_age_minutes")
    set_if_missing("last_bar_timestamp_utc")
    set_if_missing("stale_bar_threshold_minutes")
    set_if_missing("tpo_signal_permission")
    set_if_missing("tpo_source")
    set_if_missing("provider_error")
    set_if_missing("fallback_preserved_previous_context")
    set_if_missing("tpo_telegram_modifier", "tpo_telegram_modifier", "telegram_modifier")
    set_if_missing("open_relation")
    set_if_missing("auction_bias")
    set_if_missing("open_context")
    set_if_missing("open_behavior")
    set_if_missing("open_behavior_confidence")
    set_if_missing("entry_model_hint")
    set_if_missing("stop_model_hint")
    set_if_missing("battle_bias_hint")
    set_if_missing("open_behavior_reason")
    set_if_missing("session_anchor")
    set_if_missing("session_timezone")
    set_if_missing("session_open_utc")
    set_if_missing("session_open_kyiv")
    set_if_missing("current_session_id")
    set_if_missing("previous_session_id")
    set_if_missing("nearest_npoc")
    set_if_missing("nearest_npoc_distance")
    set_if_missing("ib_high")
    set_if_missing("ib_low")
    set_if_missing("ib_range")
    set_if_missing("ib_extension_up_pct")
    set_if_missing("ib_extension_down_pct")
    set_if_missing("first_hour_activity")

    primary_zone = _payload_get(normalized, "primary_interest_zone")
    if isinstance(primary_zone, dict):
        if not _is_present(normalized.get("primary_interest_zone")):
            normalized["primary_interest_zone"] = primary_zone
        if not _is_present(normalized.get("interest_zone_type")):
            normalized["interest_zone_type"] = primary_zone.get("zone_type")
        if not _is_present(normalized.get("interest_zone_price")):
            normalized["interest_zone_price"] = primary_zone.get("price")
        if not _is_present(normalized.get("interest_zone_role")):
            normalized["interest_zone_role"] = primary_zone.get("role")
        if not _is_present(normalized.get("interest_zone_reaction")):
            normalized["interest_zone_reaction"] = primary_zone.get("reaction")
        if not _is_present(normalized.get("interest_zone_reason")):
            normalized["interest_zone_reason"] = primary_zone.get("reason")
    else:
        set_if_missing("primary_interest_zone")

    set_if_missing("interest_zone_type")
    set_if_missing("interest_zone_price")
    set_if_missing("interest_zone_role")
    set_if_missing("interest_zone_reaction")

    # Forward-compatible fields for post_news_continuation_detector.
    set_if_missing("post_news_regime")
    set_if_missing("post_news_elapsed_minutes")
    set_if_missing("post_news_impulse_direction")
    set_if_missing("post_news_impulse_confirmed")
    set_if_missing("post_news_retest_level")
    set_if_missing("post_news_retest_status")
    set_if_missing("post_news_acceptance_status")
    set_if_missing("post_news_failed_move")
    set_if_missing("post_news_continuation_quality")
    set_if_missing("post_news_continuation_direction")
    set_if_missing("post_news_trade_permission")

    if not _is_present(normalized.get("risk_mode")):
        normalized["risk_mode"] = _first_present(
            normalized.get("battle_risk_mode"),
            normalized.get("battle_gate_v2_risk_mode"),
        )


def _normalize_alert_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize alert payload v3 into Telegram-compatible fields.

    Keeps original keys and adds compatibility aliases expected by both:
    - legacy HTML formatter
    - Ukrainian telegram_formatter.py

    Important:
    - trade_confidence is the value that explains why signal passed the Telegram gate.
    - scenario_probability is the scenario/background probability and can be lower.
    - Telegram displays both separately to avoid misleading "Probability: 45%" on READY signals.
    """
    normalized = dict(payload)
    _copy_metadata_aliases(normalized)
    _copy_battle_telemetry_aliases(normalized)

    alert_type = _infer_alert_type(normalized)
    if alert_type:
        normalized["alert_type"] = alert_type

    scenario_value = _first_present(
        normalized.get("scenario"),
        normalized.get("scenario_type"),
        "UNKNOWN",
    )

    normalized.setdefault("scenario", scenario_value)
    normalized.setdefault("scenario_type", scenario_value)

    normalized.setdefault(
        "watch_reason",
        normalized.get("rationale") or normalized.get("reason") or "-",
    )

    normalized.setdefault(
        "trade_confidence",
        _first_present(
            normalized.get("confidence"),
            normalized.get("probability"),
            normalized.get("trade_probability"),
            normalized.get("signal_confidence"),
        ),
    )

    normalized.setdefault(
        "scenario_probability",
        _first_present(
            normalized.get("scenario_probability"),
            normalized.get("setup_probability"),
            normalized.get("scenario_confidence"),
        ),
    )

    normalized.setdefault(
        "invalidation_level",
        normalized.get("invalidation_reference_price"),
    )

    if "target_zone" not in normalized:
        target_for_zone = normalized.get("target_reference_price")
        normalized["target_zone"] = [target_for_zone] if target_for_zone is not None else []

    direction = normalized.get("direction")
    htf_bias = normalized.get("htf_bias")

    signal_alignment, signal_alignment_marker, signal_alignment_label = _derive_signal_alignment(
        direction,
        htf_bias,
    )

    normalized.setdefault("signal_alignment", signal_alignment)
    normalized.setdefault("signal_alignment_marker", signal_alignment_marker)
    normalized.setdefault("signal_alignment_label", signal_alignment_label)

    entry = _first_present(
        normalized.get("entry_reference_price"),
        normalized.get("entry"),
    )
    stop = _first_present(
        normalized.get("invalidation_reference_price"),
        normalized.get("stop_loss"),
        normalized.get("stop"),
        normalized.get("invalidation_level"),
    )
    target = _first_present(
        normalized.get("target_reference_price"),
        normalized.get("take_profit"),
        normalized.get("target"),
    )
    rr = _first_present(
        normalized.get("risk_reward_ratio"),
        normalized.get("rr"),
        normalized.get("risk_reward"),
    )

    if entry is not None:
        normalized.setdefault("entry_reference_price", entry)

    if stop is not None:
        normalized.setdefault("invalidation_reference_price", stop)
        normalized.setdefault("stop", stop)

    if target is not None:
        normalized.setdefault("target_reference_price", target)
        normalized.setdefault("target", target)

    if rr is not None:
        normalized.setdefault("risk_reward_ratio", rr)

    stop_quality, stop_quality_reason, theoretical_rr, practical_rr = _derive_stop_quality(
        symbol=normalized.get("symbol"),
        entry=entry,
        stop=stop,
        target=target,
        rr=rr,
    )

    normalized.setdefault("stop_quality", stop_quality)
    normalized.setdefault("stop_quality_reason", stop_quality_reason)
    normalized.setdefault("theoretical_rr", theoretical_rr)
    normalized.setdefault("practical_rr", practical_rr)

    # Formatter compatibility.
    # telegram_formatter.py uses stage/signal_class and execution_status to render READY/WATCH.
    if "stage" not in normalized:
        if str(normalized.get("signal_class") or "").strip():
            normalized["stage"] = normalized.get("signal_class")
        elif normalized.get("alert_type") == "ENTRY_READY":
            normalized["stage"] = "READY"

    if "rationale" not in normalized and normalized.get("watch_reason"):
        normalized["rationale"] = normalized.get("watch_reason")

    _copy_metadata_aliases(normalized)
    _copy_battle_telemetry_aliases(normalized)
    return normalized


def _collect_caution_flags(payload: Dict[str, Any]) -> list[str]:
    flags: list[str] = []

    for key in ("caution_flags", "risk_flags", "safety_flags"):
        flags.extend(_as_text_list(_payload_get(payload, key)))

    # battle_permission_modifiers are not all warnings, but for CAUTION_BATTLE they explain why
    # the user should not read the alert as a clean green battle signal.
    flags.extend(_as_text_list(_payload_get(payload, "battle_permission_modifiers")))
    flags.extend(_as_text_list(_payload_get(payload, "battle_gate_v2_modifiers")))

    return _dedupe_keep_order(flags)


def _is_provider_unavailable(value: Any) -> bool:
    normalized = _as_upper(value)
    if not normalized:
        return False
    return any(token in normalized for token in ("UNAVAILABLE", "FAILED", "ERROR", "MISSING"))


def _build_battle_safety_lines(payload: Dict[str, Any]) -> list[str]:
    """
    Human-facing Battle Gate context block.

    The goal is not to duplicate all metadata. The goal is to prevent Telegram from
    presenting CAUTION_BATTLE / post-news reclaim / local damage as a clean trend battle.
    """
    battle_permission = _as_upper(_payload_get(payload, "battle_permission"))
    risk_mode = _as_upper(
        _first_present(
            _payload_get(payload, "risk_mode"),
            _payload_get(payload, "battle_risk_mode"),
            _payload_get(payload, "battle_gate_v2_risk_mode"),
        )
    )
    scenario_family = _as_upper(_payload_get(payload, "scenario_family"))
    news_risk_state = _as_upper(_payload_get(payload, "news_risk_state"))
    news_provider_status = _as_upper(_payload_get(payload, "news_provider_status"))
    local_structure_damaged = _as_bool(_payload_get(payload, "local_structure_damaged"))
    target_quality = _as_upper(_payload_get(payload, "target_quality"))
    auction_context_score = _payload_get(payload, "auction_context_score")
    flags = _collect_caution_flags(payload)

    should_render = (
        battle_permission == "CAUTION_BATTLE"
        or risk_mode in {"CAUTION", "CAUTION_BATTLE", "TRANSITION_CANDIDATE", "POST_NEWS_CAUTION"}
        or scenario_family in {"POST_NEWS_RECLAIM", "POST_LIQUIDATION_RECLAIM"}
        or local_structure_damaged is True
        or _is_provider_unavailable(news_risk_state)
        or _is_provider_unavailable(news_provider_status)
        or target_quality in {"SYNTHETIC", "UNKNOWN"}
        or bool(flags)
    )

    if not should_render:
        return []

    lines: list[str] = ["🛡 Battle Gate / Safety"]

    if battle_permission:
        if battle_permission == "CAUTION_BATTLE":
            lines.append("Режим: 🟡 CAUTION_BATTLE — сигнал дозволений, але це не clean battle.")
        elif battle_permission == "BATTLE_READY":
            lines.append("Режим: 🟢 BATTLE_READY")
        else:
            lines.append(f"Режим: {battle_permission}")

    if risk_mode:
        lines.append(f"Risk mode: {risk_mode}")

    if scenario_family:
        if scenario_family in {"POST_NEWS_RECLAIM", "POST_LIQUIDATION_RECLAIM"}:
            lines.append(f"Scenario family: {scenario_family} — reclaim/mean-reversion, не чисте trend continuation.")
        else:
            lines.append(f"Scenario family: {scenario_family}")

    if news_risk_state:
        lines.append(f"News risk: {news_risk_state}")

    if news_provider_status and news_provider_status != news_risk_state:
        lines.append(f"News provider: {news_provider_status}")

    if local_structure_damaged is True:
        lines.append("Local structure: DAMAGED після імпульсу / ліквідації.")
    elif local_structure_damaged is False and battle_permission == "CAUTION_BATTLE":
        lines.append("Local structure: not marked damaged")

    if target_quality:
        if target_quality == "REAL_ZONE":
            lines.append("Target quality: REAL_ZONE")
        elif target_quality == "SYNTHETIC":
            lines.append("Target quality: SYNTHETIC — battle має бути понижений.")
        else:
            lines.append(f"Target quality: {target_quality}")

    if auction_context_score not in (None, ""):
        lines.append(f"Auction score: {auction_context_score}")

    if flags:
        shown_flags = flags[:6]
        suffix = "" if len(flags) <= 6 else f" +{len(flags) - 6} more"
        lines.append("Flags: " + ", ".join(shown_flags) + suffix)

    if battle_permission == "CAUTION_BATTLE" or scenario_family in {"POST_NEWS_RECLAIM", "POST_LIQUIDATION_RECLAIM"}:
        lines.append("Management: працювати тільки за entry/SL/target; після near-target не наздоганяти.")

    return lines


def _inject_telemetry_runtime_versions(normalized: Dict[str, Any]) -> None:
    """
    Add runtime/component version fields before Battle telemetry is written.

    Upstream runners may not always pass runner_version. In that case we keep
    telemetry non-empty by using the notifier version as a clear component
    fallback, while still allowing real runner_version/stateful_runner_version
    to override it when present.
    """
    metadata = normalized.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        normalized["metadata"] = metadata

    if not _is_present(normalized.get("telegram_notifier_version")):
        normalized["telegram_notifier_version"] = TELEGRAM_NOTIFIER_VERSION

    if not _is_present(metadata.get("telegram_notifier_version")):
        metadata["telegram_notifier_version"] = TELEGRAM_NOTIFIER_VERSION

    if not _is_present(normalized.get("source_component_version")):
        normalized["source_component_version"] = TELEGRAM_NOTIFIER_VERSION

    if not _is_present(metadata.get("source_component_version")):
        metadata["source_component_version"] = TELEGRAM_NOTIFIER_VERSION

    runner_version = _first_present(
        normalized.get("runner_version"),
        normalized.get("stateful_runner_version"),
        metadata.get("runner_version"),
        metadata.get("stateful_runner_version"),
        os.getenv("AI_MARKET_ANALYST_RUNNER_VERSION"),
        os.getenv("STATEFUL_RUNNER_VERSION"),
        os.getenv("RUNNER_VERSION"),
        os.getenv("APP_RUNNER_VERSION"),
        TELEGRAM_NOTIFIER_VERSION,
    )

    if _is_present(runner_version):
        normalized["runner_version"] = runner_version
        if not _is_present(metadata.get("runner_version")):
            metadata["runner_version"] = runner_version

def _record_battle_permission_event_enriched(
    payload: Dict[str, Any],
    *,
    source: str,
    sent_to_telegram: bool,
    note: str,
) -> None:
    """Write Battle telemetry with root-level safety/TPO aliases populated."""
    telemetry_payload = _normalize_alert_payload(dict(payload))
    _copy_battle_telemetry_aliases(telemetry_payload)
    _inject_telemetry_runtime_versions(telemetry_payload)
    record_battle_permission_event(
        telemetry_payload,
        source=source,
        sent_to_telegram=sent_to_telegram,
        note=note,
    )


@dataclass
class TelegramConfig:
    enabled: bool
    bot_token: str
    chat_id: str
    parse_mode: str = "HTML"
    disable_web_page_preview: bool = True
    timeout_seconds: int = 10
    retries: int = 3
    retry_delay_seconds: float = 2.0
    paper_mode_prefix: str = "🧪 PAPER"
    live_mode_prefix: str = "🚨 LIVE"
    max_message_length: int = 3900
    use_ukrainian_formatter: bool = True
    allowed_alert_types: tuple[str, ...] = (
        "WATCH_NEW",
        "WATCH_UPGRADED",
        "TRIGGERED",
        "ENTRY_READY",
        "INVALIDATED",
    )


class TelegramNotifier:
    """
    Production-ready Telegram notifier for AI Market Analyst.

    Supported interfaces:
    - send_text(text)
    - send_admin_message(text)
    - send_alert(payload)
    - send_alert_payload(payload)

    Compatibility properties:
    - is_enabled
    - is_active
    """

    def __init__(self, config: Optional[TelegramConfig] = None) -> None:
        self.config = config or TelegramConfig(
            enabled=_env_bool("TELEGRAM_ENABLED", False),
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            parse_mode=(os.getenv("TELEGRAM_PARSE_MODE", "HTML").strip() or "HTML"),
            timeout_seconds=_safe_int(os.getenv("TELEGRAM_TIMEOUT_SECONDS", "10"), 10),
            retries=_safe_int(os.getenv("TELEGRAM_RETRIES", "3"), 3),
            retry_delay_seconds=float(os.getenv("TELEGRAM_RETRY_DELAY_SECONDS", "2")),
            paper_mode_prefix=os.getenv("TELEGRAM_PAPER_PREFIX", "🧪 PAPER").strip() or "🧪 PAPER",
            live_mode_prefix=os.getenv("TELEGRAM_LIVE_PREFIX", "🚨 LIVE").strip() or "🚨 LIVE",
            max_message_length=_safe_int(os.getenv("TELEGRAM_MAX_MESSAGE_LENGTH", "3900"), 3900),
            use_ukrainian_formatter=_env_bool("ENABLE_UKRAINIAN_TELEGRAM_FORMATTER", True),
        )

    @property
    def is_enabled(self) -> bool:
        return (
            self.config.enabled
            and bool(self.config.bot_token)
            and bool(self.config.chat_id)
        )

    @property
    def is_active(self) -> bool:
        return self.is_enabled

    def send_text(self, text: str) -> bool:
        if not self.is_enabled:
            logger.info("Telegram notifier disabled or not configured.")
            return False

        safe_text = _truncate(text, self.config.max_message_length)
        url = f"https://api.telegram.org/bot{self.config.bot_token}/sendMessage"

        payload = {
            "chat_id": self.config.chat_id,
            "text": safe_text,
            "parse_mode": self.config.parse_mode,
            "disable_web_page_preview": self.config.disable_web_page_preview,
        }

        encoded = parse.urlencode(payload).encode("utf-8")
        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.retries + 1):
            try:
                req = request.Request(url, data=encoded, method="POST")
                with request.urlopen(req, timeout=self.config.timeout_seconds) as resp:
                    body = resp.read().decode("utf-8", errors="replace")

                    if 200 <= resp.status < 300:
                        logger.info(
                            "Telegram message sent successfully. attempt=%s status=%s",
                            attempt,
                            resp.status,
                        )
                        logger.debug("Telegram response body=%s", body)
                        return True

                    logger.error(
                        "Telegram send failed. attempt=%s status=%s body=%s",
                        attempt,
                        resp.status,
                        body,
                    )

            except error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
                logger.error(
                    "Telegram HTTPError. attempt=%s/%s code=%s body=%s",
                    attempt,
                    self.config.retries,
                    exc.code,
                    body,
                )
                last_error = exc

            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Telegram send exception. attempt=%s/%s",
                    attempt,
                    self.config.retries,
                )
                last_error = exc

            if attempt < self.config.retries:
                time.sleep(self.config.retry_delay_seconds)

        logger.error("Telegram message failed after retries. last_error=%s", last_error)
        return False

    def send_admin_message(self, text: str) -> bool:
        return self.send_text(text)

    def send_alert(self, payload: Dict[str, Any]) -> bool:
        return self.send_alert_payload(payload)

    def send_alert_payload(self, payload: Dict[str, Any]) -> bool:
        if not isinstance(payload, dict):
            logger.warning("send_alert_payload received non-dict payload.")
            return False

        should_alert = bool(payload.get("should_alert", False))
        if not should_alert:
            logger.debug("alert_payload.should_alert=False, Telegram send skipped.")
            return False

        normalized_payload = _normalize_alert_payload(payload)
        alert_type = str(normalized_payload.get("alert_type", "")).strip().upper()

        if not alert_type:
            logger.info(
                "Alert type missing and could not be inferred. symbol=%s signal_class=%s execution_status=%s",
                normalized_payload.get("symbol"),
                normalized_payload.get("signal_class"),
                normalized_payload.get("execution_status"),
            )
            return False

        if self.config.allowed_alert_types and alert_type not in self.config.allowed_alert_types:
            logger.info(
                "Alert type not allowed for Telegram delivery. alert_type=%s symbol=%s signal_id=%s",
                alert_type,
                normalized_payload.get("symbol"),
                normalized_payload.get("signal_id"),
            )
            return False

        try:
            normalized_payload = apply_battle_permission(normalized_payload)
            normalized_payload = _normalize_alert_payload(normalized_payload)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Battle permission evaluation failed. Telegram alert suppressed for safety. "
                "symbol=%s alert_type=%s signal_id=%s error=%s",
                normalized_payload.get("symbol"),
                alert_type,
                normalized_payload.get("signal_id"),
                exc,
            )
            return False

        telegram_delivery_mode = str(
            normalized_payload.get("telegram_delivery_mode") or ""
        ).strip().upper()

        battle_permission = str(
            normalized_payload.get("battle_permission") or ""
        ).strip().upper()

        battle_ready = bool(normalized_payload.get("battle_ready", False))
        auction_context_score = normalized_payload.get("auction_context_score")

        metadata = normalized_payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}

        battle_blockers = metadata.get("battle_permission_blockers") or normalized_payload.get("battle_permission_blockers") or []
        battle_reasons = metadata.get("battle_permission_reasons") or normalized_payload.get("battle_permission_reasons") or []
        risk_mode = _payload_get(normalized_payload, "risk_mode", "battle_risk_mode", "battle_gate_v2_risk_mode")
        scenario_family = _payload_get(normalized_payload, "scenario_family")
        news_risk_state = _payload_get(normalized_payload, "news_risk_state")
        local_structure_damaged = _payload_get(normalized_payload, "local_structure_damaged")
        target_quality = _payload_get(normalized_payload, "target_quality")
        caution_flags = _collect_caution_flags(normalized_payload)

        if telegram_delivery_mode != "BATTLE_ALERT" or not battle_ready:
            _record_battle_permission_event_enriched(
                normalized_payload,
                source="telegram_notifier",
                sent_to_telegram=False,
                note="suppressed_by_battle_permission",
            )

            logger.info(
                "Telegram alert suppressed by battle permission. "
                "symbol=%s alert_type=%s signal_id=%s battle_permission=%s "
                "delivery_mode=%s score=%s risk_mode=%s scenario_family=%s blockers=%s reasons=%s",
                normalized_payload.get("symbol"),
                alert_type,
                normalized_payload.get("signal_id"),
                battle_permission,
                telegram_delivery_mode,
                auction_context_score,
                risk_mode,
                scenario_family,
                battle_blockers,
                battle_reasons[:5] if isinstance(battle_reasons, list) else battle_reasons,
            )
            return False

        message = self.format_alert_payload(normalized_payload)

        logger.info(
            "Sending Telegram alert. symbol=%s alert_type=%s signal_id=%s alignment=%s "
            "stop_quality=%s practical_rr=%s trade_confidence=%s scenario_probability=%s "
            "battle_permission=%s risk_mode=%s scenario_family=%s news_risk_state=%s "
            "local_structure_damaged=%s target_quality=%s caution_flags=%s auction_context_score=%s formatter=%s version=%s",
            normalized_payload.get("symbol"),
            alert_type,
            normalized_payload.get("signal_id"),
            normalized_payload.get("signal_alignment"),
            normalized_payload.get("stop_quality"),
            normalized_payload.get("practical_rr"),
            normalized_payload.get("trade_confidence"),
            normalized_payload.get("scenario_probability"),
            battle_permission,
            risk_mode,
            scenario_family,
            news_risk_state,
            local_structure_damaged,
            target_quality,
            caution_flags[:6],
            auction_context_score,
            "ukrainian" if self.config.use_ukrainian_formatter else "legacy_html",
            TELEGRAM_NOTIFIER_VERSION,
        )

        sent = self.send_text(message)

        if sent and battle_permission == "CAUTION_BATTLE":
            note = "caution_battle_alert_sent"
        elif sent:
            note = "battle_alert_sent"
        else:
            note = "battle_alert_send_failed"

        _record_battle_permission_event_enriched(
            normalized_payload,
            source="telegram_notifier",
            sent_to_telegram=sent,
            note=note,
        )

        return sent

    def format_alert_payload(self, payload: Dict[str, Any]) -> str:
        payload = _normalize_alert_payload(payload)

        if self.config.use_ukrainian_formatter:
            try:
                return self.format_alert_payload_ukrainian(payload)
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Ukrainian telegram_formatter failed. Falling back to legacy HTML formatter. symbol=%s signal_id=%s error=%s",
                    payload.get("symbol"),
                    payload.get("signal_id"),
                    exc,
                )

        return self.format_alert_payload_legacy_html(payload)

    def format_alert_payload_ukrainian(self, payload: Dict[str, Any]) -> str:
        """
        Ukrainian human-facing live alert formatter.

        telegram_formatter.py returns plain text. This notifier still sends messages
        with parse_mode=HTML by default, so we escape the whole rendered message.
        Telegram will display it as normal text while keeping line breaks and emojis.
        """
        payload = _normalize_alert_payload(payload)

        formatted = format_signal_message(payload)
        text = formatted.render()

        safety_lines = _build_battle_safety_lines(payload)
        if safety_lines:
            text = text + "\n\n" + "\n".join(safety_lines)

        return _truncate(_escape_html(text), self.config.max_message_length)

    def format_alert_payload_legacy_html(self, payload: Dict[str, Any]) -> str:
        payload = _normalize_alert_payload(payload)

        symbol = _escape_html(payload.get("symbol", "UNKNOWN"))
        alert_type = _escape_html(payload.get("alert_type", "UNKNOWN"))
        scenario_type = _escape_html(payload.get("scenario_type", "UNKNOWN"))
        direction = _escape_html(payload.get("direction", "UNKNOWN"))
        watch_reason = _escape_html(payload.get("watch_reason", "-"))
        market_state = _escape_html(payload.get("market_state", "-"))
        htf_bias = _escape_html(payload.get("htf_bias", "-"))
        invalidation_level = payload.get("invalidation_level")
        target_zone = payload.get("target_zone")

        trade_confidence_raw = _first_present(
            payload.get("trade_confidence"),
            payload.get("confidence"),
            payload.get("probability"),
            payload.get("trade_probability"),
            payload.get("signal_confidence"),
        )
        scenario_probability_raw = payload.get("scenario_probability")

        trade_confidence_pct = _format_percent(trade_confidence_raw)
        scenario_probability_pct = _format_percent(scenario_probability_raw)

        paper_mode = bool(payload.get("paper_mode", True))
        cycle_id = _escape_html(payload.get("cycle_id", "-"))

        execution_status = _escape_html(payload.get("execution_status", "-"))
        execution_model = _escape_html(payload.get("execution_model", "-"))
        entry_reference_price = payload.get("entry_reference_price")
        invalidation_reference_price = payload.get("invalidation_reference_price")
        target_reference_price = payload.get("target_reference_price")
        risk_reward_ratio = payload.get("risk_reward_ratio")
        signal_id = _escape_html(payload.get("signal_id", "-"))

        signal_alignment = str(payload.get("signal_alignment") or "UNKNOWN")
        signal_alignment_marker = str(payload.get("signal_alignment_marker") or "⚫")
        signal_alignment_label = str(payload.get("signal_alignment_label") or signal_alignment)

        stop_quality = str(payload.get("stop_quality") or "UNKNOWN")
        stop_quality_reason = str(payload.get("stop_quality_reason") or "")
        theoretical_rr = payload.get("theoretical_rr")
        practical_rr = payload.get("practical_rr")

        header = self.config.paper_mode_prefix if paper_mode else self.config.live_mode_prefix

        invalidation_str = (
            _escape_html(invalidation_level) if invalidation_level is not None else "-"
        )
        target_zone_str = _normalize_target_zone(target_zone)

        lines = [
            f"<b>{header} | {symbol}</b>",
            f"<b>{_escape_html(signal_alignment_marker)} {_escape_html(signal_alignment_label)}</b>",
        ]

        safety_lines = _build_battle_safety_lines(payload)
        if safety_lines:
            lines.append("")
            for idx, safety_line in enumerate(safety_lines):
                if idx == 0:
                    lines.append(f"<b>{_escape_html(safety_line)}</b>")
                else:
                    lines.append(_escape_html(safety_line))

        if stop_quality == "TIGHT_STOP":
            lines.append("<b>⚠️ TIGHT STOP / RR INFLATED</b>")
        elif stop_quality == "INVALID":
            lines.append("<b>⛔ INVALID STOP GEOMETRY</b>")

        lines.extend(
            [
                "",
                f"<b>Alert:</b> {alert_type}",
                f"<b>Scenario:</b> {scenario_type}",
                f"<b>Direction:</b> {direction}",
                f"<b>Trade confidence:</b> {trade_confidence_pct}",
            ]
        )

        if scenario_probability_raw is not None:
            lines.append(f"<b>Scenario probability:</b> {scenario_probability_pct}")

        lines.extend(
            [
                f"<b>Market state:</b> {market_state}",
                f"<b>HTF bias:</b> {htf_bias}",
                f"<b>Invalidation:</b> {invalidation_str}",
                f"<b>Target zone:</b> {target_zone_str}",
                f"<b>Cycle:</b> {cycle_id}",
                "",
                f"<b>Execution status:</b> {execution_status}",
                f"<b>Execution model:</b> {execution_model}",
            ]
        )

        if entry_reference_price is not None:
            lines.append(f"<b>Entry:</b> {_escape_html(entry_reference_price)}")
        if invalidation_reference_price is not None:
            lines.append(f"<b>Stop:</b> {_escape_html(invalidation_reference_price)}")
        if target_reference_price is not None:
            lines.append(f"<b>Target:</b> {_escape_html(target_reference_price)}")
        if risk_reward_ratio is not None:
            lines.append(f"<b>RR:</b> {_escape_html(risk_reward_ratio)}")

        if practical_rr is not None and theoretical_rr is not None:
            try:
                practical_rr_f = float(practical_rr)
                theoretical_rr_f = float(theoretical_rr)

                if abs(practical_rr_f - theoretical_rr_f) >= 0.05:
                    lines.append(f"<b>Practical RR:</b> {_escape_html(f'{practical_rr_f:.2f}')}")
            except (TypeError, ValueError):
                pass
        elif practical_rr is not None:
            try:
                lines.append(f"<b>Practical RR:</b> {_escape_html(f'{float(practical_rr):.2f}')}")
            except (TypeError, ValueError):
                lines.append(f"<b>Practical RR:</b> {_escape_html(practical_rr)}")

        if stop_quality in {"TIGHT_STOP", "INVALID"} and stop_quality_reason:
            lines.append(f"<b>Stop quality:</b> {_escape_html(stop_quality)}")
            lines.append(f"<b>Stop note:</b> {_escape_html(stop_quality_reason)}")

        lines.extend(
            [
                "",
                f"<b>Reason:</b> {watch_reason}",
                "",
                f"<b>ID:</b> <code>{signal_id}</code>",
            ]
        )

        return _truncate("\n".join(lines), self.config.max_message_length)

    def send_startup_message(self, worker_name: str = "main_worker") -> bool:
        return self.send_text(
            "\n".join(
                [
                    f"<b>🟢 {_escape_html(worker_name)} started</b>",
                    "",
                    "<b>Status:</b> online",
                    "<b>Mode:</b> 24/7 loop",
                    f"<b>Telegram notifier:</b> {_escape_html(TELEGRAM_NOTIFIER_VERSION)}",
                ]
            )
        )

    def send_shutdown_message(self, worker_name: str = "main_worker") -> bool:
        return self.send_text(
            "\n".join(
                [
                    f"<b>🛑 {_escape_html(worker_name)} stopped</b>",
                    "",
                    "<b>Status:</b> offline",
                ]
            )
        )

    def send_error_message(self, title: str, details: str) -> bool:
        return self.send_text(
            "\n".join(
                [
                    f"<b>❌ {_escape_html(title)}</b>",
                    "",
                    f"<pre>{_escape_html(_truncate(details, 3000))}</pre>",
                ]
            )
        )


def build_telegram_notifier() -> TelegramNotifier:
    return TelegramNotifier()


def send_alert_payload(payload: Dict[str, Any]) -> bool:
    notifier = build_telegram_notifier()
    return notifier.send_alert_payload(payload)


if __name__ == "__main__":
    sample_payload = {
        "should_alert": True,
        "symbol": "NAS100",
        "signal_class": "READY",
        "alert_type": "ENTRY_READY",
        "scenario": "TPO_OPEN_TEST_DRIVE_LONG",
        "scenario_type": "TPO_OPEN_TEST_DRIVE_LONG",
        "scenario_family": "POST_NEWS_RECLAIM",
        "direction": "LONG",
        "confidence": 0.68,
        "scenario_probability": 0.68,
        "rationale": "OPEN_TEST_DRIVE / reclaim regression payload.",
        "market_state": "TRANSITION",
        "htf_bias": "LONG",
        "signal_alignment": "TREND_ALIGNED",
        "entry_reference_price": 28855.13,
        "invalidation_reference_price": 28789.73,
        "target_reference_price": 29100.0,
        "execution_status": "EXECUTABLE",
        "execution_model": "FAILED_ACCEPTANCE_RETEST",
        "risk_reward_ratio": 3.74,
        "practical_rr": 3.74,
        "stop_quality": "OK",
        "quality_tier": "CAUTION",
        "paper_mode": False,
        "cycle_id": "2026-06-09T17:39:10.610073+00:00",
        "signal_id": "TEST_NAS100_CAUTION_BATTLE",
        "metadata": {
            "battle_permission": "CAUTION_BATTLE",
            "telegram_delivery_mode": "BATTLE_ALERT",
            "battle_ready": True,
            "auction_context_score": 3,
            "risk_mode": "CAUTION",
            "news_risk_state": "PROVIDER_UNAVAILABLE",
            "news_provider_status": "FINNHUB_UNAVAILABLE",
            "local_structure_damaged": True,
            "target_quality": "REAL_ZONE",
            "caution_flags": [
                "news_provider_unavailable_usd_sensitive",
                "local_structure_damaged",
            ],
        },
    }

    notifier = build_telegram_notifier()
    text = notifier.format_alert_payload_legacy_html(sample_payload)
    print(text)

