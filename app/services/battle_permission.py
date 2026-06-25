from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
import os
from pathlib import Path
import re
from typing import Any

try:
    from app.services.battle_gate_open_behavior_policy import evaluate_open_behavior_policy
except Exception:  # pragma: no cover
    evaluate_open_behavior_policy = None  # type: ignore[assignment]

try:
    from app.services.post_news_continuation_detector import apply_post_news_continuation
except Exception:  # pragma: no cover
    apply_post_news_continuation = None  # type: ignore[assignment]

try:
    from app.services.macro_shock_detector import apply_macro_shock_context
except Exception:  # pragma: no cover
    apply_macro_shock_context = None  # type: ignore[assignment]

try:
    from app.services.macro_event_guard import (
        MACRO_EVENT_GUARD_VERSION,
        evaluate_macro_guard,
    )
except Exception:  # pragma: no cover
    MACRO_EVENT_GUARD_VERSION = "macro-event-guard-unavailable"
    evaluate_macro_guard = None  # type: ignore[assignment]


BATTLE_PERMISSION_VERSION = "battle-permission-v1.12-dalton-auction-branch-gate"

# Signals that are structurally stale/invalid or post-shock with insufficient
# reward must stay out of user-facing Telegram delivery. They remain in
# journal/statistics/telemetry for diagnostics.
POST_SHOCK_STATISTICS_ONLY_MIN_RR = float(os.getenv("POST_SHOCK_STATISTICS_ONLY_MIN_RR", os.getenv("POST_NEWS_OTD_MIN_PRACTICAL_RR", "3.0")))
TPO_OTD_LONG_STATS_DOWNGRADE_ENABLED = str(os.getenv("TPO_OTD_LONG_STATS_DOWNGRADE_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
TPO_OTD_LONG_A_PLUS_MIN_RR = float(os.getenv("TPO_OTD_LONG_A_PLUS_MIN_RR", "3.0"))

OPEN_AUCTION_BEHAVIORS = {
    "OPEN_AUCTION",
    "OPEN_AUCTION_IN_RANGE",
    "OPEN_AUCTION_OUT_OF_RANGE",
}
TPO_DIRECTIONAL_AUCTION_BEHAVIORS = {
    "OPEN_DRIVE",
    "OPEN_DRIVE_CONFIRMED",
    "OPEN_TEST_DRIVE",
    "OPEN_TEST_DRIVE_CANDIDATE",
    "OPEN_TEST_DRIVE_CONFIRMED",
    "OPEN_REJECTION_REVERSE",
    "OPEN_AUCTION_ACCEPTED_BREAKOUT",
    "OPEN_AUCTION_OUT_OF_RANGE_ACCEPTED_BREAKOUT",
    "OPEN_AUCTION_OUT_OF_RANGE_FAILED_ACCEPTANCE",
    "OPEN_AUCTION_FAILED_ACCEPTANCE",
}
TPO_AUCTION_LTF_SETUP_FAMILIES = {
    "OPEN_DRIVE",
    "OPEN_TEST_DRIVE",
    "OPEN_REJECTION_REVERSE",
    "OPEN_AUCTION_BREAKOUT",
    "OPEN_AUCTION_BACK_TO_VALUE",
}
OPEN_AUCTION_LTF_EXCEPTION_SETUPS = {
    "OPEN_AUCTION_BREAKOUT",
    "OPEN_AUCTION_BACK_TO_VALUE",
}
TPO_AUCTION_WATCH_STATES = {
    "LTF_MODEL_PENDING",
    "BLOCKED",
    "OBSERVE_ONLY",
    "OBSERVE_ROTATION",
    "RESEARCH_ONLY",
    "NO_WATCH",
}
LTF_EXECUTABLE_OUTCOMES = {
    "CONFIRMED_EXECUTABLE",
}


class BattlePermission(str, Enum):
    BATTLE_READY = "BATTLE_READY"
    CAUTION_BATTLE = "CAUTION_BATTLE"
    RESEARCH_ONLY = "RESEARCH_ONLY"
    BLOCKED_BY_MARKET_CLOSED = "BLOCKED_BY_MARKET_CLOSED"
    BLOCKED_BY_STALE_DATA = "BLOCKED_BY_STALE_DATA"
    BLOCKED_BY_AUCTION = "BLOCKED_BY_AUCTION"
    BLOCKED_BY_HTF = "BLOCKED_BY_HTF"
    BLOCKED_BY_EXECUTION = "BLOCKED_BY_EXECUTION"
    BLOCKED_BY_RR = "BLOCKED_BY_RR"
    BLOCKED_BY_STOP_QUALITY = "BLOCKED_BY_STOP_QUALITY"
    BLOCKED_BY_QUALITY = "BLOCKED_BY_QUALITY"
    BLOCKED_BY_CONTEXT = "BLOCKED_BY_CONTEXT"
    NOT_READY = "NOT_READY"


class TelegramDeliveryMode(str, Enum):
    BATTLE_ALERT = "BATTLE_ALERT"
    RESEARCH_ALERT = "RESEARCH_ALERT"
    SUPPRESS = "SUPPRESS"


@dataclass
class BattlePermissionResult:
    battle_permission: str
    telegram_delivery_mode: str
    battle_ready: bool
    auction_context_score: int
    reasons: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    modifiers: list[str] = field(default_factory=list)

    market_is_open: bool | None = None
    market_status: str | None = None
    tpo_signal_permission: str | None = None
    tpo_telegram_modifier: str | None = None
    open_relation: str | None = None
    auction_bias: str | None = None

    # TPO/open-behavior context fields.
    open_context: str | None = None
    open_behavior: str | None = None
    open_behavior_confidence: float | None = None
    entry_model_hint: str | None = None
    stop_model_hint: str | None = None
    battle_bias_hint: str | None = None
    primary_interest_zone: dict[str, Any] | None = None
    interest_zone_type: str | None = None
    interest_zone_price: float | None = None
    interest_zone_role: str | None = None

    # Auction-state fields from tpo_open_behavior_classifier.py / tpo_watch_bridge.py.
    open_location: str | None = None
    initial_open_behavior: str | None = None
    current_open_behavior: str | None = None
    behavior_transition: str | None = None
    value_acceptance_state: str | None = None
    value_test_occurred: bool | None = None
    value_rejection_confirmed: bool | None = None
    day_type_candidate: str | None = None
    structure_state: str | None = None
    one_timeframing_state: str | None = None
    auction_state_confidence: float | None = None
    auction_state_reason: str | None = None

    tpo_watch_state: str | None = None
    tpo_watch_setup: str | None = None
    tpo_watch_active: bool | None = None
    auction_ltf_setup: str | None = None

    ltf_model_detector_version: str | None = None
    ltf_model_state: str | None = None
    ltf_model_state_full: str | None = None
    ltf_model_outcome: str | None = None
    ltf_model_type: str | None = None
    ltf_model_confirmed: bool | None = None
    ltf_model_blockers: list[str] = field(default_factory=list)
    ltf_model_warnings: list[str] = field(default_factory=list)

    direction: str | None = None
    htf_bias: str | None = None
    signal_alignment: str | None = None
    execution_status: str | None = None
    practical_rr: float | None = None
    stop_quality: str | None = None
    quality_tier: str | None = None

    # Safety Gate / scenario diagnostics.
    symbol: str | None = None
    session_label: str | None = None
    news_risk_state: str | None = None
    news_provider_status: str | None = None
    local_structure_damaged: bool | None = None
    scenario_family: str | None = None
    target_quality: str | None = None
    risk_mode: str | None = None
    caution_flags: list[str] = field(default_factory=list)

    # Statistics-only suppression fields.
    # These payloads are useful for journal/statistics, but must not reach Telegram.
    statistics_only: bool = False
    suppression_reason: str | None = None
    suppression_reasons: list[str] = field(default_factory=list)

    # Post-news continuation detector fields.
    post_news_detector_version: str | None = None
    post_news_regime: str | None = None
    post_news_trade_permission: str | None = None
    post_news_elapsed_minutes: int | None = None
    post_news_impulse_direction: str | None = None
    post_news_impulse_confirmed: bool | None = None
    post_news_retest_level: str | None = None
    post_news_retest_status: str | None = None
    post_news_acceptance_status: str | None = None
    post_news_failed_move: bool | None = None
    post_news_continuation_quality: str | None = None
    post_news_continuation_direction: str | None = None
    post_news_reasons: list[str] = field(default_factory=list)
    post_news_blockers: list[str] = field(default_factory=list)
    post_news_modifiers: list[str] = field(default_factory=list)

    # Signal lifecycle / stale READY protection.
    signal_created_at_utc: str | None = None
    signal_age_minutes: float | None = None
    signal_max_age_minutes: float | None = None
    signal_freshness_status: str | None = None

    # Macro shock detector fields.
    macro_detector_version: str | None = None
    macro_regime: str | None = None
    macro_shock_recent: bool | None = None
    macro_shock_score: float | None = None
    macro_risk_mode: str | None = None
    macro_direction_for_symbol: str | None = None
    macro_caution_flags: list[str] = field(default_factory=list)
    macro_reasons: list[str] = field(default_factory=list)

    # Macro event guard fields.
    macro_guard_version: str | None = None
    macro_guard_status: str | None = None
    macro_guard_allowed_for_battle: bool | None = None
    macro_guard_block_battle: bool | None = None
    macro_guard_research_only: bool | None = None
    macro_guard_suppress: bool | None = None
    macro_guard_reason_code: str | None = None
    macro_guard_blockers: list[str] = field(default_factory=list)
    macro_guard_requirements: list[str] = field(default_factory=list)
    macro_guard_missing_requirements: list[str] = field(default_factory=list)
    macro_guard_satisfied_requirements: list[str] = field(default_factory=list)
    macro_guard_macro_risk_status: str | None = None
    macro_guard_calendar_status: str | None = None
    macro_guard_calendar_source: str | None = None
    macro_guard_fallback_chain: list[str] = field(default_factory=list)
    macro_guard_event_title: str | None = None
    macro_guard_event_time_local: str | None = None
    macro_guard_event_currency: str | None = None
    macro_guard_event_impact: str | None = None
    macro_guard_event_source: str | None = None
    macro_guard_minutes_since_event: float | None = None
    macro_guard_minutes_until_event: float | None = None
    macro_guard_affected_symbols: list[str] = field(default_factory=list)
    macro_guard_notes: list[str] = field(default_factory=list)
    macro_guard_error: str | None = None

    # First-impulse / no-chase protection.
    entry_price: float | None = None
    invalidation_price: float | None = None
    target_price: float | None = None
    current_price: float | None = None
    impulse_progress: float | None = None
    impulse_progress_pct: float | None = None
    impulse_state: str | None = None
    fresh_retest_exists: bool | None = None
    fresh_failed_acceptance_exists: bool | None = None
    fresh_pullback_exists: bool | None = None

    # Battle Gate v2 shadow-mode fields.
    # Legacy Battle Gate remains the execution authority for now.
    battle_gate_v2_decision: str | None = None
    battle_gate_v2_risk_mode: str | None = None
    battle_gate_v2_battle_allowed: bool | None = None
    battle_gate_v2_should_suppress_telegram: bool | None = None
    battle_gate_v2_score_delta: float | None = None
    battle_gate_v2_reasons: list[str] = field(default_factory=list)
    battle_gate_v2_blockers: list[str] = field(default_factory=list)
    battle_gate_v2_modifiers: list[str] = field(default_factory=list)
    battle_gate_v2_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _deep_get(data: dict[str, Any], *paths: str) -> Any:
    """
    Reads the first non-empty value from dotted paths.

    Example:
    _deep_get(payload, "metadata.auction_context.market_status", "market_status")
    """
    for path in paths:
        current: Any = data

        for part in path.split("."):
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(part)

        if current not in (None, "", [], {}):
            return current

    return None


def _as_upper(value: Any) -> str | None:
    if value in (None, "", [], {}):
        return None
    return str(value).strip().upper()


def _as_float(value: Any) -> float | None:
    if value in (None, "", [], {}):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value

    if value is None:
        return None

    if isinstance(value, str):
        normalized = value.strip().lower()

        if normalized in {"true", "yes", "1"}:
            return True

        if normalized in {"false", "no", "0"}:
            return False

    return None


def _as_text_list(value: Any) -> list[str]:
    if value in (None, "", [], {}):
        return []

    if isinstance(value, (list, tuple, set)):
        result: list[str] = []
        for item in value:
            if item in (None, "", [], {}):
                continue
            text = str(item).strip()
            if text:
                result.append(text)
        return result

    if isinstance(value, dict):
        try:
            return [json.dumps(value, ensure_ascii=False, sort_keys=True)]
        except Exception:
            return [str(value)]

    text = str(value).strip()
    return [text] if text else []


def _dedupe_text_list(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        key = item.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(item.strip())
    return result


def _parse_datetime_utc(value: Any) -> datetime | None:
    if value in (None, "", [], {}):
        return None

    text = str(value).strip()
    if not text:
        return None

    # Normal ISO values: 2026-06-12T06:36:22.931251+00:00
    candidates = [text, text.replace("Z", "+00:00")]

    # Signal-id safe timestamp values: 2026-06-10T14-41-52.395754+00-00
    m = re.search(
        r"(\d{4}-\d{2}-\d{2}T\d{2})-(\d{2})-(\d{2}(?:\.\d+)?)(\+\d{2}-\d{2}|Z)?",
        text,
    )
    if m:
        tz = m.group(4) or "+00-00"
        tz = "+00:00" if tz in {"+00-00", "Z"} else tz.replace("-", ":", 1)
        candidates.append(f"{m.group(1)}:{m.group(2)}:{m.group(3)}{tz}")

    # Looser signal-id value after T, where every time separator was replaced by '-'.
    m = re.search(
        r"(\d{4}-\d{2}-\d{2}T\d{2})-(\d{2})-(\d{2}(?:\.\d+)?)",
        text,
    )
    if m:
        candidates.append(f"{m.group(1)}:{m.group(2)}:{m.group(3)}+00:00")

    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
        except ValueError:
            continue

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    return None


def _extract_signal_created_at(payload: dict[str, Any]) -> datetime | None:
    explicit = _deep_get(
        payload,
        "signal_created_at_utc",
        "created_at_utc",
        "created_at",
        "metadata.signal_created_at_utc",
        "metadata.created_at_utc",
        "metadata.created_at",
    )
    dt = _parse_datetime_utc(explicit)
    if dt is not None:
        return dt

    # Prefer signal_id over cycle_id for lifecycle freshness because signal_id
    # often preserves the original setup creation time while cycle_id can be the
    # current scan that rediscovered an old READY idea.
    dt = _parse_datetime_utc(_deep_get(payload, "signal_id", "metadata.signal_id"))
    if dt is not None:
        return dt

    return _parse_datetime_utc(_deep_get(payload, "cycle_id", "metadata.cycle_id"))


def _max_signal_age_minutes(*, symbol: Any, scenario: Any, execution_timeframe: Any) -> float | None:
    scenario_text = _as_upper(scenario) or ""
    tf = str(execution_timeframe or "").strip().lower()
    sym = _normalize_symbol(symbol)

    # Only enforce TTL for short-term executable ideas. Longer-term setups should
    # explicitly provide their own lifecycle fields before we block them.
    is_15m = tf in {"", "15m", "m15", "15", "15min"}
    if not is_15m:
        return None

    if "SWEEP_RETURN" in scenario_text:
        base = 240.0
    elif "OPEN_TEST_DRIVE" in scenario_text or "TPO_OPEN_TEST_DRIVE" in scenario_text:
        base = 360.0
    else:
        base = 360.0

    if sym in {"BTCUSD", "BTCUSDT", "ETHUSD", "ETHUSDT", "BTC", "ETH"}:
        # Crypto is 24/7 and can carry structure a bit longer, but not for days.
        return max(base, 720.0)

    return base


def _compute_signal_freshness(payload: dict[str, Any]) -> dict[str, Any]:
    created_at = _extract_signal_created_at(payload)
    scenario = _deep_get(payload, "scenario", "scenario_type", "metadata.scenario", "metadata.scenario_type")
    symbol = _deep_get(payload, "symbol", "instrument", "metadata.symbol", "metadata.instrument")
    execution_timeframe = _deep_get(payload, "execution_timeframe", "timeframe", "metadata.execution_timeframe", "metadata.timeframe")
    max_age = _max_signal_age_minutes(symbol=symbol, scenario=scenario, execution_timeframe=execution_timeframe)

    if created_at is None or max_age is None:
        return {
            "signal_created_at_utc": created_at.isoformat() if created_at else None,
            "signal_age_minutes": None,
            "signal_max_age_minutes": max_age,
            "signal_freshness_status": "UNKNOWN",
        }

    age_minutes = max(0.0, (datetime.now(timezone.utc) - created_at).total_seconds() / 60.0)
    if age_minutes > max_age:
        status = "STALE_READY"
    elif age_minutes > max_age * 0.75:
        status = "AGING_READY"
    else:
        status = "FRESH"

    return {
        "signal_created_at_utc": created_at.isoformat(),
        "signal_age_minutes": round(age_minutes, 3),
        "signal_max_age_minutes": max_age,
        "signal_freshness_status": status,
    }


def _collect_policy_hint_flags(inputs: dict[str, Any], v2_policy: dict[str, Any]) -> list[str]:
    flags: list[str] = []

    open_behavior = _as_upper(inputs.get("open_behavior"))
    entry_model_hint = _as_upper(inputs.get("entry_model_hint"))
    stop_model_hint = _as_upper(inputs.get("stop_model_hint"))
    battle_bias_hint = _as_upper(inputs.get("battle_bias_hint"))

    if open_behavior:
        flags.append(f"open_behavior_{open_behavior.lower()}")
    if entry_model_hint:
        flags.append(f"entry_hint_{entry_model_hint.lower()}")
    if stop_model_hint:
        flags.append(f"stop_hint_{stop_model_hint.lower()}")
    if battle_bias_hint:
        flags.append(f"battle_hint_{battle_bias_hint.lower()}")

    for key in ("modifiers", "blockers", "reasons"):
        for item in _as_text_list(v2_policy.get(key)):
            normalized = item.strip().lower().replace(" ", "_").replace("-", "_")
            if normalized:
                flags.append(normalized)

    return _dedupe_text_list(flags)


def _policy_hint_requires_research_only(*, inputs: dict[str, Any], policy_flags: list[str], post_news_allows_battle: bool) -> tuple[bool, list[str]]:
    if post_news_allows_battle:
        return False, []

    hard: list[str] = []
    open_behavior = _as_upper(inputs.get("open_behavior"))
    entry_model_hint = _as_upper(inputs.get("entry_model_hint"))
    battle_bias_hint = _as_upper(inputs.get("battle_bias_hint"))

    joined = " ".join(policy_flags).lower()

    if "battle_hint_research_only" in joined or battle_bias_hint == "RESEARCH_ONLY":
        hard.append("policy_hint_battle_research_only")

    if entry_model_hint in {"NO_DIRECTIONAL_ENTRY_MODEL", "NO_ENTRY_MODEL", "ROTATION_ONLY_IF_LTF_CONFIRMED"}:
        hard.append(f"policy_hint_entry_{entry_model_hint.lower()}")

    if open_behavior == "OPEN_AUCTION" and not _open_auction_ltf_exception(inputs):
        hard.append("policy_hint_open_auction_requires_late_continuation")

    return bool(hard), hard


def _policy_hint_requires_caution(*, inputs: dict[str, Any], policy_flags: list[str]) -> list[str]:
    caution: list[str] = []
    stop_model_hint = _as_upper(inputs.get("stop_model_hint"))

    if stop_model_hint in {"NO_STOP_MODEL", "WEAK_STOP_MODEL", "TACTICAL_STOP_ONLY"}:
        caution.append(f"policy_hint_stop_{stop_model_hint.lower()}")

    joined = " ".join(policy_flags).lower()
    if "allow_with_caution" in joined or "caution" in joined:
        caution.append("policy_hint_allow_with_caution")

    return _dedupe_text_list(caution)


def _normalize_direction(value: Any) -> str | None:
    normalized = _as_upper(value)

    if normalized in {"LONG", "BUY", "BULL", "BULLISH", "UP"}:
        return "LONG"

    if normalized in {"SHORT", "SELL", "BEAR", "BEARISH", "DOWN"}:
        return "SHORT"

    if normalized in {"NEUTRAL", "NONE", "NO_TRADE"}:
        return "NEUTRAL"

    return normalized


def _normalize_open_relation(value: Any) -> str | None:
    normalized = _as_upper(value)

    if normalized in {"OPEN_INSIDE_VA", "INSIDE_VALUE", "INSIDE_VALUE_AREA"}:
        return "INSIDE_VA"

    if normalized in {"OPEN_IN_RANGE", "IN_RANGE"}:
        return "RANGE"

    if normalized in {"OPEN_OUT_OF_RANGE", "OUTSIDE_RANGE", "OUTSIDE_PREVIOUS_RANGE"}:
        return "OUT_OF_RANGE"

    return normalized


def _direction_matches_htf(direction: str | None, htf_bias: str | None) -> bool:
    if not direction or not htf_bias:
        return False

    direction = _normalize_direction(direction)
    htf_bias = _normalize_direction(htf_bias)

    return direction in {"LONG", "SHORT"} and direction == htf_bias


def _is_neutral_htf(value: str | None) -> bool:
    normalized = _normalize_direction(value)
    return normalized in {None, "", "NEUTRAL", "NONE", "FLAT", "NO_TRADE"}


def _is_valid_stop_quality_for_battle(stop_quality: str | None) -> bool:
    if stop_quality in {None, "", "TIGHT_STOP", "BAD", "WEAK", "NO_STOP", "NONE"}:
        return False
    return True


def _normalize_symbol(value: Any) -> str | None:
    if value in (None, "", [], {}):
        return None
    return str(value).strip().upper().replace("/", "").replace(" ", "")


def _normalize_news_risk_state(value: Any) -> str | None:
    normalized = _as_upper(value)
    if not normalized:
        return None

    if normalized in {"OK", "NONE", "NO_NEWS", "LOW", "NORMAL"}:
        return "OK"

    if normalized in {
        "PROVIDER_UNAVAILABLE",
        "CALENDAR_UNAVAILABLE",
        "ECONOMIC_CALENDAR_UNAVAILABLE",
        "FINNHUB_UNAVAILABLE",
        "FINNHUB_ECONOMIC_CALENDAR_UNAVAILABLE",
        "PROVIDER_ERROR",
        "CALENDAR_ERROR",
    }:
        return "PROVIDER_UNAVAILABLE"

    if any(token in normalized for token in {"UNAVAILABLE", "PROVIDER_ERROR", "CALENDAR_ERROR", "FINNHUB_UNAVAILABLE", "429", "TIMEOUT"}):
        return "PROVIDER_UNAVAILABLE"

    if any(token in normalized for token in {"POST_NEWS", "AFTER_NEWS", "NEWS_CAUTION"}):
        return "POST_NEWS_CAUTION"

    if any(token in normalized for token in {"HIGH_IMPACT", "RED_NEWS", "USD_HIGH"}):
        return "HIGH_IMPACT"

    return normalized


def _is_news_provider_unavailable(news_risk_state: Any, news_provider_status: Any = None) -> bool:
    state = _normalize_news_risk_state(news_risk_state)
    provider = _as_upper(news_provider_status)
    haystack = " ".join(part for part in [state, provider] if part)

    if not haystack:
        return False

    return any(
        token in haystack
        for token in {
            "PROVIDER_UNAVAILABLE",
            "CALENDAR_UNAVAILABLE",
            "ECONOMIC_CALENDAR_UNAVAILABLE",
            "FINNHUB_UNAVAILABLE",
            "UNAVAILABLE",
            "PROVIDER_ERROR",
            "CALENDAR_ERROR",
            "429",
            "TIMEOUT",
        }
    )


def _is_usd_sensitive_symbol(symbol: Any) -> bool:
    normalized = _normalize_symbol(symbol)
    if not normalized:
        return False

    if "USD" in normalized:
        return True

    return normalized in {
        "NAS100",
        "NDX",
        "NASDAQ100",
        "US100",
        "SPX500",
        "SP500",
        "US500",
        "US30",
        "DJI",
        "UKOIL",
        "USOIL",
        "BRENT",
        "WTI",
        "BTCUSDT",
        "ETHUSDT",
        "BTC",
        "ETH",
        "XAU",
        "XAG",
    }


def _is_ny_or_post_news_context(inputs: dict[str, Any]) -> bool:
    candidates = [
        inputs.get("session_label"),
        inputs.get("news_risk_state"),
        inputs.get("news_provider_status"),
        inputs.get("tpo_telegram_modifier"),
    ]
    haystack = " ".join(str(value).upper() for value in candidates if value not in (None, "", [], {}))
    if not haystack:
        return False

    return any(token in haystack for token in {"NY", "NEW_YORK", "US_SESSION", "POST_NEWS", "AFTER_NEWS", "USD_HIGH"})


def _normalize_target_quality(value: Any) -> str | None:
    normalized = _as_upper(value)
    if not normalized:
        return None

    if normalized in {"REAL", "REAL_ZONE", "REAL_TARGET", "INTEREST_ZONE", "AUCTION_ZONE", "TPO_ZONE"}:
        return "REAL_ZONE"

    if normalized in {"SYNTHETIC", "SYNTHETIC_TARGET", "MECHANICAL", "RR_ONLY"}:
        return "SYNTHETIC"

    if normalized in {"UNKNOWN", "UNCONFIRMED", "NONE", "MISSING"}:
        return "UNKNOWN"

    return normalized



def _extract_trade_price_levels(payload: dict[str, Any]) -> dict[str, float | None]:
    """
    Extract execution geometry needed for no-chase protection.

    The project has used several payload shapes over time, so this stays wide
    and defensive. Missing fields never break Battle Gate; they simply disable
    the first-impulse filter for that payload.
    """
    entry_price = _as_float(
        _deep_get(
            payload,
            "metadata.entry_price",
            "metadata.entry",
            "metadata.entry_reference_price",
            "metadata.execution.entry_price",
            "metadata.execution.entry_reference_price",
            "metadata.execution_plan.entry_price",
            "metadata.execution_plan.entry",
            "metadata.execution_plan.entry_reference_price",
            "execution.entry_price",
            "execution.entry",
            "execution.entry_reference_price",
            "execution_plan.entry_price",
            "execution_plan.entry",
            "execution_plan.entry_reference_price",
            "entry_price",
            "entry",
            "entry_reference_price",
        )
    )

    invalidation_price = _as_float(
        _deep_get(
            payload,
            "metadata.invalidation_price",
            "metadata.invalidation",
            "metadata.invalidation_reference_price",
            "metadata.stop_price",
            "metadata.stop",
            "metadata.stop_loss",
            "metadata.sl",
            "metadata.execution.invalidation_price",
            "metadata.execution.invalidation",
            "metadata.execution.invalidation_reference_price",
            "metadata.execution.stop_price",
            "metadata.execution.stop",
            "metadata.execution.stop_loss",
            "metadata.execution.sl",
            "metadata.execution_plan.invalidation_price",
            "metadata.execution_plan.invalidation",
            "metadata.execution_plan.invalidation_reference_price",
            "metadata.execution_plan.stop_price",
            "metadata.execution_plan.stop",
            "metadata.execution_plan.stop_loss",
            "metadata.execution_plan.sl",
            "execution.invalidation_price",
            "execution.invalidation",
            "execution.invalidation_reference_price",
            "execution.stop_price",
            "execution.stop",
            "execution.stop_loss",
            "execution.sl",
            "execution_plan.invalidation_price",
            "execution_plan.invalidation",
            "execution_plan.invalidation_reference_price",
            "execution_plan.stop_price",
            "execution_plan.stop",
            "execution_plan.stop_loss",
            "execution_plan.sl",
            "invalidation_price",
            "invalidation",
            "invalidation_reference_price",
            "stop_price",
            "stop",
            "stop_loss",
            "sl",
        )
    )

    target_price = _as_float(
        _deep_get(
            payload,
            "metadata.target_price",
            "metadata.target",
            "metadata.take_profit",
            "metadata.tp",
            "metadata.target_reference_price",
            "metadata.execution.target_price",
            "metadata.execution.target",
            "metadata.execution.take_profit",
            "metadata.execution.tp",
            "metadata.execution.target_reference_price",
            "metadata.execution_plan.target_price",
            "metadata.execution_plan.target",
            "metadata.execution_plan.take_profit",
            "metadata.execution_plan.tp",
            "metadata.execution_plan.target_reference_price",
            "execution.target_price",
            "execution.target",
            "execution.take_profit",
            "execution.tp",
            "execution.target_reference_price",
            "execution_plan.target_price",
            "execution_plan.target",
            "execution_plan.take_profit",
            "execution_plan.tp",
            "execution_plan.target_reference_price",
            "target_price",
            "target",
            "take_profit",
            "tp",
            "target_reference_price",
        )
    )

    current_price = _as_float(
        _deep_get(
            payload,
            "metadata.current_price",
            "metadata.last_price",
            "metadata.close",
            "metadata.price",
            "metadata.execution.current_price",
            "metadata.execution.last_price",
            "metadata.execution_plan.current_price",
            "metadata.execution_plan.last_price",
            "execution.current_price",
            "execution.last_price",
            "execution_plan.current_price",
            "execution_plan.last_price",
            "market.current_price",
            "market.last_price",
            "quote.current_price",
            "quote.last_price",
            "current_price",
            "last_price",
            "close",
            "price",
        )
    )

    return {
        "entry_price": entry_price,
        "invalidation_price": invalidation_price,
        "target_price": target_price,
        "current_price": current_price,
    }


def _extract_fresh_retest_flags(payload: dict[str, Any]) -> dict[str, bool | None]:
    fresh_retest_exists = _as_bool(
        _deep_get(
            payload,
            "metadata.fresh_retest_exists",
            "metadata.fresh_retest",
            "metadata.retest.fresh",
            "metadata.retest.exists",
            "metadata.ltf_model.fresh_retest_exists",
            "metadata.execution.fresh_retest_exists",
            "metadata.execution_plan.fresh_retest_exists",
            "fresh_retest_exists",
            "fresh_retest",
            "retest.fresh",
            "retest.exists",
            "ltf_model.fresh_retest_exists",
            "execution.fresh_retest_exists",
            "execution_plan.fresh_retest_exists",
        )
    )
    fresh_failed_acceptance_exists = _as_bool(
        _deep_get(
            payload,
            "metadata.fresh_failed_acceptance_exists",
            "metadata.failed_acceptance.fresh",
            "metadata.failed_acceptance.exists",
            "metadata.ltf_model.fresh_failed_acceptance_exists",
            "metadata.execution.fresh_failed_acceptance_exists",
            "metadata.execution_plan.fresh_failed_acceptance_exists",
            "fresh_failed_acceptance_exists",
            "failed_acceptance.fresh",
            "failed_acceptance.exists",
            "ltf_model.fresh_failed_acceptance_exists",
            "execution.fresh_failed_acceptance_exists",
            "execution_plan.fresh_failed_acceptance_exists",
        )
    )
    fresh_pullback_exists = _as_bool(
        _deep_get(
            payload,
            "metadata.fresh_pullback_exists",
            "metadata.pullback.fresh",
            "metadata.pullback.exists",
            "metadata.ltf_model.fresh_pullback_exists",
            "metadata.execution.fresh_pullback_exists",
            "metadata.execution_plan.fresh_pullback_exists",
            "fresh_pullback_exists",
            "pullback.fresh",
            "pullback.exists",
            "ltf_model.fresh_pullback_exists",
            "execution.fresh_pullback_exists",
            "execution_plan.fresh_pullback_exists",
        )
    )

    return {
        "fresh_retest_exists": fresh_retest_exists,
        "fresh_failed_acceptance_exists": fresh_failed_acceptance_exists,
        "fresh_pullback_exists": fresh_pullback_exists,
    }


def _compute_first_impulse_state(inputs: dict[str, Any]) -> dict[str, Any]:
    """
    Detect READY signals where the first move is already gone.

    NORMAL:    < 30% of entry→target path already travelled.
    LATE:      30-50%; allowed only when a fresh retest / failed acceptance /
               pullback is explicitly present.
    EXHAUSTED: >= 50%; battle is blocked. This is no longer a trade entry,
               it is a historical market comment wearing a helmet.
    """
    entry = _as_float(inputs.get("entry_price"))
    target = _as_float(inputs.get("target_price"))
    current = _as_float(inputs.get("current_price"))
    direction = _normalize_direction(inputs.get("direction"))

    if entry is None or target is None or current is None or direction not in {"LONG", "SHORT"}:
        return {
            "impulse_progress": None,
            "impulse_progress_pct": None,
            "impulse_state": "UNKNOWN",
        }

    total = abs(target - entry)
    if total <= 0:
        return {
            "impulse_progress": None,
            "impulse_progress_pct": None,
            "impulse_state": "UNKNOWN",
        }

    if direction == "LONG":
        moved_toward_target = current - entry
    else:
        moved_toward_target = entry - current

    progress = max(0.0, moved_toward_target / total)

    if progress >= 0.50:
        state = "EXHAUSTED"
    elif progress >= 0.30:
        state = "LATE"
    else:
        state = "NORMAL"

    return {
        "impulse_progress": round(progress, 6),
        "impulse_progress_pct": round(progress * 100.0, 2),
        "impulse_state": state,
    }




def _payload_flag_haystack(inputs: dict[str, Any]) -> str:
    payload = inputs.get("payload") if isinstance(inputs.get("payload"), dict) else {}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

    values: list[Any] = [
        inputs.get("scenario_family"),
        inputs.get("post_news_regime"),
        inputs.get("post_news_trade_permission"),
        inputs.get("macro_regime"),
        inputs.get("macro_risk_mode"),
        inputs.get("macro_guard_status"),
        inputs.get("news_risk_state"),
        payload.get("trigger_reason"),
        metadata.get("trigger_reason"),
        payload.get("entry_timing_status"),
        metadata.get("entry_timing_status"),
    ]

    for key in (
        "flags",
        "caution_flags",
        "macro_caution_flags",
        "battle_permission_modifiers",
        "battle_permission_blockers",
        "post_news_modifiers",
        "post_news_blockers",
        "post_news_reasons",
        "reasons",
        "blockers",
        "modifiers",
    ):
        values.append(payload.get(key))
        values.append(metadata.get(key))
        values.append(inputs.get(key))

    text_items: list[str] = []
    for value in values:
        text_items.extend(_as_text_list(value))

    return " ".join(text_items).upper()


def _is_invalidated_before_alert(inputs: dict[str, Any]) -> tuple[bool, str | None]:
    direction = _normalize_direction(inputs.get("direction"))
    current = _as_float(inputs.get("current_price"))
    invalidation = _as_float(inputs.get("invalidation_price"))

    if direction not in {"LONG", "SHORT"} or current is None or invalidation is None:
        return False, None

    if direction == "LONG" and current <= invalidation:
        return True, f"current_price={current} <= invalidation={invalidation} for LONG"

    if direction == "SHORT" and current >= invalidation:
        return True, f"current_price={current} >= invalidation={invalidation} for SHORT"

    return False, None


def _is_post_shock_or_post_news_context(inputs: dict[str, Any]) -> bool:
    if inputs.get("macro_shock_recent") is True:
        return True

    macro_regime = _as_upper(inputs.get("macro_regime"))
    if macro_regime and macro_regime not in {"NO_MACRO_SHOCK", "NONE", "UNKNOWN"}:
        return True

    post_news_regime = _as_upper(inputs.get("post_news_regime"))
    if post_news_regime and post_news_regime not in {"NOT_POST_NEWS", "NONE", "UNKNOWN"}:
        return True

    macro_guard_status = _as_upper(inputs.get("macro_guard_status"))
    if macro_guard_status and any(token in macro_guard_status for token in ("POST_NEWS", "FOMC", "PRE_NEWS")):
        return True

    haystack = _payload_flag_haystack(inputs)
    return any(
        token in haystack
        for token in (
            "MACRO_SHOCK_RECENT",
            "MACRO_RISK_POST_SHOCK_CAUTION",
            "FAILED_ACCEPTANCE_RETEST_AFTER_SHOCK",
            "POST_SHOCK",
            "POST_NEWS",
        )
    )


def _post_shock_rr_below_statistics_min(inputs: dict[str, Any]) -> tuple[bool, str | None]:
    if not _is_post_shock_or_post_news_context(inputs):
        return False, None

    rr = _as_float(inputs.get("practical_rr"))
    if rr is None:
        return True, f"post-shock/post-news practical_rr is missing; minimum is {POST_SHOCK_STATISTICS_ONLY_MIN_RR:.2f}"

    if rr < POST_SHOCK_STATISTICS_ONLY_MIN_RR:
        return True, f"post-shock/post-news practical_rr={rr:.2f}; minimum is {POST_SHOCK_STATISTICS_ONLY_MIN_RR:.2f}"

    return False, None


def _is_tpo_otd_long_stats_downgrade_required(inputs: dict[str, Any]) -> tuple[bool, str | None]:
    """Suppress weak TPO OTD LONG ideas from user-facing output.

    Current production statistics show TPO OTD SHORT performing much better than
    TPO OTD LONG. Until LONG recovers statistically, LONG OTD must be A+ to be
    user-facing. Non-A+ cases remain available for journal/statistics/telemetry.
    """
    if not TPO_OTD_LONG_STATS_DOWNGRADE_ENABLED:
        return False, None

    direction = _normalize_direction(inputs.get("direction"))
    if direction != "LONG":
        return False, None

    scenario_family = _as_upper(inputs.get("scenario_family")) or ""
    scenario = _as_upper(inputs.get("scenario")) or ""
    open_behavior = _as_upper(inputs.get("open_behavior")) or ""

    is_otd = (
        scenario_family == "TPO_OPEN_TEST_DRIVE"
        or "TPO_OPEN_TEST_DRIVE" in scenario
        or (open_behavior == "OPEN_TEST_DRIVE" and "OPEN_TEST_DRIVE" in scenario)
    )
    if not is_otd:
        return False, None

    haystack = _payload_flag_haystack(inputs)
    has_weak_stats_marker = any(
        token in haystack
        for token in (
            "EARLY_NEGATIVE_DIAGNOSTIC",
            "EARLY_NEGATIVE_SIGNAL_STATISTICS",
            "EARLY_NEGATIVE",
            "LOW_SAMPLE_SIZE",
            "OTD_LONG_WEAK_STATS",
            "TPO_OTD_LONG_WEAK",
            "TPO_OPEN_TEST_DRIVE_LONG_WEAK",
        )
    )

    quality_tier = _as_upper(inputs.get("quality_tier"))
    rr = _as_float(inputs.get("practical_rr"))
    target_quality = _as_upper(inputs.get("target_quality"))
    stop_quality = _as_upper(inputs.get("stop_quality"))
    execution_status = _as_upper(inputs.get("execution_status"))
    htf_bias = _as_upper(inputs.get("htf_bias"))
    macro_guard_status = _as_upper(inputs.get("macro_guard_status"))
    news_risk_state = _as_upper(inputs.get("news_risk_state"))
    news_provider_status = _as_upper(inputs.get("news_provider_status"))

    macro_unknown = (
        macro_guard_status == "MACRO_UNKNOWN_CONSERVATIVE"
        or news_risk_state in {"UNKNOWN", "PROVIDER_UNAVAILABLE", "CALENDAR_UNAVAILABLE"}
        or news_provider_status in {"UNAVAILABLE", "ERROR", "FAILED"}
    )

    strong_exception = (
        rr is not None
        and rr >= TPO_OTD_LONG_A_PLUS_MIN_RR
        and target_quality == "REAL_ZONE"
        and _is_valid_stop_quality_for_battle(stop_quality)
        and execution_status == "EXECUTABLE"
        and htf_bias in {"BULLISH", "STRONGLY_BULLISH", "LONG"}
        and not _is_post_shock_or_post_news_context(inputs)
        and not macro_unknown
        and not has_weak_stats_marker
        and quality_tier not in {"CAUTION", "DANGER", "BLOCK", "FAIL"}
    )

    if strong_exception:
        return False, None

    detail = (
        "TPO OTD LONG is stats-downgraded: current cumulative diagnostics are weak; "
        f"requires A+ exception with practical_rr>={TPO_OTD_LONG_A_PLUS_MIN_RR:.2f}, "
        "REAL_ZONE target, OK stop, bullish HTF, clean macro/session context and no weak stats markers"
    )
    if has_weak_stats_marker:
        detail += "; weak_stats_marker=true"
    if quality_tier:
        detail += f"; quality_tier={quality_tier}"
    if rr is not None:
        detail += f"; practical_rr={rr:.2f}"
    if macro_unknown:
        detail += "; macro_unknown=true"

    return True, detail


def _build_statistics_only_result(
    *,
    inputs: dict[str, Any],
    auction_score: int,
    reasons: list[str],
    blockers: list[str],
    modifiers: list[str],
    v2_policy: dict[str, Any],
    risk_mode: str,
    suppression_reason: str,
    detail: str | None = None,
    caution_flags: list[str] | None = None,
) -> BattlePermissionResult:
    suppression_reasons = [suppression_reason]
    if detail:
        suppression_reasons.append(detail)

    return _build_result(
        inputs=inputs,
        auction_score=auction_score,
        reasons=reasons + [detail or suppression_reason, "statistics_only: suppressed from user-facing Telegram delivery"],
        blockers=_dedupe_text_list(blockers + [suppression_reason]),
        modifiers=_dedupe_text_list(modifiers + ["statistics_only"]),
        battle_permission=BattlePermission.BLOCKED_BY_CONTEXT.value,
        telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
        battle_ready=False,
        v2_policy=v2_policy,
        risk_mode=risk_mode,
        caution_flags=caution_flags or [],
        statistics_only=True,
        suppression_reason=suppression_reason,
        suppression_reasons=suppression_reasons,
    )


def _macro_guard_timezone(payload: dict[str, Any]) -> str:
    value = _deep_get(
        payload,
        "metadata.report_timezone",
        "metadata.timezone",
        "report_timezone",
        "timezone",
    )
    if value not in (None, "", [], {}):
        return str(value)
    return os.getenv("REPORT_TIMEZONE") or os.getenv("BRIEFING_AS_OF_TIMEZONE") or "Europe/Kyiv"


def _macro_guard_report_date(payload: dict[str, Any]) -> str | None:
    value = _deep_get(
        payload,
        "metadata.report_date",
        "metadata.date",
        "report_date",
        "date",
    )
    if value not in (None, "", [], {}):
        return str(value)
    value = os.getenv("REPORT_DATE")
    return str(value) if value else None


def _macro_guard_as_of(payload: dict[str, Any]) -> str | None:
    value = _deep_get(
        payload,
        "metadata.macro_guard_as_of",
        "metadata.report_as_of",
        "metadata.as_of",
        "macro_guard_as_of",
        "report_as_of",
        "as_of",
    )
    if value not in (None, "", [], {}):
        return str(value)
    for name in ("BRIEFING_AS_OF", "REPORT_AS_OF", "BRIEFING_AS_OF_UTC"):
        value = os.getenv(name)
        if value:
            return value
    return None


def _status_is_confirmed(value: Any) -> bool:
    status = _as_upper(value)
    return status in {
        "CONFIRMED",
        "CONFIRMED_EXECUTABLE",
        "EXECUTABLE",
        "VALID",
        "CLEAN",
        "PASSED",
        "ACCEPTED",
        "REJECTED",
        "FAILED_ACCEPTANCE",
        "FAILED",
        "CLEAN_REJECTION",
    }


def _macro_guard_context(inputs: dict[str, Any]) -> dict[str, Any]:
    payload = inputs.get("payload") if isinstance(inputs.get("payload"), dict) else {}
    primary_zone = inputs.get("primary_interest_zone") if isinstance(inputs.get("primary_interest_zone"), dict) else None

    post_news_otd_candidate = bool(
        _as_bool(_deep_get(payload, "metadata.post_news_otd_candidate", "post_news_otd_candidate"))
    )
    post_news_otd_rr_ok = bool(
        _as_bool(_deep_get(payload, "metadata.post_news_otd_practical_rr_ok", "post_news_otd_practical_rr_ok"))
    )
    post_news_min_rr = float(os.getenv("POST_NEWS_OTD_MIN_PRACTICAL_RR", os.getenv("MACRO_GUARD_POST_NEWS_MIN_PRACTICAL_RR", "3.0")))

    acceptance_confirmed = bool(
        _as_bool(_deep_get(payload, "metadata.acceptance_confirmed", "acceptance_confirmed"))
        or _as_bool(_deep_get(payload, "metadata.post_news_otd_acceptance_confirmed", "post_news_otd_acceptance_confirmed"))
        or _status_is_confirmed(inputs.get("post_news_acceptance_status"))
        or _status_is_confirmed(_deep_get(payload, "metadata.acceptance_status", "acceptance_status"))
    )
    retest_confirmed = bool(
        _as_bool(_deep_get(payload, "metadata.retest_confirmed", "retest_confirmed"))
        or _as_bool(_deep_get(payload, "metadata.post_news_otd_retest_confirmed", "post_news_otd_retest_confirmed"))
        or _status_is_confirmed(inputs.get("post_news_retest_status"))
        or _status_is_confirmed(_deep_get(payload, "metadata.retest_status", "retest_status"))
        or inputs.get("fresh_retest_exists") is True
        or inputs.get("fresh_failed_acceptance_exists") is True
        or inputs.get("fresh_pullback_exists") is True
    )
    ltf_confirmed = bool(
        _as_bool(_deep_get(payload, "metadata.ltf_confirmed", "ltf_confirmed", "metadata.ltf_model_confirmed", "ltf_model_confirmed"))
        or _as_bool(_deep_get(payload, "metadata.post_news_otd_ltf_confirmed", "post_news_otd_ltf_confirmed"))
        or _status_is_confirmed(_deep_get(payload, "metadata.ltf_model_status", "ltf_model_status", "metadata.ltf_status", "ltf_status"))
        or (post_news_otd_candidate and str(inputs.get("post_news_trade_permission") or "").upper() in {
            "ALLOW_BATTLE_IF_GEOMETRY_VALID",
            "ALLOW_CAUTION_BATTLE_IF_GEOMETRY_VALID",
        })
    )
    real_target = bool(
        _as_bool(_deep_get(payload, "metadata.real_target", "real_target", "metadata.has_real_target", "has_real_target"))
        or _as_bool(_deep_get(payload, "metadata.post_news_otd_real_target", "post_news_otd_real_target"))
        or inputs.get("target_quality") == "REAL_ZONE"
        or str(_deep_get(payload, "target_source", "metadata.target_source") or "").upper() in {"INTEREST_ZONE", "REAL_ZONE", "REAL_TARGET"}
        or bool(primary_zone)
        or inputs.get("target_price") is not None
        or _deep_get(payload, "target", "tp", "take_profit", "primary_target", "metadata.target", "metadata.tp", "metadata.take_profit") not in (None, "", [], {})
    )
    stop_ok = bool(
        _as_bool(_deep_get(payload, "metadata.stop_ok", "stop_ok"))
        or _as_bool(_deep_get(payload, "metadata.post_news_otd_stop_ok", "post_news_otd_stop_ok"))
        or _is_valid_stop_quality_for_battle(inputs.get("stop_quality"))
    )
    practical_rr = inputs.get("practical_rr")
    practical_rr_ok = bool(
        _as_bool(_deep_get(payload, "metadata.practical_rr_ok", "practical_rr_ok"))
        or post_news_otd_rr_ok
        or ((practical_rr is not None) and float(practical_rr or 0) >= post_news_min_rr)
    )

    return {
        **payload,
        **inputs,
        "acceptance_confirmed": acceptance_confirmed,
        "post_news_acceptance_confirmed": acceptance_confirmed,
        "retest_confirmed": retest_confirmed,
        "post_news_retest_confirmed": retest_confirmed,
        "ltf_confirmed": ltf_confirmed,
        "ltf_model_confirmed": ltf_confirmed,
        "real_target": real_target,
        "has_real_target": real_target,
        "stop_ok": stop_ok,
        "practical_rr_ok": practical_rr_ok,
        "macro_clearance": _as_bool(_deep_get(payload, "metadata.macro_clearance", "macro_clearance")) is True,
        "external_calendar_checked": _as_bool(_deep_get(payload, "metadata.external_calendar_checked", "external_calendar_checked")) is True,
        "press_conference_complete": _as_bool(_deep_get(payload, "metadata.press_conference_complete", "press_conference_complete")) is True,
        "fomc_press_conference_complete": _as_bool(_deep_get(payload, "metadata.fomc_press_conference_complete", "fomc_press_conference_complete")) is True,
    }


def _evaluate_macro_event_guard(inputs: dict[str, Any]) -> dict[str, Any]:
    symbol = inputs.get("symbol")
    payload = inputs.get("payload") if isinstance(inputs.get("payload"), dict) else {}

    if not symbol:
        return {
            "macro_guard_version": MACRO_EVENT_GUARD_VERSION,
            "macro_guard_status": "NOT_EVALUATED",
            "macro_guard_allowed_for_battle": True,
            "macro_guard_block_battle": False,
            "macro_guard_research_only": False,
            "macro_guard_suppress": False,
            "macro_guard_reason_code": "missing_symbol",
            "macro_guard_blockers": [],
            "macro_guard_requirements": [],
            "macro_guard_missing_requirements": [],
            "macro_guard_satisfied_requirements": [],
            "macro_guard_notes": ["Macro guard skipped because symbol is missing."],
        }

    if evaluate_macro_guard is None:
        return {
            "macro_guard_version": MACRO_EVENT_GUARD_VERSION,
            "macro_guard_status": "NOT_EVALUATED",
            "macro_guard_allowed_for_battle": True,
            "macro_guard_block_battle": False,
            "macro_guard_research_only": False,
            "macro_guard_suppress": False,
            "macro_guard_reason_code": "macro_event_guard_import_failed",
            "macro_guard_blockers": [],
            "macro_guard_requirements": [],
            "macro_guard_missing_requirements": [],
            "macro_guard_satisfied_requirements": [],
            "macro_guard_error": "app.services.macro_event_guard import failed",
        }

    try:
        decision = evaluate_macro_guard(
            str(symbol),
            report_date=_macro_guard_report_date(payload),
            timezone_name=_macro_guard_timezone(payload),
            as_of=_macro_guard_as_of(payload),
            context=_macro_guard_context(inputs),
        )
        return {
            "macro_guard_version": decision.version,
            "macro_guard_status": decision.status,
            "macro_guard_allowed_for_battle": decision.allowed_for_battle,
            "macro_guard_block_battle": decision.block_battle,
            "macro_guard_research_only": decision.research_only,
            "macro_guard_suppress": decision.suppress,
            "macro_guard_reason_code": decision.reason_code,
            "macro_guard_blockers": list(decision.blockers or []),
            "macro_guard_requirements": list(decision.requirements or []),
            "macro_guard_missing_requirements": list(decision.missing_requirements or []),
            "macro_guard_satisfied_requirements": list(decision.satisfied_requirements or []),
            "macro_guard_macro_risk_status": decision.macro_risk_status,
            "macro_guard_calendar_status": decision.calendar_status,
            "macro_guard_calendar_source": decision.calendar_source,
            "macro_guard_fallback_chain": list(decision.fallback_chain or []),
            "macro_guard_event_title": decision.event_title,
            "macro_guard_event_time_local": decision.event_time_local,
            "macro_guard_event_currency": decision.event_currency,
            "macro_guard_event_impact": decision.event_impact,
            "macro_guard_event_source": decision.event_source,
            "macro_guard_minutes_since_event": decision.minutes_since_event,
            "macro_guard_minutes_until_event": decision.minutes_until_event,
            "macro_guard_affected_symbols": list(decision.affected_symbols or []),
            "macro_guard_notes": list(decision.notes or []),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "macro_guard_version": MACRO_EVENT_GUARD_VERSION,
            "macro_guard_status": "ERROR",
            "macro_guard_allowed_for_battle": True,
            "macro_guard_block_battle": False,
            "macro_guard_research_only": False,
            "macro_guard_suppress": False,
            "macro_guard_reason_code": "macro_event_guard_error",
            "macro_guard_blockers": [],
            "macro_guard_requirements": [],
            "macro_guard_missing_requirements": [],
            "macro_guard_satisfied_requirements": [],
            "macro_guard_error": f"{type(exc).__name__}: {exc}",
        }

def _has_fresh_structure_after_impulse(inputs: dict[str, Any]) -> bool:
    if inputs.get("fresh_retest_exists") is True:
        return True
    if inputs.get("fresh_failed_acceptance_exists") is True:
        return True
    if inputs.get("fresh_pullback_exists") is True:
        return True

    post_news_retest_status = _as_upper(inputs.get("post_news_retest_status"))
    post_news_acceptance_status = _as_upper(inputs.get("post_news_acceptance_status"))
    post_news_failed_move = _as_bool(inputs.get("post_news_failed_move"))
    if post_news_retest_status in {"CONFIRMED", "VALID", "CLEAN", "PASSED"}:
        return True
    if post_news_acceptance_status in {"FAILED_ACCEPTANCE", "REJECTED", "FAILED", "CLEAN_REJECTION"}:
        return True
    if post_news_failed_move is True:
        return True

    return False


def _infer_target_quality(payload: dict[str, Any], primary_interest_zone: dict[str, Any] | None) -> str | None:
    explicit = _normalize_target_quality(
        _deep_get(
            payload,
            "metadata.target_quality",
            "metadata.execution.target_quality",
            "metadata.execution_plan.target_quality",
            "execution.target_quality",
            "execution_plan.target_quality",
            "target_quality",
        )
    )
    if explicit:
        return explicit

    if isinstance(primary_interest_zone, dict) and primary_interest_zone:
        zone_type = _as_upper(primary_interest_zone.get("zone_type") or primary_interest_zone.get("type"))
        zone_price = _as_float(primary_interest_zone.get("price") or primary_interest_zone.get("level"))
        if zone_type or zone_price is not None:
            return "REAL_ZONE"

    return None


def _infer_local_structure_damaged(
    payload: dict[str, Any],
    *,
    direction: str | None,
    scenario: str | None,
    entry_model_hint: str | None,
) -> bool:
    explicit_bool = _as_bool(
        _deep_get(
            payload,
            "metadata.local_structure_damaged",
            "metadata.structure.local_structure_damaged",
            "metadata.local_structure.status_damaged",
            "local_structure_damaged",
            "structure.local_structure_damaged",
        )
    )
    if explicit_bool is not None:
        return explicit_bool

    explicit_state = _as_upper(
        _deep_get(
            payload,
            "metadata.local_structure_state",
            "metadata.structure.local_structure_state",
            "local_structure_state",
            "structure.local_structure_state",
        )
    )
    if explicit_state in {"DAMAGED", "BROKEN", "STRUCTURE_DAMAGED", "LOCAL_DAMAGE"}:
        return True

    recent_impulse_direction = _normalize_direction(
        _deep_get(
            payload,
            "metadata.recent_impulse_direction",
            "metadata.liquidation_impulse_direction",
            "metadata.displacement_direction",
            "recent_impulse_direction",
            "liquidation_impulse_direction",
            "displacement_direction",
        )
    )
    impulse_strength = _as_float(
        _deep_get(
            payload,
            "metadata.recent_impulse_atr",
            "metadata.displacement_atr",
            "metadata.liquidation_impulse_atr",
            "recent_impulse_atr",
            "displacement_atr",
            "liquidation_impulse_atr",
        )
    )

    direction_norm = _normalize_direction(direction)
    if (
        direction_norm in {"LONG", "SHORT"}
        and recent_impulse_direction in {"LONG", "SHORT"}
        and recent_impulse_direction != direction_norm
        and (impulse_strength is None or impulse_strength >= 1.5)
    ):
        return True

    text = " ".join(
        str(value).upper()
        for value in [
            scenario,
            entry_model_hint,
            _deep_get(payload, "metadata.scenario_family", "scenario_family"),
            _deep_get(payload, "metadata.battle_gate_v2_risk_mode", "battle_gate_v2_risk_mode"),
            _deep_get(payload, "metadata.battle_gate_v2_modifiers", "battle_gate_v2_modifiers"),
            _deep_get(payload, "metadata.battle_gate_v2_reasons", "battle_gate_v2_reasons"),
        ]
        if value not in (None, "", [], {})
    )

    return any(
        token in text
        for token in {
            "POST_LIQUIDATION",
            "LIQUIDATION",
            "LOCAL_STRUCTURE_DAMAGED",
            "STRUCTURE_DAMAGED",
            "LOCAL_DAMAGE",
        }
    )


def _is_tpo_auction_execution_context(inputs: dict[str, Any]) -> bool:
    """Return True only for payloads that are actually in the TPO auction execution lane.

    Important: battle_permission.py enriches every signal with the current TPO store.
    Therefore raw open_behavior/current_open_behavior alone must not force a normal
    SWEEP_RETURN/TREND_CONTINUATION signal into the TPO LTF gate. The hard
    OPEN_AUCTION block remains global, but the LTF confirmation requirement is
    applied only when the payload has entered the TPO watch/LTF lane.
    """
    scenario_family = _as_upper(inputs.get("scenario_family"))
    scenario = _as_upper(inputs.get("scenario")) or ""
    tpo_watch_state = _as_upper(inputs.get("tpo_watch_state"))
    tpo_watch_setup = _as_upper(inputs.get("tpo_watch_setup"))
    auction_ltf_setup = _as_upper(inputs.get("auction_ltf_setup"))
    ltf_model_state = _as_upper(inputs.get("ltf_model_state"))
    ltf_model_state_full = _as_upper(inputs.get("ltf_model_state_full"))
    ltf_model_outcome = _as_upper(inputs.get("ltf_model_outcome"))

    if scenario_family in {"TPO_OPEN_DRIVE", "TPO_OPEN_TEST_DRIVE", "TPO_OPEN_REJECTION_REVERSE", "TPO_OPEN_AUCTION_BREAKOUT", "TPO_OPEN_AUCTION_BACK_TO_VALUE"}:
        return True
    if any(token in scenario for token in {"TPO_OPEN_DRIVE", "TPO_OPEN_TEST_DRIVE", "TPO_OPEN_REJECTION_REVERSE", "TPO_OPEN_AUCTION_BREAKOUT", "TPO_OPEN_AUCTION_BACK_TO_VALUE"}):
        return True
    if tpo_watch_state in TPO_AUCTION_WATCH_STATES:
        return True
    if tpo_watch_setup in TPO_DIRECTIONAL_AUCTION_BEHAVIORS or auction_ltf_setup in TPO_AUCTION_LTF_SETUP_FAMILIES:
        return True
    if ltf_model_state or ltf_model_state_full or ltf_model_outcome:
        return True

    return False


def _is_open_auction_context(inputs: dict[str, Any]) -> bool:
    return (
        _as_upper(inputs.get("open_behavior")) in OPEN_AUCTION_BEHAVIORS
        or _as_upper(inputs.get("current_open_behavior")) in OPEN_AUCTION_BEHAVIORS
        or _as_upper(inputs.get("initial_open_behavior")) in OPEN_AUCTION_BEHAVIORS
        or _as_upper(inputs.get("tpo_watch_setup")) in OPEN_AUCTION_BEHAVIORS
    )


def _ltf_model_executable(inputs: dict[str, Any]) -> bool:
    ltf_state = _as_upper(inputs.get("ltf_model_state"))
    ltf_state_full = _as_upper(inputs.get("ltf_model_state_full"))
    ltf_outcome = _as_upper(inputs.get("ltf_model_outcome"))
    ltf_confirmed = _as_bool(inputs.get("ltf_model_confirmed"))
    execution_status = _as_upper(inputs.get("execution_status"))

    return (
        ltf_confirmed is True
        and (ltf_state == "CONFIRMED" or ltf_state_full == "LTF_MODEL_CONFIRMED")
        and ltf_outcome in LTF_EXECUTABLE_OUTCOMES
        and execution_status == "EXECUTABLE"
    )


def _open_auction_ltf_exception(inputs: dict[str, Any]) -> bool:
    """Allow an OPEN_AUCTION context through only after a proven branch + executable LTF model."""
    if not _ltf_model_executable(inputs):
        return False

    tpo_watch_state = _as_upper(inputs.get("tpo_watch_state"))
    auction_ltf_setup = _as_upper(inputs.get("auction_ltf_setup"))
    tpo_watch_setup = _as_upper(inputs.get("tpo_watch_setup")) or ""

    return (
        tpo_watch_state == "LTF_MODEL_PENDING"
        and (
            auction_ltf_setup in OPEN_AUCTION_LTF_EXCEPTION_SETUPS
            or "OPEN_AUCTION" in tpo_watch_setup
        )
    )


def _trend_day_counter_fade_block(inputs: dict[str, Any]) -> tuple[bool, str | None]:
    """Never fade a confirmed Trend/DD day while one-timeframing has not broken."""
    direction = _normalize_direction(inputs.get("direction"))
    signal_alignment = _as_upper(inputs.get("signal_alignment"))
    day_type = _as_upper(inputs.get("day_type_candidate"))
    structure_state = _as_upper(inputs.get("structure_state"))
    one_timeframing_state = _as_upper(inputs.get("one_timeframing_state"))

    haystack = " ".join(x for x in [day_type, structure_state, one_timeframing_state] if x)
    if "ONE_TIMEFRAMING_UP" in haystack and direction == "SHORT":
        return True, "short_against_active_one_timeframing_up"
    if "ONE_TIMEFRAMING_DOWN" in haystack and direction == "LONG":
        return True, "long_against_active_one_timeframing_down"

    if day_type in {"TREND_DAY", "DOUBLE_DISTRIBUTION_TREND_DAY"} and signal_alignment == "COUNTER_TREND":
        return True, f"counter_trend_against_{day_type.lower()}"

    return False, None


def _derive_scenario_family(
    payload: dict[str, Any],
    *,
    scenario: str | None,
    open_behavior: str | None,
    entry_model_hint: str | None,
    news_risk_state: str | None,
    local_structure_damaged: bool | None,
) -> str | None:
    explicit = _as_upper(_deep_get(payload, "metadata.scenario_family", "scenario_family"))
    if explicit:
        return explicit

    scenario_norm = _as_upper(scenario) or ""
    open_behavior_norm = _as_upper(open_behavior) or ""
    entry_model_norm = _as_upper(entry_model_hint) or ""
    text = " ".join([scenario_norm, open_behavior_norm, entry_model_norm])

    if "POST_LIQUIDATION" in text or "LIQUIDATION_RECLAIM" in text:
        return "POST_LIQUIDATION_RECLAIM"

    if "POST_NEWS" in text or "NEWS_RECLAIM" in text:
        return "POST_NEWS_RECLAIM"

    if local_structure_damaged and (
        open_behavior_norm == "OPEN_TEST_DRIVE"
        or "OPEN_TEST_DRIVE" in scenario_norm
        or "FAILED_ACCEPTANCE_RETEST" in entry_model_norm
    ):
        if _normalize_news_risk_state(news_risk_state) in {"PROVIDER_UNAVAILABLE", "POST_NEWS_CAUTION", "HIGH_IMPACT"}:
            return "POST_NEWS_RECLAIM"
        return "POST_LIQUIDATION_RECLAIM"

    if "OPEN_AUCTION_BACK_TO_VALUE" in text:
        return "TPO_OPEN_AUCTION_BACK_TO_VALUE"

    if "OPEN_AUCTION_BREAKOUT" in text:
        return "TPO_OPEN_AUCTION_BREAKOUT"

    if "OPEN_REJECTION_REVERSE" in text:
        return "TPO_OPEN_REJECTION_REVERSE"

    if "OPEN_TEST_DRIVE" in text:
        return "TPO_OPEN_TEST_DRIVE"

    if "OPEN_DRIVE" in text:
        return "TPO_OPEN_DRIVE"

    if "SWEEP_RETURN" in text:
        return "SWEEP_RETURN"

    if "TREND_CONTINUATION" in text:
        return "TREND_CONTINUATION"

    return scenario_norm or None


def _v2_allows_neutral_open_test_drive_transition(
    *,
    inputs: dict[str, Any],
    v2_policy: dict[str, Any],
) -> bool:
    """
    Authoritative narrow override for the legacy HTF gate.

    Important implementation detail:
    battle_permission.extract_battle_inputs() may not always carry raw TPO fields
    such as open_behavior/open_context from the original payload. Therefore this
    helper must not re-derive the OPEN_TEST_DRIVE + HTF NEUTRAL context from
    legacy inputs. Battle Gate v2 already evaluates the raw payload and returns
    TRANSITION_CANDIDATE only for that exact model.

    So this bridge trusts the v2 decision, while legacy hard blockers remain
    enforced elsewhere in apply_battle_permission().
    """
    decision = _as_upper(v2_policy.get("decision"))
    risk_mode = _as_upper(v2_policy.get("risk_mode"))
    battle_allowed = _as_bool(v2_policy.get("battle_allowed"))
    should_suppress = _as_bool(v2_policy.get("should_suppress_telegram"))

    return (
        battle_allowed is True
        and should_suppress is not True
        and decision in {"ALLOW", "ALLOW_WITH_CAUTION"}
        and risk_mode == "TRANSITION_CANDIDATE"
    )


def _extract_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Some journal events are shaped like:
    {"payload": {"payload": signal}}

    Telegram payloads are usually already flat enough.
    This keeps the gate tolerant.
    """
    nested = payload.get("payload")

    if isinstance(nested, dict):
        nested_2 = nested.get("payload")
        if isinstance(nested_2, dict):
            return nested_2
        return nested

    return payload


_TPO_STORE_CACHE: dict[str, Any] = {
    "path": None,
    "mtime": None,
    "data": None,
}


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _set_if_missing(target: dict[str, Any], key: str, value: Any) -> None:
    if value in (None, "", [], {}):
        return
    if target.get(key) in (None, "", [], {}):
        target[key] = value


def _resolve_tpo_store_path() -> Path:
    """
    Resolve TPO store path without requiring settings import.

    Priority:
    1. TPO_STORE_PATH env.
    2. RUNTIME_DIR env + /tpo/tpo_latest.json.
    3. /var/data/runtime/tpo/tpo_latest.json on Render.
    4. runtime/tpo/tpo_latest.json locally.
    """
    explicit = os.getenv("TPO_STORE_PATH")
    if explicit:
        return Path(explicit)

    runtime_dir = os.getenv("RUNTIME_DIR")
    if runtime_dir:
        return Path(runtime_dir) / "tpo" / "tpo_latest.json"

    render_path = Path("/var/data/runtime/tpo/tpo_latest.json")
    if render_path.exists():
        return render_path

    return Path("runtime/tpo/tpo_latest.json")


def _load_tpo_store() -> dict[str, Any] | None:
    """
    Load tpo_latest.json with mtime cache.

    The store is small enough to read when changed, but this avoids parsing it
    for every signal during a busy cycle.
    """
    path = _resolve_tpo_store_path()

    try:
        stat = path.stat()
    except OSError:
        return None

    cached_path = _TPO_STORE_CACHE.get("path")
    cached_mtime = _TPO_STORE_CACHE.get("mtime")

    if cached_path == str(path) and cached_mtime == stat.st_mtime:
        data = _TPO_STORE_CACHE.get("data")
        return data if isinstance(data, dict) else None

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None

    if not isinstance(data, dict):
        return None

    _TPO_STORE_CACHE["path"] = str(path)
    _TPO_STORE_CACHE["mtime"] = stat.st_mtime
    _TPO_STORE_CACHE["data"] = data
    return data


def _extract_primary_interest_zone(*sources: Any) -> dict[str, Any] | None:
    for source in sources:
        if not isinstance(source, dict):
            continue

        zone = source.get("primary_interest_zone")
        if isinstance(zone, dict) and zone:
            return dict(zone)

        zone = source.get("interest_zone")
        if isinstance(zone, dict) and zone:
            return dict(zone)

    return None


def _get_symbol_tpo_record(symbol: str | None) -> dict[str, Any] | None:
    if not symbol:
        return None

    store = _load_tpo_store()
    if not isinstance(store, dict):
        return None

    symbols = store.get("symbols")
    if not isinstance(symbols, dict):
        return None

    exact = symbols.get(symbol)
    if isinstance(exact, dict):
        return exact

    upper_symbol = str(symbol).upper()
    for key, value in symbols.items():
        if str(key).upper() == upper_symbol and isinstance(value, dict):
            return value

    return None


def _enrich_payload_with_tpo_store(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Attach TPO/open-behavior fields from tpo_latest.json when the signal payload
    does not already contain them.

    This is intentionally defensive:
    - it never fails the gate if the store is missing/bad;
    - it does not overwrite already-present signal fields;
    - it keeps Battle Gate v2 in shadow mode but makes its inputs visible to
      payload/metadata/telemetry/statistics.
    """
    enriched = dict(payload)

    metadata = enriched.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    else:
        metadata = dict(metadata)

    symbol = _deep_get(
        enriched,
        "symbol",
        "instrument",
        "metadata.symbol",
        "metadata.instrument",
    )
    symbol = str(symbol).upper() if symbol not in (None, "", [], {}) else None

    record = _get_symbol_tpo_record(symbol)
    if not isinstance(record, dict):
        enriched["metadata"] = metadata
        return enriched

    context = record.get("context")
    if not isinstance(context, dict):
        context = {}

    filters = record.get("filters")
    if not isinstance(filters, dict):
        filters = {}

    open_behavior_record = record.get("open_behavior")
    if not isinstance(open_behavior_record, dict):
        open_behavior_record = {}

    primary_zone = _extract_primary_interest_zone(open_behavior_record, context, filters, record)

    values = {
        "market_status": _first_non_empty(
            context.get("market_status"),
            filters.get("market_status"),
            record.get("market_status"),
        ),
        "market_is_open": _first_non_empty(
            context.get("market_is_open"),
            filters.get("market_is_open"),
            record.get("market_is_open"),
        ),
        "tpo_signal_permission": _first_non_empty(
            context.get("tpo_signal_permission"),
            filters.get("tpo_signal_permission"),
            filters.get("signal_permission"),
            record.get("tpo_signal_permission"),
            record.get("signal_permission"),
        ),
        "tpo_telegram_modifier": _first_non_empty(
            context.get("tpo_telegram_modifier"),
            filters.get("tpo_telegram_modifier"),
            filters.get("telegram_modifier"),
            record.get("tpo_telegram_modifier"),
            record.get("telegram_modifier"),
        ),
        "telegram_modifier": _first_non_empty(
            filters.get("telegram_modifier"),
            context.get("telegram_modifier"),
            record.get("telegram_modifier"),
        ),
        "open_relation": _first_non_empty(
            context.get("open_relation"),
            filters.get("open_relation"),
            record.get("open_relation"),
        ),
        "auction_bias": _first_non_empty(
            context.get("auction_bias"),
            filters.get("auction_bias"),
            record.get("auction_bias"),
        ),
        "open_context": _first_non_empty(
            context.get("open_context"),
            open_behavior_record.get("open_context"),
            record.get("open_context"),
        ),
        "open_behavior": _first_non_empty(
            context.get("open_behavior"),
            open_behavior_record.get("open_behavior"),
            record.get("open_behavior") if not isinstance(record.get("open_behavior"), dict) else None,
        ),
        "open_behavior_confidence": _first_non_empty(
            context.get("open_behavior_confidence"),
            open_behavior_record.get("open_behavior_confidence"),
            open_behavior_record.get("confidence"),
            record.get("open_behavior_confidence"),
        ),
        "open_location": _first_non_empty(
            context.get("open_location"),
            open_behavior_record.get("open_location"),
            record.get("open_location"),
        ),
        "initial_open_behavior": _first_non_empty(
            context.get("initial_open_behavior"),
            open_behavior_record.get("initial_open_behavior"),
            record.get("initial_open_behavior"),
        ),
        "current_open_behavior": _first_non_empty(
            context.get("current_open_behavior"),
            context.get("updated_open_behavior"),
            open_behavior_record.get("current_open_behavior"),
            open_behavior_record.get("updated_open_behavior"),
            record.get("current_open_behavior"),
            record.get("updated_open_behavior"),
        ),
        "behavior_transition": _first_non_empty(
            context.get("behavior_transition"),
            open_behavior_record.get("behavior_transition"),
            record.get("behavior_transition"),
        ),
        "value_acceptance_state": _first_non_empty(
            context.get("value_acceptance_state"),
            open_behavior_record.get("value_acceptance_state"),
            record.get("value_acceptance_state"),
        ),
        "value_test_occurred": _first_non_empty(
            context.get("value_test_occurred"),
            open_behavior_record.get("value_test_occurred"),
            record.get("value_test_occurred"),
        ),
        "value_rejection_confirmed": _first_non_empty(
            context.get("value_rejection_confirmed"),
            open_behavior_record.get("value_rejection_confirmed"),
            record.get("value_rejection_confirmed"),
        ),
        "day_type_candidate": _first_non_empty(
            context.get("day_type_candidate"),
            open_behavior_record.get("day_type_candidate"),
            record.get("day_type_candidate"),
        ),
        "auction_state_confidence": _first_non_empty(
            context.get("auction_state_confidence"),
            open_behavior_record.get("auction_state_confidence"),
            record.get("auction_state_confidence"),
        ),
        "auction_state_reason": _first_non_empty(
            context.get("auction_state_reason"),
            open_behavior_record.get("auction_state_reason"),
            record.get("auction_state_reason"),
        ),
        "entry_model_hint": _first_non_empty(
            context.get("entry_model_hint"),
            open_behavior_record.get("entry_model_hint"),
            record.get("entry_model_hint"),
        ),
        "stop_model_hint": _first_non_empty(
            context.get("stop_model_hint"),
            open_behavior_record.get("stop_model_hint"),
            record.get("stop_model_hint"),
        ),
        "battle_bias_hint": _first_non_empty(
            context.get("battle_bias_hint"),
            open_behavior_record.get("battle_bias_hint"),
            record.get("battle_bias_hint"),
        ),
        "nearest_npoc_distance": _first_non_empty(
            context.get("nearest_npoc_distance"),
            filters.get("nearest_npoc_distance"),
            record.get("nearest_npoc_distance"),
        ),
        "ib_extension_up_pct": _first_non_empty(
            context.get("ib_extension_up_pct"),
            filters.get("ib_extension_up_pct"),
            record.get("ib_extension_up_pct"),
        ),
        "ib_extension_down_pct": _first_non_empty(
            context.get("ib_extension_down_pct"),
            filters.get("ib_extension_down_pct"),
            record.get("ib_extension_down_pct"),
        ),
        "accepted_back_inside_value": _first_non_empty(
            context.get("accepted_back_inside_value"),
            filters.get("accepted_back_inside_value"),
            record.get("accepted_back_inside_value"),
        ),
        "session_label": _first_non_empty(
            context.get("session_label"),
            context.get("session"),
            filters.get("session_label"),
            filters.get("session"),
            record.get("session_label"),
            record.get("session"),
        ),
        "news_risk_state": _first_non_empty(
            context.get("news_risk_state"),
            filters.get("news_risk_state"),
            record.get("news_risk_state"),
        ),
        "news_provider_status": _first_non_empty(
            context.get("news_provider_status"),
            context.get("calendar_status"),
            filters.get("news_provider_status"),
            filters.get("calendar_status"),
            record.get("news_provider_status"),
            record.get("calendar_status"),
        ),
        "local_structure_damaged": _first_non_empty(
            context.get("local_structure_damaged"),
            filters.get("local_structure_damaged"),
            record.get("local_structure_damaged"),
        ),
        "scenario_family": _first_non_empty(
            context.get("scenario_family"),
            filters.get("scenario_family"),
            record.get("scenario_family"),
        ),
        "target_quality": _first_non_empty(
            context.get("target_quality"),
            filters.get("target_quality"),
            record.get("target_quality"),
        ),
    }

    for key, value in values.items():
        _set_if_missing(enriched, key, value)
        _set_if_missing(metadata, key, value)

    if primary_zone:
        _set_if_missing(enriched, "primary_interest_zone", primary_zone)
        _set_if_missing(metadata, "primary_interest_zone", primary_zone)

        _set_if_missing(enriched, "interest_zone_type", primary_zone.get("zone_type"))
        _set_if_missing(metadata, "interest_zone_type", primary_zone.get("zone_type"))

        _set_if_missing(enriched, "interest_zone_price", primary_zone.get("price"))
        _set_if_missing(metadata, "interest_zone_price", primary_zone.get("price"))

        _set_if_missing(enriched, "interest_zone_role", primary_zone.get("role"))
        _set_if_missing(metadata, "interest_zone_role", primary_zone.get("role"))

    enriched["metadata"] = metadata
    return enriched



def _evaluate_v2_shadow(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Evaluate Battle Gate v2 policy in shadow mode.

    This must never break legacy Battle Gate.
    If v2 policy import/evaluation fails, legacy gate still works and we attach an error marker.
    """
    if evaluate_open_behavior_policy is None:
        return {
            "decision": None,
            "risk_mode": None,
            "battle_allowed": None,
            "should_suppress_telegram": None,
            "score_delta": None,
            "reasons": [],
            "blockers": [],
            "modifiers": [],
            "error": "battle_gate_open_behavior_policy_import_failed",
        }

    try:
        return evaluate_open_behavior_policy(payload)
    except Exception as exc:  # noqa: BLE001
        return {
            "decision": None,
            "risk_mode": None,
            "battle_allowed": None,
            "should_suppress_telegram": None,
            "score_delta": None,
            "reasons": [],
            "blockers": [],
            "modifiers": [],
            "error": f"{type(exc).__name__}: {exc}",
        }


def extract_battle_inputs(raw_payload: dict[str, Any]) -> dict[str, Any]:
    payload = _enrich_payload_with_tpo_store(_extract_payload(raw_payload))

    if apply_post_news_continuation is not None:
        try:
            payload = apply_post_news_continuation(payload)
        except Exception:  # noqa: BLE001
            # Post-news detector is advisory. It must never break legacy Battle Gate.
            pass

    if apply_macro_shock_context is not None:
        try:
            payload = apply_macro_shock_context(payload)
        except Exception:  # noqa: BLE001
            # Macro detector is advisory. It must never break legacy Battle Gate.
            pass

    freshness = _compute_signal_freshness(payload)
    trade_price_levels = _extract_trade_price_levels(payload)
    fresh_retest_flags = _extract_fresh_retest_flags(payload)

    symbol = _normalize_symbol(
        _deep_get(
            payload,
            "symbol",
            "instrument",
            "ticker",
            "metadata.symbol",
            "metadata.instrument",
            "metadata.ticker",
        )
    )

    session_label = _as_upper(
        _deep_get(
            payload,
            "metadata.session_label",
            "metadata.session",
            "metadata.report_session",
            "session_label",
            "session",
            "report_session",
        )
    )

    market_is_open = _as_bool(
        _deep_get(
            payload,
            "metadata.auction_context.market_is_open",
            "metadata.auction_filters.market_is_open",
            "auction_context.market_is_open",
            "auction_filters.market_is_open",
            "market_is_open",
        )
    )

    market_status = _as_upper(
        _deep_get(
            payload,
            "metadata.auction_context.market_status",
            "metadata.auction_filters.market_status",
            "auction_context.market_status",
            "auction_filters.market_status",
            "market_status",
        )
    )

    tpo_signal_permission = _as_upper(
        _deep_get(
            payload,
            "metadata.tpo_signal_permission",
            "metadata.signal_permission",
            "metadata.auction_filters.tpo_signal_permission",
            "metadata.auction_filters.signal_permission",
            "metadata.filters.tpo_signal_permission",
            "metadata.filters.signal_permission",
            "filters.tpo_signal_permission",
            "filters.signal_permission",
            "context.tpo_signal_permission",
            "context.signal_permission",
            "auction_filters.tpo_signal_permission",
            "auction_filters.signal_permission",
            "tpo_signal_permission",
            "signal_permission",
        )
    )

    tpo_telegram_modifier = _as_upper(
        _deep_get(
            payload,
            "metadata.tpo_telegram_modifier",
            "metadata.telegram_modifier",
            "metadata.auction_filters.telegram_modifier",
            "metadata.auction_filters.tpo_telegram_modifier",
            "metadata.filters.telegram_modifier",
            "metadata.filters.tpo_telegram_modifier",
            "filters.telegram_modifier",
            "filters.tpo_telegram_modifier",
            "context.telegram_modifier",
            "context.tpo_telegram_modifier",
            "auction_filters.telegram_modifier",
            "auction_filters.tpo_telegram_modifier",
            "telegram_modifier",
            "tpo_telegram_modifier",
        )
    )

    open_relation = _normalize_open_relation(
        _deep_get(
            payload,
            "metadata.tpo_open_relation",
            "metadata.open_relation",
            "metadata.auction_context.open_relation",
            "metadata.auction_filters.open_relation",
            "metadata.context.open_relation",
            "metadata.filters.open_relation",
            "context.open_relation",
            "filters.open_relation",
            "auction_context.open_relation",
            "auction_filters.open_relation",
            "open_relation",
        )
    )

    auction_bias = _as_upper(
        _deep_get(
            payload,
            "metadata.tpo_auction_bias",
            "metadata.auction_bias",
            "metadata.auction_context.auction_bias",
            "metadata.auction_filters.auction_bias",
            "metadata.context.auction_bias",
            "metadata.filters.auction_bias",
            "context.auction_bias",
            "filters.auction_bias",
            "auction_context.auction_bias",
            "auction_filters.auction_bias",
            "auction_bias",
        )
    )

    open_context = _as_upper(
        _deep_get(
            payload,
            "metadata.open_context",
            "metadata.context.open_context",
            "metadata.open_behavior.open_context",
            "context.open_context",
            "open_behavior.open_context",
            "open_context",
        )
    )

    open_behavior = _as_upper(
        _deep_get(
            payload,
            "metadata.open_behavior",
            "metadata.context.open_behavior",
            "metadata.open_behavior.open_behavior",
            "context.open_behavior",
            "open_behavior.open_behavior",
            "open_behavior",
        )
    )

    open_behavior_confidence = _as_float(
        _deep_get(
            payload,
            "metadata.open_behavior_confidence",
            "metadata.context.open_behavior_confidence",
            "metadata.open_behavior.open_behavior_confidence",
            "metadata.open_behavior.confidence",
            "context.open_behavior_confidence",
            "open_behavior.open_behavior_confidence",
            "open_behavior.confidence",
            "open_behavior_confidence",
        )
    )

    open_location = _as_upper(
        _deep_get(
            payload,
            "metadata.open_location",
            "metadata.context.open_location",
            "metadata.open_behavior.open_location",
            "context.open_location",
            "open_behavior.open_location",
            "open_location",
        )
    )

    initial_open_behavior = _as_upper(
        _deep_get(
            payload,
            "metadata.initial_open_behavior",
            "metadata.context.initial_open_behavior",
            "metadata.open_behavior.initial_open_behavior",
            "context.initial_open_behavior",
            "open_behavior.initial_open_behavior",
            "initial_open_behavior",
        )
    )

    current_open_behavior = _as_upper(
        _deep_get(
            payload,
            "metadata.current_open_behavior",
            "metadata.updated_open_behavior",
            "metadata.context.current_open_behavior",
            "metadata.context.updated_open_behavior",
            "metadata.open_behavior.current_open_behavior",
            "metadata.open_behavior.updated_open_behavior",
            "context.current_open_behavior",
            "context.updated_open_behavior",
            "open_behavior.current_open_behavior",
            "open_behavior.updated_open_behavior",
            "current_open_behavior",
            "updated_open_behavior",
        )
    )

    behavior_transition = _as_upper(
        _deep_get(
            payload,
            "metadata.behavior_transition",
            "metadata.context.behavior_transition",
            "metadata.open_behavior.behavior_transition",
            "context.behavior_transition",
            "open_behavior.behavior_transition",
            "behavior_transition",
        )
    )

    value_acceptance_state = _as_upper(
        _deep_get(
            payload,
            "metadata.value_acceptance_state",
            "metadata.context.value_acceptance_state",
            "metadata.open_behavior.value_acceptance_state",
            "context.value_acceptance_state",
            "open_behavior.value_acceptance_state",
            "value_acceptance_state",
        )
    )

    value_test_occurred = _as_bool(
        _deep_get(
            payload,
            "metadata.value_test_occurred",
            "metadata.context.value_test_occurred",
            "metadata.open_behavior.value_test_occurred",
            "context.value_test_occurred",
            "open_behavior.value_test_occurred",
            "value_test_occurred",
        )
    )

    value_rejection_confirmed = _as_bool(
        _deep_get(
            payload,
            "metadata.value_rejection_confirmed",
            "metadata.context.value_rejection_confirmed",
            "metadata.open_behavior.value_rejection_confirmed",
            "context.value_rejection_confirmed",
            "open_behavior.value_rejection_confirmed",
            "value_rejection_confirmed",
        )
    )

    day_type_candidate = _as_upper(
        _deep_get(
            payload,
            "metadata.day_type_candidate",
            "metadata.context.day_type_candidate",
            "metadata.open_behavior.day_type_candidate",
            "context.day_type_candidate",
            "open_behavior.day_type_candidate",
            "day_type_candidate",
        )
    )

    structure_state = _as_upper(
        _deep_get(
            payload,
            "metadata.structure_state",
            "metadata.local_structure_state",
            "metadata.auction_context.structure_state",
            "metadata.context.structure_state",
            "metadata.open_behavior.structure_state",
            "context.structure_state",
            "open_behavior.structure_state",
            "structure_state",
            "local_structure_state",
        )
    )

    one_timeframing_state = _as_upper(
        _deep_get(
            payload,
            "metadata.one_timeframing_state",
            "metadata.auction_context.one_timeframing_state",
            "metadata.context.one_timeframing_state",
            "metadata.open_behavior.one_timeframing_state",
            "context.one_timeframing_state",
            "open_behavior.one_timeframing_state",
            "one_timeframing_state",
        )
    )

    auction_state_confidence = _as_float(
        _deep_get(
            payload,
            "metadata.auction_state_confidence",
            "metadata.context.auction_state_confidence",
            "metadata.open_behavior.auction_state_confidence",
            "context.auction_state_confidence",
            "open_behavior.auction_state_confidence",
            "auction_state_confidence",
        )
    )

    auction_state_reason = _deep_get(
        payload,
        "metadata.auction_state_reason",
        "metadata.context.auction_state_reason",
        "metadata.open_behavior.auction_state_reason",
        "context.auction_state_reason",
        "open_behavior.auction_state_reason",
        "auction_state_reason",
    )

    tpo_watch_state = _as_upper(_deep_get(payload, "metadata.tpo_watch_state", "tpo_watch_state"))
    tpo_watch_setup = _as_upper(_deep_get(payload, "metadata.tpo_watch_setup", "tpo_watch_setup"))
    tpo_watch_active = _as_bool(_deep_get(payload, "metadata.tpo_watch_active", "tpo_watch_active"))
    auction_ltf_setup = _as_upper(_deep_get(payload, "metadata.auction_ltf_setup", "auction_ltf_setup"))

    ltf_model_detector_version = _deep_get(payload, "metadata.ltf_model_detector_version", "ltf_model_detector_version")
    ltf_model_state = _as_upper(_deep_get(payload, "metadata.ltf_model_state", "ltf_model_state"))
    ltf_model_state_full = _as_upper(_deep_get(payload, "metadata.ltf_model_state_full", "ltf_model_state_full"))
    ltf_model_outcome = _as_upper(_deep_get(payload, "metadata.ltf_model_outcome", "ltf_model_outcome"))
    ltf_model_type = _as_upper(_deep_get(payload, "metadata.ltf_model_type", "ltf_model_type"))
    ltf_model_confirmed = _as_bool(_deep_get(payload, "metadata.ltf_model_confirmed", "ltf_model_confirmed"))
    ltf_model_blockers = _as_text_list(_deep_get(payload, "metadata.ltf_model_blockers", "ltf_model_blockers"))
    ltf_model_warnings = _as_text_list(_deep_get(payload, "metadata.ltf_model_warnings", "ltf_model_warnings"))

    entry_model_hint = _as_upper(
        _deep_get(
            payload,
            "metadata.entry_model_hint",
            "metadata.context.entry_model_hint",
            "metadata.open_behavior.entry_model_hint",
            "context.entry_model_hint",
            "open_behavior.entry_model_hint",
            "entry_model_hint",
        )
    )

    stop_model_hint = _as_upper(
        _deep_get(
            payload,
            "metadata.stop_model_hint",
            "metadata.context.stop_model_hint",
            "metadata.open_behavior.stop_model_hint",
            "context.stop_model_hint",
            "open_behavior.stop_model_hint",
            "stop_model_hint",
        )
    )

    battle_bias_hint = _as_upper(
        _deep_get(
            payload,
            "metadata.battle_bias_hint",
            "metadata.context.battle_bias_hint",
            "metadata.open_behavior.battle_bias_hint",
            "context.battle_bias_hint",
            "open_behavior.battle_bias_hint",
            "battle_bias_hint",
        )
    )

    primary_interest_zone = _deep_get(
        payload,
        "metadata.primary_interest_zone",
        "metadata.open_behavior.primary_interest_zone",
        "open_behavior.primary_interest_zone",
        "primary_interest_zone",
    )
    if not isinstance(primary_interest_zone, dict):
        primary_interest_zone = None

    interest_zone_type = _as_upper(
        _deep_get(
            payload,
            "metadata.interest_zone_type",
            "metadata.primary_interest_zone.zone_type",
            "metadata.open_behavior.primary_interest_zone.zone_type",
            "open_behavior.primary_interest_zone.zone_type",
            "primary_interest_zone.zone_type",
            "interest_zone_type",
        )
    )

    interest_zone_price = _as_float(
        _deep_get(
            payload,
            "metadata.interest_zone_price",
            "metadata.primary_interest_zone.price",
            "metadata.open_behavior.primary_interest_zone.price",
            "open_behavior.primary_interest_zone.price",
            "primary_interest_zone.price",
            "interest_zone_price",
        )
    )

    interest_zone_role = _as_upper(
        _deep_get(
            payload,
            "metadata.interest_zone_role",
            "metadata.primary_interest_zone.role",
            "metadata.open_behavior.primary_interest_zone.role",
            "open_behavior.primary_interest_zone.role",
            "primary_interest_zone.role",
            "interest_zone_role",
        )
    )

    direction = _normalize_direction(
        _deep_get(
            payload,
            "direction",
            "trade_direction",
            "metadata.direction",
        )
    )

    htf_bias = _normalize_direction(
        _deep_get(
            payload,
            "htf_bias",
            "metadata.htf_bias",
            "context.htf_bias",
        )
    )

    signal_alignment = _as_upper(
        _deep_get(
            payload,
            "signal_alignment",
            "alignment",
            "metadata.signal_alignment",
            "metadata.alignment",
        )
    )

    execution_status = _as_upper(
        _deep_get(
            payload,
            "execution_status",
            "metadata.execution_status",
            "execution.status",
        )
    )

    practical_rr = _as_float(
        _deep_get(
            payload,
            "practical_rr",
            "rr",
            "risk_reward",
            "metadata.practical_rr",
            "metadata.rr",
            "execution.practical_rr",
        )
    )

    stop_quality = _as_upper(
        _deep_get(
            payload,
            "stop_quality",
            "metadata.stop_quality",
            "execution.stop_quality",
        )
    )

    quality_tier = _as_upper(
        _deep_get(
            payload,
            "quality_tier",
            "quality_level",
            "metadata.quality_tier",
            "metadata.quality_level",
        )
    )

    status = _as_upper(
        _deep_get(
            payload,
            "status",
            "alert_type",
            "signal_class",
        )
    )

    market_state = _as_upper(
        _deep_get(
            payload,
            "market_state",
            "metadata.market_state",
            "context.market_state",
        )
    )

    scenario = _as_upper(
        _deep_get(
            payload,
            "scenario",
            "metadata.scenario",
        )
    )

    nearest_npoc_distance = _as_float(
        _deep_get(
            payload,
            "metadata.auction_context.nearest_npoc_distance",
            "auction_context.nearest_npoc_distance",
            "nearest_npoc_distance",
        )
    )

    ib_extension_up_pct = _as_float(
        _deep_get(
            payload,
            "metadata.auction_context.ib_extension_up_pct",
            "auction_context.ib_extension_up_pct",
            "ib_extension_up_pct",
        )
    )

    ib_extension_down_pct = _as_float(
        _deep_get(
            payload,
            "metadata.auction_context.ib_extension_down_pct",
            "auction_context.ib_extension_down_pct",
            "ib_extension_down_pct",
        )
    )

    accepted_back_inside_value = _as_bool(
        _deep_get(
            payload,
            "metadata.auction_context.accepted_back_inside_value",
            "auction_context.accepted_back_inside_value",
            "accepted_back_inside_value",
        )
    )

    news_risk_state = _normalize_news_risk_state(
        _deep_get(
            payload,
            "metadata.news_risk_state",
            "metadata.news.risk_state",
            "metadata.economic_calendar.risk_state",
            "metadata.calendar.risk_state",
            "news_risk_state",
            "news.risk_state",
            "economic_calendar.risk_state",
            "calendar.risk_state",
        )
    )

    news_provider_status = _as_upper(
        _deep_get(
            payload,
            "metadata.news_provider_status",
            "metadata.news.provider_status",
            "metadata.economic_calendar.status",
            "metadata.calendar.status",
            "metadata.calendar_status",
            "news_provider_status",
            "news.provider_status",
            "economic_calendar.status",
            "calendar.status",
            "calendar_status",
            "provider_status",
        )
    )

    target_quality = _infer_target_quality(payload, primary_interest_zone)

    local_structure_damaged = _infer_local_structure_damaged(
        payload,
        direction=direction,
        scenario=scenario,
        entry_model_hint=entry_model_hint,
    )

    scenario_family = _derive_scenario_family(
        payload,
        scenario=scenario,
        open_behavior=open_behavior,
        entry_model_hint=entry_model_hint,
        news_risk_state=news_risk_state,
        local_structure_damaged=local_structure_damaged,
    )

    return {
        "payload": payload,
        "symbol": symbol,
        "session_label": session_label,
        "market_is_open": market_is_open,
        "market_status": market_status,
        "tpo_signal_permission": tpo_signal_permission,
        "tpo_telegram_modifier": tpo_telegram_modifier,
        "open_relation": open_relation,
        "auction_bias": auction_bias,
        "open_context": open_context,
        "open_behavior": open_behavior,
        "open_behavior_confidence": open_behavior_confidence,
        "open_location": open_location,
        "initial_open_behavior": initial_open_behavior,
        "current_open_behavior": current_open_behavior,
        "behavior_transition": behavior_transition,
        "value_acceptance_state": value_acceptance_state,
        "value_test_occurred": value_test_occurred,
        "value_rejection_confirmed": value_rejection_confirmed,
        "day_type_candidate": day_type_candidate,
        "structure_state": structure_state,
        "one_timeframing_state": one_timeframing_state,
        "auction_state_confidence": auction_state_confidence,
        "auction_state_reason": auction_state_reason,
        "tpo_watch_state": tpo_watch_state,
        "tpo_watch_setup": tpo_watch_setup,
        "tpo_watch_active": tpo_watch_active,
        "auction_ltf_setup": auction_ltf_setup,
        "ltf_model_detector_version": ltf_model_detector_version,
        "ltf_model_state": ltf_model_state,
        "ltf_model_state_full": ltf_model_state_full,
        "ltf_model_outcome": ltf_model_outcome,
        "ltf_model_type": ltf_model_type,
        "ltf_model_confirmed": ltf_model_confirmed,
        "ltf_model_blockers": ltf_model_blockers,
        "ltf_model_warnings": ltf_model_warnings,
        "entry_model_hint": entry_model_hint,
        "stop_model_hint": stop_model_hint,
        "battle_bias_hint": battle_bias_hint,
        "primary_interest_zone": primary_interest_zone,
        "interest_zone_type": interest_zone_type,
        "interest_zone_price": interest_zone_price,
        "interest_zone_role": interest_zone_role,
        "direction": direction,
        "htf_bias": htf_bias,
        "signal_alignment": signal_alignment,
        "execution_status": execution_status,
        "practical_rr": practical_rr,
        "stop_quality": stop_quality,
        "quality_tier": quality_tier,
        "news_risk_state": news_risk_state,
        "news_provider_status": news_provider_status,
        "local_structure_damaged": local_structure_damaged,
        "scenario_family": scenario_family,
        "target_quality": target_quality,
        "status": status,
        "market_state": market_state,
        "scenario": scenario,
        "nearest_npoc_distance": nearest_npoc_distance,
        "ib_extension_up_pct": ib_extension_up_pct,
        "ib_extension_down_pct": ib_extension_down_pct,
        "accepted_back_inside_value": accepted_back_inside_value,
        "post_news_detector_version": _deep_get(payload, "metadata.post_news_detector_version", "post_news_detector_version"),
        "post_news_regime": _as_upper(_deep_get(payload, "metadata.post_news_regime", "post_news_regime")),
        "post_news_trade_permission": _as_upper(_deep_get(payload, "metadata.post_news_trade_permission", "post_news_trade_permission")),
        "post_news_elapsed_minutes": _as_float(_deep_get(payload, "metadata.post_news_elapsed_minutes", "post_news_elapsed_minutes")),
        "post_news_impulse_direction": _normalize_direction(_deep_get(payload, "metadata.post_news_impulse_direction", "post_news_impulse_direction")),
        "post_news_impulse_confirmed": _as_bool(_deep_get(payload, "metadata.post_news_impulse_confirmed", "post_news_impulse_confirmed")),
        "post_news_retest_level": _deep_get(payload, "metadata.post_news_retest_level", "post_news_retest_level"),
        "post_news_retest_status": _as_upper(_deep_get(payload, "metadata.post_news_retest_status", "post_news_retest_status")),
        "post_news_acceptance_status": _as_upper(_deep_get(payload, "metadata.post_news_acceptance_status", "post_news_acceptance_status")),
        "post_news_failed_move": _as_bool(_deep_get(payload, "metadata.post_news_failed_move", "post_news_failed_move")),
        "post_news_continuation_quality": _as_upper(_deep_get(payload, "metadata.post_news_continuation_quality", "post_news_continuation_quality")),
        "post_news_continuation_direction": _normalize_direction(_deep_get(payload, "metadata.post_news_continuation_direction", "post_news_continuation_direction")),
        "post_news_reasons": _as_text_list(_deep_get(payload, "metadata.post_news_reasons", "post_news_reasons")),
        "post_news_blockers": _as_text_list(_deep_get(payload, "metadata.post_news_blockers", "post_news_blockers")),
        "post_news_modifiers": _as_text_list(_deep_get(payload, "metadata.post_news_modifiers", "post_news_modifiers")),
        "signal_created_at_utc": freshness.get("signal_created_at_utc"),
        "signal_age_minutes": freshness.get("signal_age_minutes"),
        "signal_max_age_minutes": freshness.get("signal_max_age_minutes"),
        "signal_freshness_status": freshness.get("signal_freshness_status"),
        "macro_detector_version": _deep_get(payload, "metadata.macro_detector_version", "macro_detector_version"),
        "macro_regime": _as_upper(_deep_get(payload, "metadata.macro_regime", "macro_regime")),
        "macro_shock_recent": _as_bool(_deep_get(payload, "metadata.macro_shock_recent", "macro_shock_recent")),
        "macro_shock_score": _as_float(_deep_get(payload, "metadata.macro_shock_score", "macro_shock_score")),
        "macro_risk_mode": _as_upper(_deep_get(payload, "metadata.macro_risk_mode", "macro_risk_mode")),
        "macro_direction_for_symbol": _as_upper(_deep_get(payload, "metadata.macro_direction_for_symbol", "macro_direction_for_symbol")),
        "macro_caution_flags": _as_text_list(_deep_get(payload, "metadata.macro_caution_flags", "macro_caution_flags")),
        "macro_reasons": _as_text_list(_deep_get(payload, "metadata.macro_reasons", "macro_reasons")),
        "entry_price": trade_price_levels.get("entry_price"),
        "invalidation_price": trade_price_levels.get("invalidation_price"),
        "target_price": trade_price_levels.get("target_price"),
        "current_price": trade_price_levels.get("current_price"),
        "fresh_retest_exists": fresh_retest_flags.get("fresh_retest_exists"),
        "fresh_failed_acceptance_exists": fresh_retest_flags.get("fresh_failed_acceptance_exists"),
        "fresh_pullback_exists": fresh_retest_flags.get("fresh_pullback_exists"),
    }


def calculate_auction_context_score(inputs: dict[str, Any]) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

    open_relation = inputs.get("open_relation")
    direction = inputs.get("direction")
    htf_bias = inputs.get("htf_bias")
    nearest_npoc_distance = inputs.get("nearest_npoc_distance")
    ib_extension_up_pct = inputs.get("ib_extension_up_pct")
    ib_extension_down_pct = inputs.get("ib_extension_down_pct")
    accepted_back_inside_value = inputs.get("accepted_back_inside_value")
    local_structure_damaged = inputs.get("local_structure_damaged")
    target_quality = inputs.get("target_quality")
    news_risk_state = inputs.get("news_risk_state")
    news_provider_status = inputs.get("news_provider_status")
    symbol = inputs.get("symbol")

    if open_relation == "OUT_OF_RANGE":
        score += 2
        reasons.append("open_relation OUT_OF_RANGE: +2")

    elif open_relation == "RANGE":
        score += 1
        reasons.append("open_relation RANGE: +1")

    elif open_relation == "INSIDE_VA":
        score -= 2
        reasons.append("open_relation INSIDE_VA: -2")

    if _direction_matches_htf(direction, htf_bias):
        score += 2
        reasons.append("direction aligned with HTF: +2")
    else:
        reasons.append("direction not aligned with HTF: +0")

    if nearest_npoc_distance is not None:
        score += 1
        reasons.append("nearest nPOC available as interest zone: +1")

    direction_norm = _normalize_direction(direction)

    if direction_norm == "LONG" and ib_extension_up_pct is not None and ib_extension_up_pct >= 0.5:
        score += 1
        reasons.append("IB upside extension >= 0.5 in LONG direction: +1")

    if direction_norm == "SHORT" and ib_extension_down_pct is not None and ib_extension_down_pct >= 0.5:
        score += 1
        reasons.append("IB downside extension >= 0.5 in SHORT direction: +1")

    if accepted_back_inside_value is True:
        score -= 2
        reasons.append("accepted back inside value: -2")

    if target_quality == "REAL_ZONE":
        score += 1
        reasons.append("target is a real interest zone: +1")
    elif target_quality == "SYNTHETIC":
        score -= 2
        reasons.append("target is synthetic/RR-only: -2")

    if local_structure_damaged is True:
        score -= 1
        reasons.append("local structure damaged after impulse: -1")

    if _is_news_provider_unavailable(news_risk_state, news_provider_status) and _is_usd_sensitive_symbol(symbol):
        score -= 1
        reasons.append("USD-sensitive symbol with unavailable news provider: -1")

    return score, reasons


def _build_result(
    *,
    inputs: dict[str, Any],
    auction_score: int,
    reasons: list[str],
    blockers: list[str],
    modifiers: list[str],
    battle_permission: str,
    telegram_delivery_mode: str,
    battle_ready: bool,
    v2_policy: dict[str, Any],
    risk_mode: str | None = None,
    caution_flags: list[str] | None = None,
    statistics_only: bool = False,
    suppression_reason: str | None = None,
    suppression_reasons: list[str] | None = None,
) -> BattlePermissionResult:
    return BattlePermissionResult(
        battle_permission=battle_permission,
        telegram_delivery_mode=telegram_delivery_mode,
        battle_ready=battle_ready,
        auction_context_score=auction_score,
        reasons=reasons,
        blockers=blockers,
        modifiers=modifiers,
        market_is_open=inputs.get("market_is_open"),
        market_status=inputs.get("market_status"),
        tpo_signal_permission=inputs.get("tpo_signal_permission"),
        tpo_telegram_modifier=inputs.get("tpo_telegram_modifier"),
        open_relation=inputs.get("open_relation"),
        auction_bias=inputs.get("auction_bias"),
        open_context=inputs.get("open_context"),
        open_behavior=inputs.get("open_behavior"),
        open_behavior_confidence=inputs.get("open_behavior_confidence"),
        entry_model_hint=inputs.get("entry_model_hint"),
        stop_model_hint=inputs.get("stop_model_hint"),
        battle_bias_hint=inputs.get("battle_bias_hint"),
        primary_interest_zone=inputs.get("primary_interest_zone"),
        interest_zone_type=inputs.get("interest_zone_type"),
        interest_zone_price=inputs.get("interest_zone_price"),
        interest_zone_role=inputs.get("interest_zone_role"),
        open_location=inputs.get("open_location"),
        initial_open_behavior=inputs.get("initial_open_behavior"),
        current_open_behavior=inputs.get("current_open_behavior"),
        behavior_transition=inputs.get("behavior_transition"),
        value_acceptance_state=inputs.get("value_acceptance_state"),
        value_test_occurred=inputs.get("value_test_occurred"),
        value_rejection_confirmed=inputs.get("value_rejection_confirmed"),
        day_type_candidate=inputs.get("day_type_candidate"),
        structure_state=inputs.get("structure_state"),
        one_timeframing_state=inputs.get("one_timeframing_state"),
        auction_state_confidence=inputs.get("auction_state_confidence"),
        auction_state_reason=inputs.get("auction_state_reason"),
        tpo_watch_state=inputs.get("tpo_watch_state"),
        tpo_watch_setup=inputs.get("tpo_watch_setup"),
        tpo_watch_active=inputs.get("tpo_watch_active"),
        auction_ltf_setup=inputs.get("auction_ltf_setup"),
        ltf_model_detector_version=inputs.get("ltf_model_detector_version"),
        ltf_model_state=inputs.get("ltf_model_state"),
        ltf_model_state_full=inputs.get("ltf_model_state_full"),
        ltf_model_outcome=inputs.get("ltf_model_outcome"),
        ltf_model_type=inputs.get("ltf_model_type"),
        ltf_model_confirmed=inputs.get("ltf_model_confirmed"),
        ltf_model_blockers=list(inputs.get("ltf_model_blockers") or []),
        ltf_model_warnings=list(inputs.get("ltf_model_warnings") or []),
        direction=inputs.get("direction"),
        htf_bias=inputs.get("htf_bias"),
        signal_alignment=inputs.get("signal_alignment"),
        execution_status=inputs.get("execution_status"),
        practical_rr=inputs.get("practical_rr"),
        stop_quality=inputs.get("stop_quality"),
        quality_tier=inputs.get("quality_tier"),
        symbol=inputs.get("symbol"),
        session_label=inputs.get("session_label"),
        news_risk_state=inputs.get("news_risk_state"),
        news_provider_status=inputs.get("news_provider_status"),
        local_structure_damaged=inputs.get("local_structure_damaged"),
        scenario_family=inputs.get("scenario_family"),
        target_quality=inputs.get("target_quality"),
        risk_mode=risk_mode,
        caution_flags=list(caution_flags or []),
        statistics_only=bool(statistics_only),
        suppression_reason=suppression_reason,
        suppression_reasons=list(suppression_reasons or []),
        post_news_detector_version=inputs.get("post_news_detector_version"),
        post_news_regime=inputs.get("post_news_regime"),
        post_news_trade_permission=inputs.get("post_news_trade_permission"),
        post_news_elapsed_minutes=int(inputs.get("post_news_elapsed_minutes")) if inputs.get("post_news_elapsed_minutes") is not None else None,
        post_news_impulse_direction=inputs.get("post_news_impulse_direction"),
        post_news_impulse_confirmed=inputs.get("post_news_impulse_confirmed"),
        post_news_retest_level=inputs.get("post_news_retest_level"),
        post_news_retest_status=inputs.get("post_news_retest_status"),
        post_news_acceptance_status=inputs.get("post_news_acceptance_status"),
        post_news_failed_move=inputs.get("post_news_failed_move"),
        post_news_continuation_quality=inputs.get("post_news_continuation_quality"),
        post_news_continuation_direction=inputs.get("post_news_continuation_direction"),
        post_news_reasons=list(inputs.get("post_news_reasons") or []),
        post_news_blockers=list(inputs.get("post_news_blockers") or []),
        post_news_modifiers=list(inputs.get("post_news_modifiers") or []),
        signal_created_at_utc=inputs.get("signal_created_at_utc"),
        signal_age_minutes=inputs.get("signal_age_minutes"),
        signal_max_age_minutes=inputs.get("signal_max_age_minutes"),
        signal_freshness_status=inputs.get("signal_freshness_status"),
        macro_detector_version=inputs.get("macro_detector_version"),
        macro_regime=inputs.get("macro_regime"),
        macro_shock_recent=inputs.get("macro_shock_recent"),
        macro_shock_score=inputs.get("macro_shock_score"),
        macro_risk_mode=inputs.get("macro_risk_mode"),
        macro_direction_for_symbol=inputs.get("macro_direction_for_symbol"),
        macro_caution_flags=list(inputs.get("macro_caution_flags") or []),
        macro_reasons=list(inputs.get("macro_reasons") or []),
        macro_guard_version=inputs.get("macro_guard_version"),
        macro_guard_status=inputs.get("macro_guard_status"),
        macro_guard_allowed_for_battle=inputs.get("macro_guard_allowed_for_battle"),
        macro_guard_block_battle=inputs.get("macro_guard_block_battle"),
        macro_guard_research_only=inputs.get("macro_guard_research_only"),
        macro_guard_suppress=inputs.get("macro_guard_suppress"),
        macro_guard_reason_code=inputs.get("macro_guard_reason_code"),
        macro_guard_blockers=list(inputs.get("macro_guard_blockers") or []),
        macro_guard_requirements=list(inputs.get("macro_guard_requirements") or []),
        macro_guard_missing_requirements=list(inputs.get("macro_guard_missing_requirements") or []),
        macro_guard_satisfied_requirements=list(inputs.get("macro_guard_satisfied_requirements") or []),
        macro_guard_macro_risk_status=inputs.get("macro_guard_macro_risk_status"),
        macro_guard_calendar_status=inputs.get("macro_guard_calendar_status"),
        macro_guard_calendar_source=inputs.get("macro_guard_calendar_source"),
        macro_guard_fallback_chain=list(inputs.get("macro_guard_fallback_chain") or []),
        macro_guard_event_title=inputs.get("macro_guard_event_title"),
        macro_guard_event_time_local=inputs.get("macro_guard_event_time_local"),
        macro_guard_event_currency=inputs.get("macro_guard_event_currency"),
        macro_guard_event_impact=inputs.get("macro_guard_event_impact"),
        macro_guard_event_source=inputs.get("macro_guard_event_source"),
        macro_guard_minutes_since_event=inputs.get("macro_guard_minutes_since_event"),
        macro_guard_minutes_until_event=inputs.get("macro_guard_minutes_until_event"),
        macro_guard_affected_symbols=list(inputs.get("macro_guard_affected_symbols") or []),
        macro_guard_notes=list(inputs.get("macro_guard_notes") or []),
        macro_guard_error=inputs.get("macro_guard_error"),
        entry_price=inputs.get("entry_price"),
        invalidation_price=inputs.get("invalidation_price"),
        target_price=inputs.get("target_price"),
        current_price=inputs.get("current_price"),
        impulse_progress=inputs.get("impulse_progress"),
        impulse_progress_pct=inputs.get("impulse_progress_pct"),
        impulse_state=inputs.get("impulse_state"),
        fresh_retest_exists=inputs.get("fresh_retest_exists"),
        fresh_failed_acceptance_exists=inputs.get("fresh_failed_acceptance_exists"),
        fresh_pullback_exists=inputs.get("fresh_pullback_exists"),
        battle_gate_v2_decision=v2_policy.get("decision"),
        battle_gate_v2_risk_mode=v2_policy.get("risk_mode"),
        battle_gate_v2_battle_allowed=v2_policy.get("battle_allowed"),
        battle_gate_v2_should_suppress_telegram=v2_policy.get("should_suppress_telegram"),
        battle_gate_v2_score_delta=v2_policy.get("score_delta"),
        battle_gate_v2_reasons=list(v2_policy.get("reasons") or []),
        battle_gate_v2_blockers=list(v2_policy.get("blockers") or []),
        battle_gate_v2_modifiers=list(v2_policy.get("modifiers") or []),
        battle_gate_v2_error=v2_policy.get("error"),
    )


def evaluate_battle_permission(raw_payload: dict[str, Any]) -> BattlePermissionResult:
    inputs = extract_battle_inputs(raw_payload)
    inputs.update(_evaluate_macro_event_guard(inputs))
    auction_score, score_reasons = calculate_auction_context_score(inputs)
    v2_policy = _evaluate_v2_shadow(inputs.get("payload") if isinstance(inputs.get("payload"), dict) else raw_payload)

    reasons: list[str] = list(score_reasons)
    blockers: list[str] = []
    modifiers: list[str] = []

    market_is_open = inputs.get("market_is_open")
    market_status = inputs.get("market_status")
    tpo_signal_permission = inputs.get("tpo_signal_permission")
    tpo_telegram_modifier = inputs.get("tpo_telegram_modifier")
    direction = inputs.get("direction")
    htf_bias = inputs.get("htf_bias")
    signal_alignment = inputs.get("signal_alignment")
    execution_status = inputs.get("execution_status")
    practical_rr = inputs.get("practical_rr")
    stop_quality = inputs.get("stop_quality")
    quality_tier = inputs.get("quality_tier")
    status = inputs.get("status")
    market_state = inputs.get("market_state")
    scenario = inputs.get("scenario")
    symbol = inputs.get("symbol")
    news_risk_state = inputs.get("news_risk_state")
    news_provider_status = inputs.get("news_provider_status")
    local_structure_damaged = inputs.get("local_structure_damaged")
    scenario_family = inputs.get("scenario_family")
    target_quality = inputs.get("target_quality")
    open_behavior = _as_upper(inputs.get("open_behavior"))
    open_location = _as_upper(inputs.get("open_location"))
    initial_open_behavior = _as_upper(inputs.get("initial_open_behavior"))
    current_open_behavior = _as_upper(inputs.get("current_open_behavior"))
    value_acceptance_state = _as_upper(inputs.get("value_acceptance_state"))
    structure_state = _as_upper(inputs.get("structure_state"))
    one_timeframing_state = _as_upper(inputs.get("one_timeframing_state"))
    tpo_watch_state = _as_upper(inputs.get("tpo_watch_state"))
    tpo_watch_setup = _as_upper(inputs.get("tpo_watch_setup"))
    tpo_watch_active = _as_bool(inputs.get("tpo_watch_active"))
    auction_ltf_setup = _as_upper(inputs.get("auction_ltf_setup"))
    ltf_model_state = _as_upper(inputs.get("ltf_model_state"))
    ltf_model_state_full = _as_upper(inputs.get("ltf_model_state_full"))
    ltf_model_outcome = _as_upper(inputs.get("ltf_model_outcome"))
    ltf_model_confirmed = _as_bool(inputs.get("ltf_model_confirmed"))
    ltf_model_blockers = list(inputs.get("ltf_model_blockers") or [])
    ltf_model_warnings = list(inputs.get("ltf_model_warnings") or [])
    tpo_auction_context = _is_tpo_auction_execution_context(inputs)
    invalidation_price = inputs.get("invalidation_price")
    current_price = inputs.get("current_price")

    post_news_regime = inputs.get("post_news_regime")
    post_news_trade_permission = inputs.get("post_news_trade_permission")
    post_news_reasons = list(inputs.get("post_news_reasons") or [])
    post_news_blockers = list(inputs.get("post_news_blockers") or [])
    post_news_modifiers = list(inputs.get("post_news_modifiers") or [])

    signal_freshness_status = inputs.get("signal_freshness_status")
    signal_age_minutes = inputs.get("signal_age_minutes")
    signal_max_age_minutes = inputs.get("signal_max_age_minutes")

    macro_regime = inputs.get("macro_regime")
    macro_shock_recent = inputs.get("macro_shock_recent")
    macro_risk_mode = inputs.get("macro_risk_mode")
    macro_caution_flags = list(inputs.get("macro_caution_flags") or [])
    macro_reasons = list(inputs.get("macro_reasons") or [])

    macro_guard_status = inputs.get("macro_guard_status")
    macro_guard_block_battle = bool(inputs.get("macro_guard_block_battle"))
    macro_guard_suppress = bool(inputs.get("macro_guard_suppress"))
    macro_guard_reason_code = inputs.get("macro_guard_reason_code")
    macro_guard_blockers = list(inputs.get("macro_guard_blockers") or [])
    macro_guard_missing_requirements = list(inputs.get("macro_guard_missing_requirements") or [])
    macro_guard_event_title = inputs.get("macro_guard_event_title")
    macro_guard_error = inputs.get("macro_guard_error")

    first_impulse = _compute_first_impulse_state(inputs)
    inputs.update(first_impulse)
    impulse_state = inputs.get("impulse_state")
    impulse_progress_pct = inputs.get("impulse_progress_pct")
    fresh_structure_after_impulse = _has_fresh_structure_after_impulse(inputs)

    post_news_allows_clean_battle = post_news_trade_permission == "ALLOW_BATTLE_IF_GEOMETRY_VALID"
    post_news_allows_caution_battle = post_news_trade_permission == "ALLOW_CAUTION_BATTLE_IF_GEOMETRY_VALID"
    post_news_allows_battle = post_news_allows_clean_battle or post_news_allows_caution_battle

    if post_news_regime and post_news_regime != "NOT_POST_NEWS":
        reasons.append(f"post_news_regime={post_news_regime}")
        reasons.extend(f"post_news: {reason}" for reason in post_news_reasons[:8])
        modifiers.extend(f"post_news_{modifier}" for modifier in post_news_modifiers if f"post_news_{modifier}" not in modifiers)

    if signal_freshness_status and signal_freshness_status != "UNKNOWN":
        reasons.append(
            f"signal_freshness={signal_freshness_status} age={signal_age_minutes}m max={signal_max_age_minutes}m"
        )

    if macro_regime and macro_regime != "NO_MACRO_SHOCK":
        reasons.append(f"macro_regime={macro_regime}")
        reasons.extend(f"macro: {reason}" for reason in macro_reasons[:6])

    if macro_guard_status and macro_guard_status not in {"MACRO_CLEAR", "NOT_EVALUATED"}:
        reasons.append(
            f"macro_guard_status={macro_guard_status} reason={macro_guard_reason_code} "
            f"event={macro_guard_event_title or '-'}"
        )
        if macro_guard_missing_requirements:
            reasons.append(f"macro_guard_missing_requirements={','.join(macro_guard_missing_requirements[:10])}")
        if macro_guard_error:
            reasons.append(f"macro_guard_error={macro_guard_error}")

    if impulse_state and impulse_state != "UNKNOWN":
        reasons.append(
            f"first_impulse_state={impulse_state} progress={impulse_progress_pct}% "
            f"fresh_structure_after_impulse={fresh_structure_after_impulse}"
        )

    if tpo_auction_context:
        reasons.append(
            "auction_state="
            f"open_location={open_location or '-'} "
            f"initial={initial_open_behavior or '-'} "
            f"current={current_open_behavior or '-'} "
            f"value_state={value_acceptance_state or '-'} "
            f"structure={structure_state or one_timeframing_state or '-'} "
            f"watch={tpo_watch_state or '-'} "
            f"ltf={ltf_model_state_full or ltf_model_state or '-'} "
            f"outcome={ltf_model_outcome or '-'}"
        )

    policy_flags = _collect_policy_hint_flags(inputs, v2_policy)
    policy_requires_research, policy_research_blockers = _policy_hint_requires_research_only(
        inputs=inputs,
        policy_flags=policy_flags,
        post_news_allows_battle=post_news_allows_battle,
    )
    policy_caution_flags = _policy_hint_requires_caution(inputs=inputs, policy_flags=policy_flags)

    caution_flags: list[str] = []

    if _is_news_provider_unavailable(news_risk_state, news_provider_status) and _is_usd_sensitive_symbol(symbol):
        caution_flags.append("news_provider_unavailable_usd_sensitive")

    if local_structure_damaged is True:
        caution_flags.append("local_structure_damaged")

    if scenario_family in {"POST_LIQUIDATION_RECLAIM", "POST_NEWS_RECLAIM"}:
        caution_flags.append(f"scenario_family_{str(scenario_family).lower()}")

    if target_quality == "UNKNOWN":
        caution_flags.append("target_quality_unknown")

    if quality_tier == "CAUTION":
        caution_flags.append("quality_tier_caution")

    if macro_shock_recent is True:
        caution_flags.append("macro_shock_recent")
        if macro_risk_mode:
            caution_flags.append(f"macro_risk_{str(macro_risk_mode).lower()}")
        caution_flags.extend(macro_caution_flags)

    caution_flags.extend(policy_caution_flags)

    if post_news_allows_caution_battle:
        caution_flags.append("post_news_caution_continuation")

    if post_news_allows_clean_battle:
        modifiers.append("post_news_clean_continuation")

    caution_flags = _dedupe_text_list(caution_flags)
    modifiers = _dedupe_text_list(modifiers)

    v2_neutral_otd_transition_allowed = _v2_allows_neutral_open_test_drive_transition(
        inputs=inputs,
        v2_policy=v2_policy,
    )

    if v2_neutral_otd_transition_allowed:
        modifiers.append("v2_neutral_otd_transition_allowed")
        reasons.append(
            "Battle Gate v2 allows OPEN_TEST_DRIVE with HTF NEUTRAL as a transition candidate; "
            "legacy HTF conflict block will not be applied to this case."
        )

    # 1. Absolute market / data blockers.
    if market_is_open is False or market_status in {"MARKET_CLOSED", "MARKET_CLOSED_AND_STALE"}:
        blockers.append("market_closed")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["market is closed; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_MARKET_CLOSED.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if market_status == "STALE_DATA" or tpo_signal_permission == "STALE_DATA":
        blockers.append("stale_data")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["market data is stale; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_STALE_DATA.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    # 1b. Statistics-only delivery suppression.
    # These are not user-facing trade ideas anymore. They remain in
    # journal/statistics/telemetry so we can measure how much noise was filtered.
    invalidated, invalidated_detail = _is_invalidated_before_alert(inputs)
    if invalidated:
        return _build_statistics_only_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons,
            blockers=blockers,
            modifiers=modifiers,
            v2_policy=v2_policy,
            risk_mode="INVALIDATED_BEFORE_ALERT",
            suppression_reason="invalidated_before_alert",
            detail=invalidated_detail,
            caution_flags=caution_flags,
        )

    post_shock_rr_low, post_shock_rr_detail = _post_shock_rr_below_statistics_min(inputs)
    if post_shock_rr_low:
        return _build_statistics_only_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons,
            blockers=blockers,
            modifiers=modifiers,
            v2_policy=v2_policy,
            risk_mode="POST_SHOCK_RR_BELOW_3",
            suppression_reason="post_shock_rr_below_3",
            detail=post_shock_rr_detail,
            caution_flags=caution_flags,
        )

    tpo_otd_long_stats_downgrade, tpo_otd_long_detail = _is_tpo_otd_long_stats_downgrade_required(inputs)
    if tpo_otd_long_stats_downgrade:
        return _build_statistics_only_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons,
            blockers=blockers,
            modifiers=modifiers,
            v2_policy=v2_policy,
            risk_mode="TPO_OTD_LONG_STATS_WEAK",
            suppression_reason="tpo_otd_long_stats_downgrade",
            detail=tpo_otd_long_detail,
            caution_flags=caution_flags,
        )

    # 2. First impulse / no-chase gate.
    # If the market has already travelled too far from planned entry to target,
    # Telegram Battle must not chase. It can only return after a new structure.
    if impulse_state == "EXHAUSTED":
        blockers.append("first_impulse_already_gone")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [
                f"first impulse already gone: price moved {impulse_progress_pct}% of the entry-to-target path; wait for a new structure"
            ],
            blockers=_dedupe_text_list(blockers),
            modifiers=modifiers,
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
            risk_mode="FIRST_IMPULSE_ALREADY_GONE",
            caution_flags=caution_flags,
        )

    if impulse_state == "LATE" and not fresh_structure_after_impulse:
        blockers.append("no_fresh_retest_after_impulse")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [
                f"price already moved {impulse_progress_pct}% toward target, but no fresh retest/failed acceptance/pullback exists"
            ],
            blockers=_dedupe_text_list(blockers),
            modifiers=modifiers,
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
            risk_mode="NO_FRESH_RETEST_AFTER_IMPULSE",
            caution_flags=caution_flags,
        )

    # 3. OPEN_AUCTION hard block.
    # OPEN_AUCTION is a rotation/research environment by default. It must not
    # reach Telegram as BATTLE_READY / CAUTION_BATTLE through legacy score,
    # HTF alignment, post-news continuation overrides, or good RR alone.
    #
    # Future exception can be added only when we explicitly detect:
    # IB break + acceptance + HTF alignment + LTF entry model + real target.
    # Until that model exists, OPEN_AUCTION remains research-only.
    open_auction_ltf_exception = _open_auction_ltf_exception(inputs)
    if _is_open_auction_context(inputs) and not open_auction_ltf_exception:
        blockers.append("open_auction_rotation_context")
        modifiers.append("open_auction_hard_block")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [
                "OPEN_AUCTION is observe/rotation context; battle disabled until acceptance/rejection branch + LTF model + real target are explicitly confirmed"
            ],
            blockers=_dedupe_text_list(blockers),
            modifiers=_dedupe_text_list(modifiers + policy_flags[:12]),
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
            risk_mode="OPEN_AUCTION_RESEARCH_ONLY",
            caution_flags=caution_flags,
        )

    if open_auction_ltf_exception:
        modifiers.append("open_auction_ltf_branch_exception")
        reasons.append(
            "OPEN_AUCTION branch exception: accepted breakout or failed-acceptance back-to-value has CONFIRMED_EXECUTABLE LTF model."
        )

    # 3b. Auction watch / LTF confirmation gate.
    # Classifier and Watch Bridge define auction context. Battle Gate must not
    # re-interpret OTD/ORR; it only consumes watch-state + LTF executable outcome.
    if tpo_auction_context:
        if tpo_watch_state in {"BLOCKED"}:
            blockers.append("tpo_watch_blocked")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + ["TPO Watch Bridge blocks this auction context; battle disabled"],
                blockers=_dedupe_text_list(blockers + ltf_model_blockers),
                modifiers=_dedupe_text_list(modifiers + ["auction_watch_blocked"]),
                battle_permission=BattlePermission.BLOCKED_BY_AUCTION.value,
                telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
                battle_ready=False,
                v2_policy=v2_policy,
                risk_mode="TPO_WATCH_BLOCKED",
                caution_flags=caution_flags,
            )

        if tpo_watch_state in {"OBSERVE_ONLY", "OBSERVE_ROTATION", "NO_WATCH", "RESEARCH_ONLY"}:
            blockers.append(f"tpo_watch_{str(tpo_watch_state or 'unknown').lower()}")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + [
                    f"TPO watch_state={tpo_watch_state}; auction context is not executable for Battle"
                ],
                blockers=_dedupe_text_list(blockers + ltf_model_blockers),
                modifiers=_dedupe_text_list(modifiers + ["auction_watch_not_executable"]),
                battle_permission=BattlePermission.RESEARCH_ONLY.value,
                telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
                battle_ready=False,
                v2_policy=v2_policy,
                risk_mode="TPO_WATCH_NOT_EXECUTABLE",
                caution_flags=caution_flags,
            )

        if tpo_watch_state != "LTF_MODEL_PENDING":
            blockers.append("tpo_watch_not_ltf_pending")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + [
                    f"TPO auction context has no LTF_MODEL_PENDING watch_state: {tpo_watch_state}; no Battle promotion"
                ],
                blockers=_dedupe_text_list(blockers + ltf_model_blockers),
                modifiers=_dedupe_text_list(modifiers + ["missing_ltf_watch_state"]),
                battle_permission=BattlePermission.NOT_READY.value,
                telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
                battle_ready=False,
                v2_policy=v2_policy,
                risk_mode="MISSING_LTF_WATCH_STATE",
                caution_flags=caution_flags,
            )

        if not _ltf_model_executable(inputs):
            blockers.append("ltf_model_not_confirmed_executable")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + [
                    "TPO auction watch is active, but LTF detector has not produced CONFIRMED_EXECUTABLE; no Battle promotion"
                ],
                blockers=_dedupe_text_list(blockers + ltf_model_blockers),
                modifiers=_dedupe_text_list(modifiers + ltf_model_warnings + ["ltf_model_required"]),
                battle_permission=BattlePermission.NOT_READY.value,
                telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
                battle_ready=False,
                v2_policy=v2_policy,
                risk_mode="LTF_MODEL_NOT_CONFIRMED_EXECUTABLE",
                caution_flags=caution_flags,
            )

        if auction_ltf_setup == "OPEN_REJECTION_REVERSE" or current_open_behavior == "OPEN_REJECTION_REVERSE":
            caution_flags.append("open_rejection_reverse_cautious_watch")

    # 3. Post-news state gate.
    # This is the key layer: first impulse remains NO CHASE, but confirmed retest
    # and acceptance can later release a clean continuation into the normal Battle Gate.
    if post_news_trade_permission in {"BLOCK_BATTLE", "SUPPRESS"}:
        blockers.extend(post_news_blockers or ["post_news_block_battle"])
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["post-news detector blocks battle: no chase / unsafe early auction"],
            blockers=_dedupe_text_list(blockers),
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_CONTEXT.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
            risk_mode="POST_NEWS_NO_CHASE",
            caution_flags=caution_flags,
        )

    if post_news_trade_permission == "RESEARCH_ONLY":
        blockers.extend(post_news_blockers or ["post_news_research_only"])
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["post-news detector allows research only until retest/acceptance is clean"],
            blockers=_dedupe_text_list(blockers),
            modifiers=modifiers,
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
            risk_mode="POST_NEWS_RESEARCH_ONLY",
            caution_flags=caution_flags,
        )

    # 3. TPO / auction research blockers.
    if tpo_signal_permission in {"MARKET_CLOSED", "RESEARCH_ONLY", "BLOCKED_BY_CONTEXT", "BLOCKED_BY_AUCTION"}:
        if post_news_allows_battle and tpo_signal_permission == "RESEARCH_ONLY":
            modifiers.append("post_news_continuation_overrides_tpo_research_only")
            reasons.append(
                "post-news detector confirms continuation; TPO RESEARCH_ONLY is not used as a hard blocker"
            )
        else:
            blockers.append(f"tpo_permission_{str(tpo_signal_permission).lower()}")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + [f"TPO permission is {tpo_signal_permission}; battle signal disabled"],
                blockers=blockers,
                modifiers=modifiers,
                battle_permission=BattlePermission.RESEARCH_ONLY.value,
                telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
                battle_ready=False,
                v2_policy=v2_policy,
            )

    if tpo_telegram_modifier == "DOWNGRADE":
        if post_news_allows_battle:
            modifiers.append("post_news_continuation_overrides_tpo_downgrade")
            reasons.append(
                "post-news detector confirms retest/acceptance continuation; TPO DOWNGRADE is not used as a hard blocker"
            )
        else:
            blockers.append("tpo_downgrade")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + ["TPO telegram modifier is DOWNGRADE; research only"],
                blockers=blockers,
                modifiers=modifiers,
                battle_permission=BattlePermission.RESEARCH_ONLY.value,
                telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
                battle_ready=False,
                v2_policy=v2_policy,
            )

    # 4. Technical readiness.
    if status not in {"READY", "ENTRY_READY", "EXECUTABLE"}:
        blockers.append("not_ready_status")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [f"status={status}; not a battle-ready signal"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.NOT_READY.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if execution_status != "EXECUTABLE":
        blockers.append("execution_not_executable")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [f"execution_status={execution_status}; not executable"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_EXECUTION.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    trend_fade_blocked, trend_fade_reason = _trend_day_counter_fade_block(inputs)
    if trend_fade_blocked:
        blockers.append("fade_against_confirmed_trend_day")
        modifiers.append("trend_day_no_counter_fade")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [
                f"confirmed Trend/DD day or active one-timeframing blocks counter-fade: {trend_fade_reason}"
            ],
            blockers=_dedupe_text_list(blockers),
            modifiers=_dedupe_text_list(modifiers),
            battle_permission=BattlePermission.BLOCKED_BY_CONTEXT.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
            risk_mode="TREND_DAY_NO_COUNTER_FADE",
            caution_flags=caution_flags,
        )

    # 5. Signal lifecycle / stale READY protection.
    if signal_freshness_status == "STALE_READY":
        blockers.append("stale_ready_signal")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [
                f"READY signal is stale: age={signal_age_minutes}m exceeds max={signal_max_age_minutes}m; "
                "new structure must generate a new signal_id"
            ],
            blockers=_dedupe_text_list(blockers),
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_CONTEXT.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
            risk_mode="STALE_READY",
            caution_flags=caution_flags,
        )

    if signal_freshness_status == "AGING_READY":
        caution_flags.append("signal_aging_ready")

    # 6. Macro event guard hard gate.
    # This is the execution-facing news lock: FOMC/high-impact/post-news states
    # must never be promoted to BATTLE_READY / BATTLE_ALERT until required
    # confirmations are present. It downgrades otherwise valid READY ideas to
    # RESEARCH_ALERT so Telegram shows the blocker instead of sending a battle call.
    if macro_guard_block_battle:
        blockers.extend(macro_guard_blockers or ["macro_event_guard"])
        modifiers.append(f"macro_guard_{str(macro_guard_status or 'blocked').lower()}")
        if macro_guard_event_title:
            modifiers.append("macro_guard_event_active")
        delivery_mode = (
            TelegramDeliveryMode.SUPPRESS.value
            if macro_guard_suppress
            else TelegramDeliveryMode.RESEARCH_ALERT.value
        )
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [
                "macro event guard blocks Battle promotion; no BATTLE_READY/BATTLE_ALERT until macro requirements are satisfied"
            ],
            blockers=_dedupe_text_list(blockers),
            modifiers=_dedupe_text_list(modifiers),
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=delivery_mode,
            battle_ready=False,
            v2_policy=v2_policy,
            risk_mode=str(macro_guard_status or "MACRO_GUARD_BLOCK"),
            caution_flags=caution_flags,
        )

    # 6. HTF alignment.
    if not _direction_matches_htf(direction, htf_bias):
        if v2_neutral_otd_transition_allowed:
            modifiers.append("legacy_htf_block_overridden_by_v2_neutral_otd")
            reasons.append(
                f"direction={direction} not aligned with htf_bias={htf_bias}, "
                "but OPEN_TEST_DRIVE + HTF NEUTRAL is treated as a valid transition candidate."
            )
        else:
            blockers.append("direction_not_aligned_with_htf")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + [f"direction={direction} not aligned with htf_bias={htf_bias}"],
                blockers=blockers,
                modifiers=modifiers,
                battle_permission=BattlePermission.BLOCKED_BY_HTF.value,
                telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
                battle_ready=False,
                v2_policy=v2_policy,
            )

    if signal_alignment == "COUNTER_TREND":
        if v2_neutral_otd_transition_allowed:
            modifiers.append("legacy_countertrend_label_overridden_by_v2_neutral_otd")
            reasons.append(
                "signal_alignment=COUNTER_TREND ignored for OPEN_TEST_DRIVE + HTF NEUTRAL transition candidate."
            )
        else:
            blockers.append("counter_trend")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + ["signal_alignment=COUNTER_TREND; battle signal disabled"],
                blockers=blockers,
                modifiers=modifiers,
                battle_permission=BattlePermission.BLOCKED_BY_HTF.value,
                telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
                battle_ready=False,
                v2_policy=v2_policy,
            )

    # 7. Open-behavior policy hints override legacy score.
    # Score is useful, but explicit policy hints are authoritative: OPEN_AUCTION
    # without a directional entry model or a research-only battle hint cannot be
    # promoted to clean BATTLE_READY by auction_score alone.
    if policy_requires_research:
        blockers.extend(policy_research_blockers)
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [
                "open-behavior/v2 policy hints require RESEARCH_ONLY; legacy auction score cannot override this"
            ],
            blockers=_dedupe_text_list(blockers),
            modifiers=_dedupe_text_list(modifiers + policy_flags[:12]),
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
            risk_mode="POLICY_RESEARCH_ONLY",
            caution_flags=caution_flags,
        )

    # 8. RR / stop / quality.
    if practical_rr is None or practical_rr < 2.0:
        blockers.append("practical_rr_below_2")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [f"practical_rr={practical_rr}; minimum is 2.0"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_RR.value,
            telegram_delivery_mode=TelegramDeliveryMode.SUPPRESS.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if stop_quality == "TIGHT_STOP":
        blockers.append("tight_stop")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["stop_quality=TIGHT_STOP; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_STOP_QUALITY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if quality_tier in {"DANGER", "BLOCK", "FAIL"}:
        blockers.append("quality_tier_blocked")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [f"quality_tier={quality_tier}; battle signal disabled"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.BLOCKED_BY_QUALITY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if quality_tier == "CAUTION" and market_state == "TRANSITION" and scenario in {"SWEEP_RETURN_LONG", "SWEEP_RETURN_SHORT"}:
        blockers.append("caution_transition_sweep_return")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["CAUTION + TRANSITION + SWEEP_RETURN; research only"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    if target_quality == "SYNTHETIC":
        blockers.append("synthetic_target")
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + ["target_quality=SYNTHETIC; battle signal disabled until target is a real interest zone"],
            blockers=blockers,
            modifiers=modifiers,
            battle_permission=BattlePermission.RESEARCH_ONLY.value,
            telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
            battle_ready=False,
            v2_policy=v2_policy,
        )

    # 7. Auction score final gate.
    if auction_score < 3:
        if v2_neutral_otd_transition_allowed:
            modifiers.append("auction_score_override_by_v2_neutral_otd")
            reasons.append(
                f"auction_context_score={auction_score} is below legacy minimum 3, "
                "but Battle Gate v2 allows this OPEN_TEST_DRIVE + HTF NEUTRAL transition candidate."
            )
        elif post_news_allows_battle:
            modifiers.append("auction_score_override_by_post_news_continuation")
            reasons.append(
                f"auction_context_score={auction_score} is below legacy minimum 3, "
                "but post-news retest/acceptance continuation is confirmed."
            )
        else:
            blockers.append("auction_context_score_below_3")
            return _build_result(
                inputs=inputs,
                auction_score=auction_score,
                reasons=reasons + [f"auction_context_score={auction_score}; minimum is 3"],
                blockers=blockers,
                modifiers=modifiers,
                battle_permission=BattlePermission.BLOCKED_BY_AUCTION.value,
                telegram_delivery_mode=TelegramDeliveryMode.RESEARCH_ALERT.value,
                battle_ready=False,
                v2_policy=v2_policy,
            )

    # 8. Battle ready / caution battle.
    if tpo_telegram_modifier == "BOOST":
        modifiers.append("tpo_boost")

    if caution_flags:
        modifiers.extend(flag for flag in caution_flags if flag not in modifiers)
        return _build_result(
            inputs=inputs,
            auction_score=auction_score,
            reasons=reasons + [
                "all hard battle permission checks passed",
                "Safety/Post-News Gate allows alert only as CAUTION_BATTLE, not clean BATTLE_READY",
            ],
            blockers=blockers,
            modifiers=_dedupe_text_list(modifiers),
            battle_permission=BattlePermission.CAUTION_BATTLE.value,
            telegram_delivery_mode=TelegramDeliveryMode.BATTLE_ALERT.value,
            battle_ready=True,
            v2_policy=v2_policy,
            risk_mode="CAUTION",
            caution_flags=caution_flags,
        )

    return _build_result(
        inputs=inputs,
        auction_score=auction_score,
        reasons=reasons + ["all battle permission checks passed"],
        blockers=blockers,
        modifiers=_dedupe_text_list(modifiers),
        battle_permission=BattlePermission.BATTLE_READY.value,
        telegram_delivery_mode=TelegramDeliveryMode.BATTLE_ALERT.value,
        battle_ready=True,
        v2_policy=v2_policy,
        risk_mode="NORMAL",
        caution_flags=[],
    )


def _attach_tpo_open_behavior_fields_to_metadata(metadata: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    metadata["market_is_open"] = result.get("market_is_open")
    metadata["market_status"] = result.get("market_status")
    metadata["tpo_signal_permission"] = result.get("tpo_signal_permission")
    metadata["tpo_telegram_modifier"] = result.get("tpo_telegram_modifier")
    metadata["open_relation"] = result.get("open_relation")
    metadata["auction_bias"] = result.get("auction_bias")

    metadata["open_context"] = result.get("open_context")
    metadata["open_behavior"] = result.get("open_behavior")
    metadata["open_behavior_confidence"] = result.get("open_behavior_confidence")
    for key in (
        "open_location",
        "initial_open_behavior",
        "current_open_behavior",
        "behavior_transition",
        "value_acceptance_state",
        "value_test_occurred",
        "value_rejection_confirmed",
        "day_type_candidate",
        "auction_state_confidence",
        "auction_state_reason",
        "tpo_watch_state",
        "tpo_watch_setup",
        "tpo_watch_active",
        "auction_ltf_setup",
        "ltf_model_detector_version",
        "ltf_model_state",
        "ltf_model_state_full",
        "ltf_model_outcome",
        "ltf_model_type",
        "ltf_model_confirmed",
        "ltf_model_blockers",
        "ltf_model_warnings",
    ):
        metadata[key] = result.get(key)
    metadata["entry_model_hint"] = result.get("entry_model_hint")
    metadata["stop_model_hint"] = result.get("stop_model_hint")
    metadata["battle_bias_hint"] = result.get("battle_bias_hint")
    metadata["primary_interest_zone"] = result.get("primary_interest_zone")
    metadata["interest_zone_type"] = result.get("interest_zone_type")
    metadata["interest_zone_price"] = result.get("interest_zone_price")
    metadata["interest_zone_role"] = result.get("interest_zone_role")

    metadata["symbol"] = result.get("symbol")
    metadata["session_label"] = result.get("session_label")
    metadata["news_risk_state"] = result.get("news_risk_state")
    metadata["news_provider_status"] = result.get("news_provider_status")
    metadata["local_structure_damaged"] = result.get("local_structure_damaged")
    metadata["scenario_family"] = result.get("scenario_family")
    metadata["target_quality"] = result.get("target_quality")
    metadata["risk_mode"] = result.get("risk_mode")
    metadata["caution_flags"] = result.get("caution_flags") or []
    metadata["statistics_only"] = result.get("statistics_only")
    metadata["suppression_reason"] = result.get("suppression_reason")
    metadata["suppression_reasons"] = result.get("suppression_reasons") or []

    metadata["post_news_detector_version"] = result.get("post_news_detector_version")
    metadata["post_news_regime"] = result.get("post_news_regime")
    metadata["post_news_trade_permission"] = result.get("post_news_trade_permission")
    metadata["post_news_elapsed_minutes"] = result.get("post_news_elapsed_minutes")
    metadata["post_news_impulse_direction"] = result.get("post_news_impulse_direction")
    metadata["post_news_impulse_confirmed"] = result.get("post_news_impulse_confirmed")
    metadata["post_news_retest_level"] = result.get("post_news_retest_level")
    metadata["post_news_retest_status"] = result.get("post_news_retest_status")
    metadata["post_news_acceptance_status"] = result.get("post_news_acceptance_status")
    metadata["post_news_failed_move"] = result.get("post_news_failed_move")
    metadata["post_news_continuation_quality"] = result.get("post_news_continuation_quality")
    metadata["post_news_continuation_direction"] = result.get("post_news_continuation_direction")
    metadata["post_news_reasons"] = result.get("post_news_reasons") or []
    metadata["post_news_blockers"] = result.get("post_news_blockers") or []
    metadata["post_news_modifiers"] = result.get("post_news_modifiers") or []

    metadata["signal_created_at_utc"] = result.get("signal_created_at_utc")
    metadata["signal_age_minutes"] = result.get("signal_age_minutes")
    metadata["signal_max_age_minutes"] = result.get("signal_max_age_minutes")
    metadata["signal_freshness_status"] = result.get("signal_freshness_status")

    metadata["macro_detector_version"] = result.get("macro_detector_version")
    metadata["macro_regime"] = result.get("macro_regime")
    metadata["macro_shock_recent"] = result.get("macro_shock_recent")
    metadata["macro_shock_score"] = result.get("macro_shock_score")
    metadata["macro_risk_mode"] = result.get("macro_risk_mode")
    metadata["macro_direction_for_symbol"] = result.get("macro_direction_for_symbol")
    metadata["macro_caution_flags"] = result.get("macro_caution_flags") or []
    metadata["macro_reasons"] = result.get("macro_reasons") or []

    metadata["macro_guard_version"] = result.get("macro_guard_version")
    metadata["macro_guard_status"] = result.get("macro_guard_status")
    metadata["macro_guard_allowed_for_battle"] = result.get("macro_guard_allowed_for_battle")
    metadata["macro_guard_block_battle"] = result.get("macro_guard_block_battle")
    metadata["macro_guard_research_only"] = result.get("macro_guard_research_only")
    metadata["macro_guard_suppress"] = result.get("macro_guard_suppress")
    metadata["macro_guard_reason_code"] = result.get("macro_guard_reason_code")
    metadata["macro_guard_blockers"] = result.get("macro_guard_blockers") or []
    metadata["macro_guard_requirements"] = result.get("macro_guard_requirements") or []
    metadata["macro_guard_missing_requirements"] = result.get("macro_guard_missing_requirements") or []
    metadata["macro_guard_satisfied_requirements"] = result.get("macro_guard_satisfied_requirements") or []
    metadata["macro_guard_macro_risk_status"] = result.get("macro_guard_macro_risk_status")
    metadata["macro_guard_calendar_status"] = result.get("macro_guard_calendar_status")
    metadata["macro_guard_calendar_source"] = result.get("macro_guard_calendar_source")
    metadata["macro_guard_fallback_chain"] = result.get("macro_guard_fallback_chain") or []
    metadata["macro_guard_event_title"] = result.get("macro_guard_event_title")
    metadata["macro_guard_event_time_local"] = result.get("macro_guard_event_time_local")
    metadata["macro_guard_event_currency"] = result.get("macro_guard_event_currency")
    metadata["macro_guard_event_impact"] = result.get("macro_guard_event_impact")
    metadata["macro_guard_event_source"] = result.get("macro_guard_event_source")
    metadata["macro_guard_minutes_since_event"] = result.get("macro_guard_minutes_since_event")
    metadata["macro_guard_minutes_until_event"] = result.get("macro_guard_minutes_until_event")
    metadata["macro_guard_affected_symbols"] = result.get("macro_guard_affected_symbols") or []
    metadata["macro_guard_notes"] = result.get("macro_guard_notes") or []
    metadata["macro_guard_error"] = result.get("macro_guard_error")

    metadata["entry_price"] = result.get("entry_price")
    metadata["invalidation_price"] = result.get("invalidation_price")
    metadata["target_price"] = result.get("target_price")
    metadata["current_price"] = result.get("current_price")
    metadata["impulse_progress"] = result.get("impulse_progress")
    metadata["impulse_progress_pct"] = result.get("impulse_progress_pct")
    metadata["impulse_state"] = result.get("impulse_state")
    metadata["fresh_retest_exists"] = result.get("fresh_retest_exists")
    metadata["fresh_failed_acceptance_exists"] = result.get("fresh_failed_acceptance_exists")
    metadata["fresh_pullback_exists"] = result.get("fresh_pullback_exists")

    return metadata


def _attach_v2_shadow_fields_to_metadata(metadata: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    metadata["battle_gate_v2_decision"] = result.get("battle_gate_v2_decision")
    metadata["battle_gate_v2_risk_mode"] = result.get("battle_gate_v2_risk_mode")
    metadata["battle_gate_v2_battle_allowed"] = result.get("battle_gate_v2_battle_allowed")
    metadata["battle_gate_v2_should_suppress_telegram"] = result.get("battle_gate_v2_should_suppress_telegram")
    metadata["battle_gate_v2_score_delta"] = result.get("battle_gate_v2_score_delta")
    metadata["battle_gate_v2_reasons"] = result.get("battle_gate_v2_reasons") or []
    metadata["battle_gate_v2_blockers"] = result.get("battle_gate_v2_blockers") or []
    metadata["battle_gate_v2_modifiers"] = result.get("battle_gate_v2_modifiers") or []
    metadata["battle_gate_v2_error"] = result.get("battle_gate_v2_error")
    return metadata


def apply_battle_permission(raw_payload: dict[str, Any]) -> dict[str, Any]:
    """
    Returns a copy of payload enriched with final battle permission fields.
    Does not mutate the input payload.

    Battle Gate v2 is currently attached in shadow mode:
    - legacy battle_permission / telegram_delivery_mode remain authoritative;
    - v2 fields are added for telemetry/statistics comparison.
    """
    payload = dict(raw_payload)
    result = evaluate_battle_permission(payload).to_dict()

    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    metadata["battle_permission"] = result["battle_permission"]
    metadata["telegram_delivery_mode"] = result["telegram_delivery_mode"]
    metadata["battle_ready"] = result["battle_ready"]
    metadata["auction_context_score"] = result["auction_context_score"]
    metadata["battle_permission_reasons"] = result["reasons"]
    metadata["battle_permission_blockers"] = result["blockers"]
    metadata["battle_permission_modifiers"] = result["modifiers"]
    metadata["battle_permission_version"] = BATTLE_PERMISSION_VERSION

    metadata = _attach_tpo_open_behavior_fields_to_metadata(metadata, result)
    metadata = _attach_v2_shadow_fields_to_metadata(metadata, result)

    payload["metadata"] = metadata
    payload["battle_permission"] = result["battle_permission"]
    payload["telegram_delivery_mode"] = result["telegram_delivery_mode"]
    payload["battle_ready"] = result["battle_ready"]
    payload["auction_context_score"] = result["auction_context_score"]
    payload["battle_permission_version"] = BATTLE_PERMISSION_VERSION

    # Root-level TPO/open-behavior fields are useful for journal, telemetry and flat statistics.
    payload["market_is_open"] = result.get("market_is_open")
    payload["market_status"] = result.get("market_status")
    payload["tpo_signal_permission"] = result.get("tpo_signal_permission")
    payload["tpo_telegram_modifier"] = result.get("tpo_telegram_modifier")
    payload["open_relation"] = result.get("open_relation")
    payload["auction_bias"] = result.get("auction_bias")
    payload["open_context"] = result.get("open_context")
    payload["open_behavior"] = result.get("open_behavior")
    payload["open_behavior_confidence"] = result.get("open_behavior_confidence")
    for key in (
        "open_location",
        "initial_open_behavior",
        "current_open_behavior",
        "behavior_transition",
        "value_acceptance_state",
        "value_test_occurred",
        "value_rejection_confirmed",
        "day_type_candidate",
        "auction_state_confidence",
        "auction_state_reason",
        "tpo_watch_state",
        "tpo_watch_setup",
        "tpo_watch_active",
        "auction_ltf_setup",
        "ltf_model_detector_version",
        "ltf_model_state",
        "ltf_model_state_full",
        "ltf_model_outcome",
        "ltf_model_type",
        "ltf_model_confirmed",
        "ltf_model_blockers",
        "ltf_model_warnings",
    ):
        payload[key] = result.get(key)
    payload["entry_model_hint"] = result.get("entry_model_hint")
    payload["stop_model_hint"] = result.get("stop_model_hint")
    payload["battle_bias_hint"] = result.get("battle_bias_hint")
    payload["primary_interest_zone"] = result.get("primary_interest_zone")
    payload["interest_zone_type"] = result.get("interest_zone_type")
    payload["interest_zone_price"] = result.get("interest_zone_price")
    payload["interest_zone_role"] = result.get("interest_zone_role")

    payload["symbol"] = result.get("symbol") or payload.get("symbol")
    payload["session_label"] = result.get("session_label")
    payload["news_risk_state"] = result.get("news_risk_state")
    payload["news_provider_status"] = result.get("news_provider_status")
    payload["local_structure_damaged"] = result.get("local_structure_damaged")
    payload["scenario_family"] = result.get("scenario_family")
    payload["target_quality"] = result.get("target_quality")
    payload["risk_mode"] = result.get("risk_mode")
    payload["caution_flags"] = result.get("caution_flags") or []
    payload["statistics_only"] = result.get("statistics_only")
    payload["suppression_reason"] = result.get("suppression_reason")
    payload["suppression_reasons"] = result.get("suppression_reasons") or []

    payload["post_news_detector_version"] = result.get("post_news_detector_version")
    payload["post_news_regime"] = result.get("post_news_regime")
    payload["post_news_trade_permission"] = result.get("post_news_trade_permission")
    payload["post_news_elapsed_minutes"] = result.get("post_news_elapsed_minutes")
    payload["post_news_impulse_direction"] = result.get("post_news_impulse_direction")
    payload["post_news_impulse_confirmed"] = result.get("post_news_impulse_confirmed")
    payload["post_news_retest_level"] = result.get("post_news_retest_level")
    payload["post_news_retest_status"] = result.get("post_news_retest_status")
    payload["post_news_acceptance_status"] = result.get("post_news_acceptance_status")
    payload["post_news_failed_move"] = result.get("post_news_failed_move")
    payload["post_news_continuation_quality"] = result.get("post_news_continuation_quality")
    payload["post_news_continuation_direction"] = result.get("post_news_continuation_direction")
    payload["post_news_reasons"] = result.get("post_news_reasons") or []
    payload["post_news_blockers"] = result.get("post_news_blockers") or []
    payload["post_news_modifiers"] = result.get("post_news_modifiers") or []

    payload["signal_created_at_utc"] = result.get("signal_created_at_utc")
    payload["signal_age_minutes"] = result.get("signal_age_minutes")
    payload["signal_max_age_minutes"] = result.get("signal_max_age_minutes")
    payload["signal_freshness_status"] = result.get("signal_freshness_status")

    payload["macro_detector_version"] = result.get("macro_detector_version")
    payload["macro_regime"] = result.get("macro_regime")
    payload["macro_shock_recent"] = result.get("macro_shock_recent")
    payload["macro_shock_score"] = result.get("macro_shock_score")
    payload["macro_risk_mode"] = result.get("macro_risk_mode")
    payload["macro_direction_for_symbol"] = result.get("macro_direction_for_symbol")
    payload["macro_caution_flags"] = result.get("macro_caution_flags") or []
    payload["macro_reasons"] = result.get("macro_reasons") or []

    payload["macro_guard_version"] = result.get("macro_guard_version")
    payload["macro_guard_status"] = result.get("macro_guard_status")
    payload["macro_guard_allowed_for_battle"] = result.get("macro_guard_allowed_for_battle")
    payload["macro_guard_block_battle"] = result.get("macro_guard_block_battle")
    payload["macro_guard_research_only"] = result.get("macro_guard_research_only")
    payload["macro_guard_suppress"] = result.get("macro_guard_suppress")
    payload["macro_guard_reason_code"] = result.get("macro_guard_reason_code")
    payload["macro_guard_blockers"] = result.get("macro_guard_blockers") or []
    payload["macro_guard_requirements"] = result.get("macro_guard_requirements") or []
    payload["macro_guard_missing_requirements"] = result.get("macro_guard_missing_requirements") or []
    payload["macro_guard_satisfied_requirements"] = result.get("macro_guard_satisfied_requirements") or []
    payload["macro_guard_macro_risk_status"] = result.get("macro_guard_macro_risk_status")
    payload["macro_guard_calendar_status"] = result.get("macro_guard_calendar_status")
    payload["macro_guard_calendar_source"] = result.get("macro_guard_calendar_source")
    payload["macro_guard_fallback_chain"] = result.get("macro_guard_fallback_chain") or []
    payload["macro_guard_event_title"] = result.get("macro_guard_event_title")
    payload["macro_guard_event_time_local"] = result.get("macro_guard_event_time_local")
    payload["macro_guard_event_currency"] = result.get("macro_guard_event_currency")
    payload["macro_guard_event_impact"] = result.get("macro_guard_event_impact")
    payload["macro_guard_event_source"] = result.get("macro_guard_event_source")
    payload["macro_guard_minutes_since_event"] = result.get("macro_guard_minutes_since_event")
    payload["macro_guard_minutes_until_event"] = result.get("macro_guard_minutes_until_event")
    payload["macro_guard_affected_symbols"] = result.get("macro_guard_affected_symbols") or []
    payload["macro_guard_notes"] = result.get("macro_guard_notes") or []
    payload["macro_guard_error"] = result.get("macro_guard_error")

    payload["entry_price"] = result.get("entry_price")
    payload["invalidation_price"] = result.get("invalidation_price")
    payload["target_price"] = result.get("target_price")
    payload["current_price"] = result.get("current_price")
    payload["impulse_progress"] = result.get("impulse_progress")
    payload["impulse_progress_pct"] = result.get("impulse_progress_pct")
    payload["impulse_state"] = result.get("impulse_state")
    payload["fresh_retest_exists"] = result.get("fresh_retest_exists")
    payload["fresh_failed_acceptance_exists"] = result.get("fresh_failed_acceptance_exists")
    payload["fresh_pullback_exists"] = result.get("fresh_pullback_exists")

    # Root-level v2 fields are useful for journal, telemetry and flat statistics.
    payload["battle_gate_v2_decision"] = result.get("battle_gate_v2_decision")
    payload["battle_gate_v2_risk_mode"] = result.get("battle_gate_v2_risk_mode")
    payload["battle_gate_v2_battle_allowed"] = result.get("battle_gate_v2_battle_allowed")
    payload["battle_gate_v2_should_suppress_telegram"] = result.get("battle_gate_v2_should_suppress_telegram")
    payload["battle_gate_v2_score_delta"] = result.get("battle_gate_v2_score_delta")
    payload["battle_gate_v2_reasons"] = result.get("battle_gate_v2_reasons") or []
    payload["battle_gate_v2_blockers"] = result.get("battle_gate_v2_blockers") or []
    payload["battle_gate_v2_modifiers"] = result.get("battle_gate_v2_modifiers") or []
    payload["battle_gate_v2_error"] = result.get("battle_gate_v2_error")

    return payload