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


BATTLE_PERMISSION_VERSION = "battle-permission-v1.7-policy-hints-ttl-macro-shock"


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

    if open_behavior == "OPEN_AUCTION":
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

    if "OPEN_TEST_DRIVE" in text:
        return "TPO_OPEN_TEST_DRIVE"

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

    # 2. Post-news state gate.
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