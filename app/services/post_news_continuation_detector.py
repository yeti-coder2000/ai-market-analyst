from __future__ import annotations

"""
Post-news continuation detector for AI Market Analyst.

Purpose
-------
After high-impact news the analyst should not chase the first impulse. This
module annotates the payload with a conservative post-news regime so Battle Gate
can distinguish:

    news impulse -> NO CHASE -> retest/acceptance -> clean continuation

The detector is intentionally dependency-free and payload-shape tolerant. It can
consume fields from root, metadata, auction_context, auction_filters,
context.auction, and tpo_context.
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable


POST_NEWS_CONTINUATION_DETECTOR_VERSION = "post-news-continuation-detector-v1.0-conservative-acceptance"


# =============================================================================
# DEFAULTS / ENUM-LIKE CONSTANTS
# =============================================================================

DEFAULT_NO_CHASE_MINUTES = 90
DEFAULT_RETEST_WAIT_MAX_MINUTES = 360
DEFAULT_POST_NEWS_STALE_MINUTES = 720
DEFAULT_MIN_PRACTICAL_RR = 2.0

REGIME_NOT_POST_NEWS = "NOT_POST_NEWS"
REGIME_NO_CHASE = "POST_NEWS_NO_CHASE"
REGIME_RETEST_WAIT = "POST_NEWS_RETEST_WAIT"
REGIME_ACCEPTANCE_CONFIRMED = "POST_NEWS_ACCEPTANCE_CONFIRMED"
REGIME_CLEAN_CONTINUATION = "POST_NEWS_CLEAN_CONTINUATION"
REGIME_CAUTION_CONTINUATION = "POST_NEWS_CAUTION_CONTINUATION"
REGIME_FAILED_MOVE = "POST_NEWS_FAILED_MOVE"
REGIME_ROTATION_ONLY = "POST_NEWS_ROTATION_ONLY"
REGIME_STALE = "POST_NEWS_STALE"

PERMISSION_NO_CHANGE = "NO_CHANGE"
PERMISSION_BLOCK_BATTLE = "BLOCK_BATTLE"
PERMISSION_RESEARCH_ONLY = "RESEARCH_ONLY"
PERMISSION_SUPPRESS = "SUPPRESS"
PERMISSION_ALLOW_BATTLE = "ALLOW_BATTLE_IF_GEOMETRY_VALID"
PERMISSION_ALLOW_CAUTION_BATTLE = "ALLOW_CAUTION_BATTLE_IF_GEOMETRY_VALID"


DIRECTION_LONG_ALIASES = {"LONG", "BUY", "BULL", "BULLISH", "UP", "UPSIDE"}
DIRECTION_SHORT_ALIASES = {"SHORT", "SELL", "BEAR", "BEARISH", "DOWN", "DOWNSIDE"}

NEWS_ACTIVE_STATES = {
    "HIGH",
    "HIGH_IMPACT",
    "RED",
    "NEWS_HIGH",
    "POST_NEWS",
    "POST_NEWS_MODE",
    "POST_NEWS_ACTIVE",
    "USD_HIGH",
    "CPI_HIGH",
    "NFP_HIGH",
    "FOMC_HIGH",
}

POST_NEWS_SCENARIO_FAMILIES = {
    "POST_NEWS_RECLAIM",
    "POST_LIQUIDATION_RECLAIM",
    "NEWS_REACTION_RECLAIM",
    "POST_NEWS_CONTINUATION",
}

DRIVE_BEHAVIORS = {
    "OPEN_DRIVE",
    "OPEN_TEST_DRIVE",
    "DRIVE",
    "TEST_DRIVE",
    "DIRECTIONAL_DRIVE",
}

AUCTION_ROTATION_BEHAVIORS = {
    "OPEN_AUCTION",
    "AUCTION",
    "ROTATION",
    "BALANCE",
    "UNCONFIRMED",
}

VALID_ENTRY_MODEL_HINTS = {
    "FAILED_ACCEPTANCE_RETEST",
    "PULLBACK_OR_FAILED_ACCEPTANCE_RETEST",
    "PULLBACK_RETEST",
    "ACCEPTANCE_RETEST",
    "STRUCTURE_RETEST",
    "BASE_PULLBACK_RETEST",
    "LTF_MODEL_CONFIRMED",
    "LIMIT_ON_RETEST",
}

VALID_RETEST_STATUSES = {
    "CONFIRMED",
    "HELD",
    "REJECTED",
    "REJECTED_LEVEL",
    "RETEST_REJECTED",
    "RETEST_HELD",
    "PULLBACK_HELD",
    "BASE_HELD",
}

WAIT_RETEST_STATUSES = {
    "WAIT",
    "WAITING",
    "PENDING",
    "NOT_TESTED",
    "NO_RETEST",
    "MISSING",
    "UNKNOWN",
}

FAILED_RETEST_STATUSES = {
    "FAILED",
    "BROKEN",
    "ACCEPTED_BACK",
    "RECLAIMED_AGAINST_DIRECTION",
    "INVALIDATED",
}

ACCEPTANCE_CONFIRMED_STATUSES = {
    "CONFIRMED",
    "ACCEPTED",
    "ACCEPTANCE_CONFIRMED",
    "ACCEPTED_OUTSIDE_VALUE",
    "ACCEPTED_BELOW_VALUE",
    "ACCEPTED_ABOVE_VALUE",
    "ACCEPTED_BELOW_RANGE",
    "ACCEPTED_ABOVE_RANGE",
    "HOLDS_BELOW_VALUE",
    "HOLDS_ABOVE_VALUE",
}

ACCEPTANCE_FAILED_STATUSES = {
    "FAILED",
    "FAILED_ACCEPTANCE",
    "ACCEPTED_BACK_INSIDE_VALUE",
    "ACCEPTED_BACK_INSIDE_RANGE",
    "FAILED_OUTSIDE_VALUE",
    "REJECTED_OUTSIDE_VALUE",
    "BACK_INSIDE_VALUE",
    "BACK_INSIDE_RANGE",
}

REAL_TARGET_QUALITIES = {
    "REAL_ZONE",
    "REAL_INTEREST_ZONE",
    "INTEREST_ZONE",
    "NPOC",
    "POC",
    "VAH",
    "VAL",
    "PREVIOUS_HIGH",
    "PREVIOUS_LOW",
    "SESSION_EXTREME",
}

SYNTHETIC_TARGET_QUALITIES = {
    "SYNTHETIC",
    "SYNTHETIC_TARGET",
    "UNKNOWN_SYNTHETIC",
}


# =============================================================================
# DATA MODEL
# =============================================================================

@dataclass(slots=True)
class PostNewsContinuationResult:
    regime: str = REGIME_NOT_POST_NEWS
    trade_permission: str = PERMISSION_NO_CHANGE
    elapsed_minutes: int | None = None
    impulse_direction: str | None = None
    impulse_confirmed: bool | None = None
    retest_level: str | None = None
    retest_status: str | None = None
    acceptance_status: str | None = None
    failed_move: bool | None = None
    continuation_quality: str | None = None
    continuation_direction: str | None = None
    reasons: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    modifiers: list[str] = field(default_factory=list)

    def to_payload_fields(self) -> dict[str, Any]:
        return {
            "post_news_detector_version": POST_NEWS_CONTINUATION_DETECTOR_VERSION,
            "post_news_regime": self.regime,
            "post_news_trade_permission": self.trade_permission,
            "post_news_elapsed_minutes": self.elapsed_minutes,
            "post_news_impulse_direction": self.impulse_direction,
            "post_news_impulse_confirmed": self.impulse_confirmed,
            "post_news_retest_level": self.retest_level,
            "post_news_retest_status": self.retest_status,
            "post_news_acceptance_status": self.acceptance_status,
            "post_news_failed_move": self.failed_move,
            "post_news_continuation_quality": self.continuation_quality,
            "post_news_continuation_direction": self.continuation_direction,
            "post_news_reasons": self.reasons,
            "post_news_blockers": self.blockers,
            "post_news_modifiers": self.modifiers,
        }


# =============================================================================
# SAFE HELPERS
# =============================================================================

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _is_empty(value: Any) -> bool:
    return value in (None, "", [], {})


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if not _is_empty(value):
            return value
    return None


def _safe_float(value: Any) -> float | None:
    if value in (None, "", [], {}):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value in (None, "", [], {}):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value in (None, "", [], {}):
        return None
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "damaged", "broken"}:
        return True
    if text in {"0", "false", "no", "n", "off", "clean", "ok"}:
        return False
    return None


def _as_upper(value: Any) -> str | None:
    if value in (None, "", [], {}):
        return None
    text = str(value).strip().upper()
    return text or None


def _as_text_list(value: Any) -> list[str]:
    if value in (None, "", [], {}):
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, dict):
        try:
            return [json.dumps(value, ensure_ascii=False, sort_keys=True)]
        except Exception:
            return [str(value)]
    text = str(value).strip()
    return [text] if text else []


def _deep_get(data: dict[str, Any], path: str) -> Any:
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _metadata(payload: dict[str, Any]) -> dict[str, Any]:
    return _safe_dict(payload.get("metadata"))


def _context_sources(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return fallback sources from most direct to most nested."""
    meta = _metadata(payload)
    context = _safe_dict(payload.get("context"))
    context_auction = _safe_dict(_deep_get(context, "auction"))
    tpo_context = _safe_dict(payload.get("tpo_context") or meta.get("tpo_context"))

    sources = [
        payload,
        meta,
        _safe_dict(payload.get("auction_context")),
        _safe_dict(payload.get("auction_filters")),
        _safe_dict(meta.get("auction_context")),
        _safe_dict(meta.get("auction_filters")),
        context_auction,
        _safe_dict(_deep_get(context, "auction.context")),
        _safe_dict(_deep_get(context, "auction.filters")),
        tpo_context,
        _safe_dict(tpo_context.get("auction_context")),
        _safe_dict(tpo_context.get("auction_filters")),
    ]
    return [s for s in sources if isinstance(s, dict) and s]


def _lookup(payload: dict[str, Any], *keys: str) -> Any:
    for source in _context_sources(payload):
        for key in keys:
            value = _deep_get(source, key) if "." in key else source.get(key)
            if not _is_empty(value):
                return value
    return None


def _lookup_upper(payload: dict[str, Any], *keys: str) -> str | None:
    return _as_upper(_lookup(payload, *keys))


def _normalize_direction(value: Any) -> str | None:
    text = _as_upper(value)
    if not text:
        return None
    if text in DIRECTION_LONG_ALIASES:
        return "LONG"
    if text in DIRECTION_SHORT_ALIASES:
        return "SHORT"
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, "", [], {}):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _minutes_between(start: datetime, end: datetime) -> int:
    return max(0, int((end - start).total_seconds() // 60))


# =============================================================================
# FEATURE EXTRACTION
# =============================================================================

def _is_post_news_context(payload: dict[str, Any]) -> bool:
    if _safe_bool(_lookup(payload, "post_news_mode", "is_post_news", "high_impact_news_active")) is True:
        return True

    news_risk_state = _lookup_upper(payload, "news_risk_state", "risk_day", "news_state", "calendar_risk")
    if news_risk_state in NEWS_ACTIVE_STATES:
        return True

    scenario_family = _lookup_upper(payload, "scenario_family")
    if scenario_family in POST_NEWS_SCENARIO_FAMILIES:
        return True

    if _lookup(payload, "post_news_elapsed_minutes") is not None:
        return True

    if _lookup(payload, "high_impact_news_ts_utc", "news_event_ts_utc", "last_high_impact_news_ts_utc") is not None:
        return True

    return False


def _infer_elapsed_minutes(payload: dict[str, Any], *, now: datetime) -> int | None:
    explicit = _safe_int(
        _lookup(
            payload,
            "post_news_elapsed_minutes",
            "minutes_since_high_impact_news",
            "minutes_since_news",
            "news_elapsed_minutes",
        )
    )
    if explicit is not None:
        return explicit

    event_dt = _parse_datetime(
        _lookup(
            payload,
            "high_impact_news_ts_utc",
            "last_high_impact_news_ts_utc",
            "news_event_ts_utc",
            "news_ts_utc",
        )
    )
    if event_dt is None:
        return None

    return _minutes_between(event_dt, now)


def _infer_impulse_direction(payload: dict[str, Any]) -> tuple[str | None, bool | None]:
    explicit = _normalize_direction(
        _lookup(
            payload,
            "post_news_impulse_direction",
            "news_impulse_direction",
            "impulse_direction",
            "initial_impulse_direction",
        )
    )
    if explicit:
        confirmed = _safe_bool(_lookup(payload, "post_news_impulse_confirmed", "impulse_confirmed"))
        return explicit, True if confirmed is None else confirmed

    # Fallback: when the post-news signal is a TPO/open continuation candidate,
    # the signal direction is a useful proxy, but not enough for CLEAN status.
    direction = _normalize_direction(_lookup(payload, "direction"))
    if direction:
        scenario = _lookup_upper(payload, "scenario", "scenario_type") or ""
        trigger = _lookup_upper(payload, "trigger_reason", "execution.trigger_reason") or ""
        if "TPO_OPEN" in scenario or "OPEN_TEST_DRIVE" in scenario or "LTF_MODEL_CONFIRMED" in trigger:
            return direction, None

    return None, None


def _infer_retest_status(payload: dict[str, Any]) -> str | None:
    status = _lookup_upper(
        payload,
        "post_news_retest_status",
        "retest_status",
        "acceptance_retest_status",
        "ltf_retest_status",
        "pullback_status",
    )
    if status:
        return status

    entry_model = _lookup_upper(payload, "entry_model_hint", "execution_model", "execution.model")
    if entry_model in VALID_ENTRY_MODEL_HINTS:
        return "CONFIRMED"

    return None


def _infer_acceptance_status(payload: dict[str, Any], *, direction: str | None) -> str | None:
    status = _lookup_upper(
        payload,
        "post_news_acceptance_status",
        "acceptance_status",
        "value_acceptance_status",
        "range_acceptance_status",
        "tpo_acceptance_status",
    )
    if status:
        return status

    open_relation = _lookup_upper(payload, "open_relation", "tpo_open_relation")
    open_context = _lookup_upper(payload, "open_context")
    auction_bias = _lookup_upper(payload, "auction_bias", "tpo_auction_bias")
    tpo_modifier = _lookup_upper(payload, "tpo_telegram_modifier", "telegram_modifier")

    if tpo_modifier == "DOWNGRADE" and open_relation in {"INSIDE_VA", "OPEN_INSIDE_VA"}:
        return "BACK_INSIDE_VALUE"

    if auction_bias in {"DIRECTIONAL_IMBALANCE", "RANGE_EXTENSION"} and open_context in {"OPEN_IN_RANGE", "OPEN_OUT_OF_RANGE", "OUT_OF_RANGE"}:
        if direction == "SHORT":
            return "ACCEPTED_BELOW_VALUE"
        if direction == "LONG":
            return "ACCEPTED_ABOVE_VALUE"
        return "ACCEPTANCE_CONFIRMED"

    return None


def _infer_failed_move(payload: dict[str, Any], *, retest_status: str | None, acceptance_status: str | None) -> bool | None:
    explicit = _safe_bool(_lookup(payload, "post_news_failed_move", "failed_move", "news_failed_move"))
    if explicit is not None:
        return explicit

    if retest_status in FAILED_RETEST_STATUSES:
        return True
    if acceptance_status in ACCEPTANCE_FAILED_STATUSES:
        return True

    return None


def _infer_retest_level(payload: dict[str, Any]) -> str | None:
    value = _lookup(
        payload,
        "post_news_retest_level",
        "retest_level",
        "primary_interest_zone",
        "interest_zone_type",
        "target_zone_role",
    )
    if value in (None, "", [], {}):
        return None
    if isinstance(value, dict):
        return value.get("type") or value.get("role") or value.get("name") or json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _target_quality(payload: dict[str, Any]) -> str | None:
    explicit = _lookup_upper(payload, "target_quality")
    if explicit:
        return explicit

    zone_type = _lookup_upper(payload, "interest_zone_type", "primary_interest_zone", "target_zone_role")
    if zone_type in REAL_TARGET_QUALITIES:
        return "REAL_ZONE"
    return None


def _is_real_target(payload: dict[str, Any]) -> bool | None:
    target_quality = _target_quality(payload)
    if target_quality in REAL_TARGET_QUALITIES:
        return True
    if target_quality in SYNTHETIC_TARGET_QUALITIES:
        return False
    return None


def _open_behavior(payload: dict[str, Any]) -> str | None:
    return _lookup_upper(payload, "open_behavior", "tpo_open_behavior")


def _entry_model_hint(payload: dict[str, Any]) -> str | None:
    return _lookup_upper(payload, "entry_model_hint", "execution_model", "execution.model")


def _has_valid_ltf_model(payload: dict[str, Any]) -> bool:
    hint = _entry_model_hint(payload)
    if hint in VALID_ENTRY_MODEL_HINTS:
        return True
    trigger = _lookup_upper(payload, "trigger_reason", "execution.trigger_reason") or ""
    return "LTF_MODEL_CONFIRMED" in trigger or "RETEST" in trigger


def _practical_rr(payload: dict[str, Any]) -> float | None:
    return _safe_float(_lookup(payload, "practical_rr", "execution.practical_rr", "risk_reward_ratio", "rr"))


def _local_structure_damaged(payload: dict[str, Any]) -> bool | None:
    return _safe_bool(_lookup(payload, "local_structure_damaged", "structure_damaged", "ltf_structure_damaged"))


# =============================================================================
# DETECTOR
# =============================================================================

def detect_post_news_continuation(
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
) -> PostNewsContinuationResult:
    """
    Return a conservative post-news continuation classification.

    The detector does not open trades by itself. It annotates the payload for
    Battle Gate. Battle Gate must still validate HTF alignment, stop quality,
    practical RR, target quality, and market status.
    """
    now = now or datetime.now(timezone.utc)

    no_chase_minutes = _env_int("POST_NEWS_NO_CHASE_MINUTES", DEFAULT_NO_CHASE_MINUTES)
    retest_wait_max_minutes = _env_int("POST_NEWS_RETEST_WAIT_MAX_MINUTES", DEFAULT_RETEST_WAIT_MAX_MINUTES)
    stale_minutes = _env_int("POST_NEWS_STALE_MINUTES", DEFAULT_POST_NEWS_STALE_MINUTES)
    min_practical_rr = _env_float("POST_NEWS_MIN_PRACTICAL_RR", DEFAULT_MIN_PRACTICAL_RR)

    if not _is_post_news_context(payload):
        return PostNewsContinuationResult(
            regime=REGIME_NOT_POST_NEWS,
            trade_permission=PERMISSION_NO_CHANGE,
            reasons=["not a post-news context"],
        )

    direction = _normalize_direction(_lookup(payload, "direction"))
    elapsed = _infer_elapsed_minutes(payload, now=now)
    impulse_direction, impulse_confirmed = _infer_impulse_direction(payload)
    retest_status = _infer_retest_status(payload)
    acceptance_status = _infer_acceptance_status(payload, direction=direction)
    failed_move = _infer_failed_move(payload, retest_status=retest_status, acceptance_status=acceptance_status)
    retest_level = _infer_retest_level(payload)
    open_behavior = _open_behavior(payload)
    target_is_real = _is_real_target(payload)
    local_damage = _local_structure_damaged(payload)
    rr = _practical_rr(payload)
    valid_ltf_model = _has_valid_ltf_model(payload)

    reasons: list[str] = []
    blockers: list[str] = []
    modifiers: list[str] = []

    if elapsed is not None:
        reasons.append(f"post-news elapsed={elapsed}m")
    else:
        modifiers.append("post_news_elapsed_unknown")

    if impulse_direction:
        reasons.append(f"impulse_direction={impulse_direction}")
    else:
        blockers.append("missing_impulse_direction")

    if retest_status:
        reasons.append(f"retest_status={retest_status}")
    if acceptance_status:
        reasons.append(f"acceptance_status={acceptance_status}")
    if open_behavior:
        reasons.append(f"open_behavior={open_behavior}")

    # Phase 1: immediate no-chase after the event.
    if elapsed is not None and elapsed <= no_chase_minutes:
        return PostNewsContinuationResult(
            regime=REGIME_NO_CHASE,
            trade_permission=PERMISSION_BLOCK_BATTLE,
            elapsed_minutes=elapsed,
            impulse_direction=impulse_direction,
            impulse_confirmed=impulse_confirmed,
            retest_level=retest_level,
            retest_status=retest_status,
            acceptance_status=acceptance_status,
            failed_move=failed_move,
            continuation_quality="NO_CHASE",
            continuation_direction=direction,
            reasons=reasons + [f"inside no-chase window <= {no_chase_minutes}m"],
            blockers=blockers + ["post_news_no_chase_window"],
            modifiers=modifiers,
        )

    if elapsed is not None and elapsed >= stale_minutes:
        return PostNewsContinuationResult(
            regime=REGIME_STALE,
            trade_permission=PERMISSION_NO_CHANGE,
            elapsed_minutes=elapsed,
            impulse_direction=impulse_direction,
            impulse_confirmed=impulse_confirmed,
            retest_level=retest_level,
            retest_status=retest_status,
            acceptance_status=acceptance_status,
            failed_move=failed_move,
            continuation_quality="STALE",
            continuation_direction=direction,
            reasons=reasons + [f"post-news context stale >= {stale_minutes}m"],
            blockers=blockers,
            modifiers=modifiers + ["post_news_stale_no_special_gate"],
        )

    # Failed move has priority over continuation.
    if failed_move is True:
        return PostNewsContinuationResult(
            regime=REGIME_FAILED_MOVE,
            trade_permission=PERMISSION_RESEARCH_ONLY,
            elapsed_minutes=elapsed,
            impulse_direction=impulse_direction,
            impulse_confirmed=impulse_confirmed,
            retest_level=retest_level,
            retest_status=retest_status,
            acceptance_status=acceptance_status,
            failed_move=True,
            continuation_quality="FAILED",
            continuation_direction=direction,
            reasons=reasons + ["post-news move failed / accepted back inside"],
            blockers=blockers + ["post_news_failed_move"],
            modifiers=modifiers,
        )

    # Pure auction/rotation should not become battle without acceptance.
    if open_behavior in AUCTION_ROTATION_BEHAVIORS and acceptance_status not in ACCEPTANCE_CONFIRMED_STATUSES:
        return PostNewsContinuationResult(
            regime=REGIME_ROTATION_ONLY,
            trade_permission=PERMISSION_RESEARCH_ONLY,
            elapsed_minutes=elapsed,
            impulse_direction=impulse_direction,
            impulse_confirmed=impulse_confirmed,
            retest_level=retest_level,
            retest_status=retest_status,
            acceptance_status=acceptance_status,
            failed_move=failed_move,
            continuation_quality="ROTATION_ONLY",
            continuation_direction=direction,
            reasons=reasons + ["open behavior remains auction/rotation without confirmed acceptance"],
            blockers=blockers + ["post_news_rotation_only"],
            modifiers=modifiers,
        )

    # Waiting state: post-news window is open, but retest/acceptance is not complete.
    if (
        retest_status in WAIT_RETEST_STATUSES
        or acceptance_status in (None, "UNKNOWN")
        or not valid_ltf_model
    ):
        wait_reason = "waiting for retest/acceptance/LTF model"
        if elapsed is not None and elapsed > retest_wait_max_minutes:
            wait_reason = f"no clean continuation inside {retest_wait_max_minutes}m retest window"

        return PostNewsContinuationResult(
            regime=REGIME_RETEST_WAIT,
            trade_permission=PERMISSION_RESEARCH_ONLY,
            elapsed_minutes=elapsed,
            impulse_direction=impulse_direction,
            impulse_confirmed=impulse_confirmed,
            retest_level=retest_level,
            retest_status=retest_status,
            acceptance_status=acceptance_status,
            failed_move=failed_move,
            continuation_quality="WAIT",
            continuation_direction=direction,
            reasons=reasons + [wait_reason],
            blockers=blockers + ["post_news_retest_acceptance_not_confirmed"],
            modifiers=modifiers,
        )

    direction_matches_impulse = bool(direction and impulse_direction and direction == impulse_direction)
    retest_ok = retest_status in VALID_RETEST_STATUSES
    acceptance_ok = acceptance_status in ACCEPTANCE_CONFIRMED_STATUSES
    drive_ok = open_behavior in DRIVE_BEHAVIORS or open_behavior is None
    rr_ok = rr is None or rr >= min_practical_rr

    clean_blockers: list[str] = []
    if not direction_matches_impulse:
        clean_blockers.append("direction_not_matching_post_news_impulse")
    if not retest_ok:
        clean_blockers.append("retest_not_confirmed")
    if not acceptance_ok:
        clean_blockers.append("acceptance_not_confirmed")
    if not drive_ok:
        clean_blockers.append("open_behavior_not_directional")
    if rr_ok is False:
        clean_blockers.append("practical_rr_below_post_news_min")

    if clean_blockers:
        return PostNewsContinuationResult(
            regime=REGIME_ACCEPTANCE_CONFIRMED if acceptance_ok else REGIME_RETEST_WAIT,
            trade_permission=PERMISSION_RESEARCH_ONLY,
            elapsed_minutes=elapsed,
            impulse_direction=impulse_direction,
            impulse_confirmed=impulse_confirmed,
            retest_level=retest_level,
            retest_status=retest_status,
            acceptance_status=acceptance_status,
            failed_move=failed_move,
            continuation_quality="PARTIAL",
            continuation_direction=direction,
            reasons=reasons + ["post-news continuation only partially confirmed"],
            blockers=blockers + clean_blockers,
            modifiers=modifiers,
        )

    caution_reasons: list[str] = []
    if target_is_real is None:
        caution_reasons.append("target_quality_unknown")
    elif target_is_real is False:
        caution_reasons.append("target_not_real_zone")

    if local_damage is True:
        caution_reasons.append("local_structure_damaged")

    if impulse_confirmed is False or impulse_confirmed is None:
        caution_reasons.append("impulse_confirmation_weak_or_inferred")

    if caution_reasons:
        return PostNewsContinuationResult(
            regime=REGIME_CAUTION_CONTINUATION,
            trade_permission=PERMISSION_ALLOW_CAUTION_BATTLE,
            elapsed_minutes=elapsed,
            impulse_direction=impulse_direction,
            impulse_confirmed=impulse_confirmed,
            retest_level=retest_level,
            retest_status=retest_status,
            acceptance_status=acceptance_status,
            failed_move=False,
            continuation_quality="CAUTION",
            continuation_direction=direction,
            reasons=reasons + ["post-news continuation confirmed with caution"],
            blockers=blockers,
            modifiers=modifiers + caution_reasons,
        )

    return PostNewsContinuationResult(
        regime=REGIME_CLEAN_CONTINUATION,
        trade_permission=PERMISSION_ALLOW_BATTLE,
        elapsed_minutes=elapsed,
        impulse_direction=impulse_direction,
        impulse_confirmed=impulse_confirmed,
        retest_level=retest_level,
        retest_status=retest_status,
        acceptance_status=acceptance_status,
        failed_move=False,
        continuation_quality="CLEAN",
        continuation_direction=direction,
        reasons=reasons + ["post-news auction resolved into clean continuation"],
        blockers=blockers,
        modifiers=modifiers,
    )


def apply_post_news_continuation(
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a shallow copy of payload enriched with post-news fields."""
    enriched = dict(payload)
    result = detect_post_news_continuation(enriched, now=now)
    fields = result.to_payload_fields()
    enriched.update(fields)

    metadata = _safe_dict(enriched.get("metadata"))
    metadata.update(fields)
    enriched["metadata"] = metadata

    return enriched


# =============================================================================
# CLI SMOKE TEST
# =============================================================================

if __name__ == "__main__":
    sample = {
        "symbol": "XAUUSD",
        "direction": "SHORT",
        "scenario_type": "TPO_OPEN_TEST_DRIVE_SHORT",
        "scenario_family": "POST_NEWS_CONTINUATION",
        "news_risk_state": "POST_NEWS",
        "post_news_elapsed_minutes": 180,
        "post_news_impulse_direction": "SHORT",
        "post_news_impulse_confirmed": True,
        "post_news_retest_status": "REJECTED",
        "post_news_acceptance_status": "ACCEPTED_BELOW_VALUE",
        "open_behavior": "OPEN_TEST_DRIVE",
        "entry_model_hint": "FAILED_ACCEPTANCE_RETEST",
        "practical_rr": 2.4,
        "target_quality": "REAL_ZONE",
        "local_structure_damaged": False,
    }
    print(json.dumps(apply_post_news_continuation(sample), ensure_ascii=False, indent=2, default=str))