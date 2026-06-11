from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)


TELEMETRY_SCHEMA_VERSION = "battle-permission-telemetry-v3-safety-fields"


# =============================================================================
# PATH HELPERS
# =============================================================================

def _runtime_dir() -> Path:
    raw = os.getenv("RUNTIME_DIR")
    if raw:
        return Path(raw)

    try:
        from app.core.settings import settings

        value = getattr(settings, "runtime_dir", None)
        if value:
            return Path(value)
    except Exception:
        pass

    render_runtime = Path("/var/data/runtime")
    if render_runtime.exists():
        return render_runtime

    return Path("runtime")


def _telemetry_path() -> Path:
    raw = os.getenv("BATTLE_PERMISSION_TELEMETRY_PATH")
    if raw:
        return Path(raw)

    return _runtime_dir() / "telemetry" / "battle_permission_events.ndjson"


# =============================================================================
# SAFE HELPERS
# =============================================================================

def _safe_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    meta = payload.get("metadata")
    return meta if isinstance(meta, dict) else {}


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value

    if isinstance(value, tuple):
        return list(value)

    if isinstance(value, set):
        return list(value)

    if value in (None, "", {}, ()):
        return []

    return [value]


def _safe_text_list(value: Any) -> list[str]:
    result: list[str] = []

    for item in _safe_list(value):
        if item in (None, "", [], {}):
            continue

        if isinstance(item, dict):
            try:
                result.append(json.dumps(item, ensure_ascii=False, sort_keys=True))
            except Exception:
                result.append(str(item))
            continue

        text = str(item).strip()
        if text:
            result.append(text)

    return _dedupe_keep_order(result)


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []

    for item in items:
        key = str(item).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(str(item).strip())

    return result


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _json_default(value: Any) -> str:
    return str(value)


def _safe_float(value: Any) -> float | None:
    if value in (None, "", [], {}):
        return None

    try:
        return float(value)
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

    if text in {"1", "true", "yes", "y", "on", "open", "active", "damaged"}:
        return True

    if text in {"0", "false", "no", "n", "off", "closed", "inactive", "clean", "ok"}:
        return False

    return None


def _nested_get(obj: dict[str, Any], *keys: str) -> Any:
    current: Any = obj

    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)

    return current


def _payload_execution(payload: dict[str, Any]) -> dict[str, Any]:
    execution = payload.get("execution")
    return execution if isinstance(execution, dict) else {}


def _payload_get(payload: dict[str, Any], *paths: str) -> Any:
    """
    Search payload root and metadata for one of the requested dotted paths.
    This is intentionally small; auction-specific lookup uses _context_get().
    """
    metadata = _safe_metadata(payload)

    for path in paths:
        if not path:
            continue

        root_value = _nested_get(payload, *path.split("."))
        if root_value not in (None, "", [], {}):
            return root_value

        meta_value = _nested_get(metadata, *path.split("."))
        if meta_value not in (None, "", [], {}):
            return meta_value

    return None


def _merge_non_empty_dicts(*values: Any) -> dict[str, Any]:
    """
    Merge dictionaries from left to right, but ignore empty override values.
    Later non-empty values override earlier ones.
    """
    result: dict[str, Any] = {}

    for value in values:
        if not isinstance(value, dict):
            continue

        for key, item in value.items():
            if item in (None, "", [], {}):
                continue
            result[key] = item

    return result


def _collect_context_sources(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Flatten the common payload shapes used by the runner, Telegram notifier and
    TPO exporter into one lookup dictionary.

    Supported locations:
    - payload root
    - payload.metadata
    - payload.auction_context / payload.auction_filters
    - payload.metadata.auction_context / payload.metadata.auction_filters
    - payload.context.auction
    - payload.context.auction.context / filters
    - payload.tpo_context
    - payload.tpo_context.auction_context / auction_filters
    """
    metadata = _safe_metadata(payload)

    context = _safe_dict(payload.get("context"))
    context_auction = _safe_dict(context.get("auction"))

    tpo_context = _safe_dict(
        _first_non_empty(
            payload.get("tpo_context"),
            metadata.get("tpo_context"),
        )
    )

    sources = [
        _safe_dict(payload.get("auction_context")),
        _safe_dict(payload.get("auction_filters")),
        _safe_dict(metadata.get("auction_context")),
        _safe_dict(metadata.get("auction_filters")),
        _safe_dict(context.get("auction_context")),
        _safe_dict(context.get("auction_filters")),
        context_auction,
        _safe_dict(context_auction.get("context")),
        _safe_dict(context_auction.get("filters")),
        tpo_context,
        _safe_dict(tpo_context.get("auction_context")),
        _safe_dict(tpo_context.get("auction_filters")),
        _safe_dict(tpo_context.get("context")),
        _safe_dict(tpo_context.get("filters")),
        # Keep root and metadata late so aliases added by telegram_notifier v1.7 win.
        metadata,
        payload,
    ]

    aliases = {
        "open_relation": _first_non_empty(
            payload.get("open_relation"),
            payload.get("tpo_open_relation"),
            metadata.get("open_relation"),
            metadata.get("tpo_open_relation"),
        ),
        "auction_bias": _first_non_empty(
            payload.get("auction_bias"),
            payload.get("tpo_auction_bias"),
            metadata.get("auction_bias"),
            metadata.get("tpo_auction_bias"),
        ),
        "telegram_modifier": _first_non_empty(
            payload.get("telegram_modifier"),
            payload.get("tpo_telegram_modifier"),
            metadata.get("telegram_modifier"),
            metadata.get("tpo_telegram_modifier"),
        ),
    }

    return _merge_non_empty_dicts(*sources, aliases)


def _context_get(context: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = context.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def _extract_entry(payload: dict[str, Any], execution: dict[str, Any]) -> float | None:
    return _safe_float(
        _first_non_empty(
            payload.get("entry_reference_price"),
            payload.get("entry"),
            payload.get("entry_price"),
            payload.get("limit_price"),
            execution.get("entry_reference_price"),
            execution.get("entry"),
            execution.get("entry_price"),
        )
    )


def _extract_stop(payload: dict[str, Any], execution: dict[str, Any]) -> float | None:
    return _safe_float(
        _first_non_empty(
            payload.get("invalidation_reference_price"),
            payload.get("stop_loss"),
            payload.get("stop"),
            payload.get("invalidation_level"),
            execution.get("invalidation_reference_price"),
            execution.get("stop_loss"),
            execution.get("stop"),
            execution.get("invalidation_level"),
        )
    )


def _extract_target(payload: dict[str, Any], execution: dict[str, Any]) -> float | None:
    return _safe_float(
        _first_non_empty(
            payload.get("target_reference_price"),
            payload.get("target"),
            payload.get("take_profit"),
            payload.get("tp"),
            execution.get("target_reference_price"),
            execution.get("target"),
            execution.get("take_profit"),
            execution.get("tp"),
        )
    )


def _extract_rr(payload: dict[str, Any], execution: dict[str, Any]) -> float | None:
    return _safe_float(
        _first_non_empty(
            payload.get("risk_reward_ratio"),
            payload.get("rr"),
            payload.get("risk_reward"),
            execution.get("risk_reward_ratio"),
            execution.get("rr"),
            execution.get("risk_reward"),
        )
    )


def _extract_theoretical_rr(payload: dict[str, Any], execution: dict[str, Any]) -> float | None:
    return _safe_float(
        _first_non_empty(
            payload.get("theoretical_rr"),
            execution.get("theoretical_rr"),
            _extract_rr(payload, execution),
        )
    )


def _extract_practical_rr(payload: dict[str, Any], execution: dict[str, Any]) -> float | None:
    return _safe_float(
        _first_non_empty(
            payload.get("practical_rr"),
            execution.get("practical_rr"),
            payload.get("risk_reward_ratio"),
            execution.get("risk_reward_ratio"),
        )
    )


def _compute_stop_distance(
    entry: float | None,
    stop: float | None,
    payload: dict[str, Any],
    execution: dict[str, Any],
) -> float | None:
    explicit = _safe_float(
        _first_non_empty(
            payload.get("stop_distance"),
            execution.get("stop_distance"),
        )
    )

    if explicit is not None:
        return explicit

    if entry is None or stop is None:
        return None

    return abs(entry - stop)


def _compute_target_distance(
    entry: float | None,
    target: float | None,
    payload: dict[str, Any],
    execution: dict[str, Any],
) -> float | None:
    explicit = _safe_float(
        _first_non_empty(
            payload.get("target_distance"),
            execution.get("target_distance"),
        )
    )

    if explicit is not None:
        return explicit

    if entry is None or target is None:
        return None

    return abs(target - entry)


def _extract_caution_flags(payload: dict[str, Any], context: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    metadata = _safe_metadata(payload)

    for key in ("caution_flags", "risk_flags", "safety_flags"):
        flags.extend(_safe_text_list(payload.get(key)))
        flags.extend(_safe_text_list(metadata.get(key)))
        flags.extend(_safe_text_list(context.get(key)))

    # For CAUTION_BATTLE these modifiers explain why it is not a clean green setup.
    flags.extend(_safe_text_list(payload.get("battle_permission_modifiers")))
    flags.extend(_safe_text_list(metadata.get("battle_permission_modifiers")))
    flags.extend(_safe_text_list(context.get("battle_permission_modifiers")))
    flags.extend(_safe_text_list(payload.get("battle_gate_v2_modifiers")))
    flags.extend(_safe_text_list(metadata.get("battle_gate_v2_modifiers")))
    flags.extend(_safe_text_list(context.get("battle_gate_v2_modifiers")))

    return _dedupe_keep_order(flags)


# =============================================================================
# EVENT BUILDER
# =============================================================================

def build_battle_permission_event(
    payload: dict[str, Any],
    *,
    source: str = "telegram_notifier",
    sent_to_telegram: bool | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """
    Build a compact but complete battle-permission telemetry event.

    v3 requirement:
    - keep v2 execution-plan fields for counterfactual tracking;
    - add Battle Gate safety-context fields;
    - add open-behavior / TPO fields using root → metadata → auction context fallback.
    """
    metadata = _safe_metadata(payload)
    execution = _payload_execution(payload)
    context = _collect_context_sources(payload)

    entry = _extract_entry(payload, execution)
    stop = _extract_stop(payload, execution)
    target = _extract_target(payload, execution)
    rr = _extract_rr(payload, execution)
    theoretical_rr = _extract_theoretical_rr(payload, execution)
    practical_rr = _extract_practical_rr(payload, execution)
    stop_distance = _compute_stop_distance(entry, stop, payload, execution)
    target_distance = _compute_target_distance(entry, target, payload, execution)

    blockers = _safe_text_list(
        _first_non_empty(
            payload.get("battle_permission_blockers"),
            metadata.get("battle_permission_blockers"),
            context.get("battle_permission_blockers"),
        )
    )

    reasons = _safe_text_list(
        _first_non_empty(
            payload.get("battle_permission_reasons"),
            metadata.get("battle_permission_reasons"),
            context.get("battle_permission_reasons"),
        )
    )

    modifiers = _safe_text_list(
        _first_non_empty(
            payload.get("battle_permission_modifiers"),
            metadata.get("battle_permission_modifiers"),
            context.get("battle_permission_modifiers"),
        )
    )

    battle_gate_v2_reasons = _safe_text_list(
        _first_non_empty(
            payload.get("battle_gate_v2_reasons"),
            metadata.get("battle_gate_v2_reasons"),
            context.get("battle_gate_v2_reasons"),
        )
    )
    battle_gate_v2_blockers = _safe_text_list(
        _first_non_empty(
            payload.get("battle_gate_v2_blockers"),
            metadata.get("battle_gate_v2_blockers"),
            context.get("battle_gate_v2_blockers"),
        )
    )
    battle_gate_v2_modifiers = _safe_text_list(
        _first_non_empty(
            payload.get("battle_gate_v2_modifiers"),
            metadata.get("battle_gate_v2_modifiers"),
            context.get("battle_gate_v2_modifiers"),
        )
    )

    caution_flags = _extract_caution_flags(payload, context)

    # Some upstream modules use VA shorthand, others use OPEN_* names.
    open_relation = _first_non_empty(
        _context_get(context, "open_relation", "tpo_open_relation", "open_context"),
        _payload_get(payload, "open_relation", "tpo_open_relation", "open_context"),
    )

    auction_bias = _first_non_empty(
        _context_get(context, "auction_bias", "tpo_auction_bias", "bias"),
        _payload_get(payload, "auction_bias", "tpo_auction_bias", "bias"),
    )

    event = {
        "schema_version": TELEMETRY_SCHEMA_VERSION,
        "event_type": "battle_permission_evaluated",
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "sent_to_telegram": sent_to_telegram,
        "note": note,

        # Signal identity.
        "symbol": payload.get("symbol"),
        "instrument": payload.get("instrument") or payload.get("symbol"),
        "signal_id": payload.get("signal_id"),
        "alert_id": payload.get("alert_id"),
        "cycle_id": payload.get("cycle_id"),
        "alert_type": payload.get("alert_type"),
        "signal_class": payload.get("signal_class") or payload.get("stage"),
        "status": payload.get("status"),
        "scenario": payload.get("scenario") or payload.get("scenario_type"),
        "scenario_type": payload.get("scenario_type") or payload.get("scenario"),
        "scenario_family": _first_non_empty(
            payload.get("scenario_family"),
            metadata.get("scenario_family"),
            context.get("scenario_family"),
        ),
        "direction": payload.get("direction"),
        "htf_bias": payload.get("htf_bias"),
        "signal_alignment": payload.get("signal_alignment"),
        "market_state": payload.get("market_state"),
        "timeframe": payload.get("timeframe") or payload.get("execution_timeframe"),

        # Confidence / quality.
        "confidence": _safe_float(
            _first_non_empty(
                payload.get("confidence"),
                payload.get("trade_confidence"),
                payload.get("signal_confidence"),
            )
        ),
        "probability": _safe_float(
            _first_non_empty(
                payload.get("probability"),
                payload.get("scenario_probability"),
                payload.get("setup_probability"),
            )
        ),
        "quality_tier": payload.get("quality_tier") or payload.get("quality_level"),
        "signal_quality_decision": payload.get("signal_quality_decision"),
        "signal_quality_score": _safe_float(payload.get("signal_quality_score")),
        "signal_quality_reason": payload.get("signal_quality_reason"),

        # Execution plan.
        "execution_status": payload.get("execution_status") or execution.get("status"),
        "execution_model": payload.get("execution_model") or execution.get("model"),
        "execution_timeframe": payload.get("execution_timeframe") or execution.get("execution_timeframe"),
        "trigger_reason": payload.get("trigger_reason") or execution.get("trigger_reason"),

        "entry_reference_price": entry,
        "invalidation_reference_price": stop,
        "target_reference_price": target,

        "risk_reward_ratio": rr,
        "theoretical_rr": theoretical_rr,
        "practical_rr": practical_rr,
        "stop_distance": stop_distance,
        "target_distance": target_distance,

        "stop_quality": payload.get("stop_quality"),
        "stop_quality_reason": payload.get("stop_quality_reason"),

        # Battle gate decision.
        "battle_permission": _first_non_empty(
            payload.get("battle_permission"),
            metadata.get("battle_permission"),
            context.get("battle_permission"),
        ),
        "telegram_delivery_mode": _first_non_empty(
            payload.get("telegram_delivery_mode"),
            metadata.get("telegram_delivery_mode"),
            context.get("telegram_delivery_mode"),
        ),
        "battle_ready": _safe_bool(
            _first_non_empty(
                payload.get("battle_ready") if "battle_ready" in payload else None,
                metadata.get("battle_ready"),
                context.get("battle_ready"),
            )
        ),
        "auction_context_score": _safe_float(
            _first_non_empty(
                payload.get("auction_context_score"),
                metadata.get("auction_context_score"),
                context.get("auction_context_score"),
                context.get("score"),
            )
        ),
        "battle_permission_blockers": blockers,
        "battle_permission_reasons": reasons,
        "battle_permission_modifiers": modifiers,
        "battle_permission_version": _first_non_empty(
            payload.get("battle_permission_version"),
            metadata.get("battle_permission_version"),
            context.get("battle_permission_version"),
        ),

        # Safety-context fields added in v3.
        "risk_mode": _first_non_empty(
            payload.get("risk_mode"),
            payload.get("battle_risk_mode"),
            payload.get("battle_gate_v2_risk_mode"),
            metadata.get("risk_mode"),
            metadata.get("battle_risk_mode"),
            metadata.get("battle_gate_v2_risk_mode"),
            context.get("risk_mode"),
            context.get("battle_risk_mode"),
            context.get("battle_gate_v2_risk_mode"),
        ),
        "news_risk_state": _first_non_empty(
            payload.get("news_risk_state"),
            metadata.get("news_risk_state"),
            context.get("news_risk_state"),
        ),
        "news_provider_status": _first_non_empty(
            payload.get("news_provider_status"),
            metadata.get("news_provider_status"),
            context.get("news_provider_status"),
        ),
        "local_structure_damaged": _safe_bool(
            _first_non_empty(
                payload.get("local_structure_damaged"),
                metadata.get("local_structure_damaged"),
                context.get("local_structure_damaged"),
            )
        ),
        "target_quality": _first_non_empty(
            payload.get("target_quality"),
            metadata.get("target_quality"),
            context.get("target_quality"),
        ),
        "caution_flags": caution_flags,
        "risk_flags": _safe_text_list(
            _first_non_empty(
                payload.get("risk_flags"),
                metadata.get("risk_flags"),
                context.get("risk_flags"),
            )
        ),
        "safety_flags": _safe_text_list(
            _first_non_empty(
                payload.get("safety_flags"),
                metadata.get("safety_flags"),
                context.get("safety_flags"),
            )
        ),

        # Battle Gate v2 shadow context, if present.
        "battle_gate_v2_decision": _first_non_empty(
            payload.get("battle_gate_v2_decision"),
            metadata.get("battle_gate_v2_decision"),
            context.get("battle_gate_v2_decision"),
        ),
        "battle_gate_v2_risk_mode": _first_non_empty(
            payload.get("battle_gate_v2_risk_mode"),
            metadata.get("battle_gate_v2_risk_mode"),
            context.get("battle_gate_v2_risk_mode"),
        ),
        "battle_gate_v2_allowed": _safe_bool(
            _first_non_empty(
                payload.get("battle_gate_v2_allowed"),
                metadata.get("battle_gate_v2_allowed"),
                context.get("battle_gate_v2_allowed"),
            )
        ),
        "battle_gate_v2_suppress": _safe_bool(
            _first_non_empty(
                payload.get("battle_gate_v2_suppress"),
                metadata.get("battle_gate_v2_suppress"),
                context.get("battle_gate_v2_suppress"),
            )
        ),
        "battle_gate_v2_score_delta": _safe_float(
            _first_non_empty(
                payload.get("battle_gate_v2_score_delta"),
                metadata.get("battle_gate_v2_score_delta"),
                context.get("battle_gate_v2_score_delta"),
            )
        ),
        "battle_gate_v2_reasons": battle_gate_v2_reasons,
        "battle_gate_v2_blockers": battle_gate_v2_blockers,
        "battle_gate_v2_modifiers": battle_gate_v2_modifiers,

        # TPO / auction context.
        "market_is_open": _safe_bool(
            _first_non_empty(
                context.get("market_is_open"),
                payload.get("market_is_open"),
                metadata.get("market_is_open"),
            )
        ),
        "market_status": _first_non_empty(
            context.get("market_status"),
            payload.get("market_status"),
            metadata.get("market_status"),
        ),
        "tpo_signal_permission": _first_non_empty(
            context.get("tpo_signal_permission"),
            payload.get("tpo_signal_permission"),
            metadata.get("tpo_signal_permission"),
        ),
        "tpo_telegram_modifier": _first_non_empty(
            context.get("tpo_telegram_modifier"),
            context.get("telegram_modifier"),
            payload.get("tpo_telegram_modifier"),
            metadata.get("tpo_telegram_modifier"),
        ),
        "open_relation": open_relation,
        "auction_bias": auction_bias,
        "open_context": _first_non_empty(
            context.get("open_context"),
            context.get("open_relation"),
            payload.get("open_context"),
            metadata.get("open_context"),
            open_relation,
        ),
        "open_behavior": _first_non_empty(
            context.get("open_behavior"),
            payload.get("open_behavior"),
            metadata.get("open_behavior"),
        ),
        "open_behavior_confidence": _safe_float(
            _first_non_empty(
                context.get("open_behavior_confidence"),
                payload.get("open_behavior_confidence"),
                metadata.get("open_behavior_confidence"),
            )
        ),
        "entry_model_hint": _first_non_empty(
            context.get("entry_model_hint"),
            payload.get("entry_model_hint"),
            metadata.get("entry_model_hint"),
        ),
        "stop_model_hint": _first_non_empty(
            context.get("stop_model_hint"),
            payload.get("stop_model_hint"),
            metadata.get("stop_model_hint"),
        ),
        "battle_bias_hint": _first_non_empty(
            context.get("battle_bias_hint"),
            payload.get("battle_bias_hint"),
            metadata.get("battle_bias_hint"),
        ),
        "primary_interest_zone": _first_non_empty(
            context.get("primary_interest_zone"),
            payload.get("primary_interest_zone"),
            metadata.get("primary_interest_zone"),
        ),

        # Session context.
        "session_label": _first_non_empty(
            context.get("session_label"),
            payload.get("session_label"),
            metadata.get("session_label"),
        ),
        "session_anchor": _first_non_empty(
            context.get("session_anchor"),
            payload.get("session_anchor"),
            metadata.get("session_anchor"),
        ),
        "session_timezone": _first_non_empty(
            context.get("session_timezone"),
            payload.get("session_timezone"),
            metadata.get("session_timezone"),
        ),
        "session_open_utc": _first_non_empty(
            context.get("session_open_utc"),
            payload.get("session_open_utc"),
            metadata.get("session_open_utc"),
        ),
        "current_session_id": _first_non_empty(
            context.get("current_session_id"),
            payload.get("current_session_id"),
            metadata.get("current_session_id"),
        ),

        # Interest-zone / IB / nPOC fields.
        "nearest_npoc": _safe_float(
            _first_non_empty(
                context.get("nearest_npoc"),
                payload.get("nearest_npoc"),
                metadata.get("nearest_npoc"),
            )
        ),
        "nearest_npoc_distance": _safe_float(
            _first_non_empty(
                context.get("nearest_npoc_distance"),
                payload.get("nearest_npoc_distance"),
                metadata.get("nearest_npoc_distance"),
            )
        ),
        "ib_extension_up_pct": _safe_float(
            _first_non_empty(
                context.get("ib_extension_up_pct"),
                payload.get("ib_extension_up_pct"),
                metadata.get("ib_extension_up_pct"),
            )
        ),
        "ib_extension_down_pct": _safe_float(
            _first_non_empty(
                context.get("ib_extension_down_pct"),
                payload.get("ib_extension_down_pct"),
                metadata.get("ib_extension_down_pct"),
            )
        ),
        "interest_zone_type": _first_non_empty(
            context.get("interest_zone_type"),
            payload.get("interest_zone_type"),
            metadata.get("interest_zone_type"),
        ),
        "interest_zone_price": _safe_float(
            _first_non_empty(
                context.get("interest_zone_price"),
                payload.get("interest_zone_price"),
                metadata.get("interest_zone_price"),
            )
        ),
        "interest_zone_role": _first_non_empty(
            context.get("interest_zone_role"),
            payload.get("interest_zone_role"),
            metadata.get("interest_zone_role"),
        ),
        "interest_zone_reaction": _first_non_empty(
            context.get("interest_zone_reaction"),
            payload.get("interest_zone_reaction"),
            metadata.get("interest_zone_reaction"),
        ),

        # Post-news fields are optional now, but keeping placeholders in telemetry
        # lets the next detector plug in without another schema break.
        "post_news_regime": _first_non_empty(
            payload.get("post_news_regime"),
            metadata.get("post_news_regime"),
            context.get("post_news_regime"),
        ),
        "post_news_elapsed_minutes": _safe_float(
            _first_non_empty(
                payload.get("post_news_elapsed_minutes"),
                metadata.get("post_news_elapsed_minutes"),
                context.get("post_news_elapsed_minutes"),
            )
        ),
        "post_news_impulse_direction": _first_non_empty(
            payload.get("post_news_impulse_direction"),
            metadata.get("post_news_impulse_direction"),
            context.get("post_news_impulse_direction"),
        ),
        "post_news_retest_status": _first_non_empty(
            payload.get("post_news_retest_status"),
            metadata.get("post_news_retest_status"),
            context.get("post_news_retest_status"),
        ),
        "post_news_acceptance_status": _first_non_empty(
            payload.get("post_news_acceptance_status"),
            metadata.get("post_news_acceptance_status"),
            context.get("post_news_acceptance_status"),
        ),
        "post_news_continuation_quality": _first_non_empty(
            payload.get("post_news_continuation_quality"),
            metadata.get("post_news_continuation_quality"),
            context.get("post_news_continuation_quality"),
        ),

        # Telegram final outcome.
        "telegram_sent": sent_to_telegram,
    }

    return event


# =============================================================================
# WRITER
# =============================================================================

def record_battle_permission_event(
    payload: dict[str, Any],
    *,
    source: str = "telegram_notifier",
    sent_to_telegram: bool | None = None,
    note: str | None = None,
) -> bool:
    """
    Best-effort telemetry writer.

    This function must never break Telegram delivery or live worker execution.
    If telemetry fails, we log the error and return False.
    """
    try:
        path = _telemetry_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        event = build_battle_permission_event(
            payload,
            source=source,
            sent_to_telegram=sent_to_telegram,
            note=note,
        )

        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, default=_json_default))
            f.write("\n")

        return True

    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to write battle permission telemetry. symbol=%s signal_id=%s error=%s",
            payload.get("symbol"),
            payload.get("signal_id"),
            exc,
        )
        return False