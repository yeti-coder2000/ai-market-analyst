from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)


TELEMETRY_SCHEMA_VERSION = "battle-permission-telemetry-v2-execution-plan"


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

    if value in (None, "", {}, ()):
        return []

    return [value]


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _json_default(value: Any) -> str:
    return str(value)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value

    if value is None:
        return None

    text = str(value).strip().lower()

    if text in {"1", "true", "yes", "y", "on"}:
        return True

    if text in {"0", "false", "no", "n", "off"}:
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


def _compute_stop_distance(entry: float | None, stop: float | None, payload: dict[str, Any], execution: dict[str, Any]) -> float | None:
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


def _compute_target_distance(entry: float | None, target: float | None, payload: dict[str, Any], execution: dict[str, Any]) -> float | None:
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


def _latest_non_empty_dict(*values: Any) -> dict[str, Any]:
    """
    Merge dictionaries from left to right.
    Later values override earlier values.
    """
    result: dict[str, Any] = {}

    for value in values:
        if isinstance(value, dict):
            result.update(value)

    return result


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

    v2 requirement:
    - save execution plan fields so suppressed EXECUTABLE signals can later be
      tracked as RESEARCH_COUNTERFACTUAL outcomes:
        entry_reference_price
        invalidation_reference_price
        target_reference_price
        risk_reward_ratio
        practical_rr

    This event is intentionally flat because downstream exporters / reports
    should not need to understand the whole Telegram payload shape.
    """
    metadata = _safe_metadata(payload)
    execution = _payload_execution(payload)

    auction_context = _safe_dict(metadata.get("auction_context"))
    auction_filters = _safe_dict(metadata.get("auction_filters"))

    # Some integrations may put auction fields directly into payload metadata.
    # Keep those as fallback sources.
    merged_auction = _latest_non_empty_dict(
        auction_context,
        auction_filters,
        {
            "market_is_open": metadata.get("market_is_open"),
            "market_status": metadata.get("market_status"),
            "open_relation": metadata.get("tpo_open_relation"),
            "auction_bias": metadata.get("tpo_auction_bias"),
            "tpo_signal_permission": metadata.get("tpo_signal_permission"),
            "telegram_modifier": metadata.get("tpo_telegram_modifier"),
        },
    )

    entry = _extract_entry(payload, execution)
    stop = _extract_stop(payload, execution)
    target = _extract_target(payload, execution)
    rr = _extract_rr(payload, execution)
    theoretical_rr = _extract_theoretical_rr(payload, execution)
    practical_rr = _extract_practical_rr(payload, execution)
    stop_distance = _compute_stop_distance(entry, stop, payload, execution)
    target_distance = _compute_target_distance(entry, target, payload, execution)

    blockers = _safe_list(
        _first_non_empty(
            metadata.get("battle_permission_blockers"),
            payload.get("battle_permission_blockers"),
        )
    )

    reasons = _safe_list(
        _first_non_empty(
            metadata.get("battle_permission_reasons"),
            payload.get("battle_permission_reasons"),
        )
    )

    modifiers = _safe_list(
        _first_non_empty(
            metadata.get("battle_permission_modifiers"),
            payload.get("battle_permission_modifiers"),
        )
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
        "battle_permission": (
            payload.get("battle_permission")
            or metadata.get("battle_permission")
        ),
        "telegram_delivery_mode": (
            payload.get("telegram_delivery_mode")
            or metadata.get("telegram_delivery_mode")
        ),
        "battle_ready": (
            payload.get("battle_ready")
            if "battle_ready" in payload
            else metadata.get("battle_ready")
        ),
        "auction_context_score": _safe_float(
            _first_non_empty(
                payload.get("auction_context_score"),
                metadata.get("auction_context_score"),
            )
        ),
        "battle_permission_blockers": blockers,
        "battle_permission_reasons": reasons,
        "battle_permission_modifiers": modifiers,

        # TPO / auction context.
        "market_is_open": _safe_bool(
            _first_non_empty(
                merged_auction.get("market_is_open"),
                auction_context.get("market_is_open"),
                auction_filters.get("market_is_open"),
            )
        ),
        "market_status": _first_non_empty(
            merged_auction.get("market_status"),
            auction_context.get("market_status"),
            auction_filters.get("market_status"),
        ),
        "tpo_signal_permission": _first_non_empty(
            merged_auction.get("tpo_signal_permission"),
            auction_filters.get("tpo_signal_permission"),
            metadata.get("tpo_signal_permission"),
        ),
        "tpo_telegram_modifier": _first_non_empty(
            merged_auction.get("telegram_modifier"),
            auction_filters.get("telegram_modifier"),
            metadata.get("tpo_telegram_modifier"),
        ),
        "open_relation": _first_non_empty(
            merged_auction.get("open_relation"),
            auction_context.get("open_relation"),
            auction_filters.get("open_relation"),
            metadata.get("tpo_open_relation"),
        ),
        "auction_bias": _first_non_empty(
            merged_auction.get("auction_bias"),
            auction_context.get("auction_bias"),
            auction_filters.get("auction_bias"),
            metadata.get("tpo_auction_bias"),
        ),
        "session_anchor": _first_non_empty(
            merged_auction.get("session_anchor"),
            auction_context.get("session_anchor"),
            auction_filters.get("session_anchor"),
            metadata.get("session_anchor"),
        ),
        "session_timezone": _first_non_empty(
            merged_auction.get("session_timezone"),
            auction_context.get("session_timezone"),
            auction_filters.get("session_timezone"),
            metadata.get("session_timezone"),
        ),
        "session_open_utc": _first_non_empty(
            merged_auction.get("session_open_utc"),
            auction_context.get("session_open_utc"),
            auction_filters.get("session_open_utc"),
            metadata.get("session_open_utc"),
        ),
        "current_session_id": _first_non_empty(
            merged_auction.get("current_session_id"),
            auction_context.get("current_session_id"),
            auction_filters.get("current_session_id"),
            metadata.get("current_session_id"),
        ),

        # IB / nPOC interest fields.
        "nearest_npoc": _safe_float(
            _first_non_empty(
                merged_auction.get("nearest_npoc"),
                auction_context.get("nearest_npoc"),
                metadata.get("nearest_npoc"),
            )
        ),
        "nearest_npoc_distance": _safe_float(
            _first_non_empty(
                merged_auction.get("nearest_npoc_distance"),
                auction_context.get("nearest_npoc_distance"),
                metadata.get("nearest_npoc_distance"),
            )
        ),
        "ib_extension_up_pct": _safe_float(
            _first_non_empty(
                merged_auction.get("ib_extension_up_pct"),
                auction_context.get("ib_extension_up_pct"),
                metadata.get("ib_extension_up_pct"),
            )
        ),
        "ib_extension_down_pct": _safe_float(
            _first_non_empty(
                merged_auction.get("ib_extension_down_pct"),
                auction_context.get("ib_extension_down_pct"),
                metadata.get("ib_extension_down_pct"),
            )
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