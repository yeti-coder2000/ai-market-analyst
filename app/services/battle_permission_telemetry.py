from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)


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


def _safe_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    meta = payload.get("metadata")
    return meta if isinstance(meta, dict) else {}


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


def build_battle_permission_event(
    payload: dict[str, Any],
    *,
    source: str = "telegram_notifier",
    sent_to_telegram: bool | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    metadata = _safe_metadata(payload)

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

    auction_context = metadata.get("auction_context")
    if not isinstance(auction_context, dict):
        auction_context = {}

    auction_filters = metadata.get("auction_filters")
    if not isinstance(auction_filters, dict):
        auction_filters = {}

    return {
        "event_type": "battle_permission_evaluated",
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "sent_to_telegram": sent_to_telegram,
        "note": note,
        "symbol": payload.get("symbol"),
        "signal_id": payload.get("signal_id"),
        "alert_type": payload.get("alert_type"),
        "signal_class": payload.get("signal_class") or payload.get("stage"),
        "scenario": payload.get("scenario") or payload.get("scenario_type"),
        "direction": payload.get("direction"),
        "htf_bias": payload.get("htf_bias"),
        "signal_alignment": payload.get("signal_alignment"),
        "execution_status": payload.get("execution_status"),
        "practical_rr": payload.get("practical_rr"),
        "stop_quality": payload.get("stop_quality"),
        "quality_tier": payload.get("quality_tier") or payload.get("quality_level"),
        "market_state": payload.get("market_state"),
        "battle_permission": payload.get("battle_permission")
        or metadata.get("battle_permission"),
        "telegram_delivery_mode": payload.get("telegram_delivery_mode")
        or metadata.get("telegram_delivery_mode"),
        "battle_ready": payload.get("battle_ready")
        if "battle_ready" in payload
        else metadata.get("battle_ready"),
        "auction_context_score": payload.get("auction_context_score")
        or metadata.get("auction_context_score"),
        "battle_permission_blockers": blockers,
        "battle_permission_reasons": reasons,
        "battle_permission_modifiers": modifiers,
        "market_is_open": _first_non_empty(
            auction_context.get("market_is_open"),
            auction_filters.get("market_is_open"),
        ),
        "market_status": _first_non_empty(
            auction_context.get("market_status"),
            auction_filters.get("market_status"),
        ),
        "tpo_signal_permission": _first_non_empty(
            metadata.get("tpo_signal_permission"),
            auction_filters.get("tpo_signal_permission"),
        ),
        "tpo_telegram_modifier": _first_non_empty(
            metadata.get("tpo_telegram_modifier"),
            auction_filters.get("telegram_modifier"),
        ),
        "open_relation": _first_non_empty(
            metadata.get("tpo_open_relation"),
            auction_context.get("open_relation"),
            auction_filters.get("open_relation"),
        ),
        "auction_bias": _first_non_empty(
            metadata.get("tpo_auction_bias"),
            auction_context.get("auction_bias"),
            auction_filters.get("auction_bias"),
        ),
    }


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