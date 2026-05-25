from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.settings import settings


STATS_DIR = settings.runtime_dir / "stats"
TELEMETRY_DIR = settings.runtime_dir / "telemetry"

SIGNAL_OUTCOMES_PATH = STATS_DIR / "signal_outcomes.json"
TELEGRAM_ALERTS_PATH = STATS_DIR / "telegram_alerts.json"
TELEGRAM_ALERTS_NDJSON_PATH = STATS_DIR / "telegram_alerts.ndjson"
BATTLE_PERMISSION_TELEMETRY_PATH = TELEMETRY_DIR / "battle_permission_events.ndjson"

SIGNALS_FLAT_JSON_PATH = STATS_DIR / "signals_flat.json"
DAILY_SUMMARY_PATH = STATS_DIR / "daily_summary.json"

EXPORTER_VERSION = "lightweight-statistics-exporter-v2-battle-permission"


FINAL_OUTCOMES = {
    "TP_HIT",
    "SL_HIT",
    "MISSED_TARGET_BEFORE_ENTRY",
    "EXPIRED",
    "EXPIRED_AFTER_ENTRY",
    "INVALID",
}

TP_STATUS = "TP_HIT"
SL_STATUS = "SL_HIT"

PRE_BATTLE_GATE_PERMISSION = "PRE_BATTLE_GATE"
LEGACY_TELEGRAM_DELIVERY_MODE = "LEGACY_TELEGRAM_ALERT"


# =============================================================================
# Generic helpers
# =============================================================================


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def load_ndjson(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue

            try:
                item = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if isinstance(item, dict):
                rows.append(item)

    return rows


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def normalize_status(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def normalize_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value

    if value in (None, "", {}, ()):
        return []

    return [value]


def safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value

    if value is None:
        return default

    text = str(value).strip().lower()

    if text in {"1", "true", "yes", "y", "on"}:
        return True

    if text in {"0", "false", "no", "n", "off"}:
        return False

    return default


def safe_optional_bool(value: Any) -> bool | None:
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


def first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def pick_latest_event(existing: dict[str, Any] | None, incoming: dict[str, Any]) -> dict[str, Any]:
    if existing is None:
        return incoming

    existing_ts = parse_dt(existing.get("ts_utc") or existing.get("created_at_utc"))
    incoming_ts = parse_dt(incoming.get("ts_utc") or incoming.get("created_at_utc"))

    if existing_ts is None and incoming_ts is not None:
        return incoming

    if existing_ts is not None and incoming_ts is not None and incoming_ts >= existing_ts:
        return incoming

    return existing


# =============================================================================
# Source extraction
# =============================================================================


def extract_outcome_signals(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        signals = payload.get("signals", [])
        if isinstance(signals, list):
            return [x for x in signals if isinstance(x, dict)]

        if isinstance(signals, dict):
            return [x for x in signals.values() if isinstance(x, dict)]

    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    return []


def extract_telegram_alerts_json(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        for key in ("alerts", "signals", "items", "records"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]

            if isinstance(value, dict):
                return [x for x in value.values() if isinstance(x, dict)]

    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    return []


def build_alert_index() -> dict[str, dict[str, Any]]:
    alerts: list[dict[str, Any]] = []

    json_payload = load_json(TELEGRAM_ALERTS_PATH, default={})
    alerts.extend(extract_telegram_alerts_json(json_payload))

    alerts.extend(load_ndjson(TELEGRAM_ALERTS_NDJSON_PATH))

    index: dict[str, dict[str, Any]] = {}

    for alert in alerts:
        signal_id = alert.get("signal_id")
        alert_id = alert.get("alert_id")

        if signal_id:
            key = str(signal_id)
            index[key] = pick_latest_event(index.get(key), alert)

        if alert_id:
            key = str(alert_id)
            index[key] = pick_latest_event(index.get(key), alert)

    return index


def build_battle_permission_index() -> dict[str, dict[str, Any]]:
    """
    Index battle permission telemetry by signal_id / alert_id.

    Current telemetry events mainly contain signal_id. alert_id support is kept
    for future-proofing.
    """
    events = load_ndjson(BATTLE_PERMISSION_TELEMETRY_PATH)
    index: dict[str, dict[str, Any]] = {}

    for event in events:
        if event.get("event_type") != "battle_permission_evaluated":
            continue

        signal_id = event.get("signal_id")
        alert_id = event.get("alert_id")

        if signal_id:
            key = str(signal_id)
            index[key] = pick_latest_event(index.get(key), event)

        if alert_id:
            key = str(alert_id)
            index[key] = pick_latest_event(index.get(key), event)

    return index


def merge_signal_with_alert(
    signal: dict[str, Any],
    alert_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    signal_id = signal.get("signal_id")
    alert_id = signal.get("alert_id")

    alert = None

    if signal_id:
        alert = alert_index.get(str(signal_id))

    if alert is None and alert_id:
        alert = alert_index.get(str(alert_id))

    merged: dict[str, Any] = {}

    if isinstance(alert, dict):
        merged.update(alert)

    merged.update(signal)

    return merged


def find_battle_event(
    item: dict[str, Any],
    battle_index: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    signal_id = item.get("signal_id")
    alert_id = item.get("alert_id")

    if signal_id and str(signal_id) in battle_index:
        return battle_index[str(signal_id)]

    if alert_id and str(alert_id) in battle_index:
        return battle_index[str(alert_id)]

    return None


# =============================================================================
# Battle permission enrichment
# =============================================================================


def extract_tpo_fields_from_item(item: dict[str, Any]) -> dict[str, Any]:
    metadata = item.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    auction_context = metadata.get("auction_context")
    if not isinstance(auction_context, dict):
        auction_context = {}

    auction_filters = metadata.get("auction_filters")
    if not isinstance(auction_filters, dict):
        auction_filters = {}

    return {
        "open_relation": first_non_empty(
            item.get("open_relation"),
            item.get("tpo_open_relation"),
            metadata.get("tpo_open_relation"),
            auction_context.get("open_relation"),
            auction_filters.get("open_relation"),
        ),
        "auction_bias": first_non_empty(
            item.get("auction_bias"),
            item.get("tpo_auction_bias"),
            metadata.get("tpo_auction_bias"),
            auction_context.get("auction_bias"),
            auction_filters.get("auction_bias"),
        ),
        "tpo_signal_permission": first_non_empty(
            item.get("tpo_signal_permission"),
            metadata.get("tpo_signal_permission"),
            auction_filters.get("tpo_signal_permission"),
        ),
        "tpo_telegram_modifier": first_non_empty(
            item.get("tpo_telegram_modifier"),
            metadata.get("tpo_telegram_modifier"),
            auction_filters.get("telegram_modifier"),
        ),
        "market_is_open": first_non_empty(
            item.get("market_is_open"),
            auction_context.get("market_is_open"),
            auction_filters.get("market_is_open"),
        ),
        "market_status": first_non_empty(
            item.get("market_status"),
            auction_context.get("market_status"),
            auction_filters.get("market_status"),
        ),
    }


def extract_battle_fields(
    item: dict[str, Any],
    battle_event: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Return battle permission fields for a flat signal.

    - If a battle telemetry event exists, use it as source of truth.
    - If no battle event exists, classify the signal as PRE_BATTLE_GATE / LEGACY.
      This prevents mixing old Telegram alerts with the new Battle Gate era.
    """
    tpo_fields = extract_tpo_fields_from_item(item)

    if isinstance(battle_event, dict):
        blockers = normalize_list(battle_event.get("battle_permission_blockers"))
        reasons = normalize_list(battle_event.get("battle_permission_reasons"))
        modifiers = normalize_list(battle_event.get("battle_permission_modifiers"))

        return {
            "battle_permission": battle_event.get("battle_permission") or "UNKNOWN",
            "telegram_delivery_mode": battle_event.get("telegram_delivery_mode") or "UNKNOWN",
            "battle_ready": safe_optional_bool(battle_event.get("battle_ready")),
            "sent_to_telegram": safe_optional_bool(battle_event.get("sent_to_telegram")),
            "auction_context_score": safe_float(battle_event.get("auction_context_score"), None),
            "battle_permission_blockers": blockers,
            "battle_permission_reasons": reasons,
            "battle_permission_modifiers": modifiers,
            "battle_permission_event_found": True,
            "battle_permission_event_ts_utc": battle_event.get("ts_utc"),
            "battle_permission_source": battle_event.get("source") or "battle_permission_telemetry",
            "open_relation": first_non_empty(battle_event.get("open_relation"), tpo_fields.get("open_relation")),
            "auction_bias": first_non_empty(battle_event.get("auction_bias"), tpo_fields.get("auction_bias")),
            "tpo_signal_permission": first_non_empty(
                battle_event.get("tpo_signal_permission"),
                tpo_fields.get("tpo_signal_permission"),
            ),
            "tpo_telegram_modifier": first_non_empty(
                battle_event.get("tpo_telegram_modifier"),
                tpo_fields.get("tpo_telegram_modifier"),
            ),
            "market_is_open": first_non_empty(battle_event.get("market_is_open"), tpo_fields.get("market_is_open")),
            "market_status": first_non_empty(battle_event.get("market_status"), tpo_fields.get("market_status")),
        }

    was_sent = first_non_empty(
        item.get("telegram_sent"),
        item.get("sent_to_telegram"),
        True if item.get("sent_at_utc") else None,
    )

    return {
        "battle_permission": PRE_BATTLE_GATE_PERMISSION,
        "telegram_delivery_mode": LEGACY_TELEGRAM_DELIVERY_MODE,
        "battle_ready": None,
        "sent_to_telegram": safe_optional_bool(was_sent),
        "auction_context_score": None,
        "battle_permission_blockers": [],
        "battle_permission_reasons": [],
        "battle_permission_modifiers": [],
        "battle_permission_event_found": False,
        "battle_permission_event_ts_utc": None,
        "battle_permission_source": "legacy_no_battle_telemetry",
        "open_relation": tpo_fields.get("open_relation"),
        "auction_bias": tpo_fields.get("auction_bias"),
        "tpo_signal_permission": tpo_fields.get("tpo_signal_permission"),
        "tpo_telegram_modifier": tpo_fields.get("tpo_telegram_modifier"),
        "market_is_open": tpo_fields.get("market_is_open"),
        "market_status": tpo_fields.get("market_status"),
    }


# =============================================================================
# Flat records
# =============================================================================


def normalize_flat_signal(
    item: dict[str, Any],
    battle_event: dict[str, Any] | None = None,
) -> dict[str, Any]:
    outcome_status = normalize_status(
        item.get("outcome_status")
        or item.get("outcome")
        or item.get("status")
    )

    result_r = safe_float(
        item.get("result_R")
        if item.get("result_R") is not None
        else item.get("result_r"),
        None,
    )

    if result_r is None:
        if outcome_status == TP_STATUS:
            result_r = safe_float(item.get("practical_rr"), None)
            if result_r is None:
                result_r = safe_float(item.get("risk_reward_ratio"), None)
            if result_r is None:
                result_r = 1.0
        elif outcome_status == SL_STATUS:
            result_r = -1.0
        elif outcome_status in {"MISSED_TARGET_BEFORE_ENTRY", "EXPIRED", "EXPIRED_AFTER_ENTRY"}:
            result_r = 0.0

    signal_id = item.get("signal_id") or item.get("alert_id") or ""

    symbol = item.get("symbol") or item.get("instrument") or ""
    scenario = item.get("scenario") or item.get("scenario_type") or ""
    direction = item.get("direction") or ""

    signal_class = (
        item.get("signal_class")
        or item.get("alert_type")
        or item.get("status")
        or ""
    )

    execution_status = item.get("execution_status")
    execution_model = item.get("execution_model")

    battle_fields = extract_battle_fields(item, battle_event)

    flat = {
        "schema_version": "1.1",
        "exporter_version": EXPORTER_VERSION,
        "signal_id": signal_id,
        "alert_id": item.get("alert_id"),
        "symbol": symbol,
        "timeframe": item.get("timeframe") or item.get("execution_timeframe") or "15m",
        "cycle_id": item.get("cycle_id"),
        "created_at_utc": (
            item.get("created_at_utc")
            or item.get("sent_at_utc")
            or item.get("cycle_id")
        ),
        "sent_at_utc": item.get("sent_at_utc"),
        "closed_at_utc": item.get("closed_at_utc"),
        "last_checked_at_utc": item.get("last_checked_at_utc"),
        "scenario": scenario,
        "scenario_type": item.get("scenario_type") or scenario,
        "signal_class": signal_class,
        "alert_type": item.get("alert_type"),
        "direction": direction,
        "market_state": item.get("market_state"),
        "htf_bias": item.get("htf_bias"),
        "confidence": safe_float(item.get("confidence"), None),
        "probability": safe_float(item.get("probability"), None),
        "phase": item.get("phase"),
        "status": item.get("status"),
        "entry_reference_price": safe_float(item.get("entry_reference_price"), None),
        "invalidation_reference_price": safe_float(item.get("invalidation_reference_price"), None),
        "target_reference_price": safe_float(item.get("target_reference_price"), None),
        "execution_status": execution_status,
        "execution_model": execution_model,
        "execution_timeframe": item.get("execution_timeframe"),
        "trigger_reason": item.get("trigger_reason"),
        "risk_reward_ratio": safe_float(item.get("risk_reward_ratio"), None),
        "theoretical_rr": safe_float(item.get("theoretical_rr"), None),
        "practical_rr": safe_float(item.get("practical_rr"), None),
        "stop_distance": safe_float(item.get("stop_distance"), None),
        "target_distance": safe_float(item.get("target_distance"), None),
        "stop_quality": item.get("stop_quality"),
        "stop_quality_reason": item.get("stop_quality_reason"),
        "signal_alignment": item.get("signal_alignment"),
        "signal_alignment_label": item.get("signal_alignment_label"),
        "signal_alignment_marker": item.get("signal_alignment_marker"),
        "signal_quality_decision": item.get("signal_quality_decision"),
        "signal_quality_score": safe_float(item.get("signal_quality_score"), None),
        "signal_quality_reason": item.get("signal_quality_reason"),
        "telegram_allowed": safe_bool(item.get("telegram_allowed"), False),
        "telegram_sent": safe_bool(item.get("telegram_sent"), False),
        "telegram_hard_gate_allowed": safe_bool(item.get("telegram_hard_gate_allowed"), False),
        "telegram_hard_gate_reason": item.get("telegram_hard_gate_reason"),
        "telegram_title": item.get("telegram_title"),
        "entry_triggered": safe_bool(item.get("entry_triggered"), False),
        "entry_triggered_at_utc": item.get("entry_triggered_at_utc"),
        "tp_hit": safe_bool(item.get("tp_hit"), False),
        "tp_hit_at_utc": item.get("tp_hit_at_utc"),
        "sl_hit": safe_bool(item.get("sl_hit"), False),
        "sl_hit_at_utc": item.get("sl_hit_at_utc"),
        "expired": safe_bool(item.get("expired"), False),
        "expired_at_utc": item.get("expired_at_utc"),
        "outcome": outcome_status or None,
        "outcome_status": outcome_status or None,
        "result_R": result_r,
        "result_r": result_r,
        "result_pct": safe_float(item.get("result_pct"), None),
        "mfe_R": safe_float(item.get("mfe_R"), None),
        "mae_R": safe_float(item.get("mae_R"), None),
        "mfe_price": safe_float(item.get("mfe_price"), None),
        "mae_price": safe_float(item.get("mae_price"), None),
        "last_price": safe_float(item.get("last_price"), None),
        "paper_mode": safe_bool(item.get("paper_mode"), False),
        "source": item.get("source"),
        "tags": item.get("tags") if isinstance(item.get("tags"), list) else [],
        "notes": item.get("notes") if isinstance(item.get("notes"), list) else [],
        # Battle Permission / TPO enrichment.
        "battle_permission": battle_fields["battle_permission"],
        "telegram_delivery_mode": battle_fields["telegram_delivery_mode"],
        "battle_ready": battle_fields["battle_ready"],
        "sent_to_telegram": battle_fields["sent_to_telegram"],
        "auction_context_score": battle_fields["auction_context_score"],
        "battle_permission_blockers": battle_fields["battle_permission_blockers"],
        "battle_permission_reasons": battle_fields["battle_permission_reasons"],
        "battle_permission_modifiers": battle_fields["battle_permission_modifiers"],
        "battle_permission_event_found": battle_fields["battle_permission_event_found"],
        "battle_permission_event_ts_utc": battle_fields["battle_permission_event_ts_utc"],
        "battle_permission_source": battle_fields["battle_permission_source"],
        "open_relation": battle_fields["open_relation"],
        "auction_bias": battle_fields["auction_bias"],
        "tpo_signal_permission": battle_fields["tpo_signal_permission"],
        "tpo_telegram_modifier": battle_fields["tpo_telegram_modifier"],
        "market_is_open": battle_fields["market_is_open"],
        "market_status": battle_fields["market_status"],
        "updated_at_utc": utc_now_iso(),
    }

    return flat


def build_flat_signals() -> list[dict[str, Any]]:
    outcome_payload = load_json(SIGNAL_OUTCOMES_PATH, default={})
    outcome_signals = extract_outcome_signals(outcome_payload)

    alert_index = build_alert_index()
    battle_index = build_battle_permission_index()

    flat: list[dict[str, Any]] = []

    for signal in outcome_signals:
        merged = merge_signal_with_alert(signal, alert_index)
        battle_event = find_battle_event(merged, battle_index)
        flat.append(normalize_flat_signal(merged, battle_event=battle_event))

    flat.sort(
        key=lambda x: (
            normalize_text(x.get("created_at_utc")),
            normalize_text(x.get("signal_id")),
        )
    )

    return flat


# =============================================================================
# Metrics
# =============================================================================


def status_counts(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(Counter(str(x.get(key) or "UNKNOWN") for x in items))


def grouped_metrics(items: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for item in items:
        grouped[str(item.get(key) or "UNKNOWN")].append(item)

    output: dict[str, dict[str, Any]] = {}

    for group_key, group_items in grouped.items():
        output[group_key] = compute_signal_summary(group_items)

    return dict(sorted(output.items(), key=lambda kv: kv[0]))


def grouped_metrics_by_list(items: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for item in items:
        values = item.get(key)

        if isinstance(values, list) and values:
            for value in values:
                grouped[str(value or "UNKNOWN")].append(item)
        else:
            grouped["NONE"].append(item)

    output: dict[str, dict[str, Any]] = {}

    for group_key, group_items in grouped.items():
        output[group_key] = compute_signal_summary(group_items)

    return dict(sorted(output.items(), key=lambda kv: kv[0]))


def compute_signal_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(items)

    tp = sum(normalize_status(x.get("outcome_status")) == TP_STATUS for x in items)
    sl = sum(normalize_status(x.get("outcome_status")) == SL_STATUS for x in items)

    missed = sum(
        normalize_status(x.get("outcome_status")) == "MISSED_TARGET_BEFORE_ENTRY"
        for x in items
    )

    expired = sum(
        normalize_status(x.get("outcome_status")) in {"EXPIRED", "EXPIRED_AFTER_ENTRY"}
        for x in items
    )

    invalid = sum(
        normalize_status(x.get("outcome_status")) == "INVALID"
        for x in items
    )

    pending_or_active = sum(
        normalize_status(x.get("outcome_status"))
        in {"", "PENDING_ENTRY", "ENTRY_TRIGGERED", "ACTIVE", "READY"}
        for x in items
    )

    closed = [
        x for x in items
        if normalize_status(x.get("outcome_status")) in {TP_STATUS, SL_STATUS}
    ]

    result_values = [
        safe_float(x.get("result_R"), None)
        for x in items
        if safe_float(x.get("result_R"), None) is not None
    ]

    rr_values = [
        safe_float(x.get("risk_reward_ratio"), None)
        for x in items
        if safe_float(x.get("risk_reward_ratio"), None) is not None
    ]

    practical_rr_values = [
        safe_float(x.get("practical_rr"), None)
        for x in items
        if safe_float(x.get("practical_rr"), None) is not None
    ]

    sent_count = sum(safe_optional_bool(x.get("sent_to_telegram")) is True for x in items)
    suppressed_count = sum(safe_optional_bool(x.get("sent_to_telegram")) is False for x in items)
    battle_ready_count = sum(safe_optional_bool(x.get("battle_ready")) is True for x in items)
    battle_event_found_count = sum(bool(x.get("battle_permission_event_found")) for x in items)

    return {
        "total_signals": total,
        "tp_hit": tp,
        "sl_hit": sl,
        "missed_before_entry": missed,
        "expired": expired,
        "invalid": invalid,
        "pending_or_active": pending_or_active,
        "closed_tp_sl": len(closed),
        "winrate_tp_sl": round(tp / len(closed), 4) if closed else 0.0,
        "avg_result_R": round(sum(result_values) / len(result_values), 4) if result_values else 0.0,
        "avg_rr": round(sum(rr_values) / len(rr_values), 4) if rr_values else 0.0,
        "avg_practical_rr": (
            round(sum(practical_rr_values) / len(practical_rr_values), 4)
            if practical_rr_values else 0.0
        ),
        "sent_to_telegram": sent_count,
        "suppressed_or_not_sent": suppressed_count,
        "battle_ready": battle_ready_count,
        "battle_permission_events_found": battle_event_found_count,
        "by_outcome_status": status_counts(items, "outcome_status"),
    }


def build_daily_summary(flat: list[dict[str, Any]]) -> dict[str, Any]:
    summary = compute_signal_summary(flat)

    return {
        "schema_version": "1.1",
        "exporter_version": EXPORTER_VERSION,
        "updated_at_utc": utc_now_iso(),
        "source_files": {
            "signal_outcomes": str(SIGNAL_OUTCOMES_PATH),
            "telegram_alerts": str(TELEGRAM_ALERTS_PATH),
            "telegram_alerts_ndjson": str(TELEGRAM_ALERTS_NDJSON_PATH),
            "battle_permission_telemetry": str(BATTLE_PERMISSION_TELEMETRY_PATH),
        },
        "summary": summary,
        "by_symbol": grouped_metrics(flat, "symbol"),
        "by_scenario": grouped_metrics(flat, "scenario"),
        "by_direction": grouped_metrics(flat, "direction"),
        "by_execution_model": grouped_metrics(flat, "execution_model"),
        "by_signal_alignment": grouped_metrics(flat, "signal_alignment"),
        "by_stop_quality": grouped_metrics(flat, "stop_quality"),
        # Battle Permission / TPO / auction metrics.
        "by_battle_permission": grouped_metrics(flat, "battle_permission"),
        "by_telegram_delivery_mode": grouped_metrics(flat, "telegram_delivery_mode"),
        "by_battle_ready": grouped_metrics(flat, "battle_ready"),
        "by_battle_permission_source": grouped_metrics(flat, "battle_permission_source"),
        "by_battle_permission_blocker": grouped_metrics_by_list(flat, "battle_permission_blockers"),
        "by_open_relation": grouped_metrics(flat, "open_relation"),
        "by_auction_bias": grouped_metrics(flat, "auction_bias"),
        "by_tpo_signal_permission": grouped_metrics(flat, "tpo_signal_permission"),
        "by_tpo_telegram_modifier": grouped_metrics(flat, "tpo_telegram_modifier"),
        "by_market_status": grouped_metrics(flat, "market_status"),
    }


# =============================================================================
# Export entrypoint
# =============================================================================


def export_lightweight_statistics() -> dict[str, Any]:
    flat = build_flat_signals()
    summary = build_daily_summary(flat)

    write_json(SIGNALS_FLAT_JSON_PATH, flat)
    write_json(DAILY_SUMMARY_PATH, summary)

    return {
        "exporter_version": EXPORTER_VERSION,
        "updated_at_utc": utc_now_iso(),
        "signals_flat_path": str(SIGNALS_FLAT_JSON_PATH),
        "daily_summary_path": str(DAILY_SUMMARY_PATH),
        "records_count": len(flat),
        "summary": summary["summary"],
        "battle_metrics": {
            "by_battle_permission": summary.get("by_battle_permission", {}),
            "by_telegram_delivery_mode": summary.get("by_telegram_delivery_mode", {}),
            "by_battle_permission_blocker": summary.get("by_battle_permission_blocker", {}),
        },
    }


def main() -> None:
    result = export_lightweight_statistics()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()