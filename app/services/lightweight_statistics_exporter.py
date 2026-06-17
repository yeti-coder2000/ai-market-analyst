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

EXPORTER_VERSION = "lightweight-statistics-exporter-v2.3-battle-telemetry-v32-aliases"


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
SYNTHETIC_TRACKING_SCOPE = "SYNTHETIC_TEST"

SYNTHETIC_SIGNAL_PREFIXES = ("TEST_", "SYNTHETIC_")
SYNTHETIC_CYCLE_IDS = {"SYNTHETIC_TEST", "TEST"}


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

def is_synthetic_record(record: dict[str, Any]) -> bool:
    if not isinstance(record, dict):
        return False

    if safe_bool(record.get("synthetic_test"), False):
        return True

    signal_id = normalize_text(record.get("signal_id")).upper()
    alert_id = normalize_text(record.get("alert_id")).upper()
    cycle_id = normalize_text(record.get("cycle_id")).upper()
    source = normalize_text(record.get("source")).upper()
    tracking_scope = normalize_text(record.get("tracking_scope")).upper()

    if tracking_scope == SYNTHETIC_TRACKING_SCOPE:
        return True

    if any(signal_id.startswith(prefix) for prefix in SYNTHETIC_SIGNAL_PREFIXES):
        return True

    if any(alert_id.startswith(prefix) for prefix in SYNTHETIC_SIGNAL_PREFIXES):
        return True

    if cycle_id in SYNTHETIC_CYCLE_IDS:
        return True

    if source == SYNTHETIC_TRACKING_SCOPE:
        return True

    return False


def mark_synthetic_flat(flat: dict[str, Any]) -> dict[str, Any]:
    if is_synthetic_record(flat):
        flat["tracking_scope"] = SYNTHETIC_TRACKING_SCOPE
        flat["synthetic_test"] = True
        flat["exclude_from_metrics"] = True
    else:
        flat["synthetic_test"] = safe_bool(flat.get("synthetic_test"), False)
        flat["exclude_from_metrics"] = safe_bool(flat.get("exclude_from_metrics"), False)

    return flat


def production_records(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item for item in items
        if isinstance(item, dict) and not safe_bool(item.get("exclude_from_metrics"), False)
    ]


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


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def append_candidate(candidates: list[dict[str, Any]], value: Any) -> None:
    if isinstance(value, dict) and value:
        candidates.append(value)


def collect_nested_candidates(item: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Collect likely containers where TPO / auction / Battle Gate fields can live.

    The project evolved over several stages, so older records may store fields at
    root level, while newer records may store them under metadata, context,
    filters, alert_payload, auction_context, auction_filters or battle_gate_v2.
    The exporter must be tolerant and read all known locations.
    """
    candidates: list[dict[str, Any]] = []
    append_candidate(candidates, item)

    metadata = as_dict(item.get("metadata"))
    context = as_dict(item.get("context"))
    filters = as_dict(item.get("filters"))
    alert_payload = as_dict(item.get("alert_payload"))
    payload = as_dict(item.get("payload"))

    for container in (metadata, context, filters, alert_payload, payload):
        append_candidate(candidates, container)

    for container in (metadata, context, filters, alert_payload, payload):
        for key in (
            "metadata",
            "context",
            "filters",
            "auction_context",
            "auction_filters",
            "tpo_context",
            "tpo_filters",
            "open_behavior",
            "open_behavior_context",
            "battle_gate_v2",
            "battle_gate",
            "battle_permission",
            "signal",
            "alert",
        ):
            append_candidate(candidates, container.get(key))

    # One more shallow pass catches structures such as
    # metadata.alert_payload.context or payload.metadata.auction_filters.
    shallow = list(candidates)
    for container in shallow:
        for key in (
            "metadata",
            "context",
            "filters",
            "auction_context",
            "auction_filters",
            "tpo_context",
            "tpo_filters",
            "open_behavior",
            "open_behavior_context",
            "battle_gate_v2",
            "battle_gate",
            "battle_permission",
            "signal",
            "alert",
        ):
            append_candidate(candidates, as_dict(container).get(key))

    # Keep order but remove duplicate dict identities.
    unique: list[dict[str, Any]] = []
    seen: set[int] = set()
    for candidate in candidates:
        marker = id(candidate)
        if marker not in seen:
            unique.append(candidate)
            seen.add(marker)

    return unique


def pick_from_candidates(candidates: list[dict[str, Any]], *keys: str) -> Any:
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        for key in keys:
            value = candidate.get(key)
            if value not in (None, "", [], {}):
                return value
    return None


def normalize_zone(zone: Any) -> dict[str, Any]:
    if not isinstance(zone, dict):
        return {
            "zone_type": None,
            "price": None,
            "distance": None,
            "role": None,
            "reaction": None,
            "reason": None,
        }

    return {
        "zone_type": first_non_empty(zone.get("zone_type"), zone.get("type"), zone.get("name")),
        "price": safe_float(first_non_empty(zone.get("price"), zone.get("level")), None),
        "distance": safe_float(zone.get("distance"), None),
        "role": first_non_empty(zone.get("role"), zone.get("zone_role")),
        "reaction": first_non_empty(zone.get("reaction"), zone.get("status")),
        "reason": zone.get("reason"),
    }


def extract_tpo_fields_from_item(item: dict[str, Any]) -> dict[str, Any]:
    candidates = collect_nested_candidates(item)

    primary_zone_raw = pick_from_candidates(
        candidates,
        "primary_interest_zone",
        "interest_zone",
        "zone",
        "reaction_zone",
        "nearest_npoc",
        "npoc_zone",
    )
    primary_zone = normalize_zone(primary_zone_raw)

    return {
        "open_relation": first_non_empty(
            pick_from_candidates(
                candidates,
                "open_relation",
                "tpo_open_relation",
                "open_type",
                "auction_open_relation",
            )
        ),
        "auction_bias": first_non_empty(
            pick_from_candidates(
                candidates,
                "auction_bias",
                "tpo_auction_bias",
                "auction_type",
                "open_auction_bias",
            )
        ),
        "tpo_signal_permission": first_non_empty(
            pick_from_candidates(
                candidates,
                "tpo_signal_permission",
                "signal_permission",
                "permission",
                "auction_signal_permission",
                "market_permission",
                "tpo_permission",
            )
        ),
        "tpo_telegram_modifier": first_non_empty(
            pick_from_candidates(
                candidates,
                "tpo_telegram_modifier",
                "telegram_modifier",
                "modifier",
                "auction_modifier",
                "permission_modifier",
                "tpo_modifier",
            )
        ),
        "market_is_open": first_non_empty(
            pick_from_candidates(
                candidates,
                "market_is_open",
                "is_market_open",
                "open",
            )
        ),
        "market_status": first_non_empty(
            pick_from_candidates(
                candidates,
                "market_status",
                "market",
                "market_state_status",
            )
        ),
        "open_context": first_non_empty(
            pick_from_candidates(
                candidates,
                "open_context",
                "tpo_open_context",
                "open_context_type",
            )
        ),
        "open_behavior": first_non_empty(
            pick_from_candidates(
                candidates,
                "open_behavior",
                "tpo_open_behavior",
                "open_behavior_type",
            )
        ),
        "open_behavior_confidence": safe_float(
            pick_from_candidates(
                candidates,
                "open_behavior_confidence",
                "behavior_confidence",
                "confidence_open_behavior",
            ),
            None,
        ),
        "entry_model_hint": first_non_empty(
            pick_from_candidates(
                candidates,
                "entry_model_hint",
                "entry_hint",
                "tpo_entry_model_hint",
                "entry_model",
            )
        ),
        "stop_model_hint": first_non_empty(
            pick_from_candidates(
                candidates,
                "stop_model_hint",
                "stop_hint",
                "tpo_stop_model_hint",
                "stop_model",
            )
        ),
        "battle_bias_hint": first_non_empty(
            pick_from_candidates(
                candidates,
                "battle_bias_hint",
                "battle_hint",
                "tpo_battle_bias_hint",
                "battle_mode_hint",
            )
        ),
        "primary_interest_zone": primary_zone_raw if isinstance(primary_zone_raw, dict) else None,
        "interest_zone_type": primary_zone["zone_type"],
        "interest_zone_price": primary_zone["price"],
        "interest_zone_distance": primary_zone["distance"],
        "interest_zone_role": primary_zone["role"],
        "interest_zone_reaction": primary_zone["reaction"],
        "interest_zone_reason": primary_zone["reason"],
    }


def extract_battle_gate_v2_fields_from_item(item: dict[str, Any]) -> dict[str, Any]:
    candidates = collect_nested_candidates(item)

    decision = first_non_empty(
        pick_from_candidates(
            candidates,
            "battle_gate_v2_decision",
            "battle_gate_decision",
            "battle_decision",
            "permission_decision",
            "gate_decision",
            "v2_decision",
            "decision",
        )
    )

    risk_mode = first_non_empty(
        pick_from_candidates(
            candidates,
            "battle_gate_v2_risk_mode",
            "battle_gate_risk_mode",
            "battle_risk_mode",
            "permission_risk_mode",
            "gate_risk_mode",
            "v2_risk_mode",
            "risk_mode",
        )
    )

    battle_allowed_raw = pick_from_candidates(
        candidates,
        "battle_gate_v2_battle_allowed",
        "battle_gate_v2_allowed",
        "battle_gate_allowed",
        "battle_allowed",
        "allowed_to_battle",
        "v2_battle_allowed",
        "v2_allowed",
        "allowed",
    )

    suppress_raw = pick_from_candidates(
        candidates,
        "battle_gate_v2_should_suppress_telegram",
        "battle_gate_v2_suppress",
        "battle_gate_suppress",
        "should_suppress_telegram",
        "suppress_telegram",
        "telegram_suppressed",
        "v2_should_suppress_telegram",
        "v2_suppress",
        "suppress",
    )

    reasons = normalize_list(
        first_non_empty(
            pick_from_candidates(
                candidates,
                "battle_gate_v2_reasons",
                "battle_gate_reasons",
                "battle_permission_reasons",
                "permission_reasons",
                "v2_reasons",
                "reasons",
            ),
            [],
        )
    )

    blockers = normalize_list(
        first_non_empty(
            pick_from_candidates(
                candidates,
                "battle_gate_v2_blockers",
                "battle_gate_blockers",
                "battle_permission_blockers",
                "permission_blockers",
                "v2_blockers",
                "blockers",
            ),
            [],
        )
    )

    modifiers = normalize_list(
        first_non_empty(
            pick_from_candidates(
                candidates,
                "battle_gate_v2_modifiers",
                "battle_gate_modifiers",
                "battle_permission_modifiers",
                "permission_modifiers",
                "v2_modifiers",
                "modifiers",
            ),
            [],
        )
    )

    return {
        "battle_gate_v2_decision": decision,
        "battle_gate_v2_risk_mode": risk_mode,
        "battle_gate_v2_battle_allowed": safe_optional_bool(battle_allowed_raw),
        "battle_gate_v2_should_suppress_telegram": safe_optional_bool(suppress_raw),
        "battle_gate_v2_score_delta": safe_float(
            pick_from_candidates(
                candidates,
                "battle_gate_v2_score_delta",
                "battle_gate_score_delta",
                "v2_score_delta",
                "score_delta",
            ),
            None,
        ),
        "battle_gate_v2_reasons": reasons,
        "battle_gate_v2_blockers": blockers,
        "battle_gate_v2_modifiers": modifiers,
        "battle_gate_v2_error": first_non_empty(
            pick_from_candidates(
                candidates,
                "battle_gate_v2_error",
                "battle_gate_error",
                "v2_error",
                "error",
            )
        ),
    }


def merge_prefer_primary(primary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    merged = dict(fallback)
    for key, value in primary.items():
        if value not in (None, "", [], {}):
            merged[key] = value
    return merged


def extract_battle_fields(
    item: dict[str, Any],
    battle_event: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Return battle permission fields for a flat signal.

    v2.3:
    - battle telemetry v3.2 root fields are treated as first-class inputs;
    - risk_mode / decision / suppress / blockers aliases are normalized into
      battle_gate_v2_* fields;
    - timing and market-status guard diagnostics can be copied from telemetry
      into flat records for later grouped metrics.
    """
    tpo_fields = extract_tpo_fields_from_item(item)
    v2_fields = extract_battle_gate_v2_fields_from_item(item)

    if isinstance(battle_event, dict):
        event_tpo_fields = extract_tpo_fields_from_item(battle_event)
        event_v2_fields = extract_battle_gate_v2_fields_from_item(battle_event)

        tpo_fields = merge_prefer_primary(event_tpo_fields, tpo_fields)
        v2_fields = merge_prefer_primary(event_v2_fields, v2_fields)

        blockers = normalize_list(
            first_non_empty(
                battle_event.get("battle_permission_blockers"),
                battle_event.get("battle_gate_v2_blockers"),
                battle_event.get("blockers"),
                v2_fields.get("battle_gate_v2_blockers"),
            )
        )
        reasons = normalize_list(
            first_non_empty(
                battle_event.get("battle_permission_reasons"),
                battle_event.get("battle_gate_v2_reasons"),
                battle_event.get("reasons"),
                v2_fields.get("battle_gate_v2_reasons"),
            )
        )
        modifiers = normalize_list(
            first_non_empty(
                battle_event.get("battle_permission_modifiers"),
                battle_event.get("battle_gate_v2_modifiers"),
                battle_event.get("modifiers"),
                v2_fields.get("battle_gate_v2_modifiers"),
            )
        )

        battle_allowed = safe_optional_bool(
            first_non_empty(
                battle_event.get("battle_gate_v2_battle_allowed"),
                battle_event.get("battle_gate_v2_allowed"),
                battle_event.get("battle_allowed"),
                v2_fields.get("battle_gate_v2_battle_allowed"),
            )
        )
        suppress_telegram = safe_optional_bool(
            first_non_empty(
                battle_event.get("battle_gate_v2_should_suppress_telegram"),
                battle_event.get("battle_gate_v2_suppress"),
                battle_event.get("suppress"),
                v2_fields.get("battle_gate_v2_should_suppress_telegram"),
            )
        )

        # Keep explicit v2 fields populated even if the event used shorter v3.2
        # root aliases such as "decision" and "risk_mode".
        v2_fields["battle_gate_v2_decision"] = first_non_empty(
            v2_fields.get("battle_gate_v2_decision"),
            battle_event.get("decision"),
            battle_event.get("battle_gate_v2_decision"),
        )
        v2_fields["battle_gate_v2_risk_mode"] = first_non_empty(
            v2_fields.get("battle_gate_v2_risk_mode"),
            battle_event.get("risk_mode"),
            battle_event.get("battle_gate_v2_risk_mode"),
        )
        v2_fields["battle_gate_v2_battle_allowed"] = battle_allowed
        v2_fields["battle_gate_v2_should_suppress_telegram"] = suppress_telegram
        v2_fields["battle_gate_v2_blockers"] = blockers
        v2_fields["battle_gate_v2_reasons"] = reasons
        v2_fields["battle_gate_v2_modifiers"] = modifiers

        return {
            "battle_permission": first_non_empty(
                battle_event.get("battle_permission"),
                battle_event.get("permission"),
                battle_event.get("decision"),
                v2_fields.get("battle_gate_v2_decision"),
                "UNKNOWN",
            ),
            "telegram_delivery_mode": first_non_empty(
                battle_event.get("telegram_delivery_mode"),
                battle_event.get("delivery_mode"),
                battle_event.get("risk_mode"),
                v2_fields.get("battle_gate_v2_risk_mode"),
                "UNKNOWN",
            ),
            "battle_ready": safe_optional_bool(
                first_non_empty(
                    battle_event.get("battle_ready"),
                    battle_event.get("battle_gate_v2_allowed"),
                    battle_event.get("battle_allowed"),
                    v2_fields.get("battle_gate_v2_battle_allowed"),
                )
            ),
            "sent_to_telegram": safe_optional_bool(battle_event.get("sent_to_telegram")),
            "auction_context_score": safe_float(battle_event.get("auction_context_score"), None),
            "battle_permission_blockers": blockers,
            "battle_permission_reasons": reasons,
            "battle_permission_modifiers": modifiers,
            "battle_permission_event_found": True,
            "battle_permission_event_ts_utc": battle_event.get("ts_utc"),
            "battle_permission_source": battle_event.get("source") or "battle_permission_telemetry",
            "battle_permission_event_schema_version": battle_event.get("schema_version"),
            "runner_version": first_non_empty(
                battle_event.get("runner_version"),
                battle_event.get("stateful_runner_version"),
            ),
            "market_status_override": first_non_empty(
                battle_event.get("market_status_override"),
                battle_event.get("tpo_market_status_override"),
            ),
            "original_market_status": first_non_empty(
                battle_event.get("original_market_status"),
                battle_event.get("market_status_original"),
            ),
            "original_tpo_signal_permission": first_non_empty(
                battle_event.get("original_tpo_signal_permission"),
                battle_event.get("tpo_signal_permission_original"),
            ),
            "current_price": safe_float(battle_event.get("current_price"), None),
            "entry_distance": safe_float(battle_event.get("entry_distance"), None),
            "entry_distance_R": safe_float(battle_event.get("entry_distance_R"), None),
            "already_moved_R": safe_float(battle_event.get("already_moved_R"), None),
            "entry_timing_status": battle_event.get("entry_timing_status"),
            "wait_retest_only": safe_optional_bool(battle_event.get("wait_retest_only")),
            "late_signal_reason": battle_event.get("late_signal_reason"),
            "entry_retest_required": safe_optional_bool(battle_event.get("entry_retest_required")),
            "execution_timing_guard_version": battle_event.get("execution_timing_guard_version"),
            **tpo_fields,
            **v2_fields,
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
        "battle_permission_event_schema_version": None,
        "runner_version": None,
        "market_status_override": None,
        "original_market_status": None,
        "original_tpo_signal_permission": None,
        "current_price": None,
        "entry_distance": None,
        "entry_distance_R": None,
        "already_moved_R": None,
        "entry_timing_status": None,
        "wait_retest_only": None,
        "late_signal_reason": None,
        "entry_retest_required": None,
        "execution_timing_guard_version": None,
        **tpo_fields,
        **v2_fields,
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
        "schema_version": "1.2",
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

        "current_price": first_non_empty(
            safe_float(item.get("current_price"), None),
            battle_fields.get("current_price"),
        ),
        "entry_distance": first_non_empty(
            safe_float(item.get("entry_distance"), None),
            battle_fields.get("entry_distance"),
        ),
        "entry_distance_R": first_non_empty(
            safe_float(item.get("entry_distance_R"), None),
            battle_fields.get("entry_distance_R"),
        ),
        "already_moved_R": first_non_empty(
            safe_float(item.get("already_moved_R"), None),
            battle_fields.get("already_moved_R"),
        ),
        "entry_timing_status": first_non_empty(
            item.get("entry_timing_status"),
            battle_fields.get("entry_timing_status"),
        ),
        "wait_retest_only": safe_bool(
            first_non_empty(item.get("wait_retest_only"), battle_fields.get("wait_retest_only")),
            False,
        ),
        "late_signal_reason": first_non_empty(
            item.get("late_signal_reason"),
            battle_fields.get("late_signal_reason"),
        ),
        "entry_retest_required": safe_bool(
            first_non_empty(item.get("entry_retest_required"), battle_fields.get("entry_retest_required")),
            False,
        ),
        "execution_timing_guard_version": first_non_empty(
            item.get("execution_timing_guard_version"),
            battle_fields.get("execution_timing_guard_version"),
        ),
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
        "tracking_scope": item.get("tracking_scope"),
        "synthetic_test": safe_bool(item.get("synthetic_test"), False),
        "exclude_from_metrics": safe_bool(item.get("exclude_from_metrics"), False),
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
        "battle_permission_event_schema_version": battle_fields["battle_permission_event_schema_version"],
        "runner_version": battle_fields["runner_version"],
        "market_status_override": battle_fields["market_status_override"],
        "original_market_status": battle_fields["original_market_status"],
        "original_tpo_signal_permission": battle_fields["original_tpo_signal_permission"],
        "open_relation": battle_fields["open_relation"],
        "auction_bias": battle_fields["auction_bias"],
        "tpo_signal_permission": battle_fields["tpo_signal_permission"],
        "tpo_telegram_modifier": battle_fields["tpo_telegram_modifier"],
        "market_is_open": battle_fields["market_is_open"],
        "market_status": battle_fields["market_status"],
        "open_context": battle_fields["open_context"],
        "open_behavior": battle_fields["open_behavior"],
        "open_behavior_confidence": battle_fields["open_behavior_confidence"],
        "entry_model_hint": battle_fields["entry_model_hint"],
        "stop_model_hint": battle_fields["stop_model_hint"],
        "battle_bias_hint": battle_fields["battle_bias_hint"],
        "primary_interest_zone": battle_fields["primary_interest_zone"],
        "interest_zone_type": battle_fields["interest_zone_type"],
        "interest_zone_price": battle_fields["interest_zone_price"],
        "interest_zone_distance": battle_fields["interest_zone_distance"],
        "interest_zone_role": battle_fields["interest_zone_role"],
        "interest_zone_reaction": battle_fields["interest_zone_reaction"],
        "interest_zone_reason": battle_fields["interest_zone_reason"],
        "battle_gate_v2_decision": battle_fields["battle_gate_v2_decision"],
        "battle_gate_v2_risk_mode": battle_fields["battle_gate_v2_risk_mode"],
        "battle_gate_v2_battle_allowed": battle_fields["battle_gate_v2_battle_allowed"],
        "battle_gate_v2_should_suppress_telegram": battle_fields["battle_gate_v2_should_suppress_telegram"],
        "battle_gate_v2_score_delta": battle_fields["battle_gate_v2_score_delta"],
        "battle_gate_v2_reasons": battle_fields["battle_gate_v2_reasons"],
        "battle_gate_v2_blockers": battle_fields["battle_gate_v2_blockers"],
        "battle_gate_v2_modifiers": battle_fields["battle_gate_v2_modifiers"],
        "battle_gate_v2_error": battle_fields["battle_gate_v2_error"],
        "updated_at_utc": utc_now_iso(),
    }

    mark_synthetic_flat(flat)

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
    production = production_records(flat)
    summary = compute_signal_summary(production)

    summary.update(
        {
            "total_records": len(flat),
            "production_records": len(production),
            "excluded_from_metrics": len(flat) - len(production),
            "synthetic_test_records": sum(
                1 for x in flat
                if x.get("tracking_scope") == SYNTHETIC_TRACKING_SCOPE
                or safe_bool(x.get("synthetic_test"), False)
            ),
        }
    )

    return {
        "schema_version": "1.2",
        "exporter_version": EXPORTER_VERSION,
        "updated_at_utc": utc_now_iso(),
        "source_files": {
            "signal_outcomes": str(SIGNAL_OUTCOMES_PATH),
            "telegram_alerts": str(TELEGRAM_ALERTS_PATH),
            "telegram_alerts_ndjson": str(TELEGRAM_ALERTS_NDJSON_PATH),
            "battle_permission_telemetry": str(BATTLE_PERMISSION_TELEMETRY_PATH),
        },
        "summary": summary,
        "by_symbol": grouped_metrics(production, "symbol"),
        "by_scenario": grouped_metrics(production, "scenario"),
        "by_direction": grouped_metrics(production, "direction"),
        "by_execution_model": grouped_metrics(production, "execution_model"),
        "by_signal_alignment": grouped_metrics(production, "signal_alignment"),
        "by_stop_quality": grouped_metrics(production, "stop_quality"),
        "by_tracking_scope": grouped_metrics(production, "tracking_scope"),
        "by_tracking_scope_all_records": grouped_metrics(flat, "tracking_scope"),
        # Battle Permission / TPO / auction metrics.
        "by_battle_permission": grouped_metrics(production, "battle_permission"),
        "by_telegram_delivery_mode": grouped_metrics(production, "telegram_delivery_mode"),
        "by_battle_ready": grouped_metrics(production, "battle_ready"),
        "by_battle_permission_source": grouped_metrics(production, "battle_permission_source"),
        "by_battle_permission_event_schema_version": grouped_metrics(
            production,
            "battle_permission_event_schema_version",
        ),
        "by_runner_version": grouped_metrics(production, "runner_version"),
        "by_battle_permission_blocker": grouped_metrics_by_list(production, "battle_permission_blockers"),
        "by_entry_timing_status": grouped_metrics(production, "entry_timing_status"),
        "by_wait_retest_only": grouped_metrics(production, "wait_retest_only"),
        "by_market_status_override": grouped_metrics(production, "market_status_override"),
        "by_original_market_status": grouped_metrics(production, "original_market_status"),
        "by_open_relation": grouped_metrics(production, "open_relation"),
        "by_auction_bias": grouped_metrics(production, "auction_bias"),
        "by_tpo_signal_permission": grouped_metrics(production, "tpo_signal_permission"),
        "by_tpo_telegram_modifier": grouped_metrics(production, "tpo_telegram_modifier"),
        "by_market_status": grouped_metrics(production, "market_status"),
        "by_open_context": grouped_metrics(production, "open_context"),
        "by_open_behavior": grouped_metrics(production, "open_behavior"),
        "by_entry_model_hint": grouped_metrics(production, "entry_model_hint"),
        "by_stop_model_hint": grouped_metrics(production, "stop_model_hint"),
        "by_battle_bias_hint": grouped_metrics(production, "battle_bias_hint"),
        "by_interest_zone_type": grouped_metrics(production, "interest_zone_type"),
        "by_interest_zone_role": grouped_metrics(production, "interest_zone_role"),
        "by_battle_gate_v2_decision": grouped_metrics(production, "battle_gate_v2_decision"),
        "by_battle_gate_v2_risk_mode": grouped_metrics(production, "battle_gate_v2_risk_mode"),
        "by_battle_gate_v2_battle_allowed": grouped_metrics(production, "battle_gate_v2_battle_allowed"),
        "by_battle_gate_v2_should_suppress_telegram": grouped_metrics(
            production,
            "battle_gate_v2_should_suppress_telegram",
        ),
        "by_battle_gate_v2_blocker": grouped_metrics_by_list(production, "battle_gate_v2_blockers"),
        "by_battle_gate_v2_modifier": grouped_metrics_by_list(production, "battle_gate_v2_modifiers"),
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
        "production_records": summary["summary"].get("production_records"),
        "excluded_from_metrics": summary["summary"].get("excluded_from_metrics"),
        "summary": summary["summary"],
        "battle_metrics": {
            "by_battle_permission": summary.get("by_battle_permission", {}),
            "by_telegram_delivery_mode": summary.get("by_telegram_delivery_mode", {}),
            "by_battle_permission_blocker": summary.get("by_battle_permission_blocker", {}),
            "by_battle_permission_event_schema_version": summary.get(
                "by_battle_permission_event_schema_version",
                {},
            ),
            "by_runner_version": summary.get("by_runner_version", {}),
            "by_entry_timing_status": summary.get("by_entry_timing_status", {}),
            "by_wait_retest_only": summary.get("by_wait_retest_only", {}),
            "by_market_status_override": summary.get("by_market_status_override", {}),
            "by_original_market_status": summary.get("by_original_market_status", {}),
            "by_tracking_scope": summary.get("by_tracking_scope", {}),
            "by_tracking_scope_all_records": summary.get("by_tracking_scope_all_records", {}),
            "by_open_context": summary.get("by_open_context", {}),
            "by_open_behavior": summary.get("by_open_behavior", {}),
            "by_entry_model_hint": summary.get("by_entry_model_hint", {}),
            "by_stop_model_hint": summary.get("by_stop_model_hint", {}),
            "by_battle_gate_v2_decision": summary.get("by_battle_gate_v2_decision", {}),
            "by_battle_gate_v2_risk_mode": summary.get("by_battle_gate_v2_risk_mode", {}),
            "by_battle_gate_v2_battle_allowed": summary.get("by_battle_gate_v2_battle_allowed", {}),
            "by_battle_gate_v2_blocker": summary.get("by_battle_gate_v2_blocker", {}),
        },
    }


def main() -> None:
    result = export_lightweight_statistics()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()