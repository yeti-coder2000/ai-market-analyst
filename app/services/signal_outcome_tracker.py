from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.core.settings import settings
from app.services.telegram_alert_store import (
    DEFAULT_TELEGRAM_ALERTS_JSON_PATH,
    load_telegram_alerts,
    save_telegram_alerts,
)


# =============================================================================
# SIGNAL OUTCOME TRACKER v2.1
# =============================================================================
# Purpose:
# - Track real Telegram ENTRY_READY signals from telegram_alerts.json.
# - Track blocked/suppressed executable Battle Permission events as
#   RESEARCH_COUNTERFACTUAL records.
# - Use existing radar_snapshot_v2.ndjson prices.
# - No external API calls.
# - No pandas.
# - Safe to run manually:
#
#   python -m app.services.signal_outcome_tracker
#
# v2.0 changes:
# - Reads runtime/telemetry/battle_permission_events.ndjson.
# - Converts blocked EXECUTABLE signals with entry/stop/target into research
#   outcome records.
# - Keeps telegram_alerts.json clean: only real Telegram alerts are saved there.
# - Writes both real and research records into signal_outcomes.json so
#   lightweight_statistics_exporter can calculate metrics by battle_permission.
#
# Important:
# - RESEARCH_COUNTERFACTUAL is not a real trade and not a Telegram alert.
# - It answers: "What would have happened if this blocked signal had been allowed?"
#
# v2.1 changes:
# - Marks TEST_* / SYNTHETIC_TEST records as SYNTHETIC_TEST.
# - Keeps synthetic records in signal_outcomes.json for audit.
# - Excludes synthetic records from production metrics.
#
# v2.3 changes:
# - Adds NEAR_TARGET_REACHED / target_status telemetry.
# - Keeps strict TP/SL touch rules: near TP is never counted as TP.
# - Adds safety-behavior buckets for CAUTION_BATTLE analysis.
# =============================================================================


SNAPSHOT_PATH = settings.runtime_dir / "radar_snapshot_v2.ndjson"
STATS_DIR = settings.runtime_dir / "stats"
TELEMETRY_DIR = settings.runtime_dir / "telemetry"

SIGNAL_OUTCOMES_PATH = STATS_DIR / "signal_outcomes.json"
BATTLE_PERMISSION_EVENTS_PATH = TELEMETRY_DIR / "battle_permission_events.ndjson"

FINAL_STATUSES = {
    "TP_HIT",
    "SL_HIT",
    "EXPIRED",
    "EXPIRED_AFTER_ENTRY",
    "MISSED_TARGET_BEFORE_ENTRY",
    "INVALID",
}

ACTIVE_STATUSES = {
    "PENDING_ENTRY",
    "ENTRY_TRIGGERED",
    "ACTIVE",
    "NEAR_TP",
    "NEAR_TARGET_REACHED",
    "OPEN_NEAR_TARGET",
    None,
    "",
}

DEFAULT_EXPIRY_HOURS = 24
DEFAULT_NEAR_TARGET_PROGRESS_THRESHOLD = 0.90
MIN_NEAR_TARGET_PROGRESS_THRESHOLD = 0.50
MAX_NEAR_TARGET_PROGRESS_THRESHOLD = 0.99

BATTLE_READY_PERMISSION = "BATTLE_READY"

RESEARCH_TELEGRAM_DELIVERY_MODES = {
    "RESEARCH_ALERT",
    "SUPPRESS",
}

RESEARCH_BATTLE_PERMISSION_PREFIXES = (
    "BLOCKED_BY_",
)

RESEARCH_TRACKING_SCOPE = "RESEARCH_COUNTERFACTUAL"
TELEGRAM_TRACKING_SCOPE = "TELEGRAM_ALERT"
SYNTHETIC_TRACKING_SCOPE = "SYNTHETIC_TEST"

SYNTHETIC_SIGNAL_PREFIXES = ("TEST_", "SYNTHETIC_")
SYNTHETIC_CYCLE_IDS = {"SYNTHETIC_TEST", "TEST"}

OUTCOME_TRACKER_VERSION = "signal-outcome-tracker-v2.3-near-target-strict-touch"

MATERIAL_FIELDS = (
    "outcome_status",
    "outcome_error",
    "entry_triggered",
    "entry_triggered_at_utc",
    "tp_hit",
    "tp_hit_at_utc",
    "sl_hit",
    "sl_hit_at_utc",
    "expired",
    "expired_at_utc",
    "closed_at_utc",
    "result_R",
    "mfe_R",
    "mfe_price",
    "mae_R",
    "mae_price",
    "near_target_reached",
    "near_target_reached_at_utc",
    "near_target_price",
    "near_target_progress",
    "best_progress_to_target",
    "best_progress_price",
    "target_status",
    "notes",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_utc(value: Any) -> datetime | None:
    if value is None:
        return None

    try:
        text = str(value).strip()
        if not text:
            return None

        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))

        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def safe_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_bool(value: Any, default: bool | None = None) -> bool | None:
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


def normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_direction(value: Any) -> str:
    direction = str(value or "").strip().upper()
    if direction in {"LONG", "SHORT"}:
        return direction
    return "UNKNOWN"


def normalize_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def env_float(
    name: str,
    default: float,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default

    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return default

    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def near_target_progress_threshold() -> float:
    return env_float(
        "OUTCOME_TRACKER_NEAR_TARGET_PROGRESS",
        DEFAULT_NEAR_TARGET_PROGRESS_THRESHOLD,
        min_value=MIN_NEAR_TARGET_PROGRESS_THRESHOLD,
        max_value=MAX_NEAR_TARGET_PROGRESS_THRESHOLD,
    )


def calc_risk_distance(entry: float, stop: float) -> float:
    return abs(entry - stop)


def calc_result_r(alert: dict[str, Any], price: float) -> float | None:
    direction = normalize_direction(alert.get("direction"))
    entry = safe_float(alert.get("entry_reference_price"))
    stop = safe_float(alert.get("invalidation_reference_price"))

    if entry is None or stop is None:
        return None

    risk = calc_risk_distance(entry, stop)
    if risk <= 0:
        return None

    if direction == "LONG":
        return round((price - entry) / risk, 4)

    if direction == "SHORT":
        return round((entry - price) / risk, 4)

    return None


def get_alert_expiry(alert: dict[str, Any]) -> datetime | None:
    explicit = parse_utc(alert.get("expires_at_utc"))
    if explicit is not None:
        return explicit

    sent_at = parse_utc(alert.get("sent_at_utc"))
    if sent_at is None:
        return None

    if alert.get("tracking_scope") == RESEARCH_TRACKING_SCOPE:
        return sent_at + timedelta(hours=DEFAULT_EXPIRY_HOURS)

    return None


def is_final(alert: dict[str, Any]) -> bool:
    return str(alert.get("outcome_status") or "").upper() in FINAL_STATUSES


def alert_key(alert: dict[str, Any]) -> str:
    return str(
        alert.get("alert_id")
        or alert.get("signal_id")
        or f"{alert.get('symbol')}_{alert.get('sent_at_utc')}"
        or id(alert)
    )


def stable_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except TypeError:
        return str(value)


def material_fingerprint(alert: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(
        (key, stable_json(alert.get(key)))
        for key in MATERIAL_FIELDS
    )


def has_material_change(before: tuple[tuple[str, str], ...], alert: dict[str, Any]) -> bool:
    return before != material_fingerprint(alert)


def add_note(alert: dict[str, Any], note: str) -> None:
    if not note:
        return

    notes = alert.get("notes")
    if not isinstance(notes, list):
        notes = []
        alert["notes"] = notes

    if note not in notes:
        notes.append(note)


def normalize_existing_alert_metrics(alerts: list[dict[str, Any]]) -> set[str]:
    changed_alerts: set[str] = set()

    for alert in alerts:
        if not isinstance(alert, dict):
            continue

        before = material_fingerprint(alert)

        mfe_r = safe_float(alert.get("mfe_R"))
        mae_r = safe_float(alert.get("mae_R"))

        if mfe_r is not None and mfe_r < 0:
            alert["mfe_R"] = 0.0
            alert["mfe_price"] = alert.get("entry_reference_price")
            add_note(
                alert,
                "mfe_R normalized to 0.0 because favorable excursion cannot be negative.",
            )

        if mae_r is not None and mae_r < 0:
            alert["mae_R"] = 0.0
            alert["mae_price"] = alert.get("entry_reference_price")
            add_note(
                alert,
                "mae_R normalized to 0.0 because adverse excursion cannot be negative.",
            )

        if has_material_change(before, alert):
            changed_alerts.add(alert_key(alert))

    return changed_alerts


def read_ndjson(path: Path) -> list[dict[str, Any]]:
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


def safe_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, "", {}, ()):  # noqa: PLC1901
        return []
    return [value]

def is_synthetic_record(record: dict[str, Any]) -> bool:
    """
    Detect synthetic/test records that must remain auditable but must not enter
    production statistics.
    """
    if not isinstance(record, dict):
        return False

    explicit = safe_bool(record.get("synthetic_test"), None)
    if explicit is True:
        return True

    signal_id = normalize_text(record.get("signal_id")).upper()
    alert_id = normalize_text(record.get("alert_id")).upper()
    cycle_id = normalize_text(record.get("cycle_id")).upper()
    source = normalize_text(record.get("source")).upper()

    if any(signal_id.startswith(prefix) for prefix in SYNTHETIC_SIGNAL_PREFIXES):
        return True

    if any(alert_id.startswith(prefix) for prefix in SYNTHETIC_SIGNAL_PREFIXES):
        return True

    if cycle_id in SYNTHETIC_CYCLE_IDS:
        return True

    if source == SYNTHETIC_TRACKING_SCOPE:
        return True

    return False


def apply_synthetic_flags(record: dict[str, Any]) -> dict[str, Any]:
    """
    Mark test records so they are stored for audit but excluded from production
    winrate / expectancy / Battle Gate statistics.
    """
    if not isinstance(record, dict):
        return record

    if is_synthetic_record(record):
        record["tracking_scope"] = SYNTHETIC_TRACKING_SCOPE
        record["synthetic_test"] = True
        record["exclude_from_metrics"] = True
        add_note(
            record,
            "Synthetic/test record. Kept for audit but excluded from production metrics.",
        )
    else:
        record.setdefault("exclude_from_metrics", False)

    return record


# =============================================================================
# BATTLE PERMISSION RESEARCH RECORDS
# =============================================================================


def is_research_battle_event(event: dict[str, Any]) -> bool:
    if not isinstance(event, dict):
        return False

    if event.get("event_type") != "battle_permission_evaluated":
        return False

    battle_permission = normalize_text(event.get("battle_permission")).upper()
    delivery_mode = normalize_text(event.get("telegram_delivery_mode")).upper()

    if battle_permission == BATTLE_READY_PERMISSION:
        return False

    if event.get("sent_to_telegram") is True:
        return False

    if delivery_mode not in RESEARCH_TELEGRAM_DELIVERY_MODES:
        return False

    if not battle_permission.startswith(RESEARCH_BATTLE_PERMISSION_PREFIXES):
        return False

    execution_status = normalize_text(event.get("execution_status")).upper()
    if execution_status != "EXECUTABLE":
        return False

    return True


def validate_battle_event_execution_plan(event: dict[str, Any]) -> tuple[bool, str]:
    required = [
        "signal_id",
        "symbol",
        "direction",
        "entry_reference_price",
        "invalidation_reference_price",
        "target_reference_price",
        "ts_utc",
    ]

    for key in required:
        if event.get(key) in (None, ""):
            return False, f"missing {key}"

    direction = normalize_direction(event.get("direction"))
    if direction not in {"LONG", "SHORT"}:
        return False, f"invalid direction {direction}"

    entry = safe_float(event.get("entry_reference_price"))
    stop = safe_float(event.get("invalidation_reference_price"))
    target = safe_float(event.get("target_reference_price"))

    if entry is None or stop is None or target is None:
        return False, "invalid entry/stop/target"

    if calc_risk_distance(entry, stop) <= 0:
        return False, "invalid zero risk distance"

    return True, "ok"


def research_alert_id(event: dict[str, Any]) -> str:
    signal_id = str(event.get("signal_id") or "")
    ts_raw = str(event.get("ts_utc") or "")
    ts_safe = ts_raw.replace(":", "-").replace("+", "_").replace(".", "_")
    return f"RESEARCH_{signal_id}_{ts_safe}"


def build_research_alert_from_battle_event(event: dict[str, Any]) -> dict[str, Any] | None:
    if not is_research_battle_event(event):
        return None

    valid, _reason = validate_battle_event_execution_plan(event)
    if not valid:
        return None

    sent_at = event.get("ts_utc") or utc_now()
    rr = safe_float(event.get("risk_reward_ratio"))
    practical_rr = safe_float(event.get("practical_rr"))

    alert = {
        "schema_version": "research-counterfactual-v1",
        "tracking_scope": RESEARCH_TRACKING_SCOPE,
        "source": "battle_permission_events",
        "alert_id": research_alert_id(event),
        "signal_id": event.get("signal_id"),
        "cycle_id": event.get("cycle_id"),
        "symbol": event.get("symbol"),
        "instrument": event.get("instrument") or event.get("symbol"),
        "timeframe": event.get("timeframe") or event.get("execution_timeframe") or "15m",
        "sent_at_utc": sent_at,
        "created_at_utc": sent_at,
        "alert_type": event.get("alert_type") or "RESEARCH_COUNTERFACTUAL",
        "signal_class": event.get("signal_class"),
        "status": event.get("status"),
        "scenario": event.get("scenario") or event.get("scenario_type"),
        "scenario_type": event.get("scenario_type") or event.get("scenario"),
        "direction": event.get("direction"),
        "htf_bias": event.get("htf_bias"),
        "signal_alignment": event.get("signal_alignment"),
        "market_state": event.get("market_state"),
        "confidence": safe_float(event.get("confidence")),
        "probability": safe_float(event.get("probability")),
        "execution_status": event.get("execution_status"),
        "execution_model": event.get("execution_model"),
        "execution_timeframe": event.get("execution_timeframe"),
        "trigger_reason": event.get("trigger_reason"),
        "entry_reference_price": safe_float(event.get("entry_reference_price")),
        "invalidation_reference_price": safe_float(event.get("invalidation_reference_price")),
        "target_reference_price": safe_float(event.get("target_reference_price")),
        "risk_reward_ratio": rr if rr is not None else practical_rr,
        "theoretical_rr": safe_float(event.get("theoretical_rr")),
        "practical_rr": practical_rr,
        "stop_distance": safe_float(event.get("stop_distance")),
        "target_distance": safe_float(event.get("target_distance")),
        "stop_quality": event.get("stop_quality"),
        "stop_quality_reason": event.get("stop_quality_reason"),
        "paper_mode": True,
        "battle_permission": event.get("battle_permission"),
        "telegram_delivery_mode": event.get("telegram_delivery_mode"),
        "battle_ready": safe_bool(event.get("battle_ready")),
        "sent_to_telegram": safe_bool(event.get("sent_to_telegram"), False),
        "telegram_sent": safe_bool(event.get("sent_to_telegram"), False),
        "auction_context_score": safe_float(event.get("auction_context_score")),
        "battle_permission_blockers": safe_list(event.get("battle_permission_blockers")),
        "battle_permission_reasons": safe_list(event.get("battle_permission_reasons")),
        "battle_permission_modifiers": safe_list(event.get("battle_permission_modifiers")),
        "risk_mode": event.get("risk_mode") or event.get("battle_gate_v2_risk_mode"),
        "scenario_family": event.get("scenario_family"),
        "news_risk_state": event.get("news_risk_state"),
        "news_provider_status": event.get("news_provider_status"),
        "local_structure_damaged": safe_bool(event.get("local_structure_damaged")),
        "target_quality": event.get("target_quality"),
        "caution_flags": safe_list(event.get("caution_flags")),
        "open_relation": event.get("open_relation"),
        "auction_bias": event.get("auction_bias"),
        "tpo_signal_permission": event.get("tpo_signal_permission"),
        "tpo_telegram_modifier": event.get("tpo_telegram_modifier"),
        "market_is_open": safe_bool(event.get("market_is_open")),
        "market_status": event.get("market_status"),
        "session_anchor": event.get("session_anchor"),
        "session_timezone": event.get("session_timezone"),
        "session_open_utc": event.get("session_open_utc"),
        "current_session_id": event.get("current_session_id"),
        "nearest_npoc": safe_float(event.get("nearest_npoc")),
        "nearest_npoc_distance": safe_float(event.get("nearest_npoc_distance")),
        "ib_extension_up_pct": safe_float(event.get("ib_extension_up_pct")),
        "ib_extension_down_pct": safe_float(event.get("ib_extension_down_pct")),
        "entry_triggered": False,
        "tp_hit": False,
        "sl_hit": False,
        "expired": False,
        "outcome_status": "PENDING_ENTRY",
        "result_R": None,
        "mfe_R": None,
        "mfe_price": None,
        "mae_R": None,
        "mae_price": None,
        "near_target_reached": False,
        "near_target_reached_at_utc": None,
        "near_target_price": None,
        "near_target_progress": None,
        "best_progress_to_target": None,
        "best_progress_price": None,
        "target_status": "NOT_REACHED",
        "notes": [
            "Research counterfactual generated from suppressed Battle Permission event.",
            "Not a real Telegram alert and not a real trade.",
        ],
    }

    return alert


def load_research_counterfactual_alerts(
    path: Path = BATTLE_PERMISSION_EVENTS_PATH,
) -> list[dict[str, Any]]:
    events = read_ndjson(path)

    alerts: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for event in events:
        alert = build_research_alert_from_battle_event(event)
        if alert is None:
            continue

        key = str(alert.get("alert_id") or alert.get("signal_id") or "")
        if not key or key in seen_ids:
            continue

        seen_ids.add(key)
        alerts.append(alert)

    return alerts


# =============================================================================
# SNAPSHOT READER
# =============================================================================


def extract_snapshot_symbol(record: dict[str, Any]) -> str:
    return normalize_symbol(
        record.get("symbol")
        or record.get("instrument")
        or record.get("ticker")
    )


def extract_snapshot_time(record: dict[str, Any]) -> datetime | None:
    return parse_utc(
        record.get("ts")
        or record.get("ts_utc")
        or record.get("created_at_utc")
        or record.get("time")
    )


def extract_snapshot_price(record: dict[str, Any]) -> float | None:
    close = extract_snapshot_close(record)
    if close is not None:
        return close

    return safe_float(
        record.get("price")
        or record.get("last_price")
        or record.get("close")
    )


def _nested_dicts_for_snapshot(record: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Return likely containers where OHLC/price fields may be stored.

    radar_snapshot_v2.ndjson has evolved over time. Some records are flat,
    while others may keep bar data under payload/data/ohlc-like keys. The
    tracker must be tolerant because outcome statistics should not silently
    miss TP/SL just because the snapshot schema moved one level deeper.
    """
    containers: list[dict[str, Any]] = []

    def add(value: Any) -> None:
        if isinstance(value, dict) and value not in containers:
            containers.append(value)

    add(record)

    for key in ("payload", "data", "ohlc", "bar", "latest_bar", "market_data"):
        value = record.get(key)
        add(value)

        if isinstance(value, dict):
            for nested_key in ("payload", "data", "ohlc", "bar", "latest_bar", "market_data"):
                add(value.get(nested_key))

    return containers


def _first_float_from_snapshot(record: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for container in _nested_dicts_for_snapshot(record):
        for key in keys:
            value = safe_float(container.get(key))
            if value is not None:
                return value
    return None


def extract_snapshot_open(record: dict[str, Any]) -> float | None:
    return _first_float_from_snapshot(record, ("open", "o", "bar_open", "candle_open"))


def extract_snapshot_high(record: dict[str, Any]) -> float | None:
    return _first_float_from_snapshot(record, ("high", "h", "bar_high", "candle_high"))


def extract_snapshot_low(record: dict[str, Any]) -> float | None:
    return _first_float_from_snapshot(record, ("low", "l", "bar_low", "candle_low"))


def extract_snapshot_close(record: dict[str, Any]) -> float | None:
    return _first_float_from_snapshot(
        record,
        ("close", "c", "price", "last_price", "bar_close", "candle_close"),
    )


def normalize_snapshot_ohlc(record: dict[str, Any]) -> tuple[float, float, float, float] | None:
    """
    Return (open, high, low, close) for a snapshot.

    If true OHLC fields are unavailable, fall back to the best point price.
    This keeps backward compatibility with older point-price snapshots while
    allowing v2.2 to resolve TP/SL with candle ranges when high/low are present.
    """
    close = extract_snapshot_close(record)
    point = close if close is not None else _first_float_from_snapshot(record, ("price", "last_price"))

    if point is None:
        return None

    open_ = extract_snapshot_open(record)
    high = extract_snapshot_high(record)
    low = extract_snapshot_low(record)

    values_for_range = [x for x in (open_, high, low, close, point) if x is not None]
    if not values_for_range:
        return None

    open_final = open_ if open_ is not None else point
    close_final = close if close is not None else point
    high_final = high if high is not None else max(values_for_range)
    low_final = low if low is not None else min(values_for_range)

    # Defensive repair for malformed records.
    high_final = max(high_final, open_final, close_final, point)
    low_final = min(low_final, open_final, close_final, point)

    return open_final, high_final, low_final, close_final


def iter_relevant_snapshots(
    *,
    snapshot_path: Path = SNAPSHOT_PATH,
    symbols: set[str],
    since_utc: datetime | None,
) -> Any:
    if not snapshot_path.exists():
        return

    with snapshot_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            if not isinstance(record, dict):
                continue

            symbol = extract_snapshot_symbol(record)
            if symbol not in symbols:
                continue

            ts = extract_snapshot_time(record)
            if ts is None:
                continue

            if since_utc is not None and ts < since_utc:
                continue

            ohlc = normalize_snapshot_ohlc(record)
            if ohlc is None:
                continue

            open_, high, low, close = ohlc

            yield {
                "symbol": symbol,
                "ts": ts,
                "price": close,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "cycle_id": record.get("cycle_id"),
            }


# =============================================================================
# OUTCOME LOGIC
# =============================================================================


def should_trigger_entry(alert: dict[str, Any], price: float) -> bool:
    direction = normalize_direction(alert.get("direction"))
    entry = safe_float(alert.get("entry_reference_price"))

    if entry is None:
        return False

    if direction == "LONG":
        return price <= entry

    if direction == "SHORT":
        return price >= entry

    return False


def is_tp_hit(alert: dict[str, Any], price: float) -> bool:
    direction = normalize_direction(alert.get("direction"))
    target = safe_float(alert.get("target_reference_price"))

    if target is None:
        return False

    if direction == "LONG":
        return price >= target

    if direction == "SHORT":
        return price <= target

    return False


def is_sl_hit(alert: dict[str, Any], price: float) -> bool:
    direction = normalize_direction(alert.get("direction"))
    stop = safe_float(alert.get("invalidation_reference_price"))

    if stop is None:
        return False

    if direction == "LONG":
        return price <= stop

    if direction == "SHORT":
        return price >= stop

    return False


def is_target_reached_before_entry(alert: dict[str, Any], price: float) -> bool:
    return is_tp_hit(alert, price)


def should_trigger_entry_range(alert: dict[str, Any], *, low: float, high: float) -> bool:
    direction = normalize_direction(alert.get("direction"))
    entry = safe_float(alert.get("entry_reference_price"))

    if entry is None:
        return False

    if direction == "LONG":
        return low <= entry

    if direction == "SHORT":
        return high >= entry

    return False


def is_tp_hit_range(alert: dict[str, Any], *, low: float, high: float) -> bool:
    direction = normalize_direction(alert.get("direction"))
    target = safe_float(alert.get("target_reference_price"))

    if target is None:
        return False

    if direction == "LONG":
        return high >= target

    if direction == "SHORT":
        return low <= target

    return False


def is_sl_hit_range(alert: dict[str, Any], *, low: float, high: float) -> bool:
    direction = normalize_direction(alert.get("direction"))
    stop = safe_float(alert.get("invalidation_reference_price"))

    if stop is None:
        return False

    if direction == "LONG":
        return low <= stop

    if direction == "SHORT":
        return high >= stop

    return False


def is_target_reached_before_entry_range(alert: dict[str, Any], *, low: float, high: float) -> bool:
    """
    Detect a limit-entry miss where target trades before entry is touched.

    If both entry and target are inside the same candle range, ordering is
    unknowable from OHLC alone. In that case we do NOT mark a miss; the
    existing conservative same-candle logic will handle it after entry.
    """
    if not is_tp_hit_range(alert, low=low, high=high):
        return False

    if should_trigger_entry_range(alert, low=low, high=high):
        return False

    return True


def _execution_price(alert: dict[str, Any], key: str, fallback: float) -> float:
    value = safe_float(alert.get(key))
    return value if value is not None else fallback


def update_mfe_mae_range(alert: dict[str, Any], *, low: float, high: float) -> None:
    direction = normalize_direction(alert.get("direction"))
    entry = safe_float(alert.get("entry_reference_price"))
    stop = safe_float(alert.get("invalidation_reference_price"))

    if entry is None or stop is None:
        return

    risk = calc_risk_distance(entry, stop)
    if risk <= 0:
        return

    if direction == "LONG":
        favorable = high - entry
        adverse = entry - low
        favorable_price = high
        adverse_price = low
    elif direction == "SHORT":
        favorable = entry - low
        adverse = high - entry
        favorable_price = low
        adverse_price = high
    else:
        return

    favorable = max(0.0, favorable)
    adverse = max(0.0, adverse)

    favorable_r = round(favorable / risk, 4)
    adverse_r = round(adverse / risk, 4)

    current_mfe_r = safe_float(alert.get("mfe_R"))
    current_mae_r = safe_float(alert.get("mae_R"))

    if current_mfe_r is None or favorable_r > current_mfe_r:
        alert["mfe_R"] = favorable_r
        alert["mfe_price"] = favorable_price if favorable_r > 0 else entry

    if current_mae_r is None or adverse_r > current_mae_r:
        alert["mae_R"] = adverse_r
        alert["mae_price"] = adverse_price if adverse_r > 0 else entry


def update_mfe_mae(alert: dict[str, Any], price: float) -> None:
    direction = normalize_direction(alert.get("direction"))
    entry = safe_float(alert.get("entry_reference_price"))
    stop = safe_float(alert.get("invalidation_reference_price"))

    if entry is None or stop is None:
        return

    risk = calc_risk_distance(entry, stop)
    if risk <= 0:
        return

    if direction == "LONG":
        favorable = price - entry
        adverse = entry - price
    elif direction == "SHORT":
        favorable = entry - price
        adverse = price - entry
    else:
        return

    favorable = max(0.0, favorable)
    adverse = max(0.0, adverse)

    favorable_r = round(favorable / risk, 4)
    adverse_r = round(adverse / risk, 4)

    current_mfe_r = safe_float(alert.get("mfe_R"))
    current_mae_r = safe_float(alert.get("mae_R"))

    if current_mfe_r is None or favorable_r > current_mfe_r:
        alert["mfe_R"] = favorable_r
        alert["mfe_price"] = price if favorable_r > 0 else entry

    if current_mae_r is None or adverse_r > current_mae_r:
        alert["mae_R"] = adverse_r
        alert["mae_price"] = price if adverse_r > 0 else entry


def target_progress_from_price(alert: dict[str, Any], price: float) -> float | None:
    """
    Return progress from entry toward target.

    1.0 means the exact target was touched. Values below 1.0 are not TP.
    This helper is intentionally separate from TP detection so the tracker can
    record a high-quality near-target event without over-crediting a win.
    """
    direction = normalize_direction(alert.get("direction"))
    entry = safe_float(alert.get("entry_reference_price"))
    target = safe_float(alert.get("target_reference_price"))

    if entry is None or target is None:
        return None

    if direction == "LONG":
        target_distance = target - entry
        if target_distance <= 0:
            return None
        return round((price - entry) / target_distance, 6)

    if direction == "SHORT":
        target_distance = entry - target
        if target_distance <= 0:
            return None
        return round((entry - price) / target_distance, 6)

    return None


def best_target_progress_from_range(alert: dict[str, Any], *, low: float, high: float) -> tuple[float | None, float | None]:
    direction = normalize_direction(alert.get("direction"))

    if direction == "LONG":
        price = high
    elif direction == "SHORT":
        price = low
    else:
        return None, None

    return target_progress_from_price(alert, price), price


def maybe_mark_best_target_progress(alert: dict[str, Any], *, low: float, high: float) -> None:
    progress, progress_price = best_target_progress_from_range(alert, low=low, high=high)
    if progress is None or progress_price is None:
        return

    progress = max(0.0, progress)
    current = safe_float(alert.get("best_progress_to_target"))

    if current is None or progress > current:
        alert["best_progress_to_target"] = progress
        alert["best_progress_price"] = progress_price


def mark_near_target_reached(
    alert: dict[str, Any],
    *,
    ts: datetime,
    price: float,
    progress: float,
) -> None:
    alert["near_target_reached"] = True
    alert["near_target_reached_at_utc"] = alert.get("near_target_reached_at_utc") or ts.isoformat()
    alert["near_target_price"] = price
    alert["near_target_progress"] = round(progress, 6)
    alert["target_status"] = "NEAR_TARGET_REACHED"
    alert["outcome_status"] = "NEAR_TARGET_REACHED"
    alert["last_checked_at_utc"] = ts.isoformat()
    alert["last_price"] = price
    add_note(
        alert,
        "Price reached near-target threshold but exact TP was not touched. Not counted as TP.",
    )


def maybe_mark_near_target_reached(alert: dict[str, Any], *, ts: datetime, low: float, high: float) -> None:
    if is_final(alert):
        return

    progress, progress_price = best_target_progress_from_range(alert, low=low, high=high)
    if progress is None or progress_price is None:
        return

    maybe_mark_best_target_progress(alert, low=low, high=high)

    if progress >= 1.0:
        return

    threshold = near_target_progress_threshold()
    if progress < threshold:
        return

    current_progress = safe_float(alert.get("near_target_progress"))
    if bool(alert.get("near_target_reached")) and current_progress is not None and progress <= current_progress:
        return

    mark_near_target_reached(alert, ts=ts, price=progress_price, progress=progress)


def mark_entry_triggered(alert: dict[str, Any], *, ts: datetime, price: float) -> None:
    alert["entry_triggered"] = True
    alert["entry_triggered_at_utc"] = alert.get("entry_triggered_at_utc") or ts.isoformat()
    alert["outcome_status"] = "ENTRY_TRIGGERED"
    alert["last_checked_at_utc"] = ts.isoformat()
    alert["last_price"] = price


def mark_tp_hit(alert: dict[str, Any], *, ts: datetime, price: float) -> None:
    rr = safe_float(alert.get("risk_reward_ratio"))
    result_r = rr if rr is not None else calc_result_r(alert, price)

    alert["tp_hit"] = True
    alert["tp_hit_at_utc"] = ts.isoformat()
    alert["target_status"] = "TP_HIT"
    alert["closed_at_utc"] = ts.isoformat()
    alert["outcome_status"] = "TP_HIT"
    alert["result_R"] = round(float(result_r), 4) if result_r is not None else None
    alert["last_checked_at_utc"] = ts.isoformat()
    alert["last_price"] = price


def mark_sl_hit(alert: dict[str, Any], *, ts: datetime, price: float) -> None:
    alert["sl_hit"] = True
    alert["sl_hit_at_utc"] = ts.isoformat()
    alert["closed_at_utc"] = ts.isoformat()
    alert["outcome_status"] = "SL_HIT"
    alert["result_R"] = -1.0
    alert["last_checked_at_utc"] = ts.isoformat()
    alert["last_price"] = price


def mark_expired(alert: dict[str, Any], *, ts: datetime, price: float | None = None) -> None:
    entry_triggered = bool(alert.get("entry_triggered"))

    alert["expired"] = True
    alert["expired_at_utc"] = ts.isoformat()
    alert["closed_at_utc"] = ts.isoformat()
    alert["outcome_status"] = "EXPIRED_AFTER_ENTRY" if entry_triggered else "EXPIRED"

    if entry_triggered and price is not None:
        alert["result_R"] = calc_result_r(alert, price)

    alert["last_checked_at_utc"] = ts.isoformat()
    if price is not None:
        alert["last_price"] = price


def mark_missed_target_before_entry(
    alert: dict[str, Any],
    *,
    ts: datetime,
    price: float,
) -> None:
    alert["outcome_status"] = "MISSED_TARGET_BEFORE_ENTRY"
    alert["target_status"] = "TARGET_BEFORE_ENTRY"
    alert["closed_at_utc"] = ts.isoformat()
    alert["expired"] = True
    alert["expired_at_utc"] = ts.isoformat()
    alert["result_R"] = 0.0
    alert["last_checked_at_utc"] = ts.isoformat()
    alert["last_price"] = price
    add_note(
        alert,
        "Target was reached before limit entry was triggered. Not counted as TP.",
    )


def validate_alert_for_tracking(alert: dict[str, Any]) -> tuple[bool, str]:
    required = [
        "signal_id",
        "symbol",
        "direction",
        "entry_reference_price",
        "invalidation_reference_price",
        "target_reference_price",
        "sent_at_utc",
    ]

    for key in required:
        if alert.get(key) in (None, ""):
            return False, f"missing {key}"

    direction = normalize_direction(alert.get("direction"))
    if direction not in {"LONG", "SHORT"}:
        return False, f"invalid direction {direction}"

    entry = safe_float(alert.get("entry_reference_price"))
    stop = safe_float(alert.get("invalidation_reference_price"))
    target = safe_float(alert.get("target_reference_price"))

    if entry is None or stop is None or target is None:
        return False, "invalid entry/stop/target"

    if calc_risk_distance(entry, stop) <= 0:
        return False, "invalid zero risk distance"

    return True, "ok"


def update_single_alert_from_snapshot(
    alert: dict[str, Any],
    *,
    ts: datetime,
    price: float,
    low: float | None = None,
    high: float | None = None,
) -> bool:
    """
    Update one alert from a market snapshot.

    v2.3 resolves entries, TP and SL with candle high/low when available and
    records NEAR_TARGET_REACHED without over-crediting it as TP.

    Strict touch rule:
    - LONG TP only when candle high >= target.
    - SHORT TP only when candle low <= target.

    If price reaches the configurable near-target threshold but does not touch
    target, the signal remains open/active as NEAR_TARGET_REACHED.
    """
    if is_final(alert):
        return False

    before = material_fingerprint(alert)

    valid, reason = validate_alert_for_tracking(alert)
    if not valid:
        alert["outcome_status"] = "INVALID"
        alert["outcome_error"] = reason
        alert["last_checked_at_utc"] = utc_now()
        alert["last_price"] = price
        return has_material_change(before, alert)

    low_value = low if low is not None else price
    high_value = high if high is not None else price

    # Defensive normalization in case a provider emits malformed ranges.
    high_value = max(high_value, low_value, price)
    low_value = min(low_value, high_value, price)

    alert["last_checked_at_utc"] = ts.isoformat()
    alert["last_price"] = price

    entry_triggered = bool(alert.get("entry_triggered"))
    entry_triggered_on_this_snapshot = False

    if not entry_triggered:
        if is_target_reached_before_entry_range(alert, low=low_value, high=high_value):
            target_price = _execution_price(alert, "target_reference_price", price)
            mark_missed_target_before_entry(alert, ts=ts, price=target_price)
            add_note(
                alert,
                "Outcome tracker v2.3 used OHLC range to detect target-before-entry.",
            )
            return has_material_change(before, alert)

        if should_trigger_entry_range(alert, low=low_value, high=high_value):
            entry_price = _execution_price(alert, "entry_reference_price", price)
            mark_entry_triggered(alert, ts=ts, price=entry_price)
            add_note(
                alert,
                "Outcome tracker v2.3 used OHLC range to detect entry trigger.",
            )
            entry_triggered = True
            entry_triggered_on_this_snapshot = True

    if entry_triggered:
        update_mfe_mae_range(alert, low=low_value, high=high_value)
        maybe_mark_best_target_progress(alert, low=low_value, high=high_value)

        sl_hit = is_sl_hit_range(alert, low=low_value, high=high_value)
        tp_hit = is_tp_hit_range(alert, low=low_value, high=high_value)

        # If both TP and SL are inside one candle, OHLC cannot tell ordering.
        # Keep the existing conservative principle: protect stats from
        # over-crediting ambiguous wins.
        if sl_hit:
            if entry_triggered_on_this_snapshot:
                add_note(
                    alert,
                    "Entry and SL detected on same snapshot range. Conservative SL.",
                )
            if tp_hit:
                add_note(
                    alert,
                    "TP and SL were both inside one snapshot range. Conservative SL.",
                )
            stop_price = _execution_price(alert, "invalidation_reference_price", price)
            mark_sl_hit(alert, ts=ts, price=stop_price)
            return has_material_change(before, alert)

        if tp_hit:
            if entry_triggered_on_this_snapshot:
                add_note(
                    alert,
                    "Entry and TP detected on same snapshot range. Conservative TP.",
                )
            target_price = _execution_price(alert, "target_reference_price", price)
            mark_tp_hit(alert, ts=ts, price=target_price)
            add_note(
                alert,
                "Outcome tracker v2.3 used OHLC range to detect exact TP touch.",
            )
            return has_material_change(before, alert)

        maybe_mark_near_target_reached(alert, ts=ts, low=low_value, high=high_value)

    return has_material_change(before, alert)


# =============================================================================
# MAIN TRACKING
# =============================================================================


def collect_active_alerts(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []

    for alert in alerts:
        if not isinstance(alert, dict):
            continue

        status = str(alert.get("outcome_status") or "PENDING_ENTRY").upper()

        if status in FINAL_STATUSES:
            continue

        active.append(alert)

    return active


def get_tracking_start_time(active_alerts: list[dict[str, Any]]) -> datetime | None:
    times: list[datetime] = []

    for alert in active_alerts:
        sent_at = parse_utc(alert.get("sent_at_utc"))
        if sent_at is not None:
            times.append(sent_at)

    if not times:
        return None

    return min(times)


def apply_expiry(alerts: list[dict[str, Any]], *, now_dt: datetime) -> set[str]:
    changed_alerts: set[str] = set()

    for alert in alerts:
        if not isinstance(alert, dict):
            continue

        if is_final(alert):
            continue

        expiry = get_alert_expiry(alert)
        if expiry is None:
            continue

        if now_dt >= expiry:
            before = material_fingerprint(alert)
            price = safe_float(alert.get("last_price"))
            mark_expired(alert, ts=now_dt, price=price)

            if has_material_change(before, alert):
                changed_alerts.add(alert_key(alert))

    return changed_alerts


def merge_tracking_records(
    telegram_alerts: list[dict[str, Any]],
    research_alerts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    all_records: list[dict[str, Any]] = []

    for alert in telegram_alerts:
        if isinstance(alert, dict):
            alert.setdefault("tracking_scope", TELEGRAM_TRACKING_SCOPE)
            alert.setdefault("source", alert.get("source") or "telegram_alerts")
            alert.setdefault("sent_to_telegram", True)
            apply_synthetic_flags(alert)
            all_records.append(alert)

    for alert in research_alerts:
        if isinstance(alert, dict):
            apply_synthetic_flags(alert)
            all_records.append(alert)

    return all_records


def track_outcomes(
    *,
    alerts_path: Path = DEFAULT_TELEGRAM_ALERTS_JSON_PATH,
    snapshot_path: Path = SNAPSHOT_PATH,
    outcomes_path: Path = SIGNAL_OUTCOMES_PATH,
    battle_events_path: Path = BATTLE_PERMISSION_EVENTS_PATH,
    dry_run: bool = False,
) -> dict[str, Any]:
    telegram_alerts = load_telegram_alerts(alerts_path)
    research_alerts = load_research_counterfactual_alerts(battle_events_path)

    normalization_changed_alerts = normalize_existing_alert_metrics(telegram_alerts)

    all_records = merge_tracking_records(telegram_alerts, research_alerts)
    active_alerts = collect_active_alerts(all_records)

    now_dt = datetime.now(timezone.utc)

    changed_alerts: set[str] = set(normalization_changed_alerts)
    snapshot_count = 0

    if active_alerts:
        symbols = {normalize_symbol(x.get("symbol")) for x in active_alerts}
        symbols = {x for x in symbols if x}

        since_utc = get_tracking_start_time(active_alerts)

        for snapshot in iter_relevant_snapshots(
            snapshot_path=snapshot_path,
            symbols=symbols,
            since_utc=since_utc,
        ):
            snapshot_count += 1

            symbol = snapshot["symbol"]
            ts = snapshot["ts"]
            price = snapshot["price"]
            low = snapshot.get("low")
            high = snapshot.get("high")

            for alert in active_alerts:
                if is_final(alert):
                    continue

                if normalize_symbol(alert.get("symbol")) != symbol:
                    continue

                sent_at = parse_utc(alert.get("sent_at_utc"))
                if sent_at is not None and ts < sent_at:
                    continue

                if update_single_alert_from_snapshot(
                    alert,
                    ts=ts,
                    price=price,
                    low=safe_float(low),
                    high=safe_float(high),
                ):
                    changed_alerts.add(alert_key(alert))

    expiry_changed_alerts = apply_expiry(all_records, now_dt=now_dt)
    changed_alerts.update(expiry_changed_alerts)

    summary = build_summary(all_records)

    if not dry_run:
        if normalization_changed_alerts or any(
            alert_key(x) in changed_alerts for x in telegram_alerts
        ):
            telegram_alerts.sort(key=lambda x: str(x.get("sent_at_utc") or ""))
            save_telegram_alerts(telegram_alerts, alerts_path)

    write_outcomes_report(
        alerts=all_records,
        summary=summary,
        path=outcomes_path,
        dry_run=dry_run,
    )

    return {
        "status": "ok",
        "tracker_version": OUTCOME_TRACKER_VERSION,
        "dry_run": dry_run,
        "snapshot_count": snapshot_count,
        "active_alerts": len(active_alerts),
        "telegram_alerts": len(telegram_alerts),
        "research_counterfactual_alerts": len(research_alerts),
        "changed": len(changed_alerts),
        "normalization_changes": len(normalization_changed_alerts),
        "summary": summary,
    }


# =============================================================================
# SUMMARY / REPORT
# =============================================================================


def count_by(alerts: list[dict[str, Any]], key: str) -> dict[str, int]:
    out: dict[str, int] = {}

    for alert in alerts:
        value = str(alert.get(key) or "UNKNOWN")
        out[value] = out.get(value, 0) + 1

    return dict(sorted(out.items(), key=lambda x: x[0]))


def count_by_list(alerts: list[dict[str, Any]], key: str, empty_label: str = "NONE") -> dict[str, int]:
    out: dict[str, int] = {}

    for alert in alerts:
        value = alert.get(key)

        if isinstance(value, list):
            if not value:
                out[empty_label] = out.get(empty_label, 0) + 1
            else:
                for item in value:
                    label = str(item or empty_label)
                    out[label] = out.get(label, 0) + 1
            continue

        label = str(value or empty_label)
        out[label] = out.get(label, 0) + 1

    return dict(sorted(out.items(), key=lambda x: x[0]))


def calc_winrate(alerts: list[dict[str, Any]]) -> float | None:
    closed = [
        x for x in alerts
        if str(x.get("outcome_status") or "").upper() in {"TP_HIT", "SL_HIT"}
    ]

    if not closed:
        return None

    wins = sum(1 for x in closed if str(x.get("outcome_status")).upper() == "TP_HIT")
    return round(wins / len(closed), 4)


def calc_avg_result_r(alerts: list[dict[str, Any]]) -> float | None:
    values = [
        safe_float(x.get("result_R"))
        for x in alerts
        if safe_float(x.get("result_R")) is not None
    ]

    if not values:
        return None

    return round(sum(v for v in values if v is not None) / len(values), 4)


def group_metrics(alerts: list[dict[str, Any]], group_key: str) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = {}

    for alert in alerts:
        key = str(alert.get(group_key) or "UNKNOWN")
        groups.setdefault(key, []).append(alert)

    result: dict[str, Any] = {}

    for key, items in sorted(groups.items(), key=lambda x: x[0]):
        result[key] = {
            "count": len(items),
            "outcome_status": count_by(items, "outcome_status"),
            "winrate": calc_winrate(items),
            "avg_result_R": calc_avg_result_r(items),
        }

    return result


def group_metrics_by_list(alerts: list[dict[str, Any]], group_key: str) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}

    for alert in alerts:
        value = alert.get(group_key)

        if isinstance(value, list):
            keys = [str(x) for x in value if str(x).strip()] or ["NONE"]
        else:
            keys = [str(value or "NONE")]

        for key in keys:
            grouped.setdefault(key, []).append(alert)

    result: dict[str, Any] = {}

    for key, items in sorted(grouped.items(), key=lambda x: x[0]):
        result[key] = {
            "count": len(items),
            "outcome_status": count_by(items, "outcome_status"),
            "winrate": calc_winrate(items),
            "avg_result_R": calc_avg_result_r(items),
        }

    return result


def production_records(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        alert for alert in alerts
        if isinstance(alert, dict) and not bool(alert.get("exclude_from_metrics"))
    ]


def build_summary(alerts: list[dict[str, Any]]) -> dict[str, Any]:
    all_records = [alert for alert in alerts if isinstance(alert, dict)]
    production = production_records(all_records)

    total_records = len(all_records)
    production_count = len(production)
    excluded_count = total_records - production_count
    synthetic_count = sum(
        1 for x in all_records
        if x.get("tracking_scope") == SYNTHETIC_TRACKING_SCOPE
        or bool(x.get("synthetic_test"))
    )

    tp = sum(1 for x in production if str(x.get("outcome_status") or "").upper() == "TP_HIT")
    sl = sum(1 for x in production if str(x.get("outcome_status") or "").upper() == "SL_HIT")
    expired = sum(
        1 for x in production
        if str(x.get("outcome_status") or "").upper() in {"EXPIRED", "EXPIRED_AFTER_ENTRY"}
    )
    missed = sum(
        1 for x in production
        if str(x.get("outcome_status") or "").upper() == "MISSED_TARGET_BEFORE_ENTRY"
    )
    near_target = sum(
        1 for x in production
        if str(x.get("outcome_status") or "").upper() in {"NEAR_TP", "NEAR_TARGET_REACHED", "OPEN_NEAR_TARGET"}
        or bool(x.get("near_target_reached"))
    )
    pending = sum(
        1 for x in production
        if str(x.get("outcome_status") or "").upper() in {
            "PENDING_ENTRY",
            "ENTRY_TRIGGERED",
            "ACTIVE",
            "NEAR_TP",
            "NEAR_TARGET_REACHED",
            "OPEN_NEAR_TARGET",
            "",
        }
    )
    research_count = sum(
        1 for x in production
        if x.get("tracking_scope") == RESEARCH_TRACKING_SCOPE
    )
    telegram_count = sum(
        1 for x in production
        if x.get("tracking_scope") == TELEGRAM_TRACKING_SCOPE
    )

    return {
        "updated_at_utc": utc_now(),
        # Backward-compatible production counters.
        "total_alerts": production_count,
        "telegram_alerts": telegram_count,
        "research_counterfactual_alerts": research_count,
        # Explicit all-record counters.
        "total_records": total_records,
        "production_records": production_count,
        "excluded_from_metrics": excluded_count,
        "synthetic_test_records": synthetic_count,
        "tp_hit": tp,
        "sl_hit": sl,
        "missed_before_entry": missed,
        "expired": expired,
        "near_target_reached": near_target,
        "pending_or_active": pending,
        "winrate": calc_winrate(production),
        "avg_result_R": calc_avg_result_r(production),
        "by_symbol": count_by(production, "symbol"),
        "by_scenario": count_by(production, "scenario"),
        "by_direction": count_by(production, "direction"),
        "by_signal_alignment": count_by(production, "signal_alignment"),
        "by_stop_quality": count_by(production, "stop_quality"),
        "by_outcome_status": count_by(production, "outcome_status"),
        "by_tracking_scope": count_by(production, "tracking_scope"),
        "by_tracking_scope_all_records": count_by(all_records, "tracking_scope"),
        "by_battle_permission": count_by(production, "battle_permission"),
        "by_risk_mode": count_by(production, "risk_mode"),
        "by_scenario_family": count_by(production, "scenario_family"),
        "by_news_risk_state": count_by(production, "news_risk_state"),
        "by_local_structure_damaged": count_by(production, "local_structure_damaged"),
        "by_target_quality": count_by(production, "target_quality"),
        "by_target_status": count_by(production, "target_status"),
        "by_caution_flag": count_by_list(production, "caution_flags"),
        "by_telegram_delivery_mode": count_by(production, "telegram_delivery_mode"),
        "by_battle_permission_blocker": count_by_list(production, "battle_permission_blockers"),
        "by_open_relation": count_by(production, "open_relation"),
        "by_auction_bias": count_by(production, "auction_bias"),
        "by_tpo_signal_permission": count_by(production, "tpo_signal_permission"),
        "by_tpo_telegram_modifier": count_by(production, "tpo_telegram_modifier"),
        "metrics_by_symbol": group_metrics(production, "symbol"),
        "metrics_by_scenario": group_metrics(production, "scenario"),
        "metrics_by_signal_alignment": group_metrics(production, "signal_alignment"),
        "metrics_by_stop_quality": group_metrics(production, "stop_quality"),
        "metrics_by_tracking_scope": group_metrics(production, "tracking_scope"),
        "metrics_by_tracking_scope_all_records": group_metrics(all_records, "tracking_scope"),
        "metrics_by_battle_permission": group_metrics(production, "battle_permission"),
        "metrics_by_risk_mode": group_metrics(production, "risk_mode"),
        "metrics_by_scenario_family": group_metrics(production, "scenario_family"),
        "metrics_by_news_risk_state": group_metrics(production, "news_risk_state"),
        "metrics_by_local_structure_damaged": group_metrics(production, "local_structure_damaged"),
        "metrics_by_target_quality": group_metrics(production, "target_quality"),
        "metrics_by_target_status": group_metrics(production, "target_status"),
        "metrics_by_caution_flag": group_metrics_by_list(production, "caution_flags"),
        "metrics_by_telegram_delivery_mode": group_metrics(production, "telegram_delivery_mode"),
        "metrics_by_battle_permission_blocker": group_metrics_by_list(production, "battle_permission_blockers"),
        "metrics_by_open_relation": group_metrics(production, "open_relation"),
        "metrics_by_auction_bias": group_metrics(production, "auction_bias"),
        "metrics_by_tpo_signal_permission": group_metrics(production, "tpo_signal_permission"),
        "metrics_by_tpo_telegram_modifier": group_metrics(production, "tpo_telegram_modifier"),
    }


def write_outcomes_report(
    *,
    alerts: list[dict[str, Any]],
    summary: dict[str, Any],
    path: Path = SIGNAL_OUTCOMES_PATH,
    dry_run: bool = False,
) -> None:
    if dry_run:
        return

    ensure_parent_dir(path)

    payload = {
        "schema_version": "2.0-research-counterfactual",
        "tracker_version": OUTCOME_TRACKER_VERSION,
        "updated_at_utc": utc_now(),
        "summary": summary,
        "signals": alerts,
    }

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def main() -> None:
    dry_run = env_bool("OUTCOME_TRACKER_DRY_RUN", False)

    result = track_outcomes(dry_run=dry_run)

    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
