from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.settings import settings


STATS_DIR = settings.runtime_dir / "stats"

SIGNAL_OUTCOMES_PATH = STATS_DIR / "signal_outcomes.json"
TELEGRAM_ALERTS_PATH = STATS_DIR / "telegram_alerts.json"
TELEGRAM_ALERTS_NDJSON_PATH = STATS_DIR / "telegram_alerts.ndjson"

SIGNALS_FLAT_JSON_PATH = STATS_DIR / "signals_flat.json"
DAILY_SUMMARY_PATH = STATS_DIR / "daily_summary.json"

EXPORTER_VERSION = "lightweight-statistics-exporter-v1"


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


def normalize_status(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


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


def extract_outcome_signals(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        signals = payload.get("signals", [])
        if isinstance(signals, list):
            return [x for x in signals if isinstance(x, dict)]

    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    return []


def extract_telegram_alerts_json(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        for key in ("alerts", "signals", "items", "records"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]

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
            index[str(signal_id)] = alert

        if alert_id:
            index[str(alert_id)] = alert

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


def normalize_flat_signal(item: dict[str, Any]) -> dict[str, Any]:
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

    flat = {
        "schema_version": "1.0",
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
        "updated_at_utc": utc_now_iso(),
    }

    return flat


def build_flat_signals() -> list[dict[str, Any]]:
    outcome_payload = load_json(SIGNAL_OUTCOMES_PATH, default={})
    outcome_signals = extract_outcome_signals(outcome_payload)

    alert_index = build_alert_index()

    flat: list[dict[str, Any]] = []

    for signal in outcome_signals:
        merged = merge_signal_with_alert(signal, alert_index)
        flat.append(normalize_flat_signal(merged))

    flat.sort(
        key=lambda x: (
            normalize_text(x.get("created_at_utc")),
            normalize_text(x.get("signal_id")),
        )
    )

    return flat


def status_counts(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(Counter(str(x.get(key) or "UNKNOWN") for x in items))


def grouped_metrics(items: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for item in items:
        grouped[str(item.get(key) or "UNKNOWN")].append(item)

    output: dict[str, dict[str, Any]] = {}

    for group_key, group_items in grouped.items():
        output[group_key] = compute_signal_summary(group_items)

    return output


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
        "by_outcome_status": status_counts(items, "outcome_status"),
    }


def build_daily_summary(flat: list[dict[str, Any]]) -> dict[str, Any]:
    summary = compute_signal_summary(flat)

    return {
        "schema_version": "1.0",
        "exporter_version": EXPORTER_VERSION,
        "updated_at_utc": utc_now_iso(),
        "source_files": {
            "signal_outcomes": str(SIGNAL_OUTCOMES_PATH),
            "telegram_alerts": str(TELEGRAM_ALERTS_PATH),
            "telegram_alerts_ndjson": str(TELEGRAM_ALERTS_NDJSON_PATH),
        },
        "summary": summary,
        "by_symbol": grouped_metrics(flat, "symbol"),
        "by_scenario": grouped_metrics(flat, "scenario"),
        "by_direction": grouped_metrics(flat, "direction"),
        "by_execution_model": grouped_metrics(flat, "execution_model"),
        "by_signal_alignment": grouped_metrics(flat, "signal_alignment"),
        "by_stop_quality": grouped_metrics(flat, "stop_quality"),
    }


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp_path.replace(path)


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
    }


def main() -> None:
    result = export_lightweight_statistics()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()