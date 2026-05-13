from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.settings import settings
from app.services.telegram_alert_store import (
    DEFAULT_TELEGRAM_ALERTS_JSON_PATH,
    load_telegram_alerts,
    save_telegram_alerts,
)


# =============================================================================
# SIGNAL OUTCOME TRACKER v1
# =============================================================================
# Purpose:
# - Track real Telegram ENTRY_READY signals from telegram_alerts.json.
# - Use existing radar_snapshot_v2.ndjson prices.
# - No external API calls.
# - No pandas.
# - Safe to run manually:
#
#   python -m app.services.signal_outcome_tracker
#
# v1 limitations:
# - Uses snapshot price points, not candle high/low.
# - Best for first operational tracking, not final-grade backtest.
# =============================================================================


SNAPSHOT_PATH = settings.runtime_dir / "radar_snapshot_v2.ndjson"
SIGNAL_OUTCOMES_PATH = settings.runtime_dir / "stats" / "signal_outcomes.json"

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
    None,
    "",
}

DEFAULT_EXPIRY_HOURS = 24


# =============================================================================
# BASIC HELPERS
# =============================================================================


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_utc(value: Any) -> datetime | None:
    if value is None:
        return None

    try:
        text = str(value).strip()
        if not text:
            return None
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def safe_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_direction(value: Any) -> str:
    direction = str(value or "").strip().upper()
    if direction in {"LONG", "SHORT"}:
        return direction
    return "UNKNOWN"


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def calc_risk_distance(entry: float, stop: float) -> float:
    return abs(entry - stop)


def calc_target_distance(entry: float, target: float) -> float:
    return abs(target - entry)


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

    return sent_at.replace(tzinfo=timezone.utc) if sent_at.tzinfo is None else sent_at


def is_final(alert: dict[str, Any]) -> bool:
    return str(alert.get("outcome_status") or "").upper() in FINAL_STATUSES


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
    return safe_float(
        record.get("price")
        or record.get("last_price")
        or record.get("close")
    )


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

            price = extract_snapshot_price(record)
            if price is None:
                continue

            yield {
                "symbol": symbol,
                "ts": ts,
                "price": price,
                "cycle_id": record.get("cycle_id"),
            }


# =============================================================================
# OUTCOME LOGIC
# =============================================================================


def should_trigger_entry(alert: dict[str, Any], price: float) -> bool:
    """
    Entry trigger logic v1.

    Most current Telegram ENTRY_READY signals use LIMIT_ON_RETEST.
    For limit-on-retest:
    - LONG entry triggers when price retraces down to entry or lower.
    - SHORT entry triggers when price retraces up to entry or higher.

    For other/unknown execution models, v1 still uses the same conservative
    trigger logic because alerts carry an explicit entry_reference_price.
    """
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
    """
    Detect missed move before limit entry.

    Example:
    - LONG limit entry is below current price.
    - Price goes directly to target without retracing to entry.
    - We mark it as MISSED_TARGET_BEFORE_ENTRY instead of fake TP.
    """
    return is_tp_hit(alert, price)


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

    favorable_r = round(favorable / risk, 4)
    adverse_r = round(adverse / risk, 4)

    current_mfe_r = safe_float(alert.get("mfe_R"))
    current_mae_r = safe_float(alert.get("mae_R"))

    if current_mfe_r is None or favorable_r > current_mfe_r:
        alert["mfe_R"] = favorable_r
        alert["mfe_price"] = price

    if current_mae_r is None or adverse_r > current_mae_r:
        alert["mae_R"] = adverse_r
        alert["mae_price"] = price


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
    alert["closed_at_utc"] = ts.isoformat()
    alert["expired"] = True
    alert["expired_at_utc"] = ts.isoformat()
    alert["result_R"] = 0.0
    alert["last_checked_at_utc"] = ts.isoformat()
    alert["last_price"] = price
    alert.setdefault("notes", [])
    if isinstance(alert["notes"], list):
        alert["notes"].append(
            "Target was reached before limit entry was triggered. Not counted as TP."
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
) -> bool:
    """
    Returns True if alert was changed.
    """
    if is_final(alert):
        return False

    changed = False

    valid, reason = validate_alert_for_tracking(alert)
    if not valid:
        alert["outcome_status"] = "INVALID"
        alert["outcome_error"] = reason
        alert["last_checked_at_utc"] = utc_now()
        return True

    alert["last_checked_at_utc"] = ts.isoformat()
    alert["last_price"] = price
    changed = True

    entry_triggered = bool(alert.get("entry_triggered"))

    if not entry_triggered:
        # If target is reached before limit entry, this is a missed move,
        # not a real TP.
        if is_target_reached_before_entry(alert, price):
            mark_missed_target_before_entry(alert, ts=ts, price=price)
            return True

        if should_trigger_entry(alert, price):
            mark_entry_triggered(alert, ts=ts, price=price)
            entry_triggered = True
            changed = True

    if entry_triggered:
        update_mfe_mae(alert, price)

        # Conservative order:
        # SL first avoids over-crediting ambiguous snapshot moves.
        if is_sl_hit(alert, price):
            mark_sl_hit(alert, ts=ts, price=price)
            return True

        if is_tp_hit(alert, price):
            mark_tp_hit(alert, ts=ts, price=price)
            return True

    return changed


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


def apply_expiry(alerts: list[dict[str, Any]], *, now_dt: datetime) -> int:
    changed = 0

    for alert in alerts:
        if is_final(alert):
            continue

        expiry = parse_utc(alert.get("expires_at_utc"))
        if expiry is None:
            continue

        if now_dt >= expiry:
            price = safe_float(alert.get("last_price"))
            mark_expired(alert, ts=now_dt, price=price)
            changed += 1

    return changed


def track_outcomes(
    *,
    alerts_path: Path = DEFAULT_TELEGRAM_ALERTS_JSON_PATH,
    snapshot_path: Path = SNAPSHOT_PATH,
    outcomes_path: Path = SIGNAL_OUTCOMES_PATH,
    dry_run: bool = False,
) -> dict[str, Any]:
    alerts = load_telegram_alerts(alerts_path)
    active_alerts = collect_active_alerts(alerts)

    now_dt = datetime.now(timezone.utc)

    if not active_alerts:
        summary = build_summary(alerts)
        write_outcomes_report(
            alerts=alerts,
            summary=summary,
            path=outcomes_path,
            dry_run=dry_run,
        )
        return {
            "status": "ok",
            "message": "no active alerts",
            "changed": 0,
            "summary": summary,
        }

    symbols = {normalize_symbol(x.get("symbol")) for x in active_alerts}
    symbols = {x for x in symbols if x}

    since_utc = get_tracking_start_time(active_alerts)

    changed_count = 0
    snapshot_count = 0

    for snapshot in iter_relevant_snapshots(
        snapshot_path=snapshot_path,
        symbols=symbols,
        since_utc=since_utc,
    ):
        snapshot_count += 1

        symbol = snapshot["symbol"]
        ts = snapshot["ts"]
        price = snapshot["price"]

        for alert in active_alerts:
            if is_final(alert):
                continue

            if normalize_symbol(alert.get("symbol")) != symbol:
                continue

            sent_at = parse_utc(alert.get("sent_at_utc"))
            if sent_at is not None and ts < sent_at:
                continue

            if update_single_alert_from_snapshot(alert, ts=ts, price=price):
                changed_count += 1

    changed_count += apply_expiry(alerts, now_dt=now_dt)

    summary = build_summary(alerts)

    if not dry_run:
        alerts.sort(key=lambda x: str(x.get("sent_at_utc") or ""))
        save_telegram_alerts(alerts, alerts_path)

    write_outcomes_report(
        alerts=alerts,
        summary=summary,
        path=outcomes_path,
        dry_run=dry_run,
    )

    return {
        "status": "ok",
        "dry_run": dry_run,
        "snapshot_count": snapshot_count,
        "active_alerts": len(active_alerts),
        "changed": changed_count,
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


def build_summary(alerts: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(alerts)

    tp = sum(1 for x in alerts if str(x.get("outcome_status") or "").upper() == "TP_HIT")
    sl = sum(1 for x in alerts if str(x.get("outcome_status") or "").upper() == "SL_HIT")
    expired = sum(
        1 for x in alerts
        if str(x.get("outcome_status") or "").upper() in {"EXPIRED", "EXPIRED_AFTER_ENTRY"}
    )
    pending = sum(
        1 for x in alerts
        if str(x.get("outcome_status") or "").upper() in {"PENDING_ENTRY", "ENTRY_TRIGGERED", ""}
    )

    return {
        "updated_at_utc": utc_now(),
        "total_alerts": total,
        "tp_hit": tp,
        "sl_hit": sl,
        "expired": expired,
        "pending_or_active": pending,
        "winrate": calc_winrate(alerts),
        "avg_result_R": calc_avg_result_r(alerts),
        "by_symbol": count_by(alerts, "symbol"),
        "by_scenario": count_by(alerts, "scenario"),
        "by_direction": count_by(alerts, "direction"),
        "by_signal_alignment": count_by(alerts, "signal_alignment"),
        "by_stop_quality": count_by(alerts, "stop_quality"),
        "by_outcome_status": count_by(alerts, "outcome_status"),
        "metrics_by_symbol": group_metrics(alerts, "symbol"),
        "metrics_by_scenario": group_metrics(alerts, "scenario"),
        "metrics_by_signal_alignment": group_metrics(alerts, "signal_alignment"),
        "metrics_by_stop_quality": group_metrics(alerts, "stop_quality"),
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
        "schema_version": "1.0",
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


# =============================================================================
# CLI
# =============================================================================


def main() -> None:
    dry_run = env_bool("OUTCOME_TRACKER_DRY_RUN", False)

    result = track_outcomes(dry_run=dry_run)

    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()