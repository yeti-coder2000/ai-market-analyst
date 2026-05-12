from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.core.settings import settings


# =============================================================================
# TELEGRAM ALERT SNAPSHOT STORE
# =============================================================================
# Purpose:
# - Store immutable snapshots of alerts that were actually sent to Telegram.
# - This is different from signals_flat.json, which represents latest lifecycle state.
# - Outcome tracking must start from Telegram alert snapshots, not latest signal state.
#
# Output files:
# - runtime/stats/telegram_alerts.json   -> current registry / easy read
# - runtime/stats/telegram_alerts.ndjson -> append-only event stream
# =============================================================================


STATS_DIR = settings.runtime_dir / "stats"
DEFAULT_TELEGRAM_ALERTS_JSON_PATH = STATS_DIR / "telegram_alerts.json"
DEFAULT_TELEGRAM_ALERTS_NDJSON_PATH = STATS_DIR / "telegram_alerts.ndjson"

SCHEMA_VERSION = "1.0"

DEFAULT_ALERT_EXPIRY_HOURS = 24


MIN_STOP_DISTANCE_BY_SYMBOL: dict[str, float] = {
    "XAUUSD": 15.0,
    "BTCUSD": 100.0,
    "ETHUSD": 8.0,
    "EURUSD": 0.0005,
    "GBPUSD": 0.0007,
    "AUDUSD": 0.0005,
    "USDJPY": 0.08,
    "USDCHF": 0.0005,
    "USDCAD": 0.0007,
    "GER40": 25.0,
    "NAS100": 35.0,
    "SPX500": 8.0,
    "UKOIL": 0.25,
}


@dataclass
class TelegramAlertSnapshot:
    # identity
    alert_id: str
    schema_version: str
    signal_id: str
    sent_at_utc: str
    alert_type: str

    # market / signal
    symbol: str
    scenario: str
    scenario_type: str
    direction: str
    htf_bias: str
    market_state: str | None = None
    phase: str | None = None
    status: str | None = None
    signal_class: str | None = None

    # classification
    signal_alignment: str | None = None
    signal_alignment_marker: str | None = None
    signal_alignment_label: str | None = None

    # probability / quality
    confidence: float | None = None
    probability: float | None = None
    alignment_score: float | None = None
    signal_quality_decision: str | None = None
    signal_quality_score: int | None = None
    signal_quality_reason: str | None = None

    # execution
    execution_status: str | None = None
    execution_model: str | None = None
    execution_timeframe: str | None = None
    trigger_reason: str | None = None

    entry_reference_price: float | None = None
    invalidation_reference_price: float | None = None
    target_reference_price: float | None = None
    risk_reward_ratio: float | None = None
    stop_distance: float | None = None
    target_distance: float | None = None

    theoretical_rr: float | None = None
    practical_rr: float | None = None
    stop_quality: str | None = None
    stop_quality_reason: str | None = None

    # telegram delivery
    telegram_sent: bool = True
    telegram_allowed: bool | None = None
    telegram_hard_gate_allowed: bool | None = None
    telegram_hard_gate_reason: str | None = None
    telegram_title: str | None = None
    telegram_body: str | None = None
    telegram_text: str | None = None

    # context
    cycle_id: str | None = None
    batch_group: str | None = None
    paper_mode: bool | None = None

    # outcome tracking placeholders
    outcome_status: str = "PENDING_ENTRY"
    entry_triggered: bool = False
    entry_triggered_at_utc: str | None = None

    tp_hit: bool = False
    tp_hit_at_utc: str | None = None

    sl_hit: bool = False
    sl_hit_at_utc: str | None = None

    expired: bool = False
    expired_at_utc: str | None = None

    closed_at_utc: str | None = None
    result_R: float | None = None
    result_pct: float | None = None

    mfe_price: float | None = None
    mae_price: float | None = None
    mfe_R: float | None = None
    mae_R: float | None = None

    last_checked_at_utc: str | None = None
    last_price: float | None = None

    expires_at_utc: str | None = None

    # raw / diagnostics
    source: str = "telegram_alert_store"
    tags: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# =============================================================================
# BASIC HELPERS
# =============================================================================


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int | None = None) -> int | None:
    if value is None:
        return default

    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default

    text = str(value).strip()
    return text if text else default


def first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if value == "":
            continue
        return value
    return None


def normalize_probability(value: Any) -> float | None:
    probability = safe_float(value, None)

    if probability is None:
        return None

    if probability < 0:
        return None

    if probability > 1.0:
        probability = probability / 100.0

    if probability > 1.0:
        return None

    return probability


def normalize_symbol(value: Any) -> str:
    return safe_str(value, "UNKNOWN").upper()


def normalize_direction(value: Any) -> str:
    direction = safe_str(value, "NEUTRAL").upper()
    if direction in {"LONG", "SHORT", "NEUTRAL"}:
        return direction
    return "NEUTRAL"


def normalize_htf_bias(value: Any) -> str:
    htf_bias = safe_str(value, "NEUTRAL").upper()
    if htf_bias in {"LONG", "SHORT", "NEUTRAL"}:
        return htf_bias
    return "NEUTRAL"


def infer_alert_type(payload: dict[str, Any]) -> str:
    explicit = safe_str(payload.get("alert_type"), "").upper()
    if explicit:
        return explicit

    signal_class = safe_str(
        first_present(
            payload.get("signal_class"),
            payload.get("stage"),
            payload.get("current_stage"),
        ),
        "",
    ).upper()

    execution_status = safe_str(payload.get("execution_status"), "").upper()

    if signal_class == "READY":
        return "ENTRY_READY"

    if execution_status == "EXECUTABLE":
        return "ENTRY_READY"

    if signal_class == "ACTIVE":
        return "TRIGGERED"

    if signal_class == "WATCH":
        return "WATCH_NEW"

    if signal_class == "RESOLVED":
        return "INVALIDATED"

    return "UNKNOWN"


def build_alert_id(signal_id: str, alert_type: str, sent_at_utc: str) -> str:
    base = f"{signal_id}_{alert_type}".strip("_")

    if base and base != "UNKNOWN_UNKNOWN":
        return base

    safe_ts = sent_at_utc.replace(":", "-")
    return f"UNKNOWN_ALERT_{safe_ts}"


# =============================================================================
# DERIVED CLASSIFICATION
# =============================================================================


def derive_signal_alignment(direction: Any, htf_bias: Any) -> tuple[str, str, str]:
    d = normalize_direction(direction)
    h = normalize_htf_bias(htf_bias)

    if d not in {"LONG", "SHORT"}:
        return "NO_DIRECTION", "⚫", "NO DIRECTION"

    if h == "NEUTRAL":
        return "NEUTRAL_HTF", "⚪", "NEUTRAL HTF"

    if h not in {"LONG", "SHORT"}:
        return "UNKNOWN_HTF", "⚫", "UNKNOWN HTF"

    if d == h:
        return "TREND_ALIGNED", "🟢", "TREND-ALIGNED"

    return "COUNTER_TREND", "🔴", "COUNTER-TREND"


def derive_stop_quality(
    *,
    symbol: str,
    entry: float | None,
    stop: float | None,
    target: float | None,
    rr: float | None,
) -> tuple[str, str, float | None, float | None, float | None, float | None]:
    """
    Returns:
    - stop_quality
    - stop_quality_reason
    - theoretical_rr
    - practical_rr
    - stop_distance
    - target_distance
    """
    theoretical_rr = rr

    if entry is None or stop is None or target is None:
        return (
            "UNKNOWN",
            "missing entry/stop/target",
            theoretical_rr,
            None,
            None,
            None,
        )

    stop_distance = abs(entry - stop)
    target_distance = abs(target - entry)

    if stop_distance <= 0:
        return (
            "INVALID",
            "stop distance is zero or negative",
            theoretical_rr,
            None,
            stop_distance,
            target_distance,
        )

    normalized_symbol = normalize_symbol(symbol)
    min_stop = MIN_STOP_DISTANCE_BY_SYMBOL.get(normalized_symbol)

    if min_stop is None:
        return (
            "OK",
            "no instrument-specific practical stop threshold",
            theoretical_rr,
            theoretical_rr,
            stop_distance,
            target_distance,
        )

    if stop_distance < min_stop:
        practical_rr = round(target_distance / min_stop, 3) if min_stop > 0 else None
        return (
            "TIGHT_STOP",
            f"stop_distance {stop_distance:.5f} below practical_min_stop {min_stop:.5f}",
            theoretical_rr,
            practical_rr,
            stop_distance,
            target_distance,
        )

    return (
        "OK",
        f"stop_distance {stop_distance:.5f} >= practical_min_stop {min_stop:.5f}",
        theoretical_rr,
        theoretical_rr,
        stop_distance,
        target_distance,
    )


# =============================================================================
# SNAPSHOT BUILDER
# =============================================================================


def build_telegram_alert_snapshot(
    payload: dict[str, Any],
    *,
    sent_at_utc: str | None = None,
    source: str = "stateful_batch_runner",
) -> TelegramAlertSnapshot:
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dict")

    sent_at = sent_at_utc or utc_now()

    signal_id = safe_str(payload.get("signal_id"), "UNKNOWN")
    alert_type = infer_alert_type(payload)

    symbol = normalize_symbol(payload.get("symbol"))
    scenario = safe_str(
        first_present(payload.get("scenario"), payload.get("scenario_type")),
        "UNKNOWN",
    ).upper()
    scenario_type = safe_str(payload.get("scenario_type"), scenario).upper()

    direction = normalize_direction(payload.get("direction"))
    htf_bias = normalize_htf_bias(payload.get("htf_bias"))

    signal_alignment, alignment_marker, alignment_label = derive_signal_alignment(
        direction,
        htf_bias,
    )

    probability = normalize_probability(
        first_present(
            payload.get("probability"),
            payload.get("confidence"),
            payload.get("scenario_probability"),
        )
    )
    confidence = normalize_probability(
        first_present(
            payload.get("confidence"),
            payload.get("probability"),
            payload.get("scenario_probability"),
        )
    )

    entry = safe_float(
        first_present(
            payload.get("entry_reference_price"),
            payload.get("entry"),
        ),
        None,
    )
    stop = safe_float(
        first_present(
            payload.get("invalidation_reference_price"),
            payload.get("stop_loss"),
            payload.get("stop"),
        ),
        None,
    )
    target = safe_float(
        first_present(
            payload.get("target_reference_price"),
            payload.get("take_profit"),
            payload.get("target"),
        ),
        None,
    )
    rr = safe_float(
        first_present(
            payload.get("risk_reward_ratio"),
            payload.get("rr"),
            payload.get("risk_reward"),
        ),
        None,
    )

    (
        stop_quality,
        stop_quality_reason,
        theoretical_rr,
        practical_rr,
        stop_distance,
        target_distance,
    ) = derive_stop_quality(
        symbol=symbol,
        entry=entry,
        stop=stop,
        target=target,
        rr=rr,
    )

    expires_at = (
        datetime.fromisoformat(sent_at.replace("Z", "+00:00"))
        + timedelta(hours=DEFAULT_ALERT_EXPIRY_HOURS)
    ).isoformat()

    alert_id = build_alert_id(
        signal_id=signal_id,
        alert_type=alert_type,
        sent_at_utc=sent_at,
    )

    tags = list(payload.get("tags") or [])
    notes: list[str] = []

    if signal_alignment == "COUNTER_TREND":
        tags.append("counter_trend")

    if signal_alignment == "TREND_ALIGNED":
        tags.append("trend_aligned")

    if stop_quality == "TIGHT_STOP":
        tags.append("tight_stop")
        notes.append("Theoretical RR may be inflated by tight stop distance.")

    return TelegramAlertSnapshot(
        alert_id=alert_id,
        schema_version=SCHEMA_VERSION,
        signal_id=signal_id,
        sent_at_utc=sent_at,
        alert_type=alert_type,
        symbol=symbol,
        scenario=scenario,
        scenario_type=scenario_type,
        direction=direction,
        htf_bias=htf_bias,
        market_state=safe_str(payload.get("market_state"), "") or None,
        phase=safe_str(payload.get("phase"), "") or None,
        status=safe_str(payload.get("status"), "") or None,
        signal_class=safe_str(
            first_present(
                payload.get("signal_class"),
                payload.get("stage"),
                payload.get("current_stage"),
            ),
            "",
        ) or None,
        signal_alignment=signal_alignment,
        signal_alignment_marker=alignment_marker,
        signal_alignment_label=alignment_label,
        confidence=confidence,
        probability=probability,
        alignment_score=safe_float(payload.get("alignment_score"), None),
        signal_quality_decision=safe_str(payload.get("signal_quality_decision"), "") or None,
        signal_quality_score=safe_int(payload.get("signal_quality_score"), None),
        signal_quality_reason=safe_str(payload.get("signal_quality_reason"), "") or None,
        execution_status=safe_str(payload.get("execution_status"), "") or None,
        execution_model=safe_str(payload.get("execution_model"), "") or None,
        execution_timeframe=safe_str(payload.get("execution_timeframe"), "") or None,
        trigger_reason=safe_str(payload.get("trigger_reason"), "") or None,
        entry_reference_price=entry,
        invalidation_reference_price=stop,
        target_reference_price=target,
        risk_reward_ratio=rr,
        stop_distance=stop_distance,
        target_distance=target_distance,
        theoretical_rr=theoretical_rr,
        practical_rr=practical_rr,
        stop_quality=stop_quality,
        stop_quality_reason=stop_quality_reason,
        telegram_sent=True,
        telegram_allowed=payload.get("telegram_allowed"),
        telegram_hard_gate_allowed=payload.get("telegram_hard_gate_allowed"),
        telegram_hard_gate_reason=safe_str(payload.get("telegram_hard_gate_reason"), "") or None,
        telegram_title=safe_str(payload.get("telegram_title"), "") or None,
        telegram_body=safe_str(payload.get("telegram_body"), "") or None,
        telegram_text=safe_str(payload.get("telegram_text"), "") or None,
        cycle_id=safe_str(payload.get("cycle_id"), "") or None,
        batch_group=safe_str(payload.get("batch_group"), "") or None,
        paper_mode=payload.get("paper_mode"),
        outcome_status="PENDING_ENTRY",
        entry_triggered=False,
        tp_hit=False,
        sl_hit=False,
        expired=False,
        expires_at_utc=expires_at,
        source=source,
        tags=sorted(set(tags)),
        notes=notes,
    )


# =============================================================================
# STORE IO
# =============================================================================


def load_telegram_alerts(
    path: Path | str = DEFAULT_TELEGRAM_ALERTS_JSON_PATH,
) -> list[dict[str, Any]]:
    path = Path(path)

    if not path.exists():
        return []

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []

    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]

    if isinstance(raw, dict):
        alerts = raw.get("alerts")
        if isinstance(alerts, list):
            return [item for item in alerts if isinstance(item, dict)]

    return []


def save_telegram_alerts(
    alerts: list[dict[str, Any]],
    path: Path | str = DEFAULT_TELEGRAM_ALERTS_JSON_PATH,
) -> None:
    path = Path(path)
    ensure_parent_dir(path)

    tmp_path = path.with_suffix(path.suffix + ".tmp")

    payload = {
        "schema_version": SCHEMA_VERSION,
        "updated_at_utc": utc_now(),
        "count": len(alerts),
        "alerts": alerts,
    }

    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def append_telegram_alert_ndjson(
    alert: dict[str, Any],
    path: Path | str = DEFAULT_TELEGRAM_ALERTS_NDJSON_PATH,
) -> None:
    path = Path(path)
    ensure_parent_dir(path)

    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(alert, ensure_ascii=False, sort_keys=True))
        f.write("\n")
        f.flush()


def find_alert_by_id(alerts: list[dict[str, Any]], alert_id: str) -> dict[str, Any] | None:
    for item in alerts:
        if item.get("alert_id") == alert_id:
            return item
    return None


def record_telegram_alert(
    payload: dict[str, Any],
    *,
    sent_at_utc: str | None = None,
    json_path: Path | str = DEFAULT_TELEGRAM_ALERTS_JSON_PATH,
    ndjson_path: Path | str = DEFAULT_TELEGRAM_ALERTS_NDJSON_PATH,
    source: str = "stateful_batch_runner",
    allow_duplicate: bool = False,
) -> dict[str, Any]:
    """
    Record immutable Telegram alert snapshot.

    Safe behavior:
    - Builds snapshot from the exact payload that was sent to Telegram.
    - Writes to JSON registry.
    - Appends to NDJSON event stream.
    - By default does not duplicate the same alert_id.
    """
    snapshot = build_telegram_alert_snapshot(
        payload,
        sent_at_utc=sent_at_utc,
        source=source,
    )
    snapshot_dict = snapshot.to_dict()

    alerts = load_telegram_alerts(json_path)
    existing = find_alert_by_id(alerts, snapshot.alert_id)

    if existing is not None and not allow_duplicate:
        return existing

    alerts.append(snapshot_dict)
    alerts.sort(key=lambda item: str(item.get("sent_at_utc") or ""))

    save_telegram_alerts(alerts, json_path)
    append_telegram_alert_ndjson(snapshot_dict, ndjson_path)

    return snapshot_dict


# =============================================================================
# SUMMARY HELPERS
# =============================================================================


def summarize_telegram_alerts(alerts: list[dict[str, Any]]) -> dict[str, Any]:
    def count_by(key: str) -> dict[str, int]:
        out: dict[str, int] = {}
        for item in alerts:
            value = str(item.get(key) or "UNKNOWN")
            out[value] = out.get(value, 0) + 1
        return out

    return {
        "count": len(alerts),
        "by_symbol": count_by("symbol"),
        "by_alert_type": count_by("alert_type"),
        "by_scenario": count_by("scenario"),
        "by_direction": count_by("direction"),
        "by_signal_alignment": count_by("signal_alignment"),
        "by_stop_quality": count_by("stop_quality"),
        "by_outcome_status": count_by("outcome_status"),
    }


def build_telegram_alerts_summary(
    path: Path | str = DEFAULT_TELEGRAM_ALERTS_JSON_PATH,
) -> dict[str, Any]:
    alerts = load_telegram_alerts(path)
    return summarize_telegram_alerts(alerts)


# =============================================================================
# CLI
# =============================================================================


def main() -> None:
    alerts = load_telegram_alerts()
    summary = summarize_telegram_alerts(alerts)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()