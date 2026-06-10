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
# - v1.1 preserves Battle Gate / Safety context so outcome statistics can group
#   CAUTION_BATTLE, reclaim scenarios, local structure damage, news risk and
#   target quality without depending on mutable journal state.
#
# Output files:
# - runtime/stats/telegram_alerts.json   -> current registry / easy read
# - runtime/stats/telegram_alerts.ndjson -> append-only event stream
# =============================================================================


STATS_DIR = settings.runtime_dir / "stats"
DEFAULT_TELEGRAM_ALERTS_JSON_PATH = STATS_DIR / "telegram_alerts.json"
DEFAULT_TELEGRAM_ALERTS_NDJSON_PATH = STATS_DIR / "telegram_alerts.ndjson"

SCHEMA_VERSION = "1.1-safety-context"
TELEGRAM_ALERT_STORE_VERSION = "telegram-alert-store-v1.1-safety-context-pass-through"

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

    # Battle Gate / Safety context.
    battle_permission: str | None = None
    telegram_delivery_mode: str | None = None
    battle_ready: bool | None = None
    auction_context_score: float | None = None
    risk_mode: str | None = None
    scenario_family: str | None = None
    news_risk_state: str | None = None
    news_provider_status: str | None = None
    local_structure_damaged: bool | None = None
    target_quality: str | None = None
    caution_flags: list[str] = field(default_factory=list)
    battle_permission_reasons: list[str] = field(default_factory=list)
    battle_permission_blockers: list[str] = field(default_factory=list)
    battle_permission_modifiers: list[str] = field(default_factory=list)

    # TPO / open-behavior context.
    market_is_open: bool | None = None
    market_status: str | None = None
    tpo_signal_permission: str | None = None
    tpo_telegram_modifier: str | None = None
    open_relation: str | None = None
    auction_bias: str | None = None
    open_context: str | None = None
    open_behavior: str | None = None
    open_behavior_confidence: float | None = None
    entry_model_hint: str | None = None
    stop_model_hint: str | None = None
    battle_bias_hint: str | None = None
    primary_interest_zone: dict[str, Any] | None = None
    interest_zone_type: str | None = None
    interest_zone_price: float | None = None
    interest_zone_role: str | None = None

    # context
    cycle_id: str | None = None
    batch_group: str | None = None
    paper_mode: bool | None = None

    # outcome tracking placeholders
    outcome_status: str = "PENDING_ENTRY"
    target_status: str | None = None
    near_target_reached: bool = False
    near_target_progress: float | None = None
    best_progress_to_target: float | None = None
    best_progress_price: float | None = None
    best_progress_at_utc: str | None = None

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
    store_version: str = TELEGRAM_ALERT_STORE_VERSION
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
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
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


def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default

    text = str(value).strip()
    return text if text else default


def safe_list_str(value: Any) -> list[str]:
    if value in (None, "", [], {}, ()):  # noqa: PLC1901
        return []

    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]

    if isinstance(value, (tuple, set)):
        return [str(item) for item in value if str(item).strip()]

    return [str(value)]


def safe_dict(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, dict) and value else None


def first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if value == "":
            continue
        if value == []:
            continue
        if value == {}:
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


def upper_or_none(value: Any) -> str | None:
    text = safe_str(value, "")
    return text.upper() if text else None


def _metadata(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = payload.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _deep_get(data: Any, path: str) -> Any:
    current = data
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def context_value(payload: dict[str, Any], *keys: str) -> Any:
    """
    Read the first non-empty value from root payload, metadata and known nested
    context containers. The notifier/battle gate can attach fields at different
    depths depending on the execution path; this keeps the alert snapshot stable.
    """
    metadata = _metadata(payload)

    containers = [
        payload,
        metadata,
        payload.get("auction_context") if isinstance(payload.get("auction_context"), dict) else None,
        payload.get("auction_filters") if isinstance(payload.get("auction_filters"), dict) else None,
        payload.get("context") if isinstance(payload.get("context"), dict) else None,
        payload.get("open_behavior") if isinstance(payload.get("open_behavior"), dict) else None,
        metadata.get("auction_context") if isinstance(metadata.get("auction_context"), dict) else None,
        metadata.get("auction_filters") if isinstance(metadata.get("auction_filters"), dict) else None,
        metadata.get("context") if isinstance(metadata.get("context"), dict) else None,
        metadata.get("open_behavior") if isinstance(metadata.get("open_behavior"), dict) else None,
    ]

    for key in keys:
        for container in containers:
            if not isinstance(container, dict):
                continue
            value = container.get(key)
            if value not in (None, "", [], {}):
                return value

        # Dotted path fallback, useful for metadata.primary_interest_zone.price.
        for root in (payload, metadata):
            value = _deep_get(root, key)
            if value not in (None, "", [], {}):
                return value

    return None


# =============================================================================
# DERIVED CLASSIFICATION
# =============================================================================


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


def derive_scenario_family(
    *,
    explicit: Any,
    scenario: str,
    entry_model_hint: str | None,
    local_structure_damaged: bool | None,
    news_risk_state: str | None,
) -> str | None:
    explicit_text = upper_or_none(explicit)
    if explicit_text:
        return explicit_text

    scenario_text = safe_str(scenario, "").upper()
    entry_model_text = safe_str(entry_model_hint, "").upper()
    news_text = safe_str(news_risk_state, "").upper()

    if "POST_NEWS_RECLAIM" in scenario_text:
        return "POST_NEWS_RECLAIM"

    if "POST_LIQUIDATION_RECLAIM" in scenario_text:
        return "POST_LIQUIDATION_RECLAIM"

    reclaim_like = (
        "FAILED_ACCEPTANCE" in entry_model_text
        or "RECLAIM" in scenario_text
        or "RECLAIM" in entry_model_text
    )

    if local_structure_damaged is True and reclaim_like:
        if news_text in {"PROVIDER_UNAVAILABLE", "HIGH_IMPACT", "POST_NEWS_CAUTION"}:
            return "POST_NEWS_RECLAIM"
        return "POST_LIQUIDATION_RECLAIM"

    if "OPEN_TEST_DRIVE" in scenario_text:
        return "TPO_OPEN_TEST_DRIVE"

    if "SWEEP_RETURN" in scenario_text:
        return "SWEEP_RETURN"

    if "TREND_CONTINUATION" in scenario_text:
        return "TREND_CONTINUATION"

    return None


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

    sent_dt = parse_utc(sent_at) or datetime.now(timezone.utc)
    expires_at = (sent_dt + timedelta(hours=DEFAULT_ALERT_EXPIRY_HOURS)).isoformat()

    alert_id = build_alert_id(
        signal_id=signal_id,
        alert_type=alert_type,
        sent_at_utc=sent_at,
    )

    # Battle Gate / Safety context. These fields are usually added by
    # apply_battle_permission() before the alert is formatted and recorded.
    battle_permission = upper_or_none(context_value(payload, "battle_permission"))
    telegram_delivery_mode = upper_or_none(context_value(payload, "telegram_delivery_mode"))
    battle_ready = safe_bool(context_value(payload, "battle_ready"), None)
    auction_context_score = safe_float(context_value(payload, "auction_context_score"), None)

    risk_mode = upper_or_none(
        context_value(
            payload,
            "risk_mode",
            "battle_gate_v2_risk_mode",
            "metadata.battle_gate_v2_risk_mode",
        )
    )
    if risk_mode is None and battle_permission == "CAUTION_BATTLE":
        risk_mode = "CAUTION"

    news_risk_state = upper_or_none(context_value(payload, "news_risk_state", "news_state"))
    news_provider_status = upper_or_none(context_value(payload, "news_provider_status", "calendar_provider_status"))
    local_structure_damaged = safe_bool(
        context_value(payload, "local_structure_damaged", "structure_damaged"),
        None,
    )
    target_quality = upper_or_none(context_value(payload, "target_quality")) or "UNKNOWN"

    caution_flags = sorted(set(safe_list_str(context_value(payload, "caution_flags", "safety_flags"))))
    battle_permission_reasons = safe_list_str(context_value(payload, "battle_permission_reasons"))
    battle_permission_blockers = safe_list_str(context_value(payload, "battle_permission_blockers"))
    battle_permission_modifiers = safe_list_str(context_value(payload, "battle_permission_modifiers"))

    market_is_open = safe_bool(context_value(payload, "market_is_open"), None)
    market_status = upper_or_none(context_value(payload, "market_status"))
    tpo_signal_permission = upper_or_none(context_value(payload, "tpo_signal_permission", "signal_permission"))
    tpo_telegram_modifier = upper_or_none(context_value(payload, "tpo_telegram_modifier", "telegram_modifier"))
    open_relation = upper_or_none(context_value(payload, "open_relation"))
    auction_bias = upper_or_none(context_value(payload, "auction_bias"))
    open_context = upper_or_none(context_value(payload, "open_context"))
    open_behavior = upper_or_none(context_value(payload, "open_behavior"))
    open_behavior_confidence = safe_float(context_value(payload, "open_behavior_confidence", "confidence"), None)
    entry_model_hint = upper_or_none(context_value(payload, "entry_model_hint"))
    stop_model_hint = upper_or_none(context_value(payload, "stop_model_hint"))
    battle_bias_hint = upper_or_none(context_value(payload, "battle_bias_hint"))
    primary_interest_zone = safe_dict(context_value(payload, "primary_interest_zone", "interest_zone"))
    interest_zone_type = upper_or_none(
        context_value(
            payload,
            "interest_zone_type",
            "primary_interest_zone.zone_type",
            "metadata.primary_interest_zone.zone_type",
        )
    )
    interest_zone_price = safe_float(
        context_value(
            payload,
            "interest_zone_price",
            "primary_interest_zone.price",
            "metadata.primary_interest_zone.price",
        ),
        None,
    )
    interest_zone_role = upper_or_none(
        context_value(
            payload,
            "interest_zone_role",
            "primary_interest_zone.role",
            "metadata.primary_interest_zone.role",
        )
    )

    scenario_family = derive_scenario_family(
        explicit=context_value(payload, "scenario_family"),
        scenario=scenario,
        entry_model_hint=entry_model_hint,
        local_structure_damaged=local_structure_damaged,
        news_risk_state=news_risk_state,
    )

    tags = safe_list_str(payload.get("tags"))
    notes: list[str] = []

    if signal_alignment == "COUNTER_TREND":
        tags.append("counter_trend")

    if signal_alignment == "TREND_ALIGNED":
        tags.append("trend_aligned")

    if stop_quality == "TIGHT_STOP":
        tags.append("tight_stop")
        notes.append("Theoretical RR may be inflated by tight stop distance.")

    if battle_permission:
        tags.append(f"battle_permission:{battle_permission.lower()}")

    if battle_permission == "CAUTION_BATTLE":
        tags.append("caution_battle")
        notes.append("Battle Gate allowed Telegram delivery with caution; not a clean battle signal.")

    if risk_mode:
        tags.append(f"risk_mode:{risk_mode.lower()}")

    if scenario_family:
        tags.append(f"scenario_family:{scenario_family.lower()}")

    if local_structure_damaged is True:
        tags.append("local_structure_damaged")

    if news_risk_state:
        tags.append(f"news_risk:{news_risk_state.lower()}")

    if news_risk_state == "PROVIDER_UNAVAILABLE":
        tags.append("news_provider_unavailable")
        notes.append("Economic calendar/news provider was unavailable at signal time.")

    if target_quality:
        tags.append(f"target_quality:{target_quality.lower()}")

    if target_quality == "SYNTHETIC":
        tags.append("synthetic_target")
        notes.append("Target was marked synthetic at signal time.")

    if target_quality == "UNKNOWN":
        tags.append("target_quality_unknown")

    for flag in caution_flags:
        normalized_flag = safe_str(flag, "").lower()
        if normalized_flag:
            tags.append(f"flag:{normalized_flag}")

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
        battle_permission=battle_permission,
        telegram_delivery_mode=telegram_delivery_mode,
        battle_ready=battle_ready,
        auction_context_score=auction_context_score,
        risk_mode=risk_mode,
        scenario_family=scenario_family,
        news_risk_state=news_risk_state,
        news_provider_status=news_provider_status,
        local_structure_damaged=local_structure_damaged,
        target_quality=target_quality,
        caution_flags=caution_flags,
        battle_permission_reasons=battle_permission_reasons,
        battle_permission_blockers=battle_permission_blockers,
        battle_permission_modifiers=battle_permission_modifiers,
        market_is_open=market_is_open,
        market_status=market_status,
        tpo_signal_permission=tpo_signal_permission,
        tpo_telegram_modifier=tpo_telegram_modifier,
        open_relation=open_relation,
        auction_bias=auction_bias,
        open_context=open_context,
        open_behavior=open_behavior,
        open_behavior_confidence=open_behavior_confidence,
        entry_model_hint=entry_model_hint,
        stop_model_hint=stop_model_hint,
        battle_bias_hint=battle_bias_hint,
        primary_interest_zone=primary_interest_zone,
        interest_zone_type=interest_zone_type,
        interest_zone_price=interest_zone_price,
        interest_zone_role=interest_zone_role,
        cycle_id=safe_str(payload.get("cycle_id"), "") or None,
        batch_group=safe_str(payload.get("batch_group"), "") or None,
        paper_mode=payload.get("paper_mode"),
        outcome_status="PENDING_ENTRY",
        target_status=None,
        near_target_reached=False,
        entry_triggered=False,
        tp_hit=False,
        sl_hit=False,
        expired=False,
        expires_at_utc=expires_at,
        source=source,
        store_version=TELEGRAM_ALERT_STORE_VERSION,
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
        "store_version": TELEGRAM_ALERT_STORE_VERSION,
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
    - Preserves Battle Gate / Safety context fields from the sent payload.
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
        return dict(sorted(out.items(), key=lambda x: x[0]))

    def count_by_list(key: str, empty_label: str = "NONE") -> dict[str, int]:
        out: dict[str, int] = {}
        for item in alerts:
            value = item.get(key)
            if isinstance(value, list):
                if not value:
                    out[empty_label] = out.get(empty_label, 0) + 1
                else:
                    for part in value:
                        label = str(part or empty_label)
                        out[label] = out.get(label, 0) + 1
                continue

            label = str(value or empty_label)
            out[label] = out.get(label, 0) + 1
        return dict(sorted(out.items(), key=lambda x: x[0]))

    return {
        "count": len(alerts),
        "store_version": TELEGRAM_ALERT_STORE_VERSION,
        "by_symbol": count_by("symbol"),
        "by_alert_type": count_by("alert_type"),
        "by_scenario": count_by("scenario"),
        "by_scenario_family": count_by("scenario_family"),
        "by_direction": count_by("direction"),
        "by_signal_alignment": count_by("signal_alignment"),
        "by_stop_quality": count_by("stop_quality"),
        "by_outcome_status": count_by("outcome_status"),
        "by_target_status": count_by("target_status"),
        "by_battle_permission": count_by("battle_permission"),
        "by_telegram_delivery_mode": count_by("telegram_delivery_mode"),
        "by_risk_mode": count_by("risk_mode"),
        "by_news_risk_state": count_by("news_risk_state"),
        "by_news_provider_status": count_by("news_provider_status"),
        "by_local_structure_damaged": count_by("local_structure_damaged"),
        "by_target_quality": count_by("target_quality"),
        "by_caution_flag": count_by_list("caution_flags"),
        "by_open_relation": count_by("open_relation"),
        "by_auction_bias": count_by("auction_bias"),
        "by_open_behavior": count_by("open_behavior"),
        "by_tpo_signal_permission": count_by("tpo_signal_permission"),
        "by_tpo_telegram_modifier": count_by("tpo_telegram_modifier"),
        "by_tag": count_by_list("tags"),
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
