from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from app.core.settings import settings


# =============================================================================
# PERSISTENT RUNTIME PATHS
# =============================================================================
# All statistics must follow the active runtime directory from settings.
# On Render production this should be /var/data/runtime, not project-local runtime/.

DEFAULT_JOURNAL_PATH = settings.radar_journal_path
DEFAULT_STATS_DIR = settings.runtime_dir / "stats"
DEFAULT_SIGNAL_RECORDS_JSON_PATH = DEFAULT_STATS_DIR / "signals_flat.json"
DEFAULT_SIGNAL_RECORDS_PARQUET_PATH = DEFAULT_STATS_DIR / "signals_flat.parquet"
DEFAULT_DAILY_SUMMARY_PATH = DEFAULT_STATS_DIR / "daily_summary.json"


KNOWN_INSTRUMENTS = {
    # core
    "XAUUSD",
    "EURUSD",
    "GBPUSD",
    "BTCUSD",
    "ETHUSD",

    # fx_major
    "USDJPY",
    "USDCHF",
    "USDCAD",
    "AUDUSD",

    # optional future fx reserve
    "NZDUSD",
    "EURJPY",
    "GBPJPY",
    "AUDJPY",

    # indices / commodities reserve
    "UKOIL",
    "GER40",
    "NAS100",
    "SPX500",

    # optional / reserve
    "DXY",
}

SYMBOL_ALIASES = {
    "GOLD": "XAUUSD",
    "XAU": "XAUUSD",
    "XAU/USD": "XAUUSD",
    "BTC/USD": "BTCUSD",
    "ETH/USD": "ETHUSD",
    "EUR/USD": "EURUSD",
    "GBP/USD": "GBPUSD",
    "USD/JPY": "USDJPY",
    "USD/CHF": "USDCHF",
    "USD/CAD": "USDCAD",
    "AUD/USD": "AUDUSD",
    "NZD/USD": "NZDUSD",
    "EUR/JPY": "EURJPY",
    "GBP/JPY": "GBPJPY",
    "AUD/JPY": "AUDJPY",
    "DAX": "GER40",
    "DE40": "GER40",
    "NDQ": "NAS100",
    "NDX": "NAS100",
    "NASDAQ": "NAS100",
    "SPX": "SPX500",
    "SP500": "SPX500",
    "SNP500": "SPX500",
    "S&P500": "SPX500",
    "BRENT": "UKOIL",
}

VALID_HTF_BIAS_VALUES = {"LONG", "SHORT", "NEUTRAL"}


@dataclass
class SignalRecord:
    signal_id: str
    symbol: str
    timeframe: str
    cycle_id: str
    created_at_utc: str

    scenario: str
    signal_class: str
    direction: str
    market_state: str
    htf_bias: str
    confidence: float

    entry_reference_price: Optional[float]
    invalidation_reference_price: Optional[float]
    target_reference_price: Optional[float]

    # Diagnostic / transparency fields.
    # These do not change signal logic. They only explain why a signal is or is not executable.
    phase: Optional[str] = None
    status: Optional[str] = None
    setup_type: Optional[str] = None
    setup_name: Optional[str] = None
    dominant_setup: Optional[str] = None
    alignment_score: Optional[float] = None

    next_expected_event: Optional[str] = None
    missing_conditions: list[str] | None = None
    reason: Optional[str] = None
    rationale: Optional[str] = None

    execution_quality_reason: Optional[str] = None
    promotion_blocker: Optional[str] = None

    signal_alignment: Optional[str] = None
    signal_alignment_marker: Optional[str] = None
    signal_alignment_label: Optional[str] = None

    stop_quality: Optional[str] = None
    stop_quality_reason: Optional[str] = None
    theoretical_rr: Optional[float] = None
    practical_rr: Optional[float] = None

    execution_status: Optional[str] = None
    execution_model: Optional[str] = None
    risk_reward_ratio: Optional[float] = None
    stop_distance: Optional[float] = None
    target_distance: Optional[float] = None
    execution_timeframe: Optional[str] = None
    trigger_reason: Optional[str] = None

    was_sent_to_telegram: bool = False
    was_deduped: bool = False

    current_stage: str = "SCENARIO_FORMING"
    final_status: Optional[str] = None
    resolution_reason: Optional[str] = None

    bars_alive: int = 0
    minutes_alive: int = 0
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    time_to_validation_min: Optional[int] = None

    runner_version: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_list_str(value: Any) -> list[str]:
    if value is None:
        return []

    if isinstance(value, list):
        return [str(x) for x in value if x is not None]

    if isinstance(value, (tuple, set)):
        return [str(x) for x in value if x is not None]

    return [str(value)]


def _metadata_dict(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    metadata = payload.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _execution_payload_dict(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    execution = payload.get("execution")
    return execution if isinstance(execution, dict) else {}


def _clean_text(value: Any) -> str | None:
    if value in [None, "", [], {}]:
        return None
    return str(value)


def _execution_quality_reason(payload: dict[str, Any] | None) -> str | None:
    payload = payload or {}
    metadata = _metadata_dict(payload)
    execution = _execution_payload_dict(payload)

    value = (
        payload.get("execution_quality_reason")
        or metadata.get("execution_quality_reason")
        or metadata.get("promotion_blocker")
        or execution.get("execution_quality_reason")
    )
    return _clean_text(value)


def _infer_promotion_blocker(payload: dict[str, Any] | None) -> str:
    """
    Human-readable reason why a signal did not become executable.

    This is intentionally diagnostic-only. It does not change signal state,
    Telegram behavior, risk model, or scenario promotion rules.
    """
    payload = payload or {}
    metadata = _metadata_dict(payload)
    execution = _execution_payload_dict(payload)

    execution_status = (
        payload.get("execution_status")
        or execution.get("status")
        or metadata.get("execution_status")
    )
    execution_status_text = str(execution_status or "").upper()

    if execution_status_text == "EXECUTABLE":
        return "EXECUTABLE"

    trigger_reason = (
        payload.get("trigger_reason")
        or execution.get("trigger_reason")
        or metadata.get("execution_quality_reason")
        or metadata.get("promotion_blocker")
    )
    if trigger_reason:
        return str(trigger_reason)

    missing_conditions = _safe_list_str(payload.get("missing_conditions"))
    if missing_conditions:
        return "missing:" + ",".join(missing_conditions)

    next_expected_event = payload.get("next_expected_event")
    if next_expected_event:
        return "waiting_for:" + str(next_expected_event)

    signal_class = str(payload.get("signal_class") or "").upper()
    status = str(payload.get("status") or "").upper()
    direction = str(payload.get("direction") or "").upper()
    htf_bias = str(payload.get("htf_bias") or metadata.get("htf_bias") or "").upper()
    scenario = str(payload.get("scenario") or payload.get("scenario_type") or "").upper()

    if scenario == "NO_ACTION":
        return "no_dominant_scenario"

    if direction == "NEUTRAL":
        return "neutral_direction"

    if htf_bias == "NEUTRAL":
        return "neutral_htf_bias"

    if status in {"NO_SETUP", "IDLE", "EDGE_FORMING"}:
        return "setup_not_ready"

    if signal_class in {"SCENARIO_FORMING", "WATCH"}:
        return "pre_ready_state"

    return "not_executable_unknown_reason"



# =============================================================================
# DERIVED SIGNAL QUALITY FIELDS: ALIGNMENT + PRACTICAL STOP QUALITY
# =============================================================================

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


def _derive_signal_alignment(direction: Any, htf_bias: Any) -> tuple[str, str, str]:
    d = str(direction or "").strip().upper()
    h = str(htf_bias or "").strip().upper()

    if d not in {"LONG", "SHORT"}:
        return "NO_DIRECTION", "⚫", "NO DIRECTION"

    if h == "NEUTRAL":
        return "NEUTRAL_HTF", "⚪", "NEUTRAL HTF"

    if h not in {"LONG", "SHORT"}:
        return "UNKNOWN_HTF", "⚫", "UNKNOWN HTF"

    if d == h:
        return "TREND_ALIGNED", "🟢", "TREND-ALIGNED"

    return "COUNTER_TREND", "🔴", "COUNTER-TREND"


def _derive_stop_quality(rec: SignalRecord) -> tuple[str, str, float | None, float | None]:
    theoretical_rr = rec.risk_reward_ratio

    entry = rec.entry_reference_price
    stop = rec.invalidation_reference_price
    target = rec.target_reference_price

    if entry is None or stop is None or target is None:
        return "UNKNOWN", "missing entry/stop/target", theoretical_rr, None

    try:
        entry_f = float(entry)
        stop_f = float(stop)
        target_f = float(target)
    except (TypeError, ValueError):
        return "UNKNOWN", "invalid entry/stop/target", theoretical_rr, None

    stop_distance = abs(entry_f - stop_f)
    target_distance = abs(target_f - entry_f)

    symbol = str(rec.symbol or "").upper()
    min_stop = MIN_STOP_DISTANCE_BY_SYMBOL.get(symbol)

    if stop_distance <= 0:
        return "INVALID", "stop distance is zero or negative", theoretical_rr, None

    if min_stop is None:
        return "OK", "no instrument-specific practical stop threshold", theoretical_rr, theoretical_rr

    if stop_distance < min_stop:
        practical_rr = round(target_distance / min_stop, 3) if min_stop > 0 else None
        return (
            "TIGHT_STOP",
            f"stop_distance {stop_distance:.5f} below practical_min_stop {min_stop:.5f}",
            theoretical_rr,
            practical_rr,
        )

    return (
        "OK",
        f"stop_distance {stop_distance:.5f} >= practical_min_stop {min_stop:.5f}",
        theoretical_rr,
        theoretical_rr,
    )


def _apply_record_derived_fields(rec: SignalRecord) -> SignalRecord:
    alignment, marker, label = _derive_signal_alignment(rec.direction, rec.htf_bias)
    rec.signal_alignment = alignment
    rec.signal_alignment_marker = marker
    rec.signal_alignment_label = label

    stop_quality, stop_reason, theoretical_rr, practical_rr = _derive_stop_quality(rec)
    rec.stop_quality = stop_quality
    rec.stop_quality_reason = stop_reason
    rec.theoretical_rr = theoretical_rr
    rec.practical_rr = practical_rr

    return rec

def _enum_value(value: Any) -> Any:
    return getattr(value, "value", value)


def _nested_get(obj: Any, *keys: str) -> Any:
    current = obj
    for key in keys:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(key)
        else:
            current = getattr(current, key, None)
    return current


def _normalize_symbol(*values: Any) -> str:
    def iter_candidates(value: Any):
        if value is None:
            return

        enum_value = getattr(value, "value", None)
        if enum_value is not None:
            yield enum_value

        enum_name = getattr(value, "name", None)
        if enum_name is not None:
            yield enum_name

        if isinstance(value, dict):
            for key in ("symbol", "instrument", "ticker", "provider_symbol"):
                if key in value:
                    yield from iter_candidates(value.get(key))
            metadata = value.get("metadata")
            if isinstance(metadata, dict):
                yield from iter_candidates(metadata)
            return

        yield value

    for value in values:
        for candidate in iter_candidates(value):
            if candidate is None:
                continue

            text = str(candidate).strip().upper()
            if not text or text == "UNKNOWN":
                continue

            text = text.replace(" ", "")
            aliased = SYMBOL_ALIASES.get(text, text)
            compact = aliased.replace("/", "")

            if aliased in KNOWN_INSTRUMENTS:
                return aliased
            if compact in KNOWN_INSTRUMENTS:
                return compact

    return "UNKNOWN"


def _normalize_htf_bias(*values: Any) -> str:
    for value in values:
        if value is None:
            continue

        value = _enum_value(value)
        text = str(value).strip().upper()

        if text in VALID_HTF_BIAS_VALUES:
            return text

    return ""


def _extract_htf_bias(payload: dict[str, Any] | None, event: dict[str, Any] | None = None) -> str:
    payload = payload or {}
    event = event or {}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    analysis = payload.get("analysis") if isinstance(payload.get("analysis"), dict) else {}
    behavioral_summary = payload.get("behavioral_summary") if isinstance(payload.get("behavioral_summary"), dict) else {}

    return _normalize_htf_bias(
        payload.get("htf_bias"),
        metadata.get("htf_bias"),
        context.get("htf_bias"),
        analysis.get("htf_bias"),
        behavioral_summary.get("htf_bias"),
        _nested_get(payload, "payload", "htf_bias"),
        _nested_get(event, "payload", "htf_bias"),
        _nested_get(event, "payload", "payload", "htf_bias"),
    )


def _context_key(cycle_id: str | None, symbol: str | None) -> tuple[str, str] | None:
    if not cycle_id or not symbol or symbol == "UNKNOWN":
        return None
    return str(cycle_id), str(symbol)


def _build_context_index(events: list[dict]) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Context index lets flat statistics recover htf_bias/market_state from
    instrument_analyzed or fallback events when signal payloads are minimal.
    """
    index: dict[tuple[str, str], dict[str, Any]] = {}

    for event in events:
        payload = event.get("payload", {}) or {}
        cycle_id = event.get("cycle_id") or payload.get("cycle_id")
        symbol = _normalize_symbol(
            payload.get("symbol"),
            payload.get("instrument"),
            event.get("symbol"),
            payload.get("metadata"),
        )
        key = _context_key(cycle_id, symbol)
        if key is None:
            continue

        entry = index.setdefault(key, {})

        htf_bias = _extract_htf_bias(payload, event)
        if htf_bias:
            entry["htf_bias"] = htf_bias

        market_state = payload.get("market_state")
        if market_state:
            entry["market_state"] = _enum_value(market_state)

        price = payload.get("price")
        if price is not None:
            entry["price"] = _safe_float(price, None)

        scenario = payload.get("scenario") or payload.get("scenario_type")
        if scenario:
            entry["scenario"] = _enum_value(scenario)

        direction = payload.get("direction")
        if direction:
            entry["direction"] = _enum_value(direction)

    return index


def _normalize_signal_id(
    signal_id: Any,
    *,
    symbol: str,
    cycle_id: str,
    scenario: str,
    direction: str,
) -> str:
    raw = str(signal_id or "").strip()

    if raw:
        if raw.startswith("UNKNOWN_") and symbol and symbol != "UNKNOWN":
            return f"{symbol}_{raw[len('UNKNOWN_'):]}"
        return raw

    safe_cycle_id = cycle_id or ""
    safe_cycle_id = safe_cycle_id.replace(":", "-")
    return f"{symbol}_{safe_cycle_id}_{scenario}_{direction}"


def _event_symbol(event: dict[str, Any], payload: dict[str, Any]) -> str:
    return _normalize_symbol(
        payload.get("symbol"),
        payload.get("instrument"),
        payload.get("metadata"),
        event.get("symbol"),
    )


def _confidence_bucket(confidence: float) -> str:
    if confidence < 0.30:
        return "0.00-0.29"
    if confidence < 0.40:
        return "0.30-0.39"
    if confidence < 0.50:
        return "0.40-0.49"
    if confidence < 0.60:
        return "0.50-0.59"
    return "0.60+"


def load_journal_events(path: Path | str = DEFAULT_JOURNAL_PATH) -> list[dict]:
    path = Path(path)
    if not path.exists():
        return []

    events: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                item = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if isinstance(item, dict) and "event_type" in item:
                events.append(item)

    return events


def build_signal_records(events: list[dict]) -> list[SignalRecord]:
    """
    Builds flat signal records from event log.

    Expected event_type:
    - signal_candidate_detected
    - signal_registered
    - signal_updated
    - signal_resolved
    - signal_deduped
    - telegram_sent

    Important:
    - signal_id is normalized for legacy UNKNOWN_ ids when event/payload symbol is known.
    - htf_bias is recovered from signal payload, metadata, or same-cycle instrument context.
    """
    records: dict[str, SignalRecord] = {}
    context_index = _build_context_index(events)

    for event in events:
        event_type = event.get("event_type")
        payload = event.get("payload", {}) or {}

        if event_type == "signal_candidate_detected":
            continue

        if event_type == "signal_registered":
            symbol = _event_symbol(event, payload)
            cycle_id = event.get("cycle_id", "")
            scenario = payload.get("scenario", "UNKNOWN")
            direction = payload.get("direction", "NEUTRAL")
            key = _context_key(cycle_id, symbol)
            context = context_index.get(key or ("", ""), {})

            signal_id = _normalize_signal_id(
                payload.get("signal_id"),
                symbol=symbol,
                cycle_id=cycle_id,
                scenario=scenario,
                direction=direction,
            )
            if not signal_id:
                continue

            if signal_id not in records:
                htf_bias = _extract_htf_bias(payload, event) or str(context.get("htf_bias") or "")
                market_state = payload.get("market_state") or context.get("market_state") or ""

                records[signal_id] = SignalRecord(
                    signal_id=signal_id,
                    symbol=symbol if symbol != "UNKNOWN" else event.get("symbol", "-"),
                    timeframe=event.get("timeframe", "15m"),
                    cycle_id=cycle_id,
                    created_at_utc=payload.get("created_at_utc", event.get("ts_utc", "")),
                    scenario=scenario,
                    signal_class=payload.get("signal_class", "SCENARIO_FORMING"),
                    direction=direction,
                    market_state=market_state,
                    htf_bias=htf_bias,
                    confidence=_safe_float(payload.get("confidence"), 0.0) or 0.0,
                    phase=payload.get("phase"),
                    status=payload.get("status"),
                    setup_type=payload.get("setup_type"),
                    setup_name=payload.get("setup_name"),
                    dominant_setup=payload.get("dominant_setup"),
                    alignment_score=_safe_float(payload.get("alignment_score"), None),
                    next_expected_event=payload.get("next_expected_event"),
                    missing_conditions=_safe_list_str(payload.get("missing_conditions")),
                    reason=payload.get("reason"),
                    rationale=payload.get("rationale"),
                    execution_quality_reason=_execution_quality_reason(payload),
                    promotion_blocker=_infer_promotion_blocker(payload),
                    entry_reference_price=_safe_float(payload.get("entry_reference_price"), None),
                    invalidation_reference_price=_safe_float(
                        payload.get("invalidation_reference_price"),
                        None,
                    ),
                    target_reference_price=_safe_float(
                        payload.get("target_reference_price"),
                        None,
                    ),
                    execution_status=payload.get("execution_status"),
                    execution_model=payload.get("execution_model"),
                    risk_reward_ratio=_safe_float(payload.get("risk_reward_ratio"), None),
                    stop_distance=_safe_float(payload.get("stop_distance"), None),
                    target_distance=_safe_float(payload.get("target_distance"), None),
                    execution_timeframe=payload.get("execution_timeframe"),
                    trigger_reason=payload.get("trigger_reason"),
                    current_stage=payload.get(
                        "current_stage",
                        payload.get("signal_class", "SCENARIO_FORMING"),
                    ),
                    runner_version=event.get("runner_version"),
                )
            continue

        if event_type == "signal_updated":
            updated_payload = payload.get("payload", {}) or {}
            symbol = _event_symbol(event, updated_payload) or _event_symbol(event, payload)
            cycle_id = event.get("cycle_id", "")
            scenario = updated_payload.get("scenario", payload.get("scenario", "UNKNOWN"))
            direction = updated_payload.get("direction", payload.get("direction", "NEUTRAL"))
            key = _context_key(cycle_id, symbol)
            context = context_index.get(key or ("", ""), {})

            signal_id = _normalize_signal_id(
                payload.get("signal_id"),
                symbol=symbol,
                cycle_id=cycle_id,
                scenario=scenario,
                direction=direction,
            )
            raw_signal_id = str(payload.get("signal_id") or "")

            if not signal_id:
                continue

            rec = records.get(signal_id)
            if rec is None and raw_signal_id:
                rec = records.get(raw_signal_id)
                if rec is not None:
                    records[signal_id] = rec
                    records.pop(raw_signal_id, None)
                    rec.signal_id = signal_id

            if rec is None:
                continue

            rec.current_stage = updated_payload.get("signal_class", rec.current_stage)
            rec.signal_class = updated_payload.get("signal_class", rec.signal_class)
            rec.confidence = (
                _safe_float(updated_payload.get("confidence"), rec.confidence)
                or rec.confidence
            )

            rec.phase = updated_payload.get("phase", rec.phase)
            rec.status = updated_payload.get("status", rec.status)
            rec.setup_type = updated_payload.get("setup_type", rec.setup_type)
            rec.setup_name = updated_payload.get("setup_name", rec.setup_name)
            rec.dominant_setup = updated_payload.get("dominant_setup", rec.dominant_setup)
            rec.alignment_score = _safe_float(
                updated_payload.get("alignment_score"),
                rec.alignment_score,
            )
            rec.next_expected_event = updated_payload.get(
                "next_expected_event",
                rec.next_expected_event,
            )
            rec.missing_conditions = _safe_list_str(
                updated_payload.get("missing_conditions", rec.missing_conditions),
            )
            rec.reason = updated_payload.get("reason", rec.reason)
            rec.rationale = updated_payload.get("rationale", rec.rationale)
            rec.execution_quality_reason = (
                _execution_quality_reason(updated_payload)
                or rec.execution_quality_reason
            )
            rec.promotion_blocker = _infer_promotion_blocker(updated_payload)

            rec.entry_reference_price = _safe_float(
                updated_payload.get("entry_reference_price"),
                rec.entry_reference_price,
            )
            rec.invalidation_reference_price = _safe_float(
                updated_payload.get("invalidation_reference_price"),
                rec.invalidation_reference_price,
            )
            rec.target_reference_price = _safe_float(
                updated_payload.get("target_reference_price"),
                rec.target_reference_price,
            )

            rec.execution_status = updated_payload.get(
                "execution_status",
                rec.execution_status,
            )
            rec.execution_model = updated_payload.get(
                "execution_model",
                rec.execution_model,
            )
            rec.risk_reward_ratio = _safe_float(
                updated_payload.get("risk_reward_ratio"),
                rec.risk_reward_ratio,
            )
            rec.stop_distance = _safe_float(
                updated_payload.get("stop_distance"),
                rec.stop_distance,
            )
            rec.target_distance = _safe_float(
                updated_payload.get("target_distance"),
                rec.target_distance,
            )
            rec.execution_timeframe = updated_payload.get(
                "execution_timeframe",
                rec.execution_timeframe,
            )
            rec.trigger_reason = updated_payload.get(
                "trigger_reason",
                rec.trigger_reason,
            )

            rec.scenario = updated_payload.get("scenario", rec.scenario)
            rec.direction = updated_payload.get("direction", rec.direction)
            rec.market_state = updated_payload.get(
                "market_state",
                rec.market_state or context.get("market_state", ""),
            )

            recovered_htf_bias = (
                _extract_htf_bias(updated_payload, event)
                or _extract_htf_bias(payload, event)
                or str(context.get("htf_bias") or "")
            )
            if recovered_htf_bias:
                rec.htf_bias = recovered_htf_bias

            if symbol and symbol != "UNKNOWN" and rec.symbol in {"", "-", "UNKNOWN"}:
                rec.symbol = symbol

            continue

        if event_type == "signal_resolved":
            symbol = _event_symbol(event, payload)
            cycle_id = event.get("cycle_id", "")
            scenario = payload.get("scenario", "UNKNOWN")
            direction = payload.get("direction", "NEUTRAL")
            signal_id = _normalize_signal_id(
                payload.get("signal_id"),
                symbol=symbol,
                cycle_id=cycle_id,
                scenario=scenario,
                direction=direction,
            )
            raw_signal_id = str(payload.get("signal_id") or "")

            if not signal_id:
                continue

            rec = records.get(signal_id)
            if rec is None and raw_signal_id:
                rec = records.get(raw_signal_id)
                if rec is not None:
                    records[signal_id] = rec
                    records.pop(raw_signal_id, None)
                    rec.signal_id = signal_id

            if rec is None:
                continue

            rec.signal_class = payload.get("signal_class", rec.signal_class)
            rec.current_stage = payload.get("signal_class", rec.current_stage)

            rec.final_status = payload.get("resolution")
            rec.resolution_reason = payload.get("resolution_note")

            rec.phase = payload.get("phase", rec.phase)
            rec.status = payload.get("status", rec.status)
            rec.setup_type = payload.get("setup_type", rec.setup_type)
            rec.setup_name = payload.get("setup_name", rec.setup_name)
            rec.dominant_setup = payload.get("dominant_setup", rec.dominant_setup)
            rec.alignment_score = _safe_float(
                payload.get("alignment_score"),
                rec.alignment_score,
            )
            rec.next_expected_event = payload.get(
                "next_expected_event",
                rec.next_expected_event,
            )
            rec.missing_conditions = _safe_list_str(
                payload.get("missing_conditions", rec.missing_conditions),
            )
            rec.reason = payload.get("reason", rec.reason)
            rec.rationale = payload.get("rationale", rec.rationale)
            rec.execution_quality_reason = (
                _execution_quality_reason(payload)
                or rec.execution_quality_reason
            )
            rec.promotion_blocker = _infer_promotion_blocker(payload)

            rec.bars_alive = _safe_int(payload.get("bars_alive"), rec.bars_alive)
            rec.minutes_alive = _safe_int(payload.get("minutes_alive"), rec.minutes_alive)
            rec.mfe_pct = _safe_float(payload.get("mfe_pct"), rec.mfe_pct) or rec.mfe_pct
            rec.mae_pct = _safe_float(payload.get("mae_pct"), rec.mae_pct) or rec.mae_pct
            rec.time_to_validation_min = payload.get(
                "time_to_validation_min",
                rec.time_to_validation_min,
            )

            rec.entry_reference_price = _safe_float(
                payload.get("entry_reference_price"),
                rec.entry_reference_price,
            )
            rec.invalidation_reference_price = _safe_float(
                payload.get("invalidation_reference_price"),
                rec.invalidation_reference_price,
            )
            rec.target_reference_price = _safe_float(
                payload.get("target_reference_price"),
                rec.target_reference_price,
            )
            rec.execution_status = payload.get("execution_status", rec.execution_status)
            rec.execution_model = payload.get("execution_model", rec.execution_model)
            rec.risk_reward_ratio = _safe_float(
                payload.get("risk_reward_ratio"),
                rec.risk_reward_ratio,
            )
            rec.stop_distance = _safe_float(
                payload.get("stop_distance"),
                rec.stop_distance,
            )
            rec.target_distance = _safe_float(
                payload.get("target_distance"),
                rec.target_distance,
            )
            rec.execution_timeframe = payload.get(
                "execution_timeframe",
                rec.execution_timeframe,
            )
            rec.trigger_reason = payload.get("trigger_reason", rec.trigger_reason)

            recovered_htf_bias = _extract_htf_bias(payload, event)
            if recovered_htf_bias:
                rec.htf_bias = recovered_htf_bias

            continue

        if event_type == "signal_deduped":
            continue

        if event_type == "telegram_sent":
            signal_id = payload.get("signal_id")
            symbol = _event_symbol(event, payload)
            signal_id = _normalize_signal_id(
                signal_id,
                symbol=symbol,
                cycle_id=event.get("cycle_id", ""),
                scenario=payload.get("scenario", "UNKNOWN"),
                direction=payload.get("direction", "NEUTRAL"),
            )

            if not signal_id or signal_id not in records:
                continue

            rec = records[signal_id]
            rec.was_sent_to_telegram = True
            continue

    for rec in records.values():
        _apply_record_derived_fields(rec)

    return list(records.values())


def compute_system_metrics(events: list[dict]) -> dict:
    cycles_total = 0
    cycles_ok = 0
    cycles_with_errors = 0

    instrument_analyzed_total = 0
    signal_registered_total = 0
    signal_updated_total = 0
    signal_resolved_total = 0
    signal_deduped_total = 0
    telegram_sent_total = 0
    telegram_failed_total = 0

    total_duration_sec = 0.0
    finished_cycles_count = 0

    for event in events:
        et = event.get("event_type")
        status = event.get("status")
        payload = event.get("payload", {}) or {}

        if et == "cycle_finished":
            cycles_total += 1
            finished_cycles_count += 1
            total_duration_sec += _safe_float(payload.get("duration_sec"), 0.0) or 0.0
            if status == "ok":
                cycles_ok += 1
            else:
                cycles_with_errors += 1

        elif et == "instrument_analyzed":
            instrument_analyzed_total += 1
        elif et == "signal_registered":
            signal_registered_total += 1
        elif et == "signal_updated":
            signal_updated_total += 1
        elif et == "signal_resolved":
            signal_resolved_total += 1
        elif et == "signal_deduped":
            signal_deduped_total += 1
        elif et == "telegram_sent":
            telegram_sent_total += 1
        elif et == "telegram_failed":
            telegram_failed_total += 1

    avg_cycle_duration_sec = round(
        total_duration_sec / finished_cycles_count,
        3,
    ) if finished_cycles_count else 0.0

    telegram_send_success_rate = round(
        telegram_sent_total / (telegram_sent_total + telegram_failed_total),
        4,
    ) if (telegram_sent_total + telegram_failed_total) else 0.0

    return {
        "cycles_total": cycles_total,
        "cycles_ok": cycles_ok,
        "cycles_with_errors": cycles_with_errors,
        "avg_cycle_duration_sec": avg_cycle_duration_sec,
        "instrument_analyzed_total": instrument_analyzed_total,
        "signal_registered_total": signal_registered_total,
        "signal_updated_total": signal_updated_total,
        "signal_resolved_total": signal_resolved_total,
        "signal_deduped_total": signal_deduped_total,
        "telegram_sent_total": telegram_sent_total,
        "telegram_failed_total": telegram_failed_total,
        "telegram_send_success_rate": telegram_send_success_rate,
    }


def compute_signal_metrics(records: list[SignalRecord]) -> dict:
    total = len(records)
    if total == 0:
        return {
            "signals_total": 0,
            "validated_total": 0,
            "invalidated_total": 0,
            "expired_total": 0,
            "validation_rate": 0.0,
            "invalidation_rate": 0.0,
            "expiry_rate": 0.0,
            "avg_mfe_pct": 0.0,
            "avg_mae_pct": 0.0,
            "avg_lifetime_min": 0.0,
            "avg_confidence": 0.0,
            "executable_signals": 0,
            "executable_rate": 0.0,
            "avg_rr": 0.0,
        }

    validated = [r for r in records if r.final_status == "VALIDATED"]
    invalidated = [r for r in records if r.final_status == "INVALIDATED"]
    expired = [r for r in records if r.final_status == "EXPIRED"]
    executable = [r for r in records if r.execution_status == "EXECUTABLE"]
    rr_values = [r.risk_reward_ratio for r in records if r.risk_reward_ratio is not None]

    avg_mfe = round(sum(r.mfe_pct for r in records) / total, 6)
    avg_mae = round(sum(r.mae_pct for r in records) / total, 6)
    avg_lifetime = round(sum(r.minutes_alive for r in records) / total, 2)
    avg_confidence = round(sum(r.confidence for r in records) / total, 6)
    avg_rr = round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0

    return {
        "signals_total": total,
        "validated_total": len(validated),
        "invalidated_total": len(invalidated),
        "expired_total": len(expired),
        "validation_rate": round(len(validated) / total, 4),
        "invalidation_rate": round(len(invalidated) / total, 4),
        "expiry_rate": round(len(expired) / total, 4),
        "avg_mfe_pct": avg_mfe,
        "avg_mae_pct": avg_mae,
        "avg_lifetime_min": avg_lifetime,
        "avg_confidence": avg_confidence,
        "executable_signals": len(executable),
        "executable_rate": round(len(executable) / total, 4),
        "avg_rr": avg_rr,
    }


def compute_metrics_by_symbol(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        grouped.setdefault(rec.symbol, []).append(rec)

    out: dict[str, dict] = {}
    for symbol, items in grouped.items():
        total = len(items)
        validated = sum(1 for r in items if r.final_status == "VALIDATED")
        invalidated = sum(1 for r in items if r.final_status == "INVALIDATED")
        expired = sum(1 for r in items if r.final_status == "EXPIRED")
        executable = sum(1 for r in items if r.execution_status == "EXECUTABLE")
        rr_values = [r.risk_reward_ratio for r in items if r.risk_reward_ratio is not None]

        out[symbol] = {
            "signals_total": total,
            "validated_total": validated,
            "invalidated_total": invalidated,
            "expired_total": expired,
            "validation_rate": round(validated / total, 4) if total else 0.0,
            "avg_mfe_pct": round(sum(r.mfe_pct for r in items) / total, 6) if total else 0.0,
            "avg_mae_pct": round(sum(r.mae_pct for r in items) / total, 6) if total else 0.0,
            "avg_confidence": round(sum(r.confidence for r in items) / total, 6) if total else 0.0,
            "executable_signals": executable,
            "executable_rate": round(executable / total, 4) if total else 0.0,
            "avg_rr": round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0,
        }
    return out


def compute_metrics_by_scenario(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        grouped.setdefault(rec.scenario, []).append(rec)

    out: dict[str, dict] = {}
    for scenario, items in grouped.items():
        total = len(items)
        validated = sum(1 for r in items if r.final_status == "VALIDATED")
        invalidated = sum(1 for r in items if r.final_status == "INVALIDATED")
        expired = sum(1 for r in items if r.final_status == "EXPIRED")
        executable = sum(1 for r in items if r.execution_status == "EXECUTABLE")
        rr_values = [r.risk_reward_ratio for r in items if r.risk_reward_ratio is not None]

        out[scenario] = {
            "signals_total": total,
            "validated_total": validated,
            "invalidated_total": invalidated,
            "expired_total": expired,
            "validation_rate": round(validated / total, 4) if total else 0.0,
            "avg_mfe_pct": round(sum(r.mfe_pct for r in items) / total, 6) if total else 0.0,
            "avg_mae_pct": round(sum(r.mae_pct for r in items) / total, 6) if total else 0.0,
            "avg_confidence": round(sum(r.confidence for r in items) / total, 6) if total else 0.0,
            "executable_signals": executable,
            "executable_rate": round(executable / total, 4) if total else 0.0,
            "avg_rr": round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0,
        }
    return out


def compute_confidence_buckets(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        bucket = _confidence_bucket(rec.confidence)
        grouped.setdefault(bucket, []).append(rec)

    out: dict[str, dict] = {}
    for bucket, items in grouped.items():
        total = len(items)
        validated = sum(1 for r in items if r.final_status == "VALIDATED")
        invalidated = sum(1 for r in items if r.final_status == "INVALIDATED")
        expired = sum(1 for r in items if r.final_status == "EXPIRED")
        executable = sum(1 for r in items if r.execution_status == "EXECUTABLE")
        rr_values = [r.risk_reward_ratio for r in items if r.risk_reward_ratio is not None]

        out[bucket] = {
            "signals_total": total,
            "validated_total": validated,
            "invalidated_total": invalidated,
            "expired_total": expired,
            "validation_rate": round(validated / total, 4) if total else 0.0,
            "avg_mfe_pct": round(sum(r.mfe_pct for r in items) / total, 6) if total else 0.0,
            "avg_mae_pct": round(sum(r.mae_pct for r in items) / total, 6) if total else 0.0,
            "executable_signals": executable,
            "executable_rate": round(executable / total, 4) if total else 0.0,
            "avg_rr": round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0,
        }
    return out


def compute_metrics_by_execution_model(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        key = rec.execution_model or "UNKNOWN"
        grouped.setdefault(key, []).append(rec)

    out: dict[str, dict] = {}
    for model, items in grouped.items():
        total = len(items)
        validated = sum(1 for r in items if r.final_status == "VALIDATED")
        invalidated = sum(1 for r in items if r.final_status == "INVALIDATED")
        expired = sum(1 for r in items if r.final_status == "EXPIRED")
        rr_values = [r.risk_reward_ratio for r in items if r.risk_reward_ratio is not None]

        out[model] = {
            "signals_total": total,
            "validated_total": validated,
            "invalidated_total": invalidated,
            "expired_total": expired,
            "validation_rate": round(validated / total, 4) if total else 0.0,
            "avg_confidence": round(sum(r.confidence for r in items) / total, 6) if total else 0.0,
            "avg_rr": round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0,
        }
    return out


def _compute_grouped_record_metrics(items: list[SignalRecord]) -> dict:
    total = len(items)
    if total == 0:
        return {
            "signals_total": 0,
            "validated_total": 0,
            "invalidated_total": 0,
            "expired_total": 0,
            "validation_rate": 0.0,
            "avg_confidence": 0.0,
            "executable_signals": 0,
            "executable_rate": 0.0,
            "avg_rr": 0.0,
        }

    validated = sum(1 for r in items if r.final_status == "VALIDATED")
    invalidated = sum(1 for r in items if r.final_status == "INVALIDATED")
    expired = sum(1 for r in items if r.final_status == "EXPIRED")
    executable = sum(1 for r in items if r.execution_status == "EXECUTABLE")
    rr_values = [r.risk_reward_ratio for r in items if r.risk_reward_ratio is not None]

    return {
        "signals_total": total,
        "validated_total": validated,
        "invalidated_total": invalidated,
        "expired_total": expired,
        "validation_rate": round(validated / total, 4),
        "avg_confidence": round(sum(r.confidence for r in items) / total, 6),
        "executable_signals": executable,
        "executable_rate": round(executable / total, 4),
        "avg_rr": round(sum(rr_values) / len(rr_values), 3) if rr_values else 0.0,
    }


def compute_metrics_by_promotion_blocker(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        key = rec.promotion_blocker or "UNKNOWN"
        grouped.setdefault(key, []).append(rec)

    return {key: _compute_grouped_record_metrics(items) for key, items in grouped.items()}


def compute_metrics_by_next_expected_event(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        key = rec.next_expected_event or "UNKNOWN"
        grouped.setdefault(key, []).append(rec)

    return {key: _compute_grouped_record_metrics(items) for key, items in grouped.items()}


def compute_metrics_by_setup_type(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        key = rec.setup_type or "UNKNOWN"
        grouped.setdefault(key, []).append(rec)

    return {key: _compute_grouped_record_metrics(items) for key, items in grouped.items()}


def compute_metrics_by_status(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        key = rec.status or "UNKNOWN"
        grouped.setdefault(key, []).append(rec)

    return {key: _compute_grouped_record_metrics(items) for key, items in grouped.items()}




def compute_metrics_by_signal_alignment(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        key = rec.signal_alignment or "UNKNOWN"
        grouped.setdefault(key, []).append(rec)

    return {key: _compute_grouped_record_metrics(items) for key, items in grouped.items()}


def compute_metrics_by_stop_quality(records: list[SignalRecord]) -> dict:
    grouped: dict[str, list[SignalRecord]] = {}
    for rec in records:
        key = rec.stop_quality or "UNKNOWN"
        grouped.setdefault(key, []).append(rec)

    return {key: _compute_grouped_record_metrics(items) for key, items in grouped.items()}

def records_to_dataframe(records: list[SignalRecord]) -> pd.DataFrame:
    rows = [r.to_dict() for r in records]
    if not rows:
        return pd.DataFrame(columns=[
            "signal_id",
            "symbol",
            "timeframe",
            "cycle_id",
            "created_at_utc",
            "scenario",
            "signal_class",
            "direction",
            "market_state",
            "htf_bias",
            "confidence",
            "phase",
            "status",
            "setup_type",
            "setup_name",
            "dominant_setup",
            "alignment_score",
            "next_expected_event",
            "missing_conditions",
            "reason",
            "rationale",
            "execution_quality_reason",
            "promotion_blocker",
            "practical_rr",
            "theoretical_rr",
            "stop_quality_reason",
            "stop_quality",
            "signal_alignment_label",
            "signal_alignment_marker",
            "signal_alignment",
            "entry_reference_price",
            "invalidation_reference_price",
            "target_reference_price",
            "execution_status",
            "execution_model",
            "risk_reward_ratio",
            "stop_distance",
            "target_distance",
            "execution_timeframe",
            "trigger_reason",
            "was_sent_to_telegram",
            "was_deduped",
            "current_stage",
            "final_status",
            "resolution_reason",
            "bars_alive",
            "minutes_alive",
            "mfe_pct",
            "mae_pct",
            "time_to_validation_min",
            "runner_version",
        ])
    return pd.DataFrame(rows)


def export_signal_records_json(
    records: list[SignalRecord],
    path: Path | str = DEFAULT_SIGNAL_RECORDS_JSON_PATH,
) -> None:
    path = Path(path)
    _ensure_parent_dir(path)
    payload = [r.to_dict() for r in records]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def export_signal_records_parquet(
    records: list[SignalRecord],
    path: Path | str = DEFAULT_SIGNAL_RECORDS_PARQUET_PATH,
) -> None:
    path = Path(path)
    _ensure_parent_dir(path)
    df = records_to_dataframe(records)
    df.to_parquet(path, index=False)


def export_daily_summary(
    summary: dict,
    path: Path | str = DEFAULT_DAILY_SUMMARY_PATH,
) -> None:
    path = Path(path)
    _ensure_parent_dir(path)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def build_statistics_bundle(
    journal_path: Path | str = DEFAULT_JOURNAL_PATH,
) -> dict:
    events = load_journal_events(journal_path)
    records = build_signal_records(events)

    bundle = {
        "system_metrics": compute_system_metrics(events),
        "signal_metrics": compute_signal_metrics(records),
        "metrics_by_symbol": compute_metrics_by_symbol(records),
        "metrics_by_scenario": compute_metrics_by_scenario(records),
        "confidence_buckets": compute_confidence_buckets(records),
        "metrics_by_execution_model": compute_metrics_by_execution_model(records),
        "metrics_by_promotion_blocker": compute_metrics_by_promotion_blocker(records),
        "metrics_by_next_expected_event": compute_metrics_by_next_expected_event(records),
        "metrics_by_setup_type": compute_metrics_by_setup_type(records),
        "metrics_by_status": compute_metrics_by_status(records),
        "metrics_by_signal_alignment": compute_metrics_by_signal_alignment(records),
        "metrics_by_stop_quality": compute_metrics_by_stop_quality(records),
        "records_count": len(records),
        "events_count": len(events),
    }
    return bundle


def build_and_export_statistics(
    journal_path: Path | str = DEFAULT_JOURNAL_PATH,
    records_json_path: Path | str = DEFAULT_SIGNAL_RECORDS_JSON_PATH,
    records_parquet_path: Path | str = DEFAULT_SIGNAL_RECORDS_PARQUET_PATH,
    daily_summary_path: Path | str = DEFAULT_DAILY_SUMMARY_PATH,
) -> dict:
    events = load_journal_events(journal_path)
    records = build_signal_records(events)
    bundle = {
        "system_metrics": compute_system_metrics(events),
        "signal_metrics": compute_signal_metrics(records),
        "metrics_by_symbol": compute_metrics_by_symbol(records),
        "metrics_by_scenario": compute_metrics_by_scenario(records),
        "confidence_buckets": compute_confidence_buckets(records),
        "metrics_by_execution_model": compute_metrics_by_execution_model(records),
        "metrics_by_promotion_blocker": compute_metrics_by_promotion_blocker(records),
        "metrics_by_next_expected_event": compute_metrics_by_next_expected_event(records),
        "metrics_by_setup_type": compute_metrics_by_setup_type(records),
        "metrics_by_status": compute_metrics_by_status(records),
        "metrics_by_signal_alignment": compute_metrics_by_signal_alignment(records),
        "metrics_by_stop_quality": compute_metrics_by_stop_quality(records),
        "records_count": len(records),
        "events_count": len(events),
    }

    export_signal_records_json(records, records_json_path)
    export_signal_records_parquet(records, records_parquet_path)
    export_daily_summary(bundle, daily_summary_path)

    return bundle


def main() -> None:
    bundle = build_and_export_statistics()
    print(json.dumps(bundle, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()