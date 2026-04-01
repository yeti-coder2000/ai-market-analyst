from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_OPEN_SIGNALS_PATH = Path("runtime/open_signals.json")


@dataclass
class TrackedSignal:
    signal_id: str
    symbol: str
    timeframe: str
    signal_class: str
    scenario: str
    direction: str
    cycle_id: str
    created_at_utc: str
    updated_at_utc: str

    market_state: str
    htf_bias: str
    probability: float

    entry_reference_price: float
    invalidation_reference_price: Optional[float]
    target_reference_price: Optional[float]

    status: str = "OPEN"
    stage: str = "SCENARIO_FORMING"

    bars_alive: int = 0
    minutes_alive: int = 0

    max_price_seen: Optional[float] = None
    min_price_seen: Optional[float] = None
    mfe_pct: float = 0.0
    mae_pct: float = 0.0

    validation_threshold_pct: Optional[float] = None
    expiry_bars: int = 8

    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "TrackedSignal":
        return cls(**payload)


@dataclass
class SignalResolution:
    signal_id: str
    symbol: str
    final_status: str
    resolution_reason: str
    resolved_at_utc: str
    bars_alive: int
    minutes_alive: int
    mfe_pct: float
    mae_pct: float
    time_to_validation_min: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_open_signals(
    path: Path | str = DEFAULT_OPEN_SIGNALS_PATH,
) -> dict[str, TrackedSignal]:
    path = Path(path)
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    return {signal_id: TrackedSignal.from_dict(item) for signal_id, item in raw.items()}


def save_open_signals(
    signals: dict[str, TrackedSignal],
    path: Path | str = DEFAULT_OPEN_SIGNALS_PATH,
) -> None:
    path = Path(path)
    _ensure_parent_dir(path)

    payload = {signal_id: signal.to_dict() for signal_id, signal in signals.items()}
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def build_signal_id(symbol: str, cycle_id: str, scenario: str, direction: str) -> str:
    safe_cycle = (cycle_id or "unknown_cycle").replace(":", "-")
    safe_scenario = (scenario or "UNKNOWN").replace(" ", "_")
    safe_direction = (direction or "NEUTRAL").replace(" ", "_")
    return f"{symbol}_{safe_cycle}_{safe_scenario}_{safe_direction}"


def classify_signal_stage(final_signal: dict, scenario: dict) -> str:
    scenario_decision = (scenario or {}).get("decision", "")
    final_status = (final_signal or {}).get("status", "")
    confidence = float(
        (final_signal or {}).get("confidence", 0.0)
        or (scenario or {}).get("alignment_score", 0.0)
        or 0.0
    )

    if scenario_decision in {"NO_TRADE", "SKIPPED"} and final_status in {"NO_SETUP", "SKIPPED"}:
        return "NO_ACTION"

    if final_status == "READY":
        return "READY"

    if final_status in {"ACTIVE", "CONFIRMED"}:
        return "ACTIVE"

    if scenario_decision == "WATCH" and confidence >= 0.35:
        return "WATCH"

    if scenario_decision in {"WATCH", "TRADEABLE"}:
        return "SCENARIO_FORMING"

    if final_status == "IDLE" and confidence > 0.0:
        return "BIAS_ONLY"

    return "NO_ACTION"


def create_tracked_signal(
    *,
    symbol: str,
    timeframe: str,
    cycle_id: str,
    scenario_payload: dict,
    final_signal_payload: dict,
    price: float,
    validation_threshold_pct: float,
    expiry_bars: int = 8,
) -> TrackedSignal:
    scenario_name = (scenario_payload or {}).get("type", "UNKNOWN")
    direction = (
        (final_signal_payload or {}).get("direction")
        or (scenario_payload or {}).get("direction")
        or "NEUTRAL"
    )

    probability = float(
        (final_signal_payload or {}).get("confidence", 0.0)
        or (scenario_payload or {}).get("alignment_score", 0.0)
        or 0.0
    )

    stage = classify_signal_stage(final_signal_payload, scenario_payload)
    signal_id = build_signal_id(symbol, cycle_id, scenario_name, direction)
    now = _utc_now_iso()

    invalidation_reference_price = (
        (final_signal_payload or {}).get("invalidation_reference_price")
        or (scenario_payload or {}).get("invalidation_reference_price")
    )

    target_reference_price = (
        (final_signal_payload or {}).get("target_reference_price")
        or (scenario_payload or {}).get("target_reference_price")
    )

    return TrackedSignal(
        signal_id=signal_id,
        symbol=symbol,
        timeframe=timeframe,
        signal_class=stage,
        scenario=scenario_name,
        direction=direction,
        cycle_id=cycle_id,
        created_at_utc=now,
        updated_at_utc=now,
        market_state=(scenario_payload or {}).get("market_state", ""),
        htf_bias=(scenario_payload or {}).get("htf_bias", ""),
        probability=probability,
        entry_reference_price=float(price),
        invalidation_reference_price=invalidation_reference_price,
        target_reference_price=target_reference_price,
        status="OPEN",
        stage=stage,
        bars_alive=0,
        minutes_alive=0,
        max_price_seen=float(price),
        min_price_seen=float(price),
        mfe_pct=0.0,
        mae_pct=0.0,
        validation_threshold_pct=validation_threshold_pct,
        expiry_bars=expiry_bars,
        meta={
            "missing_conditions": (scenario_payload or {}).get("missing_conditions", []),
            "next_expected_event": (scenario_payload or {}).get("next_expected_event"),
        },
    )


def update_signal_market_metrics(
    signal: TrackedSignal,
    *,
    current_price: float,
    current_high: Optional[float] = None,
    current_low: Optional[float] = None,
    bar_minutes: int = 15,
) -> TrackedSignal:
    signal.updated_at_utc = _utc_now_iso()
    signal.bars_alive += 1
    signal.minutes_alive += bar_minutes

    high = current_high if current_high is not None else current_price
    low = current_low if current_low is not None else current_price

    if signal.max_price_seen is None:
        signal.max_price_seen = high
    else:
        signal.max_price_seen = max(signal.max_price_seen, high)

    if signal.min_price_seen is None:
        signal.min_price_seen = low
    else:
        signal.min_price_seen = min(signal.min_price_seen, low)

    entry = signal.entry_reference_price

    if signal.direction == "LONG":
        favorable_move = ((signal.max_price_seen - entry) / entry) * 100.0
        adverse_move = ((entry - signal.min_price_seen) / entry) * 100.0
    elif signal.direction == "SHORT":
        favorable_move = ((entry - signal.min_price_seen) / entry) * 100.0
        adverse_move = ((signal.max_price_seen - entry) / entry) * 100.0
    else:
        favorable_move = 0.0
        adverse_move = 0.0

    signal.mfe_pct = round(max(signal.mfe_pct, favorable_move), 6)
    signal.mae_pct = round(max(signal.mae_pct, adverse_move), 6)

    return signal


def should_validate(signal: TrackedSignal) -> tuple[bool, str]:
    threshold = signal.validation_threshold_pct or 0.0
    if threshold > 0 and signal.mfe_pct >= threshold:
        return True, f"favorable_move_reached_{threshold:.3f}pct"
    return False, ""


def should_invalidate(
    signal: TrackedSignal,
    *,
    current_price: float,
) -> tuple[bool, str]:
    invalidation = signal.invalidation_reference_price
    if invalidation is None:
        return False, ""

    if signal.direction == "LONG" and current_price <= invalidation:
        return True, "long_invalidation_breached"

    if signal.direction == "SHORT" and current_price >= invalidation:
        return True, "short_invalidation_breached"

    return False, ""


def should_expire(signal: TrackedSignal) -> tuple[bool, str]:
    if signal.bars_alive >= signal.expiry_bars:
        return True, f"expiry_bars_reached_{signal.expiry_bars}"
    return False, ""


def maybe_promote_stage(
    signal: TrackedSignal,
    *,
    final_signal_payload: dict,
    scenario_payload: dict,
) -> tuple[TrackedSignal, Optional[dict]]:
    new_stage = classify_signal_stage(final_signal_payload, scenario_payload)
    if new_stage == signal.stage:
        return signal, None

    old_stage = signal.stage
    old_probability = signal.probability

    new_probability = float(
        (final_signal_payload or {}).get("confidence", 0.0)
        or (scenario_payload or {}).get("alignment_score", old_probability)
        or old_probability
    )

    signal.stage = new_stage
    signal.signal_class = new_stage
    signal.probability = new_probability
    signal.updated_at_utc = _utc_now_iso()

    update_event = {
        "from_stage": old_stage,
        "to_stage": new_stage,
        "probability_old": old_probability,
        "probability_new": new_probability,
        "reason": (scenario_payload or {}).get("next_expected_event"),
    }
    return signal, update_event


def resolve_signal(
    signal: TrackedSignal,
    *,
    final_status: str,
    resolution_reason: str,
) -> SignalResolution:
    signal.status = "RESOLVED"
    signal.updated_at_utc = _utc_now_iso()

    return SignalResolution(
        signal_id=signal.signal_id,
        symbol=signal.symbol,
        final_status=final_status,
        resolution_reason=resolution_reason,
        resolved_at_utc=signal.updated_at_utc,
        bars_alive=signal.bars_alive,
        minutes_alive=signal.minutes_alive,
        mfe_pct=signal.mfe_pct,
        mae_pct=signal.mae_pct,
        time_to_validation_min=signal.minutes_alive if final_status == "VALIDATED" else None,
    )


def update_open_signals_for_symbol(
    *,
    signals: dict[str, TrackedSignal],
    symbol: str,
    current_price: float,
    current_high: Optional[float],
    current_low: Optional[float],
    cycle_id: str,
    final_signal_payload: dict,
    scenario_payload: dict,
    bar_minutes: int = 15,
) -> tuple[dict[str, TrackedSignal], list[dict], list[SignalResolution]]:
    stage_updates: list[dict] = []
    resolved_items: list[SignalResolution] = []
    to_remove: list[str] = []

    for signal_id, signal in signals.items():
        if signal.symbol != symbol:
            continue
        if signal.status != "OPEN":
            continue

        update_signal_market_metrics(
            signal,
            current_price=current_price,
            current_high=current_high,
            current_low=current_low,
            bar_minutes=bar_minutes,
        )

        signal, upd = maybe_promote_stage(
            signal,
            final_signal_payload=final_signal_payload,
            scenario_payload=scenario_payload,
        )
        if upd is not None:
            stage_updates.append(
                {
                    "signal_id": signal.signal_id,
                    **upd,
                }
            )

        is_invalid, invalid_reason = should_invalidate(signal, current_price=current_price)
        if is_invalid:
            resolution = resolve_signal(
                signal,
                final_status="INVALIDATED",
                resolution_reason=invalid_reason,
            )
            resolved_items.append(resolution)
            to_remove.append(signal_id)
            continue

        is_valid, valid_reason = should_validate(signal)
        if is_valid:
            resolution = resolve_signal(
                signal,
                final_status="VALIDATED",
                resolution_reason=valid_reason,
            )
            resolved_items.append(resolution)
            to_remove.append(signal_id)
            continue

        is_expired, expire_reason = should_expire(signal)
        if is_expired:
            resolution = resolve_signal(
                signal,
                final_status="EXPIRED",
                resolution_reason=expire_reason,
            )
            resolved_items.append(resolution)
            to_remove.append(signal_id)

    for signal_id in to_remove:
        signals.pop(signal_id, None)

    return signals, stage_updates, resolved_items