from __future__ import annotations

"""
TPO LTF Model Detector for AI Market Analyst.

v1 purpose:
- Convert TPO Watch Bridge states into an operational live setup state.
- It is intentionally conservative: most OPEN_TEST_DRIVE contexts remain WATCH.
- It promotes to READY only when recent 15m structure gives a directional LTF model,
  valid stop, target, and practical RR.

Pipeline:
TPO Watch Bridge: LTF_MODEL_PENDING
→ this detector: LTF_MODEL_PENDING / LTF_MODEL_CONFIRMED / LTF_MODEL_REJECTED
→ execution geometry
→ Battle Gate
→ Telegram hard gate

This module does not call Telegram and does not read external data.
"""

from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd


LTF_MODEL_DETECTOR_VERSION = "tpo-ltf-model-detector-v1.0-failed-acceptance-retest"

MIN_CONFIRMED_RR = 2.0
MIN_BARS = 8
RECENT_WINDOW = 16
STRUCTURE_WINDOW = 5

MIN_STOP_BY_SYMBOL = {
    "XAUUSD": 1.0,
    "BTCUSD": 25.0,
    "ETHUSD": 10.0,
    "GER40": 10.0,
    "NAS100": 25.0,
    "SPX500": 5.0,
    "UKOIL": 0.10,
    "USDJPY": 0.03,
    "EURUSD": 0.0003,
    "GBPUSD": 0.0003,
    "USDCHF": 0.0003,
    "USDCAD": 0.0003,
    "AUDUSD": 0.0003,
}


@dataclass
class LTFModelResult:
    version: str = LTF_MODEL_DETECTOR_VERSION
    ltf_model_state: str = "NO_MODEL"  # NO_MODEL | PENDING | CONFIRMED | REJECTED
    ltf_model_type: str | None = None
    ltf_model_confirmed: bool = False

    direction: str | None = None
    signal_class: str = "WATCH"
    status: str = "WATCH"
    scenario: str = "TPO_OPEN_TEST_DRIVE_WATCH"
    scenario_type: str = "TPO_OPEN_TEST_DRIVE_WATCH"

    entry_reference_price: float | None = None
    invalidation_reference_price: float | None = None
    target_reference_price: float | None = None
    stop_distance: float | None = None
    target_distance: float | None = None
    risk_reward_ratio: float | None = None
    practical_rr: float | None = None

    execution_status: str = "NOT_EXECUTABLE"
    execution_model: str = "NONE"
    execution_timeframe: str = "15m"
    trigger_reason: str = "waiting_for_ltf_model_confirmation"

    confidence: float = 0.50
    probability: float = 0.50

    reasons: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _s(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip().upper()


def _f(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _symbol(payload: dict[str, Any]) -> str:
    return _s(payload.get("symbol") or payload.get("instrument"), "-")


def _min_stop(symbol: str, price: float) -> float:
    configured = MIN_STOP_BY_SYMBOL.get(symbol)
    if configured is not None:
        return configured
    return max(abs(price) * 0.0002, 0.0001)


def _prepare_ohlc(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None

    out = df.copy()
    out.columns = [str(c).lower() for c in out.columns]
    required = {"open", "high", "low", "close"}
    if not required.issubset(set(out.columns)):
        return None

    out = out.dropna(subset=["open", "high", "low", "close"])
    if len(out) < MIN_BARS:
        return None

    return out.tail(RECENT_WINDOW)


def _avg_range(df: pd.DataFrame) -> float | None:
    rng = (df["high"] - df["low"]).abs()
    if rng.empty:
        return None
    value = float(rng.tail(10).mean())
    return value if value > 0 else None


def _interest_zone_price(payload: dict[str, Any]) -> float | None:
    price = _f(payload.get("interest_zone_price"))
    if price is not None:
        return price

    zone = payload.get("primary_interest_zone")
    if isinstance(zone, dict):
        return _f(zone.get("price") or zone.get("level"))

    meta = payload.get("metadata")
    if isinstance(meta, dict):
        price = _f(meta.get("interest_zone_price"))
        if price is not None:
            return price
        zone = meta.get("primary_interest_zone")
        if isinstance(zone, dict):
            return _f(zone.get("price") or zone.get("level"))

    return None


def _detect_displacement(df: pd.DataFrame) -> tuple[str | None, dict[str, Any]]:
    """Return LONG/SHORT when the last 15m candle breaks local structure with body."""
    last = df.iloc[-1]
    prev = df.iloc[:-1].tail(STRUCTURE_WINDOW)
    if prev.empty:
        return None, {}

    close = float(last["close"])
    open_ = float(last["open"])
    high = float(last["high"])
    low = float(last["low"])
    prev_high = float(prev["high"].max())
    prev_low = float(prev["low"].min())
    avg = _avg_range(df) or max(high - low, abs(close) * 0.0002)
    body = abs(close - open_)

    diagnostics = {
        "last_open": open_,
        "last_high": high,
        "last_low": low,
        "last_close": close,
        "prev_structure_high": prev_high,
        "prev_structure_low": prev_low,
        "avg_range": avg,
        "body": body,
    }

    body_ok = body >= avg * 0.30

    if close > prev_high and body_ok:
        diagnostics["displacement"] = "bullish_breakout"
        return "LONG", diagnostics

    if close < prev_low and body_ok:
        diagnostics["displacement"] = "bearish_breakdown"
        return "SHORT", diagnostics

    diagnostics["displacement"] = "none"
    diagnostics["body_ok"] = body_ok
    return None, diagnostics


def _build_geometry(
    *,
    symbol: str,
    direction: str,
    df: pd.DataFrame,
    target_zone_price: float | None,
) -> dict[str, Any]:
    recent = df.tail(STRUCTURE_WINDOW)
    last_close = float(df.iloc[-1]["close"])
    avg = _avg_range(df) or max(abs(last_close) * 0.0005, _min_stop(symbol, last_close))

    if direction == "LONG":
        stop = float(recent["low"].min())
        risk = last_close - stop
        if risk <= 0:
            stop = last_close - avg
            risk = avg

        if target_zone_price is not None and target_zone_price > last_close:
            target = target_zone_price
        else:
            target = last_close + (risk * 2.5)

        reward = target - last_close

    else:
        stop = float(recent["high"].max())
        risk = stop - last_close
        if risk <= 0:
            stop = last_close + avg
            risk = avg

        if target_zone_price is not None and target_zone_price < last_close:
            target = target_zone_price
        else:
            target = last_close - (risk * 2.5)

        reward = last_close - target

    rr = reward / risk if risk > 0 else None

    return {
        "entry_reference_price": round(last_close, 8),
        "invalidation_reference_price": round(stop, 8),
        "target_reference_price": round(target, 8),
        "stop_distance": round(risk, 8),
        "target_distance": round(reward, 8),
        "risk_reward_ratio": round(rr, 4) if rr is not None else None,
        "practical_rr": round(rr, 4) if rr is not None else None,
    }


def detect_ltf_model(
    payload: dict[str, Any],
    *,
    df_15m: pd.DataFrame | None = None,
) -> dict[str, Any]:
    result = LTFModelResult()

    if not isinstance(payload, dict):
        result.blockers.append("payload_is_not_dict")
        return result.to_dict()

    tpo_watch_state = _s(payload.get("tpo_watch_state") or (payload.get("metadata") or {}).get("tpo_watch_state"))
    open_behavior = _s(payload.get("open_behavior") or (payload.get("metadata") or {}).get("open_behavior"))
    tpo_watch_active = bool(payload.get("tpo_watch_active") or (payload.get("metadata") or {}).get("tpo_watch_active"))
    symbol = _symbol(payload)

    result.diagnostics.update(
        {
            "symbol": symbol,
            "tpo_watch_state": tpo_watch_state,
            "open_behavior": open_behavior,
            "tpo_watch_active": tpo_watch_active,
        }
    )

    if tpo_watch_state != "LTF_MODEL_PENDING" or open_behavior != "OPEN_TEST_DRIVE" or not tpo_watch_active:
        result.ltf_model_state = "NO_MODEL"
        result.status = _s(payload.get("status"), "NO_SETUP")
        result.signal_class = _s(payload.get("signal_class"), "IDLE")
        result.scenario = str(payload.get("scenario") or payload.get("scenario_type") or "NO_ACTION")
        result.scenario_type = result.scenario
        result.trigger_reason = "no_active_tpo_otd_watch"
        result.blockers.append("no_active_tpo_otd_watch")
        return result.to_dict()

    result.ltf_model_state = "PENDING"
    result.ltf_model_type = "FAILED_ACCEPTANCE_RETEST"
    result.signal_class = "WATCH"
    result.status = "WATCH"
    result.scenario = "TPO_OPEN_TEST_DRIVE_WATCH"
    result.scenario_type = "TPO_OPEN_TEST_DRIVE_WATCH"
    result.execution_status = "NOT_EXECUTABLE"
    result.execution_model = "NONE"
    result.trigger_reason = "waiting_for_ltf_model_confirmation"
    result.reasons.append("TPO OPEN_TEST_DRIVE is active; waiting for directional 15m LTF confirmation.")

    df = _prepare_ohlc(df_15m)
    if df is None:
        result.blockers.append("missing_or_invalid_15m_ohlc")
        return result.to_dict()

    direction, diagnostics = _detect_displacement(df)
    result.diagnostics.update(diagnostics)

    if direction not in {"LONG", "SHORT"}:
        result.blockers.append("no_directional_displacement")
        result.reasons.append("No confirmed 15m displacement/BOS yet; keep WATCH, no Telegram.")
        return result.to_dict()

    zone_price = _interest_zone_price(payload)
    geometry = _build_geometry(
        symbol=symbol,
        direction=direction,
        df=df,
        target_zone_price=zone_price,
    )

    entry = geometry["entry_reference_price"]
    stop_distance = geometry["stop_distance"]
    rr = geometry["risk_reward_ratio"]
    min_stop = _min_stop(symbol, float(entry))

    result.direction = direction
    result.entry_reference_price = geometry["entry_reference_price"]
    result.invalidation_reference_price = geometry["invalidation_reference_price"]
    result.target_reference_price = geometry["target_reference_price"]
    result.stop_distance = geometry["stop_distance"]
    result.target_distance = geometry["target_distance"]
    result.risk_reward_ratio = geometry["risk_reward_ratio"]
    result.practical_rr = geometry["practical_rr"]
    result.scenario = f"TPO_OPEN_TEST_DRIVE_{direction}"
    result.scenario_type = result.scenario
    result.ltf_model_state = "CONFIRMED"
    result.ltf_model_confirmed = True
    result.ltf_model_type = "FAILED_ACCEPTANCE_RETEST"
    result.confidence = 0.64
    result.probability = 0.64
    result.reasons.append(f"15m structure confirmed directional OPEN_TEST_DRIVE model: {direction}.")

    if stop_distance is None or stop_distance < min_stop:
        result.status = "WATCH"
        result.signal_class = "WATCH"
        result.execution_status = "INCOMPLETE"
        result.execution_model = "FAILED_ACCEPTANCE_RETEST"
        result.trigger_reason = f"stop_too_tight:{stop_distance} < {min_stop}"
        result.blockers.append("stop_too_tight")
        return result.to_dict()

    if rr is None or rr < MIN_CONFIRMED_RR:
        result.status = "WATCH"
        result.signal_class = "WATCH"
        result.execution_status = "INCOMPLETE"
        result.execution_model = "FAILED_ACCEPTANCE_RETEST"
        result.trigger_reason = f"rr_too_low:{rr}"
        result.blockers.append("rr_too_low")
        return result.to_dict()

    result.status = "READY"
    result.signal_class = "READY"
    result.execution_status = "EXECUTABLE"
    result.execution_model = "FAILED_ACCEPTANCE_RETEST"
    result.trigger_reason = "ltf_model_confirmed_open_test_drive"
    result.confidence = 0.68
    result.probability = 0.68
    result.reasons.append("Execution geometry is valid; payload may continue to Battle Gate.")
    return result.to_dict()


def enrich_payload_with_ltf_model(
    payload: dict[str, Any],
    *,
    df_15m: pd.DataFrame | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    enriched = dict(payload)
    meta = enriched.get("metadata")
    if not isinstance(meta, dict):
        meta = {}

    result = detect_ltf_model(enriched, df_15m=df_15m)

    for key, value in result.items():
        meta[key] = value

    # Always expose diagnostic state.
    enriched["ltf_model_detector_version"] = result.get("version")
    enriched["ltf_model_state"] = result.get("ltf_model_state")
    enriched["ltf_model_type"] = result.get("ltf_model_type")
    enriched["ltf_model_confirmed"] = result.get("ltf_model_confirmed")
    enriched["ltf_model_reasons"] = result.get("reasons") or []
    enriched["ltf_model_blockers"] = result.get("blockers") or []
    enriched["ltf_model_diagnostics"] = result.get("diagnostics") or {}

    # If this is an active TPO watch, prevent it from being buried as NO_ACTION.
    if result.get("ltf_model_state") in {"PENDING", "CONFIRMED"}:
        enriched["scenario"] = result.get("scenario")
        enriched["scenario_type"] = result.get("scenario_type")
        enriched["status"] = result.get("status")
        enriched["signal_class"] = result.get("signal_class")
        enriched["stage"] = result.get("signal_class")
        enriched["setup_type"] = "TPO_OPEN_TEST_DRIVE"
        enriched["setup_name"] = "TPO_OPEN_TEST_DRIVE"
        enriched["execution_status"] = result.get("execution_status")
        enriched["execution_model"] = result.get("execution_model")
        enriched["trigger_reason"] = result.get("trigger_reason")
        enriched["next_expected_event"] = (
            "battle_gate_evaluation" if result.get("execution_status") == "EXECUTABLE" else "ltf_model_confirmation"
        )

        if result.get("direction") in {"LONG", "SHORT"}:
            enriched["direction"] = result.get("direction")

        for key in (
            "entry_reference_price",
            "invalidation_reference_price",
            "target_reference_price",
            "stop_distance",
            "target_distance",
            "risk_reward_ratio",
            "practical_rr",
            "execution_timeframe",
            "confidence",
            "probability",
        ):
            value = result.get(key)
            if value is not None:
                enriched[key] = value

        execution = enriched.get("execution")
        if not isinstance(execution, dict):
            execution = {}
        execution.update(
            {
                "status": result.get("execution_status"),
                "model": result.get("execution_model"),
                "entry_reference_price": result.get("entry_reference_price"),
                "invalidation_reference_price": result.get("invalidation_reference_price"),
                "target_reference_price": result.get("target_reference_price"),
                "risk_reward_ratio": result.get("risk_reward_ratio"),
                "stop_distance": result.get("stop_distance"),
                "target_distance": result.get("target_distance"),
                "execution_timeframe": result.get("execution_timeframe"),
                "trigger_reason": result.get("trigger_reason"),
            }
        )
        enriched["execution"] = execution

    enriched["metadata"] = meta
    return enriched