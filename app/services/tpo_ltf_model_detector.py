from __future__ import annotations

"""
TPO LTF Model Detector for AI Market Analyst.

v1.1 purpose:
- Convert TPO Watch Bridge states into an operational live setup state.
- Keep OPEN_TEST_DRIVE contexts visible in journal/snapshot instead of allowing
  them to be buried as NO_ACTION.
- Stay conservative: most OPEN_TEST_DRIVE contexts remain WATCH.
- Promote to READY only when recent 15m structure gives:
    directional LTF model,
    valid stop,
    valid target,
    RR >= MIN_CONFIRMED_RR.

Pipeline:
TPO Watch Bridge:
    LTF_MODEL_PENDING
→ this detector:
    NO_MODEL / PENDING / CONFIRMED / REJECTED
    plus full states:
    LTF_MODEL_NO_MODEL / LTF_MODEL_PENDING / LTF_MODEL_CONFIRMED / LTF_MODEL_REJECTED
→ execution geometry
→ Battle Gate
→ Telegram hard gate

This module:
- does not call Telegram;
- does not read external data;
- does not weaken Battle Gate;
- only enriches payloads with LTF model diagnostics and, when valid, execution geometry.
"""

from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd


LTF_MODEL_DETECTOR_VERSION = "tpo-ltf-model-detector-v1.1-debuggable-blockers"

MIN_CONFIRMED_RR = 2.0
MIN_BARS = 8
RECENT_WINDOW = 16
STRUCTURE_WINDOW = 5

COMPACT_TO_FULL_STATE = {
    "NO_MODEL": "LTF_MODEL_NO_MODEL",
    "PENDING": "LTF_MODEL_PENDING",
    "CONFIRMED": "LTF_MODEL_CONFIRMED",
    "REJECTED": "LTF_MODEL_REJECTED",
}

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

    # Compact state is kept for backward compatibility.
    ltf_model_state: str = "NO_MODEL"  # NO_MODEL | PENDING | CONFIRMED | REJECTED

    # Full state is easier to grep and safer for reports/statistics.
    ltf_model_state_full: str = "LTF_MODEL_NO_MODEL"

    # Machine-readable outcome / blocker summary.
    ltf_model_outcome: str = "NO_ACTIVE_TPO_OTD_WATCH"

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
    warnings: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def set_state(self, compact_state: str, outcome: str) -> None:
        state = str(compact_state or "NO_MODEL").upper()
        self.ltf_model_state = state
        self.ltf_model_state_full = COMPACT_TO_FULL_STATE.get(state, "LTF_MODEL_UNKNOWN")
        self.ltf_model_outcome = str(outcome or "UNKNOWN").upper()

    def add_blocker(self, blocker: str, *, trigger_reason: str | None = None) -> None:
        blocker_code = str(blocker or "UNKNOWN_BLOCKER").upper()
        self.blockers.append(blocker_code)
        self.ltf_model_outcome = blocker_code
        if trigger_reason:
            self.trigger_reason = trigger_reason

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


def _symbol(payload: dict[str, Any]) -> str:
    return _s(payload.get("symbol") or payload.get("instrument"), "-")


def _metadata(payload: dict[str, Any]) -> dict[str, Any]:
    meta = payload.get("metadata")
    return meta if isinstance(meta, dict) else {}


def _first_non_empty(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return default


def _min_stop(symbol: str, price: float) -> float:
    configured = MIN_STOP_BY_SYMBOL.get(symbol)
    if configured is not None:
        return configured
    return max(abs(price) * 0.0002, 0.0001)


def _prepare_ohlc(df: pd.DataFrame | None) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    diagnostics: dict[str, Any] = {
        "input_type": type(df).__name__ if df is not None else None,
        "input_is_dataframe": isinstance(df, pd.DataFrame),
        "input_rows": None,
        "prepared_rows": None,
        "required_columns_present": False,
        "missing_columns": [],
    }

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        diagnostics["prepare_error"] = "empty_or_not_dataframe"
        return None, diagnostics

    diagnostics["input_rows"] = len(df)

    out = df.copy()
    out.columns = [str(c).lower() for c in out.columns]

    required = {"open", "high", "low", "close"}
    missing = sorted(required.difference(set(out.columns)))
    diagnostics["missing_columns"] = missing
    diagnostics["required_columns_present"] = not missing

    if missing:
        diagnostics["prepare_error"] = "missing_required_ohlc_columns"
        return None, diagnostics

    out = out.dropna(subset=["open", "high", "low", "close"])
    diagnostics["rows_after_dropna"] = len(out)

    if len(out) < MIN_BARS:
        diagnostics["prepare_error"] = "not_enough_bars"
        diagnostics["min_bars"] = MIN_BARS
        return None, diagnostics

    prepared = out.tail(RECENT_WINDOW)
    diagnostics["prepared_rows"] = len(prepared)
    diagnostics["recent_window"] = RECENT_WINDOW
    diagnostics["structure_window"] = STRUCTURE_WINDOW

    return prepared, diagnostics


def _avg_range(df: pd.DataFrame) -> float | None:
    rng = (df["high"] - df["low"]).abs()
    if rng.empty:
        return None
    value = float(rng.tail(10).mean())
    return value if value > 0 else None


def _interest_zone_price(payload: dict[str, Any]) -> float | None:
    meta = _metadata(payload)

    price = _f(
        _first_non_empty(
            payload.get("interest_zone_price"),
            payload.get("primary_interest_zone_price"),
            meta.get("interest_zone_price"),
            meta.get("primary_interest_zone_price"),
        )
    )
    if price is not None:
        return price

    zone = payload.get("primary_interest_zone")
    if isinstance(zone, dict):
        price = _f(zone.get("price") or zone.get("level"))
        if price is not None:
            return price

    zone = meta.get("primary_interest_zone")
    if isinstance(zone, dict):
        price = _f(zone.get("price") or zone.get("level"))
        if price is not None:
            return price

    return None


def _interest_zone_summary(payload: dict[str, Any]) -> dict[str, Any]:
    meta = _metadata(payload)

    zone = payload.get("primary_interest_zone")
    if not isinstance(zone, dict):
        zone = meta.get("primary_interest_zone")

    if not isinstance(zone, dict):
        zone = {}

    return {
        "zone_type": zone.get("zone_type") or zone.get("type") or payload.get("interest_zone_type") or meta.get("interest_zone_type"),
        "zone_role": zone.get("role"),
        "zone_price": _interest_zone_price(payload),
        "zone_raw": zone,
    }


def _detect_displacement(df: pd.DataFrame) -> tuple[str | None, dict[str, Any]]:
    """
    Return LONG/SHORT when the last 15m candle breaks local structure with body.

    This is deliberately simple/conservative:
    - LONG requires close above recent structure high and a meaningful body.
    - SHORT requires close below recent structure low and a meaningful body.
    """
    last = df.iloc[-1]
    prev = df.iloc[:-1].tail(STRUCTURE_WINDOW)

    if prev.empty:
        return None, {
            "displacement": "none",
            "displacement_blocker": "not_enough_previous_structure",
        }

    close = float(last["close"])
    open_ = float(last["open"])
    high = float(last["high"])
    low = float(last["low"])

    prev_high = float(prev["high"].max())
    prev_low = float(prev["low"].min())

    avg = _avg_range(df) or max(high - low, abs(close) * 0.0002)
    body = abs(close - open_)
    candle_range = abs(high - low)

    body_threshold = avg * 0.30
    body_ok = body >= body_threshold

    diagnostics = {
        "last_open": round(open_, 8),
        "last_high": round(high, 8),
        "last_low": round(low, 8),
        "last_close": round(close, 8),
        "prev_structure_high": round(prev_high, 8),
        "prev_structure_low": round(prev_low, 8),
        "avg_range": round(avg, 8),
        "last_body": round(body, 8),
        "last_range": round(candle_range, 8),
        "body_threshold": round(body_threshold, 8),
        "body_ok": bool(body_ok),
        "close_above_structure": bool(close > prev_high),
        "close_below_structure": bool(close < prev_low),
    }

    if close > prev_high and body_ok:
        diagnostics["displacement"] = "bullish_breakout"
        diagnostics["displacement_direction"] = "LONG"
        return "LONG", diagnostics

    if close < prev_low and body_ok:
        diagnostics["displacement"] = "bearish_breakdown"
        diagnostics["displacement_direction"] = "SHORT"
        return "SHORT", diagnostics

    diagnostics["displacement"] = "none"

    if not body_ok:
        diagnostics["displacement_blocker"] = "body_too_small"
    elif not (close > prev_high or close < prev_low):
        diagnostics["displacement_blocker"] = "no_structure_break"
    else:
        diagnostics["displacement_blocker"] = "unknown_no_direction"

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
            target_source = "interest_zone"
        else:
            target = last_close + (risk * 2.5)
            target_source = "synthetic_2_5r"

        reward = target - last_close

    else:
        stop = float(recent["high"].max())
        risk = stop - last_close

        if risk <= 0:
            stop = last_close + avg
            risk = avg

        if target_zone_price is not None and target_zone_price < last_close:
            target = target_zone_price
            target_source = "interest_zone"
        else:
            target = last_close - (risk * 2.5)
            target_source = "synthetic_2_5r"

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
        "target_source": target_source,
        "geometry_direction": direction,
    }


def _is_active_tpo_otd_watch(payload: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    meta = _metadata(payload)

    tpo_watch_state = _s(
        _first_non_empty(
            payload.get("tpo_watch_state"),
            meta.get("tpo_watch_state"),
        )
    )
    open_behavior = _s(
        _first_non_empty(
            payload.get("open_behavior"),
            meta.get("open_behavior"),
        )
    )
    tpo_watch_active = bool(
        _first_non_empty(
            payload.get("tpo_watch_active"),
            meta.get("tpo_watch_active"),
            default=False,
        )
    )

    diagnostics = {
        "tpo_watch_state": tpo_watch_state,
        "open_behavior": open_behavior,
        "tpo_watch_active": tpo_watch_active,
    }

    active = (
        tpo_watch_state == "LTF_MODEL_PENDING"
        and open_behavior == "OPEN_TEST_DRIVE"
        and tpo_watch_active
    )
    return active, diagnostics


def detect_ltf_model(
    payload: dict[str, Any],
    *,
    df_15m: pd.DataFrame | None = None,
) -> dict[str, Any]:
    result = LTFModelResult()
    result.set_state("NO_MODEL", "NO_ACTIVE_TPO_OTD_WATCH")

    if not isinstance(payload, dict):
        result.add_blocker(
            "PAYLOAD_IS_NOT_DICT",
            trigger_reason="payload_is_not_dict",
        )
        result.status = "NO_SETUP"
        result.signal_class = "IDLE"
        result.scenario = "NO_ACTION"
        result.scenario_type = "NO_ACTION"
        return result.to_dict()

    symbol = _symbol(payload)
    active_watch, watch_diagnostics = _is_active_tpo_otd_watch(payload)

    result.diagnostics.update(
        {
            "symbol": symbol,
            **watch_diagnostics,
            **_interest_zone_summary(payload),
        }
    )

    if not active_watch:
        result.set_state("NO_MODEL", "NO_ACTIVE_TPO_OTD_WATCH")
        result.ltf_model_type = None
        result.ltf_model_confirmed = False

        result.status = _s(payload.get("status"), "NO_SETUP")
        result.signal_class = _s(payload.get("signal_class"), "IDLE")
        result.scenario = str(payload.get("scenario") or payload.get("scenario_type") or "NO_ACTION")
        result.scenario_type = result.scenario
        result.execution_status = str(payload.get("execution_status") or "NOT_EXECUTABLE")
        result.execution_model = str(payload.get("execution_model") or "NONE")
        result.trigger_reason = "no_active_tpo_otd_watch"
        result.add_blocker("NO_ACTIVE_TPO_OTD_WATCH", trigger_reason="no_active_tpo_otd_watch")
        return result.to_dict()

    result.set_state("PENDING", "PENDING_WAITING_FOR_LTF_MODEL_CONFIRMATION")
    result.ltf_model_type = "FAILED_ACCEPTANCE_RETEST"
    result.ltf_model_confirmed = False
    result.signal_class = "WATCH"
    result.status = "WATCH"
    result.scenario = "TPO_OPEN_TEST_DRIVE_WATCH"
    result.scenario_type = "TPO_OPEN_TEST_DRIVE_WATCH"
    result.execution_status = "NOT_EXECUTABLE"
    result.execution_model = "NONE"
    result.trigger_reason = "waiting_for_ltf_model_confirmation"
    result.reasons.append(
        "TPO OPEN_TEST_DRIVE is active; waiting for directional 15m LTF confirmation."
    )

    df, prepare_diagnostics = _prepare_ohlc(df_15m)
    result.diagnostics["ohlc_prepare"] = prepare_diagnostics

    if df is None:
        result.add_blocker(
            "PENDING_MISSING_OR_INVALID_15M_OHLC",
            trigger_reason="pending_missing_or_invalid_15m_ohlc",
        )
        result.reasons.append(
            "15m OHLC is missing/invalid; keep WATCH and do not allow execution."
        )
        return result.to_dict()

    direction, displacement_diagnostics = _detect_displacement(df)
    result.diagnostics["displacement"] = displacement_diagnostics

    if direction not in {"LONG", "SHORT"}:
        result.add_blocker(
            "PENDING_NO_DIRECTIONAL_DISPLACEMENT",
            trigger_reason="pending_no_directional_displacement",
        )
        result.reasons.append(
            "No confirmed 15m displacement/BOS yet; keep WATCH, no Telegram."
        )
        return result.to_dict()

    zone_price = _interest_zone_price(payload)
    geometry = _build_geometry(
        symbol=symbol,
        direction=direction,
        df=df,
        target_zone_price=zone_price,
    )
    result.diagnostics["geometry"] = geometry

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

    result.set_state("CONFIRMED", "CONFIRMED_PENDING_EXECUTION_FILTERS")
    result.ltf_model_confirmed = True
    result.ltf_model_type = "FAILED_ACCEPTANCE_RETEST"
    result.confidence = 0.64
    result.probability = 0.64
    result.execution_model = "FAILED_ACCEPTANCE_RETEST"
    result.reasons.append(
        f"15m structure confirmed directional OPEN_TEST_DRIVE model: {direction}."
    )

    if stop_distance is None or stop_distance < min_stop:
        result.status = "WATCH"
        result.signal_class = "WATCH"
        result.execution_status = "INCOMPLETE"
        result.trigger_reason = f"confirmed_stop_too_tight:{stop_distance} < {min_stop}"
        result.add_blocker(
            "CONFIRMED_STOP_TOO_TIGHT",
            trigger_reason=result.trigger_reason,
        )
        result.diagnostics["min_stop"] = min_stop
        return result.to_dict()

    if rr is None or rr < MIN_CONFIRMED_RR:
        result.status = "WATCH"
        result.signal_class = "WATCH"
        result.execution_status = "INCOMPLETE"
        result.trigger_reason = f"confirmed_rr_too_low:{rr}"
        result.add_blocker(
            "CONFIRMED_RR_TOO_LOW",
            trigger_reason=result.trigger_reason,
        )
        result.diagnostics["min_confirmed_rr"] = MIN_CONFIRMED_RR
        return result.to_dict()

    result.status = "READY"
    result.signal_class = "READY"
    result.execution_status = "EXECUTABLE"
    result.execution_model = "FAILED_ACCEPTANCE_RETEST"
    result.trigger_reason = "ltf_model_confirmed_open_test_drive"
    result.confidence = 0.68
    result.probability = 0.68
    result.set_state("CONFIRMED", "CONFIRMED_EXECUTABLE")
    result.diagnostics["outcome"] = "CONFIRMED_EXECUTABLE"
    result.reasons.append("Execution geometry is valid; payload may continue to Battle Gate.")

    return result.to_dict()


def _merge_ltf_result_into_metadata(meta: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    for key, value in result.items():
        meta[key] = value

    meta["ltf_model_detector_version"] = result.get("version")
    meta["ltf_model_state"] = result.get("ltf_model_state")
    meta["ltf_model_state_full"] = result.get("ltf_model_state_full")
    meta["ltf_model_outcome"] = result.get("ltf_model_outcome")
    meta["ltf_model_type"] = result.get("ltf_model_type")
    meta["ltf_model_confirmed"] = result.get("ltf_model_confirmed")
    meta["ltf_model_reasons"] = result.get("reasons") or []
    meta["ltf_model_blockers"] = result.get("blockers") or []
    meta["ltf_model_warnings"] = result.get("warnings") or []
    meta["ltf_model_diagnostics"] = result.get("diagnostics") or {}

    return meta


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
    meta = _merge_ltf_result_into_metadata(meta, result)

    # Always expose diagnostic state at top-level.
    enriched["ltf_model_detector_version"] = result.get("version")
    enriched["ltf_model_state"] = result.get("ltf_model_state")
    enriched["ltf_model_state_full"] = result.get("ltf_model_state_full")
    enriched["ltf_model_outcome"] = result.get("ltf_model_outcome")
    enriched["ltf_model_type"] = result.get("ltf_model_type")
    enriched["ltf_model_confirmed"] = result.get("ltf_model_confirmed")
    enriched["ltf_model_reasons"] = result.get("reasons") or []
    enriched["ltf_model_blockers"] = result.get("blockers") or []
    enriched["ltf_model_warnings"] = result.get("warnings") or []
    enriched["ltf_model_diagnostics"] = result.get("diagnostics") or {}

    # If this is an active TPO watch, prevent it from being buried as NO_ACTION.
    #
    # Important:
    # - PENDING remains WATCH / NOT_EXECUTABLE.
    # - CONFIRMED may still be WATCH / INCOMPLETE if RR/stop is invalid.
    # - Only CONFIRMED + EXECUTABLE becomes READY.
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
            "battle_gate_evaluation"
            if result.get("execution_status") == "EXECUTABLE"
            else "ltf_model_confirmation"
        )

        if result.get("direction") in {"LONG", "SHORT"}:
            enriched["direction"] = result.get("direction")
        elif not enriched.get("direction") or str(enriched.get("direction")).upper() == "NEUTRAL":
            # Keep no fake direction while model is pending.
            enriched["direction"] = "NEUTRAL"

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
                "ltf_model_state": result.get("ltf_model_state"),
                "ltf_model_state_full": result.get("ltf_model_state_full"),
                "ltf_model_outcome": result.get("ltf_model_outcome"),
                "ltf_model_blockers": result.get("blockers") or [],
            }
        )
        enriched["execution"] = execution

    enriched["metadata"] = meta
    return enriched