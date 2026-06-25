from __future__ import annotations

"""
TPO LTF Model Detector for AI Market Analyst.

v1.4 purpose:
- Convert TPO Watch Bridge states into an operational live setup state.
- Keep OPEN_TEST_DRIVE contexts visible in journal/snapshot instead of allowing
  them to be buried as NO_ACTION.
- Stay conservative: most OPEN_TEST_DRIVE contexts remain WATCH.
- Promote to READY only when recent 15m structure gives:
    directional LTF model,
    valid stop,
    valid target,
    RR >= MIN_CONFIRMED_RR.

v1.2 upgrade:
- Detect displacement/BOS across a recent 3-5 candle window, not only the last
  15m candle.
- Detect conservative zone reclaim / failed-acceptance behavior around the
  primary interest zone.
- Keep detailed diagnostics so PENDING states explain exactly what is missing.

v1.3 safety gate:
- A synthetic 2.5R target is research geometry only. It is useful for journal
  diagnostics, but it cannot produce EXECUTABLE / READY.
- CONFIRMED LTF model + synthetic target becomes WATCH / INCOMPLETE with
  outcome CONFIRMED_NEEDS_REAL_TARGET.
- READY requires a real target zone on the correct side of entry, valid stop,
  and RR >= MIN_CONFIRMED_RR.

v1.4 target selector:
- Select the next real target zone from the full interest_zones list, not only
  primary_interest_zone.
- For LONG, target must be above entry. For SHORT, target must be below entry.
- Prefer the nearest real zone that can satisfy MIN_CONFIRMED_RR; otherwise use
  the nearest real zone and let CONFIRMED_RR_TOO_LOW block execution.
- Synthetic 2.5R remains research-only fallback and cannot produce READY.

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
import re
from typing import Any

import pandas as pd


LTF_MODEL_DETECTOR_VERSION = "tpo-ltf-model-detector-v1.6-dalton-setup-families"

MIN_CONFIRMED_RR = 2.0
MIN_BARS = 8
RECENT_WINDOW = 16
STRUCTURE_WINDOW = 5
DISPLACEMENT_LOOKBACK = 5
RECLAIM_LOOKBACK = 5
REQUIRE_REAL_TARGET_FOR_EXECUTABLE = True
SYNTHETIC_TARGET_SOURCE = "synthetic_2_5r"
REAL_TARGET_SOURCE = "interest_zone"
TARGET_SELECTOR_SOURCE = "interest_zone_selector"

ACTIVE_TPO_WATCH_STATE = "LTF_MODEL_PENDING"
OTD_WATCH_SETUPS = {
    "OPEN_TEST_DRIVE",
    "OPEN_TEST_DRIVE_CANDIDATE",
    "OPEN_TEST_DRIVE_CONFIRMED",
}
ORR_WATCH_SETUPS = {
    "OPEN_REJECTION_REVERSE",
}
OD_WATCH_SETUPS = {
    "OPEN_DRIVE",
    "OPEN_DRIVE_CONFIRMED",
}
AUCTION_BREAKOUT_WATCH_SETUPS = {
    "OPEN_AUCTION_ACCEPTED_BREAKOUT",
    "OPEN_AUCTION_OUT_OF_RANGE_ACCEPTED_BREAKOUT",
}
AUCTION_BACK_TO_VALUE_WATCH_SETUPS = {
    "OPEN_AUCTION_OUT_OF_RANGE_FAILED_ACCEPTANCE",
    "OPEN_AUCTION_FAILED_ACCEPTANCE",
    "FAILED_ACCEPTANCE_BACK_TO_VALUE",
}
VALUE_ACCEPTED_OUTSIDE_STATES = {
    "ACCEPTED_OUTSIDE_VALUE",
    "ACCEPTED_OUTSIDE_PRIOR_VALUE",
    "ACCEPTED_OUTSIDE_RANGE",
    "ACCEPTED_OUTSIDE_PRIOR_RANGE",
    "ACCEPTED_ABOVE_VALUE",
    "ACCEPTED_BELOW_VALUE",
    "ACCEPTED_BREAKOUT",
    "ACCEPTED_EXTENSION",
    "IB_ACCEPTED_EXTENSION",
}
VALUE_REJECTED_BACK_STATES = {
    "REJECTED_BACK_INTO_PRIOR_VALUE",
    "REJECTED_BACK_INTO_PRIOR_RANGE",
    "FAILED_ACCEPTANCE_INTO_PRIOR_VALUE",
    "FAILED_ACCEPTANCE_INTO_PRIOR_RANGE",
    "ACCEPTED_BACK_INSIDE_VALUE",
    "ACCEPTED_INSIDE_VALUE",
    "VALUE_ACCEPTED_INSIDE",
    "FAILED_OUTSIDE_VALUE",
}
AUCTION_OBSERVE_STATES = {
    "OBSERVE_ONLY",
    "OBSERVE_ROTATION",
    "NO_WATCH",
    "RESEARCH_ONLY",
    "BLOCKED",
}
VALUE_ACCEPTANCE_INVALIDATES_OTD = {
    "ACCEPTED_BACK_INSIDE_VALUE",
    "ACCEPTED_INSIDE_VALUE",
    "VALUE_ACCEPTED_INSIDE",
    "FAILED_OUTSIDE_VALUE",
}

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

_QUOTED_ENUM_VALUE_RE = re.compile(r":\s*['\"]([^'\"]+)['\"]")


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

    # Auction/watch context consumed from tpo_watch_bridge.py.
    # The LTF detector does not classify auction context; it only confirms
    # the execution model when the bridge has an active watch.
    tpo_watch_state: str | None = None
    tpo_watch_setup: str | None = None
    tpo_watch_active: bool | None = None
    auction_ltf_setup: str | None = None
    open_location: str | None = None
    open_behavior: str | None = None
    initial_open_behavior: str | None = None
    current_open_behavior: str | None = None
    behavior_transition: str | None = None
    value_acceptance_state: str | None = None
    value_test_occurred: bool | None = None
    value_rejection_confirmed: bool | None = None
    day_type_candidate: str | None = None
    ltf_requires_caution: bool = False
    fresh_retest_exists: bool | None = None
    fresh_failed_acceptance_exists: bool | None = None
    fresh_pullback_exists: bool | None = None

    direction: str | None = None
    expected_direction: str | None = None
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
    target_source: str | None = None
    target_zone_type: str | None = None
    target_zone_role: str | None = None
    target_zone_reason: str | None = None

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


def _unwrap_enum_value(value: Any) -> Any:
    if value is None:
        return None

    enum_value = getattr(value, "value", None)
    if enum_value is not None and not isinstance(value, (str, bytes, int, float, bool)):
        return enum_value

    return value


def _s(value: Any, default: str = "") -> str:
    if value is None:
        return default

    value = _unwrap_enum_value(value)
    text = str(value).strip()

    if not text:
        return default

    quoted_match = _QUOTED_ENUM_VALUE_RE.search(text)
    if text.startswith("<") and quoted_match:
        text = quoted_match.group(1).strip()
    elif "." in text:
        tail = text.rsplit(".", 1)[-1].strip()
        if tail:
            text = tail

    return text.upper()


def _direction_s(value: Any, default: str = "") -> str:
    text = _s(value, default)

    if not text:
        return default

    if text in {"LONG", "BUY", "BULL", "BULLISH", "UP"}:
        return "LONG"

    if text in {"SHORT", "SELL", "BEAR", "BEARISH", "DOWN"}:
        return "SHORT"

    if text in {"NEUTRAL", "NONE", "FLAT", "NO_BIAS", "UNKNOWN"}:
        return "NEUTRAL"

    if "LONG" in text and "SHORT" not in text:
        return "LONG"

    if "SHORT" in text and "LONG" not in text:
        return "SHORT"

    return text


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off", "none", "null", ""}:
        return False
    return default


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


def _payload_direction(payload: dict[str, Any]) -> str | None:
    meta = _metadata(payload)
    direction = _direction_s(
        _first_non_empty(
            payload.get("direction"),
            meta.get("direction"),
            payload.get("raw_direction"),
            meta.get("raw_direction"),
        )
    )
    if direction in {"LONG", "SHORT", "NEUTRAL"}:
        return direction

    scenario_direction = _direction_s(
        _first_non_empty(
            payload.get("scenario"),
            payload.get("scenario_type"),
            meta.get("scenario"),
            meta.get("scenario_type"),
        )
    )
    if scenario_direction in {"LONG", "SHORT", "NEUTRAL"}:
        return scenario_direction

    return None


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
    diagnostics["displacement_lookback"] = DISPLACEMENT_LOOKBACK
    diagnostics["reclaim_lookback"] = RECLAIM_LOOKBACK

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



def _zone_price_from_dict(zone: dict[str, Any]) -> float | None:
    return _f(_first_non_empty(zone.get("price"), zone.get("level"), zone.get("value")))


def _zone_type_from_dict(zone: dict[str, Any]) -> str | None:
    value = _first_non_empty(zone.get("zone_type"), zone.get("type"), zone.get("name"))
    return _s(value) if value is not None else None


def _zone_role_from_dict(zone: dict[str, Any]) -> str | None:
    value = _first_non_empty(zone.get("role"), zone.get("zone_role"))
    return _s(value) if value is not None else None


def _append_zone_candidate(out: list[dict[str, Any]], zone: Any, *, source: str) -> None:
    if not isinstance(zone, dict) or not zone:
        return

    price = _zone_price_from_dict(zone)
    if price is None:
        return

    zone_type = _zone_type_from_dict(zone)
    zone_role = _zone_role_from_dict(zone)

    candidate = {
        "source": source,
        "zone_type": zone_type,
        "zone_role": zone_role,
        "price": float(price),
        "reason": zone.get("reason"),
        "reaction": zone.get("reaction"),
        "distance": _f(zone.get("distance")),
        "raw": dict(zone),
    }
    out.append(candidate)


def _extract_interest_zones(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Return normalized target-zone candidates from payload and metadata.

    v1.3 used only primary_interest_zone. v1.4 uses full interest_zones where
    available, while still including primary_interest_zone as a candidate.
    """
    meta = _metadata(payload)
    zones: list[dict[str, Any]] = []

    for source_name, source in (("payload", payload), ("metadata", meta)):
        primary = source.get("primary_interest_zone")
        _append_zone_candidate(zones, primary, source=f"{source_name}.primary_interest_zone")

        one = source.get("interest_zone")
        _append_zone_candidate(zones, one, source=f"{source_name}.interest_zone")

        seq = source.get("interest_zones")
        if isinstance(seq, list):
            for index, zone in enumerate(seq):
                _append_zone_candidate(zones, zone, source=f"{source_name}.interest_zones[{index}]")

    # De-duplicate by type/role/price while preserving order.
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str | None, float]] = set()
    for zone in zones:
        price = float(zone["price"])
        key = (zone.get("zone_type"), zone.get("zone_role"), round(price, 8))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(zone)

    return deduped


def _target_zone_priority(zone_type: str | None) -> int:
    # Lower is better when distances/RR are comparable.
    z = _s(zone_type)
    priority = {
        "NPOC": 10,
        "POC": 20,
        "VAH": 30,
        "VAL": 30,
        "PREVIOUS_HIGH": 40,
        "PREVIOUS_LOW": 40,
        "SESSION_HIGH": 50,
        "SESSION_LOW": 50,
    }
    return priority.get(z, 90)


def _select_real_target_zone(
    payload: dict[str, Any],
    *,
    direction: str,
    entry: float,
    risk: float,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """
    Select a real target zone on the correct side of entry.

    Rule:
    - LONG: zone price must be above entry.
    - SHORT: zone price must be below entry.
    - Prefer nearest zone that satisfies MIN_CONFIRMED_RR.
    - If no zone satisfies RR, return nearest real zone and let RR filter block.
    """
    zones = _extract_interest_zones(payload)
    diagnostics: dict[str, Any] = {
        "direction": direction,
        "entry": round(float(entry), 8),
        "risk": round(float(risk), 8),
        "zones_total": len(zones),
        "candidates": [],
        "valid_side_candidates": [],
        "selection_rule": "nearest_real_zone_meeting_min_rr_else_nearest_real_zone",
    }

    if risk <= 0:
        diagnostics["selected"] = None
        diagnostics["blocker"] = "invalid_risk"
        return None, diagnostics

    valid: list[dict[str, Any]] = []

    for zone in zones:
        price = float(zone["price"])
        if direction == "LONG":
            correct_side = price > entry
            reward = price - entry
        else:
            correct_side = price < entry
            reward = entry - price

        rr = reward / risk if risk > 0 else None
        item = {
            "source": zone.get("source"),
            "zone_type": zone.get("zone_type"),
            "zone_role": zone.get("zone_role"),
            "price": round(price, 8),
            "reason": zone.get("reason"),
            "correct_side": bool(correct_side),
            "reward": round(reward, 8),
            "rr": round(rr, 4) if rr is not None else None,
            "priority": _target_zone_priority(zone.get("zone_type")),
        }
        diagnostics["candidates"].append(item)

        if correct_side and reward > 0:
            enriched = dict(zone)
            enriched.update(item)
            valid.append(enriched)
            diagnostics["valid_side_candidates"].append(item)

    if not valid:
        diagnostics["selected"] = None
        diagnostics["blocker"] = "no_real_target_zone_on_correct_side"
        return None, diagnostics

    min_rr_candidates = [z for z in valid if (z.get("rr") is not None and float(z["rr"]) >= MIN_CONFIRMED_RR)]

    if min_rr_candidates:
        selected = sorted(
            min_rr_candidates,
            key=lambda z: (float(z["reward"]), int(z["priority"])),
        )[0]
        diagnostics["selected_reason"] = "nearest_real_zone_meeting_min_rr"
    else:
        selected = sorted(
            valid,
            key=lambda z: (float(z["reward"]), int(z["priority"])),
        )[0]
        diagnostics["selected_reason"] = "nearest_real_zone_but_rr_may_be_low"

    diagnostics["selected"] = {
        "source": selected.get("source"),
        "zone_type": selected.get("zone_type"),
        "zone_role": selected.get("zone_role"),
        "price": round(float(selected["price"]), 8),
        "reason": selected.get("reason"),
        "reward": round(float(selected["reward"]), 8),
        "rr": round(float(selected["rr"]), 4) if selected.get("rr") is not None else None,
        "priority": int(selected.get("priority", 90)),
    }
    return selected, diagnostics


def _row_snapshot(row: pd.Series) -> dict[str, float]:
    return {
        "open": round(float(row["open"]), 8),
        "high": round(float(row["high"]), 8),
        "low": round(float(row["low"]), 8),
        "close": round(float(row["close"]), 8),
    }


def _detect_last_candle_structure_break(
    df: pd.DataFrame,
    *,
    avg: float,
    expected_direction: str | None = None,
) -> tuple[str | None, dict[str, Any]]:
    last = df.iloc[-1]
    prev = df.iloc[:-1].tail(STRUCTURE_WINDOW)

    if prev.empty:
        return None, {
            "method": "last_candle_structure_break",
            "displacement": "none",
            "displacement_blocker": "not_enough_previous_structure",
        }

    close = float(last["close"])
    open_ = float(last["open"])
    high = float(last["high"])
    low = float(last["low"])

    prev_high = float(prev["high"].max())
    prev_low = float(prev["low"].min())

    body = abs(close - open_)
    candle_range = abs(high - low)

    body_threshold = avg * 0.30
    body_ok = body >= body_threshold
    close_above_structure = close > prev_high
    close_below_structure = close < prev_low

    diagnostics = {
        "method": "last_candle_structure_break",
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
        "close_above_structure": bool(close_above_structure),
        "close_below_structure": bool(close_below_structure),
        "expected_direction": expected_direction,
    }

    if close_above_structure and body_ok:
        diagnostics["displacement"] = "bullish_breakout"
        diagnostics["displacement_direction"] = "LONG"
        return "LONG", diagnostics

    if close_below_structure and body_ok:
        diagnostics["displacement"] = "bearish_breakdown"
        diagnostics["displacement_direction"] = "SHORT"
        return "SHORT", diagnostics

    diagnostics["displacement"] = "none"

    if not body_ok:
        diagnostics["displacement_blocker"] = "body_too_small"
    elif not (close_above_structure or close_below_structure):
        diagnostics["displacement_blocker"] = "no_structure_break"
    else:
        diagnostics["displacement_blocker"] = "unknown_no_direction"

    return None, diagnostics


def _detect_window_structure_break(
    df: pd.DataFrame,
    *,
    avg: float,
    expected_direction: str | None = None,
) -> tuple[str | None, dict[str, Any]]:
    """
    Detect a structure break that happened during the recent candle window.

    This fixes the v1.1 blind spot where the last candle could be a small pause
    after a valid displacement candle, causing a false PENDING/body_too_small.
    """
    rows = df.tail(DISPLACEMENT_LOOKBACK)
    diagnostics: dict[str, Any] = {
        "method": "window_structure_break",
        "lookback": DISPLACEMENT_LOOKBACK,
        "expected_direction": expected_direction,
        "avg_range": round(avg, 8),
        "body_threshold": round(avg * 0.30, 8),
        "candidates_checked": [],
    }

    if len(df) < STRUCTURE_WINDOW + 2 or rows.empty:
        diagnostics["displacement"] = "none"
        diagnostics["displacement_blocker"] = "not_enough_window_structure"
        return None, diagnostics

    candidate_indices = list(rows.index)

    for index in candidate_indices:
        pos = df.index.get_loc(index)
        if isinstance(pos, slice):
            continue
        if pos < STRUCTURE_WINDOW:
            continue

        row = df.loc[index]
        prev = df.iloc[:pos].tail(STRUCTURE_WINDOW)
        if prev.empty:
            continue

        close = float(row["close"])
        open_ = float(row["open"])
        high = float(row["high"])
        low = float(row["low"])
        body = abs(close - open_)
        candle_range = abs(high - low)
        body_threshold = avg * 0.30
        body_ok = body >= body_threshold
        prev_high = float(prev["high"].max())
        prev_low = float(prev["low"].min())
        close_above_structure = close > prev_high
        close_below_structure = close < prev_low

        candidate = {
            "index": str(index),
            "open": round(open_, 8),
            "high": round(high, 8),
            "low": round(low, 8),
            "close": round(close, 8),
            "body": round(body, 8),
            "range": round(candle_range, 8),
            "body_ok": bool(body_ok),
            "prev_structure_high": round(prev_high, 8),
            "prev_structure_low": round(prev_low, 8),
            "close_above_structure": bool(close_above_structure),
            "close_below_structure": bool(close_below_structure),
        }
        diagnostics["candidates_checked"].append(candidate)

        direction: str | None = None
        if close_above_structure and body_ok:
            direction = "LONG"
        elif close_below_structure and body_ok:
            direction = "SHORT"

        if direction is None:
            continue

        if expected_direction in {"LONG", "SHORT"} and direction != expected_direction:
            candidate["rejected_reason"] = "against_expected_direction"
            continue

        last_close = float(df.iloc[-1]["close"])
        last_hold_tolerance = avg * 0.35

        if direction == "LONG":
            holding_after_break = last_close >= (prev_high - last_hold_tolerance)
        else:
            holding_after_break = last_close <= (prev_low + last_hold_tolerance)

        candidate["holding_after_break"] = bool(holding_after_break)
        candidate["last_close"] = round(last_close, 8)
        candidate["hold_tolerance"] = round(last_hold_tolerance, 8)

        if not holding_after_break:
            candidate["rejected_reason"] = "break_not_held"
            continue

        diagnostics["displacement"] = "window_structure_break"
        diagnostics["displacement_direction"] = direction
        diagnostics["selected_candidate"] = candidate
        return direction, diagnostics

    diagnostics["displacement"] = "none"

    if diagnostics["candidates_checked"]:
        if any(c.get("body_ok") is False for c in diagnostics["candidates_checked"]):
            diagnostics["displacement_blocker"] = "window_body_too_small_or_no_valid_break"
        else:
            diagnostics["displacement_blocker"] = "window_no_structure_break"
    else:
        diagnostics["displacement_blocker"] = "no_window_candidates"

    return None, diagnostics


def _detect_zone_reclaim(
    df: pd.DataFrame,
    *,
    avg: float,
    expected_direction: str | None,
    zone_price: float | None,
) -> tuple[str | None, dict[str, Any]]:
    """
    Conservative failed-acceptance/reclaim detector around the primary zone.

    This is not a standalone entry trigger. It only confirms the LTF model when:
    - there is an expected direction from TPO/HTF context,
    - price tested or swept the reference zone recently,
    - price reclaimed/held the correct side of the zone,
    - the recent multi-candle move has meaningful directional progress.
    """
    diagnostics: dict[str, Any] = {
        "method": "zone_reclaim_window",
        "lookback": RECLAIM_LOOKBACK,
        "expected_direction": expected_direction,
        "zone_price": round(zone_price, 8) if zone_price is not None else None,
        "avg_range": round(avg, 8),
    }

    if expected_direction not in {"LONG", "SHORT"}:
        diagnostics["reclaim"] = "none"
        diagnostics["reclaim_blocker"] = "missing_expected_direction"
        return None, diagnostics

    if zone_price is None:
        diagnostics["reclaim"] = "none"
        diagnostics["reclaim_blocker"] = "missing_zone_price"
        return None, diagnostics

    recent = df.tail(RECLAIM_LOOKBACK)
    if len(recent) < 3:
        diagnostics["reclaim"] = "none"
        diagnostics["reclaim_blocker"] = "not_enough_reclaim_window"
        return None, diagnostics

    first_open = float(recent.iloc[0]["open"])
    last_close = float(recent.iloc[-1]["close"])
    recent_high = float(recent["high"].max())
    recent_low = float(recent["low"].min())

    closes = [float(x) for x in recent["close"].tolist()]
    tolerance = max(avg * 0.20, abs(zone_price) * 0.00003)
    min_progress = avg * 0.55

    if expected_direction == "LONG":
        zone_tested = recent_low <= zone_price + tolerance
        reclaimed = last_close > zone_price + tolerance
        closes_on_correct_side = sum(1 for close in closes if close > zone_price)
        directional_progress = last_close - first_open
        progress_ok = directional_progress >= min_progress
        direction = "LONG"
    else:
        zone_tested = recent_high >= zone_price - tolerance
        reclaimed = last_close < zone_price - tolerance
        closes_on_correct_side = sum(1 for close in closes if close < zone_price)
        directional_progress = first_open - last_close
        progress_ok = directional_progress >= min_progress
        direction = "SHORT"

    diagnostics.update(
        {
            "first_open": round(first_open, 8),
            "last_close": round(last_close, 8),
            "recent_high": round(recent_high, 8),
            "recent_low": round(recent_low, 8),
            "tolerance": round(tolerance, 8),
            "min_progress": round(min_progress, 8),
            "directional_progress": round(directional_progress, 8),
            "zone_tested": bool(zone_tested),
            "reclaimed": bool(reclaimed),
            "closes_on_correct_side": int(closes_on_correct_side),
            "progress_ok": bool(progress_ok),
        }
    )

    if zone_tested and reclaimed and closes_on_correct_side >= 2 and progress_ok:
        diagnostics["reclaim"] = "confirmed_failed_acceptance_reclaim"
        diagnostics["displacement"] = "zone_reclaim_window"
        diagnostics["displacement_direction"] = direction
        return direction, diagnostics

    diagnostics["reclaim"] = "none"

    blockers: list[str] = []
    if not zone_tested:
        blockers.append("zone_not_tested")
    if not reclaimed:
        blockers.append("zone_not_reclaimed")
    if closes_on_correct_side < 2:
        blockers.append("not_enough_closes_on_correct_side")
    if not progress_ok:
        blockers.append("directional_progress_too_small")

    diagnostics["reclaim_blocker"] = "+".join(blockers) if blockers else "unknown_reclaim_blocker"
    return None, diagnostics


def _detect_displacement(
    df: pd.DataFrame,
    *,
    expected_direction: str | None = None,
    zone_price: float | None = None,
) -> tuple[str | None, dict[str, Any]]:
    """
    Return LONG/SHORT when recent 15m structure confirms an OTD LTF model.

    v1.2 order:
    1. Last-candle structure break, same as v1.1.
    2. Windowed structure break across the last 3-5 candles.
    3. Conservative zone reclaim / failed acceptance around the interest zone.
    """
    last = df.iloc[-1]
    high = float(last["high"])
    low = float(last["low"])
    close = float(last["close"])

    avg = _avg_range(df) or max(high - low, abs(close) * 0.0002)

    diagnostics: dict[str, Any] = {
        "detector": "windowed_displacement_reclaim",
        "expected_direction": expected_direction,
        "zone_price": round(zone_price, 8) if zone_price is not None else None,
        "avg_range": round(avg, 8),
        "last_candle": _row_snapshot(last),
    }

    last_direction, last_diag = _detect_last_candle_structure_break(
        df,
        avg=avg,
        expected_direction=expected_direction,
    )
    diagnostics["last_candle_structure_break"] = last_diag

    if last_direction in {"LONG", "SHORT"}:
        if expected_direction in {"LONG", "SHORT"} and last_direction != expected_direction:
            diagnostics["displacement"] = "none"
            diagnostics["displacement_blocker"] = "last_candle_break_against_expected_direction"
            diagnostics["candidate_direction"] = last_direction
            return None, diagnostics

        diagnostics["displacement"] = last_diag.get("displacement")
        diagnostics["displacement_direction"] = last_direction
        diagnostics["selected_method"] = "last_candle_structure_break"
        return last_direction, diagnostics

    window_direction, window_diag = _detect_window_structure_break(
        df,
        avg=avg,
        expected_direction=expected_direction,
    )
    diagnostics["window_structure_break"] = window_diag

    if window_direction in {"LONG", "SHORT"}:
        diagnostics["displacement"] = window_diag.get("displacement")
        diagnostics["displacement_direction"] = window_direction
        diagnostics["selected_method"] = "window_structure_break"
        return window_direction, diagnostics

    reclaim_direction, reclaim_diag = _detect_zone_reclaim(
        df,
        avg=avg,
        expected_direction=expected_direction,
        zone_price=zone_price,
    )
    diagnostics["zone_reclaim_window"] = reclaim_diag

    if reclaim_direction in {"LONG", "SHORT"}:
        diagnostics["displacement"] = reclaim_diag.get("displacement")
        diagnostics["displacement_direction"] = reclaim_direction
        diagnostics["selected_method"] = "zone_reclaim_window"
        return reclaim_direction, diagnostics

    diagnostics["displacement"] = "none"

    blockers = [
        last_diag.get("displacement_blocker"),
        window_diag.get("displacement_blocker"),
        reclaim_diag.get("reclaim_blocker"),
    ]
    diagnostics["displacement_blocker"] = " | ".join(str(x) for x in blockers if x) or "no_confirmed_windowed_model"
    return None, diagnostics


def _build_geometry(
    *,
    symbol: str,
    direction: str,
    df: pd.DataFrame,
    payload: dict[str, Any],
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

        selected_zone, target_selector = _select_real_target_zone(
            payload,
            direction=direction,
            entry=last_close,
            risk=risk,
        )

        if selected_zone is not None:
            target = float(selected_zone["price"])
            target_source = REAL_TARGET_SOURCE
        elif target_zone_price is not None and target_zone_price > last_close:
            # Backward-compatible fallback when a valid single zone price is supplied
            # but the full interest_zones list is missing.
            target = target_zone_price
            target_source = REAL_TARGET_SOURCE
            target_selector["selected_reason"] = "fallback_primary_zone_price_on_correct_side"
            target_selector["selected"] = {
                "source": "fallback_primary_zone_price",
                "zone_type": None,
                "zone_role": None,
                "price": round(float(target), 8),
                "reason": None,
                "reward": round(float(target - last_close), 8),
                "rr": round(float((target - last_close) / risk), 4) if risk > 0 else None,
                "priority": 95,
            }
        else:
            target = last_close + (risk * 2.5)
            target_source = SYNTHETIC_TARGET_SOURCE

        reward = target - last_close

    else:
        stop = float(recent["high"].max())
        risk = stop - last_close

        if risk <= 0:
            stop = last_close + avg
            risk = avg

        selected_zone, target_selector = _select_real_target_zone(
            payload,
            direction=direction,
            entry=last_close,
            risk=risk,
        )

        if selected_zone is not None:
            target = float(selected_zone["price"])
            target_source = REAL_TARGET_SOURCE
        elif target_zone_price is not None and target_zone_price < last_close:
            # Backward-compatible fallback when a valid single zone price is supplied
            # but the full interest_zones list is missing.
            target = target_zone_price
            target_source = REAL_TARGET_SOURCE
            target_selector["selected_reason"] = "fallback_primary_zone_price_on_correct_side"
            target_selector["selected"] = {
                "source": "fallback_primary_zone_price",
                "zone_type": None,
                "zone_role": None,
                "price": round(float(target), 8),
                "reason": None,
                "reward": round(float(last_close - target), 8),
                "rr": round(float((last_close - target) / risk), 4) if risk > 0 else None,
                "priority": 95,
            }
        else:
            target = last_close - (risk * 2.5)
            target_source = SYNTHETIC_TARGET_SOURCE

        reward = last_close - target

    rr = reward / risk if risk > 0 else None
    selected_summary = target_selector.get("selected") if isinstance(target_selector, dict) else None

    return {
        "entry_reference_price": round(last_close, 8),
        "invalidation_reference_price": round(stop, 8),
        "target_reference_price": round(target, 8),
        "stop_distance": round(risk, 8),
        "target_distance": round(reward, 8),
        "risk_reward_ratio": round(rr, 4) if rr is not None else None,
        "practical_rr": round(rr, 4) if rr is not None else None,
        "target_source": target_source,
        "target_zone_type": selected_summary.get("zone_type") if isinstance(selected_summary, dict) else None,
        "target_zone_role": selected_summary.get("zone_role") if isinstance(selected_summary, dict) else None,
        "target_zone_reason": selected_summary.get("reason") if isinstance(selected_summary, dict) else None,
        "target_selector": target_selector,
        "geometry_direction": direction,
    }

def _scalar_from_sources(payload: dict[str, Any], *keys: str) -> Any:
    """Read the first scalar value from root, metadata and nested context/open_behavior."""
    meta = _metadata(payload)

    sources: list[Any] = [payload, meta]

    for source in (payload, meta):
        if not isinstance(source, dict):
            continue
        for nested_key in ("context", "filters", "open_behavior", "auction_state"):
            nested = source.get(nested_key)
            if isinstance(nested, dict):
                sources.append(nested)

    for key in keys:
        for source in sources:
            if not isinstance(source, dict):
                continue
            value = source.get(key)
            if value in (None, "", [], {}):
                continue
            if isinstance(value, dict):
                continue
            return value

    return None


def _auction_watch_context(payload: dict[str, Any]) -> dict[str, Any]:
    tpo_watch_state = _s(_scalar_from_sources(payload, "tpo_watch_state"))
    tpo_watch_setup = _s(_scalar_from_sources(payload, "tpo_watch_setup"))
    tpo_watch_active = _bool(_scalar_from_sources(payload, "tpo_watch_active"), default=False)

    open_behavior = _s(_scalar_from_sources(payload, "open_behavior"))
    initial_open_behavior = _s(_scalar_from_sources(payload, "initial_open_behavior"))
    current_open_behavior = _s(_scalar_from_sources(payload, "current_open_behavior", "updated_open_behavior"))
    value_acceptance_state = _s(_scalar_from_sources(payload, "value_acceptance_state"))
    value_rejection_confirmed = _bool(_scalar_from_sources(payload, "value_rejection_confirmed"), default=False)

    candidates = [
        tpo_watch_setup,
        current_open_behavior,
        open_behavior,
        initial_open_behavior,
    ]

    auction_ltf_setup: str | None = None
    if any(value in OD_WATCH_SETUPS for value in candidates):
        auction_ltf_setup = "OPEN_DRIVE"
    elif any(value in OTD_WATCH_SETUPS for value in candidates):
        auction_ltf_setup = "OPEN_TEST_DRIVE"
    elif any(value in ORR_WATCH_SETUPS for value in candidates):
        auction_ltf_setup = "OPEN_REJECTION_REVERSE"
    elif any(value in AUCTION_BREAKOUT_WATCH_SETUPS for value in candidates) or value_acceptance_state in VALUE_ACCEPTED_OUTSIDE_STATES:
        auction_ltf_setup = "OPEN_AUCTION_BREAKOUT"
    elif any(value in AUCTION_BACK_TO_VALUE_WATCH_SETUPS for value in candidates) or value_acceptance_state in VALUE_REJECTED_BACK_STATES:
        auction_ltf_setup = "OPEN_AUCTION_BACK_TO_VALUE"

    diagnostics = {
        "tpo_watch_state": tpo_watch_state,
        "tpo_watch_setup": tpo_watch_setup,
        "tpo_watch_active": tpo_watch_active,
        "auction_ltf_setup": auction_ltf_setup,
        "open_location": _s(_scalar_from_sources(payload, "open_location")) or None,
        "open_behavior": open_behavior,
        "initial_open_behavior": initial_open_behavior,
        "current_open_behavior": current_open_behavior,
        "behavior_transition": _s(_scalar_from_sources(payload, "behavior_transition")) or None,
        "value_acceptance_state": value_acceptance_state,
        "value_test_occurred": _bool(_scalar_from_sources(payload, "value_test_occurred"), default=False),
        "value_rejection_confirmed": value_rejection_confirmed,
        "day_type_candidate": _s(_scalar_from_sources(payload, "day_type_candidate")) or None,
    }

    if tpo_watch_state != ACTIVE_TPO_WATCH_STATE:
        diagnostics["active_watch"] = False
        diagnostics["watch_blocker"] = f"tpo_watch_state_not_pending:{tpo_watch_state or 'missing'}"
        return diagnostics

    if not tpo_watch_active:
        diagnostics["active_watch"] = False
        diagnostics["watch_blocker"] = "tpo_watch_not_active"
        return diagnostics

    if auction_ltf_setup is None:
        diagnostics["active_watch"] = False
        diagnostics["watch_blocker"] = "unsupported_auction_watch_setup"
        return diagnostics

    if (
        auction_ltf_setup == "OPEN_TEST_DRIVE"
        and value_acceptance_state in VALUE_ACCEPTANCE_INVALIDATES_OTD
        and not value_rejection_confirmed
    ):
        diagnostics["active_watch"] = False
        diagnostics["watch_blocker"] = f"otd_invalidated_by_value_acceptance:{value_acceptance_state}"
        return diagnostics

    if auction_ltf_setup == "OPEN_AUCTION_BREAKOUT" and value_acceptance_state not in VALUE_ACCEPTED_OUTSIDE_STATES:
        diagnostics["active_watch"] = False
        diagnostics["watch_blocker"] = f"open_auction_breakout_without_acceptance:{value_acceptance_state or 'missing'}"
        return diagnostics

    if auction_ltf_setup == "OPEN_AUCTION_BACK_TO_VALUE" and value_acceptance_state not in VALUE_REJECTED_BACK_STATES:
        diagnostics["active_watch"] = False
        diagnostics["watch_blocker"] = f"open_auction_back_to_value_without_failed_acceptance:{value_acceptance_state or 'missing'}"
        return diagnostics

    diagnostics["active_watch"] = True
    diagnostics["watch_blocker"] = None
    return diagnostics


def _is_active_tpo_auction_watch(payload: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    diagnostics = _auction_watch_context(payload)
    return bool(diagnostics.get("active_watch")), diagnostics


def _auction_setup_profile(setup_family: str | None) -> dict[str, Any]:
    """Return operational labels for each Dalton-style auction setup family."""
    family = _s(setup_family or "OPEN_TEST_DRIVE")

    profiles: dict[str, dict[str, Any]] = {
        "OPEN_DRIVE": {
            "watch_scenario": "TPO_OPEN_DRIVE_WATCH",
            "pending_model_type": "PULLBACK_CONTINUATION",
            "pending_execution_model": "PULLBACK_CONTINUATION",
            "watch_reason": (
                "TPO OPEN_DRIVE is active; wait for early continuation or pullback/retest hold, "
                "never late chase after the first impulse is gone."
            ),
            "confirmed_scenario_prefix": "TPO_OPEN_DRIVE",
            "confirmed_model_type": "PULLBACK_CONTINUATION",
            "confirmed_execution_model": "PULLBACK_CONTINUATION",
            "confirmed_label": "OPEN_DRIVE",
            "pending_confidence": 0.56,
            "confirmed_confidence": 0.70,
            "ready_confidence": 0.72,
            "requires_caution": False,
        },
        "OPEN_TEST_DRIVE": {
            "watch_scenario": "TPO_OPEN_TEST_DRIVE_WATCH",
            "pending_model_type": "FAILED_ACCEPTANCE_RETEST",
            "pending_execution_model": "FAILED_ACCEPTANCE_RETEST",
            "watch_reason": (
                "TPO OPEN_TEST_DRIVE auction-state is active; waiting for failed probe/reclaim "
                "and directional 15m confirmation."
            ),
            "confirmed_scenario_prefix": "TPO_OPEN_TEST_DRIVE",
            "confirmed_model_type": "FAILED_ACCEPTANCE_RETEST",
            "confirmed_execution_model": "FAILED_ACCEPTANCE_RETEST",
            "confirmed_label": "OPEN_TEST_DRIVE",
            "pending_confidence": 0.55,
            "confirmed_confidence": 0.64,
            "ready_confidence": 0.68,
            "requires_caution": False,
        },
        "OPEN_REJECTION_REVERSE": {
            "watch_scenario": "TPO_OPEN_REJECTION_REVERSE_WATCH",
            "pending_model_type": "RECLAIM_BOS_RETEST",
            "pending_execution_model": "RECLAIM_BOS_RETEST",
            "watch_reason": (
                "TPO OPEN_REJECTION_REVERSE is active; waiting for a very clean 15m reclaim/BOS/retest. "
                "ORR has lower conviction than OD/OTD, so no chase."
            ),
            "confirmed_scenario_prefix": "TPO_OPEN_REJECTION_REVERSE",
            "confirmed_model_type": "RECLAIM_BOS_RETEST",
            "confirmed_execution_model": "RECLAIM_BOS_RETEST",
            "confirmed_label": "OPEN_REJECTION_REVERSE",
            "pending_confidence": 0.53,
            "confirmed_confidence": 0.62,
            "ready_confidence": 0.66,
            "requires_caution": True,
        },
        "OPEN_AUCTION_BREAKOUT": {
            "watch_scenario": "TPO_OPEN_AUCTION_BREAKOUT_WATCH",
            "pending_model_type": "ACCEPTED_BREAKOUT_RETEST",
            "pending_execution_model": "ACCEPTED_BREAKOUT_RETEST",
            "watch_reason": (
                "Open auction selected accepted-breakout branch; wait for hold/retest outside old value/range."
            ),
            "confirmed_scenario_prefix": "TPO_OPEN_AUCTION_BREAKOUT",
            "confirmed_model_type": "ACCEPTED_BREAKOUT_RETEST",
            "confirmed_execution_model": "ACCEPTED_BREAKOUT_RETEST",
            "confirmed_label": "OPEN_AUCTION_BREAKOUT",
            "pending_confidence": 0.54,
            "confirmed_confidence": 0.63,
            "ready_confidence": 0.67,
            "requires_caution": False,
        },
        "OPEN_AUCTION_BACK_TO_VALUE": {
            "watch_scenario": "TPO_OPEN_AUCTION_BACK_TO_VALUE_WATCH",
            "pending_model_type": "FAILED_ACCEPTANCE_BACK_TO_VALUE",
            "pending_execution_model": "FAILED_ACCEPTANCE_BACK_TO_VALUE",
            "watch_reason": (
                "Open auction out of range/value failed to build acceptance outside; waiting for back-to-value LTF confirmation."
            ),
            "confirmed_scenario_prefix": "TPO_OPEN_AUCTION_BACK_TO_VALUE",
            "confirmed_model_type": "FAILED_ACCEPTANCE_BACK_TO_VALUE",
            "confirmed_execution_model": "FAILED_ACCEPTANCE_BACK_TO_VALUE",
            "confirmed_label": "OPEN_AUCTION_BACK_TO_VALUE",
            "pending_confidence": 0.52,
            "confirmed_confidence": 0.61,
            "ready_confidence": 0.65,
            "requires_caution": True,
        },
    }

    return profiles.get(family, profiles["OPEN_TEST_DRIVE"])


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
    expected_direction = _payload_direction(payload)
    if expected_direction == "NEUTRAL":
        expected_direction = None

    result.expected_direction = expected_direction
    active_watch, watch_diagnostics = _is_active_tpo_auction_watch(payload)

    result.tpo_watch_state = watch_diagnostics.get("tpo_watch_state")
    result.tpo_watch_setup = watch_diagnostics.get("tpo_watch_setup")
    result.tpo_watch_active = watch_diagnostics.get("tpo_watch_active")
    result.auction_ltf_setup = watch_diagnostics.get("auction_ltf_setup")
    result.open_location = watch_diagnostics.get("open_location")
    result.open_behavior = watch_diagnostics.get("open_behavior")
    result.initial_open_behavior = watch_diagnostics.get("initial_open_behavior")
    result.current_open_behavior = watch_diagnostics.get("current_open_behavior")
    result.behavior_transition = watch_diagnostics.get("behavior_transition")
    result.value_acceptance_state = watch_diagnostics.get("value_acceptance_state")
    result.value_test_occurred = watch_diagnostics.get("value_test_occurred")
    result.value_rejection_confirmed = watch_diagnostics.get("value_rejection_confirmed")
    result.day_type_candidate = watch_diagnostics.get("day_type_candidate")
    result.ltf_requires_caution = bool(_auction_setup_profile(result.auction_ltf_setup).get("requires_caution"))

    if result.ltf_requires_caution:
        if result.auction_ltf_setup == "OPEN_REJECTION_REVERSE":
            result.warnings.append("orr_requires_caution")
        elif result.auction_ltf_setup == "OPEN_AUCTION_BACK_TO_VALUE":
            result.warnings.append("open_auction_back_to_value_requires_caution")
        else:
            result.warnings.append("auction_setup_requires_caution")

    result.diagnostics.update(
        {
            "symbol": symbol,
            "expected_direction": expected_direction,
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
        watch_blocker = str(watch_diagnostics.get("watch_blocker") or "no_active_tpo_auction_watch")
        result.trigger_reason = watch_blocker
        result.add_blocker("NO_ACTIVE_TPO_AUCTION_WATCH", trigger_reason=watch_blocker)
        result.diagnostics["watch_blocker"] = watch_blocker
        return result.to_dict()

    setup_family = result.auction_ltf_setup or "OPEN_TEST_DRIVE"
    profile = _auction_setup_profile(setup_family)
    watch_scenario = str(profile["watch_scenario"])
    pending_model_type = str(profile["pending_model_type"])
    pending_execution_model = str(profile["pending_execution_model"])
    watch_reason = str(profile["watch_reason"])

    result.set_state("PENDING", "PENDING_WAITING_FOR_LTF_MODEL_CONFIRMATION")
    result.ltf_model_type = pending_model_type
    result.ltf_model_confirmed = False
    result.signal_class = "WATCH"
    result.status = "WATCH"
    result.scenario = watch_scenario
    result.scenario_type = watch_scenario
    result.execution_status = "NOT_EXECUTABLE"
    result.execution_model = "NONE"
    result.trigger_reason = "waiting_for_ltf_model_confirmation"
    result.reasons.append(watch_reason)

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

    zone_price = _interest_zone_price(payload)
    direction, displacement_diagnostics = _detect_displacement(
        df,
        expected_direction=expected_direction,
        zone_price=zone_price,
    )
    result.diagnostics["displacement"] = displacement_diagnostics

    if direction not in {"LONG", "SHORT"}:
        result.add_blocker(
            "PENDING_NO_DIRECTIONAL_DISPLACEMENT",
            trigger_reason="pending_no_directional_displacement",
        )
        result.reasons.append(
            "No confirmed 15m displacement/BOS/reclaim yet; keep WATCH, no Telegram."
        )
        return result.to_dict()

    if expected_direction in {"LONG", "SHORT"} and direction != expected_direction:
        result.add_blocker(
            "PENDING_DISPLACEMENT_AGAINST_EXPECTED_DIRECTION",
            trigger_reason="pending_displacement_against_expected_direction",
        )
        result.reasons.append(
            "Detected LTF movement conflicts with TPO/HTF expected direction; keep WATCH."
        )
        return result.to_dict()

    geometry = _build_geometry(
        symbol=symbol,
        direction=direction,
        df=df,
        payload=payload,
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
    result.target_source = geometry.get("target_source")
    result.target_zone_type = geometry.get("target_zone_type")
    result.target_zone_role = geometry.get("target_zone_role")
    result.target_zone_reason = geometry.get("target_zone_reason")

    confirmed_prefix = str(profile["confirmed_scenario_prefix"])
    confirmed_model_type = str(profile["confirmed_model_type"])
    confirmed_execution_model = str(profile["confirmed_execution_model"])
    confirmed_label = str(profile["confirmed_label"])
    confirmed_confidence = float(profile.get("confirmed_confidence", 0.64))
    result.scenario = f"{confirmed_prefix}_{direction}"
    result.confidence = confirmed_confidence
    result.probability = confirmed_confidence

    result.scenario_type = result.scenario

    result.set_state("CONFIRMED", "CONFIRMED_PENDING_EXECUTION_FILTERS")
    result.ltf_model_confirmed = True
    result.ltf_model_type = confirmed_model_type
    result.execution_model = confirmed_execution_model
    selected_method = displacement_diagnostics.get("selected_method") or displacement_diagnostics.get("displacement")
    result.fresh_retest_exists = True
    result.fresh_failed_acceptance_exists = bool(
        selected_method == "zone_reclaim_window" or result.value_rejection_confirmed is True
    )
    result.fresh_pullback_exists = bool(selected_method in {"window_structure_break", "last_candle_structure_break"})
    result.reasons.append(
        f"15m structure confirmed directional {confirmed_label} model: {direction} via {selected_method}."
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

    if REQUIRE_REAL_TARGET_FOR_EXECUTABLE and geometry.get("target_source") == SYNTHETIC_TARGET_SOURCE:
        result.status = "WATCH"
        result.signal_class = "WATCH"
        result.execution_status = "INCOMPLETE"
        result.trigger_reason = "confirmed_needs_real_target_zone"
        result.add_blocker(
            "CONFIRMED_NEEDS_REAL_TARGET",
            trigger_reason=result.trigger_reason,
        )
        result.diagnostics["real_target_required"] = True
        result.diagnostics["synthetic_target_not_executable"] = True
        result.reasons.append(
            "LTF model is confirmed, but target is synthetic 2.5R; keep WATCH until a real target zone is available."
        )
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
    result.execution_model = confirmed_execution_model
    result.trigger_reason = f"ltf_model_confirmed_{str(setup_family).lower()}"
    ready_confidence = float(profile.get("ready_confidence", result.confidence))
    result.confidence = ready_confidence
    result.probability = ready_confidence
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
    for key in (
        "tpo_watch_state",
        "tpo_watch_setup",
        "tpo_watch_active",
        "auction_ltf_setup",
        "open_location",
        "open_behavior",
        "initial_open_behavior",
        "current_open_behavior",
        "behavior_transition",
        "value_acceptance_state",
        "value_test_occurred",
        "value_rejection_confirmed",
        "day_type_candidate",
        "ltf_requires_caution",
        "fresh_retest_exists",
        "fresh_failed_acceptance_exists",
        "fresh_pullback_exists",
    ):
        meta[key] = result.get(key)
    meta["target_source"] = result.get("target_source")
    meta["target_zone_type"] = result.get("target_zone_type")
    meta["target_zone_role"] = result.get("target_zone_role")
    meta["target_zone_reason"] = result.get("target_zone_reason")

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
    for key in (
        "tpo_watch_state",
        "tpo_watch_setup",
        "tpo_watch_active",
        "auction_ltf_setup",
        "open_location",
        "open_behavior",
        "initial_open_behavior",
        "current_open_behavior",
        "behavior_transition",
        "value_acceptance_state",
        "value_test_occurred",
        "value_rejection_confirmed",
        "day_type_candidate",
        "ltf_requires_caution",
        "fresh_retest_exists",
        "fresh_failed_acceptance_exists",
        "fresh_pullback_exists",
    ):
        if result.get(key) is not None:
            enriched[key] = result.get(key)
    for key in ("target_source", "target_zone_type", "target_zone_role", "target_zone_reason"):
        if result.get(key) is not None:
            enriched[key] = result.get(key)

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
        setup_name_map = {
            "OPEN_DRIVE": "TPO_OPEN_DRIVE",
            "OPEN_TEST_DRIVE": "TPO_OPEN_TEST_DRIVE",
            "OPEN_REJECTION_REVERSE": "TPO_OPEN_REJECTION_REVERSE",
            "OPEN_AUCTION_BREAKOUT": "TPO_OPEN_AUCTION_BREAKOUT",
            "OPEN_AUCTION_BACK_TO_VALUE": "TPO_OPEN_AUCTION_BACK_TO_VALUE",
        }
        setup_name = setup_name_map.get(result.get("auction_ltf_setup"), "TPO_OPEN_TEST_DRIVE")
        enriched["setup_type"] = setup_name
        enriched["setup_name"] = setup_name
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
            "target_source",
            "target_zone_type",
            "target_zone_role",
            "target_zone_reason",
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
                "target_source": result.get("target_source"),
                "target_zone_type": result.get("target_zone_type"),
                "target_zone_role": result.get("target_zone_role"),
                "target_zone_reason": result.get("target_zone_reason"),
                "execution_timeframe": result.get("execution_timeframe"),
                "trigger_reason": result.get("trigger_reason"),
                "ltf_model_state": result.get("ltf_model_state"),
                "ltf_model_state_full": result.get("ltf_model_state_full"),
                "ltf_model_outcome": result.get("ltf_model_outcome"),
                "auction_ltf_setup": result.get("auction_ltf_setup"),
                "tpo_watch_state": result.get("tpo_watch_state"),
                "tpo_watch_setup": result.get("tpo_watch_setup"),
                "current_open_behavior": result.get("current_open_behavior"),
                "value_acceptance_state": result.get("value_acceptance_state"),
                "fresh_retest_exists": result.get("fresh_retest_exists"),
                "fresh_failed_acceptance_exists": result.get("fresh_failed_acceptance_exists"),
                "fresh_pullback_exists": result.get("fresh_pullback_exists"),
                "ltf_model_blockers": result.get("blockers") or [],
            }
        )
        enriched["execution"] = execution

    enriched["metadata"] = meta
    return enriched
